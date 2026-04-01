from __future__ import annotations

import time
import unittest

from polymarket_bot.idempotency import (
    CLAIMED_NEW,
    EXISTING_NON_TERMINAL,
    INTENT_STATUS_ACKED_PENDING,
    INTENT_STATUS_ACK_UNKNOWN,
    INTENT_STATUS_NEW,
    INTENT_STATUS_SENDING,
)
from polymarket_bot.runner import Trader
from polymarket_bot.types import OpenOrderSnapshot
from runtime_persistence_helpers import (
    DummyBroker,
    DummyDataClient,
    DummyRisk,
    DummyStrategy,
    build_signal,
    make_settings,
    new_tmp_dir,
)


def _make_trader(*, workdir, broker: DummyBroker | None = None, dry_run: bool = True) -> Trader:
    settings = make_settings(dry_run=dry_run, workdir=workdir)
    settings.enable_single_writer = False
    settings.decision_mode = "auto"
    return Trader(
        settings=settings,
        data_client=DummyDataClient(),
        strategy=DummyStrategy([]),
        risk=DummyRisk(),
        broker=broker or DummyBroker(),
    )


class RestartDoesNotDuplicateOrdersTests(unittest.TestCase):
    def test_restart_loads_existing_non_terminal_intent_without_new_row(self):
        workdir = new_tmp_dir()
        signal = build_signal(token_id="token-restart", side="BUY")
        trader_a = _make_trader(workdir=workdir)
        identity = trader_a._build_intent_identity(signal, 22.0)

        first_status, first_intent = trader_a._claim_or_load_intent(
            signal=signal,
            notional_usd=22.0,
            identity=identity,
        )
        self.assertEqual(first_status, CLAIMED_NEW)
        self.assertEqual(str(first_intent.get("status") or ""), INTENT_STATUS_NEW)

        moved, _ = trader_a._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=INTENT_STATUS_SENDING,
            expected_from_statuses=(INTENT_STATUS_NEW,),
        )
        self.assertTrue(moved)

        trader_b = _make_trader(workdir=workdir)
        second_status, second_intent = trader_b._claim_or_load_intent(
            signal=signal,
            notional_usd=22.0,
            identity=identity,
        )
        self.assertEqual(second_status, EXISTING_NON_TERMINAL)
        self.assertEqual(str(second_intent.get("status") or ""), INTENT_STATUS_SENDING)
        self.assertEqual(
            str(second_intent.get("strategy_order_uuid") or ""),
            str(identity["strategy_order_uuid"]),
        )
        self.assertEqual(
            str(second_intent.get("idempotency_key") or ""),
            str(identity["idempotency_key"]),
        )

        intents = trader_b._state_store.load_order_intents()
        self.assertEqual(len(intents), 1)
        self.assertEqual(
            str(intents[0].get("strategy_order_uuid") or ""),
            str(identity["strategy_order_uuid"]),
        )

    def test_broker_ack_unknown_probe_reuses_same_uuid_and_blocks_resend(self):
        workdir = new_tmp_dir()
        signal = build_signal(token_id="token-ack-unknown", side="BUY")
        trader_a = _make_trader(workdir=workdir, dry_run=False)
        identity = trader_a._build_intent_identity(signal, 18.0)

        status, _ = trader_a._claim_or_load_intent(
            signal=signal,
            notional_usd=18.0,
            identity=identity,
        )
        self.assertEqual(status, CLAIMED_NEW)

        moved, _ = trader_a._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=INTENT_STATUS_SENDING,
            expected_from_statuses=(INTENT_STATUS_NEW,),
        )
        self.assertTrue(moved)

        acked, _ = trader_a._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=INTENT_STATUS_ACK_UNKNOWN,
            broker_order_id="broker-open-1",
            payload_updates={
                "ack_unknown_count": 1,
                "ack_unknown_first_ts": int(time.time()),
            },
        )
        self.assertTrue(acked)

        open_order = OpenOrderSnapshot(
            order_id="broker-open-1",
            token_id=str(signal.token_id),
            side="BUY",
            status="OPEN",
            price=float(signal.price_hint),
            original_size=1.0,
            remaining_size=1.0,
            condition_id=str(signal.condition_id),
            market_slug=str(signal.market_slug),
        )
        trader_broker = DummyBroker(open_orders=[open_order])
        trader_b = _make_trader(workdir=workdir, broker=trader_broker, dry_run=False)

        existing_status, existing_intent = trader_b._claim_or_load_intent(
            signal=signal,
            notional_usd=18.0,
            identity=identity,
        )
        self.assertEqual(existing_status, EXISTING_NON_TERMINAL)
        self.assertEqual(str(existing_intent.get("status") or ""), INTENT_STATUS_ACK_UNKNOWN)

        duplicate = trader_b._find_broker_open_order_duplicate(signal)
        self.assertIsNotNone(duplicate)
        self.assertEqual(str(duplicate.get("order_id") or ""), "broker-open-1")

        promoted, _ = trader_b._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=INTENT_STATUS_ACKED_PENDING,
            broker_order_id="broker-open-1",
            recovery_reason="broker_open_detected_during_probe",
        )
        self.assertTrue(promoted)

        # Runner sends only after NEW -> SENDING CAS succeeds; existing ACK/RECOVERY paths must fail this CAS.
        can_resend, _ = trader_b._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=INTENT_STATUS_SENDING,
            expected_from_statuses=(INTENT_STATUS_NEW,),
        )
        self.assertFalse(can_resend)
        self.assertEqual(len(trader_broker.calls), 0)

        intents = trader_b._state_store.load_order_intents()
        self.assertEqual(len(intents), 1)
        self.assertEqual(
            str(intents[0].get("strategy_order_uuid") or ""),
            str(identity["strategy_order_uuid"]),
        )
        self.assertEqual(
            str(intents[0].get("status") or ""),
            INTENT_STATUS_ACKED_PENDING,
        )

    def test_new_and_sending_recovery_paths_are_distinct(self):
        workdir = new_tmp_dir()
        signal = build_signal(token_id="token-new-sending", side="BUY")
        trader_a = _make_trader(workdir=workdir)
        identity = trader_a._build_intent_identity(signal, 21.0)

        first_status, first_intent = trader_a._claim_or_load_intent(
            signal=signal,
            notional_usd=21.0,
            identity=identity,
        )
        self.assertEqual(first_status, CLAIMED_NEW)
        self.assertEqual(str(first_intent.get("status") or ""), INTENT_STATUS_NEW)

        trader_b = _make_trader(workdir=workdir)
        second_status, second_intent = trader_b._claim_or_load_intent(
            signal=signal,
            notional_usd=21.0,
            identity=identity,
        )
        self.assertEqual(second_status, EXISTING_NON_TERMINAL)
        self.assertEqual(str(second_intent.get("status") or ""), INTENT_STATUS_NEW)

        entered_sending, _ = trader_b._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=INTENT_STATUS_SENDING,
            expected_from_statuses=(INTENT_STATUS_NEW,),
        )
        self.assertTrue(entered_sending)

        trader_c = _make_trader(workdir=workdir)
        third_status, third_intent = trader_c._claim_or_load_intent(
            signal=signal,
            notional_usd=21.0,
            identity=identity,
        )
        self.assertEqual(third_status, EXISTING_NON_TERMINAL)
        self.assertEqual(str(third_intent.get("status") or ""), INTENT_STATUS_SENDING)

        # Recovery under SENDING must not pass NEW->SENDING CAS again, so send path cannot re-enter directly.
        resend_allowed, _ = trader_c._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=INTENT_STATUS_SENDING,
            expected_from_statuses=(INTENT_STATUS_NEW,),
        )
        self.assertFalse(resend_allowed)


if __name__ == "__main__":
    unittest.main()
