from __future__ import annotations

import unittest

from polymarket_bot.idempotency import (
    CLAIMED_NEW,
    EXISTING_NON_TERMINAL,
    INTENT_STATUS_ACK_UNKNOWN,
    INTENT_STATUS_MANUAL_REQUIRED,
    INTENT_STATUS_SENDING,
)
from polymarket_bot.runner import Trader
from runtime_persistence_helpers import DummyBroker, DummyDataClient, DummyRisk, DummyStrategy, build_signal, make_settings, new_tmp_dir


class TimeoutRetryReusesSameIntentTests(unittest.TestCase):
    def test_ack_unknown_retry_reuses_same_intent_and_never_creates_second_row(self):
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
        settings.enable_single_writer = False
        settings.ack_unknown_recovery_window_seconds = 300
        settings.ack_unknown_max_probes = 2

        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )
        signal = build_signal(token_id="token-timeout", side="BUY")
        identity = trader._build_intent_identity(signal, 15.0)

        claim_status, _ = trader._claim_or_load_intent(
            signal=signal,
            notional_usd=15.0,
            identity=identity,
        )
        self.assertEqual(claim_status, CLAIMED_NEW)

        marked, _ = trader._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=INTENT_STATUS_SENDING,
            expected_from_statuses=("new",),
        )
        self.assertTrue(marked)

        probe_one = trader._record_ack_unknown_probe(str(identity["strategy_order_uuid"]))
        trader._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=INTENT_STATUS_ACK_UNKNOWN,
            payload_updates={
                "ack_unknown_count": int(probe_one["count"]),
                "ack_unknown_first_ts": int(probe_one["first_ts"]),
            },
        )

        second_status, second_intent = trader._claim_or_load_intent(
            signal=signal,
            notional_usd=15.0,
            identity=identity,
        )
        self.assertEqual(second_status, EXISTING_NON_TERMINAL)
        self.assertEqual(str(second_intent.get("status") or ""), INTENT_STATUS_ACK_UNKNOWN)

        probe_two = trader._record_ack_unknown_probe(
            str(identity["strategy_order_uuid"]),
            current_count=int(second_intent.get("ack_unknown_count") or 0),
            current_first_ts=int(second_intent.get("ack_unknown_first_ts") or 0),
        )
        escalated = INTENT_STATUS_MANUAL_REQUIRED if bool(probe_two.get("manual_required")) else INTENT_STATUS_ACK_UNKNOWN
        trader._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=escalated,
            payload_updates={
                "ack_unknown_count": int(probe_two["count"]),
                "ack_unknown_first_ts": int(probe_two["first_ts"]),
            },
        )

        probe_three = trader._record_ack_unknown_probe(
            str(identity["strategy_order_uuid"]),
            current_count=int(probe_two.get("count") or 0),
            current_first_ts=int(probe_two.get("first_ts") or 0),
        )
        escalated = INTENT_STATUS_MANUAL_REQUIRED if bool(probe_three.get("manual_required")) else INTENT_STATUS_ACK_UNKNOWN
        trader._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=escalated,
            payload_updates={
                "ack_unknown_count": int(probe_three["count"]),
                "ack_unknown_first_ts": int(probe_three["first_ts"]),
            },
        )

        third_status, third_intent = trader._claim_or_load_intent(
            signal=signal,
            notional_usd=15.0,
            identity=identity,
        )
        self.assertEqual(third_status, EXISTING_NON_TERMINAL)
        self.assertEqual(str(third_intent.get("status") or ""), INTENT_STATUS_MANUAL_REQUIRED)

        intents = trader._state_store.load_order_intents()
        self.assertEqual(len(intents), 1)
        self.assertEqual(str(intents[0].get("idempotency_key") or ""), str(identity["idempotency_key"]))

    def test_manual_required_blocks_new_intent_and_resend(self):
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
        settings.enable_single_writer = False
        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )
        signal = build_signal(token_id="token-manual", side="BUY")
        identity = trader._build_intent_identity(signal, 19.0)

        claim_status, _ = trader._claim_or_load_intent(
            signal=signal,
            notional_usd=19.0,
            identity=identity,
        )
        self.assertEqual(claim_status, CLAIMED_NEW)

        locked, _ = trader._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=INTENT_STATUS_MANUAL_REQUIRED,
        )
        self.assertTrue(locked)

        # Same signal cannot create another intent.
        second_status, second_intent = trader._claim_or_load_intent(
            signal=signal,
            notional_usd=19.0,
            identity=identity,
        )
        self.assertEqual(second_status, EXISTING_NON_TERMINAL)
        self.assertEqual(str(second_intent.get("status") or ""), INTENT_STATUS_MANUAL_REQUIRED)

        # Send critical section requires NEW; MANUAL_REQUIRED must fail the CAS and block resend.
        resend_allowed, _ = trader._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=INTENT_STATUS_SENDING,
            expected_from_statuses=("new",),
        )
        self.assertFalse(resend_allowed)

        intents = trader._state_store.load_order_intents()
        self.assertEqual(len(intents), 1)
        self.assertEqual(str(intents[0].get("idempotency_key") or ""), str(identity["idempotency_key"]))


if __name__ == "__main__":
    unittest.main()
