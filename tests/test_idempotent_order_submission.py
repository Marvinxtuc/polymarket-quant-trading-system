from __future__ import annotations

import concurrent.futures
from collections import Counter
import unittest

from polymarket_bot.idempotency import CLAIMED_NEW, EXISTING_NON_TERMINAL, EXISTING_TERMINAL, INTENT_STATUS_FILLED
from polymarket_bot.runner import Trader
from runtime_persistence_helpers import (
    DummyBroker,
    DummyDataClient,
    DummyRisk,
    DummyStrategy,
    build_signal,
    make_settings,
    new_tmp_dir,
)


class IdempotentOrderSubmissionTests(unittest.TestCase):
    def _make_trader(self, *, signal_source: str = "hybrid") -> Trader:
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
        settings.enable_single_writer = False
        settings.wallet_signal_source = signal_source
        return Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )

    def test_idempotency_key_changes_with_signal_source(self):
        signal = build_signal(token_id="token-src", side="BUY")
        trader_a = self._make_trader(signal_source="hybrid")
        trader_b = self._make_trader(signal_source="trade_only")

        identity_a = trader_a._build_intent_identity(signal, 12.34)
        identity_b = trader_b._build_intent_identity(signal, 12.34)

        self.assertNotEqual(identity_a["idempotency_key"], identity_b["idempotency_key"])
        self.assertNotEqual(identity_a["strategy_order_uuid"], identity_b["strategy_order_uuid"])

    def test_claim_or_load_transitions_non_terminal_then_terminal(self):
        signal = build_signal(token_id="token-claim", side="BUY")
        trader = self._make_trader(signal_source="hybrid")
        identity = trader._build_intent_identity(signal, 20.0)

        first_status, first_intent = trader._claim_or_load_intent(
            signal=signal,
            notional_usd=20.0,
            identity=identity,
        )
        self.assertEqual(first_status, CLAIMED_NEW)
        self.assertEqual(str(first_intent.get("status") or ""), "new")

        second_status, second_intent = trader._claim_or_load_intent(
            signal=signal,
            notional_usd=20.0,
            identity=identity,
        )
        self.assertEqual(second_status, EXISTING_NON_TERMINAL)
        self.assertEqual(str(second_intent.get("status") or ""), "new")

        updated, _ = trader._set_intent_status(
            strategy_order_uuid=str(identity["strategy_order_uuid"]),
            idempotency_key=str(identity["idempotency_key"]),
            status=INTENT_STATUS_FILLED,
        )
        self.assertTrue(updated)

        third_status, third_intent = trader._claim_or_load_intent(
            signal=signal,
            notional_usd=20.0,
            identity=identity,
        )
        self.assertEqual(third_status, EXISTING_TERMINAL)
        self.assertEqual(str(third_intent.get("status") or ""), INTENT_STATUS_FILLED)

    def test_claim_or_load_is_atomic_under_concurrent_claims(self):
        workdir = new_tmp_dir()
        settings_a = make_settings(dry_run=True, workdir=workdir)
        settings_b = make_settings(dry_run=True, workdir=workdir)
        settings_a.enable_single_writer = False
        settings_b.enable_single_writer = False
        settings_a.wallet_signal_source = "hybrid"
        settings_b.wallet_signal_source = "hybrid"
        trader_a = Trader(
            settings=settings_a,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )
        trader_b = Trader(
            settings=settings_b,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )
        signal = build_signal(token_id="token-atomic", side="BUY")
        identity = trader_a._build_intent_identity(signal, 25.0)

        def _claim(idx: int) -> str:
            trader = trader_a if idx % 2 == 0 else trader_b
            status, _ = trader._claim_or_load_intent(
                signal=signal,
                notional_usd=25.0,
                identity=identity,
            )
            return str(status)

        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            statuses = list(executor.map(_claim, range(16)))

        counts = Counter(statuses)
        self.assertEqual(counts.get(CLAIMED_NEW, 0), 1)
        self.assertEqual(counts.get(EXISTING_NON_TERMINAL, 0), 15)
        intents = trader_a._state_store.load_order_intents()
        self.assertEqual(len(intents), 1)
        self.assertEqual(str(intents[0].get("idempotency_key") or ""), str(identity["idempotency_key"]))


if __name__ == "__main__":
    unittest.main()
