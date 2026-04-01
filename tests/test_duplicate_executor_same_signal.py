from __future__ import annotations

import unittest

from polymarket_bot.idempotency import CLAIMED_NEW, EXISTING_NON_TERMINAL
from polymarket_bot.runner import Trader
from runtime_persistence_helpers import DummyBroker, DummyDataClient, DummyRisk, DummyStrategy, build_signal, make_settings, new_tmp_dir


class DuplicateExecutorSameSignalTests(unittest.TestCase):
    def test_same_signal_across_two_executors_creates_single_intent(self):
        workdir = new_tmp_dir()
        settings_a = make_settings(dry_run=True, workdir=workdir)
        settings_b = make_settings(dry_run=True, workdir=workdir)
        settings_a.enable_single_writer = False
        settings_b.enable_single_writer = False

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

        signal = build_signal(token_id="token-dual", side="BUY")
        identity_a = trader_a._build_intent_identity(signal, 30.0)
        identity_b = trader_b._build_intent_identity(signal, 30.0)
        self.assertEqual(identity_a["idempotency_key"], identity_b["idempotency_key"])

        status_a, _ = trader_a._claim_or_load_intent(
            signal=signal,
            notional_usd=30.0,
            identity=identity_a,
        )
        status_b, _ = trader_b._claim_or_load_intent(
            signal=signal,
            notional_usd=30.0,
            identity=identity_b,
        )

        self.assertEqual(status_a, CLAIMED_NEW)
        self.assertEqual(status_b, EXISTING_NON_TERMINAL)
        intents = trader_a._state_store.load_order_intents()
        self.assertEqual(len(intents), 1)


if __name__ == "__main__":
    unittest.main()
