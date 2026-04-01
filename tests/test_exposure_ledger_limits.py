from __future__ import annotations

import unittest
from unittest.mock import patch

from polymarket_bot.risk import RiskManager
from polymarket_bot.runner import Trader
from runtime_persistence_helpers import DummyBroker, DummyDataClient, DummyRisk, DummyStrategy, build_signal, make_settings, new_tmp_dir


class ExposureLedgerLimitsTests(unittest.TestCase):
    def test_wallet_exposure_cap_blocks_buy(self):
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
        settings.bankroll_usd = 100.0
        settings.max_wallet_exposure_pct = 0.10
        settings.max_portfolio_exposure_pct = 1.0
        settings.max_condition_exposure_pct = 1.0

        signal = build_signal(token_id="token-new", side="BUY")
        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([signal]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )
        trader.risk = RiskManager(settings)
        trader.positions_book = {
            "token-existing": {
                "token_id": "token-existing",
                "condition_id": "condition-existing",
                "market_slug": "existing-market",
                "outcome": "YES",
                "quantity": 100.0,
                "price": 0.20,
                "notional": 20.0,
                "cost_basis_notional": 20.0,
                "opened_ts": 1,
                "last_buy_ts": 1,
                "last_trim_ts": 0,
            }
        }
        trader._refresh_risk_state()
        trader._hydrate_signal_condition_exposure(signal)
        decision = trader.risk.evaluate(signal, trader.state)

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "wallet_exposure_cap_reached")
        self.assertGreaterEqual(float(decision.snapshot.get("wallet_exposure_committed_usd") or 0.0), 20.0)
        self.assertAlmostEqual(float(decision.snapshot.get("wallet_exposure_cap_usd") or 0.0), 10.0, places=6)

    def test_legacy_local_paths_cannot_bypass_ledger_breaker_gate(self):
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
        settings.bankroll_usd = 100.0
        settings.max_wallet_exposure_pct = 0.10
        settings.max_portfolio_exposure_pct = 1.0
        settings.max_condition_exposure_pct = 1.0
        settings.max_open_positions = 10
        signal = build_signal(token_id="token-new", side="BUY")
        broker = DummyBroker()
        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([signal]),
            risk=DummyRisk(),
            broker=broker,
        )
        trader.risk = RiskManager(settings)
        trader.positions_book = {
            "token-existing": {
                "token_id": "token-existing",
                "condition_id": "condition-existing",
                "market_slug": "existing-market",
                "outcome": "YES",
                "quantity": 100.0,
                "price": 0.20,
                "notional": 20.0,
                "cost_basis_notional": 20.0,
                "opened_ts": 1,
                "last_buy_ts": 1,
                "last_trim_ts": 0,
            }
        }

        with (
            patch.object(Trader, "_enforce_condition_netting", lambda _self, sig, n: (n, {"bypass": True})),
            patch.object(Trader, "_enforce_buy_budget", lambda _self, sig, n: n),
        ):
            netting_notional, _ = trader._enforce_condition_netting(signal, 50.0)
            local_allowed_notional = trader._enforce_buy_budget(signal, netting_notional)

        self.assertGreater(local_allowed_notional, 0.0)
        trader._refresh_risk_state()
        trader._hydrate_signal_condition_exposure(signal)
        decision = trader.risk.evaluate(signal, trader.state)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "wallet_exposure_cap_reached")


if __name__ == "__main__":
    unittest.main()
