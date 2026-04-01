from __future__ import annotations

import time
import unittest

from polymarket_bot.runner import Trader
from polymarket_bot.risk import REASON_LOSS_STREAK_BREAKER_ACTIVE, RiskManager
from runtime_persistence_helpers import DummyBroker, DummyDataClient, DummyRisk, DummyStrategy, build_signal, make_settings, new_tmp_dir


class RiskBreakerPersistenceTests(unittest.TestCase):
    def test_loss_streak_breaker_persists_after_restart(self):
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
        settings.bankroll_usd = 1000.0
        settings.loss_streak_breaker_limit = 2

        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )
        trader.risk = RiskManager(settings)

        now = int(time.time())
        trader._apply_realized_pnl(-5.0, ts=now)
        trader._apply_realized_pnl(-1.0, ts=now + 1)
        trader._refresh_risk_state()
        self.assertTrue(trader.state.loss_streak_blocked)
        trader.persist_runtime_state(settings.runtime_state_path)
        if trader._writer_lock is not None:
            trader._writer_lock.release()
            trader._writer_lock = None

        restarted = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )
        restarted.risk = RiskManager(settings)
        restarted._refresh_risk_state()
        self.assertTrue(restarted.state.loss_streak_blocked)
        self.assertGreaterEqual(restarted.state.loss_streak_count, 2)

        signal = build_signal(token_id="token-restart", side="BUY")
        restarted._hydrate_signal_condition_exposure(signal)
        decision = restarted.risk.evaluate(signal, restarted.state)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, REASON_LOSS_STREAK_BREAKER_ACTIVE)

    def test_persisted_breaker_wins_over_legacy_risk_fields_after_restart(self):
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
        settings.bankroll_usd = 1000.0
        settings.loss_streak_breaker_limit = 2

        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )
        trader.risk = RiskManager(settings)
        now = int(time.time())
        trader._apply_realized_pnl(-4.0, ts=now)
        trader._apply_realized_pnl(-2.0, ts=now + 1)
        trader._refresh_risk_state()
        self.assertTrue(trader.state.loss_streak_blocked)
        trader.persist_runtime_state(settings.runtime_state_path)

        truth = trader._state_store.load_runtime_truth()  # type: ignore[union-attr]
        truth["risk"] = {
            "loss_streak_count": 0,
            "loss_streak_limit": 2,
            "loss_streak_blocked": False,
            "risk_breaker_opening_allowed": True,
            "risk_breaker_reason_codes": [],
            "risk_breaker_status": "ok",
            "valuation_currency": "USD",
        }
        trader._state_store.save_runtime_truth(truth)  # type: ignore[union-attr]

        if trader._writer_lock is not None:
            trader._writer_lock.release()
            trader._writer_lock = None

        restarted = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )
        restarted.risk = RiskManager(settings)
        restarted._refresh_risk_state()
        self.assertTrue(restarted.state.loss_streak_blocked)
        self.assertFalse(restarted.state.risk_breaker_opening_allowed)

        signal = build_signal(token_id="token-restart-compat", side="BUY")
        restarted._hydrate_signal_condition_exposure(signal)
        decision = restarted.risk.evaluate(signal, restarted.state)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, REASON_LOSS_STREAK_BREAKER_ACTIVE)


if __name__ == "__main__":
    unittest.main()
