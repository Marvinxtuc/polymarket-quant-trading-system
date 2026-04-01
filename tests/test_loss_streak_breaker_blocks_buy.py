from __future__ import annotations

import time
import unittest

from polymarket_bot.risk import REASON_LOSS_STREAK_BREAKER_ACTIVE, RiskManager
from runtime_persistence_helpers import DummyBroker, DummyDataClient, DummyRisk, DummyStrategy, build_signal, make_settings, new_tmp_dir
from polymarket_bot.runner import Trader


class LossStreakBreakerTests(unittest.TestCase):
    def test_loss_streak_reaches_limit_and_blocks_buy(self):
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
        settings.loss_streak_breaker_limit = 2
        settings.bankroll_usd = 1000.0

        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )
        trader.risk = RiskManager(settings)

        ts = int(time.time())
        trader._apply_realized_pnl(-5.0, ts=ts)
        trader._refresh_risk_state()
        self.assertEqual(trader.state.loss_streak_count, 1)
        self.assertFalse(trader.state.loss_streak_blocked)

        trader._apply_realized_pnl(-1.0, ts=ts + 1)
        trader._refresh_risk_state()
        self.assertEqual(trader.state.loss_streak_count, 2)
        self.assertTrue(trader.state.loss_streak_blocked)

        signal = build_signal(token_id="token-loss", side="BUY")
        trader._hydrate_signal_condition_exposure(signal)
        decision = trader.risk.evaluate(signal, trader.state)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, REASON_LOSS_STREAK_BREAKER_ACTIVE)

    def test_profit_resets_loss_streak_and_break_even_keeps_counter(self):
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
        settings.loss_streak_breaker_limit = 3

        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )
        trader.risk = RiskManager(settings)

        ts = int(time.time())
        trader._apply_realized_pnl(-2.0, ts=ts)
        trader._refresh_risk_state()
        self.assertEqual(trader.state.loss_streak_count, 1)

        trader._apply_realized_pnl(0.0, ts=ts + 1)
        trader._refresh_risk_state()
        self.assertEqual(trader.state.loss_streak_count, 1)

        trader._apply_realized_pnl(3.0, ts=ts + 2)
        trader._refresh_risk_state()
        self.assertEqual(trader.state.loss_streak_count, 0)
        self.assertFalse(trader.state.loss_streak_blocked)

    def test_split_exit_negative_realized_events_count_independently(self):
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
        settings.loss_streak_breaker_limit = 3

        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )
        trader.risk = RiskManager(settings)

        ts = int(time.time())
        # Simulate split close events: each negative realized close increments streak once.
        trader._apply_realized_pnl(-0.8, ts=ts)
        trader._apply_realized_pnl(-0.2, ts=ts + 1)
        trader._refresh_risk_state()
        self.assertEqual(trader.state.loss_streak_count, 2)
        self.assertFalse(trader.state.loss_streak_blocked)

        # Break-even close keeps the streak unchanged.
        trader._apply_realized_pnl(0.0, ts=ts + 2)
        trader._refresh_risk_state()
        self.assertEqual(trader.state.loss_streak_count, 2)

    def test_loss_streak_resets_on_day_roll_when_auto_reset_enabled(self):
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
        settings.loss_streak_breaker_limit = 2
        settings.risk_breaker_reset_next_day = True
        settings.risk_breaker_timezone = "UTC"

        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )
        trader.risk = RiskManager(settings)

        ts = int(time.time())
        trader._risk_breaker_state = {
            "valuation_currency": "USD",
            "timezone": "UTC",
            "day_key": "2000-01-01",
            "opening_allowed": False,
            "manual_required": False,
            "manual_lock": False,
            "reason_codes": ["loss_streak_breaker_active"],
            "loss_streak_count": 2,
            "loss_streak_limit": 2,
            "loss_streak_blocked": True,
            "intraday_drawdown_pct": 0.0,
            "intraday_drawdown_limit_pct": 0.05,
            "intraday_drawdown_blocked": False,
            "equity_now_usd": 1000.0,
            "equity_peak_usd": 1000.0,
            "updated_ts": ts,
        }

        trader._refresh_risk_state()
        self.assertEqual(trader.state.loss_streak_count, 0)
        self.assertFalse(trader.state.loss_streak_blocked)
        self.assertTrue(trader.state.risk_breaker_opening_allowed)


if __name__ == "__main__":
    unittest.main()
