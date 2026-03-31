from __future__ import annotations

import time
import unittest

from polymarket_bot.risk import REASON_INTRADAY_DRAWDOWN_BREAKER_ACTIVE, RiskManager
from runtime_persistence_helpers import DummyBroker, DummyDataClient, DummyRisk, DummyStrategy, build_signal, make_settings, new_tmp_dir
from polymarket_bot.runner import Trader


class IntradayDrawdownBreakerTests(unittest.TestCase):
    def test_intraday_drawdown_blocks_buy(self):
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
        settings.bankroll_usd = 1000.0
        settings.intraday_drawdown_breaker_pct = 0.05
        settings.risk_breaker_timezone = "UTC"

        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )
        trader.risk = RiskManager(settings)

        trader.state.equity_usd = 1000.0
        trader._refresh_risk_state()
        self.assertFalse(trader.state.intraday_drawdown_blocked)

        trader.state.daily_realized_pnl = -120.0
        trader.state.equity_usd = 900.0
        trader._refresh_risk_state()
        self.assertTrue(trader.state.intraday_drawdown_blocked)
        self.assertGreaterEqual(trader.state.intraday_drawdown_pct, 0.10)

        signal = build_signal(token_id="token-dd", side="BUY")
        trader._hydrate_signal_condition_exposure(signal)
        decision = trader.risk.evaluate(signal, trader.state)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, REASON_INTRADAY_DRAWDOWN_BREAKER_ACTIVE)

    def test_day_roll_resets_drawdown_when_auto_reset_enabled(self):
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
        settings.bankroll_usd = 1000.0
        settings.intraday_drawdown_breaker_pct = 0.05
        settings.risk_breaker_timezone = "UTC"
        settings.risk_breaker_reset_next_day = True

        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )

        trader._risk_breaker_state = {
            "valuation_currency": "USD",
            "timezone": "UTC",
            "day_key": "2000-01-01",
            "opening_allowed": False,
            "manual_required": False,
            "manual_lock": False,
            "reason_codes": ["intraday_drawdown_breaker_active"],
            "loss_streak_count": 0,
            "loss_streak_limit": 3,
            "loss_streak_blocked": False,
            "intraday_drawdown_pct": 0.20,
            "intraday_drawdown_limit_pct": 0.05,
            "intraday_drawdown_blocked": True,
            "equity_now_usd": 800.0,
            "equity_peak_usd": 1000.0,
            "updated_ts": int(time.time()),
        }
        trader.state.equity_usd = 1000.0
        trader._refresh_risk_state()
        self.assertFalse(trader.state.intraday_drawdown_blocked)
        self.assertEqual(trader.state.risk_breaker_opening_allowed, True)


if __name__ == "__main__":
    unittest.main()
