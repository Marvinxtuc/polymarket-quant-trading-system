from __future__ import annotations

import unittest

from polymarket_bot.force_exit import compute_time_exit_priority, estimate_time_exit_volatility_bps


class ExitForceModeTests(unittest.TestCase):
    def test_priority_increases_with_failures_and_force_mode(self):
        base_priority, _ = compute_time_exit_priority(
            consecutive_failures=0,
            market_volatility_bps=50.0,
            volatility_step_bps=100.0,
            force_exit=False,
        )
        retry_priority, _ = compute_time_exit_priority(
            consecutive_failures=1,
            market_volatility_bps=250.0,
            volatility_step_bps=100.0,
            force_exit=False,
        )
        force_priority, reason = compute_time_exit_priority(
            consecutive_failures=2,
            market_volatility_bps=250.0,
            volatility_step_bps=100.0,
            force_exit=True,
        )

        self.assertGreater(retry_priority, base_priority)
        self.assertGreater(force_priority, retry_priority)
        self.assertIn("force_exit", reason)

    def test_estimate_time_exit_volatility_uses_spread_and_price_dislocation(self):
        spread_volatility = estimate_time_exit_volatility_bps(
            best_bid=0.45,
            best_ask=0.55,
            midpoint=0.5,
            reference_price=0.5,
        )
        dislocation_volatility = estimate_time_exit_volatility_bps(
            best_bid=0.49,
            best_ask=0.51,
            midpoint=0.5,
            reference_price=0.62,
        )

        self.assertGreater(spread_volatility, 0.0)
        self.assertGreater(dislocation_volatility, 0.0)
        self.assertGreater(dislocation_volatility, spread_volatility / 2.0)
