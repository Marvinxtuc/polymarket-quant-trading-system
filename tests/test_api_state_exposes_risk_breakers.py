from __future__ import annotations

import unittest

from polymarket_bot.web import _api_state_payload


class ApiStateExposureTests(unittest.TestCase):
    def test_api_state_exposes_risk_breakers_and_exposure_ledger(self):
        payload = _api_state_payload(
            {
                "admission": {
                    "mode": "REDUCE_ONLY",
                    "opening_allowed": False,
                    "reason_codes": ["risk_ledger_fault"],
                    "evidence_summary": {
                        "risk_ledger_status": "fault",
                        "risk_breaker_status": "ok",
                    },
                },
                "risk_state": {
                    "valuation_currency": "USD",
                    "wallet_exposure_committed_usd": 120.0,
                    "wallet_exposure_cap_usd": 100.0,
                    "wallet_exposure_usage_pct": 1.2,
                    "portfolio_exposure_committed_usd": 120.0,
                    "portfolio_exposure_cap_usd": 100.0,
                    "portfolio_exposure_usage_pct": 1.2,
                    "condition_exposure_key": "condition:demo",
                    "condition_exposure_committed_usd": 90.0,
                    "condition_exposure_cap_usd": 80.0,
                    "condition_exposure_usage_pct": 1.125,
                    "loss_streak_count": 2,
                    "loss_streak_current": 2,
                    "loss_streak_limit": 2,
                    "loss_streak_blocked": True,
                    "intraday_drawdown_pct": 0.11,
                    "intraday_drawdown_current": 0.11,
                    "intraday_drawdown_limit_pct": 0.05,
                    "intraday_drawdown_blocked": True,
                    "breaker_latched": True,
                    "reason_codes": [
                        "loss_streak_breaker_active",
                        "intraday_drawdown_breaker_active",
                    ],
                    "risk_breaker_reason_codes": [
                        "loss_streak_breaker_active",
                        "intraday_drawdown_breaker_active",
                    ],
                },
                "risk_breakers": {
                    "valuation_currency": "USD",
                    "day_key": "2026-03-27",
                    "timezone": "UTC",
                    "opening_allowed": False,
                    "manual_required": False,
                    "manual_lock": False,
                    "reason_codes": [
                        "loss_streak_breaker_active",
                        "intraday_drawdown_breaker_active",
                    ],
                    "loss_streak_count": 2,
                    "loss_streak_limit": 2,
                    "loss_streak_blocked": True,
                    "intraday_drawdown_pct": 0.11,
                    "intraday_drawdown_limit_pct": 0.05,
                    "intraday_drawdown_blocked": True,
                    "equity_now_usd": 890.0,
                    "equity_peak_usd": 1000.0,
                },
                "exposure_ledger": [
                    {
                        "scope_type": "wallet",
                        "scope_key": "default",
                        "valuation_currency": "USD",
                        "committed_exposure_notional_usd": 120.0,
                        "cap_notional_usd": 100.0,
                        "blocked": True,
                        "reason_code": "wallet_exposure_cap_reached",
                    }
                ],
            },
            None,
        )
        self.assertEqual(payload["risk_state"]["valuation_currency"], "USD")
        self.assertGreater(payload["risk_state"]["wallet_exposure_usage_pct"], 1.0)
        self.assertGreater(payload["risk_state"]["portfolio_exposure_usage_pct"], 1.0)
        self.assertGreater(payload["risk_state"]["condition_exposure_usage_pct"], 1.0)
        self.assertEqual(payload["risk_state"]["loss_streak_current"], 2)
        self.assertTrue(payload["risk_state"]["loss_streak_blocked"])
        self.assertAlmostEqual(payload["risk_state"]["intraday_drawdown_current"], 0.11, places=6)
        self.assertTrue(payload["risk_state"]["intraday_drawdown_blocked"])
        self.assertTrue(payload["risk_state"]["breaker_latched"])
        self.assertIn("loss_streak_breaker_active", payload["risk_state"]["reason_codes"])
        self.assertIn("risk_breakers", payload)
        self.assertFalse(payload["risk_breakers"]["opening_allowed"])
        self.assertIn("exposure_ledger", payload)
        self.assertGreaterEqual(len(payload["exposure_ledger"]), 1)
        self.assertEqual(payload["exposure_ledger"][0]["scope_type"], "wallet")


if __name__ == "__main__":
    unittest.main()
