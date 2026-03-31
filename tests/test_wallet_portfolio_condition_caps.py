from __future__ import annotations

from datetime import datetime, timezone
import unittest

from polymarket_bot.config import Settings
from polymarket_bot.risk import (
    REASON_CONDITION_EXPOSURE_CAP_REACHED,
    REASON_PORTFOLIO_EXPOSURE_CAP_REACHED,
    REASON_WALLET_EXPOSURE_CAP_REACHED,
    RiskManager,
    RiskState,
)
from polymarket_bot.types import Signal


def _signal() -> Signal:
    return Signal(
        signal_id="signal-risk-cap",
        trace_id="trace-risk-cap",
        wallet="0x1111111111111111111111111111111111111111",
        market_slug="risk-cap-market",
        token_id="token-risk",
        condition_id="condition-risk",
        outcome="YES",
        side="BUY",
        confidence=1.0,
        price_hint=0.55,
        observed_size=10.0,
        observed_notional=100.0,
        timestamp=datetime.now(tz=timezone.utc),
    )


class WalletPortfolioConditionCapsTests(unittest.TestCase):
    def _settings(self) -> Settings:
        return Settings(
            _env_file=None,
            dry_run=True,
            bankroll_usd=100.0,
            risk_per_trade_pct=0.05,
            max_open_positions=99,
            min_price=0.01,
            max_price=0.99,
        )

    def test_primary_reason_priority_wallet_then_portfolio_then_condition(self):
        manager = RiskManager(self._settings())
        state = RiskState(
            trading_mode="NORMAL",
            wallet_exposure_cap_usd=10.0,
            wallet_exposure_committed_usd=10.0,
            portfolio_exposure_cap_usd=9.0,
            portfolio_exposure_committed_usd=9.0,
            condition_exposure_key="condition:condition-risk",
            condition_exposure_cap_usd=8.0,
            condition_exposure_committed_usd=8.0,
        )
        decision = manager.evaluate(_signal(), state)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, REASON_WALLET_EXPOSURE_CAP_REACHED)
        self.assertEqual(
            list(decision.snapshot.get("reason_codes") or []),
            [
                REASON_WALLET_EXPOSURE_CAP_REACHED,
                REASON_PORTFOLIO_EXPOSURE_CAP_REACHED,
                REASON_CONDITION_EXPOSURE_CAP_REACHED,
            ],
        )

    def test_portfolio_reason_wins_when_wallet_not_exceeded(self):
        manager = RiskManager(self._settings())
        state = RiskState(
            trading_mode="NORMAL",
            wallet_exposure_cap_usd=10.0,
            wallet_exposure_committed_usd=5.0,
            portfolio_exposure_cap_usd=9.0,
            portfolio_exposure_committed_usd=9.1,
            condition_exposure_key="condition:condition-risk",
            condition_exposure_cap_usd=8.0,
            condition_exposure_committed_usd=8.2,
        )
        decision = manager.evaluate(_signal(), state)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, REASON_PORTFOLIO_EXPOSURE_CAP_REACHED)
        self.assertIn(REASON_PORTFOLIO_EXPOSURE_CAP_REACHED, list(decision.snapshot.get("reason_codes") or []))

    def test_condition_reason_when_only_condition_exceeded(self):
        manager = RiskManager(self._settings())
        state = RiskState(
            trading_mode="NORMAL",
            wallet_exposure_cap_usd=10.0,
            wallet_exposure_committed_usd=5.0,
            portfolio_exposure_cap_usd=10.0,
            portfolio_exposure_committed_usd=5.0,
            condition_exposure_key="condition:condition-risk",
            condition_exposure_cap_usd=1.0,
            condition_exposure_committed_usd=2.0,
        )
        decision = manager.evaluate(_signal(), state)
        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, REASON_CONDITION_EXPOSURE_CAP_REACHED)
        self.assertEqual(
            list(decision.snapshot.get("reason_codes") or []),
            [REASON_CONDITION_EXPOSURE_CAP_REACHED],
        )


if __name__ == "__main__":
    unittest.main()
