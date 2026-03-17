from __future__ import annotations

from dataclasses import dataclass

from polymarket_bot.config import Settings
from polymarket_bot.types import RiskDecision, Signal


@dataclass(slots=True)
class RiskState:
    daily_realized_pnl: float = 0.0
    open_positions: int = 0


class RiskManager:
    def __init__(self, settings: Settings) -> None:
        self.settings = settings

    def evaluate(self, signal: Signal, state: RiskState) -> RiskDecision:
        if signal.side == "SELL":
            max_notional = max(0.0, float(signal.observed_notional or 0.0))
            snapshot = {
                "side": signal.side,
                "observed_notional": max_notional,
                "min_sell_notional": 5.0,
                "allowed": max_notional >= 5.0,
            }
            if max_notional < 5:
                return RiskDecision(False, "sell notional too small", 0.0, snapshot=snapshot)
            return RiskDecision(True, "ok", max_notional, snapshot=snapshot)

        daily_limit = self.settings.bankroll_usd * self.settings.daily_max_loss_pct
        snapshot = {
            "side": signal.side,
            "daily_limit": daily_limit,
            "daily_realized_pnl": state.daily_realized_pnl,
            "open_positions": state.open_positions,
            "max_open_positions": self.settings.max_open_positions,
            "price_hint": signal.price_hint,
            "min_price": self.settings.min_price,
            "max_price": self.settings.max_price,
            "observed_notional": float(signal.observed_notional or 0.0),
            "confidence": float(signal.confidence or 0.0),
        }
        if state.daily_realized_pnl <= -daily_limit:
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "daily_limit"
            return RiskDecision(False, "daily loss limit reached", 0.0, snapshot=snapshot)

        if state.open_positions >= self.settings.max_open_positions:
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "max_open_positions"
            return RiskDecision(False, "max open positions reached", 0.0, snapshot=snapshot)

        if not (self.settings.min_price <= signal.price_hint <= self.settings.max_price):
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "price_band"
            return RiskDecision(False, "price outside allowed band", 0.0, snapshot=snapshot)

        base_notional = self.settings.bankroll_usd * self.settings.risk_per_trade_pct
        # Scale by confidence and cap at observed smart-money notional.
        max_notional = min(base_notional * (0.7 + signal.confidence), signal.observed_notional)
        snapshot["base_notional"] = base_notional
        snapshot["proposed_max_notional"] = max_notional
        if max_notional < 5:
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "notional_too_small"
            return RiskDecision(False, "calculated notional too small", 0.0, snapshot=snapshot)

        snapshot["allowed"] = True
        return RiskDecision(True, "ok", max_notional, snapshot=snapshot)
