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
        daily_limit = self.settings.bankroll_usd * self.settings.daily_max_loss_pct
        if state.daily_realized_pnl <= -daily_limit:
            return RiskDecision(False, "daily loss limit reached", 0.0)

        if state.open_positions >= self.settings.max_open_positions:
            return RiskDecision(False, "max open positions reached", 0.0)

        if not (self.settings.min_price <= signal.price_hint <= self.settings.max_price):
            return RiskDecision(False, "price outside allowed band", 0.0)

        base_notional = self.settings.bankroll_usd * self.settings.risk_per_trade_pct
        # Scale by confidence and cap at observed smart-money notional.
        max_notional = min(base_notional * (0.7 + signal.confidence), signal.observed_notional)
        if max_notional < 5:
            return RiskDecision(False, "calculated notional too small", 0.0)

        return RiskDecision(True, "ok", max_notional)
