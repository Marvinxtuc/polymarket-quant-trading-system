from __future__ import annotations

from dataclasses import dataclass

from polymarket_bot.config import Settings
from polymarket_bot.types import RiskDecision, Signal


@dataclass(slots=True)
class RiskState:
    daily_realized_pnl: float = 0.0
    broker_closed_pnl_today: float = 0.0
    open_positions: int = 0
    tracked_notional_usd: float = 0.0
    pending_entry_notional_usd: float = 0.0
    pending_exit_notional_usd: float = 0.0
    pending_entry_orders: int = 0
    equity_usd: float = 0.0
    cash_balance_usd: float = 0.0
    positions_value_usd: float = 0.0
    account_snapshot_ts: int = 0

    @property
    def effective_daily_realized_pnl(self) -> float:
        values = [float(self.daily_realized_pnl)]
        if abs(float(self.broker_closed_pnl_today)) > 1e-9:
            values.append(float(self.broker_closed_pnl_today))
        return min(values)

    @property
    def committed_notional_usd(self) -> float:
        return max(0.0, self.tracked_notional_usd + self.pending_entry_notional_usd)

    @property
    def effective_open_positions(self) -> int:
        return max(0, int(self.open_positions) + int(self.pending_entry_orders))


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
            "broker_closed_pnl_today": state.broker_closed_pnl_today,
            "effective_daily_realized_pnl": state.effective_daily_realized_pnl,
            "open_positions": state.open_positions,
            "effective_open_positions": state.effective_open_positions,
            "max_open_positions": self.settings.max_open_positions,
            "tracked_notional_usd": state.tracked_notional_usd,
            "pending_entry_notional_usd": state.pending_entry_notional_usd,
            "pending_exit_notional_usd": state.pending_exit_notional_usd,
            "committed_notional_usd": state.committed_notional_usd,
            "bankroll_usd": self.settings.bankroll_usd,
            "equity_usd": state.equity_usd,
            "cash_balance_usd": state.cash_balance_usd,
            "positions_value_usd": state.positions_value_usd,
            "account_snapshot_ts": state.account_snapshot_ts,
            "price_hint": signal.price_hint,
            "min_price": self.settings.min_price,
            "max_price": self.settings.max_price,
            "observed_notional": float(signal.observed_notional or 0.0),
            "confidence": float(signal.confidence or 0.0),
        }
        if state.effective_daily_realized_pnl <= -daily_limit:
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "daily_limit"
            return RiskDecision(False, "daily loss limit reached", 0.0, snapshot=snapshot)

        if state.effective_open_positions >= self.settings.max_open_positions:
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "max_open_positions"
            return RiskDecision(False, "max open positions reached", 0.0, snapshot=snapshot)

        if state.committed_notional_usd >= self.settings.bankroll_usd:
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "bankroll_committed"
            return RiskDecision(False, "bankroll fully committed", 0.0, snapshot=snapshot)

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

        remaining_capacity = max(0.0, self.settings.bankroll_usd - state.committed_notional_usd)
        snapshot["remaining_capacity_usd"] = remaining_capacity
        max_notional = min(max_notional, remaining_capacity)
        snapshot["capped_max_notional"] = max_notional
        if max_notional < 5:
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "bankroll_capacity"
            return RiskDecision(False, "remaining bankroll capacity too small", 0.0, snapshot=snapshot)

        snapshot["allowed"] = True
        return RiskDecision(True, "ok", max_notional, snapshot=snapshot)
