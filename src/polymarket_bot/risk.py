from __future__ import annotations

from dataclasses import dataclass

from polymarket_bot.config import Settings
from polymarket_bot.types import RiskDecision, Signal

REASON_RISK_LEDGER_FAULT = "risk_ledger_fault"
REASON_RISK_BREAKER_STATE_INVALID = "risk_breaker_state_invalid"
REASON_LOSS_STREAK_BREAKER_ACTIVE = "loss_streak_breaker_active"
REASON_INTRADAY_DRAWDOWN_BREAKER_ACTIVE = "intraday_drawdown_breaker_active"
REASON_WALLET_EXPOSURE_CAP_REACHED = "wallet_exposure_cap_reached"
REASON_PORTFOLIO_EXPOSURE_CAP_REACHED = "portfolio_exposure_cap_reached"
REASON_CONDITION_EXPOSURE_CAP_REACHED = "condition_exposure_cap_reached"

_PRIMARY_REASON_PRIORITY = (
    REASON_RISK_LEDGER_FAULT,
    REASON_RISK_BREAKER_STATE_INVALID,
    REASON_LOSS_STREAK_BREAKER_ACTIVE,
    REASON_INTRADAY_DRAWDOWN_BREAKER_ACTIVE,
    REASON_WALLET_EXPOSURE_CAP_REACHED,
    REASON_PORTFOLIO_EXPOSURE_CAP_REACHED,
    REASON_CONDITION_EXPOSURE_CAP_REACHED,
    "daily_loss_limit_reached",
    "max_open_positions_reached",
    "bankroll_fully_committed",
    "remaining_bankroll_capacity_too_small",
    "price_outside_allowed_band",
    "calculated_notional_too_small",
)


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
    trading_mode: str = "NORMAL"
    trading_mode_reasons: tuple[str, ...] = ()
    account_state_status: str = "unknown"
    reconciliation_status: str = "unknown"
    persistence_status: str = "ok"
    risk_ledger_status: str = "ok"
    risk_breaker_status: str = "ok"
    valuation_currency: str = "USD"
    wallet_exposure_committed_usd: float = 0.0
    wallet_exposure_cap_usd: float = 0.0
    portfolio_exposure_committed_usd: float = 0.0
    portfolio_exposure_cap_usd: float = 0.0
    condition_exposure_key: str = ""
    condition_exposure_committed_usd: float = 0.0
    condition_exposure_cap_usd: float = 0.0
    loss_streak_count: int = 0
    loss_streak_limit: int = 0
    loss_streak_blocked: bool = False
    intraday_drawdown_pct: float = 0.0
    intraday_drawdown_limit_pct: float = 0.0
    intraday_drawdown_blocked: bool = False
    risk_breaker_opening_allowed: bool = True
    risk_breaker_reason_codes: tuple[str, ...] = ()
    risk_day_key: str = ""

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

    @staticmethod
    def _primary_reason(reason_codes: list[str], fallback: str) -> str:
        normalized = [str(item or "").strip() for item in reason_codes if str(item or "").strip()]
        for reason in _PRIMARY_REASON_PRIORITY:
            if reason in normalized:
                return reason
        return fallback

    @staticmethod
    def _remaining_capacity(cap_usd: float, committed_usd: float) -> float:
        if cap_usd <= 0.0:
            return 0.0
        return max(0.0, float(cap_usd) - max(0.0, float(committed_usd)))

    def evaluate(self, signal: Signal, state: RiskState) -> RiskDecision:
        if signal.side == "SELL":
            max_notional = max(0.0, float(signal.observed_notional or 0.0))
            snapshot = {
                "side": signal.side,
                "observed_notional": max_notional,
                "min_sell_notional": 5.0,
                "allowed": max_notional >= 5.0,
                "trading_mode": str(state.trading_mode or "NORMAL"),
                "trading_mode_reasons": list(state.trading_mode_reasons or ()),
            }
            if max_notional < 5:
                return RiskDecision(False, "sell_notional_too_small", 0.0, snapshot=snapshot)
            return RiskDecision(True, "ok", max_notional, snapshot=snapshot)

        daily_limit = self.settings.bankroll_usd * self.settings.daily_max_loss_pct
        wallet_cap_usd = max(0.0, float(state.wallet_exposure_cap_usd or 0.0))
        wallet_committed = max(0.0, float(state.wallet_exposure_committed_usd or 0.0))
        wallet_remaining = self._remaining_capacity(wallet_cap_usd, wallet_committed) if wallet_cap_usd > 0.0 else float("inf")

        portfolio_cap_usd = max(0.0, float(state.portfolio_exposure_cap_usd or 0.0))
        portfolio_committed = max(0.0, float(state.portfolio_exposure_committed_usd or 0.0))
        portfolio_remaining = (
            self._remaining_capacity(portfolio_cap_usd, portfolio_committed) if portfolio_cap_usd > 0.0 else float("inf")
        )

        condition_cap_usd = max(0.0, float(state.condition_exposure_cap_usd or 0.0))
        condition_committed = max(0.0, float(state.condition_exposure_committed_usd or 0.0))
        condition_remaining = (
            self._remaining_capacity(condition_cap_usd, condition_committed) if condition_cap_usd > 0.0 else float("inf")
        )

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
            "trading_mode": str(state.trading_mode or "NORMAL"),
            "trading_mode_reasons": list(state.trading_mode_reasons or ()),
            "account_state_status": str(state.account_state_status or "unknown"),
            "reconciliation_status": str(state.reconciliation_status or "unknown"),
            "persistence_status": str(state.persistence_status or "ok"),
            "risk_ledger_status": str(state.risk_ledger_status or "unknown"),
            "risk_breaker_status": str(state.risk_breaker_status or "unknown"),
            "valuation_currency": str(state.valuation_currency or "USD"),
            "wallet_exposure_committed_usd": wallet_committed,
            "wallet_exposure_cap_usd": wallet_cap_usd,
            "wallet_exposure_remaining_usd": wallet_remaining,
            "portfolio_exposure_committed_usd": portfolio_committed,
            "portfolio_exposure_cap_usd": portfolio_cap_usd,
            "portfolio_exposure_remaining_usd": portfolio_remaining,
            "condition_exposure_key": str(state.condition_exposure_key or ""),
            "condition_exposure_committed_usd": condition_committed,
            "condition_exposure_cap_usd": condition_cap_usd,
            "condition_exposure_remaining_usd": condition_remaining,
            "loss_streak_count": int(state.loss_streak_count or 0),
            "loss_streak_limit": int(state.loss_streak_limit or 0),
            "loss_streak_blocked": bool(state.loss_streak_blocked),
            "intraday_drawdown_pct": float(state.intraday_drawdown_pct or 0.0),
            "intraday_drawdown_limit_pct": float(state.intraday_drawdown_limit_pct or 0.0),
            "intraday_drawdown_blocked": bool(state.intraday_drawdown_blocked),
            "risk_breaker_opening_allowed": bool(state.risk_breaker_opening_allowed),
            "risk_breaker_reason_codes": [str(code) for code in state.risk_breaker_reason_codes if str(code).strip()],
            "risk_day_key": str(state.risk_day_key or ""),
            "price_hint": signal.price_hint,
            "min_price": self.settings.min_price,
            "max_price": self.settings.max_price,
            "observed_notional": float(signal.observed_notional or 0.0),
            "confidence": float(signal.confidence or 0.0),
        }
        reason_codes: list[str] = []
        if str(state.risk_ledger_status or "ok").strip().lower() != "ok":
            reason_codes.append(REASON_RISK_LEDGER_FAULT)
        if str(state.risk_breaker_status or "ok").strip().lower() != "ok":
            reason_codes.append(REASON_RISK_BREAKER_STATE_INVALID)
        if bool(state.loss_streak_blocked):
            reason_codes.append(REASON_LOSS_STREAK_BREAKER_ACTIVE)
        if bool(state.intraday_drawdown_blocked):
            reason_codes.append(REASON_INTRADAY_DRAWDOWN_BREAKER_ACTIVE)
        if not bool(state.risk_breaker_opening_allowed):
            for raw in state.risk_breaker_reason_codes:
                reason = str(raw or "").strip()
                if reason and reason not in reason_codes:
                    reason_codes.append(reason)

        if reason_codes:
            snapshot["reason_codes"] = reason_codes
            snapshot["primary_reason"] = self._primary_reason(reason_codes, reason_codes[0])
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "risk_breaker"
            return RiskDecision(False, str(snapshot["primary_reason"]), 0.0, snapshot=snapshot)

        cap_rejections: list[str] = []
        if wallet_cap_usd > 0.0 and wallet_remaining <= 0.0:
            cap_rejections.append(REASON_WALLET_EXPOSURE_CAP_REACHED)
        if portfolio_cap_usd > 0.0 and portfolio_remaining <= 0.0:
            cap_rejections.append(REASON_PORTFOLIO_EXPOSURE_CAP_REACHED)
        if condition_cap_usd > 0.0 and condition_remaining <= 0.0:
            cap_rejections.append(REASON_CONDITION_EXPOSURE_CAP_REACHED)
        if cap_rejections:
            snapshot["reason_codes"] = cap_rejections
            snapshot["primary_reason"] = self._primary_reason(cap_rejections, cap_rejections[0])
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "exposure_cap"
            return RiskDecision(False, str(snapshot["primary_reason"]), 0.0, snapshot=snapshot)

        if str(state.trading_mode or "NORMAL").upper() == "HALTED":
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "trading_mode"
            return RiskDecision(False, "system_halted", 0.0, snapshot=snapshot)

        if str(state.trading_mode or "NORMAL").upper() != "NORMAL":
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "trading_mode"
            return RiskDecision(False, "system_reduce_only", 0.0, snapshot=snapshot)

        if state.effective_daily_realized_pnl <= -daily_limit:
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "daily_limit"
            return RiskDecision(False, "daily_loss_limit_reached", 0.0, snapshot=snapshot)

        if state.effective_open_positions >= self.settings.max_open_positions:
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "max_open_positions"
            return RiskDecision(False, "max_open_positions_reached", 0.0, snapshot=snapshot)

        if state.committed_notional_usd >= self.settings.bankroll_usd:
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "bankroll_committed"
            return RiskDecision(False, "bankroll_fully_committed", 0.0, snapshot=snapshot)

        if not (self.settings.min_price <= signal.price_hint <= self.settings.max_price):
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "price_band"
            return RiskDecision(False, "price_outside_allowed_band", 0.0, snapshot=snapshot)

        base_notional = self.settings.bankroll_usd * self.settings.risk_per_trade_pct
        # Scale by confidence and cap at observed smart-money notional.
        max_notional = min(base_notional * (0.7 + signal.confidence), signal.observed_notional)
        max_notional = min(max_notional, wallet_remaining, portfolio_remaining, condition_remaining)
        snapshot["base_notional"] = base_notional
        snapshot["proposed_max_notional"] = max_notional
        if max_notional < 5:
            reason_codes = []
            if wallet_cap_usd > 0.0 and wallet_remaining < 5.0:
                reason_codes.append(REASON_WALLET_EXPOSURE_CAP_REACHED)
            if portfolio_cap_usd > 0.0 and portfolio_remaining < 5.0:
                reason_codes.append(REASON_PORTFOLIO_EXPOSURE_CAP_REACHED)
            if condition_cap_usd > 0.0 and condition_remaining < 5.0:
                reason_codes.append(REASON_CONDITION_EXPOSURE_CAP_REACHED)
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "notional_too_small"
            if reason_codes:
                snapshot["reason_codes"] = reason_codes
                primary = self._primary_reason(reason_codes, reason_codes[0])
                snapshot["primary_reason"] = primary
                return RiskDecision(False, primary, 0.0, snapshot=snapshot)
            return RiskDecision(False, "calculated_notional_too_small", 0.0, snapshot=snapshot)

        remaining_capacity = max(0.0, self.settings.bankroll_usd - state.committed_notional_usd)
        snapshot["remaining_capacity_usd"] = remaining_capacity
        max_notional = min(max_notional, remaining_capacity)
        snapshot["capped_max_notional"] = max_notional
        if max_notional < 5:
            snapshot["allowed"] = False
            snapshot["reject_stage"] = "bankroll_capacity"
            return RiskDecision(False, "remaining_bankroll_capacity_too_small", 0.0, snapshot=snapshot)

        snapshot["allowed"] = True
        return RiskDecision(True, "ok", max_notional, snapshot=snapshot)
