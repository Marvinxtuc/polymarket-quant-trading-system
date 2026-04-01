from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class RiskBreakerState:
    valuation_currency: str = "USD"
    timezone: str = "UTC"
    day_key: str = ""
    opening_allowed: bool = True
    manual_required: bool = False
    manual_lock: bool = False
    reason_codes: tuple[str, ...] = ()
    loss_streak_count: int = 0
    loss_streak_limit: int = 0
    loss_streak_blocked: bool = False
    intraday_drawdown_pct: float = 0.0
    intraday_drawdown_limit_pct: float = 0.0
    intraday_drawdown_blocked: bool = False
    equity_now_usd: float = 0.0
    equity_peak_usd: float = 0.0
    clear_requested_ts: int = 0
    updated_ts: int = 0

    @classmethod
    def from_payload(cls, payload: dict[str, object] | None) -> "RiskBreakerState":
        source = dict(payload or {})
        reasons = []
        for raw in list(source.get("reason_codes") or []):
            text = str(raw or "").strip()
            if text and text not in reasons:
                reasons.append(text)
        return cls(
            valuation_currency=str(source.get("valuation_currency") or "USD").strip().upper() or "USD",
            timezone=str(source.get("timezone") or "UTC").strip() or "UTC",
            day_key=str(source.get("day_key") or "").strip(),
            opening_allowed=bool(source.get("opening_allowed", True)),
            manual_required=bool(source.get("manual_required", False)),
            manual_lock=bool(source.get("manual_lock", False)),
            reason_codes=tuple(reasons),
            loss_streak_count=max(0, int(source.get("loss_streak_count") or 0)),
            loss_streak_limit=max(0, int(source.get("loss_streak_limit") or 0)),
            loss_streak_blocked=bool(source.get("loss_streak_blocked", False)),
            intraday_drawdown_pct=max(0.0, float(source.get("intraday_drawdown_pct") or 0.0)),
            intraday_drawdown_limit_pct=max(0.0, float(source.get("intraday_drawdown_limit_pct") or 0.0)),
            intraday_drawdown_blocked=bool(source.get("intraday_drawdown_blocked", False)),
            equity_now_usd=max(0.0, float(source.get("equity_now_usd") or 0.0)),
            equity_peak_usd=max(0.0, float(source.get("equity_peak_usd") or 0.0)),
            clear_requested_ts=max(0, int(source.get("clear_requested_ts") or 0)),
            updated_ts=max(0, int(source.get("updated_ts") or 0)),
        )

    def to_payload(self) -> dict[str, object]:
        return {
            "valuation_currency": str(self.valuation_currency or "USD").strip().upper() or "USD",
            "timezone": str(self.timezone or "UTC").strip() or "UTC",
            "day_key": str(self.day_key or "").strip(),
            "opening_allowed": bool(self.opening_allowed),
            "manual_required": bool(self.manual_required),
            "manual_lock": bool(self.manual_lock),
            "reason_codes": [str(code) for code in self.reason_codes if str(code).strip()],
            "loss_streak_count": int(self.loss_streak_count or 0),
            "loss_streak_limit": int(self.loss_streak_limit or 0),
            "loss_streak_blocked": bool(self.loss_streak_blocked),
            "intraday_drawdown_pct": float(self.intraday_drawdown_pct or 0.0),
            "intraday_drawdown_limit_pct": float(self.intraday_drawdown_limit_pct or 0.0),
            "intraday_drawdown_blocked": bool(self.intraday_drawdown_blocked),
            "equity_now_usd": float(self.equity_now_usd or 0.0),
            "equity_peak_usd": float(self.equity_peak_usd or 0.0),
            "clear_requested_ts": int(self.clear_requested_ts or 0),
            "updated_ts": int(self.updated_ts or 0),
        }


def default_risk_breaker_state(*, day_key: str = "", now_ts: int = 0) -> dict[str, object]:
    return RiskBreakerState(day_key=str(day_key or ""), updated_ts=int(now_ts or 0)).to_payload()
