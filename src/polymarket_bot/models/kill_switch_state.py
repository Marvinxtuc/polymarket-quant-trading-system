from __future__ import annotations

from dataclasses import dataclass, field

from polymarket_bot.kill_switch import normalize_state


@dataclass(slots=True)
class PersistedKillSwitchState:
    mode_requested: str = "none"
    phase: str = "IDLE"
    opening_allowed: bool = True
    reduce_only: bool = False
    halted: bool = False
    latched: bool = False
    broker_safe_confirmed: bool = True
    manual_required: bool = False
    auto_recover: bool = True
    reason_codes: list[str] = field(default_factory=list)
    open_buy_order_ids: list[str] = field(default_factory=list)
    non_terminal_buy_order_ids: list[str] = field(default_factory=list)
    cancel_requested_order_ids: list[str] = field(default_factory=list)
    tracked_buy_order_ids: list[str] = field(default_factory=list)
    pending_buy_order_keys: list[str] = field(default_factory=list)
    cancel_attempts: int = 0
    query_error_count: int = 0
    requested_ts: int = 0
    last_broker_check_ts: int = 0
    safe_confirmed_ts: int = 0
    updated_ts: int = 0
    last_error: str = ""

    @classmethod
    def from_payload(cls, payload: dict[str, object] | None) -> "PersistedKillSwitchState":
        normalized = normalize_state(payload)
        return cls(
            mode_requested=str(normalized.get("mode_requested") or "none"),
            phase=str(normalized.get("phase") or "IDLE"),
            opening_allowed=bool(normalized.get("opening_allowed", True)),
            reduce_only=bool(normalized.get("reduce_only", False)),
            halted=bool(normalized.get("halted", False)),
            latched=bool(normalized.get("latched", False)),
            broker_safe_confirmed=bool(normalized.get("broker_safe_confirmed", True)),
            manual_required=bool(normalized.get("manual_required", False)),
            auto_recover=bool(normalized.get("auto_recover", True)),
            reason_codes=list(normalized.get("reason_codes") or []),
            open_buy_order_ids=list(normalized.get("open_buy_order_ids") or []),
            non_terminal_buy_order_ids=list(normalized.get("non_terminal_buy_order_ids") or []),
            cancel_requested_order_ids=list(normalized.get("cancel_requested_order_ids") or []),
            tracked_buy_order_ids=list(normalized.get("tracked_buy_order_ids") or []),
            pending_buy_order_keys=list(normalized.get("pending_buy_order_keys") or []),
            cancel_attempts=int(normalized.get("cancel_attempts") or 0),
            query_error_count=int(normalized.get("query_error_count") or 0),
            requested_ts=int(normalized.get("requested_ts") or 0),
            last_broker_check_ts=int(normalized.get("last_broker_check_ts") or 0),
            safe_confirmed_ts=int(normalized.get("safe_confirmed_ts") or 0),
            updated_ts=int(normalized.get("updated_ts") or 0),
            last_error=str(normalized.get("last_error") or ""),
        )

    def to_payload(self) -> dict[str, object]:
        return {
            "mode_requested": str(self.mode_requested or "none"),
            "phase": str(self.phase or "IDLE"),
            "opening_allowed": bool(self.opening_allowed),
            "reduce_only": bool(self.reduce_only),
            "halted": bool(self.halted),
            "latched": bool(self.latched),
            "broker_safe_confirmed": bool(self.broker_safe_confirmed),
            "manual_required": bool(self.manual_required),
            "auto_recover": bool(self.auto_recover),
            "reason_codes": [str(item) for item in self.reason_codes if str(item).strip()],
            "open_buy_order_ids": [str(item) for item in self.open_buy_order_ids if str(item).strip()],
            "non_terminal_buy_order_ids": [str(item) for item in self.non_terminal_buy_order_ids if str(item).strip()],
            "cancel_requested_order_ids": [str(item) for item in self.cancel_requested_order_ids if str(item).strip()],
            "tracked_buy_order_ids": [str(item) for item in self.tracked_buy_order_ids if str(item).strip()],
            "pending_buy_order_keys": [str(item) for item in self.pending_buy_order_keys if str(item).strip()],
            "cancel_attempts": int(self.cancel_attempts or 0),
            "query_error_count": int(self.query_error_count or 0),
            "requested_ts": int(self.requested_ts or 0),
            "last_broker_check_ts": int(self.last_broker_check_ts or 0),
            "safe_confirmed_ts": int(self.safe_confirmed_ts or 0),
            "updated_ts": int(self.updated_ts or 0),
            "last_error": str(self.last_error or ""),
        }
