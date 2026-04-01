from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class PersistedControlState:
    decision_mode: str = "manual"
    pause_opening: bool = False
    reduce_only: bool = False
    emergency_stop: bool = False
    clear_stale_pending_requested_ts: int = 0
    clear_risk_breakers_requested_ts: int = 0
    updated_ts: int = 0

    @classmethod
    def from_payload(cls, payload: dict[str, object] | None) -> "PersistedControlState":
        source = dict(payload or {})
        return cls(
            decision_mode=str(source.get("decision_mode") or "manual"),
            pause_opening=bool(source.get("pause_opening", False)),
            reduce_only=bool(source.get("reduce_only", False)),
            emergency_stop=bool(source.get("emergency_stop", False)),
            clear_stale_pending_requested_ts=int(source.get("clear_stale_pending_requested_ts") or 0),
            clear_risk_breakers_requested_ts=int(source.get("clear_risk_breakers_requested_ts") or 0),
            updated_ts=int(source.get("updated_ts") or 0),
        )

    def to_payload(self) -> dict[str, object]:
        return {
            "decision_mode": str(self.decision_mode or "manual"),
            "pause_opening": bool(self.pause_opening),
            "reduce_only": bool(self.reduce_only),
            "emergency_stop": bool(self.emergency_stop),
            "clear_stale_pending_requested_ts": int(self.clear_stale_pending_requested_ts or 0),
            "clear_risk_breakers_requested_ts": int(self.clear_risk_breakers_requested_ts or 0),
            "updated_ts": int(self.updated_ts or 0),
        }
