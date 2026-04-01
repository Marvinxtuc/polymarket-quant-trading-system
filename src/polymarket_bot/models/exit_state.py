from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Mapping

TIME_EXIT_STAGE_IDLE = "idle"
TIME_EXIT_STAGE_RETRY = "retry"
TIME_EXIT_STAGE_FORCE_EXIT = "force_exit"
_VALID_TIME_EXIT_STAGES = {
    TIME_EXIT_STAGE_IDLE,
    TIME_EXIT_STAGE_RETRY,
    TIME_EXIT_STAGE_FORCE_EXIT,
}


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


@dataclass(slots=True)
class TimeExitState:
    stage: str = TIME_EXIT_STAGE_IDLE
    attempt_count: int = 0
    consecutive_failures: int = 0
    priority: int = 0
    priority_reason: str = ""
    market_volatility_bps: float = 0.0
    last_attempt_ts: int = 0
    last_failure_ts: int = 0
    last_success_ts: int = 0
    next_retry_ts: int = 0
    force_exit_armed_ts: int = 0
    last_result: str = ""
    last_error: str = ""

    def to_payload(self) -> dict[str, object]:
        payload = asdict(self)
        payload["stage"] = self.stage if self.stage in _VALID_TIME_EXIT_STAGES else TIME_EXIT_STAGE_IDLE
        payload["attempt_count"] = max(0, int(self.attempt_count))
        payload["consecutive_failures"] = max(0, int(self.consecutive_failures))
        payload["priority"] = max(0, int(self.priority))
        payload["priority_reason"] = str(self.priority_reason or "")
        payload["market_volatility_bps"] = max(0.0, float(self.market_volatility_bps or 0.0))
        payload["last_attempt_ts"] = max(0, int(self.last_attempt_ts))
        payload["last_failure_ts"] = max(0, int(self.last_failure_ts))
        payload["last_success_ts"] = max(0, int(self.last_success_ts))
        payload["next_retry_ts"] = max(0, int(self.next_retry_ts))
        payload["force_exit_armed_ts"] = max(0, int(self.force_exit_armed_ts))
        payload["last_result"] = str(self.last_result or "")
        payload["last_error"] = str(self.last_error or "")
        return payload


def default_time_exit_state() -> TimeExitState:
    return TimeExitState()


def normalize_time_exit_state(value: Mapping[str, object] | object | None) -> TimeExitState:
    if isinstance(value, TimeExitState):
        return TimeExitState(**value.to_payload())
    payload = dict(value) if isinstance(value, Mapping) else {}
    stage = str(payload.get("stage") or TIME_EXIT_STAGE_IDLE).strip().lower()
    if stage not in _VALID_TIME_EXIT_STAGES:
        stage = TIME_EXIT_STAGE_IDLE
    return TimeExitState(
        stage=stage,
        attempt_count=max(0, _safe_int(payload.get("attempt_count"))),
        consecutive_failures=max(0, _safe_int(payload.get("consecutive_failures"))),
        priority=max(0, _safe_int(payload.get("priority"))),
        priority_reason=str(payload.get("priority_reason") or ""),
        market_volatility_bps=max(0.0, _safe_float(payload.get("market_volatility_bps"))),
        last_attempt_ts=max(0, _safe_int(payload.get("last_attempt_ts"))),
        last_failure_ts=max(0, _safe_int(payload.get("last_failure_ts"))),
        last_success_ts=max(0, _safe_int(payload.get("last_success_ts"))),
        next_retry_ts=max(0, _safe_int(payload.get("next_retry_ts"))),
        force_exit_armed_ts=max(0, _safe_int(payload.get("force_exit_armed_ts"))),
        last_result=str(payload.get("last_result") or ""),
        last_error=str(payload.get("last_error") or ""),
    )
