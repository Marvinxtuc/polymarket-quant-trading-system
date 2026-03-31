from __future__ import annotations


LOOP_STATUS_IDLE = "idle"
LOOP_STATUS_RUNNING = "running"
LOOP_STATUS_ERROR = "error"
LOOP_STATUS_STOPPED = "stopped"

_ALLOWED_LOOP_STATUSES = {
    LOOP_STATUS_IDLE,
    LOOP_STATUS_RUNNING,
    LOOP_STATUS_ERROR,
    LOOP_STATUS_STOPPED,
}


def default_runner_heartbeat(*, now_ts: int = 0) -> dict[str, object]:
    ts = int(now_ts or 0)
    return {
        "last_seen_ts": ts,
        "last_cycle_started_ts": ts,
        "last_cycle_finished_ts": ts,
        "cycle_seq": 0,
        "loop_status": LOOP_STATUS_IDLE,
        "writer_active": False,
    }


def normalize_runner_heartbeat(payload: object) -> dict[str, object]:
    source = payload if isinstance(payload, dict) else {}
    normalized = default_runner_heartbeat(now_ts=int(source.get("last_seen_ts") or 0) if isinstance(source, dict) else 0)
    normalized["last_seen_ts"] = int(source.get("last_seen_ts") or 0) if isinstance(source, dict) else 0
    normalized["last_cycle_started_ts"] = int(source.get("last_cycle_started_ts") or 0) if isinstance(source, dict) else 0
    normalized["last_cycle_finished_ts"] = int(source.get("last_cycle_finished_ts") or 0) if isinstance(source, dict) else 0
    normalized["cycle_seq"] = max(0, int(source.get("cycle_seq") or 0)) if isinstance(source, dict) else 0
    status = str(source.get("loop_status") or LOOP_STATUS_IDLE).strip().lower() if isinstance(source, dict) else LOOP_STATUS_IDLE
    normalized["loop_status"] = status if status in _ALLOWED_LOOP_STATUSES else LOOP_STATUS_IDLE
    normalized["writer_active"] = bool(source.get("writer_active")) if isinstance(source, dict) else False
    return normalized


def heartbeat_age_seconds(payload: object, *, now_ts: int) -> int:
    normalized = normalize_runner_heartbeat(payload)
    last_seen_ts = int(normalized.get("last_seen_ts") or 0)
    if last_seen_ts <= 0:
        return max(0, int(now_ts or 0))
    return max(0, int(now_ts or 0) - last_seen_ts)


def heartbeat_is_stale(payload: object, *, now_ts: int, stale_after_seconds: int) -> bool:
    threshold = max(1, int(stale_after_seconds or 1))
    normalized = normalize_runner_heartbeat(payload)
    if int(normalized.get("last_seen_ts") or 0) <= 0:
        return True
    return heartbeat_age_seconds(normalized, now_ts=int(now_ts or 0)) > threshold
