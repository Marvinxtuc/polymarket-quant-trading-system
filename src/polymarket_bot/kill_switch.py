from __future__ import annotations

import time
from typing import Mapping

MODE_NONE = "none"
MODE_PAUSE_OPENING = "pause_opening"
MODE_REDUCE_ONLY = "reduce_only"
MODE_EMERGENCY_STOP = "emergency_stop"

PHASE_IDLE = "IDLE"
PHASE_REQUESTED = "REQUESTED"
PHASE_CANCELING_BUY = "CANCELING_BUY"
PHASE_WAITING_BROKER_TERMINAL = "WAITING_BROKER_TERMINAL"
PHASE_SAFE_CONFIRMED = "SAFE_CONFIRMED"
PHASE_FAILED_MANUAL_REQUIRED = "FAILED_MANUAL_REQUIRED"

_VALID_MODES = {
    MODE_NONE,
    MODE_PAUSE_OPENING,
    MODE_REDUCE_ONLY,
    MODE_EMERGENCY_STOP,
}
_VALID_PHASES = {
    PHASE_IDLE,
    PHASE_REQUESTED,
    PHASE_CANCELING_BUY,
    PHASE_WAITING_BROKER_TERMINAL,
    PHASE_SAFE_CONFIRMED,
    PHASE_FAILED_MANUAL_REQUIRED,
}
_TERMINAL_STATUSES = {
    "filled",
    "canceled",
    "failed",
    "rejected",
    "unmatched",
    "expired",
}


def requested_mode(*, pause_opening: bool, reduce_only: bool, emergency_stop: bool) -> str:
    if bool(emergency_stop):
        return MODE_EMERGENCY_STOP
    if bool(reduce_only):
        return MODE_REDUCE_ONLY
    if bool(pause_opening):
        return MODE_PAUSE_OPENING
    return MODE_NONE


def status_is_terminal(status: object) -> bool:
    normalized = str(status or "").strip().lower()
    if normalized == "cancelled":
        normalized = "canceled"
    return normalized in _TERMINAL_STATUSES


def default_state(*, now_ts: int | None = None) -> dict[str, object]:
    ts = int(now_ts or 0)
    return {
        "mode_requested": MODE_NONE,
        "phase": PHASE_IDLE,
        "opening_allowed": True,
        "reduce_only": False,
        "halted": False,
        "latched": False,
        "broker_safe_confirmed": True,
        "manual_required": False,
        "auto_recover": True,
        "reason_codes": [],
        "open_buy_order_ids": [],
        "non_terminal_buy_order_ids": [],
        "cancel_requested_order_ids": [],
        "tracked_buy_order_ids": [],
        "pending_buy_order_keys": [],
        "cancel_attempts": 0,
        "query_error_count": 0,
        "requested_ts": ts,
        "last_broker_check_ts": ts,
        "safe_confirmed_ts": 0,
        "updated_ts": ts,
        "last_error": "",
    }


def normalize_state(payload: Mapping[str, object] | None) -> dict[str, object]:
    source = dict(payload or {})
    baseline = default_state()

    mode_requested = str(source.get("mode_requested") or MODE_NONE).strip().lower()
    if mode_requested not in _VALID_MODES:
        mode_requested = MODE_NONE

    phase = str(source.get("phase") or PHASE_IDLE).strip().upper()
    if phase not in _VALID_PHASES:
        phase = PHASE_IDLE

    normalized = dict(baseline)
    normalized.update(
        {
            "mode_requested": mode_requested,
            "phase": phase,
            "opening_allowed": bool(source.get("opening_allowed", baseline["opening_allowed"])),
            "reduce_only": bool(source.get("reduce_only", baseline["reduce_only"])),
            "halted": bool(source.get("halted", baseline["halted"])),
            "latched": bool(source.get("latched", baseline["latched"])),
            "broker_safe_confirmed": bool(
                source.get("broker_safe_confirmed", baseline["broker_safe_confirmed"])
            ),
            "manual_required": bool(source.get("manual_required", baseline["manual_required"])),
            "auto_recover": bool(source.get("auto_recover", baseline["auto_recover"])),
            "reason_codes": [str(item) for item in list(source.get("reason_codes") or []) if str(item).strip()],
            "open_buy_order_ids": sorted(
                {
                    str(item).strip()
                    for item in list(source.get("open_buy_order_ids") or [])
                    if str(item).strip()
                }
            ),
            "non_terminal_buy_order_ids": sorted(
                {
                    str(item).strip()
                    for item in list(source.get("non_terminal_buy_order_ids") or [])
                    if str(item).strip()
                }
            ),
            "cancel_requested_order_ids": sorted(
                {
                    str(item).strip()
                    for item in list(source.get("cancel_requested_order_ids") or [])
                    if str(item).strip()
                }
            ),
            "tracked_buy_order_ids": sorted(
                {
                    str(item).strip()
                    for item in list(source.get("tracked_buy_order_ids") or [])
                    if str(item).strip()
                }
            ),
            "pending_buy_order_keys": sorted(
                {
                    str(item).strip()
                    for item in list(source.get("pending_buy_order_keys") or [])
                    if str(item).strip()
                }
            ),
            "cancel_attempts": int(source.get("cancel_attempts") or 0),
            "query_error_count": int(source.get("query_error_count") or 0),
            "requested_ts": int(source.get("requested_ts") or 0),
            "last_broker_check_ts": int(source.get("last_broker_check_ts") or 0),
            "safe_confirmed_ts": int(source.get("safe_confirmed_ts") or 0),
            "updated_ts": int(source.get("updated_ts") or 0),
            "last_error": str(source.get("last_error") or ""),
        }
    )
    if not normalized["updated_ts"]:
        normalized["updated_ts"] = int(time.time())
    return normalized
