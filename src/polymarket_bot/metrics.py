from __future__ import annotations

import math
from typing import Mapping

from polymarket_bot.alerts import (
    ALERT_ACCOUNT_SNAPSHOT_STALE,
    ALERT_ADMISSION_FAIL_CLOSED,
    ALERT_BUY_BLOCKED_TOO_LONG,
    ALERT_CODE_WHITELIST,
    ALERT_EVENT_STREAM_STALE,
    ALERT_HOT_WALLET_CAP_EXCEEDED,
    ALERT_KILL_SWITCH_INFLIGHT,
    ALERT_KILL_SWITCH_MANUAL_REQUIRED,
    ALERT_LEDGER_DIFF_EXCEEDED,
    ALERT_RECONCILIATION_FAIL,
    ALERT_RUNNER_HEARTBEAT_STALE,
    ALERT_SIGNER_UNHEALTHY,
    ALERT_WRITER_CONFLICT_READONLY,
    severity_for_alert_code,
)
from polymarket_bot.heartbeat import heartbeat_age_seconds, heartbeat_is_stale, normalize_runner_heartbeat


def _bool_gauge(value: object) -> float:
    return 1.0 if bool(value) else 0.0


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return default
    if math.isnan(parsed) or math.isinf(parsed):
        return default
    return parsed


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _to_dict(value: object) -> dict[str, object]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def build_observability_snapshot(
    *,
    state_payload: Mapping[str, object],
    now_ts: int,
    heartbeat_stale_after_seconds: int,
    buy_blocked_alert_after_seconds: int,
) -> dict[str, object]:
    payload = dict(state_payload or {})
    admission = _to_dict(payload.get("admission"))
    evidence = _to_dict(admission.get("evidence_summary"))
    kill_switch = _to_dict(payload.get("kill_switch"))
    signer = _to_dict(payload.get("signer_security"))
    control_plane_security = _to_dict(payload.get("control_plane_security"))
    buy_blocked = _to_dict(payload.get("buy_blocked"))
    candidates = _to_dict(payload.get("candidates"))
    candidate_observability = _to_dict(candidates.get("observability"))
    candidate_lifecycle = _to_dict(candidate_observability.get("lifecycle"))
    candidate_reason_layer_counts = {
        str(reason): _to_dict(layer_counts)
        for reason, layer_counts in _to_dict(candidate_lifecycle.get("reason_layer_counts")).items()
    }

    heartbeat = normalize_runner_heartbeat(payload.get("runner_heartbeat"))
    hb_age = heartbeat_age_seconds(heartbeat, now_ts=int(now_ts))
    hb_stale = heartbeat_is_stale(
        heartbeat,
        now_ts=int(now_ts),
        stale_after_seconds=max(1, int(heartbeat_stale_after_seconds or 1)),
    )

    opening_allowed = bool(admission.get("opening_allowed", True))
    reconciliation_status = str(evidence.get("reconciliation_status") or payload.get("reconciliation_status") or "").strip().lower()
    account_snapshot_age_seconds = _safe_int(
        evidence.get("account_snapshot_age_seconds"),
        _safe_int(_to_dict(payload.get("reconciliation")).get("account_snapshot_age_seconds"), 0),
    )
    event_stream_age_seconds = _safe_int(
        evidence.get("broker_event_sync_age_seconds"),
        _safe_int(_to_dict(payload.get("reconciliation")).get("broker_event_sync_age_seconds"), 0),
    )
    account_snapshot_stale_threshold = max(1, _safe_int(evidence.get("account_snapshot_stale_threshold_seconds"), 1))
    event_stream_stale_threshold = max(1, _safe_int(evidence.get("broker_event_stale_threshold_seconds"), 1))
    ledger_diff = _safe_float(evidence.get("ledger_diff"), 0.0)
    ledger_diff_threshold = max(0.0, _safe_float(evidence.get("ledger_diff_threshold_usd"), 0.0))

    buy_blocked_active = bool(buy_blocked.get("active", not opening_allowed))
    buy_blocked_since_ts = _safe_int(buy_blocked.get("since_ts"), 0)
    buy_blocked_duration_seconds = max(0, _safe_int(buy_blocked.get("duration_seconds"), 0))
    buy_blocked_reason = str(buy_blocked.get("reason_code") or "")

    signer_reasons = set(str(item or "").strip() for item in list(signer.get("reason_codes") or []) if str(item or "").strip())
    write_reason_codes = set(
        str(item or "").strip() for item in list(control_plane_security.get("reason_codes") or []) if str(item or "").strip()
    )

    alert_active: dict[str, bool] = {code: False for code in ALERT_CODE_WHITELIST}

    if hb_stale:
        alert_active[ALERT_RUNNER_HEARTBEAT_STALE] = True
    if not opening_allowed:
        alert_active[ALERT_ADMISSION_FAIL_CLOSED] = True
    if reconciliation_status == "fail":
        alert_active[ALERT_RECONCILIATION_FAIL] = True
    if account_snapshot_age_seconds > account_snapshot_stale_threshold:
        alert_active[ALERT_ACCOUNT_SNAPSHOT_STALE] = True
    if event_stream_age_seconds > event_stream_stale_threshold:
        alert_active[ALERT_EVENT_STREAM_STALE] = True
    if ledger_diff_threshold > 0.0 and abs(ledger_diff) > ledger_diff_threshold:
        alert_active[ALERT_LEDGER_DIFF_EXCEEDED] = True

    kill_switch_opening_allowed = bool(kill_switch.get("opening_allowed", True))
    kill_switch_manual_required = bool(kill_switch.get("manual_required", False))
    kill_switch_broker_safe_confirmed = bool(kill_switch.get("broker_safe_confirmed", False))
    if kill_switch_manual_required:
        alert_active[ALERT_KILL_SWITCH_MANUAL_REQUIRED] = True
    if not kill_switch_opening_allowed and not kill_switch_manual_required and not kill_switch_broker_safe_confirmed:
        alert_active[ALERT_KILL_SWITCH_INFLIGHT] = True

    signer_required = bool(signer.get("signer_required", False))
    signer_healthy = bool(signer.get("signer_healthy", True))
    raw_key_detected = bool(signer.get("raw_key_detected", False))
    hot_wallet_cap_ok = bool(signer.get("hot_wallet_cap_ok", True))
    if signer_required and (not signer_healthy or raw_key_detected):
        alert_active[ALERT_SIGNER_UNHEALTHY] = True
    if (not hot_wallet_cap_ok) or ("hot_wallet_cap_exceeded" in signer_reasons):
        alert_active[ALERT_HOT_WALLET_CAP_EXCEEDED] = True

    write_api_available = bool(control_plane_security.get("write_api_available", False))
    readonly_mode = bool(control_plane_security.get("readonly_mode", not write_api_available))
    if readonly_mode and ("single_writer_conflict" in write_reason_codes):
        alert_active[ALERT_WRITER_CONFLICT_READONLY] = True

    if buy_blocked_active and buy_blocked_duration_seconds >= max(1, int(buy_blocked_alert_after_seconds or 1)):
        alert_active[ALERT_BUY_BLOCKED_TOO_LONG] = True

    active_alerts = []
    for alert_code in ALERT_CODE_WHITELIST:
        if not alert_active.get(alert_code, False):
            continue
        active_alerts.append(
            {
                "alert_code": alert_code,
                "severity": severity_for_alert_code(alert_code),
            }
        )

    metrics = {
        "runner_heartbeat_age_seconds": float(hb_age),
        "runner_heartbeat_stale": _bool_gauge(hb_stale),
        "runner_writer_active": _bool_gauge(heartbeat.get("writer_active")),
        "admission_opening_allowed": _bool_gauge(opening_allowed),
        "reconciliation_fail": _bool_gauge(reconciliation_status == "fail"),
        "account_snapshot_stale": _bool_gauge(account_snapshot_age_seconds > account_snapshot_stale_threshold),
        "event_stream_stale": _bool_gauge(event_stream_age_seconds > event_stream_stale_threshold),
        "ledger_diff_exceeded": _bool_gauge(ledger_diff_threshold > 0.0 and abs(ledger_diff) > ledger_diff_threshold),
        "kill_switch_manual_required": _bool_gauge(kill_switch_manual_required),
        "signer_healthy": _bool_gauge(signer_healthy),
        "writer_readonly_mode": _bool_gauge(readonly_mode),
        "hot_wallet_cap_ok": _bool_gauge(hot_wallet_cap_ok),
        "buy_blocked": _bool_gauge(buy_blocked_active),
        "buy_blocked_duration_seconds": float(buy_blocked_duration_seconds),
        "candidate_expired_discarded_count": float(_safe_int(candidate_lifecycle.get("expired_discarded_count"), 0)),
    }

    return {
        "generated_ts": int(now_ts),
        "heartbeat": {
            **heartbeat,
            "age_seconds": int(hb_age),
            "stale": bool(hb_stale),
            "stale_after_seconds": int(max(1, int(heartbeat_stale_after_seconds or 1))),
        },
        "admission": {
            "opening_allowed": bool(opening_allowed),
            "mode": str(admission.get("mode") or ""),
            "reason_codes": [str(item) for item in list(admission.get("reason_codes") or []) if str(item).strip()],
            "snapshot_age_seconds": int(account_snapshot_age_seconds),
            "event_sync_age_seconds": int(event_stream_age_seconds),
            "ledger_diff": float(ledger_diff),
            "reconciliation_status": str(reconciliation_status or "unknown"),
        },
        "kill_switch": {
            "phase": str(kill_switch.get("phase") or ""),
            "manual_required": bool(kill_switch_manual_required),
            "broker_safe_confirmed": bool(kill_switch_broker_safe_confirmed),
            "open_buy_order_count": len(list(kill_switch.get("open_buy_order_ids") or [])),
        },
        "writer": {
            "write_api_available": bool(write_api_available),
            "readonly_mode": bool(readonly_mode),
            "source_policy": str(control_plane_security.get("source_policy") or "local_only"),
        },
        "signer": {
            "required": bool(signer_required),
            "healthy": bool(signer_healthy),
            "raw_key_detected": bool(raw_key_detected),
            "hot_wallet_cap_ok": bool(hot_wallet_cap_ok),
        },
        "buy_blocked": {
            "active": bool(buy_blocked_active),
            "since_ts": int(buy_blocked_since_ts),
            "duration_seconds": int(buy_blocked_duration_seconds),
            "reason_code": buy_blocked_reason,
        },
        "candidates": {
            "lifecycle": {
                "expired_discarded_count": int(_safe_int(candidate_lifecycle.get("expired_discarded_count"), 0)),
                "block_reasons": dict(candidate_lifecycle.get("block_reasons") or {}),
                "block_layers": dict(candidate_lifecycle.get("block_layers") or {}),
                "reason_layer_counts": candidate_reason_layer_counts,
            }
        },
        "active_alerts": active_alerts,
        "metrics": metrics,
    }


def _metric_line(name: str, value: float, labels: dict[str, str] | None = None) -> str:
    if labels:
        label_text = ",".join(f'{key}="{val}"' for key, val in sorted(labels.items()))
        return f"{name}{{{label_text}}} {value}"
    return f"{name} {value}"


def render_prometheus_metrics(snapshot: Mapping[str, object]) -> str:
    obs = dict(snapshot or {})
    metrics = _to_dict(obs.get("metrics"))
    lines: list[str] = [
        "# HELP polymarket_runner_heartbeat_age_seconds Seconds since active runner last heartbeat.",
        "# TYPE polymarket_runner_heartbeat_age_seconds gauge",
        _metric_line("polymarket_runner_heartbeat_age_seconds", _safe_float(metrics.get("runner_heartbeat_age_seconds"))),
        "# HELP polymarket_runner_heartbeat_stale Runner heartbeat stale flag (1 stale, 0 healthy).",
        "# TYPE polymarket_runner_heartbeat_stale gauge",
        _metric_line("polymarket_runner_heartbeat_stale", _safe_float(metrics.get("runner_heartbeat_stale"))),
        "# HELP polymarket_runner_writer_active Active runner writer flag (1 active, 0 inactive).",
        "# TYPE polymarket_runner_writer_active gauge",
        _metric_line("polymarket_runner_writer_active", _safe_float(metrics.get("runner_writer_active"))),
        "# HELP polymarket_admission_opening_allowed Admission gate opening_allowed (1 yes, 0 no).",
        "# TYPE polymarket_admission_opening_allowed gauge",
        _metric_line("polymarket_admission_opening_allowed", _safe_float(metrics.get("admission_opening_allowed"))),
        "# HELP polymarket_reconciliation_fail Reconciliation failed flag.",
        "# TYPE polymarket_reconciliation_fail gauge",
        _metric_line("polymarket_reconciliation_fail", _safe_float(metrics.get("reconciliation_fail"))),
        "# HELP polymarket_account_snapshot_stale Account snapshot stale flag.",
        "# TYPE polymarket_account_snapshot_stale gauge",
        _metric_line("polymarket_account_snapshot_stale", _safe_float(metrics.get("account_snapshot_stale"))),
        "# HELP polymarket_event_stream_stale Broker event stream stale flag.",
        "# TYPE polymarket_event_stream_stale gauge",
        _metric_line("polymarket_event_stream_stale", _safe_float(metrics.get("event_stream_stale"))),
        "# HELP polymarket_ledger_diff_exceeded Ledger diff threshold exceeded flag.",
        "# TYPE polymarket_ledger_diff_exceeded gauge",
        _metric_line("polymarket_ledger_diff_exceeded", _safe_float(metrics.get("ledger_diff_exceeded"))),
        "# HELP polymarket_kill_switch_manual_required Kill switch manual-required flag.",
        "# TYPE polymarket_kill_switch_manual_required gauge",
        _metric_line("polymarket_kill_switch_manual_required", _safe_float(metrics.get("kill_switch_manual_required"))),
        "# HELP polymarket_signer_healthy Signer healthy flag.",
        "# TYPE polymarket_signer_healthy gauge",
        _metric_line("polymarket_signer_healthy", _safe_float(metrics.get("signer_healthy"))),
        "# HELP polymarket_writer_readonly_mode Web/control readonly mode flag.",
        "# TYPE polymarket_writer_readonly_mode gauge",
        _metric_line("polymarket_writer_readonly_mode", _safe_float(metrics.get("writer_readonly_mode"))),
        "# HELP polymarket_hot_wallet_cap_ok Hot-wallet cap healthy flag.",
        "# TYPE polymarket_hot_wallet_cap_ok gauge",
        _metric_line("polymarket_hot_wallet_cap_ok", _safe_float(metrics.get("hot_wallet_cap_ok"))),
        "# HELP polymarket_buy_blocked Buy blocked flag (1 blocked, 0 allowed).",
        "# TYPE polymarket_buy_blocked gauge",
        _metric_line("polymarket_buy_blocked", _safe_float(metrics.get("buy_blocked"))),
        "# HELP polymarket_buy_blocked_duration_seconds Seconds since BUY became blocked.",
        "# TYPE polymarket_buy_blocked_duration_seconds gauge",
        _metric_line("polymarket_buy_blocked_duration_seconds", _safe_float(metrics.get("buy_blocked_duration_seconds"))),
        "# HELP polymarket_candidate_expired_discarded_count Expired and discarded candidate count from current state export.",
        "# TYPE polymarket_candidate_expired_discarded_count gauge",
        _metric_line("polymarket_candidate_expired_discarded_count", _safe_float(metrics.get("candidate_expired_discarded_count"))),
        "# HELP polymarket_candidate_blocked_total Candidate blocked count by reason_code and block_layer.",
        "# TYPE polymarket_candidate_blocked_total gauge",
        "# HELP polymarket_alert_active Fixed alert-code active flag.",
        "# TYPE polymarket_alert_active gauge",
    ]

    candidate_reason_layer_counts = {
        str(reason): _to_dict(layer_counts)
        for reason, layer_counts in _to_dict(_to_dict(obs.get("candidates")).get("lifecycle")).get("reason_layer_counts", {}).items()
    }
    lifetime_layers = {"candidate", "decision", "execution_precheck"}
    for layer in sorted(lifetime_layers):
        layer_counts = _to_dict(candidate_reason_layer_counts.get("candidate_lifetime_expired"))
        lines.append(
            _metric_line(
                "polymarket_candidate_blocked_total",
                float(_safe_int(layer_counts.get(layer), 0)),
                labels={"reason_code": "candidate_lifetime_expired", "block_layer": layer},
            )
        )

    active = {
        str(item.get("alert_code") or ""): True
        for item in list(obs.get("active_alerts") or [])
        if isinstance(item, dict) and str(item.get("alert_code") or "").strip()
    }
    for alert_code in ALERT_CODE_WHITELIST:
        lines.append(
            _metric_line(
                "polymarket_alert_active",
                1.0 if active.get(alert_code, False) else 0.0,
                labels={"alert_code": alert_code, "severity": severity_for_alert_code(alert_code)},
            )
        )
    lines.append("")
    return "\n".join(lines)
