from __future__ import annotations

import json
import os
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib import error as urlerror
from urllib import request as urlrequest

from polymarket_bot.i18n import t as i18n_t

DEFAULT_MONITOR_30M_WINDOW_SECONDS = 1800
DEFAULT_MONITOR_12H_WINDOW_SECONDS = 43200
MONITOR_FRESHNESS_GRACE_SECONDS = 600


def _ffv_t(key: str, params: Mapping[str, object] | None = None, *, fallback: str = "") -> str:
    return i18n_t(f"report.fullFlowValidation.{key}", dict(params or {}), fallback=fallback)


def _ffv_status_label(value: object) -> str:
    raw = str(value or "").strip().lower() or "unknown"
    return _ffv_t(f"enum.status.{raw}", fallback=raw.upper())


def _ffv_readiness_label(value: object) -> str:
    raw = str(value or "").strip().lower() or "unknown"
    return _ffv_t(f"enum.readiness.{raw}", fallback=raw.upper())


def _utc_iso(ts: int | float | None = None) -> str:
    return datetime.fromtimestamp(int(ts or time.time()), tz=timezone.utc).isoformat()


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _tail(text: object, limit: int = 1600) -> str:
    value = str(text or "")
    if len(value) <= limit:
        return value
    return value[-limit:]


def _load_json_dict(path: str | Path) -> dict[str, Any]:
    target = Path(path).expanduser()
    try:
        with target.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: str | Path, payload: Mapping[str, object]) -> None:
    target = Path(path).expanduser()
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def _stage(name: str, status: str, message: str, **details: object) -> dict[str, object]:
    return {
        "name": str(name),
        "status": str(status).lower(),
        "message": str(message),
        "details": dict(details),
    }


def http_json(
    url: str,
    *,
    method: str = "GET",
    payload: Mapping[str, object] | None = None,
    timeout: int = 5,
) -> dict[str, object]:
    data: bytes | None = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urlrequest.Request(url, data=data, method=method.upper(), headers=headers)
    raw = ""
    payload_obj: dict[str, Any] | None = None
    try:
        with urlrequest.urlopen(req, timeout=timeout) as resp:
            code = int(getattr(resp, "status", 200) or 200)
            raw = resp.read().decode("utf-8", errors="replace")
    except urlerror.HTTPError as exc:
        code = int(exc.code or 0)
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            payload_obj = json.loads(raw)
        except Exception:
            payload_obj = None
        return {
            "ok": False,
            "status_code": code,
            "payload": payload_obj if isinstance(payload_obj, dict) else None,
            "error": f"http {code}",
            "raw": _tail(raw),
        }
    except Exception as exc:
        return {
            "ok": False,
            "status_code": None,
            "payload": None,
            "error": str(exc),
            "raw": "",
        }

    try:
        payload_obj = json.loads(raw)
    except Exception:
        payload_obj = None
    return {
        "ok": isinstance(payload_obj, dict),
        "status_code": code,
        "payload": payload_obj if isinstance(payload_obj, dict) else None,
        "error": None if isinstance(payload_obj, dict) else "invalid_json",
        "raw": _tail(raw),
    }


def run_command(
    args: Sequence[str],
    *,
    cwd: str | Path | None = None,
    env: Mapping[str, str] | None = None,
    timeout: int = 180,
) -> dict[str, object]:
    merged_env = os.environ.copy()
    if env:
        merged_env.update({str(k): str(v) for k, v in env.items()})
    start = time.time()
    try:
        proc = subprocess.run(
            [str(item) for item in args],
            cwd=str(cwd) if cwd is not None else None,
            env=merged_env,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        return {
            "ok": False,
            "returncode": None,
            "stdout": str(exc.stdout or ""),
            "stderr": str(exc.stderr or ""),
            "duration_seconds": round(time.time() - start, 3),
            "cmd": [str(item) for item in args],
            "error": f"timeout after {timeout}s",
        }
    except Exception as exc:
        return {
            "ok": False,
            "returncode": None,
            "stdout": "",
            "stderr": "",
            "duration_seconds": round(time.time() - start, 3),
            "cmd": [str(item) for item in args],
            "error": str(exc),
        }

    return {
        "ok": proc.returncode == 0,
        "returncode": int(proc.returncode),
        "stdout": str(proc.stdout or ""),
        "stderr": str(proc.stderr or ""),
        "duration_seconds": round(time.time() - start, 3),
        "cmd": [str(item) for item in args],
        "error": None,
    }


def _fetch_fresh_state_payload(
    state_url: str,
    *,
    http_client: Callable[..., dict[str, object]],
    timeout_seconds: int,
    poll_interval_seconds: int = 3,
) -> tuple[dict[str, object], dict[str, object]]:
    deadline = time.time() + max(1, int(timeout_seconds))
    last_response: dict[str, object] = {}
    while True:
        response = http_client(state_url, timeout=min(timeout_seconds, 10))
        last_response = response
        payload = response.get("payload")
        if response.get("ok") and isinstance(payload, dict):
            ok, message, details = _validate_state_payload(payload)
            if ok:
                return dict(payload), {
                    "status": "pass",
                    "message": message,
                    "details": {
                        "source": "api",
                        "url": state_url,
                        **details,
                    },
                }
            if time.time() >= deadline:
                return dict(payload), {
                    "status": "fail",
                    "message": message,
                    "details": {
                        "source": "api",
                        "url": state_url,
                    },
                }
        elif time.time() >= deadline:
            return {}, {
                "status": "fail",
                "message": _ffv_t(
                    "stage.stateApiUnavailable",
                    fallback="state API unavailable or invalid JSON",
                ),
                "details": {
                    "url": state_url,
                    "status_code": response.get("status_code"),
                    "error": response.get("error"),
                    "raw": _tail(response.get("raw")),
                },
            }

        time.sleep(max(0, int(poll_interval_seconds)))


def _recommendation_kind(text: object) -> str:
    upper = str(text or "").strip().upper()
    if upper.startswith("BLOCK"):
        return "block"
    if upper.startswith("ESCALATE"):
        return "escalate"
    if upper.startswith("OBSERVE"):
        return "observe"
    if upper.startswith("CONSECUTIVE_INCONCLUSIVE"):
        return "observe"
    return "ready"


def _monitor_default_window_seconds(report_type: object) -> int:
    normalized = str(report_type or "").strip().lower()
    if normalized == "monitor_30m":
        return DEFAULT_MONITOR_30M_WINDOW_SECONDS
    if normalized == "monitor_12h":
        return DEFAULT_MONITOR_12H_WINDOW_SECONDS
    return 0


def _monitor_freshness(payload: Mapping[str, object] | None, *, now_ts: int) -> dict[str, int | bool]:
    report = dict(payload or {})
    generated_ts = _safe_int(report.get("generated_ts"))
    window_seconds = _monitor_default_window_seconds(report.get("report_type"))
    max_age_seconds = max(
        MONITOR_FRESHNESS_GRACE_SECONDS,
        window_seconds + max(MONITOR_FRESHNESS_GRACE_SECONDS, window_seconds // 10),
    ) if window_seconds > 0 else MONITOR_FRESHNESS_GRACE_SECONDS
    age_seconds = max_age_seconds + 1
    if generated_ts > 0:
        age_seconds = max(0, now_ts - generated_ts)
    return {
        "fresh": generated_ts > 0 and age_seconds <= max_age_seconds,
        "generated_ts": generated_ts,
        "age_seconds": age_seconds,
        "max_age_seconds": max_age_seconds,
    }


def _compute_operational_readiness(
    *,
    state_payload: Mapping[str, object] | None,
    monitor_30m_payload: Mapping[str, object] | None,
    monitor_12h_payload: Mapping[str, object] | None,
    eod_payload: Mapping[str, object] | None,
) -> dict[str, object]:
    state = dict(state_payload or {})
    startup = dict(state.get("startup") or {}) if isinstance(state.get("startup"), dict) else {}
    reconciliation = dict(state.get("reconciliation") or {}) if isinstance(state.get("reconciliation"), dict) else {}
    report30 = dict(monitor_30m_payload or {})
    report12 = dict(monitor_12h_payload or {})
    eod = dict(eod_payload or {})
    now_ts = max(
        _safe_int(state.get("ts")),
        _safe_int(eod.get("generated_ts")),
        int(time.time()),
    )
    report30_freshness = _monitor_freshness(report30, now_ts=now_ts)
    report12_freshness = _monitor_freshness(report12, now_ts=now_ts)
    report30_kind = _recommendation_kind(report30.get("final_recommendation")) if report30_freshness["fresh"] else "ready"
    report12_kind = _recommendation_kind(report12.get("final_recommendation")) if report12_freshness["fresh"] else "ready"

    level = "ready"
    if (
        startup.get("ready") is False
        or report30_kind == "block"
        or report12_kind == "block"
    ):
        level = "block"
    elif (
        str(reconciliation.get("status") or "").lower() == "fail"
        or str(eod.get("status") or "").lower() == "fail"
        or report30_kind == "escalate"
        or report12_kind == "escalate"
    ):
        level = "escalate"
    elif (
        str(reconciliation.get("status") or "").lower() == "warn"
        or str(eod.get("status") or "").lower() == "warn"
        or report30_kind == "observe"
        or report12_kind == "observe"
        or not report30_freshness["fresh"]
        or not report12_freshness["fresh"]
    ):
        level = "observe"

    issues: list[str] = []
    if startup.get("ready") is False:
        issues.append(f"startup_failures={_safe_int(startup.get('failure_count'))}")
    if not report30_freshness["fresh"]:
        issues.append(
            f"monitor_30m_stale={_safe_int(report30_freshness['age_seconds'])}s>max{_safe_int(report30_freshness['max_age_seconds'])}s"
        )
    if not report12_freshness["fresh"]:
        issues.append(
            f"monitor_12h_stale={_safe_int(report12_freshness['age_seconds'])}s>max{_safe_int(report12_freshness['max_age_seconds'])}s"
        )
    for issue in list(reconciliation.get("issues") or []):
        if str(issue).strip():
            issues.append(str(issue))
    return {
        "level": level,
        "startup_ready": startup.get("ready"),
        "reconciliation_status": str(reconciliation.get("status") or "unknown").lower(),
        "monitor_30m_recommendation": str(report30.get("final_recommendation") or ""),
        "monitor_12h_recommendation": str(report12.get("final_recommendation") or ""),
        "monitor_30m_fresh": bool(report30_freshness["fresh"]),
        "monitor_12h_fresh": bool(report12_freshness["fresh"]),
        "monitor_30m_age_seconds": _safe_int(report30_freshness["age_seconds"]),
        "monitor_12h_age_seconds": _safe_int(report12_freshness["age_seconds"]),
        "eod_status": str(eod.get("status") or "unknown").lower(),
        "issues": issues[:8],
    }


def _validate_state_payload(payload: Mapping[str, object], *, now_ts: int | None = None) -> tuple[bool, str, dict[str, object]]:
    config = dict(payload.get("config") or {}) if isinstance(payload.get("config"), dict) else {}
    summary = dict(payload.get("summary") or {}) if isinstance(payload.get("summary"), dict) else {}
    ts = _safe_int(payload.get("ts"))
    poll = _safe_int(config.get("poll_interval_seconds"))
    if ts <= 0:
        return False, _ffv_t("stage.stateTimestampMissing", fallback="state timestamp missing"), {}
    if poll <= 0:
        return False, _ffv_t("stage.statePollIntervalMissing", fallback="poll interval missing"), {}
    age = max(0, int(now_ts or time.time()) - ts)
    max_age = max(90, poll * 3)
    if age > max_age:
        return False, _ffv_t(
            "stage.stateStale",
            {"age": age, "maxAge": max_age},
            fallback=f"state stale: age={age}s max_age={max_age}s",
        ), {}
    details = {
        "timestamp": ts,
        "age_seconds": age,
        "poll_interval_seconds": poll,
        "execution_mode": str(config.get("execution_mode") or ("paper" if config.get("dry_run", True) else "live")).lower(),
        "broker_name": str(config.get("broker_name") or ""),
        "wallet_pool_size": _safe_int(config.get("wallet_pool_size")),
        "open_positions": _safe_int(summary.get("open_positions")),
        "max_open_positions": _safe_int(summary.get("max_open_positions")),
        "tracked_notional_usd": _safe_float(summary.get("tracked_notional_usd")),
    }
    return True, _ffv_t("stage.statePayloadFresh", fallback="state payload fresh"), details


def _summarize_monitor_payload(payload: Mapping[str, object]) -> dict[str, object]:
    counts = dict(payload.get("counts") or {}) if isinstance(payload.get("counts"), dict) else {}
    return {
        "report_type": str(payload.get("report_type") or ""),
        "sample_status": str(payload.get("sample_status") or ""),
        "final_recommendation": str(payload.get("final_recommendation") or ""),
        "exec_count": _safe_int(counts.get("exec")),
        "generated_ts": _safe_int(payload.get("generated_ts")),
    }


def _summarize_eod_payload(payload: Mapping[str, object]) -> dict[str, object]:
    ledger_summary = dict(payload.get("ledger_summary") or {}) if isinstance(payload.get("ledger_summary"), dict) else {}
    return {
        "report_status": str(payload.get("status") or ""),
        "day_key": str(payload.get("day_key") or ""),
        "fill_count": _safe_int(ledger_summary.get("fill_count")),
        "realized_pnl": _safe_float(ledger_summary.get("realized_pnl")),
        "generated_ts": _safe_int(payload.get("generated_ts")),
    }


def _summarize_replay_runtime(payload: Mapping[str, object]) -> dict[str, object]:
    events = dict(payload.get("events") or {}) if isinstance(payload.get("events"), dict) else {}
    replay = dict(payload.get("replay") or {}) if isinstance(payload.get("replay"), dict) else {}
    drift = dict(payload.get("drift") or {}) if isinstance(payload.get("drift"), dict) else {}
    return {
        "events_count": _safe_int(events.get("count")),
        "reconstructed_open_positions": _safe_int(replay.get("reconstructed_open_positions")),
        "positions_delta": _safe_int(drift.get("positions_delta")),
        "notional_delta_usd": _safe_float(drift.get("notional_delta_usd")),
    }


def _summarize_replay_calibration(payload: Mapping[str, object]) -> dict[str, object]:
    recommended = dict(payload.get("recommended") or {}) if isinstance(payload.get("recommended"), dict) else {}
    matrix = list(payload.get("matrix") or []) if isinstance(payload.get("matrix"), list) else []
    return {
        "sample_count": _safe_int(payload.get("sample_count")),
        "scenario_count": len(matrix),
        "recommended_scenario": str(recommended.get("scenario") or ""),
        "recommended_net_cashflow_proxy": _safe_float(recommended.get("net_cashflow_proxy")),
        "recommended_reject_rate": _safe_float(recommended.get("reject_rate")),
    }


def run_full_flow_validation(
    *,
    root_dir: str | Path,
    state_url: str = "http://127.0.0.1:8787/api/state",
    monitor_30m_url: str = "http://127.0.0.1:8787/api/monitor/30m",
    monitor_12h_url: str = "http://127.0.0.1:8787/api/monitor/12h",
    reconciliation_eod_url: str = "http://127.0.0.1:8787/api/reconciliation/eod",
    operator_url: str = "http://127.0.0.1:8787/api/operator",
    state_path: str = "/tmp/poly_runtime_data/state.json",
    ledger_path: str = "/tmp/poly_runtime_data/ledger.jsonl",
    runtime_state_path: str = "/tmp/poly_runtime_data/runtime_state.json",
    events_path: str = "/tmp/poly_runtime_data/events.ndjson",
    bot_log_path: str = "/tmp/poly_runtime_data/poly_bot.log",
    monitor_30m_json_path: str = "/tmp/poly_full_flow_validation/monitor_30m_report.json",
    monitor_12h_json_path: str = "/tmp/poly_full_flow_validation/monitor_12h_report.json",
    monitor_30m_state_path: str = "/tmp/poly_full_flow_validation/monitor_30m_inconclusive_state",
    monitor_12h_state_path: str = "/tmp/poly_full_flow_validation/monitor_12h_inconclusive_state",
    reconciliation_eod_json_path: str = "/tmp/poly_reconciliation_eod_report.json",
    reconciliation_eod_text_path: str = "/tmp/poly_reconciliation_eod_report.txt",
    bootstrap_stack: bool = False,
    monitor_window_seconds: int | None = None,
    monitor_30m_window_seconds: int | None = None,
    monitor_12h_window_seconds: int | None = None,
    timeout_seconds: int = 180,
    http_client: Callable[..., dict[str, object]] = http_json,
    command_runner: Callable[..., dict[str, object]] = run_command,
) -> dict[str, object]:
    root = Path(root_dir).resolve()
    scripts_dir = root / "scripts"
    generated_ts = int(time.time())
    stages: list[dict[str, object]] = []
    state_payload: dict[str, Any] = {}
    monitor_30m_payload: dict[str, Any] = {}
    monitor_12h_payload: dict[str, Any] = {}
    reconciliation_payload: dict[str, Any] = {}
    stage_lookup: dict[str, dict[str, object]] = {}
    effective_monitor_30m_window = int(
        monitor_30m_window_seconds
        if monitor_30m_window_seconds is not None
        else monitor_window_seconds
        if monitor_window_seconds is not None
        else 0
    )
    effective_monitor_12h_window = int(
        monitor_12h_window_seconds
        if monitor_12h_window_seconds is not None
        else monitor_window_seconds
        if monitor_window_seconds is not None
        else 0
    )

    def add_stage(stage: dict[str, object]) -> None:
        stages.append(stage)
        stage_lookup[str(stage.get("name") or "")] = stage

    if bootstrap_stack:
        result = command_runner(
            ["/bin/bash", str(scripts_dir / "start_poly_stack.sh")],
            cwd=root,
            env={
                "START_STACK_DISABLE_LAUNCHCTL": "1",
                "START_STACK_VERIFY_RETRIES": str(max(24, int(max(timeout_seconds, 90) // 3))),
                "START_STACK_VERIFY_INTERVAL_SECONDS": "3",
            },
            timeout=max(180, timeout_seconds),
        )
        if result.get("ok"):
            add_stage(
                _stage(
                    "stack_bootstrap",
                    "pass",
                    _ffv_t("stage.stackBootstrapPass", fallback="stack restarted and verified"),
                    stdout=_tail(result.get("stdout")),
                )
            )
        else:
            add_stage(
                _stage(
                    "stack_bootstrap",
                    "warn",
                    _ffv_t(
                        "stage.stackBootstrapWarn",
                        fallback="stack bootstrap helper returned non-zero; continuing with direct validation",
                    ),
                    returncode=result.get("returncode"),
                    stdout=_tail(result.get("stdout")),
                    stderr=_tail(result.get("stderr")),
                    error=result.get("error"),
                )
            )

    state_payload, state_stage = _fetch_fresh_state_payload(
        state_url,
        http_client=http_client,
        timeout_seconds=max(30, timeout_seconds),
    )
    add_stage(
        _stage(
            "state",
            str(state_stage.get("status") or "fail"),
            str(state_stage.get("message") or _ffv_t("stage.stateValidationFailed", fallback="state validation failed")),
            **dict(state_stage.get("details") or {}),
        )
    )
    if not state_payload:
        state_payload = _load_json_dict(state_path)

    for name, script_name, json_path, api_url, state_cache_path, window_seconds in (
        ("monitor_30m", "monitor_thresholds_30m.sh", monitor_30m_json_path, monitor_30m_url, monitor_30m_state_path, effective_monitor_30m_window),
        ("monitor_12h", "monitor_thresholds_12h.sh", monitor_12h_json_path, monitor_12h_url, monitor_12h_state_path, effective_monitor_12h_window),
    ):
        json_path_obj = Path(str(json_path)).expanduser()
        txt_path = json_path_obj.with_suffix(".txt")
        state_cache_obj = Path(str(state_cache_path)).expanduser()
        json_path_obj.parent.mkdir(parents=True, exist_ok=True)
        txt_path.parent.mkdir(parents=True, exist_ok=True)
        state_cache_obj.parent.mkdir(parents=True, exist_ok=True)
        result = command_runner(
            [
                "/bin/bash",
                str(scripts_dir / script_name),
                str(txt_path),
                str(bot_log_path),
                str(int(window_seconds)),
                str(state_cache_obj),
                str(state_path),
                str(json_path_obj),
            ],
            cwd=root,
            timeout=max(60, timeout_seconds),
        )
        payload = _load_json_dict(json_path_obj)
        if result.get("ok") and payload:
            summary = _summarize_monitor_payload(payload)
            add_stage(
                _stage(
                    f"{name}_generation",
                    "pass",
                    _ffv_t("stage.monitorReportGenerated", {"name": name}, fallback=f"{name} report generated"),
                    json_path=str(json_path_obj),
                    text_path=str(txt_path),
                    state_path=str(state_cache_obj),
                    window_seconds=int(window_seconds),
                    **summary,
                )
            )
        else:
            add_stage(
                _stage(
                    f"{name}_generation",
                    "fail",
                    _ffv_t(
                        "stage.monitorReportGenerationFailed",
                        {"name": name},
                        fallback=f"{name} report generation failed",
                    ),
                    json_path=str(json_path_obj),
                    text_path=str(txt_path),
                    state_path=str(state_cache_obj),
                    window_seconds=int(window_seconds),
                    returncode=result.get("returncode"),
                    stdout=_tail(result.get("stdout")),
                    stderr=_tail(result.get("stderr")),
                    error=result.get("error"),
                )
            )

        api_resp = http_client(api_url, timeout=min(timeout_seconds, 10))
        if api_resp.get("ok") and isinstance(api_resp.get("payload"), dict):
            payload = dict(api_resp["payload"])
            summary = _summarize_monitor_payload(payload)
            add_stage(
                _stage(
                    f"{name}_api",
                    "pass",
                    _ffv_t("stage.monitorApiReturned", {"name": name}, fallback=f"{name} API returned JSON report"),
                    url=api_url,
                    **summary,
                )
            )
            if name == "monitor_30m":
                monitor_30m_payload = payload
            else:
                monitor_12h_payload = payload
        else:
            add_stage(
                _stage(
                    f"{name}_api",
                    "fail",
                    _ffv_t(
                        "stage.monitorApiUnavailable",
                        {"name": name},
                        fallback=f"{name} API unavailable or invalid JSON",
                    ),
                    url=api_url,
                    status_code=api_resp.get("status_code"),
                    error=api_resp.get("error"),
                    raw=_tail(api_resp.get("raw")),
                )
            )

    operator_resp = http_client(
        operator_url,
        method="POST",
        payload={"command": "generate_reconciliation_report"},
        timeout=min(timeout_seconds, 15),
    )
    operator_payload = dict(operator_resp.get("payload") or {}) if isinstance(operator_resp.get("payload"), dict) else {}
    operator_error_code = str(operator_payload.get("error_code") or "").strip()
    operator_reason_code = str(operator_payload.get("reason_code") or "").strip()
    operator_write_disabled = (
        int(_safe_int(operator_resp.get("status_code"), 0)) == 503
        and operator_error_code == "writeApiDisabled"
        and operator_reason_code == "single_writer_conflict"
    )
    if operator_resp.get("ok") and isinstance(operator_resp.get("payload"), dict):
        payload = dict(operator_payload)
        add_stage(
            _stage(
                "reconciliation_generation",
                "pass",
                _ffv_t(
                    "stage.reconciliationGenerationPass",
                    fallback="operator API generated reconciliation report",
                ),
                url=operator_url,
                command=str(payload.get("command") or ""),
                json_path=str(payload.get("json_path") or reconciliation_eod_json_path),
                text_path=str(payload.get("text_path") or reconciliation_eod_text_path),
            )
        )
    elif operator_write_disabled:
        add_stage(
            _stage(
                "reconciliation_generation",
                "pass",
                _ffv_t(
                    "stage.reconciliationGenerationSkippedWriteDisabled",
                    fallback="operator API write disabled under single-writer lock; use reconciliation API snapshot",
                ),
                url=operator_url,
                status_code=operator_resp.get("status_code"),
                error=operator_resp.get("error"),
                error_code=operator_error_code,
                reason_code=operator_reason_code,
                skipped=True,
            )
        )
    else:
        add_stage(
            _stage(
                "reconciliation_generation",
                "fail",
                _ffv_t(
                    "stage.reconciliationGenerationFail",
                    fallback="operator API failed to generate reconciliation report",
                ),
                url=operator_url,
                status_code=operator_resp.get("status_code"),
                error=operator_resp.get("error"),
                raw=_tail(operator_resp.get("raw")),
            )
        )

    reconciliation_resp = http_client(reconciliation_eod_url, timeout=min(timeout_seconds, 10))
    if reconciliation_resp.get("ok") and isinstance(reconciliation_resp.get("payload"), dict):
        reconciliation_payload = dict(reconciliation_resp["payload"])
        summary = _summarize_eod_payload(reconciliation_payload)
        add_stage(
            _stage(
                "reconciliation_api",
                "pass",
                _ffv_t("stage.reconciliationApiPass", fallback="reconciliation API returned JSON report"),
                url=reconciliation_eod_url,
                **summary,
            )
        )
    else:
        add_stage(
            _stage(
                "reconciliation_api",
                "fail",
                _ffv_t(
                    "stage.reconciliationApiFail",
                    fallback="reconciliation API unavailable or invalid JSON",
                ),
                url=reconciliation_eod_url,
                status_code=reconciliation_resp.get("status_code"),
                error=reconciliation_resp.get("error"),
                raw=_tail(reconciliation_resp.get("raw")),
            )
        )

    replay_runtime_result = command_runner(
        [
            str(root / ".venv" / "bin" / "python"),
            str(scripts_dir / "replay_runtime.py"),
            "--runtime-state",
            str(runtime_state_path),
            "--events",
            str(events_path),
            "--json",
        ],
        cwd=root,
        env={"PYTHONPATH": str(root / "src")},
        timeout=max(60, timeout_seconds),
    )
    replay_runtime_payload: dict[str, Any] = {}
    try:
        replay_runtime_payload = json.loads(str(replay_runtime_result.get("stdout") or ""))
    except Exception:
        replay_runtime_payload = {}
    if replay_runtime_result.get("ok") and replay_runtime_payload:
        add_stage(
            _stage(
                "replay_runtime",
                "pass",
                _ffv_t("stage.replayRuntimePass", fallback="runtime replay completed"),
                **_summarize_replay_runtime(replay_runtime_payload),
            )
        )
    else:
        add_stage(
            _stage(
                "replay_runtime",
                "fail",
                _ffv_t("stage.replayRuntimeFail", fallback="runtime replay failed"),
                returncode=replay_runtime_result.get("returncode"),
                stdout=_tail(replay_runtime_result.get("stdout")),
                stderr=_tail(replay_runtime_result.get("stderr")),
                error=replay_runtime_result.get("error"),
            )
        )

    replay_calibration_result = command_runner(
        [
            str(root / ".venv" / "bin" / "python"),
            str(scripts_dir / "replay_calibration.py"),
            "--events",
            str(events_path),
            "--runtime-state",
            str(runtime_state_path),
            "--json",
        ],
        cwd=root,
        env={"PYTHONPATH": str(root / "src")},
        timeout=max(60, timeout_seconds),
    )
    replay_calibration_payload: dict[str, Any] = {}
    try:
        replay_calibration_payload = json.loads(str(replay_calibration_result.get("stdout") or ""))
    except Exception:
        replay_calibration_payload = {}
    if replay_calibration_result.get("ok") and replay_calibration_payload:
        add_stage(
            _stage(
                "replay_calibration",
                "pass",
                _ffv_t("stage.replayCalibrationPass", fallback="replay calibration completed"),
                **_summarize_replay_calibration(replay_calibration_payload),
            )
        )
    else:
        add_stage(
            _stage(
                "replay_calibration",
                "fail",
                _ffv_t("stage.replayCalibrationFail", fallback="replay calibration failed"),
                returncode=replay_calibration_result.get("returncode"),
                stdout=_tail(replay_calibration_result.get("stdout")),
                stderr=_tail(replay_calibration_result.get("stderr")),
                error=replay_calibration_result.get("error"),
            )
        )

    required_stage_names = {
        "state",
        "monitor_30m_generation",
        "monitor_30m_api",
        "monitor_12h_generation",
        "monitor_12h_api",
        "reconciliation_generation",
        "reconciliation_api",
        "replay_runtime",
        "replay_calibration",
    }
    validation_status = "pass" if all(
        str(stage.get("status")) == "pass"
        for stage in stages
        if str(stage.get("name") or "") in required_stage_names
    ) else "fail"
    readiness = _compute_operational_readiness(
        state_payload=state_payload,
        monitor_30m_payload=monitor_30m_payload,
        monitor_12h_payload=monitor_12h_payload,
        eod_payload=reconciliation_payload,
    )
    recommendations: list[str] = []
    if validation_status != "pass":
        failed_names = [str(stage.get("name")) for stage in stages if str(stage.get("status")) != "pass"]
        recommendations.append(
            _ffv_t(
                "recommendation.fixFailedStages",
                {"stages": ", ".join(failed_names)},
                fallback=f"Fix failed validation stages: {', '.join(failed_names)}.",
            )
        )
    else:
        recommendations.append(
            _ffv_t(
                "recommendation.pass",
                fallback="Full flow validation passed. APIs, reports, and replay tooling are wired end-to-end.",
            )
        )
    if readiness.get("level") != "ready":
        recommendations.append(
            _ffv_t(
                "recommendation.readinessReview",
                {"level": _ffv_readiness_label(readiness.get("level"))},
                fallback=(
                    f"Operational readiness is {str(readiness.get('level')).upper()}. "
                    "Review startup/reconciliation/monitor recommendations before treating results as promotion-ready."
                ),
            )
        )

    return {
        "report_version": 1,
        "generated_ts": generated_ts,
        "generated_at": _utc_iso(generated_ts),
        "validation_status": validation_status,
        "flow_standard_met": validation_status == "pass",
        "operational_readiness": readiness,
        "artifacts": {
            "state_path": state_path,
            "ledger_path": ledger_path,
            "runtime_state_path": runtime_state_path,
            "events_path": events_path,
            "monitor_30m_json_path": monitor_30m_json_path,
            "monitor_12h_json_path": monitor_12h_json_path,
            "monitor_30m_state_path": monitor_30m_state_path,
            "monitor_12h_state_path": monitor_12h_state_path,
            "reconciliation_eod_json_path": reconciliation_eod_json_path,
            "reconciliation_eod_text_path": reconciliation_eod_text_path,
        },
        "state_summary": dict(stage_lookup.get("state", {}).get("details") or {}),
        "stages": stages,
        "recommendations": recommendations,
    }


def render_full_flow_validation_report(report: Mapping[str, object]) -> str:
    readiness = dict(report.get("operational_readiness") or {}) if isinstance(report.get("operational_readiness"), dict) else {}
    recommendations = [str(item) for item in list(report.get("recommendations") or []) if str(item).strip()]
    stages = [item for item in list(report.get("stages") or []) if isinstance(item, dict)]

    lines = [
        _ffv_t("title", fallback="Polymarket Full Flow Validation Report"),
        f"{_ffv_t('field.generatedAt', fallback='generated_at')}: {report.get('generated_at')}",
        f"{_ffv_t('field.validationStatus', fallback='validation_status')}: {_ffv_status_label(report.get('validation_status'))}",
        f"{_ffv_t('field.flowStandardMet', fallback='flow_standard_met')}: {bool(report.get('flow_standard_met'))}",
        f"{_ffv_t('field.operationalReadiness', fallback='operational_readiness')}: {_ffv_readiness_label(readiness.get('level'))}",
        "",
        f"{_ffv_t('section.stages', fallback='stages')}:",
    ]

    for stage in stages:
        name = str(stage.get("name") or "-")
        status = _ffv_status_label(stage.get("status"))
        message = str(stage.get("message") or "")
        lines.append(
            _ffv_t(
                "row.stage",
                {"status": status, "name": name, "message": message},
                fallback=f"  [{status}] {name}: {message}",
            )
        )
        details = dict(stage.get("details") or {}) if isinstance(stage.get("details"), dict) else {}
        for key in ("url", "execution_mode", "broker_name", "final_recommendation", "status", "recommended_scenario", "json_path"):
            if key in details and str(details.get(key) or "").strip():
                lines.append(
                    _ffv_t(
                        "row.detail",
                        {"key": key, "value": details.get(key)},
                        fallback=f"    - {key}: {details.get(key)}",
                    )
                )

    lines.extend(
        [
            "",
            f"{_ffv_t('section.operationalReadinessDetail', fallback='operational_readiness_detail')}:",
            f"  {_ffv_t('field.startupReady', fallback='startup_ready')}: {readiness.get('startup_ready')}",
            f"  {_ffv_t('field.reconciliationStatus', fallback='reconciliation_status')}: {readiness.get('reconciliation_status')}",
            f"  {_ffv_t('field.monitor30Recommendation', fallback='monitor_30m_recommendation')}: {readiness.get('monitor_30m_recommendation')}",
            f"  {_ffv_t('field.monitor12Recommendation', fallback='monitor_12h_recommendation')}: {readiness.get('monitor_12h_recommendation')}",
            f"  {_ffv_t('field.eodStatus', fallback='eod_status')}: {readiness.get('eod_status')}",
        ]
    )

    issues = [str(item) for item in list(readiness.get("issues") or []) if str(item).strip()]
    if issues:
        lines.append(f"  {_ffv_t('section.issues', fallback='issues')}:")
        for item in issues:
            lines.append(f"    - {item}")

    lines.append("")
    lines.append(f"{_ffv_t('section.recommendations', fallback='recommendations')}:")
    for item in recommendations:
        lines.append(f"  - {item}")
    return "\n".join(lines) + "\n"


def write_full_flow_validation_report(
    report: Mapping[str, object],
    *,
    text_path: str | Path,
    json_path: str | Path,
) -> None:
    _write_json(json_path, report)
    text_target = Path(text_path).expanduser()
    text_target.parent.mkdir(parents=True, exist_ok=True)
    with text_target.open("w", encoding="utf-8") as f:
        f.write(render_full_flow_validation_report(report))
