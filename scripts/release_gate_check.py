#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.config import Settings, build_runtime_artifact_paths
from polymarket_bot.i18n import t as i18n_t


PLACEHOLDER_PATTERNS = (
    re.compile(r"待填"),
    re.compile(r"待定"),
)


def _gate_t(key: str, params: dict[str, object] | None = None, *, fallback: str = "") -> str:
    return i18n_t(f"report.releaseGate.{key}", params or {}, fallback=fallback)


def _gate_status_label(status: object) -> str:
    raw = str(status or "").strip().lower() or "unknown"
    return _gate_t(f"enum.status.{raw}", fallback=raw.upper())


def _load_json(path: Path) -> dict[str, Any] | None:
    try:
        payload = json.loads(path.expanduser().read_text(encoding="utf-8"))
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _build_check(name: str, status: str, message: str, details: dict[str, Any] | None = None) -> dict[str, Any]:
    return {
        "name": str(name),
        "status": str(status),
        "message": str(message),
        "details": dict(details or {}),
    }


def _latest_matching(pattern: str, *, exclude_substrings: tuple[str, ...] = ()) -> Path | None:
    pattern_text = str(pattern or "").strip()
    if not pattern_text:
        return None
    if pattern_text.startswith("/"):
        pattern_path = Path(pattern_text)
        parent = pattern_path.parent
        name_pattern = pattern_path.name
        if not parent.exists():
            return None
        iterator = parent.glob(name_pattern)
    else:
        iterator = ROOT.glob(pattern_text)
    candidates = [path for path in iterator if path.is_file() and not any(part and part in path.name for part in exclude_substrings)]
    if not candidates:
        return None
    candidates.sort(key=lambda item: (item.stat().st_mtime, item.name), reverse=True)
    return candidates[0]


def _scan_placeholders(path: Path) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return findings
    for idx, line in enumerate(lines, start=1):
        for pattern in PLACEHOLDER_PATTERNS:
            if pattern.search(line):
                findings.append(
                    {
                        "line": idx,
                        "marker": pattern.pattern,
                        "text": line.strip(),
                    }
                )
                break
    return findings


def _parse_rehearsal(path: Path) -> dict[str, Any]:
    summary = {
        "exists": path.exists(),
        "checkpoint_count": 0,
        "pass_count": 0,
        "fail_count": 0,
        "expected_checkpoints": 24,
        "done": False,
        "last_checkpoint": "",
    }
    if not path.exists():
        return summary
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return summary
    for line in lines:
        if line.startswith("window_hours="):
            match = re.search(r"window_hours=(\d+)", line)
            if match:
                summary["expected_checkpoints"] = int(match.group(1))
        elif re.match(r"^checkpoint\d+\b", line):
            summary["checkpoint_count"] += 1
            summary["last_checkpoint"] = line.strip()
            if line.rstrip().endswith(" pass"):
                summary["pass_count"] += 1
            else:
                summary["fail_count"] += 1
        elif line.startswith("rehearsal_done="):
            summary["done"] = True
    return summary


def build_report(
    *,
    live_settings: Settings | None = None,
    paper_settings: Settings | None = None,
    now_ts: int | None = None,
) -> dict[str, Any]:
    live_settings = live_settings or Settings(dry_run=False)
    paper_settings = paper_settings or Settings(dry_run=True)
    live_paths = build_runtime_artifact_paths(live_settings)
    paper_paths = build_runtime_artifact_paths(paper_settings)
    now = int(now_ts or time.time())

    checks: list[dict[str, Any]] = []
    blockers: list[str] = []
    advisories: list[str] = []

    validation_path = Path(live_paths["full_flow_validation_json_path"]).expanduser()
    validation = _load_json(validation_path)
    readiness_level = str(((validation or {}).get("operational_readiness") or {}).get("level") or "").lower()
    validation_ok = bool(validation and validation.get("flow_standard_met")) and str(validation.get("validation_status") or "").upper() == "PASS"
    validation_block = readiness_level in {"block", "escalate"}
    checks.append(
        _build_check(
            "full_flow_validation",
            "PASS" if validation_ok and not validation_block else "FAIL",
            _gate_t(
                "check.fullFlowValidation",
                fallback="full_flow_validation report must exist, PASS, and not be BLOCK/ESCALATE",
            ),
            {
                "path": str(validation_path),
                "exists": validation is not None,
                "validation_status": (validation or {}).get("validation_status"),
                "flow_standard_met": (validation or {}).get("flow_standard_met"),
                "operational_readiness": readiness_level or "unknown",
            },
        )
    )
    if not validation_ok:
        blockers.append(_gate_t("blocker.fullFlowValidationNotPass", fallback="full_flow_validation not PASS"))
    elif validation_block:
        blockers.append(
            _gate_t(
                "blocker.operationalReadinessBlocked",
                {"level": _gate_status_label(readiness_level)},
                fallback=f"operational_readiness is {readiness_level}",
            )
        )
    elif readiness_level == "observe":
        advisories.append(_gate_t("advisory.operationalReadinessObserve", fallback="operational_readiness remains observe"))

    rehearsal_path = Path(paper_paths["rehearsal_24h_dry_run_out_path"]).expanduser()
    rehearsal = _parse_rehearsal(rehearsal_path)
    rehearsal_ok = bool(rehearsal["exists"]) and bool(rehearsal["done"]) and int(rehearsal["fail_count"]) == 0 and int(rehearsal["checkpoint_count"]) >= int(rehearsal["expected_checkpoints"])
    checks.append(
        _build_check(
            "rehearsal_24h",
            "PASS" if rehearsal_ok else "FAIL",
            _gate_t(
                "check.rehearsal24h",
                fallback="24h dry-run rehearsal must complete all checkpoints without failure",
            ),
            {
                "path": str(rehearsal_path),
                **rehearsal,
            },
        )
    )
    if not rehearsal_ok:
        blockers.append(_gate_t("blocker.rehearsalNotClean", fallback="24h dry-run rehearsal not completed cleanly"))

    alert_path = Path(live_settings.runtime_store_path("alert_delivery_smoke.json")).expanduser()
    alert_report = _load_json(alert_path)
    alert_status = str((alert_report or {}).get("status") or "").lower()
    alert_ok = alert_status == "sent"
    checks.append(
        _build_check(
            "remote_alert_smoke",
            "PASS" if alert_ok else "FAIL",
            _gate_t(
                "check.remoteAlertSmoke",
                fallback="Remote alert smoke must be sent successfully to a real configured channel",
            ),
            {
                "path": str(alert_path),
                "exists": alert_report is not None,
                "status": alert_status or "missing",
                "blockers": list((alert_report or {}).get("blockers") or []),
            },
        )
    )
    if not alert_ok:
        blockers.append(_gate_t("blocker.remoteAlertSmokeFailed", fallback="remote alert smoke not sent successfully"))

    preflight_path = _latest_matching(
        f"{Path(live_paths['runtime_dir']).as_posix()}/live_smoke_preflight*.json",
        exclude_substrings=("local_webhook",),
    )
    if preflight_path is None:
        preflight_path = Path(live_settings.runtime_store_path("live_smoke_preflight.json")).expanduser()
    preflight_report = _load_json(preflight_path)
    preflight_status = str((preflight_report or {}).get("status") or "").lower()
    preflight_ok = preflight_status == "ready"
    checks.append(
        _build_check(
            "live_smoke_preflight",
            "PASS" if preflight_ok else "FAIL",
            _gate_t(
                "check.liveSmokePreflight",
                fallback="Live smoke preflight must report ready under the real configured environment",
            ),
            {
                "path": str(preflight_path),
                "exists": preflight_report is not None,
                "status": preflight_status or "missing",
                "blockers": list((preflight_report or {}).get("blockers") or []),
            },
        )
    )
    if not preflight_ok:
        blockers.append(_gate_t("blocker.liveSmokePreflightNotReady", fallback="live smoke preflight not ready"))

    live_smoke_path = Path(live_paths["live_smoke_summary_path"]).expanduser()
    live_smoke_report = _load_json(live_smoke_path)
    live_smoke_returncode = None
    if live_smoke_report is not None and live_smoke_report.get("returncode") is not None:
        try:
            live_smoke_returncode = int(live_smoke_report.get("returncode"))
        except (TypeError, ValueError):
            live_smoke_returncode = None
    live_smoke_ok = bool(live_smoke_report and live_smoke_report.get("ok") is True and live_smoke_returncode == 0)
    checks.append(
        _build_check(
            "live_smoke_execution",
            "PASS" if live_smoke_ok else "FAIL",
            _gate_t(
                "check.liveSmokeExecution",
                fallback="A real live smoke execution summary must exist and report success",
            ),
            {
                "path": str(live_smoke_path),
                "exists": live_smoke_report is not None,
                "status": (live_smoke_report or {}).get("status"),
                "returncode": live_smoke_returncode,
            },
        )
    )
    if not live_smoke_ok:
        blockers.append(_gate_t("blocker.liveSmokeExecutionFailed", fallback="live smoke execution summary missing or failed"))

    draft_paths = [
        _latest_matching("production_signoff_draft*.md"),
        _latest_matching("production_release_record_draft*.md"),
        _latest_matching("operations_handoff_draft*.md"),
    ]
    unresolved: list[dict[str, Any]] = []
    for path in draft_paths:
        if path is None:
            continue
        for finding in _scan_placeholders(path):
            unresolved.append(
                {
                    "path": str(path),
                    **finding,
                }
            )
    docs_ok = len(unresolved) == 0
    checks.append(
        _build_check(
            "operator_docs",
            "PASS" if docs_ok else "FAIL",
            _gate_t(
                "check.operatorDocs",
                fallback="Signoff/release/handoff drafts must not contain unresolved placeholders",
            ),
            {
                "drafts": [str(path) for path in draft_paths if path is not None],
                "unresolved_count": len(unresolved),
                "sample_unresolved": unresolved[:10],
            },
        )
    )
    if not docs_ok:
        blockers.append(
            _gate_t(
                "blocker.operatorDocsPlaceholders",
                fallback="operator signoff/release/handoff drafts still contain unresolved placeholders",
            )
        )

    status = "ready"
    if blockers:
        status = "blocked"
    elif advisories:
        status = "caution"

    return {
        "generated_at": now,
        "status": status,
        "blockers": blockers,
        "advisories": advisories,
        "checks": checks,
        "paths": {
            "live_runtime_dir": live_paths["runtime_dir"],
            "paper_runtime_dir": paper_paths["runtime_dir"],
            "release_gate_report_path": live_paths["release_gate_report_path"],
        },
    }


def _render_text(report: dict[str, Any]) -> str:
    lines = [
        _gate_t("title", fallback="Polymarket Release Gate"),
        _gate_t(
            "field.status",
            {"status": _gate_status_label(report.get("status"))},
            fallback=f"status: {str(report.get('status') or 'unknown').upper()}",
        ),
    ]
    blockers = list(report.get("blockers") or [])
    advisories = list(report.get("advisories") or [])
    if blockers:
        lines.append(f"{_gate_t('section.blockers', fallback='blockers')}:")
        lines.extend(f"- {item}" for item in blockers)
    if advisories:
        lines.append(f"{_gate_t('section.advisories', fallback='advisories')}:")
        lines.extend(f"- {item}" for item in advisories)
    lines.append(f"{_gate_t('section.checks', fallback='checks')}:")
    for check in list(report.get("checks") or []):
        lines.append(
            _gate_t(
                "row.check",
                {
                    "name": check["name"],
                    "status": _gate_status_label(check["status"]),
                    "message": check["message"],
                },
                fallback=f"- {check['name']}: {check['status']} | {check['message']}",
            )
        )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Evaluate the final Polymarket release gate from existing local artifacts.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    parser.add_argument("--report-path", default="", help="Optional JSON report output path.")
    args = parser.parse_args()

    live_settings = Settings(dry_run=False)
    report = build_report(live_settings=live_settings, paper_settings=Settings(dry_run=True))
    report_path = Path(str(args.report_path or live_settings.runtime_store_path("release_gate_report.json")).strip()).expanduser()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(_render_text(report))
    print(report_path)
    return 0 if str(report.get("status") or "").lower() == "ready" else 1


if __name__ == "__main__":
    raise SystemExit(main())
