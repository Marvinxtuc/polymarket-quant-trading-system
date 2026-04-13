#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import subprocess
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


def _finalize_t(key: str, params: dict[str, object] | None = None, *, fallback: str = "") -> str:
    return i18n_t(f"report.rehearsalFinalize.{key}", dict(params or {}), fallback=fallback)


def _default_paths() -> dict[str, str]:
    live_paths = build_runtime_artifact_paths(Settings(dry_run=False))
    paper_paths = build_runtime_artifact_paths(Settings(dry_run=True))
    return {
        "live_runtime_dir": str(live_paths["runtime_dir"]),
        "paper_runtime_dir": str(paper_paths["runtime_dir"]),
        "rehearsal_path": str(paper_paths["rehearsal_24h_dry_run_out_path"]),
        "output_path": str(Path(live_paths["runtime_dir"]) / "rehearsal_finalize_report.json"),
    }


def _parse_rehearsal(path: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {
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
        stripped = line.strip()
        if stripped.startswith("window_hours="):
            try:
                summary["expected_checkpoints"] = int(stripped.split()[0].split("=", 1)[1])
            except Exception:
                pass
        elif re.match(r"^checkpoint\d+\b", stripped):
            summary["checkpoint_count"] += 1
            summary["last_checkpoint"] = stripped
            if stripped.endswith(" pass"):
                summary["pass_count"] += 1
            else:
                summary["fail_count"] += 1
        elif stripped.startswith("rehearsal_done="):
            summary["done"] = True
    return summary


def _run_command(command: list[str], *, extra_env: dict[str, str] | None = None, timeout: int = 180) -> dict[str, Any]:
    env = dict(os.environ)
    env["PYTHONPATH"] = "src"
    if extra_env:
        env.update(extra_env)
    start = time.time()
    try:
        proc = subprocess.run(
            command,
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
            timeout=timeout,
        )
        return {
            "command": command,
            "returncode": proc.returncode,
            "ok": proc.returncode == 0,
            "duration_seconds": round(time.time() - start, 3),
            "stdout": proc.stdout[-4000:],
            "stderr": proc.stderr[-4000:],
        }
    except subprocess.TimeoutExpired as exc:
        return {
            "command": command,
            "returncode": None,
            "ok": False,
            "timeout": True,
            "duration_seconds": round(time.time() - start, 3),
            "stdout": (exc.stdout or "")[-4000:],
            "stderr": (exc.stderr or "")[-4000:],
        }


def build_report(*, rehearsal_path: str = "", output_path: str = "", run_checks: bool = False) -> dict[str, Any]:
    defaults = _default_paths()
    rehearsal_file = Path(str(rehearsal_path or defaults["rehearsal_path"]).strip()).expanduser()
    out = Path(str(output_path or defaults["output_path"]).strip()).expanduser()
    rehearsal = _parse_rehearsal(rehearsal_file)
    report: dict[str, Any] = {
        "generated_at": int(time.time()),
        "status": "pending",
        "status_label": _finalize_t("status.pending", fallback="pending"),
        "rehearsal": rehearsal,
        "paths": {
            "rehearsal_path": str(rehearsal_file),
            "output_path": str(out),
        },
        "next_actions": [
            "wait_for_rehearsal_completion",
            "configure_real_remote_alerts",
            "execute_real_live_smoke",
        ],
        "next_action_labels": [
            _finalize_t("nextAction.waitForRehearsalCompletion", fallback="wait for rehearsal completion"),
            _finalize_t("nextAction.configureRealRemoteAlerts", fallback="configure real remote alerts"),
            _finalize_t("nextAction.executeRealLiveSmoke", fallback="execute real live smoke"),
        ],
    }
    if not rehearsal["exists"]:
        report["status"] = "missing"
        report["status_label"] = _finalize_t("status.missing", fallback="missing")
        return report
    if not rehearsal["done"]:
        report["status"] = "pending"
        report["status_label"] = _finalize_t("status.pending", fallback="pending")
        return report
    if int(rehearsal["fail_count"]) > 0 or int(rehearsal["checkpoint_count"]) < int(rehearsal["expected_checkpoints"]):
        report["status"] = "failed"
        report["status_label"] = _finalize_t("status.failed", fallback="failed")
        return report

    report["status"] = "completed"
    report["status_label"] = _finalize_t("status.completed", fallback="completed")
    if run_checks:
        commands = [
            {"name": "readiness_brief", "cmd": ["make", "readiness-brief"], "env": None, "timeout": 180},
            {"name": "release_gate", "cmd": ["make", "release-gate"], "env": None, "timeout": 180},
            {"name": "fault_drill", "cmd": ["make", "fault-drill"], "env": None, "timeout": 180},
            {"name": "monitor_scheduler_smoke", "cmd": ["make", "monitor-scheduler-smoke"], "env": {"DRY_RUN": "true"}, "timeout": 180},
            {"name": "verify_paper", "cmd": ["./scripts/verify_stack.sh"], "env": {"DRY_RUN": "true"}, "timeout": 180},
        ]
        report["checks"] = []
        for item in commands:
            report["checks"].append(
                {
                    "name": item["name"],
                    **_run_command(item["cmd"], extra_env=item["env"], timeout=item["timeout"]),
                }
            )
    return report


def _render_text(report: dict[str, Any]) -> str:
    rehearsal = dict(report.get("rehearsal") or {})
    lines = [
        _finalize_t("title", fallback="Polymarket Rehearsal Finalize"),
        _finalize_t(
            "field.status",
            {"value": str(report.get("status_label") or report.get("status") or "unknown").upper()},
            fallback=f"status: {str(report.get('status') or 'unknown').upper()}",
        ),
        _finalize_t(
            "field.checkpointCount",
            {"count": rehearsal.get("checkpoint_count", 0), "expected": rehearsal.get("expected_checkpoints", 24)},
            fallback=f"checkpoint_count: {rehearsal.get('checkpoint_count', 0)}/{rehearsal.get('expected_checkpoints', 24)}",
        ),
        _finalize_t(
            "field.lastCheckpoint",
            {"value": rehearsal.get("last_checkpoint") or _finalize_t("common.na", fallback="n/a")},
            fallback=f"last_checkpoint: {rehearsal.get('last_checkpoint') or 'n/a'}",
        ),
    ]
    if report.get("checks"):
        lines.append(_finalize_t("section.checks", fallback="checks:"))
        for check in list(report["checks"]):
            lines.append(
                _finalize_t(
                    "row.check",
                    {
                        "name": check["name"],
                        "status": _finalize_t(
                            f"checkStatus.{ 'pass' if check.get('ok') else 'fail' }",
                            fallback="PASS" if check.get("ok") else "FAIL",
                        ),
                    },
                    fallback=f"- {check['name']}: {'PASS' if check.get('ok') else 'FAIL'}",
                )
            )
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=_finalize_t("cli.description", fallback="Finalize the 24h rehearsal once it completes.")
    )
    parser.add_argument(
        "--rehearsal-path",
        default="",
        help=_finalize_t("cli.rehearsalPath", fallback="Optional rehearsal file path."),
    )
    parser.add_argument(
        "--output-path",
        default="",
        help=_finalize_t("cli.outputPath", fallback="Optional JSON report output path."),
    )
    parser.add_argument(
        "--run-checks",
        action="store_true",
        help=_finalize_t(
            "cli.runChecks",
            fallback="If rehearsal is complete, also run the post-rehearsal checks.",
        ),
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help=_finalize_t("cli.json", fallback="Emit JSON instead of text."),
    )
    args = parser.parse_args()

    report = build_report(rehearsal_path=args.rehearsal_path, output_path=args.output_path, run_checks=bool(args.run_checks))
    out = Path(report["paths"]["output_path"]).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(_render_text(report))
    print(out)
    return 0 if str(report.get("status") or "").lower() == "completed" else 1


if __name__ == "__main__":
    raise SystemExit(main())
