#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.i18n import t as i18n_t

DEFAULT_RUNTIME_ROOT = "/tmp/poly_runtime_data"


def _brief_t(key: str, params: dict[str, object] | None = None, *, fallback: str = "") -> str:
    return i18n_t(f"report.readinessBrief.{key}", params or {}, fallback=fallback)


def _gate_status_label(status: object) -> str:
    raw = str(status or "").strip().lower() or "unknown"
    return i18n_t(f"report.releaseGate.enum.status.{raw}", fallback=raw.upper())


def _read_dotenv_var(key: str) -> str:
    dotenv = ROOT / ".env"
    try:
        lines = dotenv.read_text(encoding="utf-8").splitlines()
    except Exception:
        return ""
    prefix = f"{key}="
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or not line.startswith(prefix):
            continue
        value = line[len(prefix) :].strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        return value
    return ""


def _sanitize_runtime_identity(value: str) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return "default"
    sanitized = re.sub(r"[^a-z0-9_-]+", "-", text).strip("-")
    return sanitized or "default"


def _default_paths() -> dict[str, str]:
    runtime_root = str(os.getenv("RUNTIME_ROOT_PATH", "")).strip() or _read_dotenv_var("RUNTIME_ROOT_PATH") or DEFAULT_RUNTIME_ROOT
    funder = str(os.getenv("FUNDER_ADDRESS", "")).strip() or _read_dotenv_var("FUNDER_ADDRESS")
    live_identity = _sanitize_runtime_identity(funder or "default")
    live_runtime_dir = str(Path(runtime_root).expanduser() / "live" / live_identity)
    paper_runtime_dir = str(Path(runtime_root).expanduser() / "paper" / "default")
    return {
        "release_gate_path": str(Path(live_runtime_dir) / "release_gate_report.json"),
        "rehearsal_path": str(Path(paper_runtime_dir) / "24h_dry_run_rehearsal.txt"),
        "output_path": str(Path(live_runtime_dir) / "readiness_brief.json"),
    }


def _parse_rehearsal(path: Path) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "exists": path.exists(),
        "start_ts": 0,
        "end_ts": 0,
        "remaining_seconds": None,
        "checkpoint_count": 0,
        "last_checkpoint": "",
        "last_status": "",
        "done": False,
        "expected_checkpoints": 24,
    }
    if not path.exists():
        return summary

    start_dt: datetime | None = None
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except Exception:
        return summary
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("start=") and stripped.endswith(" UTC"):
            raw = stripped[len("start=") : -len(" UTC")]
            try:
                start_dt = datetime.strptime(raw, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
            except ValueError:
                start_dt = None
        elif stripped.startswith("window_hours="):
            try:
                hours = int(stripped.split()[0].split("=", 1)[1])
                summary["expected_checkpoints"] = hours
            except Exception:
                pass
        elif re.match(r"^checkpoint\d+\b", stripped):
            summary["checkpoint_count"] += 1
            summary["last_checkpoint"] = stripped
            summary["last_status"] = "pass" if stripped.endswith(" pass") else "fail"
        elif stripped.startswith("rehearsal_done="):
            summary["done"] = True
    if start_dt is not None:
        end_dt = start_dt + timedelta(hours=int(summary["expected_checkpoints"]))
        now = datetime.now(timezone.utc)
        summary["start_ts"] = int(start_dt.timestamp())
        summary["end_ts"] = int(end_dt.timestamp())
        summary["remaining_seconds"] = max(0, int((end_dt - now).total_seconds()))
    return summary


def build_brief(*, release_gate_path: str = "", rehearsal_path: str = "", output_path: str = "") -> dict[str, Any]:
    defaults = _default_paths()
    release_gate = Path(str(release_gate_path or defaults["release_gate_path"]).strip()).expanduser()
    rehearsal = Path(str(rehearsal_path or defaults["rehearsal_path"]).strip()).expanduser()
    out = Path(str(output_path or defaults["output_path"]).strip()).expanduser()

    gate_report: dict[str, Any] = {}
    if release_gate.exists():
        try:
            payload = json.loads(release_gate.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                gate_report = payload
        except Exception:
            gate_report = {}

    brief = {
        "generated_at": int(time.time()),
        "release_gate_status": str(gate_report.get("status") or "unknown"),
        "blockers": list(gate_report.get("blockers") or []),
        "advisories": list(gate_report.get("advisories") or []),
        "rehearsal": _parse_rehearsal(rehearsal),
        "paths": {
            "release_gate_path": str(release_gate),
            "rehearsal_path": str(rehearsal),
            "output_path": str(out),
        },
    }
    return brief


def _render_text(brief: dict[str, Any]) -> str:
    rehearsal = dict(brief.get("rehearsal") or {})
    lines = [
        _brief_t("title", fallback="Polymarket Readiness Brief"),
        _brief_t(
            "field.releaseGate",
            {"status": _gate_status_label(brief.get("release_gate_status"))},
            fallback=f"release_gate: {str(brief.get('release_gate_status') or 'unknown').upper()}",
        ),
        _brief_t(
            "field.checkpointCount",
            {
                "count": rehearsal.get("checkpoint_count", 0),
                "expected": rehearsal.get("expected_checkpoints", 24),
            },
            fallback=f"checkpoint_count: {rehearsal.get('checkpoint_count', 0)}/{rehearsal.get('expected_checkpoints', 24)}",
        ),
        _brief_t(
            "field.lastCheckpoint",
            {"value": rehearsal.get("last_checkpoint") or _brief_t("na", fallback="n/a")},
            fallback=f"last_checkpoint: {rehearsal.get('last_checkpoint') or 'n/a'}",
        ),
    ]
    if rehearsal.get("remaining_seconds") is not None:
        remaining = int(rehearsal["remaining_seconds"])
        hours, rem = divmod(remaining, 3600)
        minutes, seconds = divmod(rem, 60)
        lines.append(
            _brief_t(
                "field.rehearsalRemaining",
                {"hours": hours, "minutes": minutes, "seconds": seconds},
                fallback=f"rehearsal_remaining: {hours}h {minutes}m {seconds}s",
            )
        )
    blockers = list(brief.get("blockers") or [])
    advisories = list(brief.get("advisories") or [])
    if blockers:
        lines.append(f"{_brief_t('section.blockers', fallback='blockers')}:")
        lines.extend(f"- {item}" for item in blockers)
    if advisories:
        lines.append(f"{_brief_t('section.advisories', fallback='advisories')}:")
        lines.extend(f"- {item}" for item in advisories)
    return "\n".join(lines)


def main() -> int:
    parser = argparse.ArgumentParser(description="Render a concise readiness brief from existing artifacts.")
    parser.add_argument("--release-gate-path", default="", help="Optional path to release_gate_report.json.")
    parser.add_argument("--rehearsal-path", default="", help="Optional path to 24h rehearsal output.")
    parser.add_argument("--output-path", default="", help="Optional output path for the JSON brief.")
    parser.add_argument("--json", action="store_true", help="Emit JSON instead of text.")
    args = parser.parse_args()

    brief = build_brief(
        release_gate_path=args.release_gate_path,
        rehearsal_path=args.rehearsal_path,
        output_path=args.output_path,
    )
    out = Path(brief["paths"]["output_path"]).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(brief, ensure_ascii=False, indent=2), encoding="utf-8")
    if args.json:
        print(json.dumps(brief, ensure_ascii=False, indent=2))
    else:
        print(_render_text(brief))
    print(out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
