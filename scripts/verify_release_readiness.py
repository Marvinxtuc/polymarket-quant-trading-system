#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable


RESULT_LINE_PREFIX = "GATE_BLOCK_RESULT "
BLOCK_ID_PATTERN = re.compile(r"^BLOCK-\d{3}$")


def load_release_blocks(config_path: Path) -> dict[str, list[str]]:
    payload = json.loads(config_path.read_text(encoding="utf-8"))
    required = [str(item).strip().upper() for item in list(payload.get("required_blocks") or [])]
    optional = [str(item).strip().upper() for item in list(payload.get("optional_blocks") or [])]
    if not required:
        raise ValueError("required_blocks_empty")
    all_blocks = required + optional
    if len(all_blocks) != len(set(all_blocks)):
        raise ValueError("duplicate_blocks_in_release_config")
    for block_id in all_blocks:
        if not BLOCK_ID_PATTERN.match(block_id):
            raise ValueError(f"invalid_block_id={block_id}")
    return {"required_blocks": required, "optional_blocks": optional}


def parse_machine_result_line(output: str) -> dict[str, object] | None:
    for line in output.splitlines():
        stripped = line.strip()
        if not stripped.startswith(RESULT_LINE_PREFIX):
            continue
        fields = stripped[len(RESULT_LINE_PREFIX) :].split()
        parsed: dict[str, object] = {}
        for field in fields:
            if "=" not in field:
                continue
            key, value = field.split("=", 1)
            key = key.strip()
            value = value.strip()
            if key in {"static", "tests", "behavior", "docs", "overall"}:
                try:
                    parsed[key] = int(value)
                except ValueError:
                    return None
            else:
                parsed[key] = value
        required_keys = {"block_id", "static", "tests", "behavior", "docs", "overall"}
        if required_keys.issubset(parsed.keys()):
            return parsed
        return None
    return None


def _validate_report_content(block_id: str, kind: str, text: str) -> list[str]:
    errors: list[str] = []
    normalized = text.strip()
    if not normalized:
        return [f"{kind}_empty"]
    if block_id.lower() not in normalized.lower():
        errors.append(f"{kind}_missing_block_id")
    if kind == "validation":
        if not re.search(r"(validation|验证)", normalized, re.IGNORECASE):
            errors.append("validation_missing_validation_marker")
        if not re.search(r"(pass|result|proof|command|cmd|命令|go|no-go)", normalized, re.IGNORECASE):
            errors.append("validation_missing_summary_marker")
    elif kind == "regression":
        if not re.search(r"(regression|回归|gate_tests|gate_block_item)", normalized, re.IGNORECASE):
            errors.append("regression_missing_regression_marker")
        if not re.search(r"(pass|result|command|cmd|gate_tests|gate_block_item|go|no-go)", normalized, re.IGNORECASE):
            errors.append("regression_missing_summary_marker")
    elif kind == "self_check":
        if not re.search(r"(self[\s_-]*check|自检)", normalized, re.IGNORECASE):
            errors.append("self_check_missing_self_check_marker")
        if not re.search(r"(anti|scope|completion|风险|check|完成)", normalized, re.IGNORECASE):
            errors.append("self_check_missing_summary_marker")
    else:
        errors.append("unsupported_report_kind")
    return errors


def validate_block_reports(root_dir: Path, block_id: str) -> tuple[bool, list[str]]:
    block_dir = root_dir / "reports" / "blocking" / block_id
    report_files = {
        "validation": block_dir / "validation.txt",
        "regression": block_dir / "regression.txt",
        "self_check": block_dir / "self_check.md",
    }
    errors: list[str] = []
    for kind, path in report_files.items():
        if not path.exists():
            errors.append(f"{kind}_missing")
            continue
        if path.stat().st_size <= 0:
            errors.append(f"{kind}_empty")
            continue
        text = path.read_text(encoding="utf-8")
        errors.extend(_validate_report_content(block_id, kind, text))
    return (len(errors) == 0, errors)


def default_gate_runner(root_dir: Path, block_id: str) -> tuple[int, str]:
    cmd = ["bash", str(root_dir / "scripts" / "gates" / "gate_block_item.sh"), block_id]
    proc = subprocess.run(cmd, cwd=root_dir, capture_output=True, text=True, check=False)
    merged_output = (proc.stdout or "") + ("\n" + proc.stderr if proc.stderr else "")
    return proc.returncode, merged_output


def _safe_git_value(root_dir: Path, args: list[str], fallback: str) -> str:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=root_dir,
            capture_output=True,
            text=True,
            check=False,
        )
        if proc.returncode != 0:
            return fallback
        value = str(proc.stdout or "").strip()
        return value or fallback
    except Exception:
        return fallback


def atomic_write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=str(path.parent), delete=False) as handle:
        handle.write(content)
        temp_path = Path(handle.name)
    os.replace(temp_path, path)


def atomic_write_json(path: Path, payload: dict[str, object]) -> None:
    atomic_write_text(path, json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n")


def render_markdown(summary: dict[str, object]) -> str:
    lines: list[str] = []
    lines.append("# Release Go/No-Go Summary")
    lines.append("")
    lines.append(f"- Decision: **{summary.get('go_no_go', 'NO-GO')}**")
    lines.append(f"- Timestamp (UTC): `{summary.get('execution_timestamp_utc', '')}`")
    lines.append(f"- Git Branch: `{summary.get('git_branch', 'unknown')}`")
    lines.append(f"- Git Commit: `{summary.get('git_commit', 'unknown')}`")
    lines.append(f"- Command: `{summary.get('release_gate_command', '')}`")
    lines.append("")
    lines.append("## Required Blocks")
    lines.append("")
    required_blocks = list(summary.get("required_blocks") or [])
    lines.append("- " + ", ".join(str(item) for item in required_blocks))
    lines.append("")
    lines.append("## Block Results")
    lines.append("")
    for item in list(summary.get("blocks") or []):
        block_id = str(item.get("block_id") or "")
        passed = bool(item.get("passed"))
        lines.append(f"- {block_id}: {'PASS' if passed else 'FAIL'}")
        reasons = list(item.get("failure_reasons") or [])
        if reasons:
            lines.append(f"  - reasons: {', '.join(str(reason) for reason in reasons)}")
    lines.append("")
    lines.append("## Final Verdict")
    lines.append("")
    lines.append(f"- {summary.get('go_no_go', 'NO-GO')}")
    return "\n".join(lines) + "\n"


def run_release_readiness(
    *,
    root_dir: Path,
    config_path: Path,
    json_out: Path,
    md_out: Path,
    release_gate_command: str,
    gate_runner: Callable[[Path, str], tuple[int, str]] | None = None,
) -> tuple[int, dict[str, object]]:
    block_config = load_release_blocks(config_path)
    required_blocks = list(block_config["required_blocks"])
    optional_blocks = list(block_config["optional_blocks"])
    runner = gate_runner or default_gate_runner

    block_rows: list[dict[str, object]] = []
    any_required_failed = False
    aggregate_reasons: list[str] = []

    for block_id in required_blocks:
        gate_exit_code, output = runner(root_dir, block_id)
        parsed = parse_machine_result_line(output)
        failure_reasons: list[str] = []
        gate_passed = False

        if parsed is None:
            failure_reasons.append("machine_result_missing_or_invalid")
        else:
            parsed_block_id = str(parsed.get("block_id") or "").strip().upper()
            if parsed_block_id != block_id:
                failure_reasons.append("machine_result_block_id_mismatch")
            parsed_overall = int(parsed.get("overall") if parsed.get("overall") is not None else 1)
            if gate_exit_code == 0 and parsed_overall == 0:
                gate_passed = True
            elif gate_exit_code != 0:
                failure_reasons.append("gate_exit_nonzero")
            else:
                failure_reasons.append("gate_overall_nonzero")

        reports_valid, report_errors = validate_block_reports(root_dir, block_id)
        if not reports_valid:
            failure_reasons.append("report_structure_invalid")
            failure_reasons.extend(report_errors)

        passed = gate_passed and reports_valid
        if not passed:
            any_required_failed = True
            if "required_block_failed" not in aggregate_reasons:
                aggregate_reasons.append("required_block_failed")

        block_rows.append(
            {
                "block_id": block_id,
                "required": True,
                "gate_exit_code": int(gate_exit_code),
                "machine_result": parsed or {},
                "reports_valid": bool(reports_valid),
                "report_errors": list(report_errors),
                "failure_reasons": list(dict.fromkeys(failure_reasons)),
                "passed": bool(passed),
            }
        )

    required_passed = sum(1 for row in block_rows if bool(row.get("passed")))
    required_failed = len(required_blocks) - required_passed
    decision = "GO" if not any_required_failed else "NO-GO"
    if decision == "NO-GO" and "required_block_failed" not in aggregate_reasons:
        aggregate_reasons.append("required_block_failed")

    summary: dict[str, object] = {
        "go_no_go": decision,
        "execution_timestamp_utc": datetime.now(tz=timezone.utc).isoformat(),
        "release_gate_command": release_gate_command,
        "git_branch": _safe_git_value(root_dir, ["rev-parse", "--abbrev-ref", "HEAD"], "unknown"),
        "git_commit": _safe_git_value(root_dir, ["rev-parse", "HEAD"], "unknown"),
        "required_blocks": required_blocks,
        "optional_blocks": optional_blocks,
        "required_total": len(required_blocks),
        "required_passed": required_passed,
        "required_failed": required_failed,
        "reason_codes": aggregate_reasons,
        "blocks": block_rows,
    }

    atomic_write_json(json_out, summary)
    atomic_write_text(md_out, render_markdown(summary))
    return (0 if decision == "GO" else 1), summary


def run_self_test_suite() -> int:
    with tempfile.TemporaryDirectory(prefix="release-gate-selftest-") as temp_dir:
        root = Path(temp_dir)
        (root / "reports" / "blocking").mkdir(parents=True, exist_ok=True)
        cfg = root / "release_blocks.json"
        cfg.write_text(
            json.dumps({"required_blocks": ["BLOCK-001", "BLOCK-002"], "optional_blocks": []}, indent=2),
            encoding="utf-8",
        )
        for block_id in ("BLOCK-001", "BLOCK-002"):
            block_dir = root / "reports" / "blocking" / block_id
            block_dir.mkdir(parents=True, exist_ok=True)
            (block_dir / "validation.txt").write_text(f"{block_id} validation PASS command", encoding="utf-8")
            (block_dir / "regression.txt").write_text(f"{block_id} regression PASS gate_tests", encoding="utf-8")
            (block_dir / "self_check.md").write_text(f"# {block_id} Self Check\nscope anti", encoding="utf-8")

        def pass_runner(_root: Path, block_id: str) -> tuple[int, str]:
            return (
                0,
                f"GATE_BLOCK_RESULT block_id={block_id} static=0 tests=0 behavior=0 docs=0 overall=0\n",
            )

        code_go, _ = run_release_readiness(
            root_dir=root,
            config_path=cfg,
            json_out=root / "go.json",
            md_out=root / "go.md",
            release_gate_command="self-test-go",
            gate_runner=pass_runner,
        )
        assert code_go == 0, "all-pass scenario must be GO"

        def fail_runner(_root: Path, block_id: str) -> tuple[int, str]:
            if block_id == "BLOCK-002":
                return (
                    1,
                    f"GATE_BLOCK_RESULT block_id={block_id} static=0 tests=1 behavior=0 docs=0 overall=1\n",
                )
            return (
                0,
                f"GATE_BLOCK_RESULT block_id={block_id} static=0 tests=0 behavior=0 docs=0 overall=0\n",
            )

        code_fail, _ = run_release_readiness(
            root_dir=root,
            config_path=cfg,
            json_out=root / "fail.json",
            md_out=root / "fail.md",
            release_gate_command="self-test-fail",
            gate_runner=fail_runner,
        )
        assert code_fail != 0, "any required failure must be NO-GO"

        # Missing report -> NO-GO
        (root / "reports" / "blocking" / "BLOCK-001" / "self_check.md").unlink()
        code_missing, _ = run_release_readiness(
            root_dir=root,
            config_path=cfg,
            json_out=root / "missing.json",
            md_out=root / "missing.md",
            release_gate_command="self-test-missing",
            gate_runner=pass_runner,
        )
        assert code_missing != 0, "missing report structure must be NO-GO"
        print("verify_release_readiness: self-test suite ok")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Aggregate block gates into final release GO/NO-GO.")
    parser.add_argument("--root", default=str(Path(__file__).resolve().parents[1]))
    parser.add_argument("--config", default="scripts/gates/release_blocks.json")
    parser.add_argument("--json-out", default="reports/release/go_no_go_summary.json")
    parser.add_argument("--md-out", default="reports/release/go_no_go_summary.md")
    parser.add_argument("--release-gate-command", default="bash scripts/gates/gate_release_readiness.sh")
    parser.add_argument("--self-test", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.self_test:
        return run_self_test_suite()

    root_dir = Path(args.root).resolve()
    config_path = Path(args.config)
    if not config_path.is_absolute():
        config_path = (root_dir / config_path).resolve()
    json_out = Path(args.json_out)
    if not json_out.is_absolute():
        json_out = (root_dir / json_out).resolve()
    md_out = Path(args.md_out)
    if not md_out.is_absolute():
        md_out = (root_dir / md_out).resolve()

    try:
        code, summary = run_release_readiness(
            root_dir=root_dir,
            config_path=config_path,
            json_out=json_out,
            md_out=md_out,
            release_gate_command=str(args.release_gate_command),
        )
    except Exception as exc:
        fallback_summary = {
            "go_no_go": "NO-GO",
            "execution_timestamp_utc": datetime.now(tz=timezone.utc).isoformat(),
            "release_gate_command": str(args.release_gate_command),
            "git_branch": "unknown",
            "git_commit": "unknown",
            "required_blocks": [],
            "optional_blocks": [],
            "required_total": 0,
            "required_passed": 0,
            "required_failed": 0,
            "reason_codes": ["release_gate_internal_error"],
            "blocks": [],
            "error": str(exc),
        }
        try:
            atomic_write_json(json_out, fallback_summary)
            atomic_write_text(md_out, render_markdown(fallback_summary))
        except Exception:
            pass
        print(f"[verify_release_readiness] NO-GO release_gate_internal_error: {exc}")
        return 2

    print(
        "[verify_release_readiness] DECISION="
        f"{summary.get('go_no_go')} required_passed={summary.get('required_passed')} required_total={summary.get('required_total')}"
    )
    return code


if __name__ == "__main__":
    raise SystemExit(main())
