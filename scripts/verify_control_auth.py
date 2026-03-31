#!/usr/bin/env python3
"""BLOCK-006 behavior gate: control write auth + readonly boundary."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run(pattern: str) -> int:
    env = os.environ.copy()
    env["PYTHONPATH"] = "src:tests"
    cmd = [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", pattern]
    print(f"[verify_control_auth] RUN {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(ROOT), env=env, check=False)
    return int(result.returncode)


def main() -> int:
    checks = [
        "test_control_api_requires_token.py",
        "test_empty_token_rejected_in_live_mode.py",
        "test_readonly_api_still_available.py",
        "test_control_audit_log_written.py",
    ]
    for pattern in checks:
        rc = _run(pattern)
        if rc != 0:
            print(f"[verify_control_auth] FAIL pattern={pattern}")
            return rc
    print("[verify_control_auth] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
