#!/usr/bin/env python3
"""BLOCK-005 behavior gate: broker terminal confirmation and latch semantics."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _run(test_pattern: str) -> int:
    env = os.environ.copy()
    env["PYTHONPATH"] = "src:tests"
    cmd = [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", test_pattern]
    print(f"[verify_kill_switch_terminal] RUN {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(ROOT), env=env, check=False)
    return int(result.returncode)


def main() -> int:
    checks = [
        "test_cancel_requested_is_not_safe.py",
        "test_kill_switch_requires_broker_terminal.py",
        "test_emergency_stop_latched_until_broker_safe.py",
        "test_restart_preserves_kill_switch_inflight_state.py",
    ]
    for pattern in checks:
        rc = _run(pattern)
        if rc != 0:
            print(f"[verify_kill_switch_terminal] FAIL pattern={pattern}")
            return rc
    print("[verify_kill_switch_terminal] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
