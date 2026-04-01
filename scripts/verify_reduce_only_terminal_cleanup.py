#!/usr/bin/env python3
"""BLOCK-005 behavior gate: reduce-only pending BUY cleanup must wait broker terminal."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def main() -> int:
    env = os.environ.copy()
    env["PYTHONPATH"] = "src:tests"
    cmd = [
        sys.executable,
        "-m",
        "unittest",
        "discover",
        "-s",
        "tests",
        "-p",
        "test_reduce_only_cancels_pending_buy_and_waits_terminal.py",
    ]
    print(f"[verify_reduce_only_terminal_cleanup] RUN {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(ROOT), env=env, check=False)
    if result.returncode != 0:
        print("[verify_reduce_only_terminal_cleanup] FAIL")
        return int(result.returncode)
    print("[verify_reduce_only_terminal_cleanup] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
