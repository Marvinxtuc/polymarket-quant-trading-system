#!/usr/bin/env python3
"""BLOCK-002 behavior gate: verify idempotent claim/retry flows."""

from __future__ import annotations

import os
import subprocess
import sys


def _run(pattern: str) -> int:
    cmd = [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", pattern]
    env = os.environ.copy()
    env["PYTHONPATH"] = "src:tests"
    result = subprocess.run(cmd, check=False, env=env)
    return int(result.returncode)


def main() -> int:
    patterns = [
        "test_idempotent_order_submission.py",
        "test_duplicate_executor_same_signal.py",
        "test_timeout_retry_reuses_same_intent.py",
    ]
    for pattern in patterns:
        code = _run(pattern)
        if code != 0:
            return code
    return 0


if __name__ == "__main__":
    sys.exit(main())
