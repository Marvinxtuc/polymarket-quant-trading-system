#!/usr/bin/env python3
"""BLOCK-002 behavior gate: restart + broker-ack-unknown no-duplicate checks."""

from __future__ import annotations

import os
import subprocess
import sys


def main() -> int:
    cmd = [
        sys.executable,
        "-m",
        "unittest",
        "discover",
        "-s",
        "tests",
        "-p",
        "test_restart_does_not_duplicate_orders.py",
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = "src:tests"
    result = subprocess.run(cmd, check=False, env=env)
    return int(result.returncode)


if __name__ == "__main__":
    sys.exit(main())
