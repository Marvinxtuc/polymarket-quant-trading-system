#!/usr/bin/env python3
"""BLOCK-006 behavior gate: write API source policy defaults to local-only."""

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
        "test_write_api_disabled_when_not_local_or_not_authorized.py",
    ]
    print(f"[verify_write_api_local_only] RUN {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=str(ROOT), env=env, check=False)
    if result.returncode != 0:
        print("[verify_write_api_local_only] FAIL")
        return int(result.returncode)
    print("[verify_write_api_local_only] PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
