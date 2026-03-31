#!/usr/bin/env python3
"""BLOCK-003 behavior gate: single-writer conflict and standby write rejection."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.config import Settings  # noqa: E402
from polymarket_bot.locks import derive_writer_scope  # noqa: E402


def _run(pattern: str) -> int:
    env = os.environ.copy()
    env["PYTHONPATH"] = "src:tests"
    cmd = [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", pattern]
    return int(subprocess.run(cmd, cwd=str(ROOT), env=env, check=False).returncode)


def main() -> int:
    settings = Settings(_env_file=None)
    writer_scope = derive_writer_scope(
        dry_run=bool(settings.dry_run),
        funder_address=str(settings.funder_address or ""),
        watch_wallets=str(settings.watch_wallets or ""),
    )
    print(f"writer_scope={writer_scope}")
    patterns = (
        "test_single_writer_lock.py",
        "test_second_executor_is_readonly_or_exit.py",
        "test_control_state_cannot_be_overwritten_by_standby.py",
    )
    for pattern in patterns:
        code = _run(pattern)
        if code != 0:
            return code
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
