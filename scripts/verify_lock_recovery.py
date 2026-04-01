#!/usr/bin/env python3
"""BLOCK-003 behavior gate: lock recovery after holder crash/exit."""

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


def main() -> int:
    settings = Settings(_env_file=None)
    writer_scope = derive_writer_scope(
        dry_run=bool(settings.dry_run),
        funder_address=str(settings.funder_address or ""),
        watch_wallets=str(settings.watch_wallets or ""),
    )
    print(f"writer_scope={writer_scope}")
    env = os.environ.copy()
    env["PYTHONPATH"] = "src:tests"
    cmd = [sys.executable, "-m", "unittest", "discover", "-s", "tests", "-p", "test_lock_recovery_after_crash.py"]
    result = subprocess.run(cmd, cwd=str(ROOT), env=env, check=False)
    return int(result.returncode)


if __name__ == "__main__":
    raise SystemExit(main())
