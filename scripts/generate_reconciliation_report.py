#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.config import Settings, build_runtime_artifact_paths
from polymarket_bot.reconciliation_report import build_reconciliation_report_from_paths, write_report_files


def main() -> int:
    runtime_paths = build_runtime_artifact_paths(Settings())
    parser = argparse.ArgumentParser(description="Generate Polymarket reconciliation EOD report")
    parser.add_argument("--state-path", default=runtime_paths["state_path"])
    parser.add_argument("--ledger-path", default=os.getenv("LEDGER_PATH", runtime_paths["ledger_path"]))
    parser.add_argument("--day-key", default="", help="UTC day key (YYYY-MM-DD). Defaults to state reconciliation day or today.")
    parser.add_argument("--out", default=runtime_paths["reconciliation_eod_text_path"])
    parser.add_argument("--json-out", default=runtime_paths["reconciliation_eod_json_path"])
    args = parser.parse_args()

    report = build_reconciliation_report_from_paths(
        state_path=str(args.state_path),
        ledger_path=str(args.ledger_path),
        day_key=str(args.day_key or ""),
    )
    write_report_files(report, text_path=str(args.out), json_path=str(args.json_out))
    print(args.out)
    print(args.json_out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
