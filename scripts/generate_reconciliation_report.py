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

from polymarket_bot.reconciliation_report import build_reconciliation_report_from_paths, write_report_files


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate Polymarket reconciliation EOD report")
    parser.add_argument("--state-path", default="/tmp/poly_runtime_data/state.json")
    parser.add_argument("--ledger-path", default=os.getenv("LEDGER_PATH", "/tmp/poly_runtime_data/ledger.jsonl"))
    parser.add_argument("--day-key", default="", help="UTC day key (YYYY-MM-DD). Defaults to state reconciliation day or today.")
    parser.add_argument("--out", default="/tmp/poly_reconciliation_eod_report.txt")
    parser.add_argument("--json-out", default="/tmp/poly_reconciliation_eod_report.json")
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
