#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
VALIDATION_TMP = Path("/tmp/poly_full_flow_validation")
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.full_flow_validation import (
    render_full_flow_validation_report,
    run_full_flow_validation,
    write_full_flow_validation_report,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run end-to-end full flow validation for the local Polymarket stack")
    parser.add_argument("--state-url", default="http://127.0.0.1:8787/api/state")
    parser.add_argument("--monitor-30m-url", default="http://127.0.0.1:8787/api/monitor/30m")
    parser.add_argument("--monitor-12h-url", default="http://127.0.0.1:8787/api/monitor/12h")
    parser.add_argument("--reconciliation-eod-url", default="http://127.0.0.1:8787/api/reconciliation/eod")
    parser.add_argument("--operator-url", default="http://127.0.0.1:8787/api/operator")
    parser.add_argument("--state-path", default="/tmp/poly_runtime_data/state.json")
    parser.add_argument("--ledger-path", default="/tmp/poly_runtime_data/ledger.jsonl")
    parser.add_argument("--runtime-state-path", default="/tmp/poly_runtime_data/runtime_state.json")
    parser.add_argument("--events-path", default="/tmp/poly_runtime_data/events.ndjson")
    parser.add_argument("--bot-log-path", default="/tmp/poly_runtime_data/poly_bot.log")
    parser.add_argument("--monitor-30m-json-path", default=str(VALIDATION_TMP / "monitor_30m_report.json"))
    parser.add_argument("--monitor-12h-json-path", default=str(VALIDATION_TMP / "monitor_12h_report.json"))
    parser.add_argument("--monitor-30m-state-path", default=str(VALIDATION_TMP / "monitor_30m_inconclusive_state"))
    parser.add_argument("--monitor-12h-state-path", default=str(VALIDATION_TMP / "monitor_12h_inconclusive_state"))
    parser.add_argument("--reconciliation-eod-json-path", default="/tmp/poly_reconciliation_eod_report.json")
    parser.add_argument("--reconciliation-eod-text-path", default="/tmp/poly_reconciliation_eod_report.txt")
    parser.add_argument("--out", default="/tmp/poly_full_flow_validation_report.txt")
    parser.add_argument("--json-out", default="/tmp/poly_full_flow_validation_report.json")
    parser.add_argument("--monitor-window-seconds", type=int, default=None, help="Legacy shared override for both monitor scripts; use 0 for structural validation")
    parser.add_argument("--monitor-30m-window-seconds", type=int, default=None, help="Override only the 30m monitor generation window; use 0 for structural validation")
    parser.add_argument("--monitor-12h-window-seconds", type=int, default=None, help="Override only the 12h monitor generation window; use 0 for structural validation")
    parser.add_argument("--timeout-seconds", type=int, default=180)
    parser.add_argument("--bootstrap-stack", action="store_true", help="Restart the local stack before validation")
    parser.add_argument("--json", action="store_true", help="Print JSON to stdout instead of text")
    args = parser.parse_args()

    report = run_full_flow_validation(
        root_dir=ROOT,
        state_url=args.state_url,
        monitor_30m_url=args.monitor_30m_url,
        monitor_12h_url=args.monitor_12h_url,
        reconciliation_eod_url=args.reconciliation_eod_url,
        operator_url=args.operator_url,
        state_path=args.state_path,
        ledger_path=args.ledger_path,
        runtime_state_path=args.runtime_state_path,
        events_path=args.events_path,
        bot_log_path=args.bot_log_path,
        monitor_30m_json_path=args.monitor_30m_json_path,
        monitor_12h_json_path=args.monitor_12h_json_path,
        monitor_30m_state_path=args.monitor_30m_state_path,
        monitor_12h_state_path=args.monitor_12h_state_path,
        reconciliation_eod_json_path=args.reconciliation_eod_json_path,
        reconciliation_eod_text_path=args.reconciliation_eod_text_path,
        bootstrap_stack=bool(args.bootstrap_stack),
        monitor_window_seconds=int(args.monitor_window_seconds) if args.monitor_window_seconds is not None else None,
        monitor_30m_window_seconds=int(args.monitor_30m_window_seconds) if args.monitor_30m_window_seconds is not None else None,
        monitor_12h_window_seconds=int(args.monitor_12h_window_seconds) if args.monitor_12h_window_seconds is not None else None,
        timeout_seconds=int(args.timeout_seconds),
    )
    write_full_flow_validation_report(report, text_path=args.out, json_path=args.json_out)

    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(render_full_flow_validation_report(report), end="")
    print(args.out)
    print(args.json_out)
    return 0 if bool(report.get("flow_standard_met")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
