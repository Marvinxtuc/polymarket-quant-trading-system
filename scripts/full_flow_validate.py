#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.config import Settings, build_runtime_artifact_paths
from polymarket_bot.full_flow_validation import (
    render_full_flow_validation_report,
    run_full_flow_validation,
    write_full_flow_validation_report,
)

RUNTIME_PATHS = build_runtime_artifact_paths(Settings())
VALIDATION_TMP = Path(RUNTIME_PATHS["full_flow_validation_dir"])


def _read_dotenv_var(key: str) -> str:
    dotenv = ROOT / ".env"
    try:
        lines = dotenv.read_text(encoding="utf-8").splitlines()
    except Exception:
        return ""
    prefix = f"{key}="
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or not line.startswith(prefix):
            continue
        value = line[len(prefix) :].strip()
        if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
            value = value[1:-1]
        return value
    return ""


def _append_control_token(url: str) -> str:
    token = os.getenv("POLY_CONTROL_TOKEN", "").strip() or _read_dotenv_var("POLY_CONTROL_TOKEN")
    if not token:
        return url
    parsed = urlsplit(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    if query.get("token"):
        return url
    query["token"] = token
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(query), parsed.fragment))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run end-to-end full flow validation for the local Polymarket stack")
    parser.add_argument("--state-url", default=_append_control_token("http://127.0.0.1:8787/api/state"))
    parser.add_argument("--monitor-30m-url", default=_append_control_token("http://127.0.0.1:8787/api/monitor/30m"))
    parser.add_argument("--monitor-12h-url", default=_append_control_token("http://127.0.0.1:8787/api/monitor/12h"))
    parser.add_argument("--reconciliation-eod-url", default=_append_control_token("http://127.0.0.1:8787/api/reconciliation/eod"))
    parser.add_argument("--operator-url", default=_append_control_token("http://127.0.0.1:8787/api/operator"))
    parser.add_argument("--state-path", default=RUNTIME_PATHS["state_path"])
    parser.add_argument("--ledger-path", default=RUNTIME_PATHS["ledger_path"])
    parser.add_argument("--runtime-state-path", default=RUNTIME_PATHS["runtime_state_path"])
    parser.add_argument("--events-path", default=RUNTIME_PATHS["event_log_path"])
    parser.add_argument("--bot-log-path", default=RUNTIME_PATHS["bot_log_path"])
    parser.add_argument("--monitor-30m-json-path", default=RUNTIME_PATHS["full_flow_validation_monitor_30m_json_path"])
    parser.add_argument("--monitor-12h-json-path", default=RUNTIME_PATHS["full_flow_validation_monitor_12h_json_path"])
    parser.add_argument("--monitor-30m-state-path", default=RUNTIME_PATHS["full_flow_validation_monitor_30m_state_path"])
    parser.add_argument("--monitor-12h-state-path", default=RUNTIME_PATHS["full_flow_validation_monitor_12h_state_path"])
    parser.add_argument("--reconciliation-eod-json-path", default=RUNTIME_PATHS["reconciliation_eod_json_path"])
    parser.add_argument("--reconciliation-eod-text-path", default=RUNTIME_PATHS["reconciliation_eod_text_path"])
    parser.add_argument("--out", default=RUNTIME_PATHS["full_flow_validation_report_path"])
    parser.add_argument("--json-out", default=RUNTIME_PATHS["full_flow_validation_json_path"])
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
