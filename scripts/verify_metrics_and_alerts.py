#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from polymarket_bot.alerts import ALERT_CODE_WHITELIST
from polymarket_bot.metrics import build_observability_snapshot, render_prometheus_metrics


def main() -> None:
    snapshot = build_observability_snapshot(
        state_payload={
            "admission": {
                "opening_allowed": False,
                "mode": "REDUCE_ONLY",
                "reason_codes": ["startup_checks_fail"],
                "evidence_summary": {
                    "reconciliation_status": "fail",
                    "account_snapshot_age_seconds": 900,
                    "account_snapshot_stale_threshold_seconds": 300,
                    "broker_event_sync_age_seconds": 700,
                    "broker_event_stale_threshold_seconds": 300,
                    "ledger_diff": 2.0,
                    "ledger_diff_threshold_usd": 1.0,
                },
            },
            "runner_heartbeat": {
                "last_seen_ts": 1,
                "last_cycle_started_ts": 1,
                "last_cycle_finished_ts": 1,
                "cycle_seq": 1,
                "loop_status": "running",
                "writer_active": True,
            },
            "kill_switch": {
                "opening_allowed": False,
                "manual_required": True,
                "broker_safe_confirmed": False,
            },
            "signer_security": {
                "signer_required": True,
                "signer_healthy": False,
                "raw_key_detected": True,
                "hot_wallet_cap_ok": False,
                "reason_codes": ["hot_wallet_cap_exceeded"],
            },
            "control_plane_security": {
                "write_api_available": False,
                "readonly_mode": True,
                "reason_codes": ["single_writer_conflict"],
            },
            "buy_blocked": {
                "active": True,
                "reason_code": "startup_not_ready",
                "since_ts": 100,
                "duration_seconds": 1200,
                "updated_ts": 1300,
            },
        },
        now_ts=2000,
        heartbeat_stale_after_seconds=120,
        buy_blocked_alert_after_seconds=300,
    )

    active_codes = {str(item.get("alert_code")) for item in snapshot.get("active_alerts", [])}
    required = {
        "runner_heartbeat_stale",
        "admission_fail_closed",
        "reconciliation_fail",
        "account_snapshot_stale",
        "event_stream_stale",
        "ledger_diff_exceeded",
        "kill_switch_manual_required",
        "signer_unhealthy",
        "writer_conflict_readonly",
        "hot_wallet_cap_exceeded",
        "buy_blocked_too_long",
    }
    missing = sorted(required.difference(active_codes))
    if missing:
        raise AssertionError(f"missing alert codes: {missing}")

    metrics_text = render_prometheus_metrics(snapshot)
    if "polymarket_buy_blocked_duration_seconds 1200.0" not in metrics_text:
        raise AssertionError("buy_blocked_duration metric mismatch")
    if 'polymarket_alert_active{alert_code="admission_fail_closed",severity="page"} 1.0' not in metrics_text:
        raise AssertionError("missing fixed alert label projection")
    alert_lines = [line for line in metrics_text.splitlines() if line.startswith("polymarket_alert_active{")]
    if len(alert_lines) != len(ALERT_CODE_WHITELIST):
        raise AssertionError("alert gauge series count mismatch with whitelist")
    for line in alert_lines:
        if "wallet=" in line or "order_id=" in line or "market_slug=" in line or "reason_code=" in line:
            raise AssertionError(f"high-cardinality label leak detected: {line}")

    print("verify_metrics_and_alerts: ok")


if __name__ == "__main__":
    main()
