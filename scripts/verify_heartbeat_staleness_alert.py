#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

ROOT_DIR = Path(__file__).resolve().parents[1]
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from polymarket_bot.heartbeat import default_runner_heartbeat
from polymarket_bot.metrics import build_observability_snapshot
from polymarket_bot.runner import Trader


def main() -> None:
    trader = Trader.__new__(Trader)
    trader.settings = SimpleNamespace(enable_single_writer=False)
    trader._runner_heartbeat = default_runner_heartbeat(now_ts=0)
    Trader._update_runner_heartbeat(trader, now_ts=200, cycle_started=True)
    if int(trader._runner_heartbeat.get("last_seen_ts") or 0) != 200:
        raise AssertionError("active runner heartbeat update failed")

    trader.settings = SimpleNamespace(enable_single_writer=True)
    trader._writer_lock = None
    before = dict(trader._runner_heartbeat)
    Trader._update_runner_heartbeat(trader, now_ts=400, cycle_started=True)
    if before != trader._runner_heartbeat:
        raise AssertionError("inactive runner should not update heartbeat")

    stale_snapshot = build_observability_snapshot(
        state_payload={
            "admission": {
                "opening_allowed": True,
                "mode": "NORMAL",
                "reason_codes": [],
                "evidence_summary": {
                    "reconciliation_status": "ok",
                    "account_snapshot_age_seconds": 0,
                    "account_snapshot_stale_threshold_seconds": 300,
                    "broker_event_sync_age_seconds": 0,
                    "broker_event_stale_threshold_seconds": 300,
                    "ledger_diff": 0.0,
                    "ledger_diff_threshold_usd": 1.0,
                },
            },
            "runner_heartbeat": {
                "last_seen_ts": 10,
                "last_cycle_started_ts": 10,
                "last_cycle_finished_ts": 10,
                "cycle_seq": 1,
                "loop_status": "running",
                "writer_active": True,
            },
        },
        now_ts=1000,
        heartbeat_stale_after_seconds=60,
        buy_blocked_alert_after_seconds=300,
    )
    active_codes = {str(item.get("alert_code")) for item in stale_snapshot.get("active_alerts", [])}
    if "runner_heartbeat_stale" not in active_codes:
        raise AssertionError("runner_heartbeat_stale alert missing")

    print("verify_heartbeat_staleness_alert: ok")


if __name__ == "__main__":
    main()
