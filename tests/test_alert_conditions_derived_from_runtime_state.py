from __future__ import annotations

import unittest

from polymarket_bot.alerts import (
    ALERT_ACCOUNT_SNAPSHOT_STALE,
    ALERT_BUY_BLOCKED_TOO_LONG,
    ALERT_EVENT_STREAM_STALE,
    ALERT_HOT_WALLET_CAP_EXCEEDED,
    ALERT_KILL_SWITCH_MANUAL_REQUIRED,
    ALERT_RECONCILIATION_FAIL,
    ALERT_RUNNER_HEARTBEAT_STALE,
    ALERT_SIGNER_UNHEALTHY,
    ALERT_WRITER_CONFLICT_READONLY,
    ALERT_CODE_WHITELIST,
)
from polymarket_bot.metrics import build_observability_snapshot, render_prometheus_metrics


class AlertConditionsDerivedTests(unittest.TestCase):
    def test_derived_alerts_follow_fixed_whitelist_and_mapping(self):
        snapshot = build_observability_snapshot(
            state_payload={
                "admission": {
                    "opening_allowed": False,
                    "mode": "REDUCE_ONLY",
                    "reason_codes": ["startup_checks_fail"],
                    "evidence_summary": {
                        "reconciliation_status": "fail",
                        "account_snapshot_age_seconds": 700,
                        "account_snapshot_stale_threshold_seconds": 300,
                        "broker_event_sync_age_seconds": 650,
                        "broker_event_stale_threshold_seconds": 300,
                        "ledger_diff": 0.4,
                        "ledger_diff_threshold_usd": 5.0,
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
                    "since_ts": 10,
                    "duration_seconds": 400,
                    "updated_ts": 10,
                },
            },
            now_ts=1000,
            heartbeat_stale_after_seconds=120,
            buy_blocked_alert_after_seconds=300,
        )

        active_codes = {str(item.get("alert_code")) for item in snapshot.get("active_alerts", [])}
        self.assertIn(ALERT_RUNNER_HEARTBEAT_STALE, active_codes)
        self.assertIn(ALERT_RECONCILIATION_FAIL, active_codes)
        self.assertIn(ALERT_ACCOUNT_SNAPSHOT_STALE, active_codes)
        self.assertIn(ALERT_EVENT_STREAM_STALE, active_codes)
        self.assertIn(ALERT_KILL_SWITCH_MANUAL_REQUIRED, active_codes)
        self.assertIn(ALERT_SIGNER_UNHEALTHY, active_codes)
        self.assertIn(ALERT_WRITER_CONFLICT_READONLY, active_codes)
        self.assertIn(ALERT_HOT_WALLET_CAP_EXCEEDED, active_codes)
        self.assertIn(ALERT_BUY_BLOCKED_TOO_LONG, active_codes)

    def test_alert_metric_labels_are_fixed_and_low_cardinality(self):
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
                        "broker_event_sync_age_seconds": 900,
                        "broker_event_stale_threshold_seconds": 300,
                        "ledger_diff": 2.0,
                        "ledger_diff_threshold_usd": 1.0,
                    },
                },
                "buy_blocked": {"active": True, "reason_code": "startup_not_ready", "since_ts": 10, "duration_seconds": 600, "updated_ts": 610},
            },
            now_ts=1000,
            heartbeat_stale_after_seconds=120,
            buy_blocked_alert_after_seconds=300,
        )
        metrics_text = render_prometheus_metrics(snapshot)
        alert_lines = [line for line in metrics_text.splitlines() if line.startswith("polymarket_alert_active{")]
        self.assertEqual(len(alert_lines), len(ALERT_CODE_WHITELIST))
        for line in alert_lines:
            self.assertIn('alert_code="', line)
            self.assertIn('severity="', line)
            self.assertNotIn("reason_code=", line)
            self.assertNotIn("wallet=", line)
            self.assertNotIn("order_id=", line)
            self.assertNotIn("market_slug=", line)
