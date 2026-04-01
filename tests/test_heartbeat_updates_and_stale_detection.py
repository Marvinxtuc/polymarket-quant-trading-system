from __future__ import annotations

import unittest
from types import SimpleNamespace

from polymarket_bot.admission_gate import AdmissionDecision
from polymarket_bot.heartbeat import default_runner_heartbeat, normalize_runner_heartbeat
from polymarket_bot.kill_switch import default_state as default_kill_switch_state
from polymarket_bot.metrics import build_observability_snapshot
from polymarket_bot.runner import Trader


class HeartbeatUpdateTests(unittest.TestCase):
    def test_only_active_runner_updates_heartbeat(self):
        trader = Trader.__new__(Trader)
        trader.settings = SimpleNamespace(enable_single_writer=False)
        trader._runner_heartbeat = default_runner_heartbeat(now_ts=0)
        Trader._update_runner_heartbeat(trader, now_ts=100, cycle_started=True)
        updated = normalize_runner_heartbeat(trader._runner_heartbeat)
        self.assertEqual(updated["last_seen_ts"], 100)
        self.assertEqual(updated["last_cycle_started_ts"], 100)
        self.assertEqual(updated["cycle_seq"], 1)
        self.assertTrue(updated["writer_active"])

        trader.settings = SimpleNamespace(enable_single_writer=True)
        trader._writer_lock = None
        before = dict(trader._runner_heartbeat)
        Trader._update_runner_heartbeat(trader, now_ts=200, cycle_started=True)
        self.assertEqual(before, trader._runner_heartbeat)

    def test_stale_heartbeat_sets_runner_alert(self):
        snapshot = build_observability_snapshot(
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
                "buy_blocked": {
                    "active": False,
                    "reason_code": "",
                    "since_ts": 0,
                    "duration_seconds": 0,
                    "updated_ts": 10,
                },
            },
            now_ts=1000,
            heartbeat_stale_after_seconds=100,
            buy_blocked_alert_after_seconds=300,
        )
        active_codes = {str(item.get("alert_code")) for item in snapshot.get("active_alerts", [])}
        self.assertIn("runner_heartbeat_stale", active_codes)

    def test_buy_blocked_duration_start_and_clear_semantics(self):
        trader = Trader.__new__(Trader)
        trader._kill_switch_state = default_kill_switch_state(now_ts=0)
        trader._buy_blocked_since_ts = 0
        trader._admission_decision = AdmissionDecision(
            mode="REDUCE_ONLY",
            opening_allowed=False,
            reduce_only=True,
            halted=False,
            auto_recover=False,
            manual_confirmation_required=True,
            reason_codes=("startup_checks_fail",),
            action_whitelist=(),
            latch_kind="manual",
            trusted=False,
            trusted_consecutive_cycles=0,
            evidence_summary={},
            evaluated_ts=100,
            auto_latch_active=False,
            manual_latch_active=True,
        )
        Trader._refresh_buy_blocked_state(trader, now_ts=100)
        blocked_state = Trader.buy_blocked_state(trader, now_ts=130)
        self.assertTrue(blocked_state["active"])
        self.assertEqual(blocked_state["since_ts"], 100)
        self.assertEqual(blocked_state["duration_seconds"], 30)

        trader._admission_decision = AdmissionDecision(
            mode="NORMAL",
            opening_allowed=True,
            reduce_only=False,
            halted=False,
            auto_recover=True,
            manual_confirmation_required=False,
            reason_codes=(),
            action_whitelist=(),
            latch_kind="none",
            trusted=True,
            trusted_consecutive_cycles=2,
            evidence_summary={},
            evaluated_ts=140,
            auto_latch_active=False,
            manual_latch_active=False,
        )
        Trader._refresh_buy_blocked_state(trader, now_ts=140)
        cleared_state = Trader.buy_blocked_state(trader, now_ts=150)
        self.assertFalse(cleared_state["active"])
        self.assertEqual(cleared_state["since_ts"], 0)
        self.assertEqual(cleared_state["duration_seconds"], 0)
