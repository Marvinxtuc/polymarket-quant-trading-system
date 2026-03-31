from __future__ import annotations

import unittest

from polymarket_bot.admission_gate import (
    MODE_HALTED,
    MODE_NORMAL,
    MODE_REDUCE_ONLY,
    REASON_ADMISSION_GATE_INTERNAL_ERROR,
    REASON_AMBIGUOUS_PENDING_UNRESOLVED,
    REASON_AUTO_RECOVER_WARMUP,
    REASON_BOOTSTRAP_PROTECTED_EVIDENCE_MISSING,
    REASON_LEDGER_DIFF_EXCEEDED,
    REASON_OPERATOR_EMERGENCY_STOP,
    REASON_OPERATOR_MANUAL_REDUCE_ONLY,
    REASON_PERSISTENCE_FAULT,
    REASON_RECONCILIATION_FAIL,
    REASON_RECOVERY_CONFLICT_UNRESOLVED_MANUAL,
    REASON_RISK_BREAKER_STATE_INVALID,
    REASON_RISK_LEDGER_FAULT,
    REASON_STARTUP_CHECKS_FAIL,
    REASON_STALE_ACCOUNT_SNAPSHOT,
    REASON_STALE_BROKER_EVENT_STREAM,
    AdmissionEvidence,
    evaluate_admission,
)


class AdmissionGateDecisionTests(unittest.TestCase):
    def _evidence(self, **overrides: object) -> AdmissionEvidence:
        base: dict[str, object] = {
            "startup_ready": True,
            "startup_failure_count": 0,
            "reconciliation_status": "ok",
            "account_snapshot_age_seconds": 1,
            "account_snapshot_stale_threshold_seconds": 600,
            "broker_event_sync_age_seconds": 1,
            "broker_event_stale_threshold_seconds": 120,
            "ledger_diff": 0.0,
            "ledger_diff_threshold_usd": 0.01,
            "ambiguous_pending_orders": 0,
            "recovery_conflict_count": 0,
            "recovery_conflict_requires_manual": False,
            "persistence_status": "ok",
            "risk_ledger_status": "ok",
            "risk_breaker_status": "ok",
            "operator_pause_opening": False,
            "operator_reduce_only": False,
            "operator_emergency_stop": False,
            "dry_run": False,
            "bootstrap_protected": False,
            "bootstrap_evidence_fresh": True,
        }
        base.update(overrides)
        return AdmissionEvidence(**base)

    def test_startup_checks_fail_blocks_opening(self):
        decision = evaluate_admission(
            now_ts=1700000000,
            evidence=self._evidence(startup_ready=False, startup_failure_count=1),
            previous_auto_latch_active=False,
            previous_trusted_consecutive_cycles=0,
            auto_recover_min_healthy_cycles=1,
        )
        self.assertEqual(decision.mode, MODE_REDUCE_ONLY)
        self.assertFalse(decision.opening_allowed)
        self.assertIn(REASON_STARTUP_CHECKS_FAIL, decision.reason_codes)

    def test_reconciliation_fail_blocks_opening(self):
        decision = evaluate_admission(
            now_ts=1700000000,
            evidence=self._evidence(reconciliation_status="fail"),
            previous_auto_latch_active=False,
            previous_trusted_consecutive_cycles=0,
            auto_recover_min_healthy_cycles=1,
        )
        self.assertEqual(decision.mode, MODE_REDUCE_ONLY)
        self.assertFalse(decision.opening_allowed)
        self.assertIn(REASON_RECONCILIATION_FAIL, decision.reason_codes)

    def test_stale_account_snapshot_blocks_opening(self):
        decision = evaluate_admission(
            now_ts=1700000000,
            evidence=self._evidence(account_snapshot_age_seconds=9999),
            previous_auto_latch_active=False,
            previous_trusted_consecutive_cycles=0,
            auto_recover_min_healthy_cycles=1,
        )
        self.assertEqual(decision.mode, MODE_REDUCE_ONLY)
        self.assertFalse(decision.opening_allowed)
        self.assertIn(REASON_STALE_ACCOUNT_SNAPSHOT, decision.reason_codes)

    def test_stale_event_stream_blocks_opening(self):
        decision = evaluate_admission(
            now_ts=1700000000,
            evidence=self._evidence(broker_event_sync_age_seconds=9999),
            previous_auto_latch_active=False,
            previous_trusted_consecutive_cycles=0,
            auto_recover_min_healthy_cycles=1,
        )
        self.assertEqual(decision.mode, MODE_REDUCE_ONLY)
        self.assertFalse(decision.opening_allowed)
        self.assertIn(REASON_STALE_BROKER_EVENT_STREAM, decision.reason_codes)

    def test_ledger_diff_exceeded_blocks_opening(self):
        decision = evaluate_admission(
            now_ts=1700000000,
            evidence=self._evidence(ledger_diff=0.2, ledger_diff_threshold_usd=0.01),
            previous_auto_latch_active=False,
            previous_trusted_consecutive_cycles=0,
            auto_recover_min_healthy_cycles=1,
        )
        self.assertEqual(decision.mode, MODE_REDUCE_ONLY)
        self.assertFalse(decision.opening_allowed)
        self.assertIn(REASON_LEDGER_DIFF_EXCEEDED, decision.reason_codes)

    def test_unresolved_ambiguity_blocks_opening(self):
        decision = evaluate_admission(
            now_ts=1700000000,
            evidence=self._evidence(ambiguous_pending_orders=1),
            previous_auto_latch_active=False,
            previous_trusted_consecutive_cycles=0,
            auto_recover_min_healthy_cycles=1,
        )
        self.assertEqual(decision.mode, MODE_REDUCE_ONLY)
        self.assertIn(REASON_AMBIGUOUS_PENDING_UNRESOLVED, decision.reason_codes)

    def test_manual_recovery_conflict_requires_manual_confirmation(self):
        decision = evaluate_admission(
            now_ts=1700000000,
            evidence=self._evidence(recovery_conflict_count=1, recovery_conflict_requires_manual=True),
            previous_auto_latch_active=False,
            previous_trusted_consecutive_cycles=0,
            auto_recover_min_healthy_cycles=1,
        )
        self.assertEqual(decision.mode, MODE_REDUCE_ONLY)
        self.assertTrue(decision.manual_confirmation_required)
        self.assertIn(REASON_RECOVERY_CONFLICT_UNRESOLVED_MANUAL, decision.reason_codes)

    def test_persistence_fault_enters_halted(self):
        decision = evaluate_admission(
            now_ts=1700000000,
            evidence=self._evidence(persistence_status="fault"),
            previous_auto_latch_active=False,
            previous_trusted_consecutive_cycles=0,
            auto_recover_min_healthy_cycles=1,
        )
        self.assertEqual(decision.mode, MODE_HALTED)
        self.assertTrue(decision.halted)
        self.assertFalse(decision.opening_allowed)
        self.assertIn(REASON_PERSISTENCE_FAULT, decision.reason_codes)

    def test_risk_ledger_fault_enters_halted(self):
        decision = evaluate_admission(
            now_ts=1700000000,
            evidence=self._evidence(risk_ledger_status="fault"),
            previous_auto_latch_active=False,
            previous_trusted_consecutive_cycles=0,
            auto_recover_min_healthy_cycles=1,
        )
        self.assertEqual(decision.mode, MODE_HALTED)
        self.assertFalse(decision.opening_allowed)
        self.assertIn(REASON_RISK_LEDGER_FAULT, decision.reason_codes)

    def test_risk_breaker_state_invalid_enters_halted(self):
        decision = evaluate_admission(
            now_ts=1700000000,
            evidence=self._evidence(risk_breaker_status="invalid"),
            previous_auto_latch_active=False,
            previous_trusted_consecutive_cycles=0,
            auto_recover_min_healthy_cycles=1,
        )
        self.assertEqual(decision.mode, MODE_HALTED)
        self.assertFalse(decision.opening_allowed)
        self.assertIn(REASON_RISK_BREAKER_STATE_INVALID, decision.reason_codes)

    def test_operator_emergency_stop_halts_with_flatten_sell_whitelist(self):
        decision = evaluate_admission(
            now_ts=1700000000,
            evidence=self._evidence(operator_emergency_stop=True),
            previous_auto_latch_active=False,
            previous_trusted_consecutive_cycles=0,
            auto_recover_min_healthy_cycles=1,
        )
        self.assertEqual(decision.mode, MODE_HALTED)
        self.assertIn(REASON_OPERATOR_EMERGENCY_STOP, decision.reason_codes)
        self.assertIn("operator_emergency_flatten_sell", decision.action_whitelist)

    def test_operator_manual_reduce_only_is_manual_latch(self):
        decision = evaluate_admission(
            now_ts=1700000000,
            evidence=self._evidence(operator_reduce_only=True),
            previous_auto_latch_active=False,
            previous_trusted_consecutive_cycles=0,
            auto_recover_min_healthy_cycles=1,
        )
        self.assertEqual(decision.mode, MODE_REDUCE_ONLY)
        self.assertFalse(decision.opening_allowed)
        self.assertTrue(decision.manual_confirmation_required)
        self.assertIn(REASON_OPERATOR_MANUAL_REDUCE_ONLY, decision.reason_codes)

    def test_bootstrap_protection_blocks_without_fresh_evidence(self):
        decision = evaluate_admission(
            now_ts=1700000000,
            evidence=self._evidence(bootstrap_protected=True, bootstrap_evidence_fresh=False),
            previous_auto_latch_active=False,
            previous_trusted_consecutive_cycles=0,
            auto_recover_min_healthy_cycles=1,
        )
        self.assertFalse(decision.opening_allowed)
        self.assertIn(REASON_BOOTSTRAP_PROTECTED_EVIDENCE_MISSING, decision.reason_codes)

    def test_auto_recover_requires_healthy_cycle_progression(self):
        first = evaluate_admission(
            now_ts=1700000000,
            evidence=self._evidence(),
            previous_auto_latch_active=True,
            previous_trusted_consecutive_cycles=0,
            auto_recover_min_healthy_cycles=2,
        )
        self.assertEqual(first.mode, MODE_REDUCE_ONLY)
        self.assertTrue(first.auto_recover)
        self.assertIn(REASON_AUTO_RECOVER_WARMUP, first.reason_codes)
        self.assertEqual(first.trusted_consecutive_cycles, 1)

        second = evaluate_admission(
            now_ts=1700000001,
            evidence=self._evidence(),
            previous_auto_latch_active=True,
            previous_trusted_consecutive_cycles=first.trusted_consecutive_cycles,
            auto_recover_min_healthy_cycles=2,
        )
        self.assertEqual(second.mode, MODE_NORMAL)
        self.assertTrue(second.opening_allowed)
        self.assertFalse(second.auto_recover)

    def test_internal_error_reason_is_halted_class(self):
        decision = evaluate_admission(
            now_ts=1700000000,
            evidence=self._evidence(persistence_status="fault"),
            previous_auto_latch_active=False,
            previous_trusted_consecutive_cycles=0,
            auto_recover_min_healthy_cycles=1,
        )
        self.assertNotIn(REASON_ADMISSION_GATE_INTERNAL_ERROR, decision.reason_codes)


if __name__ == "__main__":
    unittest.main()
