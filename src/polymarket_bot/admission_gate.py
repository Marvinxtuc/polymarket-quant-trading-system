from __future__ import annotations

from dataclasses import dataclass

MODE_NORMAL = "NORMAL"
MODE_REDUCE_ONLY = "REDUCE_ONLY"
MODE_HALTED = "HALTED"

REASON_OPERATOR_EMERGENCY_STOP = "operator_emergency_stop"
REASON_OPERATOR_MANUAL_REDUCE_ONLY = "operator_manual_reduce_only"
REASON_OPERATOR_PAUSE_OPENING = "operator_pause_opening"
REASON_STARTUP_CHECKS_FAIL = "startup_checks_fail"
REASON_RECONCILIATION_FAIL = "reconciliation_fail"
REASON_RECONCILIATION_WARN = "reconciliation_warn"
REASON_STALE_ACCOUNT_SNAPSHOT = "stale_account_snapshot"
REASON_STALE_BROKER_EVENT_STREAM = "stale_broker_event_stream"
REASON_LEDGER_DIFF_EXCEEDED = "ledger_diff_exceeded"
REASON_AMBIGUOUS_PENDING_UNRESOLVED = "ambiguous_pending_unresolved"
REASON_RECOVERY_CONFLICT_UNRESOLVED = "recovery_conflict_unresolved"
REASON_RECOVERY_CONFLICT_UNRESOLVED_MANUAL = "recovery_conflict_unresolved_manual"
REASON_PERSISTENCE_FAULT = "persistence_fault"
REASON_ADMISSION_GATE_INTERNAL_ERROR = "admission_gate_internal_error"
REASON_BOOTSTRAP_PROTECTED_EVIDENCE_MISSING = "bootstrap_protected_evidence_missing"
REASON_AUTO_RECOVER_WARMUP = "auto_recover_warmup"
REASON_RISK_LEDGER_FAULT = "risk_ledger_fault"
REASON_RISK_BREAKER_STATE_INVALID = "risk_breaker_state_invalid"

MANUAL_LATCH_REASONS = {
    REASON_OPERATOR_EMERGENCY_STOP,
    REASON_OPERATOR_MANUAL_REDUCE_ONLY,
    REASON_PERSISTENCE_FAULT,
    REASON_ADMISSION_GATE_INTERNAL_ERROR,
    REASON_RECOVERY_CONFLICT_UNRESOLVED_MANUAL,
    REASON_RISK_LEDGER_FAULT,
    REASON_RISK_BREAKER_STATE_INVALID,
}
HALTED_REASONS = {
    REASON_OPERATOR_EMERGENCY_STOP,
    REASON_PERSISTENCE_FAULT,
    REASON_ADMISSION_GATE_INTERNAL_ERROR,
    REASON_RISK_LEDGER_FAULT,
    REASON_RISK_BREAKER_STATE_INVALID,
}

HALTED_ACTIONS_BASE = (
    "sync_read",
    "state_evaluation",
    "cancel_pending_buy",
    "persist_state_update",
)
REDUCE_ONLY_ACTIONS = (
    "sync_read",
    "state_evaluation",
    "cancel_pending_buy",
    "risk_reduction_action",
    "persist_state_update",
)


@dataclass(slots=True)
class AdmissionEvidence:
    startup_ready: bool
    startup_failure_count: int
    reconciliation_status: str
    account_snapshot_age_seconds: int
    account_snapshot_stale_threshold_seconds: int
    broker_event_sync_age_seconds: int
    broker_event_stale_threshold_seconds: int
    ledger_diff: float
    ledger_diff_threshold_usd: float
    ambiguous_pending_orders: int
    recovery_conflict_count: int
    recovery_conflict_requires_manual: bool
    persistence_status: str
    risk_ledger_status: str
    risk_breaker_status: str
    operator_pause_opening: bool
    operator_reduce_only: bool
    operator_emergency_stop: bool
    dry_run: bool
    bootstrap_protected: bool = False
    bootstrap_evidence_fresh: bool = True


@dataclass(slots=True)
class AdmissionDecision:
    mode: str
    opening_allowed: bool
    reduce_only: bool
    halted: bool
    auto_recover: bool
    manual_confirmation_required: bool
    reason_codes: tuple[str, ...]
    action_whitelist: tuple[str, ...]
    latch_kind: str
    trusted: bool
    trusted_consecutive_cycles: int
    evidence_summary: dict[str, object]
    evaluated_ts: int
    auto_latch_active: bool
    manual_latch_active: bool

    @staticmethod
    def default() -> "AdmissionDecision":
        return AdmissionDecision(
            mode=MODE_REDUCE_ONLY,
            opening_allowed=False,
            reduce_only=True,
            halted=False,
            auto_recover=False,
            manual_confirmation_required=True,
            reason_codes=(REASON_BOOTSTRAP_PROTECTED_EVIDENCE_MISSING,),
            action_whitelist=REDUCE_ONLY_ACTIONS,
            latch_kind="manual",
            trusted=False,
            trusted_consecutive_cycles=0,
            evidence_summary={},
            evaluated_ts=0,
            auto_latch_active=False,
            manual_latch_active=True,
        )


def _unique_reason_codes(reason_codes: list[str]) -> tuple[str, ...]:
    out: list[str] = []
    seen: set[str] = set()
    for item in reason_codes:
        key = str(item or "").strip()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return tuple(out)


def evaluate_admission(
    *,
    now_ts: int,
    evidence: AdmissionEvidence,
    previous_auto_latch_active: bool,
    previous_trusted_consecutive_cycles: int,
    auto_recover_min_healthy_cycles: int,
) -> AdmissionDecision:
    reason_codes: list[str] = []
    reconciliation_status = str(evidence.reconciliation_status or "unknown").strip().lower()
    persistence_status = str(evidence.persistence_status or "unknown").strip().lower() or "unknown"
    risk_ledger_status = str(evidence.risk_ledger_status or "unknown").strip().lower() or "unknown"
    risk_breaker_status = str(evidence.risk_breaker_status or "unknown").strip().lower() or "unknown"
    min_healthy_cycles = max(1, int(auto_recover_min_healthy_cycles or 1))

    if evidence.operator_emergency_stop:
        reason_codes.append(REASON_OPERATOR_EMERGENCY_STOP)
    if evidence.operator_reduce_only:
        reason_codes.append(REASON_OPERATOR_MANUAL_REDUCE_ONLY)
    if evidence.operator_pause_opening:
        reason_codes.append(REASON_OPERATOR_PAUSE_OPENING)

    if not bool(evidence.startup_ready) or int(evidence.startup_failure_count or 0) > 0:
        reason_codes.append(REASON_STARTUP_CHECKS_FAIL)
    if reconciliation_status == "fail":
        reason_codes.append(REASON_RECONCILIATION_FAIL)
    elif reconciliation_status == "warn":
        reason_codes.append(REASON_RECONCILIATION_WARN)

    if not evidence.dry_run and int(evidence.account_snapshot_age_seconds or 0) > int(
        evidence.account_snapshot_stale_threshold_seconds or 0
    ):
        reason_codes.append(REASON_STALE_ACCOUNT_SNAPSHOT)
    if not evidence.dry_run and int(evidence.broker_event_sync_age_seconds or 0) > int(
        evidence.broker_event_stale_threshold_seconds or 0
    ):
        reason_codes.append(REASON_STALE_BROKER_EVENT_STREAM)
    if abs(float(evidence.ledger_diff or 0.0)) > float(evidence.ledger_diff_threshold_usd or 0.0):
        reason_codes.append(REASON_LEDGER_DIFF_EXCEEDED)
    if int(evidence.ambiguous_pending_orders or 0) > 0:
        reason_codes.append(REASON_AMBIGUOUS_PENDING_UNRESOLVED)
    if int(evidence.recovery_conflict_count or 0) > 0:
        if bool(evidence.recovery_conflict_requires_manual):
            reason_codes.append(REASON_RECOVERY_CONFLICT_UNRESOLVED_MANUAL)
        else:
            reason_codes.append(REASON_RECOVERY_CONFLICT_UNRESOLVED)
    if persistence_status != "ok":
        reason_codes.append(REASON_PERSISTENCE_FAULT)
    if risk_ledger_status != "ok":
        reason_codes.append(REASON_RISK_LEDGER_FAULT)
    if risk_breaker_status != "ok":
        reason_codes.append(REASON_RISK_BREAKER_STATE_INVALID)

    if bool(evidence.bootstrap_protected) and not bool(evidence.bootstrap_evidence_fresh):
        reason_codes.append(REASON_BOOTSTRAP_PROTECTED_EVIDENCE_MISSING)

    reason_tuple = _unique_reason_codes(reason_codes)
    manual_latch_active = any(reason in MANUAL_LATCH_REASONS for reason in reason_tuple)
    trusted = not bool(reason_tuple)

    trusted_consecutive_cycles = 0
    auto_latch_active = False
    mode = MODE_NORMAL
    auto_recover = False

    if trusted:
        if previous_auto_latch_active:
            trusted_consecutive_cycles = max(0, int(previous_trusted_consecutive_cycles or 0)) + 1
            if trusted_consecutive_cycles < min_healthy_cycles:
                mode = MODE_REDUCE_ONLY
                auto_recover = True
                auto_latch_active = True
                reason_tuple = reason_tuple + (REASON_AUTO_RECOVER_WARMUP,)
            else:
                mode = MODE_NORMAL
        else:
            mode = MODE_NORMAL
    else:
        trusted_consecutive_cycles = 0
        if any(reason in HALTED_REASONS for reason in reason_tuple):
            mode = MODE_HALTED
        else:
            mode = MODE_REDUCE_ONLY
        if not manual_latch_active:
            auto_latch_active = True

    halted = mode == MODE_HALTED
    reduce_only = mode != MODE_NORMAL
    opening_allowed = mode == MODE_NORMAL

    if halted:
        whitelist = list(HALTED_ACTIONS_BASE)
        if REASON_OPERATOR_EMERGENCY_STOP in reason_tuple:
            whitelist.append("operator_emergency_flatten_sell")
        action_whitelist = tuple(whitelist)
    elif reduce_only:
        action_whitelist = REDUCE_ONLY_ACTIONS
    else:
        action_whitelist = ("sync_read", "state_evaluation", "persist_state_update")

    evidence_summary = {
        "startup_ready": bool(evidence.startup_ready),
        "startup_failure_count": int(evidence.startup_failure_count or 0),
        "reconciliation_status": reconciliation_status or "unknown",
        "account_snapshot_age_seconds": int(evidence.account_snapshot_age_seconds or 0),
        "broker_event_sync_age_seconds": int(evidence.broker_event_sync_age_seconds or 0),
        "ledger_diff": float(evidence.ledger_diff or 0.0),
        "ledger_diff_threshold_usd": float(evidence.ledger_diff_threshold_usd or 0.0),
        "ambiguous_pending_orders": int(evidence.ambiguous_pending_orders or 0),
        "recovery_conflict_count": int(evidence.recovery_conflict_count or 0),
        "persistence_status": persistence_status,
        "risk_ledger_status": risk_ledger_status,
        "risk_breaker_status": risk_breaker_status,
    }

    latch_kind = "manual" if manual_latch_active else ("auto" if auto_latch_active else "none")
    return AdmissionDecision(
        mode=mode,
        opening_allowed=opening_allowed,
        reduce_only=reduce_only,
        halted=halted,
        auto_recover=auto_recover,
        manual_confirmation_required=manual_latch_active,
        reason_codes=reason_tuple,
        action_whitelist=action_whitelist,
        latch_kind=latch_kind,
        trusted=trusted,
        trusted_consecutive_cycles=trusted_consecutive_cycles,
        evidence_summary=evidence_summary,
        evaluated_ts=int(now_ts),
        auto_latch_active=auto_latch_active,
        manual_latch_active=manual_latch_active,
    )
