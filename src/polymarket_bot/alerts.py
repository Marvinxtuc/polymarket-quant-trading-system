from __future__ import annotations

from dataclasses import dataclass


SEVERITY_PAGE = "page"
SEVERITY_WARNING = "warning"

ALERT_RUNNER_HEARTBEAT_STALE = "runner_heartbeat_stale"
ALERT_ADMISSION_FAIL_CLOSED = "admission_fail_closed"
ALERT_RECONCILIATION_FAIL = "reconciliation_fail"
ALERT_ACCOUNT_SNAPSHOT_STALE = "account_snapshot_stale"
ALERT_EVENT_STREAM_STALE = "event_stream_stale"
ALERT_LEDGER_DIFF_EXCEEDED = "ledger_diff_exceeded"
ALERT_KILL_SWITCH_INFLIGHT = "kill_switch_inflight"
ALERT_KILL_SWITCH_MANUAL_REQUIRED = "kill_switch_manual_required"
ALERT_SIGNER_UNHEALTHY = "signer_unhealthy"
ALERT_WRITER_CONFLICT_READONLY = "writer_conflict_readonly"
ALERT_HOT_WALLET_CAP_EXCEEDED = "hot_wallet_cap_exceeded"
ALERT_BUY_BLOCKED_TOO_LONG = "buy_blocked_too_long"

ALERT_CODE_TO_SEVERITY: dict[str, str] = {
    ALERT_RUNNER_HEARTBEAT_STALE: SEVERITY_PAGE,
    ALERT_ADMISSION_FAIL_CLOSED: SEVERITY_PAGE,
    ALERT_RECONCILIATION_FAIL: SEVERITY_PAGE,
    ALERT_ACCOUNT_SNAPSHOT_STALE: SEVERITY_WARNING,
    ALERT_EVENT_STREAM_STALE: SEVERITY_WARNING,
    ALERT_LEDGER_DIFF_EXCEEDED: SEVERITY_PAGE,
    ALERT_KILL_SWITCH_INFLIGHT: SEVERITY_WARNING,
    ALERT_KILL_SWITCH_MANUAL_REQUIRED: SEVERITY_PAGE,
    ALERT_SIGNER_UNHEALTHY: SEVERITY_PAGE,
    ALERT_WRITER_CONFLICT_READONLY: SEVERITY_PAGE,
    ALERT_HOT_WALLET_CAP_EXCEEDED: SEVERITY_PAGE,
    ALERT_BUY_BLOCKED_TOO_LONG: SEVERITY_WARNING,
}

ALERT_CODE_WHITELIST: tuple[str, ...] = tuple(ALERT_CODE_TO_SEVERITY.keys())


@dataclass(frozen=True, slots=True)
class AlertSignal:
    alert_code: str
    active: bool
    reason: str = ""

    @property
    def severity(self) -> str:
        return ALERT_CODE_TO_SEVERITY.get(self.alert_code, SEVERITY_WARNING)


def severity_for_alert_code(alert_code: str) -> str:
    code = str(alert_code or "").strip()
    return ALERT_CODE_TO_SEVERITY.get(code, SEVERITY_WARNING)


def sanitize_alert_code_list(items: object) -> tuple[str, ...]:
    if not isinstance(items, list):
        return tuple()
    seen: set[str] = set()
    codes: list[str] = []
    for raw in items:
        code = str(raw or "").strip()
        if not code or code not in ALERT_CODE_TO_SEVERITY or code in seen:
            continue
        seen.add(code)
        codes.append(code)
    return tuple(codes)

