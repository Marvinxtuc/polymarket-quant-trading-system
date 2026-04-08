from __future__ import annotations

import json
import hashlib
import logging
import os
import re
import time
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from polymarket_bot.admission_gate import (
    AdmissionDecision,
    AdmissionEvidence,
    MODE_HALTED,
    MODE_NORMAL,
    REASON_AMBIGUOUS_PENDING_UNRESOLVED,
    REASON_ADMISSION_GATE_INTERNAL_ERROR,
    REASON_LEDGER_DIFF_EXCEEDED,
    REASON_OPERATOR_MANUAL_REDUCE_ONLY,
    REASON_PERSISTENCE_FAULT,
    REASON_RISK_BREAKER_STATE_INVALID,
    REASON_RISK_LEDGER_FAULT,
    REASON_RECOVERY_CONFLICT_UNRESOLVED,
    REASON_RECOVERY_CONFLICT_UNRESOLVED_MANUAL,
    REASON_STARTUP_CHECKS_FAIL,
    REASON_STALE_ACCOUNT_SNAPSHOT,
    REASON_STALE_BROKER_EVENT_STREAM,
    evaluate_admission,
)
from polymarket_bot.brokers.base import Broker
from polymarket_bot.clients.data_api import AccountingSnapshot, MarketMetadata, PolymarketDataClient, PriceHistoryPoint
from polymarket_bot.config import Settings
from polymarket_bot.db import PersonalTerminalStore
from polymarket_bot.force_exit import (
    begin_time_exit_attempt,
    estimate_time_exit_volatility_bps,
    record_time_exit_failure,
    record_time_exit_success,
    should_attempt_time_exit,
)
from polymarket_bot.i18n import enum_label as i18n_enum_label, humanize_identifier as i18n_humanize_identifier, label as i18n_label, t as i18n_t
from polymarket_bot.idempotency import (
    CLAIMED_NEW,
    EXISTING_NON_TERMINAL,
    EXISTING_TERMINAL,
    INTENT_STATUS_ACKED_PENDING,
    INTENT_STATUS_ACK_UNKNOWN,
    INTENT_STATUS_FAILED,
    INTENT_STATUS_FILLED,
    INTENT_STATUS_MANUAL_REQUIRED,
    INTENT_STATUS_NEW,
    INTENT_STATUS_PARTIAL,
    INTENT_STATUS_SENDING,
    NON_TERMINAL_STATUSES,
    STORAGE_ERROR,
    build_intent_idempotency_key,
    normalize_status as normalize_intent_status,
)
from polymarket_bot.heartbeat import (
    LOOP_STATUS_ERROR as RUNNER_LOOP_STATUS_ERROR,
    LOOP_STATUS_IDLE as RUNNER_LOOP_STATUS_IDLE,
    LOOP_STATUS_RUNNING as RUNNER_LOOP_STATUS_RUNNING,
    LOOP_STATUS_STOPPED as RUNNER_LOOP_STATUS_STOPPED,
    default_runner_heartbeat,
    normalize_runner_heartbeat,
)
from polymarket_bot.kill_switch import (
    MODE_EMERGENCY_STOP as KILL_SWITCH_MODE_EMERGENCY_STOP,
    MODE_NONE as KILL_SWITCH_MODE_NONE,
    MODE_PAUSE_OPENING as KILL_SWITCH_MODE_PAUSE_OPENING,
    MODE_REDUCE_ONLY as KILL_SWITCH_MODE_REDUCE_ONLY,
    PHASE_CANCELING_BUY as KILL_SWITCH_PHASE_CANCELING_BUY,
    PHASE_FAILED_MANUAL_REQUIRED as KILL_SWITCH_PHASE_FAILED_MANUAL_REQUIRED,
    PHASE_IDLE as KILL_SWITCH_PHASE_IDLE,
    PHASE_REQUESTED as KILL_SWITCH_PHASE_REQUESTED,
    PHASE_SAFE_CONFIRMED as KILL_SWITCH_PHASE_SAFE_CONFIRMED,
    PHASE_WAITING_BROKER_TERMINAL as KILL_SWITCH_PHASE_WAITING_BROKER_TERMINAL,
    default_state as default_kill_switch_state,
    normalize_state as normalize_kill_switch_state,
    requested_mode as requested_kill_switch_mode,
    status_is_terminal as kill_switch_status_is_terminal,
)
from polymarket_bot.locks import (
    FileLock,
    SingleWriterLockError,
    derive_writer_scope,
)
from polymarket_bot.models.control_state import PersistedControlState
from polymarket_bot.models.exit_state import (
    TIME_EXIT_STAGE_FORCE_EXIT,
    TIME_EXIT_STAGE_IDLE,
    normalize_time_exit_state,
)
from polymarket_bot.models.exposure_ledger import ExposureLedgerEntry
from polymarket_bot.models.risk_breaker_state import RiskBreakerState, default_risk_breaker_state
from polymarket_bot.models.signer_status import SignerStatusSnapshot
from polymarket_bot.notifier import Notifier
from polymarket_bot.reconciliation_report import append_ledger_entry, load_ledger_rows
from polymarket_bot.risk import (
    REASON_CONDITION_EXPOSURE_CAP_REACHED,
    REASON_INTRADAY_DRAWDOWN_BREAKER_ACTIVE,
    REASON_LOSS_STREAK_BREAKER_ACTIVE,
    REASON_PORTFOLIO_EXPOSURE_CAP_REACHED,
    REASON_RISK_BREAKER_STATE_INVALID as RISK_REASON_BREAKER_STATE_INVALID,
    REASON_RISK_LEDGER_FAULT as RISK_REASON_LEDGER_FAULT,
    REASON_WALLET_EXPOSURE_CAP_REACHED,
    RiskManager,
    RiskState,
)
from polymarket_bot.state_store import StateStore
from polymarket_bot.strategies.wallet_follower import WalletFollowerStrategy
from polymarket_bot.types import (
    BrokerOrderEvent,
    Candidate,
    CandidateReasonFactor,
    DecisionMode,
    ExecutionResult,
    JournalEntry,
    OpenOrderSnapshot,
    OrderFillSnapshot,
    OrderStatusSnapshot,
    Signal,
    WalletProfile,
)
from polymarket_bot.wallet_history import WalletHistoryStore
from polymarket_bot.wallet_scoring import RealizedWalletMetrics

REASON_REPEAT_ENTRY_BLOCKED_EXISTING_POSITION = "repeat_entry_blocked_existing_position"
REASON_SAME_WALLET_ADD_NOT_ALLOWED = "same_wallet_add_not_allowed"
REASON_CROSS_WALLET_REPEAT_ENTRY_BLOCKED = "cross_wallet_repeat_entry_blocked"
REASON_CANDIDATE_LIFETIME_EXPIRED = "candidate_lifetime_expired"
REPEAT_ENTRY_BLOCK_REASONS = {
    REASON_REPEAT_ENTRY_BLOCKED_EXISTING_POSITION,
    REASON_SAME_WALLET_ADD_NOT_ALLOWED,
    REASON_CROSS_WALLET_REPEAT_ENTRY_BLOCKED,
}

_MARKET_WINDOW_PATTERN = re.compile(r"-(5m|15m|30m|1h)-(\d{10})$")
_MARKET_WINDOW_SECONDS = {
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
}
_MARKET_METADATA_CACHE_TTL_SECONDS = 300
_SUBMIT_UNKNOWN_PROBE_WINDOW_SECONDS = 120
_SUBMIT_UNKNOWN_SIZE_PRECISION = 6
_ORDER_INTENT_TERMINAL_STATUSES = {"filled", "canceled", "failed", "rejected", "unmatched"}
_VALID_DECISION_MODES = {"manual", "semi_auto", "auto"}


ControlState = PersistedControlState


def _legacy_trading_mode_reason(reason: str) -> str:
    normalized = str(reason or "").strip()
    if not normalized:
        return ""
    mapping = {
        REASON_STARTUP_CHECKS_FAIL: "startup_not_ready",
        REASON_STALE_ACCOUNT_SNAPSHOT: "account_state_stale",
        REASON_STALE_BROKER_EVENT_STREAM: "broker_event_stream_stale",
        REASON_OPERATOR_MANUAL_REDUCE_ONLY: "pause_opening",
        "operator_pause_opening": "pause_opening",
        "operator_emergency_stop": "emergency_stop",
        REASON_ADMISSION_GATE_INTERNAL_ERROR: "emergency_stop",
        REASON_RECOVERY_CONFLICT_UNRESOLVED: "recovery_conflict",
        REASON_RECOVERY_CONFLICT_UNRESOLVED_MANUAL: "recovery_conflict",
        REASON_RISK_LEDGER_FAULT: "risk_ledger_fault",
        REASON_RISK_BREAKER_STATE_INVALID: "risk_breaker_state_invalid",
        REASON_LOSS_STREAK_BREAKER_ACTIVE: "loss_streak_breaker_active",
        REASON_INTRADAY_DRAWDOWN_BREAKER_ACTIVE: "intraday_drawdown_breaker_active",
    }
    return mapping.get(normalized, normalized)


def _legacy_trading_mode_reasons(reason_codes: tuple[str, ...]) -> list[str]:
    projected: list[str] = []
    seen: set[str] = set()
    for raw in reason_codes:
        label = _legacy_trading_mode_reason(raw)
        if not label or label in seen:
            continue
        seen.add(label)
        projected.append(label)
    return projected


@dataclass(slots=True)
class Trader:
    settings: Settings
    data_client: PolymarketDataClient
    strategy: WalletFollowerStrategy
    risk: RiskManager
    broker: Broker
    pre_acquired_writer_lock: FileLock | None = None
    state: RiskState = field(init=False)
    log: logging.Logger = field(init=False)
    _cached_wallets: list[str] = field(init=False, default_factory=list)
    _cached_wallet_activity_counts: dict[str, int] = field(init=False, default_factory=dict)
    _cached_wallet_selection_context: dict[str, dict[str, object]] = field(init=False, default_factory=dict)
    _cached_wallets_ts: float = field(init=False, default=0.0)
    _wallet_cache_ready: bool = field(init=False, default=False)
    _wallet_activity_available: bool = field(init=False, default=False)
    last_wallets: list[str] = field(init=False, default_factory=list)
    last_signals: list[Signal] = field(init=False, default_factory=list)
    recent_orders: deque[dict[str, object]] = field(init=False, default_factory=lambda: deque(maxlen=100))
    pending_orders: dict[str, dict[str, object]] = field(init=False, default_factory=dict)
    recent_signal_cycles: deque[dict[str, object]] = field(init=False, default_factory=lambda: deque(maxlen=24))
    positions_book: dict[str, dict[str, object]] = field(init=False, default_factory=dict)
    token_reentry_until: dict[str, int] = field(init=False, default_factory=dict)
    control_state: ControlState = field(init=False, default_factory=ControlState)
    _recent_order_keys: dict[str, float] = field(init=False, default_factory=dict)
    _last_broker_reconcile_ts: float = field(init=False, default=0.0)
    _wallet_history_store: WalletHistoryStore | None = field(init=False, default=None)
    _last_control_signature: tuple[str, bool, bool, bool, int, int, int] = field(
        init=False,
        default=("manual", False, False, False, 0, 0, 0),
    )
    _last_operator_pending_cleanup_ts: int = field(init=False, default=0)
    _last_operator_risk_breaker_clear_ts: int = field(init=False, default=0)
    _signal_seq: int = field(init=False, default=0)
    _cycle_seq: int = field(init=False, default=0)
    _trace_seq: int = field(init=False, default=0)
    _trace_registry: dict[str, dict[str, object]] = field(init=False, default_factory=dict)
    _trace_order: deque[str] = field(init=False, default_factory=lambda: deque(maxlen=64))
    _active_day_key: str = field(init=False, default="")
    _last_account_sync_ts: float = field(init=False, default=0.0)
    _last_broker_event_sync_ts: int = field(init=False, default=0)
    _intent_local_status: dict[str, str] = field(init=False, default_factory=dict)
    _ack_unknown_tracker: dict[str, dict[str, float | int | bool]] = field(init=False, default_factory=dict)
    startup_checks: list[dict[str, object]] = field(init=False, default_factory=list)
    startup_ready: bool = field(init=False, default=True)
    startup_warning_count: int = field(init=False, default=0)
    startup_failure_count: int = field(init=False, default=0)
    trading_mode: str = field(init=False, default="NORMAL")
    trading_mode_reasons: list[str] = field(init=False, default_factory=list)
    trading_mode_updated_ts: int = field(init=False, default=0)
    admission_opening_allowed: bool = field(init=False, default=False)
    admission_reduce_only: bool = field(init=False, default=True)
    admission_halted: bool = field(init=False, default=False)
    admission_auto_recover: bool = field(init=False, default=False)
    admission_manual_confirmation_required: bool = field(init=False, default=True)
    admission_latch_kind: str = field(init=False, default="manual")
    admission_action_whitelist: tuple[str, ...] = field(init=False, default_factory=tuple)
    admission_evidence_summary: dict[str, object] = field(init=False, default_factory=dict)
    _admission_decision: AdmissionDecision = field(init=False, default_factory=AdmissionDecision.default)
    _admission_auto_latch_active: bool = field(init=False, default=False)
    _admission_trusted_consecutive_cycles: int = field(init=False, default=0)
    _admission_bootstrap_protected: bool = field(init=False, default=True)
    _admission_internal_error_latched: bool = field(init=False, default=False)
    account_state_status: str = field(init=False, default="unknown")
    reconciliation_status: str = field(init=False, default="unknown")
    persistence_status: str = field(init=False, default="ok")
    persistence_failure_count: int = field(init=False, default=0)
    last_persistence_failure: dict[str, object] = field(init=False, default_factory=dict)
    _last_trading_mode_signature: tuple[str, tuple[str, ...], str, str, str] = field(
        init=False,
        default=("NORMAL", tuple(), "unknown", "unknown", "ok"),
    )
    last_operator_action: dict[str, object] = field(init=False, default_factory=dict)
    decision_mode: str = field(init=False, default="manual")
    candidate_store: PersonalTerminalStore | None = field(init=False, default=None)
    notifier: Notifier | None = field(init=False, default=None)
    _candidate_notification_watermarks: dict[str, int] = field(init=False, default_factory=dict)
    _critical_notification_watermarks: dict[str, int] = field(init=False, default_factory=dict)
    _candidate_price_history_cache: dict[str, dict[str, object]] = field(init=False, default_factory=dict)
    _market_metadata_cache: dict[str, dict[str, object]] = field(init=False, default_factory=dict)
    _state_store: StateStore | None = field(init=False, default=None)
    _writer_lock: FileLock | None = field(init=False, default=None)
    writer_scope: str = field(init=False, default="")
    _recovery_conflicts: list[dict[str, object]] = field(init=False, default_factory=list)
    _recovery_block_buy_latched: bool = field(init=False, default=False)
    _kill_switch_state: dict[str, object] = field(init=False, default_factory=default_kill_switch_state)
    _signer_security_snapshot: dict[str, object] = field(init=False, default_factory=dict)
    _hot_wallet_cap_conflict_latched: bool = field(init=False, default=False)
    _runner_heartbeat: dict[str, object] = field(init=False, default_factory=default_runner_heartbeat)
    _buy_blocked_since_ts: int = field(init=False, default=0)
    _risk_breaker_state: dict[str, object] = field(init=False, default_factory=dict)
    _exposure_ledger: dict[tuple[str, str], dict[str, object]] = field(init=False, default_factory=dict)
    _risk_ledger_status: str = field(init=False, default="ok")
    _risk_breaker_status: str = field(init=False, default="ok")

    def __post_init__(self) -> None:
        self.state = RiskState()
        self.log = logging.getLogger("polybot")
        self.startup_ready = False
        self.decision_mode = self._normalize_decision_mode(self.settings.decision_mode)
        self.writer_scope = derive_writer_scope(
            dry_run=bool(getattr(self.settings, "dry_run", True)),
            funder_address=str(getattr(self.settings, "funder_address", "") or ""),
            watch_wallets=str(getattr(self.settings, "watch_wallets", "") or ""),
        )
        self._refresh_signer_security_snapshot()
        if bool(getattr(self.settings, "enable_single_writer", False)):
            try:
                if self.pre_acquired_writer_lock is not None:
                    self._writer_lock = self.pre_acquired_writer_lock
                    self._writer_lock.assert_active()
                else:
                    self._writer_lock = FileLock(
                        self.settings.wallet_lock_path,
                        timeout=2.0,
                        writer_scope=self.writer_scope,
                    )
                    self._writer_lock.acquire()
                self.log.info(
                    "SINGLE_WRITER_ACQUIRED scope=%s lock_path=%s",
                    self.writer_scope,
                    self.settings.wallet_lock_path,
                )
            except SingleWriterLockError as exc:
                self.log.error(
                    "Single-writer lock failed path=%s scope=%s reason_code=%s err=%s",
                    self.settings.wallet_lock_path,
                    self.writer_scope,
                    getattr(exc, "reason_code", ""),
                    exc,
                )
                raise
            except Exception as exc:
                self.log.error(
                    "Single-writer lock failed path=%s scope=%s err=%s",
                    self.settings.wallet_lock_path,
                    self.writer_scope,
                    exc,
                )
                raise
        candidate_db_path = str(self.settings.candidate_db_path or "").strip()
        if candidate_db_path:
            self.candidate_store = PersonalTerminalStore(candidate_db_path)
        self.notifier = Notifier(
            local_enabled=bool(getattr(self.settings, "notify_local_enabled", True)),
            webhook_url=str(getattr(self.settings, "notify_webhook_url", "") or ""),
            webhook_urls=str(getattr(self.settings, "notify_webhook_urls", "") or ""),
            telegram_bot_token=str(getattr(self.settings, "notify_telegram_bot_token", "") or ""),
            telegram_chat_id=str(getattr(self.settings, "notify_telegram_chat_id", "") or ""),
            telegram_api_base=str(getattr(self.settings, "notify_telegram_api_base", "") or ""),
            telegram_parse_mode=str(getattr(self.settings, "notify_telegram_parse_mode", "") or ""),
            log_path=str(getattr(self.settings, "notify_log_path", "") or ""),
        )
        self._active_day_key = self._utc_day_key()
        self._state_store = StateStore(
            self.settings.state_store_path,
            writer_assertion=self._assert_writer_active if bool(getattr(self.settings, "enable_single_writer", False)) else None,
        )
        if bool(getattr(self.settings, "idempotency_enabled", True)):
            try:
                self._state_store.cleanup_idempotency(window_seconds=int(self.settings.idempotency_window_seconds))
            except Exception:
                self.log.warning("Idempotency cleanup failed path=%s", self.settings.state_store_path)
        self._wallet_history_store = WalletHistoryStore(
            client=self.data_client,
            cache_path=self.settings.wallet_history_path,
            refresh_seconds=self.settings.wallet_history_refresh_seconds,
            max_wallets=self.settings.wallet_history_max_wallets,
            closed_limit=self.settings.wallet_history_closed_limit,
            resolution_limit=self.settings.wallet_history_resolution_limit,
        )
        self._reconcile_runtime_state()
        self._maybe_sync_account_state(force=True)
        self._refresh_risk_state()
        self._run_startup_checks()
        startup_now = int(time.time())
        self._update_trading_mode(self.control_state, now=startup_now)
        self._refresh_buy_blocked_state(now_ts=startup_now)
        self._update_runner_heartbeat(now_ts=startup_now, loop_status=RUNNER_LOOP_STATUS_IDLE)
        self._refresh_risk_state()

    def __del__(self) -> None:
        try:
            if self._writer_lock is not None:
                self._writer_lock.release()
        except Exception:
            # Best-effort cleanup; do not raise during GC
            pass

    def _assert_writer_active(self) -> None:
        if not bool(getattr(self.settings, "enable_single_writer", False)):
            return
        lock = self._writer_lock
        if lock is None:
            raise SingleWriterLockError(
                "single-writer lock missing",
                reason_code="single_writer_not_active",
            )
        lock.assert_active()

    def _tracked_notional_usd(self) -> float:
        return sum(max(0.0, float(pos.get("notional") or 0.0)) for pos in self.positions_book.values())

    def _pending_entry_notional_usd(self) -> float:
        total = 0.0
        for order in self.pending_orders.values():
            if str(order.get("side") or "").upper() != "BUY":
                continue
            total += max(0.0, float(order.get("requested_notional") or 0.0))
        return total

    def _pending_exit_notional_usd(self) -> float:
        total = 0.0
        for order in self.pending_orders.values():
            if str(order.get("side") or "").upper() != "SELL":
                continue
            total += max(0.0, float(order.get("requested_notional") or 0.0))
        return total

    def _pending_entry_orders(self) -> int:
        return sum(1 for order in self.pending_orders.values() if str(order.get("side") or "").upper() == "BUY")

    def _risk_wallet_scope(self) -> str:
        wallet = str(self.settings.funder_address or "").strip().lower()
        return wallet or "default"

    def _risk_portfolio_scope(self) -> str:
        return str(self.writer_scope or self._risk_wallet_scope() or "default")

    def _risk_timezone(self) -> timezone | ZoneInfo:
        raw = str(getattr(self.settings, "risk_breaker_timezone", "UTC") or "UTC").strip() or "UTC"
        try:
            return ZoneInfo(raw)
        except ZoneInfoNotFoundError:
            self._risk_breaker_status = "invalid_timezone"
            return timezone.utc
        except Exception:
            self._risk_breaker_status = "invalid_timezone"
            return timezone.utc

    def _risk_day_key(self, ts: int | None = None) -> str:
        when = int(time.time()) if ts is None else int(ts)
        tz = self._risk_timezone()
        return datetime.fromtimestamp(max(0, when), tz=tz).strftime("%Y-%m-%d")

    def _rebuild_exposure_ledger(self, *, now_ts: int) -> dict[tuple[str, str], dict[str, object]]:
        bankroll = max(0.0, float(self.settings.bankroll_usd or 0.0))
        wallet_cap = max(0.0, bankroll * float(getattr(self.settings, "max_wallet_exposure_pct", 1.0) or 0.0))
        portfolio_cap = max(0.0, bankroll * float(getattr(self.settings, "max_portfolio_exposure_pct", 1.0) or 0.0))
        condition_cap = (
            max(0.0, bankroll * float(getattr(self.settings, "max_condition_exposure_pct", 0.0) or 0.0))
            if bool(getattr(self.settings, "portfolio_netting_enabled", True))
            else 0.0
        )
        wallet_scope = self._risk_wallet_scope()
        portfolio_scope = self._risk_portfolio_scope()

        wallet_filled = 0.0
        wallet_pending_entry = 0.0
        wallet_pending_exit = 0.0
        portfolio_filled = 0.0
        portfolio_pending_entry = 0.0
        portfolio_pending_exit = 0.0
        condition_filled: dict[str, float] = {}
        condition_pending_entry: dict[str, float] = {}
        condition_pending_exit: dict[str, float] = {}

        for position in self.positions_book.values():
            notional = max(0.0, float(position.get("notional") or 0.0))
            if notional <= 0.0:
                continue
            wallet_filled += notional
            portfolio_filled += notional
            condition_key, _ = self._position_condition_exposure_key(position)
            if condition_key:
                condition_filled[condition_key] = condition_filled.get(condition_key, 0.0) + notional

        for order in self.pending_orders.values():
            side = str(order.get("side") or "").upper()
            notional = max(0.0, float(order.get("requested_notional") or 0.0))
            if notional <= 0.0:
                continue
            condition_key, _ = self._pending_order_condition_exposure_key(order)
            if side == "BUY":
                wallet_pending_entry += notional
                portfolio_pending_entry += notional
                if condition_key:
                    condition_pending_entry[condition_key] = condition_pending_entry.get(condition_key, 0.0) + notional
            elif side == "SELL":
                wallet_pending_exit += notional
                portfolio_pending_exit += notional
                if condition_key:
                    condition_pending_exit[condition_key] = condition_pending_exit.get(condition_key, 0.0) + notional

        ledger: dict[tuple[str, str], dict[str, object]] = {}

        def _make_entry(
            *,
            scope_type: str,
            scope_key: str,
            cap_notional_usd: float,
            filled_notional_usd: float,
            pending_entry_notional_usd: float,
            pending_exit_notional_usd: float,
            default_reason: str,
            condition_scope: str = "",
        ) -> dict[str, object]:
            committed = max(0.0, filled_notional_usd + pending_entry_notional_usd)
            blocked = cap_notional_usd > 0.0 and committed >= cap_notional_usd - 1e-9
            entry = ExposureLedgerEntry(
                scope_type=scope_type,
                scope_key=scope_key,
                valuation_currency="USD",
                filled_exposure_notional_usd=max(0.0, filled_notional_usd),
                pending_entry_exposure_notional_usd=max(0.0, pending_entry_notional_usd),
                pending_exit_notional_usd=max(0.0, pending_exit_notional_usd),
                committed_exposure_notional_usd=committed,
                cap_notional_usd=max(0.0, cap_notional_usd),
                blocked=blocked,
                reason_code=default_reason if blocked else "",
                wallet_scope=wallet_scope,
                condition_scope=condition_scope,
                updated_ts=int(now_ts),
            )
            return entry.to_payload()

        wallet_entry = _make_entry(
            scope_type="wallet",
            scope_key=wallet_scope,
            cap_notional_usd=wallet_cap,
            filled_notional_usd=wallet_filled,
            pending_entry_notional_usd=wallet_pending_entry,
            pending_exit_notional_usd=wallet_pending_exit,
            default_reason=REASON_WALLET_EXPOSURE_CAP_REACHED,
        )
        ledger[(str(wallet_entry["scope_type"]), str(wallet_entry["scope_key"]))] = wallet_entry

        portfolio_entry = _make_entry(
            scope_type="portfolio",
            scope_key=portfolio_scope,
            cap_notional_usd=portfolio_cap,
            filled_notional_usd=portfolio_filled,
            pending_entry_notional_usd=portfolio_pending_entry,
            pending_exit_notional_usd=portfolio_pending_exit,
            default_reason=REASON_PORTFOLIO_EXPOSURE_CAP_REACHED,
        )
        ledger[(str(portfolio_entry["scope_type"]), str(portfolio_entry["scope_key"]))] = portfolio_entry

        condition_keys = set(condition_filled.keys()) | set(condition_pending_entry.keys()) | set(condition_pending_exit.keys())
        for condition_key in sorted(condition_keys):
            entry = _make_entry(
                scope_type="condition",
                scope_key=condition_key,
                cap_notional_usd=condition_cap,
                filled_notional_usd=condition_filled.get(condition_key, 0.0),
                pending_entry_notional_usd=condition_pending_entry.get(condition_key, 0.0),
                pending_exit_notional_usd=condition_pending_exit.get(condition_key, 0.0),
                default_reason=REASON_CONDITION_EXPOSURE_CAP_REACHED,
                condition_scope=condition_key,
            )
            ledger[(str(entry["scope_type"]), str(entry["scope_key"]))] = entry

        return ledger

    def _entry_lookup(self, *, scope_type: str, scope_key: str) -> dict[str, object]:
        return dict(self._exposure_ledger.get((scope_type, scope_key)) or {})

    def _refresh_risk_breaker_state(self, *, now_ts: int) -> None:
        try:
            baseline = RiskBreakerState.from_payload(
                self._risk_breaker_state or default_risk_breaker_state(day_key=self._risk_day_key(now_ts), now_ts=now_ts)
            )
            if str(baseline.valuation_currency or "USD").upper() != "USD":
                raise ValueError("risk_breaker_state_invalid_currency")
            if baseline.day_key and not re.match(r"^\d{4}-\d{2}-\d{2}$", str(baseline.day_key)):
                raise ValueError("risk_breaker_state_invalid_day_key")
            day_key = self._risk_day_key(now_ts)
            reset_next_day = bool(getattr(self.settings, "risk_breaker_reset_next_day", True))
            manual_persists = bool(getattr(self.settings, "risk_breaker_manual_lock_persists_across_day", True))
            if not baseline.day_key:
                baseline.day_key = day_key
            if day_key != baseline.day_key and reset_next_day:
                baseline.day_key = day_key
                baseline.equity_peak_usd = 0.0
                baseline.intraday_drawdown_pct = 0.0
                baseline.intraday_drawdown_blocked = False
                if not (baseline.manual_lock and manual_persists):
                    baseline.loss_streak_count = 0
                    baseline.loss_streak_blocked = False
                    baseline.manual_required = False
                    baseline.manual_lock = False

            equity_now = max(0.0, float(self.state.equity_usd or 0.0))
            if equity_now <= 0.0:
                if baseline.equity_now_usd > 0.0:
                    equity_now = float(baseline.equity_now_usd)
                else:
                    equity_now = max(0.0, float(self.settings.bankroll_usd))
            if baseline.equity_peak_usd <= 0.0:
                baseline.equity_peak_usd = max(equity_now, float(self.settings.bankroll_usd or 0.0), 1.0)
            if equity_now > baseline.equity_peak_usd:
                baseline.equity_peak_usd = equity_now
            baseline.equity_now_usd = equity_now
            drawdown_limit = max(0.0, float(getattr(self.settings, "intraday_drawdown_breaker_pct", 0.0) or 0.0))
            baseline.intraday_drawdown_limit_pct = drawdown_limit
            if baseline.equity_peak_usd > 0.0:
                baseline.intraday_drawdown_pct = max(
                    0.0, (baseline.equity_peak_usd - baseline.equity_now_usd) / baseline.equity_peak_usd
                )
            else:
                baseline.intraday_drawdown_pct = 0.0
            has_loss_evidence = float(self.state.effective_daily_realized_pnl or 0.0) < -1e-9
            baseline.intraday_drawdown_blocked = (
                drawdown_limit > 0.0
                and has_loss_evidence
                and baseline.intraday_drawdown_pct >= drawdown_limit - 1e-12
            )
            baseline.loss_streak_limit = max(1, int(getattr(self.settings, "loss_streak_breaker_limit", 1) or 1))
            baseline.loss_streak_blocked = baseline.loss_streak_count >= baseline.loss_streak_limit
            reasons: list[str] = []
            if baseline.loss_streak_blocked:
                reasons.append(REASON_LOSS_STREAK_BREAKER_ACTIVE)
            if baseline.intraday_drawdown_blocked:
                reasons.append(REASON_INTRADAY_DRAWDOWN_BREAKER_ACTIVE)
            if baseline.manual_lock:
                reasons.append("risk_breaker_manual_lock")
            baseline.reason_codes = tuple(reasons)
            baseline.manual_required = bool(baseline.manual_lock)
            baseline.opening_allowed = not reasons
            baseline.valuation_currency = "USD"
            baseline.timezone = str(getattr(self.settings, "risk_breaker_timezone", "UTC") or "UTC").strip() or "UTC"
            baseline.updated_ts = int(now_ts)
            self._risk_breaker_state = baseline.to_payload()
        except Exception:
            self._risk_breaker_status = "invalid"
            fallback = RiskBreakerState.from_payload(default_risk_breaker_state(day_key=self._risk_day_key(now_ts), now_ts=now_ts))
            fallback.opening_allowed = False
            fallback.manual_required = True
            fallback.manual_lock = True
            fallback.reason_codes = (RISK_REASON_BREAKER_STATE_INVALID,)
            fallback.updated_ts = int(now_ts)
            self._risk_breaker_state = fallback.to_payload()

    def _refresh_risk_state(self) -> None:
        now_ts = int(time.time())
        self.state.open_positions = len(self.positions_book)
        self.state.tracked_notional_usd = self._tracked_notional_usd()
        self.state.pending_entry_notional_usd = self._pending_entry_notional_usd()
        self.state.pending_exit_notional_usd = self._pending_exit_notional_usd()
        self.state.pending_entry_orders = self._pending_entry_orders()
        self.state.trading_mode = str(self.trading_mode or "NORMAL").upper()
        self.state.trading_mode_reasons = tuple(
            str(reason or "") for reason in self.trading_mode_reasons if str(reason or "").strip()
        )
        self.state.account_state_status = str(self.account_state_status or "unknown")
        self.state.reconciliation_status = str(self.reconciliation_status or "unknown")
        self.state.persistence_status = str(self.persistence_status or "ok")
        self.state.valuation_currency = "USD"

        try:
            self._exposure_ledger = self._rebuild_exposure_ledger(now_ts=now_ts)
            self._risk_ledger_status = "ok"
        except Exception:
            self._risk_ledger_status = "fault"
            self._exposure_ledger = {}
        self._risk_breaker_status = "ok"
        self._refresh_risk_breaker_state(now_ts=now_ts)
        self.state.risk_ledger_status = str(self._risk_ledger_status or "fault")
        self.state.risk_breaker_status = str(self._risk_breaker_status or "invalid")

        wallet_entry = self._entry_lookup(scope_type="wallet", scope_key=self._risk_wallet_scope())
        portfolio_entry = self._entry_lookup(scope_type="portfolio", scope_key=self._risk_portfolio_scope())
        self.state.wallet_exposure_committed_usd = float(wallet_entry.get("committed_exposure_notional_usd") or 0.0)
        self.state.wallet_exposure_cap_usd = float(wallet_entry.get("cap_notional_usd") or 0.0)
        self.state.portfolio_exposure_committed_usd = float(portfolio_entry.get("committed_exposure_notional_usd") or 0.0)
        self.state.portfolio_exposure_cap_usd = float(portfolio_entry.get("cap_notional_usd") or 0.0)

        breaker = RiskBreakerState.from_payload(self._risk_breaker_state)
        self.state.loss_streak_count = int(breaker.loss_streak_count or 0)
        self.state.loss_streak_limit = int(breaker.loss_streak_limit or 0)
        self.state.loss_streak_blocked = bool(breaker.loss_streak_blocked)
        self.state.intraday_drawdown_pct = float(breaker.intraday_drawdown_pct or 0.0)
        self.state.intraday_drawdown_limit_pct = float(breaker.intraday_drawdown_limit_pct or 0.0)
        self.state.intraday_drawdown_blocked = bool(breaker.intraday_drawdown_blocked)
        self.state.risk_breaker_opening_allowed = bool(breaker.opening_allowed)
        self.state.risk_breaker_reason_codes = tuple(str(item) for item in breaker.reason_codes if str(item).strip())
        self.state.risk_day_key = str(breaker.day_key or "")
        self.state.condition_exposure_key = ""
        self.state.condition_exposure_committed_usd = 0.0
        self.state.condition_exposure_cap_usd = (
            max(0.0, float(self.settings.bankroll_usd) * float(self.settings.max_condition_exposure_pct))
            if bool(getattr(self.settings, "portfolio_netting_enabled", True))
            else 0.0
        )

    def _hydrate_signal_condition_exposure(self, signal: Signal) -> None:
        condition_key, _ = self._signal_condition_exposure_key(signal)
        condition_entry = self._entry_lookup(scope_type="condition", scope_key=condition_key)
        self.state.condition_exposure_key = condition_key
        self.state.condition_exposure_committed_usd = float(condition_entry.get("committed_exposure_notional_usd") or 0.0)
        self.state.condition_exposure_cap_usd = float(
            condition_entry.get("cap_notional_usd")
            or (
                max(0.0, float(self.settings.bankroll_usd) * float(self.settings.max_condition_exposure_pct))
                if bool(getattr(self.settings, "portfolio_netting_enabled", True))
                else 0.0
            )
        )

    def _available_notional_usd(self) -> float:
        pending_entry_notional = self._pending_entry_notional_usd()
        bankroll_remaining = max(
            0.0,
            self.settings.bankroll_usd - (self._tracked_notional_usd() + pending_entry_notional),
        )
        cash_balance = max(0.0, float(self.state.cash_balance_usd or 0.0))
        cash_snapshot_known = cash_balance > 0.0 or int(self.state.account_snapshot_ts or 0) > 0
        if cash_snapshot_known:
            cash_available = max(0.0, cash_balance - pending_entry_notional)
            return min(bankroll_remaining, cash_available)
        return bankroll_remaining

    @staticmethod
    def _actionable_notional_floor_usd() -> float:
        # Keep dust out, but let the bot keep trading or exiting when free cash drops below the old 5 USD floor.
        return 1.0

    @staticmethod
    def _position_dust_floor_usd() -> float:
        return 0.01

    @staticmethod
    def _condition_exposure_key(
        *,
        condition_id: object = "",
        market_slug: object = "",
        token_id: object = "",
    ) -> tuple[str, str]:
        normalized_condition = str(condition_id or "").strip().lower()
        if normalized_condition:
            return (f"condition:{normalized_condition}", "condition_id")
        normalized_slug = str(market_slug or "").strip().lower()
        if normalized_slug:
            return (f"market:{normalized_slug}", "market_slug")
        normalized_token = str(token_id or "").strip().lower()
        if normalized_token:
            return (f"token:{normalized_token}", "token_id")
        return ("", "none")

    def _signal_condition_exposure_key(self, signal: Signal) -> tuple[str, str]:
        return self._condition_exposure_key(
            condition_id=signal.condition_id,
            market_slug=signal.market_slug,
            token_id=signal.token_id,
        )

    def _position_condition_exposure_key(self, position: Mapping[str, object]) -> tuple[str, str]:
        return self._condition_exposure_key(
            condition_id=position.get("condition_id"),
            market_slug=position.get("market_slug"),
            token_id=position.get("token_id"),
        )

    def _pending_order_condition_exposure_key(self, order: Mapping[str, object]) -> tuple[str, str]:
        return self._condition_exposure_key(
            condition_id=order.get("condition_id"),
            market_slug=order.get("market_slug"),
            token_id=order.get("token_id"),
        )

    def _condition_exposure_notional_usd(self, exposure_key: str) -> float:
        if not exposure_key:
            return 0.0

        total = 0.0
        for position in self.positions_book.values():
            position_key, _ = self._position_condition_exposure_key(position)
            if position_key != exposure_key:
                continue
            total += max(0.0, float(position.get("notional") or 0.0))
        for order in self.pending_orders.values():
            if str(order.get("side") or "").upper() != "BUY":
                continue
            order_key, _ = self._pending_order_condition_exposure_key(order)
            if order_key != exposure_key:
                continue
            total += max(0.0, float(order.get("requested_notional") or 0.0))
        return total

    def _order_cache_cleanup(self, now: float) -> None:
        expired = [key for key, expire_ts in self._recent_order_keys.items() if expire_ts <= now]
        for key in expired:
            self._recent_order_keys.pop(key, None)

    def _build_order_key(self, signal: Signal, notional_usd: float) -> str:
        notional_bucket = int(round(self._safe_float(notional_usd) * 100))
        return "|".join(
            (
                str(signal.token_id or "").strip().lower(),
                str(signal.side or ""),
                str(signal.market_slug or "").strip().lower(),
                str(notional_bucket),
            )
        )

    def _is_order_duplicate(self, signal: Signal, notional_usd: float) -> bool:
        if self.settings.order_dedup_ttl_seconds <= 0:
            return False
        now = time.time()
        self._order_cache_cleanup(now)
        key = self._build_order_key(signal, notional_usd)
        ttl = max(1, int(self.settings.order_dedup_ttl_seconds))
        expire_ts = self._recent_order_keys.get(key)
        if expire_ts is not None and expire_ts > now:
            return True
        self._recent_order_keys[key] = now + ttl
        return False

    def _restore_recent_order_keys(self, payload: object) -> None:
        if not isinstance(payload, dict):
            return
        now = time.time()
        restored: dict[str, float] = {}
        for raw_key, raw_expire_ts in payload.items():
            key = str(raw_key or "").strip()
            if not key:
                continue
            expire_ts = self._safe_float(raw_expire_ts)
            if expire_ts <= now:
                continue
            restored[key] = expire_ts
        if restored:
            self._recent_order_keys = restored

    def _claim_idempotency(self, signal: Signal, notional_usd: float) -> tuple[bool, str | None]:
        if not bool(getattr(self.settings, "idempotency_enabled", False)):
            return (True, None)
        if self._state_store is None:
            return (True, None)
        uuid = self._strategy_order_uuid(signal, notional_usd)
        claimed = False
        try:
            claimed = self._state_store.register_idempotency(
                strategy_order_uuid=uuid,
                wallet=str(signal.wallet or ""),
                condition_id=str(signal.condition_id or ""),
                token_id=str(signal.token_id or ""),
                side=str(signal.side or ""),
                notional=float(notional_usd),
            )
        except Exception as exc:
            self.log.warning("Idempotency claim failed uuid=%s err=%s", uuid, exc)
            return (False, uuid)
        return (claimed, uuid)

    def _ack_unknown_window_seconds(self) -> int:
        return max(30, int(getattr(self.settings, "ack_unknown_recovery_window_seconds", 300) or 300))

    def _ack_unknown_max_probes(self) -> int:
        return max(1, int(getattr(self.settings, "ack_unknown_max_probes", 3) or 3))

    def _intent_strategy_name(self) -> str:
        configured = str(getattr(self.settings, "strategy_name", "") or "").strip().lower()
        if configured:
            return configured
        strategy_obj = getattr(self, "strategy", None)
        strategy_name = str(type(strategy_obj).__name__ if strategy_obj is not None else "strategy").strip().lower()
        return strategy_name or "strategy"

    def _intent_signal_source(self, signal: Signal) -> str:
        from_signal = str(getattr(signal, "signal_source", "") or "").strip().lower()
        if from_signal:
            return from_signal
        configured = str(getattr(self.settings, "wallet_signal_source", "") or "").strip().lower()
        return configured or "unknown"

    @staticmethod
    def _signal_timestamp_epoch(signal: Signal) -> int:
        try:
            ts = getattr(signal, "timestamp", None)
            if ts is None:
                return int(time.time())
            return int(ts.timestamp())  # datetime
        except Exception:
            return int(time.time())

    def _intent_signal_fingerprint(self, signal: Signal, notional_usd: float) -> str:
        notional_cents = int(round(self._safe_float(notional_usd) * 100.0))
        basis = "|".join(
            [
                str(signal.wallet or "").strip().lower(),
                str(signal.condition_id or "").strip().lower(),
                str(signal.token_id or "").strip().lower(),
                str(signal.side or "").strip().upper(),
                str(signal.market_slug or "").strip().lower(),
                str(signal.outcome or "").strip().lower(),
                str(notional_cents),
            ]
        )
        digest = hashlib.sha256(basis.encode("utf-8")).hexdigest()
        return digest[:32]

    def _build_intent_identity(self, signal: Signal, notional_usd: float) -> dict[str, object]:
        strategy_name = self._intent_strategy_name()
        signal_source = self._intent_signal_source(signal)
        signal_fingerprint = self._intent_signal_fingerprint(signal, notional_usd)
        notional_cents = int(round(self._safe_float(notional_usd) * 100.0))
        bucket_seconds = max(1, int(getattr(self.settings, "idempotency_signal_bucket_seconds", 300) or 300))
        signal_bucket = self._signal_timestamp_epoch(signal) // bucket_seconds
        idempotency_key = build_intent_idempotency_key(
            strategy_name=strategy_name,
            signal_source=signal_source,
            signal_fingerprint=signal_fingerprint,
            token_id=str(signal.token_id or ""),
            side=str(signal.side or ""),
            salt=str(getattr(self.settings, "intent_idempotency_salt", "") or ""),
            extra={
                "wallet": str(signal.wallet or "").strip().lower(),
                "condition_id": str(signal.condition_id or "").strip().lower(),
                "notional_cents": notional_cents,
                "signal_bucket": int(signal_bucket),
            },
        )
        strategy_order_uuid = f"so-{idempotency_key[:24]}"
        intent_id = f"intent-{idempotency_key[:24]}"
        return {
            "strategy_name": strategy_name,
            "signal_source": signal_source,
            "signal_fingerprint": signal_fingerprint,
            "signal_bucket": int(signal_bucket),
            "idempotency_key": idempotency_key,
            "strategy_order_uuid": strategy_order_uuid,
            "intent_id": intent_id,
            "notional_cents": notional_cents,
        }

    def _set_intent_status(
        self,
        *,
        strategy_order_uuid: str,
        idempotency_key: str,
        status: str,
        broker_order_id: str | None = None,
        payload_updates: dict[str, object] | None = None,
        recovery_reason: str | None = None,
        expected_from_statuses: list[str] | tuple[str, ...] | None = None,
    ) -> tuple[bool, dict[str, object]]:
        normalized = normalize_intent_status(status)
        if not strategy_order_uuid and not idempotency_key:
            return (False, {})
        if normalized:
            self._intent_local_status[strategy_order_uuid] = normalized
        if self._state_store is None:
            return (False, {})
        update_status = getattr(self._state_store, "update_intent_status", None)
        if not callable(update_status):
            return (False, {})
        try:
            ok, updated_intent = update_status(
                strategy_order_uuid=strategy_order_uuid,
                idempotency_key=idempotency_key,
                status=normalized,
                broker_order_id=broker_order_id,
                payload_updates=payload_updates,
                recovery_reason=recovery_reason,
                expected_from_statuses=expected_from_statuses,
            )
        except Exception as exc:
            self.log.warning("update_intent_status failed uuid=%s key=%s err=%s", strategy_order_uuid, idempotency_key, exc)
            return (False, {})
        if not ok or updated_intent is None:
            return (False, {})
        if hasattr(updated_intent, "to_row"):
            row = dict(updated_intent.to_row())
        elif isinstance(updated_intent, Mapping):
            row = dict(updated_intent)
        else:
            row = {}
        return (ok, row)

    def _record_ack_unknown_probe(
        self,
        strategy_order_uuid: str,
        *,
        current_count: int = 0,
        current_first_ts: int = 0,
    ) -> dict[str, object]:
        now = time.time()
        state = dict(self._ack_unknown_tracker.get(strategy_order_uuid) or {})
        first_ts = float(state.get("first_ts") or current_first_ts or now)
        count = int(max(int(state.get("count") or 0), int(current_count or 0)))
        if now - first_ts > self._ack_unknown_window_seconds():
            first_ts = now
            count = 0
        count += 1
        manual_required = count > self._ack_unknown_max_probes()
        state.update({
            "first_ts": first_ts,
            "count": count,
            "manual_required": manual_required,
        })
        self._ack_unknown_tracker[strategy_order_uuid] = state
        return state

    @staticmethod
    def _normalize_probe_confidence(value: object) -> str:
        normalized = str(value or "").strip().lower()
        if normalized in {"strong", "weak", "none"}:
            return normalized
        return "none"

    @staticmethod
    def _normalize_probe_basis(value: object) -> str:
        normalized = str(value or "").strip().lower()
        if normalized in {
            "broker_order_id",
            "unique_broker_record_match",
            "ambiguous_broker_record_match",
            "submit_digest_only",
            "no_match",
        }:
            return normalized
        return "no_match"

    @staticmethod
    def _intent_status_from_lifecycle_status(status: object) -> str:
        normalized = str(status or "").strip().lower()
        if normalized == "partially_filled":
            return INTENT_STATUS_PARTIAL
        if normalized == "filled":
            return INTENT_STATUS_FILLED
        if normalized in {"canceled", "failed", "rejected", "unmatched"}:
            return normalized
        return INTENT_STATUS_ACKED_PENDING

    def _submit_unknown_expected_size(self, payload: Mapping[str, object]) -> float:
        submitted_size = self._safe_float(payload.get("submitted_size"))
        if submitted_size > 0.0:
            return submitted_size
        submitted_price = self._safe_float(payload.get("submitted_price") or payload.get("requested_price"))
        requested_notional = self._safe_float(payload.get("requested_notional"))
        if submitted_price > 0.0 and requested_notional > 0.0:
            return max(0.0, requested_notional / submitted_price)
        return 0.0

    @staticmethod
    def _normalize_probe_price(price: float, tick_size: float) -> float:
        price = max(0.0, float(price or 0.0))
        tick_size = max(0.0, float(tick_size or 0.0))
        if price <= 0.0 or tick_size <= 0.0:
            return 0.0
        return round(round(price / tick_size) * tick_size, 8)

    @staticmethod
    def _normalize_probe_size(size: float) -> float:
        return round(max(0.0, float(size or 0.0)), _SUBMIT_UNKNOWN_SIZE_PRECISION)

    def _build_submit_unknown_payload_updates(
        self,
        *,
        payload: Mapping[str, object] | None = None,
        result: ExecutionResult | None = None,
        ack_state: Mapping[str, object] | None = None,
        probe_confidence: str = "none",
        probe_basis: str = "no_match",
        manual_required_reason: str = "",
    ) -> dict[str, object]:
        current_payload = dict(payload or {})
        result_metadata = dict(getattr(result, "metadata", {}) or {})
        submit_digest = str(result_metadata.get("submit_digest") or current_payload.get("submit_digest") or "").strip()
        submit_digest_version = str(
            result_metadata.get("submit_digest_version") or current_payload.get("submit_digest_version") or ""
        ).strip()
        submitted_price = self._safe_float(
            result_metadata.get("submitted_price")
            or current_payload.get("submitted_price")
            or getattr(result, "requested_price", 0.0),
            0.0,
        )
        submitted_size = self._safe_float(
            result_metadata.get("submitted_size") or current_payload.get("submitted_size"),
            0.0,
        )
        if submitted_size <= 0.0 and submitted_price > 0.0:
            requested_notional = self._safe_float(
                current_payload.get("requested_notional") or getattr(result, "requested_notional", 0.0),
                0.0,
            )
            if requested_notional > 0.0:
                submitted_size = max(0.0, requested_notional / submitted_price)

        tick_size = self._safe_float(
            result_metadata.get("tick_size") or current_payload.get("tick_size"),
            0.0,
        )
        first_seen_ts = int(
            (ack_state or {}).get("first_ts")
            or current_payload.get("unknown_submit_first_seen_ts")
            or current_payload.get("ack_unknown_first_ts")
            or 0
        )
        probe_count = int(
            (ack_state or {}).get("count")
            or current_payload.get("unknown_submit_probe_count")
            or current_payload.get("ack_unknown_count")
            or 0
        )
        updates: dict[str, object] = {
            "pending_class": "submit_unknown",
            "submit_digest": submit_digest,
            "submit_digest_version": submit_digest_version,
            "submitted_price": float(submitted_price),
            "submitted_size": float(submitted_size),
            "tick_size": float(tick_size),
            "unknown_submit_first_seen_ts": int(first_seen_ts),
            "unknown_submit_probe_count": int(probe_count),
            "ack_unknown_first_ts": int(first_seen_ts),
            "ack_unknown_count": int(probe_count),
            "probe_confidence": self._normalize_probe_confidence(probe_confidence),
            "probe_basis": self._normalize_probe_basis(probe_basis),
            "manual_required_reason": str(manual_required_reason or current_payload.get("manual_required_reason") or ""),
        }
        return updates

    def _classify_unknown_submit_probe(
        self,
        *,
        signal: Signal,
        intent_record: Mapping[str, object],
    ) -> dict[str, object]:
        payload = dict(intent_record.get("payload") or {})
        broker_order_id = str(intent_record.get("broker_order_id") or payload.get("order_id") or "").strip()
        submit_digest = str(payload.get("submit_digest") or "").strip()
        first_seen_ts = int(payload.get("unknown_submit_first_seen_ts") or payload.get("ack_unknown_first_ts") or 0)
        tick_size = self._safe_float(payload.get("tick_size"), 0.0)
        submitted_price = self._safe_float(payload.get("submitted_price") or payload.get("requested_price"), 0.0)
        submitted_size = self._submit_unknown_expected_size(payload)
        signal_token = str(signal.token_id or payload.get("token_id") or "").strip().lower()
        signal_side = str(signal.side or payload.get("side") or "").strip().upper()
        exact_candidates: dict[str, dict[str, object]] = {}

        if broker_order_id:
            get_order_status = getattr(self.broker, "get_order_status", None)
            if callable(get_order_status):
                try:
                    snapshot = get_order_status(broker_order_id)
                except Exception as exc:
                    self.log.warning("Unknown-submit status probe failed order_id=%s err=%s", broker_order_id, exc)
                    snapshot = None
                if isinstance(snapshot, OrderStatusSnapshot):
                    return {
                        "confidence": "strong",
                        "basis": "broker_order_id",
                        "broker_order_id": broker_order_id,
                        "intent_status": self._intent_status_from_lifecycle_status(snapshot.lifecycle_status),
                        "broker_status": str(snapshot.lifecycle_status or snapshot.normalized_status or ""),
                        "manual_required_reason": "",
                    }

        list_open_orders = getattr(self.broker, "list_open_orders", None)
        broker_open_orders = []
        if callable(list_open_orders):
            try:
                broker_open_orders = list(list_open_orders() or [])
            except Exception as exc:
                self.log.warning("Unknown-submit open-order probe failed err=%s", exc)
                broker_open_orders = []
        for snapshot in broker_open_orders:
            if not isinstance(snapshot, OpenOrderSnapshot):
                continue
            snapshot_order_id = str(snapshot.order_id or "").strip()
            snapshot_token = str(snapshot.token_id or "").strip().lower()
            snapshot_side = str(snapshot.side or "").strip().upper()
            if broker_order_id and snapshot_order_id and snapshot_order_id == broker_order_id:
                return {
                    "confidence": "strong",
                    "basis": "broker_order_id",
                    "broker_order_id": snapshot_order_id,
                    "intent_status": self._intent_status_from_lifecycle_status(snapshot.lifecycle_status),
                    "broker_status": str(snapshot.lifecycle_status or snapshot.normalized_status or ""),
                    "manual_required_reason": "",
                }
            if snapshot_token != signal_token or snapshot_side != signal_side:
                continue
            if tick_size <= 0.0 or submitted_price <= 0.0 or submitted_size <= 0.0:
                continue
            if first_seen_ts <= 0 or int(snapshot.created_ts or 0) <= 0:
                continue
            if abs(int(snapshot.created_ts) - first_seen_ts) > _SUBMIT_UNKNOWN_PROBE_WINDOW_SECONDS:
                continue
            candidate_size = self._safe_float(snapshot.original_size)
            if candidate_size <= 0.0:
                candidate_size = self._safe_float(snapshot.matched_size) + self._safe_float(snapshot.remaining_size)
            if candidate_size <= 0.0:
                continue
            if (
                self._normalize_probe_price(self._safe_float(snapshot.price), tick_size)
                == self._normalize_probe_price(submitted_price, tick_size)
                and self._normalize_probe_size(candidate_size) == self._normalize_probe_size(submitted_size)
            ):
                candidate_key = snapshot_order_id or (
                    f"open:{snapshot_token}:{snapshot_side}:{self._normalize_probe_price(self._safe_float(snapshot.price), tick_size)}:"
                    f"{self._normalize_probe_size(candidate_size)}:{int(snapshot.created_ts or 0)}"
                )
                exact_candidates[candidate_key] = {
                    "order_id": snapshot_order_id,
                    "intent_status": self._intent_status_from_lifecycle_status(snapshot.lifecycle_status),
                    "broker_status": str(snapshot.lifecycle_status or snapshot.normalized_status or ""),
                }

        list_recent_fills = getattr(self.broker, "list_recent_fills", None)
        broker_fills = []
        if callable(list_recent_fills):
            try:
                broker_fills = list(list_recent_fills(limit=400) or [])
            except Exception as exc:
                self.log.warning("Unknown-submit fill probe failed err=%s", exc)
                broker_fills = []
        for fill in broker_fills:
            if not isinstance(fill, OrderFillSnapshot):
                continue
            fill_order_id = str(fill.order_id or "").strip()
            fill_token = str(fill.token_id or "").strip().lower()
            fill_side = str(fill.side or "").strip().upper()
            if broker_order_id and fill_order_id and fill_order_id == broker_order_id:
                return {
                    "confidence": "strong",
                    "basis": "broker_order_id",
                    "broker_order_id": fill_order_id,
                    "intent_status": INTENT_STATUS_FILLED,
                    "broker_status": "filled",
                    "manual_required_reason": "",
                }
            if fill_token != signal_token or fill_side != signal_side:
                continue
            if tick_size <= 0.0 or submitted_price <= 0.0 or submitted_size <= 0.0:
                continue
            if first_seen_ts <= 0 or int(fill.timestamp or 0) <= 0:
                continue
            if abs(int(fill.timestamp) - first_seen_ts) > _SUBMIT_UNKNOWN_PROBE_WINDOW_SECONDS:
                continue
            if (
                self._normalize_probe_price(self._safe_float(fill.price), tick_size)
                == self._normalize_probe_price(submitted_price, tick_size)
                and self._normalize_probe_size(self._safe_float(fill.size)) == self._normalize_probe_size(submitted_size)
            ):
                candidate_key = fill_order_id or (
                    f"fill:{fill_token}:{fill_side}:{self._normalize_probe_price(self._safe_float(fill.price), tick_size)}:"
                    f"{self._normalize_probe_size(self._safe_float(fill.size))}:{int(fill.timestamp or 0)}"
                )
                exact_candidates[candidate_key] = {
                    "order_id": fill_order_id,
                    "intent_status": INTENT_STATUS_FILLED,
                    "broker_status": "filled",
                }

        if len(exact_candidates) == 1:
            candidate = next(iter(exact_candidates.values()))
            return {
                "confidence": "strong",
                "basis": "unique_broker_record_match",
                "broker_order_id": str(candidate.get("order_id") or ""),
                "intent_status": str(candidate.get("intent_status") or INTENT_STATUS_ACKED_PENDING),
                "broker_status": str(candidate.get("broker_status") or "open"),
                "manual_required_reason": "",
            }
        if len(exact_candidates) > 1:
            return {
                "confidence": "weak",
                "basis": "ambiguous_broker_record_match",
                "broker_order_id": "",
                "intent_status": INTENT_STATUS_ACK_UNKNOWN,
                "broker_status": "",
                "manual_required_reason": "submit_unknown_ambiguous_match",
            }
        if submit_digest:
            return {
                "confidence": "weak",
                "basis": "submit_digest_only",
                "broker_order_id": "",
                "intent_status": INTENT_STATUS_ACK_UNKNOWN,
                "broker_status": "",
                "manual_required_reason": "",
            }
        return {
            "confidence": "none",
            "basis": "no_match",
            "broker_order_id": "",
            "intent_status": INTENT_STATUS_ACK_UNKNOWN,
            "broker_status": "",
            "manual_required_reason": "submit_unknown_no_anchor",
        }

    def _find_pending_order_by_intent(
        self,
        *,
        strategy_order_uuid: str = "",
        idempotency_key: str = "",
    ) -> dict[str, object] | None:
        normalized_uuid = str(strategy_order_uuid or "").strip()
        normalized_key = str(idempotency_key or "").strip()
        for order in self.pending_orders.values():
            if normalized_uuid and str(order.get("strategy_order_uuid") or "").strip() == normalized_uuid:
                return order
            if normalized_key and str(order.get("idempotency_key") or "").strip() == normalized_key:
                return order
        return None

    def _apply_submit_unknown_contract(
        self,
        order: dict[str, object],
        *,
        now: int,
        probe_confidence: str,
        probe_basis: str,
        manual_required_reason: str,
        ack_state: Mapping[str, object] | None = None,
        clear_ambiguity: bool = False,
        broker_order_id: str = "",
        broker_status: str = "",
        payload: Mapping[str, object] | None = None,
    ) -> None:
        current_payload = dict(payload or {})
        order["pending_class"] = "normal" if clear_ambiguity else "submit_unknown"
        order["probe_confidence"] = self._normalize_probe_confidence(
            current_payload.get("probe_confidence") or probe_confidence
        )
        order["probe_basis"] = self._normalize_probe_basis(current_payload.get("probe_basis") or probe_basis)
        order["submit_digest"] = str(current_payload.get("submit_digest") or order.get("submit_digest") or "")
        order["submit_digest_version"] = str(
            current_payload.get("submit_digest_version") or order.get("submit_digest_version") or ""
        )
        order["unknown_submit_first_seen_ts"] = int(
            current_payload.get("unknown_submit_first_seen_ts")
            or current_payload.get("ack_unknown_first_ts")
            or order.get("unknown_submit_first_seen_ts")
            or 0
        )
        order["unknown_submit_probe_count"] = int(
            current_payload.get("unknown_submit_probe_count")
            or current_payload.get("ack_unknown_count")
            or order.get("unknown_submit_probe_count")
            or 0
        )
        order["manual_required_reason"] = str(
            manual_required_reason or current_payload.get("manual_required_reason") or order.get("manual_required_reason") or ""
        )
        if ack_state:
            order["ack_unknown_count"] = int(ack_state.get("count") or 0)
            order["ack_unknown_first_ts"] = int(ack_state.get("first_ts") or 0)
            order["unknown_submit_probe_count"] = int(ack_state.get("count") or 0)
            order["unknown_submit_first_seen_ts"] = int(ack_state.get("first_ts") or 0)
        if broker_order_id:
            order["order_id"] = broker_order_id
        if broker_status:
            order["broker_status"] = broker_status
        if clear_ambiguity:
            order["reconcile_ambiguous_ts"] = 0
            order["reconcile_ambiguous_reason"] = ""
            order["manual_required_reason"] = ""
        else:
            order["reconcile_ambiguous_ts"] = max(now, int(order.get("reconcile_ambiguous_ts") or 0))
            order["reconcile_ambiguous_reason"] = str(
                order["manual_required_reason"] or order.get("reconcile_ambiguous_reason") or "submit_unknown"
            )
        order["recovery_status"] = (
            "manual_required"
            if order["manual_required_reason"]
            else ("ack_unknown" if order["probe_confidence"] != "strong" else "confirmed")
        )

    def _normalize_claim_result(self, raw_result: object, default_intent: dict[str, object]) -> tuple[str, dict[str, object]]:
        claim_status = STORAGE_ERROR
        intent = dict(default_intent)
        if isinstance(raw_result, tuple) and len(raw_result) >= 2:
            claim_status = str(raw_result[0] or STORAGE_ERROR)
            if hasattr(raw_result[1], "to_row"):
                intent.update(dict(raw_result[1].to_row()))
            elif isinstance(raw_result[1], Mapping):
                intent.update(dict(raw_result[1]))
        elif isinstance(raw_result, Mapping):
            claim_status = str(raw_result.get("claim_status") or raw_result.get("status") or STORAGE_ERROR)
            intent_payload = raw_result.get("intent") or raw_result
            if isinstance(intent_payload, Mapping):
                intent.update(dict(intent_payload))

        payload = dict(intent.get("payload") or {})
        intent_status = normalize_intent_status(intent.get("status") or INTENT_STATUS_NEW)
        ack_count = int(payload.get("ack_unknown_count") or intent.get("ack_unknown_count") or intent.get("ack_count") or 0)
        ack_first_ts = int(payload.get("ack_unknown_first_ts") or intent.get("ack_unknown_first_ts") or intent.get("ack_first_ts") or 0)
        now_ts = int(time.time())
        within_window = ack_first_ts == 0 or (now_ts - ack_first_ts) <= self._ack_unknown_window_seconds()
        if ack_count > self._ack_unknown_max_probes() and within_window:
            intent_status = INTENT_STATUS_MANUAL_REQUIRED
        tracker = self._ack_unknown_tracker.get(str(intent.get("strategy_order_uuid") or "")) or {}
        if bool(tracker.get("manual_required")):
            intent_status = INTENT_STATUS_MANUAL_REQUIRED
        intent["status"] = intent_status or INTENT_STATUS_NEW
        for field_name in (
            "ack_unknown_count",
            "ack_unknown_first_ts",
            "submit_digest",
            "submit_digest_version",
            "submitted_price",
            "submitted_size",
            "tick_size",
            "unknown_submit_first_seen_ts",
            "unknown_submit_probe_count",
            "probe_confidence",
            "probe_basis",
            "manual_required_reason",
        ):
            if field_name in payload:
                intent[field_name] = payload.get(field_name)
        if ack_count:
            intent["ack_unknown_count"] = ack_count
        if ack_first_ts:
            intent["ack_unknown_first_ts"] = ack_first_ts
        return (claim_status, intent)

    def _claim_or_load_intent(
        self,
        *,
        signal: Signal,
        notional_usd: float,
        identity: Mapping[str, object],
    ) -> tuple[str, dict[str, object]]:
        strategy_order_uuid = str(identity.get("strategy_order_uuid") or "")
        idempotency_key = str(identity.get("idempotency_key") or "")
        signal_bucket = int(identity.get("signal_bucket") or 0)
        intent = {
            "strategy_order_uuid": strategy_order_uuid,
            "idempotency_key": idempotency_key,
            "intent_id": str(identity.get("intent_id") or strategy_order_uuid),
            "strategy_name": str(identity.get("strategy_name") or ""),
            "signal_source": str(identity.get("signal_source") or ""),
            "signal_fingerprint": str(identity.get("signal_fingerprint") or ""),
            "token_id": str(signal.token_id or ""),
            "condition_id": str(signal.condition_id or ""),
            "side": str(signal.side or ""),
            "status": INTENT_STATUS_NEW,
            "payload": {
                "signal_id": str(signal.signal_id or ""),
                "trace_id": str(signal.trace_id or ""),
                "wallet": str(signal.wallet or ""),
                "requested_notional": float(notional_usd),
                "requested_price": float(signal.price_hint or 0.0),
                "signal_bucket": signal_bucket,
                "ack_unknown_count": 0,
                "ack_unknown_first_ts": 0,
                "submit_digest": "",
                "submit_digest_version": "",
                "submitted_price": float(signal.price_hint or 0.0),
                "submitted_size": 0.0,
                "tick_size": 0.0,
                "unknown_submit_first_seen_ts": 0,
                "unknown_submit_probe_count": 0,
                "probe_confidence": "none",
                "probe_basis": "no_match",
                "manual_required_reason": "",
            },
        }
        if not bool(getattr(self.settings, "idempotency_enabled", False)):
            return (CLAIMED_NEW, intent)
        if self._state_store is None:
            return (STORAGE_ERROR, intent)

        claim_fn = getattr(self._state_store, "claim_or_load_intent", None)
        if callable(claim_fn):
            try:
                raw_result = claim_fn(
                    idempotency_key=idempotency_key,
                    intent_id=str(intent["intent_id"]),
                    strategy_name=str(intent.get("strategy_name") or ""),
                    signal_source=str(intent.get("signal_source") or ""),
                    signal_fingerprint=str(intent.get("signal_fingerprint") or ""),
                    strategy_order_uuid=strategy_order_uuid,
                    token_id=str(signal.token_id or ""),
                    side=str(signal.side or ""),
                    condition_id=str(signal.condition_id or ""),
                    status=INTENT_STATUS_NEW,
                    payload=dict(intent.get("payload") or {}),
                )
                return self._normalize_claim_result(raw_result, intent)
            except Exception as exc:
                self.log.error("claim_or_load_intent failed uuid=%s err=%s", strategy_order_uuid, exc)
                return (STORAGE_ERROR, intent | {"error": str(exc)})

        return (STORAGE_ERROR, intent)

    @staticmethod
    def _is_ack_unknown_result(result: ExecutionResult) -> bool:
        status = normalize_intent_status(getattr(result, "status", ""))
        metadata = dict(getattr(result, "metadata", {}) or {})
        if bool(metadata.get("ack_unknown")):
            return True
        if result.ok and not str(result.broker_order_id or "").strip():
            return True
        if status in {"ack_unknown", "unknown", "timeout"}:
            return True
        if not result.ok and status not in {"rejected", "failed", "canceled", "unmatched"} and str(result.broker_order_id or "").strip():
            return True
        return False

    @staticmethod
    def _strategy_order_uuid(signal: Signal, notional_usd: float) -> str:
        try:
            normalized_notional = float(notional_usd)
        except Exception:
            normalized_notional = 0.0
        normalized = "|".join(
            [
                str(signal.wallet or "").strip().lower(),
                str(signal.condition_id or "").strip().lower(),
                str(signal.token_id or "").strip().lower(),
                str(signal.side or "").upper(),
                f"{normalized_notional:.4f}",
                str(signal.market_slug or "").strip().lower(),
                str(signal.signal_id or "").strip().lower(),
            ]
        )
        digest = hashlib.sha1(normalized.encode("utf-8"), usedforsecurity=False).hexdigest()
        return f"so-{digest[:20]}"

    def _pending_order_conflicts_with_signal(
        self,
        order: Mapping[str, object],
        signal: Signal,
    ) -> bool:
        if str(signal.side or "").upper() != "BUY":
            return False
        if str(order.get("side") or "").upper() != "BUY":
            return False

        signal_token = str(signal.token_id or "").strip().lower()
        order_token = str(order.get("token_id") or "").strip().lower()
        if signal_token and order_token:
            return signal_token == order_token

        signal_key, _ = self._signal_condition_exposure_key(signal)
        order_key, _ = self._pending_order_condition_exposure_key(order)
        return bool(signal_key and order_key and signal_key == order_key)

    def _find_pending_order_duplicate(self, signal: Signal) -> dict[str, object] | None:
        for order in sorted(self.pending_orders.values(), key=lambda item: int(item.get("ts") or 0), reverse=True):
            if self._pending_order_conflicts_with_signal(order, signal):
                return dict(order)
        return None

    def _find_broker_open_order_duplicate(self, signal: Signal) -> dict[str, object] | None:
        if self.settings.dry_run:
            return None
        list_open_orders = getattr(self.broker, "list_open_orders", None)
        if not callable(list_open_orders):
            return None
        try:
            broker_open_orders = list_open_orders()
        except Exception as exc:
            self.log.warning("Broker open-order duplicate check failed err=%s", exc)
            return None
        if not broker_open_orders:
            return None

        for snapshot in broker_open_orders:
            if str(getattr(snapshot, "side", "") or "").upper() != "BUY":
                continue
            snapshot_token = str(getattr(snapshot, "token_id", "") or "").strip().lower()
            signal_token = str(signal.token_id or "").strip().lower()
            if snapshot_token and signal_token and snapshot_token == signal_token:
                return {
                    "order_id": str(getattr(snapshot, "order_id", "") or ""),
                    "token_id": snapshot_token,
                    "condition_id": str(getattr(snapshot, "condition_id", "") or ""),
                    "market_slug": str(getattr(snapshot, "market_slug", "") or ""),
                    "side": "BUY",
                    "source": "broker_open_orders",
                }

            signal_key, _ = self._signal_condition_exposure_key(signal)
            snapshot_key, _ = self._condition_exposure_key(
                condition_id=getattr(snapshot, "condition_id", ""),
                market_slug=getattr(snapshot, "market_slug", ""),
                token_id=getattr(snapshot, "token_id", ""),
            )
            if signal_key and snapshot_key and signal_key == snapshot_key:
                return {
                    "order_id": str(getattr(snapshot, "order_id", "") or ""),
                    "token_id": snapshot_token,
                    "condition_id": str(getattr(snapshot, "condition_id", "") or ""),
                    "market_slug": str(getattr(snapshot, "market_slug", "") or ""),
                    "side": "BUY",
                    "source": "broker_open_orders",
                }
        return None

    def _safe_float(self, value: object, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _normalize_decision_mode(value: object) -> DecisionMode:
        normalized = str(value or "").strip().lower()
        if normalized in {"manual", "semi_auto", "auto"}:
            return normalized  # type: ignore[return-value]
        return "manual"

    def list_candidates(
        self,
        *,
        statuses: list[str] | tuple[str, ...] | None = None,
        limit: int = 24,
        include_expired: bool = False,
    ) -> list[dict[str, object]]:
        if self.candidate_store is None:
            return []
        return list(
            self.candidate_store.list_candidates(
                statuses=statuses,
                limit=limit,
                include_expired=include_expired,
            )
        )

    def list_wallet_profiles(self, *, limit: int = 32) -> list[dict[str, object]]:
        if self.candidate_store is None:
            return []
        return list(self.candidate_store.list_wallet_profiles(limit=limit))

    def list_journal_entries(self, *, limit: int = 20) -> list[dict[str, object]]:
        if self.candidate_store is None:
            return []
        return list(self.candidate_store.list_journal_entries(limit=limit))

    def journal_summary(self, *, days: int = 30) -> dict[str, object]:
        if self.candidate_store is None:
            return {
                "days": int(days),
                "total_entries": 0,
                "execution_actions": 0,
                "watch_actions": 0,
                "ignore_actions": 0,
                "updated_ts": int(time.time()),
            }
        return dict(self.candidate_store.journal_summary(days=days))

    def pending_candidate_actions(self, *, limit: int = 24) -> list[dict[str, object]]:
        if self.candidate_store is None:
            return []
        return list(self.candidate_store.list_pending_actions(limit=limit))

    @staticmethod
    def _market_window_bounds(market_slug: str) -> tuple[int | None, int | None, int | None]:
        normalized = str(market_slug or "").strip().lower()
        if not normalized:
            return None, None, None
        match = _MARKET_WINDOW_PATTERN.search(normalized)
        if match is None:
            return None, None, None
        duration_seconds = _MARKET_WINDOW_SECONDS.get(str(match.group(1) or "").strip().lower())
        start_ts = int(match.group(2) or 0)
        if duration_seconds is None or start_ts <= 0:
            return None, None, None
        return start_ts, duration_seconds, start_ts + duration_seconds

    @staticmethod
    def _market_metadata_cache_key(*, condition_id: str = "", market_slug: str = "") -> str:
        normalized_condition = str(condition_id or "").strip().lower()
        if normalized_condition:
            return f"condition:{normalized_condition}"
        normalized_slug = str(market_slug or "").strip().lower()
        if normalized_slug:
            return f"slug:{normalized_slug}"
        return ""

    def _candidate_market_metadata(self, *, condition_id: str = "", market_slug: str = "") -> MarketMetadata | None:
        cache_key = self._market_metadata_cache_key(condition_id=condition_id, market_slug=market_slug)
        if not cache_key:
            return None
        now_ts = int(time.time())
        cached = self._market_metadata_cache.get(cache_key)
        if isinstance(cached, dict):
            fetched_ts = int(cached.get("fetched_ts") or 0)
            if fetched_ts > 0 and (now_ts - fetched_ts) <= _MARKET_METADATA_CACHE_TTL_SECONDS:
                metadata = cached.get("metadata")
                return metadata if isinstance(metadata, MarketMetadata) else None

        getter = getattr(self.data_client, "get_market_metadata", None)
        metadata: MarketMetadata | None = None
        if callable(getter):
            try:
                metadata = getter(str(condition_id or "").strip(), slug=str(market_slug or "").strip() or None)
            except Exception as exc:
                self.log.debug(
                    "Candidate market metadata enrich failed condition=%s slug=%s err=%s",
                    condition_id,
                    market_slug,
                    exc,
                )
                metadata = None

        self._market_metadata_cache[cache_key] = {
            "fetched_ts": now_ts,
            "metadata": metadata,
        }
        if metadata is not None:
            if metadata.condition_id:
                self._market_metadata_cache[self._market_metadata_cache_key(condition_id=metadata.condition_id)] = {
                    "fetched_ts": now_ts,
                    "metadata": metadata,
                }
            if metadata.market_slug:
                self._market_metadata_cache[self._market_metadata_cache_key(market_slug=metadata.market_slug)] = {
                    "fetched_ts": now_ts,
                    "metadata": metadata,
                }
        return metadata

    def _candidate_market_context(
        self,
        token_id: str,
        *,
        price_hint: float,
        market_slug: str = "",
        condition_id: str = "",
    ) -> dict[str, object]:
        now_ts = int(time.time())
        book = None
        midpoint = None
        try:
            book = self.data_client.get_order_book(token_id)
        except Exception as exc:
            self.log.debug("Candidate order book enrich failed token=%s err=%s", token_id, exc)
        try:
            midpoint = self.data_client.get_midpoint_price(token_id)
        except Exception as exc:
            self.log.debug("Candidate midpoint enrich failed token=%s err=%s", token_id, exc)

        history_points = self._candidate_price_history(token_id, now_ts=now_ts)
        market_metadata = self._candidate_market_metadata(condition_id=condition_id, market_slug=market_slug)
        legacy_market_start_ts, legacy_market_window_seconds, legacy_market_end_ts = self._market_window_bounds(market_slug)
        market_start_ts = legacy_market_start_ts
        market_window_seconds = legacy_market_window_seconds
        market_end_ts = legacy_market_end_ts
        market_time_source = "unknown"
        if market_metadata is not None and market_metadata.end_ts is not None and market_metadata.end_ts > 0:
            market_end_ts = int(market_metadata.end_ts)
            market_time_source = "metadata"
            if market_window_seconds is not None:
                market_start_ts = market_end_ts - int(market_window_seconds)
        elif market_end_ts is not None:
            market_time_source = "slug_legacy"

        best_bid = float(book.best_bid) if book is not None and float(book.best_bid) > 0.0 else 0.0
        best_ask = float(book.best_ask) if book is not None and float(book.best_ask) > 0.0 else 0.0
        ref_mid = float(midpoint or 0.0)
        history_reference = ref_mid if ref_mid > 0.0 else (best_ask if best_ask > 0.0 else self._history_last_price(history_points))
        momentum_5m = self._history_momentum(history_points, current_price=history_reference, now_ts=now_ts, window_seconds=300)
        momentum_30m = self._history_momentum(history_points, current_price=history_reference, now_ts=now_ts, window_seconds=1800)
        spread_pct = None
        if best_bid > 0.0 and best_ask > 0.0:
            denominator = ref_mid if ref_mid > 0.0 else ((best_bid + best_ask) / 2.0)
            if denominator > 0.0:
                spread_pct = ((best_ask - best_bid) / denominator) * 100.0

        chase_ref = best_ask if best_ask > 0.0 else 0.0
        chase_pct = None
        if chase_ref > 0.0 and price_hint > 0.0:
            chase_pct = ((chase_ref - price_hint) / price_hint) * 100.0
        market_remaining_seconds = market_end_ts - now_ts if market_end_ts is not None else None
        market_elapsed_ratio = None
        if market_start_ts is not None and market_window_seconds is not None and market_end_ts is not None:
            market_age_seconds = max(0, now_ts - market_start_ts)
            market_elapsed_ratio = min(1.0, max(0.0, market_age_seconds / max(1, market_window_seconds)))

        return {
            "current_best_bid": best_bid if best_bid > 0.0 else None,
            "current_best_ask": best_ask if best_ask > 0.0 else None,
            "current_midpoint": ref_mid if ref_mid > 0.0 else None,
            "spread_pct": spread_pct,
            "chase_pct": chase_pct,
            "momentum_5m": momentum_5m,
            "momentum_30m": momentum_30m,
            "history_points": len(history_points),
            "history_span_seconds": self._history_span_seconds(history_points),
            "market_window_seconds": market_window_seconds,
            "market_start_ts": market_start_ts,
            "market_end_ts": market_end_ts,
            "market_end_date": str(market_metadata.end_date or "") if market_metadata is not None else "",
            "market_remaining_seconds": market_remaining_seconds,
            "market_elapsed_ratio": market_elapsed_ratio,
            "market_closed": market_metadata.closed if market_metadata is not None else None,
            "market_active": market_metadata.active if market_metadata is not None else None,
            "market_accepting_orders": market_metadata.accepting_orders if market_metadata is not None else None,
            "market_time_source": market_time_source,
            "market_metadata_hit": market_metadata is not None,
        }

    def _candidate_price_history(self, token_id: str, *, now_ts: int) -> list[tuple[int, float]]:
        normalized = str(token_id or "").strip()
        if not normalized:
            return []

        cache = self._candidate_price_history_cache.get(normalized)
        if isinstance(cache, dict):
            fetched_ts = int(cache.get("fetched_ts") or 0)
            cached_points = list(cache.get("points") or [])
            if fetched_ts > 0 and (now_ts - fetched_ts) <= 120 and cached_points:
                return [
                    (int(point[0]), float(point[1]))
                    for point in cached_points
                    if isinstance(point, tuple) and len(point) == 2
                ]

        getter = getattr(self.data_client, "get_prices_history", None)
        if not callable(getter):
            getter = getattr(self.data_client, "get_price_history", None)
        points: list[tuple[int, float]] = []
        if callable(getter):
            raw_points: object = []
            try:
                raw_points = getter(
                    normalized,
                    start_ts=max(0, now_ts - 3600),
                    end_ts=now_ts,
                    fidelity=1,
                )
            except TypeError:
                try:
                    raw_points = getter(normalized, interval="1h", fidelity=1)
                except Exception as exc:
                    self.log.debug("Candidate price history enrich failed token=%s err=%s", normalized, exc)
                    raw_points = []
            except Exception as exc:
                self.log.debug("Candidate price history enrich failed token=%s err=%s", normalized, exc)
                raw_points = []

            for row in list(raw_points or []):
                parsed = self._normalize_price_history_point(row)
                if parsed is None:
                    continue
                points.append(parsed)

        points.sort(key=lambda item: item[0])
        self._candidate_price_history_cache[normalized] = {
            "fetched_ts": now_ts,
            "points": list(points),
        }
        return list(points)

    def _candidate_effective_expiry_ts(
        self,
        *,
        created_ts: int,
        market_slug: str = "",
        market_end_ts: object = None,
    ) -> int:
        base_ts = int(created_ts or 0)
        if base_ts <= 0:
            return 0
        expires_ts = base_ts + int(self.settings.candidate_lifetime_seconds)
        resolved_market_end_ts = int(market_end_ts or 0) if market_end_ts not in (None, "") else 0
        if resolved_market_end_ts <= 0:
            _, _, legacy_market_end_ts = self._market_window_bounds(str(market_slug or ""))
            resolved_market_end_ts = int(legacy_market_end_ts or 0)
        if resolved_market_end_ts > 0:
            expires_ts = min(expires_ts, resolved_market_end_ts)
        return expires_ts

    def _candidate_is_expired(
        self,
        candidate: Mapping[str, object],
        *,
        now_ts: int,
    ) -> tuple[bool, int]:
        created_ts = int(candidate.get("created_ts") or 0)
        expires_ts = self._candidate_effective_expiry_ts(
            created_ts=created_ts,
            market_slug=str(candidate.get("market_slug") or ""),
            market_end_ts=candidate.get("expires_ts"),
        )
        if expires_ts <= 0:
            return False, 0
        return expires_ts <= int(now_ts), expires_ts

    def _expire_candidate_record(
        self,
        candidate_id: str,
        *,
        now_ts: int,
        block_layer: str,
        note: str,
    ) -> dict[str, object] | None:
        if self.candidate_store is None or not candidate_id:
            return None
        candidate = self.candidate_store.get_candidate(candidate_id)
        if candidate is None:
            return None
        expired_now, expires_ts = self._candidate_is_expired(candidate, now_ts=now_ts)
        if not expired_now:
            return None
        return self.candidate_store.update_candidate_status(
            candidate_id,
            status="expired",
            note=note,
            result_tag=REASON_CANDIDATE_LIFETIME_EXPIRED,
            block_reason=REASON_CANDIDATE_LIFETIME_EXPIRED,
            block_layer=block_layer,
            lifecycle_state="expired_discarded",
            expires_ts=expires_ts if expires_ts > 0 else None,
            updated_ts=now_ts,
        )

    @staticmethod
    def _normalize_price_history_point(row: object) -> tuple[int, float] | None:
        if isinstance(row, PriceHistoryPoint):
            timestamp = int(row.timestamp or 0)
            price = float(row.price or 0.0)
            return (timestamp, price) if timestamp > 0 and price > 0.0 else None
        if isinstance(row, Mapping):
            timestamp = int(row.get("t") or row.get("timestamp") or row.get("ts") or 0)
            price = float(row.get("p") or row.get("price") or 0.0)
            return (timestamp, price) if timestamp > 0 and price > 0.0 else None
        timestamp = int(getattr(row, "timestamp", 0) or getattr(row, "t", 0) or 0)
        price = float(getattr(row, "price", 0.0) or getattr(row, "p", 0.0) or 0.0)
        return (timestamp, price) if timestamp > 0 and price > 0.0 else None

    @staticmethod
    def _history_last_price(points: list[tuple[int, float]]) -> float:
        if not points:
            return 0.0
        return float(points[-1][1])

    @staticmethod
    def _history_price_at_or_before(points: list[tuple[int, float]], target_ts: int) -> float | None:
        if not points:
            return None
        resolved: float | None = None
        for timestamp, price in points:
            if timestamp <= target_ts and price > 0.0:
                resolved = float(price)
            elif timestamp > target_ts:
                break
        return resolved

    @classmethod
    def _history_momentum(
        cls,
        points: list[tuple[int, float]],
        *,
        current_price: float,
        now_ts: int,
        window_seconds: int,
    ) -> float | None:
        if current_price <= 0.0 or not points:
            return None
        history_price = cls._history_price_at_or_before(points, now_ts - max(1, int(window_seconds)))
        if history_price is None or history_price <= 0.0:
            return None
        return ((float(current_price) - float(history_price)) / float(history_price)) * 100.0

    @staticmethod
    def _history_span_seconds(points: list[tuple[int, float]]) -> int:
        if len(points) < 2:
            return 0
        return max(0, int(points[-1][0]) - int(points[0][0]))

    @staticmethod
    def _candidate_score_from_signal(signal: Signal, market_context: Mapping[str, object]) -> float:
        base = (float(signal.confidence or 0.0) * 100.0 * 0.55) + (float(signal.wallet_score or 0.0) * 0.45)
        spread_pct = float(market_context.get("spread_pct") or 0.0)
        chase_pct = float(market_context.get("chase_pct") or 0.0)
        momentum_5m = float(market_context.get("momentum_5m") or 0.0)
        momentum_30m = float(market_context.get("momentum_30m") or 0.0)
        momentum_bias = (momentum_5m * 0.55) + (momentum_30m * 0.45)
        if spread_pct > 6.0:
            base -= min(20.0, spread_pct * 1.1)
        if signal.side == "BUY" and chase_pct > 4.0:
            base -= min(18.0, chase_pct * 1.4)
        if signal.side == "BUY":
            if momentum_bias > 0.0:
                base += min(8.0, momentum_bias * 0.45)
            else:
                base -= min(12.0, abs(momentum_bias) * 0.55)
        elif signal.side == "SELL":
            if momentum_bias < 0.0:
                base += min(6.0, abs(momentum_bias) * 0.35)
            else:
                base -= min(4.0, momentum_bias * 0.2)
        if bool(signal.cross_wallet_exit):
            base += 6.0
        if int(signal.exit_wallet_count or 0) >= 2:
            base += min(10.0, float(signal.exit_wallet_count or 0) * 2.0)
        topic_bias = str(signal.topic_bias or "").strip().lower()
        if topic_bias == "boost":
            base += 4.0
        elif topic_bias == "penalty":
            base -= 4.0
        if signal.side == "SELL":
            base = max(base, 70.0 if float(signal.exit_fraction or 0.0) >= 0.95 else 62.0)
        return round(max(0.0, min(100.0, base)), 2)

    def _candidate_skip_reason(
        self,
        signal: Signal,
        market_context: Mapping[str, object],
        *,
        existing: Mapping[str, object] | None = None,
    ) -> str | None:
        repeat_entry_reason = self._repeat_entry_block_reason(signal, existing)
        if repeat_entry_reason:
            return repeat_entry_reason
        has_orderbook = (
            market_context.get("current_best_ask") not in (None, "")
            or market_context.get("current_best_bid") not in (None, "")
        )
        has_live_ask = market_context.get("current_best_ask") not in (None, "")
        has_midpoint = market_context.get("current_midpoint") not in (None, "")
        best_bid = float(market_context.get("current_best_bid") or 0.0)
        best_ask = float(market_context.get("current_best_ask") or 0.0)
        spread_pct = float(market_context.get("spread_pct") or 0.0)
        chase_pct = float(market_context.get("chase_pct") or 0.0)
        momentum_5m = float(market_context.get("momentum_5m") or 0.0)
        momentum_30m = float(market_context.get("momentum_30m") or 0.0)
        price_hint = float(signal.price_hint or 0.0)
        has_market_reference = has_orderbook or has_midpoint or price_hint > 0.0
        market_window_seconds = (
            int(market_context.get("market_window_seconds") or 0)
            if market_context.get("market_window_seconds") not in (None, "")
            else None
        )
        market_remaining_seconds = (
            int(market_context.get("market_remaining_seconds") or 0)
            if market_context.get("market_remaining_seconds") not in (None, "")
            else None
        )
        market_closed = market_context.get("market_closed")
        market_active = market_context.get("market_active")
        market_accepting_orders = market_context.get("market_accepting_orders")
        if signal.side == "BUY":
            if market_closed is True:
                return "market_closed"
            if market_active is False:
                return "market_inactive"
            if market_accepting_orders is False:
                return "market_not_accepting_orders"
            if market_remaining_seconds is not None:
                if market_remaining_seconds <= 0:
                    return "market_window_elapsed"
                if market_window_seconds is not None and market_window_seconds <= 900 and market_remaining_seconds <= 90:
                    return "market_near_close"
            if not has_live_ask:
                return "market_data_unavailable"
            if not has_market_reference:
                return "market_data_unavailable"
            max_spread_pct = float(self.settings.candidate_buy_max_spread_pct)
            spread_chase_guard_pct = float(self.settings.candidate_buy_spread_chase_guard_pct)
            max_chase_pct = float(self.settings.candidate_buy_max_chase_pct)
            # Manual candidate review should not be polluted by obviously non-executable books.
            hard_max_spread_pct = 50.0
            if best_bid > 0.0 and best_ask > 0.0 and best_bid <= 0.005 and best_ask >= 0.995:
                return "spread_too_wide"
            if chase_pct >= max_chase_pct:
                return "chase_too_high"
            if spread_pct >= min(max_spread_pct, hard_max_spread_pct):
                return "spread_too_wide"
            if spread_pct >= 12.0 and chase_pct >= spread_chase_guard_pct:
                return "spread_too_wide"
        if signal.side == "BUY" and momentum_5m <= -8.0 and momentum_30m <= -12.0:
            return "momentum_too_weak"
        return None

    @staticmethod
    def _candidate_trigger_type(signal: Signal) -> str:
        action = str(signal.position_action or "").strip().lower()
        if action in {"entry", "add", "trim", "exit"}:
            return action
        if signal.side == "SELL":
            return "follow_exit"
        return "new_open"

    def _candidate_suggested_action(
        self,
        signal: Signal,
        *,
        score: float,
        skip_reason: str | None,
        existing: Mapping[str, object] | None = None,
    ) -> str:
        if signal.side == "SELL":
            if float(signal.exit_fraction or 0.0) >= 0.95 or bool(signal.cross_wallet_exit):
                return "close_all"
            return "close_partial"
        if skip_reason:
            return "watch"
        if existing and float(existing.get("notional") or 0.0) > 0.0 and not self._same_wallet_add_allowed(signal, existing):
            return "watch"
        if score >= 84.0 and str(signal.wallet_tier or "").upper() in {"HIGH", "CORE"}:
            return "follow"
        if score >= 72.0:
            return "buy_normal"
        if score >= 60.0:
            return "buy_small"
        return "watch"

    @staticmethod
    def _candidate_recommendation_reason(
        signal: Signal,
        *,
        market_context: Mapping[str, object],
        score: float,
        suggested_action: str,
        skip_reason: str | None,
        existing: Mapping[str, object] | None = None,
    ) -> str:
        action_label = Trader._candidate_action_label(suggested_action, signal.side)
        if skip_reason == "market_data_unavailable":
            return i18n_t("runner.candidateRecommendation.marketDataUnavailable")
        if skip_reason == "market_window_elapsed":
            return i18n_t("runner.candidateRecommendation.marketWindowElapsed")
        if skip_reason == "market_near_close":
            return i18n_t("runner.candidateRecommendation.marketNearClose")
        if skip_reason == "market_closed":
            return i18n_t("runner.candidateRecommendation.marketClosed")
        if skip_reason == "market_inactive":
            return i18n_t("runner.candidateRecommendation.marketInactive")
        if skip_reason == "market_not_accepting_orders":
            return i18n_t("runner.candidateRecommendation.marketNotAcceptingOrders")
        if skip_reason == "spread_too_wide":
            return i18n_t("runner.candidateRecommendation.spreadTooWide")
        if skip_reason == "chase_too_high":
            return i18n_t("runner.candidateRecommendation.chaseTooHigh")
        if skip_reason == "momentum_too_weak":
            return i18n_t("runner.candidateRecommendation.momentumTooWeak")
        if skip_reason == REASON_REPEAT_ENTRY_BLOCKED_EXISTING_POSITION:
            return i18n_t("runner.candidateRecommendation.repeatEntryBlockedExistingPosition")
        if skip_reason == REASON_SAME_WALLET_ADD_NOT_ALLOWED:
            return i18n_t("runner.candidateRecommendation.sameWalletAddNotAllowed")
        if skip_reason == REASON_CROSS_WALLET_REPEAT_ENTRY_BLOCKED:
            return i18n_t("runner.candidateRecommendation.crossWalletRepeatEntryBlocked")
        if skip_reason == "existing_position_conflict":
            return i18n_t("runner.candidateRecommendation.existingPositionConflict")
        if signal.side == "SELL":
            if bool(signal.cross_wallet_exit):
                return i18n_t("runner.candidateRecommendation.sellCrossWalletExit", {"count": int(signal.exit_wallet_count or 0)})
            if float(signal.exit_fraction or 0.0) >= 0.95:
                return i18n_t("runner.candidateRecommendation.sellFullExit")
            return i18n_t("runner.candidateRecommendation.sellPartialExit")
        if existing and float(existing.get("notional") or 0.0) > 0.0:
            return i18n_t("runner.candidateRecommendation.existingPositionPresent")
        momentum_5m = float(market_context.get("momentum_5m") or 0.0)
        momentum_30m = float(market_context.get("momentum_30m") or 0.0)
        if signal.side == "BUY" and momentum_5m > 0.0 and momentum_30m > 0.0:
            return i18n_t("runner.candidateRecommendation.buyMomentumStrong", {"action": action_label})
        if signal.side == "BUY" and momentum_5m < 0.0 and momentum_30m < 0.0:
            return i18n_t("runner.candidateRecommendation.buyMomentumWeak", {"action": action_label})
        if suggested_action == "follow":
            return i18n_t("runner.candidateRecommendation.follow", {"score": f"{score:.0f}"})
        if suggested_action == "buy_normal":
            return i18n_t("runner.candidateRecommendation.buyNormal", {"score": f"{score:.0f}"})
        if suggested_action == "buy_small":
            return i18n_t("runner.candidateRecommendation.buySmall", {"score": f"{score:.0f}"})
        return i18n_t("runner.candidateRecommendation.watchDefault", {"score": f"{score:.0f}"})

    @staticmethod
    def _candidate_reason_factor(
        key: str,
        label: str,
        value: object,
        *,
        direction: str = "neutral",
        weight: float = 0.0,
        detail: str = "",
    ) -> CandidateReasonFactor:
        if isinstance(value, float):
            rendered_value = f"{value:.2f}"
        elif isinstance(value, int):
            rendered_value = str(value)
        else:
            rendered_value = str(value)
        return CandidateReasonFactor(
            key=str(key),
            label=str(label),
            value=rendered_value,
            direction=str(direction or "neutral"),
            weight=float(weight),
            detail=str(detail or ""),
        )

    def _candidate_reason_factors(
        self,
        signal: Signal,
        market_context: Mapping[str, object],
        *,
        score: float,
        suggested_action: str,
        skip_reason: str | None,
        existing: Mapping[str, object] | None = None,
    ) -> list[CandidateReasonFactor]:
        factors: list[CandidateReasonFactor] = []
        wallet_score = float(signal.wallet_score or 0.0)
        confidence = float(signal.confidence or 0.0)
        wallet_tier = str(signal.wallet_tier or "LOW").upper()
        factors.append(
            self._candidate_reason_factor(
                "wallet",
                i18n_t("runner.candidateFactor.wallet.label"),
                i18n_t("runner.candidateFactor.walletValue", {"tier": self._wallet_tier_label(wallet_tier), "score": f"{wallet_score:.1f}"}),
                direction="bullish" if wallet_score >= 70.0 else "neutral",
                weight=round((wallet_score - 50.0) / 10.0, 2),
                detail=str(signal.wallet_score_summary or ""),
            )
        )
        if confidence > 0.0:
            factors.append(
                self._candidate_reason_factor(
                    "confidence",
                    i18n_t("runner.candidateFactor.confidence.label"),
                    f"{confidence:.2f}",
                    direction="bullish" if confidence >= 0.75 else "neutral",
                    weight=round((confidence - 0.5) * 10.0, 2),
                )
            )

        topic_label = str(signal.topic_label or signal.topic_key or "").strip()
        if topic_label:
            topic_bias = str(signal.topic_bias or "neutral").strip()
            topic_direction = "bullish" if topic_bias == "boost" else "bearish" if topic_bias == "penalty" else "neutral"
            factors.append(
                self._candidate_reason_factor(
                    "topic",
                    i18n_t("runner.candidateFactor.topic.label"),
                    topic_label,
                    direction=topic_direction,
                    weight=round((float(signal.topic_multiplier or 1.0) - 1.0) * 10.0, 2),
                    detail=str(signal.topic_score_summary or ""),
                )
            )

        market_closed = market_context.get("market_closed")
        market_active = market_context.get("market_active")
        market_accepting_orders = market_context.get("market_accepting_orders")
        market_time_source = str(market_context.get("market_time_source") or "").strip()
        market_remaining_seconds = market_context.get("market_remaining_seconds")
        if signal.side == "BUY" and (
            market_closed is True
            or market_active is False
            or market_accepting_orders is False
            or market_time_source == "metadata"
        ):
            market_state_parts: list[str] = []
            direction = "neutral"
            weight = 0.0
            if market_closed is True:
                market_state_parts.append(i18n_t("runner.candidateFactor.marketState.closed"))
                direction = "bearish"
                weight = -12.0
            elif market_active is False:
                market_state_parts.append(i18n_t("runner.candidateFactor.marketState.inactive"))
                direction = "bearish"
                weight = -8.0
            elif market_accepting_orders is False:
                market_state_parts.append(i18n_t("runner.candidateFactor.marketState.acceptingOrdersFalse"))
                direction = "bearish"
                weight = -10.0
            if market_time_source == "metadata":
                market_state_parts.append(i18n_t("runner.candidateFactor.marketState.metadata"))
            if market_remaining_seconds not in (None, ""):
                market_state_parts.append(i18n_t("runner.candidateFactor.marketState.remainSeconds", {"seconds": int(market_remaining_seconds)}))
            factors.append(
                self._candidate_reason_factor(
                    "market_state",
                    i18n_t("runner.candidateFactor.marketState.label"),
                    self._notification_separator().join(part for part in market_state_parts if part) or i18n_t("runner.candidateFactor.marketState.metadata"),
                    direction=direction,
                    weight=weight,
                    detail=i18n_t("runner.candidateFactor.marketState.detail"),
                )
            )

        spread_pct = float(market_context.get("spread_pct") or 0.0)
        if spread_pct > 0.0:
            factors.append(
                self._candidate_reason_factor(
                    "spread",
                    i18n_t("runner.candidateFactor.spread.label"),
                    f"{spread_pct:.2f}%",
                    direction="bearish" if spread_pct >= 6.0 else "neutral",
                    weight=round(-min(20.0, spread_pct * 1.1), 2),
                    detail=i18n_t("runner.candidateFactor.spread.detail"),
                )
            )

        chase_pct = float(market_context.get("chase_pct") or 0.0)
        if signal.side == "BUY" and chase_pct > 0.0:
            factors.append(
                self._candidate_reason_factor(
                    "chase",
                    i18n_t("runner.candidateFactor.chase.label"),
                    f"{chase_pct:.2f}%",
                    direction="bearish" if chase_pct >= 4.0 else "neutral",
                    weight=round(-min(18.0, chase_pct * 1.4), 2),
                    detail=i18n_t("runner.candidateFactor.chase.detail"),
                )
            )

        momentum_5m = float(market_context.get("momentum_5m") or 0.0)
        momentum_30m = float(market_context.get("momentum_30m") or 0.0)
        if momentum_5m != 0.0 or momentum_30m != 0.0:
            momentum_bias = (momentum_5m * 0.55) + (momentum_30m * 0.45)
            factors.append(
                self._candidate_reason_factor(
                    "momentum",
                    i18n_t("runner.candidateFactor.momentum.label"),
                    f"5m {momentum_5m:+.2f}% / 30m {momentum_30m:+.2f}%",
                    direction="bullish" if momentum_bias > 0.0 else "bearish" if momentum_bias < 0.0 else "neutral",
                    weight=round(momentum_bias * 0.45, 2),
                    detail=i18n_t("runner.candidateFactor.momentum.detail"),
                )
            )

        if existing and float(existing.get("notional") or 0.0) > 0.0:
            current_notional = float(existing.get("notional") or 0.0)
            direction = "bearish" if skip_reason in REPEAT_ENTRY_BLOCK_REASONS or skip_reason == "existing_position_conflict" else "neutral"
            factors.append(
                self._candidate_reason_factor(
                    "existing_position",
                    i18n_t("runner.candidateFactor.existingPosition.label"),
                    f"{current_notional:.2f}U",
                    direction=direction,
                    weight=-6.0 if skip_reason in REPEAT_ENTRY_BLOCK_REASONS or skip_reason == "existing_position_conflict" else 0.0,
                    detail=str(existing.get("entry_reason") or existing.get("reason") or i18n_t("runner.candidateFactor.existingPosition.detailDefault")),
                )
            )

        if signal.side == "SELL":
            exit_fraction = float(signal.exit_fraction or 0.0)
            factors.append(
                self._candidate_reason_factor(
                    "exit",
                    i18n_t("runner.candidateFactor.exit.label"),
                    f"{exit_fraction * 100.0:.0f}%",
                    direction="bearish" if exit_fraction >= 0.95 or bool(signal.cross_wallet_exit) else "neutral",
                    weight=round(8.0 + exit_fraction * 10.0, 2),
                    detail=i18n_t("runner.candidateFactor.exit.detail"),
                )
            )
            if bool(signal.cross_wallet_exit):
                factors.append(
                    self._candidate_reason_factor(
                        "resonance_exit",
                        i18n_t("runner.candidateFactor.resonanceExit.label"),
                        i18n_t("runner.candidateFactor.resonanceExit.value", {"count": int(signal.exit_wallet_count or 0)}),
                        direction="bearish",
                        weight=min(10.0, float(signal.exit_wallet_count or 0) * 2.0),
                        detail=i18n_t("runner.candidateFactor.resonanceExit.detail"),
                    )
                )

        factors.append(
            self._candidate_reason_factor(
                "decision",
                i18n_t("runner.candidateFactor.decision.label"),
                self._candidate_action_label(suggested_action, signal.side),
                direction="bullish" if suggested_action in {"follow", "buy_normal"} else "neutral",
                weight=max(-2.0, min(10.0, score / 10.0 - 5.0)),
                detail=self._candidate_recommendation_reason(
                    signal,
                    market_context=market_context,
                    score=score,
                    suggested_action=suggested_action,
                    skip_reason=skip_reason,
                    existing=existing,
                ),
            )
        )
        if skip_reason:
            factors.append(
                self._candidate_reason_factor(
                    "skip_reason",
                    i18n_t("runner.candidateFactor.skipReason.label"),
                    self._reason_label(skip_reason),
                    direction="bearish",
                    weight=-10.0,
                    detail=self._candidate_recommendation_reason(
                        signal,
                        market_context=market_context,
                        score=score,
                        suggested_action=suggested_action,
                        skip_reason=skip_reason,
                        existing=existing,
                    ),
                )
            )

        return factors

    @staticmethod
    def _candidate_explanation(
        signal: Signal,
        market_context: Mapping[str, object],
        *,
        score: float,
        suggested_action: str,
        skip_reason: str | None,
        existing: Mapping[str, object] | None = None,
    ) -> list[str]:
        notes: list[str] = []
        notes.append(
            i18n_t(
                "runner.candidateExplanation.walletSummary",
                {
                    "tier": Trader._wallet_tier_label(str(signal.wallet_tier or "WATCH")),
                    "walletScore": f"{float(signal.wallet_score or 0.0):.1f}",
                    "confidence": f"{float(signal.confidence or 0.0):.2f}",
                },
            )
        )
        topic_label = str(signal.topic_label or signal.topic_key or "").strip()
        if topic_label:
            topic_bias = str(signal.topic_bias or "neutral").strip()
            notes.append(
                i18n_t(
                    "runner.candidateExplanation.topicSummary",
                    {
                        "topic": topic_label,
                        "bias": i18n_humanize_identifier(topic_bias),
                        "multiplier": f"{float(signal.topic_multiplier or 1.0):.2f}",
                    },
                )
            )
        spread_pct = market_context.get("spread_pct")
        chase_pct = market_context.get("chase_pct")
        best_ask = market_context.get("current_best_ask")
        best_bid = market_context.get("current_best_bid")
        midpoint = market_context.get("current_midpoint")
        history_points = int(market_context.get("history_points") or 0)
        market_bits: list[str] = []
        if midpoint not in (None, ""):
            market_bits.append(i18n_t("runner.candidateExplanation.marketBit.mid", {"value": f"{float(midpoint):.3f}"}))
        if best_ask not in (None, ""):
            market_bits.append(i18n_t("runner.candidateExplanation.marketBit.ask", {"value": f"{float(best_ask):.3f}"}))
        if best_bid not in (None, ""):
            market_bits.append(i18n_t("runner.candidateExplanation.marketBit.bid", {"value": f"{float(best_bid):.3f}"}))
        if spread_pct not in (None, ""):
            market_bits.append(i18n_t("runner.candidateExplanation.marketBit.spread", {"value": f"{float(spread_pct):.2f}%"}))
        if chase_pct not in (None, "") and signal.side == "BUY":
            market_bits.append(i18n_t("runner.candidateExplanation.marketBit.chase", {"value": f"{float(chase_pct):.2f}%"}))
        if market_bits:
            notes.append(i18n_t("runner.candidateExplanation.marketPrefix", {"bits": Trader._notification_separator().join(market_bits)}))
        market_status_bits: list[str] = []
        if market_context.get("market_closed") is True:
            market_status_bits.append(i18n_t("runner.candidateExplanation.marketStatusBit.closed"))
        elif market_context.get("market_active") is False:
            market_status_bits.append(i18n_t("runner.candidateExplanation.marketStatusBit.inactive"))
        elif market_context.get("market_accepting_orders") is False:
            market_status_bits.append(i18n_t("runner.candidateExplanation.marketStatusBit.acceptingOrdersFalse"))
            market_status_bits.append("acceptingOrders=false")
        market_time_source = str(market_context.get("market_time_source") or "").strip()
        if market_time_source:
            market_status_bits.append(i18n_t("runner.candidateExplanation.marketStatusBit.timeSource", {"value": market_time_source}))
            market_status_bits.append(f"time={market_time_source}")
        market_end_date = str(market_context.get("market_end_date") or "").strip()
        if market_end_date:
            market_status_bits.append(i18n_t("runner.candidateExplanation.marketStatusBit.endDate", {"value": market_end_date}))
        market_remaining_seconds = market_context.get("market_remaining_seconds")
        if market_remaining_seconds not in (None, ""):
            market_status_bits.append(i18n_t("runner.candidateExplanation.marketStatusBit.remainSeconds", {"seconds": int(market_remaining_seconds)}))
        if market_status_bits:
            notes.append(i18n_t("runner.candidateExplanation.marketStatusPrefix", {"bits": Trader._notification_separator().join(market_status_bits)}))
        if history_points > 0:
            notes.append(
                i18n_t(
                    "runner.candidateExplanation.historyMomentum",
                    {
                        "points": history_points,
                        "momentum5m": f"{float(market_context.get('momentum_5m') or 0.0):+.2f}%",
                        "momentum30m": f"{float(market_context.get('momentum_30m') or 0.0):+.2f}%",
                    },
                )
            )
        momentum_5m = float(market_context.get("momentum_5m") or 0.0)
        momentum_30m = float(market_context.get("momentum_30m") or 0.0)
        if momentum_5m > 0.0 and momentum_30m > 0.0:
            notes.append(i18n_t("runner.candidateExplanation.trendStrong"))
        elif momentum_5m < 0.0 and momentum_30m < 0.0:
            notes.append(i18n_t("runner.candidateExplanation.trendWeak"))
        if existing and float(existing.get("notional") or 0.0) > 0.0:
            notes.append(
                i18n_t(
                    "runner.candidateExplanation.existingPosition",
                    {
                        "notional": f"{float(existing.get('notional') or 0.0):.2f}U",
                        "sourceSuffix": (
                            i18n_t(
                                "runner.candidateExplanation.existingSourceSuffix",
                                {"tier": Trader._wallet_tier_label(str(existing.get("entry_wallet_tier") or ""))},
                            )
                            if str(existing.get("entry_wallet_tier") or "").strip()
                            else ""
                        ),
                    },
                )
            )
        if bool(signal.cross_wallet_exit):
            notes.append(i18n_t("runner.candidateExplanation.sellCrossWalletExit", {"count": int(signal.exit_wallet_count or 0)}))
        elif signal.side == "SELL" and float(signal.exit_fraction or 0.0) > 0.0:
            notes.append(i18n_t("runner.candidateExplanation.sellExitFraction", {"fraction": f"{float(signal.exit_fraction or 0.0) * 100.0:.0f}%"}))
        elif signal.side == "BUY":
            notes.append(
                i18n_t(
                    "runner.candidateExplanation.buyObservedNotional",
                    {
                        "notional": f"{float(signal.observed_notional or 0.0):.2f}U",
                        "action": str(signal.position_action_label or Trader._candidate_action_label(signal.position_action, signal.side) or ""),
                    },
                )
            )
        if skip_reason:
            notes.append(i18n_t("runner.candidateExplanation.skipPrefix", {"reason": Trader._candidate_recommendation_reason(
                signal,
                market_context=market_context,
                score=score,
                suggested_action=suggested_action,
                skip_reason=skip_reason,
                existing=existing,
            )}))
        else:
            notes.append(i18n_t("runner.candidateExplanation.decisionSummary", {"action": Trader._candidate_action_label(suggested_action, signal.side), "score": f"{score:.0f}"}))
        if signal.side == "BUY" and momentum_5m > 0.0 and momentum_30m > 0.0:
            notes.append(i18n_t("runner.candidateExplanation.buyMomentumStrong"))
        elif signal.side == "BUY" and momentum_5m < 0.0 and momentum_30m < 0.0:
            notes.append(i18n_t("runner.candidateExplanation.buyMomentumWeak"))
        return notes

    def _candidate_from_signal(
        self,
        signal: Signal,
        *,
        now: int,
        existing: Mapping[str, object] | None = None,
        market_context: Mapping[str, object] | None = None,
    ) -> Candidate:
        if existing is None:
            existing = self._position_truth_for_token(signal.token_id)
        if market_context is None:
            market_context = self._candidate_market_context(
                signal.token_id,
                price_hint=float(signal.price_hint or 0.0),
                market_slug=str(signal.market_slug or ""),
                condition_id=str(signal.condition_id or ""),
            )
        score = self._candidate_score_from_signal(signal, market_context)
        skip_reason = self._candidate_skip_reason(signal, market_context, existing=existing)
        suggested_action = self._candidate_suggested_action(
            signal,
            score=score,
            skip_reason=skip_reason,
            existing=existing,
        )
        recommendation_reason = self._candidate_recommendation_reason(
            signal,
            market_context=market_context,
            score=score,
            suggested_action=suggested_action,
            skip_reason=skip_reason,
            existing=existing,
        )
        reason_factors = self._candidate_reason_factors(
            signal,
            market_context,
            score=score,
            suggested_action=suggested_action,
            skip_reason=skip_reason,
            existing=existing,
        )
        explanation = self._candidate_explanation(
            signal,
            market_context,
            score=score,
            suggested_action=suggested_action,
            skip_reason=skip_reason,
            existing=existing,
        )
        source_wallet_count = max(1, int(signal.exit_wallet_count or 0)) if bool(signal.cross_wallet_exit) else 1
        created_ts = int(now or time.time())
        expires_ts = self._candidate_effective_expiry_ts(
            created_ts=created_ts,
            market_slug=str(signal.market_slug or ""),
            market_end_ts=market_context.get("market_end_ts"),
        )
        status = "watched" if skip_reason or suggested_action == "watch" else "pending"
        return Candidate(
            id=str(signal.signal_id or ""),
            signal_id=str(signal.signal_id or ""),
            trace_id=str(signal.trace_id or ""),
            wallet=str(signal.wallet or ""),
            wallet_tag=str(signal.wallet_tier or ""),
            wallet_score=float(signal.wallet_score or 0.0),
            wallet_tier=str(signal.wallet_tier or "LOW"),
            market_slug=str(signal.market_slug or ""),
            token_id=str(signal.token_id or ""),
            condition_id=str(signal.condition_id or ""),
            outcome=str(signal.outcome or ""),
            side=str(signal.side or "BUY"),
            trigger_type=self._candidate_trigger_type(signal),
            source_wallet_count=source_wallet_count,
            observed_notional=float(signal.observed_notional or 0.0),
            observed_size=float(signal.observed_size or 0.0),
            source_avg_price=float(signal.price_hint or 0.0),
            current_best_bid=market_context.get("current_best_bid"),  # type: ignore[arg-type]
            current_best_ask=market_context.get("current_best_ask"),  # type: ignore[arg-type]
            current_midpoint=market_context.get("current_midpoint"),  # type: ignore[arg-type]
            spread_pct=market_context.get("spread_pct"),  # type: ignore[arg-type]
            momentum_5m=market_context.get("momentum_5m"),  # type: ignore[arg-type]
            momentum_30m=market_context.get("momentum_30m"),  # type: ignore[arg-type]
            chase_pct=market_context.get("chase_pct"),  # type: ignore[arg-type]
            market_time_source=str(market_context.get("market_time_source") or "unknown"),
            market_metadata_hit=bool(market_context.get("market_metadata_hit", False)),
            market_tag=str(signal.topic_label or signal.topic_key or ""),
            resolution_bucket=str(signal.topic_bias or ""),
            confidence=float(signal.confidence or 0.0),
            score=score,
            suggested_action=suggested_action,
            skip_reason=skip_reason,
            recommendation_reason=recommendation_reason,
            explanation=explanation,
            reason_factors=reason_factors,
            has_existing_position=existing is not None and float(existing.get("notional") or 0.0) > 0.0,
            existing_position_conflict=skip_reason in REPEAT_ENTRY_BLOCK_REASONS or skip_reason == "existing_position_conflict",
            existing_position_notional=float((existing or {}).get("notional") or 0.0),
            block_reason=str(skip_reason or ""),
            block_layer="candidate" if skip_reason else "",
            status=status,
            lifecycle_state="active",
            created_ts=created_ts,
            expires_ts=expires_ts,
            updated_ts=now,
            signal_snapshot=self._signal_snapshot(signal),
            topic_snapshot=self._topic_snapshot(signal),
        )

    def _sync_wallet_profiles(self, wallets: list[str]) -> None:
        if self.candidate_store is None or not wallets:
            return
        metrics_getter = getattr(self.strategy, "latest_wallet_metrics", None)
        latest_metrics = metrics_getter() if callable(metrics_getter) else {}
        now = int(time.time())
        for wallet in wallets:
            row = dict(latest_metrics.get(wallet, {}) or {})
            selection = dict(self._cached_wallet_selection_context.get(wallet, {}) or {})
            score_summary = str(row.get("score_summary") or selection.get("discovery_priority_reason") or "")
            profile = WalletProfile(
                wallet=wallet,
                tag=str(row.get("wallet_tier") or "WATCH"),
                trust_score=float(row.get("wallet_score") or 0.0),
                followability_score=float(selection.get("discovery_priority_score") or row.get("wallet_score") or 0.0),
                avg_hold_minutes=None,
                category=str(selection.get("discovery_best_topic") or ""),
                enabled=bool(row.get("trading_enabled", True)),
                notes=score_summary,
                updated_ts=now,
                payload={
                    "score_summary": score_summary,
                    "trading_enabled": bool(row.get("trading_enabled", True)),
                    "topic_profiles": list(row.get("topic_profiles") or [])[:3],
                },
            )
            self.candidate_store.upsert_wallet_profile(profile)

    def _persist_candidates(self, candidates: list[Candidate]) -> None:
        if self.candidate_store is None:
            return
        for candidate in candidates:
            self.candidate_store.upsert_candidate(candidate)

    def _refresh_active_pending_candidates(self, *, now: int) -> None:
        if self.candidate_store is None:
            return
        active_candidates = list(
            self.candidate_store.list_candidates(
                statuses=["pending", "approved", "queued", "watched", "requested", "submitted"],
                limit=1000,
                include_expired=True,
            )
            or []
        )
        if not active_candidates:
            return

        refreshed = 0
        expired = 0
        for payload in active_candidates:
            candidate_id = str(payload.get("id") or "")
            if not candidate_id:
                continue
            expired_now, expires_ts = self._candidate_is_expired(payload, now_ts=now)
            if expired_now:
                self.candidate_store.update_candidate_status(
                    candidate_id,
                    status="expired",
                    note="candidate_lifecycle:expired",
                    result_tag=REASON_CANDIDATE_LIFETIME_EXPIRED,
                    block_reason=REASON_CANDIDATE_LIFETIME_EXPIRED,
                    block_layer="candidate",
                    lifecycle_state="expired_discarded",
                    expires_ts=expires_ts if expires_ts > 0 else None,
                    updated_ts=now,
                )
                expired += 1
                continue
            signal = self._candidate_to_signal(payload)
            if signal is None:
                self.candidate_store.update_candidate_status(
                    candidate_id,
                    status="expired",
                    note="candidate_refresh:snapshot_missing",
                    result_tag="candidate_snapshot_missing",
                    block_reason="candidate_snapshot_missing",
                    block_layer="candidate",
                    lifecycle_state="expired_discarded",
                    updated_ts=now,
                )
                expired += 1
                continue
            if str(signal.side or "").upper() != "BUY":
                continue
            if str(payload.get("status") or "").strip().lower() != "pending":
                continue

            existing = self._position_truth_for_token(signal.token_id)
            market_context = self._candidate_market_context(
                signal.token_id,
                price_hint=float(signal.price_hint or 0.0),
                market_slug=str(signal.market_slug or ""),
                condition_id=str(signal.condition_id or ""),
            )
            refreshed_candidate = self._candidate_from_signal(
                signal,
                now=now,
                existing=existing,
                market_context=market_context,
            )
            refreshed_candidate.id = candidate_id
            refreshed_candidate.signal_id = str(payload.get("signal_id") or refreshed_candidate.signal_id or candidate_id)
            refreshed_candidate.status = str(payload.get("status") or refreshed_candidate.status or "pending")
            refreshed_candidate.lifecycle_state = str(payload.get("lifecycle_state") or refreshed_candidate.lifecycle_state or "active")
            refreshed_candidate.selected_action = str(payload.get("selected_action") or refreshed_candidate.selected_action or "")
            refreshed_candidate.note = str(payload.get("note") or refreshed_candidate.note or "")
            refreshed_candidate.created_ts = int(payload.get("created_ts") or refreshed_candidate.created_ts or now)
            refreshed_candidate.expires_ts = expires_ts if expires_ts > 0 else refreshed_candidate.expires_ts
            refreshed_candidate.updated_ts = now

            if refreshed_candidate.skip_reason:
                self.candidate_store.update_candidate_status(
                    candidate_id,
                    status="expired",
                    selected_action=refreshed_candidate.suggested_action,
                    note=f"candidate_refresh:{refreshed_candidate.skip_reason}",
                    result_tag=f"candidate_revalidated_{refreshed_candidate.skip_reason}",
                    block_reason=str(refreshed_candidate.skip_reason or ""),
                    block_layer="candidate",
                    lifecycle_state="expired_discarded",
                    expires_ts=refreshed_candidate.expires_ts,
                    updated_ts=now,
                )
                expired += 1
                continue

            self.candidate_store.upsert_candidate(refreshed_candidate)
            refreshed += 1

        if refreshed > 0 or expired > 0:
            self.log.info(
                "Refreshed active pending candidates kept=%d expired=%d",
                refreshed,
                expired,
            )
            self._append_event(
                "candidate_refresh",
                {
                    "refreshed": refreshed,
                    "expired": expired,
                },
            )

    @staticmethod
    def _candidate_notification_key(candidate: Candidate) -> str:
        return "|".join(
            [
                str(candidate.wallet or "").strip().lower(),
                str(candidate.market_slug or "").strip().lower(),
                str(candidate.token_id or "").strip().lower(),
                str(candidate.side or "").strip().upper(),
                str(candidate.suggested_action or "").strip().lower(),
            ]
        )

    def _should_notify_candidate(self, candidate: Candidate) -> bool:
        if not bool(getattr(self.settings, "candidate_notification_enabled", False)):
            return False
        if candidate.skip_reason:
            return False
        if candidate.side != "BUY":
            return False
        if str(candidate.suggested_action or "").strip().lower() not in {"buy_normal", "follow"}:
            return False
        if float(candidate.score or 0.0) < float(getattr(self.settings, "candidate_notification_min_score", 84.0) or 84.0):
            return False
        return True

    def _notifier_channels(self, *, include_local: bool = True) -> list[str]:
        if self.notifier is None:
            return []
        channels: list[str] = []
        if (
            include_local
            and bool(getattr(self.settings, "notify_local_enabled", True))
            and bool(getattr(self.notifier, "local_available", lambda: False)())
        ):
            channels.append("local")
        if bool(getattr(self.notifier, "webhook_targets", lambda: [])()):
            channels.append("webhook")
        if bool(getattr(self.notifier, "telegram_available", lambda: False)()):
            channels.append("telegram")
        return channels

    @staticmethod
    def _notification_i18n_extra(
        *,
        title_key: str,
        title_params: Mapping[str, object] | None = None,
        body_key: str,
        body_params: Mapping[str, object] | None = None,
    ) -> dict[str, object]:
        return {
            "i18n": {
                "title_key": title_key,
                "title_params": dict(title_params or {}),
                "body_key": body_key,
                "body_params": dict(body_params or {}),
            }
        }

    @staticmethod
    def _notification_separator() -> str:
        return i18n_t("common.separator", fallback=" · ")

    @staticmethod
    def _wallet_tier_label(value: object) -> str:
        raw = str(value or "").strip()
        return i18n_label("enum.walletTier", raw or "unknown", fallback=(raw.upper() if raw else i18n_t("enum.walletTier.unknown")))

    @staticmethod
    def _candidate_action_label(value: object, side: object | None = None) -> str:
        raw = str(value or "").strip().lower()
        if raw == "follow" and str(side or "").strip().upper() == "SELL":
            raw = "follow_sell"
        return i18n_enum_label("enum.candidateAction", raw or "default", fallback=i18n_humanize_identifier(raw or "default"))

    @staticmethod
    def _action_tag_label(value: object) -> str:
        raw = str(value or "").strip().lower()
        return i18n_enum_label("enum.actionTag", raw or "event", fallback=i18n_humanize_identifier(raw or "event"))

    @staticmethod
    def _exit_kind_label(value: object) -> str:
        raw = str(value or "").strip().lower()
        return i18n_enum_label("enum.exitKind", raw or "entry", fallback=i18n_humanize_identifier(raw or "entry"))

    @staticmethod
    def _exit_result_label(value: object) -> str:
        raw = str(value or "").strip().lower()
        if raw == "pending":
            return i18n_t("orders.status.pending")
        return i18n_enum_label("enum.exitResult", raw or "unknown", fallback=i18n_humanize_identifier(raw or "unknown"))

    @staticmethod
    def _reason_label(value: object) -> str:
        raw = str(value or "").strip()
        if not raw:
            return ""
        return i18n_enum_label("enum.reason", raw, fallback=i18n_humanize_identifier(raw))

    def _format_notification_issue(self, issue: object) -> str:
        raw = str(issue or "").strip()
        if not raw:
            return ""
        match = re.match(r"^([A-Za-z0-9_]+)=(.*)$", raw)
        if not match:
            return self._reason_label(raw)
        metric_name = str(match.group(1) or "").strip()
        metric_value = str(match.group(2) or "").strip()
        label = i18n_label("metric", metric_name, fallback=i18n_humanize_identifier(metric_name))
        translated_value = metric_value
        normalized_metric = metric_name.lower()
        if normalized_metric in {"status", "reconciliation_status"}:
            translated_value = i18n_enum_label("enum.reportStatus", metric_value.lower(), fallback=i18n_humanize_identifier(metric_value))
        elif normalized_metric in {"reason", "primary_reason"}:
            translated_value = self._reason_label(metric_value)
        elif normalized_metric == "reasons":
            translated_value = self._notification_separator().join(
                part for part in (self._reason_label(piece) for piece in metric_value.split(",")) if part
            ) or metric_value
        return i18n_t(
            "common.kvInline",
            {"label": label, "value": translated_value or metric_value},
            fallback=f"{label}={translated_value or metric_value}",
        )

    def _format_notification_issues(self, issues: list[object]) -> str:
        parts = [self._format_notification_issue(item) for item in list(issues or [])[:3] if str(item or "").strip()]
        return self._notification_separator().join(part for part in parts if part) or i18n_t(
            "notification.tradingMode.reconciliationProtect.defaultIssues"
        )

    def _format_reason_codes(self, reasons: set[str]) -> str:
        parts = [self._reason_label(reason) for reason in sorted(reasons) if str(reason or "").strip()]
        return self._notification_separator().join(part for part in parts if part) or self._reason_label("unknown")

    def _notify_critical_condition(
        self,
        *,
        key: str,
        title: str,
        body: str,
        extra: Mapping[str, object] | None = None,
        now: int | None = None,
    ) -> bool:
        if self.notifier is None or not bool(getattr(self.settings, "critical_notification_enabled", True)):
            return False
        channels = self._notifier_channels()
        if not channels:
            return False
        now_ts = int(now or time.time())
        cooldown_seconds = max(
            30,
            int(getattr(self.settings, "critical_notification_cooldown_seconds", 900) or 900),
        )
        last_ts = int(self._critical_notification_watermarks.get(key) or 0)
        if last_ts > 0 and (now_ts - last_ts) < cooldown_seconds:
            return False
        try:
            self.notifier.notify_all(
                title=title,
                body=body,
                extra=dict(extra or {}),
                channels=channels,
            )
        except Exception as exc:
            self.log.warning("Failed to send critical notification key=%s err=%s", key, exc)
            return False
        self._critical_notification_watermarks[key] = now_ts
        return True

    def _maybe_notify_trading_mode_alert(
        self,
        *,
        trading_mode_state: Mapping[str, object],
        reconciliation: Mapping[str, object] | None = None,
        now: int | None = None,
    ) -> None:
        now_ts = int(now or time.time())
        mode = str(trading_mode_state.get("mode") or "NORMAL").upper() or "NORMAL"
        reasons = {
            str(value or "").strip()
            for value in (
                self._admission_decision.reason_codes
                if self._admission_decision and self._admission_decision.reason_codes
                else tuple(trading_mode_state.get("reason_codes", []) or [])
            )
            if str(value or "").strip()
        }
        if mode == "NORMAL":
            return

        reconciliation_payload = dict(reconciliation or {})
        reconciliation_status = str(reconciliation_payload.get("status") or self.reconciliation_status or "unknown").lower()
        ambiguous_pending_orders = int(reconciliation_payload.get("ambiguous_pending_orders") or 0)
        issues = list(reconciliation_payload.get("issues") or [])
        mode_label = i18n_enum_label("enum.trading_mode", mode.lower(), fallback=mode)

        if "persistence_fault" in reasons:
            failure = dict(self.last_persistence_failure or {})
            title_params = {"mode": mode_label}
            body_params = {
                "mode": mode_label,
                "kind": i18n_humanize_identifier(str(failure.get("kind") or "unknown")),
                "path": str(failure.get("path") or "--"),
                "error": str(failure.get("message") or i18n_t("notification.tradingMode.persistenceFault.errorUnknown")),
            }
            title = i18n_t("notification.tradingMode.persistenceFault.title", title_params)
            title = f"{title} | halted"
            body = i18n_t("notification.tradingMode.persistenceFault.body", body_params)
            self._notify_critical_condition(
                key=f"persistence_fault:{str(failure.get('kind') or 'unknown')}",
                title=title,
                body=body,
                extra={
                    "mode": mode,
                    "reason_codes": sorted(reasons),
                    "persistence": self.persistence_state(),
                    **self._notification_i18n_extra(
                        title_key="notification.tradingMode.persistenceFault.title",
                        title_params=title_params,
                        body_key="notification.tradingMode.persistenceFault.body",
                        body_params=body_params,
                    ),
                },
                now=now_ts,
            )

        if REASON_STARTUP_CHECKS_FAIL in reasons:
            title_params = {"mode": mode_label}
            body_params = {
                "failures": int(self.startup_failure_count or 0),
                "warnings": int(self.startup_warning_count or 0),
            }
            title = i18n_t("notification.tradingMode.startupGateBlocked.title", title_params)
            title = f"{title} | startup gate blocked"
            body = i18n_t("notification.tradingMode.startupGateBlocked.body", body_params)
            self._notify_critical_condition(
                key="startup_gate_blocked",
                title=title,
                body=body,
                extra={
                    "mode": mode,
                    "reason_codes": sorted(reasons),
                    "startup_failure_count": int(self.startup_failure_count or 0),
                    "startup_warning_count": int(self.startup_warning_count or 0),
                    **self._notification_i18n_extra(
                        title_key="notification.tradingMode.startupGateBlocked.title",
                        title_params=title_params,
                        body_key="notification.tradingMode.startupGateBlocked.body",
                        body_params=body_params,
                    ),
                },
                now=now_ts,
            )

        if (
            REASON_STALE_ACCOUNT_SNAPSHOT in reasons
            or REASON_STALE_BROKER_EVENT_STREAM in reasons
        ):
            title_params = {"mode": mode_label}
            body_params = {
                "accountState": i18n_enum_label("enum.accountState", str(self.account_state_status or "unknown"), fallback=i18n_humanize_identifier(str(self.account_state_status or "unknown"))),
            }
            title = i18n_t("notification.tradingMode.accountStateProtect.title", title_params)
            body = i18n_t("notification.tradingMode.accountStateProtect.body", body_params)
            self._notify_critical_condition(
                key=f"account_state:{str(self.account_state_status or 'unknown')}",
                title=title,
                body=body,
                extra={
                    "mode": mode,
                    "reason_codes": sorted(reasons),
                    "account_state_status": str(self.account_state_status or "unknown"),
                    **self._notification_i18n_extra(
                        title_key="notification.tradingMode.accountStateProtect.title",
                        title_params=title_params,
                        body_key="notification.tradingMode.accountStateProtect.body",
                        body_params=body_params,
                    ),
                },
                now=now_ts,
            )

        if (
            "reconciliation_fail" in reasons
            or "reconciliation_warn" in reasons
            or REASON_LEDGER_DIFF_EXCEEDED in reasons
            or REASON_AMBIGUOUS_PENDING_UNRESOLVED in reasons
            or REASON_RECOVERY_CONFLICT_UNRESOLVED in reasons
            or REASON_RECOVERY_CONFLICT_UNRESOLVED_MANUAL in reasons
            or ambiguous_pending_orders > 0
        ):
            issue_text = self._format_notification_issues(issues)
            title_params = {"mode": mode_label}
            body_params = {
                "status": i18n_enum_label("enum.reportStatus", reconciliation_status or "unknown", fallback=i18n_humanize_identifier(reconciliation_status or "unknown")),
                "ambiguousPendingOrders": ambiguous_pending_orders,
                "issues": issue_text,
            }
            title = i18n_t("notification.tradingMode.reconciliationProtect.title", title_params)
            title = f"{title} | reconciliation protect"
            body = i18n_t("notification.tradingMode.reconciliationProtect.body", body_params)
            body = f"{body} | ambiguous_pending_orders={ambiguous_pending_orders}"
            self._notify_critical_condition(
                key=(
                    "reconciliation:"
                    f"{reconciliation_status or 'unknown'}:"
                    f"{1 if ambiguous_pending_orders > 0 else 0}"
                ),
                title=title,
                body=body,
                extra={
                    "mode": mode,
                    "reason_codes": sorted(reasons),
                    "reconciliation": reconciliation_payload,
                    **self._notification_i18n_extra(
                        title_key="notification.tradingMode.reconciliationProtect.title",
                        title_params=title_params,
                        body_key="notification.tradingMode.reconciliationProtect.body",
                        body_params=body_params,
                    ),
                },
                now=now_ts,
            )

        if mode == "HALTED" and "persistence_fault" not in reasons:
            primary_reason = next(iter(sorted(reasons)), "unknown")
            title_params = {}
            body_params = {
                "primaryReason": self._reason_label(primary_reason),
                "reasons": self._format_reason_codes(reasons),
            }
            title = i18n_t("notification.tradingMode.haltedTradingStopped.title", title_params)
            body = i18n_t("notification.tradingMode.haltedTradingStopped.body", body_params)
            self._notify_critical_condition(
                key=f"halted:{primary_reason}",
                title=title,
                body=body,
                extra={
                    "mode": mode,
                    "reason_codes": sorted(reasons),
                    **self._notification_i18n_extra(
                        title_key="notification.tradingMode.haltedTradingStopped.title",
                        title_params=title_params,
                        body_key="notification.tradingMode.haltedTradingStopped.body",
                        body_params=body_params,
                    ),
                },
                now=now_ts,
            )

    def _notify_candidates(self, candidates: list[Candidate]) -> None:
        if not candidates or self.notifier is None:
            return
        cooldown_seconds = max(30, int(getattr(self.settings, "candidate_notification_cooldown_seconds", 900) or 900))
        now = int(time.time())
        for candidate in candidates:
            if not self._should_notify_candidate(candidate):
                continue
            key = self._candidate_notification_key(candidate)
            last_ts = int(self._candidate_notification_watermarks.get(key) or 0)
            if last_ts > 0 and (now - last_ts) < cooldown_seconds:
                continue
            action_value = str(candidate.suggested_action or "").strip().lower()
            if action_value == "follow" and str(candidate.side or "").upper() == "SELL":
                action_value = "follow_sell"
            action_label = i18n_enum_label("enum.candidateAction", action_value, fallback=str(candidate.suggested_action or "").upper())
            wallet_label = str(candidate.wallet_tag or "").strip() or i18n_label(
                "enum.walletTier",
                str(candidate.wallet_tier or "WATCH"),
                fallback=str(candidate.wallet_tier or "WATCH").upper(),
            )
            title_params = {
                "action": action_label,
                "market": candidate.market_slug or candidate.token_id or "-",
            }
            spread_text = i18n_t("notification.candidate.spread", {"value": f"{float(candidate.spread_pct):.2f}"}) if candidate.spread_pct is not None else ""
            chase_text = i18n_t("notification.candidate.chase", {"value": f"{float(candidate.chase_pct):.2f}"}) if candidate.chase_pct is not None else ""
            reason_text = i18n_t("notification.candidate.reason", {"value": str(candidate.recommendation_reason or "")}) if candidate.recommendation_reason else ""
            body_params = {
                "walletLabel": wallet_label,
                "walletScore": f"{float(candidate.wallet_score or 0.0):.1f}",
                "outcome": candidate.outcome or "--",
                "notional": f"{float(candidate.observed_notional or 0.0):.2f}",
                "spread": spread_text,
                "chase": chase_text,
                "reason": reason_text,
            }
            title = i18n_t("notification.candidate.title", title_params)
            body = i18n_t("notification.candidate.body", body_params)
            extra = {
                "candidate_id": candidate.id,
                "wallet": candidate.wallet,
                "market_slug": candidate.market_slug,
                "token_id": candidate.token_id,
                "suggested_action": candidate.suggested_action,
                "score": candidate.score,
                **self._notification_i18n_extra(
                    title_key="notification.candidate.title",
                    title_params=title_params,
                    body_key="notification.candidate.body",
                    body_params=body_params,
                ),
            }
            channels = self._notifier_channels()
            if not channels:
                continue
            self.notifier.notify_all(
                title=title,
                body=body,
                extra=extra,
                channels=channels,
            )
            self._candidate_notification_watermarks[key] = now

    def _candidate_to_signal(self, candidate: Mapping[str, object]) -> Signal | None:
        snapshot = dict(candidate.get("signal_snapshot") or {})
        if not snapshot:
            return None
        ts = self._parse_iso_timestamp(snapshot.get("timestamp"))
        when = datetime.fromtimestamp(max(0, ts or int(time.time())), tz=timezone.utc)
        topic_snapshot = dict(candidate.get("topic_snapshot") or {})
        return Signal(
            signal_id=str(snapshot.get("signal_id") or candidate.get("signal_id") or ""),
            trace_id=str(snapshot.get("trace_id") or candidate.get("trace_id") or ""),
            wallet=str(snapshot.get("wallet") or candidate.get("wallet") or ""),
            market_slug=str(snapshot.get("market_slug") or candidate.get("market_slug") or ""),
            token_id=str(snapshot.get("token_id") or candidate.get("token_id") or ""),
            outcome=str(snapshot.get("outcome") or candidate.get("outcome") or ""),
            side=str(snapshot.get("side") or candidate.get("side") or "BUY"),
            confidence=float(snapshot.get("confidence") or candidate.get("confidence") or 0.0),
            price_hint=float(snapshot.get("price_hint") or candidate.get("source_avg_price") or 0.0),
            observed_size=float(snapshot.get("observed_size") or candidate.get("observed_size") or 0.0),
            observed_notional=float(snapshot.get("observed_notional") or candidate.get("observed_notional") or 0.0),
            timestamp=when,
            condition_id=str(snapshot.get("condition_id") or candidate.get("condition_id") or ""),
            wallet_score=float(snapshot.get("wallet_score") or candidate.get("wallet_score") or 0.0),
            wallet_tier=str(snapshot.get("wallet_tier") or candidate.get("wallet_tier") or "LOW"),
            wallet_score_summary=str(snapshot.get("wallet_score_summary") or ""),
            topic_key=str(topic_snapshot.get("topic_key") or ""),
            topic_label=str(topic_snapshot.get("topic_label") or candidate.get("market_tag") or ""),
            topic_sample_count=int(topic_snapshot.get("topic_sample_count") or 0),
            topic_win_rate=float(topic_snapshot.get("topic_win_rate") or 0.0),
            topic_roi=float(topic_snapshot.get("topic_roi") or 0.0),
            topic_resolved_win_rate=float(topic_snapshot.get("topic_resolved_win_rate") or 0.0),
            topic_score_summary=str(topic_snapshot.get("topic_score_summary") or ""),
            topic_bias=str(topic_snapshot.get("topic_bias") or candidate.get("resolution_bucket") or "neutral"),
            topic_multiplier=float(topic_snapshot.get("topic_multiplier") or 1.0),
            exit_fraction=float(snapshot.get("exit_fraction") or 0.0),
            exit_reason=str(snapshot.get("exit_reason") or ""),
            cross_wallet_exit=bool(snapshot.get("cross_wallet_exit", False)),
            exit_wallet_count=int(snapshot.get("exit_wallet_count") or candidate.get("source_wallet_count") or 0),
            position_action=str(snapshot.get("position_action") or candidate.get("trigger_type") or ""),
            position_action_label=str(snapshot.get("position_action_label") or ""),
        )

    def _claim_approved_candidate_plans(self) -> list[dict[str, object]]:
        if self.candidate_store is None:
            return []
        plans: list[dict[str, object]] = []
        now_ts = int(time.time())
        for candidate in self.candidate_store.list_candidates(statuses=["approved"], limit=self.settings.max_signals_per_cycle):
            expired_now, expires_ts = self._candidate_is_expired(candidate, now_ts=now_ts)
            if expired_now:
                self.candidate_store.update_candidate_status(
                    str(candidate.get("id") or ""),
                    status="expired",
                    note="candidate_lifecycle:approved_queue_expired",
                    result_tag=REASON_CANDIDATE_LIFETIME_EXPIRED,
                    block_reason=REASON_CANDIDATE_LIFETIME_EXPIRED,
                    block_layer="decision",
                    lifecycle_state="expired_discarded",
                    expires_ts=expires_ts if expires_ts > 0 else None,
                    updated_ts=now_ts,
                )
                continue
            signal = self._candidate_to_signal(candidate)
            if signal is None:
                self.candidate_store.update_candidate_status(
                    str(candidate.get("id") or ""),
                    status="expired",
                    result_tag="candidate_snapshot_missing",
                    block_reason="candidate_snapshot_missing",
                    block_layer="decision",
                    lifecycle_state="expired_discarded",
                    updated_ts=now_ts,
                )
                continue
            candidate_id = str(candidate.get("id") or "")
            self.candidate_store.update_candidate_status(
                candidate_id,
                status="queued",
                selected_action=str(candidate.get("selected_action") or ""),
                lifecycle_state="active",
                updated_ts=now_ts,
            )
            plans.append(
                {
                    "signal": signal,
                    "candidate_id": candidate_id,
                    "candidate_action": str(candidate.get("selected_action") or ""),
                    "candidate_note": str(candidate.get("note") or ""),
                    "origin": "approved_queue",
                }
            )
        return plans

    def _auto_candidate_plans(self, candidates: list[Candidate], signals: list[Signal], mode: str) -> list[dict[str, object]]:
        if mode not in {"semi_auto", "auto"}:
            return []
        candidate_by_id = {candidate.id: candidate for candidate in candidates}
        plans: list[dict[str, object]] = []
        for signal in signals:
            candidate = candidate_by_id.get(str(signal.signal_id or ""))
            if candidate is None:
                continue
            if candidate.skip_reason:
                continue
            suggested_action = str(candidate.suggested_action or "")
            if signal.side == "BUY":
                if suggested_action not in {"buy_small", "buy_normal", "follow"}:
                    continue
            elif suggested_action not in {"close_partial", "close_all"}:
                continue
            if mode == "semi_auto":
                if signal.side == "BUY":
                    if not (
                        candidate.score >= float(self.settings.candidate_auto_min_score)
                        and candidate.wallet_score >= float(self.settings.candidate_auto_min_wallet_score)
                        and str(candidate.wallet_tier or "").upper() in {"HIGH", "CORE"}
                        and suggested_action in {"buy_normal", "follow"}
                    ):
                        continue
                elif suggested_action != "close_all":
                    continue
            if self.candidate_store is not None:
                self.candidate_store.update_candidate_status(
                    candidate.id,
                    status="queued",
                    selected_action=suggested_action,
                    updated_ts=int(time.time()),
                )
            plans.append(
                {
                    "signal": signal,
                    "candidate_id": candidate.id,
                    "candidate_action": suggested_action,
                    "candidate_note": "",
                    "origin": f"{mode}_fresh",
                }
            )
        return plans

    def _candidate_action_multiplier(self, action: str) -> float:
        normalized = str(action or "").strip().lower()
        if normalized == "buy_small":
            return float(self.settings.candidate_buy_small_fraction)
        if normalized == "buy_normal":
            return float(self.settings.candidate_buy_normal_fraction)
        if normalized == "follow":
            return float(self.settings.candidate_follow_fraction)
        if normalized == "close_partial":
            return float(self.settings.candidate_close_partial_fraction)
        return 1.0

    def _apply_candidate_action_sizing(
        self,
        *,
        signal: Signal,
        action: str,
        requested_notional: float,
        existing: Mapping[str, object] | None,
    ) -> float:
        normalized = str(action or "").strip().lower()
        if normalized in {"", "watch", "ignore"}:
            return 0.0
        multiplier = self._candidate_action_multiplier(normalized)
        if signal.side == "SELL":
            position_notional = max(0.0, float((existing or {}).get("notional") or 0.0))
            if normalized == "close_partial":
                return min(position_notional * multiplier, requested_notional)
            return min(position_notional, requested_notional)
        return requested_notional * multiplier

    @staticmethod
    def _should_apply_candidate_action_sizing(origin: object, action: object) -> bool:
        normalized_origin = str(origin or "").strip().lower()
        normalized_action = str(action or "").strip().lower()
        if normalized_origin != "approved_queue":
            return False
        return normalized_action not in {"", "watch", "ignore"}

    def _record_candidate_result(
        self,
        candidate_id: str,
        *,
        action: str,
        status: str,
        rationale: str,
        result_tag: str,
        signal: Signal,
        pnl_realized: float | None = None,
    ) -> None:
        if self.candidate_store is None or not candidate_id:
            return
        self.candidate_store.update_candidate_status(
            candidate_id,
            status=status,
            selected_action=action,
            result_tag=result_tag,
            updated_ts=int(time.time()),
        )
        self.candidate_store.append_journal_entry(
            JournalEntry(
                candidate_id=candidate_id,
                action=action,
                rationale=rationale,
                result_tag=result_tag,
                created_ts=int(time.time()),
                market_slug=str(signal.market_slug or ""),
                wallet=str(signal.wallet or ""),
                pnl_realized=pnl_realized,
            )
        )

    @staticmethod
    def _utc_day_key(ts: int | None = None) -> str:
        when = int(time.time()) if ts is None else int(ts)
        return datetime.fromtimestamp(max(0, when), tz=timezone.utc).strftime("%Y-%m-%d")

    @staticmethod
    def _parse_iso_timestamp(value: object) -> int:
        text = str(value or "").strip()
        if not text:
            return 0
        try:
            if text.endswith("Z"):
                text = f"{text[:-1]}+00:00"
            return int(datetime.fromisoformat(text).timestamp())
        except ValueError:
            return 0

    def _roll_daily_state_if_needed(self, now: int | None = None) -> None:
        day_key = self._utc_day_key(now)
        if not self._active_day_key:
            self._active_day_key = day_key
            return
        if day_key == self._active_day_key:
            return
        self._active_day_key = day_key
        self.state.daily_realized_pnl = 0.0
        self.state.broker_closed_pnl_today = 0.0

    def _append_ledger_entry(self, entry_type: str, payload: dict[str, object]) -> None:
        path = str(Path(str(self.settings.ledger_path or "").strip()).expanduser())
        if not path:
            return
        try:
            append_ledger_entry(path, entry_type, payload, broker=type(self.broker).__name__)
        except Exception as exc:
            self.log.warning("Failed to append ledger path=%s err=%s", path, exc)
            self._record_persistence_fault(kind="ledger_append", path=path, error=exc)

    def _ledger_broker_name(self) -> str:
        return type(self.broker).__name__

    def _recover_daily_realized_pnl_from_ledger(self, day_key: str) -> float | None:
        path = str(Path(str(self.settings.ledger_path or "").strip()).expanduser())
        if not path:
            return None
        try:
            total = 0.0
            found_fill = False
            found_rows = False
            for row in load_ledger_rows(path, day_key=day_key, broker=self._ledger_broker_name()):
                found_rows = True
                if str(row.get("type") or "") != "fill":
                    continue
                total += self._safe_float(row.get("realized_pnl"))
                found_fill = True
            if found_fill:
                return total
            return 0.0 if found_rows else None
        except FileNotFoundError:
            return None
        except Exception as exc:
            self.log.warning("Failed to recover ledger pnl path=%s err=%s", path, exc)
            return None

    def _ledger_day_summary(self, day_key: str) -> dict[str, object]:
        path = str(Path(str(self.settings.ledger_path or "").strip()).expanduser())
        summary = {
            "available": False,
            "path": path,
            "fill_count": 0,
            "fill_notional": 0.0,
            "realized_pnl": 0.0,
            "account_sync_count": 0,
            "startup_checks_count": 0,
            "last_fill_ts": 0,
            "last_account_sync_ts": 0,
            "last_startup_checks_ts": 0,
        }
        if not path:
            return summary
        try:
            for row in load_ledger_rows(path, day_key=day_key, broker=self._ledger_broker_name()):
                summary["available"] = True
                entry_type = str(row.get("type") or "")
                ts = int(self._safe_float(row.get("ts"), 0.0))
                if entry_type == "fill":
                    summary["fill_count"] = int(summary["fill_count"]) + 1
                    summary["fill_notional"] = float(summary["fill_notional"]) + self._safe_float(row.get("notional"))
                    summary["realized_pnl"] = float(summary["realized_pnl"]) + self._safe_float(row.get("realized_pnl"))
                    summary["last_fill_ts"] = max(int(summary["last_fill_ts"]), ts)
                elif entry_type == "account_sync":
                    summary["account_sync_count"] = int(summary["account_sync_count"]) + 1
                    summary["last_account_sync_ts"] = max(int(summary["last_account_sync_ts"]), ts)
                elif entry_type == "startup_checks":
                    summary["startup_checks_count"] = int(summary["startup_checks_count"]) + 1
                    summary["last_startup_checks_ts"] = max(int(summary["last_startup_checks_ts"]), ts)
        except FileNotFoundError:
            return summary
        except Exception as exc:
            self.log.warning("Failed to summarize ledger path=%s err=%s", path, exc)
        return summary

    @staticmethod
    def _position_cost_basis_notional(position: dict[str, object] | None) -> float:
        source = position or {}
        basis = float(source.get("cost_basis_notional") or 0.0)
        if basis > 0.0:
            return basis
        return max(0.0, float(source.get("notional") or 0.0))

    def _realize_position_sell(
        self,
        position: dict[str, object] | None,
        *,
        sold_qty: float,
        sold_notional: float,
    ) -> tuple[float, float]:
        source = position or {}
        prev_qty = max(0.0, float(source.get("quantity") or 0.0))
        prev_cost_basis = self._position_cost_basis_notional(source)
        if prev_qty <= 0.0 or sold_qty <= 0.0 or prev_cost_basis <= 0.0:
            remaining_cost_basis = max(0.0, prev_cost_basis - sold_notional)
            return (0.0, remaining_cost_basis)

        sold_fraction = min(1.0, sold_qty / prev_qty)
        realized_cost_basis = min(prev_cost_basis, prev_cost_basis * sold_fraction)
        realized_pnl = float(sold_notional) - realized_cost_basis
        remaining_cost_basis = max(0.0, prev_cost_basis - realized_cost_basis)
        return (realized_pnl, remaining_cost_basis)

    def _record_fill_ledger(
        self,
        *,
        ts: int,
        side: str,
        token_id: str,
        condition_id: str,
        market_slug: str,
        quantity: float,
        notional: float,
        price: float,
        realized_pnl: float,
        signal_id: str,
        trace_id: str,
        order_id: str,
        status: str,
        source_wallet: str,
        source: str,
    ) -> None:
        self._append_ledger_entry(
            "fill",
            {
                "ts": ts,
                "side": side,
                "token_id": token_id,
                "condition_id": condition_id,
                "market_slug": market_slug,
                "quantity": quantity,
                "notional": notional,
                "price": price,
                "realized_pnl": realized_pnl,
                "signal_id": signal_id,
                "trace_id": trace_id,
                "order_id": order_id,
                "status": status,
                "source_wallet": source_wallet,
                "source": source,
            },
        )

    def _apply_realized_pnl(self, realized_pnl: float, *, ts: int) -> None:
        self._roll_daily_state_if_needed(ts)
        current_breaker = RiskBreakerState.from_payload(self._risk_breaker_state)
        epsilon = 1e-9
        if realized_pnl < -epsilon:
            current_breaker.loss_streak_count = int(current_breaker.loss_streak_count or 0) + 1
        elif realized_pnl > epsilon:
            current_breaker.loss_streak_count = 0
        current_breaker.loss_streak_limit = max(1, int(getattr(self.settings, "loss_streak_breaker_limit", 1) or 1))
        current_breaker.loss_streak_blocked = current_breaker.loss_streak_count >= current_breaker.loss_streak_limit
        self._risk_breaker_state = current_breaker.to_payload()
        if abs(realized_pnl) <= epsilon:
            return
        self.state.daily_realized_pnl += realized_pnl

    def _apply_reconciled_realized_pnl(
        self,
        realized_pnl: float,
        *,
        fill_ts: int,
        now: int,
    ) -> None:
        if abs(realized_pnl) <= 1e-9:
            return
        applied_ts = int(fill_ts or now or time.time())
        fill_day_key = self._utc_day_key(applied_ts)
        current_day_key = self._utc_day_key(now)
        if fill_day_key != current_day_key:
            self.log.warning(
                "Late fill reconciled for prior day fill_day=%s current_day=%s realized_pnl=%.6f",
                fill_day_key,
                current_day_key,
                realized_pnl,
            )
            self._append_event(
                "late_fill_prior_day",
                {
                    "fill_day_key": fill_day_key,
                    "current_day_key": current_day_key,
                    "fill_ts": applied_ts,
                    "realized_pnl": realized_pnl,
                },
            )
            return
        self._apply_realized_pnl(realized_pnl, ts=applied_ts)

    @staticmethod
    def _is_missing_orderbook_message(message: object) -> bool:
        text = str(message or "").strip().lower()
        if not text:
            return False
        return (
            "no orderbook exists" in text
            or "book?token_id" in text
            or "order book unavailable" in text
        )

    def _apply_accounting_snapshot(self, snapshot: AccountingSnapshot | None) -> None:
        if snapshot is None:
            return
        self.state.cash_balance_usd = float(snapshot.cash_balance)
        self.state.positions_value_usd = float(snapshot.positions_value)
        self.state.equity_usd = float(snapshot.equity)
        self.state.account_snapshot_ts = self._parse_iso_timestamp(snapshot.valuation_time) or int(time.time())

    def _retire_position_from_account_snapshot(
        self,
        *,
        token_id: str,
        position: Mapping[str, object],
        snapshot: AccountingSnapshot | None,
        reason: str,
        now: int,
    ) -> bool:
        normalized_token = str(token_id or "").strip()
        if not normalized_token or snapshot is None:
            return False

        active_tokens = {
            str(getattr(account_pos, "token_id", "") or "").strip()
            for account_pos in tuple(snapshot.positions or ())
            if str(getattr(account_pos, "token_id", "") or "").strip()
        }
        if normalized_token in active_tokens:
            return False

        removed = self.positions_book.pop(normalized_token, None)
        if removed is None:
            return False

        self._apply_accounting_snapshot(snapshot)
        self._refresh_risk_state()
        market_slug = str(position.get("market_slug") or normalized_token)
        trace_id = str(position.get("trace_id") or "")
        self._append_event(
            "position_retired",
            {
                "token_id": normalized_token,
                "market_slug": market_slug,
                "reason": reason,
                "source": "account_snapshot",
            },
        )
        if self.settings.token_reentry_cooldown_seconds > 0:
            self.token_reentry_until[normalized_token] = now + self.settings.token_reentry_cooldown_seconds
        if trace_id:
            self._mark_trace_closed(trace_id, now)
        self.log.warning(
            "POSITION_RETIRED slug=%s token=%s reason=%s open_positions=%d",
            market_slug,
            normalized_token,
            reason,
            self.state.open_positions,
        )
        return True

    def _closed_pnl_today(self, wallet: str, day_start_ts: int) -> float | None:
        total = 0.0
        found = False
        for row in self.data_client.iter_closed_positions(
            wallet,
            page_size=50,
            max_pages=4,
            sort_by="TIMESTAMP",
            sort_direction="DESC",
        ):
            row_ts = int(getattr(row, "timestamp", 0) or 0)
            if row_ts and row_ts < day_start_ts:
                break
            if row_ts and row_ts >= day_start_ts:
                total += float(getattr(row, "realized_pnl", 0.0) or 0.0)
                found = True
        return total if found else 0.0

    def _sync_account_state(self) -> None:
        wallet = str(self.settings.funder_address or "").strip().lower()
        if self.settings.dry_run or not wallet:
            return

        snapshot: AccountingSnapshot | None = None
        closed_pnl_today: float | None = None
        day_start = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        day_start_ts = int(day_start.timestamp())
        try:
            snapshot = self.data_client.get_accounting_snapshot(wallet)
        except Exception as exc:
            self.log.warning("Accounting snapshot sync failed wallet=%s err=%s", wallet, exc)
        try:
            closed_pnl_today = self._closed_pnl_today(wallet, day_start_ts)
        except Exception as exc:
            self.log.warning("Closed-position pnl sync failed wallet=%s err=%s", wallet, exc)

        if snapshot is not None:
            self._apply_accounting_snapshot(snapshot)
        if closed_pnl_today is not None:
            self.state.broker_closed_pnl_today = float(closed_pnl_today)

        if snapshot is not None or closed_pnl_today is not None:
            self._append_event(
                "account_sync",
                {
                    "wallet": wallet,
                    "equity_usd": float(self.state.equity_usd),
                    "cash_balance_usd": float(self.state.cash_balance_usd),
                    "positions_value_usd": float(self.state.positions_value_usd),
                    "broker_closed_pnl_today": float(self.state.broker_closed_pnl_today),
                },
            )
            self._append_ledger_entry(
                "account_sync",
                {
                    "wallet": wallet,
                    "equity_usd": float(self.state.equity_usd),
                    "cash_balance_usd": float(self.state.cash_balance_usd),
                    "positions_value_usd": float(self.state.positions_value_usd),
                    "broker_closed_pnl_today": float(self.state.broker_closed_pnl_today),
                    "ts": int(time.time()),
                },
            )

    def _maybe_sync_account_state(self, *, force: bool = False) -> None:
        if self.settings.dry_run or not str(self.settings.funder_address or "").strip():
            return
        interval = int(self.settings.account_sync_refresh_seconds)
        if (not force) and interval > 0 and (time.time() - self._last_account_sync_ts) < interval:
            return
        self._last_account_sync_ts = time.time()
        self._sync_account_state()

    def _refresh_account_state_after_order(self, result: ExecutionResult) -> None:
        if self.settings.dry_run or not str(self.settings.funder_address or "").strip():
            return

        message = str(result.message or "").strip().lower()
        should_refresh = result.ok or "not enough balance / allowance" in message
        if not should_refresh:
            return

        self._maybe_sync_account_state(force=True)
        self._refresh_risk_state()

    def _append_event(self, event_type: str, payload: dict[str, object]) -> None:
        path = str(Path(str(self.settings.event_log_path or "").strip()).expanduser())
        if not path:
            return

        payload_record = {
            "ts": int(time.time()),
            "type": event_type,
            "broker": type(self.broker).__name__,
        }
        payload_record.update(payload)
        try:
            parent = Path(path).expanduser().parent
            if str(parent) not in {"", "."}:
                parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                json.dump(payload_record, f, ensure_ascii=False)
                f.write("\n")
        except Exception as exc:
            self.log.warning("Failed to append event log path=%s err=%s", path, exc)

    @staticmethod
    def _normalize_startup_check(row: object) -> dict[str, object] | None:
        if not isinstance(row, dict):
            return None
        name = str(row.get("name") or "").strip()
        if not name:
            return None
        status = str(row.get("status") or "").strip().upper() or "WARN"
        if status not in {"PASS", "WARN", "FAIL"}:
            status = "WARN"
        payload = {
            "name": name,
            "status": status,
            "message": str(row.get("message") or "").strip(),
        }
        details = row.get("details")
        if isinstance(details, dict) and details:
            payload["details"] = dict(details)
        return payload

    def _network_smoke_startup_check(self) -> dict[str, object]:
        path = os.getenv(
            "NETWORK_SMOKE_LOG",
            getattr(self.settings, "network_smoke_log_path", "/tmp/poly_network_smoke.jsonl"),
        )
        max_age_seconds = max(60, int(getattr(self.settings, "live_network_smoke_max_age_seconds", 43200) or 43200))
        try:
            with open(path, "r", encoding="utf-8") as f:
                last_line = ""
                for raw in f:
                    if raw.strip():
                        last_line = raw.strip()
            if not last_line:
                return {
                    "name": "network_smoke",
                    "status": "FAIL",
                    "message": f"network smoke log empty at {path}; run make network-smoke before live",
                }
            payload = json.loads(last_line)
            if not isinstance(payload, dict):
                raise ValueError("latest network smoke record is not an object")
            ts = int(self._safe_float(payload.get("ts"), 0.0))
            summary = payload.get("summary") if isinstance(payload.get("summary"), dict) else {}
            exit_code = int(self._safe_float(summary.get("exit_code"), 0.0))
            age_seconds = max(0, int(time.time()) - ts) if ts > 0 else 0
            if exit_code == 2:
                status = "FAIL"
                message = f"network smoke indicates geoblock/restriction (age={age_seconds}s)"
            elif exit_code == 1:
                status = "FAIL"
                message = f"network smoke indicates endpoint/connectivity failure (age={age_seconds}s)"
            elif ts <= 0:
                status = "FAIL"
                message = "network smoke log missing timestamp; rerun make network-smoke"
            elif age_seconds > max_age_seconds:
                status = "FAIL"
                message = f"network smoke is stale ({age_seconds}s old, max={max_age_seconds}s); rerun before live"
            else:
                status = "PASS"
                message = f"network smoke passed {age_seconds}s ago (max={max_age_seconds}s)"
            return {
                "name": "network_smoke",
                "status": status,
                "message": message,
                "details": {
                    "path": path,
                    "ts": ts,
                    "age_seconds": age_seconds,
                    "max_age_seconds": max_age_seconds,
                    "exit_code": exit_code,
                    "warnings": int(self._safe_float(summary.get("warnings"), 0.0)),
                    "blocks": int(self._safe_float(summary.get("blocks"), 0.0)),
                    "failures": int(self._safe_float(summary.get("failures"), 0.0)),
                },
            }
        except FileNotFoundError:
            return {
                "name": "network_smoke",
                "status": "FAIL",
                "message": f"network smoke log not found at {path}; run make network-smoke before live",
            }
        except Exception as exc:
            return {
                "name": "network_smoke",
                "status": "FAIL",
                "message": f"network smoke log unreadable at {path}: {exc}",
            }

    def _live_admission_startup_check(self) -> dict[str, object]:
        allowance_ready = bool(getattr(self.settings, "live_allowance_ready", False))
        geoblock_ready = bool(getattr(self.settings, "live_geoblock_ready", False))
        account_ready = bool(getattr(self.settings, "live_account_ready", False))
        ready = allowance_ready and geoblock_ready and account_ready
        return {
            "name": "live_admission",
            "status": "PASS" if ready else "FAIL",
            "message": (
                "live admission confirmed"
                if ready
                else "missing live admission confirmations; set LIVE_ALLOWANCE_READY, LIVE_GEOBLOCK_READY, and LIVE_ACCOUNT_READY to true before live"
            ),
            "details": {
                "allowance_ready": allowance_ready,
                "geoblock_ready": geoblock_ready,
                "account_ready": account_ready,
            },
        }

    def _run_startup_checks(self) -> None:
        raw_checks = []
        broker_startup_checks = getattr(self.broker, "startup_checks", None)
        broker_check_names: set[str] = set()
        if callable(broker_startup_checks):
            broker_checks = broker_startup_checks()
            if isinstance(broker_checks, list):
                raw_checks.extend(broker_checks)
                broker_check_names = {
                    str(row.get("name") or "").strip()
                    for row in broker_checks
                    if isinstance(row, dict) and str(row.get("name") or "").strip()
                }
        if not self.settings.dry_run:
            if not broker_check_names.intersection({"operator_prechecks", "live_admission"}):
                raw_checks.append(self._live_admission_startup_check())
            raw_checks.append(self._network_smoke_startup_check())
            cap_check = self._hot_wallet_cap_startup_check()
            if isinstance(cap_check, dict):
                raw_checks.append(cap_check)
        if self._recovery_block_buy_latched:
            raw_checks.append(
                {
                    "name": "recovery_conflict",
                    "status": "FAIL",
                    "message": f"startup recovery has unresolved conflicts ({len(self._recovery_conflicts)})",
                    "details": {
                        "count": int(len(self._recovery_conflicts)),
                        "conflicts": list(self._recovery_conflicts),
                    },
                }
            )

        normalized = []
        for row in raw_checks:
            item = self._normalize_startup_check(row)
            if item is not None:
                normalized.append(item)
        if not normalized:
            normalized = [
                {
                    "name": "startup",
                    "status": "PASS",
                    "message": "no explicit startup checks registered",
                }
            ]

        self.startup_checks = normalized
        self.startup_warning_count = sum(1 for row in normalized if str(row.get("status")) == "WARN")
        self.startup_failure_count = sum(1 for row in normalized if str(row.get("status")) == "FAIL")
        self.startup_ready = self.startup_failure_count == 0
        self._append_event(
            "startup_checks",
            {
                "ready": bool(self.startup_ready),
                "warning_count": int(self.startup_warning_count),
                "failure_count": int(self.startup_failure_count),
                "checks": list(self.startup_checks),
            },
        )
        self._append_ledger_entry(
            "startup_checks",
            {
                "ts": int(time.time()),
                "ready": bool(self.startup_ready),
                "warning_count": int(self.startup_warning_count),
                "failure_count": int(self.startup_failure_count),
                "checks": list(self.startup_checks),
            },
        )
        if self.startup_ready:
            self.log.info(
                "Startup checks passed warnings=%d broker=%s",
                self.startup_warning_count,
                type(self.broker).__name__,
            )
        else:
            self.log.warning(
                "Startup checks failed failures=%d warnings=%d broker=%s",
                self.startup_failure_count,
                self.startup_warning_count,
                type(self.broker).__name__,
            )

    def reconciliation_summary(self, *, now: int | None = None) -> dict[str, object]:
        now_ts = int(now or time.time())
        day_key = self._utc_day_key(now_ts)
        ledger_summary = self._ledger_day_summary(day_key)
        internal_pnl = float(self.state.daily_realized_pnl)
        ledger_pnl = float(ledger_summary.get("realized_pnl") or 0.0)
        broker_closed_pnl_today = float(self.state.broker_closed_pnl_today)
        internal_vs_ledger_diff = internal_pnl - ledger_pnl
        broker_floor_gap_vs_internal = internal_pnl - broker_closed_pnl_today
        account_snapshot_ts = int(self.state.account_snapshot_ts or 0)
        account_snapshot_age_seconds = max(0, now_ts - account_snapshot_ts) if account_snapshot_ts > 0 else 0
        broker_reconcile_age_seconds = (
            max(0, now_ts - int(self._last_broker_reconcile_ts))
            if self._last_broker_reconcile_ts > 0
            else 0
        )
        broker_event_sync_age_seconds = (
            max(0, now_ts - int(self._last_broker_event_sync_ts))
            if self._last_broker_event_sync_ts > 0
            else 0
        )
        pending_exit_orders = sum(1 for order in self.pending_orders.values() if str(order.get("side") or "").upper() == "SELL")
        stale_pending_orders = sum(
            1
            for order in self.pending_orders.values()
            if (now_ts - int(order.get("ts") or now_ts)) >= self._pending_timeout_seconds()
        )
        ambiguous_pending_orders = sum(
            1
            for order in self.pending_orders.values()
            if int(order.get("reconcile_ambiguous_ts") or 0) > 0
        )
        tolerance_usd = max(0.01, self.settings.bankroll_usd * 0.0025)
        issues: list[str] = []
        status = "ok"
        if abs(internal_vs_ledger_diff) > 0.01:
            status = "fail"
            issues.append(f"internal_vs_ledger_diff={internal_vs_ledger_diff:.2f}")
        if not self.startup_ready:
            status = "fail"
            issues.append(f"startup_failures={int(self.startup_failure_count)}")
        if stale_pending_orders > 0:
            if status != "fail":
                status = "warn"
            issues.append(f"stale_pending_orders={stale_pending_orders}")
        if ambiguous_pending_orders > 0:
            if status != "fail":
                status = "warn"
            issues.append(f"ambiguous_pending_orders={ambiguous_pending_orders}")
        if self._recovery_block_buy_latched:
            status = "fail"
            issues.append(f"recovery_conflicts={len(self._recovery_conflicts)}")
        if (
            not self.settings.dry_run
            and account_snapshot_age_seconds > max(600, int(self.settings.account_sync_refresh_seconds) * 2)
        ):
            if status != "fail":
                status = "warn"
            issues.append(f"account_snapshot_stale={account_snapshot_age_seconds}s")
        if (
            not self.settings.dry_run
            and broker_reconcile_age_seconds > max(600, int(self.settings.runtime_reconcile_interval_seconds) * 2)
        ):
            if status != "fail":
                status = "warn"
            issues.append(f"broker_reconcile_stale={broker_reconcile_age_seconds}s")
        if self.pending_orders and broker_event_sync_age_seconds > max(120, int(self.settings.poll_interval_seconds) * 2):
            if status != "fail":
                status = "warn"
            issues.append(f"broker_event_stream_stale={broker_event_sync_age_seconds}s")
        if (
            not self.settings.dry_run
            and broker_closed_pnl_today < (internal_pnl - tolerance_usd)
        ):
            if status != "fail":
                status = "warn"
            issues.append(f"broker_floor_gap={broker_floor_gap_vs_internal:.2f}")

        return {
            "day_key": day_key,
            "status": status,
            "issues": issues,
            "recovery_conflicts": list(self._recovery_conflicts),
            "startup_ready": bool(self.startup_ready),
            "internal_realized_pnl": internal_pnl,
            "ledger_realized_pnl": ledger_pnl,
            "broker_closed_pnl_today": broker_closed_pnl_today,
            "effective_daily_realized_pnl": float(self.state.effective_daily_realized_pnl),
            "internal_vs_ledger_diff": internal_vs_ledger_diff,
            "broker_floor_gap_vs_internal": broker_floor_gap_vs_internal,
            "fill_count_today": int(ledger_summary.get("fill_count") or 0),
            "fill_notional_today": float(ledger_summary.get("fill_notional") or 0.0),
            "account_sync_count_today": int(ledger_summary.get("account_sync_count") or 0),
            "startup_checks_count_today": int(ledger_summary.get("startup_checks_count") or 0),
            "last_fill_ts": int(ledger_summary.get("last_fill_ts") or 0),
            "last_account_sync_ts": int(ledger_summary.get("last_account_sync_ts") or 0),
            "last_startup_checks_ts": int(ledger_summary.get("last_startup_checks_ts") or 0),
            "pending_orders": int(len(self.pending_orders)),
            "pending_entry_orders": int(self.state.pending_entry_orders),
            "pending_exit_orders": int(pending_exit_orders),
            "stale_pending_orders": int(stale_pending_orders),
            "ambiguous_pending_orders": int(ambiguous_pending_orders),
            "open_positions": int(self.state.open_positions),
            "tracked_notional_usd": float(self.state.tracked_notional_usd),
            "ledger_available": bool(ledger_summary.get("available")),
            "account_snapshot_age_seconds": int(account_snapshot_age_seconds),
            "broker_reconcile_age_seconds": int(broker_reconcile_age_seconds),
            "broker_event_sync_age_seconds": int(broker_event_sync_age_seconds),
        }

    def _account_state_status(self, *, now: int | None = None) -> str:
        if self.settings.dry_run:
            return "fresh"
        if not str(self.settings.funder_address or "").strip():
            return "unknown"

        snapshot_ts = int(self.state.account_snapshot_ts or 0)
        if snapshot_ts <= 0:
            return "unknown"

        now_ts = int(now or time.time())
        snapshot_age_seconds = max(0, now_ts - snapshot_ts)
        stale_after_seconds = max(600, int(self.settings.account_sync_refresh_seconds) * 2)
        if snapshot_age_seconds > stale_after_seconds:
            return "stale"
        return "fresh"

    def _refresh_signer_security_snapshot(self) -> None:
        raw_key_detected = bool(
            (not bool(getattr(self.settings, "dry_run", True)))
            and str(getattr(self.settings, "private_key", "") or "").strip()
        )
        summary: dict[str, object] = {}
        broker_summary = getattr(self.broker, "security_summary", None)
        if callable(broker_summary):
            try:
                payload = broker_summary()
                if isinstance(payload, dict):
                    summary = dict(payload)
            except Exception:
                summary = {}
        snapshot = SignerStatusSnapshot(
            live_mode=not bool(getattr(self.settings, "dry_run", True)),
            signer_required=not bool(getattr(self.settings, "dry_run", True)),
            signer_mode=str(summary.get("signer_mode") or ("none" if bool(getattr(self.settings, "dry_run", True)) else "unknown")),
            signer_healthy=bool(summary.get("signer_healthy", bool(getattr(self.settings, "dry_run", True)))),
            signer_identity_matched=bool(summary.get("signer_identity_matched", bool(getattr(self.settings, "dry_run", True)))),
            api_identity_matched=bool(summary.get("api_identity_matched", bool(getattr(self.settings, "dry_run", True)))),
            broker_identity_matched=bool(
                summary.get(
                    "broker_identity_matched",
                    bool(str(getattr(self.settings, "funder_address", "") or "").strip() or getattr(self.settings, "dry_run", True)),
                )
            ),
            raw_key_detected=raw_key_detected,
            funder_identity_present=bool(str(getattr(self.settings, "funder_address", "") or "").strip()),
            api_creds_configured=bool(summary.get("api_creds_configured", bool(getattr(self.settings, "dry_run", True)))),
            hot_wallet_cap_enabled=bool(summary.get("hot_wallet_cap_enabled", False)),
            hot_wallet_cap_ok=bool(summary.get("hot_wallet_cap_ok", True)),
            hot_wallet_cap_limit_usd=float(summary.get("hot_wallet_cap_limit_usd") or 0.0),
            hot_wallet_cap_value_usd=float(summary.get("hot_wallet_cap_value_usd") or 0.0),
            reason_codes=list(summary.get("reason_codes") or []),
            last_checked_ts=int(summary.get("last_checked_ts") or int(time.time())),
        )
        payload = snapshot.as_state_payload()
        if raw_key_detected:
            reason_codes = list(payload.get("reason_codes") or [])
            if "raw_private_key_forbidden_live" not in reason_codes:
                reason_codes.append("raw_private_key_forbidden_live")
            payload["reason_codes"] = reason_codes
            payload["signer_healthy"] = False
        self._signer_security_snapshot = payload

    def signer_security_state(self) -> dict[str, object]:
        return dict(self._signer_security_snapshot or {})

    def _hot_wallet_cap_threshold_usd(self) -> float:
        return max(0.0, float(getattr(self.settings, "live_hot_wallet_balance_cap_usd", 0.0) or 0.0))

    def _hot_wallet_cap_state(self, *, now: int | None = None) -> dict[str, object]:
        now_ts = int(now or time.time())
        cap = self._hot_wallet_cap_threshold_usd()
        equity = max(0.0, float(self.state.equity_usd or 0.0))
        if equity <= 0.0:
            equity = max(
                0.0,
                float(self.state.cash_balance_usd or 0.0) + float(self.state.positions_value_usd or 0.0),
            )
        enabled = (not bool(self.settings.dry_run)) and cap > 0.0
        exceeded = bool(enabled and equity > cap)
        return {
            "enabled": bool(enabled),
            "exceeded": bool(exceeded),
            "cap_usd": float(cap),
            "equity_usd": float(equity),
            "now_ts": now_ts,
        }

    def _hot_wallet_cap_startup_check(self, *, now: int | None = None) -> dict[str, object] | None:
        state = self._hot_wallet_cap_state(now=now)
        if not bool(state.get("enabled", False)):
            return None
        cap_usd = float(state.get("cap_usd") or 0.0)
        equity_usd = float(state.get("equity_usd") or 0.0)
        exceeded = bool(state.get("exceeded", False))
        return {
            "name": "hot_wallet_cap",
            "status": "FAIL" if exceeded else "PASS",
            "message": (
                f"hot wallet equity {equity_usd:.2f} exceeds cap {cap_usd:.2f}"
                if exceeded
                else f"hot wallet cap ok ({equity_usd:.2f}/{cap_usd:.2f})"
            ),
            "details": {
                "cap_usd": cap_usd,
                "equity_usd": equity_usd,
                "exceeded": exceeded,
            },
        }

    def _enforce_hot_wallet_cap(self, *, now: int | None = None) -> None:
        state = self._hot_wallet_cap_state(now=now)
        signer_security = dict(self._signer_security_snapshot or {})
        signer_security["hot_wallet_cap_enabled"] = bool(state.get("enabled", False))
        signer_security["hot_wallet_cap_ok"] = not bool(state.get("exceeded", False))
        signer_security["hot_wallet_cap_limit_usd"] = float(state.get("cap_usd") or 0.0)
        signer_security["hot_wallet_cap_value_usd"] = float(state.get("equity_usd") or 0.0)
        signer_security["last_checked_ts"] = int(state.get("now_ts") or int(time.time()))
        reason_codes = list(signer_security.get("reason_codes") or [])
        if bool(state.get("exceeded", False)):
            if "hot_wallet_cap_exceeded" not in reason_codes:
                reason_codes.append("hot_wallet_cap_exceeded")
        else:
            reason_codes = [code for code in reason_codes if str(code) != "hot_wallet_cap_exceeded"]
        signer_security["reason_codes"] = reason_codes
        self._signer_security_snapshot = signer_security
        if not bool(state.get("enabled", False)) or not bool(state.get("exceeded", False)):
            return
        if self._hot_wallet_cap_conflict_latched:
            return
        self._record_recovery_conflict(
            category="HOT_WALLET_CAP_EXCEEDED",
            details=f"equity={float(state.get('equity_usd') or 0.0):.2f} cap={float(state.get('cap_usd') or 0.0):.2f}",
        )
        self._hot_wallet_cap_conflict_latched = True

    def persistence_state(self) -> dict[str, object]:
        return {
            "status": str(self.persistence_status or "ok"),
            "failure_count": int(self.persistence_failure_count or 0),
            "last_failure": dict(self.last_persistence_failure),
        }

    def runner_heartbeat_state(self) -> dict[str, object]:
        return normalize_runner_heartbeat(self._runner_heartbeat)

    def _record_persistence_fault(
        self,
        *,
        kind: str,
        path: str,
        error: object,
        now: int | None = None,
    ) -> dict[str, object]:
        fault_ts = int(now or time.time())
        message = str(error or "").strip() or "unknown persistence failure"
        self.persistence_status = "fault"
        self.persistence_failure_count = int(self.persistence_failure_count or 0) + 1
        self.last_persistence_failure = {
            "kind": str(kind or ""),
            "path": str(path or ""),
            "message": message,
            "ts": fault_ts,
        }
        self.log.error(
            "PERSISTENCE_FAULT kind=%s path=%s err=%s count=%d",
            kind,
            path,
            message,
            self.persistence_failure_count,
        )
        payload = self.persistence_state()
        self._append_event("persistence_fault", payload)
        self._update_trading_mode(self.control_state, now=fault_ts)
        self._refresh_risk_state()
        return payload

    def record_external_persistence_fault(self, kind: str, path: str, error: object) -> dict[str, object]:
        return self._record_persistence_fault(kind=kind, path=path, error=error)

    def trading_mode_state(self) -> dict[str, object]:
        mode = str(self.trading_mode or MODE_NORMAL).upper() or MODE_NORMAL
        return {
            "mode": mode,
            "opening_allowed": bool(self.admission_opening_allowed),
            "reason_codes": list(self.trading_mode_reasons),
            "updated_ts": int(self.trading_mode_updated_ts or 0),
            "source": "admission_gate_projection",
            "account_state_status": str(self.account_state_status or "unknown"),
            "reconciliation_status": str(self.reconciliation_status or "unknown"),
            "persistence_status": str(self.persistence_status or "ok"),
        }

    def admission_state(self) -> dict[str, object]:
        decision = self._admission_decision
        return {
            "mode": str(decision.mode or MODE_REDUCE_ONLY),
            "opening_allowed": bool(decision.opening_allowed),
            "reduce_only": bool(decision.reduce_only),
            "halted": bool(decision.halted),
            "auto_recover": bool(decision.auto_recover),
            "manual_confirmation_required": bool(decision.manual_confirmation_required),
            "reason_codes": list(decision.reason_codes),
            "action_whitelist": list(decision.action_whitelist),
            "latch_kind": str(decision.latch_kind or "none"),
            "trusted": bool(decision.trusted),
            "trusted_consecutive_cycles": int(decision.trusted_consecutive_cycles or 0),
            "evidence_summary": dict(decision.evidence_summary or {}),
            "updated_ts": int(decision.evaluated_ts or 0),
        }

    def _account_snapshot_stale_threshold_seconds(self) -> int:
        configured = int(getattr(self.settings, "fail_closed_account_snapshot_stale_seconds", 0) or 0)
        if configured > 0:
            return configured
        return max(600, int(self.settings.account_sync_refresh_seconds) * 2)

    def _event_stream_stale_threshold_seconds(self) -> int:
        configured = int(getattr(self.settings, "fail_closed_event_stream_stale_seconds", 0) or 0)
        if configured > 0:
            return configured
        return max(120, int(self.settings.poll_interval_seconds) * 2)

    def _ledger_diff_fail_threshold_usd(self) -> float:
        return max(0.0, float(getattr(self.settings, "fail_closed_ledger_diff_threshold_usd", 0.01) or 0.01))

    @staticmethod
    def _recovery_conflict_requires_manual(conflicts: list[dict[str, object]]) -> bool:
        if not conflicts:
            return False
        auto_resolvable_categories = {"AMBIGUOUS_PENDING"}
        for row in conflicts:
            category = str((row or {}).get("category") or "").strip().upper()
            if category and category not in auto_resolvable_categories:
                return True
        return False

    def _bootstrap_admission_evidence_fresh(self, *, reconciliation_payload: Mapping[str, object], now_ts: int) -> bool:
        if self.settings.dry_run:
            return True
        if int(self.startup_failure_count or 0) > 0:
            return True
        account_snapshot_ts = int(self.state.account_snapshot_ts or 0)
        if account_snapshot_ts <= 0:
            return False
        account_age = max(0, now_ts - account_snapshot_ts)
        if account_age > self._account_snapshot_stale_threshold_seconds():
            return False
        reconciliation_status = str(reconciliation_payload.get("status") or "unknown").strip().lower()
        if reconciliation_status not in {"ok", "warn", "fail"}:
            return False
        if self.pending_orders and int(self._last_broker_event_sync_ts or 0) <= 0:
            return False
        return True

    def _evaluate_admission_decision(
        self,
        *,
        control: ControlState,
        now_ts: int,
        reconciliation_payload: Mapping[str, object],
    ) -> AdmissionDecision:
        try:
            evidence = AdmissionEvidence(
                startup_ready=bool(self.startup_ready),
                startup_failure_count=int(self.startup_failure_count or 0),
                reconciliation_status=str(reconciliation_payload.get("status") or "unknown"),
                account_snapshot_age_seconds=int(reconciliation_payload.get("account_snapshot_age_seconds") or 0),
                account_snapshot_stale_threshold_seconds=self._account_snapshot_stale_threshold_seconds(),
                broker_event_sync_age_seconds=int(reconciliation_payload.get("broker_event_sync_age_seconds") or 0),
                broker_event_stale_threshold_seconds=self._event_stream_stale_threshold_seconds(),
                ledger_diff=float(reconciliation_payload.get("internal_vs_ledger_diff") or 0.0),
                ledger_diff_threshold_usd=self._ledger_diff_fail_threshold_usd(),
                ambiguous_pending_orders=int(reconciliation_payload.get("ambiguous_pending_orders") or 0),
                recovery_conflict_count=int(len(self._recovery_conflicts)),
                recovery_conflict_requires_manual=self._recovery_conflict_requires_manual(self._recovery_conflicts),
                persistence_status=str(self.persistence_status or "ok"),
                risk_ledger_status=str(self.state.risk_ledger_status or "ok"),
                risk_breaker_status=str(self.state.risk_breaker_status or "ok"),
                operator_pause_opening=bool(control.pause_opening),
                operator_reduce_only=bool(control.reduce_only),
                operator_emergency_stop=bool(control.emergency_stop),
                dry_run=bool(self.settings.dry_run),
                bootstrap_protected=bool(self._admission_bootstrap_protected),
                bootstrap_evidence_fresh=self._bootstrap_admission_evidence_fresh(
                    reconciliation_payload=reconciliation_payload,
                    now_ts=now_ts,
                ),
            )
            decision = evaluate_admission(
                now_ts=now_ts,
                evidence=evidence,
                previous_auto_latch_active=bool(self._admission_auto_latch_active),
                previous_trusted_consecutive_cycles=int(self._admission_trusted_consecutive_cycles or 0),
                auto_recover_min_healthy_cycles=int(
                    getattr(self.settings, "fail_closed_recover_consecutive_cycles", 1) or 1
                ),
            )
            if self._admission_internal_error_latched and REASON_ADMISSION_GATE_INTERNAL_ERROR not in decision.reason_codes:
                reason_codes = tuple(list(decision.reason_codes) + [REASON_ADMISSION_GATE_INTERNAL_ERROR])
                decision = AdmissionDecision(
                    mode=MODE_HALTED,
                    opening_allowed=False,
                    reduce_only=True,
                    halted=True,
                    auto_recover=False,
                    manual_confirmation_required=True,
                    reason_codes=reason_codes,
                    action_whitelist=("sync_read", "state_evaluation", "cancel_pending_buy", "persist_state_update"),
                    latch_kind="manual",
                    trusted=False,
                    trusted_consecutive_cycles=0,
                    evidence_summary=dict(decision.evidence_summary or {}),
                    evaluated_ts=int(decision.evaluated_ts or now_ts),
                    auto_latch_active=False,
                    manual_latch_active=True,
                )
            return decision
        except Exception:
            self._admission_internal_error_latched = True
            return AdmissionDecision(
                mode=MODE_HALTED,
                opening_allowed=False,
                reduce_only=True,
                halted=True,
                auto_recover=False,
                manual_confirmation_required=True,
                reason_codes=(REASON_ADMISSION_GATE_INTERNAL_ERROR,),
                action_whitelist=("sync_read", "state_evaluation", "cancel_pending_buy", "persist_state_update"),
                latch_kind="manual",
                trusted=False,
                trusted_consecutive_cycles=0,
                evidence_summary={
                    "startup_ready": bool(self.startup_ready),
                    "startup_failure_count": int(self.startup_failure_count or 0),
                    "reconciliation_status": str(reconciliation_payload.get("status") or "unknown"),
                    "account_snapshot_age_seconds": int(reconciliation_payload.get("account_snapshot_age_seconds") or 0),
                    "broker_event_sync_age_seconds": int(reconciliation_payload.get("broker_event_sync_age_seconds") or 0),
                    "ledger_diff": float(reconciliation_payload.get("internal_vs_ledger_diff") or 0.0),
                    "ledger_diff_threshold_usd": self._ledger_diff_fail_threshold_usd(),
                    "ambiguous_pending_orders": int(reconciliation_payload.get("ambiguous_pending_orders") or 0),
                    "recovery_conflict_count": int(len(self._recovery_conflicts)),
                    "persistence_status": str(self.persistence_status or "ok"),
                    "risk_ledger_status": str(self.state.risk_ledger_status or "ok"),
                    "risk_breaker_status": str(self.state.risk_breaker_status or "ok"),
                },
                evaluated_ts=now_ts,
                auto_latch_active=False,
                manual_latch_active=True,
            )

    def _apply_admission_decision(
        self,
        *,
        decision: AdmissionDecision,
        reconciliation_payload: Mapping[str, object],
    ) -> None:
        self._admission_decision = decision
        self.admission_opening_allowed = bool(decision.opening_allowed)
        self.admission_reduce_only = bool(decision.reduce_only)
        self.admission_halted = bool(decision.halted)
        self.admission_auto_recover = bool(decision.auto_recover)
        self.admission_manual_confirmation_required = bool(decision.manual_confirmation_required)
        self.admission_latch_kind = str(decision.latch_kind or "none")
        self.admission_action_whitelist = tuple(decision.action_whitelist)
        self.admission_evidence_summary = dict(decision.evidence_summary or {})
        self._admission_auto_latch_active = bool(decision.auto_latch_active)
        self._admission_trusted_consecutive_cycles = int(decision.trusted_consecutive_cycles or 0)
        if decision.evidence_summary and bool(decision.evidence_summary.get("startup_ready", False)):
            self._admission_bootstrap_protected = False
        if REASON_ADMISSION_GATE_INTERNAL_ERROR not in decision.reason_codes:
            self._admission_internal_error_latched = False
        self._sync_legacy_trading_mode_projection(decision, reconciliation_payload=reconciliation_payload)

    def _sync_legacy_trading_mode_projection(
        self,
        decision: AdmissionDecision,
        *,
        reconciliation_payload: Mapping[str, object],
    ) -> None:
        self.trading_mode = str(decision.mode or MODE_REDUCE_ONLY).upper()
        self.trading_mode_reasons = _legacy_trading_mode_reasons(tuple(decision.reason_codes))
        self.trading_mode_updated_ts = int(decision.evaluated_ts or int(time.time()))
        self.account_state_status = self._account_state_status(now=int(decision.evaluated_ts or time.time()))
        self.reconciliation_status = (
            str(reconciliation_payload.get("status") or "unknown").strip().lower() or "unknown"
        )

    def _update_trading_mode(
        self,
        control: ControlState,
        *,
        now: int | None = None,
        reconciliation: dict[str, object] | None = None,
    ) -> dict[str, object]:
        now_ts = int(now or time.time())
        reconciliation_payload = dict(reconciliation or self.reconciliation_summary(now=now_ts))
        decision = self._evaluate_admission_decision(
            control=control,
            now_ts=now_ts,
            reconciliation_payload=reconciliation_payload,
        )
        self._apply_admission_decision(decision=decision, reconciliation_payload=reconciliation_payload)
        signature = (
            str(self.trading_mode or MODE_NORMAL),
            tuple(self.trading_mode_reasons),
            str(self.account_state_status or "unknown"),
            str(self.reconciliation_status or "unknown"),
            str(self.persistence_status or "ok"),
        )
        trading_mode_state = self.trading_mode_state()
        if signature != self._last_trading_mode_signature:
            self._last_trading_mode_signature = signature
            self._append_event("trading_mode", trading_mode_state)
            log_level = self.log.warning if trading_mode_state["mode"] != MODE_NORMAL else self.log.info
            log_level(
                "TRADING_MODE mode=%s opening_allowed=%s reasons=%s account_state=%s reconciliation=%s",
                trading_mode_state["mode"],
                trading_mode_state["opening_allowed"],
                ",".join(trading_mode_state["reason_codes"]) or "none",
                trading_mode_state["account_state_status"],
                trading_mode_state["reconciliation_status"],
            )
            self._maybe_notify_trading_mode_alert(
                trading_mode_state=trading_mode_state,
                reconciliation=reconciliation_payload,
                now=now_ts,
            )
        return trading_mode_state

    def kill_switch_state(self) -> dict[str, object]:
        return normalize_kill_switch_state(self._kill_switch_state)

    def _kill_switch_terminal_timeout_seconds(self) -> int:
        return max(30, int(getattr(self.settings, "kill_switch_terminal_timeout_seconds", 600) or 600))

    def _kill_switch_query_error_threshold(self) -> int:
        return max(1, int(getattr(self.settings, "kill_switch_query_error_threshold", 3) or 3))

    def _kill_switch_cancel_retry_seconds(self) -> int:
        return max(
            5,
            int(getattr(self.settings, "kill_switch_cancel_retry_seconds", self._pending_cancel_retry_seconds()) or 5),
        )

    def _persist_kill_switch_state(self) -> None:
        if self._state_store is None:
            return
        try:
            self._state_store.save_kill_switch_state(self._kill_switch_state)
        except Exception as exc:
            self.log.error("Kill switch state persist failed db=%s err=%s", self.settings.state_store_path, exc)
            self._record_persistence_fault(kind="kill_switch_state_write", path=self.settings.state_store_path, error=exc)

    def _set_kill_switch_state(self, payload: Mapping[str, object], *, now: int, persist: bool = True) -> None:
        next_state = normalize_kill_switch_state(payload)
        next_state["updated_ts"] = int(now)
        previous = normalize_kill_switch_state(self._kill_switch_state)
        if next_state == previous:
            self._kill_switch_state = next_state
            return
        self._kill_switch_state = next_state
        self._append_event("kill_switch", self.kill_switch_state())
        if persist:
            self._persist_kill_switch_state()

    @staticmethod
    def _kill_switch_reason(reason_codes: list[str], code: str) -> None:
        normalized = str(code or "").strip()
        if not normalized:
            return
        if normalized not in reason_codes:
            reason_codes.append(normalized)

    def _kill_switch_pending_buy_context(self) -> dict[str, object]:
        pending_keys: list[str] = []
        pending_order_ids: set[str] = set()
        cancel_requested_order_ids: set[str] = set()
        unresolved_local_ids: set[str] = set()

        for key, order in self.pending_orders.items():
            if str(order.get("side") or "").upper() != "BUY":
                continue
            pending_keys.append(str(key))
            broker_status = str(order.get("broker_status") or "").strip().lower()
            if broker_status == "cancelled":
                broker_status = "canceled"
            order_id = str(order.get("order_id") or "").strip()
            if not order_id:
                unresolved_local_ids.add(f"local:{key}")
                continue
            if broker_status in {"cancel_requested", "requested", "queued", "pending_cancel"}:
                cancel_requested_order_ids.add(order_id)
            if kill_switch_status_is_terminal(broker_status):
                continue
            pending_order_ids.add(order_id)

        return {
            "pending_keys": sorted({key for key in pending_keys if key}),
            "pending_order_ids": sorted(pending_order_ids),
            "cancel_requested_order_ids": sorted(cancel_requested_order_ids),
            "unresolved_local_ids": sorted(unresolved_local_ids),
        }

    def _kill_switch_load_open_buy_orders(self) -> tuple[list[OpenOrderSnapshot], str]:
        open_orders = self._load_broker_open_orders()
        if open_orders is None:
            if self.settings.dry_run:
                return ([], "")
            return ([], "broker_open_orders_unavailable")
        buy_open = [
            order
            for order in open_orders
            if str(order.side or "").strip().upper() == "BUY"
            and not kill_switch_status_is_terminal(order.lifecycle_status)
        ]
        return (buy_open, "")

    def _kill_switch_issue_cancel_requests(
        self,
        *,
        state: dict[str, object],
        now: int,
        action_reason: str,
    ) -> tuple[dict[str, object], str]:
        next_state = normalize_kill_switch_state(state)
        pending_context = self._kill_switch_pending_buy_context()
        tracked_ids = {str(item).strip() for item in list(next_state.get("tracked_buy_order_ids") or []) if str(item).strip()}
        cancel_requested = {
            str(item).strip()
            for item in list(next_state.get("cancel_requested_order_ids") or [])
            if str(item).strip()
        }
        for order_id in list(pending_context.get("pending_order_ids") or []):
            tracked_ids.add(str(order_id))
        for local_id in list(pending_context.get("unresolved_local_ids") or []):
            tracked_ids.add(str(local_id))

        retry_seconds = self._kill_switch_cancel_retry_seconds()
        failures: list[str] = []
        attempts = 0

        remaining: dict[str, dict[str, object]] = {}
        for key, order in sorted(self.pending_orders.items(), key=lambda item: int(item[1].get("ts") or 0)):
            if str(order.get("side") or "").upper() != "BUY":
                remaining[key] = order
                continue
            order_id = str(order.get("order_id") or "").strip()
            if not order_id:
                remaining[key] = order
                continue
            tracked_ids.add(order_id)
            last_request_ts = int(order.get("cancel_requested_ts") or 0)
            if last_request_ts > 0 and (now - last_request_ts) < retry_seconds:
                if str(order.get("broker_status") or "").strip().lower() in {"cancel_requested", "requested", "queued"}:
                    cancel_requested.add(order_id)
                remaining[key] = order
                continue
            attempts += 1
            outcome, updated = self._cancel_pending_order(
                order=order,
                now=now,
                position_lookup=self.positions_book,
                action_reason=action_reason,
                force=False,
            )
            if outcome == "failed":
                failures.append(f"pending_cancel_failed:{order_id}")
                remaining[key] = updated
                continue
            if outcome == "requested":
                cancel_requested.add(order_id)
                remaining[key] = updated
                continue
            if outcome == "canceled":
                continue
            remaining[key] = updated
        self.pending_orders = remaining
        self._refresh_risk_state()

        open_buys, open_err = self._kill_switch_load_open_buy_orders()
        if open_err:
            return (next_state, open_err)
        for order in open_buys:
            order_id = str(order.order_id or "").strip()
            if not order_id:
                continue
            tracked_ids.add(order_id)
            attempts += 1
            response = self.broker.cancel_order(order_id)
            status, ok, message = self._normalize_cancel_status(response if isinstance(response, Mapping) else None)
            if status == "requested" and ok:
                cancel_requested.add(order_id)
                continue
            if status == "canceled" and ok:
                continue
            failures.append(
                f"open_buy_cancel_failed:{order_id}:{message or status}"
            )

        next_state["cancel_attempts"] = int(next_state.get("cancel_attempts") or 0) + int(attempts)
        next_state["pending_buy_order_keys"] = list(pending_context.get("pending_keys") or [])
        next_state["tracked_buy_order_ids"] = sorted(tracked_ids)
        next_state["cancel_requested_order_ids"] = sorted(cancel_requested)
        next_state["open_buy_order_ids"] = sorted({str(order.order_id or "").strip() for order in open_buys if str(order.order_id or "").strip()})
        if failures:
            return (next_state, ";".join(failures))
        return (next_state, "")

    def _kill_switch_probe_terminal(
        self,
        *,
        state: dict[str, object],
        now: int,
    ) -> tuple[dict[str, object], str]:
        next_state = normalize_kill_switch_state(state)
        pending_context = self._kill_switch_pending_buy_context()
        open_buys, open_err = self._kill_switch_load_open_buy_orders()
        if open_err:
            return (next_state, open_err)

        open_buy_ids = {str(order.order_id or "").strip() for order in open_buys if str(order.order_id or "").strip()}
        tracked_ids = {str(item).strip() for item in list(next_state.get("tracked_buy_order_ids") or []) if str(item).strip()}
        for order_id in list(pending_context.get("pending_order_ids") or []):
            tracked_ids.add(str(order_id))
        for order_id in open_buy_ids:
            tracked_ids.add(order_id)
        for item in list(next_state.get("cancel_requested_order_ids") or []):
            if str(item).strip():
                tracked_ids.add(str(item).strip())

        non_terminal_ids: set[str] = set()
        cancel_requested_ids: set[str] = set()
        errors: list[str] = []

        for local_id in list(pending_context.get("unresolved_local_ids") or []):
            non_terminal_ids.add(str(local_id))

        for order_id in sorted(tracked_ids):
            if order_id.startswith("local:"):
                non_terminal_ids.add(order_id)
                continue
            if order_id in open_buy_ids:
                non_terminal_ids.add(order_id)
                continue
            snapshot: OrderStatusSnapshot | None
            try:
                snapshot = self.broker.get_order_status(order_id)
            except Exception as exc:
                errors.append(f"order_status_error:{order_id}:{exc}")
                continue
            if snapshot is None:
                non_terminal_ids.add(order_id)
                continue
            lifecycle_status = str(snapshot.lifecycle_status or snapshot.normalized_status or "").strip().lower()
            if lifecycle_status in {"cancel_requested", "requested", "queued"}:
                cancel_requested_ids.add(order_id)
                non_terminal_ids.add(order_id)
                continue
            if not snapshot.is_terminal:
                non_terminal_ids.add(order_id)

        next_state["last_broker_check_ts"] = int(now)
        next_state["open_buy_order_ids"] = sorted(open_buy_ids)
        next_state["non_terminal_buy_order_ids"] = sorted(non_terminal_ids)
        next_state["cancel_requested_order_ids"] = sorted(cancel_requested_ids)
        next_state["tracked_buy_order_ids"] = sorted(tracked_ids)
        next_state["pending_buy_order_keys"] = list(pending_context.get("pending_keys") or [])
        if errors:
            return (next_state, ";".join(errors))
        return (next_state, "")

    def _advance_kill_switch_state(self, control: ControlState, *, now: int) -> None:
        state = normalize_kill_switch_state(self._kill_switch_state)
        requested_mode = requested_kill_switch_mode(
            pause_opening=bool(control.pause_opening),
            reduce_only=bool(control.reduce_only),
            emergency_stop=bool(control.emergency_stop),
        )
        mode_requested = str(state.get("mode_requested") or KILL_SWITCH_MODE_NONE)
        phase = str(state.get("phase") or KILL_SWITCH_PHASE_IDLE)
        reason_codes = [str(item) for item in list(state.get("reason_codes") or []) if str(item).strip()]

        if requested_mode != KILL_SWITCH_MODE_NONE and (mode_requested != requested_mode or phase == KILL_SWITCH_PHASE_IDLE):
            state = default_kill_switch_state(now_ts=now)
            state["mode_requested"] = requested_mode
            state["phase"] = KILL_SWITCH_PHASE_REQUESTED
            state["requested_ts"] = int(now)
            mode_requested = requested_mode
            phase = KILL_SWITCH_PHASE_REQUESTED
            reason_codes = []

        if requested_mode != KILL_SWITCH_MODE_NONE:
            self._kill_switch_reason(reason_codes, f"operator_{requested_mode}")
            state["mode_requested"] = requested_mode
            mode_requested = requested_mode
        elif mode_requested == KILL_SWITCH_MODE_NONE:
            reason_codes = []

        if mode_requested == KILL_SWITCH_MODE_PAUSE_OPENING:
            state["phase"] = KILL_SWITCH_PHASE_SAFE_CONFIRMED
            state["broker_safe_confirmed"] = True
            state["safe_confirmed_ts"] = int(now)

        if mode_requested in {KILL_SWITCH_MODE_REDUCE_ONLY, KILL_SWITCH_MODE_EMERGENCY_STOP}:
            if phase in {KILL_SWITCH_PHASE_REQUESTED, KILL_SWITCH_PHASE_CANCELING_BUY}:
                state["phase"] = KILL_SWITCH_PHASE_CANCELING_BUY
                state, cancel_err = self._kill_switch_issue_cancel_requests(
                    state=state,
                    now=now,
                    action_reason=f"kill_switch_{mode_requested}",
                )
                if cancel_err:
                    self._kill_switch_reason(reason_codes, "kill_switch_cancel_error")
                    state["phase"] = KILL_SWITCH_PHASE_FAILED_MANUAL_REQUIRED
                    state["manual_required"] = True
                    state["last_error"] = str(cancel_err)
                else:
                    state["phase"] = KILL_SWITCH_PHASE_WAITING_BROKER_TERMINAL
            if str(state.get("phase") or "") == KILL_SWITCH_PHASE_WAITING_BROKER_TERMINAL:
                state, probe_err = self._kill_switch_probe_terminal(state=state, now=now)
                elapsed = max(0, int(now) - int(state.get("requested_ts") or now))
                if probe_err:
                    self._kill_switch_reason(reason_codes, "kill_switch_query_error")
                    state["query_error_count"] = int(state.get("query_error_count") or 0) + 1
                    state["last_error"] = str(probe_err)
                else:
                    state["query_error_count"] = 0
                    state["last_error"] = ""
                    if not list(state.get("open_buy_order_ids") or []) and not list(state.get("non_terminal_buy_order_ids") or []):
                        state["phase"] = KILL_SWITCH_PHASE_SAFE_CONFIRMED
                        state["broker_safe_confirmed"] = True
                        state["safe_confirmed_ts"] = int(now)
                    else:
                        state["broker_safe_confirmed"] = False

                if str(state.get("phase") or "") == KILL_SWITCH_PHASE_WAITING_BROKER_TERMINAL:
                    if elapsed >= self._kill_switch_terminal_timeout_seconds():
                        self._kill_switch_reason(reason_codes, "kill_switch_cancel_timeout")
                        state["phase"] = KILL_SWITCH_PHASE_FAILED_MANUAL_REQUIRED
                        state["manual_required"] = True
                    elif int(state.get("query_error_count") or 0) >= self._kill_switch_query_error_threshold():
                        self._kill_switch_reason(reason_codes, "kill_switch_query_unavailable")
                        state["phase"] = KILL_SWITCH_PHASE_FAILED_MANUAL_REQUIRED
                        state["manual_required"] = True

        if str(state.get("phase") or "") == KILL_SWITCH_PHASE_FAILED_MANUAL_REQUIRED:
            state["manual_required"] = True
            state["broker_safe_confirmed"] = False
            self._kill_switch_reason(reason_codes, "kill_switch_manual_required")

        if str(state.get("phase") or "") == KILL_SWITCH_PHASE_SAFE_CONFIRMED and requested_mode == KILL_SWITCH_MODE_NONE:
            if not bool(state.get("manual_required")):
                state = default_kill_switch_state(now_ts=now)
                reason_codes = []

        phase = str(state.get("phase") or KILL_SWITCH_PHASE_IDLE)
        mode_requested = str(state.get("mode_requested") or KILL_SWITCH_MODE_NONE)
        manual_required = bool(state.get("manual_required"))
        active_requested = requested_mode != KILL_SWITCH_MODE_NONE
        in_flight = phase in {
            KILL_SWITCH_PHASE_REQUESTED,
            KILL_SWITCH_PHASE_CANCELING_BUY,
            KILL_SWITCH_PHASE_WAITING_BROKER_TERMINAL,
        }

        if phase == KILL_SWITCH_PHASE_IDLE:
            state["opening_allowed"] = True
            state["reduce_only"] = False
            state["halted"] = False
            state["latched"] = False
            state["broker_safe_confirmed"] = True
            state["reason_codes"] = []
        else:
            state["opening_allowed"] = False
            state["reduce_only"] = mode_requested in {KILL_SWITCH_MODE_REDUCE_ONLY, KILL_SWITCH_MODE_EMERGENCY_STOP}
            state["halted"] = mode_requested == KILL_SWITCH_MODE_EMERGENCY_STOP
            state["latched"] = bool(
                manual_required
                or mode_requested == KILL_SWITCH_MODE_EMERGENCY_STOP
                or in_flight
                or active_requested
            )
            state["reason_codes"] = reason_codes
        state["auto_recover"] = bool(
            phase in {KILL_SWITCH_PHASE_WAITING_BROKER_TERMINAL, KILL_SWITCH_PHASE_SAFE_CONFIRMED}
            and not bool(state.get("manual_required"))
        )
        self._set_kill_switch_state(state, now=now, persist=True)

    def _buy_gate_reason(self) -> str:
        kill_switch = self.kill_switch_state()
        if not bool(kill_switch.get("opening_allowed", True)):
            phase = str(kill_switch.get("phase") or "")
            mode_requested = str(kill_switch.get("mode_requested") or "")
            reason_codes = [str(item) for item in list(kill_switch.get("reason_codes") or []) if str(item).strip()]
            if phase == KILL_SWITCH_PHASE_FAILED_MANUAL_REQUIRED or bool(kill_switch.get("manual_required")):
                return "kill_switch_manual_required"
            if mode_requested == KILL_SWITCH_MODE_PAUSE_OPENING:
                return "pause_opening"
            if mode_requested == KILL_SWITCH_MODE_EMERGENCY_STOP:
                return "emergency_stop"
            if "kill_switch_cancel_timeout" in reason_codes:
                return "kill_switch_cancel_timeout"
            if "kill_switch_query_unavailable" in reason_codes:
                return "kill_switch_query_unavailable"
            if "kill_switch_query_error" in reason_codes:
                return "kill_switch_query_error"
            return "kill_switch_waiting_broker_terminal"
        decision = self._admission_decision
        if bool(decision.opening_allowed):
            return ""
        for reason in decision.reason_codes:
            if reason in {"operator_pause_opening", "operator_manual_reduce_only"}:
                return "pause_opening"
            if reason == "startup_checks_fail":
                return "startup_not_ready"
            if reason == "stale_account_snapshot":
                return "account_state_stale"
            if reason in {"reconciliation_fail", "reconciliation_warn"}:
                return reason
            if reason == REASON_PERSISTENCE_FAULT:
                return "persistence_fault"
            if reason in {REASON_RECOVERY_CONFLICT_UNRESOLVED_MANUAL, "recovery_conflict_unresolved"}:
                return "recovery_conflict"
            if reason == "stale_broker_event_stream":
                return "broker_event_stream_stale"
            if reason == "ledger_diff_exceeded":
                return "ledger_diff_exceeded"
            if reason == "ambiguous_pending_unresolved":
                return "ambiguous_pending_unresolved"
            if reason in {"operator_emergency_stop", REASON_ADMISSION_GATE_INTERNAL_ERROR}:
                return "emergency_stop"
        return "system_halted" if bool(decision.halted) else "system_reduce_only"

    def _refresh_buy_blocked_state(self, *, now_ts: int) -> None:
        reason = self._buy_gate_reason()
        if reason:
            if int(self._buy_blocked_since_ts or 0) <= 0:
                self._buy_blocked_since_ts = int(now_ts or time.time())
            return
        self._buy_blocked_since_ts = 0

    def buy_blocked_state(self, *, now_ts: int | None = None) -> dict[str, object]:
        ts = int(now_ts or time.time())
        reason = self._buy_gate_reason()
        active = bool(reason)
        since_ts = int(self._buy_blocked_since_ts or 0) if active else 0
        duration_seconds = max(0, ts - since_ts) if (active and since_ts > 0) else 0
        return {
            "active": active,
            "reason_code": reason,
            "since_ts": since_ts,
            "duration_seconds": int(duration_seconds),
            "updated_ts": ts,
        }

    def _writer_active_for_runtime(self) -> bool:
        if not bool(getattr(self.settings, "enable_single_writer", False)):
            return True
        try:
            self._assert_writer_active()
        except Exception:
            return False
        return True

    def _update_runner_heartbeat(
        self,
        *,
        now_ts: int | None = None,
        loop_status: str | None = None,
        cycle_started: bool = False,
        cycle_finished: bool = False,
    ) -> None:
        if not self._writer_active_for_runtime():
            return
        ts = int(now_ts or time.time())
        heartbeat = normalize_runner_heartbeat(self._runner_heartbeat)
        heartbeat["writer_active"] = True
        heartbeat["last_seen_ts"] = ts
        if cycle_started:
            heartbeat["cycle_seq"] = max(0, int(heartbeat.get("cycle_seq") or 0)) + 1
            heartbeat["last_cycle_started_ts"] = ts
            if loop_status is None:
                loop_status = RUNNER_LOOP_STATUS_RUNNING
        if cycle_finished:
            heartbeat["last_cycle_finished_ts"] = ts
            if loop_status is None:
                loop_status = RUNNER_LOOP_STATUS_RUNNING
        if loop_status:
            heartbeat["loop_status"] = str(loop_status)
        self._runner_heartbeat = heartbeat

    def _pending_entry_cancel_reason(self, control: ControlState) -> str:
        if control.emergency_stop:
            return "emergency_stop_cancel_pending_entry"
        if control.reduce_only:
            return "reduce_only_cancel_pending_entry"
        kill_switch = self.kill_switch_state()
        if not bool(kill_switch.get("opening_allowed", True)):
            mode_requested = str(kill_switch.get("mode_requested") or "")
            phase = str(kill_switch.get("phase") or "")
            if mode_requested in {KILL_SWITCH_MODE_REDUCE_ONLY, KILL_SWITCH_MODE_EMERGENCY_STOP} and phase in {
                KILL_SWITCH_PHASE_REQUESTED,
                KILL_SWITCH_PHASE_CANCELING_BUY,
                KILL_SWITCH_PHASE_WAITING_BROKER_TERMINAL,
                KILL_SWITCH_PHASE_FAILED_MANUAL_REQUIRED,
            }:
                return "kill_switch_cancel_pending_entry"
        if not self.pending_orders:
            return ""

        if not bool(self._admission_decision.opening_allowed):
            return "trading_mode_cancel_pending_entry"
        return ""

    def _new_cycle_id(self, now: int) -> str:
        self._cycle_seq += 1
        return f"cyc-{now}-{self._cycle_seq:05d}"

    def _new_signal_id(self, now: int) -> str:
        self._signal_seq += 1
        return f"sig-{now}-{self._signal_seq:06d}"

    def _new_trace_id(self, token_id: str, now: int) -> str:
        self._trace_seq += 1
        token_tail = str(token_id or "").strip().lower()[-8:] or "trace"
        return f"trc-{now}-{self._trace_seq:05d}-{token_tail}"

    def _trace_id_for_signal(
        self,
        signal: Signal,
        existing: dict[str, object] | None,
        now: int,
    ) -> str:
        source = existing or {}
        current = str(source.get("trace_id") or "").strip()
        if current:
            return current
        return self._new_trace_id(signal.token_id, now)

    def _position_action_for_signal(self, signal: Signal, existing: dict[str, object] | None) -> tuple[str, str]:
        if str(signal.position_action or "").strip():
            return (str(signal.position_action), str(signal.position_action_label or signal.position_action))
        if signal.side == "BUY":
            return ("add", self._action_tag_label("add")) if existing else ("entry", self._action_tag_label("entry"))
        if float(signal.exit_fraction or 0.0) >= 0.95:
            return ("exit", self._action_tag_label("exit"))
        return ("trim", self._action_tag_label("trim"))

    @staticmethod
    def _normalize_wallet_address(value: object) -> str:
        return str(value or "").strip().lower()

    @staticmethod
    def _normalize_token_id(value: object) -> str:
        return str(value or "").strip()

    def _position_truth_for_token(self, token_id: object) -> dict[str, object] | None:
        normalized = self._normalize_token_id(token_id)
        if not normalized:
            return None
        position = self.positions_book.get(normalized)
        if position is not None:
            return position
        lowered = normalized.lower()
        for key, candidate in self.positions_book.items():
            if str(key or "").strip().lower() == lowered:
                return candidate
        return None

    def _same_wallet_add_allowlist(self) -> set[str]:
        raw = str(self.settings.same_wallet_add_allowlist or "")
        return {
            token
            for token in (self._normalize_wallet_address(part) for part in re.split(r"[\s,]+", raw))
            if token
        }

    def _same_wallet_add_allowed(self, signal: Signal, existing: Mapping[str, object] | None) -> bool:
        if signal.side != "BUY" or existing is None:
            return False
        signal_wallet = self._normalize_wallet_address(signal.wallet)
        entry_wallet = self._normalize_wallet_address(existing.get("entry_wallet"))
        if not signal_wallet or not entry_wallet or signal_wallet != entry_wallet:
            return False
        if not bool(self.settings.same_wallet_add_enabled):
            return False
        return signal_wallet in self._same_wallet_add_allowlist()

    def _repeat_entry_block_reason(self, signal: Signal, existing: Mapping[str, object] | None) -> str:
        if signal.side != "BUY" or existing is None:
            return ""
        if float(existing.get("notional") or 0.0) <= 0.0:
            return ""
        if self._same_wallet_add_allowed(signal, existing):
            return ""
        signal_wallet = self._normalize_wallet_address(signal.wallet)
        entry_wallet = self._normalize_wallet_address(existing.get("entry_wallet"))
        if signal_wallet and entry_wallet:
            if signal_wallet == entry_wallet:
                return REASON_SAME_WALLET_ADD_NOT_ALLOWED
            return REASON_CROSS_WALLET_REPEAT_ENTRY_BLOCKED
        return REASON_REPEAT_ENTRY_BLOCKED_EXISTING_POSITION

    def _wallet_pool_snapshot(self) -> list[dict[str, object]]:
        latest_metrics = getattr(self.strategy, "latest_wallet_metrics", None)
        if not callable(latest_metrics):
            return []
        metrics = latest_metrics()
        rows: list[dict[str, object]] = []
        for wallet, data in sorted(metrics.items(), key=lambda item: float(item[1].get("wallet_score") or 0.0), reverse=True):
            topic_profiles = list(data.get("topic_profiles") or [])
            rows.append(
                {
                    "wallet": wallet,
                    "wallet_score": float(data.get("wallet_score") or 0.0),
                    "wallet_tier": str(data.get("wallet_tier") or "LOW"),
                    "score_summary": str(data.get("score_summary") or ""),
                    "trading_enabled": bool(data.get("trading_enabled", False)),
                    "topic_profiles": topic_profiles[:3],
                }
            )
        return rows

    @staticmethod
    def _topic_snapshot(signal: Signal) -> dict[str, object]:
        return {
            "topic_key": str(signal.topic_key or ""),
            "topic_label": str(signal.topic_label or ""),
            "topic_sample_count": int(signal.topic_sample_count or 0),
            "topic_win_rate": float(signal.topic_win_rate or 0.0),
            "topic_roi": float(signal.topic_roi or 0.0),
            "topic_resolved_win_rate": float(signal.topic_resolved_win_rate or 0.0),
            "topic_score_summary": str(signal.topic_score_summary or ""),
            "topic_bias": str(signal.topic_bias or "neutral"),
            "topic_multiplier": float(signal.topic_multiplier or 1.0),
        }

    @staticmethod
    def _signal_snapshot(signal: Signal) -> dict[str, object]:
        return {
            "signal_id": str(signal.signal_id or ""),
            "trace_id": str(signal.trace_id or ""),
            "wallet": str(signal.wallet or ""),
            "condition_id": str(signal.condition_id or ""),
            "market_slug": str(signal.market_slug or ""),
            "token_id": str(signal.token_id or ""),
            "outcome": str(signal.outcome or ""),
            "side": str(signal.side or ""),
            "confidence": float(signal.confidence or 0.0),
            "price_hint": float(signal.price_hint or 0.0),
            "observed_size": float(signal.observed_size or 0.0),
            "observed_notional": float(signal.observed_notional or 0.0),
            "timestamp": signal.timestamp.isoformat(),
            "wallet_score": float(signal.wallet_score or 0.0),
            "wallet_tier": str(signal.wallet_tier or "LOW"),
            "wallet_score_summary": str(signal.wallet_score_summary or ""),
            "exit_fraction": float(signal.exit_fraction or 0.0),
            "exit_reason": str(signal.exit_reason or ""),
            "cross_wallet_exit": bool(signal.cross_wallet_exit),
            "exit_wallet_count": int(signal.exit_wallet_count or 0),
            "position_action": str(signal.position_action or ""),
            "position_action_label": str(signal.position_action_label or ""),
        }

    @staticmethod
    def _cycle_candidate_record(
        signal: Signal,
        *,
        cycle_id: str,
        wallet_pool_snapshot: list[dict[str, object]],
    ) -> dict[str, object]:
        return {
            "cycle_id": cycle_id,
            "signal_id": str(signal.signal_id or ""),
            "trace_id": str(signal.trace_id or ""),
            "candidate_snapshot": Trader._signal_snapshot(signal),
            "topic_snapshot": Trader._topic_snapshot(signal),
            "wallet_pool_snapshot": list(wallet_pool_snapshot),
            "decision_snapshot": {},
            "order_snapshot": {},
            "final_status": "candidate",
        }

    def _trace_record(
        self,
        *,
        trace_id: str,
        signal: Signal,
        cycle_id: str,
        signal_record: dict[str, object],
        opened_ts: int,
    ) -> None:
        record = self._trace_registry.get(trace_id)
        if record is None:
            record = {
                "trace_id": trace_id,
                "condition_id": str(signal.condition_id or ""),
                "token_id": signal.token_id,
                "market_slug": signal.market_slug,
                "outcome": signal.outcome,
                "opened_ts": opened_ts,
                "closed_ts": 0,
                "status": "open",
                "entry_signal_id": "",
                "last_signal_id": "",
                "entry_snapshot": {},
                "decision_chain": [],
            }
            self._trace_registry[trace_id] = record
            self._trace_order.appendleft(trace_id)
        record["condition_id"] = str(signal.condition_id or "")
        record["token_id"] = signal.token_id
        record["market_slug"] = signal.market_slug
        record["outcome"] = signal.outcome
        record["last_signal_id"] = str(signal.signal_id or "")
        record["last_ts"] = opened_ts
        chain = list(record.get("decision_chain") or [])
        chain.append(signal_record)
        record["decision_chain"] = chain[-16:]
        if signal.side == "BUY" and not record.get("entry_signal_id"):
            record["entry_signal_id"] = str(signal.signal_id or "")
            record["entry_snapshot"] = signal_record
            record["opened_ts"] = opened_ts
        while len(self._trace_registry) > 64:
            stale_trace_id = self._trace_order.pop()
            self._trace_registry.pop(stale_trace_id, None)

    def _mark_trace_closed(self, trace_id: str, closed_ts: int) -> None:
        record = self._trace_registry.get(trace_id)
        if not record:
            return
        record["closed_ts"] = closed_ts
        record["status"] = "closed"

    def _trace_records(self) -> list[dict[str, object]]:
        records: list[dict[str, object]] = []
        for trace_id in list(self._trace_order):
            record = self._trace_registry.get(trace_id)
            if not record:
                continue
            records.append(dict(record))
        return records

    def _update_strategy_activity_counts(self, counts: dict[str, int], available: bool) -> None:
        updater = getattr(self.strategy, "update_wallet_activity_counts", None)
        if not callable(updater):
            return
        try:
            updater(counts, available=available)
        except TypeError:
            updater(counts)

    def _update_strategy_selection_context(self, context: Mapping[str, Mapping[str, object]]) -> None:
        updater = getattr(self.strategy, "update_wallet_selection_context", None)
        if not callable(updater):
            return
        updater(context)

    def _discovery_history_bonus(
        self,
        metrics: RealizedWalletMetrics | None,
    ) -> tuple[float, str]:
        max_bonus = max(0.0, float(self.settings.wallet_discovery_history_bonus))
        if metrics is None or max_bonus <= 0.0 or metrics.closed_positions <= 0:
            return (0.0, "")

        scorer = getattr(self.strategy, "scorer", None)
        min_closed = max(1, int(getattr(scorer, "min_realized_sample", 5) or 5))
        if metrics.closed_positions < min_closed:
            return (0.0, "")

        strength = 0.0
        reasons: list[str] = []
        if metrics.win_rate >= 0.58:
            strength += 0.35
            reasons.append(f"win {metrics.win_rate:.0%}")
        if metrics.roi >= 0.05:
            strength += 0.35
            reasons.append(f"roi {metrics.roi:+.0%}")
        if metrics.profit_factor >= 1.25:
            strength += 0.2
            reasons.append(f"pf {metrics.profit_factor:.2f}")
        if metrics.resolved_markets >= 3 and metrics.resolved_win_rate >= 0.6:
            strength += 0.1
            reasons.append(f"resolved {metrics.resolved_win_rate:.0%}")
        if strength <= 0.0:
            return (0.0, "")
        bonus = round(min(1.0, strength) * max_bonus, 4)
        return (bonus, " / ".join(reasons))

    def _discovery_topic_bonus(
        self,
        topic_profiles: list[dict[str, object]] | None,
    ) -> tuple[float, str, str]:
        max_bonus = max(0.0, float(self.settings.wallet_discovery_topic_bonus))
        if (
            not self.settings.topic_bias_enabled
            or max_bonus <= 0.0
            or not topic_profiles
        ):
            return (0.0, "", "")

        best_bonus = 0.0
        best_topic = ""
        best_reason = ""
        for row in topic_profiles:
            sample_count = max(0, int(row.get("sample_count") or 0))
            if sample_count < self.settings.topic_min_samples:
                continue
            roi = float(row.get("roi") or 0.0)
            win_rate = float(row.get("win_rate") or 0.0)
            resolved_markets = max(0, int(row.get("resolved_markets") or 0))
            resolved_win_rate = float(row.get("resolved_win_rate") or 0.0)

            strength = 0.0
            reasons: list[str] = [f"{sample_count} samples"]
            if roi >= self.settings.topic_positive_roi:
                strength += 0.5
                reasons.append(f"roi {roi:+.0%}")
            if win_rate >= self.settings.topic_positive_win_rate:
                strength += 0.35
                reasons.append(f"win {win_rate:.0%}")
            if resolved_markets >= 2 and resolved_win_rate >= 0.6:
                strength += 0.15
                reasons.append(f"resolved {resolved_win_rate:.0%}")
            if strength <= 0.0:
                continue

            bonus = round(min(1.0, strength) * max_bonus, 4)
            if bonus <= best_bonus:
                continue
            best_bonus = bonus
            best_topic = str(row.get("label") or row.get("key") or "")
            best_reason = " / ".join(reasons)
        return (best_bonus, best_topic, best_reason)

    def _update_strategy_history(self, wallets: list[str]) -> None:
        updater = getattr(self.strategy, "update_wallet_realized_metrics", None)
        if not callable(updater):
            return
        if self._wallet_history_store is None:
            updater({})
            return

        try:
            metrics, refreshed_ts, recent_closed_markets, topic_profiles = self._wallet_history_store.sync_wallets(wallets)
        except Exception as exc:
            self.log.warning("Wallet history refresh failed wallets=%d err=%s", len(wallets), exc)
            return
        try:
            updater(
                metrics,
                refreshed_ts=refreshed_ts,
                recent_closed_markets=recent_closed_markets,
                topic_profiles=topic_profiles,
            )
        except TypeError:
            updater(metrics, refreshed_ts=refreshed_ts)

    def _load_runtime_snapshot(self) -> dict[str, object] | None:
        if self._state_store is None:
            return None
        try:
            truth = self._state_store.load_runtime_truth()
        except Exception as exc:
            self.log.error("Load runtime truth failed db=%s err=%s", self.settings.state_store_path, exc)
            self._record_persistence_fault(kind="runtime_truth_read", path=self.settings.state_store_path, error=exc)
            self._recovery_block_buy_latched = True
            return None
        runtime_payload = truth.get("runtime")
        if isinstance(runtime_payload, dict):
            return runtime_payload
        return None

    def _record_recovery_conflict(
        self,
        *,
        category: str,
        token_id: str = "",
        order_id: str = "",
        details: str = "",
    ) -> None:
        row = {
            "category": str(category or "unknown"),
            "token_id": str(token_id or ""),
            "order_id": str(order_id or ""),
            "details": str(details or ""),
            "ts": int(time.time()),
        }
        self._recovery_conflicts.append(row)
        self._recovery_block_buy_latched = True

    def _control_payload_from_runtime(self, control: ControlState | None = None) -> dict[str, object]:
        state = control or self.control_state
        return {
            "decision_mode": str(state.decision_mode or "manual"),
            "pause_opening": bool(state.pause_opening),
            "reduce_only": bool(state.reduce_only),
            "emergency_stop": bool(state.emergency_stop),
            "clear_stale_pending_requested_ts": int(state.clear_stale_pending_requested_ts or 0),
            "clear_risk_breakers_requested_ts": int(state.clear_risk_breakers_requested_ts or 0),
            "updated_ts": int(state.updated_ts or 0),
        }

    def _validate_control_payload(self, payload: Mapping[str, object]) -> list[str]:
        errors: list[str] = []
        mode = str(payload.get("decision_mode") or "").strip().lower()
        if mode not in _VALID_DECISION_MODES:
            errors.append(f"invalid_decision_mode={mode or 'empty'}")
        pause_opening = bool(payload.get("pause_opening", False))
        reduce_only = bool(payload.get("reduce_only", False))
        emergency_stop = bool(payload.get("emergency_stop", False))
        if pause_opening and reduce_only:
            errors.append("mutually_exclusive_control_bits=pause_opening+reduce_only")
        if emergency_stop and (pause_opening or reduce_only):
            errors.append("mutually_exclusive_control_bits=emergency_stop_with_other_flags")
        updated_ts = int(self._safe_float(payload.get("updated_ts"), 0))
        now_ts = int(time.time())
        if updated_ts < 0:
            errors.append("invalid_updated_ts=negative")
        if updated_ts > now_ts + 86400:
            errors.append("invalid_updated_ts=future")
        clear_ts = int(self._safe_float(payload.get("clear_stale_pending_requested_ts"), 0))
        if clear_ts < 0:
            errors.append("invalid_clear_stale_pending_requested_ts=negative")
        if clear_ts > now_ts + 86400:
            errors.append("invalid_clear_stale_pending_requested_ts=future")
        clear_breaker_ts = int(self._safe_float(payload.get("clear_risk_breakers_requested_ts"), 0))
        if clear_breaker_ts < 0:
            errors.append("invalid_clear_risk_breakers_requested_ts=negative")
        if clear_breaker_ts > now_ts + 86400:
            errors.append("invalid_clear_risk_breakers_requested_ts=future")
        return errors

    @staticmethod
    def _intent_pending_status_from_order(order: Mapping[str, object]) -> str:
        recovery_status = str(order.get("recovery_status") or "").strip().lower()
        if recovery_status in {INTENT_STATUS_ACK_UNKNOWN, INTENT_STATUS_MANUAL_REQUIRED}:
            return recovery_status
        raw = str(order.get("broker_status") or "").strip().lower()
        if raw in {"cancel_requested", "canceled", "failed", "rejected", "unmatched", "filled"}:
            return raw
        if raw in {"partially_filled", "partial"}:
            return INTENT_STATUS_PARTIAL
        if raw in {"posted", "submitted", "open", "live", "delayed"}:
            return INTENT_STATUS_ACKED_PENDING
        return raw or INTENT_STATUS_ACKED_PENDING

    def _build_order_intents_from_pending(self, orders: list[dict[str, object]]) -> list[dict[str, object]]:
        intents: list[dict[str, object]] = []
        now_ts = int(time.time())
        for order in orders:
            signal_id = str(order.get("signal_id") or "").strip()
            side = str(order.get("side") or "").strip().upper()
            token_id = str(order.get("token_id") or "").strip()
            if not signal_id or side not in {"BUY", "SELL"} or not token_id:
                continue
            intent_id = str(order.get("intent_id") or signal_id or order.get("key") or order.get("order_id") or "").strip()
            if not intent_id:
                continue
            created_ts = int(self._safe_float(order.get("ts"), now_ts))
            updated_ts = max(now_ts, int(self._safe_float(order.get("last_heartbeat_ts"), 0)), created_ts)
            intents.append(
                {
                    "intent_id": intent_id,
                    "idempotency_key": str(order.get("idempotency_key") or ""),
                    "strategy_name": str(order.get("strategy_name") or ""),
                    "signal_source": str(order.get("signal_source") or ""),
                    "signal_fingerprint": str(order.get("signal_fingerprint") or ""),
                    "strategy_order_uuid": str(order.get("strategy_order_uuid") or ""),
                    "broker_order_id": str(order.get("order_id") or ""),
                    "token_id": token_id,
                    "condition_id": str(order.get("condition_id") or ""),
                    "side": side,
                    "status": self._intent_pending_status_from_order(order),
                    "recovered_source": str(order.get("recovery_source") or ""),
                    "recovery_reason": str(order.get("recovery_status") or ""),
                    "payload": dict(order),
                    "created_ts": created_ts,
                    "updated_ts": updated_ts,
                }
            )
        return intents

    def _build_order_intents_snapshot(self) -> list[dict[str, object]]:
        intents_by_id: dict[str, dict[str, object]] = {}
        for row in self._build_order_intents_from_pending([dict(order) for order in self.pending_orders.values()]):
            intents_by_id[str(row.get("intent_id") or "")] = row
        now_ts = int(time.time())
        for row in list(self.recent_orders):
            signal_id = str(row.get("signal_id") or "").strip()
            token_id = str(row.get("token_id") or "").strip()
            side = str(row.get("side") or "").strip().upper()
            if not signal_id or not token_id or side not in {"BUY", "SELL"}:
                continue
            status = str(row.get("status") or "").strip().lower() or "posted"
            intent_id = signal_id
            if intent_id in intents_by_id:
                continue
            intents_by_id[intent_id] = {
                "intent_id": intent_id,
                "idempotency_key": str(row.get("idempotency_key") or ""),
                "strategy_name": str(row.get("strategy_name") or ""),
                "signal_source": str(row.get("signal_source") or ""),
                "signal_fingerprint": str(row.get("signal_fingerprint") or ""),
                "strategy_order_uuid": str(row.get("strategy_order_uuid") or ""),
                "broker_order_id": str(row.get("order_id") or ""),
                "token_id": token_id,
                "condition_id": str(row.get("condition_id") or ""),
                "side": side,
                "status": status,
                "recovered_source": "runtime",
                "recovery_reason": "recent_order_snapshot",
                "payload": dict(row),
                "created_ts": int(self._safe_float(row.get("ts"), now_ts)),
                "updated_ts": now_ts,
            }
        return [row for _, row in sorted(intents_by_id.items(), key=lambda item: item[0])]

    def _pending_record_from_order_intent(self, intent: Mapping[str, object]) -> dict[str, object] | None:
        status = str(intent.get("status") or "").strip().lower()
        if status in _ORDER_INTENT_TERMINAL_STATUSES:
            return None
        payload = dict(intent.get("payload") or {})
        payload.setdefault("signal_id", str(intent.get("intent_id") or ""))
        payload.setdefault("order_id", str(intent.get("broker_order_id") or ""))
        payload.setdefault("token_id", str(intent.get("token_id") or ""))
        payload.setdefault("condition_id", str(intent.get("condition_id") or ""))
        payload.setdefault("side", str(intent.get("side") or ""))
        payload.setdefault("broker_status", status or "posted")
        payload.setdefault("recovery_source", str(intent.get("recovered_source") or "db"))
        payload.setdefault("recovery_status", str(intent.get("recovery_reason") or "db_restore"))
        payload.setdefault("strategy_order_uuid", str(intent.get("strategy_order_uuid") or ""))
        payload.setdefault("idempotency_key", str(intent.get("idempotency_key") or ""))
        payload.setdefault("strategy_name", str(intent.get("strategy_name") or ""))
        payload.setdefault("signal_source", str(intent.get("signal_source") or ""))
        payload.setdefault("signal_fingerprint", str(intent.get("signal_fingerprint") or ""))
        restored = self._restore_pending_order(payload)
        if restored is None:
            return None
        return restored

    def _load_broker_open_orders(self) -> list[OpenOrderSnapshot] | None:
        try:
            rows = self.broker.list_open_orders()
        except Exception as exc:
            self.log.warning("Broker open orders unavailable during startup recovery err=%s", exc)
            return None
        if rows is None:
            return None
        return [row for row in rows if isinstance(row, OpenOrderSnapshot)]

    def _load_broker_recent_fills(self) -> list[OrderFillSnapshot] | None:
        try:
            rows = self.broker.list_recent_fills(limit=400)
        except Exception as exc:
            self.log.warning("Broker recent fills unavailable during startup recovery err=%s", exc)
            return None
        if rows is None:
            return None
        return [row for row in rows if isinstance(row, OrderFillSnapshot)]

    def _normalize_position(self, row: dict[str, object]) -> dict[str, object] | None:
        token_id = str(row.get("token_id") or "").strip()
        if not token_id:
            return None

        quantity = self._safe_float(row.get("quantity"))
        notional = self._safe_float(row.get("notional"))
        cost_basis_notional = self._safe_float(row.get("cost_basis_notional"), notional)
        if quantity <= 0 or notional <= 0:
            return None

        return {
            "token_id": token_id,
            "condition_id": str(row.get("condition_id") or ""),
            "market_slug": str(row.get("market_slug") or token_id),
            "outcome": str(row.get("outcome") or "YES"),
            "quantity": quantity,
            "price": self._safe_float(row.get("price"), 0.5),
            "notional": notional,
            "cost_basis_notional": max(0.0, cost_basis_notional if cost_basis_notional > 0.0 else notional),
            "opened_ts": int(self._safe_float(row.get("opened_ts"))),
            "last_buy_ts": int(self._safe_float(row.get("last_buy_ts") or row.get("opened_ts"))),
            "last_trim_ts": int(self._safe_float(row.get("last_trim_ts"))),
            "entry_wallet": str(row.get("entry_wallet") or ""),
            "entry_wallet_score": self._safe_float(row.get("entry_wallet_score")),
            "entry_wallet_tier": str(row.get("entry_wallet_tier") or "LOW"),
            "entry_topic_label": str(row.get("entry_topic_label") or ""),
            "entry_topic_bias": str(row.get("entry_topic_bias") or "neutral"),
            "entry_topic_multiplier": self._safe_float(row.get("entry_topic_multiplier"), 1.0),
            "entry_topic_summary": str(row.get("entry_topic_summary") or ""),
            "entry_reason": str(row.get("entry_reason") or ""),
            "trace_id": str(row.get("trace_id") or ""),
            "origin_signal_id": str(row.get("origin_signal_id") or ""),
            "last_signal_id": str(row.get("last_signal_id") or ""),
            "last_exit_kind": str(row.get("last_exit_kind") or ""),
            "last_exit_label": str(row.get("last_exit_label") or ""),
            "last_exit_summary": str(row.get("last_exit_summary") or ""),
            "last_exit_ts": int(self._safe_float(row.get("last_exit_ts"))),
            "time_exit_state": normalize_time_exit_state(row.get("time_exit_state")).to_payload(),
        }

    def _set_positions_book(self, positions: list[dict[str, object]]) -> int:
        self.positions_book = {}
        for raw_pos in positions:
            normalized = self._normalize_position(raw_pos)
            if not normalized:
                continue
            token_id = str(normalized["token_id"])
            self.positions_book[token_id] = normalized

        self._refresh_risk_state()
        return self.state.open_positions

    def _load_broker_positions(self) -> list[dict[str, object]] | None:
        wallet = (self.settings.funder_address or "").strip()
        if not wallet:
            return []

        dust_floor = self._position_dust_floor_usd()
        snapshot: AccountingSnapshot | None = None
        try:
            snapshot = self.data_client.get_accounting_snapshot(wallet)
        except Exception as exc:
            self.log.debug("Broker accounting snapshot unavailable during position bootstrap wallet=%s err=%s", wallet, exc)

        if snapshot is not None:
            snapshot_position_count = len(tuple(snapshot.positions or ()))
            if snapshot_position_count == 0 and float(snapshot.positions_value) <= 1e-9:
                return []
            if float(snapshot.positions_value) <= dust_floor:
                return []

        try:
            positions = self.data_client.get_active_positions(wallet)
        except Exception as exc:
            self.log.warning("Broker reconciliation failed. wallet=%s err=%s", wallet, exc)
            return None

        records: list[dict[str, object]] = []
        for pos in positions:
            token_id = str(getattr(pos, "token_id", "")).strip()
            if not token_id:
                continue
            quantity = self._safe_float(getattr(pos, "size", 0.0))
            notional = self._safe_float(getattr(pos, "notional", 0.0))
            if quantity <= 0 or notional <= dust_floor:
                continue

            opened_ts = int(self._safe_float(getattr(pos, "timestamp", time.time())))
            records.append(
                {
                    "token_id": token_id,
                    "condition_id": str(getattr(pos, "condition_id", "") or ""),
                    "market_slug": str(getattr(pos, "market_slug", "") or token_id),
                    "outcome": str(getattr(pos, "outcome", "") or "YES"),
                    "quantity": quantity,
                    "price": self._safe_float(getattr(pos, "avg_price", 0.5), 0.5),
                    "notional": notional,
                    "opened_ts": opened_ts,
                    "last_buy_ts": opened_ts,
                    "last_trim_ts": 0,
                }
            )
        return records

    def _reconcile_runtime_state(self) -> None:
        source = "db"
        self._recovery_conflicts = []
        self._recovery_block_buy_latched = False
        truth_payload = dict(self._state_store.load_runtime_truth() if self._state_store is not None else {})
        runtime_snapshot = dict(truth_payload.get("runtime") or {})
        recovered_risk_state = dict(truth_payload.get("risk") or {})
        recovered_reconciliation = dict(truth_payload.get("reconciliation") or {})
        recovered_risk_breakers = dict(truth_payload.get("risk_breakers") or {})
        recovered_exposure_ledger = [
            dict(row) for row in list(truth_payload.get("exposure_ledger") or []) if isinstance(row, dict)
        ]
        recovered_positions = [
            dict(row) for row in list(truth_payload.get("positions") or []) if isinstance(row, dict)
        ]
        recovered_intents = [
            dict(row) for row in list(truth_payload.get("order_intents") or []) if isinstance(row, dict)
        ]
        recovered_control = dict(truth_payload.get("control") or {})
        if runtime_snapshot:
            self._last_broker_event_sync_ts = int(
                self._safe_float(
                    runtime_snapshot.get("broker_event_sync_ts"),
                    self._last_broker_event_sync_ts,
                )
            )
            self._restore_recent_order_keys(runtime_snapshot.get("recent_order_keys"))
            signal_cycles_raw = runtime_snapshot.get("signal_cycles")
            if isinstance(signal_cycles_raw, list):
                self.recent_signal_cycles = deque(
                    [row for row in signal_cycles_raw if isinstance(row, dict)][-24:],
                    maxlen=24,
                )
            trace_records_raw = runtime_snapshot.get("trace_registry")
            if isinstance(trace_records_raw, list):
                self._trace_registry = {}
                self._trace_order = deque(maxlen=64)
                for row in trace_records_raw[-64:]:
                    trace_id = str(row.get("trace_id") or "").strip()
                    if not trace_id:
                        continue
                    self._trace_registry[trace_id] = dict(row)
                    self._trace_order.append(trace_id)
            operator_action_raw = runtime_snapshot.get("last_operator_action")
            if isinstance(operator_action_raw, dict):
                self.last_operator_action = dict(operator_action_raw)
            kill_switch_raw = runtime_snapshot.get("kill_switch")
            if isinstance(kill_switch_raw, dict):
                self._kill_switch_state = normalize_kill_switch_state(kill_switch_raw)
            self._runner_heartbeat = normalize_runner_heartbeat(runtime_snapshot.get("runner_heartbeat"))
            self._buy_blocked_since_ts = max(0, int(self._safe_float(runtime_snapshot.get("buy_blocked_since_ts"), 0)))

        if recovered_risk_state:
            recovered_day_key = str(recovered_risk_state.get("day_key") or "").strip()
            if not recovered_day_key or recovered_day_key == self._active_day_key:
                self.state.daily_realized_pnl = self._safe_float(
                    recovered_risk_state.get("daily_realized_pnl"),
                    self.state.daily_realized_pnl,
                )
                self.state.broker_closed_pnl_today = self._safe_float(
                    recovered_risk_state.get("broker_closed_pnl_today"),
                    self.state.broker_closed_pnl_today,
                )
            self.state.equity_usd = self._safe_float(
                recovered_risk_state.get("equity_usd"),
                self.state.equity_usd,
            )
            self.state.cash_balance_usd = self._safe_float(
                recovered_risk_state.get("cash_balance_usd"),
                self.state.cash_balance_usd,
            )
            self.state.positions_value_usd = self._safe_float(
                recovered_risk_state.get("positions_value_usd"),
                self.state.positions_value_usd,
            )
            self.state.account_snapshot_ts = int(
                self._safe_float(
                    recovered_risk_state.get("account_snapshot_ts"),
                    self.state.account_snapshot_ts,
                )
            )
            self._risk_ledger_status = str(recovered_risk_state.get("risk_ledger_status") or self._risk_ledger_status or "ok")
            self._risk_breaker_status = str(
                recovered_risk_state.get("risk_breaker_status") or self._risk_breaker_status or "ok"
            )

        if recovered_risk_breakers:
            self._risk_breaker_state = RiskBreakerState.from_payload(recovered_risk_breakers).to_payload()
        else:
            self._risk_breaker_state = default_risk_breaker_state(day_key=self._risk_day_key(), now_ts=int(time.time()))

        if recovered_exposure_ledger:
            recovered_rows: dict[tuple[str, str], dict[str, object]] = {}
            for row in recovered_exposure_ledger:
                normalized = ExposureLedgerEntry.from_payload(row).to_payload()
                scope_type = str(normalized.get("scope_type") or "").strip().lower()
                scope_key = str(normalized.get("scope_key") or "").strip()
                if not scope_type or not scope_key:
                    continue
                recovered_rows[(scope_type, scope_key)] = normalized
            self._exposure_ledger = recovered_rows

        db_positions_by_token: dict[str, dict[str, object]] = {}
        for row in recovered_positions:
            normalized = self._normalize_position(row)
            if normalized is None:
                continue
            db_positions_by_token[str(normalized["token_id"])] = normalized

        intents: list[dict[str, object]] = []
        for row in recovered_intents:
            token_id = str(row.get("token_id") or "").strip()
            side = str(row.get("side") or "").strip().upper()
            intent_id = str(row.get("intent_id") or "").strip()
            if not token_id or side not in {"BUY", "SELL"} or not intent_id:
                continue
            intents.append(row)

        if recovered_control:
            self.control_state = ControlState.from_payload(recovered_control)

        if not self.settings.dry_run:
            broker_positions = self._load_broker_positions()
            broker_open_orders = self._load_broker_open_orders()
            broker_fills = list(self._load_broker_recent_fills() or [])
            if broker_positions is None or broker_open_orders is None:
                self._record_recovery_conflict(
                    category="BROKER_UNAVAILABLE",
                    details="broker positions or open orders unavailable during startup recovery",
                )
            else:
                source = "broker+db"
                broker_positions_by_token: dict[str, dict[str, object]] = {}
                for row in broker_positions:
                    normalized = self._normalize_position(row)
                    if normalized is None:
                        continue
                    broker_positions_by_token[str(normalized["token_id"])] = normalized

                merged_positions: dict[str, dict[str, object]] = {}
                for token_id, broker_row in broker_positions_by_token.items():
                    db_row = db_positions_by_token.get(token_id)
                    if db_row is not None:
                        merged_positions[token_id] = self._merge_recovered_position(
                            broker_row,
                            db_row,
                            recovery_source="db_snapshot",
                        )
                    else:
                        pending_buy_intent = next(
                            (
                                intent
                                for intent in intents
                                if str(intent.get("token_id") or "") == token_id
                                and str(intent.get("side") or "").upper() == "BUY"
                                and str(intent.get("status") or "").lower() not in _ORDER_INTENT_TERMINAL_STATUSES
                            ),
                            None,
                        )
                        if pending_buy_intent is not None:
                            pending_payload = dict(pending_buy_intent.get("payload") or {})
                            pending_payload.setdefault("token_id", token_id)
                            pending_payload.setdefault("side", "BUY")
                            merged_positions[token_id] = self._seed_position_from_pending_order(
                                dict(broker_row),
                                pending_payload,
                            )
                        else:
                            merged_positions[token_id] = dict(broker_row)

                for token_id, db_row in db_positions_by_token.items():
                    if token_id in merged_positions:
                        continue
                    close_evidence = any(
                        str(fill.token_id or "") == token_id and str(fill.side or "").upper() == "SELL"
                        for fill in broker_fills
                    )
                    if close_evidence:
                        continue
                    self._record_recovery_conflict(
                        category="AMBIGUOUS_POSITION",
                        token_id=token_id,
                        details="db_position_without_broker_and_without_close_evidence",
                    )
                    merged_positions[token_id] = dict(db_row)

                open_by_order_id = {
                    str(item.order_id or "").strip(): item
                    for item in broker_open_orders
                    if str(item.order_id or "").strip()
                }
                next_intents: list[dict[str, object]] = []
                seen_open_order_ids: set[str] = set()
                for intent in intents:
                    broker_order_id = str(intent.get("broker_order_id") or "").strip()
                    status = str(intent.get("status") or "posted").strip().lower() or "posted"
                    token_id = str(intent.get("token_id") or "").strip()
                    side = str(intent.get("side") or "").strip().upper()
                    if status in _ORDER_INTENT_TERMINAL_STATUSES:
                        next_intents.append(intent)
                        continue
                    open_order = open_by_order_id.get(broker_order_id)
                    if open_order is not None:
                        seen_open_order_ids.add(broker_order_id)
                        status = str(open_order.lifecycle_status or "posted").strip().lower() or "posted"
                        updated_payload = dict(intent.get("payload") or {})
                        updated_payload.update(
                            {
                                "order_id": str(open_order.order_id or ""),
                                "token_id": str(open_order.token_id or token_id),
                                "condition_id": str(open_order.condition_id or intent.get("condition_id") or ""),
                                "side": str(open_order.side or side).upper(),
                                "broker_status": status,
                                "requested_price": float(open_order.price or 0.0),
                                "requested_notional": float(open_order.requested_notional or 0.0),
                                "matched_size_hint": float(open_order.matched_size or 0.0),
                                "matched_notional_hint": float(open_order.matched_notional or 0.0),
                                "recovery_source": "broker",
                                "recovery_status": "broker_open_order",
                            }
                        )
                        intent["status"] = status
                        intent["payload"] = updated_payload
                        intent["updated_ts"] = int(time.time())
                        next_intents.append(intent)
                        continue
                    has_fill_evidence = False
                    for fill in broker_fills:
                        if broker_order_id and str(fill.order_id or "").strip() == broker_order_id:
                            has_fill_evidence = True
                            break
                        if not broker_order_id and token_id and str(fill.token_id or "").strip() == token_id and str(fill.side or "").upper() == side:
                            has_fill_evidence = True
                            break
                    if has_fill_evidence:
                        intent["status"] = "filled"
                        intent["updated_ts"] = int(time.time())
                        next_intents.append(intent)
                        continue
                    self._record_recovery_conflict(
                        category="AMBIGUOUS_PENDING",
                        token_id=token_id,
                        order_id=broker_order_id,
                        details="db_pending_without_broker_open_and_without_fill_evidence",
                    )
                    conflict_ts = int(time.time())
                    conflict_reason = "db_pending_without_broker_open_and_without_fill_evidence"
                    updated_payload = dict(intent.get("payload") or {})
                    updated_payload["recovery_source"] = "db"
                    updated_payload["recovery_status"] = conflict_reason
                    updated_payload["reconcile_ambiguous_reason"] = conflict_reason
                    updated_payload["reconcile_ambiguous_ts"] = conflict_ts
                    intent["status"] = "posted"
                    intent["recovered_source"] = "db"
                    intent["recovery_reason"] = conflict_reason
                    intent["payload"] = updated_payload
                    intent["updated_ts"] = conflict_ts
                    next_intents.append(intent)

                for order_id, open_order in open_by_order_id.items():
                    if order_id in seen_open_order_ids:
                        continue
                    intent_id = f"broker-recovered:{order_id}"
                    status = str(open_order.lifecycle_status or "posted").strip().lower() or "posted"
                    payload = {
                        "key": intent_id,
                        "signal_id": intent_id,
                        "order_id": order_id,
                        "token_id": str(open_order.token_id or ""),
                        "condition_id": str(open_order.condition_id or ""),
                        "market_slug": str(open_order.market_slug or ""),
                        "outcome": str(open_order.outcome or "YES"),
                        "side": str(open_order.side or "").upper(),
                        "broker_status": status,
                        "requested_price": float(open_order.price or 0.0),
                        "requested_notional": float(open_order.requested_notional or 0.0),
                        "matched_size_hint": float(open_order.matched_size or 0.0),
                        "matched_notional_hint": float(open_order.matched_notional or 0.0),
                        "ts": int(open_order.created_ts or time.time()),
                        "recovery_source": "broker",
                        "recovery_status": "broker_open_without_db_intent",
                        "message": "broker recovered open order",
                        "reason": "broker recovered open order",
                    }
                    next_intents.append(
                        {
                            "intent_id": intent_id,
                            "strategy_order_uuid": "",
                            "broker_order_id": order_id,
                            "token_id": str(open_order.token_id or ""),
                            "condition_id": str(open_order.condition_id or ""),
                            "side": str(open_order.side or "").upper(),
                            "status": status,
                            "recovered_source": "broker",
                            "recovery_reason": "broker_open_without_db_intent",
                            "payload": payload,
                            "created_ts": int(open_order.created_ts or time.time()),
                            "updated_ts": int(time.time()),
                        }
                    )
                    self._record_recovery_conflict(
                        category="BROKER_OPEN_WITHOUT_INTENT",
                        token_id=str(open_order.token_id or ""),
                        order_id=order_id,
                        details="broker open order recovered from exchange without db intent",
                    )

                intents = next_intents
                db_positions_by_token = merged_positions

        self.pending_orders = {}
        for intent in intents:
            restored = self._pending_record_from_order_intent(intent)
            if restored is None:
                continue
            self.pending_orders[str(restored["key"])] = restored

        if db_positions_by_token:
            self._set_positions_book(list(db_positions_by_token.values()))
            self.log.info("Recovered positions source=%s count=%d", source, len(db_positions_by_token))
        else:
            self.positions_book = {}
            self._refresh_risk_state()
            self.log.info("No positions found for startup reconcile")

        if recovered_reconciliation:
            self.reconciliation_status = str(recovered_reconciliation.get("status") or self.reconciliation_status or "unknown")

        ledger_realized_pnl = self._recover_daily_realized_pnl_from_ledger(self._active_day_key)
        if ledger_realized_pnl is not None:
            self.state.daily_realized_pnl = float(ledger_realized_pnl)

        self._append_event(
            "runtime_reconcile",
            {
                "source": source,
                "position_count": int(len(self.positions_book)),
                "pending_count": int(len(self.pending_orders)),
                "recovery_conflicts": list(self._recovery_conflicts),
            },
        )
        self._load_control_state()
        self._kill_switch_state = normalize_kill_switch_state(self._kill_switch_state)
        self._refresh_risk_state()

    def _reconcile_runtime_with_broker(self) -> None:
        dry_run_pending_reconcile = self._supports_dry_run_pending_reconcile()
        if self.settings.dry_run and not dry_run_pending_reconcile:
            return
        if self.settings.dry_run and dry_run_pending_reconcile and not self.pending_orders:
            return

        positions = self._load_broker_positions()
        if positions is None:
            self.log.warning("Skip periodic broker reconciliation due to active wallet fetch failure")
            return

        reconciled: dict[str, dict[str, object]] = {}
        for row in positions:
            normalized = self._normalize_position(row)
            if not normalized:
                continue
            reconciled[str(normalized["token_id"])] = normalized

        previous_positions = {token_id: dict(position) for token_id, position in self.positions_book.items()}
        for token_id, normalized in list(reconciled.items()):
            prev_pos = previous_positions.get(token_id)
            if prev_pos:
                reconciled[token_id] = self._merge_recovered_position(
                    normalized,
                    prev_pos,
                    recovery_source="runtime_previous",
                )
                continue
            pending_buy = next(
                (
                    order
                    for order in sorted(self.pending_orders.values(), key=lambda item: int(item.get("ts") or 0))
                    if str(order.get("token_id") or "") == token_id and str(order.get("side") or "").upper() == "BUY"
                ),
                None,
            )
            if pending_buy:
                reconciled[token_id] = self._seed_position_from_pending_order(normalized, pending_buy)

        for order in self.pending_orders.values():
            if str(order.get("side") or "").upper() != "SELL":
                continue
            if str(order.get("broker_status") or "") not in {"submitted", "posted", "open", "live", "delayed", "partially_filled"}:
                continue
            token_id = str(order.get("token_id") or "")
            if not token_id or token_id in reconciled:
                continue
            previous_position = previous_positions.get(token_id)
            if previous_position is None:
                continue
            reconciled[token_id] = dict(previous_position)

        self._reconcile_pending_orders(
            previous_positions=previous_positions,
            next_positions=reconciled,
            now=int(time.time()),
        )

        prev_keys = set(previous_positions.keys())
        next_keys = set(reconciled.keys())
        removed = sorted(prev_keys - next_keys)
        added = sorted(next_keys - prev_keys)

        updated = []
        for token_id in sorted(prev_keys & next_keys):
            prev_pos = previous_positions.get(token_id, {})
            next_pos = reconciled.get(token_id, {})
            if abs(float(prev_pos.get("notional", 0.0) or 0.0) - float(next_pos.get("notional", 0.0) or 0.0)) > 1e-6:
                updated.append(token_id)
            elif abs(float(prev_pos.get("quantity", 0.0) or 0.0) - float(next_pos.get("quantity", 0.0) or 0.0)) > 1e-6:
                updated.append(token_id)

        if removed or added or updated:
            self.positions_book = reconciled
            self._refresh_risk_state()
            if removed:
                self._maybe_sync_account_state(force=True)
            self.log.warning(
                "Periodic broker reconciliation changed positions added=%d removed=%d updated=%d open=%d",
                len(added),
                len(removed),
                len(updated),
                self.state.open_positions,
            )
            self._append_event(
                "runtime_reconcile",
                {
                    "source": "broker_periodic",
                    "added": len(added),
                    "removed": len(removed),
                    "updated": len(updated),
                    "open_positions": self.state.open_positions,
                    "pending_orders": len(self.pending_orders),
                },
            )

    def _maybe_reconcile_runtime(self) -> None:
        interval = int(self.settings.runtime_reconcile_interval_seconds)
        if interval <= 0:
            return
        dry_run_pending_reconcile = self._supports_dry_run_pending_reconcile()
        if self.settings.dry_run and not dry_run_pending_reconcile:
            return
        if self.settings.dry_run and dry_run_pending_reconcile and not self.pending_orders:
            return

        now = time.time()
        if (now - self._last_broker_reconcile_ts) < interval:
            return
        self._last_broker_reconcile_ts = now
        self._reconcile_runtime_with_broker()

    def _supports_dry_run_pending_reconcile(self) -> bool:
        if not self.settings.dry_run:
            return False
        capability = getattr(self.broker, "supports_dry_run_pending_reconcile", None)
        if not callable(capability):
            return False
        return bool(capability())

    def _enforce_condition_netting(
        self,
        signal: Signal,
        requested_notional: float,
    ) -> tuple[float, dict[str, object]]:
        exposure_key, key_source = self._signal_condition_exposure_key(signal)
        cap_pct = float(self.settings.max_condition_exposure_pct)
        cap_usd = float(self.settings.bankroll_usd) * cap_pct
        current_exposure = self._condition_exposure_notional_usd(exposure_key)
        remaining_capacity = max(0.0, cap_usd - current_exposure)
        snapshot = {
            "enabled": bool(self.settings.portfolio_netting_enabled),
            "condition_id": str(signal.condition_id or ""),
            "condition_key": exposure_key,
            "condition_key_source": key_source,
            "condition_exposure_cap_usd": cap_usd,
            "condition_exposure_current_usd": current_exposure,
            "condition_exposure_remaining_usd": remaining_capacity,
        }
        if signal.side != "BUY":
            return requested_notional, snapshot
        if not bool(self.settings.portfolio_netting_enabled):
            return requested_notional, snapshot
        if cap_usd <= 0.0 or not exposure_key:
            return requested_notional, snapshot
        if remaining_capacity <= 0.0:
            self._append_event(
                "signal_skip",
                {
                    "wallet": signal.wallet,
                    "market_slug": signal.market_slug,
                    "token_id": signal.token_id,
                    "condition_id": str(signal.condition_id or ""),
                    "reason": "condition_exposure_cap_reached",
                    "condition_key": exposure_key,
                    "condition_key_source": key_source,
                    "condition_exposure_cap_usd": cap_usd,
                    "condition_exposure_current_usd": current_exposure,
                },
            )
            return 0.0, snapshot

        allowed_notional = min(requested_notional, remaining_capacity)
        actionable_floor = self._actionable_notional_floor_usd()
        if allowed_notional < actionable_floor:
            self._append_event(
                "signal_skip",
                {
                    "wallet": signal.wallet,
                    "market_slug": signal.market_slug,
                    "token_id": signal.token_id,
                    "condition_id": str(signal.condition_id or ""),
                    "reason": "condition_exposure_too_small",
                    "condition_key": exposure_key,
                    "condition_key_source": key_source,
                    "condition_exposure_cap_usd": cap_usd,
                    "condition_exposure_current_usd": current_exposure,
                    "condition_exposure_remaining_usd": remaining_capacity,
                    "allowed_notional": allowed_notional,
                    "actionable_notional_floor_usd": actionable_floor,
                },
            )
            return 0.0, snapshot

        if allowed_notional + 1e-9 < requested_notional:
            self.log.warning(
                "BUY condition netting clamp slug=%s token=%s requested=%.2f allowed=%.2f current_condition=%.2f cap=%.2f",
                signal.market_slug,
                signal.token_id,
                requested_notional,
                allowed_notional,
                current_exposure,
                cap_usd,
            )
            self._append_event(
                "order_notional_clamp",
                {
                    "wallet": signal.wallet,
                    "market_slug": signal.market_slug,
                    "token_id": signal.token_id,
                    "condition_id": str(signal.condition_id or ""),
                    "reason": "condition_netting",
                    "condition_key": exposure_key,
                    "condition_key_source": key_source,
                    "requested_notional": requested_notional,
                    "allowed_notional": allowed_notional,
                    "condition_exposure_cap_usd": cap_usd,
                    "condition_exposure_current_usd": current_exposure,
                    "condition_exposure_remaining_usd": remaining_capacity,
                },
            )
        return allowed_notional, snapshot

    def _enforce_buy_budget(self, signal: Signal, requested_notional: float) -> float:
        if signal.side != "BUY":
            return requested_notional

        available = self._available_notional_usd()
        if available <= 0.0:
            self._append_event(
                "signal_skip",
                {
                    "wallet": signal.wallet,
                    "market_slug": signal.market_slug,
                    "token_id": signal.token_id,
                    "reason": "budget_exhausted",
                    "available_notional": available,
                },
            )
            return 0.0

        allowed_notional = min(requested_notional, available)
        actionable_floor = self._actionable_notional_floor_usd()
        if allowed_notional < actionable_floor:
            self._append_event(
                "signal_skip",
                {
                    "wallet": signal.wallet,
                    "market_slug": signal.market_slug,
                    "token_id": signal.token_id,
                    "reason": "notional_too_small_after_budget",
                    "allowed_notional": allowed_notional,
                    "available_notional": available,
                    "actionable_notional_floor_usd": actionable_floor,
                },
            )
            return 0.0

        if allowed_notional < requested_notional:
            self.log.warning(
                "BUY budget clamp signal=%s token=%s requested=%.2f available=%.2f",
                signal.market_slug,
                signal.token_id,
                requested_notional,
                available,
            )
            self._append_event(
                "order_notional_clamp",
                {
                    "wallet": signal.wallet,
                    "market_slug": signal.market_slug,
                    "token_id": signal.token_id,
                    "requested_notional": requested_notional,
                    "allowed_notional": allowed_notional,
                    "available_notional": available,
                },
            )
        return allowed_notional

    def _sell_target_notional(
        self,
        signal: Signal,
        position: dict[str, object],
        requested_notional: float,
    ) -> float:
        current_notional = max(0.0, float(position.get("notional") or 0.0))
        if current_notional <= 0.0:
            return 0.0

        exit_fraction = max(0.0, min(1.0, float(signal.exit_fraction or 0.0)))
        if exit_fraction > 0.0:
            target_notional = current_notional * exit_fraction
        else:
            target_notional = max(0.0, float(requested_notional or 0.0))
        target_notional = min(current_notional, target_notional)
        actionable_floor = self._actionable_notional_floor_usd()
        if target_notional < actionable_floor:
            self._append_event(
                "signal_skip",
                {
                    "wallet": signal.wallet,
                    "market_slug": signal.market_slug,
                    "token_id": signal.token_id,
                    "reason": "sell_notional_too_small",
                    "target_notional": target_notional,
                    "current_notional": current_notional,
                    "exit_fraction": exit_fraction,
                    "actionable_notional_floor_usd": actionable_floor,
                },
            )
            return 0.0
        return target_notional

    def _wallet_score_multiplier(self, signal: Signal) -> float:
        score = max(0.0, float(signal.wallet_score or 0.0))
        if score < float(self.settings.min_wallet_score):
            return 0.0
        if score >= 80.0:
            return float(self.settings.wallet_score_core_multiplier)
        if score >= 65.0:
            return float(self.settings.wallet_score_trade_multiplier)
        return float(self.settings.wallet_score_watch_multiplier)

    def _apply_wallet_score_sizing(self, signal: Signal, requested_notional: float) -> float:
        if signal.side != "BUY":
            return requested_notional

        multiplier = self._wallet_score_multiplier(signal)
        if multiplier <= 0.0:
            self._append_event(
                "signal_skip",
                {
                    "wallet": signal.wallet,
                    "market_slug": signal.market_slug,
                    "token_id": signal.token_id,
                    "reason": "wallet_score_below_min",
                    "wallet_score": signal.wallet_score,
                    "wallet_tier": signal.wallet_tier,
                },
            )
            return 0.0

        adjusted_notional = requested_notional * multiplier
        if abs(adjusted_notional - requested_notional) <= 1e-9:
            return adjusted_notional

        self._append_event(
            "wallet_score_notional_adjust",
            {
                "wallet": signal.wallet,
                "market_slug": signal.market_slug,
                "token_id": signal.token_id,
                "wallet_score": signal.wallet_score,
                "wallet_tier": signal.wallet_tier,
                "multiplier": multiplier,
                "requested_notional": requested_notional,
                "adjusted_notional": adjusted_notional,
            },
        )
        return adjusted_notional

    def _topic_profile_multiplier(self, signal: Signal) -> tuple[float, str]:
        if not bool(self.settings.topic_bias_enabled):
            return 1.0, "disabled"

        sample_count = max(0, int(signal.topic_sample_count or 0))
        if signal.side != "BUY":
            return 1.0, "non_buy"
        if sample_count < int(self.settings.topic_min_samples):
            return 1.0, "insufficient_samples"

        topic_roi = float(signal.topic_roi or 0.0)
        topic_win_rate = float(signal.topic_win_rate or 0.0)
        if (
            topic_roi >= float(self.settings.topic_positive_roi)
            and topic_win_rate >= float(self.settings.topic_positive_win_rate)
        ):
            return float(self.settings.topic_boost_multiplier), "boost"
        if (
            topic_roi <= float(self.settings.topic_negative_roi)
            or topic_win_rate <= float(self.settings.topic_negative_win_rate)
        ):
            return float(self.settings.topic_penalty_multiplier), "penalty"
        return 1.0, "neutral"

    def _apply_topic_profile_sizing(self, signal: Signal, requested_notional: float) -> float:
        signal.topic_bias = "neutral"
        signal.topic_multiplier = 1.0
        if requested_notional <= 0.0:
            return requested_notional

        multiplier, bias = self._topic_profile_multiplier(signal)
        signal.topic_bias = bias
        signal.topic_multiplier = multiplier
        if abs(multiplier - 1.0) <= 1e-9:
            return requested_notional

        adjusted_notional = requested_notional * multiplier
        self._append_event(
            "topic_profile_notional_adjust",
            {
                "wallet": signal.wallet,
                "market_slug": signal.market_slug,
                "token_id": signal.token_id,
                "topic_key": signal.topic_key,
                "topic_label": signal.topic_label,
                "topic_bias": bias,
                "topic_multiplier": multiplier,
                "topic_sample_count": signal.topic_sample_count,
                "topic_win_rate": signal.topic_win_rate,
                "topic_roi": signal.topic_roi,
                "requested_notional": requested_notional,
                "adjusted_notional": adjusted_notional,
            },
        )
        return adjusted_notional

    @staticmethod
    def _order_meta(
        side: str,
        *,
        exit_kind: str = "",
        exit_label: str = "",
        exit_summary: str = "",
    ) -> dict[str, object]:
        return {
            "flow": "exit" if str(side).upper() == "SELL" else "entry",
            "exit_kind": exit_kind,
            "exit_label": exit_label,
            "exit_summary": exit_summary,
        }

    @classmethod
    def _signal_order_meta(cls, signal: Signal) -> dict[str, object]:
        if signal.side != "SELL":
            return cls._order_meta(signal.side)
        if bool(signal.cross_wallet_exit):
            return cls._order_meta(
                signal.side,
                exit_kind="resonance_exit",
                exit_label=cls._exit_kind_label("resonance_exit"),
                exit_summary=str(signal.exit_reason or f"{int(signal.exit_wallet_count or 0)} wallets"),
            )
        if str(signal.wallet or "").strip().lower() == "system-time-exit":
            return cls._order_meta(signal.side, exit_kind="time_exit", exit_label=cls._exit_kind_label("time_exit"), exit_summary=str(signal.exit_reason or "time-exit"))
        if str(signal.wallet or "").strip().lower() == "system-emergency-stop":
            return cls._order_meta(signal.side, exit_kind="emergency_exit", exit_label=cls._exit_kind_label("emergency_exit"), exit_summary=str(signal.exit_reason or "emergency-exit"))
        return cls._order_meta(
            signal.side,
            exit_kind="smart_wallet_exit",
            exit_label=cls._exit_kind_label("smart_wallet_exit"),
            exit_summary=str(signal.exit_reason or "source wallet exit"),
        )

    @staticmethod
    def _apply_position_exit_meta(
        position: dict[str, object],
        *,
        exit_kind: str,
        exit_label: str,
        exit_summary: str,
        ts: int,
    ) -> None:
        position["last_exit_kind"] = exit_kind
        position["last_exit_label"] = exit_label
        position["last_exit_summary"] = exit_summary
        position["last_exit_ts"] = ts

    @staticmethod
    def _position_hold_minutes(position: dict[str, object] | None, now: int) -> int:
        source = position or {}
        opened_ts = int(source.get("opened_ts") or 0)
        if opened_ts <= 0 or now <= opened_ts:
            return 0
        return max(0, int((now - opened_ts) // 60))

    @staticmethod
    def _position_entry_context(position: dict[str, object] | None) -> dict[str, object]:
        source = position or {}
        return {
            "entry_wallet": str(source.get("entry_wallet") or ""),
            "entry_wallet_score": float(source.get("entry_wallet_score") or 0.0),
            "entry_wallet_tier": str(source.get("entry_wallet_tier") or ""),
            "entry_topic_label": str(source.get("entry_topic_label") or ""),
            "entry_topic_bias": str(source.get("entry_topic_bias") or "neutral"),
            "entry_topic_multiplier": float(source.get("entry_topic_multiplier") or 1.0),
            "entry_topic_summary": str(source.get("entry_topic_summary") or ""),
            "entry_reason": str(source.get("entry_reason") or ""),
            "trace_id": str(source.get("trace_id") or ""),
            "origin_signal_id": str(source.get("origin_signal_id") or ""),
            "last_signal_id": str(source.get("last_signal_id") or ""),
        }

    @staticmethod
    def _time_exit_state(position: dict[str, object] | None) -> dict[str, object]:
        source = position or {}
        return normalize_time_exit_state(source.get("time_exit_state")).to_payload()

    @staticmethod
    def _set_time_exit_state(position: dict[str, object], state: object) -> dict[str, object]:
        payload = normalize_time_exit_state(state).to_payload()
        position["time_exit_state"] = payload
        return payload

    def _time_exit_market_snapshot(self, position: dict[str, object]) -> tuple[float, float, float]:
        token_id = str(position.get("token_id") or "")
        if not token_id:
            return 0.0, 0.0, 0.0
        best_bid = 0.0
        best_ask = 0.0
        midpoint = 0.0
        try:
            order_book = self.data_client.get_order_book(token_id)
        except Exception:
            order_book = None
        if order_book is not None:
            best_bid = self._safe_float(getattr(order_book, "best_bid", 0.0))
            best_ask = self._safe_float(getattr(order_book, "best_ask", 0.0))
        try:
            midpoint = self._safe_float(self.data_client.get_midpoint_price(token_id))
        except Exception:
            midpoint = 0.0
        if midpoint <= 0.0 and best_bid > 0.0 and best_ask > 0.0:
            midpoint = (best_bid + best_ask) / 2.0
        return best_bid, best_ask, midpoint

    def _time_exit_priority_state(
        self,
        position: dict[str, object],
        *,
        now: int,
    ) -> dict[str, object]:
        current_state = normalize_time_exit_state(position.get("time_exit_state"))
        best_bid, best_ask, midpoint = self._time_exit_market_snapshot(position)
        volatility_bps = estimate_time_exit_volatility_bps(
            best_bid=best_bid,
            best_ask=best_ask,
            midpoint=midpoint,
            reference_price=self._safe_float(position.get("price"), 0.0),
        )
        next_state = begin_time_exit_attempt(
            current_state,
            now_ts=now,
            market_volatility_bps=volatility_bps,
            volatility_step_bps=float(self.settings.time_exit_priority_volatility_step_bps),
        )
        return {
            "state": next_state,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "midpoint": midpoint,
            "volatility_bps": volatility_bps,
            "force_exit": next_state.stage == TIME_EXIT_STAGE_FORCE_EXIT,
            "priority": int(next_state.priority),
            "priority_reason": str(next_state.priority_reason or ""),
        }

    @staticmethod
    def _signal_entry_context(signal: Signal, reason: str) -> dict[str, object]:
        return {
            "entry_wallet": str(signal.wallet or ""),
            "entry_wallet_score": float(signal.wallet_score or 0.0),
            "entry_wallet_tier": str(signal.wallet_tier or ""),
            "entry_topic_label": str(signal.topic_label or ""),
            "entry_topic_bias": str(signal.topic_bias or "neutral"),
            "entry_topic_multiplier": float(signal.topic_multiplier or 1.0),
            "entry_topic_summary": str(signal.topic_score_summary or ""),
            "entry_reason": str(reason or ""),
            "trace_id": str(signal.trace_id or ""),
            "origin_signal_id": str(signal.signal_id or ""),
            "last_signal_id": str(signal.signal_id or ""),
        }

    @staticmethod
    def _recent_order_status(result: ExecutionResult) -> str:
        if not result.ok:
            return "REJECTED"
        if result.has_fill:
            return "FILLED"
        return "PENDING"

    @staticmethod
    def _pending_order_key(signal: Signal, order_id: str | None) -> str:
        current = str(order_id or "").strip()
        if current:
            return current
        return f"{str(signal.signal_id or '')}:{str(signal.side or '')}:{str(signal.token_id or '')}"

    @staticmethod
    def _pending_order_entry_context(order: dict[str, object]) -> dict[str, object]:
        return {
            "entry_wallet": str(order.get("entry_wallet") or ""),
            "entry_wallet_score": float(order.get("entry_wallet_score") or 0.0),
            "entry_wallet_tier": str(order.get("entry_wallet_tier") or ""),
            "entry_topic_label": str(order.get("entry_topic_label") or ""),
            "entry_topic_bias": str(order.get("entry_topic_bias") or "neutral"),
            "entry_topic_multiplier": float(order.get("entry_topic_multiplier") or 1.0),
            "entry_topic_summary": str(order.get("entry_topic_summary") or ""),
            "entry_reason": str(order.get("entry_reason") or ""),
            "trace_id": str(order.get("trace_id") or ""),
            "origin_signal_id": str(order.get("origin_signal_id") or ""),
            "last_signal_id": str(order.get("last_signal_id") or ""),
        }

    def _pending_timeout_seconds(self) -> int:
        return max(
            int(self.settings.pending_order_timeout_seconds),
            int(self.settings.runtime_reconcile_interval_seconds) * 2,
        )

    def _pending_cancel_retry_seconds(self) -> int:
        return max(60, int(self.settings.poll_interval_seconds) * 2)

    @staticmethod
    def _normalize_cancel_status(
        response: Mapping[str, object] | None,
    ) -> tuple[str, bool, str]:
        if not isinstance(response, Mapping):
            return ("failed", False, "cancel response missing")

        status = str(response.get("status") or response.get("state") or "").strip().lower()
        if status == "cancelled":
            status = "canceled"
        ok_value = response.get("ok")
        if ok_value is False:
            status = "failed"
        elif status in {"submitted", "posted", "open", "live", "delayed", "accepted", "pending", "queued", "requested", "cancel_requested"}:
            status = "requested"
        elif status in {"unknown", "ambiguous", "indeterminate"}:
            status = "unknown"
        elif not status:
            status = "requested" if ok_value is True else "unknown"
        ok = bool(ok_value) if ok_value is not None else status not in {"failed", "rejected", "error", "unsupported"}
        if not ok and status not in {"failed", "rejected", "error", "unsupported"}:
            status = "failed"
        message = str(response.get("message") or response.get("error") or "").strip()
        return (status, ok, message)

    def _record_pending_cancel_outcome(
        self,
        *,
        order: dict[str, object],
        now: int,
        position_lookup: Mapping[str, dict[str, object]],
        recent_status: str,
        broker_status: str,
        action_reason: str,
        message: str,
        ok: bool,
    ) -> None:
        token_id = str(order.get("token_id") or "")
        side = str(order.get("side") or "").upper()
        market_context = self._market_context_from_order(order)
        self.recent_orders.appendleft(
            {
                "ts": now,
                "cycle_id": str(order.get("cycle_id") or ""),
                "signal_id": str(order.get("signal_id") or ""),
                "trace_id": str(order.get("trace_id") or ""),
                "title": str(order.get("market_slug") or token_id),
                "token_id": token_id,
                "outcome": str(order.get("outcome") or ""),
                "side": side,
                "status": recent_status,
                "order_id": str(order.get("order_id") or ""),
                "broker_status": broker_status,
                "retry_count": int(order.get("cancel_request_count") or 0),
                "latency_ms": max(0, (now - int(order.get("ts") or now)) * 1000),
                "reason": f"{action_reason} | {message}",
                "source_wallet": str(order.get("wallet") or ""),
                "hold_minutes": self._position_hold_minutes(position_lookup.get(token_id), now) if side == "SELL" else 0,
                "notional": float(order.get("requested_notional") or 0.0),
                "wallet_score": float(order.get("wallet_score") or 0.0),
                "wallet_tier": str(order.get("wallet_tier") or ""),
                "position_action": str(order.get("position_action") or ""),
                "position_action_label": str(order.get("position_action_label") or ""),
                "topic_label": str(order.get("topic_label") or ""),
                "topic_bias": str(order.get("topic_bias") or "neutral"),
                "topic_multiplier": float(order.get("topic_multiplier") or 1.0),
                **self._pending_order_entry_context(order),
                **(
                    self._exit_result_meta(
                        exit_kind=str(order.get("exit_kind") or ""),
                        ok=ok,
                    )
                    if side == "SELL"
                    else {}
                ),
                **self._order_meta(
                    side,
                    exit_kind=str(order.get("exit_kind") or ""),
                    exit_label=str(order.get("exit_label") or ""),
                    exit_summary=str(order.get("exit_summary") or ""),
                ),
                **self._pending_order_snapshot(order),
                **market_context,
            }
        )
        self._append_event(
            "pending_cancel",
            {
                "wallet": str(order.get("wallet") or ""),
                "market_slug": str(order.get("market_slug") or token_id),
                "token_id": token_id,
                "cycle_id": str(order.get("cycle_id") or ""),
                "signal_id": str(order.get("signal_id") or ""),
                "trace_id": str(order.get("trace_id") or ""),
                "side": side,
                "order_id": str(order.get("order_id") or ""),
                "broker_status": broker_status,
                "reason": action_reason,
                "message": message,
                "ok": bool(ok),
                **market_context,
            },
        )

    def _cancel_pending_order(
        self,
        *,
        order: dict[str, object],
        now: int,
        position_lookup: Mapping[str, dict[str, object]],
        action_reason: str,
        force: bool = False,
    ) -> tuple[str, dict[str, object]]:
        order_id = str(order.get("order_id") or "").strip()
        retry_seconds = self._pending_cancel_retry_seconds()
        last_request_ts = int(order.get("cancel_requested_ts") or 0)
        if order_id and not force and last_request_ts > 0 and (now - last_request_ts) < retry_seconds:
            return ("throttled", order)

        response = self.broker.cancel_order(order_id) if order_id else None
        status, ok, message = self._normalize_cancel_status(response if isinstance(response, Mapping) else None)
        if not order_id:
            status = "failed"
            ok = False
            message = "missing broker order id for cancel"
        if not message:
            if status == "canceled":
                message = "broker cancel confirmed"
            elif status == "requested":
                message = "broker cancel requested"
            elif status == "unsupported":
                message = "broker cancel unavailable"
            else:
                message = "broker cancel failed"

        updated = dict(order)
        updated["cancel_last_status"] = status
        updated["cancel_last_message"] = message
        if order_id and status in {"requested", "canceled"} and ok:
            updated["cancel_requested_ts"] = now
        if order_id and (force or last_request_ts <= 0 or (now - last_request_ts) >= retry_seconds):
            updated["cancel_request_count"] = int(updated.get("cancel_request_count") or 0) + 1

        if status in {"requested", "unknown"} and ok:
            updated["broker_status"] = "cancel_requested" if status == "requested" else "cancel_unknown"
            updated["message"] = message
            updated["reason"] = message
            self._record_pending_cancel_outcome(
                order=updated,
                now=now,
                position_lookup=position_lookup,
                recent_status="CANCEL_REQUESTED" if status == "requested" else "CANCEL_UNKNOWN",
                broker_status=str(updated["broker_status"]),
                action_reason=action_reason,
                message=message,
                ok=True,
            )
            return ("requested", updated)
        if status == "canceled" and ok:
            updated["broker_status"] = "canceled"
            self._record_pending_cancel_outcome(
                order=updated,
                now=now,
                position_lookup=position_lookup,
                recent_status="CANCELED",
                broker_status="canceled",
                action_reason=action_reason,
                message=message,
                ok=True,
            )
            return ("canceled", updated)

        updated["cancel_failed_ts"] = now
        updated["message"] = message
        updated["reason"] = message
        failure_status = "CANCEL_UNSUPPORTED" if status == "unsupported" else "CANCEL_FAILED"
        self._record_pending_cancel_outcome(
            order=updated,
            now=now,
            position_lookup=position_lookup,
            recent_status=failure_status,
            broker_status=status,
            action_reason=action_reason,
            message=message,
            ok=False,
        )
        return ("failed", updated)

    def _apply_control_pending_entry_cancels(self, control: ControlState, *, now: int) -> None:
        action_reason = self._pending_entry_cancel_reason(control)
        if not action_reason:
            return

        pending_keys = [
            key
            for key, order in sorted(self.pending_orders.items(), key=lambda item: int(item[1].get("ts") or 0))
            if str(order.get("side") or "").upper() == "BUY"
        ]
        if not pending_keys:
            return

        remaining = {
            key: order
            for key, order in self.pending_orders.items()
            if key not in pending_keys
        }
        for key in pending_keys:
            order = self.pending_orders.get(key)
            if not order:
                continue
            outcome, updated = self._cancel_pending_order(
                order=order,
                now=now,
                position_lookup=self.positions_book,
                action_reason=action_reason,
                force=False,
            )
            if outcome != "canceled":
                remaining[key] = updated
        self.pending_orders = remaining
        self._refresh_risk_state()

    def _pending_order_snapshot(self, order: dict[str, object]) -> dict[str, object]:
        return {
            "order_id": str(order.get("order_id") or ""),
            "broker_status": str(order.get("broker_status") or ""),
            "requested_notional": float(order.get("requested_notional") or 0.0),
            "requested_price": float(order.get("requested_price") or 0.0),
            "matched_notional_hint": float(order.get("matched_notional_hint") or 0.0),
            "matched_size_hint": float(order.get("matched_size_hint") or 0.0),
            "matched_price_hint": float(order.get("matched_price_hint") or 0.0),
            "reconciled_notional_hint": float(order.get("reconciled_notional_hint") or 0.0),
            "reconciled_size_hint": float(order.get("reconciled_size_hint") or 0.0),
            "last_fill_ts_hint": int(order.get("last_fill_ts_hint") or 0),
            "strategy_order_uuid": str(order.get("strategy_order_uuid") or ""),
            "pending_class": str(order.get("pending_class") or ""),
            "submit_digest": str(order.get("submit_digest") or ""),
            "submit_digest_version": str(order.get("submit_digest_version") or ""),
            "probe_confidence": str(order.get("probe_confidence") or ""),
            "probe_basis": str(order.get("probe_basis") or ""),
            "unknown_submit_first_seen_ts": int(order.get("unknown_submit_first_seen_ts") or 0),
            "unknown_submit_probe_count": int(order.get("unknown_submit_probe_count") or 0),
            "manual_required_reason": str(order.get("manual_required_reason") or ""),
            "ack_unknown_count": int(order.get("ack_unknown_count") or 0),
            "ack_unknown_first_ts": int(order.get("ack_unknown_first_ts") or 0),
            "submitted_price": float(order.get("submitted_price") or 0.0),
            "submitted_size": float(order.get("submitted_size") or 0.0),
        }

    @staticmethod
    def _pending_reconcile_group_key(order: Mapping[str, object]) -> tuple[str, str]:
        return (
            str(order.get("token_id") or ""),
            str(order.get("side") or "").upper(),
        )

    def _mark_pending_order_reconcile_ambiguous(
        self,
        *,
        order: dict[str, object],
        now: int,
        reason: str,
        fill_ts: int = 0,
    ) -> None:
        order["reconcile_ambiguous_ts"] = int(fill_ts or now)
        order["reconcile_ambiguous_reason"] = str(reason or "ambiguous_pending_reconcile")
        order["message"] = str(reason or order.get("message") or "ambiguous pending reconcile")
        order["reason"] = str(reason or order.get("reason") or "ambiguous pending reconcile")
        self._append_event(
            "pending_reconcile_ambiguous",
            {
                "order_id": str(order.get("order_id") or ""),
                "token_id": str(order.get("token_id") or ""),
                "side": str(order.get("side") or "").upper(),
                "reason": str(order.get("reconcile_ambiguous_reason") or ""),
                "fill_ts": int(fill_ts or 0),
            },
        )

    def _sync_shadow_position_to_actual(
        self,
        *,
        token_id: str,
        shadow_positions: dict[str, dict[str, object]],
        next_positions: dict[str, dict[str, object]],
        position_delta_backed: bool,
    ) -> None:
        shadow = shadow_positions.get(token_id)
        if not position_delta_backed:
            if shadow is None:
                next_positions.pop(token_id, None)
                return
            next_positions[token_id] = dict(shadow)
            return

        if shadow is None:
            return

        current = next_positions.get(token_id)
        if current is None:
            next_positions[token_id] = dict(shadow)
            return

        merged = self._merge_position_context(dict(current), shadow)
        merged["cost_basis_notional"] = self._position_cost_basis_notional(shadow)
        next_positions[token_id] = merged

    @staticmethod
    def _market_context_fields() -> tuple[str, ...]:
        return (
            "best_bid",
            "best_ask",
            "midpoint",
            "tick_size",
            "min_order_size",
            "last_trade_price",
            "market_spread_bps",
            "requested_vs_mid_bps",
            "preflight_has_book",
            "neg_risk",
        )

    def _market_context_from_result(self, result: ExecutionResult) -> dict[str, object]:
        metadata = dict(result.metadata or {})
        snapshot: dict[str, object] = {}
        for field_name in self._market_context_fields():
            if field_name not in metadata:
                continue
            value = metadata.get(field_name)
            if isinstance(value, bool):
                snapshot[field_name] = bool(value)
            elif value in (None, ""):
                continue
            else:
                snapshot[field_name] = self._safe_float(value, default=0.0)
        return snapshot

    def _market_context_from_order(self, order: Mapping[str, object]) -> dict[str, object]:
        snapshot: dict[str, object] = {}
        for field_name in self._market_context_fields():
            if field_name not in order:
                continue
            value = order.get(field_name)
            if isinstance(value, bool):
                snapshot[field_name] = bool(value)
            elif value in (None, ""):
                continue
            else:
                snapshot[field_name] = self._safe_float(value, default=0.0)
        return snapshot

    @staticmethod
    def _aggregate_fill_snapshot(
        bucket: dict[str, float | str],
        fill: OrderFillSnapshot,
    ) -> dict[str, float | str]:
        prev_notional = float(bucket.get("notional") or 0.0)
        next_notional = prev_notional + float(fill.notional)
        weighted_price = float(bucket.get("price") or 0.0)
        if next_notional > 0.0 and fill.notional > 0.0 and fill.price > 0.0:
            weighted_price = (
                ((weighted_price * prev_notional) + (fill.price * fill.notional)) / next_notional
                if prev_notional > 0.0
                else fill.price
            )
        bucket["notional"] = next_notional
        bucket["size"] = float(bucket.get("size") or 0.0) + float(fill.size or 0.0)
        bucket["price"] = weighted_price
        bucket["timestamp"] = max(int(bucket.get("timestamp") or 0), int(fill.timestamp or 0))
        bucket["tx_hash"] = str(fill.tx_hash or bucket.get("tx_hash") or "")
        bucket["market_slug"] = str(fill.market_slug or bucket.get("market_slug") or "")
        bucket["outcome"] = str(fill.outcome or bucket.get("outcome") or "")
        return bucket

    @staticmethod
    def _aggregate_broker_event_fill(
        bucket: dict[str, float | str],
        event: BrokerOrderEvent,
    ) -> dict[str, float | str]:
        prev_notional = float(bucket.get("notional") or 0.0)
        next_notional = prev_notional + float(event.matched_notional or 0.0)
        weighted_price = float(bucket.get("price") or 0.0)
        if next_notional > 0.0 and float(event.matched_notional or 0.0) > 0.0 and float(event.avg_fill_price or 0.0) > 0.0:
            weighted_price = (
                ((weighted_price * prev_notional) + (float(event.avg_fill_price or 0.0) * float(event.matched_notional or 0.0))) / next_notional
                if prev_notional > 0.0
                else float(event.avg_fill_price or 0.0)
            )
        bucket["notional"] = next_notional
        bucket["size"] = float(bucket.get("size") or 0.0) + float(event.matched_size or 0.0)
        bucket["price"] = weighted_price
        bucket["timestamp"] = max(int(bucket.get("timestamp") or 0), int(event.timestamp or 0))
        bucket["tx_hash"] = str(event.tx_hash or bucket.get("tx_hash") or "")
        bucket["market_slug"] = str(event.market_slug or bucket.get("market_slug") or "")
        bucket["outcome"] = str(event.outcome or bucket.get("outcome") or "")
        return bucket

    @staticmethod
    def _status_snapshot_from_event(event: BrokerOrderEvent) -> OrderStatusSnapshot | None:
        if not event.is_status or not str(event.order_id or "").strip():
            return None
        return OrderStatusSnapshot(
            order_id=str(event.order_id or "").strip(),
            status=str(event.status or "").strip(),
            matched_notional=float(event.matched_notional or 0.0),
            matched_size=float(event.matched_size or 0.0),
            avg_fill_price=float(event.avg_fill_price or 0.0),
            original_size=0.0,
            remaining_size=0.0,
            message=str(event.message or "").strip(),
        )

    def _poll_pending_order_events(
        self,
        *,
        now: int,
    ) -> tuple[dict[str, OrderStatusSnapshot], dict[str, dict[str, float | str]], bool]:
        if not self.pending_orders:
            return ({}, {}, False)

        order_ids = [
            str(order.get("order_id") or "").strip()
            for order in self.pending_orders.values()
            if str(order.get("order_id") or "").strip()
        ]
        if not order_ids:
            return ({}, {}, False)

        oldest_pending_ts = min(int(order.get("ts") or now) for order in self.pending_orders.values())
        since_ts = max(0, self._last_broker_event_sync_ts - 5) if self._last_broker_event_sync_ts > 0 else max(0, oldest_pending_ts - 3600)
        list_order_events = getattr(self.broker, "list_order_events", None)
        if not callable(list_order_events):
            return ({}, {}, False)
        events = list_order_events(
            since_ts=since_ts,
            order_ids=order_ids,
            limit=max(200, len(order_ids) * 20),
        )
        if events is None:
            return ({}, {}, False)

        status_by_order_id: dict[str, OrderStatusSnapshot] = {}
        status_ts_by_order_id: dict[str, int] = {}
        fill_aggs: dict[str, dict[str, float | str]] = {}
        max_ts = self._last_broker_event_sync_ts
        for event in sorted(events, key=lambda item: (int(item.timestamp or 0), item.order_id, item.normalized_event_type)):
            if event.is_fill:
                max_ts = max(max_ts, int(event.timestamp or 0))
                order_id = str(event.order_id or "").strip()
                if not order_id:
                    continue
                bucket = fill_aggs.setdefault(
                    order_id,
                    {
                        "notional": 0.0,
                        "size": 0.0,
                        "price": 0.0,
                        "timestamp": 0,
                        "tx_hash": "",
                        "market_slug": "",
                        "outcome": "",
                    },
                )
                self._aggregate_broker_event_fill(bucket, event)
                continue
            snapshot = self._status_snapshot_from_event(event)
            if snapshot is None:
                continue
            event_ts = int(event.timestamp or 0)
            current_ts = int(status_ts_by_order_id.get(snapshot.order_id) or 0)
            if snapshot.order_id not in status_by_order_id or event_ts >= current_ts:
                status_by_order_id[snapshot.order_id] = snapshot
                status_ts_by_order_id[snapshot.order_id] = event_ts
        if max_ts > 0:
            self._last_broker_event_sync_ts = max_ts
        return (status_by_order_id, fill_aggs, True)

    def _register_pending_order(
        self,
        *,
        signal: Signal,
        cycle_id: str,
        result: ExecutionResult,
        order_meta: dict[str, object],
        entry_context: dict[str, object],
        previous_position: dict[str, object] | None,
        order_reason: str,
        now: int,
    ) -> dict[str, object]:
        key = self._pending_order_key(signal, result.broker_order_id)
        previous = previous_position or {}
        metadata = dict(getattr(result, "metadata", {}) or {})
        record = {
            "key": key,
            "ts": now,
            "cycle_id": cycle_id,
            "order_id": str(result.broker_order_id or ""),
            "broker_status": result.lifecycle_status,
            "signal_id": str(signal.signal_id or ""),
            "trace_id": str(signal.trace_id or ""),
            "token_id": str(signal.token_id or ""),
            "condition_id": str(signal.condition_id or ""),
            "market_slug": str(signal.market_slug or ""),
            "outcome": str(signal.outcome or ""),
            "side": str(signal.side or ""),
            "wallet": str(signal.wallet or ""),
            "wallet_score": float(signal.wallet_score or 0.0),
            "wallet_tier": str(signal.wallet_tier or ""),
            "topic_label": str(signal.topic_label or ""),
            "topic_bias": str(signal.topic_bias or "neutral"),
            "topic_multiplier": float(signal.topic_multiplier or 1.0),
            "position_action": str(signal.position_action or ""),
            "position_action_label": str(signal.position_action_label or ""),
            "requested_notional": float(result.requested_notional or 0.0),
            "requested_price": float(result.requested_price or 0.0),
            "matched_notional_hint": 0.0,
            "matched_size_hint": 0.0,
            "matched_price_hint": 0.0,
            "reconciled_notional_hint": 0.0,
            "reconciled_size_hint": 0.0,
            "last_fill_ts_hint": 0,
            "last_fill_tx_hash": "",
            "message": str(result.message or order_reason),
            "reason": str(order_reason or result.message or ""),
            "strategy_order_uuid": str(order_meta.get("strategy_order_uuid") or ""),
            "idempotency_key": str(order_meta.get("idempotency_key") or ""),
            "strategy_name": str(order_meta.get("strategy_name") or ""),
            "signal_source": str(order_meta.get("signal_source") or ""),
            "signal_fingerprint": str(order_meta.get("signal_fingerprint") or ""),
            "previous_notional": float(previous.get("notional") or 0.0),
            "previous_quantity": float(previous.get("quantity") or 0.0),
            "entry_wallet": str(entry_context.get("entry_wallet") or ""),
            "entry_wallet_score": float(entry_context.get("entry_wallet_score") or 0.0),
            "entry_wallet_tier": str(entry_context.get("entry_wallet_tier") or ""),
            "entry_topic_label": str(entry_context.get("entry_topic_label") or ""),
            "entry_topic_bias": str(entry_context.get("entry_topic_bias") or "neutral"),
            "entry_topic_multiplier": float(entry_context.get("entry_topic_multiplier") or 1.0),
            "entry_topic_summary": str(entry_context.get("entry_topic_summary") or ""),
            "entry_reason": str(entry_context.get("entry_reason") or ""),
            "origin_signal_id": str(entry_context.get("origin_signal_id") or signal.signal_id or ""),
            "last_signal_id": str(entry_context.get("last_signal_id") or signal.signal_id or ""),
            "flow": str(order_meta.get("flow") or ("exit" if signal.side == "SELL" else "entry")),
            "exit_kind": str(order_meta.get("exit_kind") or ""),
            "exit_label": str(order_meta.get("exit_label") or ""),
            "exit_summary": str(order_meta.get("exit_summary") or ""),
            "last_heartbeat_ts": 0,
            "cancel_requested_ts": 0,
            "cancel_request_count": 0,
            "cancel_last_status": "",
            "cancel_last_message": "",
            "reconcile_ambiguous_ts": 0,
            "reconcile_ambiguous_reason": "",
            "recovery_source": "runtime",
            "recovery_status": "confirmed",
            "pending_class": str(metadata.get("pending_class") or "normal"),
            "submit_digest": str(metadata.get("submit_digest") or ""),
            "submit_digest_version": str(metadata.get("submit_digest_version") or ""),
            "probe_confidence": self._normalize_probe_confidence(metadata.get("probe_confidence")),
            "probe_basis": self._normalize_probe_basis(metadata.get("probe_basis")),
            "unknown_submit_first_seen_ts": int(self._safe_float(metadata.get("unknown_submit_first_seen_ts"), 0)),
            "unknown_submit_probe_count": int(self._safe_float(metadata.get("unknown_submit_probe_count"), 0)),
            "manual_required_reason": str(metadata.get("manual_required_reason") or ""),
            "ack_unknown_count": int(self._safe_float(metadata.get("unknown_submit_probe_count"), 0)),
            "ack_unknown_first_ts": int(self._safe_float(metadata.get("unknown_submit_first_seen_ts"), 0)),
            "submitted_price": self._safe_float(metadata.get("submitted_price"), 0.0),
            "submitted_size": self._safe_float(metadata.get("submitted_size"), 0.0),
        }
        record.update(self._market_context_from_result(result))
        self.pending_orders[key] = record
        self._refresh_risk_state()
        return record

    def _pending_order_from_open_order(self, snapshot: OpenOrderSnapshot) -> dict[str, object]:
        signal_id = f"restore:{snapshot.order_id or snapshot.token_id}"
        return {
            "key": f"{signal_id}:{snapshot.side}:{snapshot.token_id}",
            "ts": int(snapshot.created_ts or time.time()),
            "cycle_id": "",
            "order_id": str(snapshot.order_id or ""),
            "broker_status": str(snapshot.lifecycle_status or snapshot.normalized_status or "live"),
            "signal_id": signal_id,
            "trace_id": "",
            "token_id": str(snapshot.token_id or ""),
            "condition_id": str(snapshot.condition_id or ""),
            "market_slug": str(snapshot.market_slug or snapshot.token_id),
            "outcome": str(snapshot.outcome or "YES"),
            "side": str(snapshot.side or ""),
            "wallet": "",
            "wallet_score": 0.0,
            "wallet_tier": "",
            "topic_label": "",
            "topic_bias": "neutral",
            "topic_multiplier": 1.0,
            "position_action": "",
            "position_action_label": "",
            "requested_notional": float(snapshot.requested_notional),
            "requested_price": float(snapshot.price or 0.0),
            "matched_notional_hint": float(snapshot.matched_notional),
            "matched_size_hint": float(snapshot.matched_size or 0.0),
            "matched_price_hint": float(snapshot.price or 0.0),
            "reconciled_notional_hint": 0.0,
            "reconciled_size_hint": 0.0,
            "last_fill_ts_hint": 0,
            "last_fill_tx_hash": "",
            "message": str(snapshot.message or "restored pending order from broker"),
            "reason": str(snapshot.message or "restored pending order from broker"),
            "previous_notional": 0.0,
            "previous_quantity": 0.0,
            "entry_wallet": "",
            "entry_wallet_score": 0.0,
            "entry_wallet_tier": "",
            "entry_topic_label": "",
            "entry_topic_bias": "neutral",
            "entry_topic_multiplier": 1.0,
            "entry_topic_summary": "",
            "entry_reason": "",
            "origin_signal_id": signal_id,
            "last_signal_id": signal_id,
            "flow": "exit" if snapshot.side == "SELL" else "entry",
            "exit_kind": "",
            "exit_label": "",
            "exit_summary": "",
            "last_heartbeat_ts": 0,
            "cancel_requested_ts": 0,
            "cancel_request_count": 0,
            "cancel_last_status": "",
            "cancel_last_message": "",
            "reconcile_ambiguous_ts": 0,
            "reconcile_ambiguous_reason": "",
            "recovery_source": "broker_open_orders",
            "recovery_status": "confirmed",
            "pending_class": "normal",
            "submit_digest": "",
            "submit_digest_version": "",
            "probe_confidence": "none",
            "probe_basis": "no_match",
            "unknown_submit_first_seen_ts": 0,
            "unknown_submit_probe_count": 0,
            "manual_required_reason": "",
            "ack_unknown_count": 0,
            "ack_unknown_first_ts": 0,
            "submitted_price": float(snapshot.price or 0.0),
            "submitted_size": float(snapshot.original_size or 0.0),
        }

    def _restore_pending_orders_from_broker(self, rows: list[dict[str, object]]) -> list[dict[str, object]]:
        broker_open_orders = self.broker.list_open_orders()
        if broker_open_orders is None:
            return rows

        recovered_by_order_id: dict[str, dict[str, object]] = {}
        for row in rows:
            restored = self._restore_pending_order(row)
            if not restored:
                continue
            order_id = str(restored.get("order_id") or "").strip()
            if order_id:
                recovered_by_order_id[order_id] = restored

        if not broker_open_orders:
            if recovered_by_order_id:
                restored_rows = []
                for restored in recovered_by_order_id.values():
                    row = dict(restored)
                    row["recovery_source"] = str(row.get("recovery_source") or "snapshot")
                    row["recovery_status"] = "uncertain_empty_broker_open_orders"
                    restored_rows.append(row)
                self.log.warning(
                    "Recovered pending orders from snapshot after empty broker open-order response count=%d",
                    len(restored_rows),
                )
                self._append_event(
                    "pending_restore",
                    {
                        "source": "snapshot_uncertain_empty_broker_open_orders",
                        "count": len(restored_rows),
                    },
                )
                return restored_rows
            return []

        restored_rows: list[dict[str, object]] = []
        for snapshot in broker_open_orders:
            order_id = str(snapshot.order_id or "").strip()
            restored = dict(recovered_by_order_id.get(order_id) or self._pending_order_from_open_order(snapshot))
            if snapshot.created_ts > 0:
                restored["ts"] = int(snapshot.created_ts)
            if snapshot.token_id:
                restored["token_id"] = snapshot.token_id
            if snapshot.condition_id:
                restored["condition_id"] = snapshot.condition_id
            if snapshot.market_slug:
                restored["market_slug"] = snapshot.market_slug
            if snapshot.outcome:
                restored["outcome"] = snapshot.outcome
            if snapshot.side in {"BUY", "SELL"}:
                restored["side"] = snapshot.side
            if snapshot.price > 0.0:
                restored["requested_price"] = float(snapshot.price)
                if float(restored.get("matched_price_hint") or 0.0) <= 0.0:
                    restored["matched_price_hint"] = float(snapshot.price)
            if snapshot.requested_notional > 0.0:
                restored["requested_notional"] = float(snapshot.requested_notional)
            restored["matched_notional_hint"] = max(
                float(restored.get("matched_notional_hint") or 0.0),
                float(snapshot.matched_notional),
            )
            restored["matched_size_hint"] = max(
                float(restored.get("matched_size_hint") or 0.0),
                float(snapshot.matched_size),
            )
            restored["broker_status"] = str(snapshot.lifecycle_status or snapshot.normalized_status or "live")
            if str(snapshot.message or "").strip():
                restored["message"] = str(snapshot.message)
                restored["reason"] = str(snapshot.message)
            restored["recovery_source"] = "broker_open_orders"
            restored["recovery_status"] = "confirmed"
            restored_rows.append(restored)

        if broker_open_orders:
            self.log.info("Recovered pending open orders from broker count=%d", len(restored_rows))
            self._append_event(
                "pending_restore",
                {
                    "source": "broker_open_orders",
                    "count": len(restored_rows),
                },
            )
        return restored_rows

    def _restore_pending_order(self, row: dict[str, object]) -> dict[str, object] | None:
        token_id = str(row.get("token_id") or "").strip()
        signal_id = str(row.get("signal_id") or "").strip()
        side = str(row.get("side") or "").strip().upper()
        if not token_id or not signal_id or side not in {"BUY", "SELL"}:
            return None
        key = str(row.get("key") or row.get("order_id") or "").strip()
        if not key:
            key = f"{signal_id}:{side}:{token_id}"
        restored = {
            "key": key,
            "ts": int(self._safe_float(row.get("ts"))),
            "cycle_id": str(row.get("cycle_id") or ""),
            "order_id": str(row.get("order_id") or ""),
            "broker_status": str(row.get("broker_status") or "posted"),
            "signal_id": signal_id,
            "trace_id": str(row.get("trace_id") or ""),
            "token_id": token_id,
            "condition_id": str(row.get("condition_id") or ""),
            "market_slug": str(row.get("market_slug") or token_id),
            "outcome": str(row.get("outcome") or "YES"),
            "side": side,
            "wallet": str(row.get("wallet") or ""),
            "wallet_score": self._safe_float(row.get("wallet_score")),
            "wallet_tier": str(row.get("wallet_tier") or ""),
            "topic_label": str(row.get("topic_label") or ""),
            "topic_bias": str(row.get("topic_bias") or "neutral"),
            "topic_multiplier": self._safe_float(row.get("topic_multiplier"), 1.0),
            "position_action": str(row.get("position_action") or ""),
            "position_action_label": str(row.get("position_action_label") or ""),
            "requested_notional": self._safe_float(row.get("requested_notional")),
            "requested_price": self._safe_float(row.get("requested_price")),
            "matched_notional_hint": self._safe_float(row.get("matched_notional_hint")),
            "matched_size_hint": self._safe_float(row.get("matched_size_hint")),
            "matched_price_hint": self._safe_float(row.get("matched_price_hint")),
            "reconciled_notional_hint": self._safe_float(row.get("reconciled_notional_hint")),
            "reconciled_size_hint": self._safe_float(row.get("reconciled_size_hint")),
            "last_fill_ts_hint": int(self._safe_float(row.get("last_fill_ts_hint"))),
            "last_fill_tx_hash": str(row.get("last_fill_tx_hash") or ""),
            "message": str(row.get("message") or ""),
            "reason": str(row.get("reason") or ""),
            "previous_notional": self._safe_float(row.get("previous_notional")),
            "previous_quantity": self._safe_float(row.get("previous_quantity")),
            "entry_wallet": str(row.get("entry_wallet") or ""),
            "entry_wallet_score": self._safe_float(row.get("entry_wallet_score")),
            "entry_wallet_tier": str(row.get("entry_wallet_tier") or ""),
            "entry_topic_label": str(row.get("entry_topic_label") or ""),
            "entry_topic_bias": str(row.get("entry_topic_bias") or "neutral"),
            "entry_topic_multiplier": self._safe_float(row.get("entry_topic_multiplier"), 1.0),
            "entry_topic_summary": str(row.get("entry_topic_summary") or ""),
            "entry_reason": str(row.get("entry_reason") or ""),
            "origin_signal_id": str(row.get("origin_signal_id") or signal_id),
            "last_signal_id": str(row.get("last_signal_id") or signal_id),
            "flow": str(row.get("flow") or ("exit" if side == "SELL" else "entry")),
            "exit_kind": str(row.get("exit_kind") or ""),
            "exit_label": str(row.get("exit_label") or ""),
            "exit_summary": str(row.get("exit_summary") or ""),
            "last_heartbeat_ts": int(self._safe_float(row.get("last_heartbeat_ts"))),
            "cancel_requested_ts": int(self._safe_float(row.get("cancel_requested_ts"))),
            "cancel_request_count": int(self._safe_float(row.get("cancel_request_count"))),
            "cancel_last_status": str(row.get("cancel_last_status") or ""),
            "cancel_last_message": str(row.get("cancel_last_message") or ""),
            "reconcile_ambiguous_ts": int(self._safe_float(row.get("reconcile_ambiguous_ts"))),
            "reconcile_ambiguous_reason": str(row.get("reconcile_ambiguous_reason") or ""),
            "recovery_source": str(row.get("recovery_source") or "snapshot"),
            "recovery_status": str(row.get("recovery_status") or "restored"),
            "pending_class": str(row.get("pending_class") or "normal"),
            "submit_digest": str(row.get("submit_digest") or ""),
            "submit_digest_version": str(row.get("submit_digest_version") or ""),
            "probe_confidence": self._normalize_probe_confidence(row.get("probe_confidence")),
            "probe_basis": self._normalize_probe_basis(row.get("probe_basis")),
            "unknown_submit_first_seen_ts": int(self._safe_float(row.get("unknown_submit_first_seen_ts"))),
            "unknown_submit_probe_count": int(self._safe_float(row.get("unknown_submit_probe_count"))),
            "manual_required_reason": str(row.get("manual_required_reason") or ""),
            "ack_unknown_count": int(self._safe_float(row.get("ack_unknown_count"))),
            "ack_unknown_first_ts": int(self._safe_float(row.get("ack_unknown_first_ts"))),
            "submitted_price": self._safe_float(row.get("submitted_price")),
            "submitted_size": self._safe_float(row.get("submitted_size")),
        }
        for field_name in self._market_context_fields():
            value = row.get(field_name)
            if isinstance(value, bool):
                restored[field_name] = bool(value)
            elif value not in (None, ""):
                restored[field_name] = self._safe_float(value)
        return restored

    def _merge_position_context(
        self,
        target: dict[str, object],
        source: dict[str, object],
    ) -> dict[str, object]:
        merged = dict(target)
        if int(source.get("opened_ts") or 0) > 0:
            merged["opened_ts"] = int(source.get("opened_ts") or 0)
        if int(source.get("last_buy_ts") or 0) > 0:
            merged["last_buy_ts"] = int(source.get("last_buy_ts") or 0)
        if int(source.get("last_trim_ts") or 0) > 0:
            merged["last_trim_ts"] = int(source.get("last_trim_ts") or 0)
        for key in (
            "condition_id",
            "cost_basis_notional",
            "entry_wallet",
            "entry_wallet_score",
            "entry_wallet_tier",
            "entry_topic_label",
            "entry_topic_bias",
            "entry_topic_multiplier",
            "entry_topic_summary",
            "entry_reason",
            "trace_id",
            "origin_signal_id",
            "last_signal_id",
            "last_exit_kind",
            "last_exit_label",
            "last_exit_summary",
            "last_exit_ts",
            "time_exit_state",
        ):
            value = source.get(key)
            if value not in (None, "", 0, 0.0):
                merged[key] = value
        return merged

    def _merge_recovered_position(
        self,
        target: dict[str, object],
        source: dict[str, object] | None,
        *,
        recovery_source: str,
    ) -> dict[str, object]:
        if not source:
            return dict(target)

        merged = self._merge_position_context(target, source)
        source_qty = max(0.0, float(source.get("quantity") or 0.0))
        target_qty = max(0.0, float(target.get("quantity") or 0.0))
        source_cost_basis = self._position_cost_basis_notional(source)
        token_id = str(target.get("token_id") or source.get("token_id") or "")

        if source_cost_basis > 0.0 and target_qty > 0.0:
            if source_qty > 0.0 and abs(target_qty - source_qty) > 1e-6:
                scaled_cost_basis = max(0.0, source_cost_basis * (target_qty / source_qty))
                merged["cost_basis_notional"] = scaled_cost_basis
                self.log.warning(
                    "Recovered position metadata with quantity mismatch token=%s recovery_source=%s source_qty=%.6f target_qty=%.6f source_cost_basis=%.6f scaled_cost_basis=%.6f",
                    token_id,
                    recovery_source,
                    source_qty,
                    target_qty,
                    source_cost_basis,
                    scaled_cost_basis,
                )
                self._append_event(
                    "position_recovery_conflict",
                    {
                        "token_id": token_id,
                        "recovery_source": recovery_source,
                        "source_qty": source_qty,
                        "target_qty": target_qty,
                        "source_cost_basis_notional": source_cost_basis,
                        "scaled_cost_basis_notional": scaled_cost_basis,
                    },
                )
            else:
                merged["cost_basis_notional"] = source_cost_basis
        return merged

    def _seed_position_from_pending_order(
        self,
        position: dict[str, object],
        order: dict[str, object],
    ) -> dict[str, object]:
        seeded = dict(position)
        seeded["condition_id"] = str(position.get("condition_id") or order.get("condition_id") or "")
        seeded["opened_ts"] = int(order.get("ts") or position.get("opened_ts") or 0)
        seeded["last_buy_ts"] = int(order.get("ts") or position.get("last_buy_ts") or position.get("opened_ts") or 0)
        seeded["last_signal_id"] = str(order.get("last_signal_id") or order.get("signal_id") or "")
        seeded["cost_basis_notional"] = max(
            0.0,
            float(order.get("matched_notional_hint") or 0.0)
            or float(order.get("requested_notional") or 0.0)
            or float(position.get("cost_basis_notional") or position.get("notional") or 0.0),
        )
        return self._merge_position_context(seeded, self._pending_order_entry_context(order))

    def _apply_pending_fill_delta(
        self,
        *,
        order: dict[str, object],
        previous_positions: dict[str, dict[str, object]],
        next_positions: dict[str, dict[str, object]],
        filled_notional: float,
        filled_qty: float,
        fill_price: float,
        fill_ts: int,
        now: int,
    ) -> tuple[float, float, float]:
        token_id = str(order.get("token_id") or "")
        side = str(order.get("side") or "").upper()
        market_slug = str(order.get("market_slug") or token_id)
        outcome = str(order.get("outcome") or "YES")
        condition_id = str(order.get("condition_id") or "")
        applied_ts = int(fill_ts or now or time.time())

        if filled_notional <= 1e-9:
            current = next_positions.get(token_id) or previous_positions.get(token_id) or {}
            return (0.0, float(current.get("notional") or 0.0), float(current.get("quantity") or 0.0))

        observed_price = max(
            0.0,
            float(fill_price or 0.0) or float(order.get("matched_price_hint") or 0.0) or float(order.get("requested_price") or 0.0),
        )
        if filled_qty <= 0.0 and observed_price > 0.0:
            filled_qty = filled_notional / max(0.01, observed_price)

        if side == "BUY":
            current = dict(next_positions.get(token_id) or previous_positions.get(token_id) or {})
            prev_qty = float(current.get("quantity") or 0.0)
            prev_notional = float(current.get("notional") or 0.0)
            new_qty = prev_qty + max(0.0, filled_qty)
            new_notional = prev_notional + filled_notional
            current.update(
                {
                    "token_id": token_id,
                    "condition_id": str(current.get("condition_id") or condition_id),
                    "market_slug": market_slug,
                    "outcome": outcome,
                    "quantity": new_qty,
                    "price": new_notional / max(0.01, new_qty),
                    "notional": new_notional,
                    "cost_basis_notional": self._position_cost_basis_notional(current) + filled_notional,
                    "opened_ts": int(current.get("opened_ts") or applied_ts or order.get("ts") or now),
                    "last_buy_ts": applied_ts,
                    "last_signal_id": str(order.get("last_signal_id") or order.get("signal_id") or ""),
                }
            )
            next_positions[token_id] = self._merge_position_context(current, self._pending_order_entry_context(order))
            return (0.0, new_notional, new_qty)

        source_position = previous_positions.get(token_id) or next_positions.get(token_id) or {}
        current = dict(next_positions.get(token_id) or source_position)
        prev_qty = float(current.get("quantity") or 0.0)
        prev_notional = float(current.get("notional") or 0.0)
        realized_pnl, remaining_cost_basis = self._realize_position_sell(
            source_position or current,
            sold_qty=filled_qty,
            sold_notional=filled_notional,
        )
        remaining_qty = max(0.0, prev_qty - max(0.0, filled_qty))
        remaining_notional = max(0.0, prev_notional - filled_notional)
        self._apply_reconciled_realized_pnl(realized_pnl, fill_ts=applied_ts, now=now)

        if remaining_notional <= self.settings.stale_position_close_notional_usd or remaining_qty <= 0.0:
            next_positions.pop(token_id, None)
            return (realized_pnl, remaining_notional, remaining_qty)

        current.update(
            {
                "token_id": token_id,
                "condition_id": str(current.get("condition_id") or condition_id),
                "market_slug": market_slug,
                "outcome": outcome,
                "quantity": remaining_qty,
                "price": observed_price or float(current.get("price") or 0.0),
                "notional": remaining_notional,
                "cost_basis_notional": remaining_cost_basis,
                "last_trim_ts": applied_ts,
                "last_signal_id": str(order.get("last_signal_id") or order.get("signal_id") or ""),
            }
        )
        self._apply_position_exit_meta(
            current,
            exit_kind=str(order.get("exit_kind") or ""),
            exit_label=str(order.get("exit_label") or ""),
            exit_summary=str(order.get("exit_summary") or ""),
            ts=applied_ts,
        )
        next_positions[token_id] = current
        return (realized_pnl, remaining_notional, remaining_qty)

    def _reconcile_pending_orders(
        self,
        *,
        previous_positions: dict[str, dict[str, object]],
        next_positions: dict[str, dict[str, object]],
        now: int,
    ) -> None:
        if not self.pending_orders:
            self._refresh_risk_state()
            return

        stream_statuses, stream_fill_aggs, stream_available = self._poll_pending_order_events(now=now)
        heartbeat_ids = [
            str(order.get("order_id") or "").strip()
            for order in self.pending_orders.values()
            if str(order.get("order_id") or "").strip()
        ]
        heartbeat_ok = bool(self.broker.heartbeat(heartbeat_ids))
        if heartbeat_ok:
            for order in self.pending_orders.values():
                if str(order.get("order_id") or "").strip():
                    order["last_heartbeat_ts"] = now

        recent_fill_aggs: dict[str, dict[str, float | str]] = dict(stream_fill_aggs)
        if heartbeat_ids and not stream_available:
            oldest_pending_ts = min(int(order.get("ts") or now) for order in self.pending_orders.values())
            recent_fills = self.broker.list_recent_fills(
                since_ts=max(0, oldest_pending_ts - 3600),
                order_ids=heartbeat_ids,
                limit=max(200, len(heartbeat_ids) * 20),
            )
            if recent_fills:
                for fill in recent_fills:
                    if not str(fill.order_id or "").strip():
                        continue
                    bucket = recent_fill_aggs.setdefault(
                        fill.order_id,
                        {
                            "notional": 0.0,
                            "size": 0.0,
                            "price": 0.0,
                            "timestamp": 0,
                            "tx_hash": "",
                            "market_slug": "",
                            "outcome": "",
                        },
                    )
                    self._aggregate_fill_snapshot(bucket, fill)

        buy_notional_delta: dict[str, float] = {}
        sell_notional_delta: dict[str, float] = {}
        buy_qty_delta: dict[str, float] = {}
        sell_qty_delta: dict[str, float] = {}
        for token_id in set(previous_positions) | set(next_positions):
            prev_pos = previous_positions.get(token_id) or {}
            next_pos = next_positions.get(token_id) or {}
            prev_notional = float(prev_pos.get("notional") or 0.0)
            next_notional = float(next_pos.get("notional") or 0.0)
            prev_qty = float(prev_pos.get("quantity") or 0.0)
            next_qty = float(next_pos.get("quantity") or 0.0)
            buy_notional_delta[token_id] = max(0.0, next_notional - prev_notional)
            sell_notional_delta[token_id] = max(0.0, prev_notional - next_notional)
            buy_qty_delta[token_id] = max(0.0, next_qty - prev_qty)
            sell_qty_delta[token_id] = max(0.0, prev_qty - next_qty)

        timeout_seconds = self._pending_timeout_seconds()
        active_statuses = {"submitted", "posted", "open", "live", "delayed", "partially_filled"}
        shadow_positions = {token_id: dict(position) for token_id, position in previous_positions.items()}
        pending_group_counts: dict[tuple[str, str], int] = {}
        for order in self.pending_orders.values():
            group_key = self._pending_reconcile_group_key(order)
            if group_key[0] and group_key[1] in {"BUY", "SELL"}:
                pending_group_counts[group_key] = pending_group_counts.get(group_key, 0) + 1
        remaining: dict[str, dict[str, object]] = {}
        for key, order in sorted(self.pending_orders.items(), key=lambda item: int(item[1].get("ts") or 0)):
            token_id = str(order.get("token_id") or "")
            side = str(order.get("side") or "").upper()
            order_id = str(order.get("order_id") or "").strip()
            previous_reported_notional = float(order.get("matched_notional_hint") or 0.0)
            previous_reported_size = float(order.get("matched_size_hint") or 0.0)
            previous_reconciled_notional = float(order.get("reconciled_notional_hint") or 0.0)
            previous_reconciled_size = float(order.get("reconciled_size_hint") or 0.0)
            status_snapshot = stream_statuses.get(order_id) if order_id else None
            if status_snapshot is None and order_id and not stream_available:
                status_snapshot = self.broker.get_order_status(order_id)
            if status_snapshot is not None:
                order["broker_status"] = status_snapshot.lifecycle_status
                order["matched_notional_hint"] = max(
                    previous_reported_notional,
                    float(status_snapshot.matched_notional or 0.0),
                )
                order["matched_size_hint"] = max(
                    previous_reported_size,
                    float(status_snapshot.matched_size or 0.0),
                )
                if status_snapshot.avg_fill_price > 0.0:
                    order["matched_price_hint"] = float(status_snapshot.avg_fill_price)
                if str(status_snapshot.message or "").strip():
                    order["message"] = str(status_snapshot.message)
                    order["reason"] = str(status_snapshot.message)

            fill_agg = recent_fill_aggs.get(order_id)
            if fill_agg is not None:
                order["matched_notional_hint"] = max(
                    float(order.get("matched_notional_hint") or 0.0),
                    float(fill_agg.get("notional") or 0.0),
                )
                order["matched_size_hint"] = max(
                    float(order.get("matched_size_hint") or 0.0),
                    float(fill_agg.get("size") or 0.0),
                )
                if float(fill_agg.get("price") or 0.0) > 0.0:
                    order["matched_price_hint"] = float(fill_agg.get("price") or 0.0)
                order["last_fill_ts_hint"] = max(
                    int(order.get("last_fill_ts_hint") or 0),
                    int(fill_agg.get("timestamp") or 0),
                )
                if str(fill_agg.get("tx_hash") or "").strip():
                    order["last_fill_tx_hash"] = str(fill_agg.get("tx_hash") or "")
                if (
                    str(order.get("broker_status") or "") in active_statuses
                    and float(order.get("matched_notional_hint") or 0.0) > 1e-6
                    and float(order.get("reconciled_notional_hint") or 0.0) + 1e-6 < float(order.get("requested_notional") or 0.0)
                ):
                    order["broker_status"] = "partially_filled"

            delta_notional_map = buy_notional_delta if side == "BUY" else sell_notional_delta
            delta_qty_map = buy_qty_delta if side == "BUY" else sell_qty_delta
            available_notional = float(delta_notional_map.get(token_id) or 0.0)
            available_qty = float(delta_qty_map.get(token_id) or 0.0)
            requested_notional = float(order.get("requested_notional") or 0.0)
            requested_remaining = max(0.0, requested_notional - previous_reconciled_notional)
            reported_outstanding_notional = max(
                0.0,
                float(order.get("matched_notional_hint") or 0.0) - previous_reconciled_notional,
            )
            reported_outstanding_size = max(
                0.0,
                float(order.get("matched_size_hint") or 0.0) - previous_reconciled_size,
            )
            group_size = pending_group_counts.get(self._pending_reconcile_group_key(order), 1)
            matched_notional = 0.0
            use_position_delta = False
            attribution_source = ""
            reconcile_source = ""
            already_reflected_without_delta = False
            if reported_outstanding_notional > 1e-6:
                matched_notional = reported_outstanding_notional
                if requested_remaining > 0.0:
                    matched_notional = min(matched_notional, requested_remaining)
                use_position_delta = available_notional > 1e-6
                reconcile_source = "broker_reconcile" if use_position_delta else "broker_trades"
                attribution_source = "order_level_fill"
                if (
                    not use_position_delta
                    and side == "BUY"
                    and token_id in previous_positions
                    and token_id in next_positions
                ):
                    already_reflected_without_delta = True
                    reconcile_source = "broker_reconcile"
            elif available_notional > 1e-6:
                if group_size > 1 and not (status_snapshot is not None and status_snapshot.is_failed):
                    self._mark_pending_order_reconcile_ambiguous(
                        order=order,
                        now=now,
                        reason=f"multiple_pending_orders_for_token={token_id}:{side}",
                    )
                    remaining[key] = order
                    continue
                use_position_delta = True
                matched_notional = min(available_notional, requested_remaining) if requested_remaining > 0.0 else available_notional
                reconcile_source = "broker_reconcile"
                attribution_source = "token_position_delta"
            if matched_notional > 1e-6:
                matched_qty = 0.0
                if reported_outstanding_notional > 1e-9 and reported_outstanding_size > 0.0:
                    matched_qty = min(
                        reported_outstanding_size,
                        reported_outstanding_size * (matched_notional / reported_outstanding_notional),
                    )
                elif use_position_delta and available_notional > 1e-9 and available_qty > 0.0:
                    matched_qty = min(available_qty, available_qty * (matched_notional / available_notional))
                observed_price = float(
                    (float(fill_agg.get("price") or 0.0) if fill_agg is not None else 0.0)
                    or (status_snapshot.avg_fill_price if status_snapshot is not None and status_snapshot.avg_fill_price > 0.0 else 0.0)
                    or float(order.get("matched_price_hint") or 0.0)
                    or float((next_positions.get(token_id) or {}).get("price") or 0.0)
                    or float(order.get("requested_price") or 0.0)
                )
                if matched_qty <= 0.0 and observed_price > 0.0:
                    matched_qty = matched_notional / max(0.01, observed_price)
                fill_ts = int(order.get("last_fill_ts_hint") or 0) if attribution_source == "order_level_fill" else 0
                reason = str(order.get("reason") or order.get("message") or "broker reconcile")
                realized_pnl = 0.0
                order["reconcile_ambiguous_ts"] = 0
                order["reconcile_ambiguous_reason"] = ""
                if already_reflected_without_delta:
                    next_position = next_positions.get(token_id)
                    if next_position is not None:
                        next_position["opened_ts"] = int(next_position.get("opened_ts") or fill_ts or order.get("ts") or now)
                        next_position["last_buy_ts"] = int(fill_ts or now)
                        next_position["last_signal_id"] = str(order.get("last_signal_id") or order.get("signal_id") or "")
                        next_positions[token_id] = self._merge_position_context(
                            next_position,
                            self._pending_order_entry_context(order),
                        )
                else:
                    realized_pnl, _, _ = self._apply_pending_fill_delta(
                        order=order,
                        previous_positions=shadow_positions,
                        next_positions=shadow_positions,
                        filled_notional=matched_notional,
                        filled_qty=matched_qty,
                        fill_price=observed_price,
                        fill_ts=fill_ts,
                        now=now,
                    )
                    if use_position_delta:
                        delta_notional_map[token_id] = max(0.0, available_notional - min(available_notional, matched_notional))
                        delta_qty_map[token_id] = max(0.0, available_qty - min(available_qty, matched_qty))
                    self._sync_shadow_position_to_actual(
                        token_id=token_id,
                        shadow_positions=shadow_positions,
                        next_positions=next_positions,
                        position_delta_backed=use_position_delta,
                    )

                order["reconciled_notional_hint"] = max(
                    float(order.get("reconciled_notional_hint") or 0.0),
                    previous_reconciled_notional + matched_notional,
                )
                order["reconciled_size_hint"] = max(
                    float(order.get("reconciled_size_hint") or 0.0),
                    previous_reconciled_size + matched_qty,
                )
                if observed_price > 0.0:
                    order["matched_price_hint"] = observed_price
                order["last_fill_ts_hint"] = max(int(order.get("last_fill_ts_hint") or 0), int(fill_ts or now))

                remaining_requested = max(
                    0.0,
                    requested_notional - float(order.get("reconciled_notional_hint") or 0.0),
                )
                unreconciled_reported = max(
                    0.0,
                    float(order.get("matched_notional_hint") or 0.0)
                    - float(order.get("reconciled_notional_hint") or 0.0),
                )
                keep_pending = str(order.get("broker_status") or "") in active_statuses and remaining_requested > 1e-6
                if unreconciled_reported > 1e-6:
                    keep_pending = True
                reconcile_status = "PARTIAL" if keep_pending else "RECONCILED"
                if attribution_source == "order_level_fill":
                    reconcile_reason = (
                        "partial fill via order-level fill + broker positions"
                        if reconcile_source == "broker_reconcile" and keep_pending
                        else "reconciled via order-level fill + broker positions"
                        if reconcile_source == "broker_reconcile"
                        else "partial fill via order-level fill"
                        if keep_pending
                        else "reconciled via order-level fill"
                    )
                else:
                    reconcile_reason = (
                        "partial fill via broker positions" if keep_pending else "reconciled via broker positions"
                    )
                remaining_position = next_positions.get(token_id) or {}
                applied_ts = int(fill_ts or now)
                market_context = self._market_context_from_order(order)
                self.recent_orders.appendleft(
                    {
                        "ts": applied_ts,
                        "cycle_id": str(order.get("cycle_id") or ""),
                        "signal_id": str(order.get("signal_id") or ""),
                        "trace_id": str(order.get("trace_id") or ""),
                        "title": str(order.get("market_slug") or token_id),
                        "token_id": token_id,
                        "outcome": str(order.get("outcome") or ""),
                        "side": side,
                        "status": reconcile_status,
                        "order_id": order_id,
                        "broker_status": str(order.get("broker_status") or ""),
                        "retry_count": 0,
                        "latency_ms": max(0, (applied_ts - int(order.get("ts") or applied_ts)) * 1000),
                        "reason": f"{reason} | {reconcile_reason}",
                        "source_wallet": str(order.get("wallet") or ""),
                        "hold_minutes": self._position_hold_minutes(previous_positions.get(token_id), applied_ts) if side == "SELL" else 0,
                        "notional": matched_notional,
                        "realized_pnl": realized_pnl,
                        "wallet_score": float(order.get("wallet_score") or 0.0),
                        "wallet_tier": str(order.get("wallet_tier") or ""),
                        "position_action": str(order.get("position_action") or ""),
                        "position_action_label": str(order.get("position_action_label") or ""),
                        "topic_label": str(order.get("topic_label") or ""),
                        "topic_bias": str(order.get("topic_bias") or "neutral"),
                        "topic_multiplier": float(order.get("topic_multiplier") or 1.0),
                        **self._pending_order_entry_context(order),
                        **(
                            self._exit_result_meta(
                                exit_kind=str(order.get("exit_kind") or ""),
                                ok=True,
                                remaining_notional=float(remaining_position.get("notional") or 0.0),
                                remaining_qty=float(remaining_position.get("quantity") or 0.0),
                            )
                            if side == "SELL"
                            else {}
                        ),
                        **self._order_meta(
                            side,
                            exit_kind=str(order.get("exit_kind") or ""),
                            exit_label=str(order.get("exit_label") or ""),
                            exit_summary=str(order.get("exit_summary") or ""),
                        ),
                        **self._pending_order_snapshot(order),
                        **market_context,
                    }
                )
                self._append_event(
                    "order_partial_fill" if keep_pending else "order_reconciled",
                    {
                        "wallet": str(order.get("wallet") or ""),
                        "source_wallet": str(order.get("wallet") or ""),
                        "market_slug": str(order.get("market_slug") or token_id),
                        "token_id": token_id,
                        "cycle_id": str(order.get("cycle_id") or ""),
                        "signal_id": str(order.get("signal_id") or ""),
                        "trace_id": str(order.get("trace_id") or ""),
                        "side": side,
                        "status": "partial_fill" if keep_pending else "reconciled",
                        "requested_notional": requested_notional,
                        "filled_notional": matched_notional,
                        "price": observed_price,
                        "fill_ts": applied_ts,
                        "order_id": str(order.get("order_id") or ""),
                        "broker_status": str(order.get("broker_status") or ""),
                        "reason": attribution_source or ("reconciled_via_positions" if reconcile_source == "broker_reconcile" else "reconciled_via_trades"),
                        "realized_pnl": realized_pnl,
                        **market_context,
                    },
                )
                self._record_fill_ledger(
                    ts=applied_ts,
                    side=side,
                    token_id=token_id,
                    condition_id=str(order.get("condition_id") or ""),
                    market_slug=str(order.get("market_slug") or token_id),
                    quantity=matched_qty,
                    notional=matched_notional,
                    price=observed_price,
                    realized_pnl=realized_pnl,
                    signal_id=str(order.get("signal_id") or ""),
                    trace_id=str(order.get("trace_id") or ""),
                    order_id=order_id,
                    status=reconcile_status,
                    source_wallet=str(order.get("wallet") or ""),
                    source=reconcile_source or ("broker_reconcile" if use_position_delta else "broker_trades"),
                )
                if keep_pending:
                    remaining[key] = order
                continue

            if status_snapshot is not None and status_snapshot.is_failed:
                failed_status = status_snapshot.lifecycle_status or "failed"
                market_context = self._market_context_from_order(order)
                self.recent_orders.appendleft(
                    {
                        "ts": now,
                        "cycle_id": str(order.get("cycle_id") or ""),
                        "signal_id": str(order.get("signal_id") or ""),
                        "trace_id": str(order.get("trace_id") or ""),
                        "title": str(order.get("market_slug") or token_id),
                        "token_id": token_id,
                        "outcome": str(order.get("outcome") or ""),
                        "side": side,
                        "status": failed_status.upper(),
                        "order_id": order_id,
                        "broker_status": failed_status,
                        "retry_count": 0,
                        "latency_ms": max(0, (now - int(order.get("ts") or now)) * 1000),
                        "reason": str(status_snapshot.message or order.get("reason") or failed_status),
                        "source_wallet": str(order.get("wallet") or ""),
                        "hold_minutes": self._position_hold_minutes(previous_positions.get(token_id), now) if side == "SELL" else 0,
                        "notional": requested_notional,
                        "wallet_score": float(order.get("wallet_score") or 0.0),
                        "wallet_tier": str(order.get("wallet_tier") or ""),
                        "position_action": str(order.get("position_action") or ""),
                        "position_action_label": str(order.get("position_action_label") or ""),
                        "topic_label": str(order.get("topic_label") or ""),
                        "topic_bias": str(order.get("topic_bias") or "neutral"),
                        "topic_multiplier": float(order.get("topic_multiplier") or 1.0),
                        **self._pending_order_entry_context(order),
                        **(
                            self._exit_result_meta(
                                exit_kind=str(order.get("exit_kind") or ""),
                                ok=False,
                            )
                            if side == "SELL"
                            else {}
                        ),
                        **self._order_meta(
                            side,
                            exit_kind=str(order.get("exit_kind") or ""),
                            exit_label=str(order.get("exit_label") or ""),
                            exit_summary=str(order.get("exit_summary") or ""),
                        ),
                        **self._pending_order_snapshot(order),
                        **market_context,
                    }
                )
                self._append_event(
                    "order_terminal",
                    {
                        "wallet": str(order.get("wallet") or ""),
                        "market_slug": str(order.get("market_slug") or token_id),
                        "token_id": token_id,
                        "cycle_id": str(order.get("cycle_id") or ""),
                        "signal_id": str(order.get("signal_id") or ""),
                        "trace_id": str(order.get("trace_id") or ""),
                        "side": side,
                        "order_id": order_id,
                        "broker_status": failed_status,
                        "reason": str(status_snapshot.message or failed_status),
                        **market_context,
                    },
                )
                continue

            if (now - int(order.get("ts") or now)) >= timeout_seconds:
                market_context = self._market_context_from_order(order)
                self.recent_orders.appendleft(
                    {
                        "ts": now,
                        "cycle_id": str(order.get("cycle_id") or ""),
                        "signal_id": str(order.get("signal_id") or ""),
                        "trace_id": str(order.get("trace_id") or ""),
                        "title": str(order.get("market_slug") or token_id),
                        "token_id": token_id,
                        "outcome": str(order.get("outcome") or ""),
                        "side": side,
                        "status": "STALE",
                        "order_id": order_id,
                        "broker_status": str(order.get("broker_status") or ""),
                        "retry_count": 0,
                        "latency_ms": max(0, (now - int(order.get("ts") or now)) * 1000),
                        "reason": f"{str(order.get('reason') or order.get('message') or 'pending order')} | no broker reconciliation yet",
                        "source_wallet": str(order.get("wallet") or ""),
                        "hold_minutes": self._position_hold_minutes(previous_positions.get(token_id), now) if side == "SELL" else 0,
                        "notional": requested_notional,
                        "wallet_score": float(order.get("wallet_score") or 0.0),
                        "wallet_tier": str(order.get("wallet_tier") or ""),
                        "position_action": str(order.get("position_action") or ""),
                        "position_action_label": str(order.get("position_action_label") or ""),
                        "topic_label": str(order.get("topic_label") or ""),
                        "topic_bias": str(order.get("topic_bias") or "neutral"),
                        "topic_multiplier": float(order.get("topic_multiplier") or 1.0),
                        **self._pending_order_entry_context(order),
                        **self._order_meta(
                            side,
                            exit_kind=str(order.get("exit_kind") or ""),
                            exit_label=str(order.get("exit_label") or ""),
                            exit_summary=str(order.get("exit_summary") or ""),
                        ),
                        **self._pending_order_snapshot(order),
                        **market_context,
                    }
                )
                self._append_event(
                    "order_stale",
                    {
                        "wallet": str(order.get("wallet") or ""),
                        "market_slug": str(order.get("market_slug") or token_id),
                        "token_id": token_id,
                        "cycle_id": str(order.get("cycle_id") or ""),
                        "signal_id": str(order.get("signal_id") or ""),
                        "trace_id": str(order.get("trace_id") or ""),
                        "side": side,
                        "order_id": str(order.get("order_id") or ""),
                        "broker_status": str(order.get("broker_status") or ""),
                        "reason": "pending_order_timeout",
                        **market_context,
                    },
                )
                outcome, updated = self._cancel_pending_order(
                    order=order,
                    now=now,
                    position_lookup=previous_positions,
                    action_reason="pending_order_timeout",
                    force=False,
                )
                if outcome != "canceled":
                    remaining[key] = updated
                continue

            remaining[key] = order

        self.pending_orders = remaining
        self._refresh_risk_state()

    def _exit_result_meta(
        self,
        *,
        exit_kind: str,
        ok: bool,
        remaining_notional: float = 0.0,
        remaining_qty: float = 0.0,
    ) -> dict[str, object]:
        if not ok:
            return {"exit_result": "reject", "exit_result_label": self._exit_result_label("reject")}
        if str(exit_kind or "") == "emergency_exit":
            return {"exit_result": "emergency", "exit_result_label": self._exit_result_label("emergency")}
        if remaining_notional <= self.settings.stale_position_close_notional_usd or remaining_qty <= 0.0:
            return {"exit_result": "full_exit", "exit_result_label": self._exit_result_label("full_exit")}
        return {"exit_result": "partial_trim", "exit_result_label": self._exit_result_label("partial_trim")}

    @staticmethod
    def _pending_exit_result_meta(side: str) -> dict[str, object]:
        if str(side).upper() != "SELL":
            return {}
        return {"exit_result": "pending", "exit_result_label": Trader._exit_result_label("pending")}

    def _decision_snapshot(
        self,
        *,
        signal: Signal,
        control: ControlState,
        existing: dict[str, object] | None,
        decision: RiskDecision | None = None,
        skip_reason: str = "",
        cooldown_remaining: int = 0,
        add_cooldown_remaining: int = 0,
        sized_notional: float = 0.0,
        final_notional: float = 0.0,
        duplicate: bool = False,
        budget_limited: bool = False,
        netting_limited: bool = False,
        netting_snapshot: dict[str, object] | None = None,
        candidate_id: str = "",
        candidate_action: str = "",
        candidate_origin: str = "",
        block_reason: str = "",
        block_layer: str = "",
    ) -> dict[str, object]:
        normalized_block_reason = str(
            block_reason or skip_reason or (decision.reason if decision and not decision.allowed else "")
        ).strip()
        normalized_block_layer = str(block_layer or ("decision" if normalized_block_reason else "")).strip()
        snapshot = {
            "control": {
                "decision_mode": str(control.decision_mode or self.decision_mode or self.settings.decision_mode or "manual"),
                "pause_opening": bool(control.pause_opening),
                "reduce_only": bool(control.reduce_only),
                "emergency_stop": bool(control.emergency_stop),
            },
            "trading_mode": self.trading_mode_state(),
            "existing_position": bool(existing),
            "existing_trace_id": str((existing or {}).get("trace_id") or ""),
            "cooldown_remaining": int(max(0, cooldown_remaining)),
            "add_cooldown_remaining": int(max(0, add_cooldown_remaining)),
            "risk_allowed": bool(decision.allowed) if decision else False,
            "risk_reason": str(decision.reason) if decision else "",
            "risk_max_notional": float(decision.max_notional) if decision else 0.0,
            "risk_snapshot": dict(decision.snapshot) if decision else {},
            "price_band": {
                "price_hint": float(signal.price_hint or 0.0),
                "min_price": float(self.settings.min_price),
                "max_price": float(self.settings.max_price),
                "in_band": bool(self.settings.min_price <= float(signal.price_hint or 0.0) <= self.settings.max_price),
            },
            "wallet_score": float(signal.wallet_score or 0.0),
            "wallet_tier": str(signal.wallet_tier or ""),
            "wallet_score_summary": str(signal.wallet_score_summary or ""),
            "topic_snapshot": self._topic_snapshot(signal),
            "condition_id": str(signal.condition_id or ""),
            "netting_snapshot": dict(netting_snapshot or {}),
            "sized_notional": float(sized_notional or 0.0),
            "final_notional": float(final_notional or 0.0),
            "budget_limited": bool(budget_limited),
            "netting_limited": bool(netting_limited),
            "duplicate": bool(duplicate),
            "skip_reason": str(skip_reason or ""),
            "block_reason": normalized_block_reason,
            "block_layer": normalized_block_layer,
            "candidate_id": str(candidate_id or ""),
            "candidate_action": str(candidate_action or ""),
            "candidate_origin": str(candidate_origin or ""),
        }
        return snapshot

    @staticmethod
    def _order_trace_snapshot(
        order: dict[str, object],
    ) -> dict[str, object]:
        return {
            "status": str(order.get("status") or ""),
            "order_id": str(order.get("order_id") or ""),
            "broker_status": str(order.get("broker_status") or ""),
            "flow": str(order.get("flow") or ""),
            "position_action": str(order.get("position_action") or ""),
            "position_action_label": str(order.get("position_action_label") or ""),
            "reason": str(order.get("reason") or ""),
            "notional": float(order.get("notional") or 0.0),
            "realized_pnl": float(order.get("realized_pnl") or 0.0),
            "retry_count": int(order.get("retry_count") or 0),
            "latency_ms": int(order.get("latency_ms") or 0),
            "exit_kind": str(order.get("exit_kind") or ""),
            "exit_label": str(order.get("exit_label") or ""),
            "exit_result": str(order.get("exit_result") or ""),
            "exit_result_label": str(order.get("exit_result_label") or ""),
        }

    @staticmethod
    def _position_trace_snapshot(
        position: dict[str, object] | None,
        *,
        is_open: bool,
    ) -> dict[str, object]:
        source = position or {}
        return {
            "is_open": bool(is_open),
            "token_id": str(source.get("token_id") or ""),
            "condition_id": str(source.get("condition_id") or ""),
            "market_slug": str(source.get("market_slug") or ""),
            "outcome": str(source.get("outcome") or ""),
            "quantity": float(source.get("quantity") or 0.0),
            "notional": float(source.get("notional") or 0.0),
            "cost_basis_notional": float(source.get("cost_basis_notional") or 0.0),
            "opened_ts": int(source.get("opened_ts") or 0),
            "last_buy_ts": int(source.get("last_buy_ts") or 0),
            "last_trim_ts": int(source.get("last_trim_ts") or 0),
            "trace_id": str(source.get("trace_id") or ""),
            "origin_signal_id": str(source.get("origin_signal_id") or ""),
            "last_signal_id": str(source.get("last_signal_id") or ""),
            "entry_wallet": str(source.get("entry_wallet") or ""),
            "entry_wallet_tier": str(source.get("entry_wallet_tier") or ""),
            "entry_topic_label": str(source.get("entry_topic_label") or ""),
            "entry_reason": str(source.get("entry_reason") or ""),
            "last_exit_label": str(source.get("last_exit_label") or ""),
            "last_exit_summary": str(source.get("last_exit_summary") or ""),
        }

    @staticmethod
    def _order_reason(signal: Signal, base_message: str) -> str:
        reason = base_message
        if signal.side == "SELL" and str(signal.exit_reason or "").strip():
            reason = f"{reason} | {str(signal.exit_reason).strip()}"
        if abs(float(signal.topic_multiplier or 1.0) - 1.0) <= 1e-9:
            return reason
        topic_label = str(signal.topic_label or signal.topic_key or "topic")
        action = "boost" if str(signal.topic_bias) == "boost" else "trim"
        return f"{reason} | {topic_label} {action} x{float(signal.topic_multiplier or 1.0):.2f}"

    def _dump_runtime_state(self) -> dict[str, object]:
        self._order_cache_cleanup(time.time())
        positions: list[dict[str, object]] = []
        for pos in self.positions_book.values():
            positions.append(
                {
                    "token_id": str(pos.get("token_id") or ""),
                    "condition_id": str(pos.get("condition_id") or ""),
                    "market_slug": str(pos.get("market_slug") or ""),
                    "outcome": str(pos.get("outcome") or "YES"),
                    "quantity": float(pos.get("quantity") or 0.0),
                    "price": float(pos.get("price") or 0.0),
                    "notional": float(pos.get("notional") or 0.0),
                    "cost_basis_notional": float(pos.get("cost_basis_notional") or 0.0),
                    "opened_ts": int(pos.get("opened_ts") or 0),
                    "last_buy_ts": int(pos.get("last_buy_ts") or 0),
                    "last_trim_ts": int(pos.get("last_trim_ts") or 0),
                    "entry_wallet": str(pos.get("entry_wallet") or ""),
                    "entry_wallet_score": float(pos.get("entry_wallet_score") or 0.0),
                    "entry_wallet_tier": str(pos.get("entry_wallet_tier") or "LOW"),
                    "entry_topic_label": str(pos.get("entry_topic_label") or ""),
                    "entry_topic_bias": str(pos.get("entry_topic_bias") or "neutral"),
                    "entry_topic_multiplier": float(pos.get("entry_topic_multiplier") or 1.0),
                    "entry_topic_summary": str(pos.get("entry_topic_summary") or ""),
                    "entry_reason": str(pos.get("entry_reason") or ""),
                    "trace_id": str(pos.get("trace_id") or ""),
                    "origin_signal_id": str(pos.get("origin_signal_id") or ""),
                    "last_signal_id": str(pos.get("last_signal_id") or ""),
                    "last_exit_kind": str(pos.get("last_exit_kind") or ""),
                    "last_exit_label": str(pos.get("last_exit_label") or ""),
                    "last_exit_summary": str(pos.get("last_exit_summary") or ""),
                    "last_exit_ts": int(pos.get("last_exit_ts") or 0),
                    "time_exit_state": self._time_exit_state(pos),
                }
            )

        return {
            "ts": int(time.time()),
            "runtime_version": 9,
            "broker_event_sync_ts": int(self._last_broker_event_sync_ts),
            "trading_mode": self.trading_mode_state(),
            "admission": self.admission_state(),
            "kill_switch": self.kill_switch_state(),
            "signer_security": self.signer_security_state(),
            "runner_heartbeat": normalize_runner_heartbeat(self._runner_heartbeat),
            "buy_blocked_since_ts": int(self._buy_blocked_since_ts or 0),
            "buy_blocked": self.buy_blocked_state(),
            "persistence": self.persistence_state(),
            "recovery": {
                "blocked_buy": bool(self._recovery_block_buy_latched),
                "conflicts": list(self._recovery_conflicts),
            },
            "startup": {
                "ready": bool(self.startup_ready),
                "warning_count": int(self.startup_warning_count),
                "failure_count": int(self.startup_failure_count),
                "checks": list(self.startup_checks),
            },
            "decision_mode": str(self.decision_mode or self.settings.decision_mode or "manual"),
            "risk_state": {
                "day_key": self._active_day_key,
                "daily_realized_pnl": float(self.state.daily_realized_pnl),
                "broker_closed_pnl_today": float(self.state.broker_closed_pnl_today),
                "open_positions": int(self.state.open_positions),
                "tracked_notional_usd": float(self.state.tracked_notional_usd),
                "pending_entry_notional_usd": float(self.state.pending_entry_notional_usd),
                "pending_exit_notional_usd": float(self.state.pending_exit_notional_usd),
                "pending_entry_orders": int(self.state.pending_entry_orders),
                "equity_usd": float(self.state.equity_usd),
                "cash_balance_usd": float(self.state.cash_balance_usd),
                "positions_value_usd": float(self.state.positions_value_usd),
                "account_snapshot_ts": int(self.state.account_snapshot_ts),
                "valuation_currency": "USD",
                "risk_ledger_status": str(self.state.risk_ledger_status or "ok"),
                "risk_breaker_status": str(self.state.risk_breaker_status or "ok"),
                "wallet_exposure_committed_usd": float(self.state.wallet_exposure_committed_usd),
                "wallet_exposure_cap_usd": float(self.state.wallet_exposure_cap_usd),
                "wallet_exposure_usage_pct": (
                    float(self.state.wallet_exposure_committed_usd) / float(self.state.wallet_exposure_cap_usd)
                    if float(self.state.wallet_exposure_cap_usd) > 0.0
                    else 0.0
                ),
                "portfolio_exposure_committed_usd": float(self.state.portfolio_exposure_committed_usd),
                "portfolio_exposure_cap_usd": float(self.state.portfolio_exposure_cap_usd),
                "portfolio_exposure_usage_pct": (
                    float(self.state.portfolio_exposure_committed_usd) / float(self.state.portfolio_exposure_cap_usd)
                    if float(self.state.portfolio_exposure_cap_usd) > 0.0
                    else 0.0
                ),
                "condition_exposure_key": str(self.state.condition_exposure_key or ""),
                "condition_exposure_committed_usd": float(self.state.condition_exposure_committed_usd),
                "condition_exposure_cap_usd": float(self.state.condition_exposure_cap_usd),
                "condition_exposure_usage_pct": (
                    float(self.state.condition_exposure_committed_usd) / float(self.state.condition_exposure_cap_usd)
                    if float(self.state.condition_exposure_cap_usd) > 0.0
                    else 0.0
                ),
                "loss_streak_count": int(self.state.loss_streak_count),
                "loss_streak_current": int(self.state.loss_streak_count),
                "loss_streak_limit": int(self.state.loss_streak_limit),
                "loss_streak_blocked": bool(self.state.loss_streak_blocked),
                "intraday_drawdown_pct": float(self.state.intraday_drawdown_pct),
                "intraday_drawdown_current": float(self.state.intraday_drawdown_pct),
                "intraday_drawdown_limit_pct": float(self.state.intraday_drawdown_limit_pct),
                "intraday_drawdown_blocked": bool(self.state.intraday_drawdown_blocked),
                "risk_breaker_opening_allowed": bool(self.state.risk_breaker_opening_allowed),
                "risk_breaker_reason_codes": list(self.state.risk_breaker_reason_codes),
                "reason_codes": list(self.state.risk_breaker_reason_codes),
                "breaker_latched": bool(
                    bool(self.state.loss_streak_blocked)
                    or bool(self.state.intraday_drawdown_blocked)
                    or not bool(self.state.risk_breaker_opening_allowed)
                ),
                "risk_day_key": str(self.state.risk_day_key or ""),
                "risk_breaker_timezone": str(getattr(self.settings, "risk_breaker_timezone", "UTC") or "UTC"),
            },
            "risk_breakers": dict(self._risk_breaker_state or {}),
            "exposure_ledger": [dict(row) for row in self._exposure_ledger.values()],
            "positions": positions,
            "pending_orders": list(self.pending_orders.values()),
            "recent_order_keys": dict(self._recent_order_keys),
            "last_operator_action": dict(self.last_operator_action),
            "signal_cycles": list(self.recent_signal_cycles),
            "trace_registry": self._trace_records(),
        }

    def persist_runtime_state(self, path: str) -> None:
        payload = self._dump_runtime_state()
        try:
            if self._state_store is None:
                raise RuntimeError("runtime truth store unavailable")
            self._state_store.save_runtime_truth(
                {
                    "runtime": dict(payload),
                    "control": self._control_payload_from_runtime(self.control_state),
                    "risk": dict(payload.get("risk_state") or {}),
                    "reconciliation": dict(self.reconciliation_summary(now=int(time.time()))),
                    "risk_breakers": dict(self._risk_breaker_state or {}),
                    "exposure_ledger": [dict(row) for row in self._exposure_ledger.values()],
                    "positions": [dict(row) for row in list(payload.get("positions") or []) if isinstance(row, dict)],
                    "order_intents": self._build_order_intents_snapshot(),
                }
            )
        except Exception as exc:
            self._record_persistence_fault(kind="runtime_truth_write", path=self.settings.state_store_path, error=exc)
            raise
        resolved_path = str(Path(path).expanduser())
        parent = Path(resolved_path).parent
        if str(parent) not in {"", "."}:
            parent.mkdir(parents=True, exist_ok=True)
        tmp_path = f"{resolved_path}.tmp"
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, ensure_ascii=False)
            os.replace(tmp_path, resolved_path)
        except Exception as exc:
            self.log.warning("Runtime export write failed path=%s err=%s", resolved_path, exc)

    def _load_control_state(self) -> ControlState:
        payload: dict[str, object] = {}
        read_failed = False
        if self._state_store is not None:
            try:
                payload = dict(self._state_store.load_control_state() or {})
            except Exception as exc:
                self.log.error("Control state read failed db=%s err=%s", self.settings.state_store_path, exc)
                self._record_persistence_fault(kind="control_state_read", path=self.settings.state_store_path, error=exc)
                read_failed = True
                payload = {}
        if read_failed:
            self._recovery_block_buy_latched = True
            self._record_recovery_conflict(
                category="CONTROL_STATE_UNAVAILABLE",
                details="control state read failed",
            )
            payload = {
                "decision_mode": "manual",
                "pause_opening": True,
                "reduce_only": True,
                "emergency_stop": False,
                "clear_stale_pending_requested_ts": 0,
                "clear_risk_breakers_requested_ts": 0,
                "updated_ts": int(time.time()),
            }
        elif not payload:
            payload = {
                "decision_mode": self._normalize_decision_mode(self.settings.decision_mode),
                "pause_opening": False,
                "reduce_only": False,
                "emergency_stop": False,
                "clear_stale_pending_requested_ts": 0,
                "clear_risk_breakers_requested_ts": 0,
                "updated_ts": 0,
            }

        validation_errors = self._validate_control_payload(payload)
        if validation_errors:
            self._recovery_block_buy_latched = True
            self._record_recovery_conflict(
                category="CONTROL_STATE_INVALID",
                details=";".join(validation_errors),
            )
            payload = {
                "decision_mode": "manual",
                "pause_opening": True,
                "reduce_only": True,
                "emergency_stop": False,
                "clear_stale_pending_requested_ts": 0,
                "clear_risk_breakers_requested_ts": 0,
                "updated_ts": int(time.time()),
            }
            if self._state_store is not None:
                try:
                    self._state_store.save_control_state(payload)
                except Exception as exc:
                    self.log.error("Persist fail-closed control state failed db=%s err=%s", self.settings.state_store_path, exc)

        state = ControlState.from_payload(payload)
        state.decision_mode = self._normalize_decision_mode(state.decision_mode)

        signature = (
            state.decision_mode,
            state.pause_opening,
            state.reduce_only,
            state.emergency_stop,
            state.clear_stale_pending_requested_ts,
            state.clear_risk_breakers_requested_ts,
            state.updated_ts,
        )
        if signature != self._last_control_signature:
            self._last_control_signature = signature
            self.log.info(
                "CONTROL decision_mode=%s pause_opening=%s reduce_only=%s emergency_stop=%s clear_stale_pending_requested_ts=%d clear_risk_breakers_requested_ts=%d updated_ts=%d",
                state.decision_mode,
                state.pause_opening,
                state.reduce_only,
                state.emergency_stop,
                state.clear_stale_pending_requested_ts,
                state.clear_risk_breakers_requested_ts,
                state.updated_ts,
            )

        self.control_state = state
        self.decision_mode = state.decision_mode
        return state

    def _apply_operator_clear_stale_pending(self, control: ControlState) -> None:
        request_ts = int(control.clear_stale_pending_requested_ts or 0)
        if request_ts <= 0 or request_ts <= self._last_operator_pending_cleanup_ts:
            return
        self._last_operator_pending_cleanup_ts = request_ts

        if not self.pending_orders:
            self.last_operator_action = {
                "name": "clear_stale_pending",
                "requested_ts": request_ts,
                "processed_ts": int(time.time()),
                "status": "noop",
                "cleared_count": 0,
                "remaining_pending_orders": 0,
                "message": "no pending orders to clear",
            }
            self._append_event(
                "operator_clear_stale_pending_noop",
                {
                    "requested_ts": request_ts,
                    "reason": "no_pending_orders",
                },
            )
            return

        now = int(time.time())
        timeout_seconds = self._pending_timeout_seconds()
        remaining: dict[str, dict[str, object]] = {}
        cleared = 0
        requested = 0
        failed = 0
        for key, order in sorted(self.pending_orders.items(), key=lambda item: int(item[1].get("ts") or 0)):
            order_ts = int(order.get("ts") or now)
            is_stale = (now - order_ts) >= timeout_seconds
            if not is_stale:
                remaining[key] = order
                continue
            outcome, updated = self._cancel_pending_order(
                order=order,
                now=now,
                position_lookup=self.positions_book,
                action_reason="operator_clear_stale_pending",
                force=True,
            )
            if outcome == "canceled":
                cleared += 1
            else:
                if outcome == "requested":
                    requested += 1
                elif outcome != "throttled":
                    failed += 1
                remaining[key] = updated

        self.pending_orders = remaining
        self._refresh_risk_state()
        action_status = "noop"
        if cleared > 0 and requested == 0 and failed == 0:
            action_status = "cleared"
        elif cleared > 0 or requested > 0:
            action_status = "partial" if failed > 0 or requested > 0 else "cleared"
            if cleared == 0 and requested > 0 and failed == 0:
                action_status = "requested"
        elif failed > 0:
            action_status = "failed"
        self.last_operator_action = {
            "name": "clear_stale_pending",
            "requested_ts": request_ts,
            "processed_ts": now,
            "status": action_status,
            "cleared_count": int(cleared),
            "requested_count": int(requested),
            "failed_count": int(failed),
            "remaining_pending_orders": int(len(self.pending_orders)),
            "message": (
                f"cleared={cleared} requested={requested} failed={failed}"
                if (cleared + requested + failed) > 0
                else "no stale pending orders matched the cleanup request"
            ),
        }
        if cleared > 0:
            self.log.warning(
                "OPERATOR_CLEAR_STALE_PENDING requested_ts=%d cleared=%d remaining=%d",
                request_ts,
                cleared,
                len(self.pending_orders),
            )
        else:
            self._append_event(
                "operator_clear_stale_pending_noop",
                {
                    "requested_ts": request_ts,
                    "reason": "no_stale_pending_orders",
                    "pending_orders": len(self.pending_orders),
                },
            )

    def _apply_operator_clear_risk_breakers(self, control: ControlState) -> None:
        request_ts = int(control.clear_risk_breakers_requested_ts or 0)
        if request_ts <= 0 or request_ts <= self._last_operator_risk_breaker_clear_ts:
            return
        self._last_operator_risk_breaker_clear_ts = request_ts
        current = RiskBreakerState.from_payload(self._risk_breaker_state)
        current.loss_streak_count = 0
        current.loss_streak_blocked = False
        current.intraday_drawdown_blocked = False
        current.intraday_drawdown_pct = 0.0
        current.reason_codes = tuple()
        current.opening_allowed = True
        current.manual_lock = False
        current.manual_required = False
        current.updated_ts = int(time.time())
        self._risk_breaker_state = current.to_payload()
        self.last_operator_action = {
            "name": "clear_risk_breakers",
            "requested_ts": request_ts,
            "processed_ts": int(time.time()),
            "status": "cleared",
            "message": "loss streak and intraday drawdown breakers cleared",
        }
        self._append_event(
            "operator_clear_risk_breakers",
            {
                "requested_ts": request_ts,
                "day_key": str(current.day_key or ""),
                "loss_streak_count": int(current.loss_streak_count or 0),
            },
        )
        self._refresh_risk_state()

    def _apply_emergency_exit(self) -> None:
        if not self.positions_book:
            return

        now = int(time.time())
        emergency_exit_label = self._exit_kind_label("emergency_exit")
        close_notional = self.settings.stale_position_close_notional_usd
        cycle_id = self._new_cycle_id(now)
        wallet_pool_snapshot = self._wallet_pool_snapshot()
        cycle_record = {
            "cycle_id": cycle_id,
            "ts": now,
            "wallets": list(self.last_wallets),
            "wallet_pool_snapshot": list(wallet_pool_snapshot),
            "candidates": [],
        }
        cycle_has_candidates = False

        for token_id in list(self.positions_book.keys()):
            position = self.positions_book.get(token_id)
            if not position:
                continue

            current_notional = float(position.get("notional") or 0.0)
            current_qty = float(position.get("quantity") or 0.0)
            if current_notional <= 0 or current_qty <= 0:
                del self.positions_book[token_id]
                self.state.open_positions = max(0, self.state.open_positions - 1)
                continue

            sig = Signal(
                signal_id=self._new_signal_id(now),
                trace_id=str(position.get("trace_id") or self._new_trace_id(token_id, now)),
                wallet="system-emergency-stop",
                market_slug=str(position.get("market_slug") or token_id),
                token_id=token_id,
                condition_id=str(position.get("condition_id") or ""),
                outcome=str(position.get("outcome") or "YES"),
                side="SELL",
                confidence=1.0,
                price_hint=float(position.get("price") or 0.5),
                observed_size=current_qty,
                observed_notional=current_notional,
                timestamp=datetime.now(tz=timezone.utc),
                position_action="exit",
                position_action_label=emergency_exit_label,
            )
            signal_record = self._cycle_candidate_record(
                sig,
                cycle_id=cycle_id,
                wallet_pool_snapshot=wallet_pool_snapshot,
            )
            cycle_record["candidates"].append(signal_record)
            cycle_has_candidates = True
            base_decision_snapshot = self._decision_snapshot(
                signal=sig,
                control=self.control_state,
                existing=position,
                sized_notional=current_notional,
                final_notional=current_notional,
            )
            base_decision_snapshot["risk_allowed"] = True
            base_decision_snapshot["risk_reason"] = "emergency_stop"
            base_decision_snapshot["risk_snapshot"] = {
                "system_exit": "emergency_stop",
                "observed_notional": current_notional,
                "close_notional_threshold": close_notional,
            }
            result = self.broker.execute(sig, current_notional)
            if not result.ok:
                entry_context = self._position_entry_context(position)
                self._append_event(
                    "emergency_exit_fail",
                    {
                        "token_id": token_id,
                        "market_slug": str(position.get("market_slug") or token_id),
                        "trace_id": sig.trace_id,
                        "signal_id": sig.signal_id,
                        "side": sig.side,
                        "flow": "exit",
                        "position_action": sig.position_action,
                        "position_action_label": sig.position_action_label,
                        "exit_kind": "emergency_exit",
                        "exit_label": emergency_exit_label,
                        "hold_minutes": self._position_hold_minutes(position, now),
                        "exit_fraction": 1.0,
                        "notional": current_notional,
                        "reason": result.message,
                        "cycle_id": cycle_id,
                        "wallet_score": 0.0,
                        "wallet_tier": "SYSTEM",
                    },
                )
                self.recent_orders.appendleft(
                    {
                        "ts": now,
                        "cycle_id": cycle_id,
                        "signal_id": sig.signal_id,
                        "trace_id": sig.trace_id,
                        "title": sig.market_slug,
                        "token_id": sig.token_id,
                        "outcome": sig.outcome,
                        "side": sig.side,
                        "status": "REJECTED",
                        "retry_count": 0,
                        "latency_ms": 0,
                        "reason": f"emergency-exit failed: {result.message}",
                        "source_wallet": "system-emergency-stop",
                        "hold_minutes": self._position_hold_minutes(position, now),
                        "topic_label": str(position.get("entry_topic_label") or ""),
                        "notional": current_notional,
                        "wallet_score": 0.0,
                        "wallet_tier": "SYSTEM",
                        "position_action": sig.position_action,
                        "position_action_label": sig.position_action_label,
                        **entry_context,
                        **self._exit_result_meta(exit_kind="emergency_exit", ok=False),
                        **self._order_meta("SELL", exit_kind="emergency_exit", exit_label=emergency_exit_label, exit_summary="emergency-exit"),
                    }
                )
                signal_record["decision_snapshot"] = dict(base_decision_snapshot)
                signal_record["order_snapshot"] = self._order_trace_snapshot(dict(self.recent_orders[0]))
                signal_record["position_snapshot"] = self._position_trace_snapshot(position, is_open=True)
                signal_record["final_status"] = "order_rejected"
                self._trace_record(
                    trace_id=str(sig.trace_id or ""),
                    signal=sig,
                    cycle_id=cycle_id,
                    signal_record=dict(signal_record),
                    opened_ts=now,
                )
                self.log.error(
                    "EMERGENCY_EXIT_FAIL slug=%s token=%s reason=%s",
                    sig.market_slug,
                    sig.token_id,
                    result.message,
                )
                continue

            if result.is_pending:
                entry_context = self._position_entry_context(position)
                pending_record = self._register_pending_order(
                    signal=sig,
                    cycle_id=cycle_id,
                    result=result,
                    order_meta=self._order_meta("SELL", exit_kind="emergency_exit", exit_label=emergency_exit_label, exit_summary="emergency-exit"),
                    entry_context=entry_context,
                    previous_position=position,
                    order_reason=result.message,
                    now=now,
                )
                self._append_event(
                    "emergency_exit_posted",
                    {
                        "token_id": token_id,
                        "market_slug": str(position.get("market_slug") or token_id),
                        "trace_id": sig.trace_id,
                        "signal_id": sig.signal_id,
                        "side": sig.side,
                        "flow": "exit",
                        "position_action": sig.position_action,
                        "position_action_label": sig.position_action_label,
                        "exit_kind": "emergency_exit",
                        "exit_label": emergency_exit_label,
                        "hold_minutes": self._position_hold_minutes(position, now),
                        "exit_fraction": 1.0,
                        "requested_notional": result.requested_notional,
                        "order_id": result.broker_order_id or "",
                        "broker_status": result.lifecycle_status,
                        "reason": result.message,
                        "cycle_id": cycle_id,
                    },
                )
                self.recent_orders.appendleft(
                    {
                        "ts": now,
                        "cycle_id": cycle_id,
                        "signal_id": sig.signal_id,
                        "trace_id": sig.trace_id,
                        "title": sig.market_slug,
                        "token_id": sig.token_id,
                        "outcome": sig.outcome,
                        "side": sig.side,
                        "status": self._recent_order_status(result),
                        "order_id": result.broker_order_id or "",
                        "broker_status": result.lifecycle_status,
                        "retry_count": 0,
                        "latency_ms": 0,
                        "reason": f"emergency-exit pending: {result.message}",
                        "source_wallet": "system-emergency-stop",
                        "hold_minutes": self._position_hold_minutes(position, now),
                        "topic_label": str(position.get("entry_topic_label") or ""),
                        "notional": result.requested_notional,
                        "wallet_score": 0.0,
                        "wallet_tier": "SYSTEM",
                        "position_action": sig.position_action,
                        "position_action_label": sig.position_action_label,
                        **entry_context,
                        **self._pending_exit_result_meta(sig.side),
                        **self._order_meta("SELL", exit_kind="emergency_exit", exit_label=emergency_exit_label, exit_summary="emergency-exit"),
                        **self._pending_order_snapshot(pending_record),
                    }
                )
                signal_record["decision_snapshot"] = dict(base_decision_snapshot)
                signal_record["order_snapshot"] = self._order_trace_snapshot(dict(self.recent_orders[0]))
                signal_record["position_snapshot"] = self._position_trace_snapshot(position, is_open=True)
                signal_record["final_status"] = "order_pending"
                self._trace_record(
                    trace_id=str(sig.trace_id or ""),
                    signal=sig,
                    cycle_id=cycle_id,
                    signal_record=dict(signal_record),
                    opened_ts=now,
                )
                self.log.warning(
                    "EMERGENCY_EXIT_POSTED slug=%s token=%s notional=%.2f status=%s order_id=%s",
                    sig.market_slug,
                    sig.token_id,
                    result.requested_notional,
                    result.lifecycle_status,
                    result.broker_order_id,
                )
                continue

            filled_qty = result.filled_notional / max(0.01, result.filled_price)
            remaining_notional = max(0.0, current_notional - result.filled_notional)
            remaining_qty = max(0.0, current_qty - filled_qty)
            position["notional"] = remaining_notional
            position["quantity"] = remaining_qty
            position["price"] = result.filled_price
            position["last_trim_ts"] = now
            position["last_signal_id"] = sig.signal_id
            self._apply_position_exit_meta(
                position,
                exit_kind="emergency_exit",
                exit_label=emergency_exit_label,
                exit_summary="emergency-exit",
                ts=now,
            )

            self._append_event(
                "emergency_exit_partial",
                {
                    "token_id": token_id,
                    "market_slug": str(position.get("market_slug") or token_id),
                    "trace_id": sig.trace_id,
                    "signal_id": sig.signal_id,
                    "side": sig.side,
                    "flow": "exit",
                    "position_action": sig.position_action,
                    "position_action_label": sig.position_action_label,
                    "exit_kind": "emergency_exit",
                    "exit_label": emergency_exit_label,
                    "exit_result": "emergency",
                    "exit_result_label": self._exit_result_label("emergency"),
                    "hold_minutes": self._position_hold_minutes(position, now),
                    "exit_fraction": 1.0,
                    "filled_notional": result.filled_notional,
                    "notional": result.filled_notional,
                    "remaining_notional": remaining_notional,
                    "remaining_qty": remaining_qty,
                    "order_id": result.broker_order_id or "",
                    "reason": "emergency_exit",
                    "cycle_id": cycle_id,
                    "wallet_score": 0.0,
                    "wallet_tier": "SYSTEM",
                },
            )

            self.recent_orders.appendleft(
                {
                    "ts": now,
                    "cycle_id": cycle_id,
                    "signal_id": sig.signal_id,
                    "trace_id": sig.trace_id,
                    "title": sig.market_slug,
                    "token_id": sig.token_id,
                    "outcome": sig.outcome,
                    "side": sig.side,
                    "status": "FILLED",
                    "retry_count": 0,
                    "latency_ms": 0,
                    "reason": "emergency-exit",
                    "source_wallet": "system-emergency-stop",
                    "hold_minutes": self._position_hold_minutes(position, now),
                    "topic_label": str(position.get("entry_topic_label") or ""),
                    "notional": result.filled_notional,
                    "wallet_score": 0.0,
                    "wallet_tier": "SYSTEM",
                    "position_action": sig.position_action,
                    "position_action_label": sig.position_action_label,
                    **self._position_entry_context(position),
                    **self._exit_result_meta(
                        exit_kind="emergency_exit",
                        ok=True,
                        remaining_notional=remaining_notional,
                        remaining_qty=remaining_qty,
                    ),
                    **self._order_meta("SELL", exit_kind="emergency_exit", exit_label=emergency_exit_label, exit_summary="emergency-exit"),
                }
            )
            position_snapshot = self._position_trace_snapshot(
                position,
                is_open=not (remaining_notional <= close_notional or remaining_qty <= 0),
            )
            position_snapshot["notional"] = remaining_notional
            position_snapshot["quantity"] = remaining_qty
            signal_record["decision_snapshot"] = dict(base_decision_snapshot)
            signal_record["order_snapshot"] = self._order_trace_snapshot(dict(self.recent_orders[0]))
            signal_record["position_snapshot"] = position_snapshot
            signal_record["final_status"] = "filled"
            self._trace_record(
                trace_id=str(sig.trace_id or ""),
                signal=sig,
                cycle_id=cycle_id,
                signal_record=dict(signal_record),
                opened_ts=now,
            )
            self.log.warning(
                "EMERGENCY_EXIT slug=%s token=%s notional=%.2f remain_notional=%.2f",
                sig.market_slug,
                sig.token_id,
                result.filled_notional,
                remaining_notional,
            )

            if remaining_notional <= close_notional or remaining_qty <= 0:
                del self.positions_book[token_id]
                self.state.open_positions = max(0, self.state.open_positions - 1)
                self._append_event(
                    "emergency_exit_close",
                    {
                        "token_id": token_id,
                        "market_slug": str(position.get("market_slug") or token_id),
                        "remaining_notional": remaining_notional,
                        "action": "position_closed",
                    },
                )
                if self.settings.token_reentry_cooldown_seconds > 0:
                    self.token_reentry_until[token_id] = now + self.settings.token_reentry_cooldown_seconds
                self._mark_trace_closed(str(sig.trace_id or ""), now)
                self.log.warning(
                    "EMERGENCY_EXIT_CLOSE slug=%s token=%s open_positions=%d",
                    sig.market_slug,
                    sig.token_id,
                    self.state.open_positions,
                )

        if cycle_has_candidates:
            self.recent_signal_cycles.appendleft(cycle_record)

    def _resolve_wallets(self) -> list[str]:
        seed_wallets = self.settings.wallet_list
        mode = self.settings.wallet_discovery_mode.strip().lower()
        if not self.settings.wallet_discovery_enabled:
            self._wallet_activity_available = False
            self._cached_wallet_activity_counts = {}
            self._update_strategy_activity_counts({}, available=False)
            self._cached_wallet_selection_context = {}
            self._update_strategy_selection_context({})
            if mode == "replace":
                return []
            return seed_wallets
        now = time.time()
        cache_age = now - self._cached_wallets_ts
        if self._wallet_cache_ready and cache_age < self.settings.wallet_discovery_refresh_seconds:
            self._update_strategy_activity_counts(
                self._cached_wallet_activity_counts,
                available=self._wallet_activity_available,
            )
            self._update_strategy_selection_context(self._cached_wallet_selection_context)
            return self._cached_wallets

        try:
            discovered_counts = self.data_client.discover_wallet_activity(
                paths=self.settings.wallet_discovery_path_list,
                limit=self.settings.wallet_discovery_limit,
            )
        except Exception as exc:
            fallback = self._cached_wallets if self._wallet_cache_ready else (seed_wallets if mode != "replace" else [])
            self.log.warning("Wallet discovery failed, fallback wallets=%d: %s", len(fallback), exc)
            self._cached_wallets = fallback
            self._cached_wallets_ts = now
            self._wallet_cache_ready = True
            self._update_strategy_activity_counts(
                self._cached_wallet_activity_counts,
                available=self._wallet_activity_available,
            )
            self._update_strategy_selection_context(self._cached_wallet_selection_context)
            return fallback

        discovered_rows = [
            (str(wallet).strip().lower(), int(count))
            for wallet, count in sorted(
                discovered_counts.items(),
                key=lambda item: (item[1], item[0]),
                reverse=True,
            )
            if str(wallet).strip() and int(count) >= self.settings.wallet_discovery_min_events
        ]

        preview_size = min(
            len(discovered_rows),
            max(
                self.settings.wallet_discovery_top_n,
                self.settings.wallet_discovery_top_n + self.settings.wallet_discovery_quality_top_n,
            ),
        )
        preview_wallets = [wallet for wallet, _ in discovered_rows[:preview_size]]
        quality_metrics: dict[str, RealizedWalletMetrics] = {}
        topic_profiles: dict[str, list[dict[str, object]]] = {}
        if (
            self.settings.wallet_discovery_quality_bias_enabled
            and self._wallet_history_store is not None
            and preview_wallets
        ):
            history_targets = list(dict.fromkeys(seed_wallets + preview_wallets))
            try:
                self._wallet_history_store.sync_wallets(
                    history_targets,
                    max_wallets=self.settings.wallet_discovery_quality_top_n,
                )
                quality_metrics, _, _, topic_profiles = self._wallet_history_store.peek_wallets(history_targets)
            except Exception as exc:
                self.log.warning("Wallet discovery quality bias failed preview=%d err=%s", len(history_targets), exc)

        selection_context: dict[str, dict[str, object]] = {}
        ranked_rows: list[tuple[str, float, int]] = []
        candidate_wallets = (
            [wallet for wallet, _ in discovered_rows]
            if mode == "replace"
            else list(dict.fromkeys(seed_wallets + [wallet for wallet, _ in discovered_rows]))
        )
        for wallet in candidate_wallets:
            activity_count = int(discovered_counts.get(wallet, 0))
            history_bonus, history_reason = self._discovery_history_bonus(quality_metrics.get(wallet))
            topic_bonus, best_topic, topic_reason = self._discovery_topic_bonus(topic_profiles.get(wallet))
            priority_score = round(activity_count + history_bonus + topic_bonus, 4)
            reason_parts = [f"{activity_count} events"]
            if history_reason:
                reason_parts.append(f"hist +{history_bonus:.2f} ({history_reason})")
            if topic_reason:
                label = best_topic or "topic"
                reason_parts.append(f"{label} +{topic_bonus:.2f} ({topic_reason})")
            selection_context[wallet] = {
                "discovery_activity_events": activity_count,
                "discovery_priority_score": priority_score,
                "discovery_history_bonus": history_bonus,
                "discovery_topic_bonus": topic_bonus,
                "discovery_priority_reason": " | ".join(reason_parts),
                "discovery_best_topic": best_topic,
            }
            ranked_rows.append((wallet, priority_score, activity_count))

        ranked_rows.sort(key=lambda row: (row[1], row[2], row[0]), reverse=True)
        rank_map = {wallet: index + 1 for index, (wallet, _, _) in enumerate(ranked_rows)}
        for wallet, context in selection_context.items():
            context["discovery_priority_rank"] = rank_map.get(wallet, 0)

        discovered_wallet_set = {wallet for wallet, _ in discovered_rows}
        discovered = [
            wallet
            for wallet, _, _ in ranked_rows
            if wallet in discovered_wallet_set
        ][: self.settings.wallet_discovery_top_n]

        if mode == "replace":
            selected = discovered
        else:
            selected = list(dict.fromkeys(seed_wallets + discovered))
            selected.sort(
                key=lambda wallet: (
                    float(selection_context.get(wallet, {}).get("discovery_priority_score") or 0.0),
                    int(selection_context.get(wallet, {}).get("discovery_activity_events") or 0),
                    wallet,
                ),
                reverse=True,
            )

        self.log.info(
            "Wallet universe resolved mode=%s seed=%d discovered=%d selected=%d quality_bias=%s",
            mode if mode else "union",
            len(seed_wallets),
            len(discovered),
            len(selected),
            "on" if self.settings.wallet_discovery_quality_bias_enabled else "off",
        )
        self._cached_wallets = selected
        self._cached_wallet_activity_counts = {
            str(wallet).strip().lower(): int(count)
            for wallet, count in discovered_counts.items()
            if str(wallet).strip()
        }
        self._cached_wallet_selection_context = selection_context
        self._cached_wallets_ts = now
        self._wallet_cache_ready = True
        self._wallet_activity_available = True
        self._update_strategy_activity_counts(
            self._cached_wallet_activity_counts,
            available=True,
        )
        self._update_strategy_selection_context(selection_context)
        return selected

    def _apply_time_exit(self) -> None:
        if not self.positions_book:
            return

        now = int(time.time())
        time_exit_label = self._exit_kind_label("time_exit")
        time_trim_label = self._action_tag_label("trim")
        utilization = 0.0
        if self.settings.max_open_positions > 0:
            utilization = self.state.open_positions / self.settings.max_open_positions
        congested = utilization >= self.settings.congested_utilization_threshold

        if congested:
            stale_seconds = self.settings.congested_stale_minutes * 60
            trim_pct = self.settings.congested_trim_pct
        else:
            stale_seconds = self.settings.stale_position_minutes * 60
            trim_pct = self.settings.stale_position_trim_pct
        trim_cooldown = self.settings.stale_position_trim_cooldown_seconds
        close_notional = self.settings.stale_position_close_notional_usd
        actionable_floor = self._actionable_notional_floor_usd()
        cycle_id = self._new_cycle_id(now)
        wallet_pool_snapshot = self._wallet_pool_snapshot()
        cycle_record = {
            "cycle_id": cycle_id,
            "ts": now,
            "wallets": list(self.last_wallets),
            "wallet_pool_snapshot": list(wallet_pool_snapshot),
            "candidates": [],
        }
        cycle_has_candidates = False
        time_exit_plan: list[dict[str, object]] = []
        for token_id in list(self.positions_book.keys()):
            position = self.positions_book.get(token_id)
            if not position:
                continue

            opened_ts = int(position.get("opened_ts") or now)
            if now - opened_ts < stale_seconds:
                continue

            current_notional = float(position.get("notional") or 0.0)
            current_qty = float(position.get("quantity") or 0.0)
            if current_notional <= 0.0 or current_qty <= 0.0:
                continue

            current_state = normalize_time_exit_state(position.get("time_exit_state"))
            if not should_attempt_time_exit(current_state, now_ts=now):
                continue

            last_trim_ts = int(position.get("last_trim_ts") or 0)
            if current_state.stage != TIME_EXIT_STAGE_FORCE_EXIT and now - last_trim_ts < trim_cooldown:
                continue

            priority_state = self._time_exit_priority_state(position, now=now)
            force_exit_active = bool(priority_state["force_exit"])
            full_close = current_notional <= close_notional or force_exit_active
            exit_fraction = 1.0 if full_close else trim_pct
            target_notional = current_notional if full_close else (current_notional * trim_pct)
            if target_notional <= 0.0:
                continue
            if not force_exit_active and target_notional < actionable_floor:
                continue

            time_exit_plan.append(
                {
                    "token_id": token_id,
                    "position": position,
                    "priority_state": priority_state,
                    "priority": int(priority_state["priority"]),
                    "hold_minutes": self._position_hold_minutes(position, now),
                    "target_notional": float(target_notional),
                    "full_close": bool(full_close),
                    "exit_fraction": float(exit_fraction),
                    "force_exit_active": force_exit_active,
                }
            )

        time_exit_plan.sort(
            key=lambda row: (
                int(row["priority"]),
                int(row["hold_minutes"]),
                float(row["target_notional"]),
            ),
            reverse=True,
        )

        for plan in time_exit_plan:
            token_id = str(plan["token_id"])
            position = self.positions_book.get(token_id)
            if not position:
                continue

            current_notional = float(position.get("notional") or 0.0)
            current_qty = float(position.get("quantity") or 0.0)
            if current_notional <= 0.0 or current_qty <= 0.0:
                continue

            current_state = begin_time_exit_attempt(
                normalize_time_exit_state(position.get("time_exit_state")),
                now_ts=now,
                market_volatility_bps=float(plan["priority_state"]["volatility_bps"]),
                volatility_step_bps=float(self.settings.time_exit_priority_volatility_step_bps),
            )
            state_payload = self._set_time_exit_state(position, current_state)
            force_exit_active = current_state.stage == TIME_EXIT_STAGE_FORCE_EXIT
            full_close = current_notional <= close_notional or force_exit_active
            exit_fraction = 1.0 if full_close else trim_pct
            target_notional = current_notional if full_close else (current_notional * trim_pct)
            if target_notional <= 0.0:
                continue
            if not force_exit_active and target_notional < actionable_floor:
                continue

            exit_summary = "time-exit force-exit" if force_exit_active else "time-exit"
            sig = Signal(
                signal_id=self._new_signal_id(now),
                trace_id=str(position.get("trace_id") or self._new_trace_id(token_id, now)),
                wallet="system-time-exit",
                market_slug=str(position.get("market_slug") or token_id),
                token_id=token_id,
                condition_id=str(position.get("condition_id") or ""),
                outcome=str(position.get("outcome") or "YES"),
                side="SELL",
                confidence=0.5,
                price_hint=float(position.get("price") or 0.5),
                observed_size=current_qty if full_close else (current_qty * trim_pct),
                observed_notional=target_notional,
                timestamp=datetime.now(tz=timezone.utc),
                exit_reason=exit_summary,
                position_action="exit" if full_close or exit_fraction >= 0.95 else "trim",
                position_action_label=time_exit_label if full_close or exit_fraction >= 0.95 else time_trim_label,
            )
            signal_record = self._cycle_candidate_record(
                sig,
                cycle_id=cycle_id,
                wallet_pool_snapshot=wallet_pool_snapshot,
            )
            cycle_record["candidates"].append(signal_record)
            cycle_has_candidates = True
            base_decision_snapshot = self._decision_snapshot(
                signal=sig,
                control=self.control_state,
                existing=position,
                sized_notional=target_notional,
                final_notional=target_notional,
            )
            base_decision_snapshot["risk_allowed"] = True
            base_decision_snapshot["risk_reason"] = "time_exit"
            base_decision_snapshot["risk_snapshot"] = {
                "system_exit": "time_exit",
                "congested": bool(congested),
                "trim_pct": float(exit_fraction),
                "stale_seconds": int(stale_seconds),
                "trim_cooldown": int(trim_cooldown),
                "close_notional_threshold": float(close_notional),
                "target_notional": float(target_notional),
                "time_exit_stage": str(state_payload.get("stage") or TIME_EXIT_STAGE_IDLE),
                "time_exit_failures": int(state_payload.get("consecutive_failures") or 0),
                "time_exit_priority": int(state_payload.get("priority") or 0),
                "time_exit_priority_reason": str(state_payload.get("priority_reason") or ""),
                "time_exit_market_volatility_bps": float(state_payload.get("market_volatility_bps") or 0.0),
                "time_exit_force_exit": bool(force_exit_active),
                "time_exit_next_retry_ts": int(state_payload.get("next_retry_ts") or 0),
            }
            result = self.broker.execute(sig, target_notional)
            if not result.ok:
                retired = False
                if self._is_missing_orderbook_message(result.message):
                    wallet = str(self.settings.funder_address or "").strip()
                    snapshot = None
                    if wallet:
                        try:
                            snapshot = self.data_client.get_accounting_snapshot(wallet)
                        except Exception as exc:
                            self.log.warning(
                                "Accounting snapshot refresh failed during time-exit retirement wallet=%s token=%s err=%s",
                                wallet,
                                token_id,
                                exc,
                            )
                    retired = self._retire_position_from_account_snapshot(
                        token_id=token_id,
                        position=position,
                        snapshot=snapshot,
                        reason=str(result.message or ""),
                        now=now,
                    )
                next_state = current_state if retired else record_time_exit_failure(
                    current_state,
                    now_ts=now,
                    retry_limit=int(self.settings.time_exit_retry_limit),
                    retry_cooldown_seconds=int(self.settings.time_exit_retry_cooldown_seconds),
                    volatility_step_bps=float(self.settings.time_exit_priority_volatility_step_bps),
                    error_message=str(result.message or ""),
                )
                if not retired:
                    self._set_time_exit_state(position, next_state)
                    if current_state.stage != TIME_EXIT_STAGE_FORCE_EXIT and next_state.stage == TIME_EXIT_STAGE_FORCE_EXIT:
                        self._append_event(
                            "time_exit_force_armed",
                            {
                                "token_id": token_id,
                                "market_slug": sig.market_slug,
                                "trace_id": sig.trace_id,
                                "priority": int(next_state.priority),
                                "failure_count": int(next_state.consecutive_failures),
                                "cycle_id": cycle_id,
                            },
                        )
                entry_context = self._position_entry_context(position)
                self._append_event(
                    "time_exit_fail",
                    {
                        "token_id": token_id,
                        "market_slug": sig.market_slug,
                        "trace_id": sig.trace_id,
                        "signal_id": sig.signal_id,
                        "side": sig.side,
                        "flow": "exit",
                        "position_action": sig.position_action,
                        "position_action_label": sig.position_action_label,
                        "exit_kind": "time_exit",
                        "exit_label": time_exit_label,
                        "hold_minutes": self._position_hold_minutes(position, now),
                        "exit_fraction": float(exit_fraction),
                        "trim_notional": target_notional,
                        "reason": result.message,
                        "cycle_id": cycle_id,
                        "wallet_score": 0.0,
                        "wallet_tier": "SYSTEM",
                        "position_retired": bool(retired),
                        "time_exit_stage": str(next_state.stage),
                        "time_exit_failure_count": int(next_state.consecutive_failures),
                        "exit_priority": int(next_state.priority),
                        "exit_priority_reason": str(next_state.priority_reason or ""),
                        "market_volatility_bps": float(next_state.market_volatility_bps or 0.0),
                        "force_exit_active": bool(next_state.stage == TIME_EXIT_STAGE_FORCE_EXIT),
                    },
                )
                self.recent_orders.appendleft(
                    {
                        "ts": now,
                        "cycle_id": cycle_id,
                        "signal_id": sig.signal_id,
                        "trace_id": sig.trace_id,
                        "title": sig.market_slug,
                        "token_id": sig.token_id,
                        "outcome": sig.outcome,
                        "side": sig.side,
                        "status": "REJECTED",
                        "retry_count": int(next_state.consecutive_failures),
                        "latency_ms": 0,
                        "reason": f"time-exit failed: {result.message}",
                        "source_wallet": "system-time-exit",
                        "hold_minutes": self._position_hold_minutes(position, now),
                        "topic_label": str(position.get("entry_topic_label") or ""),
                        "notional": target_notional,
                        "wallet_score": 0.0,
                        "wallet_tier": "SYSTEM",
                        "position_action": sig.position_action,
                        "position_action_label": sig.position_action_label,
                        "time_exit_stage": str(next_state.stage),
                        "time_exit_failure_count": int(next_state.consecutive_failures),
                        "exit_priority": int(next_state.priority),
                        "exit_priority_reason": str(next_state.priority_reason or ""),
                        "market_volatility_bps": float(next_state.market_volatility_bps or 0.0),
                        "force_exit_active": bool(next_state.stage == TIME_EXIT_STAGE_FORCE_EXIT),
                        **entry_context,
                        **self._exit_result_meta(exit_kind="time_exit", ok=False),
                        **self._order_meta(
                            "SELL",
                            exit_kind="time_exit",
                            exit_label=time_exit_label,
                            exit_summary="time-exit retired" if retired else exit_summary,
                        ),
                    }
                )
                signal_record["decision_snapshot"] = dict(base_decision_snapshot)
                signal_record["order_snapshot"] = self._order_trace_snapshot(dict(self.recent_orders[0]))
                signal_record["position_snapshot"] = self._position_trace_snapshot(position, is_open=not retired)
                signal_record["final_status"] = "position_retired" if retired else "order_rejected"
                self._trace_record(
                    trace_id=str(sig.trace_id or ""),
                    signal=sig,
                    cycle_id=cycle_id,
                    signal_record=dict(signal_record),
                    opened_ts=now,
                )
                if retired:
                    self.log.warning(
                        "TIME_EXIT_RETIRE slug=%s token=%s reason=%s",
                        sig.market_slug,
                        sig.token_id,
                        result.message,
                    )
                else:
                    self.log.error(
                        "TIME_EXIT_FAIL stage=%s priority=%d slug=%s token=%s reason=%s",
                        next_state.stage,
                        int(next_state.priority),
                        sig.market_slug,
                        sig.token_id,
                        result.message,
                    )
                continue

            if result.is_pending:
                pending_state = dict(state_payload)
                pending_state["last_result"] = "pending"
                pending_state["last_error"] = ""
                self._set_time_exit_state(position, pending_state)
                entry_context = self._position_entry_context(position)
                pending_record = self._register_pending_order(
                    signal=sig,
                    cycle_id=cycle_id,
                    result=result,
                    order_meta=self._order_meta("SELL", exit_kind="time_exit", exit_label=time_exit_label, exit_summary=exit_summary),
                    entry_context=entry_context,
                    previous_position=position,
                    order_reason=result.message,
                    now=now,
                )
                self._append_event(
                    "time_exit_posted",
                    {
                        "token_id": token_id,
                        "market_slug": sig.market_slug,
                        "trace_id": sig.trace_id,
                        "signal_id": sig.signal_id,
                        "side": sig.side,
                        "flow": "exit",
                        "position_action": sig.position_action,
                        "position_action_label": sig.position_action_label,
                        "exit_kind": "time_exit",
                        "exit_label": time_exit_label,
                        "hold_minutes": self._position_hold_minutes(position, now),
                        "exit_fraction": float(exit_fraction),
                        "requested_notional": result.requested_notional,
                        "order_id": result.broker_order_id or "",
                        "broker_status": result.lifecycle_status,
                        "reason": result.message,
                        "cycle_id": cycle_id,
                        "time_exit_stage": str(pending_state.get("stage") or TIME_EXIT_STAGE_IDLE),
                        "time_exit_failure_count": int(pending_state.get("consecutive_failures") or 0),
                        "exit_priority": int(pending_state.get("priority") or 0),
                        "exit_priority_reason": str(pending_state.get("priority_reason") or ""),
                        "market_volatility_bps": float(pending_state.get("market_volatility_bps") or 0.0),
                        "force_exit_active": bool(force_exit_active),
                    },
                )
                self.recent_orders.appendleft(
                    {
                        "ts": now,
                        "cycle_id": cycle_id,
                        "signal_id": sig.signal_id,
                        "trace_id": sig.trace_id,
                        "title": sig.market_slug,
                        "token_id": sig.token_id,
                        "outcome": sig.outcome,
                        "side": sig.side,
                        "status": self._recent_order_status(result),
                        "order_id": result.broker_order_id or "",
                        "broker_status": result.lifecycle_status,
                        "retry_count": int(pending_state.get("consecutive_failures") or 0),
                        "latency_ms": 0,
                        "reason": f"time-exit pending: {result.message}",
                        "source_wallet": "system-time-exit",
                        "hold_minutes": self._position_hold_minutes(position, now),
                        "topic_label": str(position.get("entry_topic_label") or ""),
                        "notional": result.requested_notional,
                        "wallet_score": 0.0,
                        "wallet_tier": "SYSTEM",
                        "position_action": sig.position_action,
                        "position_action_label": sig.position_action_label,
                        "time_exit_stage": str(pending_state.get("stage") or TIME_EXIT_STAGE_IDLE),
                        "time_exit_failure_count": int(pending_state.get("consecutive_failures") or 0),
                        "exit_priority": int(pending_state.get("priority") or 0),
                        "exit_priority_reason": str(pending_state.get("priority_reason") or ""),
                        "market_volatility_bps": float(pending_state.get("market_volatility_bps") or 0.0),
                        "force_exit_active": bool(force_exit_active),
                        **entry_context,
                        **self._pending_exit_result_meta(sig.side),
                        **self._order_meta("SELL", exit_kind="time_exit", exit_label=time_exit_label, exit_summary=exit_summary),
                        **self._pending_order_snapshot(pending_record),
                    }
                )
                signal_record["decision_snapshot"] = dict(base_decision_snapshot)
                signal_record["order_snapshot"] = self._order_trace_snapshot(dict(self.recent_orders[0]))
                signal_record["position_snapshot"] = self._position_trace_snapshot(position, is_open=True)
                signal_record["final_status"] = "order_pending"
                self._trace_record(
                    trace_id=str(sig.trace_id or ""),
                    signal=sig,
                    cycle_id=cycle_id,
                    signal_record=dict(signal_record),
                    opened_ts=now,
                )
                self.log.info(
                    "TIME_EXIT_POSTED stage=%s priority=%d mode=%s slug=%s token=%s notional=%.2f status=%s order_id=%s",
                    pending_state.get("stage") or TIME_EXIT_STAGE_IDLE,
                    int(pending_state.get("priority") or 0),
                    "congested" if congested else "normal",
                    sig.market_slug,
                    sig.token_id,
                    result.requested_notional,
                    result.lifecycle_status,
                    result.broker_order_id,
                )
                continue

            success_state = record_time_exit_success(current_state, now_ts=now)
            filled_qty = result.filled_notional / max(0.01, result.filled_price)
            remaining_notional = max(0.0, current_notional - result.filled_notional)
            remaining_qty = max(0.0, current_qty - filled_qty)
            position["notional"] = remaining_notional
            position["quantity"] = remaining_qty
            position["last_trim_ts"] = now
            position["price"] = result.filled_price
            position["last_signal_id"] = sig.signal_id
            self._set_time_exit_state(position, success_state)
            self._apply_position_exit_meta(
                position,
                exit_kind="time_exit",
                exit_label=time_exit_label,
                exit_summary=exit_summary,
                ts=now,
            )
            self._append_event(
                "time_exit_fill",
                {
                    "token_id": token_id,
                    "market_slug": sig.market_slug,
                    "trace_id": sig.trace_id,
                    "signal_id": sig.signal_id,
                    "side": sig.side,
                    "flow": "exit",
                    "position_action": sig.position_action,
                    "position_action_label": sig.position_action_label,
                    "exit_kind": "time_exit",
                    "exit_label": time_exit_label,
                    "exit_result": "partial_trim" if remaining_notional > close_notional and remaining_qty > 0.0 else "full_exit",
                    "exit_result_label": self._exit_result_label("partial_trim") if remaining_notional > close_notional and remaining_qty > 0.0 else self._exit_result_label("full_exit"),
                    "hold_minutes": self._position_hold_minutes(position, now),
                    "exit_fraction": float(exit_fraction),
                    "trim_notional": result.filled_notional,
                    "notional": result.filled_notional,
                    "remaining_notional": remaining_notional,
                    "order_id": result.broker_order_id or "",
                    "reason": "time_exit",
                    "cycle_id": cycle_id,
                    "wallet_score": 0.0,
                    "wallet_tier": "SYSTEM",
                    "time_exit_stage": str(success_state.stage),
                    "time_exit_failure_count": int(success_state.consecutive_failures),
                    "exit_priority": int(current_state.priority),
                    "exit_priority_reason": str(current_state.priority_reason or ""),
                    "market_volatility_bps": float(current_state.market_volatility_bps or 0.0),
                    "force_exit_active": bool(force_exit_active),
                },
            )

            self.recent_orders.appendleft(
                {
                    "ts": now,
                    "cycle_id": cycle_id,
                    "signal_id": sig.signal_id,
                    "trace_id": sig.trace_id,
                    "title": sig.market_slug,
                    "token_id": sig.token_id,
                    "outcome": sig.outcome,
                    "side": sig.side,
                    "status": "FILLED",
                    "retry_count": int(current_state.consecutive_failures),
                    "latency_ms": 0,
                    "reason": exit_summary,
                    "source_wallet": "system-time-exit",
                    "hold_minutes": self._position_hold_minutes(position, now),
                    "topic_label": str(position.get("entry_topic_label") or ""),
                    "notional": result.filled_notional,
                    "wallet_score": 0.0,
                    "wallet_tier": "SYSTEM",
                    "position_action": sig.position_action,
                    "position_action_label": sig.position_action_label,
                    "time_exit_stage": str(success_state.stage),
                    "time_exit_failure_count": int(success_state.consecutive_failures),
                    "exit_priority": int(current_state.priority),
                    "exit_priority_reason": str(current_state.priority_reason or ""),
                    "market_volatility_bps": float(current_state.market_volatility_bps or 0.0),
                    "force_exit_active": bool(force_exit_active),
                    **self._position_entry_context(position),
                    **self._exit_result_meta(
                        exit_kind="time_exit",
                        ok=True,
                        remaining_notional=remaining_notional,
                        remaining_qty=remaining_qty,
                    ),
                    **self._order_meta("SELL", exit_kind="time_exit", exit_label=time_exit_label, exit_summary=exit_summary),
                }
            )
            position_snapshot = self._position_trace_snapshot(
                position,
                is_open=not (remaining_notional <= close_notional or remaining_qty <= 0),
            )
            position_snapshot["notional"] = remaining_notional
            position_snapshot["quantity"] = remaining_qty
            signal_record["decision_snapshot"] = dict(base_decision_snapshot)
            signal_record["order_snapshot"] = self._order_trace_snapshot(dict(self.recent_orders[0]))
            signal_record["position_snapshot"] = position_snapshot
            signal_record["final_status"] = "filled"
            self._trace_record(
                trace_id=str(sig.trace_id or ""),
                signal=sig,
                cycle_id=cycle_id,
                signal_record=dict(signal_record),
                opened_ts=now,
            )
            self.log.info(
                "TIME_EXIT stage=%s priority=%d mode=%s slug=%s token=%s trim_notional=%.2f remain_notional=%.2f",
                current_state.stage,
                int(current_state.priority),
                "congested" if congested else "normal",
                sig.market_slug,
                sig.token_id,
                result.filled_notional,
                remaining_notional,
            )

            if remaining_notional <= close_notional or remaining_qty <= 0:
                del self.positions_book[token_id]
                self.state.open_positions = max(0, self.state.open_positions - 1)
                self._append_event(
                    "time_exit_close",
                    {
                        "token_id": token_id,
                        "market_slug": sig.market_slug,
                        "remaining_notional": remaining_notional,
                        "action": "position_closed",
                        "force_exit_active": bool(force_exit_active),
                    },
                )
                if self.settings.token_reentry_cooldown_seconds > 0:
                    self.token_reentry_until[token_id] = now + self.settings.token_reentry_cooldown_seconds
                self._mark_trace_closed(str(sig.trace_id or ""), now)
                self.log.info(
                    "TIME_EXIT_CLOSE stage=%s slug=%s token=%s open_positions=%d",
                    current_state.stage,
                    sig.market_slug,
                    sig.token_id,
                    self.state.open_positions,
                )

        if cycle_has_candidates:
            self.recent_signal_cycles.appendleft(cycle_record)

    def step(self) -> None:
        self._maybe_reconcile_runtime()
        self._roll_daily_state_if_needed()
        self._maybe_sync_account_state()
        self._enforce_hot_wallet_cap()
        control = self._load_control_state()
        self._apply_operator_clear_risk_breakers(control)
        cycle_start_ts = int(time.time())
        reconciliation = self.reconciliation_summary(now=cycle_start_ts)
        self._update_trading_mode(control, now=cycle_start_ts, reconciliation=reconciliation)
        self._advance_kill_switch_state(control, now=cycle_start_ts)
        self._refresh_buy_blocked_state(now_ts=cycle_start_ts)
        self._refresh_risk_state()
        self._apply_operator_clear_stale_pending(control)
        self._apply_control_pending_entry_cancels(control, now=cycle_start_ts)
        processed_sell_tokens: set[str] = set()
        if control.emergency_stop:
            self._apply_emergency_exit()
            self.last_signals = []
            self.log.warning(
                "EMERGENCY_STOP active, skip opening logic, open_positions=%d",
                self.state.open_positions,
            )
            self._append_event("emergency_stop_skip", {"reason": "control_flag"})
            return

        if bool(self._admission_decision.halted):
            self.last_wallets = []
            self.last_signals = []
            self.log.warning(
                "HALTED_MODE active, skip automatic trading actions reasons=%s",
                ",".join(self._admission_decision.reason_codes) or "none",
            )
            self._append_event(
                "admission_halted_skip",
                {
                    "reason_codes": list(self._admission_decision.reason_codes),
                    "action_whitelist": list(self._admission_decision.action_whitelist),
                },
            )
            return

        self._apply_time_exit()
        self._refresh_active_pending_candidates(now=int(time.time()))
        wallets = self._resolve_wallets()
        self.last_wallets = wallets
        fresh_signals: list[Signal] = []
        if wallets:
            self._update_strategy_history(wallets)
            self._sync_wallet_profiles(wallets)
            fresh_signals = self.strategy.generate_signals(wallets)
        else:
            self.log.warning("No wallets configured/resolved. fresh candidate generation skipped.")

        cycle_now = int(time.time())
        cycle_id = self._new_cycle_id(cycle_now)
        wallet_pool_snapshot = self._wallet_pool_snapshot()
        signal_records: list[dict[str, object]] = []
        signal_record_by_id: dict[str, dict[str, object]] = {}
        prepared_signals: list[Signal] = []
        fresh_candidates: list[Candidate] = []
        precheck_skipped = 0
        for sig in fresh_signals:
            existing = self._position_truth_for_token(sig.token_id)
            sig.signal_id = self._new_signal_id(cycle_now)
            sig.trace_id = self._trace_id_for_signal(sig, existing, cycle_now)
            action, action_label = self._position_action_for_signal(sig, existing)
            sig.position_action = action
            sig.position_action_label = action_label
            market_context = None
            if sig.side == "BUY":
                market_context = self._candidate_market_context(
                    sig.token_id,
                    price_hint=float(sig.price_hint or 0.0),
                    market_slug=str(sig.market_slug or ""),
                    condition_id=str(sig.condition_id or ""),
                )
                skip_reason = self._candidate_skip_reason(sig, market_context, existing=existing)
                if skip_reason in {
                    "market_data_unavailable",
                    "market_window_elapsed",
                    "market_near_close",
                    "market_closed",
                    "market_inactive",
                    "market_not_accepting_orders",
                }:
                    precheck_skipped += 1
                    self.log.info(
                        "DROP wallet=%s slug=%s token=%s reason=%s",
                        sig.wallet,
                        sig.market_slug,
                        sig.token_id,
                        skip_reason,
                    )
                    self._append_event(
                        "signal_precheck_skip",
                        {
                            "signal_id": str(sig.signal_id or ""),
                            "trace_id": str(sig.trace_id or ""),
                            "wallet": str(sig.wallet or ""),
                            "market_slug": str(sig.market_slug or ""),
                            "token_id": str(sig.token_id or ""),
                            "side": str(sig.side or ""),
                            "reason": skip_reason,
                        },
                    )
                    record = self._cycle_candidate_record(
                        sig,
                        cycle_id=cycle_id,
                        wallet_pool_snapshot=wallet_pool_snapshot,
                    )
                    decision_snapshot = self._decision_snapshot(
                        signal=sig,
                        control=control,
                        existing=existing,
                        skip_reason=skip_reason,
                        block_layer="candidate",
                    )
                    decision_snapshot["market_time_source"] = str(market_context.get("market_time_source") or "")
                    decision_snapshot["market_metadata_hit"] = bool(market_context.get("market_metadata_hit", False))
                    decision_snapshot["market_closed"] = market_context.get("market_closed")
                    decision_snapshot["market_active"] = market_context.get("market_active")
                    decision_snapshot["market_accepting_orders"] = market_context.get("market_accepting_orders")
                    record["decision_snapshot"] = decision_snapshot
                    record["final_status"] = "precheck_skipped"
                    signal_records.append(record)
                    signal_record_by_id[str(sig.signal_id)] = record
                    continue
            prepared_signals.append(sig)
            fresh_candidates.append(
                self._candidate_from_signal(
                    sig,
                    now=cycle_now,
                    existing=existing,
                    market_context=market_context,
                )
            )
            record = self._cycle_candidate_record(
                sig,
                cycle_id=cycle_id,
                wallet_pool_snapshot=wallet_pool_snapshot,
            )
            signal_records.append(record)
            signal_record_by_id[str(sig.signal_id)] = record
        self._persist_candidates(fresh_candidates)
        self._notify_candidates(fresh_candidates)
        self.last_signals = prepared_signals

        approved_plans = self._claim_approved_candidate_plans()
        auto_plans = self._auto_candidate_plans(fresh_candidates, prepared_signals, control.decision_mode)
        execution_plans = approved_plans + auto_plans

        for plan in execution_plans:
            sig = plan["signal"]
            signal_id = str(sig.signal_id or "")
            if signal_id and signal_id in signal_record_by_id:
                continue
            existing = self._position_truth_for_token(sig.token_id)
            if not str(sig.signal_id or "").strip():
                sig.signal_id = self._new_signal_id(cycle_now)
            if not str(sig.trace_id or "").strip():
                sig.trace_id = self._trace_id_for_signal(sig, existing, cycle_now)
            action, action_label = self._position_action_for_signal(sig, existing)
            sig.position_action = action
            sig.position_action_label = action_label
            record = self._cycle_candidate_record(
                sig,
                cycle_id=cycle_id,
                wallet_pool_snapshot=wallet_pool_snapshot,
            )
            signal_records.append(record)
            signal_record_by_id[str(sig.signal_id)] = record

        if signal_records:
            self.recent_signal_cycles.appendleft(
                {
                    "cycle_id": cycle_id,
                    "ts": cycle_now,
                    "wallets": list(wallets),
                    "wallet_pool_snapshot": list(wallet_pool_snapshot),
                    "decision_mode": str(control.decision_mode),
                    "candidates": signal_records,
                }
            )

        if not execution_plans:
            if prepared_signals:
                self.log.info(
                    "Queued %d fresh candidates for decision mode=%s",
                    len(prepared_signals),
                    control.decision_mode,
                )
            elif precheck_skipped > 0:
                self.log.info(
                    "Dropped %d fresh signals at market precheck; no tradable candidate this cycle",
                    precheck_skipped,
                )
            else:
                self.log.info("No actionable signal or approved candidate this cycle")
            return

        execution_meta_by_signal_id: dict[str, dict[str, object]] = {}
        executable_signals: list[Signal] = []
        for plan in execution_plans:
            sig = plan["signal"]
            execution_meta_by_signal_id[str(sig.signal_id or "")] = dict(plan)
            executable_signals.append(sig)

        def finalize_signal(
            sig: Signal,
            *,
            final_status: str,
            decision_snapshot: dict[str, object],
            order_snapshot: dict[str, object] | None = None,
            position_snapshot: dict[str, object] | None = None,
            now_ts: int,
        ) -> None:
            record = signal_record_by_id.get(str(sig.signal_id))
            if record is None:
                return
            record["topic_snapshot"] = self._topic_snapshot(sig)
            record["decision_snapshot"] = dict(decision_snapshot)
            record["order_snapshot"] = dict(order_snapshot or {})
            record["position_snapshot"] = dict(position_snapshot or {})
            record["final_status"] = final_status
            self._trace_record(
                trace_id=str(sig.trace_id or ""),
                signal=sig,
                cycle_id=cycle_id,
                signal_record=dict(record),
                opened_ts=now_ts,
            )

        for sig in executable_signals:
            execution_meta = execution_meta_by_signal_id.get(str(sig.signal_id or ""), {})
            candidate_id = str(execution_meta.get("candidate_id") or "")
            candidate_action = str(execution_meta.get("candidate_action") or "")
            candidate_note = str(execution_meta.get("candidate_note") or "")
            candidate_origin = str(execution_meta.get("origin") or "runtime")
            order_meta = self._signal_order_meta(sig)
            existing = self._position_truth_for_token(sig.token_id)
            now = int(time.time())
            if candidate_id:
                expired_candidate = self._expire_candidate_record(
                    candidate_id,
                    now_ts=now,
                    block_layer="execution_precheck",
                    note="candidate_lifecycle:execution_precheck_expired",
                )
                if expired_candidate is not None:
                    self._append_event(
                        "signal_skip",
                        {
                            "wallet": sig.wallet,
                            "market_slug": sig.market_slug,
                            "token_id": sig.token_id,
                            "reason": REASON_CANDIDATE_LIFETIME_EXPIRED,
                        },
                    )
                    finalize_signal(
                        sig,
                        final_status="skipped",
                        decision_snapshot=self._decision_snapshot(
                            signal=sig,
                            control=control,
                            existing=existing,
                            candidate_id=candidate_id,
                            candidate_action=candidate_action,
                            candidate_origin=candidate_origin,
                            skip_reason=REASON_CANDIDATE_LIFETIME_EXPIRED,
                            block_layer="execution_precheck",
                        ),
                        now_ts=now,
                    )
                    self._record_candidate_result(
                        candidate_id,
                        action=candidate_action or "ignore",
                        status="expired",
                        rationale=candidate_note or REASON_CANDIDATE_LIFETIME_EXPIRED,
                        result_tag=REASON_CANDIDATE_LIFETIME_EXPIRED,
                        signal=sig,
                    )
                    continue
            opening_block_reason = self._buy_gate_reason() if sig.side == "BUY" else ""
            if opening_block_reason:
                self.log.info(
                    "SKIP wallet=%s slug=%s reason=%s trading_mode=%s",
                    sig.wallet,
                    sig.market_slug,
                    opening_block_reason,
                    self.trading_mode,
                )
                self._append_event(
                    "signal_skip",
                    {
                        "wallet": sig.wallet,
                        "market_slug": sig.market_slug,
                        "token_id": sig.token_id,
                        "reason": opening_block_reason,
                        "trading_mode": self.trading_mode_state(),
                    },
                )
                finalize_signal(
                    sig,
                    final_status="skipped",
                    decision_snapshot=self._decision_snapshot(
                        signal=sig,
                        control=control,
                        existing=existing,
                        skip_reason=opening_block_reason,
                        block_layer="execution_precheck",
                    ),
                    now_ts=int(time.time()),
                )
                if candidate_id:
                    self._record_candidate_result(
                        candidate_id,
                        action=candidate_action or "ignore",
                        status="skipped",
                        rationale=candidate_note or opening_block_reason,
                        result_tag=opening_block_reason,
                        signal=sig,
                    )
                continue

            now = int(time.time())
            cooldown_until = int(self.token_reentry_until.get(sig.token_id, 0))
            if cooldown_until > 0 and cooldown_until <= now:
                del self.token_reentry_until[sig.token_id]
                cooldown_until = 0
            if (
                sig.side == "BUY"
                and sig.token_id not in self.positions_book
                and cooldown_until > now
            ):
                self.log.info(
                    "SKIP wallet=%s slug=%s reason=token reentry cooldown %ds",
                    sig.wallet,
                    sig.market_slug,
                    cooldown_until - now,
                )
                self._append_event(
                    "signal_skip",
                    {
                        "wallet": sig.wallet,
                        "market_slug": sig.market_slug,
                        "token_id": sig.token_id,
                        "reason": "token_reentry_cooldown",
                        "remaining_seconds": cooldown_until - now,
                    },
                )
                finalize_signal(
                    sig,
                    final_status="skipped",
                    decision_snapshot=self._decision_snapshot(
                        signal=sig,
                        control=control,
                        existing=existing,
                        skip_reason="token_reentry_cooldown",
                        cooldown_remaining=cooldown_until - now,
                        block_layer="execution_precheck",
                    ),
                    now_ts=now,
                )
                if candidate_id:
                    self._record_candidate_result(
                        candidate_id,
                        action=candidate_action or "ignore",
                        status="skipped",
                        rationale=candidate_note or "token_reentry_cooldown",
                        result_tag="token_reentry_cooldown",
                        signal=sig,
                    )
                continue

            existing = self._position_truth_for_token(sig.token_id)
            if sig.side == "BUY":
                repeat_entry_reason = self._repeat_entry_block_reason(sig, existing)
                if repeat_entry_reason:
                    self._append_event(
                        "signal_skip",
                        {
                            "wallet": sig.wallet,
                            "market_slug": sig.market_slug,
                            "token_id": sig.token_id,
                            "reason": repeat_entry_reason,
                        },
                    )
                    finalize_signal(
                        sig,
                        final_status="skipped",
                        decision_snapshot=self._decision_snapshot(
                            signal=sig,
                            control=control,
                            existing=existing,
                            skip_reason=repeat_entry_reason,
                            block_layer="execution_precheck",
                        ),
                        now_ts=now,
                    )
                    if candidate_id:
                        self._record_candidate_result(
                            candidate_id,
                            action=candidate_action or "ignore",
                            status="skipped",
                            rationale=candidate_note or repeat_entry_reason,
                            result_tag=repeat_entry_reason,
                            signal=sig,
                        )
                    continue
            if sig.side == "SELL":
                if sig.token_id in processed_sell_tokens:
                    self._append_event(
                        "signal_skip",
                        {
                            "wallet": sig.wallet,
                            "market_slug": sig.market_slug,
                            "token_id": sig.token_id,
                            "reason": "sell_already_processed_this_cycle",
                        },
                    )
                    finalize_signal(
                        sig,
                        final_status="skipped",
                        decision_snapshot=self._decision_snapshot(
                            signal=sig,
                            control=control,
                            existing=existing,
                            skip_reason="sell_already_processed_this_cycle",
                            block_layer="execution_precheck",
                        ),
                        now_ts=now,
                    )
                    if candidate_id:
                        self._record_candidate_result(
                            candidate_id,
                            action=candidate_action or "ignore",
                            status="skipped",
                            rationale=candidate_note or "sell_already_processed_this_cycle",
                            result_tag="sell_already_processed_this_cycle",
                            signal=sig,
                        )
                    continue
                if existing is None:
                    self._append_event(
                        "signal_skip",
                        {
                            "wallet": sig.wallet,
                            "market_slug": sig.market_slug,
                            "token_id": sig.token_id,
                            "reason": "no_open_position",
                        },
                    )
                    finalize_signal(
                        sig,
                        final_status="skipped",
                        decision_snapshot=self._decision_snapshot(
                            signal=sig,
                            control=control,
                            existing=existing,
                            skip_reason="no_open_position",
                            block_layer="execution_precheck",
                        ),
                        now_ts=now,
                    )
                    if candidate_id:
                        self._record_candidate_result(
                            candidate_id,
                            action=candidate_action or "ignore",
                            status="skipped",
                            rationale=candidate_note or "no_open_position",
                            result_tag="no_open_position",
                            signal=sig,
                        )
                    continue
                entry_wallet = str(existing.get("entry_wallet") or "").strip().lower()
                if (
                    entry_wallet
                    and entry_wallet != str(sig.wallet or "").strip().lower()
                    and not bool(sig.cross_wallet_exit)
                ):
                    self._append_event(
                        "signal_skip",
                        {
                            "wallet": sig.wallet,
                            "market_slug": sig.market_slug,
                            "token_id": sig.token_id,
                            "reason": "entry_wallet_mismatch",
                            "entry_wallet": entry_wallet,
                        },
                    )
                    finalize_signal(
                        sig,
                        final_status="skipped",
                        decision_snapshot=self._decision_snapshot(
                            signal=sig,
                            control=control,
                            existing=existing,
                            skip_reason="entry_wallet_mismatch",
                            block_layer="execution_precheck",
                        ),
                        now_ts=now,
                    )
                    if candidate_id:
                        self._record_candidate_result(
                            candidate_id,
                            action=candidate_action or "ignore",
                            status="skipped",
                            rationale=candidate_note or "entry_wallet_mismatch",
                            result_tag="entry_wallet_mismatch",
                            signal=sig,
                        )
                    continue
            if sig.side == "BUY" and existing is not None and self.settings.token_add_cooldown_seconds > 0:
                last_buy_ts = int(existing.get("last_buy_ts") or existing.get("opened_ts") or 0)
                remain = self.settings.token_add_cooldown_seconds - (now - last_buy_ts)
                if remain > 0:
                    self.log.info(
                        "SKIP wallet=%s slug=%s reason=token add cooldown %ds",
                        sig.wallet,
                        sig.market_slug,
                        remain,
                    )
                    self._append_event(
                        "signal_skip",
                        {
                            "wallet": sig.wallet,
                            "market_slug": sig.market_slug,
                            "token_id": sig.token_id,
                            "reason": "token_add_cooldown",
                            "remaining_seconds": remain,
                        },
                    )
                    finalize_signal(
                        sig,
                        final_status="skipped",
                        decision_snapshot=self._decision_snapshot(
                            signal=sig,
                            control=control,
                            existing=existing,
                            skip_reason="token_add_cooldown",
                            add_cooldown_remaining=remain,
                            block_layer="execution_precheck",
                        ),
                        now_ts=now,
                    )
                    if candidate_id:
                        self._record_candidate_result(
                            candidate_id,
                            action=candidate_action or "ignore",
                            status="skipped",
                            rationale=candidate_note or "token_add_cooldown",
                            result_tag="token_add_cooldown",
                            signal=sig,
                        )
                    continue

            self._refresh_risk_state()
            self._hydrate_signal_condition_exposure(sig)
            decision = self.risk.evaluate(sig, self.state)
            if not decision.allowed:
                self.log.info(
                    "SKIP wallet=%s slug=%s reason=%s",
                    sig.wallet,
                    sig.market_slug,
                    decision.reason,
                )
                self._append_event(
                    "signal_skip",
                    {
                        "wallet": sig.wallet,
                        "market_slug": sig.market_slug,
                        "token_id": sig.token_id,
                        "reason": decision.reason,
                    },
                )
                finalize_signal(
                    sig,
                    final_status="risk_rejected",
                    decision_snapshot=self._decision_snapshot(
                        signal=sig,
                        control=control,
                        existing=existing,
                        decision=decision,
                        skip_reason=decision.reason,
                    ),
                    now_ts=now,
                )
                if candidate_id:
                    self._record_candidate_result(
                        candidate_id,
                        action=candidate_action or "ignore",
                        status="risk_rejected",
                        rationale=candidate_note or str(decision.reason or ""),
                        result_tag=str(decision.reason or "risk_rejected"),
                        signal=sig,
                    )
                continue

            sized_notional = self._apply_wallet_score_sizing(sig, decision.max_notional)
            if sized_notional <= 0.0:
                self.log.info(
                    "SKIP wallet=%s slug=%s reason=wallet_score_gate score=%.1f tier=%s",
                    sig.wallet,
                    sig.market_slug,
                    sig.wallet_score,
                    sig.wallet_tier,
                )
                finalize_signal(
                    sig,
                    final_status="skipped",
                    decision_snapshot=self._decision_snapshot(
                        signal=sig,
                        control=control,
                        existing=existing,
                        decision=decision,
                        skip_reason="wallet_score_gate",
                        sized_notional=sized_notional,
                    ),
                    now_ts=now,
                )
                if candidate_id:
                    self._record_candidate_result(
                        candidate_id,
                        action=candidate_action or "ignore",
                        status="skipped",
                        rationale=candidate_note or "wallet_score_gate",
                        result_tag="wallet_score_gate",
                        signal=sig,
                    )
                continue

            sized_notional = self._apply_topic_profile_sizing(sig, sized_notional)
            netting_snapshot: dict[str, object] = {}
            netting_notional = sized_notional
            if sig.side == "SELL":
                notional_to_use = self._sell_target_notional(sig, existing or {}, sized_notional)
            else:
                netting_notional, netting_snapshot = self._enforce_condition_netting(sig, sized_notional)
                notional_to_use = self._enforce_buy_budget(sig, netting_notional)
            if self._should_apply_candidate_action_sizing(candidate_origin, candidate_action):
                notional_to_use = self._apply_candidate_action_sizing(
                    signal=sig,
                    action=candidate_action,
                    requested_notional=notional_to_use,
                    existing=existing,
                )
            netting_limited = sig.side == "BUY" and netting_notional + 1e-9 < sized_notional
            budget_limited = sig.side == "BUY" and notional_to_use + 1e-9 < netting_notional
            if notional_to_use <= 0.0:
                self.log.info(
                    "SKIP wallet=%s slug=%s reason=insufficient_budget",
                    sig.wallet,
                    sig.market_slug,
                )
                finalize_signal(
                    sig,
                    final_status="skipped",
                    decision_snapshot=self._decision_snapshot(
                        signal=sig,
                        control=control,
                        existing=existing,
                        decision=decision,
                        skip_reason="insufficient_budget",
                        sized_notional=sized_notional,
                        final_notional=notional_to_use,
                        budget_limited=budget_limited,
                        netting_limited=netting_limited,
                        netting_snapshot=netting_snapshot,
                    ),
                    now_ts=now,
                )
                if candidate_id:
                    self._record_candidate_result(
                        candidate_id,
                        action=candidate_action or "ignore",
                        status="skipped",
                        rationale=candidate_note or "insufficient_budget",
                        result_tag="insufficient_budget",
                        signal=sig,
                    )
                continue

            duplicate_reason = ""
            duplicate_context: dict[str, object] = {}
            identity: dict[str, object] = self._build_intent_identity(sig, notional_to_use) if sig.side == "BUY" else {}
            strategy_uuid: str | None = str(identity.get("strategy_order_uuid") or "") if sig.side == "BUY" else None
            idempotency_key: str = str(identity.get("idempotency_key") or "") if sig.side == "BUY" else ""
            claim_status = None
            intent_status = INTENT_STATUS_NEW
            intent_record: dict[str, object] = {}
            recent_rate_limited = False
            if sig.side == "BUY" and not duplicate_reason:
                claim_status, intent_record = self._claim_or_load_intent(
                    signal=sig,
                    notional_usd=notional_to_use,
                    identity=identity,
                )
                intent_status = normalize_intent_status(intent_record.get("status") or INTENT_STATUS_NEW)
                ack_count = int(intent_record.get("ack_unknown_count") or 0)
                ack_first_ts = int(intent_record.get("ack_unknown_first_ts") or 0)
                if ack_count > self._ack_unknown_max_probes() and intent_status != INTENT_STATUS_MANUAL_REQUIRED:
                    intent_status = INTENT_STATUS_MANUAL_REQUIRED
                    intent_record["status"] = intent_status
                duplicate_context.update(
                    {
                        "strategy_order_uuid": strategy_uuid or "",
                        "idempotency_key": idempotency_key,
                        "intent_status": intent_status,
                        "claim_status": claim_status,
                        "ack_unknown_count": ack_count,
                        "ack_unknown_first_ts": ack_first_ts,
                    }
                )
                if claim_status == STORAGE_ERROR:
                    duplicate_reason = "intent_storage_error"
                elif claim_status == EXISTING_TERMINAL and intent_status != INTENT_STATUS_NEW:
                    duplicate_reason = "intent_existing_terminal"
                elif claim_status == EXISTING_NON_TERMINAL:
                    if intent_status in {INTENT_STATUS_SENDING, INTENT_STATUS_ACK_UNKNOWN}:
                        # SENDING/ACK_UNKNOWN 只允许恢复探测，不允许直接重发。
                        probe = self._classify_unknown_submit_probe(signal=sig, intent_record=intent_record)
                        ack_state = self._record_ack_unknown_probe(
                            str(strategy_uuid or ""),
                            current_count=ack_count,
                            current_first_ts=ack_first_ts,
                        )
                        probe_confidence = self._normalize_probe_confidence(probe.get("confidence"))
                        probe_basis = self._normalize_probe_basis(probe.get("basis"))
                        if probe_confidence == "strong":
                            recovered_status = str(probe.get("intent_status") or INTENT_STATUS_ACKED_PENDING)
                            recovered_broker_order_id = str(probe.get("broker_order_id") or "")
                            recovered_broker_status = str(probe.get("broker_status") or "")
                            self._set_intent_status(
                                strategy_order_uuid=str(strategy_uuid or ""),
                                idempotency_key=idempotency_key,
                                status=recovered_status,
                                broker_order_id=recovered_broker_order_id,
                                payload_updates={
                                    **self._build_submit_unknown_payload_updates(
                                        payload=dict(intent_record.get("payload") or {}),
                                        ack_state=ack_state,
                                        probe_confidence=probe_confidence,
                                        probe_basis=probe_basis,
                                    ),
                                    "pending_class": "normal",
                                    "manual_required_reason": "",
                                    "reconcile_ambiguous_ts": 0,
                                    "reconcile_ambiguous_reason": "",
                                    "probe_source": probe_basis,
                                    "probe_ts": int(time.time()),
                                },
                                recovery_reason="submit_unknown_probe_recovered",
                            )
                            duplicate_reason = "intent_recovery_pending"
                            duplicate_context.update(
                                {
                                    "probe_confidence": probe_confidence,
                                    "probe_basis": probe_basis,
                                    "probe_broker_order_id": recovered_broker_order_id,
                                    "probe_broker_status": recovered_broker_status,
                                }
                            )
                            pending_order = self._find_pending_order_by_intent(
                                strategy_order_uuid=str(strategy_uuid or ""),
                                idempotency_key=idempotency_key,
                            )
                            if pending_order is not None:
                                self._apply_submit_unknown_contract(
                                    pending_order,
                                    now=int(time.time()),
                                    probe_confidence=probe_confidence,
                                    probe_basis=probe_basis,
                                    manual_required_reason="",
                                    ack_state=ack_state,
                                    clear_ambiguity=True,
                                    broker_order_id=recovered_broker_order_id,
                                    broker_status=recovered_broker_status,
                                    payload=self._build_submit_unknown_payload_updates(
                                        payload=dict(intent_record.get("payload") or {}),
                                        ack_state=ack_state,
                                        probe_confidence=probe_confidence,
                                        probe_basis=probe_basis,
                                    ),
                                )
                        else:
                            manual_required_reason = str(probe.get("manual_required_reason") or "")
                            if probe_confidence == "none":
                                manual_required_reason = manual_required_reason or "submit_unknown_no_anchor"
                            if not manual_required_reason and bool(ack_state.get("manual_required")):
                                manual_required_reason = "submit_unknown_probe_exhausted"
                            probe_status = (
                                INTENT_STATUS_MANUAL_REQUIRED
                                if manual_required_reason
                                else INTENT_STATUS_ACK_UNKNOWN
                            )
                            payload_updates = self._build_submit_unknown_payload_updates(
                                payload=dict(intent_record.get("payload") or {}),
                                ack_state=ack_state,
                                probe_confidence=probe_confidence,
                                probe_basis=probe_basis,
                                manual_required_reason=manual_required_reason,
                            ) | {
                                "pending_class": "submit_unknown",
                                "reconcile_ambiguous_ts": int(time.time()),
                                "reconcile_ambiguous_reason": manual_required_reason or "submit_unknown",
                            }
                            self._set_intent_status(
                                strategy_order_uuid=str(strategy_uuid or ""),
                                idempotency_key=idempotency_key,
                                status=probe_status,
                                payload_updates=payload_updates | {"last_probe_ts": int(time.time())},
                                recovery_reason="ack_unknown_probe_without_broker_evidence",
                            )
                            pending_order = self._find_pending_order_by_intent(
                                strategy_order_uuid=str(strategy_uuid or ""),
                                idempotency_key=idempotency_key,
                            )
                            if pending_order is not None:
                                self._apply_submit_unknown_contract(
                                    pending_order,
                                    now=int(time.time()),
                                    probe_confidence=probe_confidence,
                                    probe_basis=probe_basis,
                                    manual_required_reason=manual_required_reason,
                                    ack_state=ack_state,
                                    payload=payload_updates,
                                )
                            duplicate_reason = (
                                "intent_manual_required"
                                if probe_status == INTENT_STATUS_MANUAL_REQUIRED
                                else "intent_ack_unknown"
                            )
                            duplicate_context.update(
                                {
                                    "probe_confidence": probe_confidence,
                                    "probe_basis": probe_basis,
                                    "manual_required_reason": manual_required_reason,
                                }
                            )
                    elif intent_status == INTENT_STATUS_MANUAL_REQUIRED:
                        duplicate_reason = "intent_manual_required"
                    elif intent_status in NON_TERMINAL_STATUSES and intent_status != INTENT_STATUS_NEW:
                        duplicate_reason = "intent_existing_non_terminal"

                if not duplicate_reason:
                    pending_duplicate = self._find_pending_order_duplicate(sig)
                    if pending_duplicate is not None:
                        duplicate_reason = "pending_order_duplicate"
                        duplicate_context.update(
                            {
                                "duplicate_source": str(pending_duplicate.get("recovery_source") or "pending_orders"),
                                "duplicate_order_id": str(pending_duplicate.get("order_id") or ""),
                                "duplicate_key": str(pending_duplicate.get("key") or ""),
                                "duplicate_recovery_status": str(pending_duplicate.get("recovery_status") or ""),
                            }
                        )

                if not duplicate_reason:
                    broker_open_order_duplicate = self._find_broker_open_order_duplicate(sig)
                    if broker_open_order_duplicate is not None:
                        duplicate_reason = "broker_open_order_duplicate"
                        duplicate_context.update(dict(broker_open_order_duplicate))

                recent_rate_limited = self._is_order_duplicate(sig, notional_to_use)
                duplicate_context["recent_rate_limited"] = bool(recent_rate_limited)
                if recent_rate_limited:
                    duplicate_context["rate_limit_ttl_seconds"] = int(self.settings.order_dedup_ttl_seconds)

                if not duplicate_reason:
                    # NEW(无论 CLAIMED_NEW 还是 EXISTING_NON_TERMINAL) 进入发送临界区前必须 CAS 到 SENDING。
                    marked_sending, marked_payload = self._set_intent_status(
                        strategy_order_uuid=str(strategy_uuid or ""),
                        idempotency_key=idempotency_key,
                        status=INTENT_STATUS_SENDING,
                        expected_from_statuses=(INTENT_STATUS_NEW,),
                        payload_updates={
                            "requested_notional": float(notional_to_use),
                            "requested_price": float(sig.price_hint or 0.0),
                            "signal_id": str(sig.signal_id or ""),
                            "trace_id": str(sig.trace_id or ""),
                        },
                        recovery_reason="sending_enter_critical_section",
                    )
                    if not marked_sending:
                        duplicate_reason = "intent_claim_race"
                    elif marked_payload:
                        intent_record = dict(marked_payload)
                        intent_status = normalize_intent_status(intent_record.get("status") or intent_status)

            if duplicate_reason:
                self._append_event(
                    "order_duplicate",
                    {
                        "wallet": sig.wallet,
                        "market_slug": sig.market_slug,
                        "token_id": sig.token_id,
                        "side": sig.side,
                        "notional": notional_to_use,
                        "wallet_score": sig.wallet_score,
                        "wallet_tier": sig.wallet_tier,
                        "reason": duplicate_reason,
                        "strategy_order_uuid": strategy_uuid or "",
                        "idempotency_key": idempotency_key,
                        **duplicate_context,
                    },
                )
                self.log.info(
                    "SKIP_DUP wallet=%s slug=%s side=%s notional=%.2f reason=%s",
                    sig.wallet,
                    sig.market_slug,
                    sig.side,
                    notional_to_use,
                    duplicate_reason,
                )
                finalize_signal(
                    sig,
                    final_status="duplicate_skipped",
                    decision_snapshot=self._decision_snapshot(
                        signal=sig,
                        control=control,
                        existing=existing,
                        decision=decision,
                        sized_notional=sized_notional,
                        final_notional=notional_to_use,
                        budget_limited=budget_limited,
                        netting_limited=netting_limited,
                        netting_snapshot=netting_snapshot,
                        duplicate=True,
                    )
                    | {
                        "duplicate_reason": duplicate_reason,
                        "duplicate_context": dict(duplicate_context),
                    },
                    now_ts=now,
                )
                if candidate_id:
                    self._record_candidate_result(
                        candidate_id,
                        action=candidate_action or "ignore",
                        status="duplicate_skipped",
                        rationale=candidate_note or duplicate_reason,
                        result_tag=duplicate_reason,
                        signal=sig,
                    )
                continue

            order_meta["strategy_order_uuid"] = strategy_uuid or ""
            order_meta["idempotency_key"] = idempotency_key
            if sig.side == "BUY":
                order_meta["strategy_name"] = str(identity.get("strategy_name") or "")
                order_meta["signal_source"] = str(identity.get("signal_source") or "")
                order_meta["signal_fingerprint"] = str(identity.get("signal_fingerprint") or "")
            result = self.broker.execute(sig, notional_to_use, strategy_order_uuid=order_meta["strategy_order_uuid"])
            market_context = self._market_context_from_result(result)
            ack_unknown = self._is_ack_unknown_result(result)
            if ack_unknown:
                now = int(time.time())
                ack_state = self._record_ack_unknown_probe(
                    order_meta["strategy_order_uuid"],
                    current_count=int(intent_record.get("ack_unknown_count") or 0),
                    current_first_ts=int(intent_record.get("ack_unknown_first_ts") or 0),
                )
                ack_metadata = dict(getattr(result, "metadata", {}) or {})
                probe_confidence = "weak" if str(ack_metadata.get("submit_digest") or "").strip() else "none"
                probe_basis = "submit_digest_only" if probe_confidence == "weak" else "no_match"
                manual_required_reason = "submit_unknown_no_anchor" if probe_confidence == "none" else ""
                if bool(ack_state.get("manual_required")) and not manual_required_reason:
                    manual_required_reason = "submit_unknown_probe_exhausted"
                ack_status = INTENT_STATUS_MANUAL_REQUIRED if manual_required_reason else INTENT_STATUS_ACK_UNKNOWN
                payload_updates = self._build_submit_unknown_payload_updates(
                    payload=dict(intent_record.get("payload") or {}),
                    result=result,
                    ack_state=ack_state,
                    probe_confidence=probe_confidence,
                    probe_basis=probe_basis,
                    manual_required_reason=manual_required_reason,
                ) | {
                    "pending_class": "submit_unknown",
                    "reconcile_ambiguous_ts": now,
                    "reconcile_ambiguous_reason": manual_required_reason or "broker_ack_unknown",
                }
                self._set_intent_status(
                    strategy_order_uuid=order_meta["strategy_order_uuid"],
                    idempotency_key=idempotency_key,
                    status=ack_status,
                    broker_order_id=str(result.broker_order_id or ""),
                    payload_updates=payload_updates,
                    recovery_reason="broker_ack_unknown",
                )
                entry_context = (
                    self._position_entry_context(existing)
                    if sig.side == "SELL"
                    else self._signal_entry_context(sig, self._order_reason(sig, result.message))
                )
                ack_result = ExecutionResult(
                    ok=True,
                    broker_order_id=result.broker_order_id,
                    message=result.message,
                    filled_notional=0.0,
                    filled_price=0.0,
                    status=ack_status,
                    requested_notional=result.requested_notional or notional_to_use,
                    requested_price=result.requested_price or 0.0,
                    metadata=dict(getattr(result, "metadata", {}) or {})
                    | {
                        "pending_class": "submit_unknown",
                        "probe_confidence": probe_confidence,
                        "probe_basis": probe_basis,
                        "manual_required_reason": manual_required_reason,
                        "unknown_submit_first_seen_ts": int(ack_state.get("first_ts") or 0),
                        "unknown_submit_probe_count": int(ack_state.get("count") or 0),
                    },
                )
                pending_record = self._register_pending_order(
                    signal=sig,
                    cycle_id=cycle_id,
                    result=ack_result,
                    order_meta=order_meta,
                    entry_context=entry_context,
                    previous_position=existing,
                    order_reason=self._order_reason(sig, result.message),
                    now=now,
                )
                self._apply_submit_unknown_contract(
                    pending_record,
                    now=now,
                    probe_confidence=probe_confidence,
                    probe_basis=probe_basis,
                    manual_required_reason=manual_required_reason,
                    ack_state=ack_state,
                    payload=payload_updates,
                )
                self._append_event(
                    "order_ack_unknown",
                    {
                        "wallet": sig.wallet,
                        "market_slug": sig.market_slug,
                        "token_id": sig.token_id,
                        "side": sig.side,
                        "notional": notional_to_use,
                        "strategy_order_uuid": order_meta.get("strategy_order_uuid", ""),
                        "ack_unknown_count": ack_state.get("count"),
                        "ack_unknown_first_ts": ack_state.get("first_ts"),
                        "intent_status": ack_status,
                        "probe_confidence": probe_confidence,
                        "probe_basis": probe_basis,
                        "manual_required_reason": manual_required_reason,
                        **market_context,
                    },
                )
                order_record = self._order_trace_snapshot(self._pending_order_snapshot(pending_record))
                final_tag = "manual_required" if ack_status == INTENT_STATUS_MANUAL_REQUIRED else "ack_unknown"
                finalize_signal(
                    sig,
                    final_status="manual_required" if ack_status == INTENT_STATUS_MANUAL_REQUIRED else "order_ack_unknown",
                    decision_snapshot=self._decision_snapshot(
                        signal=sig,
                        control=control,
                        existing=existing,
                        decision=decision,
                        sized_notional=sized_notional,
                        final_notional=notional_to_use,
                        budget_limited=budget_limited,
                        netting_limited=netting_limited,
                        netting_snapshot=netting_snapshot,
                    ),
                    order_snapshot=order_record,
                    now_ts=now,
                )
                if candidate_id:
                    self._record_candidate_result(
                        candidate_id,
                        action=candidate_action or "follow",
                        status="manual_required" if ack_status == INTENT_STATUS_MANUAL_REQUIRED else "submitted",
                        rationale=candidate_note or result.message,
                        result_tag=final_tag,
                        signal=sig,
                    )
                continue

            if result.ok:
                now = int(time.time())
                existing = self._position_truth_for_token(sig.token_id)
                order_reason = self._order_reason(sig, result.message)
                if result.is_pending:
                    entry_context = (
                        self._position_entry_context(existing)
                        if sig.side == "SELL"
                        else self._signal_entry_context(sig, order_reason)
                    )
                    pending_record = self._register_pending_order(
                        signal=sig,
                        cycle_id=cycle_id,
                        result=result,
                        order_meta=order_meta,
                        entry_context=entry_context,
                        previous_position=existing,
                        order_reason=order_reason,
                        now=now,
                    )
                    self._set_intent_status(
                        strategy_order_uuid=order_meta["strategy_order_uuid"],
                        idempotency_key=idempotency_key,
                        status=INTENT_STATUS_ACKED_PENDING,
                        broker_order_id=str(result.broker_order_id or ""),
                        recovery_reason="broker_ack_pending",
                    )
                    exit_result_meta = self._pending_exit_result_meta(sig.side)
                    self._append_event(
                        "order_posted",
                        {
                            "wallet": sig.wallet,
                            "source_wallet": sig.wallet,
                            "market_slug": sig.market_slug,
                            "token_id": sig.token_id,
                            "cycle_id": cycle_id,
                            "signal_id": sig.signal_id,
                            "trace_id": sig.trace_id,
                            "side": sig.side,
                            "flow": str(order_meta.get("flow") or ("exit" if sig.side == "SELL" else "entry")),
                            "position_action": sig.position_action,
                            "position_action_label": sig.position_action_label,
                            "requested_notional": result.requested_notional,
                            "requested_price": result.requested_price,
                            "order_id": result.broker_order_id or "",
                            "broker_status": result.lifecycle_status,
                            "reason": result.message,
                            "wallet_score": sig.wallet_score,
                            "wallet_tier": sig.wallet_tier,
                            "topic_key": sig.topic_key,
                            "topic_label": sig.topic_label,
                            "topic_bias": sig.topic_bias,
                            "topic_multiplier": sig.topic_multiplier,
                            "exit_fraction": float(sig.exit_fraction or 0.0),
                            "cross_wallet_exit": bool(sig.cross_wallet_exit),
                            "exit_wallet_count": int(sig.exit_wallet_count or 0),
                            **market_context,
                        },
                    )
                    self.recent_orders.appendleft(
                        {
                            "ts": now,
                            "cycle_id": cycle_id,
                            "signal_id": sig.signal_id,
                            "trace_id": sig.trace_id,
                            "title": sig.market_slug,
                            "token_id": sig.token_id,
                            "outcome": sig.outcome,
                            "side": sig.side,
                            "status": self._recent_order_status(result),
                            "order_id": result.broker_order_id or "",
                            "broker_status": result.lifecycle_status,
                            "retry_count": 0,
                            "latency_ms": 0,
                            "reason": order_reason,
                            "source_wallet": sig.wallet,
                            "hold_minutes": self._position_hold_minutes(existing, now) if sig.side == "SELL" else 0,
                            "notional": result.requested_notional,
                            "wallet_score": sig.wallet_score,
                            "wallet_tier": sig.wallet_tier,
                            "position_action": sig.position_action,
                            "position_action_label": sig.position_action_label,
                            "topic_label": sig.topic_label,
                            "topic_bias": sig.topic_bias,
                            "topic_multiplier": sig.topic_multiplier,
                            **entry_context,
                            **exit_result_meta,
                            **order_meta,
                            **self._pending_order_snapshot(pending_record),
                            **market_context,
                        }
                    )
                    order_record = dict(self.recent_orders[0])
                    finalize_signal(
                        sig,
                        final_status="order_pending",
                        decision_snapshot=self._decision_snapshot(
                            signal=sig,
                            control=control,
                            existing=existing,
                            decision=decision,
                            sized_notional=sized_notional,
                            final_notional=notional_to_use,
                            budget_limited=budget_limited,
                            netting_limited=netting_limited,
                            netting_snapshot=netting_snapshot,
                        ),
                        order_snapshot=self._order_trace_snapshot(order_record),
                        position_snapshot=self._position_trace_snapshot(existing, is_open=bool(existing)),
                        now_ts=now,
                    )
                    self._refresh_account_state_after_order(result)
                    self.log.info(
                        "POSTED wallet=%s slug=%s token=%s side=%s status=%s notional=%.2f order_id=%s msg=%s",
                        sig.wallet,
                        sig.market_slug,
                        sig.token_id,
                        sig.side,
                        result.lifecycle_status,
                        result.requested_notional,
                        result.broker_order_id,
                        result.message,
                    )
                    if candidate_id:
                        self._record_candidate_result(
                            candidate_id,
                            action=candidate_action or "follow",
                            status="submitted",
                            rationale=candidate_note or order_reason,
                            result_tag="order_pending",
                            signal=sig,
                        )
                    continue

                qty = result.filled_notional / max(0.01, result.filled_price)
                position_action = sig.position_action
                position_action_label = sig.position_action_label
                filled_status = INTENT_STATUS_PARTIAL if result.lifecycle_status == "partially_filled" else INTENT_STATUS_FILLED
                self._set_intent_status(
                    strategy_order_uuid=order_meta["strategy_order_uuid"],
                    idempotency_key=idempotency_key,
                    status=filled_status,
                    broker_order_id=str(result.broker_order_id or ""),
                    recovery_reason="broker_filled",
                )
                position_snapshot: dict[str, object] = {}
                realized_pnl = 0.0
                if sig.side == "SELL":
                    if existing is None:
                        finalize_signal(
                            sig,
                            final_status="skipped",
                            decision_snapshot=self._decision_snapshot(
                                signal=sig,
                                control=control,
                                existing=existing,
                                decision=decision,
                                skip_reason="position_missing_after_execute",
                                sized_notional=sized_notional,
                                final_notional=notional_to_use,
                                budget_limited=budget_limited,
                                netting_limited=netting_limited,
                                netting_snapshot=netting_snapshot,
                            ),
                            now_ts=now,
                        )
                        if candidate_id:
                            self._record_candidate_result(
                                candidate_id,
                                action=candidate_action or "close_all",
                                status="skipped",
                                rationale=candidate_note or "position_missing_after_execute",
                                result_tag="position_missing_after_execute",
                                signal=sig,
                            )
                        continue
                    entry_context = self._position_entry_context(existing)
                    hold_minutes = self._position_hold_minutes(existing, now)
                    prev_qty = float(existing.get("quantity") or 0.0)
                    prev_notional = float(existing.get("notional") or 0.0)
                    remaining_qty = max(0.0, prev_qty - qty)
                    remaining_notional = max(0.0, prev_notional - result.filled_notional)
                    realized_pnl, remaining_cost_basis = self._realize_position_sell(
                        existing,
                        sold_qty=qty,
                        sold_notional=result.filled_notional,
                    )
                    self._apply_realized_pnl(realized_pnl, ts=now)
                    existing["quantity"] = remaining_qty
                    existing["notional"] = remaining_notional
                    existing["cost_basis_notional"] = remaining_cost_basis
                    existing["price"] = result.filled_price
                    existing["last_trim_ts"] = now
                    existing["last_signal_id"] = sig.signal_id
                    existing["condition_id"] = str(existing.get("condition_id") or sig.condition_id or "")
                    existing["market_slug"] = sig.market_slug
                    existing["outcome"] = sig.outcome
                    exit_meta = self._signal_order_meta(sig)
                    self._apply_position_exit_meta(
                        existing,
                        exit_kind=str(exit_meta.get("exit_kind") or ""),
                        exit_label=str(exit_meta.get("exit_label") or ""),
                        exit_summary=str(exit_meta.get("exit_summary") or ""),
                        ts=now,
                    )
                    processed_sell_tokens.add(sig.token_id)
                    exit_result_meta = self._exit_result_meta(
                        exit_kind=str(order_meta.get("exit_kind") or ""),
                        ok=True,
                        remaining_notional=remaining_notional,
                        remaining_qty=remaining_qty,
                    )
                    if str(exit_result_meta.get("exit_result") or "") == "full_exit":
                        position_action = "exit"
                        position_action_label = self._action_tag_label("exit")
                    if remaining_notional <= self.settings.stale_position_close_notional_usd or remaining_qty <= 0.0:
                        position_snapshot = self._position_trace_snapshot(existing, is_open=False)
                        position_snapshot["notional"] = remaining_notional
                        position_snapshot["quantity"] = remaining_qty
                        del self.positions_book[sig.token_id]
                        self.state.open_positions = max(0, self.state.open_positions - 1)
                        if self.settings.token_reentry_cooldown_seconds > 0:
                            self.token_reentry_until[sig.token_id] = now + self.settings.token_reentry_cooldown_seconds
                        self._mark_trace_closed(str(sig.trace_id or ""), now)
                    else:
                        position_snapshot = self._position_trace_snapshot(existing, is_open=True)
                elif existing is None:
                    entry_reason = order_reason
                    self.state.open_positions += 1
                    self.positions_book[sig.token_id] = {
                        "token_id": sig.token_id,
                        "condition_id": str(sig.condition_id or ""),
                        "market_slug": sig.market_slug,
                        "outcome": sig.outcome,
                        "quantity": qty,
                        "price": result.filled_price,
                        "notional": result.filled_notional,
                        "cost_basis_notional": result.filled_notional,
                        "opened_ts": now,
                        "last_buy_ts": now,
                        "entry_wallet": sig.wallet,
                        "entry_wallet_score": sig.wallet_score,
                        "entry_wallet_tier": sig.wallet_tier,
                        "entry_topic_label": sig.topic_label,
                        "entry_topic_bias": sig.topic_bias,
                        "entry_topic_multiplier": sig.topic_multiplier,
                        "entry_topic_summary": sig.topic_score_summary,
                        "entry_reason": entry_reason,
                        "trace_id": sig.trace_id,
                        "origin_signal_id": sig.signal_id,
                        "last_signal_id": sig.signal_id,
                        "time_exit_state": normalize_time_exit_state(None).to_payload(),
                    }
                    position_snapshot = self._position_trace_snapshot(self._position_truth_for_token(sig.token_id), is_open=True)
                else:
                    entry_reason = str(existing.get("entry_reason") or order_reason)
                    prev_qty = float(existing.get("quantity") or 0.0)
                    prev_notional = float(existing.get("notional") or 0.0)
                    new_qty = prev_qty + qty
                    new_notional = prev_notional + result.filled_notional
                    new_cost_basis = self._position_cost_basis_notional(existing) + result.filled_notional
                    existing["quantity"] = new_qty
                    existing["notional"] = new_notional
                    existing["cost_basis_notional"] = new_cost_basis
                    existing["price"] = new_notional / max(0.01, new_qty)
                    existing["condition_id"] = str(existing.get("condition_id") or sig.condition_id or "")
                    existing["market_slug"] = sig.market_slug
                    existing["outcome"] = sig.outcome
                    existing["last_buy_ts"] = now
                    prev_entry_score = float(existing.get("entry_wallet_score") or 0.0)
                    if float(sig.wallet_score or 0.0) >= prev_entry_score:
                        existing["entry_wallet"] = sig.wallet
                        existing["entry_wallet_score"] = sig.wallet_score
                        existing["entry_wallet_tier"] = sig.wallet_tier
                    existing["entry_topic_label"] = sig.topic_label
                    existing["entry_topic_bias"] = sig.topic_bias
                    existing["entry_topic_multiplier"] = sig.topic_multiplier
                    existing["entry_topic_summary"] = sig.topic_score_summary
                    existing["entry_reason"] = entry_reason
                    existing["trace_id"] = str(existing.get("trace_id") or sig.trace_id)
                    existing["origin_signal_id"] = str(existing.get("origin_signal_id") or sig.signal_id)
                    existing["last_signal_id"] = sig.signal_id
                    position_snapshot = self._position_trace_snapshot(existing, is_open=True)
                if sig.side == "BUY":
                    entry_context = {
                        "entry_wallet": sig.wallet,
                        "entry_wallet_score": sig.wallet_score,
                        "entry_wallet_tier": sig.wallet_tier,
                        "entry_topic_label": sig.topic_label,
                        "entry_topic_bias": sig.topic_bias,
                        "entry_topic_multiplier": sig.topic_multiplier,
                        "entry_topic_summary": sig.topic_score_summary,
                        "entry_reason": order_reason,
                    }
                    hold_minutes = 0
                    exit_result_meta = {}
                self.recent_orders.appendleft(
                    {
                        "ts": now,
                        "cycle_id": cycle_id,
                        "signal_id": sig.signal_id,
                        "trace_id": sig.trace_id,
                        "title": sig.market_slug,
                        "token_id": sig.token_id,
                        "outcome": sig.outcome,
                        "side": sig.side,
                        "status": "FILLED",
                        "retry_count": 0,
                        "latency_ms": 0,
                        "reason": order_reason,
                        "source_wallet": sig.wallet,
                        "hold_minutes": hold_minutes if sig.side == "SELL" else 0,
                        "notional": result.filled_notional,
                        "realized_pnl": realized_pnl,
                        "wallet_score": sig.wallet_score,
                        "wallet_tier": sig.wallet_tier,
                        "position_action": position_action,
                        "position_action_label": position_action_label,
                        "topic_label": sig.topic_label,
                        "topic_bias": sig.topic_bias,
                        "topic_multiplier": sig.topic_multiplier,
                        **entry_context,
                        **exit_result_meta,
                        **order_meta,
                    }
                )
                order_record = dict(self.recent_orders[0])
                finalize_signal(
                    sig,
                    final_status="filled",
                    decision_snapshot=self._decision_snapshot(
                        signal=sig,
                        control=control,
                        existing=existing,
                        decision=decision,
                        sized_notional=sized_notional,
                        final_notional=notional_to_use,
                        budget_limited=budget_limited,
                        netting_limited=netting_limited,
                        netting_snapshot=netting_snapshot,
                    ),
                    order_snapshot=self._order_trace_snapshot(order_record),
                    position_snapshot=position_snapshot,
                    now_ts=now,
                )
                self._append_event(
                    "order_filled",
                    {
                        "wallet": sig.wallet,
                        "source_wallet": sig.wallet,
                        "market_slug": sig.market_slug,
                        "token_id": sig.token_id,
                        "cycle_id": cycle_id,
                        "signal_id": sig.signal_id,
                        "trace_id": sig.trace_id,
                        "side": sig.side,
                        "flow": str(order_record.get("flow") or ""),
                        "position_action": str(order_record.get("position_action") or ""),
                        "position_action_label": str(order_record.get("position_action_label") or ""),
                        "exit_kind": str(order_record.get("exit_kind") or ""),
                        "exit_label": str(order_record.get("exit_label") or ""),
                        "exit_summary": str(order_record.get("exit_summary") or ""),
                        "exit_result": str(order_record.get("exit_result") or ""),
                        "exit_result_label": str(order_record.get("exit_result_label") or ""),
                        "entry_wallet": str(order_record.get("entry_wallet") or ""),
                        "entry_wallet_score": float(order_record.get("entry_wallet_score") or 0.0),
                        "entry_wallet_tier": str(order_record.get("entry_wallet_tier") or ""),
                        "entry_topic_label": str(order_record.get("entry_topic_label") or ""),
                        "entry_topic_summary": str(order_record.get("entry_topic_summary") or ""),
                        "entry_reason": str(order_record.get("entry_reason") or ""),
                        "hold_minutes": int(order_record.get("hold_minutes") or 0),
                        "notional": result.filled_notional,
                        "filled_notional": result.filled_notional,
                        "price": result.filled_price,
                        "order_id": result.broker_order_id or "",
                        "decision_max_notional": decision.max_notional,
                        "score_sized_notional": sized_notional,
                        "requested_notional": notional_to_use,
                        "realized_pnl": realized_pnl,
                        "wallet_score": sig.wallet_score,
                        "wallet_tier": sig.wallet_tier,
                        "topic_key": sig.topic_key,
                        "topic_label": sig.topic_label,
                        "topic_bias": sig.topic_bias,
                        "topic_multiplier": sig.topic_multiplier,
                        "topic_sample_count": sig.topic_sample_count,
                        "topic_win_rate": sig.topic_win_rate,
                        "topic_roi": sig.topic_roi,
                        "exit_fraction": float(sig.exit_fraction or 0.0),
                        "cross_wallet_exit": bool(sig.cross_wallet_exit),
                        "exit_wallet_count": int(sig.exit_wallet_count or 0),
                        "reason": str(order_record.get("reason") or order_reason),
                        **market_context,
                    },
                )
                self._record_fill_ledger(
                    ts=now,
                    side=str(sig.side or ""),
                    token_id=str(sig.token_id or ""),
                    condition_id=str(sig.condition_id or ""),
                    market_slug=str(sig.market_slug or ""),
                    quantity=qty,
                    notional=result.filled_notional,
                    price=result.filled_price,
                    realized_pnl=realized_pnl,
                    signal_id=str(sig.signal_id or ""),
                    trace_id=str(sig.trace_id or ""),
                    order_id=str(result.broker_order_id or ""),
                    status="FILLED",
                    source_wallet=str(sig.wallet or ""),
                    source="broker_execute",
                )
                self._refresh_account_state_after_order(result)
                self.log.info(
                    "EXEC wallet=%s slug=%s token=%s side=%s score=%.1f tier=%s notional=%.2f px=%.4f order_id=%s msg=%s",
                    sig.wallet,
                    sig.market_slug,
                    sig.token_id,
                    sig.side,
                    sig.wallet_score,
                    sig.wallet_tier,
                    result.filled_notional,
                    result.filled_price,
                    result.broker_order_id,
                    result.message,
                )
                if candidate_id:
                    self._record_candidate_result(
                        candidate_id,
                        action=candidate_action or ("close_all" if sig.side == "SELL" else "follow"),
                        status="executed",
                        rationale=candidate_note or order_reason,
                        result_tag="filled",
                        signal=sig,
                        pnl_realized=realized_pnl if sig.side == "SELL" else None,
                    )
            else:
                now = int(time.time())
                result_metadata = dict(getattr(result, "metadata", {}) or {})
                if bool(result_metadata.get("security_fail_close")):
                    security_category = str(result_metadata.get("security_category") or "SIGNER_SECURITY_FAILURE").strip().upper()
                    security_reason = str(result_metadata.get("reason_code") or result.message or "security_fail_close").strip()
                    if not any(
                        str(item.get("category") or "").strip().upper() == security_category
                        and str(item.get("details") or "").strip() == security_reason
                        for item in self._recovery_conflicts
                    ):
                        self._record_recovery_conflict(
                            category=security_category,
                            token_id=str(sig.token_id or ""),
                            details=security_reason,
                        )
                    self.log.error(
                        "SECURITY_FAIL_CLOSED category=%s token=%s reason=%s",
                        security_category,
                        sig.token_id,
                        security_reason,
                    )
                self._set_intent_status(
                    strategy_order_uuid=str(order_meta.get("strategy_order_uuid") or ""),
                    idempotency_key=idempotency_key,
                    status=INTENT_STATUS_FAILED,
                    broker_order_id=str(result.broker_order_id or ""),
                    recovery_reason=str(result.message or "broker_rejected"),
                )
                entry_context = self._position_entry_context(existing)
                exit_result_meta = (
                    self._exit_result_meta(
                        exit_kind=str(order_meta.get("exit_kind") or ""),
                        ok=False,
                    )
                    if sig.side == "SELL"
                    else {}
                )
                position_action = sig.position_action
                position_action_label = sig.position_action_label
                self._append_event(
                    "order_reject",
                    {
                        "wallet": sig.wallet,
                        "source_wallet": sig.wallet,
                        "market_slug": sig.market_slug,
                        "token_id": sig.token_id,
                        "cycle_id": cycle_id,
                        "signal_id": sig.signal_id,
                        "trace_id": sig.trace_id,
                        "side": sig.side,
                        "flow": str(order_meta.get("flow") or ("exit" if sig.side == "SELL" else "entry")),
                        "position_action": position_action,
                        "position_action_label": position_action_label,
                        "exit_kind": str(order_meta.get("exit_kind") or ""),
                        "exit_label": str(order_meta.get("exit_label") or ""),
                        "exit_summary": str(order_meta.get("exit_summary") or ""),
                        "exit_result": str(exit_result_meta.get("exit_result") or ""),
                        "exit_result_label": str(exit_result_meta.get("exit_result_label") or ""),
                        "entry_wallet": str((entry_context if sig.side == "SELL" else {}).get("entry_wallet") or sig.wallet),
                        "entry_wallet_score": float((entry_context if sig.side == "SELL" else {}).get("entry_wallet_score") or sig.wallet_score),
                        "entry_wallet_tier": str((entry_context if sig.side == "SELL" else {}).get("entry_wallet_tier") or sig.wallet_tier),
                        "entry_topic_label": str((entry_context if sig.side == "SELL" else {}).get("entry_topic_label") or sig.topic_label),
                        "entry_topic_summary": str((entry_context if sig.side == "SELL" else {}).get("entry_topic_summary") or sig.topic_score_summary),
                        "entry_reason": str((entry_context if sig.side == "SELL" else {}).get("entry_reason") or ""),
                        "hold_minutes": self._position_hold_minutes(existing, now) if sig.side == "SELL" else 0,
                        "notional": decision.max_notional,
                        "requested_notional": notional_to_use,
                        "decision_max_notional": decision.max_notional,
                        "score_sized_notional": sized_notional,
                        "reason": result.message,
                        "wallet_score": sig.wallet_score,
                        "wallet_tier": sig.wallet_tier,
                        "topic_key": sig.topic_key,
                        "topic_label": sig.topic_label,
                        "topic_bias": sig.topic_bias,
                        "topic_multiplier": sig.topic_multiplier,
                        "topic_sample_count": sig.topic_sample_count,
                        "topic_win_rate": sig.topic_win_rate,
                        "topic_roi": sig.topic_roi,
                        "exit_fraction": float(sig.exit_fraction or 0.0),
                        "cross_wallet_exit": bool(sig.cross_wallet_exit),
                        "exit_wallet_count": int(sig.exit_wallet_count or 0),
                        **market_context,
                    },
                )
                self.recent_orders.appendleft(
                    {
                        "ts": now,
                        "cycle_id": cycle_id,
                        "signal_id": sig.signal_id,
                        "trace_id": sig.trace_id,
                        "title": sig.market_slug,
                        "token_id": sig.token_id,
                        "outcome": sig.outcome,
                        "side": sig.side,
                        "status": "REJECTED",
                        "retry_count": 0,
                        "latency_ms": 0,
                        "reason": self._order_reason(sig, result.message),
                        "source_wallet": sig.wallet,
                        "hold_minutes": self._position_hold_minutes(existing, now) if sig.side == "SELL" else 0,
                        "notional": decision.max_notional,
                        "wallet_score": sig.wallet_score,
                        "wallet_tier": sig.wallet_tier,
                        "position_action": position_action,
                        "position_action_label": position_action_label,
                        "topic_label": sig.topic_label,
                        "topic_bias": sig.topic_bias,
                        "topic_multiplier": sig.topic_multiplier,
                        **(
                            entry_context
                            if sig.side == "SELL"
                            else {
                                "entry_wallet": sig.wallet,
                                "entry_wallet_score": sig.wallet_score,
                                "entry_wallet_tier": sig.wallet_tier,
                                "entry_topic_label": sig.topic_label,
                                "entry_topic_bias": sig.topic_bias,
                                "entry_topic_multiplier": sig.topic_multiplier,
                                "entry_topic_summary": sig.topic_score_summary,
                                "entry_reason": "",
                            }
                        ),
                        **exit_result_meta,
                        **order_meta,
                        **market_context,
                    }
                )
                order_record = dict(self.recent_orders[0])
                finalize_signal(
                    sig,
                    final_status="order_rejected",
                    decision_snapshot=self._decision_snapshot(
                        signal=sig,
                        control=control,
                        existing=existing,
                        decision=decision,
                        sized_notional=sized_notional,
                        final_notional=notional_to_use,
                        budget_limited=budget_limited,
                        netting_limited=netting_limited,
                        netting_snapshot=netting_snapshot,
                    ),
                    order_snapshot=self._order_trace_snapshot(order_record),
                    position_snapshot=self._position_trace_snapshot(existing, is_open=bool(existing)),
                    now_ts=now,
                )
                if candidate_id:
                    self._record_candidate_result(
                        candidate_id,
                        action=candidate_action or "follow",
                        status="rejected",
                        rationale=candidate_note or result.message,
                        result_tag="order_rejected",
                        signal=sig,
                    )
                self.log.error(
                    "FAIL wallet=%s slug=%s reason=%s",
                    sig.wallet,
                    sig.market_slug,
                    result.message,
                )
                self._refresh_account_state_after_order(result)

        self._refresh_risk_state()

    def run(self, once: bool = False) -> None:
        self._update_runner_heartbeat(loop_status=RUNNER_LOOP_STATUS_IDLE)
        while True:
            cycle_started_ts = int(time.time())
            self._update_runner_heartbeat(now_ts=cycle_started_ts, cycle_started=True)
            try:
                self.step()
                cycle_finished_ts = int(time.time())
                self._update_runner_heartbeat(now_ts=cycle_finished_ts, cycle_finished=True)
                self.persist_runtime_state(self.settings.runtime_state_path)
            except Exception:
                self._update_runner_heartbeat(loop_status=RUNNER_LOOP_STATUS_ERROR)
                raise
            if once:
                self._update_runner_heartbeat(loop_status=RUNNER_LOOP_STATUS_STOPPED)
                self.persist_runtime_state(self.settings.runtime_state_path)
                return
            time.sleep(self.settings.poll_interval_seconds)
