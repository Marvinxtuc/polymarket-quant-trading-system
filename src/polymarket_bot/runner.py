from __future__ import annotations

import json
import logging
import os
import re
import time
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from polymarket_bot.brokers.base import Broker
from polymarket_bot.clients.data_api import AccountingSnapshot, MarketMetadata, PolymarketDataClient, PriceHistoryPoint
from polymarket_bot.config import Settings
from polymarket_bot.db import PersonalTerminalStore
from polymarket_bot.i18n import enum_label as i18n_enum_label, humanize_identifier as i18n_humanize_identifier, label as i18n_label, t as i18n_t
from polymarket_bot.notifier import Notifier
from polymarket_bot.reconciliation_report import append_ledger_entry, load_ledger_rows
from polymarket_bot.risk import RiskManager, RiskState
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

_MARKET_WINDOW_PATTERN = re.compile(r"-(5m|15m|30m|1h)-(\d{10})$")
_MARKET_WINDOW_SECONDS = {
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
}
_MARKET_METADATA_CACHE_TTL_SECONDS = 300


@dataclass(slots=True)
class ControlState:
    decision_mode: str = "manual"
    pause_opening: bool = False
    reduce_only: bool = False
    emergency_stop: bool = False
    clear_stale_pending_requested_ts: int = 0
    updated_ts: int = 0


@dataclass(slots=True)
class Trader:
    settings: Settings
    data_client: PolymarketDataClient
    strategy: WalletFollowerStrategy
    risk: RiskManager
    broker: Broker
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
    _last_control_signature: tuple[str, bool, bool, bool, int, int] = field(
        init=False,
        default=("manual", False, False, False, 0, 0),
    )
    _last_operator_pending_cleanup_ts: int = field(init=False, default=0)
    _signal_seq: int = field(init=False, default=0)
    _cycle_seq: int = field(init=False, default=0)
    _trace_seq: int = field(init=False, default=0)
    _trace_registry: dict[str, dict[str, object]] = field(init=False, default_factory=dict)
    _trace_order: deque[str] = field(init=False, default_factory=lambda: deque(maxlen=64))
    _active_day_key: str = field(init=False, default="")
    _last_account_sync_ts: float = field(init=False, default=0.0)
    _last_broker_event_sync_ts: int = field(init=False, default=0)
    startup_checks: list[dict[str, object]] = field(init=False, default_factory=list)
    startup_ready: bool = field(init=False, default=True)
    startup_warning_count: int = field(init=False, default=0)
    startup_failure_count: int = field(init=False, default=0)
    trading_mode: str = field(init=False, default="NORMAL")
    trading_mode_reasons: list[str] = field(init=False, default_factory=list)
    trading_mode_updated_ts: int = field(init=False, default=0)
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

    def __post_init__(self) -> None:
        self.state = RiskState()
        self.log = logging.getLogger("polybot")
        self.decision_mode = self._normalize_decision_mode(self.settings.decision_mode)
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
        self._update_trading_mode(self.control_state, now=int(time.time()))
        self._refresh_risk_state()

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

    def _refresh_risk_state(self) -> None:
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
        if signal.side == "BUY" and existing:
            entry_wallet = str(existing.get("entry_wallet") or "").strip().lower()
            signal_wallet = str(signal.wallet or "").strip().lower()
            if entry_wallet and signal_wallet and entry_wallet != signal_wallet:
                return "existing_position_conflict"
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
        if existing and float(existing.get("notional") or 0.0) > 0.0:
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
            direction = "bearish" if skip_reason == "existing_position_conflict" else "neutral"
            factors.append(
                self._candidate_reason_factor(
                    "existing_position",
                    i18n_t("runner.candidateFactor.existingPosition.label"),
                    f"{current_notional:.2f}U",
                    direction=direction,
                    weight=-6.0 if skip_reason == "existing_position_conflict" else 0.0,
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
            notes.append(i18n_t("runner.candidateExplanation.marketPrefix", {"bits": self._notification_separator().join(market_bits)}))
        market_status_bits: list[str] = []
        if market_context.get("market_closed") is True:
            market_status_bits.append(i18n_t("runner.candidateExplanation.marketStatusBit.closed"))
        elif market_context.get("market_active") is False:
            market_status_bits.append(i18n_t("runner.candidateExplanation.marketStatusBit.inactive"))
        elif market_context.get("market_accepting_orders") is False:
            market_status_bits.append(i18n_t("runner.candidateExplanation.marketStatusBit.acceptingOrdersFalse"))
        market_time_source = str(market_context.get("market_time_source") or "").strip()
        if market_time_source:
            market_status_bits.append(i18n_t("runner.candidateExplanation.marketStatusBit.timeSource", {"value": market_time_source}))
        market_end_date = str(market_context.get("market_end_date") or "").strip()
        if market_end_date:
            market_status_bits.append(i18n_t("runner.candidateExplanation.marketStatusBit.endDate", {"value": market_end_date}))
        market_remaining_seconds = market_context.get("market_remaining_seconds")
        if market_remaining_seconds not in (None, ""):
            market_status_bits.append(i18n_t("runner.candidateExplanation.marketStatusBit.remainSeconds", {"seconds": int(market_remaining_seconds)}))
        if market_status_bits:
            notes.append(i18n_t("runner.candidateExplanation.marketStatusPrefix", {"bits": self._notification_separator().join(market_status_bits)}))
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
            existing = self.positions_book.get(signal.token_id)
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
        created_ts = int(signal.timestamp.timestamp()) if isinstance(signal.timestamp, datetime) else now
        expires_ts = created_ts + max(60, int(self.settings.candidate_ttl_seconds))
        market_end_ts = market_context.get("market_end_ts")
        if market_end_ts not in (None, ""):
            expires_ts = min(expires_ts, int(market_end_ts))
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
            existing_position_conflict=skip_reason == "existing_position_conflict",
            existing_position_notional=float((existing or {}).get("notional") or 0.0),
            status=status,
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
        pending_candidates = list(self.candidate_store.list_candidates(statuses=["pending"], limit=1000) or [])
        if not pending_candidates:
            return

        refreshed = 0
        expired = 0
        for payload in pending_candidates:
            candidate_id = str(payload.get("id") or "")
            if not candidate_id:
                continue
            signal = self._candidate_to_signal(payload)
            if signal is None:
                self.candidate_store.update_candidate_status(
                    candidate_id,
                    status="expired",
                    note="candidate_refresh:snapshot_missing",
                    result_tag="candidate_snapshot_missing",
                    updated_ts=now,
                )
                expired += 1
                continue
            if str(signal.side or "").upper() != "BUY":
                continue

            existing = self.positions_book.get(signal.token_id)
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
            refreshed_candidate.selected_action = str(payload.get("selected_action") or refreshed_candidate.selected_action or "")
            refreshed_candidate.note = str(payload.get("note") or refreshed_candidate.note or "")
            refreshed_candidate.created_ts = int(payload.get("created_ts") or refreshed_candidate.created_ts or now)
            refreshed_candidate.updated_ts = now

            if refreshed_candidate.skip_reason:
                self.candidate_store.update_candidate_status(
                    candidate_id,
                    status="expired",
                    selected_action=refreshed_candidate.suggested_action,
                    note=f"candidate_refresh:{refreshed_candidate.skip_reason}",
                    result_tag=f"candidate_revalidated_{refreshed_candidate.skip_reason}",
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
        reasons = {str(value or "").strip() for value in trading_mode_state.get("reason_codes", []) if str(value or "").strip()}
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

        if "startup_not_ready" in reasons:
            title_params = {"mode": mode_label}
            body_params = {
                "failures": int(self.startup_failure_count or 0),
                "warnings": int(self.startup_warning_count or 0),
            }
            title = i18n_t("notification.tradingMode.startupGateBlocked.title", title_params)
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

        if "account_state_unknown" in reasons or "account_state_stale" in reasons:
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
            body = i18n_t("notification.tradingMode.reconciliationProtect.body", body_params)
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
        for candidate in self.candidate_store.list_candidates(statuses=["approved"], limit=self.settings.max_signals_per_cycle):
            signal = self._candidate_to_signal(candidate)
            if signal is None:
                self.candidate_store.update_candidate_status(
                    str(candidate.get("id") or ""),
                    status="expired",
                    result_tag="candidate_snapshot_missing",
                )
                continue
            candidate_id = str(candidate.get("id") or "")
            self.candidate_store.update_candidate_status(
                candidate_id,
                status="queued",
                selected_action=str(candidate.get("selected_action") or ""),
                updated_ts=int(time.time()),
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
        if abs(realized_pnl) <= 1e-9:
            return
        self._roll_daily_state_if_needed(ts)
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

    def persistence_state(self) -> dict[str, object]:
        return {
            "status": str(self.persistence_status or "ok"),
            "failure_count": int(self.persistence_failure_count or 0),
            "last_failure": dict(self.last_persistence_failure),
        }

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
        mode = str(self.trading_mode or "NORMAL").upper() or "NORMAL"
        return {
            "mode": mode,
            "opening_allowed": mode == "NORMAL",
            "reason_codes": list(self.trading_mode_reasons),
            "updated_ts": int(self.trading_mode_updated_ts or 0),
            "source": "runner",
            "account_state_status": str(self.account_state_status or "unknown"),
            "reconciliation_status": str(self.reconciliation_status or "unknown"),
            "persistence_status": str(self.persistence_status or "ok"),
        }

    def _update_trading_mode(
        self,
        control: ControlState,
        *,
        now: int | None = None,
        reconciliation: dict[str, object] | None = None,
    ) -> dict[str, object]:
        now_ts = int(now or time.time())
        reconciliation_payload = dict(reconciliation or self.reconciliation_summary(now=now_ts))
        reconciliation_status = str(reconciliation_payload.get("status") or "unknown").strip().lower() or "unknown"
        account_state_status = self._account_state_status(now=now_ts)

        reason_codes: list[str] = []
        if bool(control.emergency_stop):
            reason_codes.append("operator_emergency_stop")
        if bool(control.pause_opening):
            reason_codes.append("operator_pause_opening")
        if bool(control.reduce_only):
            reason_codes.append("operator_reduce_only")
        if not self.startup_ready:
            reason_codes.append("startup_not_ready")
        if account_state_status == "unknown":
            reason_codes.append("account_state_unknown")
        elif account_state_status == "stale":
            reason_codes.append("account_state_stale")
        if reconciliation_status == "fail":
            reason_codes.append("reconciliation_fail")
        elif reconciliation_status == "warn":
            reason_codes.append("reconciliation_warn")
        if str(self.persistence_status or "ok") != "ok":
            reason_codes.append("persistence_fault")

        mode = "NORMAL"
        if reason_codes:
            mode = "REDUCE_ONLY"
        if str(self.persistence_status or "ok") != "ok":
            mode = "HALTED"
        if bool(control.emergency_stop):
            mode = "HALTED"

        signature = (
            mode,
            tuple(reason_codes),
            account_state_status,
            reconciliation_status,
            str(self.persistence_status or "ok"),
        )
        self.trading_mode = mode
        self.trading_mode_reasons = list(reason_codes)
        self.trading_mode_updated_ts = now_ts
        self.account_state_status = account_state_status
        self.reconciliation_status = reconciliation_status
        if signature != self._last_trading_mode_signature:
            self._last_trading_mode_signature = signature
            trading_mode_state = self.trading_mode_state()
            self._append_event("trading_mode", trading_mode_state)
            log_level = self.log.warning if mode != "NORMAL" else self.log.info
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
        return self.trading_mode_state()

    def _buy_gate_reason(self) -> str:
        if str(self.trading_mode or "NORMAL").upper() == "NORMAL":
            return ""
        for reason in self.trading_mode_reasons:
            if reason == "operator_pause_opening":
                return "pause_opening"
            if reason == "operator_reduce_only":
                return "reduce_only"
            if reason == "startup_not_ready":
                return "startup_not_ready"
            if reason in {"account_state_unknown", "account_state_stale"}:
                return reason
            if reason in {"reconciliation_fail", "reconciliation_warn"}:
                return reason
            if reason == "persistence_fault":
                return "persistence_fault"
            if reason == "operator_emergency_stop":
                return "emergency_stop"
        return "system_halted" if str(self.trading_mode or "").upper() == "HALTED" else "system_reduce_only"

    def _pending_entry_cancel_reason(self, control: ControlState) -> str:
        if control.emergency_stop:
            return "emergency_stop_cancel_pending_entry"
        if control.reduce_only:
            return "reduce_only_cancel_pending_entry"
        if not self.pending_orders:
            return ""

        system_reduce_only_reasons = {
            "startup_not_ready",
            "account_state_unknown",
            "account_state_stale",
            "reconciliation_fail",
            "persistence_fault",
        }
        if any(reason in system_reduce_only_reasons for reason in self.trading_mode_reasons):
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
        path = str(Path(str(self.settings.runtime_state_path or "").strip()).expanduser())
        try:
            with open(path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, dict):
                return payload
        except FileNotFoundError:
            return None
        except Exception as exc:
            self.log.warning(
                "Load runtime snapshot failed path=%s err=%s",
                path,
                exc,
            )
        return None

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
        source = "none"
        positions: list[dict[str, object]] = []
        broker_positions_loaded = False
        snapshot: dict[str, object] | None = None
        snapshot_positions_by_token: dict[str, dict[str, object]] = {}
        recovered_risk_state: dict[str, object] | None = None
        recovered_signal_cycles: list[dict[str, object]] = []
        recovered_trace_records: list[dict[str, object]] = []
        recovered_pending_orders: list[dict[str, object]] = []
        recovered_operator_action: dict[str, object] | None = None

        if not self.settings.dry_run:
            broker_positions = self._load_broker_positions()
            if broker_positions is not None:
                broker_positions_loaded = True
                positions = broker_positions
                if positions:
                    source = "broker"

        snapshot = self._load_runtime_snapshot()
        if isinstance(snapshot, dict):
            risk_state_raw = snapshot.get("risk_state")
            if isinstance(risk_state_raw, dict):
                recovered_risk_state = risk_state_raw
            signal_cycles_raw = snapshot.get("signal_cycles")
            if isinstance(signal_cycles_raw, list):
                recovered_signal_cycles = [row for row in signal_cycles_raw if isinstance(row, dict)]
            trace_records_raw = snapshot.get("trace_registry")
            if isinstance(trace_records_raw, list):
                recovered_trace_records = [row for row in trace_records_raw if isinstance(row, dict)]
            pending_orders_raw = snapshot.get("pending_orders")
            if isinstance(pending_orders_raw, list):
                recovered_pending_orders = [row for row in pending_orders_raw if isinstance(row, dict)]
            operator_action_raw = snapshot.get("last_operator_action")
            if isinstance(operator_action_raw, dict):
                recovered_operator_action = operator_action_raw
            self._last_broker_event_sync_ts = int(
                self._safe_float(
                    snapshot.get("broker_event_sync_ts"),
                    self._last_broker_event_sync_ts,
                )
            )
            self._restore_recent_order_keys(snapshot.get("recent_order_keys"))
            recovered = snapshot.get("positions")
            if isinstance(recovered, list):
                normalized_snapshot_positions: list[dict[str, object]] = []
                for row in recovered:
                    if not isinstance(row, dict):
                        continue
                    normalized = self._normalize_position(row)
                    if not normalized:
                        continue
                    snapshot_positions_by_token[str(normalized["token_id"])] = normalized
                    normalized_snapshot_positions.append(normalized)
                if (not positions) and (not broker_positions_loaded) and normalized_snapshot_positions:
                    positions = normalized_snapshot_positions
                    source = "snapshot"

        recovered_pending_orders = self._restore_pending_orders_from_broker(recovered_pending_orders)

        if broker_positions_loaded and positions:
            restored_pending_buys_by_token: dict[str, dict[str, object]] = {}
            for row in sorted(recovered_pending_orders, key=lambda item: int(self._safe_float(item.get("ts")))):
                restored_pending = self._restore_pending_order(row)
                if not restored_pending:
                    continue
                if str(restored_pending.get("side") or "").upper() != "BUY":
                    continue
                token_id = str(restored_pending.get("token_id") or "")
                if token_id and token_id not in restored_pending_buys_by_token:
                    restored_pending_buys_by_token[token_id] = restored_pending

            merged_positions: list[dict[str, object]] = []
            merged_count = 0
            scaled_count = 0
            seeded_from_pending_count = 0
            for row in positions:
                normalized = self._normalize_position(row)
                if not normalized:
                    continue
                token_id = str(normalized["token_id"])
                snapshot_position = snapshot_positions_by_token.get(token_id)
                if snapshot_position:
                    merged = self._merge_recovered_position(
                        normalized,
                        snapshot_position,
                        recovery_source="runtime_snapshot",
                    )
                    if (
                        abs(float(normalized.get("quantity") or 0.0) - float(snapshot_position.get("quantity") or 0.0)) > 1e-6
                        and abs(
                            float(merged.get("cost_basis_notional") or 0.0)
                            - float(snapshot_position.get("cost_basis_notional") or snapshot_position.get("notional") or 0.0)
                        ) > 1e-6
                    ):
                        scaled_count += 1
                    normalized = merged
                    merged_count += 1
                else:
                    pending_buy = restored_pending_buys_by_token.get(token_id)
                    if pending_buy is not None:
                        normalized = self._seed_position_from_pending_order(normalized, pending_buy)
                        seeded_from_pending_count += 1
                merged_positions.append(normalized)
            positions = merged_positions
            if merged_count > 0 or seeded_from_pending_count > 0:
                source = "broker+snapshot" if merged_count > 0 else "broker+pending"
                self.log.info(
                    "Recovered broker positions with startup metadata merged=%d scaled_cost_basis=%d seeded_from_pending=%d",
                    merged_count,
                    scaled_count,
                    seeded_from_pending_count,
                )
                self._append_event(
                    "runtime_reconcile",
                    {
                        "source": "broker_startup_merge",
                        "merged_positions": merged_count,
                        "scaled_cost_basis": scaled_count,
                        "seeded_from_pending": seeded_from_pending_count,
                    },
                )

        if recovered_risk_state is not None:
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
        if recovered_signal_cycles:
            self.recent_signal_cycles = deque(recovered_signal_cycles[-24:], maxlen=24)
        if recovered_trace_records:
            self._trace_registry = {}
            self._trace_order = deque(maxlen=64)
            for row in recovered_trace_records[-64:]:
                trace_id = str(row.get("trace_id") or "").strip()
                if not trace_id:
                    continue
                self._trace_registry[trace_id] = dict(row)
                self._trace_order.append(trace_id)
        if recovered_pending_orders:
            self.pending_orders = {}
            for row in recovered_pending_orders:
                restored = self._restore_pending_order(row)
                if not restored:
                    continue
                self.pending_orders[str(restored["key"])] = restored
        if recovered_operator_action is not None:
            self.last_operator_action = dict(recovered_operator_action)

        ledger_realized_pnl = self._recover_daily_realized_pnl_from_ledger(self._active_day_key)
        if ledger_realized_pnl is not None:
            self.state.daily_realized_pnl = float(ledger_realized_pnl)

        if positions:
            count = self._set_positions_book(positions)
            self.log.info("Recovered positions source=%s count=%d", source, count)
            self._append_event(
                "runtime_reconcile",
                {
                    "source": source,
                    "count": count,
                },
            )
            return

        self._refresh_risk_state()
        if recovered_risk_state is not None and source == "snapshot":
            self.log.info(
                "Recovered runtime risk_state without active positions; keeping pnl and clearing open_positions"
            )
        else:
            self.log.info("No positions found for startup reconcile")

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
        ok = bool(ok_value) if ok_value is not None else status not in {"failed", "rejected", "error", "unsupported"}
        if status in {"submitted", "posted", "open", "live", "delayed", "accepted", "pending", "queued", "requested", "cancel_requested"}:
            status = "requested" if ok else "failed"
        elif not status:
            status = "canceled" if ok else "failed"
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

        if status == "requested" and ok:
            updated["broker_status"] = "cancel_requested"
            updated["message"] = message
            updated["reason"] = message
            self._record_pending_cancel_outcome(
                order=updated,
                now=now,
                position_lookup=position_lookup,
                recent_status="CANCEL_REQUESTED",
                broker_status="cancel_requested",
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
    ) -> dict[str, object]:
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
                }
            )

        return {
            "ts": int(time.time()),
            "runtime_version": 7,
            "broker_event_sync_ts": int(self._last_broker_event_sync_ts),
            "trading_mode": self.trading_mode_state(),
            "persistence": self.persistence_state(),
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
            },
            "positions": positions,
            "pending_orders": list(self.pending_orders.values()),
            "recent_order_keys": dict(self._recent_order_keys),
            "last_operator_action": dict(self.last_operator_action),
            "signal_cycles": list(self.recent_signal_cycles),
            "trace_registry": self._trace_records(),
        }

    def persist_runtime_state(self, path: str) -> None:
        payload = self._dump_runtime_state()
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
            self._record_persistence_fault(kind="runtime_state_write", path=resolved_path, error=exc)
            raise

    def _load_control_state(self) -> ControlState:
        payload: dict[str, object] = {}
        try:
            with open(self.settings.control_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                payload = data
        except FileNotFoundError:
            payload = {}
        except Exception as exc:
            self.log.warning("Control state read failed path=%s err=%s", self.settings.control_path, exc)
            payload = {}

        state = ControlState(
            decision_mode=self._normalize_decision_mode(payload.get("decision_mode", self.settings.decision_mode)),
            pause_opening=bool(payload.get("pause_opening", False)),
            reduce_only=bool(payload.get("reduce_only", False)),
            emergency_stop=bool(payload.get("emergency_stop", False)),
            clear_stale_pending_requested_ts=int(payload.get("clear_stale_pending_requested_ts") or 0),
            updated_ts=int(payload.get("updated_ts") or 0),
        )

        signature = (
            state.decision_mode,
            state.pause_opening,
            state.reduce_only,
            state.emergency_stop,
            state.clear_stale_pending_requested_ts,
            state.updated_ts,
        )
        if signature != self._last_control_signature:
            self._last_control_signature = signature
            self.log.info(
                "CONTROL decision_mode=%s pause_opening=%s reduce_only=%s emergency_stop=%s clear_stale_pending_requested_ts=%d updated_ts=%d",
                state.decision_mode,
                state.pause_opening,
                state.reduce_only,
                state.emergency_stop,
                state.clear_stale_pending_requested_ts,
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

        for token_id in list(self.positions_book.keys()):
            position = self.positions_book.get(token_id)
            if not position:
                continue

            opened_ts = int(position.get("opened_ts") or now)
            if now - opened_ts < stale_seconds:
                continue

            last_trim_ts = int(position.get("last_trim_ts") or 0)
            if now - last_trim_ts < trim_cooldown:
                continue

            current_notional = float(position.get("notional") or 0.0)
            current_qty = float(position.get("quantity") or 0.0)
            if current_notional <= 0 or current_qty <= 0:
                continue

            full_close = current_notional <= close_notional
            exit_fraction = 1.0 if full_close else trim_pct
            target_notional = current_notional if full_close else (current_notional * trim_pct)
            if target_notional < actionable_floor:
                continue

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
                        "reason": f"time-exit failed: {result.message}",
                        "source_wallet": "system-time-exit",
                        "hold_minutes": self._position_hold_minutes(position, now),
                        "topic_label": str(position.get("entry_topic_label") or ""),
                        "notional": target_notional,
                        "wallet_score": 0.0,
                        "wallet_tier": "SYSTEM",
                        "position_action": sig.position_action,
                        "position_action_label": sig.position_action_label,
                        **entry_context,
                        **self._exit_result_meta(exit_kind="time_exit", ok=False),
                        **self._order_meta(
                            "SELL",
                            exit_kind="time_exit",
                            exit_label=time_exit_label,
                            exit_summary="time-exit retired" if retired else "time-exit failed",
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
                    self.log.error("TIME_EXIT_FAIL slug=%s token=%s reason=%s", sig.market_slug, sig.token_id, result.message)
                continue

            if result.is_pending:
                entry_context = self._position_entry_context(position)
                pending_record = self._register_pending_order(
                    signal=sig,
                    cycle_id=cycle_id,
                    result=result,
                    order_meta=self._order_meta("SELL", exit_kind="time_exit", exit_label=time_exit_label, exit_summary="time-exit"),
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
                        "reason": f"time-exit pending: {result.message}",
                        "source_wallet": "system-time-exit",
                        "hold_minutes": self._position_hold_minutes(position, now),
                        "topic_label": str(position.get("entry_topic_label") or ""),
                        "notional": result.requested_notional,
                        "wallet_score": 0.0,
                        "wallet_tier": "SYSTEM",
                        "position_action": sig.position_action,
                        "position_action_label": sig.position_action_label,
                        **entry_context,
                        **self._pending_exit_result_meta(sig.side),
                        **self._order_meta("SELL", exit_kind="time_exit", exit_label=time_exit_label, exit_summary="time-exit"),
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
                    "TIME_EXIT_POSTED mode=%s slug=%s token=%s notional=%.2f status=%s order_id=%s",
                    "congested" if congested else "normal",
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
            position["last_trim_ts"] = now
            position["price"] = result.filled_price
            position["last_signal_id"] = sig.signal_id
            self._apply_position_exit_meta(
                position,
                exit_kind="time_exit",
                exit_label=time_exit_label,
                exit_summary="time-exit trim",
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
                    "reason": "time-exit trim",
                    "source_wallet": "system-time-exit",
                    "hold_minutes": self._position_hold_minutes(position, now),
                    "topic_label": str(position.get("entry_topic_label") or ""),
                    "notional": result.filled_notional,
                    "wallet_score": 0.0,
                    "wallet_tier": "SYSTEM",
                    "position_action": sig.position_action,
                    "position_action_label": sig.position_action_label,
                    **self._position_entry_context(position),
                    **self._exit_result_meta(
                        exit_kind="time_exit",
                        ok=True,
                        remaining_notional=remaining_notional,
                        remaining_qty=remaining_qty,
                    ),
                    **self._order_meta("SELL", exit_kind="time_exit", exit_label=time_exit_label, exit_summary="time-exit trim"),
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
                "TIME_EXIT mode=%s slug=%s token=%s trim_notional=%.2f remain_notional=%.2f",
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
                    },
                )
                if self.settings.token_reentry_cooldown_seconds > 0:
                    self.token_reentry_until[token_id] = now + self.settings.token_reentry_cooldown_seconds
                self._mark_trace_closed(str(sig.trace_id or ""), now)
                self.log.info(
                    "TIME_EXIT_CLOSE slug=%s token=%s open_positions=%d",
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
        control = self._load_control_state()
        cycle_start_ts = int(time.time())
        reconciliation = self.reconciliation_summary(now=cycle_start_ts)
        self._update_trading_mode(control, now=cycle_start_ts, reconciliation=reconciliation)
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
            existing = self.positions_book.get(sig.token_id)
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
            existing = self.positions_book.get(sig.token_id)
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
            existing = self.positions_book.get(sig.token_id)
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

            existing = self.positions_book.get(sig.token_id)
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
            pending_duplicate = self._find_pending_order_duplicate(sig) if sig.side == "BUY" else None
            if pending_duplicate is not None:
                duplicate_reason = "pending_order_duplicate"
                duplicate_context = {
                    "duplicate_source": str(pending_duplicate.get("recovery_source") or "pending_orders"),
                    "duplicate_order_id": str(pending_duplicate.get("order_id") or ""),
                    "duplicate_key": str(pending_duplicate.get("key") or ""),
                    "duplicate_recovery_status": str(pending_duplicate.get("recovery_status") or ""),
                }
            else:
                broker_open_order_duplicate = self._find_broker_open_order_duplicate(sig) if sig.side == "BUY" else None
                if broker_open_order_duplicate is not None:
                    duplicate_reason = "broker_open_order_duplicate"
                    duplicate_context = dict(broker_open_order_duplicate)
                elif self._is_order_duplicate(sig, notional_to_use):
                    duplicate_reason = "recent_order_duplicate"

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

            result = self.broker.execute(sig, notional_to_use)
            market_context = self._market_context_from_result(result)
            if result.ok:
                now = int(time.time())
                existing = self.positions_book.get(sig.token_id)
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
                    }
                    position_snapshot = self._position_trace_snapshot(self.positions_book.get(sig.token_id), is_open=True)
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
        while True:
            self.step()
            self.persist_runtime_state(self.settings.runtime_state_path)
            if once:
                return
            time.sleep(self.settings.poll_interval_seconds)
