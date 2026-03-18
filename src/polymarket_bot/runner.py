from __future__ import annotations

import json
import logging
import os
import time
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from polymarket_bot.brokers.base import Broker
from polymarket_bot.clients.data_api import AccountingSnapshot, PolymarketDataClient
from polymarket_bot.config import Settings
from polymarket_bot.reconciliation_report import append_ledger_entry, load_ledger_rows
from polymarket_bot.risk import RiskManager, RiskState
from polymarket_bot.strategies.wallet_follower import WalletFollowerStrategy
from polymarket_bot.types import BrokerOrderEvent, ExecutionResult, OpenOrderSnapshot, OrderFillSnapshot, OrderStatusSnapshot, Signal
from polymarket_bot.wallet_history import WalletHistoryStore
from polymarket_bot.wallet_scoring import RealizedWalletMetrics


@dataclass(slots=True)
class ControlState:
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
    _last_control_signature: tuple[bool, bool, bool, int, int] = field(
        init=False,
        default=(False, False, False, 0, 0),
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
    last_operator_action: dict[str, object] = field(init=False, default_factory=dict)

    def __post_init__(self) -> None:
        self.state = RiskState()
        self.log = logging.getLogger("polybot")
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

    def _available_notional_usd(self) -> float:
        return max(0.0, self.settings.bankroll_usd - (self._tracked_notional_usd() + self._pending_entry_notional_usd()))

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

    def _safe_float(self, value: object, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

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
        path = str(self.settings.ledger_path or "").strip()
        if not path:
            return
        try:
            append_ledger_entry(path, entry_type, payload, broker=type(self.broker).__name__)
        except Exception as exc:
            self.log.warning("Failed to append ledger path=%s err=%s", path, exc)

    def _ledger_broker_name(self) -> str:
        return type(self.broker).__name__

    def _recover_daily_realized_pnl_from_ledger(self, day_key: str) -> float | None:
        path = str(self.settings.ledger_path or "").strip()
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
        path = str(self.settings.ledger_path or "").strip()
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
            self.state.cash_balance_usd = float(snapshot.cash_balance)
            self.state.positions_value_usd = float(snapshot.positions_value)
            self.state.equity_usd = float(snapshot.equity)
            self.state.account_snapshot_ts = self._parse_iso_timestamp(snapshot.valuation_time) or int(time.time())
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

    def _append_event(self, event_type: str, payload: dict[str, object]) -> None:
        path = self.settings.event_log_path
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
        path = os.getenv("NETWORK_SMOKE_LOG", "/tmp/poly_network_smoke.jsonl")
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
            "open_positions": int(self.state.open_positions),
            "tracked_notional_usd": float(self.state.tracked_notional_usd),
            "ledger_available": bool(ledger_summary.get("available")),
            "account_snapshot_age_seconds": int(account_snapshot_age_seconds),
            "broker_reconcile_age_seconds": int(broker_reconcile_age_seconds),
            "broker_event_sync_age_seconds": int(broker_event_sync_age_seconds),
        }

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
            return ("add", "追加买入") if existing else ("entry", "首次入场")
        if float(signal.exit_fraction or 0.0) >= 0.95:
            return ("exit", "完全退出")
        return ("trim", "部分减仓")

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
        try:
            with open(self.settings.runtime_state_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, dict):
                return payload
        except FileNotFoundError:
            return None
        except Exception as exc:
            self.log.warning(
                "Load runtime snapshot failed path=%s err=%s",
                self.settings.runtime_state_path,
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

        snapshot: AccountingSnapshot | None = None
        try:
            snapshot = self.data_client.get_accounting_snapshot(wallet)
        except Exception as exc:
            self.log.debug("Broker accounting snapshot unavailable during position bootstrap wallet=%s err=%s", wallet, exc)

        if snapshot is not None:
            snapshot_position_count = len(tuple(snapshot.positions or ()))
            if snapshot_position_count == 0 and float(snapshot.positions_value) <= 1e-9:
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
            if quantity <= 0 or notional <= 0:
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

        if not positions:
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
                recovered = snapshot.get("positions")
                if (not broker_positions_loaded) and isinstance(recovered, list):
                    positions = [r for r in recovered if isinstance(r, dict)]
                    if positions:
                        source = "snapshot"

        recovered_pending_orders = self._restore_pending_orders_from_broker(recovered_pending_orders)

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
        if self.settings.dry_run:
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
                reconciled[token_id] = self._merge_position_context(normalized, prev_pos)
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
            self.state.open_positions = len(self.positions_book)
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
        if self.settings.dry_run or interval <= 0:
            return

        now = time.time()
        if (now - self._last_broker_reconcile_ts) < interval:
            return
        self._last_broker_reconcile_ts = now
        self._reconcile_runtime_with_broker()

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
        if allowed_notional < 5.0:
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
        if allowed_notional < 5.0:
            self._append_event(
                "signal_skip",
                {
                    "wallet": signal.wallet,
                    "market_slug": signal.market_slug,
                    "token_id": signal.token_id,
                    "reason": "notional_too_small_after_budget",
                    "allowed_notional": allowed_notional,
                    "available_notional": available,
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
        if target_notional < 5.0:
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
                exit_label="共振退出",
                exit_summary=str(signal.exit_reason or f"{int(signal.exit_wallet_count or 0)} wallets"),
            )
        if str(signal.wallet or "").strip().lower() == "system-time-exit":
            return cls._order_meta(signal.side, exit_kind="time_exit", exit_label="时间退出", exit_summary=str(signal.exit_reason or "time-exit"))
        if str(signal.wallet or "").strip().lower() == "system-emergency-stop":
            return cls._order_meta(signal.side, exit_kind="emergency_exit", exit_label="紧急退出", exit_summary=str(signal.exit_reason or "emergency-exit"))
        return cls._order_meta(
            signal.side,
            exit_kind="smart_wallet_exit",
            exit_label="主钱包减仓",
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
        action_reason = ""
        if control.emergency_stop:
            action_reason = "emergency_stop_cancel_pending_entry"
        elif control.reduce_only:
            action_reason = "reduce_only_cancel_pending_entry"
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
        }

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
        events = self.broker.list_order_events(
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

    def _seed_position_from_pending_order(
        self,
        position: dict[str, object],
        order: dict[str, object],
    ) -> dict[str, object]:
        seeded = dict(position)
        seeded["condition_id"] = str(position.get("condition_id") or order.get("condition_id") or "")
        seeded["opened_ts"] = int(position.get("opened_ts") or order.get("ts") or 0)
        seeded["last_buy_ts"] = int(position.get("last_buy_ts") or order.get("ts") or 0)
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
        now: int,
    ) -> tuple[float, float, float]:
        token_id = str(order.get("token_id") or "")
        side = str(order.get("side") or "").upper()
        market_slug = str(order.get("market_slug") or token_id)
        outcome = str(order.get("outcome") or "YES")
        condition_id = str(order.get("condition_id") or "")

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
                    "opened_ts": int(current.get("opened_ts") or order.get("ts") or now),
                    "last_buy_ts": now,
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
        self._apply_realized_pnl(realized_pnl, ts=now)

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
                "last_trim_ts": now,
                "last_signal_id": str(order.get("last_signal_id") or order.get("signal_id") or ""),
            }
        )
        self._apply_position_exit_meta(
            current,
            exit_kind=str(order.get("exit_kind") or ""),
            exit_label=str(order.get("exit_label") or ""),
            exit_summary=str(order.get("exit_summary") or ""),
            ts=now,
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
        self.broker.heartbeat(heartbeat_ids)
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
        remaining: dict[str, dict[str, object]] = {}
        for key, order in sorted(self.pending_orders.items(), key=lambda item: int(item[1].get("ts") or 0)):
            token_id = str(order.get("token_id") or "")
            side = str(order.get("side") or "").upper()
            order_id = str(order.get("order_id") or "").strip()
            previous_matched_notional = float(order.get("matched_notional_hint") or 0.0)
            previous_matched_size = float(order.get("matched_size_hint") or 0.0)
            status_snapshot = stream_statuses.get(order_id) if order_id else None
            if status_snapshot is None and order_id and not stream_available:
                status_snapshot = self.broker.get_order_status(order_id)
            if status_snapshot is not None:
                order["broker_status"] = status_snapshot.lifecycle_status
                order["matched_notional_hint"] = max(
                    previous_matched_notional,
                    float(status_snapshot.matched_notional or 0.0),
                )
                order["matched_size_hint"] = max(
                    previous_matched_size,
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
                if (
                    str(order.get("broker_status") or "") in active_statuses
                    and float(order.get("matched_notional_hint") or 0.0) > 1e-6
                    and float(order.get("matched_notional_hint") or 0.0) + 1e-6 < float(order.get("requested_notional") or 0.0)
                ):
                    order["broker_status"] = "partially_filled"

            delta_notional_map = buy_notional_delta if side == "BUY" else sell_notional_delta
            delta_qty_map = buy_qty_delta if side == "BUY" else sell_qty_delta
            available_notional = float(delta_notional_map.get(token_id) or 0.0)
            available_qty = float(delta_qty_map.get(token_id) or 0.0)
            requested_notional = float(order.get("requested_notional") or 0.0)
            requested_remaining = max(0.0, requested_notional - previous_matched_notional)
            reported_incremental_notional = max(0.0, float(order.get("matched_notional_hint") or 0.0) - previous_matched_notional)
            reported_incremental_size = max(0.0, float(order.get("matched_size_hint") or 0.0) - previous_matched_size)
            use_position_delta = available_notional > 1e-6
            matched_notional = 0.0
            if use_position_delta:
                matched_notional = min(available_notional, requested_remaining) if requested_remaining > 0.0 else available_notional
            elif reported_incremental_notional > 1e-6:
                matched_notional = min(reported_incremental_notional, requested_remaining) if requested_remaining > 0.0 else reported_incremental_notional
            if matched_notional > 1e-6:
                matched_qty = 0.0
                if use_position_delta and available_notional > 1e-9 and available_qty > 0.0:
                    matched_qty = min(available_qty, available_qty * (matched_notional / available_notional))
                elif reported_incremental_size > 0.0:
                    matched_qty = reported_incremental_size
                observed_price = float(
                    (float(fill_agg.get("price") or 0.0) if fill_agg is not None else 0.0)
                    or (status_snapshot.avg_fill_price if status_snapshot is not None and status_snapshot.avg_fill_price > 0.0 else 0.0)
                    or float(order.get("matched_price_hint") or 0.0)
                    or float((next_positions.get(token_id) or {}).get("price") or 0.0)
                    or float(order.get("requested_price") or 0.0)
                )
                if matched_qty <= 0.0 and observed_price > 0.0:
                    matched_qty = matched_notional / max(0.01, observed_price)
                reason = str(order.get("reason") or order.get("message") or "broker reconcile")
                realized_pnl = 0.0
                if use_position_delta:
                    delta_notional_map[token_id] = max(0.0, available_notional - matched_notional)
                    delta_qty_map[token_id] = max(0.0, available_qty - matched_qty)
                    if side == "BUY":
                        next_position = next_positions.get(token_id)
                        if next_position is not None:
                            next_position["cost_basis_notional"] = (
                                self._position_cost_basis_notional(previous_positions.get(token_id)) + matched_notional
                            )
                    else:
                        previous_position = previous_positions.get(token_id)
                        realized_pnl, remaining_cost_basis = self._realize_position_sell(
                            previous_position,
                            sold_qty=matched_qty,
                            sold_notional=matched_notional,
                        )
                        next_position = next_positions.get(token_id)
                        if next_position is not None:
                            next_position["cost_basis_notional"] = remaining_cost_basis
                        self._apply_realized_pnl(realized_pnl, ts=now)
                else:
                    realized_pnl, _, _ = self._apply_pending_fill_delta(
                        order=order,
                        previous_positions=previous_positions,
                        next_positions=next_positions,
                        filled_notional=matched_notional,
                        filled_qty=matched_qty,
                        fill_price=observed_price,
                        now=now,
                    )

                order["matched_notional_hint"] = max(
                    float(order.get("matched_notional_hint") or 0.0),
                    previous_matched_notional + matched_notional,
                )
                order["matched_size_hint"] = max(
                    float(order.get("matched_size_hint") or 0.0),
                    previous_matched_size + matched_qty,
                )
                if observed_price > 0.0:
                    order["matched_price_hint"] = observed_price

                remaining_requested = max(0.0, requested_notional - float(order.get("matched_notional_hint") or 0.0))
                keep_pending = str(order.get("broker_status") or "") in active_statuses and remaining_requested > 1e-6
                reconcile_status = "PARTIAL" if keep_pending else "RECONCILED"
                reconcile_reason = (
                    "partial fill via broker positions" if use_position_delta and keep_pending else
                    "reconciled via broker positions" if use_position_delta else
                    "partial fill via broker trades" if keep_pending else
                    "reconciled via broker trades"
                )
                remaining_position = next_positions.get(token_id) or {}
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
                        "status": reconcile_status,
                        "order_id": order_id,
                        "broker_status": str(order.get("broker_status") or ""),
                        "retry_count": 0,
                        "latency_ms": max(0, (now - int(order.get("ts") or now)) * 1000),
                        "reason": f"{reason} | {reconcile_reason}",
                        "source_wallet": str(order.get("wallet") or ""),
                        "hold_minutes": self._position_hold_minutes(previous_positions.get(token_id), now) if side == "SELL" else 0,
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
                        "order_id": str(order.get("order_id") or ""),
                        "broker_status": str(order.get("broker_status") or ""),
                        "reason": "reconciled_via_positions" if use_position_delta else "reconciled_via_trades",
                        "realized_pnl": realized_pnl,
                        **market_context,
                    },
                )
                self._record_fill_ledger(
                    ts=now,
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
                    source="broker_reconcile" if use_position_delta else "broker_trades",
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
            return {"exit_result": "reject", "exit_result_label": "已拒绝"}
        if str(exit_kind or "") == "emergency_exit":
            return {"exit_result": "emergency", "exit_result_label": "紧急退出"}
        if remaining_notional <= self.settings.stale_position_close_notional_usd or remaining_qty <= 0.0:
            return {"exit_result": "full_exit", "exit_result_label": "完全退出"}
        return {"exit_result": "partial_trim", "exit_result_label": "部分减仓"}

    @staticmethod
    def _pending_exit_result_meta(side: str) -> dict[str, object]:
        if str(side).upper() != "SELL":
            return {}
        return {"exit_result": "pending", "exit_result_label": "待成交"}

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
    ) -> dict[str, object]:
        snapshot = {
            "control": {
                "pause_opening": bool(control.pause_opening),
                "reduce_only": bool(control.reduce_only),
                "emergency_stop": bool(control.emergency_stop),
            },
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
            "runtime_version": 6,
            "broker_event_sync_ts": int(self._last_broker_event_sync_ts),
            "startup": {
                "ready": bool(self.startup_ready),
                "warning_count": int(self.startup_warning_count),
                "failure_count": int(self.startup_failure_count),
                "checks": list(self.startup_checks),
            },
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
            "last_operator_action": dict(self.last_operator_action),
            "signal_cycles": list(self.recent_signal_cycles),
            "trace_registry": self._trace_records(),
        }

    def persist_runtime_state(self, path: str) -> None:
        payload = self._dump_runtime_state()
        parent = Path(path).expanduser().parent
        if str(parent) not in {"", "."}:
            parent.mkdir(parents=True, exist_ok=True)
        tmp_path = f"{path}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False)
        os.replace(tmp_path, path)

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
            pause_opening=bool(payload.get("pause_opening", False)),
            reduce_only=bool(payload.get("reduce_only", False)),
            emergency_stop=bool(payload.get("emergency_stop", False)),
            clear_stale_pending_requested_ts=int(payload.get("clear_stale_pending_requested_ts") or 0),
            updated_ts=int(payload.get("updated_ts") or 0),
        )

        signature = (
            state.pause_opening,
            state.reduce_only,
            state.emergency_stop,
            state.clear_stale_pending_requested_ts,
            state.updated_ts,
        )
        if signature != self._last_control_signature:
            self._last_control_signature = signature
            self.log.info(
                "CONTROL pause_opening=%s reduce_only=%s emergency_stop=%s clear_stale_pending_requested_ts=%d updated_ts=%d",
                state.pause_opening,
                state.reduce_only,
                state.emergency_stop,
                state.clear_stale_pending_requested_ts,
                state.updated_ts,
            )

        self.control_state = state
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
                position_action_label="紧急退出",
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
                        "exit_label": "紧急退出",
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
                        **self._order_meta("SELL", exit_kind="emergency_exit", exit_label="紧急退出", exit_summary="emergency-exit"),
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
                    order_meta=self._order_meta("SELL", exit_kind="emergency_exit", exit_label="紧急退出", exit_summary="emergency-exit"),
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
                        "exit_label": "紧急退出",
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
                        **self._order_meta("SELL", exit_kind="emergency_exit", exit_label="紧急退出", exit_summary="emergency-exit"),
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
                exit_label="紧急退出",
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
                    "exit_label": "紧急退出",
                    "exit_result": "emergency",
                    "exit_result_label": "紧急退出",
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
                    **self._order_meta("SELL", exit_kind="emergency_exit", exit_label="紧急退出", exit_summary="emergency-exit"),
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

            trim_notional = current_notional * trim_pct
            if trim_notional < 5:
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
                observed_size=current_qty * trim_pct,
                observed_notional=trim_notional,
                timestamp=datetime.now(tz=timezone.utc),
                position_action="exit" if trim_pct >= 0.95 else "trim",
                position_action_label="时间退出" if trim_pct >= 0.95 else "时间减仓",
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
                sized_notional=trim_notional,
                final_notional=trim_notional,
            )
            base_decision_snapshot["risk_allowed"] = True
            base_decision_snapshot["risk_reason"] = "time_exit"
            base_decision_snapshot["risk_snapshot"] = {
                "system_exit": "time_exit",
                "congested": bool(congested),
                "trim_pct": float(trim_pct),
                "stale_seconds": int(stale_seconds),
                "trim_cooldown": int(trim_cooldown),
                "close_notional_threshold": float(close_notional),
            }
            result = self.broker.execute(sig, trim_notional)
            if not result.ok:
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
                        "exit_label": "时间退出",
                        "hold_minutes": self._position_hold_minutes(position, now),
                        "exit_fraction": float(trim_pct),
                        "trim_notional": trim_notional,
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
                        "reason": f"time-exit failed: {result.message}",
                        "source_wallet": "system-time-exit",
                        "hold_minutes": self._position_hold_minutes(position, now),
                        "topic_label": str(position.get("entry_topic_label") or ""),
                        "notional": trim_notional,
                        "wallet_score": 0.0,
                        "wallet_tier": "SYSTEM",
                        "position_action": sig.position_action,
                        "position_action_label": sig.position_action_label,
                        **entry_context,
                        **self._exit_result_meta(exit_kind="time_exit", ok=False),
                        **self._order_meta("SELL", exit_kind="time_exit", exit_label="时间退出", exit_summary="time-exit failed"),
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
                self.log.error("TIME_EXIT_FAIL slug=%s token=%s reason=%s", sig.market_slug, sig.token_id, result.message)
                continue

            if result.is_pending:
                entry_context = self._position_entry_context(position)
                pending_record = self._register_pending_order(
                    signal=sig,
                    cycle_id=cycle_id,
                    result=result,
                    order_meta=self._order_meta("SELL", exit_kind="time_exit", exit_label="时间退出", exit_summary="time-exit"),
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
                        "exit_label": "时间退出",
                        "hold_minutes": self._position_hold_minutes(position, now),
                        "exit_fraction": float(trim_pct),
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
                        **self._order_meta("SELL", exit_kind="time_exit", exit_label="时间退出", exit_summary="time-exit"),
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
                exit_label="时间退出",
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
                    "exit_label": "时间退出",
                    "exit_result": "partial_trim" if remaining_notional > close_notional and remaining_qty > 0.0 else "full_exit",
                    "exit_result_label": "部分减仓" if remaining_notional > close_notional and remaining_qty > 0.0 else "完全退出",
                    "hold_minutes": self._position_hold_minutes(position, now),
                    "exit_fraction": float(trim_pct),
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
                    **self._order_meta("SELL", exit_kind="time_exit", exit_label="时间退出", exit_summary="time-exit trim"),
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
        self._refresh_risk_state()
        control = self._load_control_state()
        self._apply_operator_clear_stale_pending(control)
        self._apply_control_pending_entry_cancels(control, now=int(time.time()))
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
        wallets = self._resolve_wallets()
        self.last_wallets = wallets
        if not wallets:
            self.log.warning("No wallets configured/resolved. Check WATCH_WALLETS and discovery settings.")
            self.last_signals = []
            return
        self._update_strategy_history(wallets)

        signals = self.strategy.generate_signals(wallets)
        if not signals:
            self.last_signals = []
            self.log.info("No actionable signal this cycle")
            return

        cycle_now = int(time.time())
        cycle_id = self._new_cycle_id(cycle_now)
        wallet_pool_snapshot = self._wallet_pool_snapshot()
        signal_records: list[dict[str, object]] = []
        signal_record_by_id: dict[str, dict[str, object]] = {}
        prepared_signals: list[Signal] = []
        for sig in signals:
            existing = self.positions_book.get(sig.token_id)
            sig.signal_id = self._new_signal_id(cycle_now)
            sig.trace_id = self._trace_id_for_signal(sig, existing, cycle_now)
            action, action_label = self._position_action_for_signal(sig, existing)
            sig.position_action = action
            sig.position_action_label = action_label
            prepared_signals.append(sig)
            record = self._cycle_candidate_record(
                sig,
                cycle_id=cycle_id,
                wallet_pool_snapshot=wallet_pool_snapshot,
            )
            signal_records.append(record)
            signal_record_by_id[str(sig.signal_id)] = record
        self.last_signals = prepared_signals
        self.recent_signal_cycles.appendleft(
            {
                "cycle_id": cycle_id,
                "ts": cycle_now,
                "wallets": list(wallets),
                "wallet_pool_snapshot": list(wallet_pool_snapshot),
                "candidates": signal_records,
            }
        )

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

        for sig in prepared_signals:
            order_meta = self._signal_order_meta(sig)
            existing = self.positions_book.get(sig.token_id)
            if sig.side == "BUY" and control.pause_opening:
                self.log.info(
                    "SKIP wallet=%s slug=%s reason=pause opening enabled",
                    sig.wallet,
                    sig.market_slug,
                )
                self._append_event(
                    "signal_skip",
                    {
                        "wallet": sig.wallet,
                        "market_slug": sig.market_slug,
                        "token_id": sig.token_id,
                        "reason": "pause_opening",
                    },
                )
                finalize_signal(
                    sig,
                    final_status="skipped",
                    decision_snapshot=self._decision_snapshot(
                        signal=sig,
                        control=control,
                        existing=existing,
                        skip_reason="pause_opening",
                    ),
                    now_ts=int(time.time()),
                )
                continue
            if sig.side == "BUY" and control.reduce_only:
                self.log.info(
                    "SKIP wallet=%s slug=%s reason=reduce-only mode",
                    sig.wallet,
                    sig.market_slug,
                )
                self._append_event(
                    "signal_skip",
                    {
                        "wallet": sig.wallet,
                        "market_slug": sig.market_slug,
                        "token_id": sig.token_id,
                        "reason": "reduce_only",
                    },
                )
                finalize_signal(
                    sig,
                    final_status="skipped",
                    decision_snapshot=self._decision_snapshot(
                        signal=sig,
                        control=control,
                        existing=existing,
                        skip_reason="reduce_only",
                    ),
                    now_ts=int(time.time()),
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
                continue

            sized_notional = self._apply_topic_profile_sizing(sig, sized_notional)
            netting_snapshot: dict[str, object] = {}
            netting_notional = sized_notional
            if sig.side == "SELL":
                notional_to_use = self._sell_target_notional(sig, existing or {}, sized_notional)
            else:
                netting_notional, netting_snapshot = self._enforce_condition_netting(sig, sized_notional)
                notional_to_use = self._enforce_buy_budget(sig, netting_notional)
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
                continue

            if self._is_order_duplicate(sig, notional_to_use):
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
                    },
                )
                self.log.info(
                    "SKIP_DUP wallet=%s slug=%s side=%s notional=%.2f",
                    sig.wallet,
                    sig.market_slug,
                    sig.side,
                    notional_to_use,
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
                    ),
                    now_ts=now,
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
                        position_action_label = "完全退出"
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
                self.log.error(
                    "FAIL wallet=%s slug=%s reason=%s",
                    sig.wallet,
                    sig.market_slug,
                    result.message,
                )

        self._refresh_risk_state()

    def run(self, once: bool = False) -> None:
        while True:
            self.step()
            if once:
                return
            time.sleep(self.settings.poll_interval_seconds)
