from __future__ import annotations

import json
import logging
import os
import time
from collections import deque
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone

from polymarket_bot.brokers.base import Broker
from polymarket_bot.clients.data_api import PolymarketDataClient
from polymarket_bot.config import Settings
from polymarket_bot.risk import RiskManager, RiskState
from polymarket_bot.strategies.wallet_follower import WalletFollowerStrategy
from polymarket_bot.types import Signal
from polymarket_bot.wallet_history import WalletHistoryStore
from polymarket_bot.wallet_scoring import RealizedWalletMetrics


@dataclass(slots=True)
class ControlState:
    pause_opening: bool = False
    reduce_only: bool = False
    emergency_stop: bool = False
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
    recent_signal_cycles: deque[dict[str, object]] = field(init=False, default_factory=lambda: deque(maxlen=24))
    positions_book: dict[str, dict[str, object]] = field(init=False, default_factory=dict)
    token_reentry_until: dict[str, int] = field(init=False, default_factory=dict)
    control_state: ControlState = field(init=False, default_factory=ControlState)
    _recent_order_keys: dict[str, float] = field(init=False, default_factory=dict)
    _last_broker_reconcile_ts: float = field(init=False, default=0.0)
    _wallet_history_store: WalletHistoryStore | None = field(init=False, default=None)
    _last_control_signature: tuple[bool, bool, bool, int] = field(
        init=False,
        default=(False, False, False, 0),
    )
    _signal_seq: int = field(init=False, default=0)
    _cycle_seq: int = field(init=False, default=0)
    _trace_seq: int = field(init=False, default=0)
    _trace_registry: dict[str, dict[str, object]] = field(init=False, default_factory=dict)
    _trace_order: deque[str] = field(init=False, default_factory=lambda: deque(maxlen=64))

    def __post_init__(self) -> None:
        self.state = RiskState()
        self.log = logging.getLogger("polybot")
        self._wallet_history_store = WalletHistoryStore(
            client=self.data_client,
            cache_path=self.settings.wallet_history_path,
            refresh_seconds=self.settings.wallet_history_refresh_seconds,
            max_wallets=self.settings.wallet_history_max_wallets,
            closed_limit=self.settings.wallet_history_closed_limit,
            resolution_limit=self.settings.wallet_history_resolution_limit,
        )
        self._reconcile_runtime_state()

    def _tracked_notional_usd(self) -> float:
        return sum(max(0.0, float(pos.get("notional") or 0.0)) for pos in self.positions_book.values())

    def _available_notional_usd(self) -> float:
        return max(0.0, self.settings.bankroll_usd - self._tracked_notional_usd())

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
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                json.dump(payload_record, f, ensure_ascii=False)
                f.write("\n")
        except Exception as exc:
            self.log.warning("Failed to append event log path=%s err=%s", path, exc)

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
        if quantity <= 0 or notional <= 0:
            return None

        return {
            "token_id": token_id,
            "market_slug": str(row.get("market_slug") or token_id),
            "outcome": str(row.get("outcome") or "YES"),
            "quantity": quantity,
            "price": self._safe_float(row.get("price"), 0.5),
            "notional": notional,
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

        self.state.open_positions = len(self.positions_book)
        return self.state.open_positions

    def _load_broker_positions(self) -> list[dict[str, object]] | None:
        wallet = (self.settings.funder_address or "").strip()
        if not wallet:
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
        recovered_risk_state: dict[str, object] | None = None
        recovered_signal_cycles: list[dict[str, object]] = []
        recovered_trace_records: list[dict[str, object]] = []

        if not self.settings.dry_run:
            positions = self._load_broker_positions()
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
                recovered = snapshot.get("positions")
                if isinstance(recovered, list):
                    positions = [r for r in recovered if isinstance(r, dict)]
                    if positions:
                        source = "snapshot"

        if recovered_risk_state is not None:
            self.state.daily_realized_pnl = self._safe_float(
                recovered_risk_state.get("daily_realized_pnl"),
                self.state.daily_realized_pnl,
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

        if positions:
            count = self._set_positions_book(positions)
            self.state.open_positions = count
            self.log.info("Recovered positions source=%s count=%d", source, count)
            self._append_event(
                "runtime_reconcile",
                {
                    "source": source,
                    "count": count,
                },
            )
            return

        self.state.open_positions = 0
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

        prev_keys = set(self.positions_book.keys())
        next_keys = set(reconciled.keys())
        removed = sorted(prev_keys - next_keys)
        added = sorted(next_keys - prev_keys)

        updated = []
        for token_id in sorted(prev_keys & next_keys):
            prev_pos = self.positions_book.get(token_id, {})
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
            "sized_notional": float(sized_notional or 0.0),
            "final_notional": float(final_notional or 0.0),
            "budget_limited": bool(budget_limited),
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
            "flow": str(order.get("flow") or ""),
            "position_action": str(order.get("position_action") or ""),
            "position_action_label": str(order.get("position_action_label") or ""),
            "reason": str(order.get("reason") or ""),
            "notional": float(order.get("notional") or 0.0),
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
            "market_slug": str(source.get("market_slug") or ""),
            "outcome": str(source.get("outcome") or ""),
            "quantity": float(source.get("quantity") or 0.0),
            "notional": float(source.get("notional") or 0.0),
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
                    "market_slug": str(pos.get("market_slug") or ""),
                    "outcome": str(pos.get("outcome") or "YES"),
                    "quantity": float(pos.get("quantity") or 0.0),
                    "price": float(pos.get("price") or 0.0),
                    "notional": float(pos.get("notional") or 0.0),
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
            "runtime_version": 1,
            "risk_state": {
                "daily_realized_pnl": float(self.state.daily_realized_pnl),
                "open_positions": int(self.state.open_positions),
            },
            "positions": positions,
            "signal_cycles": list(self.recent_signal_cycles),
            "trace_registry": self._trace_records(),
        }

    def persist_runtime_state(self, path: str) -> None:
        payload = self._dump_runtime_state()
        os.makedirs(os.path.dirname(path), exist_ok=True)
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
            updated_ts=int(payload.get("updated_ts") or 0),
        )

        signature = (
            state.pause_opening,
            state.reduce_only,
            state.emergency_stop,
            state.updated_ts,
        )
        if signature != self._last_control_signature:
            self._last_control_signature = signature
            self.log.info(
                "CONTROL pause_opening=%s reduce_only=%s emergency_stop=%s updated_ts=%d",
                state.pause_opening,
                state.reduce_only,
                state.emergency_stop,
                state.updated_ts,
            )

        self.control_state = state
        return state

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
        control = self._load_control_state()
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
            if sig.side == "SELL":
                notional_to_use = self._sell_target_notional(sig, existing or {}, sized_notional)
            else:
                notional_to_use = self._enforce_buy_budget(sig, sized_notional)
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
                        duplicate=True,
                    ),
                    now_ts=now,
                )
                continue

            result = self.broker.execute(sig, notional_to_use)
            if result.ok:
                qty = result.filled_notional / max(0.01, result.filled_price)
                now = int(time.time())
                existing = self.positions_book.get(sig.token_id)
                order_reason = self._order_reason(sig, result.message)
                position_action = sig.position_action
                position_action_label = sig.position_action_label
                position_snapshot: dict[str, object] = {}
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
                    existing["quantity"] = remaining_qty
                    existing["notional"] = remaining_notional
                    existing["price"] = result.filled_price
                    existing["last_trim_ts"] = now
                    existing["last_signal_id"] = sig.signal_id
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
                        "market_slug": sig.market_slug,
                        "outcome": sig.outcome,
                        "quantity": qty,
                        "price": result.filled_price,
                        "notional": result.filled_notional,
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
                    existing["quantity"] = new_qty
                    existing["notional"] = new_notional
                    existing["price"] = new_notional / max(0.01, new_qty)
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
                        budget_limited=notional_to_use < sized_notional - 1e-9,
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
                    },
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
                        budget_limited=notional_to_use < sized_notional - 1e-9,
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

    def run(self, once: bool = False) -> None:
        while True:
            self.step()
            if once:
                return
            time.sleep(self.settings.poll_interval_seconds)
