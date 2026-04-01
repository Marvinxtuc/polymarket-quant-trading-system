from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from polymarket_bot.admission_gate import AdmissionDecision, MODE_REDUCE_ONLY
from polymarket_bot.clients.data_api import AccountingPosition, AccountingSnapshot, ClosedPosition, MarketMetadata
from polymarket_bot.config import Settings
from polymarket_bot.reconciliation_report import load_ledger_rows
from polymarket_bot.risk import RiskManager, RiskState
from polymarket_bot.runner import (
    REASON_CANDIDATE_LIFETIME_EXPIRED,
    REASON_CROSS_WALLET_REPEAT_ENTRY_BLOCKED,
    REASON_SAME_WALLET_ADD_NOT_ALLOWED,
    Trader,
)
from polymarket_bot.state_store import StateStore
from polymarket_bot.types import BrokerOrderEvent, ExecutionResult, OpenOrderSnapshot, OrderFillSnapshot, OrderStatusSnapshot, RiskDecision, Signal
from polymarket_bot.wallet_scoring import RealizedWalletMetrics


class _DummyDataClient:
    def __init__(self):
        self.active_positions = []
        self.accounting_snapshot = None
        self.closed_positions = []
        self.order_book = SimpleNamespace(best_bid=0.48, best_ask=0.52)
        self.midpoint_price = 0.5
        self.price_history: list[dict[str, object]] = []
        self.market_metadata: dict[str, MarketMetadata] = {}

    def discover_wallet_activity(self, paths, limit):
        return {}

    def get_active_positions(self, wallet):
        return list(self.active_positions)

    def get_accounting_snapshot(self, wallet):
        return self.accounting_snapshot

    def iter_closed_positions(self, wallet, **_kwargs):
        return iter(self.closed_positions)

    def get_order_book(self, _token_id: str):
        return self.order_book

    def get_midpoint_price(self, _token_id: str):
        return self.midpoint_price

    def get_price_history(self, _token_id: str, **_kwargs):
        return list(self.price_history)

    def get_market_metadata(self, condition_id: str = "", *, slug: str | None = None):
        normalized_condition = str(condition_id or "").strip()
        if normalized_condition and normalized_condition in self.market_metadata:
            return self.market_metadata[normalized_condition]
        normalized_slug = str(slug or "").strip()
        if normalized_slug and normalized_slug in self.market_metadata:
            return self.market_metadata[normalized_slug]
        return None

    def close(self):
        return None


@dataclass
class _ActivePosition:
    wallet: str
    token_id: str
    market_slug: str
    outcome: str
    avg_price: float
    size: float
    notional: float
    timestamp: int
    condition_id: str = ""


class _DummyStrategy:
    def __init__(self, signals):
        self._signals = list(signals)
        self.selection_context = {}

    def generate_signals(self, wallets):
        return list(self._signals)

    def update_wallet_selection_context(self, context):
        self.selection_context = dict(context)


class _DummyRisk:
    def evaluate(self, signal, state):
        return RiskDecision(True, "ok", 50.0)


class _DummyBroker:
    def __init__(self):
        self.calls = []
        self.heartbeat_calls: list[list[str]] = []

    def execute(self, signal, notional_usd, *, strategy_order_uuid=None):
        self.calls.append((signal, notional_usd))
        return ExecutionResult(
            ok=True,
            broker_order_id="paper-test",
            message="ok",
            filled_notional=notional_usd,
            filled_price=max(0.01, signal.price_hint),
            status="filled",
            requested_notional=notional_usd,
            requested_price=max(0.01, signal.price_hint),
        )

    def list_open_orders(self):
        return []

    def list_recent_fills(self, *, since_ts: int = 0, order_ids: list[str] | None = None, limit: int = 200):
        return []

    def get_order_status(self, order_id: str):
        _ = order_id
        return None

    def list_order_events(self, *, since_ts: int = 0, order_ids: list[str] | None = None, limit: int = 200):
        _ = (since_ts, order_ids, limit)
        return []

    def heartbeat(self, order_ids: list[str]):
        self.heartbeat_calls.append(list(order_ids))
        return True

    def cancel_order(self, order_id: str):
        normalized = str(order_id or "").strip()
        return {
            "order_id": normalized,
            "status": "canceled",
            "ok": True,
            "message": "dummy cancel",
        }


class _RejectingBroker(_DummyBroker):
    def __init__(self, message: str):
        super().__init__()
        self.message = message

    def execute(self, signal, notional_usd, *, strategy_order_uuid=None):
        self.calls.append((signal, notional_usd))
        return ExecutionResult(
            ok=False,
            broker_order_id=None,
            message=self.message,
            filled_notional=0.0,
            filled_price=0.0,
            status="rejected",
            requested_notional=notional_usd,
            requested_price=max(0.01, signal.price_hint),
        )


class _PendingBroker(_DummyBroker):
    def __init__(self):
        super().__init__()
        self.order_statuses: dict[str, OrderStatusSnapshot] = {}
        self.heartbeat_calls: list[list[str]] = []
        self.open_orders: list[OpenOrderSnapshot] | None = []
        self.recent_fills: list[OrderFillSnapshot] | None = []
        self.order_events: list[BrokerOrderEvent] | None = None
        self.cancel_requests: list[str] = []
        self.cancel_responses: dict[str, dict[str, object]] = {}

    def execute(self, signal, notional_usd, *, strategy_order_uuid=None):
        self.calls.append((signal, notional_usd))
        return ExecutionResult(
            ok=True,
            broker_order_id=f"live-{signal.token_id}",
            message="live order posted",
            filled_notional=0.0,
            filled_price=0.0,
            status="live",
            requested_notional=notional_usd,
            requested_price=max(0.01, signal.price_hint),
        )

    def get_order_status(self, order_id: str):
        return self.order_statuses.get(order_id)

    def heartbeat(self, order_ids: list[str]):
        self.heartbeat_calls.append(list(order_ids))
        return True

    def list_open_orders(self):
        return self.open_orders

    def list_recent_fills(self, *, since_ts: int = 0, order_ids: list[str] | None = None, limit: int = 200):
        return self.recent_fills

    def list_order_events(self, *, since_ts: int = 0, order_ids: list[str] | None = None, limit: int = 200):
        return self.order_events

    def cancel_order(self, order_id: str):
        normalized = str(order_id or "").strip()
        if normalized:
            self.cancel_requests.append(normalized)
        return dict(
            self.cancel_responses.get(
                normalized,
                {
                    "order_id": normalized,
                    "status": "canceled",
                    "ok": True,
                    "message": "broker cancel simulated",
                },
            )
        )


class _FakeHistoryStore:
    def __init__(self, metrics=None, topic_profiles=None):
        self.metrics = dict(metrics or {})
        self.topic_profiles = dict(topic_profiles or {})

    def sync_wallets(self, wallets, *, max_wallets=None):
        selected = []
        limit = len(wallets) if max_wallets is None else max_wallets
        for wallet in wallets:
            key = str(wallet).strip().lower()
            if key and key not in selected:
                selected.append(key)
            if len(selected) >= limit:
                break
        metrics = {wallet: self.metrics[wallet] for wallet in selected if wallet in self.metrics}
        refreshed = {wallet: 1700000000 for wallet in selected if wallet in self.metrics}
        return metrics, refreshed, {}, {wallet: list(self.topic_profiles.get(wallet, [])) for wallet in selected if wallet in self.topic_profiles}

    def peek_wallets(self, wallets):
        selected = []
        for wallet in wallets:
            key = str(wallet).strip().lower()
            if key and key not in selected:
                selected.append(key)
        metrics = {wallet: self.metrics[wallet] for wallet in selected if wallet in self.metrics}
        refreshed = {wallet: 1700000000 for wallet in selected if wallet in self.metrics}
        return metrics, refreshed, {}, {wallet: list(self.topic_profiles.get(wallet, [])) for wallet in selected if wallet in self.topic_profiles}


class _RecordingNotifier:
    def __init__(self, *, local_available: bool = True, webhook_enabled: bool = True, telegram_enabled: bool = False):
        self.calls: list[dict[str, object]] = []
        self._local_available = bool(local_available)
        self._webhook_enabled = bool(webhook_enabled)
        self._telegram_enabled = bool(telegram_enabled)

    def local_available(self) -> bool:
        return self._local_available

    def webhook_targets(self) -> list[str]:
        if not self._webhook_enabled:
            return []
        return ["https://hooks.example.local/polymarket"]

    def telegram_available(self) -> bool:
        return self._telegram_enabled

    def notify_all(self, *, title: str, body: str, extra=None, channels=None) -> dict[str, object]:
        payload = {
            "title": str(title or ""),
            "body": str(body or ""),
            "extra": dict(extra or {}),
            "channels": list(channels or []),
        }
        self.calls.append(payload)
        return payload


def _signal(
    side: str = "BUY",
    wallet_score: float = 80.0,
    wallet_tier: str = "CORE",
    **kwargs: object,
) -> Signal:
    return Signal(
        signal_id="",
        trace_id="",
        wallet=str(kwargs.get("wallet", "0x1111111111111111111111111111111111111111")),
        market_slug=str(kwargs.get("market_slug", "demo-market")),
        token_id=str(kwargs.get("token_id", "token-demo")),
        outcome=str(kwargs.get("outcome", "YES")),
        side=side,  # type: ignore[arg-type]
        confidence=0.8,
        price_hint=0.6,
        observed_size=float(kwargs.get("observed_size", 10.0)),
        observed_notional=float(kwargs.get("observed_notional", 100.0)),
        timestamp=datetime.now(tz=timezone.utc),
        condition_id=str(kwargs.get("condition_id", "")),
        wallet_score=wallet_score,
        wallet_tier=wallet_tier,
        topic_key=str(kwargs.get("topic_key", "")),
        topic_label=str(kwargs.get("topic_label", "")),
        topic_sample_count=int(kwargs.get("topic_sample_count", 0)),
        topic_win_rate=float(kwargs.get("topic_win_rate", 0.0)),
        topic_roi=float(kwargs.get("topic_roi", 0.0)),
        topic_resolved_win_rate=float(kwargs.get("topic_resolved_win_rate", 0.0)),
        topic_score_summary=str(kwargs.get("topic_score_summary", "")),
        exit_fraction=float(kwargs.get("exit_fraction", 0.0)),
        exit_reason=str(kwargs.get("exit_reason", "")),
        cross_wallet_exit=bool(kwargs.get("cross_wallet_exit", False)),
        exit_wallet_count=int(kwargs.get("exit_wallet_count", 0)),
    )


class TraderControlTests(unittest.TestCase):
    @staticmethod
    def _order_status_from_pending(raw_order: dict[str, object]) -> str:
        status = str(raw_order.get("broker_status") or "").strip().lower()
        if status in {"cancel_requested", "canceled", "failed", "rejected", "unmatched", "filled"}:
            return status
        if status in {"partial", "partially_filled"}:
            return "partially_filled"
        return status or "posted"

    def _seed_runtime_truth_from_snapshot(self, settings: Settings) -> None:
        try:
            with open(settings.runtime_state_path, "r", encoding="utf-8") as f:
                snapshot = json.load(f)
        except Exception:
            return
        if not isinstance(snapshot, dict) or not snapshot:
            return

        store = StateStore(settings.state_store_path)
        control_payload = dict(store.load_control_state() or {})
        if not control_payload:
            control_payload = {
                "decision_mode": settings.decision_mode,
                "pause_opening": False,
                "reduce_only": False,
                "emergency_stop": False,
                "clear_stale_pending_requested_ts": 0,
                "updated_ts": 0,
            }

        runtime_payload = {
            "ts": int(snapshot.get("ts") or 0),
            "runtime_version": int(snapshot.get("runtime_version") or 0),
            "broker_event_sync_ts": int(snapshot.get("broker_event_sync_ts") or 0),
            "recent_order_keys": dict(snapshot.get("recent_order_keys") or {}),
            "signal_cycles": list(snapshot.get("signal_cycles") or []),
            "trace_registry": list(snapshot.get("trace_registry") or []),
            "last_operator_action": dict(snapshot.get("last_operator_action") or {}),
        }
        risk_payload = dict(snapshot.get("risk_state") or {})
        reconciliation_payload = dict(snapshot.get("reconciliation") or {})
        positions = [dict(row) for row in list(snapshot.get("positions") or []) if isinstance(row, dict)]

        now_ts = int(time.time())
        intents_by_id: dict[str, dict[str, object]] = {}
        for order in list(snapshot.get("pending_orders") or []):
            if not isinstance(order, dict):
                continue
            intent_id = str(order.get("signal_id") or order.get("key") or order.get("order_id") or "").strip()
            token_id = str(order.get("token_id") or "").strip()
            side = str(order.get("side") or "").strip().upper()
            if not intent_id or not token_id or side not in {"BUY", "SELL"}:
                continue
            created_ts = int(order.get("ts") or now_ts)
            updated_ts = int(order.get("last_heartbeat_ts") or created_ts)
            intents_by_id[intent_id] = {
                "intent_id": intent_id,
                "strategy_order_uuid": str(order.get("strategy_order_uuid") or ""),
                "broker_order_id": str(order.get("order_id") or ""),
                "token_id": token_id,
                "condition_id": str(order.get("condition_id") or ""),
                "side": side,
                "status": self._order_status_from_pending(order),
                "recovered_source": str(order.get("recovery_source") or "legacy_snapshot"),
                "recovery_reason": str(order.get("recovery_status") or "legacy_pending_order"),
                "payload": dict(order),
                "created_ts": created_ts,
                "updated_ts": updated_ts,
            }

        for order in list(snapshot.get("recent_orders") or []):
            if not isinstance(order, dict):
                continue
            intent_id = str(order.get("signal_id") or "").strip()
            token_id = str(order.get("token_id") or "").strip()
            side = str(order.get("side") or "").strip().upper()
            if not intent_id or intent_id in intents_by_id or not token_id or side not in {"BUY", "SELL"}:
                continue
            created_ts = int(order.get("ts") or now_ts)
            intents_by_id[intent_id] = {
                "intent_id": intent_id,
                "strategy_order_uuid": str(order.get("strategy_order_uuid") or ""),
                "broker_order_id": str(order.get("order_id") or ""),
                "token_id": token_id,
                "condition_id": str(order.get("condition_id") or ""),
                "side": side,
                "status": str(order.get("status") or "posted").strip().lower() or "posted",
                "recovered_source": "legacy_snapshot",
                "recovery_reason": "legacy_recent_order",
                "payload": dict(order),
                "created_ts": created_ts,
                "updated_ts": int(order.get("updated_ts") or now_ts),
            }

        store.save_runtime_truth(
            {
                "runtime": runtime_payload,
                "control": control_payload,
                "risk": risk_payload,
                "reconciliation": reconciliation_payload,
                "positions": positions,
                "order_intents": list(intents_by_id.values()),
            }
        )

    def _make_settings(self, control_path: str, **kwargs: object) -> Settings:
        max_signals_per_cycle = int(kwargs.get("max_signals_per_cycle", 1))
        runtime_state_path = kwargs.get("runtime_state_path")
        ledger_path = kwargs.get("ledger_path")
        candidate_db_path = kwargs.get("candidate_db_path")
        if runtime_state_path is None:
            runtime_state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
            runtime_state_file.write("{}")
            runtime_state_file.flush()
            runtime_state_file.close()
            runtime_state_path = runtime_state_file.name
        if ledger_path is None:
            ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
            ledger_file.close()
            ledger_path = ledger_file.name
        if candidate_db_path is None:
            candidate_db_path = str(Path(tempfile.mkdtemp()) / "terminal.db")

        settings = Settings(
            _env_file=None,
            dry_run=bool(kwargs.get("dry_run", True)),
            decision_mode=str(kwargs.get("decision_mode", "auto")),
            wallet_discovery_enabled=bool(kwargs.get("wallet_discovery_enabled", False)),
            wallet_discovery_mode=str(kwargs.get("wallet_discovery_mode", "union")),
            watch_wallets=str(kwargs.get("watch_wallets", "0x1111111111111111111111111111111111111111")),
            wallet_discovery_top_n=int(kwargs.get("wallet_discovery_top_n", 50)),
            wallet_discovery_min_events=int(kwargs.get("wallet_discovery_min_events", 2)),
            wallet_discovery_refresh_seconds=int(kwargs.get("wallet_discovery_refresh_seconds", 900)),
            wallet_discovery_quality_bias_enabled=bool(kwargs.get("wallet_discovery_quality_bias_enabled", True)),
            wallet_discovery_quality_top_n=int(kwargs.get("wallet_discovery_quality_top_n", 16)),
            wallet_discovery_history_bonus=float(kwargs.get("wallet_discovery_history_bonus", 0.75)),
            wallet_discovery_topic_bonus=float(kwargs.get("wallet_discovery_topic_bonus", 0.5)),
            control_path=control_path,
            runtime_state_path=str(runtime_state_path),
            ledger_path=str(ledger_path),
            candidate_db_path=str(candidate_db_path),
            max_signals_per_cycle=max_signals_per_cycle,
            poll_interval_seconds=60,
            bankroll_usd=float(kwargs.get("bankroll_usd", 5000.0)),
            account_sync_refresh_seconds=int(kwargs.get("account_sync_refresh_seconds", 300)),
            order_dedup_ttl_seconds=int(kwargs.get("order_dedup_ttl_seconds", 120)),
            runtime_reconcile_interval_seconds=int(kwargs.get("runtime_reconcile_interval_seconds", 180)),
            pending_order_timeout_seconds=int(kwargs.get("pending_order_timeout_seconds", 1800)),
            portfolio_netting_enabled=bool(kwargs.get("portfolio_netting_enabled", True)),
            max_condition_exposure_pct=float(kwargs.get("max_condition_exposure_pct", 0.015)),
            notify_local_enabled=bool(kwargs.get("notify_local_enabled", True)),
            notify_webhook_url=str(kwargs.get("notify_webhook_url", "")),
            notify_webhook_urls=str(kwargs.get("notify_webhook_urls", "")),
            notify_telegram_bot_token=str(kwargs.get("notify_telegram_bot_token", "")),
            notify_telegram_chat_id=str(kwargs.get("notify_telegram_chat_id", "")),
            critical_notification_enabled=bool(kwargs.get("critical_notification_enabled", False)),
            critical_notification_cooldown_seconds=int(kwargs.get("critical_notification_cooldown_seconds", 900)),
            min_wallet_score=float(kwargs.get("min_wallet_score", 50.0)),
            wallet_score_watch_multiplier=float(kwargs.get("wallet_score_watch_multiplier", 0.4)),
            wallet_score_trade_multiplier=float(kwargs.get("wallet_score_trade_multiplier", 0.75)),
            wallet_score_core_multiplier=float(kwargs.get("wallet_score_core_multiplier", 1.0)),
            topic_bias_enabled=bool(kwargs.get("topic_bias_enabled", True)),
            topic_min_samples=int(kwargs.get("topic_min_samples", 3)),
            topic_positive_roi=float(kwargs.get("topic_positive_roi", 0.08)),
            topic_positive_win_rate=float(kwargs.get("topic_positive_win_rate", 0.6)),
            topic_negative_roi=float(kwargs.get("topic_negative_roi", -0.02)),
            topic_negative_win_rate=float(kwargs.get("topic_negative_win_rate", 0.45)),
            topic_boost_multiplier=float(kwargs.get("topic_boost_multiplier", 1.1)),
            topic_penalty_multiplier=float(kwargs.get("topic_penalty_multiplier", 0.9)),
            wallet_exit_follow_enabled=bool(kwargs.get("wallet_exit_follow_enabled", True)),
            min_wallet_decrease_usd=float(kwargs.get("min_wallet_decrease_usd", 200.0)),
            resonance_exit_enabled=bool(kwargs.get("resonance_exit_enabled", True)),
            resonance_min_wallets=int(kwargs.get("resonance_min_wallets", 2)),
            resonance_min_wallet_score=float(kwargs.get("resonance_min_wallet_score", 65.0)),
            resonance_trim_fraction=float(kwargs.get("resonance_trim_fraction", 0.35)),
            resonance_core_exit_fraction=float(kwargs.get("resonance_core_exit_fraction", 0.6)),
            same_wallet_add_enabled=bool(kwargs.get("same_wallet_add_enabled", False)),
            same_wallet_add_allowlist=str(kwargs.get("same_wallet_add_allowlist", "")),
            live_network_smoke_max_age_seconds=int(kwargs.get("live_network_smoke_max_age_seconds", 43200)),
            live_allowance_ready=bool(kwargs.get("live_allowance_ready", False)),
            live_geoblock_ready=bool(kwargs.get("live_geoblock_ready", False)),
            live_account_ready=bool(kwargs.get("live_account_ready", False)),
            funder_address=str(kwargs.get("funder_address", "")),
        )
        try:
            with open(control_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            if isinstance(payload, dict):
                control_keys = {
                    "decision_mode",
                    "pause_opening",
                    "reduce_only",
                    "emergency_stop",
                    "clear_stale_pending_requested_ts",
                    "updated_ts",
                }
                if any(key in payload for key in control_keys):
                    normalized = {
                        "decision_mode": settings.decision_mode,
                        "pause_opening": False,
                        "reduce_only": False,
                        "emergency_stop": False,
                        "clear_stale_pending_requested_ts": 0,
                        "updated_ts": 0,
                    }
                    normalized.update(payload)
                    StateStore(settings.state_store_path).save_control_state(normalized)
        except Exception:
            pass
        self._seed_runtime_truth_from_snapshot(settings)
        return settings

    def _arm_live_trader(self, trader: Trader, *, account_snapshot_ts: int | None = None) -> None:
        snapshot_ts = int(account_snapshot_ts or time.time())
        trader.startup_ready = True
        trader.startup_warning_count = 0
        trader.startup_failure_count = 0
        trader.startup_checks = []
        trader.state.account_snapshot_ts = snapshot_ts
        trader.state.cash_balance_usd = max(100.0, float(trader.state.cash_balance_usd or 0.0))
        trader.state.equity_usd = max(trader.state.cash_balance_usd, float(trader.state.equity_usd or 0.0))
        trader._update_trading_mode(trader.control_state, now=snapshot_ts)
        trader._refresh_risk_state()

    def _restore_pending_order(
        self,
        trader: Trader,
        *,
        key: str,
        order_id: str,
        side: str,
        token_id: str = "token-demo",
        requested_notional: float = 30.0,
        requested_price: float = 0.6,
        ts: int = 1700000000,
    ) -> dict[str, object]:
        restored = trader._restore_pending_order(
            {
                "key": key,
                "ts": ts,
                "cycle_id": "cycle-demo",
                "order_id": order_id,
                "broker_status": "live",
                "signal_id": f"signal:{order_id}",
                "trace_id": f"trace:{order_id}",
                "token_id": token_id,
                "condition_id": "condition-demo",
                "market_slug": "demo-market",
                "outcome": "YES",
                "side": side,
                "wallet": "0x1111111111111111111111111111111111111111",
                "wallet_score": 80.0,
                "wallet_tier": "CORE",
                "requested_notional": requested_notional,
                "requested_price": requested_price,
                "matched_notional_hint": 0.0,
                "matched_size_hint": 0.0,
                "matched_price_hint": requested_price,
                "reconciled_notional_hint": 0.0,
                "reconciled_size_hint": 0.0,
                "last_fill_ts_hint": 0,
                "last_fill_tx_hash": "",
                "message": "restored for test",
                "reason": "restored for test",
            }
        )
        self.assertIsNotNone(restored)
        return dict(restored or {})

    def test_pause_opening_blocks_buy_signal(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": True,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()
        self.assertEqual(len(broker.calls), 0)

    def test_emergency_stop_sells_existing_positions(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": True,
                    "updated_ts": 2,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 60.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertEqual(len(broker.calls), 1)
        self.assertEqual(broker.calls[0][0].side, "SELL")
        self.assertEqual(trader.state.open_positions, 0)
        self.assertNotIn("token-demo", trader.positions_book)

    def test_startup_gate_blocks_new_buy_when_startup_ready_false(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 3,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.startup_ready = False
        trader.startup_failure_count = 1
        trader.startup_checks = [{"name": "startup", "status": "FAIL", "message": "startup failed"}]
        trader.state.account_snapshot_ts = int(time.time())
        trader.state.cash_balance_usd = 100.0

        trader.step()

        self.assertEqual(len(broker.calls), 0)
        self.assertEqual(trader.trading_mode, "REDUCE_ONLY")
        self.assertIn("startup_not_ready", trader.trading_mode_reasons)

    def test_startup_gate_allows_reduce_only_sell_when_startup_ready_false(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 4,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("SELL")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.startup_ready = False
        trader.startup_failure_count = 1
        trader.startup_checks = [{"name": "startup", "status": "FAIL", "message": "startup failed"}]
        trader.state.account_snapshot_ts = int(time.time())
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 60.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
            "entry_wallet": "0x1111111111111111111111111111111111111111",
            "entry_wallet_score": 80.0,
            "entry_wallet_tier": "CORE",
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertEqual(len(broker.calls), 1)
        self.assertEqual(broker.calls[0][0].side, "SELL")
        self.assertNotIn("token-demo", trader.positions_book)

    def test_admission_gate_blocks_buy_even_when_legacy_mode_fields_show_normal(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 44,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)

        now_ts = int(time.time())
        trader._admission_decision = AdmissionDecision(
            mode=MODE_REDUCE_ONLY,
            opening_allowed=False,
            reduce_only=True,
            halted=False,
            auto_recover=False,
            manual_confirmation_required=True,
            reason_codes=("startup_checks_fail",),
            action_whitelist=("sync_read", "state_evaluation", "cancel_pending_buy", "persist_state_update"),
            latch_kind="manual",
            trusted=False,
            trusted_consecutive_cycles=0,
            evidence_summary={
                "startup_ready": False,
                "reconciliation_status": "ok",
                "account_snapshot_age_seconds": 0,
                "broker_event_sync_age_seconds": 0,
                "ledger_diff": 0.0,
                "persistence_status": "ok",
            },
            evaluated_ts=now_ts,
            auto_latch_active=False,
            manual_latch_active=True,
        )
        trader.admission_opening_allowed = False
        trader.admission_reduce_only = True
        trader.admission_halted = False

        # Deliberately forge legacy compatibility fields to NORMAL; admission gate must still block BUY.
        trader.trading_mode = "NORMAL"
        trader.trading_mode_reasons = []
        trader.trading_mode_updated_ts = now_ts

        def _noop_update_trading_mode(self, control, *, now=None, reconciliation=None):
            return self.trading_mode_state()

        with patch.object(Trader, "_update_trading_mode", _noop_update_trading_mode):
            trader.step()

        self.assertEqual(len(broker.calls), 0)
        self.assertGreaterEqual(len(trader.recent_signal_cycles), 1)
        latest_cycle = trader.recent_signal_cycles[0]
        candidate = latest_cycle["candidates"][0]
        self.assertEqual(candidate["final_status"], "skipped")
        self.assertEqual(candidate["decision_snapshot"]["skip_reason"], "startup_not_ready")

    def test_runtime_degradation_reconciliation_fail_blocks_buy(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 45,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY", token_id="token-reconcile-fail", market_slug="reconcile-fail")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)

        def _forced_reconciliation_summary(self, *, now=None):
            return {
                "day_key": "1970-01-01",
                "status": "fail",
                "issues": ["forced_reconciliation_fail"],
                "recovery_conflicts": [],
                "startup_ready": True,
                "internal_realized_pnl": 0.0,
                "ledger_realized_pnl": 0.0,
                "broker_closed_pnl_today": 0.0,
                "effective_daily_realized_pnl": 0.0,
                "internal_vs_ledger_diff": 0.0,
                "broker_floor_gap_vs_internal": 0.0,
                "fill_count_today": 0,
                "fill_notional_today": 0.0,
                "account_sync_count_today": 0,
                "startup_checks_count_today": 0,
                "last_fill_ts": 0,
                "last_account_sync_ts": 0,
                "last_startup_checks_ts": 0,
                "pending_orders": 0,
                "pending_entry_orders": 0,
                "pending_exit_orders": 0,
                "stale_pending_orders": 0,
                "ambiguous_pending_orders": 0,
                "open_positions": 0,
                "tracked_notional_usd": 0.0,
                "ledger_available": True,
                "account_snapshot_age_seconds": 0,
                "broker_reconcile_age_seconds": 0,
                "broker_event_sync_age_seconds": 0,
            }

        with patch.object(Trader, "reconciliation_summary", _forced_reconciliation_summary):
            trader.step()

        self.assertEqual(len(broker.calls), 0)
        self.assertEqual(trader.trading_mode, "REDUCE_ONLY")
        self.assertIn("reconciliation_fail", trader.trading_mode_reasons)

    def test_runtime_degradation_stale_event_stream_blocks_buy(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 46,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                poll_interval_seconds=30,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY", token_id="token-event-stale", market_slug="event-stale")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)
        now = int(time.time())
        restored = self._restore_pending_order(
            trader,
            key="pending-event-stale",
            order_id="oid-event-stale",
            side="BUY",
            token_id="token-pending-event",
            ts=now,
        )
        trader.pending_orders[restored["key"]] = restored
        trader._last_broker_event_sync_ts = now - 1000
        trader.state.account_snapshot_ts = now

        trader.step()

        self.assertEqual(len(broker.calls), 0)
        self.assertEqual(trader.trading_mode, "REDUCE_ONLY")
        self.assertIn("broker_event_stream_stale", trader.trading_mode_reasons)

    def test_runtime_degradation_ledger_diff_exceeded_blocks_buy(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 47,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY", token_id="token-ledger-diff", market_slug="ledger-diff")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)

        def _forced_reconciliation_summary(self, *, now=None):
            return {
                "day_key": "1970-01-01",
                "status": "ok",
                "issues": [],
                "recovery_conflicts": [],
                "startup_ready": True,
                "internal_realized_pnl": 0.0,
                "ledger_realized_pnl": 0.0,
                "broker_closed_pnl_today": 0.0,
                "effective_daily_realized_pnl": 0.0,
                "internal_vs_ledger_diff": 5.0,
                "broker_floor_gap_vs_internal": 0.0,
                "fill_count_today": 0,
                "fill_notional_today": 0.0,
                "account_sync_count_today": 0,
                "startup_checks_count_today": 0,
                "last_fill_ts": 0,
                "last_account_sync_ts": 0,
                "last_startup_checks_ts": 0,
                "pending_orders": 0,
                "pending_entry_orders": 0,
                "pending_exit_orders": 0,
                "stale_pending_orders": 0,
                "ambiguous_pending_orders": 0,
                "open_positions": 0,
                "tracked_notional_usd": 0.0,
                "ledger_available": True,
                "account_snapshot_age_seconds": 0,
                "broker_reconcile_age_seconds": 0,
                "broker_event_sync_age_seconds": 0,
            }

        with patch.object(Trader, "reconciliation_summary", _forced_reconciliation_summary):
            trader.step()

        self.assertEqual(len(broker.calls), 0)
        self.assertEqual(trader.trading_mode, "REDUCE_ONLY")
        self.assertIn("ledger_diff_exceeded", trader.trading_mode_reasons)

    def test_halted_mode_whitelist_blocks_regular_sell_execution(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 48,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("SELL", token_id="token-halted", market_slug="halted-market")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)
        trader.positions_book["token-halted"] = {
            "token_id": "token-halted",
            "condition_id": "condition-halted",
            "market_slug": "halted-market",
            "outcome": "YES",
            "quantity": 10.0,
            "price": 0.6,
            "notional": 6.0,
            "cost_basis_notional": 6.0,
            "opened_ts": int(time.time()) - 600,
            "last_buy_ts": int(time.time()) - 600,
            "last_trim_ts": 0,
            "entry_wallet": "0x1111111111111111111111111111111111111111",
            "entry_wallet_score": 80.0,
            "entry_wallet_tier": "CORE",
        }
        trader._refresh_risk_state()
        trader.record_external_persistence_fault("daemon_state_write", "/tmp/state.json", OSError("disk full"))

        trader.step()

        self.assertEqual(trader.trading_mode, "HALTED")
        self.assertEqual(len(broker.calls), 0)
        self.assertIn("token-halted", trader.positions_book)

    def test_reduce_only_blocks_retry_like_buy_resubmission(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 49,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        sig = _signal("BUY", token_id="token-retry-block", market_slug="retry-block")
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([sig]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)
        identity = trader._build_intent_identity(sig, 50.0)
        claim_status, _intent = trader._claim_or_load_intent(signal=sig, notional_usd=50.0, identity=identity)
        self.assertIn(claim_status, {"CLAIMED_NEW", "EXISTING_NON_TERMINAL"})
        updated, _ = trader._set_intent_status(
            strategy_order_uuid=str(identity.get("strategy_order_uuid") or ""),
            idempotency_key=str(identity.get("idempotency_key") or ""),
            status="ack_unknown",
        )
        self.assertTrue(updated)

        StateStore(trader.settings.state_store_path).save_control_state(
            {
                "decision_mode": trader.control_state.decision_mode,
                "pause_opening": False,
                "reduce_only": True,
                "emergency_stop": False,
                "clear_stale_pending_requested_ts": 0,
                "updated_ts": 50,
            }
        )

        trader.step()

        self.assertEqual(len(broker.calls), 0)
        persisted = StateStore(trader.settings.state_store_path).load_intent_by_idempotency_key(
            str(identity.get("idempotency_key") or "")
        )
        self.assertIsNotNone(persisted)
        self.assertEqual(str(persisted.status), "ack_unknown")

    def test_persisted_admission_snapshot_does_not_override_fresh_evidence(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 51,
                },
                f,
            )
            control_path = f.name

        settings = self._make_settings(control_path)
        store = StateStore(settings.state_store_path)
        store.save_runtime_truth(
            {
                "runtime": {
                    "admission": {
                        "mode": "HALTED",
                        "opening_allowed": False,
                        "reason_codes": ["persistence_fault"],
                    }
                },
                "control": {
                    "decision_mode": "auto",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "clear_stale_pending_requested_ts": 0,
                    "updated_ts": 0,
                },
                "risk": {},
                "reconciliation": {"status": "ok"},
                "positions": [],
                "order_intents": [],
            }
        )

        trader = Trader(
            settings=settings,
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )
        self._arm_live_trader(trader)

        self.assertEqual(trader.trading_mode, "NORMAL")
        self.assertNotIn("persistence_fault", trader.trading_mode_reasons)

    def test_insufficient_bootstrap_evidence_keeps_opening_blocked(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 52,
                },
                f,
            )
            control_path = f.name

        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                live_allowance_ready=True,
                live_geoblock_ready=True,
                live_account_ready=True,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        trader.startup_ready = True
        trader.startup_failure_count = 0
        trader.state.account_snapshot_ts = 0
        trader._admission_bootstrap_protected = True
        trader._update_trading_mode(
            trader.control_state,
            reconciliation={
                "status": "ok",
                "issues": [],
                "ambiguous_pending_orders": 0,
                "account_snapshot_age_seconds": 0,
                "broker_event_sync_age_seconds": 0,
                "internal_vs_ledger_diff": 0.0,
            },
        )

        self.assertEqual(trader.trading_mode, "REDUCE_ONLY")
        self.assertIn("bootstrap_protected_evidence_missing", trader.trading_mode_reasons)

    def test_pending_buy_does_not_create_position_before_reconcile(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 21,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()

        self.assertEqual(len(broker.calls), 1)
        self.assertNotIn("token-demo", trader.positions_book)
        self.assertEqual(trader.state.open_positions, 0)
        self.assertEqual(trader.recent_orders[0]["status"], "PENDING")
        self.assertEqual(len(trader.pending_orders), 1)

    def test_pending_sell_keeps_position_until_reconcile(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 22,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy(
                [
                    _signal(
                        "SELL",
                        exit_fraction=1.0,
                        exit_reason="source wallet exit",
                    )
                ]
            ),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 60.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
            "entry_wallet": "0x1111111111111111111111111111111111111111",
            "entry_wallet_score": 80.0,
            "entry_wallet_tier": "CORE",
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertEqual(len(broker.calls), 1)
        self.assertIn("token-demo", trader.positions_book)
        self.assertEqual(trader.state.open_positions, 1)
        self.assertEqual(trader.recent_orders[0]["status"], "PENDING")
        self.assertEqual(len(trader.pending_orders), 1)

    def test_broker_reconcile_promotes_pending_buy_into_runtime_position(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 23,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        data_client = _DummyDataClient()
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=data_client,
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)

        trader.step()
        self.assertEqual(len(trader.pending_orders), 1)

        data_client.active_positions = [
            _ActivePosition(
                wallet="0xabc",
                token_id="token-demo",
                market_slug="demo-market",
                outcome="YES",
                avg_price=0.6,
                size=50.0,
                notional=30.0,
                timestamp=1700000100,
            )
        ]

        trader._reconcile_runtime_with_broker()

        self.assertIn("token-demo", trader.positions_book)
        self.assertEqual(trader.positions_book["token-demo"]["entry_wallet"], "0x1111111111111111111111111111111111111111")
        self.assertEqual(trader.positions_book["token-demo"]["trace_id"][:4], "trc-")
        self.assertEqual(trader.state.open_positions, 1)
        self.assertEqual(len(trader.pending_orders), 1)
        self.assertEqual(trader.recent_orders[0]["status"], "PARTIAL")
        self.assertEqual(broker.heartbeat_calls[-1], ["live-token-demo"])

    def test_broker_terminal_cancel_clears_pending_order(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 24,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)

        trader.step()
        self.assertEqual(len(trader.pending_orders), 1)
        broker.order_statuses["live-token-demo"] = OrderStatusSnapshot(
            order_id="live-token-demo",
            status="canceled",
            message="maker order canceled",
        )

        trader._reconcile_runtime_with_broker()

        self.assertEqual(len(trader.pending_orders), 0)
        self.assertEqual(trader.recent_orders[0]["status"], "CANCELED")
        self.assertEqual(trader.state.pending_entry_notional_usd, 0.0)

    def test_pending_order_heartbeat_ts_not_updated_when_broker_heartbeat_noops(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 24,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()

        def _noop_heartbeat(order_ids: list[str]):
            broker.heartbeat_calls.append(list(order_ids))
            return False

        broker.heartbeat = _noop_heartbeat  # type: ignore[assignment]
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)

        trader.step()
        self.assertEqual(len(trader.pending_orders), 1)
        trader._reconcile_runtime_with_broker()

        pending = next(iter(trader.pending_orders.values()))
        self.assertEqual(broker.heartbeat_calls[-1], ["live-token-demo"])
        self.assertEqual(int(pending.get("last_heartbeat_ts") or 0), 0)

    def test_operator_clear_stale_pending_removes_only_stale_orders(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 30,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                pending_order_timeout_seconds=60,
                runtime_reconcile_interval_seconds=60,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)

        trader.step()
        self.assertEqual(len(trader.pending_orders), 1)
        old_key = next(iter(trader.pending_orders.keys()))
        trader.pending_orders[old_key]["ts"] = int(time.time()) - 180

        stale_order = dict(trader.pending_orders[old_key])
        fresh_order = dict(stale_order)
        fresh_order["key"] = "fresh-live-token-demo"
        fresh_order["order_id"] = "live-token-demo-fresh"
        fresh_order["signal_id"] = "sig-fresh"
        fresh_order["trace_id"] = "trc-fresh"
        fresh_order["token_id"] = "token-demo-fresh"
        fresh_order["market_slug"] = "demo-market-fresh"
        fresh_order["ts"] = int(time.time())
        trader.pending_orders[fresh_order["key"]] = fresh_order
        trader._refresh_risk_state()
        trader.strategy = _DummyStrategy([])

        StateStore(trader.settings.state_store_path).save_control_state(
            {
                "decision_mode": trader.control_state.decision_mode,
                "pause_opening": False,
                "reduce_only": False,
                "emergency_stop": False,
                "clear_stale_pending_requested_ts": int(time.time()),
                "updated_ts": 31,
            }
        )

        trader.step()

        self.assertEqual(len(trader.pending_orders), 0)
        self.assertEqual(trader.recent_orders[0]["status"], "CANCELED")
        self.assertIn("trading_mode_cancel_pending_entry", trader.recent_orders[0]["reason"])
        self.assertIn("operator_clear_stale_pending", trader.recent_orders[1]["reason"])
        self.assertEqual(broker.cancel_requests, ["live-token-demo", "live-token-demo-fresh"])
        self.assertEqual(trader.last_operator_action["name"], "clear_stale_pending")
        self.assertEqual(trader.last_operator_action["status"], "cleared")
        self.assertEqual(trader.last_operator_action["cleared_count"], 1)
        self.assertEqual(trader.last_operator_action["remaining_pending_orders"], 1)
        self.assertAlmostEqual(trader.state.pending_entry_notional_usd, 0.0, places=4)

    def test_reduce_only_cancels_pending_buy_orders(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 32,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)

        trader.step()
        self.assertEqual(len(trader.pending_orders), 1)

        StateStore(trader.settings.state_store_path).save_control_state(
            {
                "decision_mode": trader.control_state.decision_mode,
                "pause_opening": False,
                "reduce_only": True,
                "emergency_stop": False,
                "clear_stale_pending_requested_ts": 0,
                "updated_ts": 33,
            }
        )

        trader.strategy = _DummyStrategy([])
        trader.step()

        self.assertEqual(len(trader.pending_orders), 0)
        self.assertEqual(broker.cancel_requests, ["live-token-demo"])
        self.assertEqual(trader.recent_orders[0]["status"], "CANCELED")
        self.assertIn("kill_switch_reduce_only", trader.recent_orders[0]["reason"])

    def test_system_reduce_only_cancels_pending_buy_orders(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 34,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)

        trader.step()
        self.assertEqual(len(trader.pending_orders), 1)

        trader.strategy = _DummyStrategy([])
        trader.startup_ready = False
        trader.startup_failure_count = 1
        trader.startup_checks = [{"name": "startup", "status": "FAIL", "message": "startup failed"}]
        trader.step()

        self.assertEqual(len(trader.pending_orders), 0)
        self.assertEqual(broker.cancel_requests, ["live-token-demo"])
        self.assertEqual(trader.recent_orders[0]["status"], "CANCELED")
        self.assertIn("trading_mode_cancel_pending_entry", trader.recent_orders[0]["reason"])

    def test_pending_timeout_requests_cancel_and_keeps_order_when_cancel_not_terminal(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 34,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                pending_order_timeout_seconds=60,
                runtime_reconcile_interval_seconds=60,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)

        trader.step()
        order_key = next(iter(trader.pending_orders.keys()))
        trader.pending_orders[order_key]["ts"] = int(time.time()) - 180
        broker.cancel_responses["live-token-demo"] = {
            "order_id": "live-token-demo",
            "status": "requested",
            "ok": True,
            "message": "cancel queued",
        }

        trader._reconcile_runtime_with_broker()

        self.assertEqual(len(trader.pending_orders), 1)
        self.assertEqual(broker.cancel_requests, ["live-token-demo"])
        self.assertEqual(str(trader.pending_orders[order_key]["broker_status"]), "cancel_requested")
        self.assertEqual(trader.recent_orders[0]["status"], "CANCEL_REQUESTED")
        self.assertIn("pending_order_timeout", trader.recent_orders[0]["reason"])

    def test_pending_entry_exposure_flows_into_risk_state(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 25,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                bankroll_usd=100.0,
                portfolio_netting_enabled=False,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()

        self.assertEqual(trader.state.pending_entry_orders, 1)
        self.assertAlmostEqual(trader.state.pending_entry_notional_usd, 50.0, places=4)
        self.assertAlmostEqual(trader.state.committed_notional_usd, 50.0, places=4)

    def test_buy_signal_skipped_when_budget_exhausted(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 3,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, bankroll_usd=100.0),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 6.0,
            "price": 0.6,
            "notional": 96.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertEqual(len(broker.calls), 0)

    def test_condition_netting_allows_small_clip_below_legacy_five_usd_floor(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 26,
                },
                f,
            )
            control_path = f.name

        trader = Trader(
            settings=self._make_settings(
                control_path,
                bankroll_usd=100.0,
                max_condition_exposure_pct=0.015,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        allowed, snapshot = trader._enforce_condition_netting(
            _signal("BUY", token_id="token-small-cap", market_slug="small-cap", condition_id="condition-small"),
            50.0,
        )

        self.assertAlmostEqual(allowed, 1.5, places=4)
        self.assertAlmostEqual(float(snapshot["condition_exposure_cap_usd"]), 1.5, places=4)

    def test_buy_signal_uses_remaining_cash_below_legacy_five_usd_floor(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 27,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                bankroll_usd=100.0,
                portfolio_netting_enabled=False,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY", token_id="token-low-cash", market_slug="low-cash-market")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.state.cash_balance_usd = 1.8756
        trader.state.account_snapshot_ts = int(time.time())

        trader.step()

        self.assertEqual(len(broker.calls), 1)
        self.assertAlmostEqual(broker.calls[0][1], 1.8756, places=4)

    def test_time_exit_closes_full_position_below_close_threshold(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 28,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                stale_position_minutes=5,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=broker,
        )
        now_ts = int(time.time())
        trader.positions_book["token-stale-small"] = {
            "token_id": "token-stale-small",
            "condition_id": "condition-stale-small",
            "market_slug": "stale-small-market",
            "outcome": "YES",
            "quantity": 9.98,
            "price": 0.5,
            "notional": 4.99,
            "cost_basis_notional": 4.99,
            "opened_ts": now_ts - 3600,
            "last_buy_ts": now_ts - 3600,
            "last_trim_ts": 0,
            "entry_wallet": "0xold",
            "entry_wallet_score": 75.0,
            "entry_wallet_tier": "CORE",
            "entry_topic_label": "",
            "entry_topic_bias": "neutral",
            "entry_topic_multiplier": 1.0,
            "entry_topic_summary": "",
            "entry_reason": "stale position",
            "trace_id": "trc-stale-small",
            "origin_signal_id": "sig-stale-small",
            "last_signal_id": "sig-stale-small",
            "last_exit_kind": "",
            "last_exit_label": "",
            "last_exit_summary": "",
            "last_exit_ts": 0,
        }
        trader.state.open_positions = 1

        trader._apply_time_exit()

        self.assertEqual(len(broker.calls), 1)
        self.assertAlmostEqual(broker.calls[0][1], 4.99, places=4)
        self.assertNotIn("token-stale-small", trader.positions_book)

    def test_time_exit_retires_position_when_orderbook_is_missing_and_account_snapshot_cleared(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 2801,
                },
                f,
            )
            control_path = f.name

        data_client = _DummyDataClient()
        data_client.accounting_snapshot = AccountingSnapshot(
            wallet="0xabc",
            cash_balance=10.8356,
            positions_value=0.0,
            equity=10.8356,
            valuation_time="2026-03-20T06:47:12Z",
            positions=(),
        )
        broker = _RejectingBroker(
            "live preflight failed: Client error '404 Not Found' for url 'https://clob.polymarket.com/book?token_id=token-stale-closed'"
        )
        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                stale_position_minutes=5,
            ),
            data_client=data_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=broker,
        )
        now_ts = int(time.time())
        trader.positions_book["token-stale-closed"] = {
            "token_id": "token-stale-closed",
            "condition_id": "condition-stale-closed",
            "market_slug": "stale-closed-market",
            "outcome": "YES",
            "quantity": 8.96,
            "price": 0.89,
            "notional": 7.9744,
            "cost_basis_notional": 7.9744,
            "opened_ts": now_ts - 3600,
            "last_buy_ts": now_ts - 3600,
            "last_trim_ts": 0,
            "entry_wallet": "0xold",
            "entry_wallet_score": 75.0,
            "entry_wallet_tier": "CORE",
            "entry_topic_label": "",
            "entry_topic_bias": "neutral",
            "entry_topic_multiplier": 1.0,
            "entry_topic_summary": "",
            "entry_reason": "resolved position",
            "trace_id": "trc-stale-closed",
            "origin_signal_id": "sig-stale-closed",
            "last_signal_id": "sig-stale-closed",
            "last_exit_kind": "",
            "last_exit_label": "",
            "last_exit_summary": "",
            "last_exit_ts": 0,
        }
        trader.state.open_positions = 1

        trader._apply_time_exit()

        self.assertEqual(len(broker.calls), 1)
        self.assertNotIn("token-stale-closed", trader.positions_book)
        self.assertEqual(trader.state.open_positions, 0)
        self.assertAlmostEqual(trader.state.cash_balance_usd, 10.8356, places=4)
        self.assertAlmostEqual(trader.state.positions_value_usd, 0.0, places=4)
        self.assertEqual(trader.recent_orders[0]["final_status"] if "final_status" in trader.recent_orders[0] else trader.recent_orders[0]["status"], "REJECTED")
        self.assertIn("time-exit failed", trader.recent_orders[0]["reason"])

    def test_pending_buy_reserves_cash_for_following_buys(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 6,
                },
                f,
            )
            control_path = f.name

        signal_one = _signal("BUY", wallet_score=90.0, market_slug="market-one", token_id="token-one")
        signal_one.signal_id = "sig-one"
        signal_one.trace_id = "tr-one"
        signal_two = _signal("BUY", wallet_score=90.0, market_slug="market-two", token_id="token-two")
        signal_two.signal_id = "sig-two"
        signal_two.trace_id = "tr-two"

        settings = self._make_settings(
            control_path,
            bankroll_usd=100.0,
            max_signals_per_cycle=2,
            portfolio_netting_enabled=False,
        )
        settings.risk_per_trade_pct = 0.05
        broker = _PendingBroker()
        trader = Trader(
            settings=settings,
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([signal_one, signal_two]),
            risk=RiskManager(settings),
            broker=broker,
        )
        trader.state.cash_balance_usd = 9.0
        trader.state.account_snapshot_ts = 1700000000
        trader._refresh_risk_state()

        trader.step()

        self.assertEqual(len(broker.calls), 2)
        self.assertAlmostEqual(broker.calls[0][1], 7.5, places=4)
        self.assertAlmostEqual(broker.calls[1][1], 1.5, places=4)
        self.assertAlmostEqual(trader.state.pending_entry_notional_usd, 9.0, places=4)
        self.assertAlmostEqual(trader._available_notional_usd(), 0.0, places=4)
        queued = trader.list_candidates(limit=10)
        submitted = [row for row in queued if row.get("status") == "submitted"]
        skipped_budget = [row for row in queued if row.get("result_tag") == "insufficient_budget"]
        self.assertEqual(len(submitted), 2)
        self.assertEqual(len(skipped_budget), 0)

    def test_auto_mode_keeps_watch_candidate_out_of_execution_plans(self):
        class _WideSpreadDataClient(_DummyDataClient):
            def __init__(self):
                super().__init__()
                self.order_book = SimpleNamespace(best_bid=0.01, best_ask=0.99)
                self.midpoint_price = 0.945

        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "auto",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 41,
                },
                f,
            )
            control_path = f.name

        signal = _signal(
            "BUY",
            wallet_score=90.73,
            wallet_tier="CORE",
            market_slug="xrp-above-1pt6-on-march-20",
            token_id="token-watch-only",
            outcome="No",
            observed_notional=189.89,
            observed_size=199.88,
        )
        signal.confidence = 0.687978416198392
        signal.price_hint = 0.946772

        settings = self._make_settings(
            control_path,
            decision_mode="auto",
            max_signals_per_cycle=1,
        )
        broker = _DummyBroker()
        trader = Trader(
            settings=settings,
            data_client=_WideSpreadDataClient(),
            strategy=_DummyStrategy([signal]),
            risk=RiskManager(settings),
            broker=broker,
        )

        trader.step()

        queued = trader.list_candidates(limit=4)
        self.assertEqual(len(broker.calls), 0)
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0]["status"], "watched")
        self.assertEqual(queued[0]["suggested_action"], "watch")
        self.assertIsNone(queued[0].get("result_tag"))

    def test_live_order_refreshes_account_snapshot_before_following_buy(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 7,
                },
                f,
            )
            control_path = f.name

        smoke_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        smoke_file.write(
            json.dumps(
                {
                    "ts": int(time.time()),
                    "summary": {
                        "failures": 0,
                        "blocks": 0,
                        "warnings": 0,
                        "exit_code": 0,
                    },
                }
            )
        )
        smoke_file.write("\n")
        smoke_file.flush()
        smoke_file.close()

        signal_one = _signal("BUY", wallet_score=96.5, wallet_tier="CORE", market_slug="market-one", token_id="token-one")
        signal_two = _signal("BUY", wallet_score=96.5, wallet_tier="CORE", market_slug="market-two", token_id="token-two")

        settings = self._make_settings(
            control_path,
            dry_run=False,
            decision_mode="auto",
            bankroll_usd=100.0,
            max_signals_per_cycle=2,
            portfolio_netting_enabled=False,
            funder_address="0xabc",
            live_allowance_ready=True,
            live_geoblock_ready=True,
            live_account_ready=True,
            account_sync_refresh_seconds=300,
        )
        settings.risk_per_trade_pct = 0.05

        data_client = _DummyDataClient()
        data_client.accounting_snapshot = AccountingSnapshot(
            wallet="0xabc",
            cash_balance=12.0,
            positions_value=0.0,
            equity=12.0,
            valuation_time="2026-03-19T13:43:00Z",
        )

        class _BalanceUpdatingPendingBroker(_PendingBroker):
            def __init__(self, client):
                super().__init__()
                self._client = client

            def execute(self, signal, notional_usd, *, strategy_order_uuid=None):
                if not self.calls:
                    self._client.accounting_snapshot = AccountingSnapshot(
                        wallet="0xabc",
                        cash_balance=1.0,
                        positions_value=7.5,
                        equity=8.5,
                        valuation_time="2026-03-19T13:43:40Z",
                    )
                return super().execute(signal, notional_usd, strategy_order_uuid=strategy_order_uuid)

        broker = _BalanceUpdatingPendingBroker(data_client)
        previous = {
            key: os.environ.get(key)
            for key in ("NETWORK_SMOKE_LOG", "LIVE_ALLOWANCE_READY", "LIVE_GEOBLOCK_READY", "LIVE_ACCOUNT_READY")
        }
        os.environ["NETWORK_SMOKE_LOG"] = smoke_file.name
        os.environ["LIVE_ALLOWANCE_READY"] = "true"
        os.environ["LIVE_GEOBLOCK_READY"] = "true"
        os.environ["LIVE_ACCOUNT_READY"] = "true"
        try:
            trader = Trader(
                settings=settings,
                data_client=data_client,
                strategy=_DummyStrategy([signal_one, signal_two]),
                risk=RiskManager(settings),
                broker=broker,
            )
            self._arm_live_trader(trader)

            trader.step()
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertEqual(len(broker.calls), 1)
        self.assertAlmostEqual(trader.state.cash_balance_usd, 1.0, places=4)
        self.assertAlmostEqual(trader.state.pending_entry_notional_usd, broker.calls[0][1], places=4)
        queued = trader.list_candidates(limit=10)
        submitted = [row for row in queued if row.get("status") == "submitted"]
        skipped_budget = [row for row in queued if row.get("result_tag") == "insufficient_budget"]
        self.assertEqual(len(submitted), 1)
        self.assertEqual(len(skipped_budget), 1)
        self.assertIn(skipped_budget[0].get("market_slug"), {"market-one", "market-two"})

    def test_duplicate_buy_signal_is_debounced(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 4,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, max_signals_per_cycle=2, order_dedup_ttl_seconds=120),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY"), _signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.step()
        self.assertEqual(len(broker.calls), 1)

    def test_pending_buy_blocks_duplicate_even_after_ttl_expires(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 41,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                order_dedup_ttl_seconds=1,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)

        trader.step()
        self.assertEqual(len(broker.calls), 1)
        self.assertEqual(len(trader.pending_orders), 1)

        for key in list(trader._recent_order_keys.keys()):
            trader._recent_order_keys[key] = time.time() - 1
        trader.strategy = _DummyStrategy([_signal("BUY")])

        trader.step()

        self.assertEqual(len(broker.calls), 1)
        self.assertEqual(len(trader.pending_orders), 1)

    def test_runtime_snapshot_recent_order_keys_are_advisory_not_idempotency_truth(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 42,
                },
                f,
            )
            control_path = f.name

        probe_signal = _signal("BUY")
        probe_broker = _DummyBroker()
        probe_trader = Trader(
            settings=self._make_settings(control_path, order_dedup_ttl_seconds=120),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=probe_broker,
        )
        order_key = probe_trader._build_order_key(probe_signal, 50.0)
        runtime_state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "recent_order_keys": {
                    order_key: time.time() + 120,
                }
            },
            runtime_state_file,
        )
        runtime_state_file.flush()
        runtime_state_file.close()

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                runtime_state_path=runtime_state_file.name,
                order_dedup_ttl_seconds=120,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()
        self.assertEqual(len(broker.calls), 1)

        trader.step()
        self.assertEqual(len(broker.calls), 1)

    def test_wallet_score_trade_tier_reduces_order_size(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 5,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, wallet_score_trade_multiplier=0.75),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY", wallet_score=72.0, wallet_tier="TRADE")]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()
        self.assertEqual(len(broker.calls), 1)
        self.assertEqual(broker.calls[0][1], 37.5)
        self.assertTrue(trader.last_signals[0].signal_id.startswith("sig-"))
        self.assertTrue(trader.last_signals[0].trace_id.startswith("trc-"))
        self.assertEqual(trader.recent_signal_cycles[0]["candidates"][0]["final_status"], "filled")
        self.assertTrue(str(trader.recent_orders[0]["trace_id"]).startswith("trc-"))

    def test_wallet_score_below_min_is_skipped(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 6,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, min_wallet_score=50.0),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY", wallet_score=45.0, wallet_tier="LOW")]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()
        self.assertEqual(len(broker.calls), 0)

    def test_topic_profile_boost_increases_order_size(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 7,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, topic_boost_multiplier=1.1),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([
                _signal(
                    "BUY",
                    wallet_score=82.0,
                    wallet_tier="CORE",
                    market_slug="will-btc-close-above-100k",
                    topic_key="crypto",
                    topic_label="加密",
                    topic_sample_count=6,
                    topic_win_rate=0.72,
                    topic_roi=0.16,
                    topic_score_summary="加密 | 6 samples | roi +16% | win 72%",
                )
            ]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()
        self.assertEqual(len(broker.calls), 1)
        self.assertAlmostEqual(broker.calls[0][1], 55.0, places=4)
        self.assertIn("加密 boost x1.10", trader.recent_orders[0]["reason"])

    def test_topic_profile_penalty_reduces_order_size(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 8,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, topic_penalty_multiplier=0.9),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([
                _signal(
                    "BUY",
                    wallet_score=82.0,
                    wallet_tier="CORE",
                    market_slug="fed-cut-rates-in-june",
                    topic_key="macro",
                    topic_label="宏观",
                    topic_sample_count=5,
                    topic_win_rate=0.4,
                    topic_roi=-0.05,
                    topic_score_summary="宏观 | 5 samples | roi -5% | win 40%",
                )
            ]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()
        self.assertEqual(len(broker.calls), 1)
        self.assertAlmostEqual(broker.calls[0][1], 45.0, places=4)
        self.assertIn("宏观 trim x0.90", trader.recent_orders[0]["reason"])

    def test_sell_signal_reduces_existing_position_by_exit_fraction(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 8,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([
                _signal(
                    "SELL",
                    wallet="0x1111111111111111111111111111111111111111",
                    wallet_score=82.0,
                    wallet_tier="CORE",
                    observed_notional=80.0,
                    exit_fraction=0.5,
                    exit_reason="source wallet trimmed 50% | delta $400",
                )
            ]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 60.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
            "entry_wallet": "0x1111111111111111111111111111111111111111",
            "entry_wallet_score": 82.0,
            "entry_wallet_tier": "CORE",
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertEqual(len(broker.calls), 1)
        self.assertAlmostEqual(broker.calls[0][1], 30.0, places=4)
        self.assertAlmostEqual(trader.positions_book["token-demo"]["notional"], 30.0, places=4)
        self.assertIn("source wallet trimmed 50%", trader.recent_orders[0]["reason"])

    def test_sell_signal_is_ignored_when_entry_wallet_differs(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 9,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([
                _signal(
                    "SELL",
                    wallet="0x1111111111111111111111111111111111111111",
                    observed_notional=80.0,
                    exit_fraction=1.0,
                    exit_reason="source wallet fully exited",
                )
            ]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 60.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
            "entry_wallet": "0x9999999999999999999999999999999999999999",
            "entry_wallet_score": 82.0,
            "entry_wallet_tier": "CORE",
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertEqual(len(broker.calls), 0)
        self.assertAlmostEqual(trader.positions_book["token-demo"]["notional"], 60.0, places=4)

    def test_cross_wallet_sell_signal_can_trim_position_once(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 10,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([
                _signal(
                    "SELL",
                    wallet="wallet-resonance",
                    observed_notional=120.0,
                    exit_fraction=0.35,
                    exit_reason="multi-wallet exit resonance | 2 wallets trimming",
                    cross_wallet_exit=True,
                    exit_wallet_count=2,
                ),
                _signal(
                    "SELL",
                    wallet="wallet-resonance",
                    observed_notional=120.0,
                    exit_fraction=0.6,
                    exit_reason="multi-wallet exit resonance | 2 wallets | 1 CORE full exit",
                    cross_wallet_exit=True,
                    exit_wallet_count=2,
                ),
            ]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 60.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
            "entry_wallet": "0x9999999999999999999999999999999999999999",
            "entry_wallet_score": 82.0,
            "entry_wallet_tier": "CORE",
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertEqual(len(broker.calls), 1)
        self.assertAlmostEqual(broker.calls[0][1], 21.0, places=4)
        self.assertAlmostEqual(trader.positions_book["token-demo"]["notional"], 39.0, places=4)
        self.assertIn("multi-wallet exit resonance", trader.recent_orders[0]["reason"])

    def test_wallet_discovery_quality_bias_promotes_stronger_wallet(self):
        wallet_a = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        wallet_b = "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 9,
                },
                f,
            )
            control_path = f.name

        metrics = {
            wallet_b: RealizedWalletMetrics(
                closed_positions=12,
                wins=8,
                resolved_markets=4,
                resolved_wins=3,
                total_bought=1000.0,
                realized_pnl=160.0,
                gross_profit=240.0,
                gross_loss=80.0,
                win_rate=0.6667,
                resolved_win_rate=0.75,
                roi=0.16,
                profit_factor=3.0,
            )
        }
        topic_profiles = {
            wallet_b: [
                {
                    "key": "crypto",
                    "label": "加密",
                    "sample_count": 6,
                    "win_rate": 0.72,
                    "roi": 0.18,
                    "resolved_markets": 3,
                    "resolved_win_rate": 0.67,
                }
            ]
        }
        data_client = _DummyDataClient()
        data_client.discover_wallet_activity = lambda paths, limit: {wallet_a: 5, wallet_b: 4}
        strategy = _DummyStrategy([])
        trader = Trader(
            settings=self._make_settings(
                control_path,
                watch_wallets="",
                wallet_discovery_enabled=True,
                wallet_discovery_top_n=5,
                wallet_discovery_min_events=1,
                wallet_discovery_quality_top_n=2,
                wallet_discovery_history_bonus=0.75,
                wallet_discovery_topic_bonus=0.5,
            ),
            data_client=data_client,
            strategy=strategy,
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )
        trader._wallet_history_store = _FakeHistoryStore(metrics=metrics, topic_profiles=topic_profiles)

        wallets = trader._resolve_wallets()

        self.assertEqual(wallets[0], wallet_b)
        self.assertEqual(strategy.selection_context[wallet_b]["discovery_priority_rank"], 1)
        self.assertIn("hist +", strategy.selection_context[wallet_b]["discovery_priority_reason"])
        self.assertIn("加密 +", strategy.selection_context[wallet_b]["discovery_priority_reason"])

    def test_sell_fill_updates_realized_pnl_and_ledger(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name
        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.close()

        trader = Trader(
            settings=self._make_settings(control_path, ledger_path=ledger_file.name),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy(
                [
                    _signal(
                        "SELL",
                        wallet="0x9999999999999999999999999999999999999999",
                        observed_notional=30.0,
                        exit_fraction=0.5,
                    )
                ]
            ),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 60.0,
            "cost_basis_notional": 50.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
            "entry_wallet": "0x9999999999999999999999999999999999999999",
            "entry_wallet_score": 82.0,
            "entry_wallet_tier": "CORE",
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertAlmostEqual(trader.state.daily_realized_pnl, 5.0, places=4)
        self.assertAlmostEqual(trader.positions_book["token-demo"]["cost_basis_notional"], 25.0, places=4)
        self.assertAlmostEqual(float(trader.recent_orders[0]["realized_pnl"]), 5.0, places=4)
        with open(ledger_file.name, "r", encoding="utf-8") as f:
            rows = [json.loads(line) for line in f if line.strip()]
        fill_rows = [row for row in rows if str(row.get("type")) == "fill"]
        self.assertEqual(len(fill_rows), 1)
        self.assertAlmostEqual(float(fill_rows[0]["realized_pnl"]), 5.0, places=4)

    def test_startup_recovers_daily_realized_pnl_from_ledger(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name
        runtime_state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "risk_state": {"daily_realized_pnl": 0.0},
                "positions": [
                    {
                        "token_id": "token-demo",
                        "market_slug": "demo-market",
                        "outcome": "YES",
                        "quantity": 10.0,
                        "price": 0.6,
                        "notional": 6.0,
                        "cost_basis_notional": 5.0,
                    }
                ],
            },
            runtime_state_file,
        )
        runtime_state_file.flush()
        runtime_state_file.close()
        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        ledger_file.write(
            json.dumps(
                {
                    "ts": int(datetime.now(tz=timezone.utc).timestamp()),
                    "day_key": today,
                    "type": "fill",
                    "broker": "_DummyBroker",
                    "realized_pnl": -12.5,
                }
            )
        )
        ledger_file.write("\n")
        ledger_file.flush()
        ledger_file.close()

        trader = Trader(
            settings=self._make_settings(
                control_path,
                runtime_state_path=runtime_state_file.name,
                ledger_path=ledger_file.name,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        self.assertAlmostEqual(trader.state.daily_realized_pnl, -12.5, places=4)
        self.assertIn("token-demo", trader.positions_book)
        self.assertAlmostEqual(trader.positions_book["token-demo"]["cost_basis_notional"], 5.0, places=4)

    def test_startup_recovers_daily_realized_pnl_from_current_broker_only(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        runtime_state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"risk_state": {"daily_realized_pnl": 42.9075}}, runtime_state_file)
        runtime_state_file.flush()
        runtime_state_file.close()

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        today = datetime.now(tz=timezone.utc).strftime("%Y-%m-%d")
        now_ts = int(datetime.now(tz=timezone.utc).timestamp())
        ledger_file.write(
            json.dumps(
                {
                    "ts": now_ts,
                    "day_key": today,
                    "type": "fill",
                    "broker": "PaperBroker",
                    "realized_pnl": 42.9075,
                    "notional": 119.625,
                    "side": "SELL",
                }
            )
        )
        ledger_file.write("\n")
        ledger_file.write(
            json.dumps(
                {
                    "ts": now_ts,
                    "day_key": today,
                    "type": "account_sync",
                    "broker": "_DummyBroker",
                    "equity_usd": 9.85,
                }
            )
        )
        ledger_file.write("\n")
        ledger_file.flush()
        ledger_file.close()

        trader = Trader(
            settings=self._make_settings(
                control_path,
                runtime_state_path=runtime_state_file.name,
                ledger_path=ledger_file.name,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        self.assertAlmostEqual(trader.state.daily_realized_pnl, 0.0, places=4)

    def test_sqlite_ledger_path_supports_recovery_and_day_summary(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        with tempfile.TemporaryDirectory() as tmpdir:
            ledger_path = Path(tmpdir) / "ledger.db"
            now_ts = int(datetime.now(tz=timezone.utc).timestamp())
            day_key = Trader._utc_day_key(now_ts)

            trader = Trader(
                settings=self._make_settings(control_path, ledger_path=str(ledger_path)),
                data_client=_DummyDataClient(),
                strategy=_DummyStrategy([]),
                risk=_DummyRisk(),
                broker=_DummyBroker(),
            )
            trader._append_ledger_entry(
                "fill",
                {
                    "ts": now_ts,
                    "side": "SELL",
                    "notional": 18.0,
                    "realized_pnl": -12.5,
                    "source": "broker_reconcile",
                },
            )

            self.assertAlmostEqual(float(trader._recover_daily_realized_pnl_from_ledger(day_key) or 0.0), -12.5, places=4)
            summary = trader._ledger_day_summary(day_key)
            self.assertTrue(bool(summary["available"]))
            self.assertEqual(summary["fill_count"], 1)
            self.assertAlmostEqual(float(summary["realized_pnl"]), -12.5, places=4)

            recovered = Trader(
                settings=self._make_settings(control_path, ledger_path=str(ledger_path)),
                data_client=_DummyDataClient(),
                strategy=_DummyStrategy([]),
                risk=_DummyRisk(),
                broker=_DummyBroker(),
            )
            self.assertAlmostEqual(recovered.state.daily_realized_pnl, -12.5, places=4)

    def test_broker_empty_positions_prevent_snapshot_position_restore(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as control_file:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                control_file,
            )
            control_path = control_file.name

        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as runtime_state_file:
            json.dump(
                {
                    "positions": [
                        {
                            "token_id": "stale-token",
                            "market_slug": "stale-market",
                            "outcome": "YES",
                            "quantity": 10.0,
                            "price": 0.5,
                            "notional": 5.0,
                            "opened_ts": 1,
                            "last_buy_ts": 1,
                        }
                    ]
                },
                runtime_state_file,
            )
            runtime_state_path = runtime_state_file.name

        data_client = _DummyDataClient()
        data_client.active_positions = [
            _ActivePosition(
                wallet="0xabc",
                token_id="stale-token",
                market_slug="stale-market",
                outcome="YES",
                avg_price=0.5,
                size=10.0,
                notional=5.0,
                timestamp=1,
            )
        ]
        data_client.accounting_snapshot = AccountingSnapshot(
            wallet="0xabc",
            cash_balance=9.85,
            positions_value=0.0,
            equity=9.85,
            valuation_time="2026-03-18T04:02:20Z",
            positions=(),
        )

        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                runtime_state_path=runtime_state_path,
            ),
            data_client=data_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        self.assertIn("stale-token", trader.positions_book)
        self.assertEqual(trader.state.open_positions, 1)
        self.assertIn("recovery_conflict", trader.trading_mode_reasons)
        self.assertTrue(
            any(
                str(item.get("category") or "") == "AMBIGUOUS_POSITION"
                for item in list(getattr(trader, "_recovery_conflicts", []))
            )
        )

    def test_broker_dust_positions_are_ignored_at_startup(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as control_file:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                control_file,
            )
            control_path = control_file.name

        data_client = _DummyDataClient()
        data_client.active_positions = [
            _ActivePosition(
                wallet="0xabc",
                token_id="dust-token",
                market_slug="dust-market",
                outcome="YES",
                avg_price=0.24,
                size=0.001817,
                notional=0.00043608,
                timestamp=1,
            )
        ]
        data_client.accounting_snapshot = AccountingSnapshot(
            wallet="0xabc",
            cash_balance=87.236801,
            positions_value=0.00043608,
            equity=87.23723708,
            valuation_time="2026-03-25T04:47:00Z",
            positions=(
                AccountingPosition(
                    token_id="dust-token",
                    condition_id="",
                    size=0.001817,
                    price=0.24,
                    value=0.00043608,
                    valuation_time="2026-03-25T04:47:00Z",
                ),
            ),
        )

        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
            ),
            data_client=data_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        self.assertEqual(trader.positions_book, {})
        self.assertEqual(trader.state.open_positions, 0)

    def test_startup_merges_snapshot_position_metadata_into_broker_position(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as control_file:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                control_file,
            )
            control_path = control_file.name

        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as runtime_state_file:
            json.dump(
                {
                    "positions": [
                        {
                            "token_id": "token-demo",
                            "condition_id": "condition-demo",
                            "market_slug": "demo-market",
                            "outcome": "YES",
                            "quantity": 10.0,
                            "price": 0.5,
                            "notional": 5.0,
                            "cost_basis_notional": 4.8,
                            "opened_ts": 1700000001,
                            "last_buy_ts": 1700000002,
                            "last_trim_ts": 1700000003,
                            "entry_wallet": "0xold",
                            "entry_wallet_score": 75.0,
                            "entry_wallet_tier": "CORE",
                            "entry_reason": "snapshot",
                            "trace_id": "trc-demo",
                            "origin_signal_id": "sig-origin",
                            "last_signal_id": "sig-last",
                            "last_exit_kind": "trim",
                            "last_exit_label": "Trimmed",
                            "last_exit_summary": "trimmed after target",
                            "last_exit_ts": 1700000004,
                        }
                    ]
                },
                runtime_state_file,
            )
            runtime_state_path = runtime_state_file.name

        data_client = _DummyDataClient()
        data_client.active_positions = [
            _ActivePosition(
                wallet="0xabc",
                token_id="token-demo",
                condition_id="condition-demo",
                market_slug="demo-market",
                outcome="YES",
                avg_price=0.6,
                size=10.0,
                notional=6.0,
                timestamp=1700001000,
            )
        ]

        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                runtime_state_path=runtime_state_path,
            ),
            data_client=data_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        self.assertIn("token-demo", trader.positions_book)
        recovered = trader.positions_book["token-demo"]
        self.assertAlmostEqual(recovered["quantity"], 10.0, places=4)
        self.assertAlmostEqual(recovered["notional"], 6.0, places=4)
        self.assertAlmostEqual(recovered["price"], 0.6, places=4)
        self.assertAlmostEqual(recovered["cost_basis_notional"], 4.8, places=4)
        self.assertEqual(recovered["entry_wallet"], "0xold")
        self.assertEqual(recovered["entry_wallet_tier"], "CORE")
        self.assertEqual(recovered["trace_id"], "trc-demo")
        self.assertEqual(recovered["origin_signal_id"], "sig-origin")
        self.assertEqual(recovered["last_signal_id"], "sig-last")
        self.assertEqual(recovered["last_exit_label"], "Trimmed")
        self.assertEqual(recovered["opened_ts"], 1700000001)
        self.assertEqual(recovered["last_trim_ts"], 1700000003)

    def test_startup_scales_snapshot_cost_basis_when_broker_quantity_differs(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as control_file:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                control_file,
            )
            control_path = control_file.name

        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as runtime_state_file:
            json.dump(
                {
                    "positions": [
                        {
                            "token_id": "token-demo",
                            "market_slug": "demo-market",
                            "outcome": "YES",
                            "quantity": 10.0,
                            "price": 0.5,
                            "notional": 5.0,
                            "cost_basis_notional": 5.0,
                            "opened_ts": 1700000001,
                            "last_buy_ts": 1700000002,
                            "entry_wallet": "0xold",
                            "trace_id": "trc-demo",
                            "origin_signal_id": "sig-origin",
                        }
                    ]
                },
                runtime_state_file,
            )
            runtime_state_path = runtime_state_file.name

        data_client = _DummyDataClient()
        data_client.active_positions = [
            _ActivePosition(
                wallet="0xabc",
                token_id="token-demo",
                market_slug="demo-market",
                outcome="YES",
                avg_price=0.6,
                size=6.0,
                notional=3.6,
                timestamp=1700001000,
            )
        ]

        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                runtime_state_path=runtime_state_path,
            ),
            data_client=data_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        recovered = trader.positions_book["token-demo"]
        self.assertAlmostEqual(recovered["quantity"], 6.0, places=4)
        self.assertAlmostEqual(recovered["notional"], 3.6, places=4)
        self.assertAlmostEqual(recovered["cost_basis_notional"], 3.0, places=4)
        self.assertEqual(recovered["trace_id"], "trc-demo")
        self.assertEqual(recovered["entry_wallet"], "0xold")

    def test_startup_broker_position_seeds_metadata_from_restored_pending_order(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as control_file:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                control_file,
            )
            control_path = control_file.name

        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as runtime_state_file:
            json.dump(
                {
                    "pending_orders": [
                        {
                            "key": "sig-demo:BUY:token-demo",
                            "ts": 1700000001,
                            "signal_id": "sig-demo",
                            "order_id": "oid-demo",
                            "broker_status": "posted",
                            "trace_id": "trc-demo",
                            "token_id": "token-demo",
                            "condition_id": "condition-demo",
                            "market_slug": "demo-market",
                            "outcome": "YES",
                            "side": "BUY",
                            "wallet": "0xwallet",
                            "wallet_score": 88.0,
                            "wallet_tier": "CORE",
                            "requested_notional": 10.0,
                            "requested_price": 0.5,
                            "entry_wallet": "0xwallet",
                            "entry_wallet_score": 88.0,
                            "entry_wallet_tier": "CORE",
                            "entry_topic_label": "Politics",
                            "entry_topic_bias": "neutral",
                            "entry_topic_multiplier": 1.0,
                            "entry_topic_summary": "followed wallet",
                            "entry_reason": "restored pending buy",
                            "origin_signal_id": "sig-origin",
                            "last_signal_id": "sig-demo",
                        }
                    ]
                },
                runtime_state_file,
            )
            runtime_state_path = runtime_state_file.name

        data_client = _DummyDataClient()
        data_client.active_positions = [
            _ActivePosition(
                wallet="0xabc",
                token_id="token-demo",
                condition_id="condition-demo",
                market_slug="demo-market",
                outcome="YES",
                avg_price=0.6,
                size=20.0,
                notional=12.0,
                timestamp=1700001000,
            )
        ]

        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                runtime_state_path=runtime_state_path,
            ),
            data_client=data_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        self.assertIn("token-demo", trader.positions_book)
        recovered = trader.positions_book["token-demo"]
        self.assertAlmostEqual(recovered["quantity"], 20.0, places=4)
        self.assertAlmostEqual(recovered["notional"], 12.0, places=4)
        self.assertAlmostEqual(recovered["cost_basis_notional"], 10.0, places=4)
        self.assertEqual(recovered["opened_ts"], 1700000001)
        self.assertEqual(recovered["last_buy_ts"], 1700000001)
        self.assertEqual(recovered["entry_wallet"], "0xwallet")
        self.assertAlmostEqual(recovered["entry_wallet_score"], 88.0, places=4)
        self.assertEqual(recovered["entry_topic_label"], "Politics")
        self.assertEqual(recovered["trace_id"], "trc-demo")
        self.assertEqual(recovered["origin_signal_id"], "sig-origin")
        self.assertEqual(recovered["last_signal_id"], "sig-demo")

    def test_startup_restores_pending_open_orders_from_broker(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        broker.open_orders = [
            OpenOrderSnapshot(
                order_id="oid-open",
                token_id="token-demo",
                side="BUY",
                status="live",
                price=0.5,
                original_size=20.0,
                matched_size=5.0,
                remaining_size=15.0,
                created_ts=1700000000,
                market_slug="demo-market",
                outcome="YES",
            )
        ]

        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=broker,
        )

        self.assertEqual(len(trader.pending_orders), 1)
        restored = next(iter(trader.pending_orders.values()))
        self.assertEqual(restored["order_id"], "oid-open")
        self.assertEqual(restored["broker_status"], "partially_filled")
        self.assertAlmostEqual(float(restored["requested_notional"]), 10.0, places=4)
        self.assertAlmostEqual(float(restored["matched_notional_hint"]), 2.5, places=4)

    def test_startup_keeps_snapshot_pending_orders_when_broker_reports_empty_open_orders(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name
        runtime_state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "pending_orders": [
                    {
                        "key": "sig-1:BUY:token-demo",
                        "ts": 1700000000,
                        "cycle_id": "",
                        "order_id": "oid-stale",
                        "broker_status": "live",
                        "signal_id": "sig-1",
                        "trace_id": "",
                        "token_id": "token-demo",
                        "market_slug": "demo-market",
                        "outcome": "YES",
                        "side": "BUY",
                        "wallet": "",
                        "requested_notional": 12.0,
                        "requested_price": 0.6,
                    }
                ]
            },
            runtime_state_file,
        )
        runtime_state_file.flush()
        runtime_state_file.close()

        broker = _PendingBroker()
        broker.open_orders = []

        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                runtime_state_path=runtime_state_file.name,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=broker,
        )

        self.assertEqual(len(trader.pending_orders), 1)
        restored = next(iter(trader.pending_orders.values()))
        self.assertEqual(restored["order_id"], "oid-stale")
        self.assertEqual(restored["recovery_status"], "db_pending_without_broker_open_and_without_fill_evidence")
        self.assertEqual(restored["recovery_source"], "db")

    def test_restored_uncertain_pending_order_blocks_duplicate_buy_after_restart(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 43,
                },
                f,
            )
            control_path = f.name
        runtime_state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "pending_orders": [
                    {
                        "key": "sig-1:BUY:token-demo",
                        "ts": 1700000000,
                        "cycle_id": "",
                        "order_id": "oid-stale",
                        "broker_status": "live",
                        "signal_id": "sig-1",
                        "trace_id": "",
                        "token_id": "token-demo",
                        "condition_id": "condition-demo",
                        "market_slug": "demo-market",
                        "outcome": "YES",
                        "side": "BUY",
                        "wallet": "",
                        "requested_notional": 12.0,
                        "requested_price": 0.6,
                    }
                ]
            },
            runtime_state_file,
        )
        runtime_state_file.flush()
        runtime_state_file.close()

        broker = _PendingBroker()
        broker.open_orders = []

        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=True,
                runtime_state_path=runtime_state_file.name,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()

        self.assertEqual(len(broker.calls), 0)
        self.assertEqual(len(trader.pending_orders), 0)
        self.assertEqual(trader.recent_orders[0]["status"], "CANCELED")
        self.assertIn("trading_mode_cancel_pending_entry", str(trader.recent_orders[0]["reason"]))

    def test_runtime_snapshot_restores_broker_event_sync_cursor(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 29,
                },
                f,
            )
            control_path = f.name
        runtime_state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "broker_event_sync_ts": 1700000123,
                "pending_orders": [],
            },
            runtime_state_file,
        )
        runtime_state_file.flush()
        runtime_state_file.close()

        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=True,
                runtime_state_path=runtime_state_file.name,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        self.assertEqual(trader._last_broker_event_sync_ts, 1700000123)
        dumped = trader._dump_runtime_state()
        self.assertEqual(dumped["broker_event_sync_ts"], 1700000123)
        self.assertEqual(dumped["runtime_version"], 9)

    def test_live_startup_checks_fail_when_network_smoke_reports_block(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 30,
                },
                f,
            )
            control_path = f.name
        smoke_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        smoke_file.write(
            json.dumps(
                {
                    "ts": 1700001000,
                    "summary": {
                        "failures": 1,
                        "blocks": 1,
                        "warnings": 0,
                        "exit_code": 2,
                    },
                }
            )
        )
        smoke_file.write("\n")
        smoke_file.flush()
        smoke_file.close()
        previous = {key: os.environ.get(key) for key in ("NETWORK_SMOKE_LOG", "LIVE_ALLOWANCE_READY", "LIVE_GEOBLOCK_READY", "LIVE_ACCOUNT_READY")}
        os.environ["NETWORK_SMOKE_LOG"] = smoke_file.name
        os.environ["LIVE_ALLOWANCE_READY"] = "true"
        os.environ["LIVE_GEOBLOCK_READY"] = "true"
        os.environ["LIVE_ACCOUNT_READY"] = "true"
        try:
            trader = Trader(
                settings=self._make_settings(
                    control_path,
                    dry_run=False,
                    funder_address="0xabc",
                    live_allowance_ready=True,
                    live_geoblock_ready=True,
                    live_account_ready=True,
                    live_network_smoke_max_age_seconds=3600,
                ),
                data_client=_DummyDataClient(),
                strategy=_DummyStrategy([]),
                risk=_DummyRisk(),
                broker=_PendingBroker(),
            )
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertFalse(trader.startup_ready)
        self.assertGreaterEqual(trader.startup_failure_count, 1)
        smoke_check = next(row for row in trader.startup_checks if str(row.get("name")) == "network_smoke")
        self.assertEqual(smoke_check["status"], "FAIL")

    def test_live_startup_checks_fail_without_explicit_live_admission_flags(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 31,
                },
                f,
            )
            control_path = f.name
        smoke_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        smoke_file.write(
            json.dumps(
                {
                    "ts": int(time.time()),
                    "summary": {
                        "failures": 0,
                        "blocks": 0,
                        "warnings": 0,
                        "exit_code": 0,
                    },
                }
            )
        )
        smoke_file.write("\n")
        smoke_file.flush()
        smoke_file.close()
        previous = {key: os.environ.get(key) for key in ("NETWORK_SMOKE_LOG", "LIVE_ALLOWANCE_READY", "LIVE_GEOBLOCK_READY", "LIVE_ACCOUNT_READY")}
        os.environ["NETWORK_SMOKE_LOG"] = smoke_file.name
        os.environ.pop("LIVE_ALLOWANCE_READY", None)
        os.environ.pop("LIVE_GEOBLOCK_READY", None)
        os.environ.pop("LIVE_ACCOUNT_READY", None)
        try:
            trader = Trader(
                settings=self._make_settings(
                    control_path,
                    dry_run=False,
                    funder_address="0xabc",
                    live_allowance_ready=False,
                    live_geoblock_ready=False,
                    live_account_ready=False,
                    live_network_smoke_max_age_seconds=3600,
                ),
                data_client=_DummyDataClient(),
                strategy=_DummyStrategy([]),
                risk=_DummyRisk(),
                broker=_PendingBroker(),
            )
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertFalse(trader.startup_ready)
        live_check = next(row for row in trader.startup_checks if str(row.get("name")) == "live_admission")
        self.assertEqual(live_check["status"], "FAIL")
        self.assertIn("LIVE_ALLOWANCE_READY", str(live_check["message"]))

    def test_live_startup_checks_fail_when_network_smoke_is_stale(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 32,
                },
                f,
            )
            control_path = f.name
        smoke_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        smoke_file.write(
            json.dumps(
                {
                    "ts": int(time.time()) - 7200,
                    "summary": {
                        "failures": 0,
                        "blocks": 0,
                        "warnings": 0,
                        "exit_code": 0,
                    },
                }
            )
        )
        smoke_file.write("\n")
        smoke_file.flush()
        smoke_file.close()
        previous = {key: os.environ.get(key) for key in ("NETWORK_SMOKE_LOG", "LIVE_ALLOWANCE_READY", "LIVE_GEOBLOCK_READY", "LIVE_ACCOUNT_READY")}
        os.environ["NETWORK_SMOKE_LOG"] = smoke_file.name
        os.environ["LIVE_ALLOWANCE_READY"] = "true"
        os.environ["LIVE_GEOBLOCK_READY"] = "true"
        os.environ["LIVE_ACCOUNT_READY"] = "true"
        try:
            trader = Trader(
                settings=self._make_settings(
                    control_path,
                    dry_run=False,
                    funder_address="0xabc",
                    live_allowance_ready=True,
                    live_geoblock_ready=True,
                    live_account_ready=True,
                    live_network_smoke_max_age_seconds=3600,
                ),
                data_client=_DummyDataClient(),
                strategy=_DummyStrategy([]),
                risk=_DummyRisk(),
                broker=_PendingBroker(),
            )
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        self.assertFalse(trader.startup_ready)
        smoke_check = next(row for row in trader.startup_checks if str(row.get("name")) == "network_smoke")
        self.assertEqual(smoke_check["status"], "FAIL")
        self.assertIn("stale", str(smoke_check["message"]))

    def test_account_state_stale_blocks_new_buy_but_allows_sell(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 35,
                },
                f,
            )
            control_path = f.name

        stale_ts = int(time.time()) - 7200
        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc", account_sync_refresh_seconds=300),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.startup_ready = True
        trader.startup_failure_count = 0
        trader.startup_checks = []
        trader.state.account_snapshot_ts = stale_ts
        trader.state.cash_balance_usd = 100.0

        trader.step()

        self.assertEqual(len(broker.calls), 0)
        self.assertEqual(trader.trading_mode, "REDUCE_ONLY")
        self.assertIn("account_state_stale", trader.trading_mode_reasons)

        trader.strategy = _DummyStrategy([_signal("SELL")])
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 60.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
            "entry_wallet": "0x1111111111111111111111111111111111111111",
            "entry_wallet_score": 80.0,
            "entry_wallet_tier": "CORE",
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertEqual(len(broker.calls), 1)
        self.assertEqual(broker.calls[0][0].side, "SELL")

    def test_reconciliation_warn_blocks_new_buy(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 36,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(control_path),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.pending_orders["stale-live-token-demo"] = {
            "key": "stale-live-token-demo",
            "order_id": "live-token-demo",
            "token_id": "token-demo",
            "condition_id": "condition-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "side": "BUY",
            "wallet": "0x1111111111111111111111111111111111111111",
            "requested_notional": 25.0,
            "requested_price": 0.6,
            "matched_notional_hint": 0.0,
            "matched_size_hint": 0.0,
            "matched_price_hint": 0.0,
            "ts": int(time.time()) - 7200,
            "status": "PENDING",
            "broker_status": "live",
        }
        trader._refresh_risk_state()

        trader.step()

        self.assertEqual(len(broker.calls), 0)
        self.assertEqual(trader.trading_mode, "REDUCE_ONLY")
        self.assertIn("reconciliation_warn", trader.trading_mode_reasons)

    def test_reconciliation_summary_matches_ledger_after_sell_fill(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 31,
                },
                f,
            )
            control_path = f.name
        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.close()

        trader = Trader(
            settings=self._make_settings(control_path, ledger_path=ledger_file.name),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy(
                [
                    _signal(
                        "SELL",
                        wallet="0x9999999999999999999999999999999999999999",
                        observed_notional=30.0,
                        exit_fraction=0.5,
                    )
                ]
            ),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 60.0,
            "cost_basis_notional": 50.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
            "entry_wallet": "0x9999999999999999999999999999999999999999",
            "entry_wallet_score": 82.0,
            "entry_wallet_tier": "CORE",
        }
        trader.state.open_positions = 1

        trader.step()
        summary = trader.reconciliation_summary(now=int(datetime.now(tz=timezone.utc).timestamp()))

        self.assertEqual(summary["status"], "ok")
        self.assertAlmostEqual(float(summary["internal_vs_ledger_diff"]), 0.0, places=4)
        self.assertEqual(summary["fill_count_today"], 1)
        self.assertTrue(bool(summary["ledger_available"]))

    def test_recent_fill_partial_buy_keeps_pending_and_seeds_position(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 26,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)

        trader.step()
        broker.recent_fills = [
            OrderFillSnapshot(
                order_id="live-token-demo",
                token_id="token-demo",
                side="BUY",
                price=0.6,
                size=20.0,
                timestamp=1700000010,
                market_slug="demo-market",
                outcome="YES",
            )
        ]

        trader._reconcile_runtime_with_broker()

        self.assertIn("token-demo", trader.positions_book)
        self.assertAlmostEqual(trader.positions_book["token-demo"]["notional"], 12.0, places=4)
        self.assertEqual(len(trader.pending_orders), 1)
        restored = next(iter(trader.pending_orders.values()))
        self.assertEqual(restored["broker_status"], "partially_filled")
        self.assertAlmostEqual(float(restored["matched_notional_hint"]), 12.0, places=4)
        self.assertEqual(trader.recent_orders[0]["status"], "PARTIAL")

    def test_order_event_stream_reconciles_partial_buy_without_fallback_polls(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 28,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)

        trader.step()
        broker.order_statuses = {}
        broker.recent_fills = None
        broker.order_events = [
            BrokerOrderEvent(
                event_type="fill",
                order_id="live-token-demo",
                token_id="token-demo",
                side="BUY",
                timestamp=1700000010,
                matched_notional=12.0,
                matched_size=20.0,
                avg_fill_price=0.6,
                market_slug="demo-market",
                outcome="YES",
                tx_hash="0xfill",
            ),
            BrokerOrderEvent(
                event_type="status",
                order_id="live-token-demo",
                token_id="token-demo",
                side="BUY",
                timestamp=1700000011,
                status="partially_filled",
                matched_notional=12.0,
                matched_size=20.0,
                avg_fill_price=0.6,
            ),
        ]

        trader._reconcile_runtime_with_broker()

        self.assertIn("token-demo", trader.positions_book)
        self.assertAlmostEqual(trader.positions_book["token-demo"]["notional"], 12.0, places=4)
        self.assertEqual(len(trader.pending_orders), 1)
        restored = next(iter(trader.pending_orders.values()))
        self.assertEqual(restored["broker_status"], "partially_filled")
        self.assertAlmostEqual(float(restored["matched_notional_hint"]), 12.0, places=4)
        self.assertEqual(trader.recent_orders[0]["status"], "PARTIAL")

    def test_order_event_stream_targets_matching_order_when_multiple_pending_share_token(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 29,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        data_client = _DummyDataClient()
        data_client.active_positions = [
            _ActivePosition(
                wallet="0xabc",
                token_id="token-demo",
                market_slug="demo-market",
                outcome="YES",
                avg_price=0.6,
                size=20.0,
                notional=12.0,
                timestamp=1700000010,
            )
        ]
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=data_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader, account_snapshot_ts=1700000020)
        trader.positions_book = {}
        trader.state.open_positions = 0
        now_ts = int(time.time())
        trader.pending_orders = {
            "first": self._restore_pending_order(
                trader,
                key="first",
                order_id="order-1",
                side="BUY",
                ts=now_ts - 10,
            ),
            "second": self._restore_pending_order(
                trader,
                key="second",
                order_id="order-2",
                side="BUY",
                ts=now_ts - 9,
            ),
        }
        trader._refresh_risk_state()
        broker.order_events = [
            BrokerOrderEvent(
                event_type="fill",
                order_id="order-1",
                token_id="token-demo",
                side="BUY",
                timestamp=1700000010,
                matched_notional=12.0,
                matched_size=20.0,
                avg_fill_price=0.6,
                market_slug="demo-market",
                outcome="YES",
                tx_hash="0xfill-1",
            ),
            BrokerOrderEvent(
                event_type="status",
                order_id="order-1",
                token_id="token-demo",
                side="BUY",
                timestamp=1700000011,
                status="partially_filled",
                matched_notional=12.0,
                matched_size=20.0,
                avg_fill_price=0.6,
            ),
        ]

        trader._reconcile_runtime_with_broker()

        self.assertEqual(len(trader.pending_orders), 2)
        first = trader.pending_orders["first"]
        second = trader.pending_orders["second"]
        self.assertAlmostEqual(float(first["matched_notional_hint"]), 12.0, places=4)
        self.assertAlmostEqual(float(first["reconciled_notional_hint"]), 12.0, places=4)
        self.assertEqual(str(first["broker_status"]), "partially_filled")
        self.assertAlmostEqual(float(second["matched_notional_hint"]), 0.0, places=4)
        self.assertAlmostEqual(float(second["reconciled_notional_hint"]), 0.0, places=4)
        self.assertEqual(int(second["reconcile_ambiguous_ts"]), 0)
        self.assertAlmostEqual(float(trader.positions_book["token-demo"]["notional"]), 12.0, places=4)
        self.assertEqual(str(trader.recent_orders[0]["order_id"]), "order-1")
        self.assertEqual(str(trader.recent_orders[0]["status"]), "PARTIAL")

    def test_multiple_pending_same_token_without_order_fill_marks_ambiguous(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 30,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        broker.order_events = []
        data_client = _DummyDataClient()
        data_client.active_positions = [
            _ActivePosition(
                wallet="0xabc",
                token_id="token-demo",
                market_slug="demo-market",
                outcome="YES",
                avg_price=0.6,
                size=20.0,
                notional=12.0,
                timestamp=1700000010,
            )
        ]
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=data_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader, account_snapshot_ts=1700000020)
        trader.positions_book = {}
        trader.state.open_positions = 0
        now_ts = int(time.time())
        trader.pending_orders = {
            "first": self._restore_pending_order(
                trader,
                key="first",
                order_id="order-1",
                side="BUY",
                ts=now_ts - 10,
            ),
            "second": self._restore_pending_order(
                trader,
                key="second",
                order_id="order-2",
                side="BUY",
                ts=now_ts - 9,
            ),
        }
        trader._refresh_risk_state()

        trader._reconcile_runtime_with_broker()

        self.assertEqual(len(trader.pending_orders), 2)
        self.assertEqual(len(trader.recent_orders), 0)
        first = trader.pending_orders["first"]
        second = trader.pending_orders["second"]
        self.assertGreater(int(first["reconcile_ambiguous_ts"]), 0)
        self.assertGreater(int(second["reconcile_ambiguous_ts"]), 0)
        self.assertIn("multiple_pending_orders_for_token=token-demo:BUY", str(first["reconcile_ambiguous_reason"]))
        self.assertIn("multiple_pending_orders_for_token=token-demo:BUY", str(second["reconcile_ambiguous_reason"]))
        summary = trader.reconciliation_summary(now=1700000200)
        self.assertEqual(summary["status"], "warn")
        self.assertIn("ambiguous_pending_orders=2", summary["issues"])
        self.assertEqual(int(summary["ambiguous_pending_orders"]), 2)

    def test_recent_fill_sell_reconcile_updates_realized_pnl_without_position_delta(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 27,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        data_client = _DummyDataClient()
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=data_client,
            strategy=_DummyStrategy([
                _signal(
                    "SELL",
                    wallet="0x9999999999999999999999999999999999999999",
                    observed_notional=30.0,
                    exit_fraction=0.5,
                )
            ]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 60.0,
            "cost_basis_notional": 50.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
            "entry_wallet": "0x9999999999999999999999999999999999999999",
            "entry_wallet_score": 82.0,
            "entry_wallet_tier": "CORE",
        }
        trader.state.open_positions = 1
        data_client.active_positions = [
            _ActivePosition(
                wallet="0xabc",
                token_id="token-demo",
                market_slug="demo-market",
                outcome="YES",
                avg_price=0.6,
                size=100.0,
                notional=60.0,
                timestamp=1700000000,
            )
        ]

        trader.step()
        data_client.active_positions = []
        fill_ts = int(time.time())
        broker.recent_fills = [
            OrderFillSnapshot(
                order_id="live-token-demo",
                token_id="token-demo",
                side="SELL",
                price=0.6,
                size=50.0,
                timestamp=fill_ts,
                market_slug="demo-market",
                outcome="YES",
            )
        ]

        trader._reconcile_runtime_with_broker()

        self.assertEqual(len(trader.pending_orders), 0)
        self.assertIn("token-demo", trader.positions_book)
        self.assertAlmostEqual(trader.positions_book["token-demo"]["notional"], 30.0, places=4)
        self.assertAlmostEqual(trader.positions_book["token-demo"]["cost_basis_notional"], 25.0, places=4)
        self.assertAlmostEqual(trader.state.daily_realized_pnl, 5.0, places=4)
        self.assertEqual(trader.recent_orders[0]["status"], "RECONCILED")

    def test_cross_day_sell_fill_uses_fill_timestamp_for_ledger_and_recent_orders(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 31,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        data_client = _DummyDataClient()
        prev_fill_ts = int(datetime(2026, 3, 19, 23, 59, 50, tzinfo=timezone.utc).timestamp())
        reconcile_now_ts = int(datetime(2026, 3, 20, 0, 5, 0, tzinfo=timezone.utc).timestamp())
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=data_client,
            strategy=_DummyStrategy(
                [
                    _signal(
                        "SELL",
                        wallet="0x9999999999999999999999999999999999999999",
                        observed_notional=30.0,
                        exit_fraction=0.5,
                    )
                ]
            ),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 60.0,
            "cost_basis_notional": 50.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
            "entry_wallet": "0x9999999999999999999999999999999999999999",
            "entry_wallet_score": 82.0,
            "entry_wallet_tier": "CORE",
        }
        trader.state.open_positions = 1
        self._arm_live_trader(trader, account_snapshot_ts=reconcile_now_ts)
        data_client.active_positions = [
            _ActivePosition(
                wallet="0xabc",
                token_id="token-demo",
                market_slug="demo-market",
                outcome="YES",
                avg_price=0.6,
                size=100.0,
                notional=60.0,
                timestamp=prev_fill_ts - 60,
            )
        ]

        trader.step()
        data_client.active_positions = [
            _ActivePosition(
                wallet="0xabc",
                token_id="token-demo",
                market_slug="demo-market",
                outcome="YES",
                avg_price=0.6,
                size=50.0,
                notional=30.0,
                timestamp=prev_fill_ts,
            )
        ]
        broker.order_events = [
            BrokerOrderEvent(
                event_type="fill",
                order_id="live-token-demo",
                token_id="token-demo",
                side="SELL",
                timestamp=prev_fill_ts,
                matched_notional=30.0,
                matched_size=50.0,
                avg_fill_price=0.6,
                market_slug="demo-market",
                outcome="YES",
                tx_hash="0xlate-fill",
            ),
            BrokerOrderEvent(
                event_type="status",
                order_id="live-token-demo",
                token_id="token-demo",
                side="SELL",
                timestamp=prev_fill_ts + 1,
                status="filled",
                matched_notional=30.0,
                matched_size=50.0,
                avg_fill_price=0.6,
            ),
        ]

        with patch("polymarket_bot.runner.time.time", return_value=reconcile_now_ts):
            trader._reconcile_runtime_with_broker()

        prev_day_key = datetime.fromtimestamp(prev_fill_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        now_day_key = datetime.fromtimestamp(reconcile_now_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        prev_day_summary = trader._ledger_day_summary(prev_day_key)
        now_day_summary = trader._ledger_day_summary(now_day_key)

        self.assertEqual(len(trader.pending_orders), 0)
        self.assertEqual(int(trader.recent_orders[0]["ts"]), prev_fill_ts)
        self.assertEqual(str(trader.recent_orders[0]["status"]), "RECONCILED")
        self.assertAlmostEqual(float(trader.state.daily_realized_pnl), 0.0, places=4)
        self.assertAlmostEqual(float(trader.positions_book["token-demo"]["notional"]), 30.0, places=4)
        self.assertAlmostEqual(float(trader.positions_book["token-demo"]["cost_basis_notional"]), 25.0, places=4)
        self.assertEqual(int(trader.positions_book["token-demo"]["last_trim_ts"]), prev_fill_ts)
        self.assertEqual(int(prev_day_summary["fill_count"]), 1)
        self.assertAlmostEqual(float(prev_day_summary["realized_pnl"]), 5.0, places=4)
        self.assertEqual(int(prev_day_summary["last_fill_ts"]), prev_fill_ts)
        self.assertEqual(int(now_day_summary["fill_count"]), 0)
        self.assertAlmostEqual(float(now_day_summary["realized_pnl"]), 0.0, places=4)
        summary = trader.reconciliation_summary(now=reconcile_now_ts)
        self.assertEqual(summary["status"], "ok")
        self.assertAlmostEqual(float(summary["internal_vs_ledger_diff"]), 0.0, places=4)

    def test_periodic_reconcile_scales_existing_cost_basis_when_broker_quantity_changes(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        data_client = _DummyDataClient()
        data_client.active_positions = [
            _ActivePosition(
                wallet="0xabc",
                token_id="token-demo",
                market_slug="demo-market",
                outcome="YES",
                avg_price=0.6,
                size=6.0,
                notional=3.6,
                timestamp=1700001000,
            )
        ]
        data_client.accounting_snapshot = AccountingSnapshot(
            wallet="0xabc",
            cash_balance=10.0,
            positions_value=3.6,
            equity=13.6,
            valuation_time="2026-03-20T06:47:12Z",
            positions=({"token_id": "token-demo"},),
        )

        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                account_sync_refresh_seconds=60,
            ),
            data_client=data_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "condition_id": "condition-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 10.0,
            "price": 0.5,
            "notional": 5.0,
            "cost_basis_notional": 5.0,
            "opened_ts": 1700000000,
            "last_buy_ts": 1700000000,
            "last_trim_ts": 0,
            "entry_wallet": "0xold",
            "entry_wallet_score": 75.0,
            "entry_wallet_tier": "CORE",
            "entry_topic_label": "",
            "entry_topic_bias": "neutral",
            "entry_topic_multiplier": 1.0,
            "entry_topic_summary": "",
            "entry_reason": "demo",
            "trace_id": "trc-demo",
            "origin_signal_id": "sig-demo",
            "last_signal_id": "sig-demo",
            "last_exit_kind": "",
            "last_exit_label": "",
            "last_exit_summary": "",
            "last_exit_ts": 0,
        }
        trader._refresh_risk_state()

        trader._reconcile_runtime_with_broker()

        self.assertIn("token-demo", trader.positions_book)
        recovered = trader.positions_book["token-demo"]
        self.assertAlmostEqual(recovered["quantity"], 6.0, places=4)
        self.assertAlmostEqual(recovered["notional"], 3.6, places=4)
        self.assertAlmostEqual(recovered["cost_basis_notional"], 3.0, places=4)
        self.assertEqual(recovered["trace_id"], "trc-demo")
        self.assertEqual(recovered["entry_wallet"], "0xold")

    def test_ledger_append_failure_marks_persistence_fault_and_halts_opening(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )
        self._arm_live_trader(trader)

        with patch("polymarket_bot.runner.append_ledger_entry", side_effect=OSError("disk full")):
            trader._append_ledger_entry(
                "fill",
                {
                    "ts": int(time.time()),
                    "side": "SELL",
                    "notional": 12.5,
                    "realized_pnl": 1.25,
                },
            )

        persistence = trader.persistence_state()
        self.assertEqual(trader.trading_mode, "HALTED")
        self.assertIn("persistence_fault", trader.trading_mode_reasons)
        self.assertEqual(persistence["status"], "fault")
        self.assertEqual(persistence["failure_count"], 1)
        self.assertEqual(persistence["last_failure"]["kind"], "ledger_append")

    def test_runtime_state_persist_failure_marks_persistence_fault_and_raises(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        runtime_state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        runtime_state_file.write("{}")
        runtime_state_file.flush()
        runtime_state_file.close()

        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                runtime_state_path=runtime_state_file.name,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )
        self._arm_live_trader(trader)

        with patch.object(trader._state_store, "save_runtime_truth", side_effect=OSError("disk full")):
            with self.assertRaises(OSError):
                trader.persist_runtime_state(runtime_state_file.name)

        persistence = trader.persistence_state()
        self.assertEqual(trader.trading_mode, "HALTED")
        self.assertIn("persistence_fault", trader.trading_mode_reasons)
        self.assertEqual(persistence["status"], "fault")
        self.assertEqual(persistence["last_failure"]["kind"], "runtime_truth_write")

    def test_persistence_fault_cancels_pending_buy_orders(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 24,
                },
                f,
            )
            control_path = f.name

        broker = _PendingBroker()
        trader = Trader(
            settings=self._make_settings(control_path, dry_run=False, funder_address="0xabc"),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)

        trader.step()
        self.assertEqual(len(trader.pending_orders), 1)
        trader.record_external_persistence_fault("daemon_state_write", "/tmp/state.json", OSError("disk full"))
        trader.strategy = _DummyStrategy([])

        trader.step()

        self.assertEqual(len(trader.pending_orders), 0)
        self.assertEqual(trader.trading_mode, "HALTED")
        self.assertIn("persistence_fault", trader.trading_mode_reasons)
        self.assertEqual(trader.recent_orders[0]["status"], "CANCELED")
        self.assertIn("trading_mode_cancel_pending_entry", str(trader.recent_orders[0]["reason"]))

    def test_startup_gate_critical_notification_is_deduped_by_cooldown(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=True,
                critical_notification_enabled=True,
                critical_notification_cooldown_seconds=300,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )
        trader.notifier = _RecordingNotifier()

        trader.startup_ready = False
        trader.startup_failure_count = 2
        trader._update_trading_mode(
            trader.control_state,
            now=1700000100,
            reconciliation={"status": "ok", "issues": [], "ambiguous_pending_orders": 0},
        )
        self.assertEqual(len(trader.notifier.calls), 1)
        self.assertIn("startup gate blocked", trader.notifier.calls[0]["title"].lower())

        trader.startup_ready = True
        trader.startup_failure_count = 0
        trader._update_trading_mode(
            trader.control_state,
            now=1700000200,
            reconciliation={"status": "ok", "issues": [], "ambiguous_pending_orders": 0},
        )
        self.assertEqual(trader.trading_mode, "NORMAL")

        trader.startup_ready = False
        trader.startup_failure_count = 1
        trader._update_trading_mode(
            trader.control_state,
            now=1700000250,
            reconciliation={"status": "ok", "issues": [], "ambiguous_pending_orders": 0},
        )
        self.assertEqual(len(trader.notifier.calls), 1)

        trader._update_trading_mode(
            trader.control_state,
            now=1700000505,
            reconciliation={"status": "ok", "issues": [], "ambiguous_pending_orders": 0},
        )
        self.assertEqual(len(trader.notifier.calls), 1)

        trader.startup_ready = True
        trader.startup_failure_count = 0
        trader._update_trading_mode(
            trader.control_state,
            now=1700000510,
            reconciliation={"status": "ok", "issues": [], "ambiguous_pending_orders": 0},
        )
        trader.startup_ready = False
        trader.startup_failure_count = 3
        trader._update_trading_mode(
            trader.control_state,
            now=1700000900,
            reconciliation={"status": "ok", "issues": [], "ambiguous_pending_orders": 0},
        )
        self.assertEqual(len(trader.notifier.calls), 2)

    def test_persistence_fault_emits_critical_notification(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                critical_notification_enabled=True,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )
        self._arm_live_trader(trader)
        trader.notifier = _RecordingNotifier()

        trader.record_external_persistence_fault("daemon_state_write", "/tmp/state.json", OSError("disk full"))

        self.assertEqual(len(trader.notifier.calls), 1)
        self.assertIn("halted", trader.notifier.calls[0]["title"].lower())
        self.assertIn("disk full", trader.notifier.calls[0]["body"].lower())
        self.assertEqual(trader.notifier.calls[0]["extra"]["persistence"]["status"], "fault")

    def test_reconciliation_ambiguity_emits_protection_notification(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                critical_notification_enabled=True,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )
        self._arm_live_trader(trader)
        trader.notifier = _RecordingNotifier()
        pending = self._restore_pending_order(
            trader,
            key="pending-demo",
            order_id="order-demo",
            side="BUY",
        )
        pending["reconcile_ambiguous_ts"] = 1700000400
        trader.pending_orders[pending["key"]] = pending

        reconciliation = trader.reconciliation_summary(now=1700000500)
        trader._update_trading_mode(trader.control_state, now=1700000500, reconciliation=reconciliation)

        self.assertEqual(len(trader.notifier.calls), 1)
        self.assertIn("reconciliation protect", trader.notifier.calls[0]["title"].lower())
        self.assertIn("ambiguous_pending_orders=1", trader.notifier.calls[0]["body"])

    def test_account_sync_populates_equity_and_closed_pnl(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name
        data_client = _DummyDataClient()
        data_client.accounting_snapshot = AccountingSnapshot(
            wallet="0x1111111111111111111111111111111111111111",
            cash_balance=120.0,
            positions_value=45.0,
            equity=165.0,
            valuation_time="2026-03-17T12:34:56Z",
        )
        data_client.closed_positions = [
            ClosedPosition(
                wallet="0x1111111111111111111111111111111111111111",
                token_id="token-x",
                condition_id="condition-x",
                market_slug="demo-market",
                outcome="YES",
                avg_price=0.5,
                total_bought=100.0,
                realized_pnl=-14.0,
                timestamp=int(datetime.now(tz=timezone.utc).timestamp()),
                end_date="2026-03-17T12:34:56Z",
            )
        ]

        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0x1111111111111111111111111111111111111111",
                account_sync_refresh_seconds=60,
            ),
            data_client=data_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        self.assertAlmostEqual(trader.state.equity_usd, 165.0, places=4)
        self.assertAlmostEqual(trader.state.cash_balance_usd, 120.0, places=4)
        self.assertAlmostEqual(trader.state.positions_value_usd, 45.0, places=4)
        self.assertAlmostEqual(trader.state.broker_closed_pnl_today, -14.0, places=4)
        self.assertGreater(trader.state.account_snapshot_ts, 0)

    def test_periodic_reconcile_removed_positions_refreshes_account_snapshot(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 2401,
                },
                f,
            )
            control_path = f.name

        data_client = _DummyDataClient()
        data_client.accounting_snapshot = AccountingSnapshot(
            wallet="0xabc",
            cash_balance=10.8356,
            positions_value=0.0,
            equity=10.8356,
            valuation_time="2026-03-20T06:47:12Z",
            positions=(),
        )
        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                account_sync_refresh_seconds=60,
            ),
            data_client=data_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "condition_id": "condition-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 10.0,
            "price": 0.5,
            "notional": 5.0,
            "cost_basis_notional": 5.0,
            "opened_ts": 1700000000,
            "last_buy_ts": 1700000000,
            "last_trim_ts": 0,
            "entry_wallet": "0xold",
            "entry_wallet_score": 75.0,
            "entry_wallet_tier": "CORE",
            "entry_topic_label": "",
            "entry_topic_bias": "neutral",
            "entry_topic_multiplier": 1.0,
            "entry_topic_summary": "",
            "entry_reason": "demo",
            "trace_id": "trc-demo",
            "origin_signal_id": "sig-demo",
            "last_signal_id": "sig-demo",
            "last_exit_kind": "",
            "last_exit_label": "",
            "last_exit_summary": "",
            "last_exit_ts": 0,
        }
        trader._refresh_risk_state()
        data_client.active_positions = []

        trader._reconcile_runtime_with_broker()

        self.assertEqual(trader.state.open_positions, 0)
        self.assertEqual(trader.positions_book, {})
        self.assertAlmostEqual(trader.state.cash_balance_usd, 10.8356, places=4)
        self.assertAlmostEqual(trader.state.positions_value_usd, 0.0, places=4)
        self.assertAlmostEqual(trader.state.equity_usd, 10.8356, places=4)

    def test_portfolio_netting_clamps_same_condition_buy_across_tokens(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        strategy = _DummyStrategy(
            [
                _signal("BUY", token_id="token-a", market_slug="demo-a", condition_id="condition-shared", observed_notional=200.0),
                _signal("BUY", token_id="token-b", market_slug="demo-b", condition_id="condition-shared", observed_notional=200.0),
            ]
        )
        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                max_signals_per_cycle=2,
                bankroll_usd=5000.0,
                max_condition_exposure_pct=0.015,
            ),
            data_client=_DummyDataClient(),
            strategy=strategy,
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()

        self.assertEqual(len(broker.calls), 2)
        self.assertAlmostEqual(broker.calls[0][1], 50.0, places=4)
        self.assertAlmostEqual(broker.calls[1][1], 25.0, places=4)
        self.assertAlmostEqual(trader.positions_book["token-a"]["notional"], 50.0, places=4)
        self.assertAlmostEqual(trader.positions_book["token-b"]["notional"], 25.0, places=4)
        self.assertEqual(trader.positions_book["token-b"]["condition_id"], "condition-shared")

    def test_portfolio_netting_counts_pending_buy_exposure(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        strategy = _DummyStrategy(
            [
                _signal("BUY", token_id="token-a", market_slug="demo-a", condition_id="condition-shared", observed_notional=200.0),
                _signal("BUY", token_id="token-b", market_slug="demo-b", condition_id="condition-shared", observed_notional=200.0),
            ]
        )
        broker = _PendingBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                dry_run=False,
                funder_address="0xabc",
                max_signals_per_cycle=2,
                bankroll_usd=5000.0,
                max_condition_exposure_pct=0.015,
            ),
            data_client=_DummyDataClient(),
            strategy=strategy,
            risk=_DummyRisk(),
            broker=broker,
        )
        self._arm_live_trader(trader)

        trader.step()

        self.assertEqual(len(broker.calls), 2)
        self.assertAlmostEqual(broker.calls[0][1], 50.0, places=4)
        self.assertAlmostEqual(broker.calls[1][1], 25.0, places=4)
        self.assertEqual(len(trader.pending_orders), 2)

    def test_portfolio_netting_falls_back_to_market_slug_when_condition_missing(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        strategy = _DummyStrategy(
            [
                _signal("BUY", token_id="token-a", market_slug="shared-market", observed_notional=200.0),
                _signal("BUY", token_id="token-b", market_slug="shared-market", observed_notional=200.0),
            ]
        )
        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                max_signals_per_cycle=2,
                bankroll_usd=5000.0,
                max_condition_exposure_pct=0.012,
            ),
            data_client=_DummyDataClient(),
            strategy=strategy,
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()

        self.assertEqual(len(broker.calls), 2)
        self.assertAlmostEqual(broker.calls[0][1], 50.0, places=4)
        self.assertAlmostEqual(broker.calls[1][1], 10.0, places=4)
        self.assertAlmostEqual(trader.positions_book["token-b"]["notional"], 10.0, places=4)
        self.assertEqual(trader.positions_book["token-b"]["condition_id"], "")

    def test_manual_mode_queues_candidates_and_executes_after_approval(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        strategy = _DummyStrategy([_signal("BUY", token_id="token-manual", market_slug="manual-market")])
        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
            ),
            data_client=_DummyDataClient(),
            strategy=strategy,
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()

        queued = trader.list_candidates(limit=4)
        self.assertEqual(len(broker.calls), 0)
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0]["status"], "pending")

        assert trader.candidate_store is not None
        trader.candidate_store.record_candidate_action(queued[0]["id"], action="follow", note="ship it")
        strategy._signals = []

        trader.step()

        executed = trader.candidate_store.get_candidate(queued[0]["id"])
        self.assertEqual(len(broker.calls), 1)
        self.assertIsNotNone(executed)
        assert executed is not None
        self.assertEqual(executed["status"], "executed")
        self.assertIn("token-manual", trader.positions_book)
        self.assertGreaterEqual(trader.journal_summary()["total_entries"], 1)

    def test_candidate_blocks_cross_wallet_repeat_entry(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        strategy = _DummyStrategy([_signal("BUY", token_id="token-conflict", market_slug="conflict-market", wallet="0xnew")])
        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
            ),
            data_client=_DummyDataClient(),
            strategy=strategy,
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-conflict"] = {
            "token_id": "token-conflict",
            "condition_id": "condition-conflict",
            "market_slug": "conflict-market",
            "outcome": "YES",
            "quantity": 20.0,
            "price": 0.5,
            "notional": 10.0,
            "cost_basis_notional": 10.0,
            "opened_ts": int(time.time()) - 300,
            "last_buy_ts": int(time.time()) - 300,
            "last_trim_ts": 0,
            "entry_wallet": "0xold",
            "entry_wallet_score": 82.0,
            "entry_wallet_tier": "CORE",
            "entry_topic_label": "Politics",
            "entry_topic_bias": "neutral",
            "entry_topic_multiplier": 1.0,
            "entry_topic_summary": "",
            "entry_reason": "existing position",
            "trace_id": "trc-conflict",
            "origin_signal_id": "sig-conflict",
            "last_signal_id": "sig-conflict",
            "last_exit_kind": "",
            "last_exit_label": "",
            "last_exit_summary": "",
            "last_exit_ts": 0,
        }

        trader.step()

        queued = trader.list_candidates(limit=4)
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0]["skip_reason"], REASON_CROSS_WALLET_REPEAT_ENTRY_BLOCKED)
        self.assertEqual(queued[0]["block_reason"], REASON_CROSS_WALLET_REPEAT_ENTRY_BLOCKED)
        self.assertEqual(queued[0]["block_layer"], "candidate")
        self.assertEqual(queued[0]["suggested_action"], "watch")
        self.assertTrue(queued[0]["has_existing_position"])
        self.assertTrue(queued[0]["existing_position_conflict"])
        self.assertIn("新的钱包信号不能继续放大仓位", " ".join(queued[0].get("explanation") or []))
        factor_keys = [str(item.get("key") or "") for item in queued[0].get("reason_factors") or []]
        self.assertIn("existing_position", factor_keys)
        self.assertIn("skip_reason", factor_keys)
        self.assertIn("decision", factor_keys)
        self.assertIn("别的钱包", " ".join(str(item.get("detail") or "") for item in queued[0].get("reason_factors") or []))

    def test_candidate_blocks_same_wallet_add_by_default(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        wallet = "0x1111111111111111111111111111111111111111"
        strategy = _DummyStrategy([_signal("BUY", token_id="token-add-default", market_slug="add-market", wallet=wallet)])
        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
                token_add_cooldown_seconds=0,
            ),
            data_client=_DummyDataClient(),
            strategy=strategy,
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-add-default"] = {
            "token_id": "token-add-default",
            "condition_id": "condition-add-default",
            "market_slug": "add-market",
            "outcome": "YES",
            "quantity": 20.0,
            "price": 0.5,
            "notional": 10.0,
            "cost_basis_notional": 10.0,
            "opened_ts": int(time.time()) - 300,
            "last_buy_ts": int(time.time()) - 300,
            "last_trim_ts": 0,
            "entry_wallet": wallet,
            "entry_wallet_score": 82.0,
            "entry_wallet_tier": "CORE",
            "entry_reason": "existing position",
            "trace_id": "trc-add-default",
            "origin_signal_id": "sig-add-default",
            "last_signal_id": "sig-add-default",
            "last_exit_kind": "",
            "last_exit_label": "",
            "last_exit_summary": "",
            "last_exit_ts": 0,
        }

        trader.step()

        queued = trader.list_candidates(limit=4)
        self.assertEqual(len(broker.calls), 0)
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0]["skip_reason"], REASON_SAME_WALLET_ADD_NOT_ALLOWED)
        self.assertEqual(queued[0]["block_reason"], REASON_SAME_WALLET_ADD_NOT_ALLOWED)
        self.assertEqual(queued[0]["block_layer"], "candidate")
        self.assertEqual(queued[0]["status"], "watched")

    def test_cross_wallet_repeat_entry_does_not_enlarge_existing_position_in_auto_mode(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="auto",
                max_signals_per_cycle=1,
                token_add_cooldown_seconds=0,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy(
                [
                    _signal(
                        "BUY",
                        token_id="token-cross-auto",
                        market_slug="cross-auto-market",
                        wallet="0xnew000000000000000000000000000000000000",
                    )
                ]
            ),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-cross-auto"] = {
            "token_id": "token-cross-auto",
            "condition_id": "condition-cross-auto",
            "market_slug": "cross-auto-market",
            "outcome": "YES",
            "quantity": 50.0,
            "price": 0.5,
            "notional": 25.0,
            "cost_basis_notional": 25.0,
            "opened_ts": int(time.time()) - 300,
            "last_buy_ts": int(time.time()) - 300,
            "last_trim_ts": 0,
            "entry_wallet": "0xold000000000000000000000000000000000000",
            "entry_wallet_score": 80.0,
            "entry_wallet_tier": "CORE",
            "entry_reason": "existing position",
            "trace_id": "trc-cross-auto",
            "origin_signal_id": "sig-cross-auto",
            "last_signal_id": "sig-cross-auto",
            "last_exit_kind": "",
            "last_exit_label": "",
            "last_exit_summary": "",
            "last_exit_ts": 0,
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertEqual(len(broker.calls), 0)
        self.assertAlmostEqual(trader.positions_book["token-cross-auto"]["notional"], 25.0, places=4)
        queued = trader.list_candidates(limit=4)
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0]["skip_reason"], REASON_CROSS_WALLET_REPEAT_ENTRY_BLOCKED)

    def test_same_wallet_add_requires_explicit_allowlist(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        wallet = "0x1111111111111111111111111111111111111111"
        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="auto",
                max_signals_per_cycle=1,
                token_add_cooldown_seconds=0,
                same_wallet_add_enabled=True,
                same_wallet_add_allowlist=wallet,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY", token_id="token-demo", market_slug="demo-market", wallet=wallet)]),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-demo"] = {
            "token_id": "token-demo",
            "condition_id": "condition-demo",
            "market_slug": "demo-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.6,
            "notional": 25.0,
            "cost_basis_notional": 25.0,
            "opened_ts": 0,
            "last_buy_ts": 0,
            "last_trim_ts": 0,
            "entry_wallet": wallet,
            "entry_wallet_score": 82.0,
            "entry_wallet_tier": "CORE",
            "entry_reason": "existing position",
            "trace_id": "trc-allowed-add",
            "origin_signal_id": "sig-allowed-add",
            "last_signal_id": "sig-allowed-add",
            "last_exit_kind": "",
            "last_exit_label": "",
            "last_exit_summary": "",
            "last_exit_ts": 0,
        }
        trader.state.open_positions = 1

        trader.step()

        self.assertEqual(len(broker.calls), 1)
        self.assertEqual(broker.calls[0][0].position_action, "add")
        self.assertGreater(trader.positions_book["token-demo"]["notional"], 25.0)
        self.assertEqual(trader.positions_book["token-demo"]["entry_wallet"], wallet)

    def test_manual_approved_repeat_entry_cannot_bypass_execution_precheck(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        wallet = "0x1111111111111111111111111111111111111111"
        strategy = _DummyStrategy([_signal("BUY", token_id="token-manual-block", market_slug="manual-block-market", wallet=wallet)])
        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
                token_add_cooldown_seconds=0,
            ),
            data_client=_DummyDataClient(),
            strategy=strategy,
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-manual-block"] = {
            "token_id": "token-manual-block",
            "condition_id": "condition-manual-block",
            "market_slug": "manual-block-market",
            "outcome": "YES",
            "quantity": 20.0,
            "price": 0.5,
            "notional": 10.0,
            "cost_basis_notional": 10.0,
            "opened_ts": int(time.time()) - 300,
            "last_buy_ts": int(time.time()) - 300,
            "last_trim_ts": 0,
            "entry_wallet": wallet,
            "entry_wallet_score": 82.0,
            "entry_wallet_tier": "CORE",
            "entry_reason": "existing position",
            "trace_id": "trc-manual-block",
            "origin_signal_id": "sig-manual-block",
            "last_signal_id": "sig-manual-block",
            "last_exit_kind": "",
            "last_exit_label": "",
            "last_exit_summary": "",
            "last_exit_ts": 0,
        }

        trader.step()

        queued = trader.list_candidates(limit=4)
        self.assertEqual(len(queued), 1)
        self.assertEqual(queued[0]["skip_reason"], REASON_SAME_WALLET_ADD_NOT_ALLOWED)
        assert trader.candidate_store is not None
        trader.candidate_store.record_candidate_action(queued[0]["id"], action="follow", note="force it")
        strategy._signals = []

        trader.step()

        blocked = trader.candidate_store.get_candidate(queued[0]["id"])
        self.assertEqual(len(broker.calls), 0)
        self.assertIsNotNone(blocked)
        assert blocked is not None
        self.assertEqual(blocked["status"], "skipped")
        latest_cycle = trader.recent_signal_cycles[0]
        latest_candidate = latest_cycle["candidates"][0]
        self.assertEqual(latest_candidate["decision_snapshot"]["block_reason"], REASON_SAME_WALLET_ADD_NOT_ALLOWED)
        self.assertEqual(latest_candidate["decision_snapshot"]["block_layer"], "execution_precheck")

    def test_candidate_lifetime_uses_generation_timestamp_not_signal_timestamp(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        signal_time = datetime.now(timezone.utc) - timedelta(seconds=600)
        signal = _signal("BUY", token_id="token-lifetime-created", market_slug="candidate-life-market")
        signal.timestamp = signal_time
        now_ts = int(time.time())
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
                candidate_ttl_seconds=900,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        candidate = trader._candidate_from_signal(signal, now=now_ts)

        self.assertEqual(candidate.created_ts, now_ts)
        self.assertEqual(candidate.expires_ts, now_ts + 900)

    def test_approved_candidate_expires_at_decision_layer_before_queue(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        now_ts = int(time.time())
        signal = _signal("BUY", token_id="token-approved-expired", market_slug="approved-expired-market")
        signal.signal_id = "sig-approved-expired"
        signal.trace_id = "trc-approved-expired"
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
                candidate_ttl_seconds=900,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )
        candidate = trader._candidate_from_signal(signal, now=now_ts - 1800)
        candidate.status = "approved"
        candidate.selected_action = "follow"
        candidate.expires_ts = now_ts + 3600
        assert trader.candidate_store is not None
        trader.candidate_store.upsert_candidate(candidate)

        plans = trader._claim_approved_candidate_plans()

        expired = trader.candidate_store.get_candidate(candidate.id)
        self.assertEqual(plans, [])
        self.assertIsNotNone(expired)
        assert expired is not None
        self.assertEqual(expired["status"], "expired")
        self.assertEqual(expired["block_reason"], REASON_CANDIDATE_LIFETIME_EXPIRED)
        self.assertEqual(expired["block_layer"], "decision")
        self.assertEqual(expired["lifecycle_state"], "expired_discarded")

    def test_stale_queued_candidate_cannot_bypass_execution_precheck(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 1,
                },
                f,
            )
            control_path = f.name

        now_ts = int(time.time())
        signal = _signal("BUY", token_id="token-queued-expired", market_slug="queued-expired-market")
        signal.signal_id = "sig-queued-expired"
        signal.trace_id = "trc-queued-expired"
        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
                candidate_ttl_seconds=900,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=broker,
        )
        candidate = trader._candidate_from_signal(signal, now=now_ts - 1800)
        candidate.status = "queued"
        candidate.selected_action = "follow"
        candidate.note = "force stale queue"
        assert trader.candidate_store is not None
        trader.candidate_store.upsert_candidate(candidate)
        queued_signal = trader._candidate_to_signal(trader.candidate_store.get_candidate(candidate.id) or {})
        assert queued_signal is not None

        with patch.object(Trader, "_claim_approved_candidate_plans", return_value=[{
            "signal": queued_signal,
            "candidate_id": candidate.id,
            "candidate_action": "follow",
            "candidate_note": "force stale queue",
            "origin": "approved_queue",
        }]):
            trader.step()

        blocked = trader.candidate_store.get_candidate(candidate.id)
        self.assertEqual(len(broker.calls), 0)
        self.assertIsNotNone(blocked)
        assert blocked is not None
        self.assertEqual(blocked["status"], "expired")
        self.assertEqual(blocked["block_reason"], REASON_CANDIDATE_LIFETIME_EXPIRED)
        self.assertEqual(blocked["block_layer"], "execution_precheck")
        self.assertEqual(blocked["lifecycle_state"], "expired_discarded")
        latest_cycle = trader.recent_signal_cycles[0]
        latest_candidate = latest_cycle["candidates"][0]
        self.assertEqual(latest_candidate["decision_snapshot"]["block_reason"], REASON_CANDIDATE_LIFETIME_EXPIRED)
        self.assertEqual(latest_candidate["decision_snapshot"]["block_layer"], "execution_precheck")

    def test_candidate_enrich_uses_price_history_momentum(self):
        class _HistoryDataClient(_DummyDataClient):
            def __init__(self, *, history: list[dict[str, object]]):
                super().__init__()
                self.history = list(history)

            def get_order_book(self, _token_id: str):
                return type("Book", (), {"best_bid": 0.58, "best_ask": 0.64})()

            def get_midpoint_price(self, _token_id: str):
                return 0.61

            def get_price_history(self, _token_id: str, **_kwargs):
                return list(self.history)

        now_ts = int(time.time())
        signal = _signal("BUY", token_id="token-history", market_slug="history-market")
        signal.price_hint = 0.60
        signal.observed_notional = 400.0
        rich_history_client = _HistoryDataClient(
            history=[
                {"t": now_ts - 1800, "p": 0.54},
                {"t": now_ts - 600, "p": 0.58},
                {"t": now_ts - 300, "p": 0.60},
                {"t": now_ts - 60, "p": 0.62},
            ]
        )
        sparse_history_client = _HistoryDataClient(history=[])

        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 35,
                },
                f,
            )
            rich_control_path = f.name
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 36,
                },
                f,
            )
            plain_control_path = f.name

        settings = self._make_settings(
            rich_control_path,
            decision_mode="manual",
            max_signals_per_cycle=1,
        )
        trader_with_history = Trader(
            settings=settings,
            data_client=rich_history_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )
        trader_without_history = Trader(
            settings=self._make_settings(
                plain_control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
            ),
            data_client=sparse_history_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        rich_candidate = trader_with_history._candidate_from_signal(signal, now=now_ts)
        plain_candidate = trader_without_history._candidate_from_signal(signal, now=now_ts)

        self.assertGreater(float(rich_candidate.momentum_5m or 0.0), 0.0)
        self.assertGreater(float(rich_candidate.momentum_30m or 0.0), 0.0)
        self.assertGreater(float(rich_candidate.score or 0.0), float(plain_candidate.score or 0.0))
        self.assertIn("5m", " ".join(rich_candidate.explanation))
        self.assertIn("30m", " ".join(rich_candidate.explanation))
        self.assertIn("趋势", " ".join(rich_candidate.explanation))
        factor_keys = [factor.key for factor in rich_candidate.reason_factors]
        self.assertIn("momentum", factor_keys)
        self.assertIn("spread", factor_keys)
        self.assertIn("chase", factor_keys)
        self.assertIn("decision", factor_keys)
        momentum_factor = next(factor for factor in rich_candidate.reason_factors if factor.key == "momentum")
        self.assertIn("5m", momentum_factor.value)
        self.assertIn("30m", momentum_factor.value)

    def test_auto_mode_skips_buy_candidates_without_live_orderbook(self):
        class _NoOrderbookDataClient(_DummyDataClient):
            def get_order_book(self, _token_id: str):
                return None

            def get_midpoint_price(self, _token_id: str):
                return None

            def get_price_history(self, _token_id: str, **_kwargs):
                return []

        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "auto",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 37,
                },
                f,
            )
            control_path = f.name

        signal = _signal("BUY", token_id="token-no-book", market_slug="stale-market")
        signal.price_hint = 0.74
        signal.observed_notional = 250.0
        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="auto",
                max_signals_per_cycle=1,
            ),
            data_client=_NoOrderbookDataClient(),
            strategy=_DummyStrategy([signal]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()

        queued = trader.list_candidates(limit=4)
        self.assertEqual(len(broker.calls), 0)
        self.assertEqual(len(queued), 0)

    def test_candidate_from_signal_requires_live_ask_for_buy_market_data(self):
        class _BidOnlyDataClient(_DummyDataClient):
            def __init__(self):
                super().__init__()
                self.order_book = SimpleNamespace(best_bid=0.01, best_ask=0.0)
                self.midpoint_price = 0.995

        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 38,
                },
                f,
            )
            control_path = f.name

        now_ts = int(time.time())
        signal = _signal(
            "BUY",
            token_id="token-bid-only",
            market_slug=f"eth-updown-15m-{now_ts - 60}",
            observed_notional=250.0,
        )
        signal.price_hint = 0.74
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
            ),
            data_client=_BidOnlyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        candidate = trader._candidate_from_signal(signal, now=now_ts)

        self.assertEqual(candidate.skip_reason, "market_data_unavailable")
        self.assertIsNone(candidate.chase_pct)
        self.assertAlmostEqual(float(candidate.current_midpoint or 0.0), 0.995, places=4)

    def test_candidate_expiry_caps_to_short_market_end(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 39,
                },
                f,
            )
            control_path = f.name

        now_ts = int(time.time())
        market_start = now_ts - 120
        signal = _signal(
            "BUY",
            token_id="token-short-window",
            market_slug=f"btc-updown-5m-{market_start}",
        )
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        candidate = trader._candidate_from_signal(signal, now=now_ts)

        self.assertEqual(candidate.expires_ts, market_start + 300)
        self.assertEqual(candidate.market_time_source, "slug_legacy")
        self.assertFalse(candidate.market_metadata_hit)

    def test_candidate_marks_market_not_accepting_orders(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 39,
                },
                f,
            )
            control_path = f.name

        now_ts = int(time.time())
        signal = _signal(
            "BUY",
            token_id="token-no-orders",
            condition_id="condition-no-orders",
            market_slug="metadata-market",
        )
        data_client = _DummyDataClient()
        data_client.market_metadata["condition-no-orders"] = MarketMetadata(
            condition_id="condition-no-orders",
            market_slug="metadata-market",
            end_ts=now_ts + 3600,
            end_date="2026-03-22T12:00:00Z",
            closed=False,
            active=True,
            accepting_orders=False,
        )
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
            ),
            data_client=data_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        candidate = trader._candidate_from_signal(signal, now=now_ts)

        self.assertEqual(candidate.skip_reason, "market_not_accepting_orders")
        self.assertEqual(candidate.suggested_action, "watch")
        self.assertEqual(candidate.status, "watched")
        self.assertEqual(candidate.market_time_source, "metadata")
        self.assertTrue(candidate.market_metadata_hit)
        self.assertIn("acceptingOrders=false", " ".join(candidate.explanation))
        factor_keys = [factor.key for factor in candidate.reason_factors]
        self.assertIn("market_state", factor_keys)

    def test_precheck_skipped_buy_signal_is_recorded_in_recent_signal_cycles(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 40,
                },
                f,
            )
            control_path = f.name

        now_ts = int(time.time())
        data_client = _DummyDataClient()
        data_client.market_metadata["condition-no-orders"] = MarketMetadata(
            condition_id="condition-no-orders",
            market_slug="metadata-market",
            end_ts=now_ts + 3600,
            end_date="2026-03-22T12:00:00Z",
            closed=False,
            active=True,
            accepting_orders=False,
        )
        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
            ),
            data_client=data_client,
            strategy=_DummyStrategy(
                [
                    _signal(
                        "BUY",
                        token_id="token-no-orders",
                        condition_id="condition-no-orders",
                        market_slug="metadata-market",
                    )
                ]
            ),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()

        self.assertEqual(len(broker.calls), 0)
        self.assertEqual(len(trader.recent_signal_cycles), 1)
        cycle = trader.recent_signal_cycles[0]
        self.assertEqual(len(cycle["candidates"]), 1)
        record = cycle["candidates"][0]
        self.assertEqual(record["final_status"], "precheck_skipped")
        self.assertEqual(record["decision_snapshot"]["skip_reason"], "market_not_accepting_orders")
        self.assertEqual(record["decision_snapshot"]["market_time_source"], "metadata")
        self.assertTrue(record["decision_snapshot"]["market_metadata_hit"])
        self.assertFalse(record["decision_snapshot"]["market_closed"])
        self.assertTrue(record["decision_snapshot"]["market_active"])
        self.assertFalse(record["decision_snapshot"]["market_accepting_orders"])

    def test_candidate_uses_metadata_end_ts_for_elapsed_market(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 39,
                },
                f,
            )
            control_path = f.name

        now_ts = int(time.time())
        signal = _signal(
            "BUY",
            token_id="token-ended-market",
            condition_id="condition-ended-market",
            market_slug="metadata-ended-market",
        )
        data_client = _DummyDataClient()
        data_client.market_metadata["condition-ended-market"] = MarketMetadata(
            condition_id="condition-ended-market",
            market_slug="metadata-ended-market",
            end_ts=now_ts - 5,
            end_date="2026-03-20T23:59:00Z",
            closed=False,
            active=True,
            accepting_orders=True,
        )
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
            ),
            data_client=data_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        candidate = trader._candidate_from_signal(signal, now=now_ts)

        self.assertEqual(candidate.skip_reason, "market_window_elapsed")
        self.assertEqual(candidate.suggested_action, "watch")
        self.assertEqual(candidate.expires_ts, now_ts - 5)
        self.assertEqual(candidate.market_time_source, "metadata")
        self.assertTrue(candidate.market_metadata_hit)
        self.assertIn("time=metadata", " ".join(candidate.explanation))

    def test_auto_mode_skips_closed_market_buy_candidates(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "auto",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 40,
                },
                f,
            )
            control_path = f.name

        now_ts = int(time.time())
        signal = _signal(
            "BUY",
            token_id="token-closed-market",
            condition_id="condition-closed-market",
            market_slug="metadata-closed-market",
            observed_notional=250.0,
        )
        broker = _DummyBroker()
        data_client = _DummyDataClient()
        data_client.market_metadata["condition-closed-market"] = MarketMetadata(
            condition_id="condition-closed-market",
            market_slug="metadata-closed-market",
            end_ts=now_ts + 600,
            end_date="2026-03-22T12:00:00Z",
            closed=True,
            active=False,
            accepting_orders=False,
        )
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="auto",
                max_signals_per_cycle=1,
            ),
            data_client=data_client,
            strategy=_DummyStrategy([signal]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()

        queued = trader.list_candidates(limit=4)
        self.assertEqual(len(broker.calls), 0)
        self.assertEqual(len(queued), 0)

    def test_manual_mode_moves_extreme_wide_buy_candidate_to_watched(self):
        class _ExtremeWideBookDataClient(_DummyDataClient):
            def __init__(self):
                super().__init__()
                self.order_book = SimpleNamespace(best_bid=0.001, best_ask=0.999)
                self.midpoint_price = 0.998

        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 39,
                },
                f,
            )
            control_path = f.name

        signal = _signal(
            "BUY",
            token_id="token-extreme-book",
            market_slug="extreme-book-market",
            observed_notional=250.0,
        )
        signal.price_hint = 0.998
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
            ),
            data_client=_ExtremeWideBookDataClient(),
            strategy=_DummyStrategy([signal]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        trader.step()

        pending = trader.list_candidates(statuses=["pending"], limit=4)
        watched = trader.list_candidates(statuses=["watched"], limit=4)

        self.assertEqual(pending, [])
        self.assertEqual(len(watched), 1)
        self.assertEqual(watched[0]["skip_reason"], "spread_too_wide")
        self.assertEqual(watched[0]["suggested_action"], "watch")
        self.assertEqual(watched[0]["status"], "watched")

    def test_auto_mode_skips_buy_candidates_near_market_close(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "auto",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 40,
                },
                f,
            )
            control_path = f.name

        now_ts = int(time.time())
        signal = _signal(
            "BUY",
            token_id="token-near-close",
            market_slug=f"btc-updown-5m-{now_ts - 260}",
            observed_notional=250.0,
        )
        broker = _DummyBroker()
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="auto",
                max_signals_per_cycle=1,
            ),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([signal]),
            risk=_DummyRisk(),
            broker=broker,
        )

        trader.step()

        queued = trader.list_candidates(limit=4)
        self.assertEqual(len(broker.calls), 0)
        self.assertEqual(len(queued), 0)

    def test_step_expires_pending_buy_candidate_when_revalidation_fails(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 41,
                },
                f,
            )
            control_path = f.name

        now_ts = int(time.time())
        signal = _signal(
            "BUY",
            token_id="token-stale-pending",
            market_slug=f"eth-updown-15m-{now_ts - 120}",
            observed_notional=250.0,
        )
        signal.signal_id = "sig-stale-pending"
        signal.trace_id = "trc-stale-pending"
        signal.price_hint = 0.74
        data_client = _DummyDataClient()
        data_client.order_book = SimpleNamespace(best_bid=0.72, best_ask=0.74)
        data_client.midpoint_price = 0.73
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
            ),
            data_client=data_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        candidate = trader._candidate_from_signal(signal, now=now_ts)
        assert trader.candidate_store is not None
        trader.candidate_store.upsert_candidate(candidate)

        data_client.order_book = SimpleNamespace(best_bid=0.01, best_ask=0.99)
        data_client.midpoint_price = 0.50

        trader.step()

        self.assertEqual(trader.list_candidates(limit=4), [])
        expired = trader.candidate_store.get_candidate(candidate.id)
        self.assertIsNotNone(expired)
        self.assertEqual(expired["status"], "expired")
        self.assertEqual(expired["result_tag"], "candidate_revalidated_chase_too_high")

    def test_step_refreshes_pending_buy_candidate_market_snapshot(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "updated_ts": 42,
                },
                f,
            )
            control_path = f.name

        now_ts = int(time.time())
        signal = _signal(
            "BUY",
            token_id="token-refresh-pending",
            market_slug=f"btc-updown-15m-{now_ts - 120}",
            observed_notional=250.0,
        )
        signal.signal_id = "sig-refresh-pending"
        signal.trace_id = "trc-refresh-pending"
        signal.price_hint = 0.60
        data_client = _DummyDataClient()
        data_client.order_book = SimpleNamespace(best_bid=0.48, best_ask=0.52)
        data_client.midpoint_price = 0.50
        trader = Trader(
            settings=self._make_settings(
                control_path,
                decision_mode="manual",
                max_signals_per_cycle=1,
            ),
            data_client=data_client,
            strategy=_DummyStrategy([]),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        candidate = trader._candidate_from_signal(signal, now=now_ts)
        assert trader.candidate_store is not None
        trader.candidate_store.upsert_candidate(candidate)

        data_client.order_book = SimpleNamespace(best_bid=0.54, best_ask=0.56)
        data_client.midpoint_price = 0.55

        trader.step()

        refreshed = trader.list_candidates(limit=4)
        self.assertEqual(len(refreshed), 1)
        self.assertEqual(refreshed[0]["id"], candidate.id)
        self.assertAlmostEqual(float(refreshed[0]["current_best_ask"] or 0.0), 0.56, places=4)
        self.assertAlmostEqual(float(refreshed[0]["current_best_bid"] or 0.0), 0.54, places=4)
        self.assertGreaterEqual(int(refreshed[0]["updated_ts"] or 0), now_ts)


class RiskManagerTests(unittest.TestCase):
    def test_broker_closed_pnl_today_can_trip_daily_limit(self):
        settings = Settings(
            _env_file=None,
            bankroll_usd=100.0,
            risk_per_trade_pct=0.05,
            daily_max_loss_pct=0.1,
            max_open_positions=4,
            min_price=0.08,
            max_price=0.92,
        )
        state = RiskState(
            daily_realized_pnl=-2.0,
            broker_closed_pnl_today=-12.0,
            open_positions=1,
            tracked_notional_usd=10.0,
            pending_entry_notional_usd=0.0,
            pending_exit_notional_usd=0.0,
            pending_entry_orders=0,
        )

        decision = RiskManager(settings).evaluate(_signal("BUY", observed_notional=100.0), state)

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "daily_loss_limit_reached")

    def test_pending_entry_notional_caps_new_buy(self):
        settings = Settings(
            _env_file=None,
            bankroll_usd=100.0,
            risk_per_trade_pct=0.05,
            daily_max_loss_pct=0.1,
            max_open_positions=4,
            min_price=0.08,
            max_price=0.92,
        )
        state = RiskState(
            daily_realized_pnl=0.0,
            open_positions=1,
            tracked_notional_usd=30.0,
            pending_entry_notional_usd=66.0,
            pending_exit_notional_usd=0.0,
            pending_entry_orders=1,
        )

        decision = RiskManager(settings).evaluate(_signal("BUY", observed_notional=100.0), state)

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "remaining_bankroll_capacity_too_small")

    def test_pending_entry_orders_count_toward_position_cap(self):
        settings = Settings(
            _env_file=None,
            bankroll_usd=1000.0,
            risk_per_trade_pct=0.05,
            daily_max_loss_pct=0.1,
            max_open_positions=2,
            min_price=0.08,
            max_price=0.92,
        )
        state = RiskState(
            daily_realized_pnl=0.0,
            open_positions=1,
            tracked_notional_usd=100.0,
            pending_entry_notional_usd=50.0,
            pending_exit_notional_usd=0.0,
            pending_entry_orders=1,
        )

        decision = RiskManager(settings).evaluate(_signal("BUY", observed_notional=200.0), state)

        self.assertFalse(decision.allowed)
        self.assertEqual(decision.reason, "max_open_positions_reached")

    def test_trading_mode_blocks_buy_but_not_sell(self):
        settings = Settings(
            _env_file=None,
            bankroll_usd=1000.0,
            risk_per_trade_pct=0.05,
            daily_max_loss_pct=0.1,
            max_open_positions=4,
            min_price=0.08,
            max_price=0.92,
        )
        state = RiskState(
            daily_realized_pnl=0.0,
            open_positions=1,
            tracked_notional_usd=100.0,
            pending_entry_notional_usd=0.0,
            pending_exit_notional_usd=0.0,
            pending_entry_orders=0,
            trading_mode="REDUCE_ONLY",
            trading_mode_reasons=("startup_not_ready",),
        )

        buy_decision = RiskManager(settings).evaluate(_signal("BUY", observed_notional=200.0), state)
        sell_decision = RiskManager(settings).evaluate(_signal("SELL", observed_notional=200.0), state)

        self.assertFalse(buy_decision.allowed)
        self.assertEqual(buy_decision.reason, "system_reduce_only")
        self.assertTrue(sell_decision.allowed)
        self.assertEqual(sell_decision.reason, "ok")


if __name__ == "__main__":
    unittest.main()
