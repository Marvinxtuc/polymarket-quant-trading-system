from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from polymarket_bot.clients.data_api import AccountingSnapshot, ClosedPosition
from polymarket_bot.config import Settings
from polymarket_bot.risk import RiskManager, RiskState
from polymarket_bot.runner import Trader
from polymarket_bot.types import BrokerOrderEvent, ExecutionResult, OpenOrderSnapshot, OrderFillSnapshot, OrderStatusSnapshot, RiskDecision, Signal
from polymarket_bot.wallet_scoring import RealizedWalletMetrics


class _DummyDataClient:
    def __init__(self):
        self.active_positions = []
        self.accounting_snapshot = None
        self.closed_positions = []

    def discover_wallet_activity(self, paths, limit):
        return {}

    def get_active_positions(self, wallet):
        return list(self.active_positions)

    def get_accounting_snapshot(self, wallet):
        return self.accounting_snapshot

    def iter_closed_positions(self, wallet, **_kwargs):
        return iter(self.closed_positions)

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

    def execute(self, signal, notional_usd):
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
        return None


class _PendingBroker(_DummyBroker):
    def __init__(self):
        super().__init__()
        self.order_statuses: dict[str, OrderStatusSnapshot] = {}
        self.heartbeat_calls: list[list[str]] = []
        self.open_orders = None
        self.recent_fills = None
        self.order_events = None
        self.cancel_requests: list[str] = []
        self.cancel_responses: dict[str, dict[str, object]] = {}

    def execute(self, signal, notional_usd):
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

        return True
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
    def _make_settings(self, control_path: str, **kwargs: object) -> Settings:
        max_signals_per_cycle = int(kwargs.get("max_signals_per_cycle", 1))
        runtime_state_path = kwargs.get("runtime_state_path")
        ledger_path = kwargs.get("ledger_path")
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

        return Settings(
            _env_file=None,
            dry_run=bool(kwargs.get("dry_run", True)),
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
            max_signals_per_cycle=max_signals_per_cycle,
            poll_interval_seconds=60,
            bankroll_usd=float(kwargs.get("bankroll_usd", 5000.0)),
            account_sync_refresh_seconds=int(kwargs.get("account_sync_refresh_seconds", 300)),
            order_dedup_ttl_seconds=int(kwargs.get("order_dedup_ttl_seconds", 120)),
            runtime_reconcile_interval_seconds=int(kwargs.get("runtime_reconcile_interval_seconds", 180)),
            pending_order_timeout_seconds=int(kwargs.get("pending_order_timeout_seconds", 1800)),
            portfolio_netting_enabled=bool(kwargs.get("portfolio_netting_enabled", True)),
            max_condition_exposure_pct=float(kwargs.get("max_condition_exposure_pct", 0.015)),
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
            live_network_smoke_max_age_seconds=int(kwargs.get("live_network_smoke_max_age_seconds", 43200)),
            live_allowance_ready=bool(kwargs.get("live_allowance_ready", False)),
            live_geoblock_ready=bool(kwargs.get("live_geoblock_ready", False)),
            live_account_ready=bool(kwargs.get("live_account_ready", False)),
            funder_address=str(kwargs.get("funder_address", "")),
        )

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

        trader.step()
        self.assertEqual(len(trader.pending_orders), 1)
        old_key = next(iter(trader.pending_orders.keys()))
        trader.pending_orders[old_key]["ts"] = int(time.time()) - 180

        stale_order = dict(trader.pending_orders[old_key])
        fresh_order = dict(stale_order)
        fresh_order["key"] = "fresh-live-token-demo"
        fresh_order["order_id"] = "live-token-demo-fresh"
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

        trader.step()
        self.assertEqual(len(trader.pending_orders), 1)
        trader._reconcile_runtime_with_broker()

        pending = next(iter(trader.pending_orders.values()))
        self.assertEqual(broker.heartbeat_calls[-1], ["live-token-demo"])
        self.assertEqual(int(pending.get("last_heartbeat_ts") or 0), 0)

        fresh_order["signal_id"] = "sig-fresh"
        fresh_order["trace_id"] = "trc-fresh"
        fresh_order["token_id"] = "token-demo-fresh"
        fresh_order["market_slug"] = "demo-market-fresh"
        fresh_order["ts"] = int(time.time())
        trader.pending_orders[fresh_order["key"]] = fresh_order
        trader._refresh_risk_state()
        trader.strategy = _DummyStrategy([])

        with open(control_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "clear_stale_pending_requested_ts": int(time.time()),
                    "updated_ts": 31,
                },
                f,
            )

        trader.step()

        self.assertEqual(len(trader.pending_orders), 1)
        self.assertIn("fresh-live-token-demo", trader.pending_orders)
        self.assertEqual(trader.recent_orders[0]["status"], "CANCELED")
        self.assertIn("operator_clear_stale_pending", trader.recent_orders[0]["reason"])
        self.assertEqual(broker.cancel_requests, ["live-token-demo"])
        self.assertEqual(trader.last_operator_action["name"], "clear_stale_pending")
        self.assertEqual(trader.last_operator_action["status"], "cleared")
        self.assertEqual(trader.last_operator_action["cleared_count"], 1)
        self.assertEqual(trader.last_operator_action["remaining_pending_orders"], 1)
        self.assertAlmostEqual(
            trader.state.pending_entry_notional_usd,
            float(trader.pending_orders["fresh-live-token-demo"]["requested_notional"]),
            places=4,
        )

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

        trader.step()
        self.assertEqual(len(trader.pending_orders), 1)

        with open(control_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "pause_opening": False,
                    "reduce_only": True,
                    "emergency_stop": False,
                    "updated_ts": 33,
                },
                f,
            )

        trader.strategy = _DummyStrategy([])
        trader.step()

        self.assertEqual(len(trader.pending_orders), 0)
        self.assertEqual(broker.cancel_requests, ["live-token-demo"])
        self.assertEqual(trader.recent_orders[0]["status"], "CANCELED")
        self.assertIn("reduce_only_cancel_pending_entry", trader.recent_orders[0]["reason"])

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

        self.assertEqual(trader.positions_book, {})
        self.assertEqual(trader.state.open_positions, 0)

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

    def test_startup_drops_snapshot_pending_orders_when_broker_reports_none_open(self):
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

        self.assertEqual(len(trader.pending_orders), 0)

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
        self.assertEqual(dumped["runtime_version"], 6)

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
        broker.recent_fills = [
            OrderFillSnapshot(
                order_id="live-token-demo",
                token_id="token-demo",
                side="SELL",
                price=0.6,
                size=50.0,
                timestamp=1700000020,
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
        self.assertEqual(decision.reason, "daily loss limit reached")

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
        self.assertEqual(decision.reason, "remaining bankroll capacity too small")

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
        self.assertEqual(decision.reason, "max open positions reached")


if __name__ == "__main__":
    unittest.main()
