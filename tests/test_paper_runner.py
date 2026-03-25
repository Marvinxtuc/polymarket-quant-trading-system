from __future__ import annotations

import json
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from polymarket_bot.brokers.paper import PaperBroker
from polymarket_bot.config import Settings
from polymarket_bot.runner import ControlState, Trader
from polymarket_bot.types import BrokerOrderEvent, ExecutionResult, OrderFillSnapshot, RiskDecision, Signal


class _DummyDataClient:
    def __init__(self):
        self.order_book = SimpleNamespace(best_bid=0.48, best_ask=0.52)
        self.midpoint_price = 0.5

    def discover_wallet_activity(self, paths, limit):
        return {}

    def get_order_book(self, _token_id: str):
        return self.order_book

    def get_midpoint_price(self, _token_id: str):
        return self.midpoint_price

    def get_price_history(self, _token_id: str, **_kwargs):
        return []

    def close(self):
        return None


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
        return RiskDecision(True, "ok", 20.0)


class _CapabilityBroker:
    def __init__(self):
        self._fills: list[OrderFillSnapshot] = []
        self._events: list[BrokerOrderEvent] = []

    def supports_dry_run_pending_reconcile(self) -> bool:
        return True

    def execute(self, signal, notional_usd):
        price = max(0.01, float(signal.price_hint or 0.5))
        size = notional_usd / price
        order_id = f"cap-{signal.token_id}"
        self._fills = [
            OrderFillSnapshot(
                order_id=order_id,
                token_id=str(signal.token_id),
                side=str(signal.side),
                price=price,
                size=size,
                timestamp=105,
                tx_hash=f"tx-{order_id}",
                market_slug=str(signal.market_slug),
                outcome=str(signal.outcome),
            )
        ]
        self._events = [
            BrokerOrderEvent(
                event_type="fill",
                order_id=order_id,
                token_id=str(signal.token_id),
                side=str(signal.side),
                timestamp=105,
                matched_notional=notional_usd,
                matched_size=size,
                avg_fill_price=price,
                tx_hash=f"tx-{order_id}",
                market_slug=str(signal.market_slug),
                outcome=str(signal.outcome),
            )
        ]
        return ExecutionResult(
            ok=True,
            broker_order_id=order_id,
            message="capability broker posted",
            filled_notional=0.0,
            filled_price=0.0,
            status="live",
            requested_notional=notional_usd,
            requested_price=price,
        )

    def heartbeat(self, order_ids):
        return bool(order_ids)

    def list_recent_fills(self, *, since_ts=0, order_ids=None, limit=200):
        return list(self._fills)

    def list_order_events(self, *, since_ts=0, order_ids=None, limit=200):
        return list(self._events)

    def get_order_status(self, order_id):
        return None

    def list_open_orders(self):
        return []


def _signal(side: str = "BUY", **kwargs: object) -> Signal:
    return Signal(
        signal_id="",
        trace_id="",
        wallet=str(kwargs.get("wallet", "0x1111111111111111111111111111111111111111")),
        market_slug=str(kwargs.get("market_slug", "demo-market")),
        token_id=str(kwargs.get("token_id", "token-demo")),
        outcome=str(kwargs.get("outcome", "YES")),
        side=side,  # type: ignore[arg-type]
        confidence=0.8,
        price_hint=float(kwargs.get("price_hint", 0.6)),
        observed_size=float(kwargs.get("observed_size", 10.0)),
        observed_notional=float(kwargs.get("observed_notional", 20.0)),
        timestamp=datetime.now(tz=timezone.utc),
        condition_id=str(kwargs.get("condition_id", "condition-demo")),
        wallet_score=float(kwargs.get("wallet_score", 80.0)),
        wallet_tier=str(kwargs.get("wallet_tier", "CORE")),
    )


def _make_settings(control_path: str, **kwargs: object) -> Settings:
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

    return Settings(
        _env_file=None,
        dry_run=True,
        decision_mode="auto",
        watch_wallets="0x1111111111111111111111111111111111111111",
        wallet_discovery_enabled=False,
        control_path=control_path,
        runtime_state_path=str(runtime_state_path),
        ledger_path=str(ledger_path),
        candidate_db_path=str(candidate_db_path),
        paper_live_like_enabled=bool(kwargs.get("paper_live_like_enabled", True)),
        paper_fill_delay_seconds=int(kwargs.get("paper_fill_delay_seconds", 5)),
        paper_partial_fill_ratio=float(kwargs.get("paper_partial_fill_ratio", 0.5)),
        paper_fill_complete_delay_seconds=int(kwargs.get("paper_fill_complete_delay_seconds", 0)),
        paper_cancel_fail_once=bool(kwargs.get("paper_cancel_fail_once", False)),
        runtime_reconcile_interval_seconds=int(kwargs.get("runtime_reconcile_interval_seconds", 60)),
        poll_interval_seconds=int(kwargs.get("poll_interval_seconds", 30)),
        pending_order_timeout_seconds=int(kwargs.get("pending_order_timeout_seconds", 1800)),
        bankroll_usd=float(kwargs.get("bankroll_usd", 5000.0)),
        max_signals_per_cycle=1,
    )


class PaperRunnerTests(unittest.TestCase):
    def test_dry_run_live_like_paper_pending_buy_reconciles_partial_fill(self):
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

        settings = _make_settings(control_path, paper_fill_delay_seconds=5, paper_partial_fill_ratio=0.5)
        broker = PaperBroker(settings=settings)
        trader = Trader(
            settings=settings,
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )

        with patch("polymarket_bot.runner.time.time", return_value=100), patch("polymarket_bot.brokers.paper.time.time", return_value=100):
            trader.step()

        self.assertEqual(len(trader.pending_orders), 1)
        self.assertEqual(trader.positions_book, {})
        self.assertEqual(trader.recent_orders[0]["status"], "PENDING")

        with patch("polymarket_bot.runner.time.time", return_value=106), patch("polymarket_bot.brokers.paper.time.time", return_value=106):
            trader._maybe_reconcile_runtime()

        self.assertIn("token-demo", trader.positions_book)
        self.assertAlmostEqual(float(trader.positions_book["token-demo"]["notional"]), 10.0, places=4)
        self.assertAlmostEqual(float(trader.positions_book["token-demo"]["cost_basis_notional"]), 10.0, places=4)
        self.assertEqual(len(trader.pending_orders), 1)
        pending = next(iter(trader.pending_orders.values()))
        self.assertEqual(str(pending["broker_status"]), "partially_filled")
        self.assertAlmostEqual(float(pending["reconciled_notional_hint"]), 10.0, places=4)
        self.assertEqual(trader.recent_orders[0]["status"], "PARTIAL")

    def test_dry_run_live_like_paper_cancel_reject_once_keeps_then_clears_pending(self):
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

        settings = _make_settings(
            control_path,
            paper_fill_delay_seconds=300,
            paper_partial_fill_ratio=1.0,
            paper_cancel_fail_once=True,
        )
        broker = PaperBroker(settings=settings)
        trader = Trader(
            settings=settings,
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )

        with patch("polymarket_bot.runner.time.time", return_value=100), patch("polymarket_bot.brokers.paper.time.time", return_value=100):
            trader.step()

        self.assertEqual(len(trader.pending_orders), 1)

        with patch("polymarket_bot.brokers.paper.time.time", return_value=101):
            trader._apply_control_pending_entry_cancels(ControlState(reduce_only=True), now=101)

        self.assertEqual(len(trader.pending_orders), 1)
        pending = next(iter(trader.pending_orders.values()))
        self.assertEqual(str(pending["cancel_last_status"]), "failed")
        self.assertEqual(trader.recent_orders[0]["status"], "CANCEL_FAILED")

        with patch("polymarket_bot.brokers.paper.time.time", return_value=170):
            trader._apply_control_pending_entry_cancels(ControlState(reduce_only=True), now=170)

        self.assertEqual(len(trader.pending_orders), 0)
        self.assertEqual(trader.recent_orders[0]["status"], "CANCELED")

    def test_dry_run_runtime_reconcile_uses_explicit_broker_capability(self):
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

        settings = _make_settings(
            control_path,
            paper_live_like_enabled=False,
            runtime_reconcile_interval_seconds=60,
        )
        broker = _CapabilityBroker()
        trader = Trader(
            settings=settings,
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )

        with patch("polymarket_bot.runner.time.time", return_value=100):
            trader.step()

        self.assertEqual(len(trader.pending_orders), 1)
        self.assertEqual(trader.positions_book, {})

        with patch("polymarket_bot.runner.time.time", return_value=106):
            trader._maybe_reconcile_runtime()

        self.assertIn("token-demo", trader.positions_book)
        self.assertEqual(len(trader.pending_orders), 0)
        self.assertEqual(trader.recent_orders[0]["status"], "RECONCILED")


if __name__ == "__main__":
    unittest.main()
