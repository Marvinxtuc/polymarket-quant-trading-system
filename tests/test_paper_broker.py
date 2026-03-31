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
from polymarket_bot.runner import Trader
from polymarket_bot.state_store import StateStore
from polymarket_bot.types import RiskDecision, Signal


def _signal(side: str = "BUY") -> Signal:
    return Signal(
        signal_id="sig-paper",
        trace_id="trc-paper",
        wallet="0x1111111111111111111111111111111111111111",
        market_slug="demo-market",
        token_id="token-demo",
        outcome="YES",
        side=side,  # type: ignore[arg-type]
        confidence=0.8,
        price_hint=0.6,
        observed_size=10.0,
        observed_notional=100.0,
        timestamp=datetime.now(tz=timezone.utc),
        condition_id="condition-demo",
        wallet_score=80.0,
        wallet_tier="CORE",
    )


class _DummyDataClient:
    def __init__(self):
        self.order_book = SimpleNamespace(best_bid=0.48, best_ask=0.52)

    def get_active_positions(self, wallet):
        return []

    def get_accounting_snapshot(self, wallet):
        return None

    def discover_wallet_activity(self, paths, limit):
        return {}

    def get_order_book(self, token_id: str):
        return self.order_book

    def get_midpoint_price(self, token_id: str):
        return 0.5

    def get_price_history(self, token_id: str, **kwargs):
        return []

    def iter_closed_positions(self, wallet, **kwargs):
        return iter(())

    def close(self):
        return None


class _DummyStrategy:
    def __init__(self, signals):
        self._signals = list(signals)

    def generate_signals(self, wallets):
        return list(self._signals)

    def update_wallet_selection_context(self, context):
        return None


class _DummyRisk:
    def evaluate(self, signal, state):
        return RiskDecision(True, "ok", 50.0)


class PaperBrokerTests(unittest.TestCase):
    def _make_settings(self, control_path: str, **kwargs: object) -> Settings:
        runtime_state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        runtime_state_file.write("{}")
        runtime_state_file.flush()
        runtime_state_file.close()
        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.close()
        candidate_db_path = str(Path(tempfile.mkdtemp()) / "terminal.db")
        settings = Settings(
            _env_file=None,
            dry_run=bool(kwargs.get("dry_run", True)),
            decision_mode="auto",
            watch_wallets="0x1111111111111111111111111111111111111111",
            control_path=control_path,
            runtime_state_path=runtime_state_file.name,
            ledger_path=ledger_file.name,
            candidate_db_path=candidate_db_path,
            bankroll_usd=5000.0,
            poll_interval_seconds=30,
            runtime_reconcile_interval_seconds=int(kwargs.get("runtime_reconcile_interval_seconds", 60)),
            paper_live_like_enabled=bool(kwargs.get("paper_live_like_enabled", False)),
            paper_fill_delay_seconds=int(kwargs.get("paper_fill_delay_seconds", 0)),
            paper_partial_fill_ratio=float(kwargs.get("paper_partial_fill_ratio", 1.0)),
            paper_fill_complete_delay_seconds=int(kwargs.get("paper_fill_complete_delay_seconds", 0)),
            paper_cancel_fail_once=bool(kwargs.get("paper_cancel_fail_once", False)),
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
        return settings

    def test_live_like_paper_broker_emits_partial_and_completion_fills(self):
        broker = PaperBroker(
            Settings(
                _env_file=None,
                dry_run=True,
                paper_live_like_enabled=True,
                paper_fill_delay_seconds=10,
                paper_partial_fill_ratio=0.5,
                paper_fill_complete_delay_seconds=30,
            )
        )
        signal = _signal("BUY")

        with patch("polymarket_bot.brokers.paper.time.time", return_value=100):
            result = broker.execute(signal, 20.0)

        self.assertTrue(result.is_pending)
        order_id = str(result.broker_order_id or "")

        with patch("polymarket_bot.brokers.paper.time.time", return_value=105):
            status = broker.get_order_status(order_id)
            open_orders = broker.list_open_orders() or []

        self.assertIsNotNone(status)
        assert status is not None
        self.assertEqual(status.lifecycle_status, "live")
        self.assertEqual(len(open_orders), 1)

        with patch("polymarket_bot.brokers.paper.time.time", return_value=111):
            fills = broker.list_recent_fills(since_ts=0, order_ids=[order_id]) or []
            events = broker.list_order_events(since_ts=0, order_ids=[order_id]) or []
            status = broker.get_order_status(order_id)
            open_orders = broker.list_open_orders() or []

        self.assertEqual(len(fills), 1)
        self.assertAlmostEqual(fills[0].notional, 10.0, places=4)
        assert status is not None
        self.assertEqual(status.lifecycle_status, "partially_filled")
        self.assertEqual(len(open_orders), 1)
        self.assertEqual([event.event_type for event in events], ["status", "fill", "status"])

        with patch("polymarket_bot.brokers.paper.time.time", return_value=141):
            fills = broker.list_recent_fills(since_ts=0, order_ids=[order_id]) or []
            status = broker.get_order_status(order_id)
            open_orders = broker.list_open_orders() or []

        self.assertEqual(len(fills), 2)
        assert status is not None
        self.assertEqual(status.lifecycle_status, "filled")
        self.assertEqual(open_orders, [])

    def test_live_like_paper_broker_can_fail_cancel_once(self):
        broker = PaperBroker(
            Settings(
                _env_file=None,
                dry_run=True,
                paper_live_like_enabled=True,
                paper_fill_delay_seconds=300,
                paper_partial_fill_ratio=1.0,
                paper_cancel_fail_once=True,
            )
        )
        signal = _signal("BUY")

        with patch("polymarket_bot.brokers.paper.time.time", return_value=200):
            result = broker.execute(signal, 20.0)
        order_id = str(result.broker_order_id or "")

        with patch("polymarket_bot.brokers.paper.time.time", return_value=201):
            first_cancel = broker.cancel_order(order_id)
        with patch("polymarket_bot.brokers.paper.time.time", return_value=202):
            open_orders = broker.list_open_orders() or []
            second_cancel = broker.cancel_order(order_id)
            status = broker.get_order_status(order_id)

        self.assertFalse(bool(first_cancel["ok"]))
        self.assertEqual(first_cancel["status"], "failed")
        self.assertEqual(len(open_orders), 1)
        self.assertTrue(bool(second_cancel["ok"]))
        self.assertEqual(second_cancel["status"], "canceled")
        assert status is not None
        self.assertEqual(status.lifecycle_status, "canceled")

    def test_dry_run_pending_paper_order_reconciles_via_runtime_reconcile(self):
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

        settings = self._make_settings(
            control_path,
            paper_live_like_enabled=True,
            paper_fill_delay_seconds=0,
            paper_partial_fill_ratio=0.5,
            runtime_reconcile_interval_seconds=60,
        )
        broker = PaperBroker(settings=settings)
        trader = Trader(
            settings=settings,
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy([_signal("BUY")]),
            risk=_DummyRisk(),
            broker=broker,
        )

        with patch("polymarket_bot.runner.time.time", return_value=100), patch(
            "polymarket_bot.brokers.paper.time.time",
            return_value=100,
        ):
            trader.step()

        self.assertEqual(len(trader.pending_orders), 1)
        self.assertNotIn("token-demo", trader.positions_book)

        with patch("polymarket_bot.runner.time.time", return_value=161), patch(
            "polymarket_bot.brokers.paper.time.time",
            return_value=161,
        ):
            trader._maybe_reconcile_runtime()

        self.assertIn("token-demo", trader.positions_book)
        self.assertAlmostEqual(float(trader.positions_book["token-demo"]["notional"]), 25.0, places=4)
        self.assertEqual(len(trader.pending_orders), 1)
        self.assertEqual(str(trader.recent_orders[0]["status"]), "PARTIAL")


if __name__ == "__main__":
    unittest.main()
