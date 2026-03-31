from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

from polymarket_bot.config import Settings
from polymarket_bot.risk import RiskDecision
from polymarket_bot.runner import Trader
from polymarket_bot.types import ExecutionResult


class _DummyDataClient:
    def __init__(self):
        self.order_book = SimpleNamespace(best_bid=0.48, best_ask=0.52)
        self.midpoint_price = 0.5

    def discover_wallet_activity(self, paths, limit):
        return {}

    def get_active_positions(self, wallet):
        return []

    def get_accounting_snapshot(self, wallet):
        return None

    def iter_closed_positions(self, wallet, **_kwargs):
        return iter(())

    def get_order_book(self, _token_id: str):
        return self.order_book

    def get_midpoint_price(self, _token_id: str):
        return self.midpoint_price

    def get_price_history(self, _token_id: str, **_kwargs):
        return []

    def get_market_metadata(self, condition_id: str = "", *, slug: str | None = None):
        return None

    def close(self):
        return None


class _DummyStrategy:
    def generate_signals(self, wallets):
        return []

    def update_wallet_selection_context(self, context):
        return None


class _DummyRisk:
    def evaluate(self, signal, state):
        return RiskDecision(True, "ok", 50.0)


class _SequenceBroker:
    def __init__(self, outcomes: list[str]):
        self.outcomes = list(outcomes)
        self.calls: list[tuple[object, float]] = []

    def execute(self, signal, notional_usd, *, strategy_order_uuid=None):
        self.calls.append((signal, notional_usd))
        outcome = self.outcomes.pop(0) if self.outcomes else "filled"
        if outcome == "reject":
            return ExecutionResult(
                ok=False,
                broker_order_id=None,
                message="no liquidity",
                filled_notional=0.0,
                filled_price=0.0,
                status="rejected",
                requested_notional=notional_usd,
                requested_price=max(0.01, signal.price_hint),
            )
        return ExecutionResult(
            ok=True,
            broker_order_id=f"exit-{signal.token_id}-{len(self.calls)}",
            message="filled",
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
        return None

    def list_order_events(self, *, since_ts: int = 0, order_ids: list[str] | None = None, limit: int = 200):
        return []

    def heartbeat(self, order_ids: list[str]):
        return True

    def cancel_order(self, order_id: str):
        return {
            "order_id": str(order_id or ""),
            "status": "canceled",
            "ok": True,
            "message": "dummy cancel",
        }


class TimeExitUpgradeTests(unittest.TestCase):
    def _make_settings(self, control_path: str, **kwargs: object) -> Settings:
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
            stale_position_minutes=int(kwargs.get("stale_position_minutes", 5)),
            stale_position_trim_pct=float(kwargs.get("stale_position_trim_pct", 0.4)),
            stale_position_trim_cooldown_seconds=int(kwargs.get("stale_position_trim_cooldown_seconds", 900)),
            stale_position_close_notional_usd=float(kwargs.get("stale_position_close_notional_usd", 10.0)),
            time_exit_retry_limit=int(kwargs.get("time_exit_retry_limit", 2)),
            time_exit_retry_cooldown_seconds=int(kwargs.get("time_exit_retry_cooldown_seconds", 300)),
            time_exit_priority_volatility_step_bps=float(kwargs.get("time_exit_priority_volatility_step_bps", 100.0)),
        )

    @staticmethod
    def _stale_position(token_id: str, *, notional: float = 50.0, quantity: float = 100.0) -> dict[str, object]:
        now_ts = int(time.time())
        return {
            "token_id": token_id,
            "condition_id": f"condition-{token_id}",
            "market_slug": f"market-{token_id}",
            "outcome": "YES",
            "quantity": quantity,
            "price": 0.5,
            "notional": notional,
            "cost_basis_notional": notional,
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
            "trace_id": f"trc-{token_id}",
            "origin_signal_id": f"sig-{token_id}",
            "last_signal_id": f"sig-{token_id}",
            "last_exit_kind": "",
            "last_exit_label": "",
            "last_exit_summary": "",
            "last_exit_ts": 0,
        }

    def test_time_exit_failure_enters_retry_state_with_priority(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump({"pause_opening": False, "reduce_only": False, "emergency_stop": False, "updated_ts": 1}, f)
            control_path = f.name

        broker = _SequenceBroker(["reject"])
        trader = Trader(
            settings=self._make_settings(control_path, time_exit_retry_limit=2, time_exit_retry_cooldown_seconds=300),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy(),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-retry"] = self._stale_position("token-retry")
        trader.state.open_positions = 1

        before = int(time.time())
        trader._apply_time_exit()

        self.assertEqual(len(broker.calls), 1)
        state = trader.positions_book["token-retry"]["time_exit_state"]
        self.assertEqual(state["stage"], "retry")
        self.assertEqual(state["consecutive_failures"], 1)
        self.assertGreater(state["priority"], 0)
        self.assertGreater(state["market_volatility_bps"], 0.0)
        self.assertGreaterEqual(state["next_retry_ts"], before + 299)
        self.assertEqual(trader.recent_orders[0]["time_exit_stage"], "retry")
        self.assertGreater(trader.recent_orders[0]["exit_priority"], 0)

    def test_time_exit_retry_cooldown_blocks_immediate_retry(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump({"pause_opening": False, "reduce_only": False, "emergency_stop": False, "updated_ts": 2}, f)
            control_path = f.name

        broker = _SequenceBroker(["reject", "filled"])
        trader = Trader(
            settings=self._make_settings(control_path, time_exit_retry_limit=3, time_exit_retry_cooldown_seconds=300),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy(),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-cooldown"] = self._stale_position("token-cooldown")
        trader.state.open_positions = 1

        trader._apply_time_exit()
        trader._apply_time_exit()

        self.assertEqual(len(broker.calls), 1)
        self.assertIn("token-cooldown", trader.positions_book)
        self.assertEqual(trader.positions_book["token-cooldown"]["time_exit_state"]["stage"], "retry")

    def test_time_exit_force_exit_mode_closes_full_position_after_failures(self):
        with tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8") as f:
            json.dump({"pause_opening": False, "reduce_only": False, "emergency_stop": False, "updated_ts": 3}, f)
            control_path = f.name

        broker = _SequenceBroker(["reject", "filled"])
        trader = Trader(
            settings=self._make_settings(control_path, time_exit_retry_limit=1, time_exit_retry_cooldown_seconds=0),
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy(),
            risk=_DummyRisk(),
            broker=broker,
        )
        trader.positions_book["token-force"] = self._stale_position("token-force", notional=120.0, quantity=240.0)
        trader.state.open_positions = 1

        trader._apply_time_exit()
        self.assertEqual(trader.positions_book["token-force"]["time_exit_state"]["stage"], "force_exit")
        self.assertAlmostEqual(broker.calls[0][1], 48.0, places=4)

        trader._apply_time_exit()

        self.assertEqual(len(broker.calls), 2)
        self.assertAlmostEqual(broker.calls[1][1], 120.0, places=4)
        self.assertNotIn("token-force", trader.positions_book)
        self.assertTrue(bool(trader.recent_orders[0]["force_exit_active"]))
