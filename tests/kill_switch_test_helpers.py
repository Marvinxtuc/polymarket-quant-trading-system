from __future__ import annotations

import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from polymarket_bot.clients.data_api import AccountingSnapshot, MarketMetadata
from polymarket_bot.config import Settings
from polymarket_bot.types import ExecutionResult, OpenOrderSnapshot, OrderStatusSnapshot, RiskDecision, Signal


def make_settings(*, workdir: Path, dry_run: bool = True) -> Settings:
    runtime_state_path = workdir / "runtime_state_export.json"
    control_export_path = workdir / "control_export.json"
    state_store_path = workdir / "runtime_truth.db"
    ledger_path = workdir / "ledger.jsonl"
    candidate_db_path = workdir / "terminal.db"
    runtime_state_path.write_text("{}", encoding="utf-8")
    control_export_path.write_text("{}", encoding="utf-8")
    ledger_path.write_text("", encoding="utf-8")
    return Settings(
        _env_file=None,
        dry_run=dry_run,
        decision_mode="auto",
        watch_wallets="0x1111111111111111111111111111111111111111",
        control_path=str(control_export_path),
        runtime_state_path=str(runtime_state_path),
        state_store_path=str(state_store_path),
        ledger_path=str(ledger_path),
        candidate_db_path=str(candidate_db_path),
        runtime_reconcile_interval_seconds=120,
        poll_interval_seconds=30,
        kill_switch_terminal_timeout_seconds=120,
        kill_switch_query_error_threshold=2,
        kill_switch_cancel_retry_seconds=5,
        enable_single_writer=False,
        bankroll_usd=5000.0,
    )


def seed_control_state(
    settings: Settings,
    *,
    pause_opening: bool = False,
    reduce_only: bool = False,
    emergency_stop: bool = False,
    updated_ts: int | None = None,
) -> None:
    from polymarket_bot.state_store import StateStore

    StateStore(settings.state_store_path).save_control_state(
        {
            "decision_mode": "auto",
            "pause_opening": bool(pause_opening),
            "reduce_only": bool(reduce_only),
            "emergency_stop": bool(emergency_stop),
            "clear_stale_pending_requested_ts": 0,
            "updated_ts": int(updated_ts or time.time()),
        }
    )


def seed_pending_buy(
    trader,
    *,
    order_id: str,
    token_id: str = "token-kill",
    condition_id: str = "condition-token-kill",
    market_slug: str = "kill-market",
    broker_status: str = "live",
    requested_notional: float = 50.0,
    requested_price: float = 0.6,
    ts_offset_seconds: int = 5,
) -> None:
    now_ts = int(time.time())
    trader.pending_orders[f"pending:{order_id}"] = {
        "key": f"pending:{order_id}",
        "signal_id": f"sig:{order_id}",
        "cycle_id": f"cyc:{now_ts}",
        "trace_id": f"trace:{order_id}",
        "wallet": "0x1111111111111111111111111111111111111111",
        "market_slug": str(market_slug or "kill-market"),
        "token_id": str(token_id or "token-kill"),
        "condition_id": str(condition_id or ""),
        "outcome": "YES",
        "side": "BUY",
        "ts": now_ts - max(0, int(ts_offset_seconds)),
        "order_id": str(order_id or ""),
        "broker_status": str(broker_status or "live"),
        "requested_notional": float(requested_notional),
        "requested_price": float(requested_price),
        "matched_notional_hint": 0.0,
        "matched_size_hint": 0.0,
        "reconciled_notional_hint": 0.0,
        "reconciled_size_hint": 0.0,
    }


class DummyDataClient:
    def __init__(self):
        self.order_book = SimpleNamespace(best_bid=0.59, best_ask=0.61)

    def discover_wallet_activity(self, paths, limit):
        return {}

    def get_active_positions(self, wallet):
        return []

    def get_accounting_snapshot(self, wallet):
        return AccountingSnapshot(
            wallet=str(wallet or ""),
            cash_balance=1000.0,
            positions_value=0.0,
            equity=1000.0,
            valuation_time=str(int(time.time())),
            positions=(),
        )

    def iter_closed_positions(self, wallet, **_kwargs):
        return iter(())

    def get_order_book(self, token_id: str):
        return self.order_book

    def get_midpoint_price(self, token_id: str):
        return 0.6

    def get_price_history(self, token_id: str, **_kwargs):
        return []

    def get_market_metadata(self, condition_id: str = "", *, slug: str | None = None):
        now_ts = int(time.time())
        return MarketMetadata(
            condition_id=str(condition_id or "condition-kill"),
            market_slug=str(slug or "kill-market"),
            end_ts=now_ts + 3600,
            end_date=datetime.fromtimestamp(now_ts + 3600, tz=timezone.utc).isoformat(),
            closed=False,
            active=True,
            accepting_orders=True,
            token_ids=("token-kill",),
        )

    def close(self):
        return None


class DummyStrategy:
    def __init__(self, signals: list[Signal]):
        self._signals = list(signals)

    def generate_signals(self, wallets):
        return list(self._signals)

    def update_wallet_selection_context(self, context):
        _ = context
        return None


class DummyRisk:
    def evaluate(self, signal, state):
        _ = state
        return RiskDecision(allowed=True, reason="ok", max_notional=50.0, snapshot={})


class ScenarioBroker:
    def __init__(self):
        self.execute_calls: list[tuple[Signal, float]] = []
        self.cancel_calls: list[str] = []
        self.open_orders: list[OpenOrderSnapshot] = []
        self.status_by_order_id: dict[str, OrderStatusSnapshot] = {}
        self.cancel_result_by_order_id: dict[str, dict[str, object]] = {}

    def startup_checks(self):
        return []

    def execute(self, signal, notional_usd, *, strategy_order_uuid=None):
        self.execute_calls.append((signal, notional_usd))
        return ExecutionResult(
            ok=True,
            broker_order_id=f"exec-{signal.token_id}",
            message="filled",
            filled_notional=notional_usd,
            filled_price=max(0.01, signal.price_hint),
            status="filled",
            requested_notional=notional_usd,
            requested_price=max(0.01, signal.price_hint),
            metadata={"strategy_order_uuid": strategy_order_uuid} if strategy_order_uuid else {},
        )

    def cancel_order(self, order_id: str):
        normalized = str(order_id or "").strip()
        if normalized:
            self.cancel_calls.append(normalized)
        return dict(
            self.cancel_result_by_order_id.get(
                normalized,
                {
                    "order_id": normalized,
                    "status": "requested",
                    "ok": True,
                    "message": "cancel requested",
                },
            )
        )

    def list_open_orders(self):
        return list(self.open_orders)

    def get_order_status(self, order_id: str):
        return self.status_by_order_id.get(str(order_id or "").strip())

    def list_recent_fills(self, *, since_ts: int = 0, order_ids: list[str] | None = None, limit: int = 200):
        _ = (since_ts, order_ids, limit)
        return []

    def list_order_events(self, *, since_ts: int = 0, order_ids: list[str] | None = None, limit: int = 200):
        _ = (since_ts, order_ids, limit)
        return []

    def heartbeat(self, order_ids: list[str]):
        _ = order_ids
        return True


def build_signal(*, side: str = "BUY", token_id: str = "token-kill", market_slug: str = "kill-market") -> Signal:
    return Signal(
        signal_id=f"sig-{side.lower()}-{token_id}",
        trace_id=f"trace-{token_id}",
        wallet="0x1111111111111111111111111111111111111111",
        market_slug=str(market_slug or "kill-market"),
        token_id=str(token_id or "token-kill"),
        outcome="YES",
        side=side,  # type: ignore[arg-type]
        confidence=0.8,
        price_hint=0.6,
        observed_size=10.0,
        observed_notional=100.0,
        timestamp=datetime.now(tz=timezone.utc),
        condition_id=f"condition-{token_id}",
        wallet_score=80.0,
        wallet_tier="CORE",
    )


def temp_dir(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=prefix))
