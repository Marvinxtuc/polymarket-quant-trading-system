from __future__ import annotations

import json
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from polymarket_bot.clients.data_api import AccountingSnapshot
from polymarket_bot.config import Settings
from polymarket_bot.risk import RiskDecision
from polymarket_bot.types import OpenOrderSnapshot, OrderFillSnapshot, Signal


def make_settings(*, dry_run: bool, workdir: Path, funder_address: str = "") -> Settings:
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
        decision_mode="manual",
        watch_wallets="0x1111111111111111111111111111111111111111",
        control_path=str(control_export_path),
        runtime_state_path=str(runtime_state_path),
        state_store_path=str(state_store_path),
        ledger_path=str(ledger_path),
        candidate_db_path=str(candidate_db_path),
        bankroll_usd=5000.0,
        funder_address=funder_address,
    )


class DummyDataClient:
    def __init__(self, *, positions: list[dict[str, object]] | None = None):
        self._positions = list(positions or [])

    def get_accounting_snapshot(self, wallet: str) -> AccountingSnapshot:
        total_positions_value = float(sum(float(item.get("notional") or 0.0) for item in self._positions))
        return AccountingSnapshot(
            wallet=wallet,
            cash_balance=1000.0,
            positions_value=total_positions_value,
            equity=1000.0 + total_positions_value,
            valuation_time=str(int(time.time())),
            positions=(),
        )

    def get_active_positions(self, wallet: str):
        @dataclass(slots=True)
        class _Pos:
            token_id: str
            market_slug: str
            outcome: str
            avg_price: float
            size: float
            notional: float
            timestamp: int
            condition_id: str = ""

        rows: list[_Pos] = []
        for item in self._positions:
            rows.append(
                _Pos(
                    token_id=str(item.get("token_id") or ""),
                    market_slug=str(item.get("market_slug") or "demo-market"),
                    outcome=str(item.get("outcome") or "YES"),
                    avg_price=float(item.get("price") or 0.5),
                    size=float(item.get("quantity") or 0.0),
                    notional=float(item.get("notional") or 0.0),
                    timestamp=int(item.get("opened_ts") or time.time()),
                    condition_id=str(item.get("condition_id") or ""),
                )
            )
        return rows

    def iter_closed_positions(self, wallet: str, **_kwargs):
        return iter(())


class DummyStrategy:
    def __init__(self, signals: list[Signal] | None = None):
        self._signals = list(signals or [])

    def generate_signals(self, wallets: list[str]) -> list[Signal]:
        return list(self._signals)


class DummyRisk:
    def evaluate(self, signal: Signal, state) -> RiskDecision:
        return RiskDecision(allowed=True, reason="ok", max_notional=50.0, snapshot={})


class DummyBroker:
    def __init__(
        self,
        *,
        open_orders: list[OpenOrderSnapshot] | None = None,
        fills: list[OrderFillSnapshot] | None = None,
    ):
        self._open_orders = list(open_orders or [])
        self._fills = list(fills or [])
        self.calls: list[tuple[Signal, float]] = []

    def execute(self, signal: Signal, notional_usd: float, *, strategy_order_uuid: str | None = None):
        self.calls.append((signal, notional_usd))
        from polymarket_bot.types import ExecutionResult

        return ExecutionResult(
            ok=True,
            broker_order_id=f"dummy-{signal.token_id}",
            message="filled",
            filled_notional=notional_usd,
            filled_price=max(0.01, signal.price_hint),
            status="filled",
            requested_notional=notional_usd,
            requested_price=max(0.01, signal.price_hint),
            metadata={"strategy_order_uuid": strategy_order_uuid} if strategy_order_uuid else {},
        )

    def list_open_orders(self):
        return list(self._open_orders)

    def list_recent_fills(self, *, since_ts: int = 0, order_ids: list[str] | None = None, limit: int = 200):
        return list(self._fills)

    def heartbeat(self, order_ids: list[str]):
        _ = order_ids
        return True

    def get_order_status(self, order_id: str):
        _ = order_id
        return None

    def cancel_order(self, order_id: str):
        _ = order_id
        return {"status": "requested", "ok": True, "message": "cancel requested"}

    def startup_checks(self):
        return []


def build_signal(*, token_id: str = "token-demo", side: str = "BUY") -> Signal:
    return Signal(
        signal_id=f"signal-{token_id}-{side.lower()}",
        trace_id=f"trace-{token_id}",
        wallet="0x1111111111111111111111111111111111111111",
        market_slug="demo-market",
        token_id=token_id,
        outcome="YES",
        side=side,  # type: ignore[arg-type]
        confidence=0.8,
        price_hint=0.55,
        observed_size=10.0,
        observed_notional=100.0,
        timestamp=datetime.now(tz=timezone.utc),
        condition_id=f"condition-{token_id}",
        wallet_score=80.0,
        wallet_tier="CORE",
    )


def seed_control_export(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


def new_tmp_dir() -> Path:
    return Path(tempfile.mkdtemp(prefix="runtime-persistence-"))
