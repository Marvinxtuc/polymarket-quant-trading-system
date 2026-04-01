#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.clients.data_api import AccountingSnapshot  # noqa: E402
from polymarket_bot.config import Settings  # noqa: E402
from polymarket_bot.risk import RiskDecision  # noqa: E402
from polymarket_bot.runner import Trader  # noqa: E402
from polymarket_bot.state_store import StateStore  # noqa: E402


class _DataClient:
    def get_accounting_snapshot(self, wallet: str) -> AccountingSnapshot:
        return AccountingSnapshot(
            wallet=wallet,
            cash_balance=1000.0,
            positions_value=0.0,
            equity=1000.0,
            valuation_time="0",
            positions=(),
        )

    def get_active_positions(self, wallet: str):
        return []


class _Strategy:
    def generate_signals(self, wallets):
        return []


class _Risk:
    def evaluate(self, signal, state):
        return RiskDecision(allowed=True, reason="ok", max_notional=10.0, snapshot={})


class _Broker:
    def execute(self, signal, notional_usd, *, strategy_order_uuid=None):
        from polymarket_bot.types import ExecutionResult

        return ExecutionResult(
            ok=True,
            broker_order_id="dummy",
            message="filled",
            filled_notional=notional_usd,
            filled_price=max(0.01, signal.price_hint),
            status="filled",
            requested_notional=notional_usd,
            requested_price=max(0.01, signal.price_hint),
        )

    def startup_checks(self):
        return []


def _make_settings(tmpdir: Path) -> Settings:
    runtime_state_path = tmpdir / "runtime_state_export.json"
    control_path = tmpdir / "control_export.json"
    state_store_path = tmpdir / "runtime_truth.db"
    ledger_path = tmpdir / "ledger.jsonl"
    candidate_db_path = tmpdir / "terminal.db"
    runtime_state_path.write_text("{}", encoding="utf-8")
    control_path.write_text("{}", encoding="utf-8")
    ledger_path.write_text("", encoding="utf-8")
    return Settings(
        _env_file=None,
        dry_run=True,
        decision_mode="manual",
        watch_wallets="0x1111111111111111111111111111111111111111",
        runtime_state_path=str(runtime_state_path),
        control_path=str(control_path),
        state_store_path=str(state_store_path),
        ledger_path=str(ledger_path),
        candidate_db_path=str(candidate_db_path),
    )


def main() -> int:
    tmpdir = Path(tempfile.mkdtemp(prefix="verify-runtime-persistence-"))
    settings = _make_settings(tmpdir)
    store = StateStore(settings.state_store_path)
    store.save_control_state(
        {
            "decision_mode": "manual",
            "pause_opening": False,
            "reduce_only": False,
            "emergency_stop": False,
            "clear_stale_pending_requested_ts": 0,
            "updated_ts": 1,
        }
    )
    trader = Trader(
        settings=settings,
        data_client=_DataClient(),
        strategy=_Strategy(),
        risk=_Risk(),
        broker=_Broker(),
    )
    trader.positions_book = {
        "token-persist": {
            "token_id": "token-persist",
            "condition_id": "condition-persist",
            "market_slug": "persist-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.5,
            "notional": 50.0,
            "cost_basis_notional": 50.0,
            "opened_ts": 1,
            "last_buy_ts": 1,
            "last_trim_ts": 0,
        }
    }
    trader.pending_orders = {
        "pending-persist": {
            "key": "pending-persist",
            "ts": 10,
            "cycle_id": "cycle-1",
            "order_id": "order-persist",
            "broker_status": "posted",
            "signal_id": "signal-persist",
            "trace_id": "trace-persist",
            "token_id": "token-persist",
            "condition_id": "condition-persist",
            "market_slug": "persist-market",
            "outcome": "YES",
            "side": "BUY",
            "wallet": "0x1111111111111111111111111111111111111111",
            "wallet_score": 80.0,
            "wallet_tier": "CORE",
            "requested_notional": 20.0,
            "requested_price": 0.5,
            "strategy_order_uuid": "so-persist",
        }
    }
    trader._refresh_risk_state()
    trader.persist_runtime_state(settings.runtime_state_path)
    if getattr(trader, "_writer_lock", None) is not None:
        trader._writer_lock.release()
        trader._writer_lock = None

    if os.path.exists(settings.runtime_state_path):
        os.remove(settings.runtime_state_path)
    if os.path.exists(settings.control_path):
        os.remove(settings.control_path)

    restarted = Trader(
        settings=settings,
        data_client=_DataClient(),
        strategy=_Strategy(),
        risk=_Risk(),
        broker=_Broker(),
    )
    if "token-persist" not in restarted.positions_book:
        print("FAIL: position not recovered from db")
        return 1
    if len(restarted.pending_orders) != 1:
        print("FAIL: pending order not recovered from db order_intents")
        return 1
    if "recovery_conflict" in restarted.trading_mode_reasons:
        print("FAIL: unexpected recovery conflict in success path")
        return 1
    if getattr(restarted, "_writer_lock", None) is not None:
        restarted._writer_lock.release()
        restarted._writer_lock = None

    Path(settings.runtime_state_path).write_text(
        json.dumps(
            {
                "positions": [
                    {
                        "token_id": "token-dirty",
                        "quantity": 999.0,
                        "price": 0.9,
                        "notional": 899.1,
                    }
                ],
                "pending_orders": [
                    {
                        "key": "dirty-pending",
                        "signal_id": "dirty-signal",
                        "order_id": "dirty-order",
                        "token_id": "token-dirty",
                        "side": "BUY",
                        "broker_status": "live",
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    Path(settings.control_path).write_text(
        json.dumps(
            {
                "decision_mode": "manual",
                "pause_opening": True,
                "reduce_only": True,
                "emergency_stop": False,
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    restarted_dirty = Trader(
        settings=settings,
        data_client=_DataClient(),
        strategy=_Strategy(),
        risk=_Risk(),
        broker=_Broker(),
    )
    if "token-persist" not in restarted_dirty.positions_book:
        print("FAIL: db truth not restored when dirty /tmp exports exist")
        return 1
    if "token-dirty" in restarted_dirty.positions_book:
        print("FAIL: dirty /tmp runtime_state influenced position recovery")
        return 1
    if len(restarted_dirty.pending_orders) != 1:
        print("FAIL: pending restoration changed under dirty /tmp exports")
        return 1
    pending = next(iter(restarted_dirty.pending_orders.values()))
    if str(pending.get("order_id") or "") != "order-persist":
        print("FAIL: dirty /tmp pending order leaked into runtime recovery")
        return 1
    if bool(restarted_dirty.control_state.pause_opening) or bool(restarted_dirty.control_state.reduce_only):
        print("FAIL: dirty /tmp control export overrode DB control truth")
        return 1
    print("PASS: runtime persistence, tmp deletion, and dirty tmp isolation verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
