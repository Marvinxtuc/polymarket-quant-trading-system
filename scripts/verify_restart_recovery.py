#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
import tempfile
import time
from datetime import datetime, timezone
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
from polymarket_bot.types import Signal  # noqa: E402


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

    def iter_closed_positions(self, wallet: str, **_kwargs):
        return iter(())


class _Strategy:
    def __init__(self, signals=None):
        self._signals = list(signals or [])

    def generate_signals(self, wallets):
        return list(self._signals)


class _Risk:
    def evaluate(self, signal, state):
        return RiskDecision(allowed=True, reason="ok", max_notional=10.0, snapshot={})


class _Broker:
    def __init__(self):
        self.calls = []

    def execute(self, signal, notional_usd, *, strategy_order_uuid=None):
        self.calls.append((signal, notional_usd))
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

    def list_open_orders(self):
        return []

    def list_recent_fills(self, *, since_ts=0, order_ids=None, limit=200):
        return []

    def heartbeat(self, order_ids):
        _ = order_ids
        return True

    def get_order_status(self, order_id):
        _ = order_id
        return None

    def cancel_order(self, order_id):
        _ = order_id
        return {"status": "requested", "ok": True, "message": "cancel requested"}

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
    smoke_path = tmpdir / "network_smoke.jsonl"
    smoke_path.write_text(
        json.dumps(
            {
                "ts": int(time.time()),
                "summary": {"exit_code": 0, "warnings": 0, "blocks": 0, "failures": 0},
            },
            ensure_ascii=False,
        )
        + "\n",
        encoding="utf-8",
    )
    return Settings(
        _env_file=None,
        dry_run=False,
        decision_mode="manual",
        watch_wallets="0x1111111111111111111111111111111111111111",
        funder_address="0xabc0000000000000000000000000000000000000",
        live_allowance_ready=True,
        live_geoblock_ready=True,
        live_account_ready=True,
        network_smoke_log_path=str(smoke_path),
        runtime_state_path=str(runtime_state_path),
        control_path=str(control_path),
        state_store_path=str(state_store_path),
        ledger_path=str(ledger_path),
        candidate_db_path=str(candidate_db_path),
    )


def main() -> int:
    tmpdir = Path(tempfile.mkdtemp(prefix="verify-restart-recovery-"))
    settings = _make_settings(tmpdir)
    store = StateStore(settings.state_store_path)
    store.save_runtime_truth(
        {
            "runtime": {"ts": 1000, "runtime_version": 8, "broker_event_sync_ts": 1000},
            "control": {
                "decision_mode": "manual",
                "pause_opening": False,
                "reduce_only": False,
                "emergency_stop": False,
                "clear_stale_pending_requested_ts": 0,
                "updated_ts": 1000,
            },
            "risk": {"day_key": "", "daily_realized_pnl": 0.0, "broker_closed_pnl_today": 0.0},
            "reconciliation": {"status": "ok", "issues": []},
            "positions": [],
            "order_intents": [
                {
                    "intent_id": "signal-conflict",
                    "strategy_order_uuid": "so-conflict",
                    "broker_order_id": "order-conflict",
                    "token_id": "token-conflict",
                    "condition_id": "condition-conflict",
                    "side": "BUY",
                    "status": "posted",
                    "recovered_source": "db",
                    "recovery_reason": "persisted",
                    "created_ts": 1000,
                    "updated_ts": 1000,
                    "payload": {
                        "key": "pending-conflict",
                        "ts": 1000,
                        "cycle_id": "cycle-conflict",
                        "order_id": "order-conflict",
                        "broker_status": "posted",
                        "signal_id": "signal-conflict",
                        "trace_id": "trace-conflict",
                        "token_id": "token-conflict",
                        "condition_id": "condition-conflict",
                        "market_slug": "conflict-market",
                        "outcome": "YES",
                        "side": "BUY",
                        "wallet": "0x1111111111111111111111111111111111111111",
                        "wallet_score": 80.0,
                        "wallet_tier": "CORE",
                        "requested_notional": 20.0,
                        "requested_price": 0.5,
                    },
                }
            ],
        }
    )

    broker = _Broker()
    signal = Signal(
        signal_id="sig-conflict",
        trace_id="trace-conflict",
        wallet="0x1111111111111111111111111111111111111111",
        market_slug="conflict-market",
        token_id="token-conflict",
        outcome="YES",
        side="BUY",
        confidence=0.8,
        price_hint=0.5,
        observed_size=10.0,
        observed_notional=100.0,
        timestamp=datetime.now(tz=timezone.utc),
        condition_id="condition-conflict",
        wallet_score=80.0,
        wallet_tier="CORE",
    )
    trader = Trader(
        settings=settings,
        data_client=_DataClient(),
        strategy=_Strategy([signal]),
        risk=_Risk(),
        broker=broker,
    )
    mode = trader.trading_mode_state()
    if trader.startup_ready:
        print("FAIL: startup_ready should be false when recovery conflict exists")
        return 1
    if str(mode.get("mode") or "") != "REDUCE_ONLY":
        print("FAIL: trading mode should be REDUCE_ONLY under recovery conflict")
        return 1
    if bool(mode.get("opening_allowed")):
        print("FAIL: opening_allowed should be false under recovery conflict")
        return 1
    if "recovery_conflict" not in list(mode.get("reason_codes") or []):
        print("FAIL: recovery_conflict reason not present")
        return 1
    trader.step()
    if len(broker.calls) != 0:
        print("FAIL: BUY path should not execute before recovery resolves")
        return 1
    print("PASS: recovery conflict keeps startup not-ready, latches protection, and blocks BUY")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
