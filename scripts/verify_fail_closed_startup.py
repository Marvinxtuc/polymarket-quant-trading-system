#!/usr/bin/env python3
from __future__ import annotations

import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.config import Settings  # noqa: E402
from polymarket_bot.runner import Trader  # noqa: E402
from polymarket_bot.types import RiskDecision, Signal  # noqa: E402


class _FailingBroker:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[Signal, float]] = []

    def startup_checks(self):
        return [{"name": "startup_gate", "status": "FAIL", "message": "intentional startup fail"}]

    def execute(self, signal, notional_usd, *, strategy_order_uuid=None):
        self.execute_calls.append((signal, notional_usd))
        raise RuntimeError("execute should never be called when startup gate fails")

    def list_open_orders(self):
        return []

    def list_recent_fills(self, *, since_ts: int = 0, order_ids=None, limit: int = 200):
        return []

    def cancel_order(self, order_id: str):
        return {"order_id": str(order_id or ""), "status": "canceled", "ok": True, "message": "simulated"}


class _DataClient:
    def __init__(self) -> None:
        self.order_book = SimpleNamespace(best_bid=0.59, best_ask=0.61)

    def discover_wallet_activity(self, paths, limit):
        return {}

    def get_active_positions(self, wallet):
        return []

    def get_accounting_snapshot(self, wallet):
        return None

    def get_order_book(self, token_id: str):
        return self.order_book

    def get_midpoint_price(self, token_id: str):
        return 0.6

    def get_price_history(self, token_id: str, **_kwargs):
        return []

    def get_market_metadata(self, condition_id: str = "", *, slug: str | None = None):
        from polymarket_bot.clients.data_api import MarketMetadata

        now_ts = int(time.time())
        return MarketMetadata(
            condition_id=str(condition_id or "condition-startup"),
            market_slug=str(slug or "startup-market"),
            end_ts=now_ts + 3600,
            end_date=datetime.fromtimestamp(now_ts + 3600, tz=timezone.utc).isoformat(),
            closed=False,
            active=True,
            accepting_orders=True,
            token_ids=("token-startup",),
        )


class _Strategy:
    def __init__(self) -> None:
        self._signal = Signal(
            signal_id="sig-startup",
            trace_id="trc-startup",
            wallet="0x1111111111111111111111111111111111111111",
            market_slug="startup-market",
            token_id="token-startup",
            outcome="YES",
            side="BUY",
            confidence=0.8,
            price_hint=0.6,
            observed_size=100.0,
            observed_notional=100.0,
            timestamp=datetime.now(tz=timezone.utc),
            condition_id="condition-startup",
            wallet_score=80.0,
            wallet_tier="CORE",
        )

    def generate_signals(self, wallets):
        return [self._signal]

    def update_wallet_selection_context(self, context):
        return None


class _Risk:
    def evaluate(self, signal, state):
        return RiskDecision(True, "ok", 50.0)


def _settings(tmpdir: Path) -> Settings:
    return Settings(
        _env_file=None,
        dry_run=True,
        decision_mode="auto",
        watch_wallets="0x1111111111111111111111111111111111111111",
        runtime_root_path=str(tmpdir),
        state_store_path=str(tmpdir / "state.db"),
        control_path=str(tmpdir / "control.json"),
        runtime_state_path=str(tmpdir / "state.json"),
        ledger_path=str(tmpdir / "ledger.jsonl"),
        candidate_db_path=str(tmpdir / "candidates.db"),
    )


def main() -> int:
    tmpdir = Path(tempfile.mkdtemp(prefix="verify-fail-closed-startup-"))
    settings = _settings(tmpdir)

    broker = _FailingBroker()
    trader = Trader(
        settings=settings,
        data_client=_DataClient(),
        strategy=_Strategy(),
        risk=_Risk(),
        broker=broker,
    )

    trader.step()

    mode = trader.trading_mode_state()
    reasons = set(mode.get("reason_codes") or [])
    if broker.execute_calls:
        print("FAIL: BUY execution happened despite startup fail")
        return 1
    if mode.get("opening_allowed", True):
        print(f"FAIL: opening_allowed should be false, got: {mode}")
        return 2
    if "startup_not_ready" not in reasons:
        print(f"FAIL: expected startup_not_ready in reason_codes, got: {sorted(reasons)}")
        return 3
    if str(mode.get("mode") or "").upper() not in {"REDUCE_ONLY", "HALTED"}:
        print(f"FAIL: unexpected mode for startup fail-close: {mode}")
        return 4

    latest_cycle = trader.recent_signal_cycles[0] if trader.recent_signal_cycles else {}
    candidates = list(latest_cycle.get("candidates") or [])
    if not candidates:
        print("FAIL: expected candidate record to prove BUY was rejected")
        return 5
    decision_snapshot = dict(candidates[0].get("decision_snapshot") or {})
    if str(decision_snapshot.get("skip_reason") or "") != "startup_not_ready":
        print(f"FAIL: unexpected skip_reason: {decision_snapshot}")
        return 6

    print("PASS: startup fail-close blocks BUY and keeps mode protected")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
