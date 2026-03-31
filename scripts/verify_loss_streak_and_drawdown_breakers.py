#!/usr/bin/env python3
from __future__ import annotations

import tempfile
import time
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
TESTS = ROOT / "tests"
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

from polymarket_bot.risk import (  # noqa: E402
    REASON_INTRADAY_DRAWDOWN_BREAKER_ACTIVE,
    REASON_LOSS_STREAK_BREAKER_ACTIVE,
    RiskManager,
)
from polymarket_bot.runner import Trader  # noqa: E402
from runtime_persistence_helpers import (  # noqa: E402
    DummyBroker,
    DummyDataClient,
    DummyRisk,
    DummyStrategy,
    build_signal,
    make_settings,
)


def _new_trader(settings):
    trader = Trader(
        settings=settings,
        data_client=DummyDataClient(),
        strategy=DummyStrategy([]),
        risk=DummyRisk(),
        broker=DummyBroker(),
    )
    trader.risk = RiskManager(settings)
    return trader


def _verify_loss_streak_breaker() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="verify-loss-streak-"))
    settings = make_settings(dry_run=True, workdir=workdir)
    settings.bankroll_usd = 1000.0
    settings.loss_streak_breaker_limit = 2
    trader = _new_trader(settings)

    now = int(time.time())
    trader._apply_realized_pnl(-5.0, ts=now)
    trader._apply_realized_pnl(-1.0, ts=now + 1)
    trader._refresh_risk_state()
    assert trader.state.loss_streak_blocked, "loss streak breaker should latch after 2 consecutive losses"

    signal = build_signal(token_id="token-loss", side="BUY")
    trader._hydrate_signal_condition_exposure(signal)
    decision = trader.risk.evaluate(signal, trader.state)
    assert not decision.allowed
    assert decision.reason == REASON_LOSS_STREAK_BREAKER_ACTIVE


def _verify_intraday_drawdown_breaker() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="verify-drawdown-"))
    settings = make_settings(dry_run=True, workdir=workdir)
    settings.bankroll_usd = 1000.0
    settings.intraday_drawdown_breaker_pct = 0.05
    trader = _new_trader(settings)

    trader.state.equity_usd = 1000.0
    trader._refresh_risk_state()
    trader.state.daily_realized_pnl = -100.0
    trader.state.equity_usd = 900.0
    trader._refresh_risk_state()
    assert trader.state.intraday_drawdown_blocked, "drawdown breaker should block at >= threshold"

    signal = build_signal(token_id="token-dd", side="BUY")
    trader._hydrate_signal_condition_exposure(signal)
    decision = trader.risk.evaluate(signal, trader.state)
    assert not decision.allowed
    assert decision.reason == REASON_INTRADAY_DRAWDOWN_BREAKER_ACTIVE


def _verify_restart_persistence() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="verify-breaker-restart-"))
    settings = make_settings(dry_run=True, workdir=workdir)
    settings.bankroll_usd = 1000.0
    settings.loss_streak_breaker_limit = 2
    trader = _new_trader(settings)
    now = int(time.time())
    trader._apply_realized_pnl(-3.0, ts=now)
    trader._apply_realized_pnl(-2.0, ts=now + 1)
    trader._refresh_risk_state()
    trader.persist_runtime_state(settings.runtime_state_path)
    if trader._writer_lock is not None:
        trader._writer_lock.release()
        trader._writer_lock = None

    restarted = _new_trader(settings)
    restarted._refresh_risk_state()
    assert restarted.state.loss_streak_blocked, "loss streak breaker must survive restart"


def main() -> int:
    _verify_loss_streak_breaker()
    _verify_intraday_drawdown_breaker()
    _verify_restart_persistence()
    print("verify_loss_streak_and_drawdown_breakers: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
