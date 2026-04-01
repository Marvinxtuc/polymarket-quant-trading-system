#!/usr/bin/env python3
from __future__ import annotations

import tempfile
from pathlib import Path
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
TESTS = ROOT / "tests"
if str(TESTS) not in sys.path:
    sys.path.insert(0, str(TESTS))

from polymarket_bot.risk import (  # noqa: E402
    REASON_CONDITION_EXPOSURE_CAP_REACHED,
    REASON_PORTFOLIO_EXPOSURE_CAP_REACHED,
    REASON_WALLET_EXPOSURE_CAP_REACHED,
    RiskManager,
    RiskState,
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


def _verify_primary_reason_priority() -> None:
    settings = make_settings(dry_run=True, workdir=Path(tempfile.mkdtemp(prefix="verify-cap-priority-")))
    settings.bankroll_usd = 100.0
    manager = RiskManager(settings)
    signal = build_signal(token_id="token-priority", side="BUY")
    state = RiskState(
        trading_mode="NORMAL",
        wallet_exposure_cap_usd=10.0,
        wallet_exposure_committed_usd=10.0,
        portfolio_exposure_cap_usd=9.0,
        portfolio_exposure_committed_usd=9.0,
        condition_exposure_key="condition:token-priority",
        condition_exposure_cap_usd=8.0,
        condition_exposure_committed_usd=8.0,
    )
    decision = manager.evaluate(signal, state)
    assert not decision.allowed, "expected cap rejection"
    assert decision.reason == REASON_WALLET_EXPOSURE_CAP_REACHED, decision.reason
    assert list(decision.snapshot.get("reason_codes") or []) == [
        REASON_WALLET_EXPOSURE_CAP_REACHED,
        REASON_PORTFOLIO_EXPOSURE_CAP_REACHED,
        REASON_CONDITION_EXPOSURE_CAP_REACHED,
    ]


def _verify_ledger_gate_cannot_be_bypassed() -> None:
    workdir = Path(tempfile.mkdtemp(prefix="verify-cap-bypass-"))
    settings = make_settings(dry_run=True, workdir=workdir)
    settings.bankroll_usd = 100.0
    settings.max_wallet_exposure_pct = 0.10
    settings.max_portfolio_exposure_pct = 1.0
    settings.max_condition_exposure_pct = 1.0

    signal = build_signal(token_id="token-new", side="BUY")
    broker = DummyBroker()
    trader = Trader(
        settings=settings,
        data_client=DummyDataClient(),
        strategy=DummyStrategy([signal]),
        risk=DummyRisk(),
        broker=broker,
    )
    trader.risk = RiskManager(settings)
    trader.positions_book = {
        "token-existing": {
            "token_id": "token-existing",
            "condition_id": "condition-existing",
            "market_slug": "existing-market",
            "outcome": "YES",
            "quantity": 100.0,
            "price": 0.20,
            "notional": 20.0,
            "cost_basis_notional": 20.0,
            "opened_ts": 1,
            "last_buy_ts": 1,
            "last_trim_ts": 0,
        }
    }
    with (
        patch.object(Trader, "_enforce_condition_netting", lambda _self, sig, n: (n, {"bypass": True})),
        patch.object(Trader, "_enforce_buy_budget", lambda _self, sig, n: n),
    ):
        netting_notional, _ = trader._enforce_condition_netting(signal, 50.0)
        local_allowed_notional = trader._enforce_buy_budget(signal, netting_notional)
    assert local_allowed_notional > 0.0
    trader._refresh_risk_state()
    trader._hydrate_signal_condition_exposure(signal)
    decision = trader.risk.evaluate(signal, trader.state)
    assert not decision.allowed
    assert decision.reason == REASON_WALLET_EXPOSURE_CAP_REACHED
    assert len([row for row in broker.calls if str(row[0].side).upper() == "BUY"]) == 0


def main() -> int:
    _verify_primary_reason_priority()
    _verify_ledger_gate_cannot_be_bypassed()
    print("verify_exposure_caps: ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
