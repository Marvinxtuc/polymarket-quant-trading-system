#!/usr/bin/env python3
from __future__ import annotations

import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.admission_gate import AdmissionEvidence, evaluate_admission  # noqa: E402
from polymarket_bot.config import Settings  # noqa: E402
from polymarket_bot.runner import Trader  # noqa: E402
from polymarket_bot.types import RiskDecision, Signal  # noqa: E402


class _Broker:
    def __init__(self) -> None:
        self.execute_calls: list[tuple[Signal, float]] = []

    def startup_checks(self):
        return [{"name": "startup_gate", "status": "PASS", "message": "ok"}]

    def execute(self, signal, notional_usd, *, strategy_order_uuid=None):
        self.execute_calls.append((signal, notional_usd))
        raise RuntimeError("execute should never be called when admission blocks openings")

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
            condition_id=str(condition_id or "condition-untrusted"),
            market_slug=str(slug or "untrusted-market"),
            end_ts=now_ts + 3600,
            end_date=datetime.fromtimestamp(now_ts + 3600, tz=timezone.utc).isoformat(),
            closed=False,
            active=True,
            accepting_orders=True,
            token_ids=("token-untrusted",),
        )


class _Strategy:
    def __init__(self) -> None:
        self._signal = Signal(
            signal_id="sig-untrusted",
            trace_id="trc-untrusted",
            wallet="0x1111111111111111111111111111111111111111",
            market_slug="untrusted-market",
            token_id="token-untrusted",
            outcome="YES",
            side="BUY",
            confidence=0.8,
            price_hint=0.6,
            observed_size=100.0,
            observed_notional=100.0,
            timestamp=datetime.now(tz=timezone.utc),
            condition_id="condition-untrusted",
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


def _assert_reason(mode: dict[str, object], expected: str) -> None:
    reasons = set(mode.get("reason_codes") or [])
    if expected not in reasons:
        raise AssertionError(f"expected reason {expected}, got {sorted(reasons)}")


def _verify_reconciliation_fail_blocks_buy() -> None:
    tmpdir = Path(tempfile.mkdtemp(prefix="verify-untrusted-reconcile-"))
    trader = Trader(
        settings=_settings(tmpdir),
        data_client=_DataClient(),
        strategy=_Strategy(),
        risk=_Risk(),
        broker=_Broker(),
    )
    trader.startup_ready = True
    trader.startup_failure_count = 0
    trader.startup_checks = []
    trader.state.account_snapshot_ts = int(datetime.now(tz=timezone.utc).timestamp())

    def _forced_reconciliation_summary(self, *, now=None):
        return {
            "day_key": "1970-01-01",
            "status": "fail",
            "issues": ["forced_reconciliation_fail"],
            "recovery_conflicts": [],
            "startup_ready": True,
            "internal_realized_pnl": 0.0,
            "ledger_realized_pnl": 0.0,
            "broker_closed_pnl_today": 0.0,
            "effective_daily_realized_pnl": 0.0,
            "internal_vs_ledger_diff": 0.0,
            "broker_floor_gap_vs_internal": 0.0,
            "fill_count_today": 0,
            "fill_notional_today": 0.0,
            "account_sync_count_today": 0,
            "startup_checks_count_today": 0,
            "last_fill_ts": 0,
            "last_account_sync_ts": 0,
            "last_startup_checks_ts": 0,
            "pending_orders": 0,
            "pending_entry_orders": 0,
            "pending_exit_orders": 0,
            "stale_pending_orders": 0,
            "ambiguous_pending_orders": 0,
            "open_positions": 0,
            "tracked_notional_usd": 0.0,
            "ledger_available": True,
            "account_snapshot_age_seconds": 0,
            "broker_reconcile_age_seconds": 0,
            "broker_event_sync_age_seconds": 0,
        }

    with patch.object(Trader, "reconciliation_summary", _forced_reconciliation_summary):
        trader.step()

    mode = trader.trading_mode_state()
    if mode.get("opening_allowed", True):
        raise AssertionError(f"reconciliation_fail should block opening: {mode}")
    _assert_reason(mode, "reconciliation_fail")
    if trader.broker.execute_calls:
        raise AssertionError("BUY execution should be blocked when reconciliation fails")


def _verify_evidence_cases_fail_closed() -> None:
    # stale account snapshot
    stale_snapshot = evaluate_admission(
        now_ts=1700000000,
        evidence=AdmissionEvidence(
            startup_ready=True,
            startup_failure_count=0,
            reconciliation_status="ok",
            account_snapshot_age_seconds=9999,
            account_snapshot_stale_threshold_seconds=600,
            broker_event_sync_age_seconds=1,
            broker_event_stale_threshold_seconds=120,
            ledger_diff=0.0,
            ledger_diff_threshold_usd=0.01,
            ambiguous_pending_orders=0,
            recovery_conflict_count=0,
            recovery_conflict_requires_manual=False,
            persistence_status="ok",
            risk_ledger_status="ok",
            risk_breaker_status="ok",
            operator_pause_opening=False,
            operator_reduce_only=False,
            operator_emergency_stop=False,
            dry_run=False,
        ),
        previous_auto_latch_active=False,
        previous_trusted_consecutive_cycles=0,
        auto_recover_min_healthy_cycles=1,
    )
    if stale_snapshot.opening_allowed or "stale_account_snapshot" not in set(stale_snapshot.reason_codes):
        raise AssertionError(f"stale snapshot should fail-close: {stale_snapshot}")

    # ledger diff exceeded
    ledger_diff = evaluate_admission(
        now_ts=1700000000,
        evidence=AdmissionEvidence(
            startup_ready=True,
            startup_failure_count=0,
            reconciliation_status="ok",
            account_snapshot_age_seconds=1,
            account_snapshot_stale_threshold_seconds=600,
            broker_event_sync_age_seconds=1,
            broker_event_stale_threshold_seconds=120,
            ledger_diff=1.0,
            ledger_diff_threshold_usd=0.01,
            ambiguous_pending_orders=0,
            recovery_conflict_count=0,
            recovery_conflict_requires_manual=False,
            persistence_status="ok",
            risk_ledger_status="ok",
            risk_breaker_status="ok",
            operator_pause_opening=False,
            operator_reduce_only=False,
            operator_emergency_stop=False,
            dry_run=False,
        ),
        previous_auto_latch_active=False,
        previous_trusted_consecutive_cycles=0,
        auto_recover_min_healthy_cycles=1,
    )
    if ledger_diff.opening_allowed or "ledger_diff_exceeded" not in set(ledger_diff.reason_codes):
        raise AssertionError(f"ledger diff should fail-close: {ledger_diff}")


def main() -> int:
    try:
        _verify_reconciliation_fail_blocks_buy()
        _verify_evidence_cases_fail_closed()
    except AssertionError as exc:
        print(f"FAIL: {exc}")
        return 1
    print("PASS: untrusted states (reconciliation/stale snapshot/ledger diff) block openings")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
