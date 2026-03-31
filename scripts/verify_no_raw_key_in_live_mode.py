#!/usr/bin/env python3
from __future__ import annotations

import importlib.util
import json
import tempfile
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.config import Settings
from polymarket_bot.main import build_trader
from polymarket_bot.runner import Trader
from polymarket_bot.web import _api_state_payload


def _load_live_smoke_preflight_module():
    module_path = ROOT / "scripts" / "live_smoke_preflight.py"
    spec = importlib.util.spec_from_file_location("live_smoke_preflight", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@dataclass(slots=True)
class _AccountingSnapshot:
    wallet: str
    cash_balance: float
    positions_value: float
    equity: float
    valuation_time: str
    positions: tuple = ()


class _DummyDataClient:
    def get_accounting_snapshot(self, wallet: str) -> _AccountingSnapshot:
        return _AccountingSnapshot(
            wallet=wallet,
            cash_balance=1000.0,
            positions_value=0.0,
            equity=1000.0,
            valuation_time=str(int(time.time())),
        )

    def get_active_positions(self, wallet: str):  # noqa: ARG002
        return []

    def iter_closed_positions(self, wallet: str, **_kwargs: object) -> Iterator[dict[str, object]]:  # noqa: ARG002
        return iter(())


class _DummyStrategy:
    def generate_signals(self, wallets: list[str]):  # noqa: ARG002
        return []


class _DummyRisk:
    def evaluate(self, signal, state):  # noqa: ANN001,ANN201,ARG002
        from polymarket_bot.risk import RiskDecision

        return RiskDecision(allowed=True, reason="ok", max_notional=10.0, snapshot={})


class _DummyBroker:
    def execute(self, signal, notional_usd: float, *, strategy_order_uuid: str | None = None):  # noqa: ANN001
        from polymarket_bot.types import ExecutionResult

        return ExecutionResult(
            ok=True,
            broker_order_id="dummy-order",
            message="filled",
            filled_notional=notional_usd,
            filled_price=max(0.01, float(getattr(signal, "price_hint", 0.5) or 0.5)),
            status="filled",
            requested_notional=notional_usd,
            requested_price=max(0.01, float(getattr(signal, "price_hint", 0.5) or 0.5)),
            metadata={"strategy_order_uuid": strategy_order_uuid} if strategy_order_uuid else {},
        )

    def startup_checks(self):
        return []

    def list_open_orders(self):
        return []

    def list_recent_fills(self, *, since_ts: int = 0, order_ids: list[str] | None = None, limit: int = 200):  # noqa: ARG002
        return []

    def heartbeat(self, order_ids: list[str]):  # noqa: ARG002
        return True

    def get_order_status(self, order_id: str):  # noqa: ARG002
        return None

    def cancel_order(self, order_id: str):  # noqa: ARG002
        return {"status": "requested", "ok": True, "message": "cancel requested"}


def _assert_no_sentinel(text: str, sentinel: str, *, where: str) -> None:
    if sentinel in text:
        raise AssertionError(f"raw private key leaked in {where}")


def _build_live_settings(workdir: Path, *, sentinel: str) -> Settings:
    runtime_state_path = workdir / "runtime_state_export.json"
    control_path = workdir / "control_export.json"
    ledger_path = workdir / "ledger.jsonl"
    runtime_state_path.write_text("{}", encoding="utf-8")
    control_path.write_text("{}", encoding="utf-8")
    ledger_path.write_text("", encoding="utf-8")
    return Settings(
        _env_file=None,
        dry_run=False,
        runtime_root_path=str(workdir),
        runtime_state_path=str(runtime_state_path),
        control_path=str(control_path),
        ledger_path=str(ledger_path),
        state_store_path=str(workdir / "state.db"),
        candidate_db_path=str(workdir / "terminal.db"),
        funder_address="0xabc123",
        private_key=sentinel,
        signer_url="https://signer.internal.local",
        clob_api_key="api-key",
        clob_api_secret="api-secret",
        clob_api_passphrase="api-passphrase",
        live_allowance_ready=True,
        live_geoblock_ready=True,
        live_account_ready=True,
        watch_wallets="0x1111111111111111111111111111111111111111",
    )


def _verify_live_build_chain_rejects_raw_key_without_echo() -> None:
    sentinel = "RAW_PRIVATE_KEY_MUST_NOT_APPEAR"
    settings = Settings(
        _env_file=None,
        dry_run=False,
        funder_address="0xabc123",
        private_key=sentinel,
        signer_url="https://signer.internal.local",
        clob_api_key="api-key",
        clob_api_secret="api-secret",
        clob_api_passphrase="api-passphrase",
        watch_wallets="0x1111111111111111111111111111111111111111",
    )
    try:
        build_trader(settings)
    except RuntimeError as exc:
        text = str(exc)
        if "raw_private_key_forbidden_live" not in text:
            raise AssertionError(f"unexpected error when raw key present: {text}") from exc
        _assert_no_sentinel(text, sentinel, where="build_trader exception")
        return
    raise AssertionError("expected build_trader to fail when live PRIVATE_KEY is provided")


def _verify_runtime_api_and_report_no_leak() -> None:
    sentinel = "RAW_PRIVATE_KEY_MUST_NOT_APPEAR"
    with tempfile.TemporaryDirectory() as tmpdir_raw:
        workdir = Path(tmpdir_raw)
        settings = _build_live_settings(workdir, sentinel=sentinel)
        trader = Trader(
            settings=settings,
            data_client=_DummyDataClient(),
            strategy=_DummyStrategy(),
            risk=_DummyRisk(),
            broker=_DummyBroker(),
        )

        runtime_payload = trader._dump_runtime_state()
        _assert_no_sentinel(json.dumps(runtime_payload, ensure_ascii=False), sentinel, where="runtime payload")

        trader.persist_runtime_state(str(settings.runtime_state_path))
        runtime_file_text = Path(settings.runtime_state_path).read_text(encoding="utf-8")
        _assert_no_sentinel(runtime_file_text, sentinel, where="runtime state export file")

        api_payload = _api_state_payload(runtime_payload, None)
        _assert_no_sentinel(json.dumps(api_payload, ensure_ascii=False), sentinel, where="/api/state payload")

        preflight = _load_live_smoke_preflight_module()
        report, _exit_code = preflight.build_report(settings, now_ts=int(time.time()))
        report_text = json.dumps(report, ensure_ascii=False)
        _assert_no_sentinel(report_text, sentinel, where="live smoke report payload")

        report_path = workdir / "live_smoke_report.json"
        report_path.write_text(report_text, encoding="utf-8")
        _assert_no_sentinel(report_path.read_text(encoding="utf-8"), sentinel, where="live smoke report file")


def main() -> int:
    _verify_live_build_chain_rejects_raw_key_without_echo()
    print("[PASS] live build chain rejects raw key without echo")
    _verify_runtime_api_and_report_no_leak()
    print("[PASS] runtime/api/report payloads contain no raw key sentinel")
    print("[OK] verify_no_raw_key_in_live_mode")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
