from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from polymarket_bot.clients.data_api import AccountingSnapshot
from polymarket_bot.config import Settings
from polymarket_bot.locks import (
    FileLock,
    SINGLE_WRITER_CONFLICT_EXIT_CODE,
    SINGLE_WRITER_CONFLICT_REASON,
    SingleWriterLockError,
)
from polymarket_bot.risk import RiskDecision
from polymarket_bot.runner import Trader


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
        _ = wallets
        return []


class _Risk:
    def evaluate(self, signal, state):
        _ = signal
        _ = state
        return RiskDecision(allowed=True, reason="ok", max_notional=10.0, snapshot={})


class _Broker:
    def startup_checks(self):
        return []

    def execute(self, signal, notional_usd, *, strategy_order_uuid=None):
        _ = signal
        _ = notional_usd
        _ = strategy_order_uuid
        raise AssertionError("execute should not be called in this test")


def _build_settings(tmpdir: Path) -> Settings:
    lock_path = tmpdir / "wallet.lock"
    state_store_path = tmpdir / "state.db"
    runtime_state_path = tmpdir / "runtime_state.json"
    control_path = tmpdir / "control.json"
    ledger_path = tmpdir / "ledger.jsonl"
    candidate_db_path = tmpdir / "candidate.db"
    runtime_state_path.write_text("{}", encoding="utf-8")
    control_path.write_text("{}", encoding="utf-8")
    ledger_path.write_text("", encoding="utf-8")
    return Settings(
        _env_file=None,
        dry_run=True,
        enable_single_writer=True,
        wallet_lock_path=str(lock_path),
        state_store_path=str(state_store_path),
        runtime_state_path=str(runtime_state_path),
        control_path=str(control_path),
        ledger_path=str(ledger_path),
        candidate_db_path=str(candidate_db_path),
    )


class SecondExecutorTests(unittest.TestCase):
    def test_second_executor_conflict_raises_single_writer_error(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_raw:
            tmpdir = Path(tmpdir_raw)
            settings_a = _build_settings(tmpdir)
            settings_b = _build_settings(tmpdir)
            trader_a = Trader(
                settings=settings_a,
                data_client=_DataClient(),
                strategy=_Strategy(),
                risk=_Risk(),
                broker=_Broker(),
            )
            try:
                with self.assertRaises(SingleWriterLockError) as ctx:
                    Trader(
                        settings=settings_b,
                        data_client=_DataClient(),
                        strategy=_Strategy(),
                        risk=_Risk(),
                        broker=_Broker(),
                    )
                self.assertEqual(getattr(ctx.exception, "reason_code", ""), SINGLE_WRITER_CONFLICT_REASON)
            finally:
                if trader_a._writer_lock is not None:
                    trader_a._writer_lock.release()
                    trader_a._writer_lock = None

    def test_lock_conflict_exit_code_is_standardized(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_raw:
            tmpdir = Path(tmpdir_raw)
            lock_path = tmpdir / "wallet.lock"
            holder = FileLock(str(lock_path), timeout=0.0, writer_scope="paper:default")
            holder.acquire()
            try:
                env = os.environ.copy()
                env["PYTHONPATH"] = "src"
                env["DRY_RUN"] = "true"
                env["ENABLE_SINGLE_WRITER"] = "true"
                env["WALLET_LOCK_PATH"] = str(lock_path)
                env["STATE_STORE_PATH"] = str(tmpdir / "state.db")
                env["RUNTIME_STATE_PATH"] = str(tmpdir / "runtime_state.json")
                env["CONTROL_PATH"] = str(tmpdir / "control.json")
                env["LEDGER_PATH"] = str(tmpdir / "ledger.jsonl")
                env["CANDIDATE_DB_PATH"] = str(tmpdir / "terminal.db")
                (tmpdir / "runtime_state.json").write_text("{}", encoding="utf-8")
                (tmpdir / "control.json").write_text("{}", encoding="utf-8")
                (tmpdir / "ledger.jsonl").write_text("", encoding="utf-8")
                result = subprocess.run(
                    [sys.executable, "-m", "polymarket_bot.main", "--once"],
                    cwd=str(Path(__file__).resolve().parents[1]),
                    env=env,
                    check=False,
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode, SINGLE_WRITER_CONFLICT_EXIT_CODE)
            finally:
                holder.release()

    def test_live_mode_disallows_single_writer_bypass_in_main(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_raw:
            tmpdir = Path(tmpdir_raw)
            env = os.environ.copy()
            env["PYTHONPATH"] = "src"
            env["DRY_RUN"] = "false"
            env["ENABLE_SINGLE_WRITER"] = "false"
            env["WALLET_LOCK_PATH"] = str(tmpdir / "wallet.lock")
            env["STATE_STORE_PATH"] = str(tmpdir / "state.db")
            env["RUNTIME_STATE_PATH"] = str(tmpdir / "runtime_state.json")
            env["CONTROL_PATH"] = str(tmpdir / "control.json")
            env["LEDGER_PATH"] = str(tmpdir / "ledger.jsonl")
            env["CANDIDATE_DB_PATH"] = str(tmpdir / "terminal.db")
            (tmpdir / "runtime_state.json").write_text("{}", encoding="utf-8")
            (tmpdir / "control.json").write_text("{}", encoding="utf-8")
            (tmpdir / "ledger.jsonl").write_text("", encoding="utf-8")
            result = subprocess.run(
                [sys.executable, "-m", "polymarket_bot.main", "--once"],
                cwd=str(Path(__file__).resolve().parents[1]),
                env=env,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 2)

    def test_live_mode_disallows_single_writer_bypass_in_daemon(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_raw:
            tmpdir = Path(tmpdir_raw)
            env = os.environ.copy()
            env["PYTHONPATH"] = "src"
            env["DRY_RUN"] = "false"
            env["ENABLE_SINGLE_WRITER"] = "false"
            env["WALLET_LOCK_PATH"] = str(tmpdir / "wallet.lock")
            env["STATE_STORE_PATH"] = str(tmpdir / "state.db")
            env["RUNTIME_STATE_PATH"] = str(tmpdir / "runtime_state.json")
            env["CONTROL_PATH"] = str(tmpdir / "control.json")
            env["LEDGER_PATH"] = str(tmpdir / "ledger.jsonl")
            env["CANDIDATE_DB_PATH"] = str(tmpdir / "terminal.db")
            (tmpdir / "runtime_state.json").write_text("{}", encoding="utf-8")
            (tmpdir / "control.json").write_text("{}", encoding="utf-8")
            (tmpdir / "ledger.jsonl").write_text("", encoding="utf-8")
            result = subprocess.run(
                [sys.executable, "-m", "polymarket_bot.daemon", "--once"],
                cwd=str(Path(__file__).resolve().parents[1]),
                env=env,
                check=False,
                capture_output=True,
                text=True,
            )
            self.assertEqual(result.returncode, 2)

    def test_main_acquires_lock_before_build_trader(self) -> None:
        import polymarket_bot.main as main_mod

        with tempfile.TemporaryDirectory() as tmpdir_raw:
            tmpdir = Path(tmpdir_raw)
            order: list[str] = []
            settings = Settings(
                _env_file=None,
                dry_run=True,
                enable_single_writer=True,
                wallet_lock_path=str(tmpdir / "wallet.lock"),
                state_store_path=str(tmpdir / "state.db"),
                runtime_state_path=str(tmpdir / "runtime_state.json"),
                control_path=str(tmpdir / "control.json"),
                ledger_path=str(tmpdir / "ledger.jsonl"),
                candidate_db_path=str(tmpdir / "terminal.db"),
            )
            (tmpdir / "runtime_state.json").write_text("{}", encoding="utf-8")
            (tmpdir / "control.json").write_text("{}", encoding="utf-8")
            (tmpdir / "ledger.jsonl").write_text("", encoding="utf-8")

            def _fake_acquire(_self):
                order.append("lock")

            def _fake_build_trader(_settings, *, pre_acquired_writer_lock=None):
                order.append("build_trader")
                return SimpleNamespace(
                    run=lambda once=False: order.append("run"),
                    broker=SimpleNamespace(close=lambda: None),
                    data_client=SimpleNamespace(close=lambda: None),
                    _writer_lock=pre_acquired_writer_lock,
                )

            with (
                patch.object(main_mod.argparse.ArgumentParser, "parse_args", return_value=SimpleNamespace(once=True)),
                patch.object(main_mod, "Settings", return_value=settings),
                patch.object(main_mod, "setup_logger"),
                patch.object(main_mod.FileLock, "acquire", autospec=True, side_effect=_fake_acquire),
                patch.object(main_mod, "build_trader", side_effect=_fake_build_trader),
            ):
                main_mod.main()

            self.assertIn("lock", order)
            self.assertIn("build_trader", order)
            self.assertLess(order.index("lock"), order.index("build_trader"))

    def test_daemon_acquires_lock_before_build_trader(self) -> None:
        import polymarket_bot.daemon as daemon_mod

        with tempfile.TemporaryDirectory() as tmpdir_raw:
            tmpdir = Path(tmpdir_raw)
            order: list[str] = []
            settings = Settings(
                _env_file=None,
                dry_run=True,
                enable_single_writer=True,
                wallet_lock_path=str(tmpdir / "wallet.lock"),
                state_store_path=str(tmpdir / "state.db"),
                runtime_state_path=str(tmpdir / "runtime_state.json"),
                control_path=str(tmpdir / "control.json"),
                ledger_path=str(tmpdir / "ledger.jsonl"),
                candidate_db_path=str(tmpdir / "terminal.db"),
            )
            (tmpdir / "runtime_state.json").write_text("{}", encoding="utf-8")
            (tmpdir / "control.json").write_text("{}", encoding="utf-8")
            (tmpdir / "ledger.jsonl").write_text("", encoding="utf-8")

            args = SimpleNamespace(
                state_path="/tmp/poly_runtime_data/state.json",
                decision_mode_path=os.getenv("POLY_DECISION_MODE_PATH", "/tmp/poly_runtime_data/decision_mode.json"),
                candidate_actions_path=os.getenv("POLY_CANDIDATE_ACTIONS_PATH", "/tmp/poly_runtime_data/candidate_actions.json"),
                wallet_profiles_path=os.getenv("POLY_WALLET_PROFILES_PATH", "/tmp/poly_runtime_data/wallet_profiles.json"),
                journal_path=os.getenv("POLY_JOURNAL_PATH", "/tmp/poly_runtime_data/journal.json"),
                once=True,
            )

            def _fake_acquire(_self):
                order.append("lock")

            def _fake_build_trader(_settings, *, pre_acquired_writer_lock=None):
                order.append("build_trader")
                return SimpleNamespace(
                    step=lambda: None,
                    last_wallets=[],
                    last_signals=[],
                    recent_orders=[],
                    broker=SimpleNamespace(close=lambda: None),
                    data_client=SimpleNamespace(close=lambda: None),
                    _writer_lock=pre_acquired_writer_lock,
                )

            with (
                patch.object(daemon_mod.argparse.ArgumentParser, "parse_args", return_value=args),
                patch.object(daemon_mod, "Settings", return_value=settings),
                patch.object(daemon_mod, "setup_logger"),
                patch.object(daemon_mod.FileLock, "acquire", autospec=True, side_effect=_fake_acquire),
                patch.object(daemon_mod, "build_trader", side_effect=_fake_build_trader),
                patch.object(daemon_mod, "_prepare_bootstrap_trader_state"),
                patch.object(daemon_mod, "_build_state", return_value={}),
                patch.object(daemon_mod, "_persist_cycle_outputs"),
            ):
                daemon_mod.main()

            self.assertIn("lock", order)
            self.assertIn("build_trader", order)
            self.assertLess(order.index("lock"), order.index("build_trader"))


if __name__ == "__main__":
    unittest.main()
