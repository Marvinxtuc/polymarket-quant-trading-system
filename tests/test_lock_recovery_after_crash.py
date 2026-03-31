from __future__ import annotations

import subprocess
import sys
import tempfile
import unittest
import os
from pathlib import Path

from polymarket_bot.locks import FileLock
from polymarket_bot.clients.data_api import AccountingSnapshot
from polymarket_bot.config import Settings
from polymarket_bot.locks import SingleWriterLockError
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
        raise AssertionError("execute should not run in this test")


class LockRecoveryAfterCrashTests(unittest.TestCase):
    def test_lock_can_be_reacquired_after_holder_process_exits(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            lock_path = Path(tmpdir) / "wallet.lock"
            script = (
                "from polymarket_bot.locks import FileLock\n"
                "import os, sys\n"
                "lock = FileLock(sys.argv[1], timeout=0.0, writer_scope='paper:default')\n"
                "lock.acquire()\n"
                "os._exit(0)\n"
            )
            env = os.environ.copy()
            env["PYTHONPATH"] = "src"
            result = subprocess.run(
                [sys.executable, "-c", script, str(lock_path)],
                cwd=str(Path(__file__).resolve().parents[1]),
                env=env,
                check=False,
            )
            self.assertEqual(result.returncode, 0)

            lock = FileLock(str(lock_path), timeout=0.0, writer_scope="paper:default")
            lock.acquire()
            lock.release()

    def test_old_writer_loses_write_privilege_after_handover(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            settings = Settings(
                _env_file=None,
                dry_run=True,
                enable_single_writer=True,
                wallet_lock_path=str(root / "wallet.lock"),
                state_store_path=str(root / "state.db"),
                runtime_state_path=str(root / "runtime_state.json"),
                control_path=str(root / "control.json"),
                ledger_path=str(root / "ledger.jsonl"),
                candidate_db_path=str(root / "terminal.db"),
            )
            (root / "runtime_state.json").write_text("{}", encoding="utf-8")
            (root / "control.json").write_text("{}", encoding="utf-8")
            (root / "ledger.jsonl").write_text("", encoding="utf-8")

            old_writer = Trader(
                settings=settings,
                data_client=_DataClient(),
                strategy=_Strategy(),
                risk=_Risk(),
                broker=_Broker(),
            )
            self.assertIsNotNone(old_writer._writer_lock)
            old_writer._writer_lock.release()

            new_writer = Trader(
                settings=settings,
                data_client=_DataClient(),
                strategy=_Strategy(),
                risk=_Risk(),
                broker=_Broker(),
            )
            try:
                with self.assertRaises(SingleWriterLockError):
                    old_writer._state_store.save_control_state({"decision_mode": "manual"})
            finally:
                if new_writer._writer_lock is not None:
                    new_writer._writer_lock.release()
                if old_writer._writer_lock is not None:
                    old_writer._writer_lock.release()


if __name__ == "__main__":
    unittest.main()
