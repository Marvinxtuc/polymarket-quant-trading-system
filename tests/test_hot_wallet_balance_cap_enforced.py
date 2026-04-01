from __future__ import annotations

import tempfile
import time
import unittest
from pathlib import Path

from polymarket_bot.runner import Trader

from runtime_persistence_helpers import DummyBroker, DummyDataClient, DummyRisk, DummyStrategy, make_settings


class HotWalletBalanceCapTests(unittest.TestCase):
    def test_startup_check_fails_when_hot_wallet_cap_exceeded(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_raw:
            workdir = Path(tmpdir_raw)
            settings = make_settings(dry_run=False, workdir=workdir, funder_address="0xabc123")
            settings.live_hot_wallet_balance_cap_usd = 500.0
            trader = Trader(
                settings=settings,
                data_client=DummyDataClient(),
                strategy=DummyStrategy(signals=[]),
                risk=DummyRisk(),
                broker=DummyBroker(),
            )

            hot_wallet_rows = [row for row in trader.startup_checks if str(row.get("name")) == "hot_wallet_cap"]
            self.assertTrue(hot_wallet_rows)
            self.assertEqual(str(hot_wallet_rows[0].get("status")).upper(), "FAIL")
            self.assertFalse(bool(trader.startup_ready))
            self.assertFalse(bool(trader.admission_state().get("opening_allowed")))

    def test_runtime_hot_wallet_cap_exceed_latches_conflict_and_blocks_opening(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_raw:
            workdir = Path(tmpdir_raw)
            settings = make_settings(dry_run=False, workdir=workdir, funder_address="0xabc123")
            settings.live_hot_wallet_balance_cap_usd = 5000.0
            trader = Trader(
                settings=settings,
                data_client=DummyDataClient(),
                strategy=DummyStrategy(signals=[]),
                risk=DummyRisk(),
                broker=DummyBroker(),
            )

            trader.state.equity_usd = 8000.0
            trader._enforce_hot_wallet_cap(now=int(time.time()))
            reconciliation = trader.reconciliation_summary(now=int(time.time()))
            trader._update_trading_mode(trader.control_state, now=int(time.time()), reconciliation=reconciliation)

            self.assertTrue(trader._recovery_block_buy_latched)
            self.assertTrue(any(str(item.get("category") or "").upper() == "HOT_WALLET_CAP_EXCEEDED" for item in trader._recovery_conflicts))
            signer_security = trader.signer_security_state()
            self.assertIn("hot_wallet_cap_exceeded", list(signer_security.get("reason_codes") or []))
            self.assertFalse(bool(trader.admission_state().get("opening_allowed")))


if __name__ == "__main__":
    unittest.main()
