from __future__ import annotations

import unittest

from polymarket_bot.runner import Trader
from polymarket_bot.state_store import StateStore
from runtime_persistence_helpers import DummyBroker, DummyDataClient, DummyRisk, DummyStrategy, make_settings, new_tmp_dir


class RuntimeStatePersistenceTests(unittest.TestCase):
    def test_runtime_truth_persists_positions_pending_control_risk_reconciliation(self):
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
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
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )
        trader.positions_book = {
            "token-demo": {
                "token_id": "token-demo",
                "condition_id": "condition-token-demo",
                "market_slug": "demo-market",
                "outcome": "YES",
                "quantity": 100.0,
                "price": 0.55,
                "notional": 55.0,
                "cost_basis_notional": 55.0,
                "opened_ts": 100,
                "last_buy_ts": 100,
                "last_trim_ts": 0,
            }
        }
        trader.pending_orders = {
            "pending-1": {
                "key": "pending-1",
                "ts": 200,
                "cycle_id": "cycle-1",
                "order_id": "order-1",
                "broker_status": "posted",
                "signal_id": "signal-1",
                "trace_id": "trace-1",
                "token_id": "token-pending",
                "condition_id": "condition-pending",
                "market_slug": "pending-market",
                "outcome": "YES",
                "side": "BUY",
                "wallet": "0x1111111111111111111111111111111111111111",
                "wallet_score": 80.0,
                "wallet_tier": "CORE",
                "requested_notional": 20.0,
                "requested_price": 0.5,
                "strategy_order_uuid": "so-pending-1",
            }
        }
        trader._refresh_risk_state()
        trader.persist_runtime_state(settings.runtime_state_path)

        truth = store.load_runtime_truth()
        self.assertIn("runtime", truth)
        self.assertIn("control", truth)
        self.assertIn("risk", truth)
        self.assertIn("reconciliation", truth)
        self.assertEqual(len(truth["positions"]), 1)
        self.assertEqual(len(truth["order_intents"]), 1)
        self.assertEqual(truth["order_intents"][0]["token_id"], "token-pending")

    def test_pending_orders_field_in_runtime_snapshot_is_not_used_as_recovery_truth(self):
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
        store = StateStore(settings.state_store_path)
        store.save_runtime_truth(
            {
                "runtime": {
                    "ts": 1000,
                    "runtime_version": 8,
                    "broker_event_sync_ts": 1000,
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
                "order_intents": [],
            }
        )

        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )

        self.assertEqual(len(trader.pending_orders), 0)
        self.assertNotIn("token-dirty", trader.positions_book)


if __name__ == "__main__":
    unittest.main()
