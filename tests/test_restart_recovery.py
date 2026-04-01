from __future__ import annotations

import unittest

from polymarket_bot.runner import Trader
from polymarket_bot.state_store import StateStore
from polymarket_bot.types import OpenOrderSnapshot
from runtime_persistence_helpers import DummyBroker, DummyDataClient, DummyRisk, DummyStrategy, make_settings, new_tmp_dir


class RestartRecoveryTests(unittest.TestCase):
    def test_restart_recovers_from_db_without_file_truth(self):
        workdir = new_tmp_dir()
        settings = make_settings(
            dry_run=False,
            workdir=workdir,
            funder_address="0xabc0000000000000000000000000000000000000",
        )
        store = StateStore(settings.state_store_path)
        store.save_runtime_truth(
            {
                "runtime": {
                    "ts": 1000,
                    "runtime_version": 8,
                    "broker_event_sync_ts": 1000,
                    "recent_order_keys": {},
                    "signal_cycles": [],
                    "trace_registry": [],
                },
                "control": {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "clear_stale_pending_requested_ts": 0,
                    "updated_ts": 1000,
                },
                "risk": {
                    "day_key": "",
                    "daily_realized_pnl": 0.0,
                    "broker_closed_pnl_today": 0.0,
                    "equity_usd": 1000.0,
                    "cash_balance_usd": 1000.0,
                    "positions_value_usd": 55.0,
                    "account_snapshot_ts": 1000,
                },
                "reconciliation": {"status": "ok", "issues": []},
                "positions": [
                    {
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
                ],
                "order_intents": [
                    {
                        "intent_id": "signal-1",
                        "strategy_order_uuid": "so-1",
                        "broker_order_id": "order-1",
                        "token_id": "token-demo",
                        "condition_id": "condition-token-demo",
                        "side": "BUY",
                        "status": "posted",
                        "recovered_source": "db",
                        "recovery_reason": "persisted",
                        "created_ts": 1000,
                        "updated_ts": 1000,
                        "payload": {
                            "key": "pending-1",
                            "ts": 1000,
                            "cycle_id": "cycle-1",
                            "order_id": "order-1",
                            "broker_status": "posted",
                            "signal_id": "signal-1",
                            "trace_id": "trace-1",
                            "token_id": "token-demo",
                            "condition_id": "condition-token-demo",
                            "market_slug": "demo-market",
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

        broker = DummyBroker(
            open_orders=[
                OpenOrderSnapshot(
                    order_id="order-1",
                    token_id="token-demo",
                    side="BUY",
                    status="open",
                    price=0.5,
                    original_size=40.0,
                    matched_size=0.0,
                    remaining_size=40.0,
                    created_ts=1000,
                    condition_id="condition-token-demo",
                    market_slug="demo-market",
                    outcome="YES",
                )
            ]
        )
        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(
                positions=[
                    {
                        "token_id": "token-demo",
                        "condition_id": "condition-token-demo",
                        "market_slug": "demo-market",
                        "outcome": "YES",
                        "quantity": 100.0,
                        "price": 0.55,
                        "notional": 55.0,
                        "opened_ts": 100,
                    }
                ]
            ),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=broker,
        )

        self.assertEqual(len(trader.positions_book), 1)
        self.assertEqual(len(trader.pending_orders), 1)
        self.assertNotIn("recovery_conflict", trader.trading_mode_reasons)

    def test_invalid_control_payload_fail_closes_on_recovery(self):
        workdir = new_tmp_dir()
        settings = make_settings(dry_run=True, workdir=workdir)
        store = StateStore(settings.state_store_path)
        store.save_runtime_truth(
            {
                "runtime": {
                    "ts": 1000,
                    "runtime_version": 8,
                    "broker_event_sync_ts": 1000,
                    "recent_order_keys": {},
                    "signal_cycles": [],
                    "trace_registry": [],
                },
                "control": {
                    "decision_mode": "invalid-mode",
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

        self.assertTrue(trader.control_state.pause_opening)
        self.assertTrue(trader.control_state.reduce_only)
        self.assertIn("recovery_conflict", trader.trading_mode_reasons)


if __name__ == "__main__":
    unittest.main()
