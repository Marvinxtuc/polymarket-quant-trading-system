from __future__ import annotations

import os
import json
import time
import unittest

from polymarket_bot.runner import Trader
from polymarket_bot.state_store import StateStore
from polymarket_bot.types import OpenOrderSnapshot
from runtime_persistence_helpers import DummyBroker, DummyDataClient, DummyRisk, DummyStrategy, make_settings, new_tmp_dir


class TmpDeletionRecoveryTests(unittest.TestCase):
    def test_restart_recovers_after_runtime_exports_deleted(self):
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
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "clear_stale_pending_requested_ts": 0,
                    "updated_ts": 1000,
                },
                "risk": {"day_key": "", "daily_realized_pnl": 0.0, "broker_closed_pnl_today": 0.0},
                "reconciliation": {"status": "ok", "issues": []},
                "positions": [
                    {
                        "token_id": "token-only-db",
                        "condition_id": "condition-token-only-db",
                        "market_slug": "db-market",
                        "outcome": "YES",
                        "quantity": 50.0,
                        "price": 0.5,
                        "notional": 25.0,
                        "cost_basis_notional": 25.0,
                        "opened_ts": 100,
                        "last_buy_ts": 100,
                        "last_trim_ts": 0,
                    }
                ],
                "order_intents": [
                    {
                        "intent_id": "signal-db-1",
                        "strategy_order_uuid": "so-db-1",
                        "broker_order_id": "order-db-1",
                        "token_id": "token-only-db",
                        "condition_id": "condition-token-only-db",
                        "side": "BUY",
                        "status": "posted",
                        "recovered_source": "db",
                        "recovery_reason": "persisted",
                        "created_ts": 1000,
                        "updated_ts": 1000,
                        "payload": {
                            "key": "pending-db-1",
                            "ts": 1000,
                            "cycle_id": "cycle-db-1",
                            "order_id": "order-db-1",
                            "broker_status": "posted",
                            "signal_id": "signal-db-1",
                            "trace_id": "trace-db-1",
                            "token_id": "token-only-db",
                            "condition_id": "condition-token-only-db",
                            "market_slug": "db-market",
                            "outcome": "YES",
                            "side": "BUY",
                            "wallet": "0x1111111111111111111111111111111111111111",
                            "wallet_score": 80.0,
                            "wallet_tier": "CORE",
                            "requested_notional": 10.0,
                            "requested_price": 0.5,
                        },
                    }
                ],
            }
        )

        if os.path.exists(settings.runtime_state_path):
            os.remove(settings.runtime_state_path)
        if os.path.exists(settings.control_path):
            os.remove(settings.control_path)

        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(),
        )

        self.assertIn("token-only-db", trader.positions_book)
        self.assertEqual(len(trader.pending_orders), 1)
        self.assertNotIn("recovery_conflict", trader.trading_mode_reasons)

    def test_dirty_tmp_exports_do_not_override_db_and_broker_truth(self):
        workdir = new_tmp_dir()
        settings = make_settings(
            dry_run=False,
            workdir=workdir,
            funder_address="0xabc0000000000000000000000000000000000000",
        )
        settings.live_allowance_ready = True
        settings.live_geoblock_ready = True
        settings.live_account_ready = True

        smoke_path = workdir / "network_smoke.jsonl"
        smoke_path.write_text(
            json.dumps(
                {
                    "ts": int(time.time()),
                    "summary": {"exit_code": 0, "warnings": 0, "blocks": 0, "failures": 0},
                },
                ensure_ascii=False,
            )
            + "\n",
            encoding="utf-8",
        )
        settings.network_smoke_log_path = str(smoke_path)

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
                "risk": {"day_key": "", "daily_realized_pnl": 0.0, "broker_closed_pnl_today": 0.0},
                "reconciliation": {"status": "ok", "issues": []},
                "positions": [
                    {
                        "token_id": "token-db",
                        "condition_id": "condition-token-db",
                        "market_slug": "db-market",
                        "outcome": "YES",
                        "quantity": 50.0,
                        "price": 0.5,
                        "notional": 25.0,
                        "cost_basis_notional": 25.0,
                        "opened_ts": 100,
                        "last_buy_ts": 100,
                        "last_trim_ts": 0,
                    }
                ],
                "order_intents": [
                    {
                        "intent_id": "signal-db-1",
                        "strategy_order_uuid": "so-db-1",
                        "broker_order_id": "order-db-1",
                        "token_id": "token-db",
                        "condition_id": "condition-token-db",
                        "side": "BUY",
                        "status": "posted",
                        "recovered_source": "db",
                        "recovery_reason": "persisted",
                        "created_ts": 1000,
                        "updated_ts": 1000,
                        "payload": {
                            "key": "pending-db-1",
                            "ts": 1000,
                            "cycle_id": "cycle-db-1",
                            "order_id": "order-db-1",
                            "broker_status": "posted",
                            "signal_id": "signal-db-1",
                            "trace_id": "trace-db-1",
                            "token_id": "token-db",
                            "condition_id": "condition-token-db",
                            "market_slug": "db-market",
                            "outcome": "YES",
                            "side": "BUY",
                            "wallet": "0x1111111111111111111111111111111111111111",
                            "wallet_score": 80.0,
                            "wallet_tier": "CORE",
                            "requested_notional": 10.0,
                            "requested_price": 0.5,
                        },
                    }
                ],
            }
        )

        with open(settings.runtime_state_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "positions": [
                        {
                            "token_id": "token-dirty",
                            "market_slug": "dirty-market",
                            "outcome": "YES",
                            "quantity": 999.0,
                            "price": 0.9,
                            "notional": 899.1,
                        }
                    ],
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
                f,
                ensure_ascii=False,
            )
        with open(settings.control_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "decision_mode": "manual",
                    "pause_opening": True,
                    "reduce_only": True,
                    "emergency_stop": False,
                    "updated_ts": 2000,
                },
                f,
                ensure_ascii=False,
            )

        broker = DummyBroker(
            open_orders=[
                OpenOrderSnapshot(
                    order_id="order-db-1",
                    token_id="token-db",
                    side="BUY",
                    status="open",
                    price=0.5,
                    original_size=20.0,
                    matched_size=0.0,
                    remaining_size=20.0,
                    created_ts=1000,
                    condition_id="condition-token-db",
                    market_slug="db-market",
                    outcome="YES",
                )
            ]
        )
        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(
                positions=[
                    {
                        "token_id": "token-db",
                        "condition_id": "condition-token-db",
                        "market_slug": "db-market",
                        "outcome": "YES",
                        "quantity": 50.0,
                        "price": 0.5,
                        "notional": 25.0,
                        "opened_ts": 100,
                    }
                ]
            ),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=broker,
        )

        self.assertIn("token-db", trader.positions_book)
        self.assertNotIn("token-dirty", trader.positions_book)
        self.assertEqual(len(trader.pending_orders), 1)
        restored = next(iter(trader.pending_orders.values()))
        self.assertEqual(str(restored.get("order_id") or ""), "order-db-1")
        self.assertNotEqual(str(restored.get("order_id") or ""), "dirty-order")
        self.assertFalse(trader.control_state.pause_opening)
        self.assertFalse(trader.control_state.reduce_only)


if __name__ == "__main__":
    unittest.main()
