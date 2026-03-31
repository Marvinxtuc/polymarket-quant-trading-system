from __future__ import annotations

import json
import time
import unittest
from pathlib import Path

from polymarket_bot.runner import Trader
from polymarket_bot.state_store import StateStore
from runtime_persistence_helpers import (
    DummyBroker,
    DummyDataClient,
    DummyRisk,
    DummyStrategy,
    build_signal,
    make_settings,
    new_tmp_dir,
)


class BrokerDbConflictBlockBuyTests(unittest.TestCase):
    def test_db_pending_without_broker_evidence_blocks_buy(self):
        workdir = new_tmp_dir()
        settings = make_settings(
            dry_run=False,
            workdir=workdir,
            funder_address="0xabc0000000000000000000000000000000000000",
        )
        settings.live_allowance_ready = True
        settings.live_geoblock_ready = True
        settings.live_account_ready = True
        smoke_path = Path(workdir) / "network_smoke.jsonl"
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
                "positions": [],
                "order_intents": [
                    {
                        "intent_id": "signal-conflict-1",
                        "strategy_order_uuid": "so-conflict-1",
                        "broker_order_id": "order-conflict-1",
                        "token_id": "token-conflict",
                        "condition_id": "condition-conflict",
                        "side": "BUY",
                        "status": "posted",
                        "recovered_source": "db",
                        "recovery_reason": "persisted",
                        "created_ts": 1000,
                        "updated_ts": 1000,
                        "payload": {
                            "key": "pending-conflict-1",
                            "ts": 1000,
                            "cycle_id": "cycle-conflict-1",
                            "order_id": "order-conflict-1",
                            "broker_status": "posted",
                            "signal_id": "signal-conflict-1",
                            "trace_id": "trace-conflict-1",
                            "token_id": "token-conflict",
                            "condition_id": "condition-conflict",
                            "market_slug": "conflict-market",
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

        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(positions=[]),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=DummyBroker(open_orders=[], fills=[]),
        )

        state = trader.trading_mode_state()
        self.assertFalse(trader.startup_ready)
        self.assertEqual(trader.trading_mode, "REDUCE_ONLY")
        self.assertFalse(bool(state.get("opening_allowed")))
        self.assertIn("recovery_conflict", list(state.get("reason_codes") or []))
        self.assertEqual(len(trader.pending_orders), 1)
        trader.strategy = DummyStrategy([build_signal(token_id="token-conflict", side="BUY")])
        trader.step()
        self.assertEqual(len(trader.broker.calls), 0)


if __name__ == "__main__":
    unittest.main()
