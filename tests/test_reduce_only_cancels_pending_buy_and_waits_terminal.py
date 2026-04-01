from __future__ import annotations

import time
import unittest

from polymarket_bot.runner import Trader
from polymarket_bot.types import OpenOrderSnapshot, OrderStatusSnapshot

from kill_switch_test_helpers import (
    DummyDataClient,
    DummyRisk,
    DummyStrategy,
    ScenarioBroker,
    make_settings,
    seed_control_state,
    seed_pending_buy,
    temp_dir,
)


class ReduceOnlyTerminalCleanupTests(unittest.TestCase):
    def test_reduce_only_waits_for_terminal_then_releases_after_control_clear(self):
        workdir = temp_dir("kill-switch-reduce-only-")
        settings = make_settings(workdir=workdir, dry_run=True)
        broker = ScenarioBroker()
        broker.open_orders = [
            OpenOrderSnapshot(
                order_id="oid-reduce-only",
                token_id="token-kill",
                side="BUY",
                status="live",
                price=0.6,
                original_size=100.0,
                matched_size=0.0,
                remaining_size=100.0,
                created_ts=int(time.time()) - 60,
            )
        ]
        broker.status_by_order_id["oid-reduce-only"] = OrderStatusSnapshot(
            order_id="oid-reduce-only",
            status="requested",
            original_size=100.0,
            remaining_size=100.0,
            message="cancel requested",
        )

        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=broker,
        )
        seed_pending_buy(trader, order_id="oid-reduce-only", ts_offset_seconds=5)
        seed_control_state(settings, reduce_only=True)

        trader.step()
        first_state = trader.kill_switch_state()
        self.assertEqual(first_state["phase"], "WAITING_BROKER_TERMINAL")
        self.assertFalse(bool(first_state["broker_safe_confirmed"]))

        broker.open_orders = []
        broker.status_by_order_id["oid-reduce-only"] = OrderStatusSnapshot(
            order_id="oid-reduce-only",
            status="canceled",
            original_size=100.0,
            remaining_size=0.0,
            message="cancel confirmed",
        )

        trader.step()
        second_state = trader.kill_switch_state()
        self.assertEqual(second_state["phase"], "SAFE_CONFIRMED")
        self.assertTrue(bool(second_state["broker_safe_confirmed"]))
        self.assertFalse(bool(second_state["opening_allowed"]))

        seed_control_state(settings, reduce_only=False)
        trader.step()
        final_state = trader.kill_switch_state()
        self.assertEqual(final_state["phase"], "IDLE")
        self.assertTrue(bool(final_state["opening_allowed"]))


if __name__ == "__main__":
    unittest.main()
