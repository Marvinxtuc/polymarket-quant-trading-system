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


class EmergencyStopLatchTests(unittest.TestCase):
    def test_emergency_stop_stays_latched_until_broker_terminal(self):
        workdir = temp_dir("kill-switch-emergency-latch-")
        settings = make_settings(workdir=workdir, dry_run=True)
        broker = ScenarioBroker()
        order_id = "oid-emergency-buy"
        broker.open_orders = [
            OpenOrderSnapshot(
                order_id=order_id,
                token_id="token-kill",
                side="BUY",
                status="live",
                price=0.6,
                original_size=100.0,
                matched_size=0.0,
                remaining_size=100.0,
                created_ts=int(time.time()) - 50,
            )
        ]
        broker.status_by_order_id[order_id] = OrderStatusSnapshot(
            order_id=order_id,
            status="requested",
            original_size=100.0,
            remaining_size=100.0,
            message="cancel queued",
        )

        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=broker,
        )
        seed_pending_buy(trader, order_id=order_id, ts_offset_seconds=8)
        seed_control_state(settings, emergency_stop=True)

        trader.step()
        first_state = trader.kill_switch_state()
        self.assertEqual(first_state["mode_requested"], "emergency_stop")
        self.assertEqual(first_state["phase"], "WAITING_BROKER_TERMINAL")
        self.assertFalse(bool(first_state["opening_allowed"]))
        self.assertTrue(bool(first_state["halted"]))
        self.assertTrue(bool(first_state["latched"]))
        self.assertFalse(bool(first_state["broker_safe_confirmed"]))

        # Operator clears emergency_stop too early: latch must stay active while broker remains non-terminal.
        seed_control_state(settings, emergency_stop=False)
        trader.step()
        second_state = trader.kill_switch_state()
        self.assertEqual(second_state["mode_requested"], "emergency_stop")
        self.assertEqual(second_state["phase"], "WAITING_BROKER_TERMINAL")
        self.assertFalse(bool(second_state["opening_allowed"]))
        self.assertTrue(bool(second_state["halted"]))
        self.assertTrue(bool(second_state["latched"]))
        self.assertFalse(bool(second_state["broker_safe_confirmed"]))

        broker.open_orders = []
        broker.status_by_order_id[order_id] = OrderStatusSnapshot(
            order_id=order_id,
            status="canceled",
            original_size=100.0,
            remaining_size=0.0,
            message="cancel confirmed",
        )

        trader.step()
        final_state = trader.kill_switch_state()
        self.assertEqual(final_state["phase"], "IDLE")
        self.assertTrue(bool(final_state["opening_allowed"]))
        self.assertFalse(bool(final_state["halted"]))
        self.assertTrue(bool(final_state["broker_safe_confirmed"]))
        self.assertEqual(len(broker.execute_calls), 0)


if __name__ == "__main__":
    unittest.main()
