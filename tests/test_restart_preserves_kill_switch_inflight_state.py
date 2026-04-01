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
    build_signal,
    make_settings,
    seed_control_state,
    seed_pending_buy,
    temp_dir,
)


class KillSwitchRestartRecoveryTests(unittest.TestCase):
    def test_restart_continues_waiting_for_broker_terminal(self):
        workdir = temp_dir("kill-switch-restart-")
        settings = make_settings(workdir=workdir, dry_run=True)
        order_id = "oid-restart-inflight"

        broker_first = ScenarioBroker()
        broker_first.open_orders = [
            OpenOrderSnapshot(
                order_id=order_id,
                token_id="token-kill",
                side="BUY",
                status="live",
                price=0.6,
                original_size=100.0,
                matched_size=0.0,
                remaining_size=100.0,
                created_ts=int(time.time()) - 45,
            )
        ]
        broker_first.status_by_order_id[order_id] = OrderStatusSnapshot(
            order_id=order_id,
            status="requested",
            original_size=100.0,
            remaining_size=100.0,
            message="cancel queued",
        )

        first_trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([]),
            risk=DummyRisk(),
            broker=broker_first,
        )
        seed_pending_buy(first_trader, order_id=order_id, ts_offset_seconds=6)
        seed_control_state(settings, reduce_only=True)
        first_trader.step()
        first_state = first_trader.kill_switch_state()
        self.assertEqual(first_state["phase"], "WAITING_BROKER_TERMINAL")
        self.assertFalse(bool(first_state["broker_safe_confirmed"]))

        # Simulate restart after cancel has been requested but not terminal yet.
        seed_control_state(settings, reduce_only=False)
        broker_second = ScenarioBroker()
        broker_second.open_orders = list(broker_first.open_orders)
        broker_second.status_by_order_id[order_id] = OrderStatusSnapshot(
            order_id=order_id,
            status="requested",
            original_size=100.0,
            remaining_size=100.0,
            message="cancel queued",
        )

        second_trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([build_signal(side="BUY", token_id="token-restart-buy")]),
            risk=DummyRisk(),
            broker=broker_second,
        )
        recovered = second_trader.kill_switch_state()
        self.assertEqual(recovered["phase"], "WAITING_BROKER_TERMINAL")
        self.assertFalse(bool(recovered["opening_allowed"]))
        self.assertFalse(bool(recovered["broker_safe_confirmed"]))
        self.assertIn(order_id, list(recovered["tracked_buy_order_ids"]))

        second_trader.step()
        second_state = second_trader.kill_switch_state()
        self.assertEqual(second_state["phase"], "WAITING_BROKER_TERMINAL")
        self.assertFalse(bool(second_state["opening_allowed"]))
        self.assertFalse(bool(second_state["broker_safe_confirmed"]))
        self.assertEqual(len(broker_second.execute_calls), 0)


if __name__ == "__main__":
    unittest.main()
