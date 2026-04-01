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


class CancelRequestedNotSafeTests(unittest.TestCase):
    def test_cancel_requested_is_not_safe_state(self):
        workdir = temp_dir("kill-switch-cancel-requested-")
        settings = make_settings(workdir=workdir, dry_run=True)
        broker = ScenarioBroker()
        broker.open_orders = [
            OpenOrderSnapshot(
                order_id="oid-cancel-requested",
                token_id="token-kill",
                side="BUY",
                status="live",
                price=0.6,
                original_size=100.0,
                matched_size=0.0,
                remaining_size=100.0,
                created_ts=int(time.time()) - 30,
            )
        ]
        broker.status_by_order_id["oid-cancel-requested"] = OrderStatusSnapshot(
            order_id="oid-cancel-requested",
            status="requested",
            original_size=100.0,
            remaining_size=100.0,
            message="cancel queued",
        )

        trader = Trader(
            settings=settings,
            data_client=DummyDataClient(),
            strategy=DummyStrategy([build_signal(side="BUY", token_id="token-kill")]),
            risk=DummyRisk(),
            broker=broker,
        )
        seed_pending_buy(trader, order_id="oid-cancel-requested", ts_offset_seconds=3)
        seed_control_state(settings, pause_opening=False, reduce_only=True, emergency_stop=False)

        trader.step()

        kill_switch = trader.kill_switch_state()
        self.assertEqual(kill_switch["mode_requested"], "reduce_only")
        self.assertEqual(kill_switch["phase"], "WAITING_BROKER_TERMINAL")
        self.assertFalse(bool(kill_switch["opening_allowed"]))
        self.assertFalse(bool(kill_switch["broker_safe_confirmed"]))
        self.assertIn("oid-cancel-requested", list(kill_switch["non_terminal_buy_order_ids"]))
        self.assertIn("operator_reduce_only", list(kill_switch["reason_codes"]))
        self.assertEqual(len(broker.execute_calls), 0)


if __name__ == "__main__":
    unittest.main()
