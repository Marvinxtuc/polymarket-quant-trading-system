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
    temp_dir,
)


class KillSwitchBrokerTerminalRequiredTests(unittest.TestCase):
    def test_reduce_only_requires_broker_terminal_even_without_local_pending(self):
        workdir = temp_dir("kill-switch-terminal-required-")
        settings = make_settings(workdir=workdir, dry_run=True)
        broker = ScenarioBroker()
        order_id = "oid-open-buy-only-broker"
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
                created_ts=int(time.time()) - 30,
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
            strategy=DummyStrategy([build_signal(side="BUY", token_id="token-new-buy")]),
            risk=DummyRisk(),
            broker=broker,
        )
        seed_control_state(settings, reduce_only=True)

        trader.step()

        kill_switch = trader.kill_switch_state()
        self.assertEqual(kill_switch["mode_requested"], "reduce_only")
        self.assertEqual(kill_switch["phase"], "WAITING_BROKER_TERMINAL")
        self.assertFalse(bool(kill_switch["opening_allowed"]))
        self.assertFalse(bool(kill_switch["broker_safe_confirmed"]))
        self.assertIn(order_id, list(kill_switch["open_buy_order_ids"]))
        self.assertIn(order_id, list(kill_switch["non_terminal_buy_order_ids"]))
        self.assertIn("operator_reduce_only", list(kill_switch["reason_codes"]))
        self.assertGreaterEqual(len(broker.cancel_calls), 1)
        self.assertEqual(len(broker.execute_calls), 0)


if __name__ == "__main__":
    unittest.main()
