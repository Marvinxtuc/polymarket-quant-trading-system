from __future__ import annotations

import unittest
from datetime import datetime, timezone

from polymarket_bot.brokers.live_clob import LiveClobBroker
from polymarket_bot.types import Signal


class _FakeOrderArgs:
    def __init__(self, token_id, price, size, side):
        self.token_id = token_id
        self.price = price
        self.size = size
        self.side = side


class _FakeOrderType:
    GTC = "GTC"


class _FakeClient:
    def __init__(self):
        self.last_order = None

    def create_order(self, order_args):
        self.last_order = order_args
        return order_args

    def post_order(self, signed, order_type):
        return {"orderID": "oid-demo", "status": "accepted"}


def _signal(side: str) -> Signal:
    return Signal(
        wallet="0x1111111111111111111111111111111111111111",
        market_slug="demo",
        token_id="token-1",
        outcome="YES",
        side=side,  # type: ignore[arg-type]
        confidence=0.8,
        price_hint=0.5,
        observed_size=1.0,
        observed_notional=1.0,
        timestamp=datetime.now(tz=timezone.utc),
    )


class LiveClobTests(unittest.TestCase):
    def test_execute_maps_sell_side(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker._OrderArgs = _FakeOrderArgs
        broker._OrderType = _FakeOrderType
        broker._side_map = {"BUY": "BUY_FLAG", "SELL": "SELL_FLAG"}
        broker.client = _FakeClient()

        result = broker.execute(_signal("SELL"), 10.0)

        self.assertTrue(result.ok)
        self.assertEqual(broker.client.last_order.side, "SELL_FLAG")
        self.assertEqual(result.broker_order_id, "oid-demo")

    def test_execute_rejects_unknown_side(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker._OrderArgs = _FakeOrderArgs
        broker._OrderType = _FakeOrderType
        broker._side_map = {"BUY": "BUY_FLAG", "SELL": "SELL_FLAG"}
        broker.client = _FakeClient()

        result = broker.execute(_signal("HOLD"), 10.0)

        self.assertFalse(result.ok)
        self.assertIn("unsupported side", result.message)


if __name__ == "__main__":
    unittest.main()
