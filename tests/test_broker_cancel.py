from __future__ import annotations

import unittest

from polymarket_bot.brokers.live_clob import LiveClobBroker
from polymarket_bot.brokers.paper import PaperBroker
from polymarket_bot.types import OpenOrderSnapshot


class _DeleteOrderClient:
    def __init__(self):
        self.calls: list[tuple[str, str]] = []

    def delete_order(self, order_id):
        self.calls.append(("delete_order", str(order_id)))
        return {"id": order_id, "status": "cancelled", "message": "deleted"}


class _CancelAllOrdersClient:
    def __init__(self):
        self.calls: list[tuple[str, tuple[str, ...]]] = []

    def cancel_all_orders(self, order_ids):
        cleaned = tuple(str(order_id) for order_id in order_ids)
        self.calls.append(("cancel_all_orders", cleaned))
        return {"canceledOrderIDs": list(cleaned), "status": "canceled", "message": "batch canceled"}


class _CancelOpenOrdersClient:
    def __init__(self):
        self.calls: list[str] = []

    def cancel_open_orders(self):
        self.calls.append("cancel_open_orders")
        return True


class BrokerCancelTests(unittest.TestCase):
    def test_live_cancel_order_uses_delete_order_alias(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker.client = _DeleteOrderClient()

        result = broker.cancel_order("oid-1")

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result["order_id"], "oid-1")
        self.assertEqual(result["status"], "canceled")
        self.assertTrue(result["ok"])
        self.assertEqual(broker.client.calls, [("delete_order", "oid-1")])

    def test_live_cancel_orders_uses_cancel_all_orders_alias(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker.client = _CancelAllOrdersClient()

        results = broker.cancel_orders(["oid-a", "oid-b"])

        self.assertIsNotNone(results)
        assert results is not None
        self.assertEqual(len(results), 2)
        self.assertEqual([row["order_id"] for row in results], ["oid-a", "oid-b"])
        self.assertTrue(all(row["status"] == "canceled" for row in results))
        self.assertEqual(broker.client.calls, [("cancel_all_orders", ("oid-a", "oid-b"))])

    def test_live_cancel_open_orders_falls_back_to_cancel_open_orders_alias(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker.client = _CancelOpenOrdersClient()
        broker.list_open_orders = lambda: [
            OpenOrderSnapshot(
                order_id="oid-a",
                token_id="token-a",
                side="BUY",
                status="live",
                price=0.52,
                original_size=10.0,
                matched_size=0.0,
                remaining_size=10.0,
            ),
            OpenOrderSnapshot(
                order_id="oid-b",
                token_id="token-b",
                side="SELL",
                status="live",
                price=0.48,
                original_size=5.0,
                matched_size=0.0,
                remaining_size=5.0,
            ),
        ]

        results = broker.cancel_open_orders()

        self.assertIsNotNone(results)
        assert results is not None
        self.assertEqual(len(results), 2)
        self.assertEqual([row["order_id"] for row in results], ["oid-a", "oid-b"])
        self.assertTrue(all(row["status"] == "canceled" for row in results))
        self.assertEqual(broker.client.calls, ["cancel_open_orders"])

    def test_paper_cancel_methods_are_safe_and_simulated(self):
        broker = PaperBroker()

        cancel_one = broker.cancel_order("oid-paper")
        cancel_many = broker.cancel_orders(["oid-a", " ", "oid-b"])
        cancel_open = broker.cancel_open_orders()

        self.assertIsNotNone(cancel_one)
        assert cancel_one is not None
        self.assertEqual(cancel_one["status"], "canceled")
        self.assertTrue(cancel_one["ok"])
        self.assertEqual(cancel_one["order_id"], "oid-paper")
        self.assertEqual([row["order_id"] for row in cancel_many or []], ["oid-a", "oid-b"])
        self.assertEqual(cancel_open, [])


if __name__ == "__main__":
    unittest.main()
