import tempfile
import time
import unittest
from datetime import datetime, timezone

from polymarket_bot.state_store import StateStore
from polymarket_bot.runner import Trader
from polymarket_bot.types import Signal


def _signal(side: str = "BUY", token_id: str = "t1", wallet: str = "w1", condition_id: str = "c1") -> Signal:
    now = datetime.now(tz=timezone.utc)
    return Signal(
        signal_id="sig-1",
        trace_id="tr-1",
        wallet=wallet,
        market_slug="m1",
        token_id=token_id,
        outcome="YES",
        side=side,
        confidence=1.0,
        price_hint=0.5,
        observed_size=1.0,
        observed_notional=1.0,
        timestamp=now,
        condition_id=condition_id,
    )


class IdempotencyPersistenceTests(unittest.TestCase):
    def test_state_store_idempotency_claim_once(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = StateStore(f"{tmp}/state.db")
            claimed = store.register_idempotency(
                strategy_order_uuid="uuid-1",
                wallet="w1",
                condition_id="c1",
                token_id="t1",
                side="BUY",
                notional=10.0,
                created_ts=int(time.time()),
            )
            self.assertTrue(claimed)
            self.assertTrue(store.idempotency_exists("uuid-1"))
            claimed_again = store.register_idempotency(
                strategy_order_uuid="uuid-1",
                wallet="w1",
                condition_id="c1",
                token_id="t1",
                side="BUY",
                notional=10.0,
                created_ts=int(time.time()),
            )
            self.assertFalse(claimed_again)

    def test_state_store_idempotency_cleanup_removes_old_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = StateStore(f"{tmp}/state.db")
            old_ts = int(time.time()) - 200
            store.register_idempotency(
                strategy_order_uuid="old-uuid",
                wallet="w1",
                condition_id="c1",
                token_id="t1",
                side="BUY",
                notional=5.0,
                created_ts=old_ts,
            )
            removed = store.cleanup_idempotency(window_seconds=100)
            self.assertGreaterEqual(removed, 1)
            self.assertFalse(store.idempotency_exists("old-uuid"))

    def test_strategy_order_uuid_is_deterministic_and_differs_for_notional(self):
        sig = _signal()
        uuid1 = Trader._strategy_order_uuid(sig, 10.0)
        uuid2 = Trader._strategy_order_uuid(sig, 10.0)
        uuid3 = Trader._strategy_order_uuid(sig, 11.0)
        self.assertEqual(uuid1, uuid2)
        self.assertNotEqual(uuid1, uuid3)


if __name__ == "__main__":
    unittest.main()
