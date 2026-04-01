from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "live_clob_type2_smoke.py"
SPEC = importlib.util.spec_from_file_location("live_clob_type2_smoke", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"unable to load {MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class LiveClobType2SmokeTests(unittest.TestCase):
    def test_matched_size_from_order_accepts_snake_case_field(self) -> None:
        matched = MODULE._matched_size_from_order(
            {
                "id": "oid-demo",
                "status": "MATCHED",
                "size_matched": "12",
                "price": "0.084",
            }
        )

        self.assertAlmostEqual(matched, 12.0, places=6)

    def test_choose_aggressive_buy_size_aligns_to_cent_precision(self) -> None:
        size, notional = MODULE._choose_aggressive_buy_size(
            0.085,
            min_size=5.0,
            target_usd=1.0,
            max_usd=2.0,
        )

        self.assertAlmostEqual(size, 12.0, places=2)
        self.assertAlmostEqual(notional, 1.02, places=2)

    def test_choose_aggressive_buy_size_raises_when_min_size_exceeds_cap(self) -> None:
        with self.assertRaises(SystemExit) as ctx:
            MODULE._choose_aggressive_buy_size(
                0.51,
                min_size=5.0,
                target_usd=1.0,
                max_usd=2.0,
            )

        self.assertIn("cannot fit aggressive BUY within", str(ctx.exception))

    def test_post_aggressive_sell_retries_retryable_balance_error(self) -> None:
        snapshot = {
            "best_bid": 0.084,
            "best_bid_size": 100.0,
            "best_ask": 0.085,
            "best_ask_size": 100.0,
            "midpoint": 0.0,
            "min_order_size": 5.0,
            "neg_risk": True,
            "tick_size": 0.001,
            "tick_text": "0.001",
        }

        with (
            patch.object(MODULE, "_book_snapshot", return_value=snapshot),
            patch.object(MODULE, "_order_options", return_value=object()),
            patch.object(
                MODULE,
                "_post_limit_order",
                side_effect=[
                    Exception("PolyApiException[status_code=400, error_message={'error': 'not enough balance / allowance'}]"),
                    ("oid-sell", {"status": "matched"}),
                ],
            ) as post_order,
            patch.object(MODULE.time, "sleep") as sleep_mock,
        ):
            order_id, payload, price, size = MODULE._post_aggressive_sell_with_retries(
                object(),
                token_id="token-demo",
                filled_buy_size=12.0,
                sleep_seconds=0.5,
                attempts=3,
            )

        self.assertEqual(order_id, "oid-sell")
        self.assertEqual(payload["status"], "matched")
        self.assertAlmostEqual(price, 0.084, places=6)
        self.assertAlmostEqual(size, 12.0, places=6)
        self.assertEqual(post_order.call_count, 2)
        sleep_mock.assert_called_once()

    def test_post_aggressive_sell_does_not_retry_non_retryable_error(self) -> None:
        snapshot = {
            "best_bid": 0.084,
            "best_bid_size": 100.0,
            "best_ask": 0.085,
            "best_ask_size": 100.0,
            "midpoint": 0.0,
            "min_order_size": 5.0,
            "neg_risk": True,
            "tick_size": 0.001,
            "tick_text": "0.001",
        }

        with (
            patch.object(MODULE, "_book_snapshot", return_value=snapshot),
            patch.object(MODULE, "_order_options", return_value=object()),
            patch.object(
                MODULE,
                "_post_limit_order",
                side_effect=RuntimeError("upstream 500"),
            ) as post_order,
            patch.object(MODULE.time, "sleep") as sleep_mock,
        ):
            with self.assertRaises(RuntimeError):
                MODULE._post_aggressive_sell_with_retries(
                    object(),
                    token_id="token-demo",
                    filled_buy_size=12.0,
                    sleep_seconds=0.5,
                    attempts=3,
                )

        self.assertEqual(post_order.call_count, 1)
        sleep_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
