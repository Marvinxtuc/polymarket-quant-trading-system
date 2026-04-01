from __future__ import annotations

import json
import os
import tempfile
import unittest

from polymarket_bot.clients.data_api import ClosedPosition, ResolvedMarket
from polymarket_bot.wallet_history import WalletHistoryStore


class _HistoryClient:
    def __init__(self) -> None:
        self.closed_calls: list[tuple[str, int]] = []
        self.resolution_calls: list[tuple[set[str], dict[str, str] | None]] = []

    def get_closed_positions(self, wallet: str, *, limit: int = 50, **_kwargs):
        self.closed_calls.append((wallet, limit))
        if wallet.endswith("2"):
            return [
                ClosedPosition(
                    wallet=wallet,
                    token_id="token-2a",
                    condition_id="condition-2a",
                    market_slug="fed-cut-rates-in-june",
                    outcome="YES",
                    avg_price=0.51,
                    total_bought=120.0,
                    realized_pnl=24.0,
                    timestamp=2,
                    end_date="2026-03-02T00:00:00Z",
                )
            ]
        return [
            ClosedPosition(
                wallet=wallet,
                token_id="token-1a",
                condition_id="condition-1a",
                market_slug="will-btc-close-above-100k",
                outcome="YES",
                avg_price=0.48,
                total_bought=100.0,
                realized_pnl=20.0,
                timestamp=1,
                end_date="2026-03-01T00:00:00Z",
            ),
            ClosedPosition(
                wallet=wallet,
                token_id="token-1b",
                condition_id="condition-1b",
                market_slug="will-trump-win-2028",
                outcome="NO",
                avg_price=0.52,
                total_bought=80.0,
                realized_pnl=-10.0,
                timestamp=1,
                end_date="2026-03-01T00:00:00Z",
            ),
        ]

    def build_resolution_map(self, condition_ids: set[str], *, market_slugs=None, **_kwargs):
        self.resolution_calls.append((set(condition_ids), dict(market_slugs or {})))
        mapping: dict[str, ResolvedMarket] = {}
        for condition_id in condition_ids:
            if condition_id == "condition-1a":
                mapping[condition_id] = ResolvedMarket(
                    condition_id=condition_id,
                    winner_token_id="token-1a",
                    winner_outcome="YES",
                    closed=True,
                )
            elif condition_id == "condition-1b":
                mapping[condition_id] = ResolvedMarket(
                    condition_id=condition_id,
                    winner_token_id="token-x",
                    winner_outcome="YES",
                    closed=True,
                )
            elif condition_id == "condition-2a":
                mapping[condition_id] = ResolvedMarket(
                    condition_id=condition_id,
                    winner_token_id="token-2a",
                    winner_outcome="YES",
                    closed=True,
                )
        return mapping


class WalletHistoryStoreTests(unittest.TestCase):
    def test_sync_wallets_fetches_history_and_reuses_cache(self):
        client = _HistoryClient()
        with tempfile.TemporaryDirectory() as tmpdir:
            cache_path = os.path.join(tmpdir, "wallet_history.json")
            store = WalletHistoryStore(
                client=client,
                cache_path=cache_path,
                refresh_seconds=3600,
                max_wallets=2,
                closed_limit=10,
                resolution_limit=4,
            )

            metrics, refreshed_ts, recent_closed_markets, topic_profiles = store.sync_wallets(
                [
                    "0x0000000000000000000000000000000000000001",
                    "0x0000000000000000000000000000000000000002",
                    "0x0000000000000000000000000000000000000003",
                ]
            )

            self.assertEqual(set(metrics), {
                "0x0000000000000000000000000000000000000001",
                "0x0000000000000000000000000000000000000002",
            })
            self.assertEqual(len(client.closed_calls), 2)
            self.assertEqual(len(client.resolution_calls), 2)
            self.assertEqual(metrics["0x0000000000000000000000000000000000000001"].closed_positions, 2)
            self.assertAlmostEqual(metrics["0x0000000000000000000000000000000000000001"].win_rate, 0.5, places=4)
            self.assertAlmostEqual(metrics["0x0000000000000000000000000000000000000001"].resolved_win_rate, 0.5, places=4)
            self.assertIn("0x0000000000000000000000000000000000000001", refreshed_ts)
            self.assertEqual(len(recent_closed_markets["0x0000000000000000000000000000000000000001"]), 2)
            self.assertEqual(
                recent_closed_markets["0x0000000000000000000000000000000000000001"][0]["market_slug"],
                "will-btc-close-above-100k",
            )
            self.assertTrue(
                recent_closed_markets["0x0000000000000000000000000000000000000001"][0]["resolved_correct"]
            )
            labels = {row["label"] for row in topic_profiles["0x0000000000000000000000000000000000000001"]}
            self.assertEqual(labels, {"政治", "加密"})

            self.assertTrue(os.path.exists(cache_path))
            with open(cache_path, "r", encoding="utf-8") as f:
                payload = json.load(f)
            self.assertEqual(len(payload["wallets"]), 2)
            self.assertIn("recent_closed_markets", payload["wallets"][0])
            self.assertIn("topic_profiles", payload["wallets"][0])

            cached_client = _HistoryClient()
            cached_store = WalletHistoryStore(
                client=cached_client,
                cache_path=cache_path,
                refresh_seconds=3600,
                max_wallets=2,
                closed_limit=10,
                resolution_limit=4,
            )
            cached_metrics, _, cached_recent, cached_topics = cached_store.sync_wallets(
                [
                    "0x0000000000000000000000000000000000000001",
                    "0x0000000000000000000000000000000000000002",
                ]
            )

            self.assertEqual(len(cached_client.closed_calls), 0)
            self.assertAlmostEqual(
                cached_metrics["0x0000000000000000000000000000000000000001"].roi,
                metrics["0x0000000000000000000000000000000000000001"].roi,
                places=4,
            )
            self.assertEqual(
                cached_recent["0x0000000000000000000000000000000000000001"][1]["market_slug"],
                "will-trump-win-2028",
            )
            self.assertEqual(cached_topics["0x0000000000000000000000000000000000000002"][0]["label"], "宏观")
            peek_metrics, peek_refreshed, _, peek_topics = cached_store.peek_wallets(
                [
                    "0x0000000000000000000000000000000000000002",
                    "0x0000000000000000000000000000000000000003",
                ]
            )
            self.assertIn("0x0000000000000000000000000000000000000002", peek_metrics)
            self.assertNotIn("0x0000000000000000000000000000000000000003", peek_metrics)
            self.assertIn("0x0000000000000000000000000000000000000002", peek_refreshed)
            self.assertEqual(peek_topics["0x0000000000000000000000000000000000000002"][0]["label"], "宏观")


if __name__ == "__main__":
    unittest.main()
