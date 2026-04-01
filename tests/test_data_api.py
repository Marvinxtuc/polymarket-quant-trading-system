from __future__ import annotations

import io
import unittest
from typing import Any
import zipfile

import httpx

from polymarket_bot.clients.data_api import PolymarketDataClient, _is_retryable_http_error


class _StubDataClient(PolymarketDataClient):
    def __init__(self, responder):
        self.base_url = "https://data-api.polymarket.com"
        self.market_base_url = "https://clob.polymarket.com"
        self.gamma_base_url = "https://gamma-api.polymarket.com"
        self._responder = responder
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    def close(self) -> None:
        return None

    def _get_json_from_base(
        self,
        base_url: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> Any:
        normalized = dict(params or {})
        self.calls.append((base_url, path, normalized))
        return self._responder(base_url, path, normalized)

    def _get_bytes_from_base(
        self,
        base_url: str,
        path: str,
        params: dict[str, Any] | None = None,
    ) -> bytes:
        normalized = dict(params or {})
        self.calls.append((base_url, path, normalized))
        payload = self._responder(base_url, path, normalized)
        if isinstance(payload, bytes):
            return payload
        raise TypeError("Stub responder must return bytes for _get_bytes_from_base")


class DataApiClientTests(unittest.TestCase):
    def test_retryable_http_error_only_retries_transient_failures(self):
        request = httpx.Request("GET", "https://clob.polymarket.com/book")
        not_found = httpx.HTTPStatusError(
            "missing",
            request=request,
            response=httpx.Response(404, request=request),
        )
        too_many = httpx.HTTPStatusError(
            "rate limited",
            request=request,
            response=httpx.Response(429, request=request),
        )
        server_error = httpx.HTTPStatusError(
            "server error",
            request=request,
            response=httpx.Response(503, request=request),
        )
        timeout = httpx.ReadTimeout("timed out", request=request)

        self.assertFalse(_is_retryable_http_error(not_found))
        self.assertTrue(_is_retryable_http_error(too_many))
        self.assertTrue(_is_retryable_http_error(server_error))
        self.assertTrue(_is_retryable_http_error(timeout))

    def test_get_order_book_parses_tick_and_min_size(self):
        def responder(base_url: str, path: str, params: dict[str, Any]) -> Any:
            self.assertEqual(base_url, "https://clob.polymarket.com")
            self.assertEqual(path, "/book")
            self.assertEqual(params["token_id"], "token-1")
            return {
                "market": "demo-market",
                "asset_id": "token-1",
                "timestamp": "123456",
                "hash": "hash-1",
                "bids": [{"price": "0.48", "size": "120"}],
                "asks": [{"price": "0.52", "size": "95"}],
                "min_order_size": "5",
                "tick_size": "0.01",
                "neg_risk": True,
                "last_trade_price": "0.5",
            }

        client = _StubDataClient(responder)

        book = client.get_order_book("token-1")

        self.assertIsNotNone(book)
        assert book is not None
        self.assertEqual(book.market, "demo-market")
        self.assertEqual(book.asset_id, "token-1")
        self.assertEqual(book.best_bid, 0.48)
        self.assertEqual(book.best_ask, 0.52)
        self.assertEqual(book.tick_size, 0.01)
        self.assertEqual(book.min_order_size, 5.0)
        self.assertTrue(book.neg_risk)

    def test_get_midpoint_price_parses_market_response(self):
        def responder(base_url: str, path: str, params: dict[str, Any]) -> Any:
            self.assertEqual(base_url, "https://clob.polymarket.com")
            self.assertEqual(path, "/midpoint")
            self.assertEqual(params["token_id"], "token-1")
            return {"mid": "0.503"}

        client = _StubDataClient(responder)

        midpoint = client.get_midpoint_price("token-1")

        self.assertEqual(midpoint, 0.503)

    def test_get_market_metadata_parses_gamma_market_flags(self):
        def responder(base_url: str, path: str, params: dict[str, Any]) -> Any:
            self.assertEqual(base_url, "https://gamma-api.polymarket.com")
            self.assertEqual(path, "/markets")
            self.assertEqual(params["conditionId"], "condition-1")
            return [
                {
                    "conditionId": "condition-1",
                    "slug": "demo-market",
                    "endDate": "2026-03-22T12:34:56Z",
                    "closed": False,
                    "active": True,
                    "acceptingOrders": False,
                    "clobTokenIds": "[\"token-1\",\"token-2\"]",
                }
            ]

        client = _StubDataClient(responder)

        metadata = client.get_market_metadata("condition-1")

        self.assertIsNotNone(metadata)
        assert metadata is not None
        self.assertEqual(metadata.condition_id, "condition-1")
        self.assertEqual(metadata.market_slug, "demo-market")
        self.assertEqual(metadata.end_date, "2026-03-22T12:34:56Z")
        self.assertEqual(metadata.end_ts, 1774182896)
        self.assertFalse(metadata.closed)
        self.assertTrue(metadata.active)
        self.assertFalse(metadata.accepting_orders)
        self.assertEqual(metadata.token_ids, ("token-1", "token-2"))

    def test_get_accounting_snapshot_parses_zip_payload(self):
        def responder(base_url: str, path: str, params: dict[str, Any]) -> Any:
            self.assertEqual(base_url, "https://data-api.polymarket.com")
            self.assertEqual(path, "/v1/accounting/snapshot")
            self.assertEqual(params["user"], "0x1111111111111111111111111111111111111111")
            payload = io.BytesIO()
            with zipfile.ZipFile(payload, "w") as archive:
                archive.writestr(
                    "equity.csv",
                    (
                        "cashBalance,positionsValue,totalValue,valuationTime\n"
                        "120.5,45.5,166.0,2026-03-17T12:34:56Z\n"
                    ),
                )
                archive.writestr(
                    "positions.csv",
                    (
                        "asset,conditionId,size,curPrice,currentValue,valuationTime\n"
                        "token-1,condition-1,100,0.455,45.5,2026-03-17T12:34:56Z\n"
                    ),
                )
            return payload.getvalue()

        client = _StubDataClient(responder)

        snapshot = client.get_accounting_snapshot("0x1111111111111111111111111111111111111111")

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertAlmostEqual(snapshot.cash_balance, 120.5, places=4)
        self.assertAlmostEqual(snapshot.positions_value, 45.5, places=4)
        self.assertAlmostEqual(snapshot.equity, 166.0, places=4)
        self.assertEqual(snapshot.valuation_time, "2026-03-17T12:34:56Z")
        self.assertEqual(len(snapshot.positions), 1)
        self.assertEqual(snapshot.positions[0].token_id, "token-1")
        self.assertAlmostEqual(snapshot.positions[0].value, 45.5, places=4)

    def test_get_user_trades_parses_rows_and_requests_maker_fills(self):
        def responder(base_url: str, path: str, params: dict[str, Any]) -> Any:
            self.assertEqual(base_url, "https://data-api.polymarket.com")
            self.assertEqual(path, "/trades")
            self.assertFalse(params["takerOnly"])
            return [
                {
                    "proxyWallet": "0x1111111111111111111111111111111111111111",
                    "side": "BUY",
                    "asset": "token-1",
                    "conditionId": "condition-1",
                    "size": 25,
                    "price": 0.61,
                    "timestamp": 123456,
                    "slug": "market-1",
                    "outcome": "YES",
                    "transactionHash": "0xabc",
                }
            ]

        client = _StubDataClient(responder)

        fills = client.get_user_trades("0x1111111111111111111111111111111111111111")

        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0].wallet, "0x1111111111111111111111111111111111111111")
        self.assertEqual(fills[0].token_id, "token-1")
        self.assertEqual(fills[0].condition_id, "condition-1")
        self.assertEqual(fills[0].price, 0.61)
        self.assertEqual(fills[0].tx_hash, "0xabc")

    def test_iter_closed_positions_paginates_with_offset(self):
        pages = {
            0: [
                {
                    "proxyWallet": "0x1111111111111111111111111111111111111111",
                    "asset": "token-1",
                    "conditionId": "condition-1",
                    "slug": "market-1",
                    "outcome": "YES",
                    "avgPrice": 0.55,
                    "totalBought": 100,
                    "realizedPnl": 18,
                    "timestamp": 101,
                    "endDate": "2026-03-01T00:00:00Z",
                },
                {
                    "proxyWallet": "0x1111111111111111111111111111111111111111",
                    "asset": "token-2",
                    "conditionId": "condition-2",
                    "slug": "market-2",
                    "outcome": "NO",
                    "avgPrice": 0.45,
                    "totalBought": 80,
                    "realizedPnl": -4,
                    "timestamp": 102,
                    "endDate": "2026-03-02T00:00:00Z",
                },
            ],
            2: [
                {
                    "proxyWallet": "0x1111111111111111111111111111111111111111",
                    "asset": "token-3",
                    "conditionId": "condition-3",
                    "slug": "market-3",
                    "outcome": "YES",
                    "avgPrice": 0.38,
                    "totalBought": 60,
                    "realizedPnl": 12,
                    "timestamp": 103,
                    "endDate": "2026-03-03T00:00:00Z",
                }
            ],
        }

        def responder(base_url: str, path: str, params: dict[str, Any]) -> Any:
            self.assertEqual(base_url, "https://data-api.polymarket.com")
            self.assertEqual(path, "/closed-positions")
            return pages[params["offset"]]

        client = _StubDataClient(responder)

        positions = list(
            client.iter_closed_positions(
                "0x1111111111111111111111111111111111111111",
                page_size=2,
                max_pages=5,
            )
        )

        self.assertEqual(len(positions), 3)
        self.assertEqual([call[2]["offset"] for call in client.calls], [0, 2])
        self.assertEqual(positions[-1].condition_id, "condition-3")

    def test_build_resolution_map_prefers_gamma_market_lookup(self):
        def responder(base_url: str, path: str, params: dict[str, Any]) -> Any:
            self.assertEqual(path, "/markets")
            self.assertEqual(base_url, "https://gamma-api.polymarket.com")
            self.assertEqual(params["slug"], "market-3")
            return [
                {
                    "conditionId": "condition-3",
                    "closed": True,
                    "outcomes": "[\"YES\", \"NO\"]",
                    "outcomePrices": "[\"0\", \"1\"]",
                    "clobTokenIds": "[\"token-x\", \"token-y\"]",
                }
            ]

        client = _StubDataClient(responder)

        resolution_map = client.build_resolution_map(
            {"condition-3"},
            market_slugs={"condition-3": "market-3"},
        )

        self.assertEqual(set(resolution_map), {"condition-3"})
        self.assertEqual(resolution_map["condition-3"].winner_token_id, "token-y")
        self.assertEqual(resolution_map["condition-3"].winner_outcome, "NO")
        self.assertTrue(resolution_map["condition-3"].closed)
        self.assertEqual([(call[0], call[1]) for call in client.calls], [("https://gamma-api.polymarket.com", "/markets")])

    def test_build_resolution_map_extracts_winner_from_simplified_markets(self):
        def responder(base_url: str, path: str, params: dict[str, Any]) -> Any:
            if base_url == "https://gamma-api.polymarket.com":
                self.assertEqual(path, "/markets")
                return []

            self.assertEqual(base_url, "https://clob.polymarket.com")
            self.assertEqual(path, "/simplified-markets")
            if params.get("next_cursor") == "cursor-2":
                return {
                    "limit": 100,
                    "count": 1,
                    "next_cursor": "LTE=",
                    "data": [
                        {
                            "condition_id": "condition-3",
                            "closed": True,
                            "tokens": [
                                {"token_id": "token-x", "outcome": "YES", "winner": False},
                                {"token_id": "token-y", "outcome": "NO", "winner": True},
                            ],
                        }
                    ],
                }
            return {
                "limit": 100,
                "count": 2,
                "next_cursor": "cursor-2",
                "data": [
                    {
                        "condition_id": "condition-1",
                        "closed": True,
                        "tokens": [
                            {"token_id": "token-a", "outcome": "YES", "winner": True},
                            {"token_id": "token-b", "outcome": "NO", "winner": False},
                        ],
                    }
                ],
            }

        client = _StubDataClient(responder)

        resolution_map = client.build_resolution_map({"condition-3"})

        self.assertEqual(set(resolution_map), {"condition-3"})
        self.assertEqual(resolution_map["condition-3"].winner_token_id, "token-y")
        self.assertEqual(resolution_map["condition-3"].winner_outcome, "NO")
        self.assertTrue(resolution_map["condition-3"].closed)
        self.assertEqual(
            [(call[0], call[2].get("next_cursor"), call[2].get("conditionId")) for call in client.calls],
            [
                ("https://gamma-api.polymarket.com", None, "condition-3"),
                ("https://clob.polymarket.com", None, None),
                ("https://clob.polymarket.com", "cursor-2", None),
            ],
        )


if __name__ == "__main__":
    unittest.main()
