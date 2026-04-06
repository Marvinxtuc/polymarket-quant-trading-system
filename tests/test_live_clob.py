from __future__ import annotations

import os
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from polymarket_bot.brokers.live_clob import LiveClobBroker, _configure_sdk_http_timeout
from polymarket_bot.signer_client import SignerClientError, SignerHealthSnapshot
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
        self.last_signed = None
        self.heartbeat_payloads = []
        self.open_orders_payload = []
        self.trades_payload = []

    def create_order(self, order_args):
        self.last_order = order_args
        return order_args

    def post_order(self, signed, order_type):
        self.last_signed = signed
        return {"orderID": "oid-demo", "status": "live"}

    def get_order(self, order_id):
        return {
            "orderID": order_id,
            "status": "live",
            "matched_amount": "4.5",
            "matchedPrice": "0.48",
        }

    def heartbeat(self, order_ids):
        self.heartbeat_payloads.append(list(order_ids))

    def get_open_orders(self):
        return list(self.open_orders_payload)

    def get_trades(self):
        return list(self.trades_payload)


class _FakePartialCreateOrderOptions:
    def __init__(self, *, tick_size, neg_risk):
        self.tick_size = tick_size
        self.neg_risk = neg_risk


class _FakeSignerClient:
    def __init__(self):
        self.payloads = []

    def sign_order(self, payload):
        self.payloads.append(dict(payload))
        return {"signed_payload": dict(payload)}


class _SignerClientRaises:
    def sign_order(self, payload):  # noqa: ANN001
        raise SignerClientError("signer unavailable", reason_code="signer_sign_order_failed")


class _FakeMarketClient:
    def __init__(self, *, best_bid=0.47, best_ask=0.53, tick_size=0.01, min_order_size=1.0, midpoint=0.5):
        self.book = SimpleNamespace(
            best_bid=best_bid,
            best_ask=best_ask,
            tick_size=tick_size,
            min_order_size=min_order_size,
            neg_risk=False,
            last_trade_price=midpoint,
        )
        self.midpoint = midpoint

    def get_order_book(self, token_id):
        return self.book

    def get_midpoint_price(self, token_id):
        return self.midpoint


class _FakeUserStream:
    def __init__(self, events):
        self.events = list(events)
        self.calls = []
        self.closed = False

    def events_since(self, *, since_ts=0, order_ids=None, limit=200):
        self.calls.append((since_ts, list(order_ids or []), limit))
        return list(self.events)

    def close(self):
        self.closed = True


def _signal(side: str, *, price_hint: float = 0.5) -> Signal:
    return Signal(
        signal_id="",
        trace_id="",
        wallet="0x1111111111111111111111111111111111111111",
        market_slug="demo",
        token_id="token-1",
        outcome="YES",
        side=side,  # type: ignore[arg-type]
        confidence=0.8,
        price_hint=price_hint,
        observed_size=1.0,
        observed_notional=1.0,
        timestamp=datetime.now(tz=timezone.utc),
    )


class LiveClobTests(unittest.TestCase):
    def test_configure_sdk_http_timeout_sets_default_timeout(self):
        import httpx
        from py_clob_client.http_helpers import helpers as sdk_helpers

        original_client = sdk_helpers._http_client
        sdk_helpers._http_client = httpx.Client(http2=True)
        try:
            _configure_sdk_http_timeout(7.0)
            configured = sdk_helpers._http_client
            self.assertIsInstance(configured.timeout, httpx.Timeout)
            self.assertIsNotNone(configured.timeout.connect)
            self.assertIsNotNone(configured.timeout.read)
            self.assertIsNotNone(configured.timeout.write)
            self.assertIsNotNone(configured.timeout.pool)
            self.assertLessEqual(float(configured.timeout.connect), 7.0)
            self.assertLessEqual(float(configured.timeout.read), 7.0)
            self.assertLessEqual(float(configured.timeout.write), 7.0)
            self.assertLessEqual(float(configured.timeout.pool), 7.0)
        finally:
            try:
                sdk_helpers._http_client.close()
            except Exception:
                pass
            sdk_helpers._http_client = original_client

    def test_create_and_post_order_uses_external_signer_payload(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker.client = _FakeClient()
        broker._OrderType = _FakeOrderType
        broker._chain_id = 137
        broker._funder = "0xabc"
        broker._signer_client = _FakeSignerClient()
        order_args = _FakeOrderArgs(token_id="token-1", price=0.44, size=2.0, side="BUY")

        response, submit_metadata = broker._create_and_post_order(order_args, tick_size=0.01, neg_risk=True)

        self.assertEqual(response["orderID"], "oid-demo")
        self.assertEqual(len(broker._signer_client.payloads), 1)
        payload = broker._signer_client.payloads[0]
        self.assertEqual(payload["token_id"], "token-1")
        self.assertEqual(payload["side"], "BUY")
        self.assertEqual(payload["tick_size"], 0.01)
        self.assertEqual(payload["chain_id"], 137)
        self.assertEqual(payload["funder_address"], "0xabc")
        self.assertEqual(broker.client.last_signed, {"signed_payload": payload})
        self.assertEqual(float(submit_metadata["submitted_price"]), 0.44)
        self.assertEqual(float(submit_metadata["submitted_size"]), 2.0)
        self.assertEqual(float(submit_metadata["tick_size"]), 0.01)
        self.assertEqual(str(submit_metadata["submit_digest_version"]), "sdig-v1")
        self.assertTrue(bool(str(submit_metadata["submit_digest"])))

    def test_create_and_post_order_wraps_unexpected_signer_exception(self):
        class _BrokenSigner:
            def sign_order(self, payload):
                raise ValueError(f"boom:{payload.get('token_id')}")

        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker.client = _FakeClient()
        broker._OrderType = _FakeOrderType
        broker._chain_id = 137
        broker._funder = "0xabc"
        broker._signer_client = _BrokenSigner()
        order_args = _FakeOrderArgs(token_id="token-err", price=0.44, size=2.0, side="BUY")

        with self.assertRaises(SignerClientError) as ctx:
            broker._create_and_post_order(order_args, tick_size=0.005, neg_risk=False)

        self.assertEqual(ctx.exception.reason_code, "signer_sign_order_failed")

    def test_execute_signer_error_is_fail_closed_without_local_key_fallback(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker._OrderArgs = _FakeOrderArgs
        broker._OrderType = _FakeOrderType
        broker._side_map = {"BUY": "BUY_FLAG", "SELL": "SELL_FLAG"}
        broker.client = _FakeClient()
        broker.market_client = _FakeMarketClient()
        broker.maker_buffer_ticks = 1
        broker._chain_id = 137
        broker._funder = "0xabc"
        broker._signer_client = _SignerClientRaises()

        result = broker.execute(_signal("BUY"), 10.0)

        self.assertFalse(result.ok)
        self.assertEqual(result.status, "error")
        self.assertIn("live signer unavailable", result.message)
        self.assertEqual(result.metadata.get("reason_code"), "signer_sign_order_failed")
        self.assertTrue(bool(result.metadata.get("security_fail_close")))
        self.assertEqual(result.broker_order_id, None)

    def test_validate_identity_binding_rejects_mismatched_identities(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker._funder = "0xabc123"
        broker._signer_health = SimpleNamespace(
            healthy=True,
            signer_identity="0xdef456",
            api_identity="0xabc123",
            reason_code="",
            message="ok",
        )
        broker._security_reason_codes = []

        with self.assertRaises(RuntimeError):
            broker._validate_identity_binding()

        self.assertIn("signer_identity_mismatch", broker._security_reason_codes)

    def test_execute_maps_sell_side(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker._OrderArgs = _FakeOrderArgs
        broker._OrderType = _FakeOrderType
        broker._side_map = {"BUY": "BUY_FLAG", "SELL": "SELL_FLAG"}
        broker.client = _FakeClient()
        broker.market_client = _FakeMarketClient()
        broker.maker_buffer_ticks = 1
        broker._chain_id = 137
        broker._funder = "0xabc"
        broker._signer_client = _FakeSignerClient()

        result = broker.execute(_signal("SELL"), 10.0)

        self.assertTrue(result.ok)
        self.assertEqual(broker._signer_client.payloads[-1]["side"], "SELL_FLAG")
        self.assertEqual(result.broker_order_id, "oid-demo")
        self.assertTrue(result.is_pending)
        self.assertEqual(result.filled_notional, 0.0)
        self.assertEqual(result.normalized_status, "live")
        self.assertEqual(result.lifecycle_status, "live")

    def test_execute_rejects_unknown_side(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker._OrderArgs = _FakeOrderArgs
        broker._OrderType = _FakeOrderType
        broker._side_map = {"BUY": "BUY_FLAG", "SELL": "SELL_FLAG"}
        broker.client = _FakeClient()
        broker.market_client = _FakeMarketClient()
        broker.maker_buffer_ticks = 1

        result = broker.execute(_signal("HOLD"), 10.0)

        self.assertFalse(result.ok)
        self.assertIn("unsupported side", result.message)

    def test_execute_marks_matched_orders_as_filled(self):
        class _MatchedClient(_FakeClient):
            def post_order(self, signed, order_type):
                return {"orderID": "oid-filled", "status": "matched"}

        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker._OrderArgs = _FakeOrderArgs
        broker._OrderType = _FakeOrderType
        broker._side_map = {"BUY": "BUY_FLAG", "SELL": "SELL_FLAG"}
        broker.client = _MatchedClient()
        broker.market_client = _FakeMarketClient()
        broker.maker_buffer_ticks = 1
        broker._chain_id = 137
        broker._funder = "0xabc"
        broker._signer_client = _FakeSignerClient()

        result = broker.execute(_signal("BUY", price_hint=0.54), 10.0)

        self.assertTrue(result.ok)
        self.assertTrue(result.has_fill)
        self.assertEqual(result.filled_notional, 10.0)
        self.assertEqual(result.filled_price, 0.5)
        self.assertEqual(result.normalized_status, "matched")
        self.assertEqual(result.lifecycle_status, "filled")

    def test_execute_rounds_buy_price_below_best_ask_tick(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker._OrderArgs = _FakeOrderArgs
        broker._OrderType = _FakeOrderType
        broker._side_map = {"BUY": "BUY_FLAG", "SELL": "SELL_FLAG"}
        broker.client = _FakeClient()
        broker.market_client = _FakeMarketClient(best_bid=0.52, best_ask=0.53, tick_size=0.01, midpoint=0.525)
        broker.maker_buffer_ticks = 1
        broker._chain_id = 137
        broker._funder = "0xabc"
        broker._signer_client = _FakeSignerClient()

        result = broker.execute(_signal("BUY", price_hint=0.54), 10.0)

        self.assertTrue(result.ok)
        payload = broker._signer_client.payloads[-1]
        self.assertEqual(payload["price"], 0.52)
        self.assertAlmostEqual(float(payload["size"]), 10.0 / 0.52, places=6)
        self.assertEqual(result.requested_price, 0.52)
        self.assertTrue(bool(result.metadata.get("preflight_has_book")))
        self.assertAlmostEqual(float(result.metadata.get("best_bid") or 0.0), 0.52, places=4)
        self.assertAlmostEqual(float(result.metadata.get("best_ask") or 0.0), 0.53, places=4)
        self.assertAlmostEqual(float(result.metadata.get("midpoint") or 0.0), 0.525, places=4)
        self.assertAlmostEqual(float(result.metadata.get("market_spread_bps") or 0.0), 190.4761, places=3)
        self.assertAlmostEqual(float(result.metadata.get("requested_vs_mid_bps") or 0.0), -95.2381, places=3)

    def test_execute_rejects_below_min_order_size(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker._OrderArgs = _FakeOrderArgs
        broker._OrderType = _FakeOrderType
        broker._side_map = {"BUY": "BUY_FLAG", "SELL": "SELL_FLAG"}
        broker.client = _FakeClient()
        broker.market_client = _FakeMarketClient(min_order_size=50.0)
        broker.maker_buffer_ticks = 1

        result = broker.execute(_signal("BUY"), 10.0)

        self.assertFalse(result.ok)
        self.assertIn("below minimum", result.message)
        self.assertTrue(bool(result.metadata.get("preflight_has_book")))
        self.assertGreater(float(result.metadata.get("market_spread_bps") or 0.0), 0.0)

    def test_get_order_status_parses_fill_fields(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker.client = _FakeClient()

        snapshot = broker.get_order_status("oid-demo")

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.order_id, "oid-demo")
        self.assertEqual(snapshot.normalized_status, "live")
        self.assertEqual(snapshot.lifecycle_status, "partially_filled")
        self.assertAlmostEqual(snapshot.matched_size, 4.5, places=6)
        self.assertAlmostEqual(snapshot.avg_fill_price, 0.48, places=6)

    def test_get_order_status_accepts_snake_case_size_matched(self):
        class _SnakeCaseClient(_FakeClient):
            def get_order(self, order_id):
                return {
                    "id": order_id,
                    "status": "MATCHED",
                    "original_size": "12",
                    "size_matched": "12",
                    "price": "0.084",
                }

        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker.client = _SnakeCaseClient()

        snapshot = broker.get_order_status("oid-snake")

        self.assertIsNotNone(snapshot)
        assert snapshot is not None
        self.assertEqual(snapshot.order_id, "oid-snake")
        self.assertEqual(snapshot.normalized_status, "matched")
        self.assertEqual(snapshot.lifecycle_status, "filled")
        self.assertAlmostEqual(snapshot.matched_size, 12.0, places=6)
        self.assertAlmostEqual(snapshot.remaining_size, 0.0, places=6)

    def test_heartbeat_forwards_pending_order_ids(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker.client = _FakeClient()

        result = broker.heartbeat(["oid-a", "", "oid-b"])

        self.assertTrue(result)
        self.assertEqual(broker.client.heartbeat_payloads, [["oid-a", "oid-b"]])

    def test_list_open_orders_parses_partial_fill_state(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker.client = _FakeClient()
        broker._funder = "0xabc"
        broker.client.open_orders_payload = [
            {
                "orderID": "oid-open",
                "asset_id": "token-1",
                "side": "BUY",
                "status": "live",
                "price": "0.50",
                "originalSize": "20",
                "sizeMatched": "5",
                "createdAt": "1700000000",
                "slug": "demo",
                "outcome": "YES",
            }
        ]

        orders = broker.list_open_orders()

        self.assertIsNotNone(orders)
        assert orders is not None
        self.assertEqual(len(orders), 1)
        self.assertEqual(orders[0].order_id, "oid-open")
        self.assertEqual(orders[0].lifecycle_status, "partially_filled")
        self.assertAlmostEqual(orders[0].matched_notional, 2.5, places=6)
        self.assertAlmostEqual(orders[0].requested_notional, 10.0, places=6)

    def test_list_recent_fills_parses_owner_trade_rows(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker.client = _FakeClient()
        broker._funder = "0xmaker"
        broker.client.trades_payload = [
            {
                "makerOrderID": "oid-maker",
                "makerAddress": "0xmaker",
                "asset_id": "token-1",
                "side": "BUY",
                "price": "0.52",
                "size": "7",
                "timestamp": "1700000010",
                "transactionHash": "0xfill",
                "slug": "demo",
                "outcome": "YES",
            }
        ]

        fills = broker.list_recent_fills(since_ts=1700000000, order_ids=["oid-maker"])

        self.assertIsNotNone(fills)
        assert fills is not None
        self.assertEqual(len(fills), 1)
        self.assertEqual(fills[0].order_id, "oid-maker")
        self.assertAlmostEqual(fills[0].notional, 3.64, places=6)
        self.assertEqual(fills[0].tx_hash, "0xfill")

    def test_list_order_events_combines_fills_and_status_snapshots(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker.client = _FakeClient()
        broker._funder = "0xmaker"
        broker._user_stream = None
        broker.client.trades_payload = [
            {
                "makerOrderID": "oid-maker",
                "makerAddress": "0xmaker",
                "asset_id": "token-1",
                "side": "BUY",
                "price": "0.52",
                "size": "7",
                "timestamp": "1700000010",
                "transactionHash": "0xfill",
                "slug": "demo",
                "outcome": "YES",
            }
        ]

        events = broker.list_order_events(since_ts=1700000000, order_ids=["oid-maker"], limit=20)

        self.assertIsNotNone(events)
        assert events is not None
        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].event_type, "fill")
        self.assertEqual(events[0].order_id, "oid-maker")
        self.assertEqual(events[1].event_type, "status")
        self.assertEqual(events[1].status, "partially_filled")

    def test_parse_user_stream_trade_message_supports_snake_case_order_ids(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker._funder = "0xtaker"

        events = broker._parse_user_stream_message(
            {
                "event_type": "trade",
                "status": "MATCHED",
                "taker_order_id": "oid-taker",
                "taker_address": "0xtaker",
                "asset_id": "token-1",
                "side": "BUY",
                "price": "0.55",
                "size": "10",
                "timestamp": "1700000030",
                "transactionHash": "0xhash",
                "slug": "demo",
                "outcome": "YES",
            }
        )

        self.assertEqual(len(events), 2)
        self.assertEqual(events[0].event_type, "fill")
        self.assertEqual(events[0].order_id, "oid-taker")
        self.assertEqual(events[1].event_type, "status")
        self.assertEqual(events[1].status, "matched")

    def test_list_order_events_merges_user_stream_and_dedupes_duplicate_fill(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker.client = _FakeClient()
        broker._funder = "0xmaker"
        broker._user_stream = _FakeUserStream(
            [
                broker._parse_user_stream_message(
                    {
                        "event_type": "trade",
                        "status": "MATCHED",
                        "maker_order_id": "oid-maker",
                        "maker_address": "0xmaker",
                        "asset_id": "token-1",
                        "side": "BUY",
                        "price": "0.52",
                        "size": "7",
                        "timestamp": "1700000010",
                        "transactionHash": "0xfill",
                        "slug": "demo",
                        "outcome": "YES",
                    }
                )[0]
            ]
        )
        broker.client.trades_payload = [
            {
                "makerOrderID": "oid-maker",
                "makerAddress": "0xmaker",
                "asset_id": "token-1",
                "side": "BUY",
                "price": "0.52",
                "size": "7",
                "timestamp": "1700000010",
                "transactionHash": "0xfill",
                "slug": "demo",
                "outcome": "YES",
            }
        ]

        events = broker.list_order_events(since_ts=1700000000, order_ids=["oid-maker"], limit=20)

        self.assertIsNotNone(events)
        assert events is not None
        self.assertEqual(len(events), 2)
        self.assertEqual(sum(1 for event in events if event.event_type == "fill"), 1)
        self.assertEqual(sum(1 for event in events if event.event_type == "status"), 1)
        self.assertEqual(broker._user_stream.calls[0][1], ["oid-maker"])

    def test_close_shuts_down_user_stream(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker._user_stream = _FakeUserStream([])

        broker.close()

        self.assertTrue(broker._user_stream.closed)

    def test_startup_checks_surface_user_stream_warning_and_api_pass(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker.client = _FakeClient()
        broker.market_client = _FakeMarketClient()
        broker._host = "https://clob.polymarket.com"
        broker._chain_id = 137
        broker._signature_type = 0
        broker._funder = "0xabc"
        broker._api_creds = {"apiKey": "k", "secret": "s", "passphrase": "p"}
        broker._user_stream_enabled = True
        broker._user_stream_url = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

        previous = {key: os.environ.get(key) for key in ("LIVE_ALLOWANCE_READY", "LIVE_GEOBLOCK_READY", "LIVE_ACCOUNT_READY")}
        os.environ["LIVE_ALLOWANCE_READY"] = "true"
        os.environ["LIVE_GEOBLOCK_READY"] = "true"
        os.environ["LIVE_ACCOUNT_READY"] = "true"
        try:
            with patch.dict(sys.modules, {"websocket": SimpleNamespace(__name__="websocket")}):
                checks = broker.startup_checks()
        finally:
            for key, value in previous.items():
                if value is None:
                    os.environ.pop(key, None)
                else:
                    os.environ[key] = value

        assert checks is not None
        index = {str(row["name"]): row for row in checks}
        self.assertEqual(index["api_credentials"]["status"], "PASS")
        self.assertEqual(index["market_preflight"]["status"], "PASS")
        self.assertIn(index["user_stream"]["status"], {"PASS", "WARN"})
        self.assertEqual(index["operator_prechecks"]["status"], "PASS")

    def test_startup_checks_fail_without_live_admission_flags(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker.client = _FakeClient()
        broker.market_client = _FakeMarketClient()
        broker._host = "https://clob.polymarket.com"
        broker._chain_id = 137
        broker._signature_type = 0
        broker._funder = "0xabc"
        broker._api_creds = {"apiKey": "k", "secret": "s", "passphrase": "p"}
        broker._user_stream_enabled = False
        broker._user_stream_url = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

        previous = {key: os.environ.get(key) for key in ("LIVE_ALLOWANCE_READY", "LIVE_GEOBLOCK_READY", "LIVE_ACCOUNT_READY")}
        for key in previous:
            os.environ.pop(key, None)
        old_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            try:
                checks = broker.startup_checks()
            finally:
                os.chdir(old_cwd)
                for key, value in previous.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

        assert checks is not None
        index = {str(row["name"]): row for row in checks}
        self.assertEqual(index["operator_prechecks"]["status"], "FAIL")
        self.assertIn("LIVE_ALLOWANCE_READY", str(index["operator_prechecks"]["message"]))

    def test_startup_checks_fall_back_to_dotenv_flags(self):
        broker = LiveClobBroker.__new__(LiveClobBroker)
        broker.client = _FakeClient()
        broker.market_client = _FakeMarketClient()
        broker._host = "https://clob.polymarket.com"
        broker._chain_id = 137
        broker._signature_type = 2
        broker._funder = "0xabc"
        broker._api_creds = {"apiKey": "k", "secret": "s", "passphrase": "p"}
        broker._user_stream_enabled = False
        broker._user_stream_url = "wss://ws-subscriptions-clob.polymarket.com/ws/user"

        previous = {key: os.environ.get(key) for key in ("LIVE_ALLOWANCE_READY", "LIVE_GEOBLOCK_READY", "LIVE_ACCOUNT_READY")}
        for key in previous:
            os.environ.pop(key, None)

        old_cwd = os.getcwd()
        with tempfile.TemporaryDirectory() as tmpdir:
            os.chdir(tmpdir)
            Path(".env").write_text(
                "LIVE_ALLOWANCE_READY=true\nLIVE_GEOBLOCK_READY=true\nLIVE_ACCOUNT_READY=true\n",
                encoding="utf-8",
            )
            try:
                checks = broker.startup_checks()
            finally:
                os.chdir(old_cwd)
                for key, value in previous.items():
                    if value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = value

        assert checks is not None
        index = {str(row["name"]): row for row in checks}
        self.assertEqual(index["operator_prechecks"]["status"], "PASS")

    def test_identity_binding_rejects_signer_or_api_mismatch(self):
        for signer_identity, api_identity, expected_reason in (
            ("0xwrong", "0xabc", "signer_identity_mismatch"),
            ("0xabc", "0xwrong", "api_identity_mismatch"),
            ("", "0xabc", "signer_identity_missing"),
            ("0xabc", "", "api_identity_missing"),
        ):
            with self.subTest(
                signer_identity=signer_identity,
                api_identity=api_identity,
                expected_reason=expected_reason,
            ):
                broker = LiveClobBroker.__new__(LiveClobBroker)
                broker._funder = "0xabc"
                broker._security_reason_codes = []
                broker._signer_health = SignerHealthSnapshot(
                    healthy=True,
                    signer_identity=signer_identity,
                    api_identity=api_identity,
                    reason_code="",
                    message="ok",
                )

                with self.assertRaises(RuntimeError):
                    broker._validate_identity_binding()

                self.assertIn(expected_reason, broker._security_reason_codes)


if __name__ == "__main__":
    unittest.main()
