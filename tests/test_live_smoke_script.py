from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


def _load_live_smoke_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "live_clob_type2_smoke.py"
    spec = importlib.util.spec_from_file_location("live_clob_type2_smoke", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _FakeOrderArgs:
    def __init__(self, token_id, price, size, side):
        self.token_id = token_id
        self.price = price
        self.size = size
        self.side = side


class _FakeSigner:
    def __init__(self) -> None:
        self.payloads: list[dict[str, object]] = []

    def sign_order(self, payload: dict[str, object]) -> dict[str, object]:
        self.payloads.append(dict(payload))
        return {"signed_order": dict(payload)}

    def health_check(self):
        return SimpleNamespace(
            healthy=True,
            signer_identity="0xabc",
            api_identity="0xabc",
            reason_code="",
            message="ok",
        )


class _FakeClient:
    def __init__(self) -> None:
        self.last_post = None

    def post_order(self, signed, order_type):
        self.last_post = {"signed": signed, "order_type": order_type}
        return {"orderID": "oid-123", "status": "live"}


class LiveSmokeScriptTests(unittest.TestCase):
    def test_build_live_broker_uses_signer_boundary(self) -> None:
        module = _load_live_smoke_module()
        settings_ctor = Mock(
            return_value=SimpleNamespace(
                polymarket_clob_host="https://clob.polymarket.com",
                chain_id=137,
                clob_signature_type=0,
            )
        )
        bundle = SimpleNamespace(
            funder_address="0xabc",
            clob_api_key="api-key",
            clob_api_secret="api-secret",
            clob_api_passphrase="api-passphrase",
        )
        signer = _FakeSigner()
        captured_kwargs: dict[str, object] = {}

        def _capture_broker_ctor(*args, **kwargs):  # noqa: ANN002,ANN003
            captured_kwargs.update(kwargs)
            captured_kwargs["host"] = kwargs.get("host")
            captured_kwargs["chain_id"] = kwargs.get("chain_id")
            captured_kwargs["funder"] = kwargs.get("funder")
            return SimpleNamespace(client=_FakeClient())

        with (
            patch.object(module, "Settings", settings_ctor),
            patch.object(module, "resolve_live_secret_bundle", return_value=bundle),
            patch.object(module, "build_signer_client", return_value=signer),
            patch.object(module, "LiveClobBroker", side_effect=_capture_broker_ctor),
        ):
            broker = module._build_live_broker()

        self.assertIsNotNone(broker)
        settings_ctor.assert_called_once_with(dry_run=False)
        self.assertEqual(captured_kwargs["host"], "https://clob.polymarket.com")
        self.assertEqual(captured_kwargs["chain_id"], 137)
        self.assertEqual(captured_kwargs["funder"], "0xabc")
        self.assertIs(captured_kwargs["signer_client"], signer)
        self.assertEqual(captured_kwargs["api_key"], "api-key")
        self.assertEqual(captured_kwargs["api_secret"], "api-secret")
        self.assertEqual(captured_kwargs["api_passphrase"], "api-passphrase")
        self.assertFalse(bool(captured_kwargs["user_stream_enabled"]))

    def test_post_limit_order_signs_via_signer_and_respects_order_type(self) -> None:
        module = _load_live_smoke_module()
        signer = _FakeSigner()
        client = _FakeClient()
        broker = SimpleNamespace(
            _OrderArgs=_FakeOrderArgs,
            _side_map={"BUY": "BUY_FLAG", "SELL": "SELL_FLAG"},
            _signer_client=signer,
            client=client,
            _chain_id=137,
            _funder="0xabc",
        )

        order_id, response = module._post_limit_order(
            broker,
            token_id="token-1",
            side="BUY",
            price=0.44,
            size=2.0,
            order_type="FAK",
            tick_size=0.01,
            neg_risk=True,
        )

        self.assertEqual(order_id, "oid-123")
        self.assertEqual(response["status"], "live")
        self.assertEqual(client.last_post["order_type"], "FAK")
        payload = signer.payloads[-1]
        self.assertEqual(payload["token_id"], "token-1")
        self.assertEqual(payload["side"], "BUY_FLAG")
        self.assertEqual(payload["chain_id"], 137)
        self.assertEqual(payload["funder_address"], "0xabc")
        self.assertEqual(payload["tick_size"], 0.01)
        self.assertTrue(bool(payload["neg_risk"]))


if __name__ == "__main__":
    unittest.main()
