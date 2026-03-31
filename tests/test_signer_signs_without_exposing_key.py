from __future__ import annotations

import importlib.util
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from polymarket_bot.config import Settings
from polymarket_bot.main import build_trader
from polymarket_bot.runner import Trader
from polymarket_bot.signer_client import ExternalHttpSignerClient, SignerClientError, SignerHealthSnapshot
from polymarket_bot.web import _api_state_payload

from runtime_persistence_helpers import DummyBroker, DummyDataClient, DummyRisk, DummyStrategy, make_settings


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict[str, object]) -> None:
        self.status_code = status_code
        self._payload = dict(payload)

    def json(self) -> dict[str, object]:
        return dict(self._payload)


class _HealthySignerClient:
    def health_check(self) -> SignerHealthSnapshot:
        return SignerHealthSnapshot(
            healthy=True,
            signer_identity="0xabc123",
            api_identity="0xabc123",
            reason_code="",
            message="ok",
        )


def _load_live_smoke_preflight_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "live_smoke_preflight.py"
    spec = importlib.util.spec_from_file_location("live_smoke_preflight", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load module: {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class SignerSecretBoundaryTests(unittest.TestCase):
    def test_external_signer_rejects_secret_material_from_response(self) -> None:
        settings = Settings(
            _env_file=None,
            dry_run=False,
            funder_address="0xabc123",
            signer_url="https://signer.internal.local",
            clob_api_key="api-key",
            clob_api_secret="api-secret",
            clob_api_passphrase="api-passphrase",
        )
        from polymarket_bot.secrets import resolve_live_secret_bundle

        bundle = resolve_live_secret_bundle(settings)
        client = ExternalHttpSignerClient(bundle)

        with patch(
            "polymarket_bot.signer_client.httpx.post",
            return_value=_FakeResponse(
                200,
                {
                    "signed_order": {"order": "ok"},
                    "private_key": "leak",
                },
            ),
        ):
            with self.assertRaises(SignerClientError) as ctx:
                client.sign_order({"token_id": "t1"})

        self.assertEqual(ctx.exception.reason_code, "signer_response_contains_secret_material")

    def test_live_build_chain_passes_no_raw_private_key_to_broker_ctor(self) -> None:
        settings = Settings(
            _env_file=None,
            dry_run=False,
            funder_address="0xabc123",
            signer_url="https://signer.internal.local",
            clob_api_key="api-key",
            clob_api_secret="api-secret",
            clob_api_passphrase="api-passphrase",
            watch_wallets="0x1111111111111111111111111111111111111111",
        )
        captured_kwargs: dict[str, object] = {}
        signer_client = _HealthySignerClient()

        def _capture_broker_ctor(*args, **kwargs):  # noqa: ANN002,ANN003
            captured_kwargs.update(kwargs)
            return SimpleNamespace(close=lambda: None)

        def _capture_trader_ctor(**kwargs):  # noqa: ANN003
            return SimpleNamespace(**kwargs)

        with (
            patch("polymarket_bot.main.build_signer_client", return_value=signer_client),
            patch("polymarket_bot.main.LiveClobBroker", side_effect=_capture_broker_ctor),
            patch("polymarket_bot.main.Trader", side_effect=_capture_trader_ctor),
        ):
            trader_like = build_trader(settings)

        self.assertIsNotNone(trader_like)
        self.assertIn("signer_client", captured_kwargs)
        self.assertIs(captured_kwargs.get("signer_client"), signer_client)
        self.assertNotIn("private_key", captured_kwargs)
        self.assertNotIn("key", captured_kwargs)

    def test_runtime_api_and_report_payload_do_not_leak_raw_private_key(self) -> None:
        sentinel = "RAW_PRIVATE_KEY_SHOULD_NEVER_APPEAR"
        with tempfile.TemporaryDirectory() as tmpdir_raw:
            workdir = Path(tmpdir_raw)
            settings = make_settings(dry_run=False, workdir=workdir, funder_address="0xabc123")
            settings.private_key = sentinel
            settings.live_hot_wallet_balance_cap_usd = 0.0
            trader = Trader(
                settings=settings,
                data_client=DummyDataClient(),
                strategy=DummyStrategy(signals=[]),
                risk=DummyRisk(),
                broker=DummyBroker(),
            )

            runtime_payload = trader._dump_runtime_state()
            runtime_text = json.dumps(runtime_payload, ensure_ascii=False)
            self.assertNotIn(sentinel, runtime_text)

            api_payload = _api_state_payload(runtime_payload, None)
            api_text = json.dumps(api_payload, ensure_ascii=False)
            self.assertNotIn(sentinel, api_text)

            preflight = _load_live_smoke_preflight_module()
            report_settings = Settings(
                _env_file=None,
                dry_run=False,
                runtime_root_path=str(workdir),
                private_key=sentinel,
                funder_address="0xabc123",
                signer_url="https://signer.internal.local",
                clob_api_key="api-key",
                clob_api_secret="api-secret",
                clob_api_passphrase="api-passphrase",
                live_allowance_ready=True,
                live_geoblock_ready=True,
                live_account_ready=True,
            )
            report, _exit_code = preflight.build_report(report_settings, now_ts=int(time.time()))
            report_text = json.dumps(report, ensure_ascii=False)
            self.assertNotIn(sentinel, report_text)

    def test_api_signer_security_keeps_minimal_fields_and_drops_sensitive_details(self) -> None:
        payload = {
            "signer_security": {
                "live_mode": True,
                "signer_required": True,
                "signer_healthy": False,
                "raw_key_detected": False,
                "hot_wallet_cap_ok": True,
                "reason_codes": ["signer_unhealthy"],
                "signer_url": "https://signer.internal.local",
                "secret_path": "vault://kv/polymarket/live",
                "token": "SHOULD_NOT_EXPOSE",
            }
        }
        api_payload = _api_state_payload(payload, None)
        signer_security = dict(api_payload.get("signer_security") or {})

        for key in (
            "live_mode",
            "signer_required",
            "signer_healthy",
            "raw_key_detected",
            "hot_wallet_cap_ok",
            "reason_codes",
        ):
            self.assertIn(key, signer_security)

        self.assertNotIn("signer_url", signer_security)
        self.assertNotIn("secret_path", signer_security)
        self.assertNotIn("token", signer_security)


if __name__ == "__main__":
    unittest.main()
