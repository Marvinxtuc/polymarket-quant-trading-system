from __future__ import annotations

import importlib.util
import json
import os
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from pydantic import ValidationError
from polymarket_bot.config import Settings
from polymarket_bot.secrets import SecretConfigurationError, resolve_live_secret_bundle


def _load_preflight_module():
    module_path = Path(__file__).resolve().parents[1] / "scripts" / "live_smoke_preflight.py"
    spec = importlib.util.spec_from_file_location("live_smoke_preflight", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load {module_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


live_smoke_preflight = _load_preflight_module()


def _live_settings(**overrides) -> Settings:
    base_kwargs = {
        "_env_file": None,
        "dry_run": False,
        "private_key": "",
        "funder_address": "0xfunder0000000000000000000000000000000000",
        "signer_url": "https://signer.example.com",
        "clob_api_key": "k",
        "clob_api_secret": "s",
        "clob_api_passphrase": "p",
        "live_hot_wallet_balance_cap_usd": 1000.0,
    }
    base_kwargs.update(overrides)
    return Settings(**base_kwargs)


class LiveSignerGuardTests(unittest.TestCase):
    def test_raw_private_key_fail_close_without_leak(self) -> None:
        settings = _live_settings(private_key="supersecret")
        with self.assertRaises(SecretConfigurationError) as ctx:
            resolve_live_secret_bundle(settings)
        self.assertEqual(str(ctx.exception.reason_code), "raw_private_key_forbidden_live")
        self.assertNotIn("supersecret", str(ctx.exception))

    def test_signer_missing_blocks_live_startup(self) -> None:
        settings = _live_settings(signer_url="")
        with self.assertRaises(SecretConfigurationError) as ctx:
            resolve_live_secret_bundle(settings)
        self.assertEqual(str(ctx.exception.reason_code), "signer_url_missing")

    def test_hot_wallet_cap_rejects_negative(self) -> None:
        with self.assertRaises(ValidationError):
            _live_settings(live_hot_wallet_balance_cap_usd=-1)

    def test_live_report_hides_raw_private_key(self) -> None:
        with patch.dict(
            os.environ,
            {
                "DRY_RUN": "false",
                "PRIVATE_KEY": "supersecret",
                "FUNDER_ADDRESS": "0xfunder0000000000000000000000000000000000",
                "SIGNER_URL": "https://signer.example.com",
                "CLOB_API_KEY": "k",
                "CLOB_API_SECRET": "s",
                "CLOB_API_PASSPHRASE": "p",
                "LIVE_ALLOWANCE_READY": "true",
                "LIVE_GEOBLOCK_READY": "true",
                "LIVE_ACCOUNT_READY": "true",
            },
            clear=True,
        ):
            settings = Settings(_env_file=None)
            report, exit_code = live_smoke_preflight.build_report(settings, now_ts=int(time.time()))
        payload = json.dumps(report, ensure_ascii=False)
        self.assertEqual(exit_code, 1)
        self.assertNotIn("supersecret", payload)
        self.assertTrue(report.get("blocker_codes"))


if __name__ == "__main__":
    unittest.main()
