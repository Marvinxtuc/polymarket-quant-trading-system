from __future__ import annotations

import unittest
from unittest.mock import patch

from polymarket_bot.config import Settings
from polymarket_bot.main import build_trader
from polymarket_bot.secrets import SecretConfigurationError, resolve_live_secret_bundle


class LiveModeRawPrivateKeyTests(unittest.TestCase):
    def _live_settings(self, **overrides: object) -> Settings:
        base: dict[str, object] = {
            "_env_file": None,
            "dry_run": False,
            "funder_address": "0xabc123",
            "signer_url": "https://signer.internal.local",
            "clob_api_key": "api-key",
            "clob_api_secret": "api-secret",
            "clob_api_passphrase": "api-passphrase",
            "watch_wallets": "0x1111111111111111111111111111111111111111",
        }
        base.update(overrides)
        return Settings(**base)

    def test_resolve_live_secret_bundle_rejects_raw_private_key_without_leaking_value(self) -> None:
        sentinel = "RAW_PRIVATE_KEY_SHOULD_NEVER_LEAK"
        settings = self._live_settings(private_key=sentinel)

        with self.assertRaises(SecretConfigurationError) as ctx:
            resolve_live_secret_bundle(settings)

        self.assertEqual(ctx.exception.reason_code, "raw_private_key_forbidden_live")
        self.assertNotIn(sentinel, str(ctx.exception))

    def test_build_trader_fails_closed_before_signer_or_broker_when_raw_key_present(self) -> None:
        sentinel = "RAW_PRIVATE_KEY_SHOULD_NEVER_LEAK"
        settings = self._live_settings(private_key=sentinel)

        with (
            patch("polymarket_bot.main.build_signer_client") as signer_builder,
            patch("polymarket_bot.main.LiveClobBroker") as live_broker_ctor,
        ):
            with self.assertRaises(RuntimeError) as ctx:
                build_trader(settings)

        self.assertIn("raw_private_key_forbidden_live", str(ctx.exception))
        self.assertNotIn(sentinel, str(ctx.exception))
        signer_builder.assert_not_called()
        live_broker_ctor.assert_not_called()


if __name__ == "__main__":
    unittest.main()
