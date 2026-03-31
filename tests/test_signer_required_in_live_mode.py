from __future__ import annotations

import unittest
from unittest.mock import patch

from polymarket_bot.config import Settings
from polymarket_bot.main import build_trader


class SignerRequiredInLiveModeTests(unittest.TestCase):
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

    def test_build_trader_requires_signer_url_in_live_mode(self) -> None:
        settings = self._live_settings(signer_url="")

        with patch("polymarket_bot.main.build_signer_client") as signer_builder:
            with self.assertRaises(RuntimeError) as ctx:
                build_trader(settings)

        self.assertIn("signer_url_missing", str(ctx.exception))
        signer_builder.assert_not_called()

    def test_build_trader_requires_live_api_creds_in_live_mode(self) -> None:
        settings = self._live_settings(clob_api_key="", clob_api_secret="", clob_api_passphrase="")

        with patch("polymarket_bot.main.build_signer_client") as signer_builder:
            with self.assertRaises(RuntimeError) as ctx:
                build_trader(settings)

        self.assertIn("clob_api_creds_missing", str(ctx.exception))
        signer_builder.assert_not_called()


if __name__ == "__main__":
    unittest.main()
