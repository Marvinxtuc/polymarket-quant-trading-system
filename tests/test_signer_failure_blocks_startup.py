from __future__ import annotations

import unittest
from unittest.mock import patch

from polymarket_bot.config import Settings
from polymarket_bot.main import build_trader
from polymarket_bot.signer_client import SignerClientError, SignerHealthSnapshot


class _FailingSignerClient:
    def health_check(self) -> SignerHealthSnapshot:
        raise SignerClientError("signer unreachable", reason_code="signer_unreachable")


class _UnhealthySignerClient:
    def health_check(self) -> SignerHealthSnapshot:
        return SignerHealthSnapshot(
            healthy=False,
            signer_identity="0xabc123",
            api_identity="0xabc123",
            reason_code="signer_unhealthy",
            message="unhealthy",
        )


class SignerStartupFailureTests(unittest.TestCase):
    def _live_settings(self) -> Settings:
        return Settings(
            _env_file=None,
            dry_run=False,
            funder_address="0xabc123",
            signer_url="https://signer.internal.local",
            clob_api_key="api-key",
            clob_api_secret="api-secret",
            clob_api_passphrase="api-passphrase",
            watch_wallets="0x1111111111111111111111111111111111111111",
        )

    def test_signer_unavailable_blocks_startup(self) -> None:
        settings = self._live_settings()

        with (
            patch("polymarket_bot.main.build_signer_client", return_value=_FailingSignerClient()),
            patch("polymarket_bot.main.LiveClobBroker") as live_broker_ctor,
        ):
            with self.assertRaises(RuntimeError) as ctx:
                build_trader(settings)

        self.assertIn("signer_unreachable", str(ctx.exception))
        live_broker_ctor.assert_not_called()

    def test_unhealthy_signer_blocks_startup(self) -> None:
        settings = self._live_settings()

        with (
            patch("polymarket_bot.main.build_signer_client", return_value=_UnhealthySignerClient()),
            patch("polymarket_bot.main.LiveClobBroker") as live_broker_ctor,
        ):
            with self.assertRaises(RuntimeError) as ctx:
                build_trader(settings)

        self.assertIn("signer_unhealthy", str(ctx.exception))
        live_broker_ctor.assert_not_called()


if __name__ == "__main__":
    unittest.main()
