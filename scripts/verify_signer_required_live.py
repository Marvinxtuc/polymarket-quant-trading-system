#!/usr/bin/env python3
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.config import Settings
from polymarket_bot.main import build_trader
from polymarket_bot.secrets import SecretConfigurationError, resolve_live_secret_bundle
from polymarket_bot.signer_client import SignerClientError, SignerHealthSnapshot


class _FailingSignerClient:
    def health_check(self) -> SignerHealthSnapshot:
        raise SignerClientError("signer unreachable", reason_code="signer_unreachable")


class _HealthySignerClient:
    def health_check(self) -> SignerHealthSnapshot:
        return SignerHealthSnapshot(
            healthy=True,
            signer_identity="0xabc123",
            api_identity="0xabc123",
            reason_code="",
            message="ok",
        )


def _live_settings(**overrides: object) -> Settings:
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


def _verify_raw_key_forbidden() -> None:
    sentinel = "RAW_PRIVATE_KEY_MUST_NOT_PASS"
    settings = _live_settings(private_key=sentinel)
    try:
        resolve_live_secret_bundle(settings)
    except SecretConfigurationError as exc:
        if exc.reason_code != "raw_private_key_forbidden_live":
            raise AssertionError(f"unexpected reason_code: {exc.reason_code}") from exc
        if sentinel in str(exc):
            raise AssertionError("raw private key leaked in exception text")
        return
    raise AssertionError("expected raw private key to be rejected in live mode")


def _verify_signer_required() -> None:
    settings = _live_settings(signer_url="")
    try:
        build_trader(settings)
    except RuntimeError as exc:
        if "signer_url_missing" not in str(exc):
            raise AssertionError(f"unexpected error for missing signer_url: {exc}") from exc
        return
    raise AssertionError("expected build_trader to fail when signer_url is missing")


def _verify_signer_unavailable_blocks_startup() -> None:
    settings = _live_settings()
    with (
        patch("polymarket_bot.main.build_signer_client", return_value=_FailingSignerClient()),
        patch("polymarket_bot.main.LiveClobBroker") as live_broker_ctor,
    ):
        try:
            build_trader(settings)
        except RuntimeError as exc:
            if "signer_unreachable" not in str(exc):
                raise AssertionError(f"unexpected signer unavailable error: {exc}") from exc
        else:
            raise AssertionError("expected build_trader to fail when signer is unavailable")
    if live_broker_ctor.called:
        raise AssertionError("LiveClobBroker should not be constructed when signer health check fails")


def _verify_live_chain_uses_signer_boundary() -> None:
    settings = _live_settings()
    signer_client = _HealthySignerClient()
    captured_kwargs: dict[str, object] = {}

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
        build_trader(settings)

    if captured_kwargs.get("signer_client") is not signer_client:
        raise AssertionError("live broker constructor did not receive signer_client")
    if "private_key" in captured_kwargs or "key" in captured_kwargs:
        raise AssertionError("live broker constructor unexpectedly received raw private key material")


def main() -> int:
    checks = [
        ("raw_key_forbidden_live", _verify_raw_key_forbidden),
        ("signer_required_live", _verify_signer_required),
        ("signer_unavailable_fail_close", _verify_signer_unavailable_blocks_startup),
        ("live_chain_signer_boundary", _verify_live_chain_uses_signer_boundary),
    ]
    for name, fn in checks:
        fn()
        print(f"[PASS] {name}")
    print("[OK] verify_signer_required_live")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
