from __future__ import annotations

from dataclasses import dataclass

from polymarket_bot.config import Settings


def _normalize_identity(value: str) -> str:
    return str(value or "").strip().lower()


class SecretConfigurationError(RuntimeError):
    def __init__(self, message: str, *, reason_code: str) -> None:
        super().__init__(message)
        self.reason_code = str(reason_code or "secret_configuration_error")


@dataclass(frozen=True)
class LiveSecretBundle:
    funder_address: str
    signer_mode: str
    signer_url: str
    signer_health_path: str
    signer_sign_path: str
    signer_auth_token: str
    signer_timeout_seconds: float
    clob_api_key: str
    clob_api_secret: str
    clob_api_passphrase: str
    hot_wallet_balance_cap_usd: float

    @property
    def normalized_funder(self) -> str:
        return _normalize_identity(self.funder_address)


def resolve_live_secret_bundle(settings: Settings) -> LiveSecretBundle:
    if bool(getattr(settings, "dry_run", True)):
        raise SecretConfigurationError(
            "live secret bundle requested in dry-run mode",
            reason_code="live_secret_bundle_in_dry_run",
        )

    raw_private_key = str(getattr(settings, "private_key", "") or "").strip()
    if raw_private_key:
        raise SecretConfigurationError(
            "live mode forbids raw private key usage",
            reason_code="raw_private_key_forbidden_live",
        )

    funder = str(getattr(settings, "funder_address", "") or "").strip()
    if not funder:
        raise SecretConfigurationError(
            "missing funder address for live mode",
            reason_code="funder_address_missing",
        )

    signer_mode = str(getattr(settings, "live_signer_mode", "") or "").strip().lower() or "external_http"
    if signer_mode != "external_http":
        raise SecretConfigurationError(
            f"unsupported signer mode: {signer_mode}",
            reason_code="signer_mode_invalid",
        )

    signer_url = str(getattr(settings, "signer_url", "") or "").strip()
    if not signer_url:
        raise SecretConfigurationError(
            "missing signer_url in live mode",
            reason_code="signer_url_missing",
        )

    signer_health_path = str(getattr(settings, "signer_health_path", "") or "").strip() or "/health"
    signer_sign_path = str(getattr(settings, "signer_sign_path", "") or "").strip() or "/sign-order"
    signer_timeout_seconds = float(getattr(settings, "signer_timeout_seconds", 5.0) or 5.0)

    clob_api_key = str(getattr(settings, "clob_api_key", "") or "").strip()
    clob_api_secret = str(getattr(settings, "clob_api_secret", "") or "").strip()
    clob_api_passphrase = str(getattr(settings, "clob_api_passphrase", "") or "").strip()
    if not (clob_api_key and clob_api_secret and clob_api_passphrase):
        raise SecretConfigurationError(
            "missing live CLOB api creds",
            reason_code="clob_api_creds_missing",
        )

    hot_wallet_balance_cap_usd = float(getattr(settings, "live_hot_wallet_balance_cap_usd", 0.0) or 0.0)
    if hot_wallet_balance_cap_usd < 0:
        raise SecretConfigurationError(
            "live_hot_wallet_balance_cap_usd must be >= 0",
            reason_code="hot_wallet_cap_invalid",
        )

    return LiveSecretBundle(
        funder_address=funder,
        signer_mode=signer_mode,
        signer_url=signer_url,
        signer_health_path=signer_health_path,
        signer_sign_path=signer_sign_path,
        signer_auth_token=str(getattr(settings, "signer_auth_token", "") or "").strip(),
        signer_timeout_seconds=signer_timeout_seconds,
        clob_api_key=clob_api_key,
        clob_api_secret=clob_api_secret,
        clob_api_passphrase=clob_api_passphrase,
        hot_wallet_balance_cap_usd=hot_wallet_balance_cap_usd,
    )


def normalize_identity(value: str) -> str:
    return _normalize_identity(value)
