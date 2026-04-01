from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol

import httpx

from polymarket_bot.secrets import LiveSecretBundle, normalize_identity


_FORBIDDEN_SIGNER_KEYS = {
    "private_key",
    "privatekey",
    "seed",
    "mnemonic",
    "secret",
    "api_secret",
    "signing_key",
    "wallet_key",
}


class SignerClientError(RuntimeError):
    def __init__(self, message: str, *, reason_code: str) -> None:
        super().__init__(message)
        self.reason_code = str(reason_code or "signer_client_error")


@dataclass(frozen=True)
class SignerHealthSnapshot:
    healthy: bool
    signer_identity: str
    api_identity: str
    reason_code: str
    message: str


class SignerClient(Protocol):
    def health_check(self) -> SignerHealthSnapshot:
        raise NotImplementedError

    def sign_order(self, payload: dict[str, object]) -> object:
        raise NotImplementedError


def _contains_forbidden_key(value: object) -> bool:
    if isinstance(value, dict):
        for key, nested in value.items():
            normalized = str(key or "").strip().lower()
            if normalized in _FORBIDDEN_SIGNER_KEYS:
                return True
            if _contains_forbidden_key(nested):
                return True
        return False
    if isinstance(value, list):
        return any(_contains_forbidden_key(item) for item in value)
    return False


class ExternalHttpSignerClient:
    def __init__(self, bundle: LiveSecretBundle) -> None:
        self._url = str(bundle.signer_url or "").rstrip("/")
        self._health_path = str(bundle.signer_health_path or "/health").strip() or "/health"
        self._sign_path = str(bundle.signer_sign_path or "/sign-order").strip() or "/sign-order"
        self._timeout = max(0.5, float(bundle.signer_timeout_seconds or 5.0))
        self._auth_token = str(bundle.signer_auth_token or "").strip()

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self._auth_token:
            headers["Authorization"] = f"Bearer {self._auth_token}"
        return headers

    def _url_for(self, path: str) -> str:
        suffix = str(path or "").strip()
        if not suffix.startswith("/"):
            suffix = f"/{suffix}"
        return f"{self._url}{suffix}"

    def health_check(self) -> SignerHealthSnapshot:
        try:
            response = httpx.get(self._url_for(self._health_path), headers=self._headers(), timeout=self._timeout)
        except Exception as exc:
            raise SignerClientError(str(exc), reason_code="signer_unreachable") from exc
        if response.status_code >= 400:
            raise SignerClientError(
                f"signer health status {response.status_code}",
                reason_code="signer_health_http_error",
            )
        try:
            payload = response.json()
        except Exception as exc:
            raise SignerClientError(
                "invalid signer health payload",
                reason_code="signer_health_payload_invalid",
            ) from exc
        if not isinstance(payload, dict):
            raise SignerClientError(
                "invalid signer health payload type",
                reason_code="signer_health_payload_invalid",
            )

        healthy = bool(
            payload.get("healthy")
            if "healthy" in payload
            else payload.get("ok")
        )
        signer_identity = normalize_identity(
            str(
                payload.get("signer_identity")
                or payload.get("identity")
                or payload.get("address")
                or ""
            )
        )
        api_identity = normalize_identity(
            str(
                payload.get("api_identity")
                or payload.get("api_binding_identity")
                or payload.get("api_address")
                or ""
            )
        )
        reason_code = str(payload.get("reason_code") or "").strip()
        message = str(payload.get("message") or "").strip()
        return SignerHealthSnapshot(
            healthy=healthy,
            signer_identity=signer_identity,
            api_identity=api_identity,
            reason_code=reason_code,
            message=message,
        )

    def sign_order(self, payload: dict[str, object]) -> object:
        try:
            response = httpx.post(
                self._url_for(self._sign_path),
                headers=self._headers(),
                json=payload,
                timeout=self._timeout,
            )
        except Exception as exc:
            raise SignerClientError(str(exc), reason_code="signer_sign_request_failed") from exc
        if response.status_code >= 400:
            raise SignerClientError(
                f"signer sign status {response.status_code}",
                reason_code="signer_sign_http_error",
            )
        try:
            body = response.json()
        except Exception as exc:
            raise SignerClientError(
                "invalid signer response payload",
                reason_code="signer_sign_payload_invalid",
            ) from exc
        if not isinstance(body, dict):
            raise SignerClientError(
                "invalid signer response payload type",
                reason_code="signer_sign_payload_invalid",
            )
        if _contains_forbidden_key(body):
            raise SignerClientError(
                "signer response contains forbidden secret material",
                reason_code="signer_response_contains_secret_material",
            )
        signed_order = body.get("signed_order")
        if signed_order is None:
            raise SignerClientError(
                "signer response missing signed_order",
                reason_code="signer_sign_payload_invalid",
            )
        return signed_order


def build_signer_client(bundle: LiveSecretBundle) -> SignerClient:
    if str(bundle.signer_mode or "").strip().lower() != "external_http":
        raise SignerClientError(
            f"unsupported signer mode: {bundle.signer_mode}",
            reason_code="signer_mode_invalid",
        )
    return ExternalHttpSignerClient(bundle)
