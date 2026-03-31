from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(slots=True)
class SignerStatusSnapshot:
    live_mode: bool
    signer_required: bool
    signer_mode: str
    signer_healthy: bool
    signer_identity_matched: bool
    api_identity_matched: bool
    broker_identity_matched: bool
    raw_key_detected: bool
    funder_identity_present: bool
    api_creds_configured: bool
    hot_wallet_cap_enabled: bool
    hot_wallet_cap_ok: bool
    hot_wallet_cap_limit_usd: float = 0.0
    hot_wallet_cap_value_usd: float = 0.0
    reason_codes: list[str] = field(default_factory=list)
    last_checked_ts: int = 0

    def as_state_payload(self) -> dict[str, object]:
        return {
            "live_mode": bool(self.live_mode),
            "signer_required": bool(self.signer_required),
            "signer_mode": str(self.signer_mode or "none"),
            "signer_healthy": bool(self.signer_healthy),
            "signer_identity_matched": bool(self.signer_identity_matched),
            "api_identity_matched": bool(self.api_identity_matched),
            "broker_identity_matched": bool(self.broker_identity_matched),
            "raw_key_detected": bool(self.raw_key_detected),
            "funder_identity_present": bool(self.funder_identity_present),
            "api_creds_configured": bool(self.api_creds_configured),
            "hot_wallet_cap_enabled": bool(self.hot_wallet_cap_enabled),
            "hot_wallet_cap_ok": bool(self.hot_wallet_cap_ok),
            "hot_wallet_cap_limit_usd": float(self.hot_wallet_cap_limit_usd),
            "hot_wallet_cap_value_usd": float(self.hot_wallet_cap_value_usd),
            "reason_codes": list(self.reason_codes or []),
            "last_checked_ts": int(self.last_checked_ts or 0),
        }
