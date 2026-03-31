from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ExposureLedgerEntry:
    scope_type: str
    scope_key: str
    valuation_currency: str = "USD"
    filled_exposure_notional_usd: float = 0.0
    pending_entry_exposure_notional_usd: float = 0.0
    pending_exit_notional_usd: float = 0.0
    committed_exposure_notional_usd: float = 0.0
    cap_notional_usd: float = 0.0
    blocked: bool = False
    reason_code: str = ""
    wallet_scope: str = ""
    condition_scope: str = ""
    updated_ts: int = 0

    @classmethod
    def from_payload(cls, payload: dict[str, object] | None) -> "ExposureLedgerEntry":
        source = dict(payload or {})
        filled = max(0.0, float(source.get("filled_exposure_notional_usd") or 0.0))
        pending_entry = max(0.0, float(source.get("pending_entry_exposure_notional_usd") or 0.0))
        pending_exit = max(0.0, float(source.get("pending_exit_notional_usd") or 0.0))
        committed = max(0.0, float(source.get("committed_exposure_notional_usd") or (filled + pending_entry)))
        cap = max(0.0, float(source.get("cap_notional_usd") or 0.0))
        return cls(
            scope_type=str(source.get("scope_type") or "").strip().lower(),
            scope_key=str(source.get("scope_key") or "").strip(),
            valuation_currency=str(source.get("valuation_currency") or "USD").strip().upper() or "USD",
            filled_exposure_notional_usd=filled,
            pending_entry_exposure_notional_usd=pending_entry,
            pending_exit_notional_usd=pending_exit,
            committed_exposure_notional_usd=committed,
            cap_notional_usd=cap,
            blocked=bool(source.get("blocked", False)),
            reason_code=str(source.get("reason_code") or "").strip(),
            wallet_scope=str(source.get("wallet_scope") or "").strip(),
            condition_scope=str(source.get("condition_scope") or "").strip(),
            updated_ts=int(source.get("updated_ts") or 0),
        )

    def to_payload(self) -> dict[str, object]:
        return {
            "scope_type": str(self.scope_type or "").strip().lower(),
            "scope_key": str(self.scope_key or "").strip(),
            "valuation_currency": str(self.valuation_currency or "USD").strip().upper() or "USD",
            "filled_exposure_notional_usd": float(self.filled_exposure_notional_usd or 0.0),
            "pending_entry_exposure_notional_usd": float(self.pending_entry_exposure_notional_usd or 0.0),
            "pending_exit_notional_usd": float(self.pending_exit_notional_usd or 0.0),
            "committed_exposure_notional_usd": float(self.committed_exposure_notional_usd or 0.0),
            "cap_notional_usd": float(self.cap_notional_usd or 0.0),
            "blocked": bool(self.blocked),
            "reason_code": str(self.reason_code or "").strip(),
            "wallet_scope": str(self.wallet_scope or "").strip(),
            "condition_scope": str(self.condition_scope or "").strip(),
            "updated_ts": int(self.updated_ts or 0),
        }
