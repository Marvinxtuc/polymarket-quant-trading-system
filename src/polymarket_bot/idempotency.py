from __future__ import annotations

import hashlib
import json
from typing import Mapping

# Claim outcomes
CLAIMED_NEW = "CLAIMED_NEW"
EXISTING_NON_TERMINAL = "EXISTING_NON_TERMINAL"
EXISTING_TERMINAL = "EXISTING_TERMINAL"
STORAGE_ERROR = "STORAGE_ERROR"

# Order intent statuses (normalized to lowercase)
INTENT_STATUS_NEW = "new"
INTENT_STATUS_SENDING = "sending"
INTENT_STATUS_ACKED_PENDING = "acked_pending"
INTENT_STATUS_PARTIAL = "partial"
INTENT_STATUS_FILLED = "filled"
INTENT_STATUS_CANCEL_REQUESTED = "cancel_requested"
INTENT_STATUS_CANCELED = "canceled"
INTENT_STATUS_REJECTED = "rejected"
INTENT_STATUS_FAILED = "failed"
INTENT_STATUS_ACK_UNKNOWN = "ack_unknown"
INTENT_STATUS_MANUAL_REQUIRED = "manual_required"

NON_TERMINAL_STATUSES = {
    INTENT_STATUS_NEW,
    INTENT_STATUS_SENDING,
    INTENT_STATUS_ACKED_PENDING,
    INTENT_STATUS_PARTIAL,
    INTENT_STATUS_CANCEL_REQUESTED,
    INTENT_STATUS_ACK_UNKNOWN,
    INTENT_STATUS_MANUAL_REQUIRED,
}

TERMINAL_STATUSES = {
    INTENT_STATUS_FILLED,
    INTENT_STATUS_CANCELED,
    INTENT_STATUS_REJECTED,
    INTENT_STATUS_FAILED,
    "unmatched",
}


def normalize_status(value: object) -> str:
    return str(value or "").strip().lower()


def is_terminal(status: object) -> bool:
    return normalize_status(status) in TERMINAL_STATUSES


def build_intent_idempotency_key(
    *,
    strategy_name: str,
    signal_source: str,
    signal_fingerprint: str,
    token_id: str = "",
    side: str = "",
    salt: str = "",
    extra: Mapping[str, object] | None = None,
) -> str:
    """
    Deterministic idempotency key that always includes the strategy + signal identity.
    Additional fields (token/side/extra) let callers scope keys at finer granularity.
    """

    payload: dict[str, object] = {
        "strategy": str(strategy_name or "").strip(),
        "source": str(signal_source or "").strip(),
        "fingerprint": str(signal_fingerprint or "").strip(),
        "token": str(token_id or "").strip(),
        "side": str(side or "").strip().upper(),
    }
    if extra:
        payload["extra"] = {k: extra[k] for k in sorted(extra.keys())}

    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256()
    if salt:
        digest.update(str(salt).encode("utf-8"))
    digest.update(encoded.encode("utf-8"))
    return digest.hexdigest()
