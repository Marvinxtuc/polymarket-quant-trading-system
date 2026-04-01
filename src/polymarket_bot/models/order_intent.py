from __future__ import annotations

from dataclasses import dataclass

from polymarket_bot.idempotency import is_terminal, normalize_status


@dataclass(slots=True)
class PersistedOrderIntent:
    intent_id: str
    idempotency_key: str
    strategy_name: str
    signal_source: str
    signal_fingerprint: str
    token_id: str
    side: str
    status: str
    created_ts: int
    updated_ts: int
    payload: dict[str, object]
    strategy_order_uuid: str = ""
    broker_order_id: str = ""
    condition_id: str = ""
    recovered_source: str = ""
    recovery_reason: str = ""

    @property
    def is_terminal(self) -> bool:
        return is_terminal(self.status)

    def to_row(self) -> dict[str, object]:
        return {
            "intent_id": str(self.intent_id or ""),
            "idempotency_key": str(self.idempotency_key or ""),
            "strategy_name": str(self.strategy_name or ""),
            "signal_source": str(self.signal_source or ""),
            "signal_fingerprint": str(self.signal_fingerprint or ""),
            "strategy_order_uuid": str(self.strategy_order_uuid or ""),
            "broker_order_id": str(self.broker_order_id or ""),
            "token_id": str(self.token_id or ""),
            "condition_id": str(self.condition_id or ""),
            "side": str(self.side or "").upper(),
            "status": normalize_status(self.status),
            "recovered_source": str(self.recovered_source or ""),
            "recovery_reason": str(self.recovery_reason or ""),
            "payload": dict(self.payload or {}),
            "created_ts": int(self.created_ts or 0),
            "updated_ts": int(self.updated_ts or 0),
        }
