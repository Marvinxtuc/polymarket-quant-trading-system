from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

Side = Literal["BUY", "SELL"]


def _broker_lifecycle_status(
    status: str,
    *,
    has_fill: bool,
    remaining_size: float = 0.0,
) -> str:
    normalized = str(status or "").strip().lower()
    if normalized == "cancelled":
        normalized = "canceled"
    if normalized in {"matched", "filled", "mined", "confirmed"}:
        return "filled"
    if normalized == "partially_filled":
        return "partially_filled"
    if normalized in {"canceled", "failed", "rejected", "unmatched"}:
        return normalized
    if normalized in {"submitted", "posted", "open", "live", "delayed"}:
        if has_fill and remaining_size > 1e-9:
            return "partially_filled"
        if has_fill and normalized in {"submitted", "posted", "open", "live", "delayed"}:
            return "partially_filled"
        return normalized
    if has_fill and remaining_size <= 1e-9:
        return "filled"
    if has_fill:
        return "partially_filled"
    return normalized or "posted"


@dataclass(slots=True)
class Signal:
    signal_id: str
    trace_id: str
    wallet: str
    market_slug: str
    token_id: str
    outcome: str
    side: Side
    confidence: float
    price_hint: float
    observed_size: float
    observed_notional: float
    timestamp: datetime
    condition_id: str = ""
    wallet_score: float = 0.0
    wallet_tier: str = "LOW"
    wallet_score_summary: str = ""
    topic_key: str = ""
    topic_label: str = ""
    topic_sample_count: int = 0
    topic_win_rate: float = 0.0
    topic_roi: float = 0.0
    topic_resolved_win_rate: float = 0.0
    topic_score_summary: str = ""
    topic_bias: str = "neutral"
    topic_multiplier: float = 1.0
    exit_fraction: float = 0.0
    exit_reason: str = ""
    cross_wallet_exit: bool = False
    exit_wallet_count: int = 0
    position_action: str = ""
    position_action_label: str = ""


@dataclass(slots=True)
class RiskDecision:
    allowed: bool
    reason: str
    max_notional: float
    snapshot: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class ExecutionResult:
    ok: bool
    broker_order_id: str | None
    message: str
    filled_notional: float
    filled_price: float
    status: str = ""
    requested_notional: float = 0.0
    requested_price: float = 0.0
    metadata: dict[str, object] = field(default_factory=dict)

    @property
    def normalized_status(self) -> str:
        status = str(self.status or "").strip().lower()
        if status:
            return status
        if not self.ok:
            return "rejected"
        if self.has_fill:
            return "filled"
        return "posted"

    @property
    def lifecycle_status(self) -> str:
        return _broker_lifecycle_status(
            self.normalized_status,
            has_fill=self.has_fill,
            remaining_size=0.0,
        )

    @property
    def has_fill(self) -> bool:
        return self.filled_notional > 0.0 and self.filled_price > 0.0

    @property
    def is_pending(self) -> bool:
        return self.ok and not self.has_fill


@dataclass(slots=True)
class OrderStatusSnapshot:
    order_id: str
    status: str
    matched_notional: float = 0.0
    matched_size: float = 0.0
    avg_fill_price: float = 0.0
    original_size: float = 0.0
    remaining_size: float = 0.0
    message: str = ""

    @property
    def normalized_status(self) -> str:
        return str(self.status or "").strip().lower()

    @property
    def lifecycle_status(self) -> str:
        return _broker_lifecycle_status(
            self.normalized_status,
            has_fill=self.has_fill,
            remaining_size=max(0.0, float(self.remaining_size or 0.0)),
        )

    @property
    def has_fill(self) -> bool:
        return self.matched_notional > 0.0 or self.matched_size > 0.0

    @property
    def is_terminal(self) -> bool:
        return self.lifecycle_status in {
            "filled",
            "canceled",
            "failed",
            "rejected",
            "unmatched",
        }

    @property
    def is_failed(self) -> bool:
        return self.lifecycle_status in {
            "canceled",
            "failed",
            "rejected",
            "unmatched",
        }


@dataclass(slots=True)
class OpenOrderSnapshot:
    order_id: str
    token_id: str
    side: str
    status: str
    price: float
    original_size: float
    matched_size: float = 0.0
    remaining_size: float = 0.0
    created_ts: int = 0
    condition_id: str = ""
    market_slug: str = ""
    outcome: str = ""
    message: str = ""

    @property
    def normalized_status(self) -> str:
        return str(self.status or "").strip().lower()

    @property
    def lifecycle_status(self) -> str:
        return _broker_lifecycle_status(
            self.normalized_status,
            has_fill=self.matched_size > 0.0,
            remaining_size=max(0.0, float(self.remaining_size or 0.0)),
        )

    @property
    def matched_notional(self) -> float:
        return max(0.0, float(self.matched_size or 0.0) * max(0.0, float(self.price or 0.0)))

    @property
    def requested_notional(self) -> float:
        return max(0.0, float(self.original_size or 0.0) * max(0.0, float(self.price or 0.0)))


@dataclass(slots=True)
class OrderFillSnapshot:
    order_id: str
    token_id: str
    side: str
    price: float
    size: float
    timestamp: int
    tx_hash: str = ""
    market_slug: str = ""
    outcome: str = ""

    @property
    def notional(self) -> float:
        return max(0.0, float(self.size or 0.0) * max(0.0, float(self.price or 0.0)))


@dataclass(slots=True)
class BrokerOrderEvent:
    event_type: str
    order_id: str
    token_id: str
    side: str
    timestamp: int
    status: str = ""
    matched_notional: float = 0.0
    matched_size: float = 0.0
    avg_fill_price: float = 0.0
    tx_hash: str = ""
    market_slug: str = ""
    outcome: str = ""
    message: str = ""

    @property
    def normalized_event_type(self) -> str:
        return str(self.event_type or "").strip().lower()

    @property
    def is_fill(self) -> bool:
        return self.normalized_event_type == "fill"

    @property
    def is_status(self) -> bool:
        return self.normalized_event_type == "status"
