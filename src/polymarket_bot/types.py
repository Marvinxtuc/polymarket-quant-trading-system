from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

Side = Literal["BUY", "SELL"]
DecisionMode = Literal["manual", "semi_auto", "auto"]
CandidateAction = Literal["ignore", "watch", "buy_small", "buy_normal", "follow", "close_partial", "close_all"]


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
class CandidateReasonFactor:
    key: str
    label: str
    value: str
    direction: str = "neutral"
    weight: float = 0.0
    detail: str = ""


@dataclass(slots=True)
class Candidate:
    id: str
    signal_id: str
    trace_id: str
    wallet: str
    market_slug: str
    token_id: str
    outcome: str
    side: Side
    confidence: float
    wallet_tag: str = ""
    wallet_score: float = 0.0
    wallet_tier: str = "LOW"
    condition_id: str = ""
    trigger_type: str = ""
    source_wallet_count: int = 1
    observed_notional: float = 0.0
    observed_size: float = 0.0
    source_avg_price: float = 0.0
    current_best_bid: float | None = None
    current_best_ask: float | None = None
    current_midpoint: float | None = None
    spread_pct: float | None = None
    momentum_5m: float | None = None
    momentum_30m: float | None = None
    chase_pct: float | None = None
    market_time_source: str = "unknown"
    market_metadata_hit: bool = False
    market_tag: str = ""
    resolution_bucket: str = ""
    score: float = 0.0
    suggested_action: str = ""
    skip_reason: str | None = None
    recommendation_reason: str = ""
    explanation: list[str] = field(default_factory=list)
    reason_factors: list[CandidateReasonFactor] = field(default_factory=list)
    has_existing_position: bool = False
    existing_position_conflict: bool = False
    existing_position_notional: float = 0.0
    block_reason: str = ""
    block_layer: str = ""
    status: str = "pending"
    lifecycle_state: str = "active"
    selected_action: str = ""
    created_ts: int = 0
    expires_ts: int = 0
    updated_ts: int = 0
    note: str = ""
    signal_snapshot: dict[str, object] = field(default_factory=dict)
    topic_snapshot: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class WalletProfile:
    wallet: str
    tag: str = ""
    trust_score: float = 0.0
    followability_score: float = 0.0
    avg_hold_minutes: float | None = None
    category: str = ""
    enabled: bool = True
    notes: str = ""
    updated_ts: int = 0
    payload: dict[str, object] = field(default_factory=dict)


@dataclass(slots=True)
class JournalEntry:
    candidate_id: str
    action: str
    rationale: str = ""
    result_tag: str | None = None
    created_ts: int = 0
    market_slug: str = ""
    wallet: str = ""
    pnl_realized: float | None = None
    payload: dict[str, object] = field(default_factory=dict)


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
