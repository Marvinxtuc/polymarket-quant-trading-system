from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal

Side = Literal["BUY", "SELL"]


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
