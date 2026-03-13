from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

Side = Literal["BUY", "SELL"]


@dataclass(slots=True)
class Signal:
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


@dataclass(slots=True)
class RiskDecision:
    allowed: bool
    reason: str
    max_notional: float


@dataclass(slots=True)
class ExecutionResult:
    ok: bool
    broker_order_id: str | None
    message: str
    filled_notional: float
    filled_price: float
