from __future__ import annotations

from polymarket_bot.brokers.base import Broker
from polymarket_bot.types import ExecutionResult, Signal


class PaperBroker(Broker):
    def execute(self, signal: Signal, notional_usd: float) -> ExecutionResult:
        price = max(0.01, min(0.99, signal.price_hint))
        return ExecutionResult(
            ok=True,
            broker_order_id=f"paper-{signal.token_id[:8]}",
            message="paper fill",
            filled_notional=notional_usd,
            filled_price=price,
        )
