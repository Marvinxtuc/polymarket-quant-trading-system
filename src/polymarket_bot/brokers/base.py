from __future__ import annotations

from abc import ABC, abstractmethod

from polymarket_bot.types import ExecutionResult, Signal


class Broker(ABC):
    @abstractmethod
    def execute(self, signal: Signal, notional_usd: float) -> ExecutionResult:
        raise NotImplementedError
