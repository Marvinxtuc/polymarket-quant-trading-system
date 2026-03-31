from __future__ import annotations

from abc import ABC, abstractmethod

from polymarket_bot.types import BrokerOrderEvent, ExecutionResult, OpenOrderSnapshot, OrderFillSnapshot, OrderStatusSnapshot, Signal


class Broker(ABC):
    @abstractmethod
    def execute(self, signal: Signal, notional_usd: float, *, strategy_order_uuid: str | None = None) -> ExecutionResult:
        raise NotImplementedError

    def supports_dry_run_pending_reconcile(self) -> bool:
        return False

    def cancel_order(self, order_id: str) -> dict[str, object] | None:
        return None

    def cancel_orders(self, order_ids: list[str]) -> list[dict[str, object]] | None:
        return None

    def cancel_open_orders(self) -> list[dict[str, object]] | None:
        return None

    def get_order_status(self, order_id: str) -> OrderStatusSnapshot | None:
        return None

    def heartbeat(self, order_ids: list[str]) -> bool:
        return False

    def list_open_orders(self) -> list[OpenOrderSnapshot] | None:
        return None

    def list_recent_fills(
        self,
        *,
        since_ts: int = 0,
        order_ids: list[str] | None = None,
        limit: int = 200,
    ) -> list[OrderFillSnapshot] | None:
        return None

    def list_order_events(
        self,
        *,
        since_ts: int = 0,
        order_ids: list[str] | None = None,
        limit: int = 200,
    ) -> list[BrokerOrderEvent] | None:
        return None

    def startup_checks(self) -> list[dict[str, object]] | None:
        return None

    def security_summary(self) -> dict[str, object] | None:
        return None

    def close(self) -> None:
        return None
