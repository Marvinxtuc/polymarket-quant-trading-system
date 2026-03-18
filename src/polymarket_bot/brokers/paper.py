from __future__ import annotations

from polymarket_bot.brokers.base import Broker
from polymarket_bot.types import ExecutionResult, OpenOrderSnapshot, OrderFillSnapshot, Signal


class PaperBroker(Broker):
    @staticmethod
    def _cancel_result(order_id: str, *, message: str = "paper broker simulated cancel") -> dict[str, object]:
        return {
            "order_id": order_id,
            "status": "canceled",
            "ok": True,
            "message": message,
            "raw": None,
        }

    def startup_checks(self) -> list[dict[str, object]] | None:
        return [
            {
                "name": "paper_mode",
                "status": "PASS",
                "message": "paper broker active; no live exchange prerequisites required",
            }
        ]

    def execute(self, signal: Signal, notional_usd: float) -> ExecutionResult:
        price = max(0.01, min(0.99, signal.price_hint))
        return ExecutionResult(
            ok=True,
            broker_order_id=f"paper-{signal.token_id[:8]}",
            message="paper fill",
            filled_notional=notional_usd,
            filled_price=price,
            status="filled",
            requested_notional=notional_usd,
            requested_price=price,
        )

    def cancel_order(self, order_id: str) -> dict[str, object] | None:
        normalized = str(order_id or "").strip()
        if not normalized:
            return None
        return self._cancel_result(normalized)

    def cancel_orders(self, order_ids: list[str]) -> list[dict[str, object]] | None:
        normalized_ids = [str(order_id).strip() for order_id in order_ids if str(order_id).strip()]
        if not normalized_ids:
            return []
        return [self._cancel_result(order_id) for order_id in normalized_ids]

    def cancel_open_orders(self) -> list[dict[str, object]] | None:
        return []

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
