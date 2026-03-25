from __future__ import annotations

import time

from polymarket_bot.brokers.base import Broker
from polymarket_bot.config import Settings
from polymarket_bot.types import BrokerOrderEvent, ExecutionResult, OpenOrderSnapshot, OrderFillSnapshot, OrderStatusSnapshot, Signal


class PaperBroker(Broker):
    def __init__(self, settings: Settings | None = None):
        self.settings = settings
        self._order_seq = 0
        self._live_like_enabled = bool(getattr(settings, "paper_live_like_enabled", False))
        self._fill_delay_seconds = int(getattr(settings, "paper_fill_delay_seconds", 0) or 0)
        self._partial_fill_ratio = float(
            getattr(
                settings,
                "paper_partial_fill_fraction",
                getattr(settings, "paper_partial_fill_ratio", 1.0),
            )
        )
        self._completion_delay_seconds = int(getattr(settings, "paper_fill_complete_delay_seconds", 0) or 0)
        self._cancel_fail_once = bool(
            getattr(
                settings,
                "paper_cancel_reject_once",
                getattr(settings, "paper_cancel_fail_once", False),
            )
        )
        self._orders: dict[str, dict[str, object]] = {}
        self._fills: list[OrderFillSnapshot] = []
        self._events: list[BrokerOrderEvent] = []

    def supports_dry_run_pending_reconcile(self) -> bool:
        return bool(self._live_like_enabled)

    @staticmethod
    def _cancel_result(
        order_id: str,
        *,
        ok: bool = True,
        status: str = "canceled",
        message: str = "paper broker simulated cancel",
    ) -> dict[str, object]:
        return {
            "order_id": order_id,
            "status": status,
            "ok": ok,
            "message": message,
            "raw": None,
        }

    @staticmethod
    def _clamp_price(value: float) -> float:
        return max(0.01, min(0.99, float(value or 0.5)))

    @staticmethod
    def _live_order_status(order: dict[str, object]) -> str:
        matched_size = float(order.get("matched_size") or 0.0)
        remaining_size = max(0.0, float(order.get("remaining_size") or 0.0))
        status = str(order.get("status") or "live")
        if status in {"canceled", "failed", "rejected", "filled"}:
            return status
        if matched_size > 0.0 and remaining_size > 1e-9:
            return "partially_filled"
        if matched_size > 0.0 and remaining_size <= 1e-9:
            return "filled"
        return "live"

    def _next_order_id(self, token_id: str) -> str:
        self._order_seq += 1
        return f"paper-{token_id[:8]}-{self._order_seq:04d}"

    def _record_status_event(self, order: dict[str, object], *, timestamp: int, status: str, message: str = "") -> None:
        self._events.append(
            BrokerOrderEvent(
                event_type="status",
                order_id=str(order.get("order_id") or ""),
                token_id=str(order.get("token_id") or ""),
                side=str(order.get("side") or ""),
                timestamp=int(timestamp),
                status=status,
                matched_notional=float(order.get("matched_notional") or 0.0),
                matched_size=float(order.get("matched_size") or 0.0),
                avg_fill_price=float(order.get("price") or 0.0),
                market_slug=str(order.get("market_slug") or ""),
                outcome=str(order.get("outcome") or ""),
                message=message,
            )
        )

    def _advance_orders(self, *, now: int | None = None) -> None:
        if not self._live_like_enabled:
            return
        current_ts = int(now or time.time())
        fill_ratio = max(0.0, min(1.0, self._partial_fill_ratio))
        for order in self._orders.values():
            if str(order.get("status") or "") in {"canceled", "failed", "rejected", "filled"}:
                continue
            fill_stage = int(order.get("fill_stage") or 0)
            fill_ts = int(order.get("created_ts") or current_ts) + self._fill_delay_seconds
            if fill_stage == 0 and current_ts < fill_ts:
                continue
            if fill_ratio <= 0.0:
                continue
            requested_notional = float(order.get("requested_notional") or 0.0)
            original_size = float(order.get("original_size") or 0.0)
            if fill_stage == 0:
                fill_notional = requested_notional * fill_ratio
                fill_size = original_size * fill_ratio
                if fill_notional <= 0.0 or fill_size <= 0.0:
                    continue
                order["fill_stage"] = 1
                order["matched_notional"] = fill_notional
                order["matched_size"] = fill_size
                order["remaining_size"] = max(0.0, original_size - fill_size)
                order["status"] = self._live_order_status(order)
                fill = OrderFillSnapshot(
                    order_id=str(order.get("order_id") or ""),
                    token_id=str(order.get("token_id") or ""),
                    side=str(order.get("side") or ""),
                    price=float(order.get("price") or 0.0),
                    size=fill_size,
                    timestamp=fill_ts,
                    tx_hash=f"paper-fill-{order.get('order_id')}-1",
                    market_slug=str(order.get("market_slug") or ""),
                    outcome=str(order.get("outcome") or ""),
                )
                self._fills.append(fill)
                self._events.append(
                    BrokerOrderEvent(
                        event_type="fill",
                        order_id=fill.order_id,
                        token_id=fill.token_id,
                        side=fill.side,
                        timestamp=fill.timestamp,
                        matched_notional=fill.notional,
                        matched_size=fill.size,
                        avg_fill_price=fill.price,
                        tx_hash=fill.tx_hash,
                        market_slug=fill.market_slug,
                        outcome=fill.outcome,
                    )
                )
                self._record_status_event(
                    order,
                    timestamp=fill_ts + 1,
                    status=str(order.get("status") or ""),
                    message="paper live-like fill emitted",
                )
                continue

            if (
                fill_stage == 1
                and str(order.get("status") or "") == "partially_filled"
                and self._completion_delay_seconds > 0
                and current_ts >= fill_ts + self._completion_delay_seconds
            ):
                remaining_size = max(0.0, float(order.get("remaining_size") or 0.0))
                remaining_notional = remaining_size * float(order.get("price") or 0.0)
                if remaining_notional <= 0.0 or remaining_size <= 0.0:
                    continue
                order["fill_stage"] = 2
                order["matched_notional"] = requested_notional
                order["matched_size"] = original_size
                order["remaining_size"] = 0.0
                order["status"] = "filled"
                fill = OrderFillSnapshot(
                    order_id=str(order.get("order_id") or ""),
                    token_id=str(order.get("token_id") or ""),
                    side=str(order.get("side") or ""),
                    price=float(order.get("price") or 0.0),
                    size=remaining_size,
                    timestamp=fill_ts + self._completion_delay_seconds,
                    tx_hash=f"paper-fill-{order.get('order_id')}-2",
                    market_slug=str(order.get("market_slug") or ""),
                    outcome=str(order.get("outcome") or ""),
                )
                self._fills.append(fill)
                self._events.append(
                    BrokerOrderEvent(
                        event_type="fill",
                        order_id=fill.order_id,
                        token_id=fill.token_id,
                        side=fill.side,
                        timestamp=fill.timestamp,
                        matched_notional=fill.notional,
                        matched_size=fill.size,
                        avg_fill_price=fill.price,
                        tx_hash=fill.tx_hash,
                        market_slug=fill.market_slug,
                        outcome=fill.outcome,
                    )
                )
                self._record_status_event(
                    order,
                    timestamp=fill.timestamp + 1,
                    status="filled",
                    message="paper live-like completion fill emitted",
                )

    def startup_checks(self) -> list[dict[str, object]] | None:
        if self._live_like_enabled:
            return [
                {
                    "name": "paper_mode",
                    "status": "PASS",
                    "message": "paper broker active in live-like mode; pending/reconcile simulation enabled",
                }
            ]
        return [
            {
                "name": "paper_mode",
                "status": "PASS",
                "message": "paper broker active; no live exchange prerequisites required",
            }
        ]

    def execute(self, signal: Signal, notional_usd: float) -> ExecutionResult:
        price = self._clamp_price(signal.price_hint)
        if not self._live_like_enabled:
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

        created_ts = int(time.time())
        order_id = self._next_order_id(str(signal.token_id or "paper"))
        original_size = max(0.0, float(notional_usd or 0.0) / max(0.01, price))
        order = {
            "order_id": order_id,
            "token_id": str(signal.token_id or ""),
            "condition_id": str(signal.condition_id or ""),
            "market_slug": str(signal.market_slug or ""),
            "outcome": str(signal.outcome or ""),
            "side": str(signal.side or ""),
            "status": "live",
            "price": price,
            "requested_notional": float(notional_usd or 0.0),
            "original_size": original_size,
            "matched_notional": 0.0,
            "matched_size": 0.0,
            "remaining_size": original_size,
            "created_ts": created_ts,
            "fill_stage": 0,
            "cancel_failed_once": False,
        }
        self._orders[order_id] = order
        self._record_status_event(order, timestamp=created_ts, status="live", message="paper live-like order posted")
        return ExecutionResult(
            ok=True,
            broker_order_id=order_id,
            message="paper live-like order posted",
            filled_notional=0.0,
            filled_price=0.0,
            status="live",
            requested_notional=notional_usd,
            requested_price=price,
        )

    def cancel_order(self, order_id: str) -> dict[str, object] | None:
        normalized = str(order_id or "").strip()
        if not normalized:
            return None
        if not self._live_like_enabled:
            return self._cancel_result(normalized)

        self._advance_orders()
        order = self._orders.get(normalized)
        if order is None:
            return self._cancel_result(normalized)
        if self._cancel_fail_once and not bool(order.get("cancel_failed_once")):
            order["cancel_failed_once"] = True
            self._record_status_event(order, timestamp=int(time.time()), status=self._live_order_status(order), message="paper simulated cancel rejection")
            return self._cancel_result(
                normalized,
                ok=False,
                status="failed",
                message="paper simulated cancel rejection",
            )
        order["status"] = "canceled"
        order["remaining_size"] = 0.0
        self._record_status_event(order, timestamp=int(time.time()), status="canceled", message="paper simulated cancel")
        return self._cancel_result(normalized)

    def cancel_orders(self, order_ids: list[str]) -> list[dict[str, object]] | None:
        normalized_ids = [str(order_id).strip() for order_id in order_ids if str(order_id).strip()]
        if not normalized_ids:
            return []
        return [row for row in (self.cancel_order(order_id) for order_id in normalized_ids) if row is not None]

    def cancel_open_orders(self) -> list[dict[str, object]] | None:
        if not self._live_like_enabled:
            return []
        self._advance_orders()
        open_order_ids = [order.order_id for order in self.list_open_orders() or []]
        return self.cancel_orders(open_order_ids) or []

    def heartbeat(self, order_ids: list[str]) -> bool:
        if not self._live_like_enabled:
            return False
        self._advance_orders()
        normalized_ids = [str(order_id).strip() for order_id in order_ids if str(order_id).strip()]
        return all(order_id in self._orders for order_id in normalized_ids)

    def get_order_status(self, order_id: str) -> OrderStatusSnapshot | None:
        normalized = str(order_id or "").strip()
        if not normalized:
            return None
        self._advance_orders()
        order = self._orders.get(normalized)
        if order is None:
            return None
        return OrderStatusSnapshot(
            order_id=normalized,
            status=self._live_order_status(order),
            matched_notional=float(order.get("matched_notional") or 0.0),
            matched_size=float(order.get("matched_size") or 0.0),
            avg_fill_price=float(order.get("price") or 0.0),
            original_size=float(order.get("original_size") or 0.0),
            remaining_size=max(0.0, float(order.get("remaining_size") or 0.0)),
            message="paper live-like status",
        )

    def list_open_orders(self) -> list[OpenOrderSnapshot] | None:
        if not self._live_like_enabled:
            return None
        self._advance_orders()
        rows: list[OpenOrderSnapshot] = []
        for order in self._orders.values():
            lifecycle_status = self._live_order_status(order)
            remaining_size = max(0.0, float(order.get("remaining_size") or 0.0))
            if lifecycle_status in {"canceled", "failed", "rejected", "filled"} or remaining_size <= 1e-9:
                continue
            rows.append(
                OpenOrderSnapshot(
                    order_id=str(order.get("order_id") or ""),
                    token_id=str(order.get("token_id") or ""),
                    side=str(order.get("side") or ""),
                    status=lifecycle_status,
                    price=float(order.get("price") or 0.0),
                    original_size=float(order.get("original_size") or 0.0),
                    matched_size=float(order.get("matched_size") or 0.0),
                    remaining_size=remaining_size,
                    created_ts=int(order.get("created_ts") or 0),
                    condition_id=str(order.get("condition_id") or ""),
                    market_slug=str(order.get("market_slug") or ""),
                    outcome=str(order.get("outcome") or ""),
                    message="paper live-like open order",
                )
            )
        return rows

    def list_recent_fills(
        self,
        *,
        since_ts: int = 0,
        order_ids: list[str] | None = None,
        limit: int = 200,
    ) -> list[OrderFillSnapshot] | None:
        if not self._live_like_enabled:
            return None
        self._advance_orders()
        normalized_ids = {str(order_id).strip() for order_id in (order_ids or []) if str(order_id).strip()}
        rows = [
            fill
            for fill in self._fills
            if int(fill.timestamp or 0) >= int(since_ts or 0)
            and (not normalized_ids or str(fill.order_id or "").strip() in normalized_ids)
        ]
        return rows[: max(0, int(limit or 0))]

    def list_order_events(
        self,
        *,
        since_ts: int = 0,
        order_ids: list[str] | None = None,
        limit: int = 200,
    ) -> list[BrokerOrderEvent] | None:
        if not self._live_like_enabled:
            return None
        self._advance_orders()
        normalized_ids = {str(order_id).strip() for order_id in (order_ids or []) if str(order_id).strip()}
        rows = [
            event
            for event in self._events
            if int(event.timestamp or 0) >= int(since_ts or 0)
            and (not normalized_ids or str(event.order_id or "").strip() in normalized_ids)
        ]
        return rows[: max(0, int(limit or 0))]
