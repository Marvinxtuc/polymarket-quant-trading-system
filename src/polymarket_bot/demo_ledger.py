from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal


OrderSide = Literal["BUY", "SELL"]
OrderStatus = Literal["SUBMITTED", "PARTIAL_FILL", "FILLED", "REJECTED", "CANCELED"]


@dataclass(frozen=True)
class LedgerOrder:
    order_id: str
    intent_id: str
    market_id: str
    side: OrderSide
    quantity: float
    limit_price: float
    status: OrderStatus
    accepted_quantity: float
    filled_quantity: float
    remaining_quantity: float
    rejected_reason: str
    event_time: int
    record_time: int


@dataclass(frozen=True)
class LedgerFill:
    fill_id: str
    order_id: str
    intent_id: str
    market_id: str
    side: OrderSide
    fill_quantity: float
    fill_price: float
    event_time: int
    fill_time: int
    record_time: int


@dataclass(frozen=True)
class PositionRecord:
    market_id: str
    quantity: float
    avg_price: float
    mark_price: float
    realized_pnl: float
    unrealized_pnl: float
    updated_at: int


@dataclass(frozen=True)
class EquitySnapshot:
    event_time: int
    record_time: int
    cash_balance_usd: float
    positions_value_usd: float
    equity: float
    realized_pnl: float
    unrealized_pnl: float
    fee_usd: float


@dataclass(frozen=True)
class ScenarioSpec:
    scenario_id: str
    expected: str
    initial_cash_balance_usd: float
    initial_positions: dict[str, PositionRecord]
    orders: list[LedgerOrder]
    fills: list[LedgerFill]
    attempted_fills: list[LedgerFill]
    final_positions: dict[str, PositionRecord]
    equity_snapshots: list[EquitySnapshot]
    positions_by_tick: list[dict[str, object]]
    state_projection: dict[str, object]
    expected_failed_assertions: tuple[str, ...] = ()


class DemoLedgerIsolationGuard:
    def __init__(self, base_dir: Path, *, scenario_id: str, run_id: str) -> None:
        self.base_dir = base_dir.expanduser().resolve()
        expected = Path("runtime") / "demo_ledger" / scenario_id / run_id
        if expected.as_posix() not in self.base_dir.as_posix():
            raise RuntimeError(
                f"demo ledger runtime path must live under runtime/demo_ledger/<scenario_id>/<run_id>: {self.base_dir}"
            )

    def ensure_file_path(self, path: Path) -> Path:
        resolved = path.expanduser().resolve()
        try:
            resolved.relative_to(self.base_dir)
        except ValueError as exc:
            raise RuntimeError(f"path escapes demo ledger runtime root: {resolved}") from exc
        return resolved


class TimeSemanticValidator:
    def validate_fill(self, fill: LedgerFill) -> tuple[bool, str]:
        if not (fill.event_time <= fill.fill_time <= fill.record_time):
            return False, "fill_time_order"
        return True, ""

    def validate_fill_sequence(self, fills: list[LedgerFill]) -> tuple[bool, str]:
        grouped: dict[str, list[LedgerFill]] = {}
        for fill in fills:
            grouped.setdefault(fill.order_id, []).append(fill)
        for order_id, order_fills in grouped.items():
            ordered = sorted(order_fills, key=lambda item: (item.fill_time, item.record_time, item.fill_id))
            for idx in range(1, len(ordered)):
                prev = ordered[idx - 1]
                current = ordered[idx]
                if current.fill_time < prev.fill_time or current.record_time < prev.record_time:
                    return False, f"fill_sequence:{order_id}"
        return True, ""


def _round4(value: float) -> float:
    return round(float(value), 4)


def _positions_value(positions: dict[str, PositionRecord]) -> float:
    return _round4(sum(position.quantity * position.mark_price for position in positions.values()))


def _signed_fill_value(fill: LedgerFill) -> float:
    gross = fill.fill_quantity * fill.fill_price
    return -gross if fill.side == "BUY" else gross


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def _scenario_state_projection(
    scenario_id: str,
    summary: dict[str, object],
    orders: list[LedgerOrder],
    positions: dict[str, PositionRecord],
    *,
    force_projection_mismatch: bool = False,
) -> dict[str, object]:
    projected_summary = dict(summary)
    if force_projection_mismatch:
        projected_summary["equity"] = _round4(float(projected_summary["equity"]) + 1.25)
    return {
        "active_namespace": "demo",
        "namespaces": {
            "demo": {
                "meta": {
                    "namespace": "demo",
                    "mode": "demo_ledger",
                    "scenario_id": scenario_id,
                    "status": "stopped",
                },
                "summary": projected_summary,
                "orders": {"recent": [order.__dict__ for order in orders]},
                "positions": [position.__dict__ for position in positions.values()],
            }
        },
    }


def _build_scenarios(base_event_time: int) -> list[ScenarioSpec]:
    full_open_initial_positions: dict[str, PositionRecord] = {}
    full_open_final_positions = {
        "mkt_full_open": PositionRecord(
            market_id="mkt_full_open",
            quantity=10.0,
            avg_price=0.5,
            mark_price=0.5,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            updated_at=base_event_time + 2,
        )
    }
    partial_final_positions = {
        "mkt_partial": PositionRecord(
            market_id="mkt_partial",
            quantity=10.0,
            avg_price=0.512,
            mark_price=0.52,
            realized_pnl=0.0,
            unrealized_pnl=0.08,
            updated_at=base_event_time + 12,
        )
    }
    mark_only_initial_positions = {
        "mkt_mark": PositionRecord(
            market_id="mkt_mark",
            quantity=10.0,
            avg_price=0.5,
            mark_price=0.5,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            updated_at=base_event_time + 20,
        )
    }
    mark_only_final_positions = {
        "mkt_mark": PositionRecord(
            market_id="mkt_mark",
            quantity=10.0,
            avg_price=0.5,
            mark_price=0.62,
            realized_pnl=0.0,
            unrealized_pnl=1.2,
            updated_at=base_event_time + 21,
        )
    }
    close_initial_positions = {
        "mkt_close": PositionRecord(
            market_id="mkt_close",
            quantity=10.0,
            avg_price=0.5,
            mark_price=0.5,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            updated_at=base_event_time + 30,
        )
    }
    close_final_positions = {
        "mkt_close": PositionRecord(
            market_id="mkt_close",
            quantity=0.0,
            avg_price=0.0,
            mark_price=0.7,
            realized_pnl=2.0,
            unrealized_pnl=0.0,
            updated_at=base_event_time + 32,
        )
    }
    merge_final_positions = {
        "mkt_merge": PositionRecord(
            market_id="mkt_merge",
            quantity=5.0,
            avg_price=0.42,
            mark_price=0.46,
            realized_pnl=0.0,
            unrealized_pnl=0.2,
            updated_at=base_event_time + 52,
        )
    }
    flat_zero_initial_positions = {
        "mkt_flat": PositionRecord(
            market_id="mkt_flat",
            quantity=5.0,
            avg_price=0.42,
            mark_price=0.46,
            realized_pnl=0.0,
            unrealized_pnl=0.2,
            updated_at=base_event_time + 60,
        )
    }
    flat_zero_final_positions = {
        "mkt_flat": PositionRecord(
            market_id="mkt_flat",
            quantity=0.0,
            avg_price=0.0,
            mark_price=0.44,
            realized_pnl=0.1,
            unrealized_pnl=0.0,
            updated_at=base_event_time + 62,
        )
    }
    invalid_time_final_positions = {
        "mkt_bad_time": PositionRecord(
            market_id="mkt_bad_time",
            quantity=10.0,
            avg_price=0.5,
            mark_price=0.5,
            realized_pnl=0.0,
            unrealized_pnl=0.0,
            updated_at=base_event_time + 70,
        )
    }

    return [
        ScenarioSpec(
            scenario_id="single_full_fill_open",
            expected="PASS",
            initial_cash_balance_usd=10000.0,
            initial_positions=full_open_initial_positions,
            orders=[
                LedgerOrder(
                    order_id="ord_full_open_01",
                    intent_id="intent_full_open_01",
                    market_id="mkt_full_open",
                    side="BUY",
                    quantity=10.0,
                    limit_price=0.5,
                    status="FILLED",
                    accepted_quantity=10.0,
                    filled_quantity=10.0,
                    remaining_quantity=0.0,
                    rejected_reason="",
                    event_time=base_event_time + 1,
                    record_time=base_event_time + 2,
                )
            ],
            fills=[
                LedgerFill(
                    fill_id="fill_full_open_01",
                    order_id="ord_full_open_01",
                    intent_id="intent_full_open_01",
                    market_id="mkt_full_open",
                    side="BUY",
                    fill_quantity=10.0,
                    fill_price=0.5,
                    event_time=base_event_time + 1,
                    fill_time=base_event_time + 2,
                    record_time=base_event_time + 2,
                )
            ],
            attempted_fills=[
                LedgerFill(
                    fill_id="fill_full_open_01",
                    order_id="ord_full_open_01",
                    intent_id="intent_full_open_01",
                    market_id="mkt_full_open",
                    side="BUY",
                    fill_quantity=10.0,
                    fill_price=0.5,
                    event_time=base_event_time + 1,
                    fill_time=base_event_time + 2,
                    record_time=base_event_time + 2,
                )
            ],
            final_positions=full_open_final_positions,
            equity_snapshots=[
                EquitySnapshot(
                    event_time=base_event_time + 2,
                    record_time=base_event_time + 2,
                    cash_balance_usd=9995.0,
                    positions_value_usd=5.0,
                    equity=10000.0,
                    realized_pnl=0.0,
                    unrealized_pnl=0.0,
                    fee_usd=0.0,
                )
            ],
            positions_by_tick=[
                {
                    "tick": 1,
                    "event_time": base_event_time + 2,
                    "positions": [full_open_final_positions["mkt_full_open"].__dict__],
                }
            ],
            state_projection={},
        ),
        ScenarioSpec(
            scenario_id="partial_then_full",
            expected="PASS",
            initial_cash_balance_usd=10000.0,
            initial_positions={},
            orders=[
                LedgerOrder(
                    order_id="ord_partial_01",
                    intent_id="intent_partial_01",
                    market_id="mkt_partial",
                    side="BUY",
                    quantity=10.0,
                    limit_price=0.52,
                    status="FILLED",
                    accepted_quantity=10.0,
                    filled_quantity=10.0,
                    remaining_quantity=0.0,
                    rejected_reason="",
                    event_time=base_event_time + 10,
                    record_time=base_event_time + 12,
                )
            ],
            fills=[
                LedgerFill(
                    fill_id="fill_partial_01",
                    order_id="ord_partial_01",
                    intent_id="intent_partial_01",
                    market_id="mkt_partial",
                    side="BUY",
                    fill_quantity=4.0,
                    fill_price=0.5,
                    event_time=base_event_time + 10,
                    fill_time=base_event_time + 11,
                    record_time=base_event_time + 11,
                ),
                LedgerFill(
                    fill_id="fill_partial_02",
                    order_id="ord_partial_01",
                    intent_id="intent_partial_01",
                    market_id="mkt_partial",
                    side="BUY",
                    fill_quantity=6.0,
                    fill_price=0.52,
                    event_time=base_event_time + 10,
                    fill_time=base_event_time + 12,
                    record_time=base_event_time + 12,
                ),
            ],
            attempted_fills=[
                LedgerFill(
                    fill_id="fill_partial_01",
                    order_id="ord_partial_01",
                    intent_id="intent_partial_01",
                    market_id="mkt_partial",
                    side="BUY",
                    fill_quantity=4.0,
                    fill_price=0.5,
                    event_time=base_event_time + 10,
                    fill_time=base_event_time + 11,
                    record_time=base_event_time + 11,
                ),
                LedgerFill(
                    fill_id="fill_partial_02",
                    order_id="ord_partial_01",
                    intent_id="intent_partial_01",
                    market_id="mkt_partial",
                    side="BUY",
                    fill_quantity=6.0,
                    fill_price=0.52,
                    event_time=base_event_time + 10,
                    fill_time=base_event_time + 12,
                    record_time=base_event_time + 12,
                ),
            ],
            final_positions=partial_final_positions,
            equity_snapshots=[
                EquitySnapshot(
                    event_time=base_event_time + 11,
                    record_time=base_event_time + 11,
                    cash_balance_usd=9998.0,
                    positions_value_usd=2.0,
                    equity=10000.0,
                    realized_pnl=0.0,
                    unrealized_pnl=0.0,
                    fee_usd=0.0,
                ),
                EquitySnapshot(
                    event_time=base_event_time + 12,
                    record_time=base_event_time + 12,
                    cash_balance_usd=9994.88,
                    positions_value_usd=5.2,
                    equity=10000.08,
                    realized_pnl=0.0,
                    unrealized_pnl=0.08,
                    fee_usd=0.0,
                ),
            ],
            positions_by_tick=[
                {
                    "tick": 1,
                    "event_time": base_event_time + 11,
                    "positions": [
                        {
                            "market_id": "mkt_partial",
                            "quantity": 4.0,
                            "avg_price": 0.5,
                            "mark_price": 0.5,
                            "realized_pnl": 0.0,
                            "unrealized_pnl": 0.0,
                            "updated_at": base_event_time + 11,
                        }
                    ],
                },
                {
                    "tick": 2,
                    "event_time": base_event_time + 12,
                    "positions": [partial_final_positions["mkt_partial"].__dict__],
                },
            ],
            state_projection={},
        ),
        ScenarioSpec(
            scenario_id="mark_to_market_only",
            expected="PASS",
            initial_cash_balance_usd=9995.0,
            initial_positions=mark_only_initial_positions,
            orders=[],
            fills=[],
            attempted_fills=[],
            final_positions=mark_only_final_positions,
            equity_snapshots=[
                EquitySnapshot(
                    event_time=base_event_time + 21,
                    record_time=base_event_time + 21,
                    cash_balance_usd=9995.0,
                    positions_value_usd=6.2,
                    equity=10001.2,
                    realized_pnl=0.0,
                    unrealized_pnl=1.2,
                    fee_usd=0.0,
                )
            ],
            positions_by_tick=[
                {
                    "tick": 1,
                    "event_time": base_event_time + 21,
                    "positions": [mark_only_final_positions["mkt_mark"].__dict__],
                }
            ],
            state_projection={"force_projection_mismatch": True},
        ),
        ScenarioSpec(
            scenario_id="close_realizes_pnl",
            expected="PASS",
            initial_cash_balance_usd=9995.0,
            initial_positions=close_initial_positions,
            orders=[
                LedgerOrder(
                    order_id="ord_close_01",
                    intent_id="intent_close_01",
                    market_id="mkt_close",
                    side="SELL",
                    quantity=10.0,
                    limit_price=0.7,
                    status="FILLED",
                    accepted_quantity=10.0,
                    filled_quantity=10.0,
                    remaining_quantity=0.0,
                    rejected_reason="",
                    event_time=base_event_time + 31,
                    record_time=base_event_time + 32,
                )
            ],
            fills=[
                LedgerFill(
                    fill_id="fill_close_01",
                    order_id="ord_close_01",
                    intent_id="intent_close_01",
                    market_id="mkt_close",
                    side="SELL",
                    fill_quantity=10.0,
                    fill_price=0.7,
                    event_time=base_event_time + 31,
                    fill_time=base_event_time + 32,
                    record_time=base_event_time + 32,
                )
            ],
            attempted_fills=[
                LedgerFill(
                    fill_id="fill_close_01",
                    order_id="ord_close_01",
                    intent_id="intent_close_01",
                    market_id="mkt_close",
                    side="SELL",
                    fill_quantity=10.0,
                    fill_price=0.7,
                    event_time=base_event_time + 31,
                    fill_time=base_event_time + 32,
                    record_time=base_event_time + 32,
                )
            ],
            final_positions=close_final_positions,
            equity_snapshots=[
                EquitySnapshot(
                    event_time=base_event_time + 32,
                    record_time=base_event_time + 32,
                    cash_balance_usd=10002.0,
                    positions_value_usd=0.0,
                    equity=10002.0,
                    realized_pnl=2.0,
                    unrealized_pnl=0.0,
                    fee_usd=0.0,
                )
            ],
            positions_by_tick=[
                {
                    "tick": 1,
                    "event_time": base_event_time + 32,
                    "positions": [close_final_positions["mkt_close"].__dict__],
                }
            ],
            state_projection={},
        ),
        ScenarioSpec(
            scenario_id="reject_pollution_free",
            expected="PASS",
            initial_cash_balance_usd=10000.0,
            initial_positions={},
            orders=[
                LedgerOrder(
                    order_id="ord_reject_01",
                    intent_id="intent_reject_01",
                    market_id="mkt_reject",
                    side="BUY",
                    quantity=10.0,
                    limit_price=0.5,
                    status="REJECTED",
                    accepted_quantity=0.0,
                    filled_quantity=0.0,
                    remaining_quantity=10.0,
                    rejected_reason="RISK_MAX_OPEN_POSITIONS",
                    event_time=base_event_time + 40,
                    record_time=base_event_time + 40,
                )
            ],
            fills=[],
            attempted_fills=[],
            final_positions={},
            equity_snapshots=[
                EquitySnapshot(
                    event_time=base_event_time + 40,
                    record_time=base_event_time + 40,
                    cash_balance_usd=10000.0,
                    positions_value_usd=0.0,
                    equity=10000.0,
                    realized_pnl=0.0,
                    unrealized_pnl=0.0,
                    fee_usd=0.0,
                )
            ],
            positions_by_tick=[
                {"tick": 1, "event_time": base_event_time + 40, "positions": []}
            ],
            state_projection={},
        ),
        ScenarioSpec(
            scenario_id="multi_fill_merge_single_position",
            expected="PASS",
            initial_cash_balance_usd=10000.0,
            initial_positions={},
            orders=[
                LedgerOrder(
                    order_id="ord_merge_01",
                    intent_id="intent_merge_01",
                    market_id="mkt_merge",
                    side="BUY",
                    quantity=3.0,
                    limit_price=0.4,
                    status="FILLED",
                    accepted_quantity=3.0,
                    filled_quantity=3.0,
                    remaining_quantity=0.0,
                    rejected_reason="",
                    event_time=base_event_time + 50,
                    record_time=base_event_time + 51,
                ),
                LedgerOrder(
                    order_id="ord_merge_02",
                    intent_id="intent_merge_02",
                    market_id="mkt_merge",
                    side="BUY",
                    quantity=2.0,
                    limit_price=0.45,
                    status="FILLED",
                    accepted_quantity=2.0,
                    filled_quantity=2.0,
                    remaining_quantity=0.0,
                    rejected_reason="",
                    event_time=base_event_time + 51,
                    record_time=base_event_time + 52,
                ),
            ],
            fills=[
                LedgerFill(
                    fill_id="fill_merge_01",
                    order_id="ord_merge_01",
                    intent_id="intent_merge_01",
                    market_id="mkt_merge",
                    side="BUY",
                    fill_quantity=3.0,
                    fill_price=0.4,
                    event_time=base_event_time + 50,
                    fill_time=base_event_time + 51,
                    record_time=base_event_time + 51,
                ),
                LedgerFill(
                    fill_id="fill_merge_02",
                    order_id="ord_merge_02",
                    intent_id="intent_merge_02",
                    market_id="mkt_merge",
                    side="BUY",
                    fill_quantity=2.0,
                    fill_price=0.45,
                    event_time=base_event_time + 51,
                    fill_time=base_event_time + 52,
                    record_time=base_event_time + 52,
                ),
            ],
            attempted_fills=[
                LedgerFill(
                    fill_id="fill_merge_01",
                    order_id="ord_merge_01",
                    intent_id="intent_merge_01",
                    market_id="mkt_merge",
                    side="BUY",
                    fill_quantity=3.0,
                    fill_price=0.4,
                    event_time=base_event_time + 50,
                    fill_time=base_event_time + 51,
                    record_time=base_event_time + 51,
                ),
                LedgerFill(
                    fill_id="fill_merge_02",
                    order_id="ord_merge_02",
                    intent_id="intent_merge_02",
                    market_id="mkt_merge",
                    side="BUY",
                    fill_quantity=2.0,
                    fill_price=0.45,
                    event_time=base_event_time + 51,
                    fill_time=base_event_time + 52,
                    record_time=base_event_time + 52,
                ),
            ],
            final_positions=merge_final_positions,
            equity_snapshots=[
                EquitySnapshot(
                    event_time=base_event_time + 52,
                    record_time=base_event_time + 52,
                    cash_balance_usd=9997.9,
                    positions_value_usd=2.3,
                    equity=10000.2,
                    realized_pnl=0.0,
                    unrealized_pnl=0.2,
                    fee_usd=0.0,
                )
            ],
            positions_by_tick=[
                {
                    "tick": 1,
                    "event_time": base_event_time + 51,
                    "positions": [
                        {
                            "market_id": "mkt_merge",
                            "quantity": 3.0,
                            "avg_price": 0.4,
                            "mark_price": 0.4,
                            "realized_pnl": 0.0,
                            "unrealized_pnl": 0.0,
                            "updated_at": base_event_time + 51,
                        }
                    ],
                },
                {
                    "tick": 2,
                    "event_time": base_event_time + 52,
                    "positions": [merge_final_positions["mkt_merge"].__dict__],
                },
            ],
            state_projection={},
        ),
        ScenarioSpec(
            scenario_id="flat_position_converges_zero",
            expected="PASS",
            initial_cash_balance_usd=9997.9,
            initial_positions=flat_zero_initial_positions,
            orders=[
                LedgerOrder(
                    order_id="ord_flat_01",
                    intent_id="intent_flat_01",
                    market_id="mkt_flat",
                    side="SELL",
                    quantity=5.0,
                    limit_price=0.44,
                    status="FILLED",
                    accepted_quantity=5.0,
                    filled_quantity=5.0,
                    remaining_quantity=0.0,
                    rejected_reason="",
                    event_time=base_event_time + 61,
                    record_time=base_event_time + 62,
                )
            ],
            fills=[
                LedgerFill(
                    fill_id="fill_flat_01",
                    order_id="ord_flat_01",
                    intent_id="intent_flat_01",
                    market_id="mkt_flat",
                    side="SELL",
                    fill_quantity=5.0,
                    fill_price=0.44,
                    event_time=base_event_time + 61,
                    fill_time=base_event_time + 62,
                    record_time=base_event_time + 62,
                )
            ],
            attempted_fills=[
                LedgerFill(
                    fill_id="fill_flat_01",
                    order_id="ord_flat_01",
                    intent_id="intent_flat_01",
                    market_id="mkt_flat",
                    side="SELL",
                    fill_quantity=5.0,
                    fill_price=0.44,
                    event_time=base_event_time + 61,
                    fill_time=base_event_time + 62,
                    record_time=base_event_time + 62,
                )
            ],
            final_positions=flat_zero_final_positions,
            equity_snapshots=[
                EquitySnapshot(
                    event_time=base_event_time + 62,
                    record_time=base_event_time + 62,
                    cash_balance_usd=10000.1,
                    positions_value_usd=0.0,
                    equity=10000.1,
                    realized_pnl=0.1,
                    unrealized_pnl=0.0,
                    fee_usd=0.0,
                )
            ],
            positions_by_tick=[
                {
                    "tick": 1,
                    "event_time": base_event_time + 62,
                    "positions": [flat_zero_final_positions["mkt_flat"].__dict__],
                }
            ],
            state_projection={},
        ),
        ScenarioSpec(
            scenario_id="invalid_time_order_failed_hard",
            expected="FAILED_HARD",
            initial_cash_balance_usd=10000.0,
            initial_positions={},
            orders=[
                LedgerOrder(
                    order_id="ord_bad_time_01",
                    intent_id="intent_bad_time_01",
                    market_id="mkt_bad_time",
                    side="BUY",
                    quantity=10.0,
                    limit_price=0.5,
                    status="SUBMITTED",
                    accepted_quantity=10.0,
                    filled_quantity=0.0,
                    remaining_quantity=10.0,
                    rejected_reason="",
                    event_time=base_event_time + 70,
                    record_time=base_event_time + 70,
                )
            ],
            fills=[],
            attempted_fills=[
                LedgerFill(
                    fill_id="fill_bad_time_01",
                    order_id="ord_bad_time_01",
                    intent_id="intent_bad_time_01",
                    market_id="mkt_bad_time",
                    side="BUY",
                    fill_quantity=10.0,
                    fill_price=0.5,
                    event_time=base_event_time + 70,
                    fill_time=base_event_time + 69,
                    record_time=base_event_time + 70,
                )
            ],
            final_positions={},
            equity_snapshots=[
                EquitySnapshot(
                    event_time=base_event_time + 70,
                    record_time=base_event_time + 70,
                    cash_balance_usd=10000.0,
                    positions_value_usd=0.0,
                    equity=10000.0,
                    realized_pnl=0.0,
                    unrealized_pnl=0.0,
                    fee_usd=0.0,
                )
            ],
            positions_by_tick=[
                {
                    "tick": 1,
                    "event_time": base_event_time + 70,
                    "positions": [],
                }
            ],
            state_projection={},
            expected_failed_assertions=("fill_time_order",),
        ),
    ]


def _event_rows(spec: ScenarioSpec) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    event_seq = 0
    truth_fill_ids = {fill.fill_id for fill in spec.fills}

    def append(event_type: str, payload: dict[str, object]) -> None:
        nonlocal event_seq
        event_seq += 1
        rows.append({"event_type": event_type, "event_id": f"evt_{event_seq:06d}", **payload})

    for order in spec.orders:
        append(
            "order_terminal",
            {
                "order_id": order.order_id,
                "intent_id": order.intent_id,
                "market_id": order.market_id,
                "status": order.status,
                "event_time": order.event_time,
                "record_time": order.record_time,
            },
        )
    for fill in spec.attempted_fills:
        append(
            "fill_recorded" if fill.fill_id in truth_fill_ids else "fill_blocked",
            {
                "fill_id": fill.fill_id,
                "order_id": fill.order_id,
                "intent_id": fill.intent_id,
                "market_id": fill.market_id,
                "side": fill.side,
                "fill_quantity": fill.fill_quantity,
                "fill_price": fill.fill_price,
                "event_time": fill.event_time,
                "fill_time": fill.fill_time,
                "record_time": fill.record_time,
            },
        )
    for snapshot in spec.equity_snapshots:
        append(
            "equity_snapshot",
            {
                "cash_balance_usd": snapshot.cash_balance_usd,
                "positions_value_usd": snapshot.positions_value_usd,
                "equity": snapshot.equity,
                "event_time": snapshot.event_time,
                "record_time": snapshot.record_time,
            },
        )
    return rows


def _sum_signed_fills(fills: list[LedgerFill], market_id: str) -> float:
    total = 0.0
    for fill in fills:
        if fill.market_id != market_id:
            continue
        total += fill.fill_quantity if fill.side == "BUY" else -fill.fill_quantity
    return _round4(total)


def _weighted_open_avg(initial_qty: float, initial_avg: float, fills: list[LedgerFill], market_id: str) -> float:
    qty = initial_qty
    cost = initial_qty * initial_avg
    for fill in sorted(fills, key=lambda item: (item.fill_time, item.record_time, item.fill_id)):
        if fill.market_id != market_id:
            continue
        if fill.side == "BUY":
            qty += fill.fill_quantity
            cost += fill.fill_quantity * fill.fill_price
        else:
            closed = min(qty, fill.fill_quantity)
            qty -= closed
            if qty <= 0:
                qty = 0.0
                cost = 0.0
            elif initial_qty > 0:
                cost = qty * (cost / max(qty + closed, 1e-9))
    if qty <= 0:
        return 0.0
    return _round4(cost / qty)


def _validate_scenario(
    spec: ScenarioSpec,
    *,
    preapply_failed_assertions: list[str],
    truth_apply_started: bool,
    truth_apply_completed: bool,
    invalid_fill_blocked: bool,
    incremental_truth_mutation: bool,
) -> tuple[dict[str, bool], list[str], dict[str, object]]:
    failed: list[str] = []
    last_equity = spec.equity_snapshots[-1]
    order_fill_consistent = True
    cash_consistent = True
    fill_position_consistent = True
    position_equity_consistent = True
    reject_pollution_free = True
    realized_unrealized_consistent = True
    fill_sequence_consistent = True

    fills_by_order: dict[str, float] = {}
    fill_times_by_order: dict[str, list[tuple[int, int]]] = {}
    for fill in spec.fills:
        fills_by_order[fill.order_id] = _round4(fills_by_order.get(fill.order_id, 0.0) + fill.fill_quantity)
        fill_times_by_order.setdefault(fill.order_id, []).append((fill.fill_time, fill.record_time))
        if not (fill.event_time <= fill.fill_time <= fill.record_time):
            failed.append("fill_time_order")
            order_fill_consistent = False
        if fill.record_time < fill.event_time:
            failed.append("fill_record_time")
            order_fill_consistent = False
    for order_id, times in fill_times_by_order.items():
        for idx in range(1, len(times)):
            if times[idx][0] < times[idx - 1][0] or times[idx][1] < times[idx - 1][1]:
                fill_sequence_consistent = False
                failed.append(f"fill_sequence:{order_id}")
                order_fill_consistent = False
                break

    for order in spec.orders:
        total_fill_qty = _round4(fills_by_order.get(order.order_id, 0.0))
        if total_fill_qty > order.quantity + 1e-9:
            order_fill_consistent = False
            failed.append(f"fill_gt_order:{order.order_id}")
        if _round4(order.remaining_quantity) != _round4(order.quantity - total_fill_qty):
            order_fill_consistent = False
            failed.append(f"remaining_mismatch:{order.order_id}")
        if order.status == "REJECTED":
            if total_fill_qty != 0.0:
                reject_pollution_free = False
                order_fill_consistent = False
                failed.append(f"reject_has_fill:{order.order_id}")
            if order.accepted_quantity != 0.0:
                reject_pollution_free = False
                failed.append(f"reject_accepted_qty:{order.order_id}")
        if order.status == "FILLED" and _round4(order.remaining_quantity) != 0.0:
            order_fill_consistent = False
            failed.append(f"filled_has_remaining:{order.order_id}")

    expected_cash_delta = _round4(sum(_signed_fill_value(fill) for fill in spec.fills))
    actual_cash_delta = _round4(last_equity.cash_balance_usd - spec.initial_cash_balance_usd)
    if expected_cash_delta != actual_cash_delta:
        cash_consistent = False
        failed.append("cash_delta_mismatch")

    buy_cash_delta = _round4(sum(-fill.fill_quantity * fill.fill_price for fill in spec.fills if fill.side == "BUY"))
    sell_cash_delta = _round4(sum(fill.fill_quantity * fill.fill_price for fill in spec.fills if fill.side == "SELL"))
    if spec.scenario_id == "mark_to_market_only" and actual_cash_delta != 0.0:
        cash_consistent = False
        failed.append("mark_only_cash_nonzero")
    if any(fill.side == "BUY" for fill in spec.fills) and buy_cash_delta >= 0.0:
        cash_consistent = False
        failed.append("buy_cash_direction")
    if any(fill.side == "SELL" for fill in spec.fills) and sell_cash_delta <= 0.0:
        cash_consistent = False
        failed.append("sell_cash_direction")

    final_positions_value = _positions_value(spec.final_positions)
    if _round4(last_equity.positions_value_usd) != final_positions_value:
        position_equity_consistent = False
        failed.append("positions_value_mismatch")
    if _round4(last_equity.equity) != _round4(last_equity.cash_balance_usd + last_equity.positions_value_usd):
        position_equity_consistent = False
        failed.append("equity_formula_mismatch")

    for market_id, final_position in spec.final_positions.items():
        initial_position = spec.initial_positions.get(
            market_id,
            PositionRecord(market_id=market_id, quantity=0.0, avg_price=0.0, mark_price=0.0, realized_pnl=0.0, unrealized_pnl=0.0, updated_at=0),
        )
        expected_qty = _round4(initial_position.quantity + _sum_signed_fills(spec.fills, market_id))
        if _round4(final_position.quantity) != expected_qty:
            fill_position_consistent = False
            failed.append(f"position_qty_mismatch:{market_id}")
        if _round4(final_position.quantity) == 0.0 and (_round4(final_position.avg_price) != 0.0 or _round4(final_position.mark_price * final_position.quantity) != 0.0):
            fill_position_consistent = False
            failed.append(f"flat_position_not_reset:{market_id}")
        expected_avg = _weighted_open_avg(initial_position.quantity, initial_position.avg_price, spec.fills, market_id)
        if _round4(final_position.quantity) > 0.0 and _round4(final_position.avg_price) != expected_avg:
            fill_position_consistent = False
            failed.append(f"avg_price_mismatch:{market_id}")

    if spec.scenario_id == "mark_to_market_only":
        if _round4(last_equity.realized_pnl) != 0.0 or _round4(last_equity.unrealized_pnl) == 0.0:
            realized_unrealized_consistent = False
            failed.append("mark_only_pnl_split")
    if spec.scenario_id in {"close_realizes_pnl", "flat_position_converges_zero"}:
        if _round4(last_equity.realized_pnl) == 0.0:
            realized_unrealized_consistent = False
            failed.append("close_realized_zero")
        if _round4(last_equity.unrealized_pnl) != 0.0:
            realized_unrealized_consistent = False
            failed.append("close_unrealized_nonzero")

    if any(order.status == "REJECTED" for order in spec.orders):
        if actual_cash_delta != 0.0 or final_positions_value != _positions_value(spec.initial_positions) or _round4(last_equity.equity - (spec.initial_cash_balance_usd + _positions_value(spec.initial_positions))) != 0.0:
            reject_pollution_free = False
            failed.append("reject_pollution")

    state_summary = (
        spec.state_projection.get("namespaces", {})
        .get("demo", {})
        .get("summary", {})
    )
    state_projection_ignored = _round4(float(state_summary.get("equity", last_equity.equity))) != _round4(last_equity.equity)

    if preapply_failed_assertions:
        failed.extend(preapply_failed_assertions)
    if preapply_failed_assertions and truth_apply_started:
        failed.append("failed_hard_after_truth_apply")
    if preapply_failed_assertions and incremental_truth_mutation:
        failed.append("incremental_truth_mutation")

    invariants = {
        "cash_consistent": cash_consistent,
        "order_fill_consistent": order_fill_consistent,
        "fill_position_consistent": fill_position_consistent,
        "position_equity_consistent": position_equity_consistent,
        "reject_pollution_free": reject_pollution_free,
        "realized_unrealized_consistent": realized_unrealized_consistent,
        "fill_sequence_consistent": fill_sequence_consistent,
        "state_projection_ignored": state_projection_ignored or not state_summary,
        "time_validation_passed": not preapply_failed_assertions,
        "truth_apply_started": truth_apply_started,
        "truth_apply_completed": truth_apply_completed,
        "truth_layer_unchanged": not incremental_truth_mutation,
        "incremental_truth_mutation": incremental_truth_mutation,
        "invalid_fill_blocked": invalid_fill_blocked,
    }
    metrics = {
        "fill_count": len(spec.fills),
        "position_delta": _round4(
            sum(position.quantity for position in spec.final_positions.values())
            - sum(position.quantity for position in spec.initial_positions.values())
        ),
        "cash_delta": actual_cash_delta,
        "equity_delta": _round4(last_equity.equity - (spec.initial_cash_balance_usd + _positions_value(spec.initial_positions))),
    }
    return invariants, failed, metrics


def _materialize_scenario(base_dir: Path, spec: ScenarioSpec) -> None:
    events_path = base_dir / "events.jsonl"
    orders_path = base_dir / "orders.jsonl"
    fills_path = base_dir / "fills.jsonl"
    positions_path = base_dir / "positions.json"
    positions_by_tick_path = base_dir / "positions_by_tick.jsonl"
    equity_path = base_dir / "equity.jsonl"
    state_path = base_dir / "state.json"
    snapshot_path = base_dir / "order_terminal_snapshot.json"
    reconciliation_path = base_dir / "reconciliation_summary.json"
    scenario_result_path = base_dir / "scenario_result.json"
    for path in (
        events_path,
        orders_path,
        fills_path,
        positions_path,
        positions_by_tick_path,
        equity_path,
        state_path,
        snapshot_path,
        reconciliation_path,
        scenario_result_path,
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
    _write_jsonl(events_path, _event_rows(spec))
    _write_jsonl(orders_path, [order.__dict__ for order in spec.orders])
    _write_jsonl(fills_path, [fill.__dict__ for fill in spec.fills])
    _write_json(positions_path, {market_id: position.__dict__ for market_id, position in spec.final_positions.items()})
    _write_jsonl(equity_path, [snapshot.__dict__ for snapshot in spec.equity_snapshots])
    _write_jsonl(positions_by_tick_path, spec.positions_by_tick)
    _write_json(snapshot_path, {"scenario_id": spec.scenario_id, "orders": [order.__dict__ for order in spec.orders]})


def _baseline_equity_snapshot(spec: ScenarioSpec) -> EquitySnapshot:
    initial_positions_value = _positions_value(spec.initial_positions)
    return EquitySnapshot(
        event_time=spec.orders[0].event_time if spec.orders else int(time.time()),
        record_time=spec.orders[0].record_time if spec.orders else int(time.time()),
        cash_balance_usd=spec.initial_cash_balance_usd,
        positions_value_usd=initial_positions_value,
        equity=_round4(spec.initial_cash_balance_usd + initial_positions_value),
        realized_pnl=_round4(sum(position.realized_pnl for position in spec.initial_positions.values())),
        unrealized_pnl=_round4(sum(position.unrealized_pnl for position in spec.initial_positions.values())),
        fee_usd=0.0,
    )


def run_demo_ledger_suite(*, scenario_ids: list[str] | None = None) -> dict[str, object]:
    summary_root = Path("runtime/demo_ledger/summary")
    summary_root.mkdir(parents=True, exist_ok=True)
    base_event_time = int(time.time())
    scenarios = _build_scenarios(base_event_time)
    if scenario_ids:
        requested = set(scenario_ids)
        scenarios = [scenario for scenario in scenarios if scenario.scenario_id in requested]
    results: list[dict[str, object]] = []
    failed_hard: dict[str, object] | None = None
    validator = TimeSemanticValidator()

    for spec in scenarios:
        run_id = f"{spec.scenario_id}-{int(time.time())}"
        base_dir = Path("runtime/demo_ledger") / spec.scenario_id / run_id
        base_dir.mkdir(parents=True, exist_ok=True)
        guard = DemoLedgerIsolationGuard(base_dir, scenario_id=spec.scenario_id, run_id=run_id)
        guard.ensure_file_path(base_dir / "scenario_result.json")

        preapply_failed_assertions: list[str] = []
        invalid_fill_blocked = False
        truth_apply_started = False
        truth_apply_completed = False
        for attempted_fill in spec.attempted_fills:
            valid, reason = validator.validate_fill(attempted_fill)
            if not valid:
                preapply_failed_assertions.append(reason)
                invalid_fill_blocked = True
                break
        if not preapply_failed_assertions and spec.attempted_fills:
            sequence_ok, reason = validator.validate_fill_sequence(spec.attempted_fills)
            if not sequence_ok:
                preapply_failed_assertions.append(reason)
                invalid_fill_blocked = True

        if preapply_failed_assertions:
            truth_spec = ScenarioSpec(
                scenario_id=spec.scenario_id,
                expected=spec.expected,
                initial_cash_balance_usd=spec.initial_cash_balance_usd,
                initial_positions=spec.initial_positions,
                orders=spec.orders,
                fills=[],
                attempted_fills=spec.attempted_fills,
                final_positions=spec.initial_positions,
                equity_snapshots=[_baseline_equity_snapshot(spec)],
                positions_by_tick=[
                    {
                        "tick": 0,
                        "event_time": _baseline_equity_snapshot(spec).event_time,
                        "positions": [position.__dict__ for position in spec.initial_positions.values()],
                    }
                ],
                state_projection=spec.state_projection,
                expected_failed_assertions=spec.expected_failed_assertions,
            )
        else:
            truth_apply_started = bool(spec.fills or spec.equity_snapshots or spec.positions_by_tick or spec.final_positions != spec.initial_positions)
            truth_apply_completed = truth_apply_started
            truth_spec = spec

        final_summary = truth_spec.equity_snapshots[-1]
        state_projection = _scenario_state_projection(
            truth_spec.scenario_id,
            {
                "cash_balance_usd": final_summary.cash_balance_usd,
                "positions_value_usd": final_summary.positions_value_usd,
                "equity": final_summary.equity,
                "realized_pnl": final_summary.realized_pnl,
                "unrealized_pnl": final_summary.unrealized_pnl,
                "fee_usd": final_summary.fee_usd,
                "open_positions": sum(1 for position in truth_spec.final_positions.values() if position.quantity > 0),
            },
            truth_spec.orders,
            truth_spec.final_positions,
            force_projection_mismatch=bool(spec.state_projection.get("force_projection_mismatch")),
        )
        truth_spec = ScenarioSpec(
            scenario_id=truth_spec.scenario_id,
            expected=truth_spec.expected,
            initial_cash_balance_usd=truth_spec.initial_cash_balance_usd,
            initial_positions=truth_spec.initial_positions,
            orders=truth_spec.orders,
            fills=truth_spec.fills,
            attempted_fills=truth_spec.attempted_fills,
            final_positions=truth_spec.final_positions,
            equity_snapshots=truth_spec.equity_snapshots,
            positions_by_tick=truth_spec.positions_by_tick,
            state_projection=state_projection,
            expected_failed_assertions=truth_spec.expected_failed_assertions,
        )
        _materialize_scenario(base_dir, truth_spec)
        _write_json(base_dir / "state.json", truth_spec.state_projection)

        invariants, failed_assertions, metrics = _validate_scenario(
            truth_spec,
            preapply_failed_assertions=preapply_failed_assertions,
            truth_apply_started=truth_apply_started,
            truth_apply_completed=truth_apply_completed,
            invalid_fill_blocked=invalid_fill_blocked,
            incremental_truth_mutation=bool(truth_apply_started and preapply_failed_assertions),
        )
        actual = "FAILED_HARD" if failed_assertions else "PASS"
        expected_failed = set(truth_spec.expected_failed_assertions)
        if truth_spec.expected == "FAILED_HARD":
            passed = (
                actual == "FAILED_HARD"
                and expected_failed.issubset(set(name.split(":")[0] for name in failed_assertions))
                and metrics["fill_count"] == 0
                and metrics["position_delta"] == 0.0
                and metrics["cash_delta"] == 0.0
                and metrics["equity_delta"] == 0.0
                and not truth_apply_started
                and not truth_apply_completed
            )
        else:
            passed = actual == "PASS"
        scenario_result = {
            "scenario_id": truth_spec.scenario_id,
            "expected": truth_spec.expected,
            "actual": actual,
            "fill_count": metrics["fill_count"],
            "position_delta": metrics["position_delta"],
            "cash_delta": metrics["cash_delta"],
            "equity_delta": metrics["equity_delta"],
            "failed_hard_before_truth_apply": bool(preapply_failed_assertions and not truth_apply_started),
            "passed": passed,
            "failed_assertions": failed_assertions,
            "runtime_dir": str(base_dir),
            "ledger_truth_files": [
                "orders.jsonl",
                "fills.jsonl",
                "positions.json",
                "equity.jsonl",
            ],
            "projection_file": "state.json",
            "scope": {
                "fees_enabled": False,
                "short_inventory_enabled": False,
            },
            "invariants": invariants,
        }
        reconciliation_summary = {
            "scenario_id": truth_spec.scenario_id,
            "ledger_truth_layer": [
                "orders.jsonl",
                "fills.jsonl",
                "positions.json",
                "equity.jsonl",
            ],
            "projection_layer": ["state.json"],
            "fee_usd": 0.0,
            "short_inventory_enabled": False,
            "time_validation_passed": not preapply_failed_assertions,
            "truth_apply_started": truth_apply_started,
            "truth_apply_completed": truth_apply_completed,
            "truth_layer_unchanged": not (truth_apply_started and preapply_failed_assertions),
            "incremental_truth_mutation": bool(truth_apply_started and preapply_failed_assertions),
            "invalid_fill_blocked": invalid_fill_blocked,
            "invariants": invariants,
            "failed_assertions": failed_assertions,
        }
        _write_json(base_dir / "scenario_result.json", scenario_result)
        _write_json(base_dir / "reconciliation_summary.json", reconciliation_summary)
        results.append(scenario_result)

        if not passed and truth_spec.expected != "FAILED_HARD":
            failed_hard = {
                "scenario_id": truth_spec.scenario_id,
                "failure_type": "invariant_broken",
                "failed_assertions": failed_assertions,
            }
            break
        if truth_spec.expected == "FAILED_HARD":
            if actual != "FAILED_HARD":
                failed_hard = {
                    "scenario_id": truth_spec.scenario_id,
                    "failure_type": "expected_failed_hard_missing",
                    "failed_assertions": failed_assertions,
                }
            else:
                failed_hard = {
                    "scenario_id": truth_spec.scenario_id,
                    "failure_type": "expected_failed_hard",
                    "failed_assertions": failed_assertions,
                }
            break

    all_passed = (
        len(results) == len(scenarios)
        and all(row["passed"] for row in results)
        and results[-1]["scenario_id"] == "invalid_time_order_failed_hard"
        and results[-1]["actual"] == "FAILED_HARD"
    )
    normal_rows = [row for row in results if row["expected"] == "PASS"]
    invalid_time_row = next((row for row in results if row["scenario_id"] == "invalid_time_order_failed_hard"), None)
    report = {
        "scenario_count": len(results),
        "all_passed": all_passed,
        "failed_hard": failed_hard,
        "scope": {
            "fees_enabled": False,
            "short_inventory_enabled": False,
            "ledger_truth_layer": ["orders.jsonl", "fills.jsonl", "positions.json", "equity.jsonl"],
            "projection_layer": ["state.json"],
        },
        "invariant_summary": {
            "cash_consistent": all(row["invariants"]["cash_consistent"] for row in normal_rows),
            "order_fill_consistent": all(row["invariants"]["order_fill_consistent"] for row in normal_rows),
            "fill_position_consistent": all(row["invariants"]["fill_position_consistent"] for row in normal_rows),
            "position_equity_consistent": all(row["invariants"]["position_equity_consistent"] for row in normal_rows),
            "reject_pollution_free": all(row["invariants"]["reject_pollution_free"] for row in normal_rows),
            "time_order_fail_fast": bool(invalid_time_row and invalid_time_row["actual"] == "FAILED_HARD"),
        },
        "results": results,
    }
    _write_json(summary_root / "report.json", report)
    lines = [
        "# Demo Ledger Summary",
        "",
        "- ledger truth layer: `orders.jsonl / fills.jsonl / positions.json / equity.jsonl`",
        "- projection layer: `state.json`",
        "- fees_enabled: `false`",
        "- short_inventory_enabled: `false`",
        f"- scenario_count: `{len(results)}`",
        f"- all_passed: `{all_passed}`",
        "",
        "| scenario_id | expected | actual | fill_count | cash_delta | equity_delta | passed |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in results:
        lines.append(
            f"| {row['scenario_id']} | {row['expected']} | {row['actual']} | {row['fill_count']} | {row['cash_delta']} | {row['equity_delta']} | {row['passed']} |"
        )
    if failed_hard:
        lines.extend(["", "## FAILED_HARD", "", json.dumps(failed_hard, ensure_ascii=False)])
    _write_text(summary_root / "report.md", "\n".join(lines) + "\n")
    return report
