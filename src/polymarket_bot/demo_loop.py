from __future__ import annotations

import json
import random
import signal
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Literal, Protocol


OrderSide = Literal["BUY", "SELL"]
OrderStatus = Literal["SUBMITTED", "PARTIAL_FILL", "FILLED", "REJECTED", "CANCELED"]
IntentAction = Literal["OPEN", "CLOSE", "REDUCE"]

RISK_MAX_OPEN_POSITIONS = "RISK_MAX_OPEN_POSITIONS"
RISK_CASH_INSUFFICIENT = "RISK_CASH_INSUFFICIENT"
RISK_LOSS_STREAK_BREAKER = "RISK_LOSS_STREAK_BREAKER"
RISK_REDUCE_ONLY = "RISK_REDUCE_ONLY"
RISK_HALTED = "RISK_HALTED"
RISK_MARKET_EXPOSURE = "RISK_MARKET_EXPOSURE"
RISK_PORTFOLIO_EXPOSURE = "RISK_PORTFOLIO_EXPOSURE"
RISK_STATE_INVALID = "RISK_STATE_INVALID"


@dataclass(frozen=True)
class OrderIntent:
    intent_id: str
    namespace: str
    market_id: str
    side: OrderSide
    action: IntentAction
    quantity: float
    limit_price: float
    signal_type: str
    edge: float
    event_time: int


@dataclass(frozen=True)
class ExecutionAck:
    order_id: str
    intent_id: str
    namespace: str
    status: OrderStatus
    accepted_quantity: float
    rejected_reason: str
    event_time: int
    record_time: int


@dataclass(frozen=True)
class FillEvent:
    fill_id: str
    order_id: str
    intent_id: str
    namespace: str
    market_id: str
    side: OrderSide
    fill_quantity: float
    fill_price: float
    fill_status: Literal["PARTIAL", "FULL"]
    event_time: int
    fill_time: int
    record_time: int


@dataclass(frozen=True)
class PositionSnapshot:
    namespace: str
    market_id: str
    quantity: float
    avg_price: float
    mark_price: float
    unrealized_pnl: float
    updated_at: int


class ExecutionBackend(Protocol):
    def name(self) -> str: ...

    def namespace(self) -> str: ...

    def get_open_positions(self) -> list[PositionSnapshot]: ...

    def validate_intent(self, intent: OrderIntent) -> tuple[bool, str]: ...

    def submit_intent(self, intent: OrderIntent) -> ExecutionAck: ...

    def poll_fills(self, now_ts: int) -> list[FillEvent]: ...

    def cancel_order(self, order_id: str, now_ts: int) -> ExecutionAck: ...

    def get_order_status(self, order_id: str) -> ExecutionAck | None: ...

    def shutdown(self) -> None: ...


@dataclass
class DemoCandidate:
    candidate_id: str
    market_id: str
    title: str
    probability: float
    baseline_probability: float
    edge: float
    signal_type: str
    side: OrderSide
    status: str
    action_label: str
    event_time: int


@dataclass
class DemoMarket:
    market_id: str
    title: str
    regime: Literal["up", "down", "flat"]
    baseline_probability: float
    probability: float
    last_probability: float


@dataclass
class DemoOrderRecord:
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


@dataclass
class DemoPosition:
    market_id: str
    quantity: float = 0.0
    avg_price: float = 0.0
    mark_price: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    updated_at: int = 0


class RunIsolationGuard:
    def __init__(self, base_dir: Path, *, namespace: str, run_id: str) -> None:
        self.base_dir = base_dir.expanduser().resolve()
        self.namespace = str(namespace or "").strip()
        self.run_id = str(run_id or "").strip()
        expected = Path("runtime") / "demo" / self.run_id
        if self.namespace != "demo":
            raise RuntimeError("demo runner requires namespace=demo")
        if expected.as_posix() not in self.base_dir.as_posix():
            raise RuntimeError(f"demo runtime path must live under runtime/demo/<run_id>: {self.base_dir}")

    def ensure_file_path(self, path: Path) -> Path:
        resolved = path.expanduser().resolve()
        try:
            resolved.relative_to(self.base_dir)
        except ValueError as exc:
            raise RuntimeError(f"path escapes demo runtime root: {resolved}") from exc
        return resolved


class JsonlLedger:
    def __init__(self, base_dir: Path, *, guard: RunIsolationGuard, namespace: str, run_id: str) -> None:
        self.base_dir = base_dir
        self.guard = guard
        self.namespace = namespace
        self.run_id = run_id
        self.events_path = self.guard.ensure_file_path(base_dir / "events.jsonl")
        self.orders_path = self.guard.ensure_file_path(base_dir / "orders.jsonl")
        self.fills_path = self.guard.ensure_file_path(base_dir / "fills.jsonl")
        self.equity_path = self.guard.ensure_file_path(base_dir / "equity.jsonl")
        self.positions_path = self.guard.ensure_file_path(base_dir / "positions.json")
        self._event_seq = 0
        self.events_path.parent.mkdir(parents=True, exist_ok=True)
        for path in (self.events_path, self.orders_path, self.fills_path, self.equity_path):
            path.touch(exist_ok=True)

    def _append_jsonl(self, path: Path, payload: dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False) + "\n")

    def _next_event_id(self) -> str:
        self._event_seq += 1
        return f"evt_{self._event_seq:06d}"

    def append_event(self, event_type: str, payload: dict[str, object]) -> None:
        event_time = int(payload.get("event_time") or 0)
        fill_time = int(payload.get("fill_time") or 0)
        record_time = int(payload.get("record_time") or 0)
        min_record_time = max(event_time, fill_time)
        if record_time < min_record_time:
            payload = dict(payload)
            payload["record_time"] = min_record_time
        record = {
            "event_type": event_type,
            "event_id": self._next_event_id(),
            "namespace": self.namespace,
            "run_id": self.run_id,
            **payload,
        }
        self._append_jsonl(self.events_path, record)

    def append_order(self, order: DemoOrderRecord) -> None:
        self._append_jsonl(self.orders_path, asdict(order))

    def append_fill(self, fill: FillEvent) -> None:
        self._append_jsonl(self.fills_path, asdict(fill))

    def append_equity(self, payload: dict[str, object]) -> None:
        event_time = int(payload.get("event_time") or 0)
        record_time = int(payload.get("record_time") or 0)
        if record_time < event_time:
            payload = dict(payload)
            payload["record_time"] = event_time
        self._append_jsonl(self.equity_path, payload)

    def write_positions(self, positions: dict[str, DemoPosition]) -> None:
        serializable = {
            market_id: asdict(position)
            for market_id, position in sorted(positions.items(), key=lambda item: item[0])
        }
        self.positions_path.parent.mkdir(parents=True, exist_ok=True)
        self.positions_path.write_text(json.dumps(serializable, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    @property
    def last_event_seq(self) -> int:
        return self._event_seq


class FakeMarketDataEngine:
    def __init__(self, *, seed: int) -> None:
        self._rng = random.Random(int(seed))
        self._markets = [
            DemoMarket(
                market_id="fed-cut-2026",
                title="Fed cuts in 2026",
                regime="up",
                baseline_probability=0.42,
                probability=0.42,
                last_probability=0.42,
            ),
            DemoMarket(
                market_id="btc-100k-2026",
                title="BTC 100k in 2026",
                regime="flat",
                baseline_probability=0.55,
                probability=0.55,
                last_probability=0.55,
            ),
            DemoMarket(
                market_id="eth-etf-q4",
                title="ETH ETF by Q4",
                regime="down",
                baseline_probability=0.61,
                probability=0.61,
                last_probability=0.61,
            ),
        ]

    def _step(self, regime: str) -> float:
        drift = {"up": 0.022, "down": -0.024, "flat": 0.0}.get(regime, 0.0)
        noise = self._rng.uniform(-0.012, 0.012)
        return drift + noise

    def tick(self) -> list[DemoMarket]:
        updated: list[DemoMarket] = []
        for market in self._markets:
            market.last_probability = market.probability
            market.probability = min(0.95, max(0.05, market.probability + self._step(market.regime)))
            updated.append(market)
        return updated


class DemoCandidatePipeline:
    def generate(self, markets: list[DemoMarket], *, tick: int, event_time: int) -> list[DemoCandidate]:
        out: list[DemoCandidate] = []
        for market in markets:
            edge = round(market.probability - market.baseline_probability, 4)
            if abs(edge) < 0.025:
                continue
            side: OrderSide = "BUY" if edge > 0 else "SELL"
            signal_type = "threshold_breakout" if abs(edge) >= 0.06 else "mean_reversion_watch"
            status = "pending" if abs(edge) >= 0.06 else "watched"
            action_label = "buy_normal" if side == "BUY" else "close_partial"
            out.append(
                DemoCandidate(
                    candidate_id=f"cand_{tick:04d}_{market.market_id}",
                    market_id=market.market_id,
                    title=market.title,
                    probability=round(market.probability, 4),
                    baseline_probability=round(market.baseline_probability, 4),
                    edge=edge,
                    signal_type=signal_type,
                    side=side,
                    status=status,
                    action_label=action_label,
                    event_time=event_time,
                )
            )
        return out


class DemoStrategy:
    def __init__(self, *, order_quantity: float = 10.0, open_threshold: float = 0.06, close_threshold: float = -0.035) -> None:
        self.order_quantity = float(order_quantity)
        self.open_threshold = float(open_threshold)
        self.close_threshold = float(close_threshold)

    def evaluate(
        self,
        candidates: list[DemoCandidate],
        positions: list[PositionSnapshot],
        *,
        event_time: int,
        tick: int,
    ) -> list[OrderIntent]:
        positions_by_market = {position.market_id: position for position in positions}
        intents: list[OrderIntent] = []
        for idx, candidate in enumerate(candidates, start=1):
            current = positions_by_market.get(candidate.market_id)
            if candidate.edge >= self.open_threshold and (current is None or current.quantity <= 0.0):
                intents.append(
                    OrderIntent(
                        intent_id=f"intent_{tick:04d}_{idx:02d}",
                        namespace="demo",
                        market_id=candidate.market_id,
                        side="BUY",
                        action="OPEN",
                        quantity=self.order_quantity,
                        limit_price=candidate.probability,
                        signal_type=candidate.signal_type,
                        edge=candidate.edge,
                        event_time=event_time,
                    )
                )
            elif candidate.edge <= self.close_threshold and current is not None and current.quantity > 0.0:
                intents.append(
                    OrderIntent(
                        intent_id=f"intent_{tick:04d}_{idx:02d}",
                        namespace="demo",
                        market_id=candidate.market_id,
                        side="SELL",
                        action="CLOSE",
                        quantity=min(self.order_quantity, current.quantity),
                        limit_price=candidate.probability,
                        signal_type=candidate.signal_type,
                        edge=candidate.edge,
                        event_time=event_time,
                    )
                )
        return intents


class DemoExecutionBackend(ExecutionBackend):
    def __init__(self, *, seed: int, namespace: str, max_open_positions: int = 2) -> None:
        self._rng = random.Random(int(seed) + 7)
        self._namespace = namespace
        self._positions: dict[str, DemoPosition] = {}
        self._orders: dict[str, DemoOrderRecord] = {}
        self._pending_fill_plan: dict[str, list[tuple[float, Literal["PARTIAL", "FULL"]]]] = {}
        self._fill_seq = 0
        self.max_open_positions = int(max_open_positions)
        self.validate_calls = 0
        self.submit_calls = 0
        self._validation_cache: dict[str, tuple[bool, str]] = {}

    def name(self) -> str:
        return "demo"

    def namespace(self) -> str:
        return self._namespace

    def get_open_positions(self) -> list[PositionSnapshot]:
        snapshots: list[PositionSnapshot] = []
        for market_id, position in sorted(self._positions.items()):
            if position.quantity <= 0:
                continue
            snapshots.append(
                PositionSnapshot(
                    namespace=self._namespace,
                    market_id=market_id,
                    quantity=round(position.quantity, 4),
                    avg_price=round(position.avg_price, 4),
                    mark_price=round(position.mark_price, 4),
                    unrealized_pnl=round(position.unrealized_pnl, 4),
                    updated_at=position.updated_at,
                )
            )
        return snapshots

    def validate_intent(self, intent: OrderIntent) -> tuple[bool, str]:
        self.validate_calls += 1
        open_count = sum(1 for position in self._positions.values() if position.quantity > 0)
        if intent.side == "BUY" and intent.action == "OPEN" and open_count >= self.max_open_positions:
            decision = (False, RISK_MAX_OPEN_POSITIONS)
            self._validation_cache[intent.intent_id] = decision
            return decision
        if intent.quantity <= 0:
            decision = (False, RISK_STATE_INVALID)
            self._validation_cache[intent.intent_id] = decision
            return decision
        decision = (True, "")
        self._validation_cache[intent.intent_id] = decision
        return decision

    def submit_intent(self, intent: OrderIntent) -> ExecutionAck:
        self.submit_calls += 1
        allowed, reason = self._validation_cache.get(intent.intent_id, (False, RISK_STATE_INVALID))
        if not allowed:
            raise RuntimeError(f"submit_intent requires a prior allowed validate_intent() result, got {reason or 'unknown'}")
        record_time = int(time.time())
        record_time = max(record_time, intent.event_time)
        order_id = f"ord_{intent.intent_id}"
        status: OrderStatus = "SUBMITTED"
        accepted_quantity = intent.quantity
        order = DemoOrderRecord(
            order_id=order_id,
            intent_id=intent.intent_id,
            market_id=intent.market_id,
            side=intent.side,
            quantity=intent.quantity,
            limit_price=intent.limit_price,
            status=status,
            accepted_quantity=accepted_quantity,
            filled_quantity=0.0,
            remaining_quantity=intent.quantity,
            rejected_reason="",
            event_time=intent.event_time,
            record_time=record_time,
        )
        self._orders[order_id] = order
        partial = self._rng.random() < 0.45
        if partial and intent.quantity > 1:
            first_fill = round(intent.quantity * 0.5, 4)
            second_fill = round(intent.quantity - first_fill, 4)
            self._pending_fill_plan[order_id] = [(first_fill, "PARTIAL"), (second_fill, "FULL")]
        else:
            self._pending_fill_plan[order_id] = [(intent.quantity, "FULL")]
        return ExecutionAck(
            order_id=order_id,
            intent_id=intent.intent_id,
            namespace=self._namespace,
            status=status,
            accepted_quantity=accepted_quantity,
            rejected_reason="",
            event_time=intent.event_time,
            record_time=record_time,
        )

    def record_rejection(self, intent: OrderIntent, reason_code: str) -> ExecutionAck:
        record_time = max(int(time.time()), intent.event_time)
        order_id = f"ord_{intent.intent_id}"
        order = DemoOrderRecord(
            order_id=order_id,
            intent_id=intent.intent_id,
            market_id=intent.market_id,
            side=intent.side,
            quantity=intent.quantity,
            limit_price=intent.limit_price,
            status="REJECTED",
            accepted_quantity=0.0,
            filled_quantity=0.0,
            remaining_quantity=intent.quantity,
            rejected_reason=str(reason_code or RISK_STATE_INVALID),
            event_time=intent.event_time,
            record_time=record_time,
        )
        self._orders[order_id] = order
        return ExecutionAck(
            order_id=order_id,
            intent_id=intent.intent_id,
            namespace=self._namespace,
            status="REJECTED",
            accepted_quantity=0.0,
            rejected_reason=str(reason_code or RISK_STATE_INVALID),
            event_time=intent.event_time,
            record_time=record_time,
        )

    def poll_fills(self, now_ts: int) -> list[FillEvent]:
        fills: list[FillEvent] = []
        for order_id in list(self._pending_fill_plan.keys()):
            plan = self._pending_fill_plan.get(order_id) or []
            if not plan:
                self._pending_fill_plan.pop(order_id, None)
                continue
            fill_quantity, fill_status = plan.pop(0)
            order = self._orders[order_id]
            self._fill_seq += 1
            fill_id = f"fill_{order_id}_{self._fill_seq:04d}"
            fill = FillEvent(
                fill_id=fill_id,
                order_id=order_id,
                intent_id=order.intent_id,
                namespace=self._namespace,
                market_id=order.market_id,
                side=order.side,
                fill_quantity=fill_quantity,
                fill_price=order.limit_price,
                fill_status=fill_status,
                event_time=order.event_time,
                fill_time=now_ts,
                record_time=max(int(time.time()), now_ts),
            )
            fills.append(fill)
            order.filled_quantity = round(order.filled_quantity + fill_quantity, 4)
            order.remaining_quantity = round(max(0.0, order.quantity - order.filled_quantity), 4)
            order.status = "FILLED" if order.remaining_quantity <= 0 else "PARTIAL_FILL"
            self._apply_fill(fill)
            if not plan:
                self._pending_fill_plan.pop(order_id, None)
        return fills

    def _apply_fill(self, fill: FillEvent) -> None:
        position = self._positions.setdefault(fill.market_id, DemoPosition(market_id=fill.market_id))
        if fill.side == "BUY":
            total_cost = position.avg_price * position.quantity + fill.fill_price * fill.fill_quantity
            position.quantity = round(position.quantity + fill.fill_quantity, 4)
            position.avg_price = round(total_cost / max(position.quantity, 1e-9), 6)
        else:
            closed = min(position.quantity, fill.fill_quantity)
            position.realized_pnl = round(position.realized_pnl + (fill.fill_price - position.avg_price) * closed, 4)
            position.quantity = round(max(0.0, position.quantity - closed), 4)
            if position.quantity <= 0:
                position.avg_price = 0.0
        position.mark_price = fill.fill_price
        position.updated_at = fill.fill_time

    def mark_to_market(self, prices: dict[str, float], now_ts: int) -> None:
        for market_id, position in self._positions.items():
            mark_price = float(prices.get(market_id, position.mark_price or position.avg_price or 0.0))
            position.mark_price = mark_price
            position.unrealized_pnl = round((mark_price - position.avg_price) * position.quantity, 4)
            position.updated_at = now_ts

    def cancel_order(self, order_id: str, now_ts: int) -> ExecutionAck:
        order = self._orders[order_id]
        order.status = "CANCELED"
        order.record_time = int(time.time())
        self._pending_fill_plan.pop(order_id, None)
        return ExecutionAck(
            order_id=order_id,
            intent_id=order.intent_id,
            namespace=self._namespace,
            status="CANCELED",
            accepted_quantity=order.accepted_quantity,
            rejected_reason="",
            event_time=now_ts,
            record_time=order.record_time,
        )

    def get_order_status(self, order_id: str) -> ExecutionAck | None:
        order = self._orders.get(order_id)
        if order is None:
            return None
        return ExecutionAck(
            order_id=order.order_id,
            intent_id=order.intent_id,
            namespace=self._namespace,
            status=order.status,
            accepted_quantity=order.accepted_quantity,
            rejected_reason=order.rejected_reason,
            event_time=order.event_time,
            record_time=order.record_time,
        )

    def recent_orders(self) -> list[DemoOrderRecord]:
        return [self._orders[key] for key in sorted(self._orders.keys())]

    def shutdown(self) -> None:
        self._pending_fill_plan.clear()


class DemoStatePublisher:
    def __init__(self, base_dir: Path, *, guard: RunIsolationGuard, namespace: str, run_id: str, seed: int, tick_interval_seconds: int) -> None:
        self.base_dir = base_dir
        self.guard = guard
        self.namespace = namespace
        self.run_id = run_id
        self.seed = seed
        self.tick_interval_seconds = tick_interval_seconds
        self.state_path = self.guard.ensure_file_path(base_dir / "state.json")
        self.index_path = self.guard.ensure_file_path(base_dir / "index.json")
        self.ui_state_path = self.guard.ensure_file_path(base_dir / "ui_state.json")

    def publish(
        self,
        *,
        tick: int,
        started_at: int,
        updated_at: int,
        status: str,
        summary: dict[str, object],
        candidates: list[DemoCandidate],
        orders: list[DemoOrderRecord],
        positions: list[PositionSnapshot],
        ledger_last_event_seq: int,
        timeline: list[dict[str, object]],
    ) -> None:
        namespaced = {
            "meta": {
                "namespace": self.namespace,
                "run_id": self.run_id,
                "mode": "demo",
                "seed": self.seed,
                "tick": tick,
                "tick_interval_seconds": self.tick_interval_seconds,
                "started_at": started_at,
                "updated_at": updated_at,
                "status": status,
            },
            "summary": summary,
            "candidates": {
                "summary": {
                    "count": len(candidates),
                    "pending": sum(1 for item in candidates if item.status == "pending"),
                    "approved": 0,
                    "watched": sum(1 for item in candidates if item.status == "watched"),
                    "executed": sum(1 for item in orders if item.status in {"FILLED", "PARTIAL_FILL"}),
                },
                "items": [asdict(item) for item in candidates],
            },
            "orders": {
                "pending": [asdict(item) for item in orders if item.status in {"SUBMITTED", "PARTIAL_FILL"}],
                "recent": [asdict(item) for item in orders[-20:]],
            },
            "positions": [asdict(item) for item in positions],
            "ledger": {
                "last_event_seq": ledger_last_event_seq,
                "last_record_time": updated_at,
            },
            "timeline": timeline[-20:],
        }
        state_payload = {
            "active_namespace": self.namespace,
            "namespaces": {
                self.namespace: namespaced,
            },
        }
        ui_state = {
            "ts": updated_at,
            "config": {
                "dry_run": True,
                "execution_mode": "demo",
                "broker_name": "DemoExecutionBackend",
                "poll_interval_seconds": self.tick_interval_seconds,
                "bankroll_usd": summary.get("bankroll_usd", 0.0),
                "max_open_positions": 2,
            },
            "summary": {
                "equity": summary.get("equity", 0.0),
                "cash_balance_usd": summary.get("cash_balance_usd", 0.0),
                "positions_value_usd": summary.get("positions_value_usd", 0.0),
                "open_positions": summary.get("open_positions", 0),
                "signals": len(candidates),
                "pnl_today": summary.get("pnl_today", 0.0),
            },
            "trading_mode": {
                "mode": "NORMAL",
                "opening_allowed": True,
                "reason_codes": [],
                "updated_ts": updated_at,
                "source": "demo_runner",
                "account_state_status": "demo",
                "reconciliation_status": "demo",
                "persistence_status": "ok",
            },
            "control": {
                "decision_mode": "manual",
                "pause_opening": False,
                "reduce_only": False,
                "emergency_stop": False,
                "updated_ts": updated_at,
            },
            "decision_mode": {
                "mode": "manual",
                "updated_ts": updated_at,
                "updated_by": "demo",
                "note": "demo_mode",
                "available_modes": ["manual", "semi_auto", "auto"],
            },
            "candidates": namespaced["candidates"],
            "positions": [asdict(item) for item in positions],
            "orders": [asdict(item) for item in orders[-20:]],
            "timeline": timeline[-20:],
            "namespaces": state_payload["namespaces"],
            "active_namespace": self.namespace,
        }
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(state_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        self.ui_state_path.write_text(json.dumps(ui_state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        self.index_path.write_text(
            json.dumps(
                {
                    "active_namespace": self.namespace,
                    "state_files": {
                        self.namespace: str(self.state_path),
                        f"{self.namespace}_ui": str(self.ui_state_path),
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )


@dataclass
class DemoRunResult:
    run_id: str
    base_dir: Path
    ticks_completed: int
    candidates_generated: int
    orders_created: int
    fills_recorded: int
    final_equity: float
    open_positions: int
    stop_reason: str


class DemoRunner:
    def __init__(
        self,
        *,
        seed: int,
        max_ticks: int,
        tick_seconds: int,
        runtime_root: str = "runtime/demo",
    ) -> None:
        self.seed = int(seed)
        self.max_ticks = int(max_ticks)
        self.tick_seconds = int(tick_seconds)
        self.started_at = int(time.time())
        self.run_id = f"demo-seed{self.seed}-ticks{self.max_ticks}-{self.started_at}"
        self.base_dir = Path(runtime_root).expanduser() / self.run_id
        self.guard = RunIsolationGuard(self.base_dir, namespace="demo", run_id=self.run_id)
        self.engine = FakeMarketDataEngine(seed=self.seed)
        self.pipeline = DemoCandidatePipeline()
        self.strategy = DemoStrategy()
        self.backend = DemoExecutionBackend(seed=self.seed, namespace="demo")
        self.ledger = JsonlLedger(self.base_dir, guard=self.guard, namespace="demo", run_id=self.run_id)
        self.publisher = DemoStatePublisher(
            self.base_dir,
            guard=self.guard,
            namespace="demo",
            run_id=self.run_id,
            seed=self.seed,
            tick_interval_seconds=self.tick_seconds,
        )
        self.stop_file = self.guard.ensure_file_path(self.base_dir / "STOP")
        self._stop_requested = False
        self._timeline: list[dict[str, object]] = []
        self._cash_balance = 10000.0
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, _signum: int, _frame: object) -> None:
        self._stop_requested = True

    def _should_stop(self) -> tuple[bool, str]:
        if self._stop_requested:
            return True, "signal_stop"
        if self.stop_file.exists():
            return True, "stop_file"
        return False, ""

    def _append_timeline(self, kind: str, payload: dict[str, object]) -> None:
        self._timeline.append({"kind": kind, **payload})

    def _summary(self) -> tuple[dict[str, object], list[PositionSnapshot]]:
        positions = self.backend.get_open_positions()
        positions_value = sum(position.quantity * position.mark_price for position in positions)
        pnl = sum(position.unrealized_pnl for position in positions)
        realized = sum(self.backend._positions[market_id].realized_pnl for market_id in self.backend._positions)
        summary = {
            "bankroll_usd": 10000.0,
            "cash_balance_usd": round(self._cash_balance, 4),
            "positions_value_usd": round(positions_value, 4),
            "equity": round(self._cash_balance + positions_value, 4),
            "open_positions": len([position for position in positions if position.quantity > 0]),
            "pnl_today": round(realized + pnl, 4),
        }
        return summary, positions

    def run(self) -> DemoRunResult:
        total_candidates = 0
        total_orders = 0
        total_fills = 0
        last_candidates: list[DemoCandidate] = []
        stop_reason = "max_ticks"
        self.base_dir.mkdir(parents=True, exist_ok=True)
        for tick in range(1, self.max_ticks + 1):
            should_stop, stop_reason = self._should_stop()
            if should_stop:
                break
            event_time = self.started_at + tick * self.tick_seconds
            markets = self.engine.tick()
            market_prices = {market.market_id: market.probability for market in markets}
            self.backend.mark_to_market(market_prices, event_time)
            candidates = self.pipeline.generate(markets, tick=tick, event_time=event_time)
            last_candidates = candidates
            total_candidates += len(candidates)
            for candidate in candidates:
                record_time = max(int(time.time()), candidate.event_time)
                self.ledger.append_event(
                    "candidate_generated",
                    {
                        "market_id": candidate.market_id,
                        "candidate_id": candidate.candidate_id,
                        "probability": candidate.probability,
                        "edge": candidate.edge,
                        "signal_type": candidate.signal_type,
                        "event_time": candidate.event_time,
                        "record_time": record_time,
                    },
                )
                self._append_timeline(
                    "candidate",
                    {
                        "market_id": candidate.market_id,
                        "edge": candidate.edge,
                        "signal_type": candidate.signal_type,
                        "event_time": candidate.event_time,
                    },
                )
            intents = self.strategy.evaluate(candidates, self.backend.get_open_positions(), event_time=event_time, tick=tick)
            for intent in intents:
                record_time = max(int(time.time()), intent.event_time)
                allowed, reason_code = self.backend.validate_intent(intent)
                self.ledger.append_event(
                    "risk_decision",
                    {
                        "intent_id": intent.intent_id,
                        "market_id": intent.market_id,
                        "side": intent.side,
                        "action": intent.action,
                        "allowed": bool(allowed),
                        "reason_code": str(reason_code or ""),
                        "reason_detail": "" if allowed else str(reason_code or ""),
                        "event_time": intent.event_time,
                        "record_time": record_time,
                    },
                )
                self.ledger.append_event(
                    "order_intent_created",
                    {
                        "intent_id": intent.intent_id,
                        "market_id": intent.market_id,
                        "side": intent.side,
                        "quantity": intent.quantity,
                        "limit_price": intent.limit_price,
                        "event_time": intent.event_time,
                        "record_time": record_time,
                    },
                )
                ack = self.backend.submit_intent(intent) if allowed else self.backend.record_rejection(intent, reason_code)
                total_orders += 1
                self.ledger.append_event(
                    "order_acknowledged",
                    {
                        "order_id": ack.order_id,
                        "intent_id": ack.intent_id,
                        "status": ack.status,
                        "accepted_quantity": ack.accepted_quantity,
                        "rejected_reason": ack.rejected_reason,
                        "event_time": ack.event_time,
                        "record_time": ack.record_time,
                    },
                )
                self.ledger.append_order(self.backend._orders[ack.order_id])
                self._append_timeline(
                    "order_ack",
                    {
                        "market_id": intent.market_id,
                        "order_id": ack.order_id,
                        "status": ack.status,
                        "event_time": ack.event_time,
                    },
                )
            fills = self.backend.poll_fills(event_time)
            for fill in fills:
                total_fills += 1
                if fill.side == "BUY":
                    self._cash_balance = round(self._cash_balance - fill.fill_quantity * fill.fill_price, 4)
                else:
                    self._cash_balance = round(self._cash_balance + fill.fill_quantity * fill.fill_price, 4)
                self.ledger.append_fill(fill)
                self.ledger.append_event(
                    "fill_recorded",
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
                self._append_timeline(
                    "fill",
                    {
                        "market_id": fill.market_id,
                        "order_id": fill.order_id,
                        "fill_quantity": fill.fill_quantity,
                        "fill_status": fill.fill_status,
                        "fill_time": fill.fill_time,
                    },
                )
            summary, positions = self._summary()
            self.ledger.write_positions(self.backend._positions)
            self.ledger.append_equity(
                {
                    "namespace": "demo",
                    "run_id": self.run_id,
                    "event_type": "equity_snapshot",
                    "event_time": event_time,
                    "record_time": max(int(time.time()), event_time),
                    **summary,
                }
            )
            self.publisher.publish(
                tick=tick,
                started_at=self.started_at,
                updated_at=event_time,
                status="running" if tick < self.max_ticks else "completed",
                summary=summary,
                candidates=last_candidates,
                orders=self.backend.recent_orders(),
                positions=positions,
                ledger_last_event_seq=self.ledger.last_event_seq,
                timeline=self._timeline,
            )
        else:
            stop_reason = "max_ticks"
            tick = self.max_ticks
        final_summary, final_positions = self._summary()
        self.ledger.append_event(
            "demo_stopped",
            {
                "stop_reason": stop_reason,
                "event_time": self.started_at + max(1, tick) * self.tick_seconds,
                "record_time": max(int(time.time()), self.started_at + max(1, tick) * self.tick_seconds),
            },
        )
        self.publisher.publish(
            tick=max(1, tick),
            started_at=self.started_at,
            updated_at=self.started_at + max(1, tick) * self.tick_seconds,
            status="stopped",
            summary=final_summary,
            candidates=last_candidates,
            orders=self.backend.recent_orders(),
            positions=final_positions,
            ledger_last_event_seq=self.ledger.last_event_seq,
            timeline=self._timeline,
        )
        self.backend.shutdown()
        return DemoRunResult(
            run_id=self.run_id,
            base_dir=self.base_dir,
            ticks_completed=max(1, tick),
            candidates_generated=total_candidates,
            orders_created=total_orders,
            fills_recorded=total_fills,
            final_equity=float(final_summary["equity"]),
            open_positions=int(final_summary["open_positions"]),
            stop_reason=stop_reason,
        )
