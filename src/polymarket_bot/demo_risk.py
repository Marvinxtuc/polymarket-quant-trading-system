from __future__ import annotations

import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path

from polymarket_bot.demo_loop import (
    DemoOrderRecord,
    DemoPosition,
    ExecutionAck,
    JsonlLedger,
    OrderIntent,
    PositionSnapshot,
    RISK_CASH_INSUFFICIENT,
    RISK_HALTED,
    RISK_LOSS_STREAK_BREAKER,
    RISK_MARKET_EXPOSURE,
    RISK_MAX_OPEN_POSITIONS,
    RISK_PORTFOLIO_EXPOSURE,
    RISK_REDUCE_ONLY,
    RISK_STATE_INVALID,
)


STANDARD_REASON_CODES = {
    RISK_MAX_OPEN_POSITIONS,
    RISK_CASH_INSUFFICIENT,
    RISK_LOSS_STREAK_BREAKER,
    RISK_REDUCE_ONLY,
    RISK_HALTED,
    RISK_MARKET_EXPOSURE,
    RISK_PORTFOLIO_EXPOSURE,
    RISK_STATE_INVALID,
}


@dataclass(frozen=True)
class RiskScenario:
    scenario_id: str
    reason_code: str
    cash_balance_usd: float
    max_open_positions: int
    reduce_only: bool
    halted: bool
    loss_streak_breaker_active: bool
    risk_state_valid: bool
    market_exposure_limit_usd: float
    portfolio_exposure_limit_usd: float
    preload_positions: tuple[tuple[str, float, float], ...]
    intent: OrderIntent


class DemoRiskIsolationGuard:
    def __init__(self, base_dir: Path, *, scenario_id: str, run_id: str) -> None:
        self.base_dir = base_dir.expanduser().resolve()
        expected = Path("runtime") / "demo_risk" / scenario_id / run_id
        if expected.as_posix() not in self.base_dir.as_posix():
            raise RuntimeError(f"demo risk runtime path must live under runtime/demo_risk/<scenario_id>/<run_id>: {self.base_dir}")

    def ensure_file_path(self, path: Path) -> Path:
        resolved = path.expanduser().resolve()
        try:
            resolved.relative_to(self.base_dir)
        except ValueError as exc:
            raise RuntimeError(f"path escapes demo risk runtime root: {resolved}") from exc
        return resolved


class ScenarioExecutionBackend:
    def __init__(self, scenario: RiskScenario) -> None:
        self.scenario = scenario
        self.validate_calls = 0
        self.submit_calls = 0
        self._orders: dict[str, DemoOrderRecord] = {}
        self._positions: dict[str, DemoPosition] = {}
        for market_id, qty, avg_price in scenario.preload_positions:
            self._positions[market_id] = DemoPosition(
                market_id=market_id,
                quantity=qty,
                avg_price=avg_price,
                mark_price=avg_price,
                realized_pnl=0.0,
                unrealized_pnl=0.0,
                updated_at=scenario.intent.event_time,
            )

    def get_open_positions(self) -> list[PositionSnapshot]:
        out: list[PositionSnapshot] = []
        for market_id, position in sorted(self._positions.items()):
            if position.quantity <= 0:
                continue
            out.append(
                PositionSnapshot(
                    namespace="demo",
                    market_id=market_id,
                    quantity=position.quantity,
                    avg_price=position.avg_price,
                    mark_price=position.mark_price,
                    unrealized_pnl=position.unrealized_pnl,
                    updated_at=position.updated_at,
                )
            )
        return out

    def validate_intent(self, intent: OrderIntent) -> tuple[bool, str]:
        self.validate_calls += 1
        if self.scenario.reason_code not in STANDARD_REASON_CODES:
            raise RuntimeError(f"non-standard reason code configured: {self.scenario.reason_code}")
        if not self.scenario.risk_state_valid:
            return False, RISK_STATE_INVALID
        if self.scenario.halted:
            return False, RISK_HALTED
        if self.scenario.reduce_only and intent.action == "OPEN":
            return False, RISK_REDUCE_ONLY
        if self.scenario.loss_streak_breaker_active:
            return False, RISK_LOSS_STREAK_BREAKER
        open_count = sum(1 for snapshot in self.get_open_positions() if snapshot.quantity > 0)
        if intent.action == "OPEN" and open_count >= self.scenario.max_open_positions:
            return False, RISK_MAX_OPEN_POSITIONS
        current_market_exposure = sum(
            position.quantity * position.mark_price
            for position in self._positions.values()
            if position.market_id == intent.market_id
        )
        requested_notional = intent.quantity * intent.limit_price
        if current_market_exposure + requested_notional > self.scenario.market_exposure_limit_usd:
            return False, RISK_MARKET_EXPOSURE
        current_portfolio_exposure = sum(position.quantity * position.mark_price for position in self._positions.values())
        if current_portfolio_exposure + requested_notional > self.scenario.portfolio_exposure_limit_usd:
            return False, RISK_PORTFOLIO_EXPOSURE
        if self.scenario.cash_balance_usd < requested_notional:
            return False, RISK_CASH_INSUFFICIENT
        return True, ""

    def submit_intent(self, intent: OrderIntent) -> ExecutionAck:
        self.submit_calls += 1
        record_time = max(int(time.time()), intent.event_time)
        order_id = f"ord_{intent.intent_id}"
        record = DemoOrderRecord(
            order_id=order_id,
            intent_id=intent.intent_id,
            market_id=intent.market_id,
            side=intent.side,
            quantity=intent.quantity,
            limit_price=intent.limit_price,
            status="SUBMITTED",
            accepted_quantity=intent.quantity,
            filled_quantity=0.0,
            remaining_quantity=intent.quantity,
            rejected_reason="",
            event_time=intent.event_time,
            record_time=record_time,
        )
        self._orders[order_id] = record
        return ExecutionAck(
            order_id=order_id,
            intent_id=intent.intent_id,
            namespace="demo",
            status="SUBMITTED",
            accepted_quantity=intent.quantity,
            rejected_reason="",
            event_time=intent.event_time,
            record_time=record_time,
        )

    def record_rejection(self, intent: OrderIntent, reason_code: str) -> ExecutionAck:
        record_time = max(int(time.time()), intent.event_time)
        order_id = f"ord_{intent.intent_id}"
        record = DemoOrderRecord(
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
            rejected_reason=reason_code,
            event_time=intent.event_time,
            record_time=record_time,
        )
        self._orders[order_id] = record
        return ExecutionAck(
            order_id=order_id,
            intent_id=intent.intent_id,
            namespace="demo",
            status="REJECTED",
            accepted_quantity=0.0,
            rejected_reason=reason_code,
            event_time=intent.event_time,
            record_time=record_time,
        )

    def poll_fills(self, _now_ts: int) -> list[object]:
        return []

    def recent_orders(self) -> list[DemoOrderRecord]:
        return [self._orders[key] for key in sorted(self._orders.keys())]


def _build_scenarios(seed: int, base_event_time: int) -> list[RiskScenario]:
    del seed
    return [
        RiskScenario(
            scenario_id="max_open_positions",
            reason_code=RISK_MAX_OPEN_POSITIONS,
            cash_balance_usd=10000.0,
            max_open_positions=2,
            reduce_only=False,
            halted=False,
            loss_streak_breaker_active=False,
            risk_state_valid=True,
            market_exposure_limit_usd=1_000_000.0,
            portfolio_exposure_limit_usd=1_000_000.0,
            preload_positions=(("mkt_a", 10.0, 0.5), ("mkt_b", 10.0, 0.6)),
            intent=OrderIntent("intent_max_open_positions", "demo", "mkt_c", "BUY", "OPEN", 10.0, 0.45, "risk_test", 0.12, base_event_time + 1),
        ),
        RiskScenario(
            scenario_id="cash_insufficient",
            reason_code=RISK_CASH_INSUFFICIENT,
            cash_balance_usd=5.0,
            max_open_positions=10,
            reduce_only=False,
            halted=False,
            loss_streak_breaker_active=False,
            risk_state_valid=True,
            market_exposure_limit_usd=1_000_000.0,
            portfolio_exposure_limit_usd=1_000_000.0,
            preload_positions=(),
            intent=OrderIntent("intent_cash_insufficient", "demo", "mkt_cash", "BUY", "OPEN", 10.0, 1.0, "risk_test", 0.12, base_event_time + 2),
        ),
        RiskScenario(
            scenario_id="loss_streak_breaker",
            reason_code=RISK_LOSS_STREAK_BREAKER,
            cash_balance_usd=10000.0,
            max_open_positions=10,
            reduce_only=False,
            halted=False,
            loss_streak_breaker_active=True,
            risk_state_valid=True,
            market_exposure_limit_usd=1_000_000.0,
            portfolio_exposure_limit_usd=1_000_000.0,
            preload_positions=(),
            intent=OrderIntent("intent_loss_streak_breaker", "demo", "mkt_loss", "BUY", "OPEN", 10.0, 0.55, "risk_test", 0.12, base_event_time + 3),
        ),
        RiskScenario(
            scenario_id="reduce_only",
            reason_code=RISK_REDUCE_ONLY,
            cash_balance_usd=10000.0,
            max_open_positions=10,
            reduce_only=True,
            halted=False,
            loss_streak_breaker_active=False,
            risk_state_valid=True,
            market_exposure_limit_usd=1_000_000.0,
            portfolio_exposure_limit_usd=1_000_000.0,
            preload_positions=(),
            intent=OrderIntent("intent_reduce_only", "demo", "mkt_reduce", "BUY", "OPEN", 10.0, 0.55, "risk_test", 0.12, base_event_time + 4),
        ),
        RiskScenario(
            scenario_id="halted",
            reason_code=RISK_HALTED,
            cash_balance_usd=10000.0,
            max_open_positions=10,
            reduce_only=False,
            halted=True,
            loss_streak_breaker_active=False,
            risk_state_valid=True,
            market_exposure_limit_usd=1_000_000.0,
            portfolio_exposure_limit_usd=1_000_000.0,
            preload_positions=(),
            intent=OrderIntent("intent_halted", "demo", "mkt_halt", "BUY", "OPEN", 10.0, 0.55, "risk_test", 0.12, base_event_time + 5),
        ),
        RiskScenario(
            scenario_id="market_exposure_cap",
            reason_code=RISK_MARKET_EXPOSURE,
            cash_balance_usd=10000.0,
            max_open_positions=10,
            reduce_only=False,
            halted=False,
            loss_streak_breaker_active=False,
            risk_state_valid=True,
            market_exposure_limit_usd=8.0,
            portfolio_exposure_limit_usd=1_000_000.0,
            preload_positions=(("mkt_cap", 10.0, 0.5),),
            intent=OrderIntent("intent_market_exposure_cap", "demo", "mkt_cap", "BUY", "OPEN", 10.0, 0.4, "risk_test", 0.12, base_event_time + 6),
        ),
        RiskScenario(
            scenario_id="portfolio_exposure_cap",
            reason_code=RISK_PORTFOLIO_EXPOSURE,
            cash_balance_usd=10000.0,
            max_open_positions=10,
            reduce_only=False,
            halted=False,
            loss_streak_breaker_active=False,
            risk_state_valid=True,
            market_exposure_limit_usd=1_000_000.0,
            portfolio_exposure_limit_usd=12.0,
            preload_positions=(("mkt_p1", 10.0, 0.7), ("mkt_p2", 5.0, 0.7)),
            intent=OrderIntent("intent_portfolio_exposure_cap", "demo", "mkt_p3", "BUY", "OPEN", 10.0, 0.6, "risk_test", 0.12, base_event_time + 7),
        ),
        RiskScenario(
            scenario_id="risk_state_invalid",
            reason_code=RISK_STATE_INVALID,
            cash_balance_usd=10000.0,
            max_open_positions=10,
            reduce_only=False,
            halted=False,
            loss_streak_breaker_active=False,
            risk_state_valid=False,
            market_exposure_limit_usd=1_000_000.0,
            portfolio_exposure_limit_usd=1_000_000.0,
            preload_positions=(),
            intent=OrderIntent("intent_risk_state_invalid", "demo", "mkt_invalid", "BUY", "OPEN", 10.0, 0.55, "risk_test", 0.12, base_event_time + 8),
        ),
    ]


def _protect_state(scenario: RiskScenario, reason_code: str) -> dict[str, object]:
    return {
        "opening_allowed": False,
        "reduce_only": bool(scenario.reduce_only or reason_code == RISK_REDUCE_ONLY),
        "halted": bool(scenario.halted or reason_code == RISK_HALTED),
        "loss_streak_breaker_active": bool(scenario.loss_streak_breaker_active or reason_code == RISK_LOSS_STREAK_BREAKER),
        "reason_codes": [reason_code],
        "risk_state_valid": bool(scenario.risk_state_valid),
    }


def _scenario_state_payload(scenario: RiskScenario, ack: ExecutionAck, backend: ScenarioExecutionBackend, protect_state: dict[str, object], run_id: str) -> tuple[dict[str, object], dict[str, object]]:
    positions = backend.get_open_positions()
    cash = scenario.cash_balance_usd
    positions_value = sum(position.quantity * position.mark_price for position in positions)
    summary = {
        "bankroll_usd": 10000.0,
        "cash_balance_usd": cash,
        "positions_value_usd": positions_value,
        "equity": cash + positions_value,
        "open_positions": len(positions),
        "pnl_today": 0.0,
    }
    state = {
        "active_namespace": "demo",
        "namespaces": {
            "demo": {
                "meta": {
                    "namespace": "demo",
                    "run_id": run_id,
                    "mode": "demo_risk",
                    "seed": 42,
                    "tick": 1,
                    "tick_interval_seconds": 1,
                    "started_at": scenario.intent.event_time,
                    "updated_at": scenario.intent.event_time,
                    "status": "stopped",
                    "scenario_id": scenario.scenario_id,
                },
                "summary": summary,
                "orders": {"pending": [], "recent": [asdict(order) for order in backend.recent_orders()]},
                "positions": [asdict(position) for position in positions],
                "risk_protection": protect_state,
            }
        },
    }
    ui_state = {
        "ts": scenario.intent.event_time,
        "config": {
            "dry_run": True,
            "execution_mode": "demo_risk",
            "broker_name": "ScenarioExecutionBackend",
            "poll_interval_seconds": 1,
            "bankroll_usd": 10000.0,
        },
        "summary": {
            "equity": summary["equity"],
            "cash_balance_usd": summary["cash_balance_usd"],
            "positions_value_usd": summary["positions_value_usd"],
            "open_positions": summary["open_positions"],
            "signals": 1,
            "pnl_today": 0.0,
        },
        "trading_mode": {
            "mode": "PROTECTED",
            "opening_allowed": False,
            "reason_codes": [ack.rejected_reason],
            "updated_ts": scenario.intent.event_time,
            "source": "demo_risk",
            "account_state_status": "demo_risk",
            "reconciliation_status": "demo_risk",
            "persistence_status": "ok",
        },
        "risk_protection": protect_state,
    }
    return state, ui_state


def run_demo_risk_suite(*, seed: int = 42) -> dict[str, object]:
    summary_root = Path("runtime/demo_risk/summary")
    summary_root.mkdir(parents=True, exist_ok=True)
    base_event_time = int(time.time())
    scenarios = _build_scenarios(seed, base_event_time)
    results: list[dict[str, object]] = []
    failed_hard: dict[str, object] | None = None

    for scenario in scenarios:
        run_id = f"{scenario.scenario_id}-{int(time.time())}"
        base_dir = Path("runtime/demo_risk") / scenario.scenario_id / run_id
        base_dir.mkdir(parents=True, exist_ok=True)
        guard = DemoRiskIsolationGuard(base_dir, scenario_id=scenario.scenario_id, run_id=run_id)
        ledger = JsonlLedger(base_dir, guard=guard, namespace="demo", run_id=run_id)
        backend = ScenarioExecutionBackend(scenario)
        initial_positions = backend.get_open_positions()
        initial_summary_cash = scenario.cash_balance_usd
        initial_positions_value = sum(position.quantity * position.mark_price for position in initial_positions)
        initial_equity = initial_summary_cash + initial_positions_value

        allowed, reason_code = backend.validate_intent(scenario.intent)
        ledger.append_event(
            "risk_decision",
            {
                "scenario_id": scenario.scenario_id,
                "intent_id": scenario.intent.intent_id,
                "market_id": scenario.intent.market_id,
                "side": scenario.intent.side,
                "action": scenario.intent.action,
                "allowed": bool(allowed),
                "reason_code": reason_code,
                "reason_detail": reason_code,
                "event_time": scenario.intent.event_time,
                "record_time": scenario.intent.event_time,
            },
        )
        if allowed:
            ack = backend.submit_intent(scenario.intent)
        else:
            ack = backend.record_rejection(scenario.intent, reason_code)
        ledger.append_order(backend.recent_orders()[0])
        ledger.append_event(
            "order_acknowledged",
            {
                "scenario_id": scenario.scenario_id,
                "order_id": ack.order_id,
                "intent_id": ack.intent_id,
                "status": ack.status,
                "accepted_quantity": ack.accepted_quantity,
                "rejected_reason": ack.rejected_reason,
                "event_time": ack.event_time,
                "record_time": ack.record_time,
            },
        )
        fills = backend.poll_fills(scenario.intent.event_time)
        for fill in fills:
            ledger.append_fill(fill)
        final_positions = backend.get_open_positions()
        final_positions_value = sum(position.quantity * position.mark_price for position in final_positions)
        final_equity = scenario.cash_balance_usd + final_positions_value
        ledger.write_positions(backend._positions)
        ledger.append_equity(
            {
                "namespace": "demo",
                "run_id": run_id,
                "event_type": "equity_snapshot",
                "event_time": scenario.intent.event_time,
                "record_time": scenario.intent.event_time,
                "bankroll_usd": 10000.0,
                "cash_balance_usd": scenario.cash_balance_usd,
                "positions_value_usd": final_positions_value,
                "equity": final_equity,
                "open_positions": len(final_positions),
                "pnl_today": 0.0,
            }
        )
        protect_state = _protect_state(scenario, ack.rejected_reason or reason_code or scenario.reason_code)
        state_payload, ui_state_payload = _scenario_state_payload(scenario, ack, backend, protect_state, run_id)
        (base_dir / "state.json").write_text(json.dumps(state_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        (base_dir / "ui_state.json").write_text(json.dumps(ui_state_payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        (base_dir / "index.json").write_text(
            json.dumps(
                {
                    "active_namespace": "demo",
                    "state_files": {
                        "demo": str(base_dir / "state.json"),
                        "demo_ui": str(base_dir / "ui_state.json"),
                    },
                },
                ensure_ascii=False,
                indent=2,
            )
            + "\n",
            encoding="utf-8",
        )

        position_delta = round(
            sum(position.quantity for position in final_positions) - sum(position.quantity for position in initial_positions),
            4,
        )
        equity_delta = round(final_equity - initial_equity, 4)
        scenario_result = {
            "scenario_id": scenario.scenario_id,
            "expected": "REJECTED",
            "actual": ack.status,
            "reason_code": ack.rejected_reason or reason_code,
            "fill_count": len(fills),
            "position_delta": position_delta,
            "equity_delta": equity_delta,
            "protect_state_entered": bool(protect_state["reason_codes"]),
            "passed": ack.status == "REJECTED" and len(fills) == 0 and position_delta == 0.0 and equity_delta == 0.0,
            "validate_calls": backend.validate_calls,
            "submit_calls": backend.submit_calls,
            "run_id": run_id,
            "runtime_dir": str(base_dir),
        }
        (base_dir / "scenario_result.json").write_text(json.dumps(scenario_result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
        results.append(scenario_result)
        if not scenario_result["passed"]:
            failed_hard = {
                "scenario_id": scenario.scenario_id,
                "intent_id": scenario.intent.intent_id,
                "order_id": ack.order_id,
                "tick": 1,
                "market_id": scenario.intent.market_id,
                "reason_code": scenario_result["reason_code"],
                "failure_type": (
                    "违规 fill" if scenario_result["fill_count"] > 0 else
                    "违规持仓" if scenario_result["position_delta"] != 0 else
                    "违规权益" if scenario_result["equity_delta"] != 0 else
                    "状态异常"
                ),
            }
            break

    summary = {
        "seed": seed,
        "scenario_count": len(results),
        "all_passed": failed_hard is None and len(results) == len(scenarios),
        "failed_hard": failed_hard,
        "results": results,
    }
    (summary_root / "report.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    md_lines = [
        "# Demo Risk Summary",
        "",
        f"- seed: `{seed}`",
        f"- scenario_count: `{len(results)}`",
        f"- all_passed: `{summary['all_passed']}`",
        "",
        "| scenario_id | actual | reason_code | fill_count | position_delta | equity_delta | passed |",
        "| --- | --- | --- | ---: | ---: | ---: | --- |",
    ]
    for row in results:
        md_lines.append(
            f"| {row['scenario_id']} | {row['actual']} | {row['reason_code']} | {row['fill_count']} | {row['position_delta']} | {row['equity_delta']} | {row['passed']} |"
        )
    if failed_hard:
        md_lines.extend(["", "## FAILED_HARD", "", json.dumps(failed_hard, ensure_ascii=False)])
    (summary_root / "report.md").write_text("\n".join(md_lines) + "\n", encoding="utf-8")
    return summary
