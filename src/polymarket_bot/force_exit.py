from __future__ import annotations

from dataclasses import replace

from polymarket_bot.models.exit_state import (
    TIME_EXIT_STAGE_FORCE_EXIT,
    TIME_EXIT_STAGE_IDLE,
    TIME_EXIT_STAGE_RETRY,
    TimeExitState,
)


def estimate_time_exit_volatility_bps(
    *,
    best_bid: float | None,
    best_ask: float | None,
    midpoint: float | None,
    reference_price: float | None,
) -> float:
    midpoint_value = float(midpoint or 0.0)
    if midpoint_value <= 0.0:
        bid_value = float(best_bid or 0.0)
        ask_value = float(best_ask or 0.0)
        if bid_value > 0.0 and ask_value > 0.0:
            midpoint_value = (bid_value + ask_value) / 2.0
    if midpoint_value <= 0.0:
        return 0.0

    spread_bps = 0.0
    bid_value = float(best_bid or 0.0)
    ask_value = float(best_ask or 0.0)
    if bid_value > 0.0 and ask_value > 0.0 and ask_value >= bid_value:
        spread_bps = ((ask_value - bid_value) / midpoint_value) * 10000.0

    reference_value = float(reference_price or 0.0)
    displacement_bps = 0.0
    if reference_value > 0.0:
        displacement_bps = abs(reference_value - midpoint_value) / midpoint_value * 10000.0
    return max(0.0, max(spread_bps, displacement_bps))


def compute_time_exit_priority(
    *,
    consecutive_failures: int,
    market_volatility_bps: float,
    volatility_step_bps: float,
    force_exit: bool,
) -> tuple[int, str]:
    failures = max(0, int(consecutive_failures))
    step_bps = max(1.0, float(volatility_step_bps or 1.0))
    volatility_bonus = min(40, int(max(0.0, float(market_volatility_bps or 0.0)) // step_bps) * 10)
    failure_bonus = min(40, failures * 20)
    force_bonus = 40 if force_exit else 0
    priority = min(100, 10 + volatility_bonus + failure_bonus + force_bonus)
    reason_parts = [f"failures={failures}", f"volatility={float(market_volatility_bps or 0.0):.1f}bps"]
    if force_exit:
        reason_parts.append("force_exit")
    return priority, " | ".join(reason_parts)


def begin_time_exit_attempt(
    current: TimeExitState,
    *,
    now_ts: int,
    market_volatility_bps: float,
    volatility_step_bps: float,
) -> TimeExitState:
    force_mode = current.stage == TIME_EXIT_STAGE_FORCE_EXIT
    priority, priority_reason = compute_time_exit_priority(
        consecutive_failures=current.consecutive_failures,
        market_volatility_bps=market_volatility_bps,
        volatility_step_bps=volatility_step_bps,
        force_exit=force_mode,
    )
    return replace(
        current,
        priority=priority,
        priority_reason=priority_reason,
        market_volatility_bps=max(0.0, float(market_volatility_bps or 0.0)),
        last_attempt_ts=max(0, int(now_ts)),
        attempt_count=max(0, int(current.attempt_count)) + 1,
    )


def record_time_exit_failure(
    current: TimeExitState,
    *,
    now_ts: int,
    retry_limit: int,
    retry_cooldown_seconds: int,
    volatility_step_bps: float,
    error_message: str,
) -> TimeExitState:
    failures = max(0, int(current.consecutive_failures)) + 1
    limit = max(1, int(retry_limit or 1))
    previously_forced = current.stage == TIME_EXIT_STAGE_FORCE_EXIT
    force_exit = failures >= limit
    priority, priority_reason = compute_time_exit_priority(
        consecutive_failures=failures,
        market_volatility_bps=current.market_volatility_bps,
        volatility_step_bps=volatility_step_bps,
        force_exit=force_exit,
    )
    if force_exit and not previously_forced:
        next_retry_ts = max(0, int(now_ts))
    else:
        next_retry_ts = max(0, int(now_ts)) + max(0, int(retry_cooldown_seconds or 0))
    return replace(
        current,
        stage=TIME_EXIT_STAGE_FORCE_EXIT if force_exit else TIME_EXIT_STAGE_RETRY,
        consecutive_failures=failures,
        priority=priority,
        priority_reason=priority_reason,
        last_failure_ts=max(0, int(now_ts)),
        next_retry_ts=next_retry_ts,
        force_exit_armed_ts=max(current.force_exit_armed_ts, int(now_ts)) if force_exit else int(current.force_exit_armed_ts or 0),
        last_result="failed",
        last_error=str(error_message or ""),
    )


def record_time_exit_success(current: TimeExitState, *, now_ts: int) -> TimeExitState:
    return replace(
        current,
        stage=TIME_EXIT_STAGE_IDLE,
        consecutive_failures=0,
        priority=0,
        priority_reason="",
        last_success_ts=max(0, int(now_ts)),
        next_retry_ts=0,
        last_result="filled",
        last_error="",
    )


def should_attempt_time_exit(current: TimeExitState, *, now_ts: int) -> bool:
    return max(0, int(current.next_retry_ts or 0)) <= max(0, int(now_ts))
