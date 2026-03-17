from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from polymarket_bot.config import Settings


REPLAY_EVENT_TYPES = {
    "order_filled",
    "order_reject",
    "time_exit_fill",
    "time_exit_fail",
    "emergency_exit_partial",
    "emergency_exit_fail",
}


@dataclass(slots=True)
class ReplayScenario:
    name: str
    wallet_score_watch_multiplier: float
    wallet_score_trade_multiplier: float
    wallet_score_core_multiplier: float
    topic_boost_multiplier: float
    topic_penalty_multiplier: float
    topic_min_samples: int
    topic_positive_roi: float
    topic_positive_win_rate: float
    topic_negative_roi: float
    topic_negative_win_rate: float
    stale_position_minutes: int
    stale_position_trim_pct: float
    resonance_trim_fraction: float
    resonance_core_exit_fraction: float

    @classmethod
    def from_settings(cls, settings: Settings, *, name: str = "baseline") -> ReplayScenario:
        return cls(
            name=name,
            wallet_score_watch_multiplier=float(settings.wallet_score_watch_multiplier),
            wallet_score_trade_multiplier=float(settings.wallet_score_trade_multiplier),
            wallet_score_core_multiplier=float(settings.wallet_score_core_multiplier),
            topic_boost_multiplier=float(settings.topic_boost_multiplier),
            topic_penalty_multiplier=float(settings.topic_penalty_multiplier),
            topic_min_samples=int(settings.topic_min_samples),
            topic_positive_roi=float(settings.topic_positive_roi),
            topic_positive_win_rate=float(settings.topic_positive_win_rate),
            topic_negative_roi=float(settings.topic_negative_roi),
            topic_negative_win_rate=float(settings.topic_negative_win_rate),
            stale_position_minutes=int(settings.stale_position_minutes),
            stale_position_trim_pct=float(settings.stale_position_trim_pct),
            resonance_trim_fraction=float(settings.resonance_trim_fraction),
            resonance_core_exit_fraction=float(settings.resonance_core_exit_fraction),
        )

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any], settings: Settings) -> ReplayScenario:
        baseline = cls.from_settings(settings)
        return cls(
            name=str(payload.get("name") or baseline.name),
            wallet_score_watch_multiplier=float(payload.get("wallet_score_watch_multiplier") or baseline.wallet_score_watch_multiplier),
            wallet_score_trade_multiplier=float(payload.get("wallet_score_trade_multiplier") or baseline.wallet_score_trade_multiplier),
            wallet_score_core_multiplier=float(payload.get("wallet_score_core_multiplier") or baseline.wallet_score_core_multiplier),
            topic_boost_multiplier=float(payload.get("topic_boost_multiplier") or baseline.topic_boost_multiplier),
            topic_penalty_multiplier=float(payload.get("topic_penalty_multiplier") or baseline.topic_penalty_multiplier),
            topic_min_samples=int(payload.get("topic_min_samples") or baseline.topic_min_samples),
            topic_positive_roi=float(payload.get("topic_positive_roi") or baseline.topic_positive_roi),
            topic_positive_win_rate=float(payload.get("topic_positive_win_rate") or baseline.topic_positive_win_rate),
            topic_negative_roi=float(payload.get("topic_negative_roi") or baseline.topic_negative_roi),
            topic_negative_win_rate=float(payload.get("topic_negative_win_rate") or baseline.topic_negative_win_rate),
            stale_position_minutes=int(payload.get("stale_position_minutes") or baseline.stale_position_minutes),
            stale_position_trim_pct=float(payload.get("stale_position_trim_pct") or baseline.stale_position_trim_pct),
            resonance_trim_fraction=float(payload.get("resonance_trim_fraction") or baseline.resonance_trim_fraction),
            resonance_core_exit_fraction=float(payload.get("resonance_core_exit_fraction") or baseline.resonance_core_exit_fraction),
        )


def default_replay_scenarios(settings: Settings) -> list[ReplayScenario]:
    baseline = ReplayScenario.from_settings(settings, name="baseline")
    return [
        baseline,
        ReplayScenario.from_mapping(
            {
                "name": "faster_time_exit",
                "stale_position_minutes": max(5, int(settings.stale_position_minutes // 2 or 5)),
                "stale_position_trim_pct": min(0.95, float(settings.stale_position_trim_pct) + 0.15),
            },
            settings,
        ),
        ReplayScenario.from_mapping(
            {
                "name": "softer_resonance",
                "resonance_trim_fraction": max(0.1, float(settings.resonance_trim_fraction) - 0.1),
                "resonance_core_exit_fraction": max(0.2, float(settings.resonance_core_exit_fraction) - 0.15),
            },
            settings,
        ),
        ReplayScenario.from_mapping(
            {
                "name": "topic_neutral",
                "topic_boost_multiplier": 1.0,
                "topic_penalty_multiplier": 1.0,
            },
            settings,
        ),
    ]


def load_replay_scenarios(path: Path | None, settings: Settings) -> list[ReplayScenario]:
    if path is None or not path.exists():
        return default_replay_scenarios(settings)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default_replay_scenarios(settings)
    if isinstance(payload, dict):
        payload = [payload]
    if not isinstance(payload, list):
        return default_replay_scenarios(settings)
    scenarios: list[ReplayScenario] = []
    for item in payload:
        if not isinstance(item, Mapping):
            continue
        scenarios.append(ReplayScenario.from_mapping(item, settings))
    return scenarios or default_replay_scenarios(settings)


def iter_replay_events(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                raw = line.strip()
                if not raw:
                    continue
                try:
                    item = json.loads(raw)
                except Exception:
                    continue
                if isinstance(item, dict) and str(item.get("type") or "") in REPLAY_EVENT_TYPES:
                    yield item
    except Exception:
        return


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def normalize_replay_event(event: Mapping[str, Any]) -> dict[str, Any]:
    event_type = str(event.get("type") or "")
    side = str(event.get("side") or "").upper()
    flow = str(event.get("flow") or ("exit" if side == "SELL" else "entry"))
    status = "FILLED" if event_type in {"order_filled", "time_exit_fill", "emergency_exit_partial"} else "REJECTED"
    title = str(event.get("market_slug") or event.get("title") or event.get("token_id") or "-")
    exit_kind = str(event.get("exit_kind") or "").strip().lower()
    exit_fraction = _safe_float(event.get("exit_fraction"))
    notional = _safe_float(
        event.get("notional")
        or event.get("filled_notional")
        or event.get("requested_notional")
        or event.get("trim_notional")
    )
    if event_type.startswith("time_exit"):
        exit_kind = exit_kind or "time_exit"
        flow = "exit"
        side = side or "SELL"
        if exit_fraction <= 0.0:
            exit_fraction = _safe_float(event.get("stale_position_trim_pct"))
    elif event_type.startswith("emergency_exit"):
        exit_kind = exit_kind or "emergency_exit"
        flow = "exit"
        side = side or "SELL"
        if exit_fraction <= 0.0:
            exit_fraction = 1.0

    return {
        "ts": _safe_int(event.get("ts")),
        "type": event_type,
        "cycle_id": str(event.get("cycle_id") or ""),
        "title": title,
        "token_id": str(event.get("token_id") or ""),
        "trace_id": str(event.get("trace_id") or ""),
        "signal_id": str(event.get("signal_id") or ""),
        "side": side,
        "status": status,
        "flow": flow,
        "wallet": str(event.get("wallet") or event.get("source_wallet") or ""),
        "source_wallet": str(event.get("source_wallet") or event.get("wallet") or ""),
        "wallet_score": _safe_float(event.get("wallet_score")),
        "wallet_tier": str(event.get("wallet_tier") or ""),
        "topic_label": str(event.get("topic_label") or event.get("entry_topic_label") or ""),
        "topic_sample_count": _safe_int(event.get("topic_sample_count")),
        "topic_win_rate": _safe_float(event.get("topic_win_rate")),
        "topic_roi": _safe_float(event.get("topic_roi")),
        "topic_bias": str(event.get("topic_bias") or ""),
        "topic_multiplier": _safe_float(event.get("topic_multiplier"), 1.0),
        "decision_max_notional": _safe_float(event.get("decision_max_notional")),
        "score_sized_notional": _safe_float(event.get("score_sized_notional")),
        "requested_notional": _safe_float(event.get("requested_notional")),
        "filled_notional": _safe_float(event.get("filled_notional") or event.get("trim_notional") or event.get("notional")),
        "notional": notional,
        "hold_minutes": _safe_int(event.get("hold_minutes")),
        "position_action": str(event.get("position_action") or ""),
        "position_action_label": str(event.get("position_action_label") or ""),
        "exit_kind": exit_kind,
        "exit_label": str(event.get("exit_label") or ""),
        "exit_result": str(event.get("exit_result") or ""),
        "exit_result_label": str(event.get("exit_result_label") or ""),
        "exit_fraction": exit_fraction,
        "exit_wallet_count": _safe_int(event.get("exit_wallet_count")),
        "cross_wallet_exit": bool(event.get("cross_wallet_exit", False)),
        "reason": str(event.get("reason") or ""),
    }


def load_runtime_cycle_index(path: Path | None) -> dict[str, dict[str, Any]]:
    if path is None or not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    if not isinstance(payload, Mapping):
        return {}
    cycles = payload.get("signal_cycles")
    if not isinstance(cycles, list):
        return {}

    index: dict[str, dict[str, Any]] = {}
    for cycle in cycles:
        if not isinstance(cycle, Mapping):
            continue
        cycle_id = str(cycle.get("cycle_id") or "").strip()
        if not cycle_id:
            continue
        wallet_pool_snapshot = list(cycle.get("wallet_pool_snapshot") or [])
        wallets = list(cycle.get("wallets") or [])
        normalized_pool: list[dict[str, Any]] = []
        for item in wallet_pool_snapshot:
            if not isinstance(item, Mapping):
                continue
            normalized_pool.append(
                {
                    "wallet": str(item.get("wallet") or ""),
                    "wallet_score": round(_safe_float(item.get("wallet_score")), 2),
                    "wallet_tier": str(item.get("wallet_tier") or ""),
                    "trading_enabled": bool(item.get("trading_enabled", False)),
                }
            )
        if not normalized_pool and wallets:
            normalized_pool = [{"wallet": str(wallet), "wallet_score": 0.0, "wallet_tier": ""} for wallet in wallets]
        digest_source = json.dumps(normalized_pool, ensure_ascii=False, sort_keys=True)
        digest = hashlib.sha1(digest_source.encode("utf-8")).hexdigest()[:8] if normalized_pool else ""
        top_wallets = [str(item.get("wallet") or "") for item in normalized_pool[:3] if str(item.get("wallet") or "")]
        index[cycle_id] = {
            "cycle_id": cycle_id,
            "wallet_pool_version": digest,
            "wallet_pool_label": f"{digest}:{len(normalized_pool)}w" if digest else "",
            "wallet_pool_size": len(normalized_pool),
            "wallet_pool_top": top_wallets,
        }
    return index


def load_replay_samples(
    path: Path,
    *,
    runtime_state_path: Path | None = None,
) -> list[dict[str, Any]]:
    cycle_index = load_runtime_cycle_index(runtime_state_path)
    samples: list[dict[str, Any]] = []
    for event in iter_replay_events(path):
        sample = normalize_replay_event(event)
        cycle_id = str(sample.get("cycle_id") or "")
        cycle_meta = cycle_index.get(cycle_id, {})
        sample["wallet_pool_version"] = str(cycle_meta.get("wallet_pool_version") or "")
        sample["wallet_pool_label"] = str(cycle_meta.get("wallet_pool_label") or "")
        sample["wallet_pool_size"] = _safe_int(cycle_meta.get("wallet_pool_size"))
        sample["wallet_pool_top"] = list(cycle_meta.get("wallet_pool_top") or [])
        samples.append(sample)
    return samples


def _wallet_score_multiplier(score: float, scenario: ReplayScenario) -> float:
    if score >= 80.0:
        return float(scenario.wallet_score_core_multiplier)
    if score >= 65.0:
        return float(scenario.wallet_score_trade_multiplier)
    return float(scenario.wallet_score_watch_multiplier)


def _topic_multiplier(sample: Mapping[str, Any], scenario: ReplayScenario) -> float:
    sample_count = _safe_int(sample.get("topic_sample_count"))
    if sample_count < int(scenario.topic_min_samples):
        return 1.0
    roi = _safe_float(sample.get("topic_roi"))
    win_rate = _safe_float(sample.get("topic_win_rate"))
    if roi >= float(scenario.topic_positive_roi) and win_rate >= float(scenario.topic_positive_win_rate):
        return float(scenario.topic_boost_multiplier)
    if roi <= float(scenario.topic_negative_roi) or win_rate <= float(scenario.topic_negative_win_rate):
        return float(scenario.topic_penalty_multiplier)
    return 1.0


def _topic_matches(sample: Mapping[str, Any], topic_filter: set[str] | None) -> bool:
    if not topic_filter:
        return True
    value = str(sample.get("topic_label") or "").strip().lower()
    return value in topic_filter if value else False


def _wallet_pool_matches(sample: Mapping[str, Any], wallet_pool_filter: set[str] | None) -> bool:
    if not wallet_pool_filter:
        return True
    pool_version = str(sample.get("wallet_pool_version") or "").strip().lower()
    pool_label = str(sample.get("wallet_pool_label") or "").strip().lower()
    if pool_version and pool_version in wallet_pool_filter:
        return True
    if pool_label and pool_label in wallet_pool_filter:
        return True
    return False


def _base_entry_notional(sample: Mapping[str, Any]) -> float:
    decision_max = _safe_float(sample.get("decision_max_notional"))
    if decision_max > 0:
        return decision_max
    score_sized = _safe_float(sample.get("score_sized_notional"))
    if score_sized > 0:
        return score_sized
    requested = _safe_float(sample.get("requested_notional"))
    if requested > 0:
        return requested
    return _safe_float(sample.get("filled_notional") or sample.get("notional"))


def _simulate_entry_notional(sample: Mapping[str, Any], scenario: ReplayScenario) -> float:
    base = _base_entry_notional(sample)
    if base <= 0:
        return 0.0
    sized = base * _wallet_score_multiplier(_safe_float(sample.get("wallet_score")), scenario)
    return sized * _topic_multiplier(sample, scenario)


def _simulate_exit_notional(sample: Mapping[str, Any], scenario: ReplayScenario) -> tuple[float, bool]:
    filled = _safe_float(sample.get("filled_notional") or sample.get("notional"))
    if filled <= 0:
        return 0.0, False

    exit_kind = str(sample.get("exit_kind") or "").strip().lower()
    exit_fraction = _safe_float(sample.get("exit_fraction"))
    hold_minutes = _safe_int(sample.get("hold_minutes"))
    tier = str(sample.get("wallet_tier") or "").upper()

    if exit_kind == "time_exit":
        if hold_minutes > 0 and hold_minutes < int(scenario.stale_position_minutes):
            return 0.0, True
        baseline_fraction = exit_fraction if exit_fraction > 0 else 0.4
        base_position = filled / max(0.01, baseline_fraction)
        return min(base_position, base_position * float(scenario.stale_position_trim_pct)), False

    if exit_kind == "resonance_exit":
        baseline_fraction = exit_fraction if exit_fraction > 0 else (0.6 if tier == "CORE" else 0.35)
        base_position = filled / max(0.01, baseline_fraction)
        target_fraction = (
            float(scenario.resonance_core_exit_fraction)
            if tier == "CORE" or baseline_fraction >= 0.5
            else float(scenario.resonance_trim_fraction)
        )
        return min(base_position, base_position * target_fraction), False

    return filled, False


def evaluate_replay_scenario(
    samples: Iterable[Mapping[str, Any]],
    scenario: ReplayScenario,
    *,
    topic_filter: set[str] | None = None,
    wallet_pool_filter: set[str] | None = None,
) -> dict[str, Any]:
    filtered = [
        sample
        for sample in samples
        if _topic_matches(sample, topic_filter)
        and _wallet_pool_matches(sample, wallet_pool_filter)
    ]
    exit_mix: Counter[str] = Counter()
    reject_mix: Counter[str] = Counter()
    entry_count = 0
    exit_count = 0
    rejected_count = 0
    deferred_exit_count = 0
    total_hold_minutes = 0
    hold_samples = 0
    max_hold_minutes = 0
    simulated_entry_notional = 0.0
    simulated_exit_notional = 0.0

    for sample in filtered:
        status = str(sample.get("status") or "").upper()
        flow = str(sample.get("flow") or "")
        hold_minutes = _safe_int(sample.get("hold_minutes"))
        if hold_minutes > 0:
            total_hold_minutes += hold_minutes
            hold_samples += 1
            max_hold_minutes = max(max_hold_minutes, hold_minutes)

        if flow == "entry":
            if status == "FILLED":
                entry_count += 1
                simulated_entry_notional += _simulate_entry_notional(sample, scenario)
            elif status == "REJECTED":
                rejected_count += 1
                reject_mix[str(sample.get("reason") or "unknown")] += 1
            continue

        exit_kind = str(sample.get("exit_kind") or "exit")
        if status == "FILLED":
            exit_count += 1
            simulated_notional, deferred = _simulate_exit_notional(sample, scenario)
            if deferred:
                deferred_exit_count += 1
            else:
                simulated_exit_notional += simulated_notional
                exit_mix[exit_kind] += 1
        elif status == "REJECTED":
            rejected_count += 1
            reject_mix[str(sample.get("reason") or "unknown")] += 1

    total_actions = entry_count + exit_count + rejected_count
    reject_rate = 0.0 if total_actions <= 0 else rejected_count / total_actions
    avg_hold_minutes = 0.0 if hold_samples <= 0 else total_hold_minutes / hold_samples
    cashflow_proxy = simulated_exit_notional - simulated_entry_notional
    return {
        "scenario": scenario.name,
        "sample_count": len(filtered),
        "entry_count": int(entry_count),
        "exit_count": int(exit_count),
        "rejected_count": int(rejected_count),
        "deferred_exit_count": int(deferred_exit_count),
        "reject_rate": round(reject_rate, 4),
        "avg_hold_minutes": round(avg_hold_minutes, 1),
        "max_hold_minutes": int(max_hold_minutes),
        "simulated_entry_notional": round(simulated_entry_notional, 2),
        "simulated_exit_notional": round(simulated_exit_notional, 2),
        "cashflow_proxy": round(cashflow_proxy, 2),
        "exit_mix": dict(exit_mix),
        "reject_mix": dict(reject_mix.most_common(5)),
        "topic_filter": sorted(topic_filter) if topic_filter else [],
        "wallet_pool_filter": sorted(wallet_pool_filter) if wallet_pool_filter else [],
    }


def evaluate_replay_matrix(
    samples: Iterable[Mapping[str, Any]],
    scenarios: Iterable[ReplayScenario],
    *,
    topic_filter: set[str] | None = None,
    wallet_pool_filter: set[str] | None = None,
) -> dict[str, Any]:
    scenario_rows = [
        evaluate_replay_scenario(
            samples,
            scenario,
            topic_filter=topic_filter,
            wallet_pool_filter=wallet_pool_filter,
        )
        for scenario in scenarios
    ]
    scenario_rows.sort(
        key=lambda row: (
            float(row.get("cashflow_proxy") or 0.0),
            -float(row.get("reject_rate") or 0.0),
            -float(row.get("avg_hold_minutes") or 0.0),
            str(row.get("scenario") or ""),
        ),
        reverse=True,
    )
    recommended = scenario_rows[0] if scenario_rows else {}
    return {
        "summary": {
            "scenarios": int(len(scenario_rows)),
            "topic_filter": sorted(topic_filter) if topic_filter else [],
            "wallet_pool_filter": sorted(wallet_pool_filter) if wallet_pool_filter else [],
        },
        "rows": scenario_rows,
        "recommended": recommended,
    }


def summarize_wallet_pools(samples: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    buckets: dict[str, dict[str, Any]] = {}
    for sample in samples:
        pool_version = str(sample.get("wallet_pool_version") or "").strip()
        if not pool_version:
            continue
        bucket = buckets.setdefault(
            pool_version,
            {
                "wallet_pool_version": pool_version,
                "wallet_pool_label": str(sample.get("wallet_pool_label") or ""),
                "wallet_pool_size": _safe_int(sample.get("wallet_pool_size")),
                "wallet_pool_top": list(sample.get("wallet_pool_top") or []),
                "sample_count": 0,
            },
        )
        bucket["sample_count"] = int(bucket.get("sample_count") or 0) + 1
    return sorted(
        buckets.values(),
        key=lambda row: (
            int(row.get("sample_count") or 0),
            int(row.get("wallet_pool_size") or 0),
            str(row.get("wallet_pool_version") or ""),
        ),
        reverse=True,
    )


def format_replay_matrix(matrix: Mapping[str, Any]) -> str:
    rows = list(matrix.get("rows") or [])
    if not rows:
        return "no replay rows"
    lines = [
        "scenario | sample | entry | exit | reject | reject_rate | avg_hold | cashflow_proxy",
        "--- | --- | --- | --- | --- | --- | --- | ---",
    ]
    for row in rows:
        lines.append(
            f"{row['scenario']} | {row['sample_count']} | {row['entry_count']} | {row['exit_count']} | "
            f"{row['rejected_count']} | {row['reject_rate']:.1%} | {row['avg_hold_minutes']:.1f}m | {row['cashflow_proxy']:.2f}"
        )
    return "\n".join(lines)
