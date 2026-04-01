#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.config import Settings
from polymarket_bot.replay_calibration import (
    evaluate_replay_matrix,
    format_replay_matrix,
    load_replay_samples,
    load_replay_scenarios,
    summarize_wallet_pools,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Lightweight replay calibration for polymarket runtime events")
    parser.add_argument("--events", default="/tmp/poly_runtime_data/events.ndjson")
    parser.add_argument("--runtime-state", default="/tmp/poly_runtime_data/runtime_state.json")
    parser.add_argument("--scenario-file", default="", help="JSON file containing one or many replay scenarios")
    parser.add_argument("--topic", default="", help="Comma-separated topic filter, e.g. crypto,politics")
    parser.add_argument("--wallet-pool", default="", help="Comma-separated wallet-pool version or label filter")
    parser.add_argument("--list-wallet-pools", action="store_true", help="List discovered wallet-pool versions")
    parser.add_argument("--json", action="store_true", help="Output JSON instead of markdown table")
    args = parser.parse_args()

    settings = Settings()
    events_path = Path(args.events).expanduser()
    runtime_state_path = Path(args.runtime_state).expanduser()
    scenario_path = Path(args.scenario_file).expanduser() if args.scenario_file else None
    topic_filter = {
        item.strip().lower()
        for item in str(args.topic or "").split(",")
        if item.strip()
    }
    wallet_pool_filter = {
        item.strip().lower()
        for item in str(args.wallet_pool or "").split(",")
        if item.strip()
    }

    samples = load_replay_samples(events_path, runtime_state_path=runtime_state_path)
    wallet_pools = summarize_wallet_pools(samples)
    if args.list_wallet_pools:
        payload = {
            "events_path": str(events_path),
            "runtime_state_path": str(runtime_state_path),
            "wallet_pools": wallet_pools,
        }
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    scenarios = load_replay_scenarios(scenario_path, settings)
    matrix = evaluate_replay_matrix(
        samples,
        scenarios,
        topic_filter=topic_filter or None,
        wallet_pool_filter=wallet_pool_filter or None,
    )
    payload = {
        "events_path": str(events_path),
        "runtime_state_path": str(runtime_state_path),
        "sample_count": len(samples),
        "wallet_pools": wallet_pools,
        **matrix,
    }

    if args.json:
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0

    print(f"events: {events_path}")
    print(f"samples: {len(samples)}")
    if topic_filter:
        print(f"topic_filter: {', '.join(sorted(topic_filter))}")
    if wallet_pool_filter:
        print(f"wallet_pool_filter: {', '.join(sorted(wallet_pool_filter))}")
    print()
    print(format_replay_matrix(matrix))
    recommended = payload.get("recommended") or {}
    if recommended:
        print("\nrecommended:")
        print(
            f"{recommended.get('scenario')} | gross_cashflow={float(recommended.get('cashflow_proxy') or 0.0):.2f} "
            f"| net_cashflow={float(recommended.get('net_cashflow_proxy') or 0.0):.2f} "
            f"| reject_rate={float(recommended.get('reject_rate') or 0.0):.1%} "
            f"| avg_hold={float(recommended.get('avg_hold_minutes') or 0.0):.1f}m"
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
