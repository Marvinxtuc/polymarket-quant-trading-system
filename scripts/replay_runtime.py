#!/usr/bin/env python3
from __future__ import annotations

import argparse
import collections
from collections.abc import Iterator
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        with path.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception:
        return None
    return payload if isinstance(payload, dict) else None


def _iter_events(path: Path) -> Iterator[dict[str, Any]]:
    if not path.exists():
        return
    try:
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    item = json.loads(line)
                except Exception:
                    continue
                if isinstance(item, dict):
                    yield item
    except Exception:
        return


def _fmt_ts(ts: Any) -> str:
    try:
        ts_int = int(ts)
    except (TypeError, ValueError):
        return "-"
    return datetime.fromtimestamp(ts_int, tz=timezone.utc).isoformat()


def _safe_ts(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _replay_positions(events: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], float, int]:
    positions: dict[str, float] = {}

    for ev in events:
        etype = str(ev.get("type") or "").strip()
        token = str(ev.get("token_id") or "").strip()
        if not token:
            continue

        if etype in {"order_filled", "time_exit_fill", "emergency_exit_partial"}:
            side = str(ev.get("side") or ("SELL" if etype in {"time_exit_fill", "emergency_exit_partial"} else "")).upper()
            notional = float(ev.get("notional") or ev.get("trim_notional") or 0.0)
            if notional <= 0:
                continue

            if side == "BUY":
                positions[token] = positions.get(token, 0.0) + notional
            elif side == "SELL":
                remaining = max(0.0, positions.get(token, 0.0) - notional)
                positions[token] = remaining

    final = [
        {"token_id": token, "notional": notional}
        for token, notional in positions.items()
        if notional > 0
    ]
    final_notional = sum(item["notional"] for item in final)
    final_count = len(final)

    return final, final_notional, final_count


def main() -> int:
    parser = argparse.ArgumentParser(description="Replay and summarize polymarket runtime artifacts")
    parser.add_argument("--runtime-state", default="/tmp/poly_runtime_data/runtime_state.json")
    parser.add_argument("--events", default="/tmp/poly_runtime_data/events.ndjson")
    parser.add_argument("--json", action="store_true", help="Output summary as JSON")
    args = parser.parse_args()

    runtime_path = Path(args.runtime_state).expanduser()
    events_path = Path(args.events).expanduser()

    runtime = _load_json(runtime_path)
    events: list[dict[str, Any]] = list(_iter_events(events_path))

    runtime_open_positions = 0
    runtime_notional = 0.0
    runtime_version = runtime.get("runtime_version", 0) if isinstance(runtime, dict) else 0
    runtime_ts = runtime.get("ts") if isinstance(runtime, dict) else None

    if isinstance(runtime, dict):
        runtime_positions = runtime.get("positions")
        if isinstance(runtime_positions, list):
            for item in runtime_positions:
                if not isinstance(item, dict):
                    continue
                runtime_open_positions += 1
                runtime_notional += float(item.get("notional") or 0.0)

    event_types = collections.Counter(ev.get("type") for ev in events if isinstance(ev, dict))
    event_timeline = sorted([ev for ev in events if isinstance(ev, dict)], key=lambda e: _safe_ts(e.get("ts")))
    replay_positions, replay_notional, replay_count = _replay_positions(events)

    buy_total = 0.0
    sell_total = 0.0
    for ev in events:
        if not isinstance(ev, dict):
            continue
        etype = str(ev.get("type") or "")
        if etype not in {"order_filled", "time_exit_fill", "emergency_exit_partial"}:
            continue

        amount = float(ev.get("notional") or ev.get("trim_notional") or 0.0)
        if amount <= 0:
            continue

        side = str(ev.get("side") or "").upper()
        if etype in {"time_exit_fill", "emergency_exit_partial"}:
            side = side or "SELL"

        if side == "BUY":
            buy_total += amount
        elif side == "SELL":
            sell_total += amount

    summary = {
        "runtime_state": {
            "path": str(runtime_path),
            "exists": bool(runtime is not None),
            "runtime_version": runtime_version,
            "timestamp": _fmt_ts(runtime_ts),
            "open_positions": int(runtime_open_positions),
            "tracked_notional_usd": float(runtime_notional),
        },
        "events": {
            "path": str(events_path),
            "exists": bool(events_path.exists()),
            "count": int(len(events)),
            "last_event_ts": _fmt_ts(event_timeline[-1].get("ts") if event_timeline else None),
            "types": dict(event_types),
            "total_buy_notional": float(buy_total),
            "total_sell_notional": float(sell_total),
        },
        "replay": {
            "reconstructed_open_positions": int(replay_count),
            "reconstructed_tracked_notional_usd": float(replay_notional),
            "positions": replay_positions[:20],
            "tail_events": event_timeline[-10:],
        },
        "drift": {
            "positions_delta": int(replay_count) - int(runtime_open_positions),
            "notional_delta_usd": float(replay_notional - runtime_notional),
        },
    }

    if args.json:
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return 0

    print("-- runtime state --")
    runtime_section = summary["runtime_state"]
    print(f"path: {runtime_section['path']}")
    print(f"exists: {runtime_section['exists']}")
    print(f"version: {runtime_section['runtime_version']}")
    print(f"ts: {runtime_section['timestamp']}")
    print(f"open_positions: {runtime_section['open_positions']}")
    print(f"tracked_notional_usd: {runtime_section['tracked_notional_usd']:.4f}")

    print("\n-- events --")
    events_section = summary["events"]
    print(f"path: {events_section['path']}")
    print(f"exists: {events_section['exists']}")
    print(f"count: {events_section['count']}")
    print(f"last_event_ts: {events_section['last_event_ts']}")
    print(f"total_buy_notional: {events_section['total_buy_notional']:.4f}")
    print(f"total_sell_notional: {events_section['total_sell_notional']:.4f}")
    for typ, qty in sorted(events_section["types"].items()):
        print(f"  {typ}: {qty}")

    print("\n-- replay --")
    replay_section = summary["replay"]
    print(f"reconstructed_open_positions: {replay_section['reconstructed_open_positions']}")
    print(f"reconstructed_tracked_notional_usd: {replay_section['reconstructed_tracked_notional_usd']:.4f}")
    print(f"delta_positions: {summary['drift']['positions_delta']}")
    print(f"delta_notional_usd: {summary['drift']['notional_delta_usd']:.4f}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
