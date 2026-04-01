#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


CHECKPOINT_RE = re.compile(r"^checkpoint(?P<num>\d+)\b")


@dataclass
class Checkpoint:
    number: int
    label: str
    ts: str
    open_positions: str
    max_open_positions: str
    slot_utilization_pct: str
    tracked_notional_usd: str
    available_notional_usd: str
    daily_loss_used_pct: str
    cooldown_skips_last_window: str
    time_exit_close_last_window: str
    report: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--thread-id", required=True)
    parser.add_argument("--rollout-path", required=True)
    parser.add_argument("--threads-db", required=True)
    parser.add_argument("--automation-db", required=True)
    parser.add_argument("--automation-toml", required=True)
    parser.add_argument("--automation-id", required=True)
    parser.add_argument("--automation-thread-id")
    parser.add_argument("--rehearsal-file", required=True)
    parser.add_argument("--state-file", required=True)
    parser.add_argument("--interval-seconds", type=int, default=300)
    parser.add_argument("--daemon", action="store_true")
    parser.add_argument("--pause-automation", action="store_true")
    parser.add_argument("--archive-automation-thread", action="store_true")
    return parser.parse_args()


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def epoch_ms() -> int:
    return int(time.time() * 1000)


def read_state(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}


def write_state(path: Path, state: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def parse_latest_checkpoint(path: Path) -> Checkpoint | None:
    if not path.exists():
        return None

    latest: Checkpoint | None = None
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        match = CHECKPOINT_RE.match(line)
        if not match:
            continue

        parts = line.split()
        if len(parts) < 18:
            continue

        latest = Checkpoint(
            number=int(match.group("num")),
            label=parts[0],
            ts=f"{parts[1]} {parts[2]}",
            open_positions=parts[5],
            max_open_positions=parts[6],
            slot_utilization_pct=parts[7],
            tracked_notional_usd=parts[8],
            available_notional_usd=parts[9],
            daily_loss_used_pct=parts[10],
            cooldown_skips_last_window=parts[12],
            time_exit_close_last_window=parts[13],
            report=parts[17],
        )

    return latest


def build_message(checkpoint: Checkpoint) -> str:
    lines = [
        "检测到新的 10h 演练 checkpoint，已按要求回到本线程汇报：",
        "",
        f"- checkpoint 编号: `{checkpoint.label}`",
        f"- 时间: `{checkpoint.ts}`",
        f"- open/max: `{checkpoint.open_positions}/{checkpoint.max_open_positions}`",
        f"- slot utilization: `{checkpoint.slot_utilization_pct}%`",
        f"- tracked/available notional: `{checkpoint.tracked_notional_usd} / {checkpoint.available_notional_usd} USD`",
        f"- daily loss used: `{checkpoint.daily_loss_used_pct}%`",
        f"- cooldown skips: `{checkpoint.cooldown_skips_last_window}`",
        f"- time_exit_close: `{checkpoint.time_exit_close_last_window}`",
        f"- 结论: `{checkpoint.report}`",
        "",
    ]

    if checkpoint.number >= 10:
        lines.append("已到 `checkpoint10`，本轮 10h 演练完成。")
    else:
        lines.append("当前尚未到 `checkpoint10`，继续等待下一条 checkpoint。")

    return "\n".join(lines)


def append_message(rollout_path: Path, message: str) -> None:
    timestamp = utc_now_iso()
    records = [
        {
            "timestamp": timestamp,
            "type": "event_msg",
            "payload": {
                "type": "agent_message",
                "message": message,
                "phase": "final_answer",
            },
        },
        {
            "timestamp": timestamp,
            "type": "response_item",
            "payload": {
                "type": "message",
                "role": "assistant",
                "content": [{"type": "output_text", "text": message}],
                "phase": "final_answer",
            },
        },
    ]

    rollout_path.parent.mkdir(parents=True, exist_ok=True)
    with rollout_path.open("a", encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False))
            handle.write("\n")


def touch_thread(threads_db: Path, thread_id: str) -> None:
    now_ms = epoch_ms()
    with sqlite3.connect(threads_db) as conn:
        conn.execute(
            "UPDATE threads SET updated_at = ? WHERE id = ?",
            (now_ms, thread_id),
        )
        conn.commit()


def pause_automation(automation_toml: Path, automation_db: Path, automation_id: str) -> None:
    text = automation_toml.read_text(encoding="utf-8")
    if 'status = "ACTIVE"' in text:
        text = text.replace('status = "ACTIVE"', 'status = "PAUSED"', 1)
        automation_toml.write_text(text, encoding="utf-8")

    now_ms = epoch_ms()
    with sqlite3.connect(automation_db) as conn:
        conn.execute(
            "UPDATE automations SET status = 'PAUSED', updated_at = ? WHERE id = ?",
            (now_ms, automation_id),
        )
        conn.commit()


def archive_automation_thread(threads_db: Path, automation_db: Path, automation_id: str, automation_thread_id: str) -> None:
    now_ms = epoch_ms()
    with sqlite3.connect(threads_db) as conn:
        conn.execute(
            "UPDATE threads SET archived = 1, archived_at = ?, updated_at = ? WHERE id = ?",
            (now_ms, now_ms, automation_thread_id),
        )
        conn.commit()

    with sqlite3.connect(automation_db) as conn:
        conn.execute(
            "UPDATE automation_runs SET status = 'ARCHIVED', updated_at = ? WHERE automation_id = ? AND thread_id = ?",
            (now_ms, automation_id, automation_thread_id),
        )
        conn.commit()


def maybe_report(args: argparse.Namespace) -> bool:
    rehearsal_file = Path(args.rehearsal_file)
    state_file = Path(args.state_file)
    rollout_path = Path(args.rollout_path)
    threads_db = Path(args.threads_db)

    latest = parse_latest_checkpoint(rehearsal_file)
    if latest is None:
        return False

    state = read_state(state_file)
    last_checkpoint = int(state.get("last_reported_checkpoint_number", 0))
    if latest.number <= last_checkpoint:
        return latest.number >= 10

    message = build_message(latest)
    append_message(rollout_path, message)
    touch_thread(threads_db, args.thread_id)

    state.update(
        {
            "last_reported_checkpoint_number": latest.number,
            "last_reported_checkpoint_label": latest.label,
            "last_reported_checkpoint_ts": latest.ts,
            "last_message_at": utc_now_iso(),
        }
    )
    write_state(state_file, state)

    print(f"reported {latest.label} to thread {args.thread_id}", flush=True)
    return latest.number >= 10


def main() -> int:
    args = parse_args()

    if args.pause_automation:
        pause_automation(Path(args.automation_toml), Path(args.automation_db), args.automation_id)

    if args.archive_automation_thread and args.automation_thread_id:
        archive_automation_thread(
            Path(args.threads_db),
            Path(args.automation_db),
            args.automation_id,
            args.automation_thread_id,
        )

    done = maybe_report(args)
    if not args.daemon:
        return 0

    while not done:
        time.sleep(max(args.interval_seconds, 30))
        done = maybe_report(args)

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        sys.exit(130)
