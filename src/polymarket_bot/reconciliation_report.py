from __future__ import annotations

import json
import sqlite3
import time
from contextlib import closing
from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def _utc_day_key(ts: int | None = None) -> str:
    moment = datetime.fromtimestamp(int(ts or time.time()), tz=timezone.utc)
    return moment.strftime("%Y-%m-%d")


_SQLITE_LEDGER_SUFFIXES = {".db", ".sqlite", ".sqlite3"}


def _is_sqlite_ledger_path(path: str) -> bool:
    return Path(str(path or "").strip()).suffix.lower() in _SQLITE_LEDGER_SUFFIXES


def _ensure_parent_dir(path: str) -> None:
    parent = Path(str(path or "").strip()).expanduser().parent
    if str(parent) in {"", "."}:
        return
    parent.mkdir(parents=True, exist_ok=True)


def _ledger_record_from_payload(
    entry_type: str,
    payload: Mapping[str, object],
    *,
    broker: str = "",
) -> dict[str, object]:
    record = dict(payload)
    ts = _safe_int(record.get("ts"), int(time.time()))
    record["ts"] = ts
    record["day_key"] = str(record.get("day_key") or _utc_day_key(ts))
    record["type"] = str(entry_type or record.get("type") or "")
    record["broker"] = str(record.get("broker") or broker or "")
    return record


def append_ledger_entry(
    path: str,
    entry_type: str,
    payload: Mapping[str, object],
    *,
    broker: str = "",
) -> None:
    target = str(path or "").strip()
    if not target:
        return

    record = _ledger_record_from_payload(entry_type, payload, broker=broker)
    if _is_sqlite_ledger_path(target):
        _ensure_parent_dir(target)
        with closing(sqlite3.connect(target, timeout=5.0)) as conn:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=5000")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS ledger_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts INTEGER NOT NULL,
                    day_key TEXT NOT NULL,
                    type TEXT NOT NULL,
                    broker TEXT NOT NULL DEFAULT '',
                    payload_json TEXT NOT NULL
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ledger_entries_day_key ON ledger_entries(day_key, id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_ledger_entries_type ON ledger_entries(type, id)")
            conn.execute(
                """
                INSERT INTO ledger_entries (ts, day_key, type, broker, payload_json)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    int(record.get("ts") or 0),
                    str(record.get("day_key") or ""),
                    str(record.get("type") or ""),
                    str(record.get("broker") or ""),
                    json.dumps(record, ensure_ascii=False),
                ),
            )
            conn.commit()
        return

    _ensure_parent_dir(target)
    with open(target, "a", encoding="utf-8") as f:
        json.dump(record, f, ensure_ascii=False)
        f.write("\n")


def load_json_dict(path: str) -> dict[str, object]:
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            payload = json.load(f)
        if isinstance(payload, dict):
            return payload
    except FileNotFoundError:
        return {}
    except Exception:
        return {}
    return {}


def load_ledger_rows(
    path: str,
    *,
    day_key: str | None = None,
    broker: str | None = None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    if not path:
        return rows
    target = str(path or "").strip()
    if not target:
        return rows
    if _is_sqlite_ledger_path(target):
        try:
            with closing(sqlite3.connect(target, timeout=5.0)) as conn:
                conn.row_factory = sqlite3.Row
                try:
                    if day_key and broker:
                        cursor = conn.execute(
                            """
                            SELECT ts, day_key, type, broker, payload_json
                            FROM ledger_entries
                            WHERE day_key = ? AND broker = ?
                            ORDER BY id
                            """,
                            (str(day_key), str(broker)),
                        )
                    elif day_key:
                        cursor = conn.execute(
                            """
                            SELECT ts, day_key, type, broker, payload_json
                            FROM ledger_entries
                            WHERE day_key = ?
                            ORDER BY id
                            """,
                            (str(day_key),),
                        )
                    else:
                        cursor = conn.execute(
                            """
                            SELECT ts, day_key, type, broker, payload_json
                            FROM ledger_entries
                            ORDER BY id
                            """
                        )
                except sqlite3.OperationalError:
                    return rows
                for row in cursor:
                    try:
                        payload = json.loads(str(row["payload_json"] or ""))
                    except Exception:
                        continue
                    if not isinstance(payload, dict):
                        continue
                    payload.setdefault("ts", _safe_int(row["ts"]))
                    payload.setdefault("day_key", str(row["day_key"] or ""))
                    payload.setdefault("type", str(row["type"] or ""))
                    payload.setdefault("broker", str(row["broker"] or ""))
                    rows.append(payload)
        except FileNotFoundError:
            return rows
        except Exception:
            return rows
        return rows
    try:
        with open(target, "r", encoding="utf-8") as f:
            for line in f:
                text = line.strip()
                if not text:
                    continue
                try:
                    row = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if not isinstance(row, dict):
                    continue
                if day_key and str(row.get("day_key") or "") != day_key:
                    continue
                if broker and str(row.get("broker") or "") != broker:
                    continue
                rows.append(row)
    except FileNotFoundError:
        return rows
    except Exception:
        return rows
    return rows


def summarize_ledger(rows: list[dict[str, object]]) -> dict[str, object]:
    counts_by_type: dict[str, int] = defaultdict(int)
    fill_by_source: dict[str, dict[str, object]] = {}
    fill_by_side: dict[str, dict[str, object]] = {}
    total_entries = 0
    fill_count = 0
    fill_notional = 0.0
    realized_pnl = 0.0
    buy_fill_count = 0
    sell_fill_count = 0
    latest_ts = 0

    for row in rows:
        total_entries += 1
        entry_type = str(row.get("type") or "")
        counts_by_type[entry_type] += 1
        ts = _safe_int(row.get("ts"))
        latest_ts = max(latest_ts, ts)
        if entry_type != "fill":
            continue
        fill_count += 1
        notional = max(0.0, _safe_float(row.get("notional")))
        pnl = _safe_float(row.get("realized_pnl"))
        side = str(row.get("side") or "").upper() or "UNKNOWN"
        source = str(row.get("source") or "unknown")
        fill_notional += notional
        realized_pnl += pnl
        if side == "BUY":
            buy_fill_count += 1
        elif side == "SELL":
            sell_fill_count += 1

        source_bucket = fill_by_source.setdefault(
            source,
            {"source": source, "fill_count": 0, "notional": 0.0, "realized_pnl": 0.0},
        )
        source_bucket["fill_count"] = int(source_bucket["fill_count"]) + 1
        source_bucket["notional"] = float(source_bucket["notional"]) + notional
        source_bucket["realized_pnl"] = float(source_bucket["realized_pnl"]) + pnl

        side_bucket = fill_by_side.setdefault(
            side,
            {"side": side, "fill_count": 0, "notional": 0.0, "realized_pnl": 0.0},
        )
        side_bucket["fill_count"] = int(side_bucket["fill_count"]) + 1
        side_bucket["notional"] = float(side_bucket["notional"]) + notional
        side_bucket["realized_pnl"] = float(side_bucket["realized_pnl"]) + pnl

    return {
        "total_entries": total_entries,
        "fill_count": fill_count,
        "buy_fill_count": buy_fill_count,
        "sell_fill_count": sell_fill_count,
        "fill_notional": fill_notional,
        "realized_pnl": realized_pnl,
        "account_sync_count": int(counts_by_type.get("account_sync", 0)),
        "startup_checks_count": int(counts_by_type.get("startup_checks", 0)),
        "counts_by_type": dict(sorted(counts_by_type.items())),
        "fill_by_source": sorted(
            fill_by_source.values(),
            key=lambda item: (
                -int(item.get("fill_count") or 0),
                -float(item.get("notional") or 0.0),
                str(item.get("source") or ""),
            ),
        ),
        "fill_by_side": sorted(
            fill_by_side.values(),
            key=lambda item: (
                str(item.get("side") or ""),
            ),
        ),
        "latest_ts": latest_ts,
    }


def build_reconciliation_report(
    *,
    state: Mapping[str, object],
    ledger_rows: list[dict[str, object]],
    state_path: str = "",
    ledger_path: str = "",
    day_key: str = "",
    generated_ts: int | None = None,
) -> dict[str, object]:
    ts = int(generated_ts or time.time())
    startup = dict(state.get("startup") or {}) if isinstance(state.get("startup"), dict) else {}
    reconciliation = dict(state.get("reconciliation") or {}) if isinstance(state.get("reconciliation"), dict) else {}
    summary = dict(state.get("summary") or {}) if isinstance(state.get("summary"), dict) else {}
    resolved_day_key = str(day_key or reconciliation.get("day_key") or _utc_day_key(ts))
    ledger_summary = summarize_ledger(ledger_rows)

    internal_realized_pnl = _safe_float(reconciliation.get("internal_realized_pnl"), _safe_float(summary.get("internal_pnl_today")))
    ledger_realized_pnl = _safe_float(reconciliation.get("ledger_realized_pnl"), _safe_float(ledger_summary.get("realized_pnl")))
    broker_closed_pnl_today = _safe_float(reconciliation.get("broker_closed_pnl_today"), _safe_float(summary.get("broker_closed_pnl_today")))
    status = str(reconciliation.get("status") or "ok").lower()
    issues = [str(item) for item in list(reconciliation.get("issues") or []) if str(item).strip()]
    startup_ready = bool(startup.get("ready", True))

    recommendations: list[str] = []
    if not startup_ready:
        recommendations.append("Resolve startup readiness failures before trusting any live execution metrics.")
    if status == "fail":
        recommendations.append("Execution reconciliation is failing. Treat ledger/state drift as a production incident.")
    elif status == "warn":
        recommendations.append("Execution reconciliation has warnings. Review stale pending orders and sync freshness.")
    else:
        recommendations.append("Execution reconciliation is aligned for the selected day.")
    if int(ledger_summary.get("fill_count") or 0) <= 0:
        recommendations.append("No fill entries were recorded for the selected day.")
    if abs(_safe_float(reconciliation.get("internal_vs_ledger_diff"))) > 0.01:
        recommendations.append("Internal PnL and ledger PnL differ. Reconcile before promoting results.")

    return {
        "report_version": 1,
        "generated_ts": ts,
        "generated_at": datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(),
        "day_key": resolved_day_key,
        "state_path": state_path,
        "ledger_path": ledger_path,
        "status": status,
        "issues": issues,
        "startup": startup,
        "reconciliation": reconciliation,
        "state_summary": {
            "open_positions": _safe_int(reconciliation.get("open_positions"), _safe_int(summary.get("open_positions"))),
            "pending_orders": _safe_int(reconciliation.get("pending_orders")),
            "pending_entry_orders": _safe_int(reconciliation.get("pending_entry_orders")),
            "pending_exit_orders": _safe_int(reconciliation.get("pending_exit_orders")),
            "stale_pending_orders": _safe_int(reconciliation.get("stale_pending_orders")),
            "tracked_notional_usd": _safe_float(reconciliation.get("tracked_notional_usd"), _safe_float(summary.get("tracked_notional_usd"))),
            "equity_usd": _safe_float(summary.get("equity")),
            "cash_balance_usd": _safe_float(summary.get("cash_balance_usd")),
            "positions_value_usd": _safe_float(summary.get("positions_value_usd")),
            "internal_realized_pnl": internal_realized_pnl,
            "ledger_realized_pnl": ledger_realized_pnl,
            "broker_closed_pnl_today": broker_closed_pnl_today,
            "effective_daily_realized_pnl": _safe_float(reconciliation.get("effective_daily_realized_pnl"), internal_realized_pnl),
            "internal_vs_ledger_diff": _safe_float(reconciliation.get("internal_vs_ledger_diff"), internal_realized_pnl - ledger_realized_pnl),
            "broker_floor_gap_vs_internal": _safe_float(reconciliation.get("broker_floor_gap_vs_internal"), internal_realized_pnl - broker_closed_pnl_today),
            "account_snapshot_age_seconds": _safe_int(reconciliation.get("account_snapshot_age_seconds")),
            "broker_reconcile_age_seconds": _safe_int(reconciliation.get("broker_reconcile_age_seconds")),
            "broker_event_sync_age_seconds": _safe_int(reconciliation.get("broker_event_sync_age_seconds")),
        },
        "ledger_summary": ledger_summary,
        "recommendations": recommendations,
    }


def build_reconciliation_report_from_paths(
    *,
    state_path: str,
    ledger_path: str,
    day_key: str = "",
    generated_ts: int | None = None,
) -> dict[str, object]:
    state = load_json_dict(state_path)
    resolved_day_key = str(day_key or (state.get("reconciliation", {}) if isinstance(state.get("reconciliation"), dict) else {}).get("day_key") or _utc_day_key(generated_ts))
    config = dict(state.get("config") or {}) if isinstance(state.get("config"), dict) else {}
    broker_name = str(config.get("broker_name") or "").strip() or None
    rows = load_ledger_rows(ledger_path, day_key=resolved_day_key, broker=broker_name)
    return build_reconciliation_report(
        state=state,
        ledger_rows=rows,
        state_path=state_path,
        ledger_path=ledger_path,
        day_key=resolved_day_key,
        generated_ts=generated_ts,
    )


def render_reconciliation_report(report: Mapping[str, object]) -> str:
    startup = dict(report.get("startup") or {}) if isinstance(report.get("startup"), dict) else {}
    reconciliation = dict(report.get("reconciliation") or {}) if isinstance(report.get("reconciliation"), dict) else {}
    state_summary = dict(report.get("state_summary") or {}) if isinstance(report.get("state_summary"), dict) else {}
    ledger_summary = dict(report.get("ledger_summary") or {}) if isinstance(report.get("ledger_summary"), dict) else {}
    recommendations = [str(item) for item in list(report.get("recommendations") or []) if str(item).strip()]
    issues = [str(item) for item in list(report.get("issues") or []) if str(item).strip()]

    lines = [
        "Polymarket Reconciliation EOD Report",
        f"generated_at: {report.get('generated_at')}",
        f"day_key: {report.get('day_key')}",
        f"status: {report.get('status')}",
        f"state_path: {report.get('state_path')}",
        f"ledger_path: {report.get('ledger_path')}",
        "",
        "startup:",
        f"  ready: {startup.get('ready', True)}",
        f"  warning_count: {startup.get('warning_count', 0)}",
        f"  failure_count: {startup.get('failure_count', 0)}",
        "",
        "state_summary:",
        f"  open_positions: {state_summary.get('open_positions', 0)}",
        f"  pending_orders: {state_summary.get('pending_orders', 0)}",
        f"  tracked_notional_usd: {state_summary.get('tracked_notional_usd', 0.0)}",
        f"  internal_realized_pnl: {state_summary.get('internal_realized_pnl', 0.0)}",
        f"  ledger_realized_pnl: {state_summary.get('ledger_realized_pnl', 0.0)}",
        f"  internal_vs_ledger_diff: {state_summary.get('internal_vs_ledger_diff', 0.0)}",
        f"  broker_floor_gap_vs_internal: {state_summary.get('broker_floor_gap_vs_internal', 0.0)}",
        f"  account_snapshot_age_seconds: {state_summary.get('account_snapshot_age_seconds', 0)}",
        f"  broker_reconcile_age_seconds: {state_summary.get('broker_reconcile_age_seconds', 0)}",
        f"  broker_event_sync_age_seconds: {state_summary.get('broker_event_sync_age_seconds', 0)}",
        "",
        "ledger_summary:",
        f"  total_entries: {ledger_summary.get('total_entries', 0)}",
        f"  fill_count: {ledger_summary.get('fill_count', 0)}",
        f"  buy_fill_count: {ledger_summary.get('buy_fill_count', 0)}",
        f"  sell_fill_count: {ledger_summary.get('sell_fill_count', 0)}",
        f"  fill_notional: {ledger_summary.get('fill_notional', 0.0)}",
        f"  realized_pnl: {ledger_summary.get('realized_pnl', 0.0)}",
        f"  account_sync_count: {ledger_summary.get('account_sync_count', 0)}",
        f"  startup_checks_count: {ledger_summary.get('startup_checks_count', 0)}",
        "",
        "issues:",
    ]
    if issues:
        lines.extend(f"  - {item}" for item in issues)
    else:
        lines.append("  - (none)")

    lines.append("")
    lines.append("recommendations:")
    if recommendations:
        lines.extend(f"  - {item}" for item in recommendations)
    else:
        lines.append("  - (none)")

    by_source = list(ledger_summary.get("fill_by_source") or [])
    if by_source:
        lines.append("")
        lines.append("fill_by_source:")
        for row in by_source:
            if not isinstance(row, Mapping):
                continue
            lines.append(
                "  - "
                f"{row.get('source')}: count={row.get('fill_count', 0)} "
                f"notional={row.get('notional', 0.0)} pnl={row.get('realized_pnl', 0.0)}"
            )

    fill_by_side = list(ledger_summary.get("fill_by_side") or [])
    if fill_by_side:
        lines.append("")
        lines.append("fill_by_side:")
        for row in fill_by_side:
            if not isinstance(row, Mapping):
                continue
            lines.append(
                "  - "
                f"{row.get('side')}: count={row.get('fill_count', 0)} "
                f"notional={row.get('notional', 0.0)} pnl={row.get('realized_pnl', 0.0)}"
            )

    return "\n".join(lines) + "\n"


def write_report_files(report: Mapping[str, object], *, text_path: str, json_path: str) -> None:
    text = render_reconciliation_report(report)
    _ensure_parent_dir(text_path)
    with open(text_path, "w", encoding="utf-8") as f:
        f.write(text)
    _ensure_parent_dir(json_path)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
        f.write("\n")
