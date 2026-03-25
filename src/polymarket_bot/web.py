from __future__ import annotations

import argparse
import csv
from html import escape as html_escape
import io
import json
import os
import tempfile
import time
import threading
from http.cookies import SimpleCookie
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit
import urllib.error
import urllib.request

from polymarket_bot.config import Settings, build_runtime_artifact_paths
from polymarket_bot.db import PersonalTerminalStore
from polymarket_bot.i18n import (
    current_locale as i18n_current_locale,
    enum_label as i18n_enum_label,
    humanize_identifier as i18n_humanize_identifier,
    label as i18n_label,
    t as i18n_t,
)
from polymarket_bot.reconciliation_report import (
    build_reconciliation_report_from_paths,
    write_report_files,
)


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


AUTH_COOKIE_NAME = "poly_dashboard_token"


def _web_field_label(field: str) -> str:
    return i18n_label(
        "web.field",
        field,
        fallback=i18n_humanize_identifier(field),
    )


def _api_error_payload(
    code: str,
    params: dict[str, object] | None = None,
    *,
    fallback: str = "",
    detail: str = "",
) -> dict[str, object]:
    payload: dict[str, object] = {
        "error": i18n_t(f"web.api.{code}", params, fallback=fallback or code),
        "error_code": code,
    }
    if detail:
        payload["error_detail"] = detail
    return payload


def _safe_write_json(path: str, payload: dict) -> None:
    parent = Path(path).expanduser().parent
    if str(parent) not in {"", "."}:
        parent.mkdir(parents=True, exist_ok=True)
    parent_dir = parent if parent.exists() else Path(".")
    with tempfile.NamedTemporaryFile(
        "w",
        encoding="utf-8",
        dir=str(parent_dir),
        prefix=f"{Path(path).name}.",
        suffix=".tmp",
        delete=False,
    ) as f:
        json.dump(payload, f, ensure_ascii=False)
        tmp_path = f.name
    os.replace(tmp_path, path)


def _default_decision_mode() -> dict:
    return {
        "mode": "auto",
        "updated_ts": 0,
        "updated_by": "",
        "note": "",
        "available_modes": ["manual", "semi_auto", "auto"],
    }


def _empty_candidates() -> dict:
    return {
        "summary": {
            "count": 0,
            "candidate": 0,
            "filled": 0,
            "rejected": 0,
            "skipped": 0,
            "waiting_review": 0,
            "queued_actions": 0,
        },
        "observability": {
            "updated_ts": 0,
            "candidate_count": 0,
            "market_metadata": {
                "hits": 0,
                "misses": 0,
                "coverage_pct": 0.0,
            },
            "market_time_source": {
                "metadata": 0,
                "slug_legacy": 0,
                "unknown": 0,
            },
            "skip_reasons": {},
            "recent_cycles": {
                "cycles": 0,
                "signals": 0,
                "precheck_skipped": 0,
                "market_time_source": {
                    "metadata": 0,
                    "slug_legacy": 0,
                    "unknown": 0,
                },
                "skip_reasons": {},
            },
        },
        "items": [],
    }


def _merge_candidate_observability_defaults(observability: dict | None) -> dict:
    defaults = dict((_empty_candidates().get("observability") or {}))
    source = dict(observability or {})
    merged = dict(defaults)
    merged.update(source)
    merged["market_metadata"] = {
        **dict(defaults.get("market_metadata") or {}),
        **dict(source.get("market_metadata") or {}),
    }
    merged["market_time_source"] = {
        **dict(defaults.get("market_time_source") or {}),
        **dict(source.get("market_time_source") or {}),
    }
    default_recent_cycles = dict(defaults.get("recent_cycles") or {})
    source_recent_cycles = dict(source.get("recent_cycles") or {})
    merged["recent_cycles"] = {
        **default_recent_cycles,
        **source_recent_cycles,
        "market_time_source": {
            **dict(default_recent_cycles.get("market_time_source") or {}),
            **dict(source_recent_cycles.get("market_time_source") or {}),
        },
        "skip_reasons": dict(source_recent_cycles.get("skip_reasons") or default_recent_cycles.get("skip_reasons") or {}),
    }
    merged["skip_reasons"] = dict(source.get("skip_reasons") or defaults.get("skip_reasons") or {})
    return merged


def _api_state_payload(state: dict, candidate_store: PersonalTerminalStore, *, days: int = 30, recent_days: int = 7) -> dict:
    payload = dict(state)
    candidates = payload.get("candidates")
    if not isinstance(candidates, dict):
        candidates = _empty_candidates()
    else:
        merged_candidates = _empty_candidates()
        merged_candidates.update(candidates)
        merged_candidates["observability"] = _merge_candidate_observability_defaults(candidates.get("observability"))
        candidates = merged_candidates
    payload["candidates"] = candidates
    payload["stats"] = _stats_from_store(candidate_store, days=days, recent_days=recent_days)
    payload["archive"] = _archive_from_store(candidate_store, days=days, recent_days=recent_days)
    return payload


def _empty_wallet_profiles() -> dict:
    return {
        "summary": {
            "count": 0,
            "watched": 0,
            "annotated": 0,
            "updated_ts": 0,
        },
        "items": [],
    }


def _empty_journal_summary() -> dict:
    return {
        "count": 0,
        "latest_ts": 0,
        "latest_note": "",
        "recent": [],
        "top_tags": [],
    }


def _empty_stats_summary() -> dict:
    return {
        "days": 30,
        "recent_days": 7,
        "updated_ts": 0,
        "candidates": {
            "days": 30,
            "window_start_ts": 0,
            "total_candidates": 0,
            "avg_score": 0.0,
            "by_status": [],
            "updated_ts": 0,
        },
        "candidate_actions": {
            "days": 30,
            "window_start_ts": 0,
            "total_actions": 0,
            "total_notional": 0.0,
            "by_action": [],
            "updated_ts": 0,
        },
        "journal": _empty_journal_summary(),
        "archive": _empty_archive_summary(),
        "wallet_profiles": {
            "count": 0,
            "enabled": 0,
            "watched": 0,
        },
        "totals": {
            "candidate_count": 0,
            "action_count": 0,
            "journal_count": 0,
        },
    }


def _empty_archive_summary() -> dict:
    return {
        "days": 30,
        "recent_days": 7,
        "window_start_ts": 0,
        "recent_window_start_ts": 0,
        "day_count": 0,
        "summary": {
            "candidate_count": 0,
            "action_count": 0,
            "journal_count": 0,
            "days": 30,
        },
        "daily_rows": [],
        "recent_summary": {
            "days": 7,
            "window_start_ts": 0,
            "day_count": 0,
            "candidate_count": 0,
            "action_count": 0,
            "journal_count": 0,
            "updated_ts": 0,
        },
        "updated_ts": 0,
    }


def _empty_pending_actions() -> dict:
    return {
        "summary": {
            "count": 0,
            "waiting": 0,
            "pending": 0,
        },
        "items": [],
    }


def _empty_notifier() -> dict:
    return {
        "local_available": False,
        "webhook_configured": False,
        "telegram_configured": False,
        "channels": [],
        "delivery_stats": {
            "event_count": 0,
            "delivery_count": 0,
            "ok_events": 0,
            "failed_events": 0,
            "by_channel": {},
        },
        "recent": [],
        "last": {},
        "updated_ts": 0,
    }


def _default_candidate_action_store() -> dict:
    return {"items": []}


def _default_wallet_profile_store() -> dict:
    return {"profiles": {}, "updated_ts": 0}


def _default_journal_store() -> dict:
    return {"notes": []}


def _flatten_csv_value(value):
    if isinstance(value, (dict, list, tuple)):
        return json.dumps(value, ensure_ascii=False, sort_keys=True)
    return value


def _rows_to_csv(rows: list[dict[str, object]]) -> str:
    buffer = io.StringIO()
    normalized_rows = [dict(row) for row in rows]
    if not normalized_rows:
        return ""
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in normalized_rows:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in normalized_rows:
        writer.writerow({key: _flatten_csv_value(row.get(key)) for key in fieldnames})
    return buffer.getvalue()


def _runtime_store_path(state_path: str, filename: str, env_name: str) -> str:
    env_value = os.getenv(env_name, "").strip()
    if env_value:
        return env_value
    return str(Path(state_path).expanduser().parent / filename)


def _as_str_list(value) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        return [value.strip()]
    return []


def _normalize_decision_mode(payload: dict | None) -> dict:
    source = dict(payload or {})
    mode = str(source.get("mode") or "auto").strip().lower()
    if mode not in {"manual", "semi_auto", "auto"}:
        mode = "auto"
    return {
        "mode": mode,
        "updated_ts": int(source.get("updated_ts") or 0),
        "updated_by": str(source.get("updated_by") or ""),
        "note": str(source.get("note") or ""),
        "available_modes": ["manual", "semi_auto", "auto"],
    }


def _candidate_action_index(payload: dict | None) -> dict[str, dict]:
    index: dict[str, dict] = {}
    for raw in list((payload or {}).get("items") or []):
        if not isinstance(raw, dict):
            continue
        signal_id = str(raw.get("signal_id") or "").strip()
        if not signal_id:
            continue
        row = dict(raw)
        row["updated_ts"] = int(row.get("updated_ts") or row.get("requested_ts") or 0)
        existing = index.get(signal_id)
        if existing is None or int(row.get("updated_ts") or 0) >= int(existing.get("updated_ts") or 0):
            index[signal_id] = row
    return index


def _state_candidates(state: dict) -> dict:
    current = state.get("candidates")
    signal_review = dict(state.get("signal_review") or {})
    fallback_cycles = list(signal_review.get("cycles") or [])
    current_observability = {}
    if isinstance(current, dict):
        current_observability = _merge_candidate_observability_defaults(current.get("observability"))
    if (
        isinstance(current, dict)
        and (
            not fallback_cycles
            or (
                isinstance(current.get("items"), list)
                and list(current.get("items") or [])
            )
        )
    ):
        payload = _empty_candidates()
        payload.update(current)
        payload["observability"] = _merge_candidate_observability_defaults(current.get("observability"))
        return payload

    items = []
    for cycle in list(signal_review.get("cycles") or []):
        if not isinstance(cycle, dict):
            continue
        cycle_id = str(cycle.get("cycle_id") or "")
        cycle_ts = int(cycle.get("ts") or 0)
        for raw in list(cycle.get("candidates") or []):
            if not isinstance(raw, dict):
                continue
            final_status = str(raw.get("final_status") or "candidate")
            decision_snapshot = dict(raw.get("decision_snapshot") or {})
            candidate_snapshot = dict(raw.get("candidate_snapshot") or {})
            review_only_status = "executed" if final_status == "filled" else "rejected" if "reject" in final_status else "watched"
            items.append(
                {
                    "cycle_id": cycle_id,
                    "cycle_ts": cycle_ts,
                    "signal_id": str(raw.get("signal_id") or ""),
                    "trace_id": str(raw.get("trace_id") or ""),
                    "title": str(raw.get("title") or ""),
                    "token_id": str(raw.get("token_id") or ""),
                    "outcome": str(raw.get("outcome") or ""),
                    "wallet": str(raw.get("wallet") or ""),
                    "side": str(raw.get("side") or ""),
                    "action": str(raw.get("action") or ""),
                    "action_label": str(raw.get("action_label") or ""),
                    "final_status": final_status,
                    "wallet_score": float(raw.get("wallet_score") or 0.0),
                    "wallet_tier": str(raw.get("wallet_tier") or ""),
                    "topic_label": str(raw.get("topic_label") or ""),
                    "topic_bias": str(raw.get("topic_bias") or ""),
                    "topic_multiplier": float(raw.get("topic_multiplier") or 1.0),
                    "decision_reason": str(raw.get("decision_reason") or ""),
                    "skip_reason": str(decision_snapshot.get("skip_reason") or ""),
                    "market_time_source": str(decision_snapshot.get("market_time_source") or ""),
                    "market_metadata_hit": bool(decision_snapshot.get("market_metadata_hit", False)),
                    "suggested_action": "watch",
                    "status": review_only_status,
                    "sized_notional": float(raw.get("sized_notional") or 0.0),
                    "final_notional": float(raw.get("final_notional") or 0.0),
                    "budget_limited": bool(raw.get("budget_limited", False)),
                    "duplicate": bool(raw.get("duplicate", False)),
                    "order_status": str(raw.get("order_status") or ""),
                    "order_reason": str(raw.get("order_reason") or ""),
                    "order_notional": float(raw.get("order_notional") or 0.0),
                    "decision_snapshot": decision_snapshot,
                    "signal_snapshot": candidate_snapshot,
                    "review_action": "",
                    "review_status": "closed",
                    "review_note": "",
                    "review_updated_ts": 0,
                }
            )
    payload = _empty_candidates()
    payload["items"] = items[:32]
    payload["observability"] = current_observability
    payload["summary"] = {
        "count": len(items),
        "candidate": sum(1 for row in items if str(row.get("final_status") or "") == "candidate"),
        "filled": sum(1 for row in items if str(row.get("final_status") or "") == "filled"),
        "rejected": sum(1 for row in items if "reject" in str(row.get("final_status") or "")),
        "skipped": sum(
            1
            for row in items
            if str(row.get("final_status") or "") not in {"candidate", "filled"}
            and "reject" not in str(row.get("final_status") or "")
        ),
        "waiting_review": sum(1 for row in items if str(row.get("review_status") or "") == "waiting"),
        "queued_actions": 0,
    }
    return payload


def _merged_candidates(state: dict, action_store: dict | None) -> dict:
    payload = _state_candidates(state)
    index = _candidate_action_index(action_store)
    items = []
    for raw in list(payload.get("items") or []):
        if not isinstance(raw, dict):
            continue
        row = dict(raw)
        action = index.get(str(row.get("signal_id") or ""), {})
        final_status = str(row.get("final_status") or "candidate")
        row["review_action"] = str(action.get("action") or row.get("review_action") or "")
        row["review_status"] = str(action.get("status") or row.get("review_status") or ("waiting" if final_status == "candidate" else "closed"))
        row["review_note"] = str(action.get("note") or row.get("review_note") or "")
        row["review_updated_ts"] = int(action.get("updated_ts") or row.get("review_updated_ts") or 0)
        items.append(row)
    payload["items"] = items
    payload["summary"] = {
        "count": len(items),
        "candidate": sum(1 for row in items if str(row.get("final_status") or "") == "candidate"),
        "filled": sum(1 for row in items if str(row.get("final_status") or "") == "filled"),
        "rejected": sum(1 for row in items if "reject" in str(row.get("final_status") or "")),
        "skipped": sum(
            1
            for row in items
            if str(row.get("final_status") or "") not in {"candidate", "filled"}
            and "reject" not in str(row.get("final_status") or "")
        ),
        "waiting_review": sum(1 for row in items if str(row.get("review_status") or "") == "waiting"),
        "queued_actions": sum(
            1
            for row in items
            if str(row.get("review_status") or "") in {"pending", "queued", "requested"}
        ),
    }
    return payload


def _candidate_observability(items: list[dict]) -> dict:
    observability = _merge_candidate_observability_defaults(None)
    if not items:
        return observability

    metadata_hits = 0
    time_sources: dict[str, int] = {"metadata": 0, "slug_legacy": 0, "unknown": 0}
    skip_reasons: dict[str, int] = {}
    candidate_count = 0

    for raw in items:
        if not isinstance(raw, dict):
            continue
        candidate_count += 1
        metadata_hit = bool(raw.get("market_metadata_hit"))
        if metadata_hit:
            metadata_hits += 1
        time_source = str(raw.get("market_time_source") or "").strip().lower()
        if not time_source:
            time_source = "metadata" if metadata_hit else "unknown"
        time_sources[time_source] = int(time_sources.get(time_source, 0) or 0) + 1
        skip_reason = str(raw.get("skip_reason") or "").strip()
        if skip_reason:
            skip_reasons[skip_reason] = int(skip_reasons.get(skip_reason, 0) or 0) + 1

    metadata_misses = max(0, candidate_count - metadata_hits)
    coverage_pct = round((metadata_hits / candidate_count) * 100.0, 1) if candidate_count > 0 else 0.0
    observability["updated_ts"] = max(
        int(time.time()),
        max(int(raw.get("updated_ts") or raw.get("created_ts") or 0) for raw in items if isinstance(raw, dict)),
    )
    observability["candidate_count"] = candidate_count
    observability["market_metadata"] = {
        "hits": metadata_hits,
        "misses": metadata_misses,
        "coverage_pct": coverage_pct,
    }
    observability["market_time_source"] = dict(
        sorted(
            time_sources.items(),
            key=lambda item: (-int(item[1] or 0), str(item[0] or "")),
        )
    )
    observability["skip_reasons"] = dict(
        sorted(
            skip_reasons.items(),
            key=lambda item: (-int(item[1] or 0), str(item[0] or "")),
        )
    )
    return observability


def _wallet_profile_store_map(payload: dict | None) -> dict[str, dict]:
    mapped: dict[str, dict] = {}
    store = dict((payload or {}).get("profiles") or {})
    for wallet, raw in store.items():
        key = str(wallet or "").strip().lower()
        if key and isinstance(raw, dict):
            mapped[key] = dict(raw)
    for raw in list((payload or {}).get("items") or []):
        if not isinstance(raw, dict):
            continue
        key = str(raw.get("wallet") or "").strip().lower()
        if key:
            mapped[key] = dict(raw)
    return mapped


def _merged_wallet_profiles(state: dict, profile_store: dict | None) -> dict:
    current = state.get("wallet_profiles")
    base_items = []
    if isinstance(current, dict) and isinstance(current.get("items"), list):
        base_items = [dict(row) for row in list(current.get("items") or []) if isinstance(row, dict)]
    else:
        for raw in list(state.get("wallets") or []):
            if not isinstance(raw, dict):
                continue
            base_items.append(
                {
                    "wallet": str(raw.get("wallet") or ""),
                    "score": float(raw.get("score") or 0.0),
                    "tier": str(raw.get("tier") or ""),
                    "trading_enabled": bool(raw.get("trading_enabled", False)),
                    "history_available": bool(raw.get("history_available", False)),
                    "recent_activity_events": raw.get("recent_activity_events"),
                    "closed_positions": int(raw.get("closed_positions") or 0),
                    "resolved_markets": int(raw.get("resolved_markets") or 0),
                    "alias": "",
                    "status": "",
                    "note": "",
                    "tags": [],
                    "watch": False,
                    "priority": 0,
                    "updated_ts": 0,
                }
            )

    store_map = _wallet_profile_store_map(profile_store)
    items = []
    seen = set()
    for raw in base_items:
        wallet = str(raw.get("wallet") or "").strip().lower()
        if not wallet:
            continue
        seen.add(wallet)
        overlay = store_map.get(wallet, {})
        row = dict(raw)
        row["wallet"] = wallet
        row["alias"] = str(overlay.get("alias") or row.get("alias") or "")
        row["status"] = str(overlay.get("status") or row.get("status") or "")
        row["note"] = str(overlay.get("note") or row.get("note") or "")
        row["tags"] = _as_str_list(overlay.get("tags") or row.get("tags"))
        row["watch"] = bool(overlay.get("watch", row.get("watch", False)))
        row["priority"] = int(overlay.get("priority") or row.get("priority") or 0)
        row["updated_ts"] = int(overlay.get("updated_ts") or row.get("updated_ts") or 0)
        items.append(row)
    for wallet, overlay in store_map.items():
        if wallet in seen:
            continue
        items.append(
            {
                "wallet": wallet,
                "score": 0.0,
                "tier": "",
                "trading_enabled": False,
                "history_available": False,
                "recent_activity_events": None,
                "closed_positions": 0,
                "resolved_markets": 0,
                "alias": str(overlay.get("alias") or ""),
                "status": str(overlay.get("status") or ""),
                "note": str(overlay.get("note") or ""),
                "tags": _as_str_list(overlay.get("tags")),
                "watch": bool(overlay.get("watch", False)),
                "priority": int(overlay.get("priority") or 0),
                "updated_ts": int(overlay.get("updated_ts") or 0),
            }
        )
    items.sort(
        key=lambda row: (
            int(row.get("priority") or 0),
            float(row.get("score") or 0.0),
            str(row.get("wallet") or ""),
        ),
        reverse=True,
    )
    return {
        "summary": {
            "count": len(items),
            "watched": sum(1 for row in items if bool(row.get("watch", False))),
            "annotated": sum(
                1
                for row in items
                if str(row.get("alias") or "").strip()
                or str(row.get("note") or "").strip()
                or list(row.get("tags") or [])
            ),
            "updated_ts": int((profile_store or {}).get("updated_ts") or 0),
        },
        "items": items[:32],
    }


def _journal_notes(payload: dict | None) -> list[dict]:
    notes = []
    for raw in list((payload or {}).get("notes") or []):
        if not isinstance(raw, dict):
            continue
        notes.append(
            {
                "note_id": str(raw.get("note_id") or ""),
                "ts": int(raw.get("ts") or 0),
                "text": str(raw.get("text") or ""),
                "tags": _as_str_list(raw.get("tags")),
                "wallet": str(raw.get("wallet") or ""),
                "signal_id": str(raw.get("signal_id") or ""),
                "trace_id": str(raw.get("trace_id") or ""),
            }
        )
    notes.sort(key=lambda row: int(row.get("ts") or 0), reverse=True)
    return notes


def _journal_summary_from_store(payload: dict | None) -> dict:
    notes = _journal_notes(payload)
    tag_counts: dict[str, int] = {}
    for note in notes:
        for tag in list(note.get("tags") or []):
            tag_counts[tag] = tag_counts.get(tag, 0) + 1
    latest = notes[0] if notes else {}
    return {
        "count": len(notes),
        "latest_ts": int(latest.get("ts") or 0),
        "latest_note": str(latest.get("text") or ""),
        "recent": notes[:5],
        "top_tags": [
            {"tag": tag, "count": count}
            for tag, count in sorted(tag_counts.items(), key=lambda item: (item[1], item[0]), reverse=True)[:8]
        ],
    }


def _pending_actions_from_candidates(candidates: dict, action_store: dict | None) -> dict:
    index = _candidate_action_index(action_store)
    items = []
    for raw in list(candidates.get("items") or []):
        if not isinstance(raw, dict):
            continue
        if str(raw.get("final_status") or "") != "candidate":
            continue
        signal_id = str(raw.get("signal_id") or "")
        action = index.get(signal_id)
        if action:
            items.append(
                {
                    "type": "candidate_action",
                    "signal_id": signal_id,
                    "trace_id": str(raw.get("trace_id") or ""),
                    "wallet": str(raw.get("wallet") or ""),
                    "title": str(raw.get("title") or ""),
                    "token_id": str(raw.get("token_id") or ""),
                    "requested_action": str(action.get("action") or ""),
                    "status": str(action.get("status") or "pending"),
                    "note": str(action.get("note") or ""),
                    "updated_ts": int(action.get("updated_ts") or 0),
                }
            )
            continue
        items.append(
            {
                "type": "candidate_review",
                "signal_id": signal_id,
                "trace_id": str(raw.get("trace_id") or ""),
                "wallet": str(raw.get("wallet") or ""),
                "title": str(raw.get("title") or ""),
                "token_id": str(raw.get("token_id") or ""),
                "requested_action": "review",
                "status": "waiting",
                "note": "",
                "updated_ts": int(raw.get("cycle_ts") or 0),
            }
        )
    items.sort(key=lambda row: int(row.get("updated_ts") or 0), reverse=True)
    return {
        "summary": {
            "count": len(items),
            "waiting": sum(1 for row in items if str(row.get("status") or "") == "waiting"),
            "pending": sum(1 for row in items if str(row.get("status") or "") in {"pending", "queued", "requested"}),
        },
        "items": items[:32],
    }


def _candidate_row_key(row: dict | None) -> str:
    if not isinstance(row, dict):
        return ""
    for key in ("id", "signal_id", "trace_id", "token_id", "market_slug"):
        value = str(row.get(key) or "").strip()
        if value:
            return value.lower()
    return ""


def _merge_candidate_row(base: dict[str, Any], overlay: dict[str, Any]) -> dict[str, Any]:
    row = dict(base)
    for key, value in overlay.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip() and key not in {"review_action", "review_status", "review_note", "selected_action", "note"}:
            continue
        row[key] = value
    return row


def _merge_candidate_rows(primary: list[dict[str, Any]], secondary: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for source in (primary, secondary):
        for raw in list(source or []):
            if not isinstance(raw, dict):
                continue
            key = _candidate_row_key(raw)
            if not key:
                continue
            if key not in merged:
                merged[key] = dict(raw)
                order.append(key)
            else:
                merged[key] = _merge_candidate_row(merged[key], raw)
    return [merged[key] for key in order]


def _candidate_relation_match(candidate: dict[str, Any], row: dict[str, Any]) -> bool:
    if not isinstance(candidate, dict) or not isinstance(row, dict):
        return False
    candidate_keys = {
        str(candidate.get("id") or "").strip().lower(),
        str(candidate.get("signal_id") or "").strip().lower(),
        str(candidate.get("trace_id") or "").strip().lower(),
        str(candidate.get("token_id") or "").strip().lower(),
        str(candidate.get("market_slug") or "").strip().lower(),
        str(candidate.get("wallet") or "").strip().lower(),
    }
    row_keys = {
        str(row.get("candidate_id") or "").strip().lower(),
        str(row.get("signal_id") or "").strip().lower(),
        str(row.get("trace_id") or "").strip().lower(),
        str(row.get("token_id") or "").strip().lower(),
        str(row.get("market_slug") or "").strip().lower(),
        str(row.get("wallet") or "").strip().lower(),
    }
    candidate_snapshot = row.get("candidate_snapshot")
    if isinstance(candidate_snapshot, dict):
        row_keys.update(
            {
                str(candidate_snapshot.get("candidate_id") or "").strip().lower(),
                str(candidate_snapshot.get("id") or "").strip().lower(),
                str(candidate_snapshot.get("signal_id") or "").strip().lower(),
                str(candidate_snapshot.get("trace_id") or "").strip().lower(),
                str(candidate_snapshot.get("token_id") or "").strip().lower(),
                str(candidate_snapshot.get("market_slug") or "").strip().lower(),
                str(candidate_snapshot.get("wallet") or "").strip().lower(),
            }
        )
    return bool(candidate_keys & row_keys)


def _exit_kind_label(kind: str, fallback: str = "") -> str:
    if fallback:
        return fallback
    raw = str(kind or "").strip()
    return i18n_enum_label(
        "enum.exitKind",
        raw or "default",
        fallback=i18n_humanize_identifier(raw) or i18n_t("enum.exitKind.default", fallback="Exit"),
    )


def _timeline_kind_label(kind: str) -> str:
    raw = str(kind or "").strip().lower()
    if raw in {"order", "trace", "cycle", "action", "event"}:
        return i18n_t(f"common.entity.{raw}", fallback=i18n_humanize_identifier(raw))
    return i18n_humanize_identifier(raw) or i18n_t("common.entity.event", fallback="Event")


def _order_action_meta(order: dict[str, Any]) -> tuple[str, str]:
    flow = str(order.get("flow") or "")
    side = str(order.get("side") or "").upper()
    action = str(order.get("position_action") or "").strip().lower()
    action_label = str(order.get("position_action_label") or "").strip()
    if action and action_label:
        return action, action_label
    if flow == "exit":
        if action == "trim":
            return "trim", action_label or i18n_t("enum.actionTag.trim", fallback="Trim")
        if action == "exit":
            return "exit", action_label or i18n_t("enum.actionTag.exit", fallback="Exit")
        return "exit", action_label or _exit_kind_label(str(order.get("exit_kind") or ""), str(order.get("exit_label") or ""))
    if action == "add":
        return "add", action_label or i18n_t("enum.actionTag.add", fallback="Add")
    if action == "entry":
        return "entry", action_label or i18n_t("enum.actionTag.entry", fallback="Entry")
    if side == "BUY":
        return "entry", action_label or i18n_enum_label("enum.side", "buy", fallback="Buy")
    if side == "SELL":
        return "exit", action_label or i18n_enum_label("enum.side", "sell", fallback="Sell")
    return action or side.lower(), action_label or side or i18n_t("enum.actionTag.event", fallback="Event")


def _timeline_text(order: dict[str, Any]) -> str:
    _action, action_label = _order_action_meta(order)
    title = str(order.get("title") or i18n_t("common.dash", fallback="-"))
    return f"{action_label} {title}"


def _candidate_detail_timeline_text(item: dict[str, Any]) -> str:
    kind = str(item.get("kind") or "").strip().lower()
    text = str(item.get("text") or item.get("label") or item.get("action") or "").strip()
    separator = i18n_t("common.separator", fallback=" · ")
    if kind == "order":
        status = str(item.get("status") or "").strip()
        action = str(item.get("action_label") or item.get("action") or "").strip()
        parts = [action or _timeline_kind_label(kind)]
        if status:
            parts.append(status)
        if text:
            parts.append(text)
        return separator.join(parts)
    if kind == "journal":
        action = str(item.get("action") or "").strip()
        rationale = str(item.get("rationale") or "").strip()
        return separator.join(part for part in [action, rationale or text] if part)
    if kind == "trace":
        return text or str(item.get("trace_id") or _timeline_kind_label(kind))
    if kind == "cycle":
        return text or str(item.get("cycle_id") or _timeline_kind_label(kind))
    if kind == "action":
        action = str(item.get("action") or "").strip()
        note = str(item.get("note") or "").strip()
        return separator.join(part for part in [action, note or text] if part)
    return text or _timeline_kind_label(kind)


def _candidate_detail_payload(
    state: dict,
    candidate_store: PersonalTerminalStore | None,
    *,
    candidate_id: str,
    decision_mode_path: str,
    candidate_actions_path: str,
    wallet_profiles_path: str,
    journal_path: str,
) -> dict:
    runtime_state = _state_with_personal_console(
        state,
        decision_mode_path=decision_mode_path,
        candidate_actions_path=candidate_actions_path,
        wallet_profiles_path=wallet_profiles_path,
        journal_path=journal_path,
    )
    candidate_base = None
    if candidate_store is not None:
        try:
            candidate_base = candidate_store.candidate_detail(candidate_id, related_limit=50)
        except Exception:
            candidate_base = None
    candidate_row = dict((candidate_base or {}).get("candidate") or {})
    runtime_candidates = list(((runtime_state.get("candidates") or {}).get("items") or []))
    runtime_match = next(
        (
            row
            for row in runtime_candidates
            if _candidate_row_key(row) == candidate_id.strip().lower()
            or str(row.get("id") or "").strip().lower() == candidate_id.strip().lower()
            or str(row.get("signal_id") or "").strip().lower() == candidate_id.strip().lower()
            or str(row.get("trace_id") or "").strip().lower() == candidate_id.strip().lower()
        ),
        None,
    )
    if runtime_match is not None:
        candidate_row = _merge_candidate_row(candidate_row, runtime_match) if candidate_row else dict(runtime_match)

    if not candidate_row:
        for row in runtime_candidates:
            if _candidate_row_key(row) == candidate_id.strip().lower():
                candidate_row = dict(row)
                break

    if not candidate_row:
        return {}

    related_actions = list((candidate_base or {}).get("related_actions") or [])
    related_journal = list((candidate_base or {}).get("related_journal") or [])
    trace_id = str(candidate_row.get("trace_id") or candidate_row.get("signal_id") or "").strip()
    signal_id = str(candidate_row.get("signal_id") or "").strip()
    wallet = str(candidate_row.get("wallet") or "").strip()
    market_slug = str(candidate_row.get("market_slug") or "").strip()
    token_id = str(candidate_row.get("token_id") or "").strip()

    if candidate_store is not None:
        try:
            extra_journal = candidate_store.list_journal_entries(limit=200)
        except Exception:
            extra_journal = []
        if not related_journal:
            related_journal = [
                row
                for row in extra_journal
                if _candidate_relation_match(
                    {
                        "id": candidate_row.get("id"),
                        "signal_id": signal_id,
                        "trace_id": trace_id,
                        "token_id": token_id,
                        "market_slug": market_slug,
                        "wallet": wallet,
                    },
                    row,
                )
            ][:25]

    signal_review = dict(runtime_state.get("signal_review") or {})
    traces = [dict(row) for row in list(signal_review.get("traces") or []) if isinstance(row, dict)]
    cycles = [dict(row) for row in list(signal_review.get("cycles") or []) if isinstance(row, dict)]
    trace = next(
        (
            row
            for row in traces
            if str(row.get("trace_id") or "").strip().lower() == trace_id.lower()
            or str(row.get("entry_signal_id") or "").strip().lower() == signal_id.lower()
            or str(row.get("last_signal_id") or "").strip().lower() == signal_id.lower()
        ),
        {}
    )

    related_cycles = []
    for cycle in cycles:
        cycle_candidates = [row for row in list(cycle.get("candidates") or []) if isinstance(row, dict)]
        if not cycle_candidates:
            continue
        if any(
            _candidate_relation_match(
                {
                    "id": candidate_row.get("id"),
                    "signal_id": signal_id,
                    "trace_id": trace_id,
                    "token_id": token_id,
                    "market_slug": market_slug,
                    "wallet": wallet,
                },
                row,
            )
            for row in cycle_candidates
        ):
            related_cycles.append(cycle)

    decision_chain = list(trace.get("decision_chain") or [])
    if not decision_chain:
        for cycle in related_cycles:
            for raw in list(cycle.get("candidates") or []):
                if not isinstance(raw, dict):
                    continue
                candidate_snapshot = dict(raw.get("candidate_snapshot") or {})
                if not candidate_snapshot:
                    continue
                if str(candidate_snapshot.get("signal_id") or "").strip().lower() != signal_id.lower() and str(candidate_snapshot.get("trace_id") or "").strip().lower() != trace_id.lower():
                    continue
                decision_chain.append(
                    {
                        "cycle_id": str(cycle.get("cycle_id") or ""),
                        "ts": int(cycle.get("ts") or 0),
                        "signal_id": str(candidate_snapshot.get("signal_id") or ""),
                        "trace_id": str(candidate_snapshot.get("trace_id") or ""),
                        "wallet": str(candidate_snapshot.get("wallet") or ""),
                        "title": str(candidate_snapshot.get("market_slug") or ""),
                        "side": str(candidate_snapshot.get("side") or ""),
                        "action": str(candidate_snapshot.get("position_action") or raw.get("action") or ""),
                        "action_label": str(candidate_snapshot.get("position_action_label") or raw.get("action_label") or ""),
                        "wallet_score": float(candidate_snapshot.get("wallet_score") or 0.0),
                        "wallet_tier": str(candidate_snapshot.get("wallet_tier") or ""),
                        "topic_label": str((raw.get("topic_snapshot") or {}).get("topic_label") or ""),
                        "topic_bias": str((raw.get("topic_snapshot") or {}).get("topic_bias") or ""),
                        "topic_multiplier": float((raw.get("topic_snapshot") or {}).get("topic_multiplier") or 1.0),
                        "order_status": str((raw.get("order_snapshot") or {}).get("status") or ""),
                        "order_reason": str((raw.get("order_snapshot") or {}).get("reason") or ""),
                        "order_notional": float((raw.get("order_snapshot") or {}).get("notional") or 0.0),
                        "final_status": str(raw.get("final_status") or ""),
                    }
                )

    orders = []
    for raw_order in list(runtime_state.get("orders") or []):
        if not isinstance(raw_order, dict):
            continue
        if _candidate_relation_match(
            {
                "id": candidate_row.get("id"),
                "signal_id": signal_id,
                "trace_id": trace_id,
                "token_id": token_id,
                "market_slug": market_slug,
                "wallet": wallet,
            },
            raw_order,
        ):
            orders.append(dict(raw_order))
    orders.sort(key=lambda row: int(row.get("ts") or 0), reverse=True)

    timeline = []
    for item in orders[:8]:
        timeline.append(
            {
                "kind": "order",
                "ts": int(item.get("ts") or 0),
                "text": _candidate_detail_timeline_text(item),
                "action": _order_action_meta(item)[0],
                "action_label": _order_action_meta(item)[1],
                "status": str(item.get("status") or "").upper(),
                "flow": str(item.get("flow") or ""),
                "trace_id": str(item.get("trace_id") or ""),
            }
        )
    for entry in related_journal[:8]:
        timeline.append(
            {
                "kind": "journal",
                "ts": int(entry.get("created_ts") or 0),
                "text": str(entry.get("rationale") or entry.get("text") or ""),
                "action": str(entry.get("action") or ""),
                "result_tag": str(entry.get("result_tag") or ""),
                "candidate_id": str(entry.get("candidate_id") or ""),
                "trace_id": str(entry.get("trace_id") or ""),
            }
        )
    if trace:
        timeline.append(
            {
                "kind": "trace",
                "ts": int(trace.get("opened_ts") or trace.get("ts") or 0),
                "text": f"trace {trace_id}",
                "trace_id": trace_id,
            }
        )
    for cycle in related_cycles[:4]:
        timeline.append(
            {
                "kind": "cycle",
                "ts": int(cycle.get("ts") or 0),
                "text": str(cycle.get("cycle_id") or ""),
                "cycle_id": str(cycle.get("cycle_id") or ""),
            }
        )
    timeline.sort(key=lambda row: int(row.get("ts") or 0), reverse=True)

    return {
        "candidate": candidate_row,
        "related_actions": related_actions,
        "related_journal": related_journal[:25],
        "trace": trace or {},
        "related_cycles": related_cycles,
        "decision_chain": decision_chain[:16],
        "orders": orders[:16],
        "timeline": timeline[:24],
        "summary": {
            "related_action_count": len(related_actions),
            "related_journal_count": len(related_journal[:25]),
            "order_count": len(orders),
            "decision_chain_count": len(decision_chain[:16]),
            "cycle_count": len(related_cycles),
            "trace_found": bool(trace),
        },
    }


def _stats_from_store(candidate_store: PersonalTerminalStore | None, *, days: int = 30, recent_days: int = 7) -> dict:
    if candidate_store is None:
        payload = _empty_stats_summary()
        payload["days"] = int(days)
        payload["recent_days"] = int(recent_days)
        return payload
    try:
        return dict(candidate_store.stats_summary(days=days, recent_days=recent_days))
    except Exception:
        return _empty_stats_summary()


def _archive_from_store(candidate_store: PersonalTerminalStore | None, *, days: int = 30, recent_days: int = 7) -> dict:
    if candidate_store is None:
        payload = _empty_archive_summary()
        payload["days"] = int(days)
        payload["recent_days"] = int(recent_days)
        return payload
    try:
        return dict(candidate_store.archive_summary(days=days, recent_days=recent_days))
    except Exception:
        return _empty_archive_summary()


def _candidate_actions_for_export(candidate_store: PersonalTerminalStore | None, *, days: int, limit: int) -> list[dict]:
    if candidate_store is None:
        return []
    try:
        return list(candidate_store.list_candidate_actions(limit=limit, days=days))
    except Exception:
        return []


def _export_bundle(candidate_store: PersonalTerminalStore | None, *, scope: str, days: int, recent_days: int, limit: int) -> dict:
    normalized_scope = str(scope or "bundle").strip().lower()
    if candidate_store is None:
        return {
            "scope": normalized_scope,
            "days": int(days),
            "recent_days": int(recent_days),
            "limit": int(limit),
            "items": [],
        }
    if normalized_scope == "candidates":
        items = list(candidate_store.list_candidates(limit=limit, include_expired=True))
        return {
            "scope": "candidates",
            "days": int(days),
            "recent_days": int(recent_days),
            "limit": int(limit),
            "summary": {
                "count": len(items),
                "pending": sum(1 for item in items if str(item.get("status") or "") == "pending"),
                "approved": sum(1 for item in items if str(item.get("status") or "") == "approved"),
                "watched": sum(1 for item in items if str(item.get("status") or "") == "watched"),
                "executed": sum(1 for item in items if str(item.get("status") or "") == "executed"),
            },
            "items": items,
        }
    if normalized_scope == "actions":
        items = _candidate_actions_for_export(candidate_store, days=days, limit=limit)
        return {
            "scope": "actions",
            "days": int(days),
            "recent_days": int(recent_days),
            "limit": int(limit),
            "summary": {
                "count": len(items),
                "approved": sum(1 for item in items if str(item.get("status") or "") in {"approved", "executed"}),
                "watched": sum(1 for item in items if str(item.get("status") or "") == "watched"),
                "ignored": sum(1 for item in items if str(item.get("status") or "") == "ignored"),
            },
            "items": items,
        }
    if normalized_scope == "journal":
        items = list(candidate_store.list_journal_entries(limit=limit))
        return {
            "scope": "journal",
            "days": int(days),
            "recent_days": int(recent_days),
            "limit": int(limit),
            "summary": dict(candidate_store.journal_summary(days=days)),
            "items": items,
        }
    if normalized_scope == "wallet_profiles":
        items = list(candidate_store.list_wallet_profiles(limit=limit))
        return {
            "scope": "wallet_profiles",
            "days": int(days),
            "recent_days": int(recent_days),
            "limit": int(limit),
            "summary": {
                "count": len(items),
                "enabled": sum(1 for item in items if bool(item.get("enabled", True))),
                "watched": sum(1 for item in items if bool(item.get("watch", False))),
            },
            "items": items,
        }
    if normalized_scope == "archive":
        return _archive_from_store(candidate_store, days=days, recent_days=recent_days)
    if normalized_scope == "stats":
        return _stats_from_store(candidate_store, days=days, recent_days=recent_days)
    if normalized_scope not in {"bundle", "all"}:
        raise ValueError(f"unsupported export scope: {normalized_scope}")
    return {
        "scope": "bundle",
        "days": int(days),
        "recent_days": int(recent_days),
        "limit": int(limit),
        "stats": _stats_from_store(candidate_store, days=days, recent_days=recent_days),
        "archive": _archive_from_store(candidate_store, days=days, recent_days=recent_days),
        "candidates": _export_bundle(candidate_store, scope="candidates", days=days, recent_days=recent_days, limit=limit),
        "candidate_actions": _export_bundle(candidate_store, scope="actions", days=days, recent_days=recent_days, limit=limit),
        "wallet_profiles": _export_bundle(candidate_store, scope="wallet_profiles", days=days, recent_days=recent_days, limit=limit),
        "journal": _export_bundle(candidate_store, scope="journal", days=days, recent_days=recent_days, limit=limit),
    }


def _csv_text_from_items(items: list[dict]) -> str:
    if not items:
        return ""
    buffer = io.StringIO()
    fieldnames: list[str] = []
    seen: set[str] = set()
    for row in items:
        for key in row:
            if key not in seen:
                seen.add(key)
                fieldnames.append(key)
    writer = csv.DictWriter(buffer, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in items:
        writer.writerow({key: _flatten_csv_value(row.get(key)) for key in fieldnames})
    return buffer.getvalue()


def _state_with_personal_console(
    state: dict,
    *,
    decision_mode_path: str,
    candidate_actions_path: str,
    wallet_profiles_path: str,
    journal_path: str,
) -> dict:
    payload = dict(state)
    decision_mode = _normalize_decision_mode(_load_json(decision_mode_path, _default_decision_mode()))
    candidate_actions = _load_json(candidate_actions_path, _default_candidate_action_store())
    wallet_profiles_store = _load_json(wallet_profiles_path, _default_wallet_profile_store())
    journal_store = _load_json(journal_path, _default_journal_store())
    candidates = _merged_candidates(payload, candidate_actions)
    wallet_profiles = _merged_wallet_profiles(payload, wallet_profiles_store)
    journal_summary = _journal_summary_from_store(journal_store)
    pending_actions = _pending_actions_from_candidates(candidates, candidate_actions)
    payload["decision_mode"] = decision_mode
    payload["candidates"] = candidates
    payload["wallet_profiles"] = wallet_profiles
    payload["journal_summary"] = journal_summary
    payload["pending_actions"] = pending_actions
    return payload


def _empty_state() -> dict:
    return {
        "ts": 0,
        "config": {
            "dry_run": True,
            "execution_mode": "paper",
            "broker_name": "PaperBroker",
            "poll_interval_seconds": 0,
            "bankroll_usd": 0.0,
            "risk_per_trade_pct": 0.0,
            "daily_max_loss_pct": 0.0,
            "max_open_positions": 0,
            "min_wallet_increase_usd": 0.0,
            "max_signals_per_cycle": 0,
            "wallet_pool_size": 0,
            "min_wallet_score": 0.0,
            "wallet_history_refresh_seconds": 0,
            "wallet_history_max_wallets": 0,
            "history_min_closed_positions": 0,
            "history_strong_closed_positions": 0,
            "history_strong_resolved_markets": 0,
            "wallet_score_watch_multiplier": 0.0,
            "wallet_score_trade_multiplier": 0.0,
            "wallet_score_core_multiplier": 0.0,
            "topic_bias_enabled": False,
            "topic_min_samples": 0,
            "topic_positive_roi": 0.0,
            "topic_positive_win_rate": 0.0,
            "topic_negative_roi": 0.0,
            "topic_negative_win_rate": 0.0,
            "topic_boost_multiplier": 0.0,
            "topic_penalty_multiplier": 0.0,
            "wallet_exit_follow_enabled": False,
            "min_wallet_decrease_usd": 0.0,
            "resonance_exit_enabled": False,
            "resonance_min_wallets": 0,
            "resonance_min_wallet_score": 0.0,
            "resonance_trim_fraction": 0.0,
            "resonance_core_exit_fraction": 0.0,
            "token_add_cooldown_seconds": 0,
            "token_reentry_cooldown_seconds": 0,
            "stale_position_minutes": 0,
            "stale_position_trim_pct": 0.0,
            "stale_position_trim_cooldown_seconds": 0,
            "stale_position_close_notional_usd": 0.0,
            "congested_utilization_threshold": 0.0,
            "congested_stale_minutes": 0,
            "congested_trim_pct": 0.0,
            "min_price": 0.0,
            "max_price": 0.0,
            "wallet_discovery_enabled": False,
            "wallet_discovery_mode": "",
            "wallet_discovery_quality_bias_enabled": False,
            "wallet_discovery_quality_top_n": 0,
            "wallet_discovery_history_bonus": 0.0,
            "wallet_discovery_topic_bonus": 0.0,
            "account_sync_refresh_seconds": 0,
        },
        "control": {
            "pause_opening": False,
            "reduce_only": False,
            "emergency_stop": False,
            "clear_stale_pending_requested_ts": 0,
            "updated_ts": 0,
        },
        "trading_mode": {
            "mode": "NORMAL",
            "opening_allowed": True,
            "reason_codes": [],
            "updated_ts": 0,
            "source": "runner",
            "account_state_status": "unknown",
            "reconciliation_status": "unknown",
            "persistence_status": "ok",
        },
        "persistence": {
            "status": "ok",
            "failure_count": 0,
            "last_failure": {},
        },
        "mode": "manual",
        "decision_mode": _default_decision_mode(),
        "startup_ready": True,
        "startup": {
            "ready": True,
            "warning_count": 0,
            "failure_count": 0,
            "checks": [],
        },
        "reconciliation_status": "ok",
        "reconciliation": {
            "day_key": "",
            "status": "ok",
            "issues": [],
            "startup_ready": True,
            "internal_realized_pnl": 0.0,
            "ledger_realized_pnl": 0.0,
            "broker_closed_pnl_today": 0.0,
            "effective_daily_realized_pnl": 0.0,
            "internal_vs_ledger_diff": 0.0,
            "broker_floor_gap_vs_internal": 0.0,
            "fill_count_today": 0,
            "fill_notional_today": 0.0,
            "account_sync_count_today": 0,
            "startup_checks_count_today": 0,
            "last_fill_ts": 0,
            "last_account_sync_ts": 0,
            "last_startup_checks_ts": 0,
            "pending_orders": 0,
            "pending_entry_orders": 0,
            "pending_exit_orders": 0,
            "stale_pending_orders": 0,
            "open_positions": 0,
            "tracked_notional_usd": 0.0,
            "ledger_available": False,
            "account_snapshot_age_seconds": 0,
            "broker_reconcile_age_seconds": 0,
            "broker_event_sync_age_seconds": 0,
        },
        "summary": {
            "pnl_today": 0.0,
            "internal_pnl_today": 0.0,
            "broker_closed_pnl_today": 0.0,
            "equity": 0.0,
            "cash_balance_usd": 0.0,
            "positions_value_usd": 0.0,
            "account_snapshot_ts": 0,
            "open_positions": 0,
            "max_open_positions": 0,
            "slot_utilization_pct": 0.0,
            "exposure_pct": 0.0,
            "signals": 0,
            "tracked_notional_usd": 0.0,
            "available_notional_usd": 0.0,
            "notional_utilization_pct": 0.0,
            "base_per_trade_notional": 0.0,
            "theoretical_max_order_notional": 0.0,
            "per_trade_notional": 0.0,
            "daily_loss_budget_usd": 0.0,
            "daily_loss_used_pct": 0.0,
            "daily_loss_remaining_pct": 0.0,
            "slot_remaining": 0,
            "est_openings": 0,
        },
        "account": {
            "equity_usd": 0.0,
            "cash_balance_usd": 0.0,
            "positions_value_usd": 0.0,
            "tracked_notional_usd": 0.0,
            "available_notional_usd": 0.0,
            "account_snapshot_ts": 0,
        },
        "account_equity": 0.0,
        "cash_balance_usd": 0.0,
        "positions_value_usd": 0.0,
        "tracked_notional_usd": 0.0,
        "available_notional_usd": 0.0,
        "account_snapshot_ts": 0,
        "open_positions": 0,
        "operator_feedback": {
            "last_action": {},
        },
        "candidates": _empty_candidates(),
        "wallet_profiles": _empty_wallet_profiles(),
        "journal_summary": _empty_journal_summary(),
        "stats": _empty_stats_summary(),
        "archive": _empty_archive_summary(),
        "pending_actions": _empty_pending_actions(),
        "notifier": _empty_notifier(),
        "positions": [],
        "orders": [],
        "pending_order_details": [],
        "wallets": [],
        "sources": [],
        "alerts": [],
        "timeline": [],
        "exit_review": {
            "summary": {
                "total_exit_orders": 0,
                "filled_exit_orders": 0,
                "rejected_exit_orders": 0,
                "total_notional": 0.0,
                "latest_exit_ts": 0,
                "topics": 0,
                "sources": 0,
                "avg_hold_minutes": 0.0,
                "max_hold_minutes": 0,
            },
            "by_kind": [],
            "by_topic": [],
            "by_source": [],
            "recent_exits": [],
        },
        "signal_review": {
            "summary": {
                "cycles": 0,
                "candidates": 0,
                "filled": 0,
                "rejected": 0,
                "skipped": 0,
                "traces": 0,
                "open_traces": 0,
                "closed_traces": 0,
            },
            "cycles": [],
            "traces": [],
        },
        "attribution_review": {
            "summary": {
                "windows": ["24h", "7d", "30d", "all"],
                "available_orders": 0,
                "available_exits": 0,
            },
            "windows": {
                "24h": {
                    "key": "24h",
                    "label": "24h",
                    "summary": {
                        "order_count": 0,
                        "filled_count": 0,
                        "rejected_count": 0,
                        "exit_count": 0,
                        "wallets": 0,
                        "topics": 0,
                        "exit_types": 0,
                        "reject_high_score_count": 0,
                    },
                    "by_wallet": [],
                    "by_topic": [],
                    "by_exit_kind": [],
                    "wallet_topic": [],
                    "topic_exit": [],
                    "source_result": [],
                    "reject_reasons": [],
                    "hold_buckets": [],
                    "rankings": {
                        "top_wallets": [],
                        "bottom_wallets": [],
                        "top_topics": [],
                        "bottom_topics": [],
                    },
                },
                "7d": {
                    "key": "7d",
                    "label": "7d",
                    "summary": {
                        "order_count": 0,
                        "filled_count": 0,
                        "rejected_count": 0,
                        "exit_count": 0,
                        "wallets": 0,
                        "topics": 0,
                        "exit_types": 0,
                        "reject_high_score_count": 0,
                    },
                    "by_wallet": [],
                    "by_topic": [],
                    "by_exit_kind": [],
                    "wallet_topic": [],
                    "topic_exit": [],
                    "source_result": [],
                    "reject_reasons": [],
                    "hold_buckets": [],
                    "rankings": {
                        "top_wallets": [],
                        "bottom_wallets": [],
                        "top_topics": [],
                        "bottom_topics": [],
                    },
                },
                "30d": {
                    "key": "30d",
                    "label": "30d",
                    "summary": {
                        "order_count": 0,
                        "filled_count": 0,
                        "rejected_count": 0,
                        "exit_count": 0,
                        "wallets": 0,
                        "topics": 0,
                        "exit_types": 0,
                        "reject_high_score_count": 0,
                    },
                    "by_wallet": [],
                    "by_topic": [],
                    "by_exit_kind": [],
                    "wallet_topic": [],
                    "topic_exit": [],
                    "source_result": [],
                    "reject_reasons": [],
                    "hold_buckets": [],
                    "rankings": {
                        "top_wallets": [],
                        "bottom_wallets": [],
                        "top_topics": [],
                        "bottom_topics": [],
                    },
                },
                "all": {
                    "key": "all",
                    "label": i18n_t("common.all", fallback="All"),
                    "summary": {
                        "order_count": 0,
                        "filled_count": 0,
                        "rejected_count": 0,
                        "exit_count": 0,
                        "wallets": 0,
                        "topics": 0,
                        "exit_types": 0,
                        "reject_high_score_count": 0,
                    },
                    "by_wallet": [],
                    "by_topic": [],
                    "by_exit_kind": [],
                    "wallet_topic": [],
                    "topic_exit": [],
                    "source_result": [],
                    "reject_reasons": [],
                    "hold_buckets": [],
                    "rankings": {
                        "top_wallets": [],
                        "bottom_wallets": [],
                        "top_topics": [],
                        "bottom_topics": [],
                    },
                },
            },
        },
    }


def _empty_monitor_report(report_type: str) -> dict:
    return {
        "report_type": report_type,
        "generated_ts": 0,
        "window_start": "",
        "window_end": "",
        "window_seconds": 0,
        "log_file": "",
        "sample_status": "unknown",
        "counts": {},
        "ratios": {},
        "recommendation": "",
        "final_recommendation": "",
        "consecutive_inconclusive_windows": 0,
        "daemon_state_file": "",
        "startup_ready": None,
        "startup": {},
        "reconciliation_status": "unknown",
        "reconciliation_issue_summary": "",
        "reconciliation": {},
    }


def _empty_reconciliation_eod_report() -> dict:
    return {
        "report_version": 1,
        "generated_ts": 0,
        "generated_at": "",
        "day_key": "",
        "state_path": "",
        "ledger_path": "",
        "status": "unknown",
        "issues": [],
        "startup": {},
        "reconciliation": {},
        "state_summary": {},
        "ledger_summary": {},
        "recommendations": [],
    }


def _default_control() -> dict:
    return {
        "decision_mode": "manual",
        "pause_opening": False,
        "reduce_only": False,
        "emergency_stop": False,
        "clear_stale_pending_requested_ts": 0,
        "updated_ts": 0,
    }


def _load_json(path: str, fallback: dict) -> dict:
    payload = dict(fallback)
    try:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                payload.update(data)
    except Exception:
        return dict(fallback)
    return payload


def _extract_token(headers, query: dict[str, list[str]]) -> str:
    value = headers.get("X-Auth-Token", "").strip()
    if value:
        return value
    auth = headers.get("Authorization", "").strip()
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    query_token = query.get("token", [""])[0].strip()
    if query_token:
        return query_token
    raw_cookie = headers.get("Cookie", "").strip()
    if raw_cookie:
        try:
            cookie = SimpleCookie()
            cookie.load(raw_cookie)
            morsel = cookie.get(AUTH_COOKIE_NAME)
            if morsel is not None:
                return str(morsel.value or "").strip()
        except Exception:
            return ""
    return ""


def _query_token(query: dict[str, list[str]]) -> str:
    return query.get("token", [""])[0].strip()


def _query_values(query: dict[str, list[str]], key: str) -> list[str]:
    values: list[str] = []
    for raw in list(query.get(key, []) or []):
        for part in str(raw or "").split(","):
            text = part.strip()
            if text:
                values.append(text)
    return values


def _query_limit(
    query: dict[str, list[str]],
    key: str,
    *,
    default: int,
    minimum: int = 1,
    maximum: int = 200,
) -> int:
    raw = str(query.get(key, [default])[0] or default).strip()
    try:
        value = int(raw)
    except (TypeError, ValueError):
        value = int(default)
    return max(minimum, min(maximum, value))


def _parse_optional_float(value: object, *, field: str) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(
            i18n_t(
                "web.api.fieldMustBeNumber",
                {"field": _web_field_label(field)},
                fallback=f"{field} must be a number",
            )
        ) from exc


def _parse_bool_value(value: object, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    if text in {"1", "true", "yes", "on"}:
        return True
    if text in {"0", "false", "no", "off"}:
        return False
    raise ValueError(
        i18n_t(
            "web.api.fieldMustBeBoolean",
            {"field": _web_field_label(field)},
            fallback=f"{field} must be a boolean",
        )
    )


def _strip_token_from_path(raw_path: str) -> str:
    parsed = urlsplit(raw_path)
    query = parse_qs(parsed.query, keep_blank_values=False)
    if "token" in query:
        query.pop("token", None)
    clean_query = urlencode(query, doseq=True)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path or "/", clean_query, parsed.fragment))


def _read_repo_dotenv_var(key: str) -> str:
    dotenv = Path(__file__).resolve().parents[2] / ".env"
    if not dotenv.exists():
        return ""
    for raw in dotenv.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        env_key, value = line.split("=", 1)
        if env_key.strip() == key:
            return value.strip().strip('"').strip("'")
    return ""


def _env_or_repo_dotenv(key: str, default: str = "") -> str:
    value = str(os.getenv(key, "")).strip()
    if value:
        return value
    value = _read_repo_dotenv_var(key)
    if value:
        return value
    return default


def _truthy_text(value: object) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "on"}


def _empty_blockbeats_feed(
    *,
    source: str = "disabled",
    status: str = "disabled",
    message: str = "",
    message_key: str = "",
    message_params: dict | None = None,
) -> dict:
    return {
        "source": source,
        "status": status,
        "message": message,
        "message_key": message_key,
        "message_params": dict(message_params or {}),
        "items": [],
    }


def _empty_blockbeats_payload() -> dict:
    return {
        "updated_ts": 0,
        "status": "disabled",
        "stale_after_seconds": 180,
        "prediction": _empty_blockbeats_feed(),
        "important": _empty_blockbeats_feed(),
        "errors": [],
    }


def _blockbeats_extract_items(payload: dict | None) -> list[dict]:
    data = (payload or {}).get("data")
    if isinstance(data, list):
        return [dict(row) for row in data if isinstance(row, dict)]
    if isinstance(data, dict):
        for key in ("data", "items", "list", "records"):
            nested = data.get(key)
            if isinstance(nested, list):
                return [dict(row) for row in nested if isinstance(row, dict)]
    return []


def _normalize_blockbeats_time(value: object) -> object:
    if isinstance(value, (int, float)):
        return int(value)
    text = str(value or "").strip()
    if not text:
        return ""
    if text.isdigit():
        if len(text) >= 13:
            return int(text[:10])
        return int(text)
    return text


def _normalize_blockbeats_item(raw: dict) -> dict:
    return {
        "id": str(raw.get("id") or raw.get("news_id") or ""),
        "title": str(raw.get("title") or raw.get("name") or ""),
        "content": str(raw.get("content") or raw.get("summary") or raw.get("brief") or ""),
        "link": str(raw.get("link") or raw.get("url") or ""),
        "url": str(raw.get("url") or raw.get("link") or ""),
        "create_time": _normalize_blockbeats_time(
            raw.get("create_time") or raw.get("created_at") or raw.get("publish_time") or raw.get("ts") or ""
        ),
    }


def _fetch_blockbeats_json(url: str, *, api_key: str = "", timeout: float = 20.0) -> dict:
    headers = {"accept": "application/json"}
    if api_key.strip():
        headers["api-key"] = api_key.strip()
    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace").strip()
        raise RuntimeError(body or f"HTTP {exc.code}") from exc
    except Exception as exc:
        raise RuntimeError(str(exc)) from exc
    if not isinstance(payload, dict):
        raise RuntimeError("invalid BlockBeats payload")
    return payload


def _fetch_blockbeats_dashboard(limit: int = 6) -> dict:
    api_key = _env_or_repo_dotenv("BLOCKBEATS_API_KEY")
    base_url = _env_or_repo_dotenv("BLOCKBEATS_BASE_URL", "https://api-pro.theblockbeats.info/v1")
    public_base_url = _env_or_repo_dotenv("BLOCKBEATS_PUBLIC_BASE_URL", "https://api.theblockbeats.news/v1/open-api")
    lang = _env_or_repo_dotenv("BLOCKBEATS_LANG", "en")
    stale_after_seconds = max(30, int(float(_env_or_repo_dotenv("BLOCKBEATS_CACHE_SECONDS", "180") or 180)))
    timeout = max(5.0, float(_env_or_repo_dotenv("BLOCKBEATS_MAX_TIME_SECONDS", "20") or 20))
    allow_public_fallback = _truthy_text(_env_or_repo_dotenv("BLOCKBEATS_ALLOW_PUBLIC_FALLBACK", "1"))

    payload = _empty_blockbeats_payload()
    payload["stale_after_seconds"] = stale_after_seconds
    errors: list[str] = []

    prediction_feed = _empty_blockbeats_feed()
    important_feed = _empty_blockbeats_feed()

    prediction_url = f"{base_url}/newsflash/prediction?page=1&size={limit}&lang={lang}"
    important_url = f"{base_url}/newsflash/important?page=1&size={limit}&lang={lang}"
    public_prediction_url = f"{public_base_url}/open-flash?page=1&size={limit}&type=push&lang={lang}"

    if api_key:
        try:
            prediction_payload = _fetch_blockbeats_json(prediction_url, api_key=api_key, timeout=timeout)
            prediction_feed = {
                "source": "pro",
                "status": "ok",
                "message": str(prediction_payload.get("message") or ""),
                "items": [_normalize_blockbeats_item(row) for row in _blockbeats_extract_items(prediction_payload)],
            }
        except Exception as exc:
            errors.append(f"prediction: {exc}")
            if allow_public_fallback:
                try:
                    public_prediction_payload = _fetch_blockbeats_json(public_prediction_url, timeout=timeout)
                    prediction_feed = {
                        "source": "public_fallback",
                        "status": "degraded",
                        "message": i18n_t("blockbeats.message.proPredictionFallback"),
                        "message_key": "blockbeats.message.proPredictionFallback",
                        "message_params": {},
                        "items": [_normalize_blockbeats_item(row) for row in _blockbeats_extract_items(public_prediction_payload)],
                    }
                except Exception as fallback_exc:
                    errors.append(f"prediction_fallback: {fallback_exc}")
                    prediction_feed = _empty_blockbeats_feed(
                        source="error",
                        status="error",
                        message=i18n_t("blockbeats.message.feedUnavailable", {"feed": "prediction", "reason": str(fallback_exc)}),
                        message_key="blockbeats.message.feedUnavailable",
                        message_params={"feed": "prediction", "reason": str(fallback_exc)},
                    )
            else:
                prediction_feed = _empty_blockbeats_feed(
                    source="error",
                    status="error",
                    message=i18n_t("blockbeats.message.feedUnavailable", {"feed": "prediction", "reason": str(exc)}),
                    message_key="blockbeats.message.feedUnavailable",
                    message_params={"feed": "prediction", "reason": str(exc)},
                )

        try:
            important_payload = _fetch_blockbeats_json(important_url, api_key=api_key, timeout=timeout)
            important_feed = {
                "source": "pro",
                "status": "ok",
                "message": str(important_payload.get("message") or ""),
                "items": [_normalize_blockbeats_item(row) for row in _blockbeats_extract_items(important_payload)],
            }
        except Exception as exc:
            errors.append(f"important: {exc}")
            important_feed = _empty_blockbeats_feed(
                source="error",
                status="error",
                message=i18n_t("blockbeats.message.feedUnavailable", {"feed": "important", "reason": str(exc)}),
                message_key="blockbeats.message.feedUnavailable",
                message_params={"feed": "important", "reason": str(exc)},
            )
    elif allow_public_fallback:
        try:
            public_prediction_payload = _fetch_blockbeats_json(public_prediction_url, timeout=timeout)
            prediction_feed = {
                "source": "public_fallback",
                "status": "degraded",
                "message": i18n_t("blockbeats.message.apiKeyMissingFallback"),
                "message_key": "blockbeats.message.apiKeyMissingFallback",
                "message_params": {},
                "items": [_normalize_blockbeats_item(row) for row in _blockbeats_extract_items(public_prediction_payload)],
            }
            important_feed = _empty_blockbeats_feed(
                source="disabled",
                status="disabled",
                message=i18n_t("blockbeats.message.importantRequiresApiKey"),
                message_key="blockbeats.message.importantRequiresApiKey",
            )
            errors.append("important: BLOCKBEATS_API_KEY not set")
        except Exception as exc:
            errors.append(f"prediction_fallback: {exc}")
            prediction_feed = _empty_blockbeats_feed(
                source="error",
                status="error",
                message=i18n_t("blockbeats.message.feedUnavailable", {"feed": "prediction", "reason": str(exc)}),
                message_key="blockbeats.message.feedUnavailable",
                message_params={"feed": "prediction", "reason": str(exc)},
            )
            important_feed = _empty_blockbeats_feed(
                source="disabled",
                status="disabled",
                message=i18n_t("blockbeats.message.importantRequiresApiKey"),
                message_key="blockbeats.message.importantRequiresApiKey",
            )
    else:
        prediction_feed = _empty_blockbeats_feed(
            source="disabled",
            status="disabled",
            message=i18n_t("blockbeats.message.apiKeyMissing"),
            message_key="blockbeats.message.apiKeyMissing",
        )
        important_feed = _empty_blockbeats_feed(
            source="disabled",
            status="disabled",
            message=i18n_t("blockbeats.message.apiKeyMissing"),
            message_key="blockbeats.message.apiKeyMissing",
        )
        errors.append("BLOCKBEATS_API_KEY not set")

    payload["updated_ts"] = int(time.time())
    payload["prediction"] = prediction_feed
    payload["important"] = important_feed
    payload["errors"] = errors
    has_items = bool(prediction_feed["items"] or important_feed["items"])
    if not api_key and prediction_feed["items"]:
        payload["status"] = "degraded"
    elif errors and has_items:
        payload["status"] = "degraded"
    elif errors:
        payload["status"] = "error"
    else:
        payload["status"] = "ok"
    return payload


def build_handler(
    frontend_dir: str,
    state_path: str,
    control_path: str,
    control_token: str,
    monitor_30m_json_path: str,
    monitor_12h_json_path: str,
    reconciliation_eod_json_path: str,
    reconciliation_eod_text_path: str,
    ledger_path: str,
    decision_mode_path: str = "",
    candidate_actions_path: str = "",
    wallet_profiles_path: str = "",
    journal_path: str = "",
    candidate_db_path: str = "",
    public_state_path: str = "",
):
    resolved_decision_mode_path = decision_mode_path or _runtime_store_path(
        state_path,
        "decision_mode.json",
        "POLY_DECISION_MODE_PATH",
    )
    resolved_candidate_actions_path = candidate_actions_path or _runtime_store_path(
        state_path,
        "candidate_actions.json",
        "POLY_CANDIDATE_ACTIONS_PATH",
    )
    resolved_wallet_profiles_path = wallet_profiles_path or _runtime_store_path(
        state_path,
        "wallet_profiles.json",
        "POLY_WALLET_PROFILES_PATH",
    )
    resolved_journal_path = journal_path or _runtime_store_path(
        state_path,
        "journal.json",
        "POLY_JOURNAL_PATH",
    )
    resolved_candidate_db_path = candidate_db_path or _runtime_store_path(
        state_path,
        "decision_terminal.db",
        "POLY_CANDIDATE_DB_PATH",
    )
    resolved_public_state_path = public_state_path or _runtime_store_path(
        state_path,
        "poly_public_state.json",
        "POLY_PUBLIC_STATE_PATH",
    )
    candidate_store = PersonalTerminalStore(resolved_candidate_db_path)
    blockbeats_cache_ttl = max(30, int(float(_env_or_repo_dotenv("BLOCKBEATS_CACHE_SECONDS", "180") or 180)))
    blockbeats_limit = max(2, min(12, int(float(_env_or_repo_dotenv("BLOCKBEATS_DASHBOARD_LIMIT", "6") or 6))))
    blockbeats_cache_lock = threading.Lock()
    blockbeats_cache: dict[str, object] = {
        "fetched_at": 0.0,
        "payload": _empty_blockbeats_payload(),
    }

    def _sync_public_state_snapshot() -> None:
        if not resolved_public_state_path:
            return
        if not os.path.exists(state_path):
            return
        state = _load_json(state_path, _empty_state())
        payload = _api_state_payload(state, candidate_store)
        try:
            _safe_write_json(resolved_public_state_path, payload)
        except Exception:
            return

    def _cached_blockbeats_payload(force: bool = False) -> dict:
        now = time.time()
        with blockbeats_cache_lock:
            cached_payload = blockbeats_cache.get("payload")
            fetched_at = float(blockbeats_cache.get("fetched_at") or 0.0)
        if isinstance(cached_payload, dict):
            cache_ttl = max(30, int(float(cached_payload.get("stale_after_seconds") or blockbeats_cache_ttl)))
        else:
            cache_ttl = blockbeats_cache_ttl
        if not force and isinstance(cached_payload, dict) and fetched_at > 0 and (now - fetched_at) < cache_ttl:
            return cached_payload
        try:
            payload = _fetch_blockbeats_dashboard(limit=blockbeats_limit)
        except Exception as exc:
            error_message = i18n_t("blockbeats.message.dashboardUnavailable", {"reason": str(exc)})
            payload = _empty_blockbeats_payload()
            payload["updated_ts"] = int(now)
            payload["stale_after_seconds"] = blockbeats_cache_ttl
            payload["status"] = "error"
            payload["errors"] = [error_message]
            payload["prediction"] = _empty_blockbeats_feed(
                source="error",
                status="error",
                message=error_message,
                message_key="blockbeats.message.dashboardUnavailable",
                message_params={"reason": str(exc)},
            )
            payload["important"] = _empty_blockbeats_feed(
                source="error",
                status="error",
                message=error_message,
                message_key="blockbeats.message.dashboardUnavailable",
                message_params={"reason": str(exc)},
            )
        with blockbeats_cache_lock:
            blockbeats_cache["fetched_at"] = now
            blockbeats_cache["payload"] = payload
        return payload

    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            self._set_auth_cookie: str = ""
            super().__init__(*args, directory=frontend_dir, **kwargs)

        def log_message(self, format: str, *args) -> None:
            return

        def end_headers(self) -> None:
            if self._set_auth_cookie:
                self.send_header(
                    "Set-Cookie",
                    f"{AUTH_COOKIE_NAME}={self._set_auth_cookie}; Path=/; HttpOnly; SameSite=Lax",
                )
                self._set_auth_cookie = ""
            header_buffer = getattr(self, "_headers_buffer", [])
            if not any(b"Cache-Control:" in chunk for chunk in header_buffer):
                self.send_header("Cache-Control", "no-store")
            super().end_headers()

        def _json_response(self, payload: dict, status: HTTPStatus = HTTPStatus.OK) -> None:
            raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _html_response(self, body: str, status: HTTPStatus = HTTPStatus.OK) -> None:
            raw = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _text_response(self, body: str, content_type: str, status: HTTPStatus = HTTPStatus.OK) -> None:
            raw = body.encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _is_authorized(self, query: dict[str, list[str]]) -> bool:
            if not control_token:
                return True
            got = _extract_token(self.headers, query)
            return got == control_token

        def _maybe_arm_cookie(self, query: dict[str, list[str]]) -> None:
            query_token = _query_token(query)
            if control_token and query_token and query_token == control_token:
                self._set_auth_cookie = control_token

        def _redirect_with_cookie(self, location: str, query: dict[str, list[str]]) -> None:
            self._maybe_arm_cookie(query)
            self.send_response(HTTPStatus.FOUND)
            self.send_header("Location", location)
            self.send_header("Cache-Control", "no-store")
            self.end_headers()

        def _unauthorized_page(self) -> str:
            locale = html_escape(i18n_current_locale())
            title = html_escape(i18n_t("web.auth.title", fallback="Polymarket Dashboard Access"))
            heading = html_escape(i18n_t("web.auth.heading", fallback="Protected access"))
            description = html_escape(
                i18n_t(
                    "web.auth.description",
                    {"tokenQuery": "?token=..."},
                    fallback="This dashboard requires an access token. Open a shared link with ?token=... or enter the token below.",
                )
            )
            placeholder = html_escape(i18n_t("web.auth.placeholder", fallback="Enter access token"))
            submit = html_escape(i18n_t("web.auth.submit", fallback="Open dashboard"))
            return f"""<!doctype html>
<html lang="{locale}">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>{title}</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background:#0f172a; color:#e2e8f0; margin:0; display:grid; min-height:100vh; place-items:center; }
    main { width:min(92vw, 460px); background:#111827; border:1px solid #334155; border-radius:16px; padding:24px; box-shadow:0 20px 60px rgba(0,0,0,.35); }
    h1 { margin:0 0 10px; font-size:22px; }
    p { margin:0 0 16px; color:#94a3b8; line-height:1.5; }
    input { width:100%; box-sizing:border-box; padding:12px 14px; border-radius:10px; border:1px solid #475569; background:#020617; color:#e2e8f0; margin:0 0 12px; }
    button { width:100%; padding:12px 14px; border-radius:10px; border:0; background:#22c55e; color:#052e16; font-weight:700; cursor:pointer; }
    code { color:#f8fafc; }
  </style>
</head>
<body>
  <main>
    <h1>{heading}</h1>
    <p>{description}</p>
    <form method="GET" action="/">
      <input name="token" type="password" placeholder="{placeholder}" autocomplete="current-password" />
      <button type="submit">{submit}</button>
    </form>
  </main>
</body>
</html>"""

        def _read_json_body(self) -> dict:
            try:
                content_len = int(self.headers.get("Content-Length", "0"))
            except ValueError:
                content_len = 0
            if content_len <= 0:
                return {}
            raw = self.rfile.read(min(content_len, 8192))
            if not raw:
                return {}
            try:
                payload = json.loads(raw.decode("utf-8"))
                return payload if isinstance(payload, dict) else {}
            except Exception:
                return {}

        @staticmethod
        def sync_public_state_snapshot() -> None:
            _sync_public_state_snapshot()

        def do_GET(self) -> None:
            parsed = urlsplit(self.path)
            path = parsed.path
            query = parse_qs(parsed.query, keep_blank_values=False)

            if control_token and not self._is_authorized(query):
                if path.startswith("/api/"):
                    self._json_response(_api_error_payload("unauthorized", fallback="unauthorized"), HTTPStatus.UNAUTHORIZED)
                else:
                    self._html_response(self._unauthorized_page(), HTTPStatus.UNAUTHORIZED)
                return

            if control_token and _query_token(query) == control_token and not path.startswith("/api/"):
                self._redirect_with_cookie(_strip_token_from_path(self.path), query)
                return

            self._maybe_arm_cookie(query)

            if path == "/api/state":
                payload = _api_state_payload(
                    _load_json(state_path, _empty_state()),
                    candidate_store,
                    days=_query_limit(query, "days", default=30, maximum=365),
                    recent_days=_query_limit(query, "recent_days", default=7, maximum=90),
                )
                payload["control"] = _load_json(control_path, _default_control())
                _sync_public_state_snapshot()
                self._json_response(payload)
                return

            if path == "/api/control":
                if not self._is_authorized(query):
                    self._json_response(_api_error_payload("unauthorized", fallback="unauthorized"), HTTPStatus.UNAUTHORIZED)
                    return
                payload = _load_json(control_path, _default_control())
                self._json_response(payload)
                return

            if path == "/api/monitor/30m":
                payload = _load_json(monitor_30m_json_path, _empty_monitor_report("monitor_30m"))
                self._json_response(payload)
                return

            if path == "/api/monitor/12h":
                payload = _load_json(monitor_12h_json_path, _empty_monitor_report("monitor_12h"))
                self._json_response(payload)
                return

            if path == "/api/reconciliation/eod":
                payload = _load_json(reconciliation_eod_json_path, _empty_reconciliation_eod_report())
                self._json_response(payload)
                return

            if path == "/api/blockbeats":
                force = False
                if query.get("force"):
                    try:
                        force = _parse_bool_value(query.get("force", ["false"])[0], field="force")
                    except ValueError:
                        self._json_response(_api_error_payload("forceInvalidBool", fallback="force must be true/false"), HTTPStatus.BAD_REQUEST)
                        return
                payload = _cached_blockbeats_payload(force=force)
                self._json_response(payload)
                return

            if path.startswith("/api/candidates/") and path != "/api/candidates/":
                candidate_id = path[len("/api/candidates/") :].strip("/")
                if not candidate_id:
                    self._json_response(_api_error_payload("candidateIdRequired", fallback="candidate_id is required"), HTTPStatus.BAD_REQUEST)
                    return
                state = _load_json(state_path, _empty_state())
                payload = _candidate_detail_payload(
                    state,
                    candidate_store,
                    candidate_id=candidate_id,
                    decision_mode_path=resolved_decision_mode_path,
                    candidate_actions_path=resolved_candidate_actions_path,
                    wallet_profiles_path=resolved_wallet_profiles_path,
                    journal_path=resolved_journal_path,
                )
                if not payload:
                    error_payload = _api_error_payload(
                        "candidateNotFound",
                        {"candidateId": candidate_id},
                        fallback="candidate not found",
                    )
                    error_payload["candidate_id"] = candidate_id
                    self._json_response(error_payload, HTTPStatus.NOT_FOUND)
                    return
                self._json_response(payload)
                return

            if path == "/api/candidates":
                state = _load_json(state_path, _empty_state())
                runtime_state = _state_with_personal_console(
                    state,
                    decision_mode_path=resolved_decision_mode_path,
                    candidate_actions_path=resolved_candidate_actions_path,
                    wallet_profiles_path=resolved_wallet_profiles_path,
                    journal_path=resolved_journal_path,
                )
                runtime_candidates = list(((runtime_state.get("candidates") or {}).get("items") or []))
                runtime_candidate_observability = _merge_candidate_observability_defaults(
                    ((runtime_state.get("candidates") or {}).get("observability") or {})
                )
                statuses = _query_values(query, "status")
                limit = _query_limit(query, "limit", default=24)
                wallet = str(query.get("wallet", [""])[0] or "").strip().lower()
                market_slug = str(query.get("market_slug", [""])[0] or "").strip().lower()
                candidate_id = str(query.get("candidate_id", [""])[0] or "").strip()
                trace_id = str(query.get("trace_id", [""])[0] or "").strip()
                signal_id = str(query.get("signal_id", [""])[0] or "").strip()
                search = str(query.get("search", [query.get("q", [""])[0] or ""])[0] or "").strip()
                side = str(query.get("side", [""])[0] or "").strip()
                action = ",".join(_query_values(query, "action"))
                sort = str(query.get("sort", ["score"])[0] or "score").strip()
                order = str(query.get("order", ["desc"])[0] or "desc").strip().lower()
                include_expired = _parse_bool_value(query.get("include_expired", ["false"])[0], field="include_expired") if query.get("include_expired") else False
                store_candidates = candidate_store.list_candidates(
                    statuses=statuses or None,
                    limit=max(limit, 1000),
                    include_expired=include_expired,
                    wallet=wallet,
                    market_slug=market_slug,
                    candidate_id=candidate_id,
                    trace_id=trace_id,
                    signal_id=signal_id,
                    search=search,
                    side=side,
                    action=action,
                    sort=sort,
                    order=order,
                )
                if candidate_store is not None:
                    store_items = list(store_candidates)
                    items = store_items if store_items else _merge_candidate_rows([], runtime_candidates)
                else:
                    items = _merge_candidate_rows([], runtime_candidates)
                if candidate_id:
                    items = [row for row in items if str(row.get("id") or "").strip() == candidate_id or str(row.get("signal_id") or "").strip() == candidate_id or str(row.get("trace_id") or "").strip() == candidate_id]
                if trace_id:
                    items = [row for row in items if str(row.get("trace_id") or "").strip() == trace_id]
                if signal_id:
                    items = [row for row in items if str(row.get("signal_id") or "").strip() == signal_id]
                if side:
                    items = [row for row in items if str(row.get("side") or "").strip().upper() == side.upper()]
                if search:
                    tokens = [tok for tok in search.lower().split() if tok]
                    if tokens:
                        filtered = []
                        for row in items:
                            blob = json.dumps(row, ensure_ascii=False, sort_keys=True, default=str).lower()
                            if all(token in blob for token in tokens):
                                filtered.append(row)
                        items = filtered
                if action:
                    action_tokens = [token.strip() for token in action.lower().split(",") if token.strip()]
                    if action_tokens:
                        filtered = []
                        for row in items:
                            haystack = " ".join(
                                str(row.get(key) or "").strip().lower()
                                for key in ("action", "selected_action", "suggested_action", "review_action", "status", "skip_reason", "note", "result_tag")
                            )
                            if any(token in haystack for token in action_tokens):
                                filtered.append(row)
                        items = filtered
                reverse = order != "asc"
                items.sort(
                    key=lambda row: candidate_store._candidate_sort_value(row, sort),
                    reverse=reverse,
                )
                items = items[: max(1, limit)]
                observability = _candidate_observability(items)
                observability["recent_cycles"] = dict(runtime_candidate_observability.get("recent_cycles") or {})
                payload = {
                    "summary": {
                        "count": len(items),
                        "pending": sum(1 for item in items if str(item.get("status") or "") == "pending"),
                        "approved": sum(1 for item in items if str(item.get("status") or "") == "approved"),
                        "watched": sum(1 for item in items if str(item.get("status") or "") == "watched"),
                        "executed": sum(1 for item in items if str(item.get("status") or "") == "executed"),
                    },
                    "observability": observability,
                    "filters": {
                        "status": statuses,
                        "limit": limit,
                        "wallet": wallet,
                        "market_slug": market_slug,
                        "candidate_id": candidate_id,
                        "trace_id": trace_id,
                        "signal_id": signal_id,
                        "search": search,
                        "side": side,
                        "action": action,
                        "sort": sort,
                        "order": order,
                        "include_expired": include_expired,
                    },
                    "items": items,
                }
                self._json_response(payload)
                return

            if path == "/api/wallet-profiles":
                items = candidate_store.list_wallet_profiles(limit=_query_limit(query, "limit", default=100))
                payload = {
                    "summary": {
                        "count": len(items),
                        "enabled": sum(1 for item in items if bool(item.get("enabled", True))),
                    },
                    "items": items,
                }
                self._json_response(payload)
                return

            if path == "/api/journal":
                limit = _query_limit(query, "limit", default=20)
                payload = dict(candidate_store.journal_summary(days=30))
                payload["notes"] = candidate_store.list_journal_entries(limit=limit)
                payload["limit"] = limit
                self._json_response(payload)
                return

            if path == "/api/stats":
                days = _query_limit(query, "days", default=30, maximum=365)
                recent_days = _query_limit(query, "recent_days", default=7, maximum=90)
                payload = _stats_from_store(candidate_store, days=days, recent_days=recent_days)
                self._json_response(payload)
                return

            if path == "/api/archive":
                days = _query_limit(query, "days", default=30, maximum=365)
                recent_days = _query_limit(query, "recent_days", default=7, maximum=90)
                payload = _archive_from_store(candidate_store, days=days, recent_days=recent_days)
                self._json_response(payload)
                return

            if path == "/api/export":
                scope = str(query.get("scope", ["bundle"])[0] or "bundle").strip().lower()
                fmt = str(query.get("format", ["json"])[0] or "json").strip().lower()
                days = _query_limit(query, "days", default=30, maximum=365)
                recent_days = _query_limit(query, "recent_days", default=7, maximum=90)
                limit = _query_limit(query, "limit", default=200, maximum=2000)
                try:
                    payload = _export_bundle(candidate_store, scope=scope, days=days, recent_days=recent_days, limit=limit)
                except ValueError as exc:
                    self._json_response(
                        _api_error_payload(
                            "validationFailed",
                            fallback="request validation failed",
                            detail=str(exc),
                        ),
                        HTTPStatus.BAD_REQUEST,
                    )
                    return
                if fmt == "csv":
                    if scope == "bundle":
                        self._json_response(
                            _api_error_payload(
                                "csvRequiresSingleScope",
                                fallback="csv export requires a single scope",
                            ),
                            HTTPStatus.BAD_REQUEST,
                        )
                        return
                    if scope == "archive":
                        rows = list(payload.get("daily_rows") or [])
                    elif scope == "stats":
                        rows = [payload]
                    else:
                        rows = list(payload.get("items") or [])
                    csv_text = _csv_text_from_items(rows)
                    self._text_response(csv_text, "text/csv; charset=utf-8")
                    return
                if fmt != "json":
                    self._json_response(
                        _api_error_payload(
                            "exportUnsupportedFormat",
                            fallback="unsupported export format",
                        ),
                        HTTPStatus.BAD_REQUEST,
                    )
                    return
                self._json_response(payload)
                return

            if path == "/api/mode":
                control = _load_json(control_path, _default_control())
                payload = {
                    "mode": str(control.get("decision_mode") or "manual"),
                    "updated_ts": int(control.get("updated_ts") or 0),
                    "updated_by": "control",
                    "note": "",
                    "available_modes": ["manual", "semi_auto", "auto"],
                }
                self._json_response(payload)
                return

            if path == "/":
                self.path = "/index.html"
            return super().do_GET()

        def do_POST(self) -> None:
            parsed = urlsplit(self.path)
            path = parsed.path
            query = parse_qs(parsed.query, keep_blank_values=False)

            if path not in {
                "/api/control",
                "/api/operator",
                "/api/candidate/action",
                "/api/mode",
                "/api/journal/note",
                "/api/wallet-profiles/update",
            }:
                self._json_response(_api_error_payload("notFound", fallback="not found"), HTTPStatus.NOT_FOUND)
                return
            if not self._is_authorized(query):
                self._json_response(_api_error_payload("unauthorized", fallback="unauthorized"), HTTPStatus.UNAUTHORIZED)
                return
            self._maybe_arm_cookie(query)

            incoming = self._read_json_body()
            if path == "/api/candidate/action":
                candidate_id = str(incoming.get("candidate_id") or incoming.get("signal_id") or "").strip()
                action = str(incoming.get("action") or "").strip().lower()
                if not candidate_id or not action:
                    self._json_response(
                        _api_error_payload(
                            "candidateActionRequired",
                            fallback="candidate_id and action are required",
                        ),
                        HTTPStatus.BAD_REQUEST,
                    )
                    return
                requested_ts = int(time.time())
                try:
                    updated = candidate_store.record_candidate_action(
                        candidate_id,
                        action=action,
                        notional=float(incoming.get("notional") or 0.0),
                        note=str(incoming.get("note") or ""),
                        payload={
                            "requested_ts": int(incoming.get("requested_ts") or requested_ts),
                            "updated_by": str(incoming.get("updated_by") or "api"),
                        },
                        created_ts=requested_ts,
                        idempotency_key=str(incoming.get("idempotency_key") or "").strip() or None,
                    )
                except ValueError as exc:
                    self._json_response(
                        _api_error_payload(
                            "validationFailed",
                            fallback="request validation failed",
                            detail=str(exc),
                        ),
                        HTTPStatus.BAD_REQUEST,
                    )
                    return
                if updated is None:
                    self._json_response(
                        _api_error_payload(
                            "candidateNotFound",
                            {"candidateId": candidate_id},
                            fallback="candidate not found",
                        ),
                        HTTPStatus.NOT_FOUND,
                    )
                    return
                idempotent_replay = bool(updated.get("_idempotent_replay", False))
                note = str(incoming.get("note") or "").strip()
                if note and not idempotent_replay:
                    candidate_store.append_journal_entry(
                        {
                            "candidate_id": candidate_id,
                            "action": action,
                            "rationale": note,
                            "created_ts": requested_ts,
                            "market_slug": str(updated.get("market_slug") or ""),
                            "wallet": str(updated.get("wallet") or ""),
                            "payload": {"source": "candidate_action_api"},
                        }
                    )
                self._json_response({"ok": True, "candidate": updated, "idempotent_replay": idempotent_replay})
                return

            if path == "/api/mode":
                mode = str(incoming.get("mode") or "").strip().lower()
                if mode not in {"manual", "semi_auto", "auto"}:
                    self._json_response(_api_error_payload("invalidMode", fallback="invalid mode"), HTTPStatus.BAD_REQUEST)
                    return
                payload = _load_json(control_path, _default_control())
                payload["decision_mode"] = mode
                payload["updated_ts"] = int(time.time())
                _safe_write_json(control_path, payload)
                self._json_response(
                    {
                        "ok": True,
                        "decision_mode": {
                            "mode": mode,
                            "updated_ts": int(payload["updated_ts"]),
                            "updated_by": str(incoming.get("updated_by") or "api"),
                            "note": str(incoming.get("note") or ""),
                            "available_modes": ["manual", "semi_auto", "auto"],
                        },
                    }
                )
                return

            if path == "/api/journal/note":
                text = str(incoming.get("text") or incoming.get("note") or incoming.get("rationale") or "").strip()
                if not text:
                    self._json_response(_api_error_payload("textRequired", fallback="text is required"), HTTPStatus.BAD_REQUEST)
                    return
                ts = int(time.time())
                note = candidate_store.append_journal_entry(
                    {
                        "candidate_id": str(incoming.get("candidate_id") or ""),
                        "action": str(incoming.get("action") or "note"),
                        "rationale": text,
                        "result_tag": str(incoming.get("result_tag") or ""),
                        "created_ts": ts,
                        "market_slug": str(incoming.get("market_slug") or ""),
                        "wallet": str(incoming.get("wallet") or ""),
                        "pnl_realized": incoming.get("pnl_realized"),
                        "payload": {
                            "tags": _as_str_list(incoming.get("tags")),
                            "signal_id": str(incoming.get("signal_id") or ""),
                            "trace_id": str(incoming.get("trace_id") or ""),
                        },
                    }
                )
                self._json_response({"ok": True, "note": note, "summary": candidate_store.journal_summary(days=30)})
                return

            if path == "/api/wallet-profiles/update":
                wallet = str(incoming.get("wallet") or "").strip().lower()
                if not wallet:
                    self._json_response(_api_error_payload("walletRequired", fallback="wallet is required"), HTTPStatus.BAD_REQUEST)
                    return
                allowed_fields = {
                    "wallet",
                    "tag",
                    "trust_score",
                    "followability_score",
                    "avg_hold_minutes",
                    "category",
                    "enabled",
                    "notes",
                    "tags",
                }
                unknown_fields = sorted(key for key in incoming if key not in allowed_fields)
                if unknown_fields:
                    error_payload = _api_error_payload(
                        "walletProfileFieldsUnsupported",
                        fallback="unsupported wallet profile fields",
                    )
                    error_payload["fields"] = unknown_fields
                    self._json_response(error_payload, HTTPStatus.BAD_REQUEST)
                    return
                existing_profiles = {
                    str(row.get("wallet") or "").strip().lower(): row
                    for row in candidate_store.list_wallet_profiles(limit=256)
                }
                existing = dict(existing_profiles.get(wallet) or {})
                try:
                    profile = {
                        "wallet": wallet,
                        "tag": str(incoming.get("tag") if "tag" in incoming else existing.get("tag") or ""),
                        "trust_score": (
                            _parse_optional_float(incoming.get("trust_score"), field="trust_score")
                            if "trust_score" in incoming
                            else float(existing.get("trust_score") or 0.0)
                        ),
                        "followability_score": (
                            _parse_optional_float(incoming.get("followability_score"), field="followability_score")
                            if "followability_score" in incoming
                            else float(existing.get("followability_score") or 0.0)
                        ),
                        "avg_hold_minutes": (
                            _parse_optional_float(incoming.get("avg_hold_minutes"), field="avg_hold_minutes")
                            if "avg_hold_minutes" in incoming
                            else existing.get("avg_hold_minutes")
                        ),
                        "category": str(incoming.get("category") if "category" in incoming else existing.get("category") or ""),
                        "enabled": (
                            _parse_bool_value(incoming.get("enabled"), field="enabled")
                            if "enabled" in incoming
                            else bool(existing.get("enabled", True))
                        ),
                        "notes": str(incoming.get("notes") if "notes" in incoming else existing.get("notes") or ""),
                        "updated_ts": int(time.time()),
                        "payload": {
                            "tags": _as_str_list(
                                incoming.get("tags")
                                if "tags" in incoming
                                else (existing.get("payload") or {}).get("tags")
                            )
                        },
                    }
                except ValueError as exc:
                    self._json_response(
                        _api_error_payload(
                            "validationFailed",
                            fallback="request validation failed",
                            detail=str(exc),
                        ),
                        HTTPStatus.BAD_REQUEST,
                    )
                    return
                candidate_store.upsert_wallet_profile(profile)
                self._json_response({"ok": True, "wallet_profile": profile})
                return

            if path == "/api/operator":
                command = str(incoming.get("command", "")).strip().lower()
                if command not in {"generate_reconciliation_report", "clear_stale_pending"}:
                    self._json_response(_api_error_payload("invalidCommand", fallback="invalid command"), HTTPStatus.BAD_REQUEST)
                    return
                if command == "clear_stale_pending":
                    payload = _load_json(control_path, _default_control())
                    requested_ts = int(time.time())
                    payload["clear_stale_pending_requested_ts"] = requested_ts
                    payload["updated_ts"] = requested_ts
                    _safe_write_json(control_path, payload)
                    self._json_response(
                        {
                            "ok": True,
                            "command": command,
                            "requested_ts": requested_ts,
                            "message": i18n_t(
                                "web.api.stalePendingQueued",
                                fallback="stale pending cleanup request queued",
                            ),
                        }
                    )
                    return
                requested_day_key = str(incoming.get("day_key", "")).strip()
                try:
                    report = build_reconciliation_report_from_paths(
                        state_path=state_path,
                        ledger_path=ledger_path,
                        day_key=requested_day_key,
                    )
                    write_report_files(
                        report,
                        text_path=reconciliation_eod_text_path,
                        json_path=reconciliation_eod_json_path,
                    )
                except Exception as exc:
                    self._json_response(
                        {
                            "ok": False,
                            "command": command,
                            **_api_error_payload(
                                "reportGenerationFailed",
                                fallback="report generation failed",
                                detail=str(exc),
                            ),
                        },
                        HTTPStatus.INTERNAL_SERVER_ERROR,
                    )
                    return
                self._json_response(
                    {
                        "ok": True,
                        "command": command,
                        "generated_ts": int(report.get("generated_ts") or 0),
                        "day_key": str(report.get("day_key") or ""),
                        "status": str(report.get("status") or "unknown"),
                        "json_path": reconciliation_eod_json_path,
                        "text_path": reconciliation_eod_text_path,
                        "recommendations": list(report.get("recommendations") or []),
                    }
                )
                return

            payload = _load_json(control_path, _default_control())
            command = str(incoming.get("command", "")).strip().lower()
            value = bool(incoming.get("value", True))

            if command in {"pause_opening", "reduce_only", "emergency_stop"}:
                payload[command] = value
            elif command == "clear_all":
                payload["pause_opening"] = False
                payload["reduce_only"] = False
                payload["emergency_stop"] = False
                payload["clear_stale_pending_requested_ts"] = 0
            else:
                self._json_response(_api_error_payload("invalidCommand", fallback="invalid command"), HTTPStatus.BAD_REQUEST)
                return

            payload["updated_ts"] = int(time.time())
            _safe_write_json(control_path, payload)
            self._json_response(payload)

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket dashboard web server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8787)
    parser.add_argument("--state-path", default="/tmp/poly_runtime_data/state.json")
    parser.add_argument("--control-path", default="/tmp/poly_runtime_data/control.json")
    parser.add_argument("--control-token", default=os.getenv("POLY_CONTROL_TOKEN", ""))
    parser.add_argument("--monitor-30m-json-path", default="/tmp/poly_monitor_30m_report.json")
    parser.add_argument("--monitor-12h-json-path", default="/tmp/poly_monitor_12h_report.json")
    parser.add_argument("--reconciliation-eod-json-path", default="/tmp/poly_reconciliation_eod_report.json")
    parser.add_argument("--reconciliation-eod-text-path", default="/tmp/poly_reconciliation_eod_report.txt")
    parser.add_argument("--ledger-path", default=os.getenv("LEDGER_PATH", "/tmp/poly_runtime_data/ledger.jsonl"))
    parser.add_argument("--public-state-path", default=os.getenv("POLY_PUBLIC_STATE_PATH", "/tmp/poly_public_state.json"))
    parser.add_argument(
        "--decision-mode-path",
        default=os.getenv("POLY_DECISION_MODE_PATH", "/tmp/poly_runtime_data/decision_mode.json"),
    )
    parser.add_argument(
        "--candidate-actions-path",
        default=os.getenv("POLY_CANDIDATE_ACTIONS_PATH", "/tmp/poly_runtime_data/candidate_actions.json"),
    )
    parser.add_argument(
        "--wallet-profiles-path",
        default=os.getenv("POLY_WALLET_PROFILES_PATH", "/tmp/poly_runtime_data/wallet_profiles.json"),
    )
    parser.add_argument(
        "--journal-path",
        default=os.getenv("POLY_JOURNAL_PATH", "/tmp/poly_runtime_data/journal.json"),
    )
    parser.add_argument(
        "--candidate-db-path",
        default=os.getenv("POLY_CANDIDATE_DB_PATH", "/tmp/poly_runtime_data/decision_terminal.db"),
    )
    parser.add_argument("--frontend-dir", default="")
    args = parser.parse_args()
    settings = Settings()
    runtime_paths = build_runtime_artifact_paths(settings)
    if args.state_path == "/tmp/poly_runtime_data/state.json":
        args.state_path = runtime_paths["state_path"]
    if args.control_path == "/tmp/poly_runtime_data/control.json":
        args.control_path = runtime_paths["control_path"]
    if args.monitor_30m_json_path == "/tmp/poly_monitor_30m_report.json":
        args.monitor_30m_json_path = runtime_paths["monitor_30m_json_path"]
    if args.monitor_12h_json_path == "/tmp/poly_monitor_12h_report.json":
        args.monitor_12h_json_path = runtime_paths["monitor_12h_json_path"]
    if args.reconciliation_eod_json_path == "/tmp/poly_reconciliation_eod_report.json":
        args.reconciliation_eod_json_path = runtime_paths["reconciliation_eod_json_path"]
    if args.reconciliation_eod_text_path == "/tmp/poly_reconciliation_eod_report.txt":
        args.reconciliation_eod_text_path = runtime_paths["reconciliation_eod_text_path"]
    if args.ledger_path == os.getenv("LEDGER_PATH", "/tmp/poly_runtime_data/ledger.jsonl"):
        args.ledger_path = runtime_paths["ledger_path"]
    if args.public_state_path == os.getenv("POLY_PUBLIC_STATE_PATH", "/tmp/poly_public_state.json"):
        args.public_state_path = runtime_paths["public_state_path"]
    if args.decision_mode_path == os.getenv("POLY_DECISION_MODE_PATH", "/tmp/poly_runtime_data/decision_mode.json"):
        args.decision_mode_path = runtime_paths["decision_mode_path"]
    if args.candidate_actions_path == os.getenv("POLY_CANDIDATE_ACTIONS_PATH", "/tmp/poly_runtime_data/candidate_actions.json"):
        args.candidate_actions_path = runtime_paths["candidate_actions_path"]
    if args.wallet_profiles_path == os.getenv("POLY_WALLET_PROFILES_PATH", "/tmp/poly_runtime_data/wallet_profiles.json"):
        args.wallet_profiles_path = runtime_paths["wallet_profiles_path"]
    if args.journal_path == os.getenv("POLY_JOURNAL_PATH", "/tmp/poly_runtime_data/journal.json"):
        args.journal_path = runtime_paths["journal_path"]
    if args.candidate_db_path == os.getenv("POLY_CANDIDATE_DB_PATH", "/tmp/poly_runtime_data/decision_terminal.db"):
        args.candidate_db_path = runtime_paths["candidate_db_path"]

    if args.frontend_dir:
        frontend = Path(args.frontend_dir)
    else:
        frontend = Path(__file__).resolve().parents[3] / "frontend"
    frontend = frontend.resolve()

    handler = build_handler(
        str(frontend),
        args.state_path,
        args.control_path,
        args.control_token,
        args.monitor_30m_json_path,
        args.monitor_12h_json_path,
        args.reconciliation_eod_json_path,
        args.reconciliation_eod_text_path,
        args.ledger_path,
        args.decision_mode_path,
        args.candidate_actions_path,
        args.wallet_profiles_path,
        args.journal_path,
        args.candidate_db_path,
        args.public_state_path,
    )
    server = ReusableThreadingHTTPServer((args.host, args.port), handler)
    try:
        if hasattr(handler, "sync_public_state_snapshot"):
            handler.sync_public_state_snapshot()

            def _background_public_state_sync() -> None:
                while True:
                    time.sleep(60)
                    try:
                        handler.sync_public_state_snapshot()
                    except Exception:
                        continue

            threading.Thread(target=_background_public_state_sync, daemon=True).start()
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
