from __future__ import annotations

import argparse
import json
import logging
import os
import time
from datetime import datetime
from typing import Any

from polymarket_bot.config import Settings
from polymarket_bot.main import build_trader, setup_logger


def _safe_write_json(path: str, payload: dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp_path, path)


def _fmt_ago(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds}s前"
    return f"{max(1, seconds // 60)}m前"


def _wallet_sort_key(item: tuple[str, dict[str, Any]]) -> tuple[float, float, str]:
    wallet, metrics = item
    return (
        float(metrics.get("wallet_score") or 0.0),
        float(metrics.get("total_notional") or 0.0),
        wallet,
    )


def _position_exit_modes(settings: Settings) -> list[str]:
    modes: list[str] = []
    if settings.wallet_exit_follow_enabled:
        modes.append("主钱包减仓")
    if settings.resonance_exit_enabled:
        modes.append(f"{int(settings.resonance_min_wallets)}钱包共振")
    modes.append("time-exit")
    return modes


def _timeline_text(order: dict[str, Any]) -> str:
    _action, action_label = _order_action_meta(order)
    title = str(order.get("title") or "-")
    return f"{action_label} {title}"


def _exit_kind_label(kind: str, fallback: str = "") -> str:
    value = str(kind or "").strip().lower()
    if fallback:
        return fallback
    if value == "resonance_exit":
        return "共振退出"
    if value == "smart_wallet_exit":
        return "主钱包减仓"
    if value == "time_exit":
        return "时间退出"
    if value == "emergency_exit":
        return "紧急退出"
    return "退出"


def _exit_source_label(source_wallet: str) -> str:
    value = str(source_wallet or "").strip()
    if not value:
        return "未标记来源"
    if value == "system-time-exit":
        return "系统时间退出"
    if value == "system-emergency-stop":
        return "系统紧急退出"
    return value


def _order_action_meta(order: dict[str, Any]) -> tuple[str, str]:
    flow = str(order.get("flow") or "")
    side = str(order.get("side") or "").upper()
    action = str(order.get("position_action") or "").strip().lower()
    action_label = str(order.get("position_action_label") or "").strip()
    if action and action_label:
        return action, action_label
    if flow == "exit":
        if action == "trim":
            return "trim", action_label or "部分减仓"
        if action == "exit":
            return "exit", action_label or "完全退出"
        return "exit", action_label or _exit_kind_label(str(order.get("exit_kind") or ""), str(order.get("exit_label") or ""))
    if action == "add":
        return "add", action_label or "追加买入"
    if action == "entry":
        return "entry", action_label or "首次入场"
    if side == "BUY":
        return "entry", action_label or "买入"
    if side == "SELL":
        return "exit", action_label or "卖出"
    return action or side.lower(), action_label or side or "事件"


def _wallet_pool_preview(rows: list[dict[str, Any]] | None, limit: int = 4) -> list[dict[str, Any]]:
    preview: list[dict[str, Any]] = []
    for row in list(rows or [])[:limit]:
        preview.append(
            {
                "wallet": str(row.get("wallet") or ""),
                "wallet_score": float(row.get("wallet_score") or 0.0),
                "wallet_tier": str(row.get("wallet_tier") or ""),
                "topic_profiles": list(row.get("topic_profiles") or [])[:2],
            }
        )
    return preview


def _parse_iso_ts(value: Any) -> int:
    raw = str(value or "").strip()
    if not raw:
        return 0
    try:
        return int(datetime.fromisoformat(raw).timestamp())
    except ValueError:
        return 0


def _bucket_sort_key(bucket: dict[str, Any]) -> tuple[int, int, int, float, int, str]:
    return (
        int(bucket.get("filled_count") or 0),
        int(bucket.get("count") or 0),
        int(bucket.get("rejected_count") or 0),
        float(bucket.get("notional") or 0.0),
        int(bucket.get("latest_ts") or 0),
        str(bucket.get("label") or ""),
    )


def _topic_label_for_order(order: dict[str, Any]) -> str:
    return str(
        order.get("entry_topic_label")
        or order.get("topic_label")
        or "未标记题材"
    ).strip() or "未标记题材"


def _wallet_label_for_order(order: dict[str, Any]) -> str:
    source_wallet = str(order.get("source_wallet") or "").strip()
    entry_wallet = str(order.get("entry_wallet") or "").strip()
    if source_wallet:
        return source_wallet
    if entry_wallet:
        return entry_wallet
    return "unknown-source"


def _result_label_for_order(order: dict[str, Any]) -> str:
    flow = str(order.get("flow") or "")
    if flow == "exit":
        exit_result_label = str(order.get("exit_result_label") or "").strip()
        if exit_result_label:
            return exit_result_label
    status = str(order.get("status") or "").upper()
    if status == "FILLED":
        return "已成交"
    if status == "REJECTED":
        return "已拒绝"
    if status == "PENDING":
        return "待成交"
    if status == "CANCELED":
        return "已撤单"
    return status or "未知"


def _reject_reason_label(reason: str) -> str:
    value = str(reason or "").strip().lower()
    if not value:
        return "未知拒单"
    if "cooldown" in value:
        return "冷却限制"
    if "price" in value:
        return "价格带限制"
    if "daily loss" in value or "daily limit" in value:
        return "日损上限"
    if "max open positions" in value or "open positions" in value:
        return "槽位限制"
    if "liquidity" in value:
        return "流动性不足"
    if "duplicate" in value:
        return "重复信号"
    if "wallet_score" in value:
        return "钱包评分限制"
    if "budget" in value or "notional" in value:
        return "预算限制"
    if "pause" in value:
        return "暂停开仓"
    if "reduce_only" in value or "reduce-only" in value:
        return "只减仓模式"
    return value[:36]


def _hold_bucket(minutes: int) -> tuple[str, str]:
    mins = max(0, int(minutes or 0))
    if mins <= 0:
        return ("unknown", "未记录")
    if mins < 30:
        return ("lt30m", "<30m")
    if mins < 120:
        return ("30m_2h", "30m-2h")
    if mins < 720:
        return ("2h_12h", "2h-12h")
    if mins < 1440:
        return ("12h_24h", "12h-24h")
    return ("gt24h", "24h+")


def _attribution_bucket(
    *,
    key: str,
    label: str,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "key": key,
        "label": label,
        "count": 0,
        "filled_count": 0,
        "rejected_count": 0,
        "entry_count": 0,
        "exit_count": 0,
        "notional": 0.0,
        "filled_notional": 0.0,
        "latest_ts": 0,
        "hold_total_minutes": 0,
        "hold_samples": 0,
        "max_hold_minutes": 0,
        "wallet_score_total": 0.0,
        "wallet_score_samples": 0,
        "high_score_rejected_count": 0,
    }
    if extra:
        payload.update(extra)
    return payload


def _touch_attribution_bucket(bucket: dict[str, Any], order: dict[str, Any]) -> None:
    status = str(order.get("status") or "").upper()
    flow = str(order.get("flow") or "")
    notional = max(0.0, float(order.get("notional") or 0.0))
    ts = int(order.get("ts") or 0)
    hold_minutes = max(0, int(order.get("hold_minutes") or 0))
    wallet_score = float(order.get("wallet_score") or order.get("entry_wallet_score") or 0.0)

    bucket["count"] = int(bucket.get("count") or 0) + 1
    bucket["latest_ts"] = max(int(bucket.get("latest_ts") or 0), ts)
    bucket["notional"] = float(bucket.get("notional") or 0.0) + notional
    if wallet_score > 0:
        bucket["wallet_score_total"] = float(bucket.get("wallet_score_total") or 0.0) + wallet_score
        bucket["wallet_score_samples"] = int(bucket.get("wallet_score_samples") or 0) + 1
    if flow == "exit":
        bucket["exit_count"] = int(bucket.get("exit_count") or 0) + 1
    else:
        bucket["entry_count"] = int(bucket.get("entry_count") or 0) + 1
    if hold_minutes > 0:
        bucket["hold_total_minutes"] = int(bucket.get("hold_total_minutes") or 0) + hold_minutes
        bucket["hold_samples"] = int(bucket.get("hold_samples") or 0) + 1
        bucket["max_hold_minutes"] = max(int(bucket.get("max_hold_minutes") or 0), hold_minutes)
    if status == "FILLED":
        bucket["filled_count"] = int(bucket.get("filled_count") or 0) + 1
        bucket["filled_notional"] = float(bucket.get("filled_notional") or 0.0) + notional
    elif status == "REJECTED":
        bucket["rejected_count"] = int(bucket.get("rejected_count") or 0) + 1
        if wallet_score >= 65.0:
            bucket["high_score_rejected_count"] = int(bucket.get("high_score_rejected_count") or 0) + 1


def _finalize_attribution_bucket(bucket: dict[str, Any]) -> dict[str, Any]:
    count = max(0, int(bucket.get("count") or 0))
    hold_samples = max(0, int(bucket.get("hold_samples") or 0))
    wallet_score_samples = max(0, int(bucket.get("wallet_score_samples") or 0))
    bucket["reject_rate"] = 0.0 if count <= 0 else round(int(bucket.get("rejected_count") or 0) / count, 4)
    bucket["fill_rate"] = 0.0 if count <= 0 else round(int(bucket.get("filled_count") or 0) / count, 4)
    bucket["avg_hold_minutes"] = 0.0 if hold_samples <= 0 else round(int(bucket.get("hold_total_minutes") or 0) / hold_samples, 1)
    bucket["avg_wallet_score"] = 0.0 if wallet_score_samples <= 0 else round(float(bucket.get("wallet_score_total") or 0.0) / wallet_score_samples, 1)
    bucket["max_hold_minutes"] = int(bucket.get("max_hold_minutes") or 0)
    bucket.pop("hold_total_minutes", None)
    bucket.pop("hold_samples", None)
    bucket.pop("wallet_score_total", None)
    bucket.pop("wallet_score_samples", None)
    return bucket


def _windowed_orders(orders: list[dict[str, Any]], *, now: int, cutoff_seconds: int) -> list[dict[str, Any]]:
    if cutoff_seconds <= 0:
        return list(orders)
    cutoff = now - cutoff_seconds
    return [order for order in orders if int(order.get("ts") or 0) >= cutoff]


def _build_attribution_window(window_orders: list[dict[str, Any]]) -> dict[str, Any]:
    by_wallet: dict[str, dict[str, Any]] = {}
    by_topic: dict[str, dict[str, Any]] = {}
    by_exit_kind: dict[str, dict[str, Any]] = {}
    wallet_topic: dict[str, dict[str, Any]] = {}
    topic_exit: dict[str, dict[str, Any]] = {}
    source_result: dict[str, dict[str, Any]] = {}
    reject_reasons: dict[str, dict[str, Any]] = {}
    hold_buckets: dict[str, dict[str, Any]] = {}

    order_count = 0
    filled_count = 0
    rejected_count = 0
    exit_count = 0
    reject_high_score_count = 0

    for order in window_orders:
        order_count += 1
        status = str(order.get("status") or "").upper()
        flow = str(order.get("flow") or "")
        wallet = _wallet_label_for_order(order)
        topic = _topic_label_for_order(order)
        result_label = _result_label_for_order(order)
        exit_kind = str(order.get("exit_kind") or "").strip().lower() or "entry"
        exit_label = _exit_kind_label(exit_kind, str(order.get("exit_label") or "")) if flow == "exit" else "开仓"
        wallet_key = wallet or "unknown-source"
        topic_key = topic or "未标记题材"

        _touch_attribution_bucket(
            by_wallet.setdefault(wallet_key, _attribution_bucket(key=wallet_key, label=wallet_key, extra={"wallet": wallet_key})),
            order,
        )
        _touch_attribution_bucket(
            by_topic.setdefault(topic_key, _attribution_bucket(key=topic_key, label=topic_key, extra={"topic_label": topic_key})),
            order,
        )
        if flow == "exit":
            exit_count += 1
            _touch_attribution_bucket(
                by_exit_kind.setdefault(exit_kind, _attribution_bucket(key=exit_kind, label=exit_label, extra={"exit_kind": exit_kind})),
                order,
            )
            topic_exit_key = f"{topic_key}::{exit_kind}"
            _touch_attribution_bucket(
                topic_exit.setdefault(
                    topic_exit_key,
                    _attribution_bucket(
                        key=topic_exit_key,
                        label=f"{topic_key} / {exit_label}",
                        extra={"topic_label": topic_key, "exit_kind": exit_kind, "exit_label": exit_label},
                    ),
                ),
                order,
            )

        wallet_topic_key = f"{wallet_key}::{topic_key}"
        _touch_attribution_bucket(
            wallet_topic.setdefault(
                wallet_topic_key,
                _attribution_bucket(
                    key=wallet_topic_key,
                    label=f"{wallet_key} / {topic_key}",
                    extra={"wallet": wallet_key, "topic_label": topic_key},
                ),
            ),
            order,
        )
        source_result_key = f"{wallet_key}::{result_label}"
        _touch_attribution_bucket(
            source_result.setdefault(
                source_result_key,
                _attribution_bucket(
                    key=source_result_key,
                    label=f"{wallet_key} / {result_label}",
                    extra={"source_wallet": wallet_key, "result_label": result_label},
                ),
            ),
            order,
        )

        if status == "FILLED":
            filled_count += 1
        elif status == "REJECTED":
            rejected_count += 1
            if float(order.get("wallet_score") or order.get("entry_wallet_score") or 0.0) >= 65.0:
                reject_high_score_count += 1
            reason_label = _reject_reason_label(str(order.get("reason") or ""))
            _touch_attribution_bucket(
                reject_reasons.setdefault(
                    reason_label,
                    _attribution_bucket(key=reason_label, label=reason_label, extra={"reason_label": reason_label}),
                ),
                order,
            )

        hold_minutes = max(0, int(order.get("hold_minutes") or 0))
        if hold_minutes > 0:
            hold_key, hold_label = _hold_bucket(hold_minutes)
            _touch_attribution_bucket(
                hold_buckets.setdefault(
                    hold_key,
                    _attribution_bucket(key=hold_key, label=hold_label, extra={"hold_label": hold_label}),
                ),
                order,
            )

    wallet_rows = [_finalize_attribution_bucket(bucket) for bucket in by_wallet.values()]
    topic_rows = [_finalize_attribution_bucket(bucket) for bucket in by_topic.values()]
    exit_rows = [_finalize_attribution_bucket(bucket) for bucket in by_exit_kind.values()]
    wallet_topic_rows = [_finalize_attribution_bucket(bucket) for bucket in wallet_topic.values()]
    topic_exit_rows = [_finalize_attribution_bucket(bucket) for bucket in topic_exit.values()]
    source_result_rows = [_finalize_attribution_bucket(bucket) for bucket in source_result.values()]
    reject_rows = [_finalize_attribution_bucket(bucket) for bucket in reject_reasons.values()]
    hold_rows = [_finalize_attribution_bucket(bucket) for bucket in hold_buckets.values()]

    top_wallets = sorted(wallet_rows, key=lambda row: (int(row.get("filled_count") or 0), float(row.get("filled_notional") or 0.0), str(row.get("label") or "")), reverse=True)[:4]
    bottom_wallets = sorted(wallet_rows, key=lambda row: (float(row.get("reject_rate") or 0.0), int(row.get("rejected_count") or 0), str(row.get("label") or "")), reverse=True)[:4]
    top_topics = sorted(topic_rows, key=lambda row: (int(row.get("filled_count") or 0), float(row.get("filled_notional") or 0.0), str(row.get("label") or "")), reverse=True)[:4]
    bottom_topics = sorted(topic_rows, key=lambda row: (float(row.get("reject_rate") or 0.0), int(row.get("rejected_count") or 0), str(row.get("label") or "")), reverse=True)[:4]

    return {
        "summary": {
            "order_count": int(order_count),
            "filled_count": int(filled_count),
            "rejected_count": int(rejected_count),
            "exit_count": int(exit_count),
            "wallets": int(len(wallet_rows)),
            "topics": int(len(topic_rows)),
            "exit_types": int(len(exit_rows)),
            "reject_high_score_count": int(reject_high_score_count),
        },
        "by_wallet": sorted(wallet_rows, key=_bucket_sort_key, reverse=True)[:6],
        "by_topic": sorted(topic_rows, key=_bucket_sort_key, reverse=True)[:6],
        "by_exit_kind": sorted(exit_rows, key=_bucket_sort_key, reverse=True)[:6],
        "wallet_topic": sorted(wallet_topic_rows, key=_bucket_sort_key, reverse=True)[:8],
        "topic_exit": sorted(topic_exit_rows, key=_bucket_sort_key, reverse=True)[:8],
        "source_result": sorted(source_result_rows, key=_bucket_sort_key, reverse=True)[:8],
        "reject_reasons": sorted(reject_rows, key=_bucket_sort_key, reverse=True)[:6],
        "hold_buckets": sorted(hold_rows, key=lambda row: (float(row.get("avg_hold_minutes") or 0.0), int(row.get("count") or 0)), reverse=True)[:6],
        "rankings": {
            "top_wallets": top_wallets,
            "bottom_wallets": bottom_wallets,
            "top_topics": top_topics,
            "bottom_topics": bottom_topics,
        },
    }


def _build_attribution_review(orders: list[dict[str, Any]], *, now: int) -> dict[str, Any]:
    windows = [
        ("24h", "24h", 24 * 60 * 60),
        ("7d", "7d", 7 * 24 * 60 * 60),
        ("30d", "30d", 30 * 24 * 60 * 60),
        ("all", "全部", 0),
    ]
    payload_windows: dict[str, Any] = {}
    for key, label, cutoff in windows:
        window_orders = _windowed_orders(orders, now=now, cutoff_seconds=cutoff)
        payload_windows[key] = {
            "key": key,
            "label": label,
            **_build_attribution_window(window_orders),
        }
    return {
        "summary": {
            "windows": [key for key, _, _ in windows],
            "available_orders": int(len(orders)),
            "available_exits": int(sum(1 for order in orders if str(order.get("flow") or "") == "exit")),
        },
        "windows": payload_windows,
    }


def _find_exit_position_context(order: dict[str, Any], positions: list[dict[str, Any]]) -> dict[str, Any]:
    token_id = str(order.get("token_id") or "").strip()
    market_slug = str(order.get("title") or order.get("market_slug") or "").strip()
    outcome = str(order.get("outcome") or "").strip()
    matched: dict[str, Any] | None = None

    if token_id:
        matched = next((pos for pos in positions if str(pos.get("token_id") or "").strip() == token_id), None)
    if matched is None and market_slug:
        for pos in positions:
            pos_market_slug = str(pos.get("market_slug") or pos.get("title") or "").strip()
            pos_outcome = str(pos.get("outcome") or "").strip()
            if pos_market_slug != market_slug:
                continue
            if outcome and pos_outcome and pos_outcome != outcome:
                continue
            matched = pos
            break

    source = matched or {}
    return {
        "is_open": bool(matched),
        "token_id": str(source.get("token_id") or token_id),
        "market_slug": str(source.get("market_slug") or market_slug),
        "outcome": str(source.get("outcome") or outcome),
        "notional": float(source.get("notional") or 0.0),
        "quantity": float(source.get("quantity") or 0.0),
        "opened_ts": int(source.get("opened_ts") or 0),
        "entry_wallet": str(source.get("entry_wallet") or order.get("entry_wallet") or ""),
        "entry_wallet_score": float(source.get("entry_wallet_score") or order.get("entry_wallet_score") or 0.0),
        "entry_wallet_tier": str(source.get("entry_wallet_tier") or order.get("entry_wallet_tier") or ""),
        "entry_topic_label": str(source.get("entry_topic_label") or order.get("entry_topic_label") or order.get("topic_label") or ""),
        "entry_topic_summary": str(source.get("entry_topic_summary") or order.get("entry_topic_summary") or ""),
        "entry_reason": str(source.get("entry_reason") or order.get("entry_reason") or ""),
        "trace_id": str(source.get("trace_id") or order.get("trace_id") or ""),
        "origin_signal_id": str(source.get("origin_signal_id") or order.get("origin_signal_id") or ""),
        "last_signal_id": str(source.get("last_signal_id") or order.get("last_signal_id") or ""),
        "last_exit_label": str(source.get("last_exit_label") or ""),
        "last_exit_summary": str(source.get("last_exit_summary") or ""),
    }


def _same_order_context(candidate: dict[str, Any], target: dict[str, Any]) -> bool:
    target_token = str(target.get("token_id") or "").strip()
    candidate_token = str(candidate.get("token_id") or "").strip()
    if target_token and candidate_token:
        return target_token == candidate_token

    target_market = str(target.get("title") or target.get("market_slug") or "").strip()
    candidate_market = str(candidate.get("title") or candidate.get("market_slug") or "").strip()
    if not target_market or target_market != candidate_market:
        return False

    target_outcome = str(target.get("outcome") or "").strip()
    candidate_outcome = str(candidate.get("outcome") or "").strip()
    if target_outcome and candidate_outcome and target_outcome != candidate_outcome:
        return False
    return True


def _build_exit_event_chain(order: dict[str, Any], orders: list[dict[str, Any]]) -> list[dict[str, Any]]:
    related = [candidate for candidate in orders if _same_order_context(candidate, order)]
    related_sorted = sorted(related, key=lambda item: int(item.get("ts") or 0))
    chain: list[dict[str, Any]] = []
    for item in related_sorted[-8:]:
        side = str(item.get("side") or "").upper()
        flow = str(item.get("flow") or "")
        action, action_label = _order_action_meta(item)
        chain.append(
            {
                "ts": int(item.get("ts") or 0),
                "side": side,
                "flow": flow,
                "action": action,
                "status": str(item.get("status") or "").upper(),
                "action_label": action_label,
                "notional": float(item.get("notional") or 0.0),
                "hold_minutes": int(item.get("hold_minutes") or 0),
                "exit_kind": str(item.get("exit_kind") or ""),
                "exit_result": str(item.get("exit_result") or ""),
                "exit_result_label": str(item.get("exit_result_label") or ""),
                "trace_id": str(item.get("trace_id") or ""),
                "signal_id": str(item.get("signal_id") or ""),
                "reason": str(item.get("exit_summary") or item.get("reason") or ""),
            }
        )
    return chain


def _build_signal_review(
    signal_cycles: list[dict[str, Any]],
    trace_records: list[dict[str, Any]],
    positions: list[dict[str, Any]],
    orders: list[dict[str, Any]],
) -> dict[str, Any]:
    position_by_trace = {
        str(position.get("trace_id") or "").strip(): position
        for position in positions
        if str(position.get("trace_id") or "").strip()
    }
    latest_order_by_trace: dict[str, dict[str, Any]] = {}
    for order in sorted(orders, key=lambda item: int(item.get("ts") or 0), reverse=True):
        trace_id = str(order.get("trace_id") or "").strip()
        if trace_id and trace_id not in latest_order_by_trace:
            latest_order_by_trace[trace_id] = order

    cycles: list[dict[str, Any]] = []
    total_candidates = 0
    total_filled = 0
    total_rejected = 0
    total_skipped = 0
    for cycle in list(signal_cycles)[:6]:
        raw_candidates = [row for row in list(cycle.get("candidates") or []) if isinstance(row, dict)]
        candidates: list[dict[str, Any]] = []
        filled_count = 0
        rejected_count = 0
        skipped_count = 0
        for candidate in raw_candidates:
            candidate_snapshot = dict(candidate.get("candidate_snapshot") or {})
            decision_snapshot = dict(candidate.get("decision_snapshot") or {})
            order_snapshot = dict(candidate.get("order_snapshot") or {})
            topic_snapshot = dict(candidate.get("topic_snapshot") or {})
            wallet_pool_snapshot = _wallet_pool_preview(list(candidate.get("wallet_pool_snapshot") or []))
            action = str(candidate_snapshot.get("position_action") or "")
            action_label = str(candidate_snapshot.get("position_action_label") or action or candidate_snapshot.get("side") or "")
            final_status = str(candidate.get("final_status") or "candidate")
            if final_status == "filled":
                filled_count += 1
            elif "reject" in final_status:
                rejected_count += 1
            elif final_status != "candidate":
                skipped_count += 1
            candidates.append(
                {
                    "signal_id": str(candidate_snapshot.get("signal_id") or ""),
                    "trace_id": str(candidate_snapshot.get("trace_id") or ""),
                    "title": str(candidate_snapshot.get("market_slug") or ""),
                    "token_id": str(candidate_snapshot.get("token_id") or ""),
                    "outcome": str(candidate_snapshot.get("outcome") or ""),
                    "wallet": str(candidate_snapshot.get("wallet") or ""),
                    "side": str(candidate_snapshot.get("side") or ""),
                    "wallet_score": float(candidate_snapshot.get("wallet_score") or 0.0),
                    "wallet_tier": str(candidate_snapshot.get("wallet_tier") or ""),
                    "action": action,
                    "action_label": action_label,
                    "final_status": final_status,
                    "decision_reason": str(decision_snapshot.get("skip_reason") or decision_snapshot.get("risk_reason") or ""),
                    "sized_notional": float(decision_snapshot.get("sized_notional") or 0.0),
                    "final_notional": float(decision_snapshot.get("final_notional") or 0.0),
                    "budget_limited": bool(decision_snapshot.get("budget_limited", False)),
                    "duplicate": bool(decision_snapshot.get("duplicate", False)),
                    "cooldown_remaining": int(decision_snapshot.get("cooldown_remaining") or 0),
                    "add_cooldown_remaining": int(decision_snapshot.get("add_cooldown_remaining") or 0),
                    "topic_label": str(topic_snapshot.get("topic_label") or ""),
                    "topic_bias": str(topic_snapshot.get("topic_bias") or ""),
                    "topic_multiplier": float(topic_snapshot.get("topic_multiplier") or 1.0),
                    "order_status": str(order_snapshot.get("status") or ""),
                    "order_reason": str(order_snapshot.get("reason") or ""),
                    "order_notional": float(order_snapshot.get("notional") or 0.0),
                    "wallet_pool_preview": wallet_pool_snapshot,
                }
            )
        total_candidates += len(candidates)
        total_filled += filled_count
        total_rejected += rejected_count
        total_skipped += skipped_count
        cycles.append(
            {
                "cycle_id": str(cycle.get("cycle_id") or ""),
                "ts": int(cycle.get("ts") or 0),
                "wallet_count": len(list(cycle.get("wallets") or [])),
                "wallet_pool_preview": _wallet_pool_preview(list(cycle.get("wallet_pool_snapshot") or [])),
                "candidate_count": len(candidates),
                "filled_count": filled_count,
                "rejected_count": rejected_count,
                "skipped_count": skipped_count,
                "candidates": candidates,
            }
        )

    traces: list[dict[str, Any]] = []
    open_traces = 0
    closed_traces = 0
    for trace in list(trace_records)[:12]:
        trace_id = str(trace.get("trace_id") or "").strip()
        if not trace_id:
            continue
        status = str(trace.get("status") or "open")
        if status == "closed":
            closed_traces += 1
        else:
            open_traces += 1
        decision_chain: list[dict[str, Any]] = []
        for item in list(trace.get("decision_chain") or [])[-8:]:
            if not isinstance(item, dict):
                continue
            candidate_snapshot = dict(item.get("candidate_snapshot") or {})
            decision_snapshot = dict(item.get("decision_snapshot") or {})
            order_snapshot = dict(item.get("order_snapshot") or {})
            position_snapshot = dict(item.get("position_snapshot") or {})
            topic_snapshot = dict(item.get("topic_snapshot") or {})
            candidate_ts = _parse_iso_ts(candidate_snapshot.get("timestamp"))
            action = str(candidate_snapshot.get("position_action") or "")
            action_label = str(candidate_snapshot.get("position_action_label") or action or candidate_snapshot.get("side") or "")
            decision_chain.append(
                {
                    "cycle_id": str(item.get("cycle_id") or ""),
                    "signal_id": str(candidate_snapshot.get("signal_id") or ""),
                    "ts": int(candidate_ts or position_snapshot.get("last_buy_ts") or position_snapshot.get("last_trim_ts") or trace.get("last_ts") or 0),
                    "wallet": str(candidate_snapshot.get("wallet") or ""),
                    "title": str(candidate_snapshot.get("market_slug") or ""),
                    "side": str(candidate_snapshot.get("side") or ""),
                    "action": action,
                    "action_label": action_label,
                    "final_status": str(item.get("final_status") or ""),
                    "wallet_score": float(candidate_snapshot.get("wallet_score") or 0.0),
                    "wallet_tier": str(candidate_snapshot.get("wallet_tier") or ""),
                    "topic_label": str(topic_snapshot.get("topic_label") or ""),
                    "topic_bias": str(topic_snapshot.get("topic_bias") or ""),
                    "topic_multiplier": float(topic_snapshot.get("topic_multiplier") or 1.0),
                    "topic_score_summary": str(topic_snapshot.get("topic_score_summary") or ""),
                    "risk_reason": str(decision_snapshot.get("risk_reason") or ""),
                    "skip_reason": str(decision_snapshot.get("skip_reason") or ""),
                    "duplicate": bool(decision_snapshot.get("duplicate", False)),
                    "budget_limited": bool(decision_snapshot.get("budget_limited", False)),
                    "cooldown_remaining": int(decision_snapshot.get("cooldown_remaining") or 0),
                    "add_cooldown_remaining": int(decision_snapshot.get("add_cooldown_remaining") or 0),
                    "final_notional": float(decision_snapshot.get("final_notional") or 0.0),
                    "order_status": str(order_snapshot.get("status") or ""),
                    "order_reason": str(order_snapshot.get("reason") or ""),
                    "order_notional": float(order_snapshot.get("notional") or 0.0),
                    "position_is_open": bool(position_snapshot.get("is_open", False)),
                    "position_notional": float(position_snapshot.get("notional") or 0.0),
                    "position_quantity": float(position_snapshot.get("quantity") or 0.0),
                    "wallet_pool_preview": _wallet_pool_preview(list(item.get("wallet_pool_snapshot") or [])),
                }
            )
        latest_order = latest_order_by_trace.get(trace_id, {})
        latest_action, latest_action_label = _order_action_meta(latest_order) if latest_order else ("", "")
        entry_snapshot = dict(trace.get("entry_snapshot") or {})
        entry_candidate_snapshot = dict(entry_snapshot.get("candidate_snapshot") or {})
        traces.append(
            {
                "trace_id": trace_id,
                "token_id": str(trace.get("token_id") or ""),
                "market_slug": str(trace.get("market_slug") or ""),
                "outcome": str(trace.get("outcome") or ""),
                "status": status,
                "opened_ts": int(trace.get("opened_ts") or 0),
                "closed_ts": int(trace.get("closed_ts") or 0),
                "entry_signal_id": str(trace.get("entry_signal_id") or ""),
                "last_signal_id": str(trace.get("last_signal_id") or ""),
                "entry_wallet": str(entry_candidate_snapshot.get("wallet") or ""),
                "entry_wallet_score": float(entry_candidate_snapshot.get("wallet_score") or 0.0),
                "entry_wallet_tier": str(entry_candidate_snapshot.get("wallet_tier") or ""),
                "entry_topic_label": str((entry_snapshot.get("topic_snapshot") or {}).get("topic_label") or ""),
                "entry_reason": str((entry_snapshot.get("order_snapshot") or {}).get("reason") or ""),
                "latest_action": latest_action,
                "latest_action_label": latest_action_label,
                "latest_order_status": str(latest_order.get("status") or ""),
                "current_position": dict(position_by_trace.get(trace_id) or {}),
                "decision_chain": decision_chain,
            }
        )

    return {
        "summary": {
            "cycles": len(cycles),
            "candidates": total_candidates,
            "filled": total_filled,
            "rejected": total_rejected,
            "skipped": total_skipped,
            "traces": len(traces),
            "open_traces": open_traces,
            "closed_traces": closed_traces,
        },
        "cycles": cycles,
        "traces": traces,
    }


def _build_exit_review(orders: list[dict[str, Any]], positions: list[dict[str, Any]]) -> dict[str, Any]:
    exit_orders = [order for order in orders if str(order.get("flow") or "") == "exit"]
    by_kind: dict[str, dict[str, Any]] = {}
    by_topic: dict[str, dict[str, Any]] = {}
    by_source: dict[str, dict[str, Any]] = {}
    filled_count = 0
    rejected_count = 0
    total_notional = 0.0
    latest_exit_ts = 0
    total_hold_minutes = 0
    hold_samples = 0
    max_hold_minutes = 0

    def touch_bucket(
        buckets: dict[str, dict[str, Any]],
        key: str,
        label: str,
        order: dict[str, Any],
        *,
        source_wallet: str = "",
        exit_kind: str = "",
        topic_label: str = "",
    ) -> None:
        bucket = buckets.setdefault(
            key,
            {
                "key": key,
                "label": label,
                "count": 0,
                "filled_count": 0,
                "rejected_count": 0,
                "notional": 0.0,
                "latest_ts": 0,
                "source_wallet": source_wallet,
                "exit_kind": exit_kind,
                "topic_label": topic_label,
                "hold_total_minutes": 0,
                "hold_samples": 0,
                "max_hold_minutes": 0,
            },
        )
        status = str(order.get("status") or "").upper()
        notional = max(0.0, float(order.get("notional") or 0.0))
        ts = int(order.get("ts") or 0)
        hold_minutes = max(0, int(order.get("hold_minutes") or 0))
        bucket["count"] = int(bucket.get("count") or 0) + 1
        bucket["latest_ts"] = max(int(bucket.get("latest_ts") or 0), ts)
        if hold_minutes > 0:
            bucket["hold_total_minutes"] = int(bucket.get("hold_total_minutes") or 0) + hold_minutes
            bucket["hold_samples"] = int(bucket.get("hold_samples") or 0) + 1
            bucket["max_hold_minutes"] = max(int(bucket.get("max_hold_minutes") or 0), hold_minutes)
        if status == "FILLED":
            bucket["filled_count"] = int(bucket.get("filled_count") or 0) + 1
            bucket["notional"] = float(bucket.get("notional") or 0.0) + notional
        elif status == "REJECTED":
            bucket["rejected_count"] = int(bucket.get("rejected_count") or 0) + 1

    for order in exit_orders:
        status = str(order.get("status") or "").upper()
        ts = int(order.get("ts") or 0)
        notional = max(0.0, float(order.get("notional") or 0.0))
        hold_minutes = max(0, int(order.get("hold_minutes") or 0))
        latest_exit_ts = max(latest_exit_ts, ts)
        if hold_minutes > 0:
            total_hold_minutes += hold_minutes
            hold_samples += 1
            max_hold_minutes = max(max_hold_minutes, hold_minutes)
        if status == "FILLED":
            filled_count += 1
            total_notional += notional
        elif status == "REJECTED":
            rejected_count += 1

        exit_kind = str(order.get("exit_kind") or "").strip().lower()
        exit_label = _exit_kind_label(exit_kind, str(order.get("exit_label") or ""))
        topic_label = str(order.get("topic_label") or "").strip() or "未标记题材"
        source_wallet = str(order.get("source_wallet") or "").strip()
        source_label = _exit_source_label(source_wallet)
        touch_bucket(by_kind, exit_kind or "exit", exit_label, order, exit_kind=exit_kind or "exit")
        touch_bucket(by_topic, topic_label, topic_label, order, topic_label=topic_label)
        touch_bucket(by_source, source_wallet or "unknown-source", source_label, order, source_wallet=source_wallet)

    recent_exits = []
    for order in sorted(exit_orders, key=lambda item: int(item.get("ts") or 0), reverse=True)[:12]:
        exit_kind = str(order.get("exit_kind") or "").strip().lower()
        source_wallet = str(order.get("source_wallet") or "").strip()
        hold_minutes = max(0, int(order.get("hold_minutes") or 0))
        recent_exits.append(
            {
                "ts": int(order.get("ts") or 0),
                "title": str(order.get("title") or "-"),
                "token_id": str(order.get("token_id") or ""),
                "outcome": str(order.get("outcome") or ""),
                "status": str(order.get("status") or "").upper(),
                "exit_kind": exit_kind,
                "exit_label": _exit_kind_label(exit_kind, str(order.get("exit_label") or "")),
                "exit_result": str(order.get("exit_result") or ""),
                "exit_result_label": str(order.get("exit_result_label") or ""),
                "reason": str(order.get("reason") or ""),
                "exit_summary": str(order.get("exit_summary") or order.get("reason") or ""),
                "topic_label": str(order.get("topic_label") or "").strip() or "未标记题材",
                "source_wallet": source_wallet,
                "source_label": _exit_source_label(source_wallet),
                "notional": float(order.get("notional") or 0.0),
                "hold_minutes": hold_minutes,
                "trace_id": str(order.get("trace_id") or ""),
                "signal_id": str(order.get("signal_id") or ""),
                "origin_signal_id": str(order.get("origin_signal_id") or ""),
                "wallet_score": float(order.get("wallet_score") or 0.0),
                "wallet_tier": str(order.get("wallet_tier") or ""),
                "entry_wallet": str(order.get("entry_wallet") or ""),
                "entry_wallet_score": float(order.get("entry_wallet_score") or 0.0),
                "entry_wallet_tier": str(order.get("entry_wallet_tier") or ""),
                "entry_topic_label": str(order.get("entry_topic_label") or ""),
                "entry_topic_summary": str(order.get("entry_topic_summary") or ""),
                "entry_reason": str(order.get("entry_reason") or ""),
                "current_position": _find_exit_position_context(order, positions),
                "event_chain": _build_exit_event_chain(order, orders),
            }
        )

    def finalize_bucket(bucket: dict[str, Any]) -> dict[str, Any]:
        hold_count = int(bucket.get("hold_samples") or 0)
        bucket["avg_hold_minutes"] = 0.0 if hold_count <= 0 else round(int(bucket.get("hold_total_minutes") or 0) / hold_count, 1)
        bucket["max_hold_minutes"] = int(bucket.get("max_hold_minutes") or 0)
        bucket.pop("hold_total_minutes", None)
        bucket.pop("hold_samples", None)
        return bucket

    return {
        "summary": {
            "total_exit_orders": int(len(exit_orders)),
            "filled_exit_orders": int(filled_count),
            "rejected_exit_orders": int(rejected_count),
            "total_notional": float(round(total_notional, 2)),
            "latest_exit_ts": int(latest_exit_ts),
            "topics": int(len(by_topic)),
            "sources": int(len(by_source)),
            "avg_hold_minutes": 0.0 if hold_samples <= 0 else round(total_hold_minutes / hold_samples, 1),
            "max_hold_minutes": int(max_hold_minutes),
        },
        "by_kind": [finalize_bucket(bucket) for bucket in sorted(by_kind.values(), key=_bucket_sort_key, reverse=True)[:4]],
        "by_topic": [finalize_bucket(bucket) for bucket in sorted(by_topic.values(), key=_bucket_sort_key, reverse=True)[:4]],
        "by_source": [finalize_bucket(bucket) for bucket in sorted(by_source.values(), key=_bucket_sort_key, reverse=True)[:4]],
        "recent_exits": recent_exits,
    }


def _build_state(trader, settings: Settings) -> dict[str, Any]:
    now = int(time.time())
    execution_mode = "paper" if settings.dry_run else "live"
    broker_name = type(trader.broker).__name__
    wallet_metrics = trader.strategy.latest_wallet_metrics()
    scorer = getattr(trader.strategy, "scorer", None)
    history_min_closed_positions = int(getattr(scorer, "min_realized_sample", 5) or 5)
    history_strong_closed_positions = int(getattr(scorer, "strong_realized_sample", 15) or 15)
    history_strong_resolved_markets = int(getattr(scorer, "strong_resolved_sample", 10) or 10)
    wallets_sorted = sorted(
        wallet_metrics.items(),
        key=_wallet_sort_key,
        reverse=True,
    )

    wallets = []
    for wallet, metrics in wallets_sorted[:8]:
        wallets.append(
            {
                "wallet": wallet,
                "score": float(metrics.get("wallet_score") or 0.0),
                "tier": str(metrics.get("wallet_tier") or "LOW"),
                "notional": float(metrics.get("total_notional", 0.0)),
                "positions": int(metrics.get("positions", 0)),
                "unique_markets": int(metrics.get("unique_markets", 0)),
                "top_market_share": float(metrics.get("top_market_share", 0.0)),
                "score_summary": str(metrics.get("score_summary") or ""),
                "trading_enabled": bool(metrics.get("trading_enabled", False)),
                "history_available": bool(metrics.get("history_available", False)),
                "history_refresh_ts": int(metrics.get("history_refresh_ts") or 0),
                "win_rate": float(metrics.get("win_rate") or 0.0),
                "roi": float(metrics.get("roi") or 0.0),
                "resolved_win_rate": float(metrics.get("resolved_win_rate") or 0.0),
                "closed_positions": int(metrics.get("closed_positions") or 0),
                "resolved_markets": int(metrics.get("resolved_markets") or 0),
                "profit_factor": float(metrics.get("profit_factor") or 0.0),
                "activity_known": bool(metrics.get("activity_known", False)),
                "recent_activity_events": metrics.get("recent_activity_events"),
                "discovery_activity_events": int(metrics.get("discovery_activity_events") or 0),
                "discovery_priority_score": float(metrics.get("discovery_priority_score") or 0.0),
                "discovery_history_bonus": float(metrics.get("discovery_history_bonus") or 0.0),
                "discovery_topic_bonus": float(metrics.get("discovery_topic_bonus") or 0.0),
                "discovery_priority_rank": int(metrics.get("discovery_priority_rank") or 0),
                "discovery_priority_reason": str(metrics.get("discovery_priority_reason") or ""),
                "discovery_best_topic": str(metrics.get("discovery_best_topic") or ""),
                "score_components": metrics.get("score_components") or {},
                "realized_metrics": metrics.get("realized_metrics") or {},
                "recent_closed_markets": metrics.get("recent_closed_markets") or [],
                "topic_profiles": metrics.get("topic_profiles") or [],
            }
        )

    sources = []
    total_notional = sum(max(0.0, float(m.get("total_notional", 0.0))) for _, m in wallets_sorted[:8])
    for wallet, metrics in wallets_sorted[:8]:
        wallet_notional = max(0.0, float(metrics.get("total_notional", 0.0)))
        weight = 0.0 if total_notional <= 0 else wallet_notional / total_notional
        sources.append(
            {
                "name": wallet,
                "score": float(metrics.get("wallet_score") or 0.0),
                "tier": str(metrics.get("wallet_tier") or "LOW"),
                "weight": round(weight, 2),
                "status": "在线",
                "updated": _fmt_ago(int(max(0, now - trader._cached_wallets_ts if trader._cached_wallets_ts else 0))),
                "positions": int(metrics.get("positions", 0)),
                "unique_markets": int(metrics.get("unique_markets", 0)),
                "trading_enabled": bool(metrics.get("trading_enabled", False)),
                "history_available": bool(metrics.get("history_available", False)),
                "history_refresh_ts": int(metrics.get("history_refresh_ts") or 0),
                "closed_positions": int(metrics.get("closed_positions") or 0),
                "win_rate": float(metrics.get("win_rate") or 0.0),
                "roi": float(metrics.get("roi") or 0.0),
                "resolved_win_rate": float(metrics.get("resolved_win_rate") or 0.0),
                "resolved_markets": int(metrics.get("resolved_markets") or 0),
                "profit_factor": float(metrics.get("profit_factor") or 0.0),
                "activity_known": bool(metrics.get("activity_known", False)),
                "recent_activity_events": metrics.get("recent_activity_events"),
                "discovery_activity_events": int(metrics.get("discovery_activity_events") or 0),
                "discovery_priority_score": float(metrics.get("discovery_priority_score") or 0.0),
                "discovery_history_bonus": float(metrics.get("discovery_history_bonus") or 0.0),
                "discovery_topic_bonus": float(metrics.get("discovery_topic_bonus") or 0.0),
                "discovery_priority_rank": int(metrics.get("discovery_priority_rank") or 0),
                "discovery_priority_reason": str(metrics.get("discovery_priority_reason") or ""),
                "discovery_best_topic": str(metrics.get("discovery_best_topic") or ""),
                "topic_profiles": metrics.get("topic_profiles") or [],
            }
        )

    positions = []
    exit_modes = _position_exit_modes(settings)
    for pos in list(trader.positions_book.values())[:8]:
        notional = float(pos.get("notional") or 0.0)
        last_exit_label = str(pos.get("last_exit_label") or "")
        last_exit_summary = str(pos.get("last_exit_summary") or "")
        last_exit_ts = int(pos.get("last_exit_ts") or 0)
        positions.append(
            {
                "title": str(pos.get("market_slug") or pos.get("token_id") or "-"),
                "token_id": str(pos.get("token_id") or ""),
                "market_slug": str(pos.get("market_slug") or ""),
                "outcome": str(pos.get("outcome") or "YES"),
                "quantity": float(pos.get("quantity") or 0.0),
                "notional": notional,
                "book_price": float(pos.get("price") or 0.0),
                "opened_ts": int(pos.get("opened_ts") or now),
                "reason": (
                    (
                        f"wallet follower / {str(pos.get('entry_wallet_tier') or 'LOW')}"
                        f" / {str(pos.get('entry_topic_label') or '').strip()}"
                    ).rstrip(" /")
                    if pos.get("entry_wallet")
                    else "wallet follower"
                ),
                "exit_rule": " / ".join(exit_modes),
                "exit_modes": list(exit_modes),
                "last_exit_kind": str(pos.get("last_exit_kind") or ""),
                "last_exit_label": last_exit_label,
                "last_exit_summary": last_exit_summary,
                "last_exit_ts": last_exit_ts,
                "entry_wallet": str(pos.get("entry_wallet") or ""),
                "entry_wallet_score": float(pos.get("entry_wallet_score") or 0.0),
                "entry_wallet_tier": str(pos.get("entry_wallet_tier") or "LOW"),
                "entry_topic_label": str(pos.get("entry_topic_label") or ""),
                "entry_topic_bias": str(pos.get("entry_topic_bias") or "neutral"),
                "entry_topic_multiplier": float(pos.get("entry_topic_multiplier") or 1.0),
                "entry_topic_summary": str(pos.get("entry_topic_summary") or ""),
                "entry_reason": str(pos.get("entry_reason") or ""),
                "trace_id": str(pos.get("trace_id") or ""),
                "origin_signal_id": str(pos.get("origin_signal_id") or ""),
                "last_signal_id": str(pos.get("last_signal_id") or ""),
            }
        )

    orders = []
    for raw_order in list(trader.recent_orders)[:20]:
        order = dict(raw_order)
        order["flow"] = str(order.get("flow") or ("exit" if str(order.get("side") or "").upper() == "SELL" else "entry"))
        order["exit_kind"] = str(order.get("exit_kind") or "")
        order["exit_label"] = str(order.get("exit_label") or "")
        order["exit_summary"] = str(order.get("exit_summary") or "")
        order["token_id"] = str(order.get("token_id") or "")
        order["outcome"] = str(order.get("outcome") or "")
        order["signal_id"] = str(order.get("signal_id") or "")
        order["trace_id"] = str(order.get("trace_id") or "")
        order["origin_signal_id"] = str(order.get("origin_signal_id") or "")
        order["last_signal_id"] = str(order.get("last_signal_id") or "")
        order["entry_wallet"] = str(order.get("entry_wallet") or "")
        order["entry_wallet_score"] = float(order.get("entry_wallet_score") or 0.0)
        order["entry_wallet_tier"] = str(order.get("entry_wallet_tier") or "")
        order["entry_topic_label"] = str(order.get("entry_topic_label") or "")
        order["entry_topic_summary"] = str(order.get("entry_topic_summary") or "")
        order["entry_reason"] = str(order.get("entry_reason") or "")
        order["position_action"] = str(order.get("position_action") or "")
        order["position_action_label"] = str(order.get("position_action_label") or "")
        order["hold_minutes"] = int(order.get("hold_minutes") or 0)
        order["exit_result"] = str(order.get("exit_result") or "")
        order["exit_result_label"] = str(order.get("exit_result_label") or "")
        orders.append(order)
    exit_review = _build_exit_review(orders, positions)
    signal_review = _build_signal_review(
        list(getattr(trader, "recent_signal_cycles", []) or []),
        list(getattr(trader, "_trace_records", lambda: [])() or []),
        positions,
        orders,
    )
    attribution_review = _build_attribution_review(orders, now=now)
    timeline = [
        {
            "time": time.strftime("%H:%M", time.localtime(int(o.get("ts", now)))),
            "text": _timeline_text(o),
            "action": _order_action_meta(o)[0],
            "action_label": _order_action_meta(o)[1],
            "status": str(o.get("status") or "").upper(),
            "flow": str(o.get("flow") or ""),
            "trace_id": str(o.get("trace_id") or ""),
        }
        for o in orders[:8]
    ]

    alerts: list[dict[str, str]] = []
    if not trader.last_wallets:
        alerts.append({"cls": "yellow", "tag": "关注", "message": "未解析到监控钱包，检查 discovery 配置或网络"})
    elif wallets_sorted and float(wallets_sorted[0][1].get("wallet_score") or 0.0) < float(settings.min_wallet_score):
        alerts.append({"cls": "yellow", "tag": "观察", "message": "当前钱包池质量分偏低，建议复核 seed 钱包或 discovery 来源"})
    if any(str(o.get("status")) == "REJECTED" for o in orders[:5]):
        alerts.append({"cls": "red", "tag": "处理", "message": "最近存在下单失败，请检查风控与流动性"})
    recent_exit = next(
        (
            o
            for o in orders
            if str(o.get("status") or "").upper() == "FILLED" and str(o.get("flow") or "") == "exit"
        ),
        None,
    )
    if recent_exit is not None:
        exit_kind = str(recent_exit.get("exit_kind") or "")
        title = str(recent_exit.get("title") or "-")
        if exit_kind == "resonance_exit":
            alerts.append({"cls": "yellow", "tag": "共振", "message": f"{title} 出现多钱包共振减仓"})
        elif exit_kind == "smart_wallet_exit":
            alerts.append({"cls": "yellow", "tag": "跟随", "message": f"{title} 已跟随主钱包减仓"})
        elif exit_kind == "time_exit":
            alerts.append({"cls": "green", "tag": "风控", "message": f"{title} 触发时间退出"})
        elif exit_kind == "emergency_exit":
            alerts.append({"cls": "red", "tag": "紧急", "message": f"{title} 触发紧急退出"})
    if not alerts:
        alerts.append({"cls": "green", "tag": "正常", "message": "系统运行正常，数据持续更新"})

    slot_utilization_pct = 0.0
    if settings.max_open_positions > 0:
        slot_utilization_pct = min(100.0, trader.state.open_positions / settings.max_open_positions * 100.0)
    tracked_notional_usd = sum(max(0.0, float(pos.get("notional") or 0.0)) for pos in trader.positions_book.values())
    available_notional_usd = max(0.0, settings.bankroll_usd - tracked_notional_usd)
    notional_utilization_pct = 0.0 if settings.bankroll_usd <= 0 else min(100.0, tracked_notional_usd / settings.bankroll_usd * 100.0)
    base_per_trade_notional = settings.bankroll_usd * settings.risk_per_trade_pct
    theoretical_max_order_notional = base_per_trade_notional * 1.65
    daily_loss_budget_usd = settings.bankroll_usd * settings.daily_max_loss_pct
    daily_loss_used_usd = min(daily_loss_budget_usd, max(0.0, -trader.state.daily_realized_pnl))
    daily_loss_used_pct = 0.0 if daily_loss_budget_usd <= 0 else (daily_loss_used_usd / daily_loss_budget_usd * 100.0)
    daily_loss_remaining_pct = max(0.0, 100.0 - daily_loss_used_pct)
    risk_budget_openings = (
        0
        if base_per_trade_notional <= 0
        else int((daily_loss_budget_usd - daily_loss_used_usd) / base_per_trade_notional)
    )
    slot_remaining = max(0, settings.max_open_positions - trader.state.open_positions)
    est_openings = max(0, min(slot_remaining, risk_budget_openings))

    return {
        "ts": now,
        "config": {
            "dry_run": bool(settings.dry_run),
            "execution_mode": execution_mode,
            "broker_name": broker_name,
            "poll_interval_seconds": int(settings.poll_interval_seconds),
            "bankroll_usd": float(settings.bankroll_usd),
            "risk_per_trade_pct": float(settings.risk_per_trade_pct),
            "daily_max_loss_pct": float(settings.daily_max_loss_pct),
            "max_open_positions": int(settings.max_open_positions),
            "min_wallet_increase_usd": float(settings.min_wallet_increase_usd),
            "max_signals_per_cycle": int(settings.max_signals_per_cycle),
            "wallet_pool_size": int(len(trader.last_wallets)),
            "min_wallet_score": float(settings.min_wallet_score),
            "wallet_history_refresh_seconds": int(settings.wallet_history_refresh_seconds),
            "wallet_history_max_wallets": int(settings.wallet_history_max_wallets),
            "history_min_closed_positions": history_min_closed_positions,
            "history_strong_closed_positions": history_strong_closed_positions,
            "history_strong_resolved_markets": history_strong_resolved_markets,
            "wallet_score_watch_multiplier": float(settings.wallet_score_watch_multiplier),
            "wallet_score_trade_multiplier": float(settings.wallet_score_trade_multiplier),
            "wallet_score_core_multiplier": float(settings.wallet_score_core_multiplier),
            "topic_bias_enabled": bool(settings.topic_bias_enabled),
            "topic_min_samples": int(settings.topic_min_samples),
            "topic_positive_roi": float(settings.topic_positive_roi),
            "topic_positive_win_rate": float(settings.topic_positive_win_rate),
            "topic_negative_roi": float(settings.topic_negative_roi),
            "topic_negative_win_rate": float(settings.topic_negative_win_rate),
            "topic_boost_multiplier": float(settings.topic_boost_multiplier),
            "topic_penalty_multiplier": float(settings.topic_penalty_multiplier),
            "wallet_exit_follow_enabled": bool(settings.wallet_exit_follow_enabled),
            "min_wallet_decrease_usd": float(settings.min_wallet_decrease_usd),
            "resonance_exit_enabled": bool(settings.resonance_exit_enabled),
            "resonance_min_wallets": int(settings.resonance_min_wallets),
            "resonance_min_wallet_score": float(settings.resonance_min_wallet_score),
            "resonance_trim_fraction": float(settings.resonance_trim_fraction),
            "resonance_core_exit_fraction": float(settings.resonance_core_exit_fraction),
            "token_add_cooldown_seconds": int(settings.token_add_cooldown_seconds),
            "token_reentry_cooldown_seconds": int(settings.token_reentry_cooldown_seconds),
            "stale_position_minutes": int(settings.stale_position_minutes),
            "stale_position_trim_pct": float(settings.stale_position_trim_pct),
            "stale_position_trim_cooldown_seconds": int(settings.stale_position_trim_cooldown_seconds),
            "stale_position_close_notional_usd": float(settings.stale_position_close_notional_usd),
            "congested_utilization_threshold": float(settings.congested_utilization_threshold),
            "congested_stale_minutes": int(settings.congested_stale_minutes),
            "congested_trim_pct": float(settings.congested_trim_pct),
            "min_price": float(settings.min_price),
            "max_price": float(settings.max_price),
            "wallet_discovery_enabled": bool(settings.wallet_discovery_enabled),
            "wallet_discovery_mode": str(settings.wallet_discovery_mode),
            "wallet_discovery_quality_bias_enabled": bool(settings.wallet_discovery_quality_bias_enabled),
            "wallet_discovery_quality_top_n": int(settings.wallet_discovery_quality_top_n),
            "wallet_discovery_history_bonus": float(settings.wallet_discovery_history_bonus),
            "wallet_discovery_topic_bonus": float(settings.wallet_discovery_topic_bonus),
        },
        "control": {
            "pause_opening": bool(trader.control_state.pause_opening),
            "reduce_only": bool(trader.control_state.reduce_only),
            "emergency_stop": bool(trader.control_state.emergency_stop),
            "updated_ts": int(trader.control_state.updated_ts),
        },
        "summary": {
            "pnl_today": float(trader.state.daily_realized_pnl),
            "equity": float(settings.bankroll_usd + trader.state.daily_realized_pnl),
            "open_positions": int(trader.state.open_positions),
            "max_open_positions": int(settings.max_open_positions),
            "slot_utilization_pct": float(round(slot_utilization_pct, 2)),
            "exposure_pct": float(round(slot_utilization_pct, 2)),
            "signals": int(len(trader.last_signals)),
            "tracked_notional_usd": float(tracked_notional_usd),
            "available_notional_usd": float(available_notional_usd),
            "notional_utilization_pct": float(round(notional_utilization_pct, 2)),
            "base_per_trade_notional": float(base_per_trade_notional),
            "theoretical_max_order_notional": float(theoretical_max_order_notional),
            "per_trade_notional": float(base_per_trade_notional),
            "daily_loss_budget_usd": float(daily_loss_budget_usd),
            "daily_loss_used_pct": float(round(daily_loss_used_pct, 2)),
            "daily_loss_remaining_pct": float(round(daily_loss_remaining_pct, 2)),
            "slot_remaining": int(slot_remaining),
            "est_openings": int(max(0, est_openings)),
        },
        "positions": positions,
        "orders": orders,
        "wallets": wallets,
        "sources": sources,
        "alerts": alerts,
        "timeline": timeline,
        "exit_review": exit_review,
        "signal_review": signal_review,
        "attribution_review": attribution_review,
    }


def _build_wallet_score_cache(trader) -> dict[str, Any]:
    now = int(time.time())
    wallet_metrics = trader.strategy.latest_wallet_metrics()
    wallets_sorted = sorted(wallet_metrics.items(), key=_wallet_sort_key, reverse=True)
    wallets = []
    for wallet, metrics in wallets_sorted:
        wallets.append(
            {
                "wallet": wallet,
                "wallet_score": float(metrics.get("wallet_score") or 0.0),
                "wallet_tier": str(metrics.get("wallet_tier") or "LOW"),
                "total_notional": float(metrics.get("total_notional") or 0.0),
                "positions": int(metrics.get("positions") or 0),
                "unique_markets": int(metrics.get("unique_markets") or 0),
                "top_market_share": float(metrics.get("top_market_share") or 0.0),
                "recent_activity_events": metrics.get("recent_activity_events"),
                "activity_known": bool(metrics.get("activity_known", False)),
                "discovery_activity_events": int(metrics.get("discovery_activity_events") or 0),
                "discovery_priority_score": float(metrics.get("discovery_priority_score") or 0.0),
                "discovery_history_bonus": float(metrics.get("discovery_history_bonus") or 0.0),
                "discovery_topic_bonus": float(metrics.get("discovery_topic_bonus") or 0.0),
                "discovery_priority_rank": int(metrics.get("discovery_priority_rank") or 0),
                "discovery_priority_reason": str(metrics.get("discovery_priority_reason") or ""),
                "discovery_best_topic": str(metrics.get("discovery_best_topic") or ""),
                "trading_enabled": bool(metrics.get("trading_enabled", False)),
                "history_refresh_ts": int(metrics.get("history_refresh_ts") or 0),
                "closed_positions": int(metrics.get("closed_positions") or 0),
                "win_rate": float(metrics.get("win_rate") or 0.0),
                "resolved_win_rate": float(metrics.get("resolved_win_rate") or 0.0),
                "roi": float(metrics.get("roi") or 0.0),
                "profit_factor": float(metrics.get("profit_factor") or 0.0),
                "score_components": metrics.get("score_components") or {},
                "realized_metrics": metrics.get("realized_metrics") or {},
                "recent_closed_markets": metrics.get("recent_closed_markets") or [],
                "topic_profiles": metrics.get("topic_profiles") or [],
                "score_summary": str(metrics.get("score_summary") or ""),
            }
        )
    return {
        "ts": now,
        "version": 1,
        "wallets": wallets,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Polymarket runtime daemon")
    parser.add_argument("--state-path", default="/tmp/poly_runtime_data/state.json")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    args = parser.parse_args()

    settings = Settings()
    setup_logger(settings.log_level)
    log = logging.getLogger("polybot.daemon")
    trader = build_trader(settings)
    try:
        while True:
            trader.step()
            payload = _build_state(trader, settings)
            _safe_write_json(args.state_path, payload)
            try:
                _safe_write_json(settings.wallet_score_path, _build_wallet_score_cache(trader))
            except Exception as exc:
                log.warning("persist_wallet_scores failed path=%s err=%s", settings.wallet_score_path, exc)
            try:
                trader.persist_runtime_state(settings.runtime_state_path)
            except Exception as exc:
                log.warning("persist_runtime_state failed path=%s err=%s", settings.runtime_state_path, exc)
            log.info(
                "state_updated wallets=%d signals=%d orders=%d",
                len(trader.last_wallets),
                len(trader.last_signals),
                len(trader.recent_orders),
            )
            if args.once:
                return
            time.sleep(settings.poll_interval_seconds)
    finally:
        trader.data_client.close()


if __name__ == "__main__":
    main()
