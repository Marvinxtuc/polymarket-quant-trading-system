from __future__ import annotations

import os
import time
import tempfile
import unittest
from collections import deque
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from polymarket_bot.config import Settings
from polymarket_bot.daemon import (
    _build_state,
    _build_wallet_score_cache,
    _persist_cycle_outputs,
    _prepare_bootstrap_trader_state,
    _safe_write_json,
)
from polymarket_bot.notifier import Notifier


class _Strategy:
    def latest_wallet_metrics(self):
        return {
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa": {
                "wallet_score": 72.5,
                "wallet_tier": "TRADE",
                "total_notional": 1800.0,
                "positions": 4,
                "unique_markets": 5,
                "top_market_share": 0.42,
                "score_summary": "TRADE 72.5 | 4 pos | 5 mkts",
                "trading_enabled": True,
                "history_available": True,
                "history_refresh_ts": 123,
                "closed_positions": 16,
                "resolved_markets": 8,
                "win_rate": 0.6875,
                "resolved_win_rate": 0.75,
                "roi": 0.14,
                "profit_factor": 2.1,
                "realized_metrics": {
                    "closed_positions": 16,
                    "wins": 11,
                    "resolved_markets": 8,
                    "resolved_wins": 6,
                    "total_bought": 2400.0,
                    "realized_pnl": 336.0,
                    "gross_profit": 480.0,
                    "gross_loss": 144.0,
                    "win_rate": 0.6875,
                    "resolved_win_rate": 0.75,
                    "roi": 0.14,
                    "profit_factor": 2.1,
                },
                "score_components": {
                    "notional": 18.0,
                    "positions": 7.5,
                    "unique_markets": 7.5,
                    "concentration": 16.0,
                    "activity": 20.0,
                },
                "recent_closed_markets": [
                    {
                        "market_slug": "will-btc-close-above-100k",
                        "outcome": "YES",
                        "token_id": "token-a",
                        "total_bought": 120.0,
                        "realized_pnl": 24.0,
                        "roi": 0.2,
                        "timestamp": 1700000000,
                        "end_date": "2026-03-01T00:00:00Z",
                        "resolved": True,
                        "resolved_correct": True,
                        "winner_outcome": "YES",
                    }
                ],
                "topic_profiles": [
                    {
                        "key": "crypto",
                        "label": "加密",
                        "sample_count": 9,
                        "wins": 6,
                        "win_rate": 0.6667,
                        "realized_pnl": 280.0,
                        "roi": 0.17,
                        "resolved_markets": 5,
                        "resolved_wins": 4,
                        "resolved_win_rate": 0.8,
                        "sample_share": 0.56,
                    },
                    {
                        "key": "politics",
                        "label": "政治",
                        "sample_count": 7,
                        "wins": 5,
                        "win_rate": 0.7143,
                        "realized_pnl": 56.0,
                        "roi": 0.08,
                        "resolved_markets": 3,
                        "resolved_wins": 2,
                        "resolved_win_rate": 0.6667,
                        "sample_share": 0.44,
                    },
                ],
                "activity_known": True,
                "recent_activity_events": 12,
                "discovery_activity_events": 12,
                "discovery_priority_score": 12.9,
                "discovery_history_bonus": 0.75,
                "discovery_topic_bonus": 0.15,
                "discovery_priority_rank": 1,
                "discovery_priority_reason": "12 events | hist +0.75 (win 69% / roi +14%) | 加密 +0.15 (9 samples / roi +17%)",
                "discovery_best_topic": "加密",
            }
        }


class DaemonStateTests(unittest.TestCase):
    def test_build_state_exposes_wallet_score_fields(self):
        notifier_dir = tempfile.TemporaryDirectory()
        self.addCleanup(notifier_dir.cleanup)
        notifier_log = str(Path(notifier_dir.name) / "notifier.jsonl")
        with patch.dict("os.environ", {"NOTIFY_LOG_PATH": notifier_log}):
            Notifier(log_path=notifier_log).notify_local(title="Ops", body="check queue")
        pending_candidate = {
            "id": "cand-live",
            "status": "pending",
            "suggested_action": "watch",
            "market_slug": "will-btc-close-above-100k",
            "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "market_time_source": "metadata",
            "market_metadata_hit": True,
            "skip_reason": "market_not_accepting_orders",
        }
        skipped_candidate = {
            "id": "cand-skipped",
            "status": "skipped",
            "suggested_action": "watch",
            "market_slug": "xrp-above-1pt6-on-march-20",
            "wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        }

        def _list_candidates(**kwargs):
            rows = [pending_candidate, skipped_candidate]
            statuses = {str(status) for status in (kwargs.get("statuses") or [])}
            if statuses:
                rows = [row for row in rows if str(row.get("status") or "") in statuses]
            return rows[: int(kwargs.get("limit") or len(rows))]

        trader = SimpleNamespace(
            broker=SimpleNamespace(),
            strategy=_Strategy(),
            list_candidates=_list_candidates,
            candidate_store=SimpleNamespace(
                stats_summary=lambda **_: {
                    "days": 30,
                    "recent_days": 7,
                    "updated_ts": int(time.time()),
                    "candidates": {"total_candidates": 2, "by_status": [{"status": "approved", "count": 1}]},
                    "candidate_actions": {"total_actions": 1, "by_action": [{"action": "follow", "count": 1}]},
                    "journal": {"total_entries": 1, "execution_actions": 1, "watch_actions": 0, "ignore_actions": 0, "updated_ts": int(time.time())},
                    "archive": {"day_count": 1, "summary": {"candidate_count": 2, "action_count": 1, "journal_count": 1}},
                    "wallet_profiles": {"count": 1, "enabled": 1, "watched": 0},
                    "totals": {"candidate_count": 2, "action_count": 1, "journal_count": 1},
                },
                archive_summary=lambda **_: {
                    "days": 30,
                    "recent_days": 7,
                    "day_count": 1,
                    "summary": {"candidate_count": 2, "action_count": 1, "journal_count": 1, "days": 30},
                    "daily_rows": [{"day_key": "2026-03-17", "candidate_count": 2, "action_count": 1, "journal_count": 1}],
                    "recent_summary": {"candidate_count": 2, "action_count": 1, "journal_count": 1, "day_count": 1, "updated_ts": int(time.time()), "window_start_ts": 0, "days": 7},
                    "updated_ts": int(time.time()),
                },
            ),
            positions_book={
                "token-a": {
                    "token_id": "token-a",
                    "market_slug": "will-btc-close-above-100k",
                    "outcome": "YES",
                    "quantity": 80.0,
                    "price": 0.61,
                    "notional": 48.8,
                    "opened_ts": int(time.time()) - 1800,
                    "entry_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "entry_wallet_score": 72.5,
                    "entry_wallet_tier": "TRADE",
                    "entry_topic_label": "加密",
                    "entry_topic_bias": "boost",
                    "entry_topic_multiplier": 1.1,
                    "entry_topic_summary": "加密主题优势 +10%",
                    "entry_reason": "wallet follower | 加密 boost x1.10",
                    "trace_id": "trc-1",
                    "origin_signal_id": "sig-1",
                    "last_signal_id": "sig-2",
                    "last_exit_kind": "resonance_exit",
                    "last_exit_label": "共振退出",
                    "last_exit_summary": "multi-wallet exit resonance | 2 wallets trimming",
                    "last_exit_ts": int(time.time()) - 60,
                }
            },
            recent_orders=deque(
                [
                    {
                        "ts": int(time.time()) - 600,
                        "title": "will-btc-close-above-100k",
                        "token_id": "token-a",
                        "outcome": "YES",
                        "side": "BUY",
                        "status": "FILLED",
                        "retry_count": 0,
                        "latency_ms": 0,
                        "reason": "paper fill | 加密 boost x1.10",
                        "cycle_id": "cyc-1",
                        "signal_id": "sig-1",
                        "trace_id": "trc-1",
                        "flow": "entry",
                        "position_action": "entry",
                        "position_action_label": "首次入场",
                        "source_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "entry_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "entry_wallet_score": 72.5,
                        "entry_wallet_tier": "TRADE",
                        "entry_topic_label": "加密",
                        "entry_topic_summary": "加密主题优势 +10%",
                        "entry_reason": "wallet follower | 加密 boost x1.10",
                        "notional": 48.8,
                    },
                    {
                        "ts": int(time.time()) - 30,
                        "title": "will-btc-close-above-100k",
                        "token_id": "token-a",
                        "outcome": "YES",
                        "side": "SELL",
                        "status": "FILLED",
                        "retry_count": 0,
                        "latency_ms": 0,
                        "reason": "paper fill | multi-wallet exit resonance | 2 wallets trimming",
                        "cycle_id": "cyc-2",
                        "signal_id": "sig-2",
                        "trace_id": "trc-1",
                        "flow": "exit",
                        "exit_kind": "resonance_exit",
                        "exit_label": "共振退出",
                        "exit_summary": "multi-wallet exit resonance | 2 wallets trimming",
                        "position_action": "trim",
                        "position_action_label": "部分减仓",
                        "source_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "entry_wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                        "entry_wallet_score": 72.5,
                        "entry_wallet_tier": "TRADE",
                        "entry_topic_label": "加密",
                        "entry_topic_summary": "加密主题优势 +10%",
                        "entry_reason": "wallet follower | 加密 boost x1.10",
                        "topic_label": "加密",
                        "hold_minutes": 30,
                        "exit_result": "partial_trim",
                        "exit_result_label": "部分减仓",
                        "notional": 24.0,
                    },
                    {
                        "ts": int(time.time()) - 90,
                        "title": "fed-cut-before-june",
                        "token_id": "token-b",
                        "outcome": "YES",
                        "side": "SELL",
                        "status": "REJECTED",
                        "retry_count": 0,
                        "latency_ms": 0,
                        "reason": "time-exit failed: no liquidity",
                        "cycle_id": "cyc-3",
                        "signal_id": "sig-3",
                        "trace_id": "trc-2",
                        "flow": "exit",
                        "exit_kind": "time_exit",
                        "exit_label": "时间退出",
                        "exit_summary": "time-exit failed",
                        "position_action": "trim",
                        "position_action_label": "时间减仓",
                        "source_wallet": "system-time-exit",
                        "topic_label": "宏观",
                        "hold_minutes": 65,
                        "exit_result": "reject",
                        "exit_result_label": "已拒绝",
                        "notional": 12.0,
                    }
                ]
            ),
            pending_orders={
                "sig-4:BUY:token-c": {
                    "key": "sig-4:BUY:token-c",
                    "ts": int(time.time()) - 240,
                    "cycle_id": "cyc-4",
                    "signal_id": "sig-4",
                    "trace_id": "trc-3",
                    "order_id": "ord-123",
                    "broker_status": "live",
                    "market_slug": "eth-above-5k",
                    "token_id": "token-c",
                    "condition_id": "cond-c",
                    "outcome": "YES",
                    "side": "BUY",
                    "flow": "entry",
                    "wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "wallet_score": 68.0,
                    "wallet_tier": "WATCH",
                    "topic_label": "加密",
                    "requested_notional": 30.0,
                    "requested_price": 0.42,
                    "matched_notional_hint": 12.0,
                    "matched_size_hint": 28.0,
                    "matched_price_hint": 0.43,
                    "reason": "resting on book",
                    "message": "maker order live",
                    "entry_wallet": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                    "entry_wallet_score": 68.0,
                    "entry_wallet_tier": "WATCH",
                    "entry_topic_label": "加密",
                    "last_heartbeat_ts": int(time.time()) - 60,
                }
            },
            state=SimpleNamespace(daily_realized_pnl=0.0, open_positions=1),
            last_wallets=["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
            last_signals=[],
            recent_signal_cycles=deque(
                [
                    {
                        "cycle_id": "cyc-2",
                        "ts": int(time.time()) - 30,
                        "wallets": ["0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"],
                        "wallet_pool_snapshot": [
                            {
                                "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                "wallet_score": 72.5,
                                "wallet_tier": "TRADE",
                                "topic_profiles": [{"label": "加密", "sample_count": 9}],
                            }
                        ],
                        "candidates": [
                            {
                                "cycle_id": "cyc-2",
                                "candidate_snapshot": {
                                    "signal_id": "sig-2",
                                    "trace_id": "trc-1",
                                    "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                    "market_slug": "will-btc-close-above-100k",
                                    "token_id": "token-a",
                                    "outcome": "YES",
                                    "side": "SELL",
                                    "wallet_score": 72.5,
                                    "wallet_tier": "TRADE",
                                    "position_action": "trim",
                                    "position_action_label": "部分减仓",
                                    "timestamp": "2026-03-17T00:00:00+00:00",
                                },
                                "topic_snapshot": {"topic_label": "加密", "topic_bias": "boost", "topic_multiplier": 1.1},
                                "wallet_pool_snapshot": [
                                    {
                                        "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                        "wallet_score": 72.5,
                                        "wallet_tier": "TRADE",
                                        "topic_profiles": [{"label": "加密", "sample_count": 9}],
                                    }
                                ],
                                "decision_snapshot": {
                                    "risk_reason": "ok",
                                    "final_notional": 24.0,
                                    "budget_limited": False,
                                },
                                "order_snapshot": {"status": "FILLED", "reason": "paper fill | multi-wallet exit resonance | 2 wallets trimming", "notional": 24.0},
                                "position_snapshot": {"is_open": True, "notional": 24.8, "quantity": 40.0},
                                "final_status": "filled",
                            }
                        ],
                    }
                ]
            ),
            _trace_records=lambda: [
                {
                    "trace_id": "trc-1",
                    "token_id": "token-a",
                    "market_slug": "will-btc-close-above-100k",
                    "outcome": "YES",
                    "opened_ts": int(time.time()) - 600,
                    "closed_ts": 0,
                    "status": "open",
                    "entry_signal_id": "sig-1",
                    "last_signal_id": "sig-2",
                    "entry_snapshot": {
                        "candidate_snapshot": {
                            "signal_id": "sig-1",
                            "trace_id": "trc-1",
                            "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                            "market_slug": "will-btc-close-above-100k",
                            "wallet_score": 72.5,
                            "wallet_tier": "TRADE",
                            "position_action": "entry",
                            "position_action_label": "首次入场",
                            "timestamp": "2026-03-17T00:00:00+00:00",
                        },
                        "topic_snapshot": {"topic_label": "加密"},
                        "order_snapshot": {"reason": "paper fill | 加密 boost x1.10"},
                    },
                    "decision_chain": [
                        {
                            "cycle_id": "cyc-1",
                            "candidate_snapshot": {
                                "signal_id": "sig-1",
                                "trace_id": "trc-1",
                                "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                "market_slug": "will-btc-close-above-100k",
                                "side": "BUY",
                                "wallet_score": 72.5,
                                "wallet_tier": "TRADE",
                                "position_action": "entry",
                                "position_action_label": "首次入场",
                                "timestamp": "2026-03-17T00:00:00+00:00",
                            },
                            "topic_snapshot": {"topic_label": "加密", "topic_bias": "boost", "topic_multiplier": 1.1},
                            "decision_snapshot": {"risk_reason": "ok", "final_notional": 48.8},
                            "order_snapshot": {"status": "FILLED", "reason": "paper fill | 加密 boost x1.10", "notional": 48.8},
                            "position_snapshot": {"is_open": True, "notional": 48.8, "quantity": 80.0},
                            "final_status": "filled",
                        },
                        {
                            "cycle_id": "cyc-2",
                            "candidate_snapshot": {
                                "signal_id": "sig-2",
                                "trace_id": "trc-1",
                                "wallet": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                                "market_slug": "will-btc-close-above-100k",
                                "side": "SELL",
                                "wallet_score": 72.5,
                                "wallet_tier": "TRADE",
                                "position_action": "trim",
                                "position_action_label": "部分减仓",
                                "timestamp": "2026-03-17T00:30:00+00:00",
                            },
                            "topic_snapshot": {"topic_label": "加密", "topic_bias": "boost", "topic_multiplier": 1.1},
                            "decision_snapshot": {"risk_reason": "ok", "final_notional": 24.0},
                            "order_snapshot": {"status": "FILLED", "reason": "paper fill | multi-wallet exit resonance | 2 wallets trimming", "notional": 24.0},
                            "position_snapshot": {"is_open": True, "notional": 24.8, "quantity": 40.0},
                            "final_status": "filled",
                        },
                    ],
                }
            ],
            _cached_wallets_ts=time.time() - 30,
            control_state=SimpleNamespace(
                pause_opening=False,
                reduce_only=False,
                emergency_stop=False,
                clear_stale_pending_requested_ts=int(time.time()) - 10,
                updated_ts=0,
            ),
            last_operator_action={
                "name": "clear_stale_pending",
                "requested_ts": int(time.time()) - 10,
                "processed_ts": int(time.time()) - 5,
                "status": "cleared",
                "cleared_count": 1,
                "remaining_pending_orders": 0,
                "message": "cleared 1 stale pending orders",
            },
            startup_ready=False,
            startup_warning_count=1,
            startup_failure_count=1,
            startup_checks=[
                {"name": "network_smoke", "status": "FAIL", "message": "network smoke indicates geoblock/restriction"},
                {"name": "user_stream", "status": "WARN", "message": "websocket-client missing"},
            ],
            trading_mode_state=lambda: {
                "mode": "REDUCE_ONLY",
                "opening_allowed": False,
                "reason_codes": ["startup_not_ready", "reconciliation_warn"],
                "updated_ts": int(time.time()) - 15,
                "source": "runner",
                "account_state_status": "fresh",
                "reconciliation_status": "warn",
            },
            reconciliation_summary=lambda now=None: {
                "day_key": "2026-03-17",
                "status": "warn",
                "issues": ["stale_pending_orders=1"],
                "startup_ready": False,
                "internal_realized_pnl": 0.0,
                "ledger_realized_pnl": 0.0,
                "broker_closed_pnl_today": 0.0,
                "effective_daily_realized_pnl": 0.0,
                "internal_vs_ledger_diff": 0.0,
                "broker_floor_gap_vs_internal": 0.0,
                "fill_count_today": 1,
                "fill_notional_today": 48.8,
                "account_sync_count_today": 0,
                "startup_checks_count_today": 1,
                "last_fill_ts": int(time.time()) - 60,
                "last_account_sync_ts": 0,
                "last_startup_checks_ts": int(time.time()) - 30,
                "pending_orders": 1,
                "pending_entry_orders": 1,
                "pending_exit_orders": 0,
                "stale_pending_orders": 1,
                "open_positions": 1,
                "tracked_notional_usd": 48.8,
                "ledger_available": True,
                "account_snapshot_age_seconds": 0,
                "broker_reconcile_age_seconds": 0,
                "broker_event_sync_age_seconds": 180,
            },
        )
        with patch.dict(
            "os.environ",
            {
                "NOTIFY_LOG_PATH": notifier_log,
                "WALLET_EXIT_FOLLOW_ENABLED": "1",
                "POLY_WALLET_EXIT_FOLLOW_ENABLED": "true",
            },
        ):
            settings = Settings(_env_file=None)
            payload = _build_state(trader, settings)
        first_exit_order = next(order for order in payload["orders"] if order["flow"] == "exit")
        self.assertEqual(payload["wallets"][0]["score"], 72.5)
        self.assertEqual(payload["wallets"][0]["tier"], "TRADE")
        self.assertEqual(payload["sources"][0]["score"], 72.5)
        self.assertTrue(payload["wallets"][0]["trading_enabled"])
        self.assertTrue(payload["wallets"][0]["history_available"])
        self.assertAlmostEqual(payload["wallets"][0]["roi"], 0.14, places=4)
        self.assertAlmostEqual(payload["sources"][0]["resolved_win_rate"], 0.75, places=4)
        self.assertEqual(payload["sources"][0]["resolved_markets"], 8)
        self.assertEqual(payload["wallets"][0]["recent_activity_events"], 12)
        self.assertAlmostEqual(payload["wallets"][0]["discovery_priority_score"], 12.9, places=4)
        self.assertEqual(payload["wallets"][0]["discovery_best_topic"], "加密")
        self.assertEqual(payload["sources"][0]["discovery_priority_rank"], 1)
        self.assertAlmostEqual(payload["wallets"][0]["profit_factor"], 2.1, places=4)
        self.assertIn("realized_metrics", payload["wallets"][0])
        self.assertIn("score_components", payload["wallets"][0])
        self.assertEqual(payload["wallets"][0]["recent_closed_markets"][0]["winner_outcome"], "YES")
        self.assertEqual(payload["wallets"][0]["topic_profiles"][0]["label"], "加密")
        self.assertEqual(payload["positions"][0]["last_exit_kind"], "resonance_exit")
        self.assertEqual(payload["positions"][0]["last_exit_label"], "共振退出")
        self.assertIn(payload["positions"][0]["suggested_action"], {"hold", "close_partial", "close_all"})
        self.assertTrue(str(payload["positions"][0]["suggested_reason"]).strip())
        self.assertGreaterEqual(int(payload["positions"][0]["hold_minutes"]), 0)
        self.assertEqual(payload["pending_order_details"][0]["order_id"], "ord-123")
        self.assertEqual(payload["pending_order_details"][0]["broker_status"], "live")
        self.assertEqual(payload["pending_order_details"][0]["matched_notional_hint"], 12.0)
        self.assertEqual(payload["operator_feedback"]["last_action"]["name"], "clear_stale_pending")
        self.assertEqual(payload["operator_feedback"]["last_action"]["cleared_count"], 1)
        self.assertFalse(payload["startup"]["ready"])
        self.assertEqual(payload["startup"]["failure_count"], 1)
        self.assertEqual(payload["startup"]["checks"][0]["name"], "network_smoke")
        self.assertEqual(payload["reconciliation"]["status"], "warn")
        self.assertEqual(payload["reconciliation"]["fill_count_today"], 1)
        self.assertIn("notifier", payload)
        self.assertIn("recent", payload["notifier"])
        self.assertIn("channels", payload["notifier"])
        self.assertIn("delivery_stats", payload["notifier"])
        self.assertIn("telegram_configured", payload["notifier"])
        self.assertEqual(payload["notifier"]["last"]["title"], "Ops")
        self.assertEqual(first_exit_order["exit_kind"], "resonance_exit")
        self.assertIn("部分减仓 will-btc-close-above-100k", [item["text"] for item in payload["timeline"]])
        self.assertEqual(payload["alerts"][0]["tag"], "处理")
        self.assertIn("自检", [alert["tag"] for alert in payload["alerts"]])
        self.assertIn("对账", [alert["tag"] for alert in payload["alerts"]])
        self.assertIn("共振", [alert["tag"] for alert in payload["alerts"]])
        self.assertEqual(payload["exit_review"]["summary"]["total_exit_orders"], 2)
        self.assertEqual(payload["exit_review"]["summary"]["filled_exit_orders"], 1)
        self.assertEqual(payload["exit_review"]["summary"]["rejected_exit_orders"], 1)
        self.assertAlmostEqual(payload["exit_review"]["summary"]["total_notional"], 24.0, places=4)
        self.assertAlmostEqual(payload["exit_review"]["summary"]["avg_hold_minutes"], 47.5, places=4)
        self.assertEqual(payload["exit_review"]["summary"]["max_hold_minutes"], 65)
        self.assertEqual(payload["exit_review"]["by_kind"][0]["label"], "共振退出")
        self.assertEqual(payload["exit_review"]["by_topic"][0]["label"], "加密")
        self.assertAlmostEqual(payload["account"]["equity_usd"], payload["summary"]["equity"], places=4)
        self.assertAlmostEqual(payload["account"]["cash_balance_usd"], payload["summary"]["cash_balance_usd"], places=4)
        self.assertAlmostEqual(payload["available_notional_usd"], payload["summary"]["available_notional_usd"], places=4)
        self.assertEqual(payload["open_positions"], payload["summary"]["open_positions"])
        self.assertEqual(payload["mode"], payload["decision_mode"]["mode"])
        self.assertEqual(payload["trading_mode"]["mode"], "REDUCE_ONLY")
        self.assertFalse(payload["trading_mode"]["opening_allowed"])
        self.assertEqual(payload["trading_mode"]["reason_codes"], ["startup_not_ready", "reconciliation_warn"])
        self.assertFalse(payload["startup_ready"])
        self.assertEqual(payload["reconciliation_status"], payload["reconciliation"]["status"])
        self.assertEqual(
            payload["exit_review"]["by_source"][0]["source_wallet"],
            "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        )
        self.assertEqual(payload["exit_review"]["recent_exits"][0]["title"], "will-btc-close-above-100k")
        self.assertEqual(payload["exit_review"]["recent_exits"][0]["source_label"], "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        self.assertEqual(payload["exit_review"]["recent_exits"][0]["exit_result_label"], "部分减仓")
        self.assertEqual(payload["exit_review"]["recent_exits"][0]["hold_minutes"], 30)
        self.assertEqual(payload["exit_review"]["recent_exits"][0]["trace_id"], "trc-1")
        self.assertTrue(payload["exit_review"]["recent_exits"][0]["current_position"]["is_open"])
        self.assertEqual(payload["exit_review"]["recent_exits"][0]["current_position"]["entry_wallet_tier"], "TRADE")
        self.assertEqual(payload["exit_review"]["recent_exits"][0]["current_position"]["entry_reason"], "wallet follower | 加密 boost x1.10")
        self.assertEqual(payload["exit_review"]["recent_exits"][0]["current_position"]["trace_id"], "trc-1")
        self.assertEqual(len(payload["exit_review"]["recent_exits"][0]["event_chain"]), 2)
        self.assertEqual(payload["exit_review"]["recent_exits"][0]["event_chain"][0]["action"], "entry")
        self.assertEqual(payload["exit_review"]["recent_exits"][0]["event_chain"][1]["action"], "trim")
        self.assertEqual(payload["exit_review"]["recent_exits"][1]["status"], "REJECTED")
        self.assertFalse(payload["exit_review"]["recent_exits"][1]["current_position"]["is_open"])
        self.assertEqual(payload["positions"][0]["trace_id"], "trc-1")
        self.assertEqual(payload["orders"][0]["position_action"], "entry")
        self.assertEqual(payload["signal_review"]["summary"]["traces"], 1)
        self.assertEqual(payload["signal_review"]["summary"]["cycles"], 1)
        self.assertEqual(payload["signal_review"]["cycles"][0]["candidate_count"], 1)
        self.assertEqual(payload["signal_review"]["cycles"][0]["candidates"][0]["final_status"], "filled")
        self.assertEqual(payload["signal_review"]["traces"][0]["trace_id"], "trc-1")
        self.assertEqual(payload["signal_review"]["traces"][0]["decision_chain"][0]["action"], "entry")
        self.assertEqual(payload["signal_review"]["traces"][0]["decision_chain"][1]["action"], "trim")
        self.assertEqual(payload["candidates"]["summary"]["count"], 1)
        self.assertEqual(payload["candidates"]["summary"]["pending"], 1)
        self.assertEqual([item["id"] for item in payload["candidates"]["items"]], ["cand-live"])
        self.assertEqual(payload["candidates"]["observability"]["candidate_count"], 1)
        self.assertEqual(payload["candidates"]["observability"]["market_metadata"]["hits"], 1)
        self.assertEqual(payload["candidates"]["observability"]["market_metadata"]["misses"], 0)
        self.assertEqual(payload["candidates"]["observability"]["market_time_source"]["metadata"], 1)
        self.assertEqual(payload["candidates"]["observability"]["skip_reasons"]["market_not_accepting_orders"], 1)
        self.assertEqual(payload["attribution_review"]["summary"]["available_orders"], 3)
        self.assertEqual(payload["attribution_review"]["windows"]["24h"]["summary"]["order_count"], 3)
        self.assertEqual(payload["attribution_review"]["windows"]["24h"]["summary"]["rejected_count"], 1)
        self.assertEqual(payload["attribution_review"]["windows"]["24h"]["by_wallet"][0]["wallet"], "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        self.assertEqual(payload["attribution_review"]["windows"]["24h"]["by_topic"][0]["topic_label"], "加密")
        self.assertEqual(payload["attribution_review"]["windows"]["24h"]["by_exit_kind"][0]["exit_kind"], "resonance_exit")
        self.assertEqual(payload["attribution_review"]["windows"]["24h"]["wallet_topic"][0]["topic_label"], "加密")
        self.assertEqual(payload["attribution_review"]["windows"]["24h"]["topic_exit"][0]["exit_kind"], "resonance_exit")
        self.assertIn(
            "部分减仓",
            [row["result_label"] for row in payload["attribution_review"]["windows"]["24h"]["source_result"]],
        )
        self.assertEqual(payload["attribution_review"]["windows"]["24h"]["reject_reasons"][0]["reason_label"], "流动性不足")
        self.assertEqual(payload["attribution_review"]["windows"]["24h"]["hold_buckets"][0]["hold_label"], "30m-2h")
        self.assertEqual(payload["attribution_review"]["windows"]["24h"]["rankings"]["top_wallets"][0]["wallet"], "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
        self.assertEqual(payload["config"]["wallet_history_refresh_seconds"], 1800)
        self.assertEqual(payload["config"]["history_min_closed_positions"], 5)
        self.assertEqual(payload["config"]["history_strong_closed_positions"], 15)
        self.assertEqual(payload["config"]["history_strong_resolved_markets"], 10)
        self.assertTrue(payload["config"]["topic_bias_enabled"])
        self.assertAlmostEqual(payload["config"]["topic_boost_multiplier"], 1.1, places=4)
        self.assertTrue(payload["config"]["wallet_exit_follow_enabled"])
        self.assertAlmostEqual(payload["config"]["min_wallet_decrease_usd"], 200.0, places=4)
        self.assertTrue(payload["config"]["resonance_exit_enabled"])
        self.assertEqual(payload["config"]["resonance_min_wallets"], 2)
        self.assertAlmostEqual(payload["config"]["resonance_trim_fraction"], 0.35, places=4)
        self.assertTrue(payload["config"]["wallet_discovery_quality_bias_enabled"])
        self.assertEqual(payload["config"]["wallet_discovery_quality_top_n"], 16)
        self.assertEqual(payload["stats"]["totals"]["candidate_count"], 2)
        self.assertEqual(payload["stats"]["candidate_actions"]["total_actions"], 1)
        self.assertEqual(payload["archive"]["summary"]["journal_count"], 1)
        self.assertEqual(payload["archive"]["day_count"], 1)

        cache_payload = _build_wallet_score_cache(trader)
        self.assertEqual(cache_payload["wallets"][0]["wallet_score"], 72.5)
        self.assertEqual(cache_payload["wallets"][0]["wallet_tier"], "TRADE")
        self.assertEqual(cache_payload["wallets"][0]["recent_activity_events"], 12)
        self.assertTrue(cache_payload["wallets"][0]["trading_enabled"])
        self.assertEqual(cache_payload["wallets"][0]["closed_positions"], 16)
        self.assertAlmostEqual(cache_payload["wallets"][0]["win_rate"], 0.6875, places=4)
        self.assertEqual(cache_payload["wallets"][0]["discovery_priority_rank"], 1)

    def test_build_state_surfaces_recent_cycle_skip_observability_without_active_candidates(self):
        trader = SimpleNamespace(
            broker=SimpleNamespace(),
            strategy=SimpleNamespace(latest_wallet_metrics=lambda: {}),
            _cached_wallet_activity_counts={},
            _cached_wallet_selection_context={},
            _cached_wallets_ts=time.time(),
            positions_book={},
            pending_orders={},
            recent_orders=deque(maxlen=16),
            last_signals=[],
            last_wallets=["0xabc"],
            recent_signal_cycles=deque(
                [
                    {
                        "cycle_id": "cyc-skip",
                        "ts": int(time.time()) - 15,
                        "wallets": ["0xabc"],
                        "wallet_pool_snapshot": [],
                        "decision_mode": "manual",
                        "candidates": [
                            {
                                "signal_id": "sig-skip",
                                "trace_id": "trc-skip",
                                "wallet": "0xabc",
                                "market_slug": "demo-market",
                                "token_id": "token-1",
                                "outcome": "YES",
                                "side": "BUY",
                                "candidate_snapshot": {
                                    "signal_id": "sig-skip",
                                    "trace_id": "trc-skip",
                                    "wallet": "0xabc",
                                    "market_slug": "demo-market",
                                    "token_id": "token-1",
                                    "outcome": "YES",
                                    "side": "BUY",
                                },
                                "decision_snapshot": {
                                    "skip_reason": "market_not_accepting_orders",
                                    "market_time_source": "metadata",
                                    "market_metadata_hit": True,
                                },
                                "final_status": "precheck_skipped",
                            }
                        ],
                    }
                ],
                maxlen=6,
            ),
            _trace_records=lambda: [],
            state=SimpleNamespace(
                daily_realized_pnl=0.0,
                broker_closed_pnl_today=0.0,
                open_positions=0,
                tracked_notional_usd=0.0,
                pending_entry_notional_usd=0.0,
                pending_exit_notional_usd=0.0,
                pending_entry_orders=0,
                equity_usd=0.0,
                cash_balance_usd=0.0,
                positions_value_usd=0.0,
                account_snapshot_ts=0,
            ),
            control_state=SimpleNamespace(
                decision_mode="manual",
                pause_opening=False,
                reduce_only=False,
                emergency_stop=False,
                clear_stale_pending_requested_ts=0,
                updated_ts=0,
            ),
            last_operator_action={},
            startup_ready=True,
            startup_warning_count=0,
            startup_failure_count=0,
            startup_checks=[],
            trading_mode_state=lambda: {
                "mode": "NORMAL",
                "opening_allowed": True,
                "reason_codes": [],
                "updated_ts": int(time.time()),
                "source": "runner",
                "account_state_status": "fresh",
                "reconciliation_status": "ok",
                "persistence_status": "ok",
            },
            reconciliation_summary=lambda now=None: {"status": "ok", "issues": [], "day_key": "2026-03-20"},
            candidate_store=None,
            list_candidates=lambda **_: [],
            list_wallet_profiles=lambda **_: [],
            list_journal_entries=lambda **_: [],
            journal_summary=lambda **_: {},
            pending_candidate_actions=lambda **_: [],
        )

        payload = _build_state(trader, Settings(_env_file=None))

        self.assertEqual(payload["candidates"]["summary"]["count"], 0)
        self.assertEqual(payload["candidates"]["observability"]["candidate_count"], 0)
        self.assertEqual(payload["candidates"]["observability"]["recent_cycles"]["cycles"], 1)
        self.assertEqual(payload["candidates"]["observability"]["recent_cycles"]["signals"], 1)
        self.assertEqual(payload["candidates"]["observability"]["recent_cycles"]["precheck_skipped"], 1)
        self.assertEqual(
            payload["candidates"]["observability"]["recent_cycles"]["skip_reasons"]["market_not_accepting_orders"],
            1,
        )
        self.assertEqual(
            payload["candidates"]["observability"]["recent_cycles"]["market_time_source"]["metadata"],
            1,
        )

    def test_notifier_summary_reports_multiple_channels_and_delivery_stats(self):
        notifier_dir = tempfile.TemporaryDirectory()
        self.addCleanup(notifier_dir.cleanup)
        notifier_log = str(Path(notifier_dir.name) / "notifier.jsonl")

        class _Response:
            def __init__(self) -> None:
                self.status = 200

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return json.dumps({"ok": True, "result": {"message_id": 9}}).encode("utf-8")

        def _fake_which(name: str) -> str | None:
            return "/usr/bin/notify-send" if name == "notify-send" else None

        with patch.dict(
            "os.environ",
            {
                "POLY_NOTIFY_WEBHOOK_URL": "https://hooks.example.local/primary",
                "POLY_NOTIFY_WEBHOOK_URLS": "https://hooks.example.local/secondary,https://hooks.example.local/tertiary",
                "POLY_NOTIFY_TELEGRAM_BOT_TOKEN": "123456:telegram-token",
                "POLY_NOTIFY_TELEGRAM_CHAT_ID": "123456789",
                "POLY_NOTIFY_TELEGRAM_PARSE_MODE": "",
            },
        ):
            with patch("polymarket_bot.notifier.shutil.which", side_effect=_fake_which):
                with patch("polymarket_bot.notifier.subprocess.run", return_value=None):
                    with patch("polymarket_bot.notifier.request.urlopen", return_value=_Response()):
                        notifier = Notifier(log_path=notifier_log)
                        local_payload = notifier.notify_local(title="Ops", body="check queue")
                        webhook_payload = notifier.notify_webhook(
                            title="Webhook", body="fan out", extra={"severity": "warn"}
                        )
                        telegram_payload = notifier.notify_telegram(title="Telegram", body="fan out")
                        summary = notifier.summary(limit=10)

        self.assertTrue(local_payload["ok"])
        self.assertTrue(webhook_payload["ok"])
        self.assertTrue(telegram_payload["ok"])
        self.assertEqual(webhook_payload["delivery_count"], 3)
        self.assertEqual(telegram_payload["delivery_count"], 1)
        self.assertEqual(summary["delivery_stats"]["event_count"], 3)
        self.assertEqual(summary["delivery_stats"]["delivery_count"], 5)
        self.assertEqual(summary["delivery_stats"]["ok_events"], 3)
        self.assertEqual(summary["delivery_stats"]["failed_events"], 0)
        self.assertIn("local", summary["delivery_stats"]["by_channel"])
        self.assertIn("webhook", summary["delivery_stats"]["by_channel"])
        self.assertIn("telegram", summary["delivery_stats"]["by_channel"])
        self.assertEqual(summary["delivery_stats"]["by_channel"]["webhook"]["deliveries"], 3)
        self.assertEqual(summary["delivery_stats"]["by_channel"]["telegram"]["ok"], 1)
        channel_names = [row["name"] for row in summary["channels"]]
        self.assertIn("local", channel_names)
        self.assertIn("webhook", channel_names)
        self.assertIn("telegram", channel_names)
        self.assertTrue(summary["telegram_configured"])
        self.assertTrue(summary["webhook_configured"])
        self.assertEqual(summary["last"]["channel"], "telegram")
        self.assertEqual(summary["last_success"]["channel"], "telegram")
        self.assertEqual(summary["channels"][1]["target_count"], 3)

    def test_settings_expose_notification_defaults(self):
        settings = Settings(_env_file=None)
        self.assertTrue(settings.notify_local_enabled)
        self.assertEqual(settings.notify_webhook_url_list, [])
        self.assertFalse(settings.notify_telegram_enabled)
        self.assertEqual(settings.notify_telegram_api_base, "https://api.telegram.org")

    def test_build_state_notifier_summary_uses_settings_notification_config(self):
        trader = SimpleNamespace(
            broker=SimpleNamespace(),
            strategy=SimpleNamespace(latest_wallet_metrics=lambda: {}),
            positions_book={},
            pending_orders={},
            recent_orders=deque(),
            last_signals=[],
            last_wallets=["0xabc"],
            recent_signal_cycles=deque(),
            _trace_records=lambda: [],
            _cached_wallets_ts=time.time(),
            state=SimpleNamespace(open_positions=0, tracked_notional_usd=0.0, daily_realized_pnl=0.0),
            control_state=SimpleNamespace(
                pause_opening=False,
                reduce_only=False,
                emergency_stop=False,
                clear_stale_pending_requested_ts=0,
                updated_ts=0,
            ),
            last_operator_action={},
            startup_ready=True,
            startup_warning_count=0,
            startup_failure_count=0,
            startup_checks=[],
            trading_mode_state=lambda: {
                "mode": "NORMAL",
                "opening_allowed": True,
                "reason_codes": [],
                "updated_ts": int(time.time()),
                "source": "runner",
                "account_state_status": "fresh",
                "reconciliation_status": "ok",
                "persistence_status": "ok",
            },
            reconciliation_summary=lambda now=None: {"status": "ok", "issues": [], "day_key": "2026-03-20"},
            candidate_store=None,
            list_candidates=lambda **_: [],
            list_wallet_profiles=lambda **_: [],
            list_journal_entries=lambda **_: [],
            journal_summary=lambda **_: {},
            pending_candidate_actions=lambda **_: [],
        )

        with patch.dict(
            "os.environ",
            {
                "POLY_NOTIFY_WEBHOOK_URL": "",
                "POLY_NOTIFY_WEBHOOK_URLS": "",
                "POLY_NOTIFY_TELEGRAM_BOT_TOKEN": "",
                "POLY_NOTIFY_TELEGRAM_CHAT_ID": "",
            },
            clear=False,
        ):
            settings = Settings(
                _env_file=None,
                notify_local_enabled=False,
                notify_webhook_url="https://hooks.example.local/ops",
                notify_log_path=str(Path(tempfile.gettempdir()) / "daemon-state-notifier.jsonl"),
            )
            payload = _build_state(trader, settings)

        self.assertTrue(payload["notifier"]["webhook_configured"])
        channels = {row["name"]: row for row in payload["notifier"]["channels"]}
        self.assertFalse(channels["local"]["configured"])
        self.assertEqual(channels["webhook"]["target_count"], 1)

    def test_build_state_includes_persistence_snapshot(self):
        trader = SimpleNamespace(
            broker=SimpleNamespace(),
            strategy=SimpleNamespace(latest_wallet_metrics=lambda: {}),
            _cached_wallet_activity_counts={},
            _cached_wallet_selection_context={},
            _cached_wallets_ts=time.time(),
            positions_book={},
            pending_orders={},
            recent_orders=deque(maxlen=16),
            last_signals=[],
            last_wallets=[],
            recent_signal_cycles=deque(maxlen=4),
            state=SimpleNamespace(
                daily_realized_pnl=0.0,
                broker_closed_pnl_today=0.0,
                open_positions=0,
                tracked_notional_usd=0.0,
                pending_entry_notional_usd=0.0,
                pending_exit_notional_usd=0.0,
                pending_entry_orders=0,
                equity_usd=0.0,
                cash_balance_usd=0.0,
                positions_value_usd=0.0,
                account_snapshot_ts=0,
            ),
            control_state=SimpleNamespace(
                decision_mode="auto",
                pause_opening=False,
                reduce_only=False,
                emergency_stop=False,
                clear_stale_pending_requested_ts=0,
                updated_ts=0,
            ),
            last_operator_action={},
            startup_ready=True,
            startup_warning_count=0,
            startup_failure_count=0,
            startup_checks=[],
            trading_mode_state=lambda: {
                "mode": "HALTED",
                "opening_allowed": False,
                "reason_codes": ["persistence_fault"],
                "updated_ts": int(time.time()),
                "source": "runner",
                "account_state_status": "fresh",
                "reconciliation_status": "ok",
                "persistence_status": "fault",
            },
            persistence_state=lambda: {
                "status": "fault",
                "failure_count": 2,
                "last_failure": {"kind": "runtime_state_write", "path": "/tmp/runtime.json", "message": "disk full", "ts": 1},
            },
            reconciliation_summary=lambda now=None: {"status": "ok", "issues": [], "day_key": "2026-03-20"},
        )

        payload = _build_state(trader, Settings(_env_file=None))

        self.assertEqual(payload["trading_mode"]["mode"], "HALTED")
        self.assertEqual(payload["trading_mode"]["persistence_status"], "fault")
        self.assertEqual(payload["persistence"]["status"], "fault")
        self.assertEqual(payload["persistence"]["failure_count"], 2)
        self.assertEqual(payload["persistence"]["last_failure"]["kind"], "runtime_state_write")

    def test_persist_cycle_outputs_records_state_write_fault_and_raises(self):
        settings = Settings(_env_file=None)
        payload = {"summary": {"open_positions": 0}}
        errors: list[tuple[str, str, str]] = []
        trader = SimpleNamespace(
            strategy=SimpleNamespace(latest_wallet_metrics=lambda: {}),
            record_external_persistence_fault=lambda kind, path, error: errors.append((kind, path, str(error))),
            persist_runtime_state=lambda path: None,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            state_path = str(Path(tmpdir) / "state.json")
            wallet_score_path = str(Path(tmpdir) / "wallet_scores.json")
            with patch("polymarket_bot.daemon._safe_write_json", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    _persist_cycle_outputs(
                        trader,
                        settings,
                        state_path=state_path,
                        payload=payload,
                        wallet_score_path=wallet_score_path,
                        log=SimpleNamespace(warning=lambda *args, **kwargs: None),
                    )

        self.assertEqual(errors, [("daemon_state_write", state_path, "disk full")])

    def test_prepare_bootstrap_trader_state_loads_control_before_updating_trading_mode(self):
        call_order: list[str] = []
        loaded_control = SimpleNamespace(
            decision_mode="manual",
            pause_opening=True,
            reduce_only=False,
            emergency_stop=False,
            clear_stale_pending_requested_ts=0,
            updated_ts=123,
        )
        captured: dict[str, object] = {}
        trader = SimpleNamespace(
            control_state=SimpleNamespace(
                decision_mode="auto",
                pause_opening=False,
                reduce_only=False,
                emergency_stop=False,
                clear_stale_pending_requested_ts=0,
                updated_ts=0,
            ),
            _load_control_state=lambda: call_order.append("load_control") or loaded_control,
            reconciliation_summary=lambda now=None: call_order.append("reconciliation") or {"status": "ok", "day_key": "2026-03-20"},
            _update_trading_mode=lambda control, now=None, reconciliation=None: (
                call_order.append("update_mode"),
                captured.update({"control": control, "reconciliation": reconciliation, "now": now}),
            ),
            _refresh_risk_state=lambda: call_order.append("refresh_risk"),
        )

        with patch("polymarket_bot.daemon.time.time", return_value=1700000100):
            _prepare_bootstrap_trader_state(trader)

        self.assertEqual(call_order, ["load_control", "reconciliation", "update_mode", "refresh_risk"])
        self.assertIs(captured["control"], loaded_control)
        self.assertEqual(captured["reconciliation"], {"status": "ok", "day_key": "2026-03-20"})
        self.assertEqual(captured["now"], 1700000100)

    def test_safe_write_json_supports_home_and_bare_filename_paths(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            prev_cwd = os.getcwd()
            os.chdir(tmpdir)
            try:
                _safe_write_json("state.json", {"ok": True})
            finally:
                os.chdir(prev_cwd)
            self.assertTrue((Path(tmpdir) / "state.json").exists())

        with tempfile.TemporaryDirectory() as home_dir:
            with patch.dict("os.environ", {"HOME": home_dir}):
                _safe_write_json("~/daemon-state.json", {"ok": True})
            self.assertTrue((Path(home_dir) / "daemon-state.json").exists())


if __name__ == "__main__":
    unittest.main()
