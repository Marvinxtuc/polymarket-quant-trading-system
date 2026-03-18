from __future__ import annotations

import argparse
import json
import os
import time
from http.cookies import SimpleCookie
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

from polymarket_bot.reconciliation_report import (
    build_reconciliation_report_from_paths,
    write_report_files,
)


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


AUTH_COOKIE_NAME = "poly_dashboard_token"


def _safe_write_json(path: str, payload: dict) -> None:
    parent = Path(path).expanduser().parent
    if str(parent) not in {"", "."}:
        parent.mkdir(parents=True, exist_ok=True)
    tmp_path = f"{path}.tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    os.replace(tmp_path, path)


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
        "startup": {
            "ready": True,
            "warning_count": 0,
            "failure_count": 0,
            "checks": [],
        },
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
        "operator_feedback": {
            "last_action": {},
        },
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
                    "label": "全部",
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


def _strip_token_from_path(raw_path: str) -> str:
    parsed = urlsplit(raw_path)
    query = parse_qs(parsed.query, keep_blank_values=False)
    if "token" in query:
        query.pop("token", None)
    clean_query = urlencode(query, doseq=True)
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path or "/", clean_query, parsed.fragment))


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
):
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
            return """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Polymarket Dashboard Access</title>
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
    <h1>访问受保护</h1>
    <p>这个控制台需要访问令牌。你可以直接打开带 <code>?token=...</code> 的分享链接，或者在这里输入 token。</p>
    <form method="GET" action="/">
      <input name="token" type="password" placeholder="输入访问 token" autocomplete="current-password" />
      <button type="submit">打开控制台</button>
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

        def do_GET(self) -> None:
            parsed = urlsplit(self.path)
            path = parsed.path
            query = parse_qs(parsed.query, keep_blank_values=False)

            if control_token and not self._is_authorized(query):
                if path.startswith("/api/"):
                    self._json_response({"error": "unauthorized"}, HTTPStatus.UNAUTHORIZED)
                else:
                    self._html_response(self._unauthorized_page(), HTTPStatus.UNAUTHORIZED)
                return

            if control_token and _query_token(query) == control_token and not path.startswith("/api/"):
                self._redirect_with_cookie(_strip_token_from_path(self.path), query)
                return

            self._maybe_arm_cookie(query)

            if path == "/api/state":
                payload = _load_json(state_path, _empty_state())
                self._json_response(payload)
                return

            if path == "/api/control":
                if not self._is_authorized(query):
                    self._json_response({"error": "unauthorized"}, HTTPStatus.UNAUTHORIZED)
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

            if path == "/":
                self.path = "/index.html"
            return super().do_GET()

        def do_POST(self) -> None:
            parsed = urlsplit(self.path)
            path = parsed.path
            query = parse_qs(parsed.query, keep_blank_values=False)

            if path not in {"/api/control", "/api/operator"}:
                self._json_response({"error": "not found"}, HTTPStatus.NOT_FOUND)
                return
            if not self._is_authorized(query):
                self._json_response({"error": "unauthorized"}, HTTPStatus.UNAUTHORIZED)
                return
            self._maybe_arm_cookie(query)

            incoming = self._read_json_body()
            if path == "/api/operator":
                command = str(incoming.get("command", "")).strip().lower()
                if command not in {"generate_reconciliation_report", "clear_stale_pending"}:
                    self._json_response({"error": "invalid command"}, HTTPStatus.BAD_REQUEST)
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
                            "message": "stale pending cleanup request queued",
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
                            "error": str(exc),
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
                self._json_response({"error": "invalid command"}, HTTPStatus.BAD_REQUEST)
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
    parser.add_argument("--frontend-dir", default="")
    args = parser.parse_args()

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
    )
    server = ReusableThreadingHTTPServer((args.host, args.port), handler)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
