from __future__ import annotations

import argparse
import json
import os
import time
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit


class ReusableThreadingHTTPServer(ThreadingHTTPServer):
    allow_reuse_address = True
    daemon_threads = True


def _safe_write_json(path: str, payload: dict) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
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
        },
        "control": {
            "pause_opening": False,
            "reduce_only": False,
            "emergency_stop": False,
            "updated_ts": 0,
        },
        "summary": {
            "pnl_today": 0.0,
            "equity": 0.0,
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
        "positions": [],
        "orders": [],
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


def _default_control() -> dict:
    return {
        "pause_opening": False,
        "reduce_only": False,
        "emergency_stop": False,
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
    return query_token


def build_handler(frontend_dir: str, state_path: str, control_path: str, control_token: str):
    class Handler(SimpleHTTPRequestHandler):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, directory=frontend_dir, **kwargs)

        def log_message(self, format: str, *args) -> None:
            return

        def _json_response(self, payload: dict, status: HTTPStatus = HTTPStatus.OK) -> None:
            raw = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def _is_authorized(self, query: dict[str, list[str]]) -> bool:
            if not control_token:
                return True
            got = _extract_token(self.headers, query)
            return got == control_token

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

            if path == "/":
                self.path = "/index.html"
            return super().do_GET()

        def do_POST(self) -> None:
            parsed = urlsplit(self.path)
            path = parsed.path
            query = parse_qs(parsed.query, keep_blank_values=False)

            if path != "/api/control":
                self._json_response({"error": "not found"}, HTTPStatus.NOT_FOUND)
                return
            if not self._is_authorized(query):
                self._json_response({"error": "unauthorized"}, HTTPStatus.UNAUTHORIZED)
                return

            incoming = self._read_json_body()
            payload = _load_json(control_path, _default_control())
            command = str(incoming.get("command", "")).strip().lower()
            value = bool(incoming.get("value", True))

            if command in {"pause_opening", "reduce_only", "emergency_stop"}:
                payload[command] = value
            elif command == "clear_all":
                payload["pause_opening"] = False
                payload["reduce_only"] = False
                payload["emergency_stop"] = False
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
    parser.add_argument("--frontend-dir", default="")
    args = parser.parse_args()

    if args.frontend_dir:
        frontend = Path(args.frontend_dir)
    else:
        frontend = Path(__file__).resolve().parents[3] / "frontend"
    frontend = frontend.resolve()

    handler = build_handler(str(frontend), args.state_path, args.control_path, args.control_token)
    server = ReusableThreadingHTTPServer((args.host, args.port), handler)
    try:
        server.serve_forever()
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
