from __future__ import annotations

import io
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from polymarket_bot.db import PersonalTerminalStore
from polymarket_bot.i18n import t as i18n_t
from polymarket_bot.reconciliation_report import append_ledger_entry
from polymarket_bot.state_store import StateStore
from polymarket_bot.types import Candidate
from polymarket_bot.web import build_handler


class _FakeSocket:
    def __init__(self, request_text: str):
        self._rfile = io.BytesIO(request_text.encode("utf-8"))
        self._wfile = io.BytesIO()

    def makefile(self, mode: str, *args, **kwargs):
        if "r" in mode:
            return self._rfile
        return self._wfile

    def sendall(self, data: bytes) -> None:
        self._wfile.write(data)

    def close(self) -> None:
        return None

    @property
    def raw_response(self) -> bytes:
        return self._wfile.getvalue()


class WebApiTests(unittest.TestCase):
    @staticmethod
    def _dispatch(handler_cls, path: str, *, token: str = "") -> dict:
        auth_header = f"X-Auth-Token: {token}\r\n" if token else ""
        request = f"GET {path} HTTP/1.1\r\nHost: localhost\r\n{auth_header}\r\n"
        sock = _FakeSocket(request)
        server = SimpleNamespace(server_name="localhost", server_port=8787)
        handler_cls(sock, ("127.0.0.1", 12345), server)
        raw = sock.raw_response.decode("utf-8", errors="ignore")
        body = raw.split("\r\n\r\n", 1)[1]
        return json.loads(body)

    @staticmethod
    def _dispatch_post(handler_cls, path: str, payload: dict, *, token: str = "") -> tuple[int, dict]:
        raw_body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        auth_header = ""
        if token:
            auth_header = f"X-Auth-Token: {token}\r\n"
        request = (
            f"POST {path} HTTP/1.1\r\n"
            f"Host: localhost\r\n"
            f"{auth_header}"
            f"Content-Type: application/json\r\n"
            f"Content-Length: {len(raw_body)}\r\n"
            f"\r\n"
        ).encode("utf-8") + raw_body
        sock = _FakeSocket(request.decode("utf-8", errors="ignore"))
        server = SimpleNamespace(server_name="localhost", server_port=8787)
        handler_cls(sock, ("127.0.0.1", 12345), server)
        raw = sock.raw_response.decode("utf-8", errors="ignore")
        header_text, body = raw.split("\r\n\r\n", 1)
        status_line = header_text.splitlines()[0]
        status_code = int(status_line.split(" ")[1])
        return status_code, json.loads(body)

    @staticmethod
    def _dispatch_raw(handler_cls, path: str, *, token: str = "") -> tuple[int, str, str]:
        auth_header = f"X-Auth-Token: {token}\r\n" if token else ""
        request = f"GET {path} HTTP/1.1\r\nHost: localhost\r\n{auth_header}\r\n"
        sock = _FakeSocket(request)
        server = SimpleNamespace(server_name="localhost", server_port=8787)
        handler_cls(sock, ("127.0.0.1", 12345), server)
        raw = sock.raw_response.decode("utf-8", errors="ignore")
        header_text, body = raw.split("\r\n\r\n", 1)
        status_line = header_text.splitlines()[0]
        status_code = int(status_line.split(" ")[1])
        return status_code, header_text, body

    def test_monitor_and_reconciliation_endpoints_serve_json_files(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "summary": {"open_positions": 1},
                "trading_mode": {
                    "mode": "REDUCE_ONLY",
                    "opening_allowed": False,
                    "reason_codes": ["startup_not_ready"],
                    "updated_ts": 7,
                    "source": "runner",
                },
            },
            state_file,
        )
        state_file.flush()
        state_file.close()

        public_state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "summary": {"open_positions": 0},
                "trading_mode": {
                    "mode": "NORMAL",
                    "opening_allowed": True,
                    "reason_codes": [],
                    "updated_ts": 0,
                    "source": "runner",
                },
            },
            public_state_file,
        )
        public_state_file.flush()
        public_state_file.close()

        control_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"pause_opening": False}, control_file)
        control_file.flush()
        control_file.close()

        monitor_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"report_type": "monitor_30m", "final_recommendation": "OBSERVE"}, monitor_file)
        monitor_file.flush()
        monitor_file.close()

        monitor12_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"report_type": "monitor_12h", "final_recommendation": "BLOCK"}, monitor12_file)
        monitor12_file.flush()
        monitor12_file.close()

        reconciliation_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"status": "warn", "day_key": "2026-03-17"}, reconciliation_file)
        reconciliation_file.flush()
        reconciliation_file.close()

        reconciliation_text_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        reconciliation_text_file.write("old report")
        reconciliation_text_file.flush()
        reconciliation_text_file.close()

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.write(
            json.dumps(
                {
                    "type": "fill",
                    "day_key": "2026-03-17",
                    "ts": 1710000000,
                    "side": "SELL",
                    "source": "paper",
                    "notional": 12.5,
                    "realized_pnl": 1.25,
                },
                ensure_ascii=False,
            )
            + "\n"
        )
        ledger_file.flush()
        ledger_file.close()

        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            "",
            monitor_file.name,
            monitor12_file.name,
            reconciliation_file.name,
            reconciliation_text_file.name,
            ledger_file.name,
            public_state_path=public_state_file.name,
        )

        monitor_payload = self._dispatch(handler_cls, "/api/monitor/30m")
        monitor12_payload = self._dispatch(handler_cls, "/api/monitor/12h")
        reconciliation_payload = self._dispatch(handler_cls, "/api/reconciliation/eod")
        state_payload = self._dispatch(handler_cls, "/api/state")

        updated_state = json.loads(Path(public_state_file.name).read_text(encoding="utf-8"))
        self.assertEqual(updated_state["summary"]["open_positions"], 1)
        self.assertEqual(state_payload["summary"]["open_positions"], 1)
        self.assertEqual(state_payload["trading_mode"]["mode"], "REDUCE_ONLY")
        self.assertFalse(state_payload["control"]["pause_opening"])
        self.assertIn("observability", state_payload["candidates"])
        self.assertEqual(state_payload["candidates"]["observability"]["market_metadata"]["hits"], 0)
        self.assertEqual(state_payload["candidates"]["observability"]["market_time_source"]["unknown"], 0)
        self.assertEqual(state_payload["candidates"]["observability"]["recent_cycles"]["signals"], 0)
        self.assertEqual(updated_state["trading_mode"]["mode"], "REDUCE_ONLY")

        state_file = Path(state_file.name)
        state_file.write_text(
            json.dumps(
                {
                    "summary": {"open_positions": 3},
                    "trading_mode": {
                        "mode": "HALTED",
                        "opening_allowed": False,
                        "reason_codes": ["operator_emergency_stop"],
                        "updated_ts": 8,
                        "source": "runner",
                    },
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        state_payload_again = self._dispatch(handler_cls, "/api/state")
        updated_state_again = json.loads(Path(public_state_file.name).read_text(encoding="utf-8"))

        frontend_dir.cleanup()

        self.assertEqual(monitor_payload["report_type"], "monitor_30m")
        self.assertEqual(monitor_payload["final_recommendation"], "OBSERVE")
        self.assertEqual(monitor12_payload["report_type"], "monitor_12h")
        self.assertEqual(monitor12_payload["final_recommendation"], "BLOCK")
        self.assertEqual(reconciliation_payload["status"], "warn")
        self.assertEqual(reconciliation_payload["day_key"], "2026-03-17")
        self.assertEqual(state_payload_again["summary"]["open_positions"], 3)
        self.assertEqual(updated_state_again["summary"]["open_positions"], 3)
        self.assertEqual(state_payload_again["trading_mode"]["mode"], "HALTED")
        self.assertEqual(updated_state_again["trading_mode"]["mode"], "HALTED")

    def test_api_state_includes_admission_decision_and_evidence_summary(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "summary": {"open_positions": 0},
                "trading_mode": {
                    "mode": "REDUCE_ONLY",
                    "opening_allowed": False,
                    "reason_codes": ["startup_not_ready"],
                    "updated_ts": 17,
                    "source": "runner",
                },
                "admission": {
                    "mode": "REDUCE_ONLY",
                    "opening_allowed": False,
                    "reduce_only": True,
                    "halted": False,
                    "reason_codes": ["startup_checks_fail"],
                    "evidence_summary": {
                        "reconciliation_status": "fail",
                        "account_snapshot_age_seconds": 901,
                        "broker_event_sync_age_seconds": 301,
                        "ledger_diff": 1.7,
                    },
                    "updated_ts": 17,
                },
            },
            state_file,
        )
        state_file.flush()
        state_file.close()

        control_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"pause_opening": False}, control_file)
        control_file.flush()
        control_file.close()

        monitor_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor_file)
        monitor_file.flush()
        monitor_file.close()

        monitor12_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor12_file)
        monitor12_file.flush()
        monitor12_file.close()

        reconciliation_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, reconciliation_file)
        reconciliation_file.flush()
        reconciliation_file.close()

        reconciliation_text_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        reconciliation_text_file.write("")
        reconciliation_text_file.flush()
        reconciliation_text_file.close()

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.write("")
        ledger_file.flush()
        ledger_file.close()

        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            "",
            monitor_file.name,
            monitor12_file.name,
            reconciliation_file.name,
            reconciliation_text_file.name,
            ledger_file.name,
        )

        payload = self._dispatch(handler_cls, "/api/state")
        admission = payload["admission"]
        self.assertEqual(admission["mode"], "REDUCE_ONLY")
        self.assertFalse(admission["opening_allowed"])
        self.assertIn("startup_checks_fail", admission["reason_codes"])
        evidence = admission["evidence_summary"]
        self.assertEqual(evidence["reconciliation_status"], "fail")
        self.assertEqual(evidence["account_snapshot_age_seconds"], 901)
        self.assertEqual(evidence["broker_event_sync_age_seconds"], 301)
        self.assertAlmostEqual(float(evidence["ledger_diff"]), 1.7, places=4)

    def test_api_state_preserves_time_exit_state_machine_payload(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "summary": {"open_positions": 1},
                "config": {
                    "time_exit_retry_limit": 2,
                    "time_exit_retry_cooldown_seconds": 300,
                    "time_exit_priority_volatility_step_bps": 100.0,
                },
                "positions": [
                    {
                        "token_id": "token-time-exit",
                        "market_slug": "market-time-exit",
                        "outcome": "YES",
                        "time_exit_state": {
                            "stage": "force_exit",
                            "attempt_count": 3,
                            "consecutive_failures": 2,
                            "priority": 80,
                            "priority_reason": "failures=2 | volatility=800.0bps | force_exit",
                            "market_volatility_bps": 800.0,
                            "last_attempt_ts": 111,
                            "last_failure_ts": 111,
                            "last_success_ts": 0,
                            "next_retry_ts": 0,
                            "force_exit_armed_ts": 111,
                            "last_result": "failed",
                            "last_error": "no liquidity",
                        },
                    }
                ],
                "recent_orders": [
                    {
                        "title": "market-time-exit",
                        "token_id": "token-time-exit",
                        "side": "SELL",
                        "status": "REJECTED",
                        "time_exit_stage": "force_exit",
                        "time_exit_failure_count": 2,
                        "exit_priority": 80,
                        "exit_priority_reason": "failures=2 | volatility=800.0bps | force_exit",
                        "market_volatility_bps": 800.0,
                        "force_exit_active": True,
                    }
                ],
            },
            state_file,
        )
        state_file.flush()
        state_file.close()

        public_state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"summary": {"open_positions": 0}}, public_state_file)
        public_state_file.flush()
        public_state_file.close()

        control_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"pause_opening": False}, control_file)
        control_file.flush()
        control_file.close()

        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            "",
            "",
            "",
            "",
            "",
            "",
            public_state_path=public_state_file.name,
        )

        payload = self._dispatch(handler_cls, "/api/state")

        frontend_dir.cleanup()

        self.assertEqual(payload["config"]["time_exit_retry_limit"], 2)
        self.assertEqual(payload["positions"][0]["time_exit_state"]["stage"], "force_exit")
        self.assertEqual(payload["positions"][0]["time_exit_state"]["consecutive_failures"], 2)
        self.assertTrue(bool(payload["recent_orders"][0]["force_exit_active"]))

    def test_api_state_includes_kill_switch_status_and_broker_safety_fields(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "summary": {"open_positions": 1},
                "kill_switch": {
                    "mode_requested": "reduce_only",
                    "phase": "WAITING_BROKER_TERMINAL",
                    "opening_allowed": False,
                    "reduce_only": True,
                    "halted": False,
                    "latched": True,
                    "broker_safe_confirmed": False,
                    "manual_required": False,
                    "reason_codes": ["operator_reduce_only", "kill_switch_waiting_broker_terminal"],
                    "open_buy_order_ids": ["oid-open-buy"],
                    "non_terminal_buy_order_ids": ["oid-open-buy"],
                    "cancel_requested_order_ids": ["oid-open-buy"],
                    "tracked_buy_order_ids": ["oid-open-buy"],
                    "pending_buy_order_keys": ["pending:oid-open-buy"],
                    "cancel_attempts": 2,
                    "query_error_count": 0,
                    "requested_ts": 100,
                    "last_broker_check_ts": 101,
                    "updated_ts": 102,
                },
            },
            state_file,
        )
        state_file.flush()
        state_file.close()

        control_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"pause_opening": False}, control_file)
        control_file.flush()
        control_file.close()

        monitor_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor_file)
        monitor_file.flush()
        monitor_file.close()

        monitor12_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor12_file)
        monitor12_file.flush()
        monitor12_file.close()

        reconciliation_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, reconciliation_file)
        reconciliation_file.flush()
        reconciliation_file.close()

        reconciliation_text_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        reconciliation_text_file.write("")
        reconciliation_text_file.flush()
        reconciliation_text_file.close()

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.write("")
        ledger_file.flush()
        ledger_file.close()

        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            "",
            monitor_file.name,
            monitor12_file.name,
            reconciliation_file.name,
            reconciliation_text_file.name,
            ledger_file.name,
        )

        payload = self._dispatch(handler_cls, "/api/state")
        kill_switch = payload["kill_switch"]
        self.assertEqual(kill_switch["mode_requested"], "reduce_only")
        self.assertEqual(kill_switch["phase"], "WAITING_BROKER_TERMINAL")
        self.assertFalse(kill_switch["opening_allowed"])
        self.assertFalse(kill_switch["broker_safe_confirmed"])
        self.assertFalse(kill_switch["manual_required"])
        self.assertIn("operator_reduce_only", kill_switch["reason_codes"])
        self.assertEqual(kill_switch["open_buy_order_ids"], ["oid-open-buy"])

    def test_blockbeats_endpoint_serves_cached_dashboard_payload(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, state_file)
        state_file.flush()
        state_file.close()

        control_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, control_file)
        control_file.flush()
        control_file.close()

        monitor_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor_file)
        monitor_file.flush()
        monitor_file.close()

        monitor12_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor12_file)
        monitor12_file.flush()
        monitor12_file.close()

        reconciliation_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, reconciliation_file)
        reconciliation_file.flush()
        reconciliation_file.close()

        reconciliation_text_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        reconciliation_text_file.write("")
        reconciliation_text_file.flush()
        reconciliation_text_file.close()

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.close()

        dashboard_payload = {
            "updated_ts": 1710000000,
            "status": "ok",
            "stale_after_seconds": 180,
            "prediction": {
                "source": "pro",
                "status": "ok",
                "message": "",
                "items": [
                    {
                        "id": "pred-1",
                        "title": "Prediction headline",
                        "content": "prediction detail",
                        "url": "https://example.com/prediction",
                        "create_time": 1710000000,
                        "tags": ["prediction"],
                    }
                ],
            },
            "important": {
                "source": "pro",
                "status": "ok",
                "message": "",
                "items": [
                    {
                        "id": "imp-1",
                        "title": "Important headline",
                        "content": "important detail",
                        "url": "https://example.com/important",
                        "create_time": 1710000060,
                        "tags": ["important"],
                    }
                ],
            },
            "errors": [],
        }

        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            "",
            monitor_file.name,
            monitor12_file.name,
            reconciliation_file.name,
            reconciliation_text_file.name,
            ledger_file.name,
        )

        with patch("polymarket_bot.web._fetch_blockbeats_dashboard", return_value=dashboard_payload) as mock_fetch:
            first_payload = self._dispatch(handler_cls, "/api/blockbeats")
            second_payload = self._dispatch(handler_cls, "/api/blockbeats")
            forced_payload = self._dispatch(handler_cls, "/api/blockbeats?force=true")

        frontend_dir.cleanup()

        self.assertEqual(first_payload["status"], "ok")
        self.assertEqual(len(first_payload["prediction"]["items"]), 1)
        self.assertEqual(len(second_payload["important"]["items"]), 1)
        self.assertEqual(first_payload["prediction"]["source"], "pro")
        self.assertEqual(second_payload["important"]["source"], "pro")
        self.assertEqual(forced_payload["updated_ts"], 1710000000)
        self.assertEqual(mock_fetch.call_count, 2)

    def test_static_index_is_served_with_no_store_cache_control(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text(
            '<!doctype html><title data-i18n="app.title">ok</title>',
            encoding="utf-8",
        )

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, state_file)
        state_file.flush()
        state_file.close()

        control_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, control_file)
        control_file.flush()
        control_file.close()

        monitor_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor_file)
        monitor_file.flush()
        monitor_file.close()

        monitor12_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor12_file)
        monitor12_file.flush()
        monitor12_file.close()

        reconciliation_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, reconciliation_file)
        reconciliation_file.flush()
        reconciliation_file.close()

        reconciliation_text_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        reconciliation_text_file.write("")
        reconciliation_text_file.flush()
        reconciliation_text_file.close()

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.close()

        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            "",
            monitor_file.name,
            monitor12_file.name,
            reconciliation_file.name,
            reconciliation_text_file.name,
            ledger_file.name,
        )

        status_code, headers, body = self._dispatch_raw(handler_cls, "/")
        frontend_dir.cleanup()

        self.assertEqual(status_code, 200)
        self.assertIn("Cache-Control: no-store", headers)
        self.assertIn('data-i18n="app.title"', body)

    def test_state_endpoint_overlays_latest_control_file(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "summary": {"open_positions": 0},
                "control": {"pause_opening": False, "reduce_only": False, "emergency_stop": False},
            },
            state_file,
        )
        state_file.flush()
        state_file.close()

        control_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"pause_opening": True, "reduce_only": True, "emergency_stop": False}, control_file)
        control_file.flush()
        control_file.close()

        monitor_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"report_type": "monitor_30m"}, monitor_file)
        monitor_file.flush()
        monitor_file.close()

        monitor12_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"report_type": "monitor_12h"}, monitor12_file)
        monitor12_file.flush()
        monitor12_file.close()

        reconciliation_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, reconciliation_file)
        reconciliation_file.flush()
        reconciliation_file.close()

        reconciliation_text_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        reconciliation_text_file.write("report")
        reconciliation_text_file.flush()
        reconciliation_text_file.close()

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.write("")
        ledger_file.flush()
        ledger_file.close()
        state_store_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        state_store_file.close()
        StateStore(state_store_file.name).save_control_state(
            {
                "decision_mode": "manual",
                "pause_opening": True,
                "reduce_only": True,
                "emergency_stop": False,
                "clear_stale_pending_requested_ts": 0,
                "updated_ts": int(time.time()),
            }
        )

        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            "",
            monitor_file.name,
            monitor12_file.name,
            reconciliation_file.name,
            reconciliation_text_file.name,
            ledger_file.name,
            state_store_path=state_store_file.name,
        )

        payload = self._dispatch(handler_cls, "/api/state")
        frontend_dir.cleanup()

        self.assertTrue(payload["control"]["pause_opening"])
        self.assertTrue(payload["control"]["reduce_only"])

    def test_operator_endpoint_generates_reconciliation_report(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")
        ledger_dir = tempfile.TemporaryDirectory()
        self.addCleanup(ledger_dir.cleanup)
        ledger_path = Path(ledger_dir.name) / "ledger.db"

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "startup": {"ready": True, "warning_count": 0, "failure_count": 0, "checks": []},
                "reconciliation": {
                    "day_key": "2026-03-17",
                    "status": "ok",
                    "issues": [],
                    "internal_realized_pnl": 1.25,
                    "ledger_realized_pnl": 1.25,
                },
                "summary": {"internal_pnl_today": 1.25, "broker_closed_pnl_today": 1.25},
            },
            state_file,
        )
        state_file.flush()
        state_file.close()

        control_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"pause_opening": False}, control_file)
        control_file.flush()
        control_file.close()

        monitor_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"report_type": "monitor_30m"}, monitor_file)
        monitor_file.flush()
        monitor_file.close()

        monitor12_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"report_type": "monitor_12h"}, monitor12_file)
        monitor12_file.flush()
        monitor12_file.close()

        reconciliation_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, reconciliation_file)
        reconciliation_file.flush()
        reconciliation_file.close()

        reconciliation_text_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        reconciliation_text_file.write("stale")
        reconciliation_text_file.flush()
        reconciliation_text_file.close()

        append_ledger_entry(
            str(ledger_path),
            "fill",
            {
                "day_key": "2026-03-17",
                "ts": 1710000000,
                "side": "SELL",
                "source": "paper",
                "notional": 12.5,
                "realized_pnl": 1.25,
            },
            broker="PaperBroker",
        )

        control_token = "test-control-token-1234"
        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            control_token,
            monitor_file.name,
            monitor12_file.name,
            reconciliation_file.name,
            reconciliation_text_file.name,
            str(ledger_path),
        )

        status_code, payload = self._dispatch_post(
            handler_cls,
            "/api/operator",
            {"command": "generate_reconciliation_report"},
            token=control_token,
        )

        generated_json = json.loads(Path(reconciliation_file.name).read_text(encoding="utf-8"))
        generated_text = Path(reconciliation_text_file.name).read_text(encoding="utf-8")

        frontend_dir.cleanup()

        self.assertEqual(status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["command"], "generate_reconciliation_report")
        self.assertEqual(payload["status"], "ok")
        self.assertEqual(payload["day_key"], "2026-03-17")
        self.assertEqual(generated_json["day_key"], "2026-03-17")
        self.assertEqual(generated_json["ledger_summary"]["fill_count"], 1)
        self.assertIn(i18n_t("report.reconciliation.title"), generated_text)

    def test_operator_endpoint_queues_clear_stale_pending_request(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "candidates": {
                    "observability": {
                        "recent_cycles": {
                            "cycles": 3,
                            "signals": 5,
                            "precheck_skipped": 2,
                            "market_time_source": {
                                "metadata": 4,
                                "slug_legacy": 1,
                                "unknown": 0,
                            },
                            "skip_reasons": {
                                "market_not_accepting_orders": 2,
                            },
                        }
                    }
                }
            },
            state_file,
        )
        state_file.flush()
        state_file.close()

        control_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"pause_opening": True, "reduce_only": False, "emergency_stop": False, "updated_ts": 7}, control_file)
        control_file.flush()
        control_file.close()

        monitor_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor_file)
        monitor_file.flush()
        monitor_file.close()

        monitor12_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor12_file)
        monitor12_file.flush()
        monitor12_file.close()

        reconciliation_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, reconciliation_file)
        reconciliation_file.flush()
        reconciliation_file.close()

        reconciliation_text_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        reconciliation_text_file.write("stale")
        reconciliation_text_file.flush()
        reconciliation_text_file.close()

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.close()

        control_token = "test-control-token-1234"
        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            control_token,
            monitor_file.name,
            monitor12_file.name,
            reconciliation_file.name,
            reconciliation_text_file.name,
            ledger_file.name,
        )

        status_code, payload = self._dispatch_post(
            handler_cls,
            "/api/operator",
            {"command": "clear_stale_pending"},
            token=control_token,
        )

        control_payload = json.loads(Path(control_file.name).read_text(encoding="utf-8"))
        frontend_dir.cleanup()

        self.assertEqual(status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["command"], "clear_stale_pending")
        self.assertGreater(int(payload["requested_ts"]), 0)
        self.assertFalse(control_payload["pause_opening"])
        self.assertEqual(int(control_payload["clear_stale_pending_requested_ts"]), int(payload["requested_ts"]))

    def test_candidate_action_and_mode_endpoints_use_runtime_store(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "candidates": {
                    "summary": {"count": 1},
                    "items": [],
                    "observability": {
                        "recent_cycles": {
                            "cycles": 3,
                            "signals": 5,
                            "precheck_skipped": 2,
                            "market_time_source": {
                                "metadata": 4,
                                "slug_legacy": 1,
                                "unknown": 0,
                            },
                            "skip_reasons": {
                                "market_not_accepting_orders": 2,
                            },
                        }
                    },
                },
                "trading_mode": {
                    "mode": "REDUCE_ONLY",
                    "opening_allowed": False,
                    "reason_codes": ["startup_not_ready"],
                    "updated_ts": 1,
                    "source": "runner",
                },
            },
            state_file,
        )
        state_file.flush()
        state_file.close()

        control_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"decision_mode": "manual", "updated_ts": 0}, control_file)
        control_file.flush()
        control_file.close()

        monitor_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor_file)
        monitor_file.flush()
        monitor_file.close()

        monitor12_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor12_file)
        monitor12_file.flush()
        monitor12_file.close()

        reconciliation_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, reconciliation_file)
        reconciliation_file.flush()
        reconciliation_file.close()

        reconciliation_text_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        reconciliation_text_file.write("")
        reconciliation_text_file.flush()
        reconciliation_text_file.close()

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.close()

        db_path = str(Path(tempfile.mkdtemp()) / "terminal.db")
        now_ts = int(time.time())
        store = PersonalTerminalStore(db_path)
        self.addCleanup(store.close)
        store.upsert_candidate(
            Candidate(
                id="cand-1",
                signal_id="sig-1",
                trace_id="trc-1",
                wallet="0xabc",
                market_slug="demo-market",
                token_id="token-1",
                outcome="YES",
                side="BUY",
                confidence=0.8,
                score=77.0,
                market_time_source="metadata",
                market_metadata_hit=True,
                created_ts=now_ts,
                expires_ts=now_ts + 3600,
                updated_ts=now_ts,
                signal_snapshot={"signal_id": "sig-1"},
            )
        )
        store.upsert_candidate(
            Candidate(
                id="cand-2",
                signal_id="sig-2",
                trace_id="trc-2",
                wallet="0xdef",
                market_slug="other-market",
                token_id="token-2",
                outcome="NO",
                side="BUY",
                confidence=0.5,
                score=51.0,
                status="watched",
                market_time_source="slug_legacy",
                market_metadata_hit=False,
                skip_reason="spread_too_wide",
                created_ts=now_ts + 1,
                expires_ts=now_ts + 3600,
                updated_ts=now_ts + 1,
                signal_snapshot={"signal_id": "sig-2"},
            )
        )

        control_token = "test-control-token-1234"
        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            control_token,
            monitor_file.name,
            monitor12_file.name,
            reconciliation_file.name,
            reconciliation_text_file.name,
            ledger_file.name,
            candidate_db_path=db_path,
        )

        status_code, payload = self._dispatch_post(
            handler_cls,
            "/api/candidate/action",
            {"candidate_id": "cand-1", "action": "follow", "note": "looks good"},
            token=control_token,
        )
        replay_status, replay_payload = self._dispatch_post(
            handler_cls,
            "/api/candidate/action",
            {"candidate_id": "cand-1", "action": "follow", "note": "looks good"},
            token=control_token,
        )
        mode_status, mode_payload = self._dispatch_post(
            handler_cls,
            "/api/mode",
            {"mode": "semi_auto"},
            token=control_token,
        )
        candidates_payload = self._dispatch(
            handler_cls,
            "/api/candidates?status=approved&limit=1&wallet=0xabc&market_slug=demo-market",
            token=control_token,
        )
        journal_payload = self._dispatch(handler_cls, "/api/journal?limit=1", token=control_token)
        stats_payload = self._dispatch(handler_cls, "/api/stats?days=30&recent_days=1", token=control_token)
        archive_payload = self._dispatch(handler_cls, "/api/archive?days=30&recent_days=1", token=control_token)
        export_json_payload = self._dispatch(handler_cls, "/api/export?scope=actions&format=json&days=30&limit=2", token=control_token)
        export_csv_status, export_csv_headers, export_csv_body = self._dispatch_raw(
            handler_cls,
            "/api/export?scope=journal&format=csv&days=30&limit=2",
            token=control_token,
        )
        profiles_status, profiles_payload = self._dispatch_post(
            handler_cls,
            "/api/wallet-profiles/update",
            {"wallet": "0xabc", "tag": "CORE", "invalid_field": "boom"},
            token=control_token,
        )
        invalid_score_status, invalid_score_payload = self._dispatch_post(
            handler_cls,
            "/api/wallet-profiles/update",
            {"wallet": "0xabc", "trust_score": "oops"},
            token=control_token,
        )

        updated_candidate = store.get_candidate("cand-1")
        control_after = json.loads(Path(control_file.name).read_text(encoding="utf-8"))
        action_notes = store.list_journal_entries(limit=5)
        frontend_dir.cleanup()

        self.assertEqual(status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["candidate"]["status"], "approved")
        self.assertFalse(payload["idempotent_replay"])
        self.assertEqual(replay_status, 200)
        self.assertTrue(replay_payload["idempotent_replay"])
        self.assertEqual(updated_candidate["selected_action"], "follow")
        self.assertEqual(mode_status, 200)
        self.assertTrue(mode_payload["ok"])
        self.assertEqual(mode_payload["decision_mode"]["mode"], "semi_auto")
        self.assertNotIn("REDUCE_ONLY", json.dumps(mode_payload, ensure_ascii=False))
        self.assertEqual(control_after["decision_mode"], "semi_auto")
        self.assertEqual(candidates_payload["summary"]["count"], 1)
        self.assertEqual(candidates_payload["items"][0]["id"], "cand-1")
        self.assertEqual(candidates_payload["observability"]["candidate_count"], 1)
        self.assertEqual(candidates_payload["observability"]["market_metadata"]["hits"], 1)
        self.assertEqual(candidates_payload["observability"]["market_time_source"]["metadata"], 1)
        self.assertEqual(candidates_payload["observability"]["skip_reasons"], {})
        self.assertEqual(candidates_payload["observability"]["recent_cycles"]["cycles"], 3)
        self.assertEqual(candidates_payload["observability"]["recent_cycles"]["signals"], 5)
        self.assertEqual(candidates_payload["observability"]["recent_cycles"]["precheck_skipped"], 2)
        self.assertEqual(
            candidates_payload["observability"]["recent_cycles"]["skip_reasons"]["market_not_accepting_orders"],
            2,
        )
        self.assertEqual(candidates_payload["filters"]["wallet"], "0xabc")
        self.assertEqual(journal_payload["limit"], 1)
        self.assertEqual(len(journal_payload["notes"]), 1)
        self.assertEqual(stats_payload["totals"]["candidate_count"], 2)
        self.assertEqual(stats_payload["candidate_actions"]["total_actions"], 1)
        self.assertEqual(archive_payload["summary"]["candidate_count"], 2)
        self.assertEqual(archive_payload["summary"]["journal_count"], 1)
        self.assertEqual(export_json_payload["scope"], "actions")
        self.assertEqual(export_json_payload["summary"]["count"], 1)
        self.assertEqual(export_csv_status, 200)
        self.assertIn("Content-Type: text/csv", export_csv_headers)
        self.assertIn("candidate_id", export_csv_body)
        self.assertEqual(action_notes[0]["candidate_id"], "cand-1")
        self.assertEqual(len(action_notes), 1)
        self.assertEqual(profiles_status, 400)
        self.assertEqual(profiles_payload["error_code"], "walletProfileFieldsUnsupported")
        self.assertEqual(profiles_payload["error"], i18n_t("web.api.walletProfileFieldsUnsupported"))
        self.assertEqual(profiles_payload["fields"], ["invalid_field"])
        self.assertEqual(invalid_score_status, 400)
        self.assertEqual(invalid_score_payload["error_code"], "validationFailed")
        self.assertEqual(invalid_score_payload["error"], i18n_t("web.api.validationFailed"))
        self.assertEqual(
            invalid_score_payload["error_detail"],
            i18n_t("web.api.fieldMustBeNumber", {"field": i18n_t("web.field.trust_score")}),
        )

    def test_candidates_endpoint_supports_search_sort_and_detail_chain(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        now_ts = int(time.time())

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "candidates": {
                    "summary": {"count": 2},
                    "items": [
                        {
                            "id": "cand-1",
                            "signal_id": "sig-1",
                            "trace_id": "trc-1",
                            "wallet": "0xabc",
                            "market_slug": "demo-market",
                            "token_id": "token-1",
                            "outcome": "YES",
                            "side": "BUY",
                            "score": 77.0,
                            "status": "approved",
                            "selected_action": "follow",
                            "review_action": "follow",
                            "review_status": "approved",
                            "review_note": "looks good",
                            "created_ts": now_ts,
                            "updated_ts": now_ts,
                        },
                        {
                            "id": "cand-2",
                            "signal_id": "sig-2",
                            "trace_id": "trc-2",
                            "wallet": "0xdef",
                            "market_slug": "other-market",
                            "token_id": "token-2",
                            "outcome": "NO",
                            "side": "BUY",
                            "score": 51.0,
                            "status": "watched",
                            "selected_action": "watch",
                            "review_action": "watch",
                            "review_status": "watched",
                            "review_note": "need more confirmation",
                            "created_ts": now_ts - 1,
                            "updated_ts": now_ts - 1,
                        },
                    ],
                },
                "signal_review": {
                    "traces": [
                        {
                            "trace_id": "trc-1",
                            "entry_signal_id": "sig-1",
                            "last_signal_id": "sig-1",
                            "opened_ts": now_ts - 120,
                            "decision_chain": [
                                {
                                    "ts": now_ts - 120,
                                    "signal_id": "sig-1",
                                    "trace_id": "trc-1",
                                    "wallet": "0xabc",
                                    "action": "follow",
                                    "action_label": "跟随",
                                    "final_status": "filled",
                                }
                            ],
                        }
                    ],
                    "cycles": [
                        {
                            "cycle_id": "cycle-1",
                            "ts": now_ts - 60,
                            "candidates": [
                                {
                                    "candidate_snapshot": {
                                        "signal_id": "sig-1",
                                        "trace_id": "trc-1",
                                        "wallet": "0xabc",
                                        "market_slug": "demo-market",
                                        "side": "BUY",
                                        "position_action": "follow",
                                        "position_action_label": "首次入场",
                                        "wallet_score": 88.0,
                                        "wallet_tier": "TRADE",
                                    },
                                    "final_status": "filled",
                                    "action": "follow",
                                    "action_label": "跟随",
                                    "note": "looks good",
                                    "topic_snapshot": {
                                        "topic_label": "politics",
                                        "topic_bias": "boost",
                                        "topic_multiplier": 1.1,
                                    },
                                    "order_snapshot": {
                                        "status": "FILLED",
                                        "reason": "entry",
                                        "notional": 10.5,
                                    },
                                }
                            ],
                        }
                    ],
                },
                "orders": [
                    {
                        "ts": now_ts - 30,
                        "title": "demo-market",
                        "token_id": "token-1",
                        "outcome": "YES",
                        "side": "BUY",
                        "status": "FILLED",
                        "retry_count": 0,
                        "latency_ms": 120,
                        "reason": "entry | looks good",
                        "cycle_id": "cycle-1",
                        "signal_id": "sig-1",
                        "trace_id": "trc-1",
                        "flow": "entry",
                        "position_action": "entry",
                        "position_action_label": "首次入场",
                        "source_wallet": "0xabc",
                        "entry_wallet": "0xabc",
                        "notional": 10.5,
                    }
                ],
            },
            state_file,
        )
        state_file.flush()
        state_file.close()

        control_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"decision_mode": "manual", "updated_ts": now_ts}, control_file)
        control_file.flush()
        control_file.close()

        monitor_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor_file)
        monitor_file.flush()
        monitor_file.close()

        monitor12_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor12_file)
        monitor12_file.flush()
        monitor12_file.close()

        reconciliation_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, reconciliation_file)
        reconciliation_file.flush()
        reconciliation_file.close()

        reconciliation_text_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        reconciliation_text_file.write("")
        reconciliation_text_file.flush()
        reconciliation_text_file.close()

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.close()

        db_path = str(Path(tempfile.mkdtemp()) / "terminal.db")
        store = PersonalTerminalStore(db_path)
        self.addCleanup(store.close)
        store.upsert_candidate(
            Candidate(
                id="cand-1",
                signal_id="sig-1",
                trace_id="trc-1",
                wallet="0xabc",
                market_slug="demo-market",
                token_id="token-1",
                outcome="YES",
                side="BUY",
                confidence=0.8,
                score=77.0,
                status="approved",
                created_ts=now_ts,
                expires_ts=now_ts + 3600,
                updated_ts=now_ts,
                signal_snapshot={"signal_id": "sig-1", "trace_id": "trc-1"},
            )
        )
        store.record_candidate_action(
            "cand-1",
            action="follow",
            note="looks good",
            created_ts=now_ts + 10,
            idempotency_key="cand-1-follow",
        )
        store.append_journal_entry(
            {
                "candidate_id": "cand-1",
                "market_slug": "demo-market",
                "wallet": "0xabc",
                "action": "follow",
                "rationale": "looks good",
                "result_tag": "filled",
                "created_ts": now_ts + 20,
            }
        )

        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            "",
            monitor_file.name,
            monitor12_file.name,
            reconciliation_file.name,
            reconciliation_text_file.name,
            ledger_file.name,
            candidate_db_path=db_path,
        )

        list_payload = self._dispatch(handler_cls, "/api/candidates?search=demo-market&sort=score&order=desc&limit=5")
        detail_payload = self._dispatch(handler_cls, "/api/candidates/cand-1")

        frontend_dir.cleanup()

        self.assertEqual(list_payload["summary"]["count"], 1)
        self.assertEqual(list_payload["items"][0]["id"], "cand-1")
        self.assertEqual(list_payload["filters"]["search"], "demo-market")
        self.assertEqual(list_payload["filters"]["sort"], "score")
        self.assertEqual(list_payload["filters"]["order"], "desc")
        self.assertEqual(detail_payload["candidate"]["id"], "cand-1")
        self.assertEqual(detail_payload["candidate"]["status"], "approved")
        self.assertEqual(detail_payload["summary"]["related_action_count"], 1)
        self.assertEqual(detail_payload["summary"]["related_journal_count"], 1)
        self.assertEqual(detail_payload["summary"]["order_count"], 1)
        self.assertEqual(detail_payload["summary"]["decision_chain_count"], 1)
        self.assertTrue(detail_payload["summary"]["trace_found"])
        self.assertEqual(detail_payload["trace"]["trace_id"], "trc-1")
        self.assertTrue(any(row["kind"] == "order" for row in detail_payload["timeline"]))
        self.assertTrue(any(row["kind"] == "journal" for row in detail_payload["timeline"]))
        self.assertTrue(any(row["kind"] == "trace" for row in detail_payload["timeline"]))
        self.assertTrue(any(row["kind"] == "cycle" for row in detail_payload["timeline"]))

    def test_candidates_endpoint_falls_back_to_runtime_rows_when_store_empty(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        now_ts = int(time.time())

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "candidates": {
                    "summary": {"count": 1},
                    "items": [
                        {
                            "id": "runtime-only",
                            "signal_id": "sig-runtime",
                            "trace_id": "trc-runtime",
                            "wallet": "0xruntime",
                            "market_slug": "runtime-market",
                            "token_id": "runtime-token",
                            "outcome": "YES",
                            "side": "BUY",
                            "score": 99.0,
                            "status": "pending",
                            "created_ts": now_ts,
                            "updated_ts": now_ts,
                        }
                    ],
                }
            },
            state_file,
        )
        state_file.flush()
        state_file.close()

        control_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"decision_mode": "manual", "updated_ts": now_ts}, control_file)
        control_file.flush()
        control_file.close()

        monitor_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor_file)
        monitor_file.flush()
        monitor_file.close()

        monitor12_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor12_file)
        monitor12_file.flush()
        monitor12_file.close()

        reconciliation_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, reconciliation_file)
        reconciliation_file.flush()
        reconciliation_file.close()

        reconciliation_text_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        reconciliation_text_file.write("")
        reconciliation_text_file.flush()
        reconciliation_text_file.close()

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.close()

        db_path = str(Path(tempfile.mkdtemp()) / "terminal.db")
        store = PersonalTerminalStore(db_path)
        self.addCleanup(store.close)

        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            "",
            monitor_file.name,
            monitor12_file.name,
            reconciliation_file.name,
            reconciliation_text_file.name,
            ledger_file.name,
            candidate_db_path=db_path,
        )

        list_payload = self._dispatch(handler_cls, "/api/candidates?limit=5")
        detail_payload = self._dispatch(handler_cls, "/api/candidates/runtime-only")

        frontend_dir.cleanup()

        self.assertEqual(list_payload["summary"]["count"], 1)
        self.assertEqual(list_payload["items"][0]["id"], "runtime-only")
        self.assertEqual(detail_payload["candidate"]["id"], "runtime-only")
        self.assertEqual(detail_payload["candidate"]["status"], "pending")

    def test_candidates_endpoint_falls_back_to_signal_review_when_store_and_runtime_queue_empty(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        now_ts = int(time.time())

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "candidates": {
                    "summary": {"count": 0},
                    "items": [],
                    "observability": {
                        "recent_cycles": {
                            "cycles": 1,
                            "signals": 1,
                            "precheck_skipped": 1,
                            "market_time_source": {"metadata": 1, "slug_legacy": 0, "unknown": 0},
                            "skip_reasons": {"market_not_accepting_orders": 1},
                        }
                    },
                },
                "signal_review": {
                    "cycles": [
                        {
                            "cycle_id": "cycle-replay",
                            "ts": now_ts - 30,
                            "candidates": [
                                {
                                    "signal_id": "sig-replay",
                                    "trace_id": "trc-replay",
                                    "title": "demo-replay-market",
                                    "token_id": "token-replay",
                                    "outcome": "YES",
                                    "wallet": "0xabc",
                                    "side": "BUY",
                                    "wallet_score": 66.0,
                                    "wallet_tier": "TRADE",
                                    "action": "add",
                                    "action_label": "事件加仓",
                                    "final_status": "precheck_skipped",
                                    "candidate_snapshot": {
                                        "signal_id": "sig-replay",
                                        "trace_id": "trc-replay",
                                        "wallet": "0xabc",
                                        "market_slug": "demo-replay-market",
                                        "token_id": "token-replay",
                                        "outcome": "YES",
                                        "side": "BUY",
                                        "wallet_score": 66.0,
                                        "wallet_tier": "TRADE",
                                        "position_action": "add",
                                        "position_action_label": "事件加仓",
                                    },
                                    "decision_snapshot": {
                                        "skip_reason": "market_not_accepting_orders",
                                        "block_reason": "market_not_accepting_orders",
                                        "block_layer": "candidate",
                                        "market_time_source": "metadata",
                                        "market_metadata_hit": True,
                                    },
                                }
                            ],
                        }
                    ]
                },
            },
            state_file,
        )
        state_file.flush()
        state_file.close()

        control_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, control_file)
        control_file.flush()
        control_file.close()

        monitor_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor_file)
        monitor_file.flush()
        monitor_file.close()

        monitor12_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor12_file)
        monitor12_file.flush()
        monitor12_file.close()

        reconciliation_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, reconciliation_file)
        reconciliation_file.flush()
        reconciliation_file.close()

        reconciliation_text_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        reconciliation_text_file.write("")
        reconciliation_text_file.flush()
        reconciliation_text_file.close()

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.close()

        db_path = str(Path(tempfile.mkdtemp()) / "terminal.db")
        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            "",
            monitor_file.name,
            monitor12_file.name,
            reconciliation_file.name,
            reconciliation_text_file.name,
            ledger_file.name,
            candidate_db_path=db_path,
        )

        list_payload = self._dispatch(handler_cls, "/api/candidates")
        detail_payload = self._dispatch(handler_cls, "/api/candidates/sig-replay")

        frontend_dir.cleanup()

        self.assertEqual(list_payload["summary"]["count"], 1)
        self.assertEqual(list_payload["items"][0]["signal_id"], "sig-replay")
        self.assertEqual(list_payload["items"][0]["status"], "watched")
        self.assertEqual(list_payload["items"][0]["skip_reason"], "market_not_accepting_orders")
        self.assertEqual(list_payload["items"][0]["block_reason"], "market_not_accepting_orders")
        self.assertEqual(list_payload["items"][0]["block_layer"], "candidate")
        self.assertEqual(list_payload["items"][0]["market_time_source"], "metadata")
        self.assertTrue(list_payload["items"][0]["market_metadata_hit"])
        self.assertEqual(list_payload["observability"]["recent_cycles"]["signals"], 1)
        self.assertEqual(detail_payload["candidate"]["signal_id"], "sig-replay")
        self.assertEqual(detail_payload["candidate"]["skip_reason"], "market_not_accepting_orders")

    def test_state_and_metrics_surface_candidate_lifetime_expiration_summary(self):
        frontend_dir = tempfile.TemporaryDirectory()
        self.addCleanup(frontend_dir.cleanup)
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, state_file)
        state_file.flush()
        state_file.close()

        control_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, control_file)
        control_file.flush()
        control_file.close()

        monitor_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor_file)
        monitor_file.flush()
        monitor_file.close()

        monitor12_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor12_file)
        monitor12_file.flush()
        monitor12_file.close()

        reconciliation_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, reconciliation_file)
        reconciliation_file.flush()
        reconciliation_file.close()

        reconciliation_text_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        reconciliation_text_file.write("")
        reconciliation_text_file.flush()
        reconciliation_text_file.close()

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.close()

        db_path = str(Path(tempfile.mkdtemp()) / "terminal.db")
        store = PersonalTerminalStore(db_path)
        self.addCleanup(store.close)
        now_ts = int(time.time())
        store.upsert_candidate(
            Candidate(
                id="cand-expired-state",
                signal_id="sig-expired-state",
                trace_id="trc-expired-state",
                wallet="0xabc",
                market_slug="expired-state-market",
                token_id="token-expired-state",
                outcome="YES",
                side="BUY",
                confidence=0.8,
                score=70.0,
                status="approved",
                created_ts=now_ts - 2000,
                expires_ts=now_ts - 900,
                updated_ts=now_ts - 1800,
                signal_snapshot={"signal_id": "sig-expired-state", "timestamp": "2026-03-30T00:00:00+00:00"},
            )
        )

        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            "",
            monitor_file.name,
            monitor12_file.name,
            reconciliation_file.name,
            reconciliation_text_file.name,
            ledger_file.name,
            candidate_db_path=db_path,
            allow_read_side_effects=False,
        )

        state_payload = self._dispatch(handler_cls, "/api/state")
        metrics_status, _headers, metrics_body = self._dispatch_raw(handler_cls, "/metrics")

        lifecycle = state_payload["candidates"]["observability"]["lifecycle"]
        self.assertEqual(metrics_status, 200)
        self.assertEqual(lifecycle["expired_discarded_count"], 1)
        self.assertEqual(lifecycle["block_reasons"]["candidate_lifetime_expired"], 1)
        self.assertEqual(lifecycle["block_layers"]["candidate"], 1)
        self.assertEqual(lifecycle["reason_layer_counts"]["candidate_lifetime_expired"]["candidate"], 1)
        self.assertIn("polymarket_candidate_expired_discarded_count 1.0", metrics_body)
        self.assertIn(
            'polymarket_candidate_blocked_total{block_layer="candidate",reason_code="candidate_lifetime_expired"} 1.0',
            metrics_body,
        )
