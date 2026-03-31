from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from polymarket_bot.state_store import StateStore
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


def _dispatch_raw(handler_cls, path: str) -> tuple[int, str, str]:
    request = f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n"
    sock = _FakeSocket(request)
    server = SimpleNamespace(server_name="localhost", server_port=8787)
    handler_cls(sock, ("127.0.0.1", 12345), server)
    raw = sock.raw_response.decode("utf-8", errors="ignore")
    header_text, body = raw.split("\r\n\r\n", 1)
    status_code = int(header_text.splitlines()[0].split(" ")[1])
    return status_code, header_text, body


class MetricsExposedTests(unittest.TestCase):
    def _build_handler(self, *, state_payload: dict, public_state_payload: dict) -> tuple[type, Path, Path]:
        frontend_dir = tempfile.TemporaryDirectory()
        self.addCleanup(frontend_dir.cleanup)
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(state_payload, state_file)
        state_file.flush()
        state_file.close()
        state_path = Path(state_file.name)
        self.addCleanup(lambda: state_path.unlink(missing_ok=True))

        control_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"pause_opening": False, "reduce_only": False, "emergency_stop": False}, control_file)
        control_file.flush()
        control_file.close()
        self.addCleanup(lambda: Path(control_file.name).unlink(missing_ok=True))

        monitor_30m = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor_30m)
        monitor_30m.flush()
        monitor_30m.close()
        self.addCleanup(lambda: Path(monitor_30m.name).unlink(missing_ok=True))

        monitor_12h = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, monitor_12h)
        monitor_12h.flush()
        monitor_12h.close()
        self.addCleanup(lambda: Path(monitor_12h.name).unlink(missing_ok=True))

        reconciliation_json = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, reconciliation_json)
        reconciliation_json.flush()
        reconciliation_json.close()
        self.addCleanup(lambda: Path(reconciliation_json.name).unlink(missing_ok=True))

        reconciliation_text = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        reconciliation_text.write("")
        reconciliation_text.flush()
        reconciliation_text.close()
        self.addCleanup(lambda: Path(reconciliation_text.name).unlink(missing_ok=True))

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.write("")
        ledger_file.flush()
        ledger_file.close()
        self.addCleanup(lambda: Path(ledger_file.name).unlink(missing_ok=True))

        public_state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(public_state_payload, public_state_file)
        public_state_file.flush()
        public_state_file.close()
        public_path = Path(public_state_file.name)
        self.addCleanup(lambda: public_path.unlink(missing_ok=True))

        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            "",
            monitor_30m.name,
            monitor_12h.name,
            reconciliation_json.name,
            reconciliation_text.name,
            ledger_file.name,
            public_state_path=str(public_path),
            allow_read_side_effects=True,
            heartbeat_stale_after_seconds=120,
            buy_blocked_alert_after_seconds=300,
        )
        return handler_cls, public_path, state_path

    def test_metrics_endpoint_exposes_required_series(self):
        handler_cls, _, _ = self._build_handler(
            state_payload={
                "admission": {
                    "opening_allowed": False,
                    "mode": "REDUCE_ONLY",
                    "reason_codes": ["startup_checks_fail"],
                    "evidence_summary": {
                        "reconciliation_status": "fail",
                        "account_snapshot_age_seconds": 999,
                        "account_snapshot_stale_threshold_seconds": 300,
                        "broker_event_sync_age_seconds": 888,
                        "broker_event_stale_threshold_seconds": 300,
                        "ledger_diff": 2.5,
                        "ledger_diff_threshold_usd": 1.0,
                    },
                },
                "kill_switch": {
                    "opening_allowed": False,
                    "manual_required": True,
                    "broker_safe_confirmed": False,
                },
                "runner_heartbeat": {
                    "last_seen_ts": 0,
                    "last_cycle_started_ts": 0,
                    "last_cycle_finished_ts": 0,
                    "cycle_seq": 0,
                    "loop_status": "idle",
                    "writer_active": False,
                },
                "buy_blocked": {
                    "active": True,
                    "reason_code": "startup_not_ready",
                    "since_ts": 1,
                    "duration_seconds": 500,
                    "updated_ts": 1,
                },
            },
            public_state_payload={"sentinel": True},
        )

        status, headers, body = _dispatch_raw(handler_cls, "/metrics")
        self.assertEqual(status, 200)
        self.assertIn("Content-Type: text/plain; version=0.0.4; charset=utf-8", headers)
        self.assertIn("polymarket_runner_heartbeat_age_seconds", body)
        self.assertIn("polymarket_buy_blocked_duration_seconds", body)
        self.assertIn('polymarket_alert_active{alert_code="admission_fail_closed",severity="page"} 1.0', body)
        self.assertIn('polymarket_alert_active{alert_code="kill_switch_manual_required",severity="page"} 1.0', body)
        self.assertIn('polymarket_alert_active{alert_code="buy_blocked_too_long",severity="warning"} 1.0', body)

    def test_metrics_endpoint_has_no_side_effects(self):
        handler_cls, public_path, _ = self._build_handler(
            state_payload={
                "admission": {"opening_allowed": True, "mode": "NORMAL", "reason_codes": [], "evidence_summary": {}},
                "runner_heartbeat": {"last_seen_ts": 1, "last_cycle_started_ts": 1, "last_cycle_finished_ts": 1, "cycle_seq": 1, "loop_status": "running", "writer_active": True},
                "buy_blocked": {"active": False, "reason_code": "", "since_ts": 0, "duration_seconds": 0, "updated_ts": 1},
            },
            public_state_payload={"sentinel": "before"},
        )

        before_public = public_path.read_text(encoding="utf-8")
        status, _, _ = _dispatch_raw(handler_cls, "/metrics")
        after_public = public_path.read_text(encoding="utf-8")

        self.assertEqual(status, 200)
        self.assertEqual(before_public, after_public)

    def test_metrics_scrape_does_not_write_runtime_or_public_state_or_trigger_write_paths(self):
        handler_cls, public_path, state_path = self._build_handler(
            state_payload={
                "admission": {"opening_allowed": True, "mode": "NORMAL", "reason_codes": [], "evidence_summary": {}},
                "runner_heartbeat": {
                    "last_seen_ts": 10,
                    "last_cycle_started_ts": 10,
                    "last_cycle_finished_ts": 10,
                    "cycle_seq": 1,
                    "loop_status": "running",
                    "writer_active": True,
                },
                "buy_blocked": {"active": False, "reason_code": "", "since_ts": 0, "duration_seconds": 0, "updated_ts": 10},
                "sentinel_runtime": "stable",
            },
            public_state_payload={"sentinel": "public-before"},
        )

        before_public = public_path.read_text(encoding="utf-8")
        before_state = state_path.read_text(encoding="utf-8")

        with (
            patch("polymarket_bot.web._safe_write_json", side_effect=AssertionError("unexpected write in metrics scrape")),
            patch.object(StateStore, "save_control_state", side_effect=AssertionError("unexpected control write in metrics scrape")),
            patch.object(StateStore, "save_runtime_state", side_effect=AssertionError("unexpected runtime write in metrics scrape")),
        ):
            status, _, _ = _dispatch_raw(handler_cls, "/metrics")

        after_public = public_path.read_text(encoding="utf-8")
        after_state = state_path.read_text(encoding="utf-8")
        self.assertEqual(status, 200)
        self.assertEqual(before_public, after_public)
        self.assertEqual(before_state, after_state)
