from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from polymarket_bot.reconciliation_report import append_ledger_entry
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
    def _dispatch(handler_cls, path: str) -> dict:
        request = f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n"
        sock = _FakeSocket(request)
        server = SimpleNamespace(server_name="localhost", server_port=8787)
        handler_cls(sock, ("127.0.0.1", 12345), server)
        raw = sock.raw_response.decode("utf-8", errors="ignore")
        body = raw.split("\r\n\r\n", 1)[1]
        return json.loads(body)

    @staticmethod
    def _dispatch_post(handler_cls, path: str, payload: dict) -> tuple[int, dict]:
        raw_body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        request = (
            f"POST {path} HTTP/1.1\r\n"
            f"Host: localhost\r\n"
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

    def test_monitor_and_reconciliation_endpoints_serve_json_files(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"summary": {"open_positions": 1}}, state_file)
        state_file.flush()
        state_file.close()

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
        )

        monitor_payload = self._dispatch(handler_cls, "/api/monitor/30m")
        monitor12_payload = self._dispatch(handler_cls, "/api/monitor/12h")
        reconciliation_payload = self._dispatch(handler_cls, "/api/reconciliation/eod")

        frontend_dir.cleanup()

        self.assertEqual(monitor_payload["report_type"], "monitor_30m")
        self.assertEqual(monitor_payload["final_recommendation"], "OBSERVE")
        self.assertEqual(monitor12_payload["report_type"], "monitor_12h")
        self.assertEqual(monitor12_payload["final_recommendation"], "BLOCK")
        self.assertEqual(reconciliation_payload["status"], "warn")
        self.assertEqual(reconciliation_payload["day_key"], "2026-03-17")

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

        handler_cls = build_handler(
            frontend_dir.name,
            state_file.name,
            control_file.name,
            "",
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
        self.assertIn("Polymarket Reconciliation EOD Report", generated_text)

    def test_operator_endpoint_queues_clear_stale_pending_request(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({}, state_file)
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

        status_code, payload = self._dispatch_post(
            handler_cls,
            "/api/operator",
            {"command": "clear_stale_pending"},
        )

        control_payload = json.loads(Path(control_file.name).read_text(encoding="utf-8"))
        frontend_dir.cleanup()

        self.assertEqual(status_code, 200)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["command"], "clear_stale_pending")
        self.assertGreater(int(payload["requested_ts"]), 0)
        self.assertTrue(control_payload["pause_opening"])
        self.assertEqual(int(control_payload["clear_stale_pending_requested_ts"]), int(payload["requested_ts"]))
