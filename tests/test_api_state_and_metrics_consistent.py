from __future__ import annotations

import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

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


def _dispatch(handler_cls, path: str) -> tuple[int, str]:
    request = f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n"
    sock = _FakeSocket(request)
    server = SimpleNamespace(server_name="localhost", server_port=8787)
    handler_cls(sock, ("127.0.0.1", 12345), server)
    raw = sock.raw_response.decode("utf-8", errors="ignore")
    header_text, body = raw.split("\r\n\r\n", 1)
    status_code = int(header_text.splitlines()[0].split(" ")[1])
    return status_code, body


def _metric_value(metrics_text: str, metric_name: str) -> float:
    for line in metrics_text.splitlines():
        row = line.strip()
        if not row or row.startswith("#"):
            continue
        if row.startswith(metric_name + " "):
            return float(row.split(" ", 1)[1].strip())
    raise AssertionError(f"metric not found: {metric_name}")


class ApiStateMetricsConsistentTests(unittest.TestCase):
    def test_api_state_and_metrics_share_same_observability_projection(self):
        frontend_dir = tempfile.TemporaryDirectory()
        self.addCleanup(frontend_dir.cleanup)
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        state_payload = {
            "admission": {
                "opening_allowed": False,
                "mode": "REDUCE_ONLY",
                "reason_codes": ["startup_checks_fail"],
                "evidence_summary": {
                    "reconciliation_status": "warn",
                    "account_snapshot_age_seconds": 333,
                    "account_snapshot_stale_threshold_seconds": 300,
                    "broker_event_sync_age_seconds": 222,
                    "broker_event_stale_threshold_seconds": 300,
                    "ledger_diff": 0.3,
                    "ledger_diff_threshold_usd": 1.0,
                },
            },
            "buy_blocked": {
                "active": True,
                "reason_code": "startup_not_ready",
                "since_ts": 1700000000,
                "duration_seconds": 450,
                "updated_ts": 1700000450,
            },
            "runner_heartbeat": {
                "last_seen_ts": 1700000400,
                "last_cycle_started_ts": 1700000390,
                "last_cycle_finished_ts": 1700000400,
                "cycle_seq": 42,
                "loop_status": "running",
                "writer_active": True,
            },
            "kill_switch": {
                "opening_allowed": True,
                "manual_required": False,
                "broker_safe_confirmed": True,
            },
        }
        json.dump(state_payload, state_file)
        state_file.flush()
        state_file.close()
        self.addCleanup(lambda: Path(state_file.name).unlink(missing_ok=True))

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
            allow_read_side_effects=False,
            heartbeat_stale_after_seconds=120,
            buy_blocked_alert_after_seconds=300,
        )

        status_state, state_body = _dispatch(handler_cls, "/api/state")
        status_metrics, metrics_body = _dispatch(handler_cls, "/metrics")
        self.assertEqual(status_state, 200)
        self.assertEqual(status_metrics, 200)

        api_state = json.loads(state_body)
        observability = dict(api_state.get("observability") or {})
        api_metrics = dict(observability.get("metrics") or {})

        self.assertAlmostEqual(
            float(api_metrics.get("admission_opening_allowed", 0.0)),
            _metric_value(metrics_body, "polymarket_admission_opening_allowed"),
            places=6,
        )
        self.assertAlmostEqual(
            float(api_metrics.get("buy_blocked_duration_seconds", 0.0)),
            _metric_value(metrics_body, "polymarket_buy_blocked_duration_seconds"),
            places=6,
        )
        self.assertAlmostEqual(
            float(api_metrics.get("account_snapshot_stale", 0.0)),
            _metric_value(metrics_body, "polymarket_account_snapshot_stale"),
            places=6,
        )
        self.assertAlmostEqual(
            float(api_metrics.get("event_stream_stale", 0.0)),
            _metric_value(metrics_body, "polymarket_event_stream_stale"),
            places=6,
        )
        self.assertEqual(
            int(observability.get("admission", {}).get("snapshot_age_seconds", 0)),
            333,
        )
        self.assertEqual(
            int(observability.get("admission", {}).get("event_sync_age_seconds", 0)),
            222,
        )
        self.assertIn('polymarket_alert_active{alert_code="admission_fail_closed",severity="page"} 1.0', metrics_body)
