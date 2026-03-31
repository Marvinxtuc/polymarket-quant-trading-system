from __future__ import annotations

import io
import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from polymarket_bot.state_store import StateStore
from polymarket_bot.web import build_handler


class _FakeSocket:
    def __init__(self, request_bytes: bytes):
        self._rfile = io.BytesIO(request_bytes)
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
    def raw_response(self) -> str:
        return self._wfile.getvalue().decode("utf-8", errors="ignore")


def _dispatch_get(handler_cls, path: str) -> tuple[int, dict]:
    request = f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode("utf-8")
    sock = _FakeSocket(request)
    server = SimpleNamespace(server_name="localhost", server_port=8787)
    handler_cls(sock, ("127.0.0.1", 12345), server)
    raw = sock.raw_response
    header_text, body = raw.split("\r\n\r\n", 1)
    status = int(header_text.splitlines()[0].split(" ")[1])
    return status, json.loads(body)


def _dispatch_post(handler_cls, path: str, payload: dict) -> tuple[int, dict]:
    raw_body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    request = (
        f"POST {path} HTTP/1.1\r\n"
        "Host: localhost\r\n"
        "Content-Type: application/json\r\n"
        f"Content-Length: {len(raw_body)}\r\n"
        "\r\n"
    ).encode("utf-8") + raw_body
    sock = _FakeSocket(request)
    server = SimpleNamespace(server_name="localhost", server_port=8787)
    handler_cls(sock, ("127.0.0.1", 12345), server)
    raw = sock.raw_response
    header_text, body = raw.split("\r\n\r\n", 1)
    status = int(header_text.splitlines()[0].split(" ")[1])
    return status, json.loads(body)


class StandbyWebWriteGuardTests(unittest.TestCase):
    def test_standby_instance_cannot_write_control_state(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_raw:
            tmpdir = Path(tmpdir_raw)
            frontend_dir = tmpdir / "frontend"
            frontend_dir.mkdir(parents=True, exist_ok=True)
            (frontend_dir / "index.html").write_text("ok", encoding="utf-8")

            state_path = tmpdir / "state.json"
            control_path = tmpdir / "control.json"
            public_state_path = tmpdir / "public_state.json"
            monitor_30m = tmpdir / "monitor30.json"
            monitor_12h = tmpdir / "monitor12.json"
            reconciliation_json = tmpdir / "recon.json"
            reconciliation_txt = tmpdir / "recon.txt"
            ledger_path = tmpdir / "ledger.jsonl"
            state_store_path = tmpdir / "state.db"

            state_path.write_text(json.dumps({"summary": {"open_positions": 1}}, ensure_ascii=False), encoding="utf-8")
            control_path.write_text(json.dumps({"pause_opening": False}, ensure_ascii=False), encoding="utf-8")
            public_state_path.write_text(json.dumps({"summary": {"open_positions": 99}}, ensure_ascii=False), encoding="utf-8")
            monitor_30m.write_text("{}", encoding="utf-8")
            monitor_12h.write_text("{}", encoding="utf-8")
            reconciliation_json.write_text("{}", encoding="utf-8")
            reconciliation_txt.write_text("", encoding="utf-8")
            ledger_path.write_text("", encoding="utf-8")

            store = StateStore(str(state_store_path))
            store.save_control_state(
                {
                    "decision_mode": "manual",
                    "pause_opening": False,
                    "reduce_only": False,
                    "emergency_stop": False,
                    "clear_stale_pending_requested_ts": 0,
                    "updated_ts": 1,
                }
            )

            handler = build_handler(
                str(frontend_dir),
                str(state_path),
                str(control_path),
                "",
                str(monitor_30m),
                str(monitor_12h),
                str(reconciliation_json),
                str(reconciliation_txt),
                str(ledger_path),
                public_state_path=str(public_state_path),
                state_store_path=str(state_store_path),
                enable_write_api=False,
                allow_read_side_effects=False,
                writer_scope="paper:default",
            )

            status, payload = _dispatch_post(handler, "/api/control", {"command": "reduce_only", "value": True})
            self.assertEqual(status, 503)
            self.assertEqual(str(payload.get("reason_code") or ""), "single_writer_conflict")

            control_truth = StateStore(str(state_store_path)).load_control_state() or {}
            self.assertFalse(bool(control_truth.get("reduce_only")))

    def test_standby_get_state_has_no_export_side_effect(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_raw:
            tmpdir = Path(tmpdir_raw)
            frontend_dir = tmpdir / "frontend"
            frontend_dir.mkdir(parents=True, exist_ok=True)
            (frontend_dir / "index.html").write_text("ok", encoding="utf-8")

            state_path = tmpdir / "state.json"
            control_path = tmpdir / "control.json"
            public_state_path = tmpdir / "public_state.json"
            monitor_30m = tmpdir / "monitor30.json"
            monitor_12h = tmpdir / "monitor12.json"
            reconciliation_json = tmpdir / "recon.json"
            reconciliation_txt = tmpdir / "recon.txt"
            ledger_path = tmpdir / "ledger.jsonl"
            state_store_path = tmpdir / "state.db"

            state_path.write_text(json.dumps({"summary": {"open_positions": 1}}, ensure_ascii=False), encoding="utf-8")
            control_path.write_text("{}", encoding="utf-8")
            public_state_path.write_text(json.dumps({"summary": {"open_positions": 77}}, ensure_ascii=False), encoding="utf-8")
            monitor_30m.write_text("{}", encoding="utf-8")
            monitor_12h.write_text("{}", encoding="utf-8")
            reconciliation_json.write_text("{}", encoding="utf-8")
            reconciliation_txt.write_text("", encoding="utf-8")
            ledger_path.write_text("", encoding="utf-8")
            StateStore(str(state_store_path)).save_control_state({"decision_mode": "manual"})

            handler = build_handler(
                str(frontend_dir),
                str(state_path),
                str(control_path),
                "",
                str(monitor_30m),
                str(monitor_12h),
                str(reconciliation_json),
                str(reconciliation_txt),
                str(ledger_path),
                public_state_path=str(public_state_path),
                state_store_path=str(state_store_path),
                enable_write_api=False,
                allow_read_side_effects=False,
                writer_scope="paper:default",
            )

            status, _ = _dispatch_get(handler, "/api/state")
            self.assertEqual(status, 200)
            public_payload = json.loads(public_state_path.read_text(encoding="utf-8"))
            self.assertEqual(public_payload["summary"]["open_positions"], 77)

    def test_standby_get_blockbeats_does_not_fetch_or_cache_write(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_raw:
            tmpdir = Path(tmpdir_raw)
            frontend_dir = tmpdir / "frontend"
            frontend_dir.mkdir(parents=True, exist_ok=True)
            (frontend_dir / "index.html").write_text("ok", encoding="utf-8")

            state_path = tmpdir / "state.json"
            control_path = tmpdir / "control.json"
            public_state_path = tmpdir / "public_state.json"
            monitor_30m = tmpdir / "monitor30.json"
            monitor_12h = tmpdir / "monitor12.json"
            reconciliation_json = tmpdir / "recon.json"
            reconciliation_txt = tmpdir / "recon.txt"
            ledger_path = tmpdir / "ledger.jsonl"
            state_store_path = tmpdir / "state.db"

            state_path.write_text("{}", encoding="utf-8")
            control_path.write_text("{}", encoding="utf-8")
            public_state_path.write_text(json.dumps({"summary": {"open_positions": 88}}, ensure_ascii=False), encoding="utf-8")
            monitor_30m.write_text("{}", encoding="utf-8")
            monitor_12h.write_text("{}", encoding="utf-8")
            reconciliation_json.write_text("{}", encoding="utf-8")
            reconciliation_txt.write_text("", encoding="utf-8")
            ledger_path.write_text("", encoding="utf-8")
            StateStore(str(state_store_path)).save_control_state({"decision_mode": "manual"})

            with patch("polymarket_bot.web._fetch_blockbeats_dashboard") as mocked_fetch:
                handler = build_handler(
                    str(frontend_dir),
                    str(state_path),
                    str(control_path),
                    "",
                    str(monitor_30m),
                    str(monitor_12h),
                    str(reconciliation_json),
                    str(reconciliation_txt),
                    str(ledger_path),
                    public_state_path=str(public_state_path),
                    state_store_path=str(state_store_path),
                    enable_write_api=False,
                    allow_read_side_effects=False,
                    writer_scope="paper:default",
                )
                status, payload = _dispatch_get(handler, "/api/blockbeats?force=true")
                self.assertEqual(status, 200)
                self.assertIsInstance(payload, dict)
                mocked_fetch.assert_not_called()

    def test_web_main_acquires_lock_before_write_handler_registration(self) -> None:
        import polymarket_bot.web as web_mod

        with tempfile.TemporaryDirectory() as tmpdir_raw:
            tmpdir = Path(tmpdir_raw)
            order: list[str] = []
            settings = web_mod.Settings(
                _env_file=None,
                dry_run=True,
                enable_single_writer=True,
                wallet_lock_path=str(tmpdir / "wallet.lock"),
                state_store_path=str(tmpdir / "state.db"),
                runtime_state_path=str(tmpdir / "runtime_state.json"),
                control_path=str(tmpdir / "control.json"),
                ledger_path=str(tmpdir / "ledger.jsonl"),
                candidate_db_path=str(tmpdir / "terminal.db"),
            )
            (tmpdir / "runtime_state.json").write_text("{}", encoding="utf-8")
            (tmpdir / "control.json").write_text("{}", encoding="utf-8")
            (tmpdir / "ledger.jsonl").write_text("", encoding="utf-8")
            frontend_dir = tmpdir / "frontend"
            frontend_dir.mkdir(parents=True, exist_ok=True)

            args = SimpleNamespace(
                host="127.0.0.1",
                port=8787,
                state_path="/tmp/poly_runtime_data/state.json",
                control_path="/tmp/poly_runtime_data/control.json",
                state_store_path=os.getenv("POLY_STATE_STORE_PATH", "/tmp/poly_runtime_data/state.db"),
                control_token="test-control-token-1234",
                monitor_30m_json_path="/tmp/poly_monitor_30m_report.json",
                monitor_12h_json_path="/tmp/poly_monitor_12h_report.json",
                reconciliation_eod_json_path="/tmp/poly_reconciliation_eod_report.json",
                reconciliation_eod_text_path="/tmp/poly_reconciliation_eod_report.txt",
                ledger_path=os.getenv("LEDGER_PATH", "/tmp/poly_runtime_data/ledger.jsonl"),
                public_state_path=os.getenv("POLY_PUBLIC_STATE_PATH", "/tmp/poly_public_state.json"),
                write_source_policy="local_only",
                trusted_proxy_cidrs="",
                control_audit_log_path="",
                decision_mode_path=os.getenv("POLY_DECISION_MODE_PATH", "/tmp/poly_runtime_data/decision_mode.json"),
                candidate_actions_path=os.getenv("POLY_CANDIDATE_ACTIONS_PATH", "/tmp/poly_runtime_data/candidate_actions.json"),
                wallet_profiles_path=os.getenv("POLY_WALLET_PROFILES_PATH", "/tmp/poly_runtime_data/wallet_profiles.json"),
                journal_path=os.getenv("POLY_JOURNAL_PATH", "/tmp/poly_runtime_data/journal.json"),
                candidate_db_path=os.getenv("POLY_CANDIDATE_DB_PATH", "/tmp/poly_runtime_data/decision_terminal.db"),
                enable_write_api="true",
                frontend_dir=str(frontend_dir),
            )

            class _DummyServer:
                def __init__(self, *args, **kwargs):
                    _ = args
                    _ = kwargs

                def serve_forever(self):
                    return None

                def server_close(self):
                    return None

            def _fake_acquire(_self):
                order.append("lock")

            def _fake_build_handler(*args, **kwargs):
                order.append("build_handler")
                return SimpleNamespace()

            with (
                patch.object(web_mod.argparse.ArgumentParser, "parse_args", return_value=args),
                patch.object(web_mod, "Settings", return_value=settings),
                patch.object(web_mod.FileLock, "acquire", autospec=True, side_effect=_fake_acquire),
                patch.object(web_mod, "build_handler", side_effect=_fake_build_handler),
                patch.object(web_mod, "ReusableThreadingHTTPServer", _DummyServer),
            ):
                web_mod.main()

            self.assertIn("lock", order)
            self.assertIn("build_handler", order)
            self.assertLess(order.index("lock"), order.index("build_handler"))


if __name__ == "__main__":
    unittest.main()
