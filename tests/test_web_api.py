from __future__ import annotations

import io
import json
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

from polymarket_bot.db import PersonalTerminalStore
from polymarket_bot.reconciliation_report import append_ledger_entry
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

    @staticmethod
    def _dispatch_raw(handler_cls, path: str) -> tuple[int, str, str]:
        request = f"GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n"
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
        json.dump({"summary": {"open_positions": 1}}, state_file)
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

        state_file = Path(state_file.name)
        state_file.write_text(json.dumps({"summary": {"open_positions": 3}}, ensure_ascii=False), encoding="utf-8")
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

    def test_candidate_action_and_mode_endpoints_use_runtime_store(self):
        frontend_dir = tempfile.TemporaryDirectory()
        Path(frontend_dir.name, "index.html").write_text("ok", encoding="utf-8")

        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump({"candidates": {"summary": {"count": 1}, "items": []}}, state_file)
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
                created_ts=now_ts + 1,
                expires_ts=now_ts + 3600,
                updated_ts=now_ts + 1,
                signal_snapshot={"signal_id": "sig-2"},
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
        )

        status_code, payload = self._dispatch_post(
            handler_cls,
            "/api/candidate/action",
            {"candidate_id": "cand-1", "action": "follow", "note": "looks good"},
        )
        replay_status, replay_payload = self._dispatch_post(
            handler_cls,
            "/api/candidate/action",
            {"candidate_id": "cand-1", "action": "follow", "note": "looks good"},
        )
        mode_status, mode_payload = self._dispatch_post(
            handler_cls,
            "/api/mode",
            {"mode": "semi_auto"},
        )
        candidates_payload = self._dispatch(handler_cls, "/api/candidates?status=approved&limit=1&wallet=0xabc&market_slug=demo-market")
        journal_payload = self._dispatch(handler_cls, "/api/journal?limit=1")
        stats_payload = self._dispatch(handler_cls, "/api/stats?days=30&recent_days=1")
        archive_payload = self._dispatch(handler_cls, "/api/archive?days=30&recent_days=1")
        export_json_payload = self._dispatch(handler_cls, "/api/export?scope=actions&format=json&days=30&limit=2")
        export_csv_status, export_csv_headers, export_csv_body = self._dispatch_raw(
            handler_cls,
            "/api/export?scope=journal&format=csv&days=30&limit=2",
        )
        profiles_status, profiles_payload = self._dispatch_post(
            handler_cls,
            "/api/wallet-profiles/update",
            {"wallet": "0xabc", "tag": "CORE", "invalid_field": "boom"},
        )
        invalid_score_status, invalid_score_payload = self._dispatch_post(
            handler_cls,
            "/api/wallet-profiles/update",
            {"wallet": "0xabc", "trust_score": "oops"},
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
        self.assertEqual(control_after["decision_mode"], "semi_auto")
        self.assertEqual(candidates_payload["summary"]["count"], 1)
        self.assertEqual(candidates_payload["items"][0]["id"], "cand-1")
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
        self.assertEqual(profiles_payload["error"], "unsupported wallet profile fields")
        self.assertEqual(profiles_payload["fields"], ["invalid_field"])
        self.assertEqual(invalid_score_status, 400)
        self.assertEqual(invalid_score_payload["error"], "trust_score must be a number")

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
