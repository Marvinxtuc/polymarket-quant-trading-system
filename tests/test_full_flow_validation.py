from __future__ import annotations

import json
import tempfile
import time
import unittest
from pathlib import Path

from polymarket_bot.full_flow_validation import render_full_flow_validation_report, run_full_flow_validation
from polymarket_bot.i18n import t as i18n_t


class FullFlowValidationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tempdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tempdir.name)
        (self.root / "scripts").mkdir(parents=True, exist_ok=True)
        (self.root / ".venv" / "bin").mkdir(parents=True, exist_ok=True)

        self.paths = {
            "state_path": self.root / "state.json",
            "ledger_path": self.root / "ledger.db",
            "runtime_state_path": self.root / "runtime_state.json",
            "events_path": self.root / "events.ndjson",
            "bot_log_path": self.root / "poly_bot.log",
            "monitor_30m_json_path": self.root / "monitor_30m.json",
            "monitor_12h_json_path": self.root / "monitor_12h.json",
            "monitor_30m_state_path": self.root / "monitor_30m.state",
            "monitor_12h_state_path": self.root / "monitor_12h.state",
            "reconciliation_eod_json_path": self.root / "reconciliation_eod.json",
            "reconciliation_eod_text_path": self.root / "reconciliation_eod.txt",
        }
        now_ts = int(time.time())

        self.state_payload = {
            "ts": now_ts,
            "config": {
                "dry_run": True,
                "execution_mode": "paper",
                "broker_name": "PaperBroker",
                "poll_interval_seconds": 60,
                "wallet_pool_size": 47,
            },
            "startup": {"ready": True, "warning_count": 0, "failure_count": 0, "checks": []},
            "reconciliation": {
                "status": "ok",
                "issues": [],
                "startup_ready": True,
                "internal_vs_ledger_diff": 0.0,
                "broker_floor_gap_vs_internal": 0.0,
            },
            "summary": {
                "open_positions": 0,
                "max_open_positions": 12,
                "tracked_notional_usd": 0.0,
            },
        }
        self.monitor_30m_payload = {
            "report_type": "monitor_30m",
            "generated_ts": now_ts - 60,
            "sample_status": "INCONCLUSIVE",
            "final_recommendation": "OBSERVE: no recent EXEC samples in quick validation window.",
            "counts": {"exec": 0},
        }
        self.monitor_12h_payload = {
            "report_type": "monitor_12h",
            "generated_ts": now_ts - 120,
            "sample_status": "INCONCLUSIVE",
            "final_recommendation": "OBSERVE: no recent EXEC samples in quick validation window.",
            "counts": {"exec": 0},
        }
        self.reconciliation_payload = {
            "generated_ts": now_ts - 30,
            "status": "ok",
            "day_key": "2026-03-18",
            "issues": [],
            "ledger_summary": {"fill_count": 0, "realized_pnl": 0.0},
        }
        self.replay_runtime_payload = {
            "events": {"count": 12},
            "replay": {"reconstructed_open_positions": 0},
            "drift": {"positions_delta": 0, "notional_delta_usd": 0.0},
        }
        self.replay_calibration_payload = {
            "sample_count": 12,
            "matrix": [{"scenario": "base"}],
            "recommended": {
                "scenario": "base",
                "net_cashflow_proxy": 123.45,
                "reject_rate": 0.0,
            },
        }

    def tearDown(self) -> None:
        self.tempdir.cleanup()

    def _http_client(self, missing: set[str] | None = None):
        missing = missing or set()

        def client(url: str, *, method: str = "GET", payload=None, timeout: int = 5):
            if url in missing:
                return {
                    "ok": False,
                    "status_code": 404,
                    "payload": None,
                    "error": "http 404",
                    "raw": "",
                }
            if method.upper() == "POST":
                return {
                    "ok": True,
                    "status_code": 200,
                    "payload": {
                        "command": "generate_reconciliation_report",
                        "json_path": str(self.paths["reconciliation_eod_json_path"]),
                        "text_path": str(self.paths["reconciliation_eod_text_path"]),
                    },
                    "error": None,
                    "raw": "",
                }
            if url.endswith("/api/state"):
                return {"ok": True, "status_code": 200, "payload": self.state_payload, "error": None, "raw": ""}
            if url.endswith("/api/monitor/30m"):
                return {"ok": True, "status_code": 200, "payload": self.monitor_30m_payload, "error": None, "raw": ""}
            if url.endswith("/api/monitor/12h"):
                return {"ok": True, "status_code": 200, "payload": self.monitor_12h_payload, "error": None, "raw": ""}
            if url.endswith("/api/reconciliation/eod"):
                return {"ok": True, "status_code": 200, "payload": self.reconciliation_payload, "error": None, "raw": ""}
            raise AssertionError(f"unexpected url {url}")

        return client

    def _command_runner(self, fail_scripts: set[str] | None = None):
        fail_scripts = fail_scripts or set()

        def runner(args, *, cwd=None, env=None, timeout=180):
            args = [str(item) for item in args]
            joined = " ".join(args)
            if "start_poly_stack.sh" in joined:
                return {
                    "ok": "start_poly_stack.sh" not in fail_scripts,
                    "returncode": 0 if "start_poly_stack.sh" not in fail_scripts else 1,
                    "stdout": "stack_runtime=direct\nurl=http://127.0.0.1:8787\n",
                    "stderr": "",
                    "duration_seconds": 0.01,
                    "cmd": args,
                    "error": None,
                }
            if "monitor_thresholds_30m.sh" in joined:
                Path(args[7]).write_text(json.dumps(self.monitor_30m_payload), encoding="utf-8")
                Path(args[5]).write_text("1", encoding="utf-8")
                Path(args[2]).write_text("monitor 30m text", encoding="utf-8")
                return {"ok": True, "returncode": 0, "stdout": str(args[2]), "stderr": "", "duration_seconds": 0.01, "cmd": args, "error": None}
            if "monitor_thresholds_12h.sh" in joined:
                Path(args[7]).write_text(json.dumps(self.monitor_12h_payload), encoding="utf-8")
                Path(args[5]).write_text("1", encoding="utf-8")
                Path(args[2]).write_text("monitor 12h text", encoding="utf-8")
                return {"ok": True, "returncode": 0, "stdout": str(args[2]), "stderr": "", "duration_seconds": 0.01, "cmd": args, "error": None}
            if "replay_runtime.py" in joined:
                return {
                    "ok": True,
                    "returncode": 0,
                    "stdout": json.dumps(self.replay_runtime_payload),
                    "stderr": "",
                    "duration_seconds": 0.01,
                    "cmd": args,
                    "error": None,
                }
            if "replay_calibration.py" in joined:
                return {
                    "ok": True,
                    "returncode": 0,
                    "stdout": json.dumps(self.replay_calibration_payload),
                    "stderr": "",
                    "duration_seconds": 0.01,
                    "cmd": args,
                    "error": None,
                }
            raise AssertionError(f"unexpected command {joined}")

        return runner

    def test_run_full_flow_validation_passes_when_all_stages_are_healthy(self):
        report = run_full_flow_validation(
            root_dir=self.root,
            state_path=str(self.paths["state_path"]),
            ledger_path=str(self.paths["ledger_path"]),
            runtime_state_path=str(self.paths["runtime_state_path"]),
            events_path=str(self.paths["events_path"]),
            bot_log_path=str(self.paths["bot_log_path"]),
            monitor_30m_json_path=str(self.paths["monitor_30m_json_path"]),
            monitor_12h_json_path=str(self.paths["monitor_12h_json_path"]),
            monitor_30m_state_path=str(self.paths["monitor_30m_state_path"]),
            monitor_12h_state_path=str(self.paths["monitor_12h_state_path"]),
            reconciliation_eod_json_path=str(self.paths["reconciliation_eod_json_path"]),
            reconciliation_eod_text_path=str(self.paths["reconciliation_eod_text_path"]),
            bootstrap_stack=True,
            monitor_window_seconds=0,
            http_client=self._http_client(),
            command_runner=self._command_runner(),
        )

        self.assertTrue(report["flow_standard_met"])
        self.assertEqual(report["validation_status"], "pass")
        self.assertEqual(report["operational_readiness"]["level"], "observe")
        stage_names = [stage["name"] for stage in report["stages"]]
        self.assertIn("stack_bootstrap", stage_names)
        self.assertIn("monitor_30m_api", stage_names)
        self.assertIn("reconciliation_api", stage_names)
        text = render_full_flow_validation_report(report)
        self.assertIn(
            f"{i18n_t('report.fullFlowValidation.field.validationStatus')}: {i18n_t('report.fullFlowValidation.enum.status.pass')}",
            text,
        )
        self.assertIn(
            f"{i18n_t('report.fullFlowValidation.field.operationalReadiness')}: {i18n_t('report.fullFlowValidation.enum.readiness.observe')}",
            text,
        )

    def test_run_full_flow_validation_fails_when_monitor_api_is_missing(self):
        report = run_full_flow_validation(
            root_dir=self.root,
            state_path=str(self.paths["state_path"]),
            ledger_path=str(self.paths["ledger_path"]),
            runtime_state_path=str(self.paths["runtime_state_path"]),
            events_path=str(self.paths["events_path"]),
            bot_log_path=str(self.paths["bot_log_path"]),
            monitor_30m_json_path=str(self.paths["monitor_30m_json_path"]),
            monitor_12h_json_path=str(self.paths["monitor_12h_json_path"]),
            monitor_30m_state_path=str(self.paths["monitor_30m_state_path"]),
            monitor_12h_state_path=str(self.paths["monitor_12h_state_path"]),
            reconciliation_eod_json_path=str(self.paths["reconciliation_eod_json_path"]),
            reconciliation_eod_text_path=str(self.paths["reconciliation_eod_text_path"]),
            bootstrap_stack=False,
            monitor_window_seconds=0,
            http_client=self._http_client(missing={"http://127.0.0.1:8787/api/monitor/30m"}),
            command_runner=self._command_runner(),
        )

        self.assertFalse(report["flow_standard_met"])
        self.assertEqual(report["validation_status"], "fail")
        failed = {stage["name"] for stage in report["stages"] if stage["status"] != "pass"}
        self.assertIn("monitor_30m_api", failed)

    def test_run_full_flow_validation_tracks_isolated_monitor_state_and_individual_windows(self):
        report = run_full_flow_validation(
            root_dir=self.root,
            state_path=str(self.paths["state_path"]),
            ledger_path=str(self.paths["ledger_path"]),
            runtime_state_path=str(self.paths["runtime_state_path"]),
            events_path=str(self.paths["events_path"]),
            bot_log_path=str(self.paths["bot_log_path"]),
            monitor_30m_json_path=str(self.paths["monitor_30m_json_path"]),
            monitor_12h_json_path=str(self.paths["monitor_12h_json_path"]),
            monitor_30m_state_path=str(self.paths["monitor_30m_state_path"]),
            monitor_12h_state_path=str(self.paths["monitor_12h_state_path"]),
            reconciliation_eod_json_path=str(self.paths["reconciliation_eod_json_path"]),
            reconciliation_eod_text_path=str(self.paths["reconciliation_eod_text_path"]),
            bootstrap_stack=False,
            monitor_30m_window_seconds=30,
            monitor_12h_window_seconds=90,
            http_client=self._http_client(),
            command_runner=self._command_runner(),
        )

        stage_map = {stage["name"]: stage for stage in report["stages"]}
        self.assertEqual(stage_map["monitor_30m_generation"]["details"]["state_path"], str(self.paths["monitor_30m_state_path"]))
        self.assertEqual(stage_map["monitor_12h_generation"]["details"]["state_path"], str(self.paths["monitor_12h_state_path"]))
        self.assertEqual(stage_map["monitor_30m_generation"]["details"]["window_seconds"], 30)
        self.assertEqual(stage_map["monitor_12h_generation"]["details"]["window_seconds"], 90)

    def test_run_full_flow_validation_marks_stale_monitor_reports_as_observe(self):
        self.monitor_30m_payload["generated_ts"] = self.state_payload["ts"] - 10_000
        self.monitor_30m_payload["final_recommendation"] = "OK"
        self.monitor_12h_payload["generated_ts"] = self.state_payload["ts"] - 120
        self.monitor_12h_payload["final_recommendation"] = "OK"

        report = run_full_flow_validation(
            root_dir=self.root,
            state_path=str(self.paths["state_path"]),
            ledger_path=str(self.paths["ledger_path"]),
            runtime_state_path=str(self.paths["runtime_state_path"]),
            events_path=str(self.paths["events_path"]),
            bot_log_path=str(self.paths["bot_log_path"]),
            monitor_30m_json_path=str(self.paths["monitor_30m_json_path"]),
            monitor_12h_json_path=str(self.paths["monitor_12h_json_path"]),
            monitor_30m_state_path=str(self.paths["monitor_30m_state_path"]),
            monitor_12h_state_path=str(self.paths["monitor_12h_state_path"]),
            reconciliation_eod_json_path=str(self.paths["reconciliation_eod_json_path"]),
            reconciliation_eod_text_path=str(self.paths["reconciliation_eod_text_path"]),
            bootstrap_stack=False,
            monitor_window_seconds=0,
            http_client=self._http_client(),
            command_runner=self._command_runner(),
        )

        readiness = report["operational_readiness"]
        self.assertEqual(readiness["level"], "observe")
        self.assertFalse(readiness["monitor_30m_fresh"])
        self.assertIn("monitor_30m_stale", " ".join(readiness["issues"]))


if __name__ == "__main__":
    unittest.main()
