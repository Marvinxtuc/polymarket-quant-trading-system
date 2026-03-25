from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

from polymarket_bot.config import Settings
from polymarket_bot.i18n import t as i18n_t


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "release_gate_check.py"
    spec = importlib.util.spec_from_file_location("release_gate_check", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class ReleaseGateCheckTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = _load_module()

    def _write_json(self, path: Path, payload: dict[str, object]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def test_build_report_blocks_when_required_evidence_missing(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            live = Settings(dry_run=False, runtime_root_path=tmpdir, funder_address="0xabc")
            paper = Settings(dry_run=True, runtime_root_path=tmpdir)
            report = self.module.build_report(live_settings=live, paper_settings=paper, now_ts=123)

        self.assertEqual(report["status"], "blocked")
        self.assertIn(i18n_t("report.releaseGate.blocker.rehearsalNotClean"), report["blockers"])
        self.assertIn(i18n_t("report.releaseGate.blocker.remoteAlertSmokeFailed"), report["blockers"])
        self.assertIn(i18n_t("report.releaseGate.blocker.liveSmokePreflightNotReady"), report["blockers"])

    def test_build_report_caution_when_only_observe_and_all_hard_gates_pass(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            live = Settings(dry_run=False, runtime_root_path=tmpdir, funder_address="0xabc")
            paper = Settings(dry_run=True, runtime_root_path=tmpdir)
            live_paths = self.module.build_runtime_artifact_paths(live)
            paper_paths = self.module.build_runtime_artifact_paths(paper)

            self._write_json(
                Path(live_paths["full_flow_validation_json_path"]),
                {
                    "validation_status": "PASS",
                    "flow_standard_met": True,
                    "operational_readiness": {"level": "observe"},
                },
            )
            rehearsal_path = Path(paper_paths["rehearsal_24h_dry_run_out_path"])
            rehearsal_path.parent.mkdir(parents=True, exist_ok=True)
            rehearsal_path.write_text(
                "\n".join(
                    [
                        "# 24h paper rehearsal",
                        "window_hours=2 interval_seconds=3600",
                        "checkpoint1 2026-03-20 00:00:00 0 0 0 12 0 0 0 0 0 0 0 0 0 0 pass",
                        "checkpoint2 2026-03-20 01:00:00 3600 0 0 12 0 0 0 0 0 0 0 0 0 0 pass",
                        "rehearsal_done=123 checkpoints=2",
                    ]
                ),
                encoding="utf-8",
            )
            self._write_json(Path(live.runtime_store_path("alert_delivery_smoke.json")), {"status": "sent", "blockers": []})
            self._write_json(Path(live.runtime_store_path("live_smoke_preflight.json")), {"status": "ready", "blockers": []})
            self._write_json(Path(live_paths["live_smoke_summary_path"]), {"ok": True, "returncode": 0, "status": "passed"})

            (Path(tmpdir) / "production_signoff_draft.md").write_text("all clear\n", encoding="utf-8")
            (Path(tmpdir) / "production_release_record_draft.md").write_text("all clear\n", encoding="utf-8")
            (Path(tmpdir) / "operations_handoff_draft.md").write_text("all clear\n", encoding="utf-8")

            original_root = self.module.ROOT
            try:
                self.module.ROOT = Path(tmpdir)
                report = self.module.build_report(live_settings=live, paper_settings=paper, now_ts=123)
            finally:
                self.module.ROOT = original_root

        self.assertEqual(report["status"], "caution")
        self.assertEqual(report["blockers"], [])
        self.assertIn(i18n_t("report.releaseGate.advisory.operationalReadinessObserve"), report["advisories"])

    def test_build_report_flags_placeholder_docs(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            live = Settings(dry_run=False, runtime_root_path=tmpdir, funder_address="0xabc")
            paper = Settings(dry_run=True, runtime_root_path=tmpdir)
            live_paths = self.module.build_runtime_artifact_paths(live)
            paper_paths = self.module.build_runtime_artifact_paths(paper)

            self._write_json(
                Path(live_paths["full_flow_validation_json_path"]),
                {
                    "validation_status": "PASS",
                    "flow_standard_met": True,
                    "operational_readiness": {"level": "ready"},
                },
            )
            rehearsal_path = Path(paper_paths["rehearsal_24h_dry_run_out_path"])
            rehearsal_path.parent.mkdir(parents=True, exist_ok=True)
            rehearsal_path.write_text(
                "\n".join(
                    [
                        "window_hours=1 interval_seconds=3600",
                        "checkpoint1 2026-03-20 00:00:00 0 0 0 12 0 0 0 0 0 0 0 0 0 0 pass",
                        "rehearsal_done=123 checkpoints=1",
                    ]
                ),
                encoding="utf-8",
            )
            self._write_json(Path(live.runtime_store_path("alert_delivery_smoke.json")), {"status": "sent", "blockers": []})
            self._write_json(Path(live.runtime_store_path("live_smoke_preflight.json")), {"status": "ready", "blockers": []})
            self._write_json(Path(live_paths["live_smoke_summary_path"]), {"ok": True, "returncode": 0, "status": "passed"})

            signoff = Path(tmpdir) / "production_signoff_draft.md"
            signoff.write_text("批准人：待填\n", encoding="utf-8")
            (Path(tmpdir) / "production_release_record_draft.md").write_text("ok\n", encoding="utf-8")
            (Path(tmpdir) / "operations_handoff_draft.md").write_text("ok\n", encoding="utf-8")

            original_root = self.module.ROOT
            try:
                self.module.ROOT = Path(tmpdir)
                report = self.module.build_report(live_settings=live, paper_settings=paper, now_ts=123)
            finally:
                self.module.ROOT = original_root

        self.assertEqual(report["status"], "blocked")
        self.assertIn(i18n_t("report.releaseGate.blocker.operatorDocsPlaceholders"), report["blockers"])


if __name__ == "__main__":
    unittest.main()
