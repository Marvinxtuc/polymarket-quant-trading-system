from __future__ import annotations

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path

def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "readiness_brief.py"
    spec = importlib.util.spec_from_file_location("readiness_brief", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class ReadinessBriefTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = _load_module()

    def test_parse_rehearsal_reports_remaining(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "24h_dry_run_rehearsal.txt"
            path.write_text(
                "\n".join(
                    [
                        "# 24h paper rehearsal",
                        "start=2026-03-20 11:04:15 UTC",
                        "window_hours=24 interval_seconds=3600",
                        "checkpoint1 2026-03-20 19:04:15 0 13 0 12 0 0 100 0 0 0 0 0 0 0 pass",
                    ]
                ),
                encoding="utf-8",
            )
            summary = self.module._parse_rehearsal(path)

        self.assertTrue(summary["exists"])
        self.assertEqual(summary["checkpoint_count"], 1)
        self.assertEqual(summary["last_status"], "pass")
        self.assertGreater(summary["end_ts"], summary["start_ts"])
        self.assertIsNotNone(summary["remaining_seconds"])

    def test_build_brief_reads_existing_release_gate(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            release_gate = Path(tmpdir) / "live" / "0xabc" / "release_gate_report.json"
            rehearsal = Path(tmpdir) / "paper" / "default" / "24h_dry_run_rehearsal.txt"

            release_gate.parent.mkdir(parents=True, exist_ok=True)
            release_gate.write_text(
                json.dumps({"status": "blocked", "blockers": ["x"], "advisories": ["y"]}),
                encoding="utf-8",
            )
            rehearsal.parent.mkdir(parents=True, exist_ok=True)
            rehearsal.write_text(
                "\n".join(
                    [
                        "start=2026-03-20 11:04:15 UTC",
                        "window_hours=24 interval_seconds=3600",
                        "checkpoint1 2026-03-20 19:04:15 0 13 0 12 0 0 100 0 0 0 0 0 0 0 pass",
                    ]
                ),
                encoding="utf-8",
            )
            output = Path(tmpdir) / "readiness_brief.json"
            brief = self.module.build_brief(
                release_gate_path=str(release_gate),
                rehearsal_path=str(rehearsal),
                output_path=str(output),
            )

        self.assertEqual(brief["release_gate_status"], "blocked")
        self.assertEqual(brief["blockers"], ["x"])
        self.assertEqual(brief["advisories"], ["y"])
        self.assertEqual(brief["rehearsal"]["checkpoint_count"], 1)


if __name__ == "__main__":
    unittest.main()
