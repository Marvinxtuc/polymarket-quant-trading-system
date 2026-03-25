from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "rehearsal_finalize.py"
    spec = importlib.util.spec_from_file_location("rehearsal_finalize", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class RehearsalFinalizeTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = _load_module()

    def test_pending_when_rehearsal_not_done(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            rehearsal = Path(tmpdir) / "24h_dry_run_rehearsal.txt"
            rehearsal.write_text(
                "\n".join(
                    [
                        "# 24h paper rehearsal",
                        "window_hours=24 interval_seconds=3600",
                        "checkpoint1 2026-03-20 19:04:15 0 13 0 12 0 0 100 0 0 0 0 0 0 0 pass",
                    ]
                ),
                encoding="utf-8",
            )
            report = self.module.build_report(rehearsal_path=str(rehearsal), output_path=str(Path(tmpdir) / "out.json"))

        self.assertEqual(report["status"], "pending")
        self.assertEqual(report["rehearsal"]["checkpoint_count"], 1)

    def test_completed_and_runs_checks_when_done(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            rehearsal = Path(tmpdir) / "24h_dry_run_rehearsal.txt"
            rehearsal.write_text(
                "\n".join(
                    [
                        "# 24h paper rehearsal",
                        "window_hours=2 interval_seconds=3600",
                        "checkpoint1 2026-03-20 19:04:15 0 13 0 12 0 0 100 0 0 0 0 0 0 0 pass",
                        "checkpoint2 2026-03-20 20:04:15 3600 98 0 12 0 0 100 0 0 0 0 0 0 0 pass",
                        "rehearsal_done=123 checkpoints=2",
                    ]
                ),
                encoding="utf-8",
            )

            calls = []

            def fake_run(command, *, extra_env=None, timeout=180):
                calls.append((tuple(command), dict(extra_env or {}), timeout))
                return {"command": command, "returncode": 0, "ok": True, "duration_seconds": 0.1, "stdout": "", "stderr": ""}

            original = self.module._run_command
            self.module._run_command = fake_run
            try:
                report = self.module.build_report(
                    rehearsal_path=str(rehearsal),
                    output_path=str(Path(tmpdir) / "out.json"),
                    run_checks=True,
                )
            finally:
                self.module._run_command = original

        self.assertEqual(report["status"], "completed")
        self.assertEqual(len(report["checks"]), 5)
        self.assertEqual(calls[0][0], ("make", "readiness-brief"))
        self.assertEqual(calls[1][0], ("make", "release-gate"))


if __name__ == "__main__":
    unittest.main()
