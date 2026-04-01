from __future__ import annotations

import os
import subprocess
import tempfile
import unittest
from pathlib import Path


class MonitorSchedulerScriptTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = Path(__file__).resolve().parents[1]
        self.script = self.repo / "scripts" / "monitor_scheduler_status.sh"
        self.runtime_root = tempfile.TemporaryDirectory()
        self.addCleanup(self.runtime_root.cleanup)
        self.env = os.environ.copy()
        self.env.update(
            {
                "PYTHONPATH": str(self.repo / "src"),
                "RUNTIME_ROOT_PATH": self.runtime_root.name,
                "DRY_RUN": "true",
            }
        )
        self.monitor_dir = Path(self.runtime_root.name) / "paper" / "default" / "monitor_reports"
        self.monitor_dir.mkdir(parents=True, exist_ok=True)
        self.method_file = self.monitor_dir / "method"

    def _run_status(self) -> str:
        proc = subprocess.run(
            [str(self.script)],
            cwd=self.repo,
            env=self.env,
            check=True,
            capture_output=True,
            text=True,
        )
        return proc.stdout

    def test_status_reports_live_nohup_pid(self) -> None:
        log_path = self.monitor_dir / "monitor-reports-nohup.log"
        self.method_file.write_text(
            f"mode=nohup\nstarted=2026-03-20 19:01:54 CST\npid={os.getpid()}\nlog={log_path}\n",
            encoding="utf-8",
        )

        output = self._run_status()

        self.assertIn("monitor-scheduler: mode=nohup", output)
        self.assertIn(f"pid={os.getpid()}", output)
        self.assertIn(f"log={log_path}", output)
        self.assertNotIn("status=stale", output)

    def test_status_marks_dead_nohup_pid_as_stale(self) -> None:
        log_path = self.monitor_dir / "monitor-reports-nohup.log"
        self.method_file.write_text(
            f"mode=nohup\nstarted=2026-03-20 19:01:54 CST\npid=999999\nlog={log_path}\n",
            encoding="utf-8",
        )

        output = self._run_status()

        self.assertIn("monitor-scheduler: mode=nohup", output)
        self.assertIn("status=stale", output)
        self.assertIn("stale_pid=999999", output)
        self.assertIn(f"log={log_path}", output)


if __name__ == "__main__":
    unittest.main()
