from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path


class RunLiveSmokeWrapperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = Path(__file__).resolve().parents[1]
        self.script = self.repo / "scripts" / "run_live_smoke.sh"
        self.env = os.environ.copy()
        self.env["PYTHONPATH"] = str(self.repo / "src")

    def _run(self, *, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
        env = dict(self.env)
        if extra_env:
            env.update(extra_env)
        return subprocess.run(
            [str(self.script)],
            cwd=self.repo,
            env=env,
            text=True,
            capture_output=True,
        )

    def test_refuses_without_token(self) -> None:
        proc = self._run()
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("missing token id", proc.stderr)

    def test_refuses_without_explicit_ack(self) -> None:
        proc = self._run(extra_env={"LIVE_SMOKE_TOKEN_ID": "demo-token"})
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("LIVE_SMOKE_ACK=YES", proc.stderr)

    def test_refuses_when_notional_exceeds_cap(self) -> None:
        proc = self._run(
            extra_env={
                "LIVE_SMOKE_TOKEN_ID": "demo-token",
                "LIVE_SMOKE_ACK": "YES",
                "LIVE_SMOKE_RESTING_USD": "3.0",
                "LIVE_SMOKE_AGGRESSIVE_USD": "1.0",
                "LIVE_SMOKE_MAX_USD": "2.0",
            }
        )
        self.assertNotEqual(proc.returncode, 0)
        self.assertIn("must be <=", proc.stderr)

    def test_writes_summary_when_stubbed_commands_succeed(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_py = Path(tmpdir) / "fake_python.py"
            summary = Path(tmpdir) / "live_smoke_execution_summary.json"
            log = Path(tmpdir) / "live_smoke_execution.log"
            fake_py.write_text(
                "\n".join(
                    [
                        "#!/usr/bin/env python3",
                        "import sys",
                        "script = sys.argv[1] if len(sys.argv) > 1 else ''",
                        "if script.endswith('live_smoke_preflight.py'):",
                        "    print('preflight ok')",
                        "    raise SystemExit(0)",
                        "if script.endswith('live_clob_type2_smoke.py'):",
                        "    print('smoke ok')",
                        "    raise SystemExit(0)",
                        "raise SystemExit(1)",
                    ]
                ),
                encoding="utf-8",
            )
            fake_py.chmod(0o755)

            proc = self._run(
                extra_env={
                    "LIVE_SMOKE_TOKEN_ID": "demo-token",
                    "LIVE_SMOKE_ACK": "YES",
                    "LIVE_SMOKE_PY_BIN": str(fake_py),
                    "LIVE_SMOKE_SUMMARY_PATH": str(summary),
                    "LIVE_SMOKE_LOG_PATH": str(log),
                }
            )

            self.assertEqual(proc.returncode, 0)
            self.assertTrue(summary.exists())
            payload = json.loads(summary.read_text(encoding="utf-8"))
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["status"], "passed")
            self.assertEqual(payload["token_id"], "demo-token")
            self.assertTrue(log.exists())

    def test_stops_when_preflight_fails(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            fake_py = Path(tmpdir) / "fake_python.py"
            summary = Path(tmpdir) / "live_smoke_execution_summary.json"
            log = Path(tmpdir) / "live_smoke_execution.log"
            marker = Path(tmpdir) / "smoke_called"
            fake_py.write_text(
                "\n".join(
                    [
                        "#!/usr/bin/env python3",
                        "import os",
                        "import sys",
                        "from pathlib import Path",
                        "script = sys.argv[1] if len(sys.argv) > 1 else ''",
                        "if script.endswith('live_smoke_preflight.py'):",
                        "    print('preflight blocked')",
                        "    raise SystemExit(1)",
                        "if script.endswith('live_clob_type2_smoke.py'):",
                        "    Path(os.environ['SMOKE_MARKER']).write_text('called', encoding='utf-8')",
                        "    print('smoke should not run')",
                        "    raise SystemExit(0)",
                        "raise SystemExit(1)",
                    ]
                ),
                encoding="utf-8",
            )
            fake_py.chmod(0o755)

            proc = self._run(
                extra_env={
                    "LIVE_SMOKE_TOKEN_ID": "demo-token",
                    "LIVE_SMOKE_ACK": "YES",
                    "LIVE_SMOKE_PY_BIN": str(fake_py),
                    "LIVE_SMOKE_SUMMARY_PATH": str(summary),
                    "LIVE_SMOKE_LOG_PATH": str(log),
                    "SMOKE_MARKER": str(marker),
                }
            )

            self.assertNotEqual(proc.returncode, 0)
            self.assertFalse(marker.exists())
            self.assertTrue(summary.exists())
            payload = json.loads(summary.read_text(encoding="utf-8"))
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["status"], "failed")
            self.assertTrue(log.exists())


if __name__ == "__main__":
    unittest.main()
