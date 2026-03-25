from __future__ import annotations

import json
import os
import subprocess
import tempfile
import unittest
from pathlib import Path

from polymarket_bot.i18n import t as i18n_t


class VerifyAlertDeliveryLocalTests(unittest.TestCase):
    def setUp(self) -> None:
        self.repo = Path(__file__).resolve().parents[1]

    def test_local_alert_delivery_script_writes_summary_and_payload(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            env = os.environ.copy()
            env.update(
                {
                    "PYTHONPATH": str(self.repo / "src"),
                    "RUNTIME_ROOT_PATH": tmpdir,
                    "DRY_RUN": "false",
                    "FUNDER_ADDRESS": "0xabc",
                    "PRIVATE_KEY": "secret",
                }
            )
            proc = subprocess.run(
                [str(self.repo / ".venv" / "bin" / "python"), str(self.repo / "scripts" / "verify_alert_delivery_local.py")],
                cwd=self.repo,
                env=env,
                text=True,
                capture_output=True,
                check=True,
            )

            summary_path = Path(proc.stdout.strip().splitlines()[-1])
            self.assertTrue(summary_path.exists())
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
            self.assertEqual(summary["status"], "sent")
            self.assertEqual(summary["status_label"], i18n_t("report.alertDeliveryLocal.status.sent"))
            self.assertTrue(summary["payload_present"])


if __name__ == "__main__":
    unittest.main()
