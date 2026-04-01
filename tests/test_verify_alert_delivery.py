from __future__ import annotations

import importlib.util
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from polymarket_bot.config import Settings
from polymarket_bot.i18n import t as i18n_t


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "verify_alert_delivery.py"
    spec = importlib.util.spec_from_file_location("verify_alert_delivery", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class VerifyAlertDeliveryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = _load_module()

    def test_build_report_blocks_without_remote_channels(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = Settings(
                _env_file=None,
                dry_run=False,
                runtime_root_path=tmpdir,
                funder_address="0xabc",
                notify_log_path=str(Path(tmpdir) / "notifier.jsonl"),
                notify_local_enabled=False,
            )
            report, exit_code = self.module.build_report(
                settings=settings,
                send_remote=False,
                include_local=False,
                title="t",
                body="b",
            )

        self.assertEqual(exit_code, 1)
        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["status_label"], i18n_t("report.alertDelivery.status.blocked"))
        self.assertIn(i18n_t("report.alertDelivery.blocker.remoteChannelNotConfigured"), report["blockers"])

    def test_build_report_ready_when_remote_configured_but_not_sent(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = Settings(
                _env_file=None,
                dry_run=False,
                runtime_root_path=tmpdir,
                funder_address="0xabc",
                notify_webhook_url="https://hooks.example.local/ops",
                notify_log_path=str(Path(tmpdir) / "notifier.jsonl"),
                notify_local_enabled=False,
            )
            report, exit_code = self.module.build_report(
                settings=settings,
                send_remote=False,
                include_local=False,
                title="t",
                body="b",
            )

        self.assertEqual(exit_code, 0)
        self.assertEqual(report["status"], "ready_to_send")
        self.assertEqual(report["status_label"], i18n_t("report.alertDelivery.status.ready_to_send"))
        self.assertEqual(report["remote_channels"], ["webhook"])

    def test_build_report_sends_remote_when_enabled(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = Settings(
                _env_file=None,
                dry_run=False,
                runtime_root_path=tmpdir,
                funder_address="0xabc",
                notify_webhook_url="https://hooks.example.local/ops",
                notify_log_path=str(Path(tmpdir) / "notifier.jsonl"),
                notify_local_enabled=False,
            )
            with patch.object(
                self.module.Notifier,
                "notify_all",
                return_value={"ok": True, "delivery_count": 1, "deliveries": []},
            ) as mock_notify:
                report, exit_code = self.module.build_report(
                    settings=settings,
                    send_remote=True,
                    include_local=False,
                    title="t",
                    body="b",
                )

        self.assertEqual(exit_code, 0)
        self.assertEqual(report["status"], "sent")
        self.assertEqual(report["status_label"], i18n_t("report.alertDelivery.status.sent"))
        mock_notify.assert_called_once()


if __name__ == "__main__":
    unittest.main()
