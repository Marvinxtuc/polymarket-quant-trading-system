from __future__ import annotations

import importlib.util
import json
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from polymarket_bot.config import Settings


def _load_module():
    path = Path(__file__).resolve().parents[1] / "scripts" / "live_smoke_preflight.py"
    spec = importlib.util.spec_from_file_location("live_smoke_preflight", path)
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    spec.loader.exec_module(module)
    return module


class LiveSmokePreflightTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.module = _load_module()

    def _write_state(self, settings: Settings, payload: dict) -> None:
        state_path = Path(settings.runtime_store_path("state.json"))
        state_path.parent.mkdir(parents=True, exist_ok=True)
        state_path.write_text(json.dumps(payload), encoding="utf-8")

    def test_build_report_ready_when_live_state_is_safe(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = Settings(
                _env_file=None,
                dry_run=False,
                runtime_root_path=tmpdir,
                funder_address="0xabc",
                signer_url="https://signer.internal.local",
                clob_api_key="api-key",
                clob_api_secret="api-secret",
                clob_api_passphrase="api-passphrase",
                live_allowance_ready=True,
                live_geoblock_ready=True,
                live_account_ready=True,
                notify_webhook_url="https://hooks.example.local/ops",
                poll_interval_seconds=60,
            )
            now = int(time.time())
            self._write_state(
                settings,
                {
                    "ts": now,
                    "config": {"poll_interval_seconds": 60},
                    "control": {"decision_mode": "manual", "pause_opening": True},
                    "startup": {"ready": True},
                    "reconciliation": {"status": "ok"},
                    "persistence": {"status": "ok"},
                    "summary": {"open_positions": 0, "tracked_notional_usd": 0.0},
                },
            )

            with patch.dict(
                self.module.os.environ,
                {
                    "LIVE_SMOKE_TOKEN_ID": "",
                    "LIVE_SMOKE_RESTING_USD": "",
                    "LIVE_SMOKE_AGGRESSIVE_USD": "",
                    "LIVE_SMOKE_MAX_USD": "",
                },
                clear=False,
            ):
                report, exit_code = self.module.build_report(settings, now_ts=now)

        self.assertEqual(exit_code, 0)
        self.assertEqual(report["status"], "ready")
        self.assertEqual(report["blockers"], [])

    def test_build_report_blocks_when_remote_alert_and_state_are_not_ready(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = Settings(
                _env_file=None,
                dry_run=False,
                runtime_root_path=tmpdir,
                funder_address="0xabc",
                signer_url="https://signer.internal.local",
                clob_api_key="api-key",
                clob_api_secret="api-secret",
                clob_api_passphrase="api-passphrase",
                live_allowance_ready=True,
                live_geoblock_ready=True,
                live_account_ready=True,
                poll_interval_seconds=60,
            )
            now = int(time.time())
            self._write_state(
                settings,
                {
                    "ts": now - 600,
                    "config": {"poll_interval_seconds": 60},
                    "control": {"decision_mode": "semi_auto", "pause_opening": False},
                    "startup": {"ready": False},
                    "reconciliation": {"status": "warn"},
                    "persistence": {"status": "ok"},
                    "summary": {"open_positions": 1, "tracked_notional_usd": 5.0},
                },
            )

            with patch.dict(
                self.module.os.environ,
                {
                    "LIVE_SMOKE_TOKEN_ID": "",
                    "LIVE_SMOKE_RESTING_USD": "",
                    "LIVE_SMOKE_AGGRESSIVE_USD": "",
                    "LIVE_SMOKE_MAX_USD": "",
                },
                clear=False,
            ):
                report, exit_code = self.module.build_report(settings, now_ts=now)

        self.assertEqual(exit_code, 1)
        self.assertEqual(report["status"], "blocked")
        self.assertIn("remoteAlertNotConfigured", report["blocker_codes"])
        self.assertIn("stateStale", report["blocker_codes"])
        self.assertIn("decisionModeNotManual", report["blocker_codes"])
        self.assertIn("pauseOpeningDisabled", report["blocker_codes"])

    def test_build_report_blocks_when_collateral_balance_allowance_is_insufficient(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            settings = Settings(
                _env_file=None,
                dry_run=False,
                runtime_root_path=tmpdir,
                funder_address="0xabc",
                signer_url="https://signer.internal.local",
                clob_api_key="api-key",
                clob_api_secret="api-secret",
                clob_api_passphrase="api-passphrase",
                live_allowance_ready=True,
                live_geoblock_ready=True,
                live_account_ready=True,
                notify_webhook_url="https://hooks.example.local/ops",
                poll_interval_seconds=60,
            )
            now = int(time.time())
            self._write_state(
                settings,
                {
                    "ts": now,
                    "config": {"poll_interval_seconds": 60},
                    "control": {"decision_mode": "manual", "pause_opening": True},
                    "startup": {"ready": True},
                    "reconciliation": {"status": "ok"},
                    "persistence": {"status": "ok"},
                    "summary": {"open_positions": 0, "tracked_notional_usd": 0.0},
                },
            )

            with patch.dict(self.module.os.environ, {"LIVE_SMOKE_TOKEN_ID": "token-demo", "LIVE_SMOKE_MAX_USD": "2.0"}, clear=False):
                with patch.object(
                    self.module,
                    "_evaluate_collateral_budget",
                    return_value={
                        "enabled": True,
                        "skipped": False,
                        "ok": False,
                        "token_id": "token-demo",
                        "required_usd": 2.0,
                        "required_units": 2000000,
                        "resting_usd": 1.0,
                        "aggressive_usd": 1.0,
                        "max_usd": 2.0,
                        "balance_units": 0,
                        "allowance_units": 0,
                        "balance_usd": 0.0,
                        "allowance_usd": 0.0,
                        "error": "",
                        "response_keys": ["balance", "allowance"],
                    },
                ):
                    report, exit_code = self.module.build_report(settings, now_ts=now)

        self.assertEqual(exit_code, 1)
        self.assertEqual(report["status"], "blocked")
        self.assertIn("collateralBalanceAllowanceInsufficient", report["blocker_codes"])


if __name__ == "__main__":
    unittest.main()
