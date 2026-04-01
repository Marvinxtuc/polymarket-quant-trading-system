from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

from polymarket_bot.i18n import t as i18n_t


ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "check_env.py"
SPEC = importlib.util.spec_from_file_location("poly_check_env", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise RuntimeError(f"unable to load {MODULE_PATH}")
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


class CheckEnvTests(unittest.TestCase):
    def test_validate_env_allows_optional_example_keys_to_be_missing(self) -> None:
        env_actual = {
            "DRY_RUN": "true",
            "POLL_INTERVAL_SECONDS": "30",
            "BANKROLL_USD": "5000",
            "RISK_PER_TRADE_PCT": "0.01",
            "DAILY_MAX_LOSS_PCT": "0.03",
            "MAX_OPEN_POSITIONS": "8",
            "POLYMARKET_DATA_API": "https://data-api.polymarket.com",
            "POLYMARKET_CLOB_HOST": "https://clob.polymarket.com",
        }
        env_example = dict(env_actual)
        env_example["CANDIDATE_DB_PATH"] = "/tmp/poly_runtime_data/decision_terminal.db"

        problems, warnings = MODULE.validate_env(env_actual, env_example)

        self.assertEqual(problems, [])
        self.assertEqual(
            warnings,
            [i18n_t("script.checkEnv.warning.missingOptionalKeys", {"keys": "CANDIDATE_DB_PATH"})],
        )

    def test_validate_env_warns_when_blockbeats_key_is_missing(self) -> None:
        env_actual = {
            "DRY_RUN": "true",
            "POLL_INTERVAL_SECONDS": "30",
            "BANKROLL_USD": "5000",
            "RISK_PER_TRADE_PCT": "0.01",
            "DAILY_MAX_LOSS_PCT": "0.03",
            "MAX_OPEN_POSITIONS": "8",
            "POLYMARKET_DATA_API": "https://data-api.polymarket.com",
            "POLYMARKET_CLOB_HOST": "https://clob.polymarket.com",
        }
        env_example = dict(env_actual)
        env_example["BLOCKBEATS_API_KEY"] = ""

        problems, warnings = MODULE.validate_env(env_actual, env_example)

        self.assertEqual(problems, [])
        self.assertEqual(
            warnings,
            [
                i18n_t("script.checkEnv.warning.blockbeatsUnavailable")
            ],
        )

    def test_validate_env_accepts_when_blockbeats_key_is_present(self) -> None:
        env_actual = {
            "DRY_RUN": "true",
            "POLL_INTERVAL_SECONDS": "30",
            "BANKROLL_USD": "5000",
            "RISK_PER_TRADE_PCT": "0.01",
            "DAILY_MAX_LOSS_PCT": "0.03",
            "MAX_OPEN_POSITIONS": "8",
            "POLYMARKET_DATA_API": "https://data-api.polymarket.com",
            "POLYMARKET_CLOB_HOST": "https://clob.polymarket.com",
            "BLOCKBEATS_API_KEY": "bb-demo",
        }
        env_example = dict(env_actual)

        problems, warnings = MODULE.validate_env(env_actual, env_example)

        self.assertEqual(problems, [])
        self.assertEqual(warnings, [])

    def test_validate_env_requires_live_secrets_and_flags_when_not_dry_run(self) -> None:
        env_actual = {
            "DRY_RUN": "false",
            "POLL_INTERVAL_SECONDS": "30",
            "BANKROLL_USD": "5000",
            "RISK_PER_TRADE_PCT": "0.01",
            "DAILY_MAX_LOSS_PCT": "0.03",
            "MAX_OPEN_POSITIONS": "8",
            "POLYMARKET_DATA_API": "https://data-api.polymarket.com",
            "POLYMARKET_CLOB_HOST": "https://clob.polymarket.com",
            "LIVE_ALLOWANCE_READY": "false",
            "LIVE_GEOBLOCK_READY": "false",
            "LIVE_ACCOUNT_READY": "false",
        }
        env_example = dict(env_actual)

        problems, warnings = MODULE.validate_env(env_actual, env_example)

        self.assertIn(i18n_t("script.checkEnv.problem.dryRunRequiresKey", {"key": "FUNDER_ADDRESS"}), problems)
        self.assertIn(i18n_t("script.checkEnv.problem.dryRunRequiresKey", {"key": "SIGNER_URL"}), problems)
        self.assertIn(i18n_t("script.checkEnv.problem.dryRunRequiresKey", {"key": "CLOB_API_KEY"}), problems)
        self.assertIn(i18n_t("script.checkEnv.problem.dryRunRequiresKey", {"key": "CLOB_API_SECRET"}), problems)
        self.assertIn(i18n_t("script.checkEnv.problem.dryRunRequiresKey", {"key": "CLOB_API_PASSPHRASE"}), problems)
        self.assertIn(i18n_t("script.checkEnv.problem.dryRunRequiresTrue", {"key": "LIVE_ALLOWANCE_READY"}), problems)
        self.assertIn(i18n_t("script.checkEnv.problem.dryRunRequiresTrue", {"key": "LIVE_GEOBLOCK_READY"}), problems)
        self.assertIn(i18n_t("script.checkEnv.problem.dryRunRequiresTrue", {"key": "LIVE_ACCOUNT_READY"}), problems)
        self.assertIn(
            i18n_t("script.checkEnv.warning.remoteAlertMissing"),
            warnings,
        )

    def test_validate_env_accepts_ready_live_configuration(self) -> None:
        env_actual = {
            "DRY_RUN": "false",
            "POLL_INTERVAL_SECONDS": "30",
            "BANKROLL_USD": "5000",
            "RISK_PER_TRADE_PCT": "0.01",
            "DAILY_MAX_LOSS_PCT": "0.03",
            "MAX_OPEN_POSITIONS": "8",
            "POLYMARKET_DATA_API": "https://data-api.polymarket.com",
            "POLYMARKET_CLOB_HOST": "https://clob.polymarket.com",
            "FUNDER_ADDRESS": "0xabc",
            "SIGNER_URL": "https://signer.internal.local",
            "CLOB_API_KEY": "api-key",
            "CLOB_API_SECRET": "api-secret",
            "CLOB_API_PASSPHRASE": "api-passphrase",
            "LIVE_ALLOWANCE_READY": "true",
            "LIVE_GEOBLOCK_READY": "true",
            "LIVE_ACCOUNT_READY": "true",
            "POLY_NOTIFY_WEBHOOK_URL": "https://hooks.example.local/polymarket",
        }
        env_example = dict(env_actual)

        problems, warnings = MODULE.validate_env(env_actual, env_example)

        self.assertEqual(problems, [])
        self.assertEqual(warnings, [])

    def test_validate_env_warns_when_live_has_no_remote_alert_channel(self) -> None:
        env_actual = {
            "DRY_RUN": "false",
            "POLL_INTERVAL_SECONDS": "30",
            "BANKROLL_USD": "5000",
            "RISK_PER_TRADE_PCT": "0.01",
            "DAILY_MAX_LOSS_PCT": "0.03",
            "MAX_OPEN_POSITIONS": "8",
            "POLYMARKET_DATA_API": "https://data-api.polymarket.com",
            "POLYMARKET_CLOB_HOST": "https://clob.polymarket.com",
            "FUNDER_ADDRESS": "0xabc",
            "SIGNER_URL": "https://signer.internal.local",
            "CLOB_API_KEY": "api-key",
            "CLOB_API_SECRET": "api-secret",
            "CLOB_API_PASSPHRASE": "api-passphrase",
            "LIVE_ALLOWANCE_READY": "true",
            "LIVE_GEOBLOCK_READY": "true",
            "LIVE_ACCOUNT_READY": "true",
        }
        env_example = dict(env_actual)

        problems, warnings = MODULE.validate_env(env_actual, env_example)

        self.assertEqual(problems, [])
        self.assertIn(
            i18n_t("script.checkEnv.warning.remoteAlertMissing"),
            warnings,
        )

    def test_validate_env_accepts_live_when_remote_alert_channel_is_configured(self) -> None:
        env_actual = {
            "DRY_RUN": "true",
            "POLL_INTERVAL_SECONDS": "30",
            "BANKROLL_USD": "5000",
            "RISK_PER_TRADE_PCT": "0.01",
            "DAILY_MAX_LOSS_PCT": "0.03",
            "MAX_OPEN_POSITIONS": "8",
            "POLYMARKET_DATA_API": "https://data-api.polymarket.com",
            "POLYMARKET_CLOB_HOST": "https://clob.polymarket.com",
            "FUNDER_ADDRESS": "0xabc",
            "SIGNER_URL": "https://signer.internal.local",
            "CLOB_API_KEY": "api-key",
            "CLOB_API_SECRET": "api-secret",
            "CLOB_API_PASSPHRASE": "api-passphrase",
            "LIVE_ALLOWANCE_READY": "true",
            "LIVE_GEOBLOCK_READY": "true",
            "LIVE_ACCOUNT_READY": "true",
        }
        env_example = dict(env_actual)
        env_actual["DRY_RUN"] = "false"
        env_actual["POLY_NOTIFY_WEBHOOK_URL"] = "https://hooks.example.local/polymarket"

        problems, warnings = MODULE.validate_env(env_actual, env_example)

        self.assertEqual(problems, [])
        self.assertEqual(warnings, [])

    def test_validate_env_rejects_raw_private_key_in_live_mode(self) -> None:
        env_actual = {
            "DRY_RUN": "false",
            "POLL_INTERVAL_SECONDS": "30",
            "BANKROLL_USD": "5000",
            "RISK_PER_TRADE_PCT": "0.01",
            "DAILY_MAX_LOSS_PCT": "0.03",
            "MAX_OPEN_POSITIONS": "8",
            "POLYMARKET_DATA_API": "https://data-api.polymarket.com",
            "POLYMARKET_CLOB_HOST": "https://clob.polymarket.com",
            "PRIVATE_KEY": "raw-key-should-not-be-used",
            "FUNDER_ADDRESS": "0xabc",
            "SIGNER_URL": "https://signer.internal.local",
            "CLOB_API_KEY": "api-key",
            "CLOB_API_SECRET": "api-secret",
            "CLOB_API_PASSPHRASE": "api-passphrase",
            "LIVE_ALLOWANCE_READY": "true",
            "LIVE_GEOBLOCK_READY": "true",
            "LIVE_ACCOUNT_READY": "true",
        }
        env_example = dict(env_actual)

        problems, _warnings = MODULE.validate_env(env_actual, env_example)

        expected = i18n_t("script.checkEnv.problem.liveRawPrivateKeyForbidden")
        self.assertTrue(
            expected in problems
            or any("PRIVATE_KEY" in str(item) and "forbids" in str(item) for item in problems)
        )


if __name__ == "__main__":
    unittest.main()
