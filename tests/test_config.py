from __future__ import annotations

import unittest

from polymarket_bot.config import Settings, build_runtime_artifact_paths


class SettingsRuntimePathTests(unittest.TestCase):
    def test_default_runtime_paths_are_namespaced_for_live_account(self):
        settings = Settings(_env_file=None, dry_run=False, funder_address="0xAbCDEF1234")

        self.assertIn("/live/0xabcdef1234/", settings.runtime_state_path)
        self.assertIn("/live/0xabcdef1234/", settings.ledger_path)
        self.assertIn("/live/0xabcdef1234/", settings.control_path)
        self.assertIn("/live/0xabcdef1234/", settings.candidate_db_path)
        self.assertIn("/live/0xabcdef1234/", settings.public_state_path)
        self.assertIn("/live/0xabcdef1234/", settings.network_smoke_log_path)

    def test_explicit_runtime_paths_are_preserved(self):
        settings = Settings(
            _env_file=None,
            runtime_state_path="/tmp/custom/runtime.json",
            ledger_path="/tmp/custom/ledger.jsonl",
            candidate_db_path="/tmp/custom/terminal.db",
        )

        self.assertEqual(settings.runtime_state_path, "/tmp/custom/runtime.json")
        self.assertEqual(settings.ledger_path, "/tmp/custom/ledger.jsonl")
        self.assertEqual(settings.candidate_db_path, "/tmp/custom/terminal.db")

    def test_runtime_artifact_paths_follow_namespace(self):
        settings = Settings(_env_file=None, dry_run=True)

        paths = build_runtime_artifact_paths(settings)

        self.assertIn("/paper/default/", paths["monitor_30m_json_path"])
        self.assertIn("/paper/default/", paths["monitor_12h_json_path"])
        self.assertIn("/paper/default/", paths["reconciliation_eod_json_path"])
        self.assertIn("/paper/default/", paths["full_flow_validation_report_path"])


if __name__ == "__main__":
    unittest.main()
