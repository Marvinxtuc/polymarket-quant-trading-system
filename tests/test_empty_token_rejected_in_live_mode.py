from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import polymarket_bot.web as web_mod


class EmptyTokenRejectedInLiveModeTests(unittest.TestCase):
    def test_live_mode_rejects_missing_or_weak_token_when_write_api_requested(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir_raw:
            tmpdir = Path(tmpdir_raw)
            frontend_dir = tmpdir / "frontend"
            frontend_dir.mkdir(parents=True, exist_ok=True)
            (frontend_dir / "index.html").write_text("ok", encoding="utf-8")

            settings = web_mod.Settings(
                _env_file=None,
                dry_run=False,
                enable_single_writer=True,
                wallet_lock_path=str(tmpdir / "wallet.lock"),
                state_store_path=str(tmpdir / "state.db"),
                runtime_state_path=str(tmpdir / "runtime_state.json"),
                control_path=str(tmpdir / "control.json"),
                ledger_path=str(tmpdir / "ledger.jsonl"),
                candidate_db_path=str(tmpdir / "terminal.db"),
                funder_address="0xabc123",
            )
            (tmpdir / "runtime_state.json").write_text("{}", encoding="utf-8")
            (tmpdir / "control.json").write_text("{}", encoding="utf-8")
            (tmpdir / "ledger.jsonl").write_text("", encoding="utf-8")

            for token_value in ("", "token"):
                args = SimpleNamespace(
                    host="127.0.0.1",
                    port=8787,
                    state_path="/tmp/poly_runtime_data/state.json",
                    control_path="/tmp/poly_runtime_data/control.json",
                    state_store_path="/tmp/poly_runtime_data/state.db",
                    control_token=token_value,
                    write_source_policy="local_only",
                    trusted_proxy_cidrs="",
                    control_audit_log_path="",
                    monitor_30m_json_path="/tmp/poly_monitor_30m_report.json",
                    monitor_12h_json_path="/tmp/poly_monitor_12h_report.json",
                    reconciliation_eod_json_path="/tmp/poly_reconciliation_eod_report.json",
                    reconciliation_eod_text_path="/tmp/poly_reconciliation_eod_report.txt",
                    ledger_path="/tmp/poly_runtime_data/ledger.jsonl",
                    public_state_path="/tmp/poly_public_state.json",
                    decision_mode_path="/tmp/poly_runtime_data/decision_mode.json",
                    candidate_actions_path="/tmp/poly_runtime_data/candidate_actions.json",
                    wallet_profiles_path="/tmp/poly_runtime_data/wallet_profiles.json",
                    journal_path="/tmp/poly_runtime_data/journal.json",
                    candidate_db_path="/tmp/poly_runtime_data/decision_terminal.db",
                    enable_write_api="true",
                    frontend_dir=str(frontend_dir),
                )

                with self.subTest(token=token_value):
                    with (
                        patch.object(web_mod.argparse.ArgumentParser, "parse_args", return_value=args),
                        patch.object(web_mod, "Settings", return_value=settings),
                        patch.object(web_mod.FileLock, "acquire", autospec=True) as mocked_acquire,
                    ):
                        with self.assertRaises(SystemExit) as ctx:
                            web_mod.main()
                        self.assertEqual(int(ctx.exception.code), 4)
                        mocked_acquire.assert_not_called()


if __name__ == "__main__":
    unittest.main()
