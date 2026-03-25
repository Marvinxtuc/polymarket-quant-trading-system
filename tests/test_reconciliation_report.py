from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from polymarket_bot.i18n import t as i18n_t
from polymarket_bot.reconciliation_report import append_ledger_entry, build_reconciliation_report_from_paths, render_reconciliation_report


class ReconciliationReportTests(unittest.TestCase):
    def test_build_report_from_paths_summarizes_fills_and_renders_text(self):
        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "startup": {
                    "ready": True,
                    "warning_count": 1,
                    "failure_count": 0,
                },
                "reconciliation": {
                    "day_key": "2026-03-17",
                    "status": "warn",
                    "issues": ["broker_event_stream_stale=240s"],
                    "internal_realized_pnl": 5.0,
                    "ledger_realized_pnl": 5.0,
                    "broker_closed_pnl_today": 2.0,
                    "internal_vs_ledger_diff": 0.0,
                    "broker_floor_gap_vs_internal": 3.0,
                    "pending_orders": 1,
                    "open_positions": 1,
                },
                "summary": {
                    "open_positions": 1,
                    "tracked_notional_usd": 48.8,
                    "equity": 5005.0,
                    "cash_balance_usd": 4200.0,
                    "positions_value_usd": 805.0,
                    "internal_pnl_today": 5.0,
                    "broker_closed_pnl_today": 2.0,
                },
            },
            state_file,
        )
        state_file.flush()
        state_file.close()

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.write(
            json.dumps(
                {
                    "day_key": "2026-03-17",
                    "type": "fill",
                    "ts": 1700000001,
                    "side": "BUY",
                    "notional": 48.8,
                    "realized_pnl": 0.0,
                    "source": "paper",
                }
            )
        )
        ledger_file.write("\n")
        ledger_file.write(
            json.dumps(
                {
                    "day_key": "2026-03-17",
                    "type": "fill",
                    "ts": 1700000300,
                    "side": "SELL",
                    "notional": 30.0,
                    "realized_pnl": 5.0,
                    "source": "broker_reconcile",
                }
            )
        )
        ledger_file.write("\n")
        ledger_file.write(
            json.dumps(
                {
                    "day_key": "2026-03-17",
                    "type": "account_sync",
                    "ts": 1700000400,
                }
            )
        )
        ledger_file.write("\n")
        ledger_file.flush()
        ledger_file.close()

        report = build_reconciliation_report_from_paths(
            state_path=state_file.name,
            ledger_path=ledger_file.name,
            day_key="2026-03-17",
            generated_ts=1700000500,
        )

        self.assertEqual(report["status"], "warn")
        self.assertEqual(report["ledger_summary"]["fill_count"], 2)
        self.assertEqual(report["ledger_summary"]["buy_fill_count"], 1)
        self.assertEqual(report["ledger_summary"]["sell_fill_count"], 1)
        self.assertAlmostEqual(float(report["ledger_summary"]["realized_pnl"]), 5.0, places=4)
        source_names = [row["source"] for row in report["ledger_summary"]["fill_by_source"]]
        self.assertIn("broker_reconcile", source_names)
        self.assertIn("paper", source_names)
        self.assertEqual(report["state_summary"]["pending_orders"], 1)
        self.assertEqual(report["status_label"], i18n_t("enum.reportStatus.warn"))

        text = render_reconciliation_report(report)

        self.assertIn(i18n_t("report.reconciliation.title"), text)
        self.assertIn(f"{i18n_t('report.reconciliation.field.status')}: {i18n_t('enum.reportStatus.warn')}", text)
        self.assertIn(f"{i18n_t('report.reconciliation.section.fill_by_source')}:", text)
        self.assertIn(i18n_t("report.reconciliation.source.broker_reconcile"), text)

    def test_build_report_from_sqlite_ledger_summarizes_fills(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            ledger_path = Path(tmpdir) / "ledger.db"
            append_ledger_entry(
                str(ledger_path),
                "fill",
                {
                    "ts": 1710000001,
                    "side": "BUY",
                    "notional": 18.0,
                    "realized_pnl": 0.0,
                    "source": "paper",
                },
                broker="PaperBroker",
            )
            append_ledger_entry(
                str(ledger_path),
                "fill",
                {
                    "ts": 1710000300,
                    "side": "SELL",
                    "notional": 22.5,
                    "realized_pnl": 3.5,
                    "source": "broker_reconcile",
                },
                broker="LiveBroker",
            )

            state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
            json.dump({"startup": {"ready": True, "warning_count": 0, "failure_count": 0, "checks": []}}, state_file)
            state_file.flush()
            state_file.close()

            report = build_reconciliation_report_from_paths(
                state_path=state_file.name,
                ledger_path=str(ledger_path),
                generated_ts=1710000400,
            )

            self.assertEqual(report["ledger_path"], str(ledger_path))
            self.assertEqual(report["ledger_summary"]["fill_count"], 2)
            self.assertEqual(report["ledger_summary"]["buy_fill_count"], 1)
            self.assertEqual(report["ledger_summary"]["sell_fill_count"], 1)
            self.assertAlmostEqual(float(report["ledger_summary"]["realized_pnl"]), 3.5, places=4)
            self.assertIn("broker_reconcile", [row["source"] for row in report["ledger_summary"]["fill_by_source"]])

    def test_build_report_filters_ledger_by_current_broker(self):
        state_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        json.dump(
            {
                "config": {"broker_name": "LiveClobBroker"},
                "startup": {"ready": True, "warning_count": 0, "failure_count": 0},
                "reconciliation": {
                    "day_key": "2026-03-18",
                    "status": "ok",
                    "issues": [],
                    "internal_realized_pnl": -0.15,
                    "ledger_realized_pnl": -0.15,
                    "broker_closed_pnl_today": -0.15,
                    "internal_vs_ledger_diff": 0.0,
                    "broker_floor_gap_vs_internal": 0.0,
                    "pending_orders": 0,
                    "open_positions": 0,
                },
                "summary": {
                    "open_positions": 0,
                    "tracked_notional_usd": 0.0,
                    "equity": 9.85,
                    "cash_balance_usd": 9.85,
                    "positions_value_usd": 0.0,
                    "internal_pnl_today": -0.15,
                    "broker_closed_pnl_today": -0.15,
                },
            },
            state_file,
        )
        state_file.flush()
        state_file.close()

        ledger_file = tempfile.NamedTemporaryFile("w", delete=False, encoding="utf-8")
        ledger_file.write(
            json.dumps(
                {
                    "day_key": "2026-03-18",
                    "type": "fill",
                    "ts": 1710000001,
                    "side": "SELL",
                    "notional": 119.625,
                    "realized_pnl": 42.9075,
                    "source": "paper",
                    "broker": "PaperBroker",
                }
            )
        )
        ledger_file.write("\n")
        ledger_file.write(
            json.dumps(
                {
                    "day_key": "2026-03-18",
                    "type": "fill",
                    "ts": 1710000300,
                    "side": "SELL",
                    "notional": 0.9,
                    "realized_pnl": -0.15,
                    "source": "broker_reconcile",
                    "broker": "LiveClobBroker",
                }
            )
        )
        ledger_file.write("\n")
        ledger_file.flush()
        ledger_file.close()

        report = build_reconciliation_report_from_paths(
            state_path=state_file.name,
            ledger_path=ledger_file.name,
            day_key="2026-03-18",
            generated_ts=1710000400,
        )

        self.assertEqual(report["ledger_summary"]["fill_count"], 1)
        self.assertEqual(report["ledger_summary"]["sell_fill_count"], 1)
        self.assertAlmostEqual(float(report["ledger_summary"]["realized_pnl"]), -0.15, places=4)
        self.assertEqual(report["ledger_summary"]["fill_by_source"][0]["source"], "broker_reconcile")
