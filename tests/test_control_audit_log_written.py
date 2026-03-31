from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest.mock import patch

from control_api_test_helpers import dispatch_request, make_minimal_handler


class ControlAuditLogTests(unittest.TestCase):
    def test_audit_log_contains_rejected_and_success_write_events(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="test-control-token-1234",
            enable_write_api=True,
            source_policy="local_only",
        )
        self.addCleanup(tmpdir.cleanup)
        audit_path = Path(tmpdir.name) / "control_audit_events.jsonl"

        rejected_status, _, _ = dispatch_request(
            handler,
            method="POST",
            path="/api/control",
            payload={"command": "reduce_only", "value": True},
            headers={"X-Auth-Token": "bad-token"},
        )
        success_status, _, _ = dispatch_request(
            handler,
            method="POST",
            path="/api/control",
            payload={"command": "reduce_only", "value": True},
            headers={"X-Auth-Token": "test-control-token-1234"},
        )

        self.assertEqual(rejected_status, 401)
        self.assertEqual(success_status, 200)
        self.assertTrue(audit_path.exists())

        rows = []
        for raw in audit_path.read_text(encoding="utf-8").splitlines():
            if not raw.strip():
                continue
            rows.append(json.loads(raw))

        self.assertGreaterEqual(len(rows), 2)
        statuses = {str(row.get("status") or "") for row in rows}
        self.assertIn("rejected", statuses)
        self.assertTrue("success" in statuses or "accepted" in statuses)
        reason_codes = {str(row.get("reason_code") or "") for row in rows}
        self.assertIn("control_token_invalid", reason_codes)
        serialized = "\n".join(json.dumps(row, ensure_ascii=False) for row in rows)
        self.assertNotIn("test-control-token-1234", serialized)
        self.assertNotIn("bad-token", serialized)

    def test_denied_requests_are_audited_for_missing_token_and_source_not_allowed(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="test-control-token-1234",
            enable_write_api=True,
            source_policy="local_only",
        )
        self.addCleanup(tmpdir.cleanup)
        audit_path = Path(tmpdir.name) / "control_audit_events.jsonl"

        missing_status, _, _ = dispatch_request(
            handler,
            method="POST",
            path="/api/control",
            payload={"command": "reduce_only", "value": True},
        )
        source_status, _, _ = dispatch_request(
            handler,
            method="POST",
            path="/api/control",
            client_ip="198.51.100.9",
            payload={"command": "reduce_only", "value": True},
            headers={"X-Auth-Token": "test-control-token-1234"},
        )

        self.assertEqual(missing_status, 401)
        self.assertEqual(source_status, 403)
        rows = [json.loads(line) for line in audit_path.read_text(encoding="utf-8").splitlines() if line.strip()]
        reason_codes = {str(row.get("reason_code") or "") for row in rows}
        self.assertIn("control_token_missing", reason_codes)
        self.assertIn("source_not_allowed", reason_codes)
        statuses = {str(row.get("status") or "") for row in rows}
        self.assertIn("rejected", statuses)

    def test_audit_write_failure_blocks_write_request(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="test-control-token-1234",
            enable_write_api=True,
            source_policy="local_only",
        )
        self.addCleanup(tmpdir.cleanup)

        with patch("polymarket_bot.web._safe_append_jsonl", side_effect=OSError("audit disk full")):
            status, payload, _ = dispatch_request(
                handler,
                method="POST",
                path="/api/control",
                payload={"command": "reduce_only", "value": True},
                headers={"X-Auth-Token": "test-control-token-1234"},
            )
        self.assertEqual(status, 503)
        self.assertEqual(str(payload.get("reason_code") or ""), "control_audit_write_failed")

        control_status, control_payload, _ = dispatch_request(
            handler,
            method="GET",
            path="/api/control",
            headers={"X-Auth-Token": "test-control-token-1234"},
        )
        self.assertEqual(control_status, 200)
        self.assertFalse(bool(control_payload.get("reduce_only")))


if __name__ == "__main__":
    unittest.main()
