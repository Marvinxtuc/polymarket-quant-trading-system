from __future__ import annotations

import unittest

from control_api_test_helpers import dispatch_request, make_minimal_handler


class ControlApiRequiresTokenTests(unittest.TestCase):
    def test_missing_and_wrong_token_are_rejected(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="test-control-token-1234",
            enable_write_api=True,
            live_mode=False,
        )
        self.addCleanup(tmpdir.cleanup)

        missing_status, missing_payload, _ = dispatch_request(
            handler,
            method="POST",
            path="/api/control",
            payload={"command": "reduce_only", "value": True},
        )
        wrong_status, wrong_payload, _ = dispatch_request(
            handler,
            method="POST",
            path="/api/control",
            payload={"command": "reduce_only", "value": True},
            headers={"X-Auth-Token": "wrong-token"},
        )
        ok_status, ok_payload, _ = dispatch_request(
            handler,
            method="POST",
            path="/api/control",
            payload={"command": "reduce_only", "value": True},
            headers={"X-Auth-Token": "test-control-token-1234"},
        )

        self.assertEqual(missing_status, 401)
        self.assertEqual(str(missing_payload.get("reason_code") or ""), "control_token_missing")
        self.assertEqual(wrong_status, 401)
        self.assertEqual(str(wrong_payload.get("reason_code") or ""), "control_token_invalid")
        self.assertEqual(ok_status, 200)
        self.assertTrue(bool(ok_payload.get("reduce_only")))


if __name__ == "__main__":
    unittest.main()
