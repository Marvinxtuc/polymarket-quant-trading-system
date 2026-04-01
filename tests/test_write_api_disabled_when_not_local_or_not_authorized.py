from __future__ import annotations

import unittest

from control_api_test_helpers import dispatch_request, make_minimal_handler


class WriteApiSourceAndAvailabilityTests(unittest.TestCase):
    def test_non_local_source_is_rejected_even_with_valid_token(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="test-control-token-1234",
            enable_write_api=True,
            source_policy="local_only",
        )
        self.addCleanup(tmpdir.cleanup)

        status, payload, _ = dispatch_request(
            handler,
            method="POST",
            path="/api/control",
            client_ip="198.51.100.20",
            payload={"command": "reduce_only", "value": True},
            headers={"X-Auth-Token": "test-control-token-1234"},
        )
        self.assertEqual(status, 403)
        self.assertEqual(str(payload.get("reason_code") or ""), "source_not_allowed")
        self.assertFalse(bool(payload.get("ok")))

    def test_write_api_disabled_when_token_is_missing(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="",
            enable_write_api=True,
            source_policy="local_only",
        )
        self.addCleanup(tmpdir.cleanup)

        status, payload, _ = dispatch_request(
            handler,
            method="POST",
            path="/api/control",
            payload={"command": "reduce_only", "value": True},
        )
        self.assertEqual(status, 503)
        self.assertEqual(str(payload.get("error_code") or ""), "writeApiDisabled")
        self.assertIn(str(payload.get("reason_code") or ""), {"control_token_missing", "write_api_disabled"})

    def test_unlisted_write_path_is_rejected_by_method_path_whitelist(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="test-control-token-1234",
            enable_write_api=True,
            source_policy="local_only",
        )
        self.addCleanup(tmpdir.cleanup)

        status, payload, _ = dispatch_request(
            handler,
            method="POST",
            path="/api/state",
            payload={"noop": True},
            headers={"X-Auth-Token": "test-control-token-1234"},
        )
        self.assertEqual(status, 404)
        self.assertEqual(str(payload.get("reason_code") or ""), "write_route_not_allowed")

    def test_xff_is_ignored_without_trusted_proxy_config(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="test-control-token-1234",
            enable_write_api=True,
            source_policy="local_only",
            trusted_proxy_cidrs="",
        )
        self.addCleanup(tmpdir.cleanup)

        status, payload, _ = dispatch_request(
            handler,
            method="POST",
            path="/api/control",
            client_ip="198.51.100.30",
            payload={"command": "reduce_only", "value": True},
            headers={
                "X-Auth-Token": "test-control-token-1234",
                "X-Forwarded-For": "127.0.0.1",
            },
        )
        self.assertEqual(status, 403)
        self.assertEqual(str(payload.get("reason_code") or ""), "source_not_allowed")

    def test_trusted_proxy_can_forward_client_ip_when_explicitly_configured(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="test-control-token-1234",
            enable_write_api=True,
            source_policy="local_only",
            trusted_proxy_cidrs="10.0.0.0/8",
        )
        self.addCleanup(tmpdir.cleanup)

        status, payload, _ = dispatch_request(
            handler,
            method="POST",
            path="/api/control",
            client_ip="10.0.0.5",
            payload={"command": "reduce_only", "value": True},
            headers={
                "X-Auth-Token": "test-control-token-1234",
                "X-Forwarded-For": "127.0.0.1",
            },
        )
        self.assertEqual(status, 200)
        self.assertTrue(bool(payload.get("reduce_only")))


if __name__ == "__main__":
    unittest.main()
