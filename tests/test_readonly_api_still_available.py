from __future__ import annotations

import json
import unittest
from pathlib import Path
from unittest.mock import patch

from control_api_test_helpers import dispatch_request, make_minimal_handler


class ReadonlyApiAvailabilityTests(unittest.TestCase):
    def test_loopback_local_host_can_read_without_token(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="test-control-token-1234",
            enable_write_api=True,
            source_policy="local_only",
        )
        self.addCleanup(tmpdir.cleanup)

        status, payload, _ = dispatch_request(
            handler,
            method="GET",
            path="/api/state",
            client_ip="127.0.0.1",
            headers={"Host": "127.0.0.1:8787"},
        )
        self.assertEqual(status, 200)
        self.assertIn("summary", payload)

    def test_loopback_with_non_local_host_still_requires_token(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="test-control-token-1234",
            enable_write_api=True,
            source_policy="local_only",
        )
        self.addCleanup(tmpdir.cleanup)

        status, payload, _ = dispatch_request(
            handler,
            method="GET",
            path="/api/state",
            client_ip="127.0.0.1",
            headers={"Host": "example.trycloudflare.com"},
        )
        self.assertEqual(status, 401)
        self.assertEqual(str(payload.get("error_code") or ""), "unauthorized")

    def test_non_local_host_root_still_returns_protected_page(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="test-control-token-1234",
            enable_write_api=True,
            source_policy="local_only",
        )
        self.addCleanup(tmpdir.cleanup)

        status, _payload, _ = dispatch_request(
            handler,
            method="GET",
            path="/",
            client_ip="127.0.0.1",
            headers={"Host": "example.trycloudflare.com"},
        )
        self.assertEqual(status, 401)

    def test_readonly_get_endpoints_still_available(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="",
            enable_write_api=False,
            source_policy="local_only",
        )
        self.addCleanup(tmpdir.cleanup)

        status, payload, _ = dispatch_request(handler, method="GET", path="/api/state")
        self.assertEqual(status, 200)
        security = dict(payload.get("control_plane_security") or {})
        self.assertFalse(bool(security.get("write_api_available")))
        self.assertTrue(bool(security.get("readonly_mode")))
        self.assertIn("write_api_requested", security)
        self.assertIn("token_configured", security)
        self.assertIn("source_policy", security)
        serialized = json.dumps(security, ensure_ascii=False)
        self.assertNotIn("test-control-token", serialized)

    def test_write_api_available_single_source_visible_in_state_and_post_gate(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="",
            enable_write_api=True,
            source_policy="local_only",
        )
        self.addCleanup(tmpdir.cleanup)

        state_status, state_payload, _ = dispatch_request(handler, method="GET", path="/api/state")
        self.assertEqual(state_status, 200)
        security = dict(state_payload.get("control_plane_security") or {})
        self.assertTrue(bool(security.get("write_api_requested")))
        self.assertFalse(bool(security.get("write_api_available")))
        self.assertTrue(bool(security.get("readonly_mode")))

        post_status, post_payload, _ = dispatch_request(
            handler,
            method="POST",
            path="/api/control",
            payload={"command": "reduce_only", "value": True},
        )
        self.assertEqual(post_status, 503)
        self.assertEqual(str(post_payload.get("error_code") or ""), "writeApiDisabled")
        self.assertEqual(str(post_payload.get("reason_code") or ""), "control_token_missing")

    def test_readonly_mode_rejects_write_and_unlisted_get(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="",
            enable_write_api=False,
            source_policy="local_only",
        )
        self.addCleanup(tmpdir.cleanup)

        post_status, post_payload, _ = dispatch_request(
            handler,
            method="POST",
            path="/api/control",
            payload={"command": "reduce_only", "value": True},
        )
        self.assertEqual(post_status, 503)
        self.assertEqual(str(post_payload.get("error_code") or ""), "writeApiDisabled")

        get_status, get_payload, _ = dispatch_request(
            handler,
            method="GET",
            path="/api/not-allowed",
        )
        self.assertEqual(get_status, 404)
        self.assertEqual(str(get_payload.get("error_code") or ""), "notFound")

    def test_state_exposes_minimal_control_plane_security_fields_only(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="test-control-token-1234",
            enable_write_api=True,
            source_policy="local_only",
            trusted_proxy_cidrs="10.0.0.0/8",
        )
        self.addCleanup(tmpdir.cleanup)

        status, payload, _ = dispatch_request(handler, method="GET", path="/api/state", headers={"X-Auth-Token": "test-control-token-1234"})
        self.assertEqual(status, 200)
        security = dict(payload.get("control_plane_security") or {})
        required = {
            "token_configured",
            "write_api_requested",
            "write_api_available",
            "write_api_enabled",
            "readonly_mode",
            "live_mode",
            "source_policy",
            "trusted_proxy_configured",
            "reason_codes",
        }
        self.assertTrue(required.issubset(set(security.keys())))
        serialized = json.dumps(security, ensure_ascii=False).lower()
        self.assertNotIn("test-control-token-1234", serialized)
        self.assertNotIn("wallet_lock_path", serialized)
        self.assertNotIn("state_store_path", serialized)
        self.assertNotIn("trusted_proxy_cidrs", serialized)
        self.assertNotIn("host", serialized)
        self.assertNotIn("port", serialized)

    def test_readonly_get_requests_do_not_trigger_side_effect_writes(self) -> None:
        handler, tmpdir = make_minimal_handler(
            control_token="",
            enable_write_api=False,
            source_policy="local_only",
        )
        self.addCleanup(tmpdir.cleanup)
        public_state_path = Path(tmpdir.name) / "public_state.json"
        before = public_state_path.read_text(encoding="utf-8")

        with (
            patch("polymarket_bot.web._safe_write_json") as mocked_safe_write,
            patch("polymarket_bot.web._fetch_blockbeats_dashboard") as mocked_fetch_blockbeats,
        ):
            state_status, _, _ = dispatch_request(handler, method="GET", path="/api/state")
            blockbeats_status, _, _ = dispatch_request(handler, method="GET", path="/api/blockbeats")

        after = public_state_path.read_text(encoding="utf-8")
        self.assertEqual(state_status, 200)
        self.assertEqual(blockbeats_status, 200)
        self.assertEqual(before, after)
        mocked_safe_write.assert_not_called()
        mocked_fetch_blockbeats.assert_not_called()


if __name__ == "__main__":
    unittest.main()
