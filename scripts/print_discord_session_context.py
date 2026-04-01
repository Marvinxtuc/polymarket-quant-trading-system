#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path


CHANNEL_ID = "1483402853648302081"
PROJECT_ROOT = Path(__file__).resolve().parents[1]
SESSIONS_INDEX = Path(
    os.environ.get(
        "OPENCLAW_SESSIONS_INDEX",
        str(Path.home() / ".openclaw" / "agents" / "polymarket" / "sessions" / "sessions.json"),
    )
)


def main() -> int:
    if not SESSIONS_INDEX.exists():
        print(json.dumps({"error": "sessions_index_missing"}, ensure_ascii=False))
        return 1
    try:
        payload = json.loads(SESSIONS_INDEX.read_text(encoding="utf-8"))
    except Exception:
        print(json.dumps({"error": "sessions_index_unreadable"}, ensure_ascii=False))
        return 1
    if not isinstance(payload, dict):
        print(json.dumps({"error": "sessions_index_invalid"}, ensure_ascii=False))
        return 1
    for key, value in payload.items():
        if not isinstance(value, dict):
            continue
        if ":acp:binding:discord:default:" not in str(key):
            continue
        if str(value.get("groupId") or "") != CHANNEL_ID:
            continue
        acp = value.get("acp")
        if not isinstance(acp, dict):
            acp = {}
        identity = acp.get("identity")
        if not isinstance(identity, dict):
            identity = {}
        out = {
            "workspace": str(acp.get("cwd") or os.environ.get("POLYMARKET_WORKSPACE") or PROJECT_ROOT),
            "discord_channel_id": str(value.get("groupId") or ""),
            "discord_channel_name": str(value.get("groupChannel") or ""),
            "openclaw_session_key": key,
            "openclaw_session_id": str(value.get("sessionId") or ""),
            "acp_backend": str(acp.get("backend") or ""),
            "acp_agent": str(acp.get("agent") or ""),
            "acp_mode": str(acp.get("mode") or ""),
            "acpx_record_id": str(identity.get("acpxRecordId") or ""),
            "acpx_session_id": str(identity.get("acpxSessionId") or ""),
            "updated_at": int(value.get("updatedAt") or 0),
        }
        print(json.dumps(out, ensure_ascii=False, indent=2))
        return 0
    print(json.dumps({"error": "discord_acp_binding_not_found"}, ensure_ascii=False))
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
