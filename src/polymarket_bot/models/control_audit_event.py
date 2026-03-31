from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True)
class ControlAuditEvent:
    ts: int
    method: str
    path: str
    action: str
    status: str
    reason_code: str
    http_status: int
    source_ip: str
    client_ip: str
    xff_used: bool
    write_api_available: bool
    live_mode: bool
    authorized: bool
