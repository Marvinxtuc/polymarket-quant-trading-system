from __future__ import annotations

import io
import json
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from polymarket_bot.web import build_handler


class FakeSocket:
    def __init__(self, request_bytes: bytes):
        self._rfile = io.BytesIO(request_bytes)
        self._wfile = io.BytesIO()

    def makefile(self, mode: str, *args, **kwargs):
        if "r" in mode:
            return self._rfile
        return self._wfile

    def sendall(self, data: bytes) -> None:
        self._wfile.write(data)

    def close(self) -> None:
        return None

    @property
    def raw_response(self) -> str:
        return self._wfile.getvalue().decode("utf-8", errors="ignore")


def dispatch_request(
    handler_cls,
    *,
    method: str,
    path: str,
    client_ip: str = "127.0.0.1",
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
) -> tuple[int, dict[str, Any], str]:
    body_bytes = b""
    if payload is not None:
        body_bytes = json.dumps(payload, ensure_ascii=False).encode("utf-8")

    request_headers = dict(headers or {})
    host = str(request_headers.pop("Host", "localhost"))
    header_lines = [f"{method} {path} HTTP/1.1", f"Host: {host}"]
    for key, value in request_headers.items():
        header_lines.append(f"{key}: {value}")
    if body_bytes:
        header_lines.append("Content-Type: application/json")
        header_lines.append(f"Content-Length: {len(body_bytes)}")
    header_lines.append("")
    header_lines.append("")
    request_bytes = ("\r\n".join(header_lines)).encode("utf-8") + body_bytes

    sock = FakeSocket(request_bytes)
    server = SimpleNamespace(server_name="localhost", server_port=8787)
    handler_cls(sock, (client_ip, 12345), server)

    raw = sock.raw_response
    header_text, body = raw.split("\r\n\r\n", 1)
    status = int(header_text.splitlines()[0].split(" ")[1])
    try:
        parsed = json.loads(body) if body else {}
    except json.JSONDecodeError:
        parsed = {}
    return status, parsed, header_text


def make_minimal_handler(
    *,
    control_token: str,
    enable_write_api: bool,
    live_mode: bool = False,
    source_policy: str = "local_only",
    trusted_proxy_cidrs: str = "",
    control_audit_log_path: str = "",
) -> tuple[Any, tempfile.TemporaryDirectory]:
    tmpdir = tempfile.TemporaryDirectory()
    root = Path(tmpdir.name)

    frontend_dir = root / "frontend"
    frontend_dir.mkdir(parents=True, exist_ok=True)
    (frontend_dir / "index.html").write_text("ok", encoding="utf-8")

    state_path = root / "state.json"
    state_path.write_text(json.dumps({"summary": {"open_positions": 0}}, ensure_ascii=False), encoding="utf-8")
    control_path = root / "control.json"
    control_path.write_text(json.dumps({"pause_opening": False}, ensure_ascii=False), encoding="utf-8")
    monitor_30m = root / "monitor30.json"
    monitor_30m.write_text("{}", encoding="utf-8")
    monitor_12h = root / "monitor12.json"
    monitor_12h.write_text("{}", encoding="utf-8")
    recon_json = root / "recon.json"
    recon_json.write_text("{}", encoding="utf-8")
    recon_txt = root / "recon.txt"
    recon_txt.write_text("", encoding="utf-8")
    ledger_path = root / "ledger.jsonl"
    ledger_path.write_text("", encoding="utf-8")
    state_store_path = root / "state.db"
    public_state_path = root / "public_state.json"
    public_state_path.write_text(json.dumps({"summary": {"open_positions": 0}}, ensure_ascii=False), encoding="utf-8")
    if not control_audit_log_path:
        control_audit_log_path = str(root / "control_audit_events.jsonl")

    handler_cls = build_handler(
        str(frontend_dir),
        str(state_path),
        str(control_path),
        control_token,
        str(monitor_30m),
        str(monitor_12h),
        str(recon_json),
        str(recon_txt),
        str(ledger_path),
        public_state_path=str(public_state_path),
        state_store_path=str(state_store_path),
        enable_write_api=enable_write_api,
        write_api_requested=enable_write_api,
        writer_scope="paper:default",
        live_mode=live_mode,
        control_source_policy=source_policy,
        trusted_proxy_cidrs=trusted_proxy_cidrs,
        allow_read_side_effects=enable_write_api,
        control_token_min_length=16,
        control_audit_log_path=str(control_audit_log_path),
    )
    return handler_cls, tmpdir
