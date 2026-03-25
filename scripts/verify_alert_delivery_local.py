#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.config import Settings
from polymarket_bot.i18n import t as i18n_t


def _alert_local_t(key: str, params: dict[str, object] | None = None, *, fallback: str = "") -> str:
    return i18n_t(f"report.alertDeliveryLocal.{key}", params or {}, fallback=fallback)


class _SinkHandler(BaseHTTPRequestHandler):
    payload_path: Path | None = None

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0") or 0)
        payload = self.rfile.read(length)
        if self.payload_path is not None:
            self.payload_path.parent.mkdir(parents=True, exist_ok=True)
            self.payload_path.write_bytes(payload)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok": true}')

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local webhook sink and verify notifier end-to-end delivery.")
    parser.add_argument("--port", type=int, default=18999, help="Local webhook sink port.")
    parser.add_argument("--report-path", default="", help="Optional JSON report path.")
    parser.add_argument("--payload-path", default="", help="Optional captured payload path.")
    parser.add_argument("--title", default="Polymarket Alert Smoke", help="Alert title.")
    parser.add_argument("--body", default="Remote alert delivery smoke test.", help="Alert body.")
    args = parser.parse_args()

    settings = Settings()
    runtime_dir = Path(settings.runtime_namespace_dir()).expanduser()
    report_path = Path(str(args.report_path or runtime_dir / "alert_delivery_smoke_local.json")).expanduser()
    payload_path = Path(str(args.payload_path or runtime_dir / "alert_smoke_local_payload.json")).expanduser()
    report_path.parent.mkdir(parents=True, exist_ok=True)
    payload_path.parent.mkdir(parents=True, exist_ok=True)

    handler = type("SinkHandler", (_SinkHandler,), {"payload_path": payload_path})
    server = HTTPServer(("127.0.0.1", int(args.port)), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        env = os.environ.copy()
        env.update(
            {
                "PYTHONPATH": str(SRC),
                "POLY_NOTIFY_WEBHOOK_URL": f"http://127.0.0.1:{int(args.port)}/ops",
                "NOTIFY_LOCAL_ENABLED": "false",
            }
        )
        proc = subprocess.run(
            [
                str(ROOT / ".venv" / "bin" / "python"),
                str(ROOT / "scripts" / "verify_alert_delivery.py"),
                "--send-remote",
                "--report-path",
                str(report_path),
                "--title",
                str(args.title or ""),
                "--body",
                str(args.body or ""),
            ],
            cwd=ROOT,
            env=env,
            text=True,
            capture_output=True,
        )
    finally:
        server.shutdown()
        server.server_close()

    payload_text = payload_path.read_text(encoding="utf-8") if payload_path.exists() else ""
    report: dict[str, object] = {
        "status": "sent" if proc.returncode == 0 else "failed",
        "status_label": _alert_local_t(
            f"status.{'sent' if proc.returncode == 0 else 'failed'}",
            fallback="SENT" if proc.returncode == 0 else "FAILED",
        ),
        "returncode": int(proc.returncode),
        "report_path": str(report_path),
        "payload_path": str(payload_path),
        "webhook_url": f"http://127.0.0.1:{int(args.port)}/ops",
        "payload_present": bool(payload_text),
        "stdout": proc.stdout,
        "stderr": proc.stderr,
    }
    summary_path = report_path.with_name(report_path.stem + "_summary.json")
    summary_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(summary_path)
    return proc.returncode


if __name__ == "__main__":
    raise SystemExit(main())
