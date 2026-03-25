#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.config import Settings
from polymarket_bot.i18n import t as i18n_t
from polymarket_bot.notifier import Notifier


def _alert_t(key: str, params: dict[str, object] | None = None, *, fallback: str = "") -> str:
    return i18n_t(f"report.alertDelivery.{key}", params or {}, fallback=fallback)


def build_report(
    *,
    settings: Settings,
    send_remote: bool,
    include_local: bool,
    title: str,
    body: str,
) -> tuple[dict[str, object], int]:
    notifier = Notifier(
        local_enabled=bool(getattr(settings, "notify_local_enabled", True)),
        webhook_url=str(getattr(settings, "notify_webhook_url", "") or ""),
        webhook_urls=str(getattr(settings, "notify_webhook_urls", "") or ""),
        telegram_bot_token=str(getattr(settings, "notify_telegram_bot_token", "") or ""),
        telegram_chat_id=str(getattr(settings, "notify_telegram_chat_id", "") or ""),
        telegram_api_base=str(getattr(settings, "notify_telegram_api_base", "") or ""),
        telegram_parse_mode=str(getattr(settings, "notify_telegram_parse_mode", "") or ""),
        log_path=str(getattr(settings, "notify_log_path", "") or ""),
    )
    summary = notifier.summary(limit=5)
    remote_channels: list[str] = []
    if bool(summary.get("webhook_configured")):
        remote_channels.append("webhook")
    if bool(summary.get("telegram_configured")):
        remote_channels.append("telegram")

    status = "blocked"
    blockers: list[str] = []
    deliveries: dict[str, object] = {}
    exit_code = 1

    if not remote_channels:
        blockers.append(_alert_t("blocker.remoteChannelNotConfigured", fallback="remote alert channel not configured"))
    elif not send_remote:
        status = "ready_to_send"
        exit_code = 0
    else:
        channels = list(remote_channels)
        if include_local and bool(summary.get("local_available")):
            channels.insert(0, "local")
        deliveries = notifier.notify_all(
            title=title,
            body=body,
            extra={
                "kind": "alert_delivery_smoke",
                "ts": int(time.time()),
                "send_remote": True,
            },
            channels=channels,
        )
        if bool(deliveries.get("ok")):
            status = "sent"
            exit_code = 0
        else:
            status = "failed"
            blockers.append(
                _alert_t(
                    "blocker.deliveryFailed",
                    fallback="one or more configured alert channels failed delivery",
                )
            )

    report: dict[str, object] = {
        "generated_at": int(time.time()),
        "status": status,
        "status_label": _alert_t(f"status.{status}", fallback=status.upper()),
        "send_remote": bool(send_remote),
        "include_local": bool(include_local),
        "title": str(title or ""),
        "body": str(body or ""),
        "dry_run": bool(settings.dry_run),
        "notify_log_path": str(getattr(settings, "notify_log_path", "") or ""),
        "webhook_configured": bool(summary.get("webhook_configured")),
        "telegram_configured": bool(summary.get("telegram_configured")),
        "local_available": bool(summary.get("local_available")),
        "remote_channels": remote_channels,
        "blockers": blockers,
        "summary": summary,
    }
    if deliveries:
        report["delivery_result"] = deliveries
    return report, exit_code


def main() -> int:
    parser = argparse.ArgumentParser(description="Preflight or send a Polymarket remote alert delivery smoke test.")
    parser.add_argument("--send-remote", action="store_true", help="Actually send remote webhook/telegram test notifications.")
    parser.add_argument("--include-local", action="store_true", help="Also send the local notification channel when available.")
    parser.add_argument("--title", default="Polymarket Alert Smoke", help="Alert title.")
    parser.add_argument("--body", default="Remote alert delivery smoke test.", help="Alert body.")
    parser.add_argument("--report-path", default="", help="Optional output path for the JSON report.")
    args = parser.parse_args()

    settings = Settings()
    report, exit_code = build_report(
        settings=settings,
        send_remote=bool(args.send_remote),
        include_local=bool(args.include_local),
        title=str(args.title or ""),
        body=str(args.body or ""),
    )
    report_path = str(args.report_path or settings.runtime_store_path("alert_delivery_smoke.json")).strip()
    Path(report_path).expanduser().parent.mkdir(parents=True, exist_ok=True)
    Path(report_path).expanduser().write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(report_path)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
