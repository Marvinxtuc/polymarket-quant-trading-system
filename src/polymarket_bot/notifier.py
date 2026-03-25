from __future__ import annotations

import json
import os
import shutil
import subprocess
import time
from collections import deque
from pathlib import Path
from typing import Any
from urllib import request
from urllib.parse import urlsplit

from polymarket_bot.i18n import t as i18n_t


def _split_targets(raw: str) -> list[str]:
    values: list[str] = []
    text = str(raw or "").replace("\n", ",").replace(";", ",")
    for chunk in text.split(","):
        value = chunk.strip()
        if value and value not in values:
            values.append(value)
    return values


def _mask_middle(value: str, *, keep: int = 4) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= keep * 2:
        return "*" * max(4, len(text))
    return f"{text[:keep]}…{text[-keep:]}"


def _redact_url(url: str) -> str:
    parsed = urlsplit(str(url or "").strip())
    if not parsed.scheme or not parsed.netloc:
        return str(url or "").strip()
    parts = [part for part in parsed.path.split("/") if part]
    if not parts:
        return f"{parsed.scheme}://{parsed.netloc}"
    visible = "/".join(parts[:2])
    if len(parts) > 2:
        visible = f"{visible}/…"
    return f"{parsed.scheme}://{parsed.netloc}/{visible}"


def _default_log_path() -> str:
    return os.getenv("POLY_NOTIFIER_LOG_PATH", "/tmp/poly_runtime_data/notifier_events.jsonl")


def _default_notification_title() -> str:
    return i18n_t("notifier.defaultTitle", fallback="Polymarket")


def _delivery_detail(code: str) -> str:
    return i18n_t(f"notifier.delivery.{code}", fallback=code)


def _delivery_failure_detail(exc: Exception) -> str:
    return i18n_t(
        "notifier.delivery.deliveryFailed",
        {"reason": str(exc)},
        fallback=f"delivery failed: {exc}",
    )


class Notifier:
    def __init__(
        self,
        *,
        webhook_url: str = "",
        webhook_urls: str = "",
        telegram_bot_token: str = "",
        telegram_chat_id: str = "",
        telegram_api_base: str = "",
        telegram_parse_mode: str = "",
        log_path: str = "",
        local_enabled: bool = True,
    ) -> None:
        self.webhook_url = str(webhook_url or os.getenv("POLY_NOTIFY_WEBHOOK_URL", "")).strip()
        self.webhook_urls = _split_targets(
            webhook_urls or os.getenv("POLY_NOTIFY_WEBHOOK_URLS", "")
        )
        self.telegram_bot_token = str(
            telegram_bot_token or os.getenv("POLY_NOTIFY_TELEGRAM_BOT_TOKEN", "")
        ).strip()
        self.telegram_chat_id = str(telegram_chat_id or os.getenv("POLY_NOTIFY_TELEGRAM_CHAT_ID", "")).strip()
        self.telegram_api_base = str(
            telegram_api_base or os.getenv("POLY_NOTIFY_TELEGRAM_API_BASE", "https://api.telegram.org")
        ).strip()
        self.telegram_parse_mode = str(
            telegram_parse_mode or os.getenv("POLY_NOTIFY_TELEGRAM_PARSE_MODE", "")
        ).strip()
        self.log_path = str(log_path or _default_log_path()).strip()
        self.local_enabled = bool(local_enabled)

    def close(self) -> None:
        return None

    def __enter__(self) -> Notifier:
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @staticmethod
    def local_channel() -> str:
        if shutil.which("terminal-notifier"):
            return "terminal-notifier"
        if shutil.which("osascript"):
            return "osascript"
        if shutil.which("notify-send"):
            return "notify-send"
        return ""

    def local_available(self) -> bool:
        return self.local_enabled and bool(self.local_channel())

    def webhook_targets(self) -> list[str]:
        targets: list[str] = []
        for raw in [self.webhook_url, *self.webhook_urls]:
            value = str(raw or "").strip()
            if value and value not in targets:
                targets.append(value)
        return targets

    def telegram_available(self) -> bool:
        return bool(self.telegram_bot_token and self.telegram_chat_id)

    @staticmethod
    def _event_ok(payload: dict[str, Any]) -> bool:
        return bool(payload.get("ok"))

    @staticmethod
    def _event_delivery_count(payload: dict[str, Any]) -> int:
        try:
            count = int(payload.get("delivery_count") or 0)
        except (TypeError, ValueError):
            count = 0
        if count > 0:
            return count
        deliveries = payload.get("deliveries")
        if isinstance(deliveries, list) and deliveries:
            return len(deliveries)
        return 1

    @staticmethod
    def _event_channel(payload: dict[str, Any]) -> str:
        return str(payload.get("channel") or "unknown").strip().lower() or "unknown"

    def _append_event(self, payload: dict[str, Any]) -> dict[str, Any]:
        record = dict(payload)
        record.setdefault("ts", int(time.time()))
        parent = Path(self.log_path).expanduser().parent
        if str(parent) not in {"", "."}:
            parent.mkdir(parents=True, exist_ok=True)
        with open(self.log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        return record

    def notify_local(self, *, title: str, body: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        channel = self.local_channel()
        payload = {
            "channel": "local",
            "backend": channel or "unavailable",
            "title": str(title or ""),
            "body": str(body or ""),
            "extra": dict(extra or {}),
            "ok": False,
            "delivery_count": 1,
            "deliveries": [{"target": "local", "backend": channel or "unavailable", "ok": False}],
        }
        if not self.local_enabled:
            payload["detail_code"] = "local_disabled"
            payload["detail"] = _delivery_detail("localDisabled")
            return self._append_event(payload)
        if not channel:
            payload["detail_code"] = "local_backend_unavailable"
            payload["detail"] = _delivery_detail("localBackendUnavailable")
            return self._append_event(payload)
        try:
            if channel == "terminal-notifier":
                subprocess.run(
                    [channel, "-title", str(title or _default_notification_title()), "-message", str(body or "")],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            elif channel == "osascript":
                script = (
                    f"display notification {json.dumps(str(body or ''))} "
                    f"with title {json.dumps(str(title or _default_notification_title()))}"
                )
                subprocess.run(
                    [channel, "-e", script],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            else:
                subprocess.run(
                    [channel, str(title or _default_notification_title()), str(body or "")],
                    check=False,
                    stdout=subprocess.DEVNULL,
                    stderr=subprocess.DEVNULL,
                )
            payload["ok"] = True
            payload["deliveries"] = [{"target": "local", "backend": channel, "ok": True}]
        except Exception as exc:
            payload["detail_code"] = "delivery_failed"
            payload["detail"] = _delivery_failure_detail(exc)
        return self._append_event(payload)

    def _notify_single_webhook(
        self,
        target_url: str,
        *,
        title: str,
        body: str,
        extra: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        body_payload = {
            "title": str(title or ""),
            "body": str(body or ""),
            "extra": dict(extra or {}),
            "source": "polymarket-personal-terminal",
            "channel": "webhook",
            "ts": int(time.time()),
        }
        req = request.Request(
            target_url,
            data=json.dumps(body_payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        result = {
            "target": _redact_url(target_url),
            "ok": False,
        }
        try:
            with request.urlopen(req, timeout=5) as resp:
                result["status_code"] = int(getattr(resp, "status", 0) or 0)
                result["ok"] = 200 <= int(result["status_code"] or 0) < 300
        except Exception as exc:
            result["detail_code"] = "delivery_failed"
            result["detail"] = _delivery_failure_detail(exc)
        return result

    def notify_webhook(self, *, title: str, body: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        targets = self.webhook_targets()
        payload = {
            "channel": "webhook",
            "title": str(title or ""),
            "body": str(body or ""),
            "ok": False,
            "delivery_count": len(targets) if targets else 1,
            "deliveries": [],
            "targets": [_redact_url(target) for target in targets],
        }
        if not targets:
            payload["detail_code"] = "webhook_not_configured"
            payload["detail"] = _delivery_detail("webhookNotConfigured")
            return self._append_event(payload)
        deliveries: list[dict[str, Any]] = []
        ok_count = 0
        for target in targets:
            result = self._notify_single_webhook(target, title=title, body=body, extra=extra)
            deliveries.append(result)
            if bool(result.get("ok")):
                ok_count += 1
        payload["deliveries"] = deliveries
        payload["ok"] = ok_count == len(targets)
        payload["delivery_count"] = len(targets)
        payload["success_count"] = ok_count
        payload["failure_count"] = len(targets) - ok_count
        if deliveries:
            payload["status_code"] = deliveries[-1].get("status_code", 0)
        return self._append_event(payload)

    def notify_telegram(self, *, title: str, body: str, extra: dict[str, Any] | None = None) -> dict[str, Any]:
        payload = {
            "channel": "telegram",
            "title": str(title or ""),
            "body": str(body or ""),
            "extra": dict(extra or {}),
            "ok": False,
            "delivery_count": 1,
            "deliveries": [],
            "chat_id": _mask_middle(self.telegram_chat_id, keep=2),
            "api_base": self.telegram_api_base or "https://api.telegram.org",
        }
        if not self.telegram_available():
            payload["detail_code"] = "telegram_not_configured"
            payload["detail"] = _delivery_detail("telegramNotConfigured")
            return self._append_event(payload)
        api_base = (self.telegram_api_base or "https://api.telegram.org").rstrip("/")
        url = f"{api_base}/bot{self.telegram_bot_token}/sendMessage"
        message = f"{str(title or _default_notification_title())}\n{str(body or '')}".strip()
        request_payload: dict[str, Any] = {
            "chat_id": self.telegram_chat_id,
            "text": message,
            "disable_web_page_preview": True,
        }
        if self.telegram_parse_mode:
            request_payload["parse_mode"] = self.telegram_parse_mode
        req = request.Request(
            url,
            data=json.dumps(request_payload, ensure_ascii=False).encode("utf-8"),
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=5) as resp:
                payload["status_code"] = int(getattr(resp, "status", 0) or 0)
                payload["ok"] = 200 <= int(payload["status_code"] or 0) < 300
                try:
                    parsed = json.loads((resp.read() or b"{}").decode("utf-8"))
                    if isinstance(parsed, dict):
                        payload["telegram_ok"] = bool(parsed.get("ok", payload["ok"]))
                        payload["telegram_result"] = {
                            "ok": bool(parsed.get("ok", payload["ok"])),
                            "description": str(parsed.get("description") or ""),
                            "message_id": (
                                parsed.get("result", {}).get("message_id")
                                if isinstance(parsed.get("result"), dict)
                                else None
                            ),
                        }
                except Exception:
                    pass
                payload["deliveries"] = [{"target": payload["chat_id"], "ok": payload["ok"], "status_code": payload.get("status_code", 0)}]
        except Exception as exc:
            localized_detail = _delivery_failure_detail(exc)
            payload["detail_code"] = "delivery_failed"
            payload["detail"] = localized_detail
            payload["deliveries"] = [{"target": payload["chat_id"], "ok": False, "detail_code": "delivery_failed", "detail": localized_detail}]
        return self._append_event(payload)

    def notify_all(
        self,
        *,
        title: str,
        body: str,
        extra: dict[str, Any] | None = None,
        channels: list[str] | tuple[str, ...] | None = None,
    ) -> dict[str, Any]:
        requested = [str(channel).strip().lower() for channel in (channels or ["local", "webhook", "telegram"])]
        results: list[dict[str, Any]] = []
        for channel in requested:
            if channel == "local":
                results.append(self.notify_local(title=title, body=body, extra=extra))
            elif channel == "webhook":
                results.append(self.notify_webhook(title=title, body=body, extra=extra))
            elif channel == "telegram":
                results.append(self.notify_telegram(title=title, body=body, extra=extra))
        ok_count = sum(1 for row in results if self._event_ok(row))
        return {
            "channel": "multi",
            "title": str(title or ""),
            "body": str(body or ""),
            "extra": dict(extra or {}),
            "ok": ok_count == len(results) if results else False,
            "delivery_count": sum(self._event_delivery_count(row) for row in results),
            "deliveries": results,
            "target_channels": requested,
            "success_count": ok_count,
            "failure_count": len(results) - ok_count,
        }

    def recent(self, *, limit: int = 5) -> list[dict[str, Any]]:
        resolved_limit = max(1, int(limit))
        if not self.log_path or not os.path.exists(self.log_path):
            return []
        rows: deque[dict[str, Any]] = deque(maxlen=resolved_limit)
        with open(self.log_path, "r", encoding="utf-8") as f:
            for raw in f:
                text = raw.strip()
                if not text:
                    continue
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    rows.append(payload)
        return list(reversed(list(rows)))

    def summary(self, *, limit: int = 5) -> dict[str, Any]:
        recent = self.recent(limit=limit)
        by_channel: dict[str, dict[str, Any]] = {}
        total_deliveries = 0
        ok_events = 0
        failure_events = 0
        for event in recent:
            channel = self._event_channel(event)
            count = self._event_delivery_count(event)
            total_deliveries += count
            if self._event_ok(event):
                ok_events += 1
            else:
                failure_events += 1
            channel_stats = by_channel.setdefault(
                channel,
                {
                    "events": 0,
                    "deliveries": 0,
                    "ok": 0,
                    "failed": 0,
                },
            )
            channel_stats["events"] += 1
            channel_stats["deliveries"] += count
            if self._event_ok(event):
                channel_stats["ok"] += 1
            else:
                channel_stats["failed"] += 1
        last_success = next((event for event in recent if self._event_ok(event)), {})
        last_failure = next((event for event in recent if not self._event_ok(event)), {})
        return {
            "local_available": self.local_available(),
            "webhook_configured": bool(self.webhook_targets()),
            "telegram_configured": self.telegram_available(),
            "channels": [
                {
                    "name": "local",
                    "configured": bool(self.local_enabled),
                    "available": self.local_available(),
                    "backend": self.local_channel() or "unavailable",
                },
                {
                    "name": "webhook",
                    "configured": bool(self.webhook_targets()),
                    "available": bool(self.webhook_targets()),
                    "targets": [_redact_url(target) for target in self.webhook_targets()],
                    "target_count": len(self.webhook_targets()),
                },
                {
                    "name": "telegram",
                    "configured": self.telegram_available(),
                    "available": self.telegram_available(),
                    "chat_id": _mask_middle(self.telegram_chat_id, keep=2) if self.telegram_chat_id else "",
                    "api_base": self.telegram_api_base or "https://api.telegram.org",
                    "parse_mode": self.telegram_parse_mode,
                },
            ],
            "delivery_stats": {
                "event_count": len(recent),
                "delivery_count": total_deliveries,
                "ok_events": ok_events,
                "failed_events": failure_events,
                "by_channel": by_channel,
            },
            "recent": recent,
            "last": recent[0] if recent else {},
            "last_success": last_success,
            "last_failure": last_failure,
            "updated_ts": int(time.time()),
        }
