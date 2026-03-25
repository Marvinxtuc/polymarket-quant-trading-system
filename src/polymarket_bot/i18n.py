from __future__ import annotations

import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any


DEFAULT_LOCALE = "zh-CN"
FALLBACK_LOCALE = "en"


def _locales_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "frontend" / "locales"


@lru_cache(maxsize=8)
def _load_locale(locale: str) -> dict[str, Any]:
    path = _locales_dir() / f"{locale}.json"
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_key(messages: dict[str, Any], key: str) -> Any:
    node: Any = messages
    for part in str(key or "").split("."):
        if not part:
            continue
        if not isinstance(node, dict) or part not in node:
            return None
        node = node.get(part)
    return node


def _format_message(template: str, params: dict[str, object] | None = None) -> str:
    text = str(template or "")
    values = dict(params or {})
    for key, value in values.items():
        text = text.replace(f"{{{{{key}}}}}", str(value))
    return text


def current_locale() -> str:
    return str(os.getenv("POLY_UI_LOCALE", DEFAULT_LOCALE) or DEFAULT_LOCALE).strip() or DEFAULT_LOCALE


def humanize_identifier(value: object) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
    text = re.sub(r"[_\-\s]+", " ", text)
    return text.strip()


def t(
    key: str,
    params: dict[str, object] | None = None,
    *,
    locale: str | None = None,
    fallback: str = "",
) -> str:
    active_locale = str(locale or current_locale() or DEFAULT_LOCALE).strip() or DEFAULT_LOCALE
    for candidate in (active_locale, DEFAULT_LOCALE, FALLBACK_LOCALE):
        messages = _load_locale(candidate)
        resolved = _resolve_key(messages, key)
        if isinstance(resolved, str):
            return _format_message(resolved, params)
    return fallback or key


def label(
    prefix: str,
    value: object,
    *,
    locale: str | None = None,
    fallback: str = "",
) -> str:
    raw = str(value or "").strip()
    if not raw:
        return fallback
    normalized = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", raw)
    normalized = re.sub(r"[\s\-]+", "_", normalized).lower()
    key = f"{prefix}.{normalized}"
    translated = t(key, locale=locale, fallback="")
    if translated and translated != key:
        return translated
    return fallback or humanize_identifier(raw)


def enum_label(prefix: str, value: object, *, locale: str | None = None, fallback: str = "") -> str:
    normalized = str(value or "").strip()
    if not normalized:
        return fallback
    key = f"{prefix}.{normalized}"
    return t(key, locale=locale, fallback=fallback or normalized)
