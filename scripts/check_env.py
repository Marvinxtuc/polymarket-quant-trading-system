#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.i18n import t as i18n_t

CORE_REQUIRED_KEYS = (
    "DRY_RUN",
    "POLL_INTERVAL_SECONDS",
    "BANKROLL_USD",
    "RISK_PER_TRADE_PCT",
    "DAILY_MAX_LOSS_PCT",
    "MAX_OPEN_POSITIONS",
    "POLYMARKET_DATA_API",
    "POLYMARKET_CLOB_HOST",
)
LIVE_REQUIRED_KEYS = (
    "PRIVATE_KEY",
    "FUNDER_ADDRESS",
    "LIVE_ALLOWANCE_READY",
    "LIVE_GEOBLOCK_READY",
    "LIVE_ACCOUNT_READY",
)
REMOTE_NOTIFY_KEYS = (
    "POLY_NOTIFY_WEBHOOK_URL",
    "POLY_NOTIFY_WEBHOOK_URLS",
    "POLY_NOTIFY_TELEGRAM_BOT_TOKEN",
    "POLY_NOTIFY_TELEGRAM_CHAT_ID",
    "NOTIFY_WEBHOOK_URL",
    "NOTIFY_WEBHOOK_URLS",
    "NOTIFY_TELEGRAM_BOT_TOKEN",
    "NOTIFY_TELEGRAM_CHAT_ID",
)
FEATURE_OPTIONAL_KEYS = ("BLOCKBEATS_API_KEY",)


def _check_env_t(key: str, params: dict[str, object] | None = None, *, fallback: str = "") -> str:
    return i18n_t(f"script.checkEnv.{key}", dict(params or {}), fallback=fallback)


def _prefixed_line(level: str, message: str) -> str:
    prefix = _check_env_t(f"output.prefix.{level}", fallback=f"{level.upper()}: ")
    return f"{prefix}{message}"


def load_env_kv(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    data: dict[str, str] = {}
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        data[key.strip()] = value.strip()
    return data


def is_truthy(value: str | None) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def validate_env(env_actual: dict[str, str], env_example: dict[str, str]) -> tuple[list[str], list[str]]:
    problems: list[str] = []
    warnings: list[str] = []

    for key in CORE_REQUIRED_KEYS:
        if not str(env_actual.get(key, "")).strip():
            problems.append(
                _check_env_t("problem.missingRequiredKey", {"key": key}, fallback=f"missing required key in .env: {key}")
            )

    missing_example_keys = sorted(k for k in env_example.keys() if k not in env_actual)
    optional_missing = [
        k
        for k in missing_example_keys
        if k not in CORE_REQUIRED_KEYS and k not in LIVE_REQUIRED_KEYS and k not in FEATURE_OPTIONAL_KEYS
    ]
    if optional_missing:
        warnings.append(
            _check_env_t(
                "warning.missingOptionalKeys",
                {"keys": ", ".join(optional_missing)},
                fallback="missing optional keys in .env: " + ", ".join(optional_missing),
            )
        )

    blockbeats_declared = "BLOCKBEATS_API_KEY" in env_example or "BLOCKBEATS_API_KEY" in env_actual
    if blockbeats_declared and not str(env_actual.get("BLOCKBEATS_API_KEY", "")).strip():
        warnings.append(
            _check_env_t(
                "warning.blockbeatsUnavailable",
                fallback="BLOCKBEATS_API_KEY not set; shared BlockBeats skill and scripts/blockbeats_query.sh will be unavailable",
            )
        )

    dry_run = is_truthy(env_actual.get("DRY_RUN", env_example.get("DRY_RUN", "true")))
    if not dry_run:
        for key in ("PRIVATE_KEY", "FUNDER_ADDRESS"):
            value = env_actual.get(key, "").strip()
            if not value:
                problems.append(
                    _check_env_t(
                        "problem.dryRunRequiresKey",
                        {"key": key},
                        fallback=f"DRY_RUN=false requires {key} to be set",
                    )
                )
        for key in ("LIVE_ALLOWANCE_READY", "LIVE_GEOBLOCK_READY", "LIVE_ACCOUNT_READY"):
            if not is_truthy(env_actual.get(key, env_example.get(key, ""))):
                problems.append(
                    _check_env_t(
                        "problem.dryRunRequiresTrue",
                        {"key": key},
                        fallback=f"DRY_RUN=false requires {key}=true",
                    )
                )
        webhook_ready = bool(
            str(env_actual.get("POLY_NOTIFY_WEBHOOK_URL", "") or env_actual.get("NOTIFY_WEBHOOK_URL", "")).strip()
            or str(env_actual.get("POLY_NOTIFY_WEBHOOK_URLS", "") or env_actual.get("NOTIFY_WEBHOOK_URLS", "")).strip()
        )
        telegram_ready = bool(
            str(env_actual.get("POLY_NOTIFY_TELEGRAM_BOT_TOKEN", "") or env_actual.get("NOTIFY_TELEGRAM_BOT_TOKEN", "")).strip()
            and str(env_actual.get("POLY_NOTIFY_TELEGRAM_CHAT_ID", "") or env_actual.get("NOTIFY_TELEGRAM_CHAT_ID", "")).strip()
        )
        if not webhook_ready and not telegram_ready:
            warnings.append(
                _check_env_t(
                    "warning.remoteAlertMissing",
                    fallback="DRY_RUN=false has no remote alert channel configured (webhook or telegram)",
                )
            )

    return problems, warnings


def main() -> int:
    parser = argparse.ArgumentParser(
        description=_check_env_t("cli.description", fallback="Validate .env against .env.example")
    )
    parser.add_argument(
        "--warn-only",
        action="store_true",
        help=_check_env_t("cli.warnOnly", fallback="Do not exit non-zero on problems"),
    )
    args = parser.parse_args()

    base = ROOT
    env_path = base / ".env"
    env_example_path = base / ".env.example"

    problems: list[str] = []
    warnings: list[str] = []

    if not env_example_path.exists():
        problems.append(
            _check_env_t(
                "problem.missingExample",
                fallback="missing .env.example (cannot validate expected keys)",
            )
        )
        if args.warn_only:
            print(_prefixed_line("warn", problems[-1]))
            return 0
        print(_prefixed_line("error", problems[-1]))
        return 1

    env_example = load_env_kv(env_example_path)
    env_actual = load_env_kv(env_path)
    env_actual.update({str(k): str(v) for k, v in os.environ.items() if str(k)})

    if not env_path.exists():
        problems.append(
            _check_env_t(
                "problem.missingEnv",
                fallback="missing .env (create it from .env.example)",
            )
        )
    else:
        validation_problems, warnings = validate_env(env_actual, env_example)
        problems.extend(validation_problems)

    if problems:
        for item in problems:
            print(_prefixed_line("warn" if args.warn_only else "error", item))
        for item in warnings:
            print(_prefixed_line("warn", item))
        return 0 if args.warn_only else 1

    for item in warnings:
        print(_prefixed_line("warn", item))

    print(_check_env_t("output.ok", fallback="OK: .env looks good"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
