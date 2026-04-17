#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
import time
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.config import Settings
from polymarket_bot.i18n import t as i18n_t
from polymarket_bot.secrets import normalize_identity, resolve_live_secret_bundle
from polymarket_bot.signer_client import build_signer_client

USDC_BASE_UNITS = Decimal("1000000")


def _preflight_t(key: str, params: dict[str, object] | None = None, *, fallback: str = "") -> str:
    return i18n_t(f"report.liveSmokePreflight.{key}", dict(params or {}), fallback=fallback)


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "y", "on"}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _smoke_budget_usd() -> tuple[float, float, float, float]:
    resting_usd = max(0.0, _safe_float(os.getenv("LIVE_SMOKE_RESTING_USD"), 1.0))
    aggressive_usd = max(0.0, _safe_float(os.getenv("LIVE_SMOKE_AGGRESSIVE_USD"), 1.0))
    max_usd = max(0.0, _safe_float(os.getenv("LIVE_SMOKE_MAX_USD"), 2.0))
    required_usd = max(0.01, resting_usd, aggressive_usd, max_usd)
    return required_usd, resting_usd, aggressive_usd, max_usd


def _to_units(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    text = str(value or "").strip()
    if not text:
        return None
    try:
        numeric = Decimal(text)
    except Exception:
        return None
    if not numeric.is_finite():
        return None
    if numeric <= 0:
        return 0
    return int(numeric.to_integral_value(rounding=ROUND_FLOOR))


def _extract_units_from_value(value: Any) -> int | None:
    parsed = _to_units(value)
    if parsed is not None:
        return parsed
    if not isinstance(value, dict):
        return None
    for key in ("raw", "amount", "value", "units", "balance", "allowance"):
        if key not in value:
            continue
        parsed = _to_units(value.get(key))
        if parsed is not None:
            return parsed
    return None


def _extract_named_units(payload: Any, aliases: tuple[str, ...]) -> int | None:
    normalized_aliases = {str(alias or "").strip().lower().replace("-", "_") for alias in aliases if str(alias or "").strip()}
    queue: list[Any] = [payload]
    visited = 0
    while queue and visited < 200:
        visited += 1
        current = queue.pop(0)
        if isinstance(current, dict):
            for key, value in current.items():
                normalized_key = str(key or "").strip().lower().replace("-", "_")
                if normalized_key == "allowances" and "allowance" in normalized_aliases and isinstance(value, dict):
                    parsed_values = [_to_units(item) for item in value.values()]
                    valid_values = [int(item) for item in parsed_values if item is not None]
                    if valid_values:
                        return max(valid_values)
                if normalized_key in normalized_aliases:
                    parsed = _extract_units_from_value(value)
                    if parsed is not None:
                        return parsed
                if isinstance(value, (dict, list, tuple)):
                    queue.append(value)
            continue
        if isinstance(current, (list, tuple)):
            for value in current:
                if isinstance(value, (dict, list, tuple)):
                    queue.append(value)
    return None


class _AddressOnlySigner:
    def __init__(self, address: str) -> None:
        self._address = str(address or "").strip().lower()

    def address(self) -> str:
        return self._address


def _evaluate_collateral_budget(settings: Settings, *, enabled: bool = True) -> dict[str, Any]:
    required_usd, resting_usd, aggressive_usd, max_usd = _smoke_budget_usd()
    required_units = int((Decimal(str(required_usd)) * USDC_BASE_UNITS).to_integral_value(rounding=ROUND_CEILING))
    token_id = str(os.getenv("LIVE_SMOKE_TOKEN_ID") or "").strip()
    result: dict[str, Any] = {
        "enabled": bool(enabled),
        "skipped": False,
        "ok": True,
        "token_id": token_id,
        "required_usd": required_usd,
        "required_units": required_units,
        "resting_usd": resting_usd,
        "aggressive_usd": aggressive_usd,
        "max_usd": max_usd,
        "balance_units": None,
        "allowance_units": None,
        "balance_usd": None,
        "allowance_usd": None,
        "api_header_identity": "",
        "signer_api_identity": "",
        "signer_health_error": "",
        "error": "",
        "response_keys": [],
    }
    if not enabled:
        result.update({"skipped": True, "skip_reason": "live_secrets_not_ready", "ok": True})
        return result
    if not token_id:
        result.update({"skipped": True, "skip_reason": "live_smoke_token_id_missing", "ok": True})
        return result

    payload: Any = {}
    try:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import ApiCreds, AssetType, BalanceAllowanceParams
    except Exception as exc:
        result.update({"ok": False, "error": f"py_clob_client_unavailable: {exc}"})
        return result

    try:
        funder_identity = normalize_identity(str(settings.funder_address or ""))
        api_header_identity = funder_identity
        signer_api_identity = ""
        signer_health_error = ""
        try:
            bundle = resolve_live_secret_bundle(settings)
            signer_client = build_signer_client(bundle)
            signer_health = signer_client.health_check()
            signer_api_identity = normalize_identity(getattr(signer_health, "api_identity", ""))
            if signer_api_identity:
                api_header_identity = signer_api_identity
        except Exception as exc:
            signer_health_error = str(exc or "")

        client = ClobClient(
            str(settings.polymarket_clob_host or "").strip(),
            chain_id=int(settings.chain_id),
            key=None,
            signature_type=int(settings.clob_signature_type),
            funder=str(settings.funder_address or "").strip(),
        )
        client.set_api_creds(
            ApiCreds(
                api_key=str(settings.clob_api_key or "").strip(),
                api_secret=str(settings.clob_api_secret or "").strip(),
                api_passphrase=str(settings.clob_api_passphrase or "").strip(),
            )
        )
        client.signer = _AddressOnlySigner(api_header_identity or funder_identity)
        client.mode = client._get_client_mode()
        payload = client.get_balance_allowance(
            BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                token_id=None,
                signature_type=int(settings.clob_signature_type),
            )
        )
        balance_units = _extract_named_units(payload, ("balance", "balance_raw", "raw_balance"))
        allowance_units = _extract_named_units(payload, ("allowance", "allowance_raw", "raw_allowance"))
        if balance_units is None or allowance_units is None:
            detail = sorted(payload.keys()) if isinstance(payload, dict) else [type(payload).__name__]
            raise RuntimeError(f"unable to parse balance/allowance from response: {detail}")
    except Exception as exc:
        result.update(
            {
                "ok": False,
                "error": str(exc or "unknown error"),
                "api_header_identity": str(locals().get("api_header_identity") or ""),
                "signer_api_identity": str(locals().get("signer_api_identity") or ""),
                "signer_health_error": str(locals().get("signer_health_error") or ""),
            }
        )
        if isinstance(payload, dict):
            result["response_keys"] = sorted(str(key) for key in payload.keys())[:40]
        return result

    balance_usd = float((Decimal(balance_units) / USDC_BASE_UNITS)) if balance_units is not None else 0.0
    allowance_usd = float((Decimal(allowance_units) / USDC_BASE_UNITS)) if allowance_units is not None else 0.0
    ok = bool(balance_units >= required_units and allowance_units >= required_units)
    result.update(
        {
            "ok": ok,
            "balance_units": int(balance_units),
            "allowance_units": int(allowance_units),
            "balance_usd": balance_usd,
            "allowance_usd": allowance_usd,
            "api_header_identity": api_header_identity,
            "signer_api_identity": signer_api_identity,
            "signer_health_error": signer_health_error,
            "response_keys": sorted(str(key) for key in payload.keys())[:40] if isinstance(payload, dict) else [],
        }
    )
    return result


def _check(
    name: str,
    ok: bool,
    message: str,
    *,
    message_code: str = "",
    details: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "name": name,
        "status": "PASS" if ok else "FAIL",
        "message": str(message or ""),
        "message_code": str(message_code or ""),
        "details": dict(details or {}),
    }


def build_report(settings: Settings, *, now_ts: int | None = None) -> tuple[dict[str, Any], int]:
    now = int(now_ts or time.time())
    checks: list[dict[str, Any]] = []
    blockers: list[str] = []
    blocker_codes: list[str] = []

    def add_blocker(code: str) -> None:
        blocker_codes.append(code)
        blockers.append(_preflight_t(f"blocker.{code}", fallback=code))

    checks.append(
        _check(
            "dry_run_disabled",
            not bool(settings.dry_run),
            _preflight_t("check.dryRunDisabled", fallback="DRY_RUN=false required for live smoke preflight"),
            message_code="dryRunDisabled",
            details={"dry_run": bool(settings.dry_run)},
        )
    )
    if bool(settings.dry_run):
        add_blocker("dryRunTrue")

    raw_private_key_present = bool(str(settings.private_key or "").strip())
    funder_ready = bool(str(settings.funder_address or "").strip())
    signer_ready = bool(str(getattr(settings, "signer_url", "") or "").strip())
    api_creds_ready = bool(
        str(getattr(settings, "clob_api_key", "") or "").strip()
        and str(getattr(settings, "clob_api_secret", "") or "").strip()
        and str(getattr(settings, "clob_api_passphrase", "") or "").strip()
    )
    checks.append(
        _check(
            "live_secrets",
            funder_ready and signer_ready and api_creds_ready and not raw_private_key_present,
            _preflight_t(
                "check.liveSecrets",
                fallback="FUNDER_ADDRESS + signer endpoint + CLOB API creds required, and PRIVATE_KEY must be empty in live mode",
            ),
            message_code="liveSecrets",
            details={
                "raw_private_key_present": raw_private_key_present,
                "funder_ready": funder_ready,
                "signer_ready": signer_ready,
                "api_creds_ready": api_creds_ready,
            },
        )
    )
    if not (funder_ready and signer_ready and api_creds_ready and not raw_private_key_present):
        add_blocker("liveSecretsMissing")

    live_flags_ok = bool(settings.live_allowance_ready and settings.live_geoblock_ready and settings.live_account_ready)
    checks.append(
        _check(
            "live_admission_flags",
            live_flags_ok,
            _preflight_t(
                "check.liveAdmissionFlags",
                fallback="LIVE_ALLOWANCE_READY, LIVE_GEOBLOCK_READY, LIVE_ACCOUNT_READY must all be true",
            ),
            message_code="liveAdmissionFlags",
            details={
                "live_allowance_ready": bool(settings.live_allowance_ready),
                "live_geoblock_ready": bool(settings.live_geoblock_ready),
                "live_account_ready": bool(settings.live_account_ready),
            },
        )
    )
    if not live_flags_ok:
        add_blocker("liveAdmissionFlagsMissing")

    remote_alert_ok = bool(settings.notify_webhook_url_list or settings.notify_telegram_enabled)
    checks.append(
        _check(
            "remote_alert_configured",
            remote_alert_ok,
            _preflight_t(
                "check.remoteAlertConfigured",
                fallback="Remote alert channel must be configured before live smoke",
            ),
            message_code="remoteAlertConfigured",
            details={
                "webhook_targets": len(settings.notify_webhook_url_list),
                "telegram_enabled": bool(settings.notify_telegram_enabled),
            },
        )
    )
    if not remote_alert_ok:
        add_blocker("remoteAlertNotConfigured")

    collateral_budget = _evaluate_collateral_budget(
        settings,
        enabled=funder_ready and api_creds_ready and not raw_private_key_present,
    )
    collateral_required_usd = _safe_float(collateral_budget.get("required_usd"), 0.0)
    collateral_ok = bool(collateral_budget.get("ok"))
    checks.append(
        _check(
            "collateral_balance_allowance",
            collateral_ok,
            _preflight_t(
                "check.collateralBalanceAllowance",
                {"requiredUsd": f"{collateral_required_usd:.2f}"},
                fallback=f"Collateral balance/allowance must cover live smoke budget (${collateral_required_usd:.2f})",
            ),
            message_code="collateralBalanceAllowance",
            details=collateral_budget,
        )
    )
    if not collateral_ok:
        if str(collateral_budget.get("error") or "").strip():
            add_blocker("collateralBalanceAllowanceUnavailable")
        else:
            add_blocker("collateralBalanceAllowanceInsufficient")

    state_path = Path(settings.runtime_store_path("state.json")).expanduser()
    state_payload: dict[str, Any] = {}
    state_exists = state_path.exists()
    if state_exists:
        try:
            payload = json.loads(state_path.read_text(encoding="utf-8"))
            if isinstance(payload, dict):
                state_payload = payload
        except Exception:
            state_payload = {}
    checks.append(
        _check(
            "state_file_present",
            state_exists and bool(state_payload),
            _preflight_t("check.stateFilePresent", fallback="Live state file must exist and be readable"),
            message_code="stateFilePresent",
            details={"state_path": str(state_path)},
        )
    )
    if not (state_exists and state_payload):
        add_blocker("stateFileMissing")

    state_ts = int(state_payload.get("ts") or 0)
    poll_interval = int(state_payload.get("config", {}).get("poll_interval_seconds") or settings.poll_interval_seconds or 30)
    max_age = max(30, poll_interval * 3)
    state_age = max(0, now - state_ts) if state_ts else 10**9
    checks.append(
        _check(
            "state_fresh",
            bool(state_ts and state_age <= max_age),
            _preflight_t(
                "check.stateFresh",
                {"maxAge": max_age},
                fallback=f"Live state age must be <= {max_age}s before smoke",
            ),
            message_code="stateFresh",
            details={"state_ts": state_ts, "state_age_seconds": state_age, "max_age_seconds": max_age},
        )
    )
    if not (state_ts and state_age <= max_age):
        add_blocker("stateStale")

    control = state_payload.get("control", {}) if isinstance(state_payload.get("control"), dict) else {}
    checks.append(
        _check(
            "decision_mode_manual",
            str(control.get("decision_mode") or "") == "manual",
            _preflight_t("check.decisionModeManual", fallback="decision_mode must be manual before smoke"),
            message_code="decisionModeManual",
        )
    )
    if str(control.get("decision_mode") or "") != "manual":
        add_blocker("decisionModeNotManual")
    checks.append(
        _check(
            "pause_opening_enabled",
            bool(control.get("pause_opening")),
            _preflight_t("check.pauseOpeningEnabled", fallback="pause_opening must remain true before smoke"),
            message_code="pauseOpeningEnabled",
        )
    )
    if not bool(control.get("pause_opening")):
        add_blocker("pauseOpeningDisabled")

    startup_ready = bool(state_payload.get("startup", {}).get("ready"))
    checks.append(
        _check(
            "startup_ready",
            startup_ready,
            _preflight_t("check.startupReady", fallback="startup_ready must be true"),
            message_code="startupReady",
        )
    )
    if not startup_ready:
        add_blocker("startupNotReady")

    reconciliation_status = str(state_payload.get("reconciliation", {}).get("status") or "")
    checks.append(
        _check(
            "reconciliation_ok",
            reconciliation_status == "ok",
            _preflight_t("check.reconciliationOk", fallback="reconciliation.status must be ok"),
            message_code="reconciliationOk",
            details={"reconciliation_status": reconciliation_status},
        )
    )
    if reconciliation_status != "ok":
        add_blocker("reconciliationNotOk")

    persistence_status = str(state_payload.get("persistence", {}).get("status") or "")
    checks.append(
        _check(
            "persistence_ok",
            persistence_status == "ok",
            _preflight_t("check.persistenceOk", fallback="persistence.status must be ok"),
            message_code="persistenceOk",
            details={"persistence_status": persistence_status},
        )
    )
    if persistence_status != "ok":
        add_blocker("persistenceNotOk")

    summary = state_payload.get("summary", {}) if isinstance(state_payload.get("summary"), dict) else {}
    open_positions = int(summary.get("open_positions") or 0)
    tracked_notional = float(summary.get("tracked_notional_usd") or 0.0)
    checks.append(
        _check(
            "open_positions_zero",
            open_positions == 0,
            _preflight_t("check.openPositionsZero", fallback="open_positions must be 0 before live smoke"),
            message_code="openPositionsZero",
            details={"open_positions": open_positions},
        )
    )
    if open_positions != 0:
        add_blocker("openPositionsNonZero")
    checks.append(
        _check(
            "tracked_notional_zero",
            tracked_notional <= 0.0,
            _preflight_t(
                "check.trackedNotionalZero",
                fallback="tracked_notional_usd must be 0 before live smoke",
            ),
            message_code="trackedNotionalZero",
            details={"tracked_notional_usd": tracked_notional},
        )
    )
    if tracked_notional > 0.0:
        add_blocker("trackedNotionalNonZero")

    report = {
        "generated_at": now,
        "status": "ready" if not blockers else "blocked",
        "status_label": _preflight_t(
            f"status.{ 'ready' if not blockers else 'blocked' }",
            fallback="ready" if not blockers else "blocked",
        ),
        "blockers": blockers,
        "blocker_codes": blocker_codes,
        "checks": checks,
        "state_path": str(state_path),
        "funder_address": str(settings.funder_address or ""),
        "dry_run": bool(settings.dry_run),
    }
    return report, (0 if not blockers else 1)


def main() -> int:
    parser = argparse.ArgumentParser(
        description=_preflight_t(
            "cli.description",
            fallback="Safe preflight for live connectivity smoke. Does not place orders.",
        )
    )
    parser.add_argument(
        "--report-path",
        default="",
        help=_preflight_t("cli.reportPath", fallback="Optional JSON report output path."),
    )
    args = parser.parse_args()

    settings = Settings()
    report, exit_code = build_report(settings)
    report_path = str(args.report_path or settings.runtime_store_path("live_smoke_preflight.json")).strip()
    Path(report_path).expanduser().parent.mkdir(parents=True, exist_ok=True)
    Path(report_path).expanduser().write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=False, indent=2))
    print(report_path)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
