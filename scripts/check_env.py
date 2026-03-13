#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path


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


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate .env against .env.example")
    parser.add_argument("--warn-only", action="store_true", help="Do not exit non-zero on problems")
    args = parser.parse_args()

    base = Path(__file__).resolve().parents[1]
    env_path = base / ".env"
    env_example_path = base / ".env.example"

    problems: list[str] = []

    if not env_example_path.exists():
        problems.append("missing .env.example (cannot validate expected keys)")
        if args.warn_only:
            print("WARN: " + problems[-1])
            return 0
        print("ERROR: " + problems[-1])
        return 1

    env_example = load_env_kv(env_example_path)
    env_actual = load_env_kv(env_path)

    if not env_path.exists():
        problems.append("missing .env (create it from .env.example)")

    missing_keys = sorted(k for k in env_example.keys() if k not in env_actual)
    if missing_keys:
        problems.append("missing keys in .env: " + ", ".join(missing_keys))

    # If DRY_RUN is false, require live secrets.
    dry_run_val = env_actual.get("DRY_RUN", env_example.get("DRY_RUN", "true")).strip().lower()
    dry_run = dry_run_val in {"1", "true", "yes", "y", "on"}
    if not dry_run:
        for key in ("PRIVATE_KEY", "FUNDER_ADDRESS"):
            value = env_actual.get(key, "").strip()
            if not value:
                problems.append(f"DRY_RUN=false requires {key} to be set")

    if problems:
        for item in problems:
            print(("WARN: " if args.warn_only else "ERROR: ") + item)
        return 0 if args.warn_only else 1

    print("OK: .env looks good")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
