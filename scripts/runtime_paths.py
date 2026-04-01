#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import shlex
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from polymarket_bot.config import Settings, build_runtime_artifact_paths


def main() -> int:
    parser = argparse.ArgumentParser(description="Resolve namespaced runtime paths for the current Polymarket execution context")
    parser.add_argument("keys", nargs="*", help="Optional path keys to print")
    parser.add_argument("--format", choices=("plain", "json", "shell"), default="plain")
    args = parser.parse_args()

    settings = Settings()
    paths = build_runtime_artifact_paths(settings)

    if args.keys:
        missing = [key for key in args.keys if key not in paths]
        if missing:
            raise SystemExit(f"unknown runtime path keys: {', '.join(sorted(missing))}")
        selected = {key: paths[key] for key in args.keys}
    else:
        selected = dict(paths)

    if args.format == "json":
        print(json.dumps(selected, ensure_ascii=False, indent=2))
        return 0
    if args.format == "shell":
        for key, value in selected.items():
            print(f"{key.upper()}={shlex.quote(str(value))}")
        return 0

    if args.keys:
        for key in args.keys:
            print(selected[key])
    else:
        for key in sorted(selected):
            print(f"{key}={selected[key]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
