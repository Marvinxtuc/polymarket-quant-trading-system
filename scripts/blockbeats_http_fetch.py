#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
import urllib.error
import urllib.request
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch a BlockBeats endpoint with Python/OpenSSL")
    parser.add_argument("--url", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--api-key", default="")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    headers = {"accept": "application/json"}
    if args.api_key.strip():
        headers["api-key"] = args.api_key.strip()

    request = urllib.request.Request(args.url, headers=headers)

    try:
        with urllib.request.urlopen(request, timeout=args.timeout) as response:
            payload = response.read()
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", "replace").strip()
        print(body or f"HTTP {exc.code}: {exc.reason}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1

    Path(args.output).write_bytes(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
