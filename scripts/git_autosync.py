#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path


def utc_now() -> str:
    return datetime.now(tz=timezone.utc).replace(microsecond=0).isoformat()


def run_git(repo: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *args],
        cwd=str(repo),
        text=True,
        capture_output=True,
        check=check,
    )


def git_output(repo: Path, *args: str) -> str:
    return run_git(repo, *args).stdout.strip()


def git_status(repo: Path) -> str:
    return git_output(repo, "status", "--porcelain")


def has_staged_changes(repo: Path) -> bool:
    proc = subprocess.run(
        ["git", "diff", "--cached", "--quiet"],
        cwd=str(repo),
        text=True,
        capture_output=True,
        check=False,
    )
    return proc.returncode != 0


def write_status(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def build_commit_message(prefix: str) -> str:
    return f"{prefix} {datetime.now().strftime('%Y-%m-%d %H:%M:%S %Z')}".strip()


def detect_branch(repo: Path) -> str:
    branch = git_output(repo, "rev-parse", "--abbrev-ref", "HEAD")
    return branch or "HEAD"


def sync_once(
    *,
    repo: Path,
    remote: str,
    commit_prefix: str,
) -> tuple[bool, dict[str, object]]:
    started_at = utc_now()
    run_git(repo, "add", "-A")
    if not has_staged_changes(repo):
        return (
            True,
            {
                "sync_started_at": started_at,
                "sync_finished_at": utc_now(),
                "status": "noop",
                "message": "no staged changes after git add -A",
            },
        )

    commit_message = build_commit_message(commit_prefix)
    commit_proc = run_git(repo, "commit", "-m", commit_message, check=False)
    if commit_proc.returncode != 0:
        return (
            False,
            {
                "sync_started_at": started_at,
                "sync_finished_at": utc_now(),
                "status": "commit_failed",
                "message": (commit_proc.stderr or commit_proc.stdout or "git commit failed").strip(),
                "commit_message": commit_message,
            },
        )

    head_sha = git_output(repo, "rev-parse", "HEAD")
    push_proc = run_git(repo, "push", remote, "HEAD", check=False)
    if push_proc.returncode != 0:
        return (
            False,
            {
                "sync_started_at": started_at,
                "sync_finished_at": utc_now(),
                "status": "push_failed",
                "message": (push_proc.stderr or push_proc.stdout or "git push failed").strip(),
                "commit_message": commit_message,
                "commit_sha": head_sha,
            },
        )

    return (
        True,
        {
            "sync_started_at": started_at,
            "sync_finished_at": utc_now(),
            "status": "synced",
            "message": "git commit + push succeeded",
            "commit_message": commit_message,
            "commit_sha": head_sha,
            "branch": detect_branch(repo),
            "remote": remote,
        },
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Auto commit + auto push git watcher")
    parser.add_argument("--repo", default=".", help="Repository path")
    parser.add_argument("--remote", default=os.getenv("GIT_AUTOSYNC_REMOTE", "origin"))
    parser.add_argument("--interval", type=float, default=float(os.getenv("GIT_AUTOSYNC_POLL_SECONDS", "2")))
    parser.add_argument("--debounce", type=float, default=float(os.getenv("GIT_AUTOSYNC_DEBOUNCE_SECONDS", "3")))
    parser.add_argument("--commit-prefix", default=os.getenv("GIT_AUTOSYNC_COMMIT_PREFIX", "auto: sync"))
    parser.add_argument(
        "--status-path",
        default=os.getenv("GIT_AUTOSYNC_STATUS_PATH", "/tmp/poly_git_autosync/status.json"),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo = Path(args.repo).resolve()
    status_path = Path(args.status_path).resolve()
    stop_requested = False

    def _handle_signal(_signum: int, _frame) -> None:
        nonlocal stop_requested
        stop_requested = True

    signal.signal(signal.SIGTERM, _handle_signal)
    signal.signal(signal.SIGINT, _handle_signal)

    base_status = {
        "repo": str(repo),
        "remote": str(args.remote),
        "started_at": utc_now(),
        "pid": os.getpid(),
        "watching": True,
        "status": "idle",
        "branch": detect_branch(repo),
        "last_seen_dirty_at": "",
        "last_sync": {},
        "last_error": "",
    }
    write_status(status_path, base_status)

    dirty_since = 0.0
    dirty_snapshot = ""
    while not stop_requested:
        try:
            snapshot = git_status(repo)
            current = dict(base_status)
            current["branch"] = detect_branch(repo)
            current["loop_ts"] = utc_now()
            current["dirty"] = bool(snapshot.strip())

            if not snapshot.strip():
                dirty_since = 0.0
                dirty_snapshot = ""
                current["status"] = "idle"
                write_status(status_path, current)
                time.sleep(max(0.5, float(args.interval)))
                continue

            now = time.time()
            if snapshot != dirty_snapshot:
                dirty_snapshot = snapshot
                dirty_since = now

            current["status"] = "debouncing"
            current["last_seen_dirty_at"] = datetime.fromtimestamp(dirty_since, tz=timezone.utc).replace(microsecond=0).isoformat()
            current["pending_changes"] = snapshot.splitlines()
            write_status(status_path, current)

            if now - dirty_since < max(0.5, float(args.debounce)):
                time.sleep(max(0.5, float(args.interval)))
                continue

            ok, sync_status = sync_once(
                repo=repo,
                remote=str(args.remote),
                commit_prefix=str(args.commit_prefix),
            )
            current["last_sync"] = sync_status
            current["status"] = str(sync_status.get("status") or ("synced" if ok else "error"))
            current["last_error"] = "" if ok else str(sync_status.get("message") or "unknown error")
            write_status(status_path, current)

            if ok:
                dirty_since = 0.0
                dirty_snapshot = ""
            else:
                dirty_since = time.time()
        except Exception as exc:  # pragma: no cover - process safety net
            current = dict(base_status)
            current["status"] = "error"
            current["loop_ts"] = utc_now()
            current["last_error"] = str(exc)
            write_status(status_path, current)
        time.sleep(max(0.5, float(args.interval)))

    final_status = dict(base_status)
    final_status["watching"] = False
    final_status["stopped_at"] = utc_now()
    final_status["status"] = "stopped"
    write_status(status_path, final_status)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
