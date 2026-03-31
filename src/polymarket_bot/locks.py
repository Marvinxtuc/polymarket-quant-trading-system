from __future__ import annotations

import fcntl
import io
import json
import os
import re
import subprocess
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


SINGLE_WRITER_CONFLICT_REASON = "single_writer_conflict"
SINGLE_WRITER_CONFLICT_EXIT_CODE = 42

_REMOTE_FILESYSTEM_TOKENS = {
    "nfs",
    "nfs4",
    "smb",
    "smbfs",
    "cifs",
    "afp",
    "afpfs",
    "sshfs",
    "fuse",
    "fuseblk",
    "webdav",
    "davfs",
    "9p",
    "ceph",
    "gluster",
    "lustre",
}
_LOCAL_FILESYSTEM_TOKENS = {
    "apfs",
    "hfs",
    "hfs+",
    "hfsplus",
    "ufs",
    "ufs2",
    "ext2",
    "ext3",
    "ext4",
    "xfs",
    "btrfs",
    "zfs",
    "tmpfs",
    "overlay",
    "devfs",
}


class SingleWriterLockError(RuntimeError):
    """Raised when single-writer ownership is unavailable or invalid."""

    def __init__(self, message: str, *, reason_code: str = SINGLE_WRITER_CONFLICT_REASON) -> None:
        super().__init__(message)
        self.reason_code = str(reason_code or SINGLE_WRITER_CONFLICT_REASON)


def derive_writer_scope(*, dry_run: bool, funder_address: str = "", watch_wallets: str = "") -> str:
    mode = "paper" if bool(dry_run) else "live"
    identity = ""
    if not bool(dry_run):
        identity = str(funder_address or "").strip().lower()
    if not identity:
        wallets = [chunk.strip().lower() for chunk in str(watch_wallets or "").split(",") if chunk.strip()]
        identity = wallets[0] if wallets else "default"
    return f"{mode}:{identity}"


def _filesystem_type(path: Path) -> tuple[str, bool | None]:
    try:
        proc = subprocess.run(["mount"], capture_output=True, text=True, check=False)
    except Exception as exc:
        raise SingleWriterLockError(
            f"unable to inspect filesystem mounts for writer lock path: {path}",
            reason_code="single_writer_fs_unconfirmed",
        ) from exc
    if proc.returncode != 0:
        raise SingleWriterLockError(
            f"unable to inspect filesystem mounts for writer lock path: {path}",
            reason_code="single_writer_fs_unconfirmed",
        )

    target = str(path.resolve())
    try:
        target_dev = path.stat().st_dev
    except Exception as exc:
        raise SingleWriterLockError(
            f"unable to stat writer lock path: {path}",
            reason_code="single_writer_fs_unconfirmed",
        ) from exc
    best_match: tuple[int, str, str, bool | None] | None = None
    bsd_pattern = re.compile(r"^.+ on (?P<mount>.+) \((?P<opts>[^)]+)\)$")
    linux_pattern = re.compile(r"^.+ on (?P<mount>.+) type (?P<fstype>[^ ]+) \((?P<opts>[^)]+)\)$")
    for raw in proc.stdout.splitlines():
        line = str(raw or "").strip()
        if not line:
            continue
        mount_point = ""
        fs_type = ""
        local_hint: bool | None = None
        linux_match = linux_pattern.match(line)
        if linux_match is not None:
            mount_point = str(linux_match.group("mount") or "").strip()
            fs_type = str(linux_match.group("fstype") or "").strip().lower()
            opts = [chunk.strip().lower() for chunk in str(linux_match.group("opts") or "").split(",")]
            if "local" in opts:
                local_hint = True
            elif "remote" in opts:
                local_hint = False
        else:
            bsd_match = bsd_pattern.match(line)
            if bsd_match is not None:
                mount_point = str(bsd_match.group("mount") or "").strip()
                opts = [chunk.strip().lower() for chunk in str(bsd_match.group("opts") or "").split(",")]
                if opts:
                    fs_type = opts[0]
                if "local" in opts:
                    local_hint = True
                elif "remote" in opts:
                    local_hint = False
        if not mount_point or not fs_type:
            continue
        normalized_mount = mount_point.rstrip("/") or "/"
        try:
            mount_dev = Path(normalized_mount).stat().st_dev
        except Exception:
            continue
        if mount_dev != target_dev:
            continue
        if target != normalized_mount and not target.startswith(f"{normalized_mount}/") and normalized_mount != "/":
            continue
        rank = len(normalized_mount)
        if best_match is None or rank > best_match[0]:
            best_match = (rank, normalized_mount, fs_type, local_hint)

    if best_match is not None:
        return (best_match[2], best_match[3])
    raise SingleWriterLockError(
        f"unable to determine filesystem type for writer lock path: {path}",
        reason_code="single_writer_fs_unconfirmed",
    )


def _is_local_filesystem(fs_type: str, local_hint: bool | None) -> bool:
    if local_hint is True:
        return True
    if local_hint is False:
        return False
    token = str(fs_type or "").strip().lower()
    if not token:
        return False
    for remote_token in _REMOTE_FILESYSTEM_TOKENS:
        if remote_token in token:
            return False
    for local_token in _LOCAL_FILESYSTEM_TOKENS:
        if local_token in token:
            return True
    return False


def _ensure_local_writable_directory(lock_path: Path) -> None:
    directory = lock_path.parent
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        raise SingleWriterLockError(
            f"unable to create writer lock directory: {directory}",
            reason_code="single_writer_lock_path_unavailable",
        ) from exc

    fs_type, local_hint = _filesystem_type(directory)
    if not _is_local_filesystem(fs_type, local_hint):
        raise SingleWriterLockError(
            f"writer lock path must be local filesystem; path={directory} fs={fs_type}",
            reason_code="single_writer_lock_path_non_local",
        )

    if not os.access(directory, os.W_OK | os.X_OK):
        raise SingleWriterLockError(
            f"writer lock directory is not writable: {directory}",
            reason_code="single_writer_lock_path_unwritable",
        )


@contextmanager
def file_lock(path: str, *, timeout: float | None = None, writer_scope: str = "") -> Iterator[None]:
    """Acquire an exclusive, non-reentrant file lock."""
    lock = FileLock(path, timeout=timeout, writer_scope=writer_scope)
    lock.acquire()
    try:
        yield
    finally:
        lock.release()


class FileLock:
    """A process lock holder with ownership assertions."""

    def __init__(self, path: str, *, timeout: float | None = None, writer_scope: str = "") -> None:
        self.path = Path(path).expanduser()
        self.timeout = timeout
        self.writer_scope = str(writer_scope or "")
        self._fh: io.TextIOWrapper | None = None

    @property
    def is_active(self) -> bool:
        return self._fh is not None

    def acquire(self) -> None:
        if self._fh is not None:
            self.assert_active()
            return
        _ensure_local_writable_directory(self.path)
        fh = open(self.path, "a+", encoding="utf-8")
        start = time.time()
        delay = 0.05
        while True:
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if self.timeout is not None and (time.time() - start) >= self.timeout:
                    fh.close()
                    raise SingleWriterLockError(
                        f"lock busy: {self.path}",
                        reason_code=SINGLE_WRITER_CONFLICT_REASON,
                    )
                time.sleep(delay)
                delay = min(0.5, delay * 1.5)
        metadata = {
            "pid": os.getpid(),
            "writer_scope": self.writer_scope,
            "acquired_ts": int(time.time()),
        }
        fh.seek(0)
        fh.truncate(0)
        fh.write(json.dumps(metadata, ensure_ascii=True))
        fh.flush()
        os.fsync(fh.fileno())
        self._fh = fh

    def assert_active(self) -> None:
        if self._fh is None:
            raise SingleWriterLockError(
                f"writer lock is not active: {self.path}",
                reason_code="single_writer_not_active",
            )
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise SingleWriterLockError(
                f"writer lock ownership lost: {self.path}",
                reason_code="single_writer_ownership_lost",
            ) from exc

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            try:
                self._fh.close()
            finally:
                self._fh = None

    def __enter__(self) -> "FileLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.release()
