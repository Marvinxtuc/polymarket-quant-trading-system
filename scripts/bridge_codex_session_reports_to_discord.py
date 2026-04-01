#!/usr/bin/env python3
from __future__ import annotations

import argparse
import fcntl
import hashlib
import tempfile
import json
import os
import re
import shutil
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_HOME = Path.home()
DEFAULT_SESSIONS_DIR = Path(
    os.environ.get("OPENCLAW_SESSIONS_DIR", str(DEFAULT_HOME / ".openclaw" / "agents" / "polymarket" / "sessions"))
)
DEFAULT_CODEX_INDEX = Path(os.environ.get("CODEX_SESSION_INDEX", str(DEFAULT_HOME / ".codex" / "session_index.jsonl")))
DEFAULT_CODEX_SESSIONS_ROOT = Path(os.environ.get("CODEX_SESSIONS_ROOT", str(DEFAULT_HOME / ".codex" / "sessions")))
DEFAULT_OPENCLAW_CONFIG = Path(os.environ.get("OPENCLAW_CONFIG", str(DEFAULT_HOME / ".openclaw" / "openclaw.json")))
DEFAULT_STATE_FILE = Path("/tmp/poly_codex_discord_bridge/state.json")
DEFAULT_LOCK_FILE = Path("/tmp/poly_codex_discord_bridge/bridge.lock")
DEFAULT_DISCORD_TARGET = "channel:1483402853648302081"
DEFAULT_DISCORD_CHANNEL = "discord"
DEFAULT_WORKSPACE = os.environ.get("POLYMARKET_WORKSPACE", str(PROJECT_ROOT))
MAX_TEXT_CHARS = 1400
MAX_SENT_IDS = 2000
CODEX_BOOTSTRAP_RECENT = 3
CODEX_RECENT_SESSION_SCAN_LIMIT = 40
DISCORD_API_BASE = "https://discord.com/api/v10"
MIRROR_OPENCLAW_REPORTS = False
DISCORD_CONTEXT_PATH = ".openclaw/discord-session-context.json"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bridge non-Discord polymarket Codex session reports into the Discord #polymarket channel."
    )
    parser.add_argument("--sessions-dir", default=str(DEFAULT_SESSIONS_DIR))
    parser.add_argument("--codex-index", default=str(DEFAULT_CODEX_INDEX))
    parser.add_argument("--codex-sessions-root", default=str(DEFAULT_CODEX_SESSIONS_ROOT))
    parser.add_argument("--openclaw-config", default=str(DEFAULT_OPENCLAW_CONFIG))
    parser.add_argument("--lock-file", default=str(DEFAULT_LOCK_FILE))
    parser.add_argument("--state-file", default=str(DEFAULT_STATE_FILE))
    parser.add_argument("--workspace", default=DEFAULT_WORKSPACE)
    parser.add_argument("--channel", default=DEFAULT_DISCORD_CHANNEL)
    parser.add_argument("--target", default=DEFAULT_DISCORD_TARGET)
    parser.add_argument("--account", default="default")
    parser.add_argument("--openclaw-bin", default=shutil.which("openclaw") or "openclaw")
    parser.add_argument("--bootstrap-only", action="store_true")
    parser.add_argument("--silent", action="store_true", default=True)
    return parser.parse_args()


def load_state(path: Path) -> dict[str, Any]:
    default_payload = {
        "sent_message_ids": [],
        "bootstrapped": False,
        "bootstrapped_sources": {},
        "session_threads": {},
        "processed_topic_message_ids": [],
    }
    if not path.exists():
        return default_payload
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default_payload
    if not isinstance(payload, dict):
        return default_payload
    payload.setdefault("sent_message_ids", [])
    payload.setdefault("bootstrapped", False)
    payload.setdefault("bootstrapped_sources", {})
    payload.setdefault("session_threads", {})
    payload.setdefault("processed_topic_message_ids", [])
    if payload.get("bootstrapped") and not payload["bootstrapped_sources"].get("openclaw"):
        payload["bootstrapped_sources"]["openclaw"] = True
    return payload


def save_state(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    trimmed = dict(payload)
    ids = list(trimmed.get("sent_message_ids", []))
    trimmed["sent_message_ids"] = ids[-MAX_SENT_IDS:]
    topic_ids = list(trimmed.get("processed_topic_message_ids", []))
    trimmed["processed_topic_message_ids"] = topic_ids[-MAX_SENT_IDS:]
    tmp = path.with_suffix(f"{path.suffix}.tmp")
    tmp.write_text(json.dumps(trimmed, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp.replace(path)


def ensure_codex_session_index_entry(*, codex_index: Path, session_id: str, thread_name: str) -> None:
    existing = False
    if codex_index.exists():
        try:
            for raw in codex_index.read_text(encoding="utf-8", errors="ignore").splitlines():
                text = raw.strip()
                if not text:
                    continue
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    continue
                if str(payload.get("id") or "").strip() == session_id:
                    existing = True
                    break
        except Exception:
            return
    if existing:
        return
    codex_index.parent.mkdir(parents=True, exist_ok=True)
    row = {
        "id": session_id,
        "thread_name": thread_name,
        "updated_at": time.strftime("%Y-%m-%dT%H:%M:%S.000000Z", time.gmtime()),
    }
    with codex_index.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def export_acp_binding_context(*, sessions_dir: Path, workspace: str, codex_index: Path) -> None:
    sessions_index = sessions_dir / "sessions.json"
    if not sessions_index.exists():
        return
    try:
        payload = json.loads(sessions_index.read_text(encoding="utf-8"))
    except Exception:
        return
    if not isinstance(payload, dict):
        return
    binding_key = ""
    binding_value: dict[str, Any] | None = None
    for key, value in payload.items():
        if not isinstance(value, dict):
            continue
        if ":acp:binding:discord:" not in str(key):
            continue
        if str(value.get("groupId") or "") != "1483402853648302081":
            continue
        binding_key = str(key)
        binding_value = value
        break
    if not binding_key or not isinstance(binding_value, dict):
        return
    acp = binding_value.get("acp")
    if not isinstance(acp, dict):
        return
    identity = acp.get("identity")
    if not isinstance(identity, dict):
        identity = {}
    out = {
        "workspace": workspace,
        "discord_channel_id": str(binding_value.get("groupId") or ""),
        "discord_channel_name": str(binding_value.get("groupChannel") or ""),
        "openclaw_session_key": binding_key,
        "openclaw_session_id": str(binding_value.get("sessionId") or ""),
        "acp_backend": str(acp.get("backend") or ""),
        "acp_agent": str(acp.get("agent") or ""),
        "acp_mode": str(acp.get("mode") or ""),
        "acpx_record_id": str(identity.get("acpxRecordId") or ""),
        "acpx_session_id": str(identity.get("acpxSessionId") or ""),
        "updated_at": int(time.time()),
    }
    acpx_session_id = str(identity.get("acpxSessionId") or "").strip()
    if acpx_session_id:
        ensure_codex_session_index_entry(
            codex_index=codex_index,
            session_id=acpx_session_id,
            thread_name="Discord #polymarket main channel",
        )
    target = Path(workspace).expanduser().resolve() / DISCORD_CONTEXT_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(out, ensure_ascii=False, indent=2), encoding="utf-8")


def load_main_acpx_session_id(sessions_dir: Path) -> str:
    sessions_index = sessions_dir / "sessions.json"
    if not sessions_index.exists():
        return ""
    try:
        payload = json.loads(sessions_index.read_text(encoding="utf-8"))
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    for key, value in payload.items():
        if ":acp:binding:discord:default:" not in str(key):
            continue
        if not isinstance(value, dict):
            continue
        if str(value.get("groupId") or "") != "1483402853648302081":
            continue
        acp = value.get("acp")
        if not isinstance(acp, dict):
            return ""
        identity = acp.get("identity")
        if not isinstance(identity, dict):
            return ""
        return str(identity.get("acpxSessionId") or "").strip()
    return ""


def iter_session_files(sessions_dir: Path) -> list[Path]:
    return sorted(
        [path for path in sessions_dir.glob("*.jsonl") if path.is_file()],
        key=lambda path: path.stat().st_mtime,
    )


def load_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            text = raw.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    return rows


def load_excluded_session_ids(sessions_dir: Path) -> set[str]:
    path = sessions_dir / "sessions.json"
    if not path.exists():
        return set()
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return set()
    if not isinstance(payload, dict):
        return set()
    excluded: set[str] = set()
    for key, value in payload.items():
        if not isinstance(value, dict):
            continue
        session_id = str(value.get("sessionId") or "").strip()
        if not session_id:
            continue
        if ":discord:" in str(key) or ":binding:" in str(key):
            excluded.add(session_id)
            continue
        if str(value.get("channel") or "").strip().lower() == "discord":
            excluded.add(session_id)
            continue
        origin = value.get("origin")
        if isinstance(origin, dict) and str(origin.get("provider") or "").strip().lower() == "discord":
            excluded.add(session_id)
    return excluded


def _message_text_blocks(message: dict[str, Any]) -> list[str]:
    content = message.get("content")
    if not isinstance(content, list):
        return []
    blocks: list[str] = []
    for item in content:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "text":
            continue
        text = str(item.get("text") or "").strip()
        if text:
            blocks.append(text)
    return blocks


def _joined_message_text(message: dict[str, Any]) -> str:
    text = "\n".join(_message_text_blocks(message)).strip()
    text = text.replace("[[reply_to_current]]", "").strip()
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def is_discord_origin(rows: list[dict[str, Any]]) -> bool:
    for row in rows[:16]:
        if row.get("type") != "message":
            continue
        message = row.get("message")
        if not isinstance(message, dict):
            continue
        if str(message.get("role") or "") != "user":
            continue
        text = _joined_message_text(message)
        if not text:
            continue
        if "A new session was started via /new or /reset." in text:
            return True
        if '"conversation_label": "Guild #polymarket channel id:1483402853648302081"' in text:
            return True
        if '"group_channel": "#polymarket"' in text and '"is_group_chat": true' in text:
            return True
    return False


def is_report_worthy(text: str) -> bool:
    normalized = str(text or "").strip()
    if len(normalized) < 60:
        return False
    lower = normalized.lower()
    ignore_patterns = [
        "session startup complete",
        "what do you want to tackle first",
        "i’m online and ready",
        "i'm online and ready",
        "`proj help`",
        "pm status",
    ]
    if any(pattern in lower for pattern in ignore_patterns):
        return False
    return True


def build_report_text(*, session_id: str, timestamp: str, workspace: str, body: str) -> str:
    cleaned = str(body or "").strip()
    cleaned = re.sub(r"\s+\n", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    if len(cleaned) > MAX_TEXT_CHARS:
        cleaned = f"{cleaned[:MAX_TEXT_CHARS].rstrip()}..."
    return (
        "Polymarket thread report\n"
        f"- session: `{session_id}`\n"
        f"- workspace: `{workspace}`\n"
        f"- time: `{timestamp}`\n"
        "- update:\n"
        f"{cleaned}"
    )


def build_report_text_with_source(
    *,
    source: str,
    session_id: str,
    timestamp: str,
    workspace: str,
    body: str,
    thread_name: str = "",
) -> str:
    cleaned = str(body or "").strip()
    cleaned = re.sub(r"\s+\n", "\n", cleaned)
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    if len(cleaned) > MAX_TEXT_CHARS:
        cleaned = f"{cleaned[:MAX_TEXT_CHARS].rstrip()}..."
    lines = [
        "Polymarket thread report",
        f"- source: `{source}`",
        f"- session: `{session_id}`",
    ]
    if thread_name:
        lines.append(f"- thread: `{thread_name}`")
    lines.extend(
        [
            f"- workspace: `{workspace}`",
            f"- time: `{timestamp}`",
            "- update:",
            cleaned,
        ]
    )
    return "\n".join(lines)


def build_thread_starter_text(*, source: str, session_id: str, thread_name: str, workspace: str) -> str:
    lines = [
        "Polymarket session thread opened",
        f"- source: `{source}`",
        f"- session: `{session_id}`",
    ]
    if thread_name:
        lines.append(f"- thread: `{thread_name}`")
    lines.extend(
        [
            f"- workspace: `{workspace}`",
            "- updates: this Discord thread will receive follow-up progress for this task.",
        ]
    )
    return "\n".join(lines)


def send_discord_message(
    *,
    openclaw_bin: str,
    channel: str,
    target: str,
    account: str,
    body: str,
    silent: bool,
) -> tuple[bool, str, dict[str, Any]]:
    cmd = [
        openclaw_bin,
        "message",
        "send",
        "--channel",
        channel,
        "--target",
        target,
        "--account",
        account,
        "--message",
        body,
        "--json",
    ]
    if silent:
        cmd.append("--silent")
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    ok = proc.returncode == 0
    detail = (proc.stdout or proc.stderr or "").strip()
    payload: dict[str, Any] = {}
    if proc.stdout:
        try:
            parsed = json.loads(proc.stdout)
            if isinstance(parsed, dict):
                payload = parsed
        except json.JSONDecodeError:
            payload = {}
    return ok, detail, payload


def parse_discord_channel_id(target: str) -> str:
    text = str(target or "").strip()
    if text.startswith("channel:"):
        return text.split(":", 1)[1].strip()
    return text


def parse_topic_thread_id_from_filename(path: Path) -> str:
    match = re.search(r"-topic-(\d+)\.jsonl$", path.name)
    return match.group(1) if match else ""


def load_discord_token(config_path: Path) -> str:
    try:
        payload = json.loads(config_path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    if not isinstance(payload, dict):
        return ""
    channels = payload.get("channels")
    if not isinstance(channels, dict):
        return ""
    discord = channels.get("discord")
    if not isinstance(discord, dict):
        return ""
    token = str(discord.get("token") or "").strip()
    return token


def session_thread_key(source: str, session_id: str) -> str:
    return f"{source}:{session_id}"


def find_codex_session_for_thread(state: dict[str, Any], thread_id: str) -> dict[str, str] | None:
    mappings = state.get("session_threads")
    if not isinstance(mappings, dict):
        return None
    for key, value in mappings.items():
        if not isinstance(value, dict):
            continue
        if str(value.get("thread_id") or "") != str(thread_id):
            continue
        if not str(key).startswith("codex-desktop:"):
            continue
        session_id = str(value.get("session_id") or "").strip()
        workspace = str(value.get("workspace") or "").strip()
        if not session_id or not workspace:
            continue
        return {
            "session_id": session_id,
            "workspace": workspace,
            "thread_id": str(thread_id),
        }
    return None


def make_thread_name(source: str, session_id: str, thread_name: str) -> str:
    base = re.sub(r"\s+", " ", str(thread_name or "").replace("\n", " ")).strip()
    prefix = f"{source[:6]}-{session_id[:8]}"
    if base:
        name = f"{prefix} {base}"
    else:
        name = f"{prefix} task"
    return name[:95].rstrip()


def _first_text_content(message: dict[str, Any]) -> str:
    content = message.get("content")
    if not isinstance(content, list):
        return ""
    for item in content:
        if not isinstance(item, dict):
            continue
        if item.get("type") != "text":
            continue
        text = str(item.get("text") or "").strip()
        if text:
            return text
    return ""


def extract_topic_user_prompt(raw: str) -> str:
    text = str(raw or "").strip()
    if not text:
        return ""
    if text.startswith("A new session was started via /new or /reset."):
        return ""
    marker = "Sender (untrusted metadata):"
    if marker in text:
        try:
            tail = text.split(marker, 1)[1]
            parts = tail.split("```")
            if len(parts) >= 3:
                text = parts[-1].strip()
        except Exception:
            pass
    text = text.replace("[[reply_to_current]]", "").strip()
    return text


def collect_topic_inbound_messages(
    *,
    sessions_dir: Path,
    state: dict[str, Any],
    processed_ids: set[str],
) -> list[dict[str, str]]:
    candidates: list[dict[str, str]] = []
    for session_file in sorted(sessions_dir.glob("*-topic-*.jsonl"), key=lambda path: path.stat().st_mtime):
        thread_id = parse_topic_thread_id_from_filename(session_file)
        if not thread_id:
            continue
        codex_binding = find_codex_session_for_thread(state, thread_id)
        if codex_binding is None:
            continue
        rows = load_jsonl(session_file)
        for row in rows:
            if row.get("type") != "message":
                continue
            message = row.get("message")
            if not isinstance(message, dict):
                continue
            if str(message.get("role") or "") != "user":
                continue
            row_id = str(row.get("id") or "").strip()
            if not row_id:
                continue
            state_id = f"topic:{thread_id}:{row_id}"
            if state_id in processed_ids:
                continue
            prompt = extract_topic_user_prompt(_first_text_content(message))
            if not prompt:
                continue
            candidates.append(
                {
                    "state_id": state_id,
                    "thread_id": thread_id,
                    "timestamp": str(row.get("timestamp") or ""),
                    "prompt": prompt,
                    "codex_session_id": codex_binding["session_id"],
                    "workspace": codex_binding["workspace"],
                }
            )
    candidates.sort(key=lambda item: (item["timestamp"], item["thread_id"], item["state_id"]))
    return candidates


def resume_codex_session(
    *,
    session_id: str,
    prompt: str,
    workspace: str,
) -> tuple[bool, str]:
    with tempfile.NamedTemporaryFile(prefix="codex-bridge-", suffix=".out", delete=False) as handle:
        output_path = Path(handle.name)
    cmd = [
        "codex",
        "-c",
        f'experimental_compact_prompt_file="{DEFAULT_HOME / ".codex" / "default"}"',
        "exec",
        "--dangerously-bypass-approvals-and-sandbox",
        "-C",
        workspace,
        "--output-last-message",
        str(output_path),
        "resume",
        session_id,
        prompt,
    ]
    proc = subprocess.run(cmd, text=True, capture_output=True, check=False)
    text = ""
    if output_path.exists():
        try:
            text = output_path.read_text(encoding="utf-8").strip()
        except Exception:
            text = ""
        output_path.unlink(missing_ok=True)
    if proc.returncode == 0 and text:
        return True, text
    detail = text or (proc.stderr or proc.stdout or "").strip()
    return False, detail


def discord_api_post(url: str, token: str, body: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        url,
        data=json.dumps(body, ensure_ascii=False).encode("utf-8"),
        headers={
            "Authorization": f"Bot {token}",
            "Content-Type": "application/json",
            "User-Agent": "poly-codex-bridge/1.0",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        raw = response.read().decode("utf-8")
    parsed = json.loads(raw)
    return parsed if isinstance(parsed, dict) else {}


def send_discord_message_via_api(*, channel_id: str, token: str, body: str) -> dict[str, Any]:
    return discord_api_post(
        f"{DISCORD_API_BASE}/channels/{channel_id}/messages",
        token,
        {
            "content": body,
            "allowed_mentions": {"parse": []},
        },
    )


def create_discord_thread_from_message(*, channel_id: str, message_id: str, token: str, name: str) -> str:
    payload = discord_api_post(
        f"{DISCORD_API_BASE}/channels/{channel_id}/messages/{message_id}/threads",
        token,
        {
            "name": name,
            "auto_archive_duration": 1440,
        },
    )
    thread_id = str(payload.get("id") or "").strip()
    return thread_id


def ensure_session_thread(
    *,
    candidate: dict[str, str],
    state: dict[str, Any],
    parent_target: str,
    openclaw_bin: str,
    channel: str,
    account: str,
    silent: bool,
    discord_token: str,
) -> tuple[str, str]:
    key = session_thread_key(candidate.get("source", "unknown"), candidate["session_id"])
    mapping = state.setdefault("session_threads", {})
    existing = mapping.get(key)
    if isinstance(existing, dict):
        thread_id = str(existing.get("thread_id") or "").strip()
        if thread_id:
            return thread_id, str(existing.get("thread_name") or "")
    parent_channel_id = parse_discord_channel_id(parent_target)
    if not parent_channel_id or not discord_token:
        return "", ""
    thread_name = make_thread_name(candidate.get("source", "task"), candidate["session_id"], candidate.get("thread_name", ""))
    starter = build_thread_starter_text(
        source=candidate.get("source", "unknown"),
        session_id=candidate["session_id"],
        thread_name=candidate.get("thread_name", ""),
        workspace=candidate["workspace"],
    )
    ok, detail, payload = send_discord_message(
        openclaw_bin=openclaw_bin,
        channel=channel,
        target=parent_target,
        account=account,
        body=starter,
        silent=silent,
    )
    if not ok:
        state["last_error"] = detail
        return "", ""
    result = payload.get("payload", {}).get("result", {}) if isinstance(payload.get("payload"), dict) else {}
    message_id = str(result.get("messageId") or "").strip()
    if not message_id:
        state["last_error"] = "starter message sent but messageId missing"
        return "", ""
    try:
        thread_id = create_discord_thread_from_message(
            channel_id=parent_channel_id,
            message_id=message_id,
            token=discord_token,
            name=thread_name,
        )
    except urllib.error.HTTPError as err:
        state["last_error"] = f"discord thread create failed: {err.code}"
        return "", ""
    except Exception as err:
        state["last_error"] = f"discord thread create failed: {err}"
        return "", ""
    if not thread_id:
        state["last_error"] = "discord thread create returned empty id"
        return "", ""
    mapping[key] = {
        "thread_id": thread_id,
        "thread_name": thread_name,
        "starter_message_id": message_id,
        "source": candidate.get("source", "unknown"),
        "session_id": candidate["session_id"],
        "workspace": candidate["workspace"],
        "updated_at": int(time.time()),
    }
    return thread_id, thread_name


def collect_openclaw_candidates(
    *,
    sessions_dir: Path,
    workspace: str,
    sent_ids: set[str],
) -> tuple[list[dict[str, str]], list[str]]:
    candidates: list[dict[str, str]] = []
    existing_ids: list[str] = []
    excluded_session_ids = load_excluded_session_ids(sessions_dir)
    for session_file in iter_session_files(sessions_dir):
        rows = load_jsonl(session_file)
        if not rows:
            continue
        session_id = ""
        for row in rows:
            if row.get("type") == "session":
                session_id = str(row.get("id") or session_file.stem)
                break
        if not session_id:
            session_id = session_file.stem
        if session_id in excluded_session_ids:
            continue
        if is_discord_origin(rows):
            continue
        for row in rows:
            if row.get("type") != "message":
                continue
            message = row.get("message")
            if not isinstance(message, dict):
                continue
            if str(message.get("role") or "") != "assistant":
                continue
            if str(message.get("model") or "") == "delivery-mirror":
                continue
            message_id = str(row.get("id") or "")
            if not message_id:
                continue
            state_id = f"openclaw:{message_id}"
            existing_ids.append(state_id)
            if state_id in sent_ids or message_id in sent_ids:
                continue
            text = _joined_message_text(message)
            if not is_report_worthy(text):
                continue
            candidates.append(
                {
                    "message_id": state_id,
                    "session_id": session_id,
                    "timestamp": str(row.get("timestamp") or ""),
                    "text": text,
                    "workspace": workspace,
                    "source": "openclaw",
                    "thread_name": "",
                }
            )
    candidates.sort(key=lambda item: (item["timestamp"], item["session_id"], item["message_id"]))
    return candidates, existing_ids


def load_recent_codex_sessions(index_path: Path, limit: int = CODEX_RECENT_SESSION_SCAN_LIMIT) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    if not index_path.exists():
        return rows
    with index_path.open("r", encoding="utf-8") as handle:
        for raw in handle:
            text = raw.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                continue
            if not isinstance(payload, dict):
                continue
            session_id = str(payload.get("id") or "").strip()
            if not session_id:
                continue
            rows.append(
                {
                    "session_id": session_id,
                    "thread_name": str(payload.get("thread_name") or "").strip(),
                    "updated_at": str(payload.get("updated_at") or "").strip(),
                }
            )
    return rows[-limit:]


def _codex_session_id_from_file(path: Path) -> str:
    match = re.search(r"([0-9a-f]{8}-[0-9a-f-]{27,})\.jsonl$", path.name)
    return match.group(1) if match else path.stem


def find_codex_rollout_file(root: Path, session_id: str) -> Path | None:
    for path in root.glob(f"**/rollout-*{session_id}.jsonl"):
        if path.is_file():
            return path
    return None


def _hash_text(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()[:12]


def _codex_task_complete_text(row: dict[str, Any]) -> str:
    if row.get("type") != "event_msg":
        return ""
    payload = row.get("payload")
    if not isinstance(payload, dict):
        return ""
    if str(payload.get("type") or "") != "task_complete":
        return ""
    text = str(payload.get("last_agent_message") or "").strip()
    text = text.replace("[[reply_to_current]]", "").strip()
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text


def collect_codex_candidates(
    *,
    index_path: Path,
    sessions_root: Path,
    workspace: str,
    sent_ids: set[str],
) -> tuple[list[dict[str, str]], list[str]]:
    candidates: list[dict[str, str]] = []
    existing_ids: list[str] = []
    recent_sessions = load_recent_codex_sessions(index_path)
    workspace_norm = str(Path(workspace).expanduser().resolve())

    for session_meta in recent_sessions:
        session_id = session_meta["session_id"]
        rollout_file = find_codex_rollout_file(sessions_root, session_id)
        if rollout_file is None:
            continue
        rows = load_jsonl(rollout_file)
        if not rows:
            continue
        session_id = _codex_session_id_from_file(rollout_file)
        rollout_workspace = ""
        for row in rows:
            if row.get("type") != "session_meta":
                continue
            payload = row.get("payload")
            if not isinstance(payload, dict):
                continue
            session_id = str(payload.get("id") or session_id)
            rollout_workspace = str(payload.get("cwd") or "").strip()
            break
        if not rollout_workspace:
            continue
        try:
            rollout_workspace_norm = str(Path(rollout_workspace).expanduser().resolve())
        except Exception:
            rollout_workspace_norm = rollout_workspace
        if rollout_workspace_norm != workspace_norm:
            continue
        thread_name = session_meta.get("thread_name", "")
        latest_candidate: dict[str, str] | None = None
        for row in rows:
            text = _codex_task_complete_text(row)
            if not is_report_worthy(text):
                continue
            timestamp = str(row.get("timestamp") or "")
            state_id = f"codex:{session_id}:{timestamp}:{_hash_text(text)}"
            candidate = (
                {
                    "message_id": state_id,
                    "session_id": session_id,
                    "timestamp": timestamp,
                    "text": text,
                    "workspace": rollout_workspace_norm,
                    "source": "codex-desktop",
                    "thread_name": thread_name,
                }
            )
            if latest_candidate is None or (
                candidate["timestamp"],
                candidate["message_id"],
            ) > (
                latest_candidate["timestamp"],
                latest_candidate["message_id"],
            ):
                latest_candidate = candidate
        if latest_candidate is None:
            continue
        existing_ids.append(latest_candidate["message_id"])
        if latest_candidate["message_id"] in sent_ids:
            continue
        candidates.append(latest_candidate)
    candidates.sort(key=lambda item: (item["timestamp"], item["session_id"], item["message_id"]))
    return candidates, existing_ids


def codex_bootstrap_seed_ids(candidates: list[dict[str, str]], existing_ids: list[str]) -> list[str]:
    if not candidates:
        return existing_ids
    latest_per_session: dict[str, dict[str, str]] = {}
    for candidate in candidates:
        session_id = candidate["session_id"]
        current = latest_per_session.get(session_id)
        if current is None or (
            candidate["timestamp"],
            candidate["message_id"],
        ) > (
            current["timestamp"],
            current["message_id"],
        ):
            latest_per_session[session_id] = candidate
    keep_ids = {
        item["message_id"]
        for item in sorted(
            latest_per_session.values(),
            key=lambda item: (item["timestamp"], item["session_id"], item["message_id"]),
        )[-CODEX_BOOTSTRAP_RECENT:]
    }
    return [message_id for message_id in existing_ids if message_id not in keep_ids]


def main() -> int:
    args = parse_args()
    sessions_dir = Path(args.sessions_dir).expanduser().resolve()
    codex_index = Path(args.codex_index).expanduser().resolve()
    codex_sessions_root = Path(args.codex_sessions_root).expanduser().resolve()
    openclaw_config = Path(args.openclaw_config).expanduser().resolve()
    lock_file = Path(args.lock_file).expanduser().resolve()
    state_file = Path(args.state_file).expanduser().resolve()
    lock_file.parent.mkdir(parents=True, exist_ok=True)
    lock_handle = lock_file.open("w", encoding="utf-8")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        return 0
    state = load_state(state_file)
    export_acp_binding_context(
        sessions_dir=sessions_dir,
        workspace=str(args.workspace),
        codex_index=codex_index,
    )
    main_acpx_session_id = load_main_acpx_session_id(sessions_dir)
    discord_token = load_discord_token(openclaw_config)
    sent_ids = set(str(item) for item in state.get("sent_message_ids", []))
    processed_topic_ids = set(str(item) for item in state.get("processed_topic_message_ids", []))
    if MIRROR_OPENCLAW_REPORTS:
        openclaw_candidates, openclaw_existing_ids = collect_openclaw_candidates(
            sessions_dir=sessions_dir,
            workspace=str(args.workspace),
            sent_ids=sent_ids,
        )
    else:
        openclaw_candidates, openclaw_existing_ids = [], []
    codex_candidates, codex_existing_ids = collect_codex_candidates(
        index_path=codex_index,
        sessions_root=codex_sessions_root,
        workspace=str(args.workspace),
        sent_ids=sent_ids,
    )
    candidates = [*openclaw_candidates, *codex_candidates]
    candidates.sort(key=lambda item: (item["timestamp"], item["session_id"], item["message_id"]))
    topic_inbound = collect_topic_inbound_messages(
        sessions_dir=sessions_dir,
        state=state,
        processed_ids=processed_topic_ids,
    )

    bootstrapped_sources = dict(state.get("bootstrapped_sources") or {})
    pending_bootstrap_ids: list[str] = []
    if args.bootstrap_only or not bootstrapped_sources.get("openclaw"):
        pending_bootstrap_ids.extend(openclaw_existing_ids)
        bootstrapped_sources["openclaw"] = True
    if args.bootstrap_only or not bootstrapped_sources.get("codex"):
        pending_bootstrap_ids.extend(codex_bootstrap_seed_ids(codex_candidates, codex_existing_ids))
        bootstrapped_sources["codex"] = True
    if args.bootstrap_only or pending_bootstrap_ids:
        state["sent_message_ids"] = list(dict.fromkeys([*state.get("sent_message_ids", []), *pending_bootstrap_ids]))
        state["bootstrapped"] = True
        state["bootstrapped_sources"] = bootstrapped_sources
        state["bootstrapped_at"] = state.get("bootstrapped_at") or int(time.time())
        save_state(state_file, state)
        if args.bootstrap_only:
            return 0

    new_sent_ids = list(state.get("sent_message_ids", []))
    new_processed_topic_ids = list(state.get("processed_topic_message_ids", []))
    for candidate in candidates:
        direct_main_channel = (
            candidate.get("source") == "codex-desktop"
            and candidate["session_id"] == main_acpx_session_id
        )
        thread_id = ""
        if not direct_main_channel:
            thread_id, _thread_label = ensure_session_thread(
                candidate=candidate,
                state=state,
                parent_target=str(args.target),
                openclaw_bin=str(args.openclaw_bin),
                channel=str(args.channel),
                account=str(args.account),
                silent=bool(args.silent),
                discord_token=discord_token,
            )
        report = (
            candidate["text"].strip()
            if direct_main_channel
            else build_report_text_with_source(
                source=candidate.get("source", "unknown"),
                session_id=candidate["session_id"],
                timestamp=candidate["timestamp"],
                workspace=candidate["workspace"],
                body=candidate["text"],
                thread_name=candidate.get("thread_name", ""),
            )
        )
        if thread_id and discord_token:
            try:
                send_discord_message_via_api(channel_id=thread_id, token=discord_token, body=report)
                ok, detail = True, ""
            except urllib.error.HTTPError as err:
                ok, detail = False, f"discord thread send failed: {err.code}"
            except Exception as err:
                ok, detail = False, f"discord thread send failed: {err}"
        else:
            target = str(args.target)
            ok, detail, _payload = send_discord_message(
                openclaw_bin=str(args.openclaw_bin),
                channel=str(args.channel),
                target=target,
                account=str(args.account),
                body=report,
                silent=bool(args.silent),
            )
        if ok:
            new_sent_ids.append(candidate["message_id"])
            key = session_thread_key(candidate.get("source", "unknown"), candidate["session_id"])
            mapping = state.setdefault("session_threads", {})
            existing = mapping.get(key)
            if isinstance(existing, dict):
                existing["updated_at"] = int(time.time())
        else:
            state["last_error"] = detail
            save_state(state_file, state)
            return 1

    for inbound in topic_inbound:
        ok, reply = resume_codex_session(
            session_id=inbound["codex_session_id"],
            prompt=inbound["prompt"],
            workspace=inbound["workspace"],
        )
        if not ok:
            state["last_error"] = reply
            save_state(state_file, state)
            return 1
        try:
            send_discord_message_via_api(
                channel_id=inbound["thread_id"],
                token=discord_token,
                body=reply,
            )
        except urllib.error.HTTPError as err:
            state["last_error"] = f"discord topic reply failed: {err.code}"
            save_state(state_file, state)
            return 1
        except Exception as err:
            state["last_error"] = f"discord topic reply failed: {err}"
            save_state(state_file, state)
            return 1
        new_processed_topic_ids.append(inbound["state_id"])

    state["sent_message_ids"] = new_sent_ids
    state["processed_topic_message_ids"] = new_processed_topic_ids
    state["bootstrapped"] = True
    state["bootstrapped_sources"] = bootstrapped_sources
    state["last_run_at"] = int(time.time())
    state["last_sent_count"] = len(candidates)
    save_state(state_file, state)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
