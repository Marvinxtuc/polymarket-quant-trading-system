from __future__ import annotations

import json
import sqlite3
import time
import hashlib
import re
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any


_ACTION_STATUS_MAP = {
    "ignore": "ignored",
    "watch": "watched",
    "buy_small": "approved",
    "buy_normal": "approved",
    "follow": "approved",
    "close_partial": "approved",
    "close_all": "approved",
}

_MARKET_WINDOW_PATTERN = re.compile(r"-(5m|15m|30m|1h)-(\d{10})$")
_MARKET_WINDOW_SECONDS = {
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
}


def _candidate_market_end_ts(market_slug: str) -> int:
    normalized = str(market_slug or "").strip().lower()
    if not normalized:
        return 0
    match = _MARKET_WINDOW_PATTERN.search(normalized)
    if match is None:
        return 0
    duration_seconds = _MARKET_WINDOW_SECONDS.get(str(match.group(1) or "").strip().lower())
    start_ts = int(match.group(2) or 0)
    if duration_seconds is None or start_ts <= 0:
        return 0
    return start_ts + duration_seconds


class PersonalTerminalStore:
    def __init__(self, path: str) -> None:
        self.path = str(path or "").strip()
        self._closed = False
        self.ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        if self._closed:
            raise RuntimeError("personal terminal store is closed")
        if not self.path:
            raise ValueError("candidate db path is required")
        parent = Path(self.path).expanduser().parent
        if str(parent) not in {"", "."}:
            parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def connection(self):
        conn = self._connect()
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def close(self) -> None:
        self._closed = True

    def __enter__(self) -> PersonalTerminalStore:
        if self._closed:
            self._closed = False
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    @staticmethod
    def _payload_dict(value: object) -> dict[str, Any]:
        if is_dataclass(value):
            payload = asdict(value)
        elif isinstance(value, Mapping):
            payload = dict(value)
        else:
            raise TypeError(f"unsupported payload type: {type(value)!r}")
        return {str(key): payload[key] for key in payload}

    @staticmethod
    def _decode_payload(raw: object) -> dict[str, Any]:
        if not isinstance(raw, str) or not raw.strip():
            return {}
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return {}
        if not isinstance(payload, dict):
            return {}
        return payload

    @staticmethod
    def _day_key_from_ts(ts: int) -> str:
        if int(ts or 0) <= 0:
            return ""
        return time.strftime("%Y-%m-%d", time.localtime(int(ts)))

    @staticmethod
    def _window_cutoff_ts(days: int) -> int:
        return int(time.time()) - max(1, int(days)) * 86400

    def ensure_schema(self) -> None:
        with self.connection() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS candidates (
                    id TEXT PRIMARY KEY,
                    signal_id TEXT NOT NULL DEFAULT '',
                    trace_id TEXT NOT NULL DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'pending',
                    selected_action TEXT NOT NULL DEFAULT '',
                    wallet TEXT NOT NULL DEFAULT '',
                    market_slug TEXT NOT NULL DEFAULT '',
                    token_id TEXT NOT NULL DEFAULT '',
                    outcome TEXT NOT NULL DEFAULT '',
                    side TEXT NOT NULL DEFAULT '',
                    score REAL NOT NULL DEFAULT 0,
                    confidence REAL NOT NULL DEFAULT 0,
                    suggested_action TEXT NOT NULL DEFAULT '',
                    skip_reason TEXT,
                    created_ts INTEGER NOT NULL DEFAULT 0,
                    expires_ts INTEGER NOT NULL DEFAULT 0,
                    updated_ts INTEGER NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_candidates_status_score
                    ON candidates(status, score DESC, created_ts DESC);
                CREATE INDEX IF NOT EXISTS idx_candidates_wallet
                    ON candidates(wallet, created_ts DESC);

                CREATE TABLE IF NOT EXISTS candidate_actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    candidate_id TEXT NOT NULL,
                    action TEXT NOT NULL,
                    idempotency_key TEXT NOT NULL DEFAULT '',
                    notional REAL NOT NULL DEFAULT 0,
                    note TEXT NOT NULL DEFAULT '',
                    created_ts INTEGER NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_candidate_actions_candidate
                    ON candidate_actions(candidate_id, created_ts DESC);

                CREATE TABLE IF NOT EXISTS wallet_profiles (
                    wallet TEXT PRIMARY KEY,
                    tag TEXT NOT NULL DEFAULT '',
                    trust_score REAL NOT NULL DEFAULT 0,
                    followability_score REAL NOT NULL DEFAULT 0,
                    avg_hold_minutes REAL,
                    category TEXT NOT NULL DEFAULT '',
                    enabled INTEGER NOT NULL DEFAULT 1,
                    notes TEXT NOT NULL DEFAULT '',
                    updated_ts INTEGER NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_wallet_profiles_rank
                    ON wallet_profiles(enabled, trust_score DESC, updated_ts DESC);

                CREATE TABLE IF NOT EXISTS journal_entries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    candidate_id TEXT NOT NULL DEFAULT '',
                    market_slug TEXT NOT NULL DEFAULT '',
                    wallet TEXT NOT NULL DEFAULT '',
                    action TEXT NOT NULL DEFAULT '',
                    rationale TEXT NOT NULL DEFAULT '',
                    result_tag TEXT,
                    pnl_realized REAL,
                    created_ts INTEGER NOT NULL DEFAULT 0,
                    payload_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_journal_entries_created
                    ON journal_entries(created_ts DESC);
                """
            )
            columns = {
                str(row["name"])
                for row in conn.execute("PRAGMA table_info(candidate_actions)").fetchall()
            }
            if "idempotency_key" not in columns:
                conn.execute(
                    "ALTER TABLE candidate_actions ADD COLUMN idempotency_key TEXT NOT NULL DEFAULT ''"
                )
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_candidate_actions_idempotency
                    ON candidate_actions(candidate_id, idempotency_key)
                    WHERE idempotency_key <> ''
                """
            )

    def upsert_candidate(self, candidate: object) -> dict[str, Any]:
        payload = self._payload_dict(candidate)
        now = int(time.time())
        payload.setdefault("status", "pending")
        payload.setdefault("selected_action", "")
        payload.setdefault("created_ts", now)
        payload["updated_ts"] = int(payload.get("updated_ts") or now)
        record = json.dumps(payload, ensure_ascii=False)
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO candidates (
                    id, signal_id, trace_id, status, selected_action, wallet, market_slug,
                    token_id, outcome, side, score, confidence, suggested_action, skip_reason,
                    created_ts, expires_ts, updated_ts, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(id) DO UPDATE SET
                    signal_id=excluded.signal_id,
                    trace_id=excluded.trace_id,
                    status=excluded.status,
                    selected_action=excluded.selected_action,
                    wallet=excluded.wallet,
                    market_slug=excluded.market_slug,
                    token_id=excluded.token_id,
                    outcome=excluded.outcome,
                    side=excluded.side,
                    score=excluded.score,
                    confidence=excluded.confidence,
                    suggested_action=excluded.suggested_action,
                    skip_reason=excluded.skip_reason,
                    created_ts=excluded.created_ts,
                    expires_ts=excluded.expires_ts,
                    updated_ts=excluded.updated_ts,
                    payload_json=excluded.payload_json
                """,
                (
                    str(payload.get("id") or ""),
                    str(payload.get("signal_id") or ""),
                    str(payload.get("trace_id") or ""),
                    str(payload.get("status") or "pending"),
                    str(payload.get("selected_action") or ""),
                    str(payload.get("wallet") or ""),
                    str(payload.get("market_slug") or ""),
                    str(payload.get("token_id") or ""),
                    str(payload.get("outcome") or ""),
                    str(payload.get("side") or ""),
                    float(payload.get("score") or 0.0),
                    float(payload.get("confidence") or 0.0),
                    str(payload.get("suggested_action") or ""),
                    str(payload.get("skip_reason") or ""),
                    int(payload.get("created_ts") or now),
                    int(payload.get("expires_ts") or 0),
                    int(payload.get("updated_ts") or now),
                    record,
                ),
            )
        return payload

    def get_candidate(self, candidate_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM candidates WHERE id = ?",
                (str(candidate_id or ""),),
            ).fetchone()
        return self._candidate_row_to_dict(row)

    def _candidate_row_to_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        payload = self._decode_payload(row["payload_json"])
        payload.setdefault("id", str(row["id"] or ""))
        payload["signal_id"] = str(row["signal_id"] or payload.get("signal_id") or "")
        payload["trace_id"] = str(row["trace_id"] or payload.get("trace_id") or "")
        payload["status"] = str(row["status"] or payload.get("status") or "pending")
        payload["selected_action"] = str(row["selected_action"] or payload.get("selected_action") or "")
        payload["wallet"] = str(row["wallet"] or payload.get("wallet") or "")
        payload["market_slug"] = str(row["market_slug"] or payload.get("market_slug") or "")
        payload["token_id"] = str(row["token_id"] or payload.get("token_id") or "")
        payload["outcome"] = str(row["outcome"] or payload.get("outcome") or "")
        payload["side"] = str(row["side"] or payload.get("side") or "")
        payload["score"] = float(row["score"] or payload.get("score") or 0.0)
        payload["confidence"] = float(row["confidence"] or payload.get("confidence") or 0.0)
        payload["suggested_action"] = str(row["suggested_action"] or payload.get("suggested_action") or "")
        payload["skip_reason"] = str(row["skip_reason"] or payload.get("skip_reason") or "") or None
        payload["created_ts"] = int(row["created_ts"] or payload.get("created_ts") or 0)
        payload["expires_ts"] = int(row["expires_ts"] or payload.get("expires_ts") or 0)
        payload["updated_ts"] = int(row["updated_ts"] or payload.get("updated_ts") or 0)
        return payload

    @staticmethod
    def _candidate_search_blob(payload: Mapping[str, Any]) -> str:
        try:
            return json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).lower()
        except Exception:
            return str(payload).lower()

    @staticmethod
    def _candidate_sort_value(payload: Mapping[str, Any], sort: str) -> Any:
        normalized = str(sort or "score").strip().lower()
        if normalized in {"freshness", "recency"}:
            return int(payload.get("updated_ts") or payload.get("created_ts") or 0)
        if normalized in {"score", "wallet_score", "confidence", "observed_notional", "source_wallet_count", "momentum_5m", "momentum_30m", "chase_pct"}:
            return float(payload.get(normalized) or 0.0)
        if normalized in {"created_ts", "updated_ts", "expires_ts"}:
            return int(payload.get(normalized) or 0)
        if normalized in {"status", "selected_action", "suggested_action", "wallet", "market_slug", "side", "trace_id", "signal_id"}:
            return str(payload.get(normalized) or "").strip().lower()
        if normalized == "default":
            return (
                int(payload.get("updated_ts") or payload.get("created_ts") or 0),
                float(payload.get("score") or 0.0),
            )
        return (
            int(payload.get("updated_ts") or payload.get("created_ts") or 0),
            float(payload.get("score") or 0.0),
        )

    @staticmethod
    def _candidate_matches_action(payload: Mapping[str, Any], action: str) -> bool:
        normalized = str(action or "").strip().lower()
        if not normalized:
            return True
        haystack = " ".join(
            str(payload.get(key) or "").strip().lower()
            for key in ("action", "selected_action", "suggested_action", "review_action", "status", "skip_reason", "note", "result_tag")
        )
        tokens = [part for chunk in normalized.split(",") for part in chunk.split() if part]
        if not tokens:
            return True
        return any(token in haystack for token in tokens)

    @staticmethod
    def _candidate_matches_search(payload: Mapping[str, Any], search: str) -> bool:
        normalized = " ".join(str(search or "").strip().lower().split())
        if not normalized:
            return True
        blob = PersonalTerminalStore._candidate_search_blob(payload)
        return all(part in blob for part in normalized.split(" ") if part)

    def expire_candidates(self, now: int | None = None) -> int:
        current_ts = int(now or time.time())
        with self.connection() as conn:
            cursor = conn.execute(
                """
                UPDATE candidates
                   SET status = 'expired', updated_ts = ?
                 WHERE status IN ('pending', 'watched', 'approved')
                   AND expires_ts > 0
                   AND expires_ts < ?
                """,
                (current_ts, current_ts),
            )
            updated = int(cursor.rowcount or 0)
            rows = conn.execute(
                """
                SELECT id, market_slug, expires_ts
                  FROM candidates
                 WHERE status IN ('pending', 'watched', 'approved')
                """
            ).fetchall()
            for row in rows:
                market_end_ts = _candidate_market_end_ts(str(row["market_slug"] or ""))
                if market_end_ts <= 0 or market_end_ts >= current_ts:
                    continue
                next_expiry = int(row["expires_ts"] or 0)
                if next_expiry <= 0 or market_end_ts < next_expiry:
                    next_expiry = market_end_ts
                expire_cursor = conn.execute(
                    """
                    UPDATE candidates
                       SET status = 'expired', expires_ts = ?, updated_ts = ?
                     WHERE id = ?
                    """,
                    (next_expiry, current_ts, str(row["id"] or "")),
                )
                updated += int(expire_cursor.rowcount or 0)
            return updated

    def list_candidates(
        self,
        *,
        statuses: list[str] | tuple[str, ...] | None = None,
        limit: int = 50,
        include_expired: bool = False,
        wallet: str = "",
        market_slug: str = "",
        candidate_id: str = "",
        trace_id: str = "",
        signal_id: str = "",
        search: str = "",
        side: str = "",
        action: str = "",
        sort: str = "score",
        order: str = "desc",
    ) -> list[dict[str, Any]]:
        self.expire_candidates()
        clauses: list[str] = []
        params: list[object] = []
        if statuses:
            placeholders = ", ".join("?" for _ in statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend([str(status) for status in statuses])
        normalized_wallet = str(wallet or "").strip().lower()
        if normalized_wallet:
            clauses.append("LOWER(wallet) = ?")
            params.append(normalized_wallet)
        normalized_market_slug = str(market_slug or "").strip().lower()
        if normalized_market_slug:
            clauses.append("LOWER(market_slug) = ?")
            params.append(normalized_market_slug)
        normalized_candidate_id = str(candidate_id or "").strip()
        if normalized_candidate_id:
            clauses.append("id = ?")
            params.append(normalized_candidate_id)
        normalized_trace_id = str(trace_id or "").strip()
        if normalized_trace_id:
            clauses.append("trace_id = ?")
            params.append(normalized_trace_id)
        normalized_signal_id = str(signal_id or "").strip()
        if normalized_signal_id:
            clauses.append("signal_id = ?")
            params.append(normalized_signal_id)
        if not include_expired:
            clauses.append("status <> 'expired'")
            clauses.append("(expires_ts = 0 OR expires_ts >= ?)")
            params.append(int(time.time()))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT *
              FROM candidates
              {where}
        """
        with self.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        items: list[dict[str, Any]] = []
        normalized_side = str(side or "").strip().upper()
        for row in rows:
            payload = self._candidate_row_to_dict(row)
            if payload is None:
                continue
            if normalized_side and str(payload.get("side") or "").strip().upper() != normalized_side:
                continue
            if not self._candidate_matches_action(payload, action):
                continue
            if not self._candidate_matches_search(payload, search):
                continue
            items.append(payload)
        reverse = str(order or "desc").strip().lower() != "asc"
        items.sort(key=lambda payload: self._candidate_sort_value(payload, sort), reverse=reverse)
        return items[: max(1, int(limit))]

    def find_candidate(self, candidate_id: str) -> dict[str, Any] | None:
        candidate = self.get_candidate(candidate_id)
        if candidate is not None:
            return candidate
        normalized = str(candidate_id or "").strip()
        if not normalized:
            return None
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT *
                  FROM candidates
                 WHERE signal_id = ?
                    OR trace_id = ?
                ORDER BY updated_ts DESC, created_ts DESC, id DESC
                 LIMIT 1
                """,
                (normalized, normalized),
            ).fetchone()
        return self._candidate_row_to_dict(row)

    def candidate_detail(self, candidate_id: str, *, related_limit: int = 25) -> dict[str, Any] | None:
        candidate = self.find_candidate(candidate_id)
        if candidate is None:
            return None
        candidate_id_value = str(candidate.get("id") or "")
        trace_id = str(candidate.get("trace_id") or "")
        signal_id = str(candidate.get("signal_id") or "")
        wallet = str(candidate.get("wallet") or "")
        market_slug = str(candidate.get("market_slug") or "")
        token_id = str(candidate.get("token_id") or "")
        related_actions = self.list_candidate_actions(
            limit=related_limit,
            candidate_id=candidate_id_value,
        )
        journal_rows = self.list_journal_entries(limit=max(related_limit * 4, related_limit))
        related_journal = [
            row
            for row in journal_rows
            if candidate_id_value
            and (
                str(row.get("candidate_id") or "") == candidate_id_value
                or (trace_id and str(row.get("trace_id") or "") == trace_id)
                or (signal_id and str(row.get("signal_id") or "") == signal_id)
                or (wallet and str(row.get("wallet") or "").strip().lower() == wallet.strip().lower())
                or (market_slug and str(row.get("market_slug") or "").strip().lower() == market_slug.strip().lower())
            )
        ]
        return {
            "candidate": candidate,
            "candidate_id": candidate_id_value,
            "signal_id": signal_id,
            "trace_id": trace_id,
            "lookup": {
                "wallet": wallet,
                "market_slug": market_slug,
                "token_id": token_id,
            },
            "related_actions": related_actions,
            "related_journal": related_journal[:related_limit],
            "summary": {
                "related_action_count": len(related_actions),
                "related_journal_count": len(related_journal[:related_limit]),
                "action_count": len(related_actions),
                "journal_count": len(related_journal[:related_limit]),
            },
        }

    def _candidate_action_row_to_dict(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        if row is None:
            return None
        payload = self._decode_payload(row["payload_json"])
        payload["id"] = int(row["id"] or 0)
        payload["candidate_id"] = str(row["candidate_id"] or payload.get("candidate_id") or "")
        payload["action"] = str(row["action"] or payload.get("action") or "")
        payload["idempotency_key"] = str(row["idempotency_key"] or payload.get("idempotency_key") or "")
        payload["notional"] = float(row["notional"] or payload.get("notional") or 0.0)
        payload["note"] = str(row["note"] or payload.get("note") or "")
        payload["created_ts"] = int(row["created_ts"] or payload.get("created_ts") or 0)
        payload["status"] = str(payload.get("candidate_status") or payload.get("status") or "")
        return payload

    def _candidate_action_by_idempotency(
        self,
        candidate_id: str,
        idempotency_key: str,
    ) -> dict[str, Any] | None:
        if not idempotency_key:
            return None
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT *
                  FROM candidate_actions
                 WHERE candidate_id = ?
                   AND idempotency_key = ?
                 ORDER BY id DESC
                 LIMIT 1
                """,
                (str(candidate_id or ""), str(idempotency_key or "")),
            ).fetchone()
        return self._candidate_action_row_to_dict(row)

    def update_candidate_status(
        self,
        candidate_id: str,
        *,
        status: str,
        selected_action: str | None = None,
        note: str | None = None,
        result_tag: str | None = None,
        updated_ts: int | None = None,
    ) -> dict[str, Any] | None:
        payload = self.get_candidate(candidate_id)
        if payload is None:
            return None
        now = int(updated_ts or time.time())
        payload["status"] = str(status or payload.get("status") or "pending")
        if selected_action is not None:
            payload["selected_action"] = str(selected_action)
        if note is not None:
            payload["note"] = str(note)
        if result_tag is not None:
            payload["result_tag"] = str(result_tag)
        payload["updated_ts"] = now
        self.upsert_candidate(payload)
        return payload

    def record_candidate_action(
        self,
        candidate_id: str,
        *,
        action: str,
        notional: float = 0.0,
        note: str = "",
        payload: Mapping[str, Any] | None = None,
        created_ts: int | None = None,
        idempotency_key: str | None = None,
    ) -> dict[str, Any] | None:
        candidate = self.get_candidate(candidate_id)
        if candidate is None:
            return None
        action_name = str(action or "").strip().lower()
        if action_name not in _ACTION_STATUS_MAP:
            raise ValueError(f"unsupported candidate action: {action_name}")
        now = int(created_ts or time.time())
        action_payload = dict(payload or {})
        action_payload.setdefault("candidate_status", _ACTION_STATUS_MAP[action_name])
        stable_key = str(idempotency_key or action_payload.get("idempotency_key") or "").strip()
        if not stable_key:
            stable_payload = {
                str(key): value
                for key, value in action_payload.items()
                if str(key) not in {"requested_ts", "created_ts", "updated_ts", "updated_by", "idempotency_key"}
            }
            fingerprint = json.dumps(
                {
                    "action": action_name,
                    "notional": float(notional or 0.0),
                    "note": str(note or ""),
                    "payload": stable_payload,
                },
                ensure_ascii=False,
                sort_keys=True,
            )
            stable_key = hashlib.sha1(
                f"{str(candidate_id)}|{fingerprint}".encode("utf-8")
            ).hexdigest()
        action_payload["idempotency_key"] = stable_key
        existing_action = self._candidate_action_by_idempotency(candidate_id, stable_key)
        if existing_action is not None:
            updated = self.get_candidate(candidate_id)
            if updated is not None:
                updated["_idempotent_replay"] = True
                updated["_last_action"] = existing_action
            return updated
        try:
            with self.connection() as conn:
                conn.execute(
                    """
                    INSERT INTO candidate_actions (candidate_id, action, idempotency_key, notional, note, created_ts, payload_json)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        str(candidate_id),
                        action_name,
                        stable_key,
                        float(notional or 0.0),
                        str(note or ""),
                        now,
                        json.dumps(action_payload, ensure_ascii=False),
                    ),
                )
        except sqlite3.IntegrityError:
            updated = self.get_candidate(candidate_id)
            existing_action = self._candidate_action_by_idempotency(candidate_id, stable_key)
            if updated is not None:
                updated["_idempotent_replay"] = True
                updated["_last_action"] = existing_action or {}
            return updated
        updated = self.update_candidate_status(
            candidate_id,
            status=_ACTION_STATUS_MAP[action_name],
            selected_action=action_name,
            note=note,
            updated_ts=now,
        )
        if updated is not None:
            updated["_idempotent_replay"] = False
            updated["_last_action"] = self._candidate_action_by_idempotency(candidate_id, stable_key) or {}
        return updated

    def list_pending_actions(self, *, limit: int = 50) -> list[dict[str, Any]]:
        return self.list_candidates(statuses=["approved", "watched"], limit=limit)

    def list_candidate_actions(
        self,
        *,
        limit: int = 100,
        candidate_id: str = "",
        action: str = "",
        days: int | None = None,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[object] = []
        normalized_candidate_id = str(candidate_id or "").strip()
        if normalized_candidate_id:
            clauses.append("candidate_id = ?")
            params.append(normalized_candidate_id)
        normalized_action = str(action or "").strip().lower()
        if normalized_action:
            clauses.append("LOWER(action) = ?")
            params.append(normalized_action)
        if days is not None:
            # Windowed export/statistics keep the query surface small for the UI.
            clauses.append("created_ts >= ?")
            params.append(self._window_cutoff_ts(days))
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        sql = f"""
            SELECT *
              FROM candidate_actions
              {where}
             ORDER BY created_ts DESC, id DESC
             LIMIT ?
        """
        params.append(max(1, int(limit)))
        with self.connection() as conn:
            rows = conn.execute(sql, params).fetchall()
        actions: list[dict[str, Any]] = []
        for row in rows:
            payload = self._candidate_action_row_to_dict(row)
            if payload is not None:
                actions.append(payload)
        return actions

    def upsert_wallet_profile(self, profile: object) -> dict[str, Any]:
        payload = self._payload_dict(profile)
        now = int(time.time())
        payload["updated_ts"] = int(payload.get("updated_ts") or now)
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO wallet_profiles (
                    wallet, tag, trust_score, followability_score, avg_hold_minutes,
                    category, enabled, notes, updated_ts, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(wallet) DO UPDATE SET
                    tag=excluded.tag,
                    trust_score=excluded.trust_score,
                    followability_score=excluded.followability_score,
                    avg_hold_minutes=excluded.avg_hold_minutes,
                    category=excluded.category,
                    enabled=excluded.enabled,
                    notes=excluded.notes,
                    updated_ts=excluded.updated_ts,
                    payload_json=excluded.payload_json
                """,
                (
                    str(payload.get("wallet") or ""),
                    str(payload.get("tag") or ""),
                    float(payload.get("trust_score") or 0.0),
                    float(payload.get("followability_score") or 0.0),
                    payload.get("avg_hold_minutes"),
                    str(payload.get("category") or ""),
                    1 if bool(payload.get("enabled", True)) else 0,
                    str(payload.get("notes") or ""),
                    int(payload.get("updated_ts") or now),
                    json.dumps(payload, ensure_ascii=False),
                ),
            )
        return payload

    def list_wallet_profiles(self, *, limit: int = 100) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM wallet_profiles
                ORDER BY enabled DESC, trust_score DESC, followability_score DESC, updated_ts DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        profiles: list[dict[str, Any]] = []
        for row in rows:
            payload = self._decode_payload(row["payload_json"])
            payload.setdefault("wallet", str(row["wallet"] or ""))
            payload["tag"] = str(row["tag"] or payload.get("tag") or "")
            payload["trust_score"] = float(row["trust_score"] or payload.get("trust_score") or 0.0)
            payload["followability_score"] = float(
                row["followability_score"] or payload.get("followability_score") or 0.0
            )
            payload["avg_hold_minutes"] = row["avg_hold_minutes"] if row["avg_hold_minutes"] is not None else payload.get("avg_hold_minutes")
            payload["category"] = str(row["category"] or payload.get("category") or "")
            payload["enabled"] = bool(int(row["enabled"] or 0))
            payload["notes"] = str(row["notes"] or payload.get("notes") or "")
            payload["updated_ts"] = int(row["updated_ts"] or payload.get("updated_ts") or 0)
            profiles.append(payload)
        return profiles

    def append_journal_entry(self, entry: object) -> dict[str, Any]:
        payload = self._payload_dict(entry)
        now = int(time.time())
        payload["created_ts"] = int(payload.get("created_ts") or now)
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO journal_entries (
                    candidate_id, market_slug, wallet, action, rationale, result_tag,
                    pnl_realized, created_ts, payload_json
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(payload.get("candidate_id") or ""),
                    str(payload.get("market_slug") or ""),
                    str(payload.get("wallet") or ""),
                    str(payload.get("action") or ""),
                    str(payload.get("rationale") or ""),
                    payload.get("result_tag"),
                    payload.get("pnl_realized"),
                    int(payload.get("created_ts") or now),
                    json.dumps(payload, ensure_ascii=False),
                ),
            )
        return payload

    def list_journal_entries(self, *, limit: int = 50) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT * FROM journal_entries
                ORDER BY created_ts DESC, id DESC
                LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
        entries: list[dict[str, Any]] = []
        for row in rows:
            payload = self._decode_payload(row["payload_json"])
            payload.setdefault("candidate_id", str(row["candidate_id"] or ""))
            payload["market_slug"] = str(row["market_slug"] or payload.get("market_slug") or "")
            payload["wallet"] = str(row["wallet"] or payload.get("wallet") or "")
            payload["action"] = str(row["action"] or payload.get("action") or "")
            payload["rationale"] = str(row["rationale"] or payload.get("rationale") or "")
            payload["result_tag"] = row["result_tag"] if row["result_tag"] is not None else payload.get("result_tag")
            payload["pnl_realized"] = row["pnl_realized"] if row["pnl_realized"] is not None else payload.get("pnl_realized")
            payload["created_ts"] = int(row["created_ts"] or payload.get("created_ts") or 0)
            entries.append(payload)
        return entries

    def journal_summary(self, *, days: int = 30) -> dict[str, Any]:
        cutoff_ts = int(time.time()) - max(1, int(days)) * 86400
        with self.connection() as conn:
            row = conn.execute(
                """
                SELECT
                    COUNT(*) AS total_entries,
                    SUM(CASE WHEN action IN ('buy_small', 'buy_normal', 'follow', 'close_partial', 'close_all') THEN 1 ELSE 0 END) AS execution_actions,
                    SUM(CASE WHEN action = 'watch' THEN 1 ELSE 0 END) AS watch_actions,
                    SUM(CASE WHEN action = 'ignore' THEN 1 ELSE 0 END) AS ignore_actions
                FROM journal_entries
                WHERE created_ts >= ?
                """,
                (cutoff_ts,),
            ).fetchone()
        return {
            "days": int(days),
            "total_entries": int((row["total_entries"] if row is not None else 0) or 0),
            "execution_actions": int((row["execution_actions"] if row is not None else 0) or 0),
            "watch_actions": int((row["watch_actions"] if row is not None else 0) or 0),
            "ignore_actions": int((row["ignore_actions"] if row is not None else 0) or 0),
            "updated_ts": int(time.time()),
        }

    def decision_stats(self, *, days: int = 30, top_n: int = 5) -> dict[str, Any]:
        cutoff_ts = int(time.time()) - max(1, int(days)) * 86400
        candidates = [row for row in self.list_candidates(limit=1000, include_expired=True) if int(row.get("created_ts") or 0) >= cutoff_ts]
        actions = [row for row in self.list_candidate_actions(limit=1000) if int(row.get("created_ts") or 0) >= cutoff_ts]
        journal = [row for row in self.list_journal_entries(limit=1000) if int(row.get("created_ts") or 0) >= cutoff_ts]

        status_counts: dict[str, int] = {}
        action_counts: dict[str, int] = {}
        wallet_counts: dict[str, int] = {}
        market_counts: dict[str, int] = {}
        trigger_counts: dict[str, int] = {}
        for row in candidates:
            status = str(row.get("status") or "pending")
            status_counts[status] = status_counts.get(status, 0) + 1
            trigger = str(row.get("trigger_type") or "unknown")
            trigger_counts[trigger] = trigger_counts.get(trigger, 0) + 1
            wallet = str(row.get("wallet") or "").strip().lower()
            if wallet:
                wallet_counts[wallet] = wallet_counts.get(wallet, 0) + 1
            market_slug = str(row.get("market_slug") or "").strip().lower()
            if market_slug:
                market_counts[market_slug] = market_counts.get(market_slug, 0) + 1
        for row in actions:
            action = str(row.get("action") or "")
            if action:
                action_counts[action] = action_counts.get(action, 0) + 1

        daily: dict[str, dict[str, Any]] = {}
        for row in candidates:
            created_ts = int(row.get("created_ts") or 0)
            if created_ts <= 0:
                continue
            day_key = time.strftime("%Y-%m-%d", time.localtime(created_ts))
            bucket = daily.setdefault(
                day_key,
                {"day": day_key, "candidates": 0, "approved": 0, "executed": 0, "watched": 0, "journal_notes": 0},
            )
            bucket["candidates"] += 1
            status = str(row.get("status") or "")
            if status == "approved":
                bucket["approved"] += 1
            elif status == "executed":
                bucket["executed"] += 1
            elif status == "watched":
                bucket["watched"] += 1
        for row in journal:
            created_ts = int(row.get("created_ts") or 0)
            if created_ts <= 0:
                continue
            day_key = time.strftime("%Y-%m-%d", time.localtime(created_ts))
            bucket = daily.setdefault(
                day_key,
                {"day": day_key, "candidates": 0, "approved": 0, "executed": 0, "watched": 0, "journal_notes": 0},
            )
            bucket["journal_notes"] += 1

        def _top_rows(source: dict[str, int]) -> list[dict[str, Any]]:
            return [
                {"key": key, "count": count}
                for key, count in sorted(source.items(), key=lambda item: (-item[1], item[0]))[: max(1, int(top_n))]
            ]

        score_values = [float(row.get("score") or 0.0) for row in candidates]
        return {
            "days": int(days),
            "updated_ts": int(time.time()),
            "summary": {
                "candidate_count": len(candidates),
                "action_count": len(actions),
                "journal_count": len(journal),
                "avg_score": round(sum(score_values) / len(score_values), 2) if score_values else 0.0,
                "high_conviction_count": sum(1 for value in score_values if value >= 80.0),
            },
            "status_counts": status_counts,
            "action_counts": action_counts,
            "trigger_counts": trigger_counts,
            "top_wallets": _top_rows(wallet_counts),
            "top_markets": _top_rows(market_counts),
            "daily": [daily[key] for key in sorted(daily.keys(), reverse=True)[: max(1, int(days))]],
        }

    def archive_summary(self, *, days: int = 14) -> dict[str, Any]:
        stats = self.decision_stats(days=days, top_n=10)
        recent_actions = self.list_candidate_actions(limit=200)
        recent_journal = self.list_journal_entries(limit=200)
        cutoff_ts = int(time.time()) - max(1, int(days)) * 86400
        return {
            "days": int(days),
            "updated_ts": int(time.time()),
            "daily": list(stats.get("daily") or []),
            "recent_actions": [row for row in recent_actions if int(row.get("created_ts") or 0) >= cutoff_ts][:25],
            "recent_journal": [row for row in recent_journal if int(row.get("created_ts") or 0) >= cutoff_ts][:25],
        }

    def export_rows(
        self,
        kind: str,
        *,
        limit: int = 500,
        statuses: list[str] | tuple[str, ...] | None = None,
    ) -> list[dict[str, Any]]:
        normalized = str(kind or "").strip().lower()
        if normalized == "candidates":
            return self.list_candidates(limit=limit, statuses=list(statuses or []), include_expired=True)
        if normalized == "candidate_actions":
            return self.list_candidate_actions(limit=limit)
        if normalized == "wallet_profiles":
            return self.list_wallet_profiles(limit=limit)
        if normalized == "journal":
            return self.list_journal_entries(limit=limit)
        raise ValueError(f"unsupported export kind: {kind}")

    def candidate_summary(self, *, days: int = 30) -> dict[str, Any]:
        cutoff_ts = self._window_cutoff_ts(days)
        with self.connection() as conn:
            rows = conn.execute(
                """
                SELECT status, COUNT(*) AS count, COALESCE(SUM(score), 0) AS score_sum
                  FROM candidates
                 WHERE created_ts >= ?
                 GROUP BY status
                """,
                (cutoff_ts,),
            ).fetchall()
        by_status: list[dict[str, Any]] = []
        total_candidates = 0
        total_score = 0.0
        for row in rows:
            status = str(row["status"] or "")
            count = int(row["count"] or 0)
            score_sum = float(row["score_sum"] or 0.0)
            by_status.append({"status": status, "count": count, "score_sum": score_sum})
            total_candidates += count
            total_score += score_sum
        by_status.sort(key=lambda item: (int(item.get("count") or 0), str(item.get("status") or "")), reverse=True)
        return {
            "days": int(days),
            "window_start_ts": cutoff_ts,
            "total_candidates": int(total_candidates),
            "avg_score": 0.0 if total_candidates <= 0 else round(total_score / total_candidates, 2),
            "by_status": by_status,
            "updated_ts": int(time.time()),
        }

    def candidate_action_summary(self, *, days: int = 30) -> dict[str, Any]:
        cutoff_ts = self._window_cutoff_ts(days)
        actions = self.list_candidate_actions(limit=5000, days=days)
        by_action: dict[str, dict[str, Any]] = {}
        total_notional = 0.0
        for row in actions:
            action = str(row.get("action") or "").strip().lower() or "unknown"
            bucket = by_action.setdefault(
                action,
                {
                    "action": action,
                    "count": 0,
                    "notional": 0.0,
                    "latest_ts": 0,
                },
            )
            bucket["count"] = int(bucket.get("count") or 0) + 1
            notional = max(0.0, float(row.get("notional") or 0.0))
            bucket["notional"] = float(bucket.get("notional") or 0.0) + notional
            bucket["latest_ts"] = max(int(bucket.get("latest_ts") or 0), int(row.get("created_ts") or 0))
            total_notional += notional
        rows = sorted(by_action.values(), key=lambda item: (int(item.get("count") or 0), float(item.get("notional") or 0.0), str(item.get("action") or "")), reverse=True)
        return {
            "days": int(days),
            "window_start_ts": cutoff_ts,
            "total_actions": int(len(actions)),
            "total_notional": float(round(total_notional, 4)),
            "by_action": rows,
            "updated_ts": int(time.time()),
        }

    def archive_summary(self, *, days: int = 30, recent_days: int = 7) -> dict[str, Any]:
        cutoff_ts = self._window_cutoff_ts(days)
        recent_cutoff_ts = self._window_cutoff_ts(recent_days)
        candidates = self.list_candidates(limit=5000, include_expired=True)
        actions = self.list_candidate_actions(limit=5000, days=days)
        journal_entries = self.list_journal_entries(limit=5000)

        candidate_by_day: dict[str, dict[str, Any]] = {}
        for row in candidates:
            created_ts = int(row.get("created_ts") or 0)
            if created_ts < cutoff_ts:
                continue
            day_key = self._day_key_from_ts(created_ts)
            if not day_key:
                continue
            bucket = candidate_by_day.setdefault(
                day_key,
                {
                    "day_key": day_key,
                    "candidate_count": 0,
                    "pending_count": 0,
                    "approved_count": 0,
                    "watched_count": 0,
                    "executed_count": 0,
                    "expired_count": 0,
                    "avg_score": 0.0,
                    "score_total": 0.0,
                },
            )
            bucket["candidate_count"] = int(bucket.get("candidate_count") or 0) + 1
            status = str(row.get("status") or "").lower()
            if status == "pending":
                bucket["pending_count"] = int(bucket.get("pending_count") or 0) + 1
            elif status == "approved":
                bucket["approved_count"] = int(bucket.get("approved_count") or 0) + 1
            elif status == "watched":
                bucket["watched_count"] = int(bucket.get("watched_count") or 0) + 1
            elif status == "executed":
                bucket["executed_count"] = int(bucket.get("executed_count") or 0) + 1
            elif status == "expired":
                bucket["expired_count"] = int(bucket.get("expired_count") or 0) + 1
            score = float(row.get("score") or 0.0)
            bucket["score_total"] = float(bucket.get("score_total") or 0.0) + score

        action_by_day: dict[str, dict[str, Any]] = {}
        for row in actions:
            created_ts = int(row.get("created_ts") or 0)
            if created_ts < cutoff_ts:
                continue
            day_key = self._day_key_from_ts(created_ts)
            if not day_key:
                continue
            bucket = action_by_day.setdefault(
                day_key,
                {
                    "day_key": day_key,
                    "action_count": 0,
                    "approved_count": 0,
                    "ignored_count": 0,
                    "watched_count": 0,
                    "notional": 0.0,
                },
            )
            bucket["action_count"] = int(bucket.get("action_count") or 0) + 1
            action_name = str(row.get("action") or "").lower()
            if action_name in {"buy_small", "buy_normal", "follow", "close_partial", "close_all"}:
                bucket["approved_count"] = int(bucket.get("approved_count") or 0) + 1
            elif action_name == "watch":
                bucket["watched_count"] = int(bucket.get("watched_count") or 0) + 1
            elif action_name == "ignore":
                bucket["ignored_count"] = int(bucket.get("ignored_count") or 0) + 1
            bucket["notional"] = float(bucket.get("notional") or 0.0) + max(0.0, float(row.get("notional") or 0.0))

        journal_by_day: dict[str, dict[str, Any]] = {}
        for row in journal_entries:
            created_ts = int(row.get("created_ts") or 0)
            if created_ts < cutoff_ts:
                continue
            day_key = self._day_key_from_ts(created_ts)
            if not day_key:
                continue
            bucket = journal_by_day.setdefault(
                day_key,
                {
                    "day_key": day_key,
                    "journal_count": 0,
                    "execution_actions": 0,
                    "watch_actions": 0,
                    "ignore_actions": 0,
                },
            )
            bucket["journal_count"] = int(bucket.get("journal_count") or 0) + 1
            action_name = str(row.get("action") or "").lower()
            if action_name in {"buy_small", "buy_normal", "follow", "close_partial", "close_all"}:
                bucket["execution_actions"] = int(bucket.get("execution_actions") or 0) + 1
            elif action_name == "watch":
                bucket["watch_actions"] = int(bucket.get("watch_actions") or 0) + 1
            elif action_name == "ignore":
                bucket["ignore_actions"] = int(bucket.get("ignore_actions") or 0) + 1

        daily_keys = sorted(set(candidate_by_day) | set(action_by_day) | set(journal_by_day))
        daily_rows: list[dict[str, Any]] = []
        for day_key in daily_keys:
            candidate_row = candidate_by_day.get(day_key, {})
            action_row = action_by_day.get(day_key, {})
            journal_row = journal_by_day.get(day_key, {})
            candidate_count = int(candidate_row.get("candidate_count") or 0)
            score_total = float(candidate_row.get("score_total") or 0.0)
            daily_rows.append(
                {
                    "day_key": day_key,
                    "candidate_count": candidate_count,
                    "pending_count": int(candidate_row.get("pending_count") or 0),
                    "approved_count": int(candidate_row.get("approved_count") or 0),
                    "watched_count": int(candidate_row.get("watched_count") or 0),
                    "executed_count": int(candidate_row.get("executed_count") or 0),
                    "expired_count": int(candidate_row.get("expired_count") or 0),
                    "candidate_avg_score": 0.0 if candidate_count <= 0 else round(score_total / candidate_count, 2),
                    "action_count": int(action_row.get("action_count") or 0),
                    "action_approved_count": int(action_row.get("approved_count") or 0),
                    "action_ignored_count": int(action_row.get("ignored_count") or 0),
                    "action_watched_count": int(action_row.get("watched_count") or 0),
                    "action_notional": float(round(float(action_row.get("notional") or 0.0), 4)),
                    "journal_count": int(journal_row.get("journal_count") or 0),
                    "journal_execution_actions": int(journal_row.get("execution_actions") or 0),
                    "journal_watch_actions": int(journal_row.get("watch_actions") or 0),
                    "journal_ignore_actions": int(journal_row.get("ignore_actions") or 0),
                }
            )

        total_candidate_count = sum(int(row.get("candidate_count") or 0) for row in daily_rows)
        total_action_count = sum(int(row.get("action_count") or 0) for row in daily_rows)
        total_journal_count = sum(int(row.get("journal_count") or 0) for row in daily_rows)
        recent_day_key = self._day_key_from_ts(recent_cutoff_ts)
        recent_rows = [row for row in daily_rows if row.get("day_key") and str(row.get("day_key")) >= recent_day_key]
        recent_summary = {
            "days": int(recent_days),
            "window_start_ts": recent_cutoff_ts,
            "day_count": len(recent_rows),
            "candidate_count": sum(int(row.get("candidate_count") or 0) for row in recent_rows),
            "action_count": sum(int(row.get("action_count") or 0) for row in recent_rows),
            "journal_count": sum(int(row.get("journal_count") or 0) for row in recent_rows),
            "updated_ts": int(time.time()),
        }
        return {
            "days": int(days),
            "recent_days": int(recent_days),
            "window_start_ts": cutoff_ts,
            "recent_window_start_ts": recent_cutoff_ts,
            "day_count": len(daily_rows),
            "summary": {
                "candidate_count": int(total_candidate_count),
                "action_count": int(total_action_count),
                "journal_count": int(total_journal_count),
                "days": int(days),
            },
            "daily_rows": sorted(daily_rows, key=lambda row: str(row.get("day_key") or "")),
            "recent_summary": recent_summary,
            "updated_ts": int(time.time()),
        }

    def stats_summary(self, *, days: int = 30, recent_days: int = 7) -> dict[str, Any]:
        candidate_summary = self.candidate_summary(days=days)
        action_summary = self.candidate_action_summary(days=days)
        journal_summary = self.journal_summary(days=days)
        archive_summary = self.archive_summary(days=days, recent_days=recent_days)
        wallet_profiles = self.list_wallet_profiles(limit=500)
        candidate_count = candidate_summary.get("total_candidates", 0)
        return {
            "days": int(days),
            "recent_days": int(recent_days),
            "updated_ts": int(time.time()),
            "candidates": candidate_summary,
            "candidate_actions": action_summary,
            "journal": journal_summary,
            "archive": archive_summary,
            "wallet_profiles": {
                "count": len(wallet_profiles),
                "enabled": sum(1 for row in wallet_profiles if bool(row.get("enabled", True))),
                "watched": sum(1 for row in wallet_profiles if bool(row.get("watch", False))),
            },
            "totals": {
                "candidate_count": int(candidate_count),
                "action_count": int(action_summary.get("total_actions", 0) or 0),
                "journal_count": int(journal_summary.get("total_entries", 0) or 0),
            },
        }
