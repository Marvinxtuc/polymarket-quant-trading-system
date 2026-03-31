from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable

from polymarket_bot.kill_switch import normalize_state as normalize_kill_switch_state
from polymarket_bot.idempotency import (
    CLAIMED_NEW,
    EXISTING_NON_TERMINAL,
    EXISTING_TERMINAL,
    STORAGE_ERROR,
    normalize_status,
    is_terminal,
)
from polymarket_bot.models import ExposureLedgerEntry, PersistedOrderIntent, RiskBreakerState
from polymarket_bot.models.risk_breaker_state import default_risk_breaker_state


def _connect(db_path: str, *, read_only: bool = False) -> sqlite3.Connection:
    path = Path(db_path).expanduser()
    if bool(read_only):
        if not path.exists():
            raise FileNotFoundError(f"state store does not exist: {path}")
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0, isolation_level=None)
        conn.execute("PRAGMA query_only=ON;")
        conn.execute("PRAGMA busy_timeout=5000;")
        return conn
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path), timeout=5.0, isolation_level=None)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA busy_timeout=5000;")
    return conn


class StateStore:
    def __init__(
        self,
        db_path: str,
        *,
        migration_path: str | None = None,
        writer_assertion: Callable[[], None] | None = None,
        read_only: bool = False,
        ensure_schema: bool = True,
    ) -> None:
        self.db_path = db_path
        self.migration_path = migration_path or str(
            Path(__file__).resolve().parents[2] / "migrations" / "001_init_runtime_tables.sql"
        )
        self._writer_assertion = writer_assertion
        self._read_only = bool(read_only)
        if bool(ensure_schema) and not self._read_only:
            self._init_schema()

    def _with_conn(self, fn: Callable[[sqlite3.Connection], Any]) -> Any:
        conn = _connect(self.db_path, read_only=self._read_only)
        try:
            return fn(conn)
        finally:
            conn.close()

    def _assert_writer_active(self) -> None:
        if self._read_only:
            raise RuntimeError("state store is read-only")
        checker = self._writer_assertion
        if callable(checker):
            checker()

    def _with_write_conn(self, fn: Callable[[sqlite3.Connection], Any]) -> Any:
        self._assert_writer_active()
        return self._with_conn(fn)

    @staticmethod
    def _decode_payload(text: object) -> dict[str, object] | None:
        if not isinstance(text, str) or not text.strip():
            return None
        try:
            payload = json.loads(text)
        except Exception:
            return None
        return payload if isinstance(payload, dict) else None

    @staticmethod
    def _encode_payload(payload: dict[str, object]) -> str:
        return json.dumps(payload, ensure_ascii=False)

    def _init_schema(self) -> None:
        migration_root = Path(str(self.migration_path or "")).expanduser()
        migration_paths: list[Path] = []

        if migration_root.is_dir():
            migration_paths = sorted(
                (path for path in migration_root.glob("[0-9][0-9][0-9]_*.sql")),
                key=lambda p: p.name,
            )
        else:
            parent = migration_root.parent
            if parent.exists():
                migration_paths = sorted(
                    (path for path in parent.glob("[0-9][0-9][0-9]_*.sql")),
                    key=lambda p: p.name,
                )
            if not migration_paths:
                migration_paths.append(migration_root)

        if not migration_paths:
            raise FileNotFoundError(f"missing runtime migration file: {migration_root}")

        def _create(conn: sqlite3.Connection) -> None:
            for path in migration_paths:
                sql_script = path.read_text(encoding="utf-8")
                try:
                    conn.executescript(sql_script)
                except sqlite3.OperationalError as exc:
                    message = str(exc).lower()
                    if "duplicate column name" in message:
                        continue
                    raise

        self._with_write_conn(_create)

    def save_runtime_state(self, payload: dict[str, object]) -> None:
        encoded = self._encode_payload(payload)
        now_ts = int(time.time())

        def _save(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO runtime_state (id, payload, updated_ts)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_ts=excluded.updated_ts
                """,
                (encoded, now_ts),
            )

        self._with_write_conn(_save)

    def load_runtime_state(self) -> dict[str, object] | None:
        def _load(conn: sqlite3.Connection) -> dict[str, object] | None:
            row = conn.execute("SELECT payload FROM runtime_state WHERE id=1").fetchone()
            if not row:
                return None
            return self._decode_payload(row[0])

        return self._with_conn(_load)

    def save_kill_switch_state(self, payload: dict[str, object]) -> None:
        normalized_payload = normalize_kill_switch_state(payload)
        now_ts = int(time.time())

        def _save(conn: sqlite3.Connection) -> None:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT payload FROM runtime_state WHERE id=1").fetchone()
            runtime_payload = self._decode_payload(row[0]) if row else {}
            if runtime_payload is None:
                runtime_payload = {}
            runtime_payload["kill_switch"] = dict(normalized_payload)
            runtime_payload["ts"] = now_ts
            runtime_payload["runtime_version"] = max(
                int(runtime_payload.get("runtime_version") or 0),
                8,
            )
            conn.execute(
                """
                INSERT INTO runtime_state (id, payload, updated_ts)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_ts=excluded.updated_ts
                """,
                (self._encode_payload(runtime_payload), now_ts),
            )
            conn.execute("COMMIT")

        self._with_write_conn(_save)

    def load_kill_switch_state(self) -> dict[str, object] | None:
        runtime_payload = self.load_runtime_state() or {}
        raw = runtime_payload.get("kill_switch")
        if not isinstance(raw, dict):
            return None
        return normalize_kill_switch_state(raw)

    def save_risk_state(self, payload: dict[str, object]) -> None:
        encoded = self._encode_payload(payload)
        now_ts = int(time.time())

        def _save(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO risk_state (id, payload, updated_ts)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_ts=excluded.updated_ts
                """,
                (encoded, now_ts),
            )

        self._with_write_conn(_save)

    def load_risk_state(self) -> dict[str, object] | None:
        def _load(conn: sqlite3.Connection) -> dict[str, object] | None:
            row = conn.execute("SELECT payload FROM risk_state WHERE id=1").fetchone()
            if not row:
                return None
            return self._decode_payload(row[0])

        return self._with_conn(_load)

    def replace_exposure_ledger(self, entries: list[dict[str, object]]) -> None:
        now_ts = int(time.time())

        def _replace(conn: sqlite3.Connection) -> None:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM exposure_ledger")
            for row in entries:
                payload = ExposureLedgerEntry.from_payload(dict(row or {})).to_payload()
                scope_type = str(payload.get("scope_type") or "").strip().lower()
                scope_key = str(payload.get("scope_key") or "").strip()
                if not scope_type or not scope_key:
                    continue
                payload["updated_ts"] = int(now_ts)
                conn.execute(
                    """
                    INSERT INTO exposure_ledger (scope_type, scope_key, payload, updated_ts)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(scope_type, scope_key) DO UPDATE
                    SET payload=excluded.payload, updated_ts=excluded.updated_ts
                    """,
                    (
                        scope_type,
                        scope_key,
                        self._encode_payload(payload),
                        now_ts,
                    ),
                )
            conn.execute("COMMIT")

        self._with_write_conn(_replace)

    def load_exposure_ledger(self) -> list[dict[str, object]]:
        def _load(conn: sqlite3.Connection) -> list[dict[str, object]]:
            rows = conn.execute(
                """
                SELECT payload FROM exposure_ledger
                ORDER BY scope_type ASC, scope_key ASC
                """
            ).fetchall()
            out: list[dict[str, object]] = []
            for row in rows:
                payload = self._decode_payload(row[0])
                if payload is None:
                    continue
                out.append(ExposureLedgerEntry.from_payload(payload).to_payload())
            return out

        return self._with_conn(_load)

    def save_risk_breaker_state(self, payload: dict[str, object]) -> None:
        normalized = RiskBreakerState.from_payload(payload).to_payload()
        now_ts = int(time.time())
        normalized["updated_ts"] = now_ts

        def _save(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO risk_breaker_state (id, payload, updated_ts)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_ts=excluded.updated_ts
                """,
                (self._encode_payload(normalized), now_ts),
            )

        self._with_write_conn(_save)

    def load_risk_breaker_state(self) -> dict[str, object]:
        def _load(conn: sqlite3.Connection) -> dict[str, object]:
            row = conn.execute("SELECT payload FROM risk_breaker_state WHERE id=1").fetchone()
            if not row:
                return default_risk_breaker_state()
            payload = self._decode_payload(row[0]) or default_risk_breaker_state()
            return RiskBreakerState.from_payload(payload).to_payload()

        return self._with_conn(_load)

    def save_reconciliation_state(self, payload: dict[str, object]) -> None:
        encoded = self._encode_payload(payload)
        now_ts = int(time.time())

        def _save(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO reconciliation_state (id, payload, updated_ts)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_ts=excluded.updated_ts
                """,
                (encoded, now_ts),
            )

        self._with_write_conn(_save)

    def load_reconciliation_state(self) -> dict[str, object] | None:
        def _load(conn: sqlite3.Connection) -> dict[str, object] | None:
            row = conn.execute("SELECT payload FROM reconciliation_state WHERE id=1").fetchone()
            if not row:
                return None
            return self._decode_payload(row[0])

        return self._with_conn(_load)

    def replace_positions(self, positions: list[dict[str, object]]) -> None:
        now_ts = int(time.time())

        def _replace(conn: sqlite3.Connection) -> None:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM positions_snapshot")
            for row in positions:
                token_id = str(row.get("token_id") or "").strip()
                if not token_id:
                    continue
                conn.execute(
                    """
                    INSERT INTO positions_snapshot (token_id, payload, updated_ts)
                    VALUES (?, ?, ?)
                    """,
                    (
                        token_id,
                        self._encode_payload(dict(row)),
                        now_ts,
                    ),
                )
            conn.execute("COMMIT")

        self._with_write_conn(_replace)

    def load_positions(self) -> list[dict[str, object]]:
        def _load(conn: sqlite3.Connection) -> list[dict[str, object]]:
            rows = conn.execute(
                "SELECT payload FROM positions_snapshot ORDER BY token_id ASC"
            ).fetchall()
            positions: list[dict[str, object]] = []
            for row in rows:
                payload = self._decode_payload(row[0])
                if payload is not None:
                    positions.append(payload)
            return positions

        return self._with_conn(_load)

    def _order_intents_base_select(self) -> str:
        return """
            SELECT
                intent_id,
                idempotency_key,
                strategy_name,
                signal_source,
                signal_fingerprint,
                strategy_order_uuid,
                broker_order_id,
                token_id,
                condition_id,
                side,
                status,
                recovered_source,
                recovery_reason,
                payload,
                created_ts,
                updated_ts
            FROM order_intents
        """

    def replace_order_intents(self, intents: list[dict[str, object]]) -> None:
        now_ts = int(time.time())

        def _replace(conn: sqlite3.Connection) -> None:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("DELETE FROM order_intents")
            for row in intents:
                intent_id = str(row.get("intent_id") or "").strip()
                idempotency_key = str(row.get("idempotency_key") or "").strip()
                strategy_name = str(row.get("strategy_name") or "").strip()
                signal_source = str(row.get("signal_source") or "").strip()
                signal_fingerprint = str(row.get("signal_fingerprint") or "").strip()
                token_id = str(row.get("token_id") or "").strip()
                side = str(row.get("side") or "").strip().upper()
                status = normalize_status(row.get("status")) or "new"
                created_ts = int(row.get("created_ts") or now_ts)
                updated_ts = int(row.get("updated_ts") or now_ts)
                if not intent_id or not token_id or side not in {"BUY", "SELL"}:
                    continue
                conn.execute(
                    """
                    INSERT INTO order_intents (
                        intent_id,
                        idempotency_key,
                        strategy_name,
                        signal_source,
                        signal_fingerprint,
                        strategy_order_uuid,
                        broker_order_id,
                        token_id,
                        condition_id,
                        side,
                        status,
                        recovered_source,
                        recovery_reason,
                        payload,
                        created_ts,
                        updated_ts
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        intent_id,
                        idempotency_key,
                        strategy_name,
                        signal_source,
                        signal_fingerprint,
                        str(row.get("strategy_order_uuid") or ""),
                        str(row.get("broker_order_id") or ""),
                        token_id,
                        str(row.get("condition_id") or ""),
                        side,
                        status,
                        str(row.get("recovered_source") or ""),
                        str(row.get("recovery_reason") or ""),
                        self._encode_payload(dict(row.get("payload") or {})),
                        created_ts,
                        updated_ts,
                    ),
                )
            conn.execute("COMMIT")

        self._with_write_conn(_replace)

    def load_order_intents(self) -> list[dict[str, object]]:
        def _load(conn: sqlite3.Connection) -> list[dict[str, object]]:
            rows = conn.execute(
                f"""{self._order_intents_base_select()}
                ORDER BY created_ts ASC, intent_id ASC
                """
            ).fetchall()
            intents: list[dict[str, object]] = []
            for row in rows:
                payload = self._decode_payload(row[13]) or {}
                intents.append(
                    {
                        "intent_id": str(row[0] or ""),
                        "idempotency_key": str(row[1] or ""),
                        "strategy_name": str(row[2] or ""),
                        "signal_source": str(row[3] or ""),
                        "signal_fingerprint": str(row[4] or ""),
                        "strategy_order_uuid": str(row[5] or ""),
                        "broker_order_id": str(row[6] or ""),
                        "token_id": str(row[7] or ""),
                        "condition_id": str(row[8] or ""),
                        "side": str(row[9] or "").upper(),
                        "status": normalize_status(row[10]),
                        "recovered_source": str(row[11] or ""),
                        "recovery_reason": str(row[12] or ""),
                        "payload": payload,
                        "created_ts": int(row[14] or 0),
                        "updated_ts": int(row[15] or 0),
                    }
                )
            return intents

        return self._with_conn(_load)

    def load_pending_order_intents(self) -> list[dict[str, object]]:
        rows = self.load_order_intents()
        return [row for row in rows if not is_terminal(row.get("status"))]

    def _intent_from_row(self, row: tuple) -> PersistedOrderIntent:
        payload = self._decode_payload(row[13]) or {}
        return PersistedOrderIntent(
            intent_id=str(row[0] or ""),
            idempotency_key=str(row[1] or ""),
            strategy_name=str(row[2] or ""),
            signal_source=str(row[3] or ""),
            signal_fingerprint=str(row[4] or ""),
            token_id=str(row[7] or ""),
            side=str(row[9] or "").upper(),
            status=normalize_status(row[10]),
            created_ts=int(row[14] or 0),
            updated_ts=int(row[15] or 0),
            payload=payload,
            strategy_order_uuid=str(row[5] or ""),
            broker_order_id=str(row[6] or ""),
            condition_id=str(row[8] or ""),
            recovered_source=str(row[11] or ""),
            recovery_reason=str(row[12] or ""),
        )

    def load_intent_by_idempotency_key(self, idempotency_key: str) -> PersistedOrderIntent | None:
        key = str(idempotency_key or "").strip()
        if not key:
            return None

        def _load(conn: sqlite3.Connection) -> PersistedOrderIntent | None:
            row = conn.execute(
                f"""{self._order_intents_base_select()}
                WHERE idempotency_key=?
                LIMIT 1
                """,
                (key,),
            ).fetchone()
            if not row:
                return None
            return self._intent_from_row(row)

        return self._with_conn(_load)

    def update_intent_status(
        self,
        *,
        status: str,
        idempotency_key: str = "",
        strategy_order_uuid: str = "",
        broker_order_id: str | None = None,
        payload_updates: dict[str, object] | None = None,
        recovery_reason: str | None = None,
        expected_from_statuses: list[str] | tuple[str, ...] | None = None,
    ) -> tuple[bool, PersistedOrderIntent | None]:
        normalized_status = normalize_status(status)
        if not normalized_status:
            return (False, None)
        lookup_key = str(idempotency_key or "").strip()
        lookup_uuid = str(strategy_order_uuid or "").strip()
        if not lookup_key and not lookup_uuid:
            return (False, None)

        expected = {normalize_status(value) for value in list(expected_from_statuses or []) if normalize_status(value)}

        def _update(conn: sqlite3.Connection) -> tuple[bool, PersistedOrderIntent | None]:
            try:
                conn.execute("BEGIN IMMEDIATE")
                where_sql = "idempotency_key=?" if lookup_key else "strategy_order_uuid=?"
                where_value = lookup_key if lookup_key else lookup_uuid
                row = conn.execute(
                    f"""{self._order_intents_base_select()}
                    WHERE {where_sql}
                    LIMIT 1
                    """,
                    (where_value,),
                ).fetchone()
                if not row:
                    conn.execute("COMMIT")
                    return (False, None)
                current = self._intent_from_row(row)
                current_status = normalize_status(current.status)
                if expected and current_status not in expected:
                    conn.execute("COMMIT")
                    return (False, current)

                next_payload = dict(current.payload or {})
                if payload_updates:
                    next_payload.update(dict(payload_updates))
                next_broker_order_id = current.broker_order_id
                if broker_order_id is not None:
                    next_broker_order_id = str(broker_order_id or "")
                next_recovery_reason = current.recovery_reason
                if recovery_reason is not None:
                    next_recovery_reason = str(recovery_reason or "")
                now_ts = int(time.time())
                conn.execute(
                    f"""
                    UPDATE order_intents
                    SET
                        status=?,
                        broker_order_id=?,
                        recovery_reason=?,
                        payload=?,
                        updated_ts=?
                    WHERE {where_sql}
                    """,
                    (
                        normalized_status,
                        next_broker_order_id,
                        next_recovery_reason,
                        self._encode_payload(next_payload),
                        now_ts,
                        where_value,
                    ),
                )
                updated = conn.execute(
                    f"""{self._order_intents_base_select()}
                    WHERE {where_sql}
                    LIMIT 1
                    """,
                    (where_value,),
                ).fetchone()
                conn.execute("COMMIT")
                if not updated:
                    return (False, None)
                return (True, self._intent_from_row(updated))
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                return (False, None)

        return self._with_write_conn(_update)

    def save_runtime_truth(self, payload: dict[str, object]) -> None:
        runtime_payload = dict(payload.get("runtime") or {})
        control_payload = dict(payload.get("control") or {})
        risk_payload = dict(payload.get("risk") or {})
        reconciliation_payload = dict(payload.get("reconciliation") or {})
        risk_breaker_payload = RiskBreakerState.from_payload(payload.get("risk_breakers")).to_payload()
        exposure_ledger = [
            ExposureLedgerEntry.from_payload(row).to_payload()
            for row in list(payload.get("exposure_ledger") or [])
            if isinstance(row, dict)
        ]
        positions = [dict(row) for row in list(payload.get("positions") or []) if isinstance(row, dict)]
        order_intents = [dict(row) for row in list(payload.get("order_intents") or []) if isinstance(row, dict)]
        now_ts = int(time.time())

        def _save(conn: sqlite3.Connection) -> None:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                """
                INSERT INTO runtime_state (id, payload, updated_ts)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_ts=excluded.updated_ts
                """,
                (self._encode_payload(runtime_payload), now_ts),
            )
            conn.execute(
                """
                INSERT INTO control_state (id, payload, updated_ts)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_ts=excluded.updated_ts
                """,
                (self._encode_payload(control_payload), now_ts),
            )
            conn.execute(
                """
                INSERT INTO risk_state (id, payload, updated_ts)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_ts=excluded.updated_ts
                """,
                (self._encode_payload(risk_payload), now_ts),
            )
            conn.execute(
                """
                INSERT INTO reconciliation_state (id, payload, updated_ts)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_ts=excluded.updated_ts
                """,
                (self._encode_payload(reconciliation_payload), now_ts),
            )
            conn.execute(
                """
                INSERT INTO risk_breaker_state (id, payload, updated_ts)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_ts=excluded.updated_ts
                """,
                (self._encode_payload(risk_breaker_payload), now_ts),
            )
            conn.execute("DELETE FROM exposure_ledger")
            for row in exposure_ledger:
                scope_type = str(row.get("scope_type") or "").strip().lower()
                scope_key = str(row.get("scope_key") or "").strip()
                if not scope_type or not scope_key:
                    continue
                row["updated_ts"] = now_ts
                conn.execute(
                    """
                    INSERT INTO exposure_ledger (scope_type, scope_key, payload, updated_ts)
                    VALUES (?, ?, ?, ?)
                    ON CONFLICT(scope_type, scope_key) DO UPDATE
                    SET payload=excluded.payload, updated_ts=excluded.updated_ts
                    """,
                    (
                        scope_type,
                        scope_key,
                        self._encode_payload(row),
                        now_ts,
                    ),
                )
            conn.execute("DELETE FROM positions_snapshot")
            for row in positions:
                token_id = str(row.get("token_id") or "").strip()
                if not token_id:
                    continue
                conn.execute(
                    """
                    INSERT INTO positions_snapshot (token_id, payload, updated_ts)
                    VALUES (?, ?, ?)
                    """,
                    (token_id, self._encode_payload(row), now_ts),
                )
            conn.execute("DELETE FROM order_intents")
            for row in order_intents:
                intent_id = str(row.get("intent_id") or "").strip()
                idempotency_key = str(row.get("idempotency_key") or "").strip()
                strategy_name = str(row.get("strategy_name") or "").strip()
                signal_source = str(row.get("signal_source") or "").strip()
                signal_fingerprint = str(row.get("signal_fingerprint") or "").strip()
                token_id = str(row.get("token_id") or "").strip()
                side = str(row.get("side") or "").strip().upper()
                status = normalize_status(row.get("status")) or "new"
                if not intent_id or not token_id or side not in {"BUY", "SELL"}:
                    continue
                conn.execute(
                    """
                    INSERT INTO order_intents (
                        intent_id,
                        idempotency_key,
                        strategy_name,
                        signal_source,
                        signal_fingerprint,
                        strategy_order_uuid,
                        broker_order_id,
                        token_id,
                        condition_id,
                        side,
                        status,
                        recovered_source,
                        recovery_reason,
                        payload,
                        created_ts,
                        updated_ts
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        intent_id,
                        idempotency_key,
                        strategy_name,
                        signal_source,
                        signal_fingerprint,
                        str(row.get("strategy_order_uuid") or ""),
                        str(row.get("broker_order_id") or ""),
                        token_id,
                        str(row.get("condition_id") or ""),
                        side,
                        status,
                        str(row.get("recovered_source") or ""),
                        str(row.get("recovery_reason") or ""),
                        self._encode_payload(dict(row.get("payload") or {})),
                        int(row.get("created_ts") or now_ts),
                        int(row.get("updated_ts") or now_ts),
                    ),
                )
            conn.execute("COMMIT")

        self._with_write_conn(_save)

    def load_runtime_truth(self) -> dict[str, object]:
        return {
            "runtime": dict(self.load_runtime_state() or {}),
            "control": dict(self.load_control_state() or {}),
            "risk": dict(self.load_risk_state() or {}),
            "reconciliation": dict(self.load_reconciliation_state() or {}),
            "risk_breakers": dict(self.load_risk_breaker_state() or {}),
            "exposure_ledger": list(self.load_exposure_ledger()),
            "positions": list(self.load_positions()),
            "order_intents": list(self.load_order_intents()),
        }

    def claim_or_load_intent(
        self,
        *,
        idempotency_key: str,
        intent_id: str,
        token_id: str,
        side: str,
        status: str,
        payload: dict[str, object],
        strategy_name: str,
        signal_source: str,
        signal_fingerprint: str,
        strategy_order_uuid: str = "",
        broker_order_id: str = "",
        condition_id: str = "",
        created_ts: int | None = None,
        recovered_source: str = "",
        recovery_reason: str = "",
    ) -> tuple[str, PersistedOrderIntent | None]:
        now_ts = int(created_ts or time.time())
        normalized_status = normalize_status(status) or "new"
        normalized_side = str(side or "").strip().upper()
        key = str(idempotency_key or "").strip()

        if not key or not intent_id or not token_id or normalized_side not in {"BUY", "SELL"}:
            return (STORAGE_ERROR, None)

        insert_sql = """
            INSERT INTO order_intents (
                intent_id,
                idempotency_key,
                strategy_name,
                signal_source,
                signal_fingerprint,
                strategy_order_uuid,
                broker_order_id,
                token_id,
                condition_id,
                side,
                status,
                recovered_source,
                recovery_reason,
                payload,
                created_ts,
                updated_ts
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """

        def _claim(conn: sqlite3.Connection) -> tuple[str, PersistedOrderIntent | None]:
            try:
                conn.execute("BEGIN IMMEDIATE")
                row = conn.execute(
                    f"""{self._order_intents_base_select()}
                    WHERE idempotency_key=?
                    LIMIT 1
                    """,
                    (key,),
                ).fetchone()
                if row:
                    intent = self._intent_from_row(row)
                    conn.execute("COMMIT")
                    return (
                        EXISTING_TERMINAL if intent.is_terminal else EXISTING_NON_TERMINAL,
                        intent,
                    )

                encoded_payload = self._encode_payload(dict(payload or {}))
                conn.execute(
                    insert_sql,
                    (
                        intent_id,
                        key,
                        str(strategy_name or ""),
                        str(signal_source or ""),
                        str(signal_fingerprint or ""),
                        str(strategy_order_uuid or ""),
                        str(broker_order_id or ""),
                        token_id,
                        str(condition_id or ""),
                        normalized_side,
                        normalized_status,
                        str(recovered_source or ""),
                        str(recovery_reason or ""),
                        encoded_payload,
                        now_ts,
                        now_ts,
                    ),
                )
                conn.execute("COMMIT")
                return (
                    CLAIMED_NEW,
                    PersistedOrderIntent(
                        intent_id=intent_id,
                        idempotency_key=key,
                        strategy_name=str(strategy_name or ""),
                        signal_source=str(signal_source or ""),
                        signal_fingerprint=str(signal_fingerprint or ""),
                        strategy_order_uuid=str(strategy_order_uuid or ""),
                        broker_order_id=str(broker_order_id or ""),
                        token_id=token_id,
                        condition_id=str(condition_id or ""),
                        side=normalized_side,
                        status=normalized_status,
                        payload=dict(payload or {}),
                        created_ts=now_ts,
                        updated_ts=now_ts,
                        recovered_source=str(recovered_source or ""),
                        recovery_reason=str(recovery_reason or ""),
                    ),
                )
            except sqlite3.IntegrityError:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                try:
                    row = conn.execute(
                        f"""{self._order_intents_base_select()}
                        WHERE idempotency_key=?
                        LIMIT 1
                        """,
                        (key,),
                    ).fetchone()
                except Exception:
                    return (STORAGE_ERROR, None)
                if not row:
                    return (STORAGE_ERROR, None)
                intent = self._intent_from_row(row)
                return (
                    EXISTING_TERMINAL if intent.is_terminal else EXISTING_NON_TERMINAL,
                    intent,
                )
            except Exception:
                try:
                    conn.execute("ROLLBACK")
                except Exception:
                    pass
                return (STORAGE_ERROR, None)

        return self._with_write_conn(_claim)

    def register_idempotency(
        self,
        *,
        strategy_order_uuid: str,
        wallet: str,
        condition_id: str,
        token_id: str,
        side: str,
        notional: float,
        created_ts: int | None = None,
    ) -> bool:
        created = int(created_ts or time.time())

        def _insert(conn: sqlite3.Connection) -> bool:
            try:
                conn.execute(
                    """
                    INSERT INTO idempotency_keys (strategy_order_uuid, wallet, condition_id, token_id, side, notional, created_ts)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        strategy_order_uuid,
                        wallet,
                        condition_id,
                        token_id,
                        side,
                        notional,
                        created,
                    ),
                )
                return True
            except sqlite3.IntegrityError:
                return False

        return self._with_write_conn(_insert)

    def idempotency_exists(self, strategy_order_uuid: str) -> bool:
        def _exists(conn: sqlite3.Connection) -> bool:
            row = conn.execute(
                "SELECT 1 FROM idempotency_keys WHERE strategy_order_uuid=?",
                (strategy_order_uuid,),
            ).fetchone()
            return bool(row)

        return self._with_conn(_exists)

    def cleanup_idempotency(self, *, window_seconds: int) -> int:
        cutoff = int(time.time()) - max(0, int(window_seconds))

        def _cleanup(conn: sqlite3.Connection) -> int:
            cur = conn.execute(
                "DELETE FROM idempotency_keys WHERE created_ts < ?",
                (cutoff,),
            )
            return cur.rowcount if cur else 0

        return self._with_write_conn(_cleanup)

    def save_control_state(self, payload: dict[str, object]) -> None:
        encoded = self._encode_payload(payload)
        now_ts = int(time.time())

        def _save(conn: sqlite3.Connection) -> None:
            conn.execute(
                """
                INSERT INTO control_state (id, payload, updated_ts)
                VALUES (1, ?, ?)
                ON CONFLICT(id) DO UPDATE SET payload=excluded.payload, updated_ts=excluded.updated_ts
                """,
                (encoded, now_ts),
            )

        self._with_write_conn(_save)

    def load_control_state(self) -> dict[str, object] | None:
        def _load(conn: sqlite3.Connection) -> dict[str, object] | None:
            row = conn.execute("SELECT payload FROM control_state WHERE id=1").fetchone()
            if not row:
                return None
            payload = self._decode_payload(row[0])
            if payload is None:
                raise ValueError("malformed control_state payload")
            return payload

        return self._with_conn(_load)
