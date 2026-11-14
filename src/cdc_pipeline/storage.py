from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any, Iterable


CURRENT_SCHEMA = 2


def connect(path: str | Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn


def _column_names(conn: sqlite3.Connection, table: str) -> set[str]:
    return {row["name"] for row in conn.execute(f"PRAGMA table_info({table})")}


def _migrate(conn: sqlite3.Connection) -> None:
    """Add audit columns introduced after the initial schema.

    SQLite cannot add a NOT NULL column without a default; the defaults keep
    rows written by older builds readable.
    """
    events_cols = _column_names(conn, "applied_events")
    if "status" not in events_cols:
        conn.execute("ALTER TABLE applied_events ADD COLUMN status TEXT NOT NULL DEFAULT 'applied'")
    if "payload_hash" not in events_cols:
        conn.execute("ALTER TABLE applied_events ADD COLUMN payload_hash TEXT NOT NULL DEFAULT ''")

    batches_cols = _column_names(conn, "applied_batches")
    if "schema_version" not in batches_cols:
        conn.execute(
            "ALTER TABLE applied_batches ADD COLUMN schema_version INTEGER NOT NULL DEFAULT 1"
        )
    if "recovered_at" not in batches_cols:
        conn.execute("ALTER TABLE applied_batches ADD COLUMN recovered_at TEXT")


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS dimension_customer (
            customer_id TEXT NOT NULL,
            valid_from TEXT NOT NULL,
            valid_to TEXT,
            is_current INTEGER NOT NULL CHECK (is_current IN (0, 1)),
            name TEXT,
            email TEXT,
            tier TEXT,
            attributes_json TEXT NOT NULL,
            source_event_id TEXT NOT NULL,
            source_event_version INTEGER NOT NULL,
            PRIMARY KEY (customer_id, valid_from)
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uq_customer_current
          ON dimension_customer(customer_id) WHERE is_current = 1;
        CREATE TABLE IF NOT EXISTS applied_events (
            event_id TEXT PRIMARY KEY,
            customer_id TEXT NOT NULL,
            event_version INTEGER NOT NULL,
            event_ts TEXT NOT NULL,
            applied_at TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'applied',
            payload_hash TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS applied_batches (
            batch_id TEXT PRIMARY KEY,
            manifest_hash TEXT NOT NULL,
            applied_at TEXT NOT NULL,
            event_count INTEGER NOT NULL,
            status TEXT NOT NULL,
            schema_version INTEGER NOT NULL DEFAULT 1,
            recovered_at TEXT
        );
        CREATE TABLE IF NOT EXISTS pipeline_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )
    _migrate(conn)
    conn.execute(
        "INSERT OR IGNORE INTO pipeline_meta(key, value) VALUES ('schema_version', ?)",
        (str(CURRENT_SCHEMA),),
    )
    conn.commit()


def read_jsonl(path: str | Path) -> Iterable[dict[str, Any]]:
    with Path(path).open(encoding="utf-8") as fh:
        for number, line in enumerate(fh, 1):
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON on line {number}") from exc
            if not isinstance(value, dict):
                raise ValueError(f"line {number} must contain a JSON object")
            yield value
