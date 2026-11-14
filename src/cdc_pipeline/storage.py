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
            applied_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS applied_batches (
            batch_id TEXT PRIMARY KEY,
            manifest_hash TEXT NOT NULL,
            applied_at TEXT NOT NULL,
            event_count INTEGER NOT NULL,
            status TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pipeline_meta (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """
    )
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

