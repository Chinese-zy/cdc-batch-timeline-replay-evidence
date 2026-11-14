from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from .storage import connect, ensure_schema

LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class ApplyResult:
    batch_id: str
    applied: int
    skipped_duplicates: int
    skipped_stale: int
    rejected: int
    recovered: bool
    metrics: Mapping[str, int]


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _manifest_hash(manifest: Mapping[str, Any]) -> str:
    payload = json.dumps(manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _validate_event(event: Mapping[str, Any]) -> None:
    required = {"event_id", "customer_id", "version", "event_ts", "op"}
    missing = sorted(required - set(event))
    if missing:
        raise ValueError(f"missing event fields: {', '.join(missing)}")
    if event["op"] not in {"upsert", "delete"}:
        raise ValueError(f"unsupported operation: {event['op']}")
    if not isinstance(event["version"], int) or event["version"] < 1:
        raise ValueError("version must be a positive integer")
    datetime.fromisoformat(str(event["event_ts"]).replace("Z", "+00:00"))


def _event_payload(event: Mapping[str, Any]) -> dict[str, Any]:
    reserved = {"event_id", "customer_id", "version", "event_ts", "op"}
    payload = dict(event.get("payload") or {})
    extra = {k: v for k, v in event.items() if k not in reserved and k != "payload"}
    payload.update(extra)
    return payload


def _close_current(conn: sqlite3.Connection, customer_id: str, valid_to: str) -> None:
    conn.execute(
        "UPDATE dimension_customer SET valid_to = ?, is_current = 0 "
        "WHERE customer_id = ? AND is_current = 1",
        (valid_to, customer_id),
    )


def _insert_version(
    conn: sqlite3.Connection,
    event: Mapping[str, Any],
    payload: Mapping[str, Any],
    valid_from: str,
    is_current: int,
) -> None:
    conn.execute(
        """
        INSERT INTO dimension_customer(
          customer_id, valid_from, valid_to, is_current,
          name, email, tier, attributes_json, source_event_id, source_event_version
        ) VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(event["customer_id"]),
            valid_from,
            is_current,
            payload.get("name"),
            payload.get("email"),
            payload.get("tier"),
            json.dumps(dict(payload), ensure_ascii=False, sort_keys=True),
            str(event["event_id"]),
            int(event["version"]),
        ),
    )


def apply_batch(
    db_path: str | Path,
    manifest: Mapping[str, Any],
    events: Iterable[Mapping[str, Any]],
) -> ApplyResult:
    """Apply one manifest-described CDC batch.

    The TODOs intentionally leave important replay and temporal semantics unfinished.
    """
    raise NotImplementedError("implement transactional CDC/SCD2 replay")


def recover_manifest(db_path: str | Path, manifest: Mapping[str, Any]) -> bool:
    """Recover a manifest left in an in-progress state after a crash."""
    raise NotImplementedError("implement manifest recovery")

