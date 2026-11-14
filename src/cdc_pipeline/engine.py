from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Counter, Iterable, Mapping

from .storage import CURRENT_SCHEMA, connect, ensure_schema

LOGGER = logging.getLogger(__name__)

STATUS_IN_PROGRESS = "in_progress"
STATUS_COMPLETE = "complete"
STATUS_RECOVERED = "recovered"

EVENT_APPLIED = "applied"
EVENT_STALE = "stale"
EVENT_REJECTED = "rejected"

_KNOWN_FIELDS = ("name", "email", "tier")
_RESERVED_EVENT_KEYS = {"event_id", "customer_id", "version", "event_ts", "op", "payload"}


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


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _manifest_hash(manifest: Mapping[str, Any]) -> str:
    """Deterministic SHA-256 over a canonicalized manifest.

    Key order and whitespace never affect the hash.
    """
    return hashlib.sha256(_canonical_json(manifest).encode("utf-8")).hexdigest()


def _normalize_ts(raw: Any) -> tuple[datetime, str]:
    """Parse an ISO-8601 timestamp and return (aware datetime, canonical text)."""
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError("event_ts must be a non-empty ISO-8601 string")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"unparseable event_ts: {raw!r}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed, parsed.isoformat()


def _validate_manifest(manifest: Mapping[str, Any]) -> tuple[Any, list[str]]:
    if not isinstance(manifest, Mapping):
        raise ValueError("manifest must be a JSON object")
    missing = sorted({"batch_id", "event_ids", "schema_version"} - set(manifest))
    if missing:
        raise ValueError(f"missing manifest fields: {', '.join(missing)}")
    batch_id = manifest["batch_id"]
    if not isinstance(batch_id, str) or not batch_id.strip():
        raise ValueError("manifest batch_id must be a non-empty string")
    event_ids = manifest["event_ids"]
    if not isinstance(event_ids, list) or not all(
        isinstance(eid, str) and eid.strip() for eid in event_ids
    ):
        raise ValueError("manifest event_ids must be a list of non-empty strings")
    if len(set(event_ids)) != len(event_ids):
        raise ValueError("manifest event_ids must not contain duplicates")
    schema_version = manifest["schema_version"]
    if isinstance(schema_version, bool) or not isinstance(schema_version, int):
        raise ValueError("manifest schema_version must be an integer")
    if schema_version < 1 or schema_version > CURRENT_SCHEMA:
        raise ValueError(
            f"unsupported schema_version {schema_version}; pipeline supports up to {CURRENT_SCHEMA}"
        )
    return batch_id, event_ids


def _validate_event(event: Mapping[str, Any]) -> None:
    if not isinstance(event, Mapping):
        raise ValueError("each event must be a JSON object")
    required = {"event_id", "customer_id", "version", "event_ts", "op"}
    missing = sorted(required - set(event))
    if missing:
        raise ValueError(f"missing event fields: {', '.join(missing)}")
    if not isinstance(event["event_id"], str) or not event["event_id"].strip():
        raise ValueError("event_id must be a non-empty string")
    if not isinstance(event["customer_id"], str) or not event["customer_id"].strip():
        raise ValueError("customer_id must be a non-empty string")
    if isinstance(event["version"], bool) or not isinstance(event["version"], int):
        raise ValueError("version must be a positive integer")
    if event["version"] < 1:
        raise ValueError("version must be a positive integer")
    if event["op"] not in {"upsert", "delete"}:
        raise ValueError(f"unsupported operation: {event['op']!r}")
    _normalize_ts(event["event_ts"])
    payload = event.get("payload")
    if payload is not None and not isinstance(payload, Mapping):
        raise ValueError("event payload must be an object")


def _event_payload(event: Mapping[str, Any]) -> dict[str, Any]:
    """All data fields of an event: payload dict merged with top-level extras."""
    payload: dict[str, Any] = dict(event.get("payload") or {})
    extras = {k: v for k, v in event.items() if k not in _RESERVED_EVENT_KEYS}
    payload.update(extras)
    return payload


def _payload_fingerprint(event: Mapping[str, Any]) -> str:
    body = {"op": event["op"], "payload": _event_payload(event)}
    return hashlib.sha256(_canonical_json(body).encode("utf-8")).hexdigest()


def _load_timeline(conn: sqlite3.Connection, customer_id: str) -> list[sqlite3.Row]:
    rows = conn.execute(
        "SELECT rowid AS row_id, customer_id, valid_from, valid_to, is_current, "
        "name, email, tier, attributes_json, source_event_id, source_event_version "
        "FROM dimension_customer WHERE customer_id = ? ORDER BY valid_from",
        (customer_id,),
    ).fetchall()
    return rows


def _max_version(conn: sqlite3.Connection, customer_id: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(event_version), 0) AS v FROM applied_events WHERE customer_id = ?",
        (customer_id,),
    ).fetchone()
    return int(row["v"])


def _record_event(
    conn: sqlite3.Connection,
    event: Mapping[str, Any],
    status: str,
    payload_hash: str,
    applied_at: str,
) -> None:
    conn.execute(
        "INSERT INTO applied_events("
        "event_id, customer_id, event_version, event_ts, applied_at, status, payload_hash"
        ") VALUES (?, ?, ?, ?, ?, ?, ?)",
        (
            str(event["event_id"]),
            str(event["customer_id"]),
            int(event["version"]),
            _normalize_ts(event["event_ts"])[1],
            applied_at,
            status,
            payload_hash,
        ),
    )


def _write_version(
    conn: sqlite3.Connection,
    customer_id: str,
    valid_from: str,
    valid_to: str | None,
    is_current: int,
    merged: Mapping[str, Any],
    event: Mapping[str, Any],
) -> None:
    conn.execute(
        """
        INSERT INTO dimension_customer(
          customer_id, valid_from, valid_to, is_current,
          name, email, tier, attributes_json, source_event_id, source_event_version
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            customer_id,
            valid_from,
            valid_to,
            is_current,
            merged.get("name"),
            merged.get("email"),
            merged.get("tier"),
            _canonical_json(dict(merged)),
            str(event["event_id"]),
            int(event["version"]),
        ),
    )


def _sync_row(conn: sqlite3.Connection, row: sqlite3.Row, merged: Mapping[str, Any]) -> None:
    conn.execute(
        "UPDATE dimension_customer SET name = ?, email = ?, tier = ?, attributes_json = ? "
        "WHERE rowid = ?",
        (
            merged.get("name"),
            merged.get("email"),
            merged.get("tier"),
            _canonical_json(dict(merged)),
            row["row_id"],
        ),
    )


def _apply_upsert(
    conn: sqlite3.Connection,
    event: Mapping[str, Any],
    payload: Mapping[str, Any],
    ts_text: str,
) -> None:
    """Insert an upsert into the SCD2 timeline at its event_ts position."""
    customer_id = str(event["customer_id"])
    timeline = _load_timeline(conn, customer_id)

    same_ts = next((r for r in timeline if r["valid_from"] == ts_text), None)
    if same_ts is not None:
        raise ValueError(
            f"event {event['event_id']!r}: another version already starts at {ts_text} "
            f"for customer {customer_id!r}"
        )

    prev_row = next((r for r in reversed(timeline) if r["valid_from"] < ts_text), None)
    next_row = next((r for r in timeline if r["valid_from"] > ts_text), None)

    # A true gap: the previous row ended before this event (typically a prior
    # delete). The new event re-creates the customer with no inherited state.
    in_gap = (
        prev_row is not None
        and prev_row["valid_to"] is not None
        and ts_text > prev_row["valid_to"]
    )

    base: dict[str, Any] = {}
    if prev_row is not None and not in_gap:
        base = json.loads(prev_row["attributes_json"])
    merged = {**base, **dict(payload)}

    # Boundaries: stay inside the interval in which this event belongs.
    candidate_valid_to = next_row["valid_from"] if next_row is not None else None
    if not in_gap and prev_row is not None and prev_row["valid_to"] is not None:
        if candidate_valid_to is None or prev_row["valid_to"] < candidate_valid_to:
            candidate_valid_to = prev_row["valid_to"]
    is_current = 1 if candidate_valid_to is None else 0

    if prev_row is not None and not in_gap:
        conn.execute(
            "UPDATE dimension_customer SET valid_to = ?, is_current = 0 WHERE rowid = ?",
            (ts_text, prev_row["row_id"]),
        )
    # When next_row exists the new row is always closed at next_row.valid_from,
    # so next_row keeps its is_current flag (including the unique current row).

    _write_version(
        conn, customer_id, ts_text, candidate_valid_to, is_current, merged, event
    )

    # A true deletion gap: the previous row was closed at a time where no
    # successor row starts. Contiguous boundaries (a row starts exactly at
    # prev.valid_to) are not gaps. Propagation must never cross a gap.
    gap = None
    if prev_row is not None and prev_row["valid_to"] is not None:
        if not any(r["valid_from"] == prev_row["valid_to"] for r in timeline):
            gap = prev_row["valid_to"]

    # Propagate fields carried by a late event to every later version inside the
    # same live interval; each later row's own fields still win.
    cursor = next_row
    while cursor is not None:
        if gap is not None and cursor["valid_from"] >= gap:
            break
        row_merged = {**merged, **json.loads(cursor["attributes_json"])}
        _sync_row(conn, cursor, row_merged)
        merged = row_merged
        cursor = next(
            (r for r in timeline if r["valid_from"] > cursor["valid_from"]),
            None,
        )


def _apply_delete(
    conn: sqlite3.Connection,
    event: Mapping[str, Any],
    ts_text: str,
) -> None:
    """Close the timeline at event_ts; a delete is itself a hard boundary."""
    customer_id = str(event["customer_id"])
    timeline = _load_timeline(conn, customer_id)

    same_ts = next((r for r in timeline if r["valid_from"] == ts_text), None)
    if same_ts is not None:
        raise ValueError(
            f"delete {event['event_id']!r}: a version already starts at {ts_text} "
            f"for customer {customer_id!r}"
        )

    prev_row = next((r for r in reversed(timeline) if r["valid_from"] < ts_text), None)
    if prev_row is None:
        # Delete before any known state: nothing existed to delete.
        return

    if prev_row["valid_to"] is None:
        conn.execute(
            "UPDATE dimension_customer SET valid_to = ?, is_current = 0 WHERE rowid = ?",
            (ts_text, prev_row["row_id"]),
        )
    elif prev_row["valid_to"] > ts_text:
        # The row was nominally bounded by a later version that the late delete
        # now removes; pull its boundary back to the delete time.
        conn.execute(
            "UPDATE dimension_customer SET valid_to = ? WHERE rowid = ?",
            (ts_text, prev_row["row_id"]),
        )
    # Versions starting after the delete are orphaned by it and removed.
    conn.execute(
        "DELETE FROM dimension_customer WHERE customer_id = ? AND valid_from > ?",
        (customer_id, ts_text),
    )


def apply_batch(
    db_path: str | Path,
    manifest: Mapping[str, Any],
    events: Iterable[Mapping[str, Any]],
) -> ApplyResult:
    """Apply one manifest-described CDC batch transactionally.

    Any validation, parsing or write failure rolls the whole batch back: no
    dimension rows, event ledgers or half-written batch markers survive.
    """
    batch_id, manifest_event_ids = _validate_manifest(manifest)
    manifest_hash = _manifest_hash(manifest)

    # Materialize first: a broken iterator must never touch the database.
    materialized = list(events)
    for event in materialized:
        _validate_event(event)

    seen: set[str] = set()
    for event in materialized:
        eid = str(event["event_id"])
        if eid in seen:
            raise ValueError(f"duplicate event_id within batch: {eid}")
        seen.add(eid)

    actual_counts: Counter[str] = Counter(str(e["event_id"]) for e in materialized)
    if actual_counts != Counter(manifest_event_ids):
        raise ValueError(
            "batch event_ids do not match manifest exactly: "
            f"expected {manifest_event_ids}"
        )

    applied_at = _utc_now()
    counters = {
        "applied": 0,
        "skipped_duplicates": 0,
        "skipped_stale": 0,
        "rejected": 0,
        "upserts": 0,
        "deletes": 0,
        "conflicts": 0,
    }

    conn = connect(db_path)
    created_marker = False
    try:
        ensure_schema(conn)
        conn.execute("BEGIN IMMEDIATE")

        existing = conn.execute(
            "SELECT status, manifest_hash, event_count, schema_version "
            "FROM applied_batches WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()

        recovered_replay = False
        if existing is not None:
            if existing["manifest_hash"] != manifest_hash:
                raise ValueError(
                    f"batch_id {batch_id!r} already recorded with a different manifest"
                )
            if existing["status"] == STATUS_COMPLETE:
                # Idempotent replay of the exact same manifest.
                counters["skipped_duplicates"] = len(materialized)
                conn.rollback()
                LOGGER.info(
                    "batch %s already complete; replayed idempotently "
                    "(events=%d, skipped_duplicates=%d)",
                    batch_id,
                    len(materialized),
                    counters["skipped_duplicates"],
                )
                return _result(batch_id, counters, recovered=False)
            if existing["status"] == STATUS_RECOVERED:
                recovered_replay = True
            # in_progress / recovered: replay proceeds under the same marker.
        else:
            conn.execute(
                "INSERT INTO applied_batches("
                "batch_id, manifest_hash, applied_at, event_count, status, "
                "schema_version, recovered_at"
                ") VALUES (?, ?, ?, ?, ?, ?, NULL)",
                (
                    batch_id,
                    manifest_hash,
                    applied_at,
                    len(materialized),
                    STATUS_IN_PROGRESS,
                    int(manifest["schema_version"]),
                ),
            )
            created_marker = True

        for event in materialized:
            event_id = str(event["event_id"])
            customer_id = str(event["customer_id"])
            version = int(event["version"])
            fingerprint = _payload_fingerprint(event)

            ledger = conn.execute(
                "SELECT status, payload_hash FROM applied_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()
            if ledger is not None:
                counters["skipped_duplicates"] += 1
                continue

            if version <= _max_version(conn, customer_id):
                conflict = conn.execute(
                    "SELECT 1 FROM applied_events "
                    "WHERE customer_id = ? AND event_version = ? AND payload_hash != ?",
                    (customer_id, version, fingerprint),
                ).fetchone()
                if conflict is not None:
                    # Same version, different content: never silently overwrite.
                    counters["rejected"] += 1
                    counters["conflicts"] += 1
                    _record_event(conn, event, EVENT_REJECTED, fingerprint, applied_at)
                    LOGGER.warning(
                        "rejected conflicting event event_id=%s customer_id=%s version=%d "
                        "batch_id=%s",
                        event_id,
                        customer_id,
                        version,
                        batch_id,
                    )
                    continue
                counters["skipped_stale"] += 1
                _record_event(conn, event, EVENT_STALE, fingerprint, applied_at)
                continue

            _, ts_text = _normalize_ts(event["event_ts"])
            if event["op"] == "upsert":
                _apply_upsert(conn, event, _event_payload(event), ts_text)
                counters["upserts"] += 1
            else:
                _apply_delete(conn, event, ts_text)
                counters["deletes"] += 1

            counters["applied"] += 1
            _record_event(conn, event, EVENT_APPLIED, fingerprint, applied_at)

        conn.execute(
            "UPDATE applied_batches SET status = ?, event_count = ? WHERE batch_id = ?",
            (STATUS_COMPLETE, len(materialized), batch_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        if created_marker:
            # Marker created by this call and the transaction failed: remove it
            # so the batch can be retried cleanly. A marker left by a real crash
            # stays in_progress for recover_manifest().
            try:
                conn.execute("DELETE FROM applied_batches WHERE batch_id = ?", (batch_id,))
                conn.commit()
            except sqlite3.Error:
                pass
        LOGGER.exception(
            "batch %s failed and was rolled back (rejected=%d)",
            batch_id,
            counters["rejected"],
        )
        raise
    finally:
        conn.close()

    LOGGER.info(
        "batch %s complete applied=%d skipped_duplicates=%d skipped_stale=%d rejected=%d",
        batch_id,
        counters["applied"],
        counters["skipped_duplicates"],
        counters["skipped_stale"],
        counters["rejected"],
    )
    return _result(batch_id, counters, recovered=recovered_replay)


def _result(batch_id: str, counters: Mapping[str, int], recovered: bool) -> ApplyResult:
    metrics = {
        "total_events": (
            counters["applied"]
            + counters["skipped_duplicates"]
            + counters["skipped_stale"]
            + counters["rejected"]
        ),
        "upserts": counters["upserts"],
        "deletes": counters["deletes"],
        "skipped_duplicates": counters["skipped_duplicates"],
        "skipped_stale": counters["skipped_stale"],
        "rejected": counters["rejected"],
        "conflicts": counters["conflicts"],
    }
    return ApplyResult(
        batch_id=batch_id,
        applied=counters["applied"],
        skipped_duplicates=counters["skipped_duplicates"],
        skipped_stale=counters["skipped_stale"],
        rejected=counters["rejected"],
        recovered=recovered,
        metrics=metrics,
    )


def recover_manifest(db_path: str | Path, manifest: Mapping[str, Any]) -> bool:
    """Mark an in_progress batch as recovered so it can be replayed.

    Completed batches are never touched. Returns True when a marker was
    recovered, False when there was nothing to recover.
    """
    batch_id, _ = _validate_manifest(manifest)
    manifest_hash = _manifest_hash(manifest)
    conn = connect(db_path)
    try:
        ensure_schema(conn)
        row = conn.execute(
            "SELECT status FROM applied_batches WHERE batch_id = ?",
            (batch_id,),
        ).fetchone()
        if row is None or row["status"] != STATUS_IN_PROGRESS:
            LOGGER.info(
                "recovery skipped for batch %s (state=%s)",
                batch_id,
                row["status"] if row is not None else "absent",
            )
            return False
        conn.execute(
            "UPDATE applied_batches "
            "SET status = ?, manifest_hash = ?, schema_version = ?, recovered_at = ? "
            "WHERE batch_id = ? AND status = ?",
            (
                STATUS_RECOVERED,
                manifest_hash,
                int(manifest["schema_version"]),
                _utc_now(),
                batch_id,
                STATUS_IN_PROGRESS,
            ),
        )
        conn.commit()
        LOGGER.info("batch %s recovered and is replayable", batch_id)
        return True
    finally:
        conn.close()
