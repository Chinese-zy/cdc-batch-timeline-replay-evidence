from __future__ import annotations

import json
import sqlite3
import tempfile
import unittest
from pathlib import Path

from cdc_pipeline.engine import apply_batch, recover_manifest
from cdc_pipeline.storage import connect, ensure_schema


def event(event_id, customer_id, version, ts, payload=None, op="upsert"):
    return {
        "event_id": event_id,
        "customer_id": customer_id,
        "version": version,
        "event_ts": ts,
        "op": op,
        "payload": payload or {},
    }


class EngineTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "state.db"

    def tearDown(self):
        self.tmp.cleanup()

    def manifest(self, batch_id="b-1", events=None):
        return {
            "batch_id": batch_id,
            "source": "fixtures/events.jsonl",
            "event_ids": [e["event_id"] for e in (events or [])],
            "schema_version": 1,
        }

    def rows(self):
        with connect(self.db) as conn:
            return conn.execute(
                "SELECT customer_id, valid_from, valid_to, is_current, name, email, tier, attributes_json, source_event_version "
                "FROM dimension_customer ORDER BY customer_id, valid_from"
            ).fetchall()

    def test_first_upsert_is_current_and_keeps_unknown_attributes(self):
        events = [event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A", "email": "a@x", "loyalty": {"points": 3}})]
        result = apply_batch(self.db, self.manifest(events=events), events)
        self.assertEqual((result.applied, result.rejected), (1, 0))
        row = self.rows()[0]
        self.assertEqual((row[0], row[3], row[4], row[5]), ("c1", 1, "A", "a@x"))
        self.assertEqual(json.loads(row[7])["loyalty"], {"points": 3})

    def test_replay_is_idempotent_and_manifest_mismatch_is_rejected(self):
        events = [event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A"})]
        manifest = self.manifest(events=events)
        first = apply_batch(self.db, manifest, events)
        second = apply_batch(self.db, manifest, events)
        self.assertEqual(first.applied, 1)
        self.assertEqual(second.skipped_duplicates, 1)
        bad = dict(manifest, event_ids=["other"])
        with self.assertRaises(ValueError):
            apply_batch(self.db, bad, events)

    def test_late_event_rewrites_the_correct_interval_without_two_currents(self):
        first = event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A", "tier": "bronze"})
        second = event("e2", "c1", 2, "2026-01-03T00:00:00+00:00", {"name": "B", "tier": "silver"})
        late = event("e-late", "c1", 3, "2026-01-02T00:00:00+00:00", {"email": "b@x"})
        apply_batch(self.db, self.manifest("b1", [first]), [first])
        apply_batch(self.db, self.manifest("b2", [second]), [second])
        result = apply_batch(self.db, self.manifest("b3", [late]), [late])
        self.assertEqual(result.applied, 1)
        rows = self.rows()
        self.assertEqual(sum(r[3] for r in rows), 1)
        self.assertEqual([r[1] for r in rows], ["2026-01-01T00:00:00+00:00", "2026-01-02T00:00:00+00:00", "2026-01-03T00:00:00+00:00"])
        self.assertEqual(rows[0][4:7], ("A", None, "bronze"))
        self.assertEqual(rows[1][4:7], ("A", "b@x", "bronze"))
        self.assertEqual(rows[2][4:7], ("B", "b@x", "silver"))

    def test_stale_version_is_skipped_and_delete_closes_current(self):
        fresh = event("e2", "c1", 2, "2026-01-03T00:00:00+00:00", {"name": "B"})
        stale = event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A"})
        delete = event("e3", "c1", 3, "2026-01-04T00:00:00+00:00", op="delete")
        apply_batch(self.db, self.manifest("b1", [fresh]), [fresh])
        stale_result = apply_batch(self.db, self.manifest("b2", [stale]), [stale])
        delete_result = apply_batch(self.db, self.manifest("b3", [delete]), [delete])
        self.assertEqual(stale_result.skipped_stale, 1)
        self.assertEqual(delete_result.applied, 1)
        self.assertEqual(sum(r[3] for r in self.rows()), 0)

    def test_recovery_marks_abandoned_batch_replayable(self):
        manifest = self.manifest("crashed", [])
        with connect(self.db) as conn:
            ensure_schema(conn)
            conn.execute(
                "INSERT INTO applied_batches(batch_id, manifest_hash, applied_at, event_count, status) VALUES (?, ?, ?, ?, ?)",
                ("crashed", "wrong", "2026-01-01T00:00:00+00:00", 2, "in_progress"),
            )
            conn.commit()
        self.assertTrue(recover_manifest(self.db, manifest))
        with connect(self.db) as conn:
            status = conn.execute("SELECT status FROM applied_batches WHERE batch_id='crashed'").fetchone()[0]
        self.assertEqual(status, "recovered")


if __name__ == "__main__":
    unittest.main()

