from __future__ import annotations

import io
import json
import logging
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from cdc_pipeline.engine import (
    STATUS_COMPLETE,
    STATUS_IN_PROGRESS,
    STATUS_RECOVERED,
    _manifest_hash,
    apply_batch,
    recover_manifest,
)
from cdc_pipeline.storage import CURRENT_SCHEMA, connect, ensure_schema

SRC = Path(__file__).resolve().parents[1] / "src"


def event(event_id, customer_id, version, ts, payload=None, op="upsert"):
    return {
        "event_id": event_id,
        "customer_id": customer_id,
        "version": version,
        "event_ts": ts,
        "op": op,
        "payload": payload or {},
    }


def manifest(batch_id, events, schema_version=1):
    return {
        "batch_id": batch_id,
        "source": "fixtures/events.jsonl",
        "event_ids": [e["event_id"] for e in events],
        "schema_version": schema_version,
    }


class CDCReplayTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "state.db"

    def tearDown(self):
        self.tmp.cleanup()

    # -- helpers ---------------------------------------------------------

    def rows(self, customer_id="c1"):
        with connect(self.db) as conn:
            return conn.execute(
                "SELECT customer_id, valid_from, valid_to, is_current, name, email, "
                "tier, attributes_json, source_event_id, source_event_version "
                "FROM dimension_customer WHERE customer_id = ? ORDER BY valid_from",
                (customer_id,),
            ).fetchall()

    def ledger(self, event_id):
        with connect(self.db) as conn:
            return conn.execute(
                "SELECT status FROM applied_events WHERE event_id = ?",
                (event_id,),
            ).fetchone()

    def batch_row(self, batch_id):
        with connect(self.db) as conn:
            return conn.execute(
                "SELECT status, manifest_hash, event_count, schema_version "
                "FROM applied_batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()

    def assert_no_overlaps(self, customer_id="c1"):
        rows = self.rows(customer_id)
        for left, right in zip(rows, rows[1:]):
            if left["valid_to"] is not None:
                self.assertLessEqual(left["valid_to"], right["valid_from"])
        currents = [r for r in rows if r["is_current"] == 1]
        self.assertLessEqual(len(currents), 1)

    # -- 1. deterministic hashing & manifest validation ------------------

    def test_manifest_hash_is_order_independent_and_stable(self):
        m1 = {"batch_id": "b", "event_ids": ["e1", "e2"], "schema_version": 1}
        m2 = {"schema_version": 1, "event_ids": ["e1", "e2"], "batch_id": "b"}
        m3 = {"batch_id": "b", "event_ids": ["e1", "e2"], "schema_version": 2}
        self.assertEqual(_manifest_hash(m1), _manifest_hash(m2))
        self.assertEqual(len(_manifest_hash(m1)), 64)
        self.assertNotEqual(_manifest_hash(m1), _manifest_hash(m3))

    def test_manifest_with_duplicate_event_ids_is_rejected(self):
        m = {"batch_id": "b", "event_ids": ["e1", "e1"], "schema_version": 1}
        with self.assertRaises(ValueError):
            apply_batch(self.db, m, [
                event("e1", "c1", 1, "2026-01-01T00:00:00+00:00"),
                event("e1", "c1", 2, "2026-01-02T00:00:00+00:00"),
            ])

    def test_event_set_must_match_manifest_exactly(self):
        with connect(self.db) as conn:
            ensure_schema(conn)
        events = [event("e1", "c1", 1, "2026-01-01T00:00:00+00:00")]
        m = manifest("b", events)
        m["event_ids"] = ["e1", "e2"]
        with self.assertRaises(ValueError):
            apply_batch(self.db, m, events)
        # A failed pre-write validation leaves nothing behind.
        with connect(self.db) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) AS n FROM applied_batches").fetchone()["n"], 0
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) AS n FROM dimension_customer").fetchone()["n"], 0
            )

    def test_unsupported_schema_version_rejected(self):
        events = [event("e1", "c1", 1, "2026-01-01T00:00:00+00:00")]
        m = manifest("b", events, schema_version=CURRENT_SCHEMA + 1)
        with self.assertRaises(ValueError):
            apply_batch(self.db, m, events)

    # -- 2. transaction rollback -----------------------------------------

    def test_failed_batch_rolls_back_everything_including_marker(self):
        with connect(self.db) as conn:
            ensure_schema(conn)
        good = event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A"})
        bad = event("e2", "c1", 2, "not-a-timestamp")
        events = [good, bad]
        with self.assertRaises(ValueError):
            apply_batch(self.db, manifest("b1", events), iter(events))
        with connect(self.db) as conn:
            self.assertEqual(
                conn.execute("SELECT COUNT(*) AS n FROM dimension_customer").fetchone()["n"], 0
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) AS n FROM applied_events").fetchone()["n"], 0
            )
            self.assertEqual(
                conn.execute("SELECT COUNT(*) AS n FROM applied_batches").fetchone()["n"], 0
            )

    def test_timestamp_collision_rolls_back_whole_batch(self):
        first = [event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A"})]
        apply_batch(self.db, manifest("b1", first), first)
        colliding = [
            event("e2", "c1", 2, "2026-01-02T00:00:00+00:00", {"name": "B"}),
            event("e3", "c1", 3, "2026-01-01T00:00:00+00:00", {"name": "C"}),
        ]
        with self.assertRaises(ValueError):
            apply_batch(self.db, manifest("b2", colliding), colliding)
        rows = self.rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["source_event_id"], "e1")
        with connect(self.db) as conn:
            self.assertEqual(
                conn.execute(
                    "SELECT COUNT(*) AS n FROM applied_events WHERE event_id IN ('e2','e3')"
                ).fetchone()["n"],
                0,
            )

    # -- 3. idempotency & manifest hash mismatch -------------------------

    def test_replay_returns_equivalent_counts_and_keeps_complete_status(self):
        events = [event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A"})]
        m = manifest("b1", events)
        first = apply_batch(self.db, m, events)
        second = apply_batch(self.db, m, events)
        self.assertEqual(first.applied, 1)
        self.assertEqual(second.skipped_duplicates, 1)
        self.assertEqual(second.applied, 0)
        self.assertEqual(second.metrics["total_events"], 1)
        self.assertEqual(self.batch_row("b1")["status"], STATUS_COMPLETE)

    def test_same_batch_id_different_manifest_is_rejected(self):
        events = [event("e1", "c1", 1, "2026-01-01T00:00:00+00:00")]
        m = manifest("b1", events)
        apply_batch(self.db, m, events)
        altered = dict(m, source="somewhere-else")
        self.assertNotEqual(_manifest_hash(m), _manifest_hash(altered))
        with self.assertRaises(ValueError):
            apply_batch(self.db, altered, events)
        self.assertEqual(self.batch_row("b1")["status"], STATUS_COMPLETE)

    # -- 4. out-of-order / late SCD2 inserts -----------------------------

    def test_late_event_splits_interval_and_keeps_single_current(self):
        e1 = event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A", "tier": "bronze"})
        e2 = event("e2", "c1", 2, "2026-01-03T00:00:00+00:00", {"name": "B", "tier": "silver"})
        late = event("e3", "c1", 3, "2026-01-02T00:00:00+00:00", {"email": "b@x"})
        apply_batch(self.db, manifest("b1", [e1]), [e1])
        apply_batch(self.db, manifest("b2", [e2]), [e2])
        result = apply_batch(self.db, manifest("b3", [late]), [late])
        self.assertEqual(result.applied, 1)
        rows = self.rows()
        self.assertEqual(
            [r["valid_from"] for r in rows],
            ["2026-01-01T00:00:00+00:00", "2026-01-02T00:00:00+00:00", "2026-01-03T00:00:00+00:00"],
        )
        self.assertEqual(rows[0]["valid_to"], "2026-01-02T00:00:00+00:00")
        self.assertEqual(rows[1]["valid_to"], "2026-01-03T00:00:00+00:00")
        self.assertIsNone(rows[2]["valid_to"])
        self.assertEqual(sum(r["is_current"] for r in rows), 1)
        self.assertEqual(rows[2]["is_current"], 1)
        self.assert_no_overlaps()

    def test_late_upsert_before_all_existing_versions(self):
        e2 = event("e2", "c1", 2, "2026-01-03T00:00:00+00:00", {"name": "B"})
        early = event("e3", "c1", 3, "2026-01-01T00:00:00+00:00", {"name": "A"})
        apply_batch(self.db, manifest("b1", [e2]), [e2])
        apply_batch(self.db, manifest("b2", [early]), [early])
        rows = self.rows()
        self.assertEqual([r["valid_from"] for r in rows][0], "2026-01-01T00:00:00+00:00")
        self.assertIsNone(rows[1]["valid_to"])
        self.assertEqual(rows[1]["is_current"], 1)
        self.assertEqual(rows[0]["is_current"], 0)
        self.assert_no_overlaps()

    # -- 5. field merging & schema evolution -----------------------------

    def test_upsert_merges_fields_and_keeps_unknown_attributes(self):
        e1 = event(
            "e1", "c1", 1, "2026-01-01T00:00:00+00:00",
            {"name": "A", "email": "a@x", "loyalty": {"points": 3}, "region": "cn"},
        )
        e2 = event("e2", "c1", 2, "2026-01-02T00:00:00+00:00", {"tier": "gold"})
        apply_batch(self.db, manifest("b1", [e1]), [e1])
        apply_batch(self.db, manifest("b2", [e2]), [e2])
        current = self.rows()[-1]
        attrs = json.loads(current["attributes_json"])
        self.assertEqual(current["name"], "A")
        self.assertEqual(current["email"], "a@x")
        self.assertEqual(current["tier"], "gold")
        self.assertEqual(attrs["loyalty"], {"points": 3})
        self.assertEqual(attrs["region"], "cn")

    def test_schema_evolution_fields_survive_late_event_propagation(self):
        e1 = event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A"})
        e2 = event("e2", "c1", 2, "2026-01-03T00:00:00+00:00", {"name": "B"})
        late = event(
            "e3", "c1", 3, "2026-01-02T00:00:00+00:00",
            {"score": 99, "connection_string": "postgres://example"},
        )
        apply_batch(self.db, manifest("b1", [e1]), [e1])
        apply_batch(self.db, manifest("b2", [e2]), [e2])
        apply_batch(self.db, manifest("b3", [late]), [late])
        rows = self.rows()
        for row in rows[1:]:
            attrs = json.loads(row["attributes_json"])
            self.assertIn("score", attrs)
            self.assertEqual(attrs["score"], 99)
            self.assertIn("connection_string", attrs)

    # -- 6. stale versions & same-version conflicts ----------------------

    def test_stale_event_is_skipped_but_audited(self):
        fresh = event("e2", "c1", 2, "2026-01-03T00:00:00+00:00", {"name": "B"})
        stale = event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A"})
        apply_batch(self.db, manifest("b1", [fresh]), [fresh])
        result = apply_batch(self.db, manifest("b2", [stale]), [stale])
        self.assertEqual(result.skipped_stale, 1)
        self.assertEqual(result.applied, 0)
        self.assertEqual(self.ledger("e1")["status"], "stale")
        self.assertEqual(len(self.rows()), 1)

    def test_same_version_different_content_is_rejected_not_overwritten(self):
        first = event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A"})
        apply_batch(self.db, manifest("b1", [first]), [first])
        # Version 2 first, so version-1 replay is judged against a recorded v1.
        v2 = event("e2", "c1", 2, "2026-01-02T00:00:00+00:00", {"name": "B"})
        apply_batch(self.db, manifest("b2", [v2]), [v2])
        conflicting = event("e9", "c1", 1, "2026-01-01T12:00:00+00:00", {"name": "EVIL"})
        result = apply_batch(self.db, manifest("b3", [conflicting]), [conflicting])
        self.assertEqual(result.rejected, 1)
        self.assertEqual(result.metrics["conflicts"], 1)
        self.assertEqual(self.ledger("e9")["status"], "rejected")
        rows = self.rows()
        self.assertEqual([r["name"] for r in rows], ["A", "B"])

    def test_same_version_same_content_is_a_harmless_noop(self):
        first = event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A"})
        apply_batch(self.db, manifest("b1", [first]), [first])
        v2 = event("e2", "c1", 2, "2026-01-02T00:00:00+00:00", {"name": "B"})
        apply_batch(self.db, manifest("b2", [v2]), [v2])
        duplicate_content = event("e8", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A"})
        result = apply_batch(self.db, manifest("b3", [duplicate_content]), [duplicate_content])
        self.assertEqual(result.skipped_stale, 1)
        self.assertEqual(result.rejected, 0)
        self.assertEqual(len(self.rows()), 2)

    # -- 7. deletes ------------------------------------------------------

    def test_delete_closes_current_and_late_delete_removes_orphans(self):
        e1 = event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A"})
        e2 = event("e2", "c1", 2, "2026-01-03T00:00:00+00:00", {"name": "B"})
        e3 = event("e3", "c1", 3, "2026-01-05T00:00:00+00:00", {"name": "C"})
        apply_batch(self.db, manifest("b1", [e1]), [e1])
        apply_batch(self.db, manifest("b2", [e2]), [e2])
        apply_batch(self.db, manifest("b3", [e3]), [e3])
        late_delete = event("ed", "c1", 4, "2026-01-04T00:00:00+00:00", op="delete")
        result = apply_batch(self.db, manifest("b4", [late_delete]), [late_delete])
        self.assertEqual(result.applied, 1)
        rows = self.rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[1]["valid_to"], "2026-01-04T00:00:00+00:00")
        self.assertEqual(sum(r["is_current"] for r in rows), 0)
        self.assert_no_overlaps()

    def test_late_upsert_before_a_delete_does_not_reopen_customer(self):
        e1 = event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A"})
        d = event("ed", "c1", 2, "2026-01-05T00:00:00+00:00", op="delete")
        apply_batch(self.db, manifest("b1", [e1]), [e1])
        apply_batch(self.db, manifest("b2", [d]), [d])
        late = event("e3", "c1", 3, "2026-01-03T00:00:00+00:00", {"name": "A2"})
        result = apply_batch(self.db, manifest("b3", [late]), [late])
        self.assertEqual(result.applied, 1)
        rows = self.rows()
        self.assertEqual(len(rows), 2)
        self.assertEqual([r["valid_to"] for r in rows], [
            "2026-01-03T00:00:00+00:00",
            "2026-01-05T00:00:00+00:00",
        ])
        self.assertEqual(sum(r["is_current"] for r in rows), 0)
        self.assert_no_overlaps()

    def test_upsert_after_delete_reopens_without_carrying_old_state(self):
        e1 = event("e1", "c1", 1, "2026-01-01T00:00:00+00:00",
                   {"name": "A", "email": "a@x", "tier": "gold"})
        d = event("ed", "c1", 2, "2026-01-03T00:00:00+00:00", op="delete")
        reopen = event("e3", "c1", 3, "2026-01-05T00:00:00+00:00", {"name": "Z"})
        apply_batch(self.db, manifest("b1", [e1]), [e1])
        apply_batch(self.db, manifest("b2", [d]), [d])
        result = apply_batch(self.db, manifest("b3", [reopen]), [reopen])
        self.assertEqual(result.applied, 1)
        rows = self.rows()
        self.assertEqual(len(rows), 2)
        new = rows[-1]
        self.assertEqual(new["name"], "Z")
        self.assertIsNone(new["email"])
        self.assertIsNone(new["tier"])
        self.assertEqual(json.loads(new["attributes_json"]), {"name": "Z"})
        self.assertEqual(new["is_current"], 1)
        self.assertEqual(rows[0]["is_current"], 0)
        self.assert_no_overlaps()

    # -- 8. recovery -----------------------------------------------------

    def test_recovery_then_replay_completes_and_stays_auditable(self):
        events = [event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A"})]
        m = manifest("crashed", events)
        with connect(self.db) as conn:
            ensure_schema(conn)
            conn.execute(
                "INSERT INTO applied_batches(batch_id, manifest_hash, applied_at, "
                "event_count, status, schema_version) VALUES (?, ?, ?, ?, ?, ?)",
                ("crashed", "stale-hash", "2026-01-01T00:00:00+00:00", 1,
                 STATUS_IN_PROGRESS, 1),
            )
            conn.commit()
        self.assertTrue(recover_manifest(self.db, m))
        self.assertEqual(self.batch_row("crashed")["status"], STATUS_RECOVERED)
        self.assertEqual(self.batch_row("crashed")["manifest_hash"], _manifest_hash(m))
        result = apply_batch(self.db, m, events)
        self.assertTrue(result.recovered)
        self.assertEqual(result.applied, 1)
        self.assertEqual(self.batch_row("crashed")["status"], STATUS_COMPLETE)

    def test_recovery_never_touches_complete_batches_and_is_idempotent(self):
        events = [event("e1", "c1", 1, "2026-01-01T00:00:00+00:00")]
        m = manifest("done", events)
        apply_batch(self.db, m, events)
        self.assertFalse(recover_manifest(self.db, m))
        self.assertEqual(self.batch_row("done")["status"], STATUS_COMPLETE)
        self.assertFalse(recover_manifest(self.db, manifest("missing", [])))

    # -- 9. metrics, serialization & log redaction -----------------------

    def test_metrics_are_complete_and_json_serializable(self):
        events = [
            event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A"}),
            event("e2", "c1", 2, "2026-01-02T00:00:00+00:00", op="delete"),
        ]
        result = apply_batch(self.db, manifest("b1", events), events)
        encoded = json.dumps(result.__dict__, ensure_ascii=False, sort_keys=True)
        decoded = json.loads(encoded)
        self.assertEqual(decoded["metrics"]["total_events"], 2)
        self.assertEqual(decoded["metrics"]["upserts"], 1)
        self.assertEqual(decoded["metrics"]["deletes"], 1)
        self.assertEqual(decoded["applied"], 2)

    def test_logs_never_contain_payload_secrets(self):
        stream = io.StringIO()
        handler = logging.StreamHandler(stream)
        logger = logging.getLogger("cdc_pipeline.engine")
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        try:
            secret = "postgres://user:supersecret@db.internal:5432/prod"
            events = [
                event(
                    "e1", "c1", 1, "2026-01-01T00:00:00+00:00",
                    {"name": "A", "password": "hunter2", "connection_string": secret},
                ),
                event("e-bad", "c1", 2, "nonsense-ts", {"api_key": "sk-shhh"}),
            ]
            with self.assertRaises(ValueError):
                apply_batch(self.db, manifest("b1", events), events)
        finally:
            logger.removeHandler(handler)
        output = stream.getvalue()
        self.assertNotIn("supersecret", output)
        self.assertNotIn("hunter2", output)
        self.assertNotIn("sk-shhh", output)
        self.assertNotIn("db.internal", output)

    # -- 10. CLI end to end ----------------------------------------------

    def test_cli_emits_serializable_json_and_recovers(self):
        events = [event("e1", "c1", 1, "2026-01-01T00:00:00+00:00", {"name": "A"})]
        manifest_path = Path(self.tmp.name) / "manifest.json"
        events_path = Path(self.tmp.name) / "events.jsonl"
        manifest_path.write_text(json.dumps(manifest("cli-1", events)), encoding="utf-8")
        events_path.write_text(
            "\n".join(json.dumps(e, ensure_ascii=False) for e in events), encoding="utf-8"
        )
        env = dict(__import__("os").environ, PYTHONPATH=str(SRC))
        run = subprocess.run(
            [sys.executable, "-m", "cdc_pipeline.cli",
             "--db", str(self.db), "--manifest", str(manifest_path),
             "--events", str(events_path)],
            capture_output=True, text=True, env=env, check=True,
        )
        payload = json.loads(run.stdout.strip())
        self.assertEqual(payload["batch_id"], "cli-1")
        self.assertEqual(payload["applied"], 1)

        # Idempotent replay through the CLI.
        run2 = subprocess.run(
            [sys.executable, "-m", "cdc_pipeline.cli",
             "--db", str(self.db), "--manifest", str(manifest_path),
             "--events", str(events_path)],
            capture_output=True, text=True, env=env, check=True,
        )
        self.assertEqual(json.loads(run2.stdout)["skipped_duplicates"], 1)

        # Recovery path reports structured JSON.
        run3 = subprocess.run(
            [sys.executable, "-m", "cdc_pipeline.cli",
             "--db", str(self.db), "--manifest", str(manifest_path),
             "--events", str(events_path), "--recover"],
            capture_output=True, text=True, env=env, check=True,
        )
        self.assertEqual(json.loads(run3.stdout), {"recovered": False})


if __name__ == "__main__":
    unittest.main()
