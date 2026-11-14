# CDC SCD2 Replay

This repository is a deliberately incomplete, standard-library-only data pipeline exercise.
It consumes append-only JSONL change events and maintains a SQLite customer dimension with
history, replay-safe batches, schema evolution, and recovery metadata.

Run the visible tests with:

```bash
python -m unittest discover -s tests -v
```

The implementation is intentionally incomplete. The task prompt is the source of truth for
the missing behavior; do not weaken tests or add third-party dependencies.

