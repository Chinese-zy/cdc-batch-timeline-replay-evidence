from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from .engine import apply_batch, recover_manifest
from .storage import read_jsonl


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--events", required=True)
    parser.add_argument("--recover", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    manifest = json.loads(Path(args.manifest).read_text(encoding="utf-8"))
    if args.recover:
        recovered = recover_manifest(args.db, manifest)
        print(json.dumps({"recovered": recovered}, ensure_ascii=False))
        return 0
    result = apply_batch(args.db, manifest, read_jsonl(args.events))
    print(json.dumps(result.__dict__, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

