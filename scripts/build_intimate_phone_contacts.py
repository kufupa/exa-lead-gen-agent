#!/usr/bin/env python3
"""Rebuild fullJSONs/intimate_phone_contacts.json from jsons/*.enriched.json (structured phone)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from lead_aggregates.store import AggregatesStore  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--jsons-dir", type=Path, default=Path("jsons"))
    p.add_argument("--fulljsons-dir", type=Path, default=Path("fullJSONs"))
    args = p.parse_args()
    store = AggregatesStore(args.fulljsons_dir, args.jsons_dir)
    store.rebuild_phone_document_only()
    doc_path = args.fulljsons_dir / "intimate_phone_contacts.json"
    print(f"Wrote {doc_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
