#!/usr/bin/env python3
"""Rebuild fullJSONs aggregates (master, intimate phone/email, url registry) from jsons/*.enriched.json."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# Repo root on sys.path for `lead_aggregates`
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
    store.rebuild_all()
    print(f"Rebuilt aggregates under {args.fulljsons_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
