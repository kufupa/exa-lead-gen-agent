from __future__ import annotations

import argparse

from phone_crm.config import load_settings
from phone_crm.db import open_connection
from phone_crm.queries import load_warehouse, sync_rows


def run_sync(json_path: str, *, dry_run: bool = False) -> tuple[int, int]:
    rows = load_warehouse(json_path)
    if dry_run:
        return len(rows), len(rows)

    settings = load_settings()
    with open_connection(settings) as conn:
        upserted = sync_rows(conn, rows)
    return len(rows), upserted


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--json", default=None, help="Path to all_enriched_leads JSON")
    parser.add_argument("--dry-run", action="store_true", help="Parse only; do not write DB")
    parser.add_argument("--version", action="store_true", help="Print version row count only")
    args = parser.parse_args(argv)

    if args.version:
        print("phone_crm_sync.py")
        return 0

    if args.json:
        json_path = args.json
    elif args.dry_run:
        json_path = "fullJSONs/all_enriched_leads.json"
    else:
        json_path = load_settings().crm_json_path

    if not json_path:
        raise RuntimeError("JSON path is required for sync")

    seen, upserted = run_sync(json_path, dry_run=args.dry_run)

    if args.dry_run:
        print(f"seen={seen} would_upsert={upserted}")
    else:
        print(f"seen={seen} upserted={upserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
