#!/usr/bin/env python3
"""Print generated cold emails from outreach_email_state.json to stdout (copy-paste friendly).

Only rows with a non-empty generation.body are shown. Use --hotel-url (repeatable) to filter by hotel netloc.

With --interactive: show one email at a time; prompt Done? [y/n/q]; y writes generation.review_marked_done_at_utc
to outreach_email_state.json (locked) then shows the next; n advances without writing (this run only); q quits.
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from outreach.indexes import rebuild_indexes  # noqa: E402
from outreach.netloc_filter import netlocs_from_hotel_urls, row_matches_hotel_netlocs  # noqa: E402
from outreach.schema import validate_state  # noqa: E402
from outreach.store import OutreachStore  # noqa: E402


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _collect_rows(
    by_id: dict,
    netlocs: frozenset[str],
    *,
    skip_reviewed: bool,
    skipped_ids: set[str] | None = None,
) -> list[dict]:
    rows_out: list[dict] = []
    skip_ids = skipped_ids or set()
    for oid in sorted(by_id.keys()):
        if oid in skip_ids:
            continue
        row = by_id.get(oid)
        if not isinstance(row, dict):
            continue
        if netlocs and not row_matches_hotel_netlocs(row, netlocs):
            continue
        gen = row.get("generation")
        if not isinstance(gen, dict):
            continue
        body = (gen.get("body") or "").strip()
        if not body:
            continue
        if skip_reviewed and (gen.get("review_marked_done_at_utc") or "").strip():
            continue
        snap = row.get("intimate_snapshot") if isinstance(row.get("intimate_snapshot"), dict) else {}
        rows_out.append(
            {
                "outreach_id": oid,
                "primary_email": row.get("primary_email"),
                "target_url": row.get("target_url"),
                "hotel_canonical_url": row.get("hotel_canonical_url"),
                "full_name": snap.get("full_name"),
                "title": snap.get("title"),
                "company": snap.get("company"),
                "subject": (gen.get("subject") or "").strip(),
                "body": body,
                "batch_job_id": gen.get("batch_job_id"),
            }
        )
    return rows_out


def _print_block(r: dict) -> None:
    sep = "=" * 80
    print(sep)
    print(f"outreach_id:   {r['outreach_id']}")
    print(f"to:            {r.get('primary_email')}")
    print(f"hotel:         {r.get('hotel_canonical_url')}")
    print(f"contact:       {r.get('full_name')} | {r.get('title')} | {r.get('company')}")
    print(f"batch_job_id:  {r.get('batch_job_id')}")
    print()
    print(f"Subject: {r.get('subject')}")
    print()
    print(r.get("body"))
    print(sep)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--fulljsons-dir", type=Path, default=Path("fullJSONs"))
    p.add_argument("--state-path", type=Path, default=None)
    p.add_argument(
        "--hotel-url",
        action="append",
        default=[],
        metavar="URL",
        help="Repeatable. Only rows whose hotel_canonical_url netloc matches these URLs.",
    )
    p.add_argument(
        "--json",
        action="store_true",
        help="Print one JSON array of objects instead of plain text blocks.",
    )
    p.add_argument(
        "--interactive",
        action="store_true",
        help="One at a time; Done? y/n writes review_marked_done_at_utc on y, then next.",
    )
    p.add_argument(
        "--include-reviewed",
        action="store_true",
        help="With --interactive or --json: include rows already marked done in review.",
    )
    p.add_argument(
        "--max-items",
        type=int,
        default=0,
        help="Interactive only: stop after N reviewed items (0 = no limit).",
    )
    args = p.parse_args()

    if args.max_items < 0:
        print("--max-items must be >= 0", file=sys.stderr)
        return 2

    if args.json and args.interactive:
        print("Cannot use --json with --interactive.", file=sys.stderr)
        return 2

    root = Path.cwd().resolve()
    fj = (root / args.fulljsons_dir).resolve()
    state_path = args.state_path or (fj / "outreach_email_state.json")
    state_path = state_path if state_path.is_absolute() else (root / state_path).resolve()

    if not state_path.is_file():
        print(f"No state file: {state_path}", file=sys.stderr)
        return 2

    netlocs = netlocs_from_hotel_urls(list(args.hotel_url or []))
    skip_reviewed = not args.include_reviewed

    if args.interactive:
        store = OutreachStore(state_path)
        marked = 0
        skipped: set[str] = set()
        reviewed = 0
        while True:
            doc = store.load_validated()
            by_id = doc.get("by_id") or {}
            rows_out = _collect_rows(by_id, netlocs, skip_reviewed=skip_reviewed, skipped_ids=skipped)
            if not rows_out:
                print("\n(no more generated emails to review in this filter.)", file=sys.stderr)
                break
            r = rows_out[0]
            _print_block(r)
            ans = input("Done? [y/n/q]: ").strip().lower()
            if ans == "q":
                print("quit", file=sys.stderr)
                break
            oid = str(r["outreach_id"])
            if ans == "y":

                def mark_done(doc: dict) -> None:
                    row = doc.get("by_id", {}).get(oid)
                    if not isinstance(row, dict):
                        return
                    gen = row.get("generation")
                    if not isinstance(gen, dict):
                        return
                    gen["review_marked_done_at_utc"] = _now_iso()
                    doc["updated_at_utc"] = _now_iso()
                    doc["indexes"] = rebuild_indexes(doc.get("by_id") or {})
                    problems = validate_state(doc)
                    if problems:
                        raise ValueError("; ".join(problems[:10]))

                store.update_in_place(mark_done)
                marked += 1
                skipped.discard(oid)
                print(f"[saved] review_marked_done_at_utc for {oid}", file=sys.stderr)
            else:
                # n or anything else: advance without persisting (session skip only)
                skipped.add(oid)
                print(f"[skip] not saved; will not show again this run: {oid}", file=sys.stderr)
            reviewed += 1
            if args.max_items and reviewed >= args.max_items:
                print(f"\nReached max-items={args.max_items}; stopping.", file=sys.stderr)
                break
        print(f"\n# marked done this session: {marked}", file=sys.stderr)
        return 0

    doc = json.loads(state_path.read_text(encoding="utf-8"))
    by_id = doc.get("by_id") or {}
    rows_out = _collect_rows(by_id, netlocs, skip_reviewed=False, skipped_ids=None)

    if args.json:
        print(json.dumps(rows_out, ensure_ascii=False, indent=2))
        return 0

    for i, r in enumerate(rows_out):
        if i:
            print()
        _print_block(r)

    print(f"\n# total: {len(rows_out)}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
