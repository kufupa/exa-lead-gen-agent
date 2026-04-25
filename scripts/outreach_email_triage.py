#!/usr/bin/env python3
"""Approve or decline outreach rows (no LLM). Updates fullJSONs/outreach_email_state.json under lock.

Triage status on each row:
  pending            - not reviewed yet (included in --interactive until you y/n)
  approved_generate  - you approved: cold-email generation may run for this contact
  declined           - you declined: no generation; hidden from default queues
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from lead_aggregates.builders import has_structured_phone  # noqa: E402

from outreach.batch_cold_email import build_intimate_index_by_outreach_id  # noqa: E402
from outreach.indexes import rebuild_indexes  # noqa: E402
from outreach.netloc_filter import netlocs_from_hotel_urls, row_matches_hotel_netlocs  # noqa: E402
from outreach.schema import (  # noqa: E402
    TRIAGE_APPROVED,
    TRIAGE_DECLINED,
    TRIAGE_PENDING,
    validate_state,
)
from outreach.store import OutreachStore  # noqa: E402
from outreach.sync import load_intimate_doc  # noqa: E402


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _summary(doc: dict) -> None:
    by_id = doc.get("by_id") or {}
    counts: dict[str, int] = {}
    for row in by_id.values():
        if not isinstance(row, dict):
            continue
        t = row.get("triage") or {}
        st = t.get("status") if isinstance(t, dict) else "?"
        counts[st] = counts.get(st, 0) + 1
    print("triage counts:", counts, "total:", len(by_id))


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--fulljsons-dir", type=Path, default=Path("fullJSONs"))
    p.add_argument("--state-path", type=Path, default=None)
    p.add_argument("--summary", action="store_true", help="Print triage status counts and exit")
    p.add_argument("--approve-all-pending", action="store_true")
    p.add_argument("--decline-all-pending", action="store_true")
    p.add_argument("--interactive", action="store_true", help="Prompt y/n for each pending row")
    p.add_argument(
        "--hotel-url",
        action="append",
        default=[],
        metavar="URL",
        help="Repeatable. If set, only rows whose hotel_canonical_url netloc matches these URLs' netlocs.",
    )
    p.add_argument(
        "--intimate-path",
        type=Path,
        default=None,
        help="With --email-only-no-phone: intimate JSON (default: <fulljsons-dir>/intimate_email_contacts.json).",
    )
    p.add_argument(
        "--email-only-no-phone",
        action="store_true",
        help="Skip rows whose intimate contact has structured phone or phone2.",
    )
    args = p.parse_args()

    root = Path.cwd().resolve()
    fj = (root / args.fulljsons_dir).resolve()
    state_path = args.state_path or (fj / "outreach_email_state.json")
    state_path = state_path if state_path.is_absolute() else (root / state_path).resolve()
    store = OutreachStore(state_path)

    intimate_path = args.intimate_path or (fj / "intimate_email_contacts.json")
    intimate_path = intimate_path if intimate_path.is_absolute() else (root / intimate_path).resolve()
    intimate_index: dict | None = None
    if args.email_only_no_phone:
        if not intimate_path.is_file():
            print(f"Missing intimate file for --email-only-no-phone: {intimate_path}", file=sys.stderr)
            return 2
        intimate_index = build_intimate_index_by_outreach_id(load_intimate_doc(intimate_path))

    def _email_only_row(oid: str, row: dict) -> bool:
        if not args.email_only_no_phone or intimate_index is None:
            return True
        ir = intimate_index.get(oid)
        if ir is None:
            return False
        return not has_structured_phone(ir)

    if args.summary:

        def read_only() -> None:
            doc = store.load_validated()
            _summary(doc)

        store.run_locked(read_only)
        return 0

    n_flags = sum([args.approve_all_pending, args.decline_all_pending, args.interactive])
    if n_flags != 1:
        print("Specify exactly one of --approve-all-pending, --decline-all-pending, --interactive", file=sys.stderr)
        return 2

    netlocs = netlocs_from_hotel_urls(list(args.hotel_url or []))

    def mutate(doc: dict) -> None:
        by_id = doc.setdefault("by_id", {})
        assert isinstance(by_id, dict)
        changed = 0
        if args.approve_all_pending or args.decline_all_pending:
            new_status = TRIAGE_APPROVED if args.approve_all_pending else TRIAGE_DECLINED
            pending_total = 0
            pending_matched = 0
            for oid, row in by_id.items():
                if not isinstance(row, dict):
                    continue
                triage = row.setdefault("triage", {})
                if triage.get("status") != TRIAGE_PENDING:
                    continue
                pending_total += 1
                if not row_matches_hotel_netlocs(row, netlocs):
                    continue
                if not _email_only_row(str(oid), row):
                    continue
                pending_matched += 1
                triage["status"] = new_status
                triage["decided_at_utc"] = _now()
                changed += 1
            if netlocs:
                print(f"hotel netloc filter: {sorted(netlocs)} | pending matched {pending_matched} of {pending_total}")
            print(f"updated {changed} rows -> {new_status}")
        else:
            pending_rows = [
                (oid, row)
                for oid, row in by_id.items()
                if isinstance(row, dict)
                and isinstance(row.get("triage"), dict)
                and row["triage"].get("status") == TRIAGE_PENDING
                and row_matches_hotel_netlocs(row, netlocs)
                and _email_only_row(str(oid), row)
            ]
            if netlocs:
                all_pending = sum(
                    1
                    for r in by_id.values()
                    if isinstance(r, dict)
                    and isinstance(r.get("triage"), dict)
                    and r["triage"].get("status") == TRIAGE_PENDING
                )
                print(f"hotel netloc filter: {sorted(netlocs)} | interactive queue {len(pending_rows)} of {all_pending} pending")
            pending_rows.sort(key=lambda x: (x[1].get("hotel_canonical_url") or "", x[0]))
            for oid, row in pending_rows:
                snap = row.get("intimate_snapshot") or {}
                name = snap.get("full_name") or "?"
                em = row.get("primary_email") or "?"
                hotel = row.get("hotel_canonical_url") or "?"
                ans = input(f"{oid} | {hotel} | {em} | {name}\nApprove generate? [y/n/q] ").strip().lower()
                if ans == "q":
                    print("quit")
                    break
                triage = row.setdefault("triage", {})
                if ans == "y":
                    triage["status"] = TRIAGE_APPROVED
                elif ans == "n":
                    triage["status"] = TRIAGE_DECLINED
                else:
                    print("skip (no change)")
                    continue
                triage["decided_at_utc"] = _now()
                changed += 1
            print(f"interactive updates: {changed}")

        doc["updated_at_utc"] = _now()
        doc["indexes"] = rebuild_indexes(doc.get("by_id") or {})
        problems = validate_state(doc)
        if problems:
            raise ValueError("; ".join(problems[:10]))

    store.update_in_place(mutate)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
