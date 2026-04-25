#!/usr/bin/env python3
"""Merge fullJSONs/intimate_email_contacts.json into fullJSONs/outreach_email_state.json (locked)."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from outreach.schema import validate_state  # noqa: E402
from outreach.store import OutreachStore  # noqa: E402
from outreach.sync import load_intimate_doc, merge_intimates_into_state  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--fulljsons-dir", type=Path, default=Path("fullJSONs"))
    p.add_argument(
        "--intimate-path",
        type=Path,
        default=None,
        help="Default: <fulljsons-dir>/intimate_email_contacts.json",
    )
    p.add_argument(
        "--state-path",
        type=Path,
        default=None,
        help="Default: <fulljsons-dir>/outreach_email_state.json",
    )
    p.add_argument(
        "--refresh-snapshots",
        action="store_true",
        help="Refresh intimate_snapshot for existing rows (also when row hash changes)",
    )
    p.add_argument(
        "--email-only-no-phone",
        action="store_true",
        help="Do not sync contacts that have structured phone or phone2 (email-only outreach).",
    )
    args = p.parse_args()

    root = Path.cwd().resolve()
    fj = (root / args.fulljsons_dir).resolve()
    intimate = args.intimate_path or (fj / "intimate_email_contacts.json")
    intimate = intimate if intimate.is_absolute() else (root / intimate).resolve()
    state_path = args.state_path or (fj / "outreach_email_state.json")
    state_path = state_path if state_path.is_absolute() else (root / state_path).resolve()

    if not intimate.is_file():
        print(f"Missing intimate file: {intimate}", file=sys.stderr)
        return 2

    intimate_doc = load_intimate_doc(intimate)
    store = OutreachStore(state_path)

    def op() -> tuple[int, int]:
        base = store.load_unlocked()
        merged, added, refreshed = merge_intimates_into_state(
            intimate_doc,
            base,
            refresh_snapshots=args.refresh_snapshots,
            skip_structured_phone_contacts=args.email_only_no_phone,
        )
        problems = validate_state(merged)
        if problems:
            raise ValueError("; ".join(problems[:10]))
        store.save(merged)
        return added, refreshed

    added, refreshed = store.run_locked(op)
    print(f"Wrote {state_path} (added={added}, snapshot_updates={refreshed})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
