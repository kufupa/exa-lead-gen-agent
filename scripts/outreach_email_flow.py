#!/usr/bin/env python3
"""One entrypoint: sync intimates -> state, then interactive triage (y/n per row), optional xAI cold-email batch.

Triage statuses (stored in outreach_email_state.json):
  pending           - not reviewed yet (shows up in the interactive queue)
  approved_generate - you answered y; cold-email batch may run for this row
  declined          - you answered n; skipped, no LLM

Use --hotel-url (repeatable) so the interactive queue only includes contacts for those hotel sites
(same netloc rules as outreach_email_triage.py).

Examples:
  python scripts/outreach_email_flow.py --hotel-url "https://www.apexhotels.co.uk/"
  python scripts/outreach_email_flow.py --hotel-url "https://a.com/" --generate
  python scripts/outreach_email_flow.py --hotel-url "https://a.com/" --generate --model other-model-id
  python scripts/outreach_email_flow.py --email-only-no-phone --hotel-url "https://www.example.com/" --generate
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts._repo_dotenv import load_repo_dotenv  # noqa: E402

from outreach.schema import validate_state  # noqa: E402
from outreach.store import OutreachStore  # noqa: E402
from outreach.sync import load_intimate_doc, merge_intimates_into_state  # noqa: E402


def _run_sync(
    *,
    intimate_path: Path,
    state_path: Path,
    refresh_snapshots: bool,
    skip_structured_phone_contacts: bool,
) -> int:
    if not intimate_path.is_file():
        print(f"Missing intimate file: {intimate_path}", file=sys.stderr)
        return 2
    intimate_doc = load_intimate_doc(intimate_path)
    store = OutreachStore(state_path)

    def op() -> tuple[int, int]:
        base = store.load_unlocked()
        merged, added, refreshed = merge_intimates_into_state(
            intimate_doc,
            base,
            refresh_snapshots=refresh_snapshots,
            skip_structured_phone_contacts=skip_structured_phone_contacts,
        )
        problems = validate_state(merged)
        if problems:
            raise ValueError("; ".join(problems[:10]))
        store.save(merged)
        return added, refreshed

    added, refreshed = store.run_locked(op)
    print(f"[sync] {state_path} added={added} snapshot_updates={refreshed}")
    return 0


def _extend_hotel_urls(cmd: list[str], hotel_urls: list[str]) -> None:
    for u in hotel_urls:
        if (u or "").strip():
            cmd.extend(["--hotel-url", u.strip()])


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--fulljsons-dir", type=Path, default=Path("fullJSONs"))
    p.add_argument("--intimate-path", type=Path, default=None)
    p.add_argument("--state-path", type=Path, default=None)
    p.add_argument(
        "--hotel-url",
        action="append",
        default=[],
        metavar="URL",
        help="Repeatable. Restrict interactive triage (and optional --generate) to these hotel netlocs.",
    )
    p.add_argument("--skip-sync", action="store_true", help="Do not merge intimate_email_contacts into state")
    p.add_argument("--skip-triage", action="store_true", help="Skip interactive y/n (e.g. only --generate after prior triage)")
    p.add_argument(
        "--refresh-snapshots",
        action="store_true",
        help="On sync: refresh intimate_snapshot when row hash changes",
    )
    p.add_argument(
        "--email-only-no-phone",
        action="store_true",
        help=(
            "Only contacts with no structured phone/phone2: skip them on sync, "
            "and restrict triage + --generate to the same (cold email only)."
        ),
    )
    p.add_argument(
        "--generate",
        action="store_true",
        help="After triage, run xAI cold-email batch (needs XAI_API_KEY). Respects same --hotel-url filter.",
    )
    p.add_argument(
        "--model",
        default=None,
        help="Only with --generate: forwarded as --model (else child uses OUTREACH_EMAIL_MODEL or grok-4.20-reasoning).",
    )
    p.add_argument("--generate-limit", type=int, default=0, help="Max rows for --generate (0 = all matching)")
    args = p.parse_args()
    load_repo_dotenv(_ROOT)

    root = Path.cwd().resolve()
    fj = (root / args.fulljsons_dir).resolve()
    intimate = args.intimate_path or (fj / "intimate_email_contacts.json")
    intimate = intimate if intimate.is_absolute() else (root / intimate).resolve()
    state_path = args.state_path or (fj / "outreach_email_state.json")
    state_path = state_path if state_path.is_absolute() else (root / state_path).resolve()
    hotel_urls = [u for u in (args.hotel_url or []) if (u or "").strip()]

    if not args.skip_sync:
        rc = _run_sync(
            intimate_path=intimate,
            state_path=state_path,
            refresh_snapshots=args.refresh_snapshots,
            skip_structured_phone_contacts=args.email_only_no_phone,
        )
        if rc != 0:
            return rc

    if not args.skip_triage:
        cmd = [
            sys.executable,
            str(_ROOT / "scripts" / "outreach_email_triage.py"),
            "--interactive",
            "--fulljsons-dir",
            str(fj),
            "--state-path",
            str(state_path),
        ]
        _extend_hotel_urls(cmd, hotel_urls)
        if args.email_only_no_phone:
            cmd.append("--email-only-no-phone")
        print("[triage] starting interactive queue (y=approve cold email, n=decline, q=quit)...")
        r = subprocess.run(cmd, cwd=str(root))
        if r.returncode != 0:
            return r.returncode

    if args.generate:
        cmd = [
            sys.executable,
            str(_ROOT / "scripts" / "outreach_email_generate_xai.py"),
            "--fulljsons-dir",
            str(fj),
            "--state-path",
            str(state_path),
            "--intimate-path",
            str(intimate),
        ]
        if (args.model or "").strip():
            cmd.extend(["--model", args.model.strip()])
        _extend_hotel_urls(cmd, hotel_urls)
        if args.generate_limit and args.generate_limit > 0:
            cmd.extend(["--limit", str(args.generate_limit)])
        if args.email_only_no_phone:
            cmd.append("--email-only-no-phone")
        print("[generate] running xAI batch...")
        r = subprocess.run(cmd, cwd=str(root))
        if r.returncode != 0:
            return r.returncode

    print("[flow] done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
