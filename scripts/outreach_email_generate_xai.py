#!/usr/bin/env python3
"""Run xAI realtime to draft cold emails for triage-approved outreach rows. Needs XAI_API_KEY.

Model: --model overrides OUTREACH_EMAIL_MODEL; if neither set, uses grok-4.20-reasoning.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts._repo_dotenv import load_repo_dotenv  # noqa: E402

from lead_aggregates.builders import has_structured_phone  # noqa: E402

from outreach.batch_cold_email import (  # noqa: E402
    apply_generation_results,
    build_intimate_index_by_outreach_id,
    build_user_message,
    default_prompt_paths,
    generation_candidates,
    load_prompt_pair,
    run_cold_email_realtime,
    user_prompt_hash,
)
from outreach.netloc_filter import netlocs_from_hotel_urls, row_matches_hotel_netlocs  # noqa: E402
from outreach.schema import validate_state  # noqa: E402
from outreach.store import OutreachStore  # noqa: E402
from outreach.sync import load_intimate_doc  # noqa: E402


def _contact_payload(
    oid: str,
    *,
    intimate_row: dict | None,
    state_row: dict,
) -> dict:
    base: dict = dict(intimate_row) if intimate_row else {}
    if not base:
        snap = state_row.get("intimate_snapshot") or {}
        base = {
            "full_name": snap.get("full_name"),
            "title": snap.get("title"),
            "company": snap.get("company"),
            "phase1_research": {"source_enriched_json": snap.get("source_enriched_json")},
        }
    base["outreach_id"] = oid
    base["outreach_primary_email"] = state_row.get("primary_email")
    base["outreach_target_url"] = state_row.get("target_url")
    base["outreach_hotel_canonical_url"] = state_row.get("hotel_canonical_url")
    return base


DEFAULT_OUTREACH_XAI_MODEL = "grok-4.20-reasoning"


def _resolve_model(cli: str | None) -> str:
    v = (cli or "").strip()
    if v:
        return v
    v = (os.environ.get("OUTREACH_EMAIL_MODEL") or "").strip()
    if v:
        return v
    return DEFAULT_OUTREACH_XAI_MODEL


def _default_concurrency() -> int:
    return min(12, os.cpu_count() or 4)


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--fulljsons-dir", type=Path, default=Path("fullJSONs"))
    p.add_argument("--intimate-path", type=Path, default=None)
    p.add_argument("--state-path", type=Path, default=None)
    p.add_argument(
        "--model",
        default=None,
        help=f"xAI model id. Else OUTREACH_EMAIL_MODEL; else default {DEFAULT_OUTREACH_XAI_MODEL}.",
    )
    p.add_argument("--max-turns", type=int, default=8)
    p.add_argument(
        "--concurrency",
        type=int,
        default=None,
        help=f"Realtime worker threads (default: min(12, os.cpu_count()) = {_default_concurrency()})",
    )
    p.add_argument("--limit", type=int, default=0, help="Max rows this run (0 = all)")
    p.add_argument("--dry-run", action="store_true", help="Print candidate count and exit")
    p.add_argument("--system-prompt", type=Path, default=None)
    p.add_argument("--user-template", type=Path, default=None)
    p.add_argument(
        "--system-prompt-id",
        default="cold_email_system_ansh_v1",
        help="Stored on each generation row for traceability (prompt text is from --system-prompt file).",
    )
    p.add_argument(
        "--hotel-url",
        action="append",
        default=[],
        metavar="URL",
        help="Repeatable. If set, only rows whose hotel_canonical_url netloc matches these URLs' netlocs.",
    )
    p.add_argument(
        "--email-only-no-phone",
        action="store_true",
        help="Only generate for intimate rows with no structured phone or phone2.",
    )
    args = p.parse_args()
    load_repo_dotenv(_ROOT)

    root = Path.cwd().resolve()
    fj = (root / args.fulljsons_dir).resolve()
    intimate_path = args.intimate_path or (fj / "intimate_email_contacts.json")
    intimate_path = intimate_path if intimate_path.is_absolute() else (root / intimate_path).resolve()
    state_path = args.state_path or (fj / "outreach_email_state.json")
    state_path = state_path if state_path.is_absolute() else (root / state_path).resolve()

    if not intimate_path.is_file():
        print(f"Missing {intimate_path}", file=sys.stderr)
        return 2

    store = OutreachStore(state_path)
    intimate_doc = load_intimate_doc(intimate_path)
    index = build_intimate_index_by_outreach_id(intimate_doc)

    sp_default, ut_default = default_prompt_paths(_ROOT)
    system_path = args.system_prompt or sp_default
    user_path = args.user_template or ut_default
    system_path = system_path if system_path.is_absolute() else (root / system_path).resolve()
    user_path = user_path if user_path.is_absolute() else (root / user_path).resolve()
    system_text, user_template = load_prompt_pair(system_path, user_path)
    netlocs = netlocs_from_hotel_urls(list(args.hotel_url or []))

    def _email_only_ok(oid: str) -> bool:
        if not args.email_only_no_phone:
            return True
        ir = index.get(oid)
        if ir is None:
            return False
        return not has_structured_phone(ir)

    def pick_ids() -> list[str]:
        doc = store.load_validated()
        out = generation_candidates(doc)
        valid_current_ids = set(index)
        out = [oid for oid in out if oid in valid_current_ids]
        if netlocs:
            by_id = doc.get("by_id") or {}
            out = [oid for oid in out if row_matches_hotel_netlocs(by_id.get(oid) or {}, netlocs)]
        out = [oid for oid in out if _email_only_ok(oid)]
        if args.limit and args.limit > 0:
            out = out[: args.limit]
        return out

    ids = store.run_locked(pick_ids)
    if args.dry_run:
        suffix = f" (hotel netloc filter: {sorted(netlocs)})" if netlocs else ""
        print(f"candidates (approved, no body yet): {len(ids)}{suffix}")
        return 0

    resolved_model = _resolve_model(args.model)
    concurrency = args.concurrency if args.concurrency is not None else _default_concurrency()

    if not ids:
        print("No candidates (need triage approved_generate with empty generation.body).")
        return 0

    jobs: list[tuple[str, dict]] = []
    user_hashes: dict[str, str] = {}

    def locked_build() -> None:
        nonlocal jobs, user_hashes
        doc = store.load_validated()
        by_id = doc.get("by_id") or {}
        for oid in ids:
            row = by_id.get(oid)
            if not isinstance(row, dict):
                continue
            intimate_row = index.get(oid)
            payload = _contact_payload(oid, intimate_row=intimate_row, state_row=row)
            ut = build_user_message(contact_json=payload, template_body=user_template)
            user_hashes[oid] = user_prompt_hash(ut)
            jobs.append((oid, payload))

    store.run_locked(locked_build)

    rows, failures, batch_id = run_cold_email_realtime(
        jobs,
        model=resolved_model,
        max_turns=args.max_turns,
        system_prompt_text=system_text,
        user_template_text=user_template,
        concurrency=concurrency,
    )

    def merge_fresh(doc: dict) -> None:
        apply_generation_results(
            doc,
            batch_id=batch_id,
            model=resolved_model,
            system_prompt_id=args.system_prompt_id,
            outreach_ids=ids,
            user_prompt_hashes=user_hashes,
            rows_ok=rows,
            failures=failures,
        )
        problems = validate_state(doc)
        if problems:
            raise ValueError("; ".join(problems[:10]))

    store.update_in_place(merge_fresh)
    print(f"job_id={batch_id} ok={len(rows)} fail={len(failures)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
