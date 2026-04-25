#!/usr/bin/env python3
"""Run hotel research + enrichment for many URLs with concurrency; commit to fullJSONs under a single file lock."""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from hotel_decision_maker_research import default_json_path_from_url
from lead_aggregates.store import AggregatesStore
from lead_aggregates.urls import canonical_hotel_url


def _read_urls_file(path: Path) -> list[str]:
    lines: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        lines.append(s)
    return lines


def _dedupe_urls(urls: list[str]) -> list[str]:
    out: list[str] = []
    canon_seen: set[str] = set()
    for u in urls:
        c = canonical_hotel_url(u)
        if c in canon_seen:
            continue
        canon_seen.add(c)
        out.append(u)
    return out


def _run_one(
    url: str,
    *,
    root: Path,
    store: AggregatesStore,
    agent_count: int,
    skip_if_enriched: bool,
) -> tuple[str, str]:
    """Returns (canonical_url, status_message)."""
    canon = canonical_hotel_url(url)
    if skip_if_enriched and store.enriched_entry_file_exists(canon):
        return canon, "skipped (already enriched)"

    research = Path(default_json_path_from_url(url))
    if not research.is_absolute():
        research = (root / research).resolve()
    enriched = research.parent / (research.stem + ".enriched.json")
    csv_path = root / "csv" / f"{research.stem}.csv"

    research_rel = research.relative_to(root).as_posix()
    enriched_rel = enriched.relative_to(root).as_posix()

    store.mark_researching(
        canonical_url=canon,
        research_json=research_rel,
        enriched_json=enriched_rel,
    )

    env = os.environ.copy()
    r1 = subprocess.run(
        [
            sys.executable,
            str(root / "hotel_decision_maker_research.py"),
            "--url",
            url,
            "--out-json",
            str(research),
            "--out-csv",
            str(csv_path),
            "--agent-count",
            str(agent_count),
        ],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
    )
    if r1.returncode != 0:
        err = (r1.stderr or r1.stdout or "research failed")[-4000:]
        store.commit_after_enrich(
            canonical_url=canon,
            research_json=research_rel,
            enriched_json=enriched_rel,
            error=err,
        )
        return canon, f"failed research: {r1.returncode}"

    r2 = subprocess.run(
        [
            sys.executable,
            str(root / "hotel_contact_enrichment.py"),
            "--in-json",
            str(research),
            "--out-json",
            str(enriched),
            "--mode",
            "realtime",
            "--pretty",
        ],
        cwd=root,
        env=env,
        capture_output=True,
        text=True,
    )
    if r2.returncode != 0:
        err = (r2.stderr or r2.stdout or "enrichment failed")[-4000:]
        store.commit_after_enrich(
            canonical_url=canon,
            research_json=research_rel,
            enriched_json=enriched_rel,
            error=err,
        )
        return canon, f"failed enrichment: {r2.returncode}"

    if os.environ.get("LINKEDIN_ENRICH", "").strip().lower() in ("1", "true", "yes"):
        r3 = subprocess.run(
            [
                sys.executable,
                str(root / "scripts" / "linkedin_exa_enrich.py"),
                "--in-json",
                str(enriched),
                "--out-json",
                str(enriched),
                "--discover-missing",
                "--pretty",
            ],
            cwd=root,
            env=env,
            capture_output=True,
            text=True,
        )
        if r3.returncode != 0:
            print(
                f"[warn] linkedin enrichment failed for {canon}: {r3.returncode}\n"
                f"{(r3.stderr or r3.stdout or 'linkedin enrichment failed')[-2000:]}",
                file=sys.stderr,
            )

    store.commit_after_enrich(
        canonical_url=canon,
        research_json=research_rel,
        enriched_json=enriched_rel,
        error=None,
    )
    return canon, "ok"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", action="append", default=[], help="Hotel URL (repeatable)")
    p.add_argument("--urls-file", type=Path, default=None, help="Text file, one URL per line")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--jsons-dir", type=Path, default=Path("jsons"))
    p.add_argument("--fulljsons-dir", type=Path, default=Path("fullJSONs"))
    p.add_argument("--agent-count", type=int, default=16)
    p.add_argument(
        "--skip-if-enriched",
        action="store_true",
        help="Skip URL if registry status is enriched and enriched JSON exists",
    )
    args = p.parse_args()

    root = Path.cwd().resolve()
    urls: list[str] = list(args.url)
    if args.urls_file is not None:
        urls.extend(_read_urls_file(args.urls_file))
    urls = _dedupe_urls(urls)
    if not urls:
        print("No URLs to process.", file=sys.stderr)
        return 2

    store = AggregatesStore(args.fulljsons_dir.resolve(), args.jsons_dir.resolve())

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {
            ex.submit(
                _run_one,
                u,
                root=root,
                store=store,
                agent_count=args.agent_count,
                skip_if_enriched=args.skip_if_enriched,
            ): u
            for u in urls
        }
        for fut in as_completed(futs):
            u = futs[fut]
            try:
                canon, msg = fut.result()
                print(f"{canon} :: {msg}")
            except Exception as e:
                print(f"{u} :: exception: {e}", file=sys.stderr)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
