#!/usr/bin/env python3
"""Run hotel research + enrichment (+ LinkedIn Exa phase by default) for many URLs; commit to fullJSONs under a single file lock.

Set LINKEDIN_ENRICH=0 (or false/no/off/disabled) to skip phase 3.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from hotel_decision_maker_research import default_json_path_from_url
from lead_aggregates.store import AggregatesStore
from lead_aggregates.urls import canonical_hotel_url


def _linkedin_enrich_enabled() -> bool:
    """Run phase 3 (LinkedIn Exa script) unless LINKEDIN_ENRICH is explicitly disabled."""
    v = os.environ.get("LINKEDIN_ENRICH", "").strip().lower()
    if not v:
        return True
    return v not in ("0", "false", "no", "off", "disabled")


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


def _make_run_claim() -> str:
    return f"{uuid.uuid4()}::pid{os.getpid()}"


def _phase3_script(root: Path) -> Path:
    return root / "scripts" / "linkedin_exa_enrich.py"


def _run_one(
    url: str,
    *,
    root: Path,
    store: AggregatesStore,
    skip_if_enriched: bool,
    phase2_model: str | None = None,
    run_claim: str | None = None,
    phase1_timeout_sec: float = 0.0,
    phase2_timeout_sec: float = 0.0,
    phase3_timeout_sec: float = 0.0,
) -> tuple[str, str]:
    """Returns (canonical_url, status_message)."""
    claim = run_claim or _make_run_claim()
    t1 = phase1_timeout_sec if phase1_timeout_sec > 0 else None
    t2 = phase2_timeout_sec if phase2_timeout_sec > 0 else None
    t3 = phase3_timeout_sec if phase3_timeout_sec > 0 else None

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
        run_claim=claim,
    )

    env = os.environ.copy()
    try:
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
            ],
            cwd=root,
            env=env,
            capture_output=True,
            text=True,
            timeout=t1,
        )
    except subprocess.TimeoutExpired:
        err = "phase1 subprocess timeout"
        store.commit_after_enrich(
            canonical_url=canon,
            research_json=research_rel,
            enriched_json=enriched_rel,
            error=err,
            run_claim=claim,
        )
        return canon, "failed research: timeout"
    except OSError as e:
        err = f"phase1 spawn error: {e}"
        store.commit_after_enrich(
            canonical_url=canon,
            research_json=research_rel,
            enriched_json=enriched_rel,
            error=err,
            run_claim=claim,
        )
        return canon, f"failed research: os_error {e!r}"

    if r1.returncode != 0:
        err = (r1.stderr or r1.stdout or "research failed")[-4000:]
        store.commit_after_enrich(
            canonical_url=canon,
            research_json=research_rel,
            enriched_json=enriched_rel,
            error=err,
            run_claim=claim,
        )
        return canon, f"failed research: {r1.returncode}"

    r2_cmd = [
        sys.executable,
        str(root / "hotel_contact_enrichment.py"),
        "--in-json",
        str(research),
        "--out-json",
        str(enriched),
        "--pretty",
    ]
    if phase2_model:
        r2_cmd.extend(["--model", phase2_model])

    try:
        r2 = subprocess.run(
            r2_cmd,
            cwd=root,
            env=env,
            capture_output=True,
            text=True,
            timeout=t2,
        )
    except subprocess.TimeoutExpired:
        err = "phase2 subprocess timeout"
        store.commit_after_enrich(
            canonical_url=canon,
            research_json=research_rel,
            enriched_json=enriched_rel,
            error=err,
            run_claim=claim,
        )
        return canon, "failed enrichment: timeout"
    except OSError as e:
        err = f"phase2 spawn error: {e}"
        store.commit_after_enrich(
            canonical_url=canon,
            research_json=research_rel,
            enriched_json=enriched_rel,
            error=err,
            run_claim=claim,
        )
        return canon, f"failed enrichment: os_error {e!r}"

    if r2.returncode != 0:
        err = (r2.stderr or r2.stdout or "enrichment failed")[-4000:]
        store.commit_after_enrich(
            canonical_url=canon,
            research_json=research_rel,
            enriched_json=enriched_rel,
            error=err,
            run_claim=claim,
        )
        return canon, f"failed enrichment: {r2.returncode}"

    if _linkedin_enrich_enabled():
        p3 = _phase3_script(root)
        if not p3.is_file():
            print(
                f"[warn] phase3 script missing at {p3}; skip LinkedIn enrichment for {canon}",
                file=sys.stderr,
            )
        else:
            try:
                r3 = subprocess.run(
                    [
                        sys.executable,
                        str(p3),
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
                    timeout=t3,
                )
                if r3.returncode != 0:
                    print(
                        f"[warn] linkedin enrichment failed for {canon}: {r3.returncode}\n"
                        f"{(r3.stderr or r3.stdout or 'linkedin enrichment failed')[-2000:]}",
                        file=sys.stderr,
                    )
            except subprocess.TimeoutExpired:
                print(f"[warn] linkedin enrichment timeout for {canon}", file=sys.stderr)
            except OSError as e:
                print(f"[warn] linkedin enrichment spawn error for {canon}: {e}", file=sys.stderr)

    reg_ok = store.commit_after_enrich(
        canonical_url=canon,
        research_json=research_rel,
        enriched_json=enriched_rel,
        error=None,
        run_claim=claim,
    )
    if not reg_ok:
        return canon, "ok (aggregates_updated_registry_claim_mismatch)"
    return canon, "ok"


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--url", action="append", default=[], help="Hotel URL (repeatable)")
    p.add_argument("--urls-file", type=Path, default=None, help="Text file, one URL per line")
    p.add_argument("--workers", type=int, default=4)
    p.add_argument("--jsons-dir", type=Path, default=Path("jsons"))
    p.add_argument("--fulljsons-dir", type=Path, default=Path("fullJSONs"))
    p.add_argument(
        "--phase2-model",
        default=None,
        help="Optional model override for phase 2 (default: contact_enrichment default)",
    )
    p.add_argument(
        "--phase1-timeout-sec",
        type=float,
        default=0.0,
        help="Subprocess timeout for phase 1 (0 = no timeout)",
    )
    p.add_argument(
        "--phase2-timeout-sec",
        type=float,
        default=0.0,
        help="Subprocess timeout for phase 2 (0 = no timeout)",
    )
    p.add_argument(
        "--phase3-timeout-sec",
        type=float,
        default=0.0,
        help="Subprocess timeout for phase 3 LinkedIn enrich (0 = no timeout)",
    )
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
    any_hard_fail = False

    with ThreadPoolExecutor(max_workers=max(1, args.workers)) as ex:
        futs = {
            ex.submit(
                _run_one,
                u,
                root=root,
                store=store,
                skip_if_enriched=args.skip_if_enriched,
                phase2_model=(args.phase2_model.strip() if args.phase2_model else None),
                phase1_timeout_sec=args.phase1_timeout_sec,
                phase2_timeout_sec=args.phase2_timeout_sec,
                phase3_timeout_sec=args.phase3_timeout_sec,
            ): u
            for u in urls
        }
        for fut in as_completed(futs):
            u = futs[fut]
            try:
                canon, msg = fut.result()
                print(f"{canon} :: {msg}")
                if msg.startswith("failed ") or msg.startswith("exception:"):
                    any_hard_fail = True
            except Exception as e:
                print(f"{u} :: exception: {e}", file=sys.stderr)
                any_hard_fail = True

    return 1 if any_hard_fail else 0


if __name__ == "__main__":
    raise SystemExit(main())
