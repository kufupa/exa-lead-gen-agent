#!/usr/bin/env python3
"""LinkedIn profile enrichment via Exa fetch + Grok structuring (no web_search tool).

Reads *.enriched.json, extracts linkedin_urls, fetches via Exa, structures via Grok,
writes linkedin_profile back onto each contact.

Usage:
  python scripts/linkedin_exa_enrich.py --in-json jsons/foo.enriched.json --out-json jsons/foo.enriched.json --pretty
  python scripts/linkedin_exa_enrich.py --in-json jsons/foo.enriched.json --dry-run
  python scripts/linkedin_exa_enrich.py --jsons-dir jsons/ --all  # batch all enriched files
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts._repo_dotenv import load_repo_dotenv  # noqa: E402

from linkedin_enrich.exa_fetch import (  # noqa: E402
    discover_linkedin_urls,
    fetch_linkedin_profiles,
    normalize_linkedin_url,
)
from linkedin_enrich.grok_structure import structure_profile  # noqa: E402
from pipeline_metrics import (  # noqa: E402
    XaiRates,
    estimate_exa_cost,
    estimate_xai_cost,
    merge_xai_usage_dicts,
)

DEFAULT_MODEL = "grok-4.20-reasoning"


def _process_one_file(
    path: Path,
    out_path: Path,
    *,
    exa_client: Any,
    xai_api_key: str,
    model: str,
    discover_missing: bool,
    skip_existing: bool,
    pretty: bool,
    dry_run: bool,
) -> dict[str, int]:
    data = json.loads(path.read_text(encoding="utf-8"))
    contacts = data.get("contacts", [])
    if not isinstance(contacts, list):
        return {"skipped": 0, "fetched": 0, "structured": 0, "errors": 0}

    urls_to_fetch: list[str] = []
    for c in contacts:
        if skip_existing and c.get("linkedin_profile"):
            continue
        li = (c.get("linkedin_url") or "").strip()
        if li:
            urls_to_fetch.append(li)

    discovered: dict[str, str] = {}
    missing: list[dict[str, str]] = []
    if discover_missing:
        missing = [
            {"full_name": c.get("full_name", ""), "company": c.get("company", "")}
            for c in contacts
            if not (c.get("linkedin_url") or "").strip()
            and not (skip_existing and c.get("linkedin_profile"))
        ]
        if missing and not dry_run:
            discovered = discover_linkedin_urls(exa_client, missing)

    stats: dict[str, int] = {
        "skipped": 0,
        "fetched": 0,
        "structured": 0,
        "discovered": len(discovered),
        "errors": 0,
    }

    if dry_run:
        stats["would_fetch"] = len(set(normalize_linkedin_url(u) for u in urls_to_fetch))
        stats["would_discover"] = (
            len(
                [
                    c
                    for c in contacts
                    if not (c.get("linkedin_url") or "").strip()
                    and not (skip_existing and c.get("linkedin_profile"))
                ]
            )
            if discover_missing
            else 0
        )
        return stats

    all_urls = list(urls_to_fetch)
    for _key, url in discovered.items():
        all_urls.append(url)

    exa_search_requests = len(missing) if discover_missing and missing else 0
    uniq_norm_urls = {normalize_linkedin_url(u) for u in all_urls if normalize_linkedin_url(u)}
    exa_content_pages = len(uniq_norm_urls)

    profile_markdowns = fetch_linkedin_profiles(exa_client, all_urls) if all_urls else {}
    stats["fetched"] = len(profile_markdowns)

    xai_usage_parts: list[dict[str, Any]] = []

    for c in contacts:
        if (c.get("linkedin_url") or "").strip():
            continue
        name = (c.get("full_name") or "").strip()
        company = (c.get("company") or "").strip()
        key = f"{name}|{company}"
        if key in discovered:
            raw_li = (discovered[key] or "").strip()
            c["linkedin_url"] = normalize_linkedin_url(raw_li) or raw_li

    for c in contacts:
        if skip_existing and c.get("linkedin_profile"):
            stats["skipped"] += 1
            continue
        li = (c.get("linkedin_url") or "").strip()
        if not li:
            continue
        norm = normalize_linkedin_url(li)
        if not norm:
            continue
        md = profile_markdowns.get(norm, "")
        if not md.strip():
            continue
        try:
            profile, usage_one = structure_profile(
                api_key=xai_api_key,
                model=model,
                linkedin_url=norm,
                markdown=md,
            )
            xai_usage_parts.append(usage_one)
            if profile:
                c["linkedin_profile"] = profile.model_dump()
                c["linkedin_url"] = norm
                stats["structured"] += 1
            else:
                stats["errors"] += 1
        except Exception as e:
            print(f"  error structuring {li}: {e}", file=sys.stderr)
            stats["errors"] += 1

    for c in contacts:
        li = (c.get("linkedin_url") or "").strip()
        if not li:
            continue
        n = normalize_linkedin_url(li)
        if n:
            c["linkedin_url"] = n

    merged_xai_usage = merge_xai_usage_dicts(*xai_usage_parts) if xai_usage_parts else {}
    xai_rates = XaiRates()
    xai_cost = estimate_xai_cost(merged_xai_usage, rates=xai_rates) if merged_xai_usage else estimate_xai_cost({}, rates=xai_rates)
    exa_cost = estimate_exa_cost(search_requests=exa_search_requests, content_pages=exa_content_pages)
    combined = round(float(xai_cost.get("approx_total_usd", 0)) + float(exa_cost.get("approx_total_usd", 0)), 6)

    enriched_at = datetime.now(timezone.utc).isoformat()
    data["linkedin_enrichment"] = {
        "version": 1,
        "enriched_at_utc": enriched_at,
        "model": model,
        "stats": stats,
        "telemetry": {
            "exa": {
                "search_requests": exa_search_requests,
                "content_pages": exa_content_pages,
                "cost_estimate": exa_cost,
            },
            "xai": {
                "usage": merged_xai_usage,
                "cost_estimate": xai_cost,
            },
            "combined_approx_usd": combined,
        },
    }
    data["contacts"] = contacts

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".tmp")
    txt = json.dumps(data, ensure_ascii=False, indent=2 if pretty else None)
    if not pretty:
        txt = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    tmp.write_text(txt, encoding="utf-8")
    tmp.replace(out_path)

    return stats


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--in-json", type=Path, help="Single enriched JSON to process")
    p.add_argument("--out-json", type=Path, help="Output path (default: overwrite in-json)")
    p.add_argument("--jsons-dir", type=Path, default=Path("jsons"), help="Directory of enriched JSONs (with --all)")
    p.add_argument("--all", action="store_true", help="Process all *.enriched.json in --jsons-dir")
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--discover-missing", action="store_true", help="Use Exa people search to find missing LinkedIn URLs")
    p.add_argument("--no-skip-existing", action="store_true", help="Re-enrich contacts that already have linkedin_profile")
    p.add_argument("--pretty", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    load_repo_dotenv(_ROOT)

    exa_key = (os.environ.get("EXA_API_KEY") or "").strip()
    xai_key = (os.environ.get("XAI_API_KEY") or "").strip()
    if not args.dry_run and not exa_key:
        print("Missing EXA_API_KEY.", file=sys.stderr)
        return 1
    if not xai_key and not args.dry_run:
        print("Missing XAI_API_KEY.", file=sys.stderr)
        return 1

    exa_client = None
    if not args.dry_run:
        from exa_py import Exa
        exa_client = Exa(api_key=exa_key)

    files: list[tuple[Path, Path]] = []
    if args.all:
        for f in sorted(args.jsons_dir.glob("*.enriched.json")):
            files.append((f, f))
    elif args.in_json:
        out = args.out_json or args.in_json
        files.append((args.in_json, out))
    else:
        print("Provide --in-json or --all.", file=sys.stderr)
        return 1

    total_stats: dict[str, int] = {}
    for in_path, out_path in files:
        print(f"Processing {in_path.name}...")
        stats = _process_one_file(
            in_path,
            out_path,
            exa_client=exa_client,
            xai_api_key=xai_key,
            model=args.model,
            discover_missing=args.discover_missing,
            skip_existing=not args.no_skip_existing,
            pretty=args.pretty,
            dry_run=args.dry_run,
        )
        print(f"  {stats}")
        for k, v in stats.items():
            total_stats[k] = total_stats.get(k, 0) + v

    print(f"\nTotal: {total_stats}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
