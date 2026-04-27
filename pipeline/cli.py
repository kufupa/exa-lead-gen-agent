from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from scripts._repo_dotenv import load_repo_dotenv

from pipeline.candidates import (
    dedupe_candidates,
    initial_hotel_from_url,
    leads_from_people_gap_sources,
    promote_discovery_to_candidates,
)
from pipeline.config import PipelineConfig
from pipeline.contact_mining import mine_contacts_v4
from pipeline.exa_verify import run_exa_jobs
from pipeline.gap_planner import plan_exa_jobs
from pipeline.grok_discovery import grok_discovery_dry_run_plan, run_grok_discovery
from pipeline.io import (
    build_pipeline_ui_json,
    run_id_for_url,
    write_pipeline_ui_artifact,
)
from pipeline.legacy_export import persist_pipeline_ui
from pipeline.models import GrokDiscoveryResult, PipelineRunResult
from pipeline.review_board import build_review_rows
from pipeline.telemetry import new_telemetry


def _load_exa_client():
    key = (os.environ.get("EXA_API_KEY") or "").strip()
    if not key:
        return None
    from exa_py import Exa

    return Exa(api_key=key)


def run_pipeline(
    hotel_url: str,
    config: PipelineConfig,
    *,
    out_dir: Path,
    jsons_dir: Path = Path("jsons"),
    fulljsons_dir: Path = Path("fullJSONs"),
    aggregate_sync: bool = True,
    dry_run: bool = False,
) -> PipelineRunResult:
    load_repo_dotenv(Path(__file__).resolve().parent.parent)
    tel = new_telemetry()

    if dry_run:
        hotel = initial_hotel_from_url(hotel_url)
        discovery = GrokDiscoveryResult(hotel=hotel, aliases=[], drafts=[])
        jobs, manual = plan_exa_jobs(
            discovery,
            max_jobs=config.exa_search_cap(),
            max_people_gap_searches=config.max_people_gap_searches,
            max_person_verify_searches=config.max_person_verify_searches,
        )
        plan = {
            **grok_discovery_dry_run_plan(hotel_url),
            "gap_jobs": [j.model_dump() for j in jobs],
            "needs_manual_org_review": manual,
        }
        return PipelineRunResult(
            hotel=hotel,
            candidates=[],
            review_rows=[],
            telemetry=tel,
            source_pack_json=json.dumps(plan, indent=2),
        )

    xai_key = (os.environ.get("XAI_API_KEY") or "").strip()
    if not xai_key:
        raise ValueError("XAI_API_KEY is required for pipeline v4 (Grok discovery)")

    discovery, _usages = run_grok_discovery(hotel_url, xai_key, tel)
    jobs, manual_review = plan_exa_jobs(
        discovery,
        max_jobs=config.exa_search_cap(),
        max_people_gap_searches=config.max_people_gap_searches,
        max_person_verify_searches=config.max_person_verify_searches,
    )

    exa = _load_exa_client()
    exa_by: dict[str, list] = {}
    if exa is not None and not manual_review and jobs:
        exa_by = run_exa_jobs(
            jobs,
            exa,
            tel,
            max_searches=config.exa_search_cap(),
            max_fetches=config.exa_fetch_cap(),
        )

    rough, rejected_drafts = promote_discovery_to_candidates(discovery, exa_by)
    global_src = exa_by.get("_global") or []
    if global_src:
        rough.extend(leads_from_people_gap_sources(discovery.hotel, list(discovery.aliases), global_src))
    rough = dedupe_candidates(rough[: config.max_candidates])

    mined = mine_contacts_v4(discovery.hotel, rough, config, exa, tel)
    rows = build_review_rows(discovery.hotel, mined)

    ui = build_pipeline_ui_json(
        input_url=hotel_url.strip(),
        resolved_org=discovery.hotel,
        aliases=list(discovery.aliases),
        candidates=mined,
        rejected_candidates=rejected_drafts,
        telemetry=tel,
        needs_manual_org_review=manual_review,
    )
    rid = run_id_for_url(hotel_url)
    write_pipeline_ui_artifact(out_dir, rid, ui)
    if aggregate_sync:
        persist_pipeline_ui(ui, jsons_dir=jsons_dir, fulljsons_dir=fulljsons_dir)

    return PipelineRunResult(
        hotel=ui.resolved_org,
        candidates=mined,
        review_rows=rows,
        telemetry=tel,
        source_pack_json=json.dumps(
            {"gap_jobs": [j.model_dump() for j in jobs], "needs_manual_org_review": manual_review},
            ensure_ascii=False,
        ),
    )


def main(argv: list[str] | None = None) -> int:
    load_repo_dotenv(Path(__file__).resolve().parent.parent)
    p = argparse.ArgumentParser(description="Grok-led hotel stakeholder pipeline (v4)")
    sub = p.add_subparsers(dest="cmd", required=True)

    run_p = sub.add_parser("run", help="Run pipeline for one hotel URL")
    run_p.add_argument("url")
    run_p.add_argument("--out", type=Path, default=Path("outputs/pipeline"))
    run_p.add_argument("--max-candidates", type=int, default=50)
    run_p.add_argument("--max-exa-searches", type=int, default=26)
    run_p.add_argument("--max-people-gap-searches", type=int, default=10)
    run_p.add_argument("--max-person-verify-searches", type=int, default=10)
    run_p.add_argument("--max-exa-fetches", type=int, default=14)
    run_p.add_argument("--skip-contact-mining", action="store_true")
    run_p.add_argument("--dry-run", action="store_true")
    run_p.add_argument("--json", action="store_true", help="Print PipelineRunResult JSON to stdout")
    run_p.add_argument("--jsons-dir", type=Path, default=Path("jsons"))
    run_p.add_argument("--fulljsons-dir", type=Path, default=Path("fullJSONs"))
    run_p.add_argument(
        "--no-aggregate-sync",
        action="store_true",
        help="Do not write jsons/*.enriched.json or rebuild fullJSONs",
    )

    many = sub.add_parser("run-many", help="Run for each URL in a text file")
    many.add_argument("urls_file", type=Path)
    many.add_argument("--concurrency", type=int, default=2)
    many.add_argument("--out", type=Path, default=Path("outputs/pipeline"))
    many.add_argument("--dry-run", action="store_true")
    many.add_argument("--jsons-dir", type=Path, default=Path("jsons"))
    many.add_argument("--fulljsons-dir", type=Path, default=Path("fullJSONs"))
    many.add_argument(
        "--no-aggregate-sync",
        action="store_true",
        help="Do not write jsons/*.enriched.json or rebuild fullJSONs",
    )

    args = p.parse_args(argv)

    if args.cmd == "run":
        cfg = PipelineConfig(
            max_candidates=args.max_candidates,
            max_exa_searches=args.max_exa_searches,
            max_exa_fetches=args.max_exa_fetches,
            max_people_gap_searches=args.max_people_gap_searches,
            max_person_verify_searches=args.max_person_verify_searches,
            skip_linkedin=False,
            skip_contact_mining=args.skip_contact_mining,
        )
        try:
            res = run_pipeline(
                args.url,
                cfg,
                out_dir=args.out,
                jsons_dir=args.jsons_dir,
                fulljsons_dir=args.fulljsons_dir,
                aggregate_sync=not args.no_aggregate_sync and not args.dry_run,
                dry_run=args.dry_run,
            )
        except Exception as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if args.json:
            print(res.model_dump_json(indent=2))
        else:
            print(f"candidates={len(res.candidates)} review_rows={len(res.review_rows)}")
            if args.dry_run and res.source_pack_json:
                print(res.source_pack_json)
        return 0

    if args.cmd == "run-many":
        lines = [ln.strip() for ln in args.urls_file.read_text(encoding="utf-8").splitlines() if ln.strip()]
        cfg = PipelineConfig(skip_linkedin=False, skip_contact_mining=False)
        for u in lines:
            print(f"--- {u}")
            try:
                run_pipeline(
                    u,
                    cfg,
                    out_dir=args.out,
                    jsons_dir=args.jsons_dir,
                    fulljsons_dir=args.fulljsons_dir,
                    aggregate_sync=not args.no_aggregate_sync and not args.dry_run,
                    dry_run=args.dry_run,
                )
            except Exception as e:
                print(f"error {u}: {e}", file=sys.stderr)
        return 0

    return 1


if __name__ == "__main__":
    raise SystemExit(main())
