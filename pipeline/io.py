from __future__ import annotations

import csv
import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pipeline.models import (
    CandidateDraft,
    CandidateLead,
    HotelOrg,
    OrgAlias,
    PipelineRunResult,
    PipelineTelemetry,
    PipelineUiJson,
    ReviewRow,
)
from pipeline.telemetry import finalize_exa_cost_on_telemetry


def run_id_for_url(url: str) -> str:
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()[:10]
    slug = re.sub(r"[^a-z0-9]+", "_", url.lower())[:40].strip("_") or "hotel"
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{ts}__{slug}__{h}"


def ensure_run_dir(base: Path, run_id: str) -> Path:
    d = base / run_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def write_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def write_review_csv(path: Path, rows: list[ReviewRow]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(ReviewRow.model_fields.keys())
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for r in rows:
            w.writerow(r.model_dump())


def write_run_artifacts(base_out: Path, run_id: str, result: PipelineRunResult, source_pack_json: str | None) -> Path:
    d = ensure_run_dir(base_out, run_id)
    write_json(d / "pipeline_result.json", result.model_dump())
    write_review_csv(d / "review_board.csv", result.review_rows)
    write_json(d / "telemetry.json", result.telemetry.model_dump())
    if source_pack_json:
        (d / "source_pack.json").write_text(source_pack_json, encoding="utf-8")
    return d


def build_pipeline_ui_json(
    *,
    input_url: str,
    resolved_org: HotelOrg,
    aliases: list[OrgAlias],
    candidates: list[CandidateLead],
    rejected_candidates: list[CandidateDraft],
    telemetry: PipelineTelemetry,
    needs_manual_org_review: bool = False,
) -> PipelineUiJson:
    """Assemble UI JSON with provider costs and quality_metrics."""
    exa_est = finalize_exa_cost_on_telemetry(telemetry)
    exa_usd = float(exa_est.get("approx_total_usd", 0.0))
    xai_usd = sum(float(s.estimated_usd) for s in telemetry.stages if s.provider == "xai")
    tier1 = sum(1 for c in candidates if c.role_tier == 1)
    tier2 = sum(1 for c in candidates if c.role_tier == 2)
    useful = sum(1 for c in candidates if c.role_tier in (1, 2))
    direct_routes = sum(
        1
        for c in candidates
        for r in c.contact_routes
        if r.kind in ("email", "phone") and r.confidence in ("high", "medium")
    )
    dept_routes = sum(
        1 for c in candidates for r in c.contact_routes if r.kind in ("generic_email", "switchboard", "contact_form")
    )
    return PipelineUiJson(
        input_url=input_url,
        resolved_org=resolved_org,
        aliases=list(aliases),
        candidates=list(candidates),
        rejected_candidates=list(rejected_candidates),
        provider_costs={
            "xai_usd": round(xai_usd, 6),
            "exa_usd": round(exa_usd, 6),
            "total_usd": round(xai_usd + exa_usd, 6),
        },
        quality_metrics={
            "candidate_count": len(candidates),
            "useful_candidate_count": useful,
            "tier1_count": tier1,
            "tier2_count": tier2,
            "direct_route_count": direct_routes,
            "department_route_count": dept_routes,
            "rejected_count": len(rejected_candidates),
            "exa_searches": telemetry.exa_search_requests,
            "exa_fetches": telemetry.exa_content_pages,
            "xai_calls": sum(s.calls for s in telemetry.stages if s.provider == "xai"),
        },
        telemetry=telemetry,
        needs_manual_org_review=needs_manual_org_review,
    )


def write_pipeline_ui_artifact(base_out: Path, run_id: str, ui: PipelineUiJson) -> Path:
    d = ensure_run_dir(base_out, run_id)
    write_json(d / "pipeline_result.json", ui.model_dump())
    write_json(d / "telemetry.json", ui.telemetry.model_dump())
    return d
