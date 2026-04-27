from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lead_aggregates.atomic import atomic_write_json
from lead_aggregates.store import AggregatesStore
from lead_aggregates.urls import canonical_hotel_url
from legacy.hotel_decision_maker_research import default_json_path_from_url
from pipeline.io import build_pipeline_ui_json
from pipeline.models import (
    CandidateLead,
    ContactRoute,
    PipelineRunResult,
    PipelineUiJson,
    SourceRef,
)

_DIRECT_ROUTE_ORDER = {"high": 0, "medium": 1, "low": 2}
_OTHER_ROUTE_KINDS = {"generic_email", "switchboard", "contact_form", "pattern", "unknown"}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _resolved_hotel_name(ui: PipelineUiJson) -> str:
    hotel = ui.resolved_org
    return hotel.canonical_name or hotel.property_name or (hotel.domains[0] if hotel.domains else ui.input_url)


def _routes_by_kind(candidate: CandidateLead, kind: str) -> list[ContactRoute]:
    routes = [r for r in candidate.contact_routes if r.kind == kind and (r.value or "").strip()]
    return sorted(
        routes,
        key=lambda r: (_DIRECT_ROUTE_ORDER.get(r.confidence, 9), (r.value or "").lower()),
    )


def _first_two_values(routes: list[ContactRoute]) -> tuple[str | None, str | None]:
    values: list[str] = []
    seen: set[str] = set()
    for route in routes:
        value = (route.value or "").strip()
        key = value.lower()
        if not value or key in seen:
            continue
        values.append(value)
        seen.add(key)
        if len(values) == 2:
            break
    return (values[0] if values else None, values[1] if len(values) > 1 else None)


def _decision_maker_score(candidate: CandidateLead) -> str:
    if candidate.role_tier in (1, 2):
        return "high"
    if candidate.role_tier == 3:
        return "medium"
    return "low"


def _intimacy_grade(candidate: CandidateLead) -> str:
    if _routes_by_kind(candidate, "email") or _routes_by_kind(candidate, "phone"):
        return "high"
    if any(r.kind in _OTHER_ROUTE_KINDS for r in candidate.contact_routes):
        return "medium"
    return "low"


def _source_type(source: SourceRef) -> str:
    return {
        "hotel_site": "official_site",
        "linkedin": "linkedin",
        "press": "news",
        "directory": "directory",
        "social": "x",
        "company_site": "official_site",
    }.get(source.source_type, "other")


def _quote_or_fact(source: SourceRef) -> str:
    text = source.snippet or source.fetched_text or source.title or source.url
    return text[:1000]


def _legacy_evidence(evidence: list[SourceRef]) -> list[dict[str, Any]]:
    return [
        {
            "source_url": source.url,
            "source_type": _source_type(source),
            "quote_or_fact": _quote_or_fact(source),
        }
        for source in evidence
        if source.url
    ]


def _other_contact_detail(candidate: CandidateLead) -> str | None:
    parts: list[str] = []
    for route in candidate.contact_routes:
        value = (route.value or "").strip()
        if route.kind not in _OTHER_ROUTE_KINDS or not value:
            continue
        parts.append(f"{route.kind}: {value}")
    if candidate.notes:
        parts.extend(candidate.notes)
    return "; ".join(parts) if parts else None


def _evidence_summary(candidate: CandidateLead) -> str:
    bits: list[str] = []
    for source in candidate.evidence[:5]:
        bit = source.snippet or source.title or source.url
        if bit:
            bits.append(bit.replace("\n", " ")[:240])
    bits.extend(candidate.notes[:3])
    return " | ".join(bits)


def _fit_reason(candidate: CandidateLead) -> str:
    if candidate.reason_kept:
        return candidate.reason_kept
    return (
        f"Pipeline v4 kept as tier {candidate.role_tier} / {candidate.role_family}; "
        f"role confidence {candidate.current_role_confidence}."
    )


def _legacy_contact(candidate: CandidateLead, fallback_company: str) -> dict[str, Any]:
    email, email2 = _first_two_values(_routes_by_kind(candidate, "email"))
    phone, phone2 = _first_two_values(_routes_by_kind(candidate, "phone"))
    linkedin = candidate.linkedin_url or next(
        (r.value for r in _routes_by_kind(candidate, "linkedin")),
        None,
    )
    return {
        "full_name": candidate.full_name,
        "title": candidate.title,
        "company": candidate.company or fallback_company,
        "linkedin_url": linkedin,
        "email": email,
        "email2": email2,
        "phone": phone,
        "phone2": phone2,
        "x_handle": None,
        "other_contact_detail": _other_contact_detail(candidate),
        "decision_maker_score": _decision_maker_score(candidate),
        "intimacy_grade": _intimacy_grade(candidate),
        "fit_reason": _fit_reason(candidate),
        "contact_evidence_summary": _evidence_summary(candidate),
        "evidence": _legacy_evidence(candidate.evidence),
    }


def pipeline_ui_to_enriched_doc(
    ui: PipelineUiJson,
    *,
    generated_at_utc: str | None = None,
) -> dict[str, Any]:
    at = generated_at_utc or _now_iso()
    hotel_name = _resolved_hotel_name(ui)
    return {
        "target_url": ui.input_url,
        "generated_at_utc": at,
        "model": "pipeline-v4",
        "agent_count": None,
        "max_turns": None,
        "max_turns_effective": None,
        "min_contacts": None,
        "target_contacts": None,
        "max_contacts": len(ui.candidates),
        "extra_contact_pass": False,
        "strict_evidence": True,
        "allow_linkedin": True,
        "usage": None,
        "contact_enrichment": {
            "version": 4,
            "enriched_at_utc": at,
            "mode": "pipeline-v4",
            "model": "pipeline-v4",
            "concurrency": None,
            "skipped_pre_enrichment": 0,
            "attempted": len(ui.candidates),
            "succeeded": len(ui.candidates),
            "failed": 0,
        },
        "pipeline_v4": ui.model_dump(mode="json"),
        "contacts": [_legacy_contact(candidate, hotel_name) for candidate in ui.candidates],
    }


def enriched_path_for_pipeline_ui(ui: PipelineUiJson, jsons_dir: Path) -> Path:
    default = Path(default_json_path_from_url(canonical_hotel_url(ui.input_url)))
    return jsons_dir / f"{default.stem}.enriched.json"


def write_pipeline_enriched_json(
    ui: PipelineUiJson,
    jsons_dir: Path,
    *,
    generated_at_utc: str | None = None,
) -> Path:
    path = enriched_path_for_pipeline_ui(ui, jsons_dir)
    doc = pipeline_ui_to_enriched_doc(ui, generated_at_utc=generated_at_utc)
    atomic_write_json(path, doc)
    return path


def persist_pipeline_ui(
    ui: PipelineUiJson,
    *,
    jsons_dir: Path,
    fulljsons_dir: Path,
    rebuild_fulljsons: bool = True,
) -> Path:
    path = write_pipeline_enriched_json(ui, jsons_dir)
    if rebuild_fulljsons:
        AggregatesStore(fulljsons_dir, jsons_dir).rebuild_all()
    return path


def load_pipeline_artifact(path: Path) -> PipelineUiJson:
    """Load v4 `pipeline_result.json` — `PipelineUiJson` or older `PipelineRunResult` on disk."""
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, dict) and "resolved_org" in data and "input_url" in data:
        return PipelineUiJson.model_validate(data)
    if isinstance(data, dict) and "hotel" in data and "candidates" in data:
        pr = PipelineRunResult.model_validate(data)
        return build_pipeline_ui_json(
            input_url=(pr.hotel.input_url or "").strip() or (data.get("hotel") or {}).get("input_url", ""),
            resolved_org=pr.hotel,
            aliases=[],
            candidates=pr.candidates,
            rejected_candidates=[],
            telemetry=pr.telemetry,
            needs_manual_org_review=False,
        )
    raise ValueError(
        f"Unrecognized pipeline_result.json schema in {path}: expected PipelineUiJson or PipelineRunResult",
    )


def load_pipeline_ui_json(path: Path) -> PipelineUiJson:
    """Back-compat alias; prefers [`load_pipeline_artifact`]."""
    return load_pipeline_artifact(path)
