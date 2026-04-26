from __future__ import annotations

import json
import re
from typing import Any

from pipeline.config import PipelineConfig
from pipeline.models import CandidateLead, HotelOrg, SourceRef


def _trim(text: str | None, max_chars: int) -> str:
    if not text:
        return ""
    t = text.strip()
    if len(t) <= max_chars:
        return t
    return t[: max_chars - 3] + "..."


def _norm_key(name: str, title: str | None) -> str:
    n = re.sub(r"\s+", " ", (name or "").strip().lower())
    t = re.sub(r"\s+", " ", (title or "").strip().lower())
    return f"{n}|{t}"


def build_source_pack(
    hotel: HotelOrg,
    candidates: list[CandidateLead],
    orphan_sources: list[SourceRef],
    config: PipelineConfig,
) -> dict[str, Any]:
    """
    Compact JSON-serializable pack for Grok validation.
    Groups evidence by candidate; includes orphan URLs for attribution.
    """
    per_ref = max(800, config.max_source_chars_per_ref // 4)
    groups: list[dict[str, Any]] = []
    for c in candidates[: config.source_pack_max_candidates]:
        sources: list[dict[str, Any]] = []
        for s in c.evidence:
            body = _trim(s.snippet, per_ref) or _trim(s.fetched_text, per_ref)
            sources.append(
                {
                    "url": s.url,
                    "title": s.title,
                    "snippet": body,
                    "date": s.published_date,
                    "query": s.query,
                }
            )
        groups.append(
            {
                "candidate_key": _norm_key(c.full_name, c.title),
                "candidate_id": c.candidate_id,
                "name_hints": [c.full_name],
                "title_hints": [t for t in [c.title] if t],
                "company_hints": [c.company] if c.company else [],
                "role_tier_hint": c.role_tier,
                "sources": sources,
                "possible_conflicts": [],
            }
        )

    orphan_block: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for s in orphan_sources:
        u = (s.url or "").strip()
        if not u or u in seen_urls:
            continue
        seen_urls.add(u)
        body = _trim(s.snippet, per_ref) or _trim(s.fetched_text, per_ref)
        orphan_block.append(
            {
                "url": u,
                "title": s.title,
                "snippet": body,
                "date": s.published_date,
                "query": s.query,
            }
        )

    hotel_blob = {
        "input_url": hotel.input_url,
        "canonical_name": hotel.canonical_name,
        "property_name": hotel.property_name,
        "brand_name": hotel.brand_name,
        "management_company": hotel.management_company,
        "ownership_group": hotel.ownership_group,
        "domains": hotel.domains,
        "location_hint": hotel.location_hint,
    }

    return {
        "hotel": hotel_blob,
        "candidate_groups": groups,
        "orphan_sources": orphan_block[:200],
    }


def source_pack_to_json(pack: dict[str, Any]) -> str:
    return json.dumps(pack, ensure_ascii=False, indent=2)
