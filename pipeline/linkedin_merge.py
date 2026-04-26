from __future__ import annotations

import time
from typing import Any

from pipeline.candidates import candidate_from_linkedin_source, dedupe_candidates
from pipeline.config import PipelineConfig
from pipeline.exa_discovery import ExaClientProtocol, _item_to_source, _result_items, _search_with_optional_category
from pipeline.models import CandidateLead, HotelOrg, SourceRef
from pipeline.telemetry import record_exa_stage


def merge_linkedin(
    hotel: HotelOrg,
    candidates: list[CandidateLead],
    config: PipelineConfig,
    exa_client: ExaClientProtocol | None,
    telemetry: Any,
) -> list[CandidateLead]:
    if config.skip_linkedin or exa_client is None:
        return candidates

    name_hint = hotel.property_name or hotel.canonical_name or ""
    extra: list[CandidateLead] = []

    queries = [
        f'"{name_hint}" "general manager" site:linkedin.com/in',
        f'"{name_hint}" "director of sales" site:linkedin.com/in',
    ]
    if hotel.management_company:
        queries.append(f'"{hotel.management_company}" "{name_hint}" site:linkedin.com/in')

    pool_sources: list[SourceRef] = []
    for q in queries:
        t0 = time.perf_counter()
        res = _search_with_optional_category(exa_client, q, num_results=4, category="people")
        for it in _result_items(res):
            src = _item_to_source(it, q)
            if src.url:
                pool_sources.append(src)
                cand = candidate_from_linkedin_source(src, hotel)
                if cand:
                    extra.append(cand)
        record_exa_stage(
            telemetry,
            stage=f"linkedin_merge:{q[:32]}",
            search_delta=1,
            fetch_delta=0,
            seconds=time.perf_counter() - t0,
        )

    def linkedin_key(c: CandidateLead) -> str:
        if c.linkedin_url:
            return c.linkedin_url.strip().lower()
        return ""

    by_li: dict[str, CandidateLead] = {linkedin_key(c): c for c in candidates if linkedin_key(c)}

    merged_list = list(candidates)
    for c in extra:
        key = (c.linkedin_url or "").strip().lower()
        if not key:
            continue
        match = by_li.get(key)
        if match:
            new_ev = list(match.evidence)
            seen = {e.url for e in new_ev}
            for e in c.evidence:
                if e.url not in seen:
                    new_ev.append(e)
                    seen.add(e.url)
            idx = next((i for i, x in enumerate(merged_list) if x.candidate_id == match.candidate_id), -1)
            if idx >= 0:
                merged_list[idx] = match.model_copy(
                    update={
                        "evidence": new_ev,
                        "linkedin_url": match.linkedin_url or c.linkedin_url,
                    }
                )
        elif c.role_tier <= 2:
            merged_list.append(c)
            by_li[key] = c

    return dedupe_candidates(merged_list)
