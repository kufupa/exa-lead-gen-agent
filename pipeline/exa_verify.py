from __future__ import annotations

import time
from typing import Any

from pipeline.exa_discovery import ExaClientProtocol, _item_to_source, _result_items, _search_with_optional_category
from pipeline.models import ExaJob, SourceRef
from pipeline.telemetry import record_exa_stage


def run_exa_jobs(
    jobs: list[ExaJob],
    exa_client: ExaClientProtocol | None,
    telemetry: Any,
    *,
    max_searches: int,
    max_fetches: int,
) -> dict[str, list[SourceRef]]:
    """
    Run only planner-emitted Exa jobs. Attach results by job.candidate_id (draft_id).
    """
    out: dict[str, list[SourceRef]] = {}
    if exa_client is None or not jobs:
        return out

    searches = 0
    fetches = 0

    for job in jobs:
        if searches >= max_searches:
            break
        t0 = time.perf_counter()
        cat = job.category if job.kind in ("profile_lookup", "people_gap") else None
        res = _search_with_optional_category(
            exa_client,
            job.query,
            num_results=job.max_results,
            category=cat,
        )
        searches += 1
        record_exa_stage(
            telemetry,
            stage=f"exa_verify:{job.kind}",
            search_delta=1,
            fetch_delta=0,
            seconds=time.perf_counter() - t0,
            notes=[job.job_id],
        )
        new_sources: list[SourceRef] = []
        for it in _result_items(res):
            url = str(getattr(it, "url", "") or "").strip()
            if not url:
                continue
            new_sources.append(_item_to_source(it, job.query))
        key = job.candidate_id or "_global"
        out.setdefault(key, []).extend(new_sources)

    # Optional shallow content pass for top official URLs per bucket
    for key, lst in list(out.items()):
        for src in list(lst):
            if fetches >= max_fetches:
                break
            u = (src.url or "").lower()
            if not src.url or "linkedin.com" in u:
                continue
            t1 = time.perf_counter()
            try:
                contents = exa_client.get_contents([src.url], text=True)
            except Exception:
                continue
            fetches += 1
            text = ""
            try:
                results = getattr(contents, "results", None) or []
                if results:
                    text = str(getattr(results[0], "text", "") or "")[:8000]
            except Exception:
                text = ""
            record_exa_stage(
                telemetry,
                stage="exa_fetch",
                search_delta=0,
                fetch_delta=1,
                seconds=time.perf_counter() - t1,
                notes=[(src.url or "")[:80]],
            )
            enriched = src.model_copy(update={"fetched_text": text or src.fetched_text})
            rep = [enriched if s.url == src.url else s for s in out[key]]
            out[key] = rep

    return out
