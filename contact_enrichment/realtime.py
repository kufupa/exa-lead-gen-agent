from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Callable

from hotel_decision_maker_research import Contact

from contact_enrichment.identity import request_id
from contact_enrichment.prompts import (
    build_system_prompt,
    build_user_wave_a,
    build_user_wave_b,
    build_user_wave_c_final,
    missing_fields_hint,
)
from contact_enrichment.types import ChannelResearchRow


@dataclass
class RealtimeJob:
    contact: Contact


@dataclass
class RealtimeResult:
    request_id: str
    row: ChannelResearchRow | None
    error: str | None = None


def _run_single_contact(
    *,
    api_key: str,
    model: str,
    max_turns: int,
    target_url: str,
    job: RealtimeJob,
) -> RealtimeResult:
    rid = request_id(job.contact)
    try:
        from xai_sdk import Client
        from xai_sdk.chat import system, user
        from xai_sdk.tools import web_search, x_search
    except ImportError as e:  # pragma: no cover
        return RealtimeResult(request_id=rid, row=None, error=f"xai_sdk import failed: {e}")

    client = Client(api_key=api_key)
    chat = client.chat.create(
        model=model,
        tools=[web_search(), x_search()],
        max_turns=max_turns,
        store_messages=True,
        response_format=ChannelResearchRow,
    )
    chat.append(system(build_system_prompt()))
    chat.append(user(build_user_wave_a(target_url, job.contact, rid)))
    _ = chat.sample()
    chat.append(user(build_user_wave_b(target_url, job.contact, rid, missing_fields_hint(job.contact))))
    _ = chat.sample()
    chat.append(user(build_user_wave_c_final(target_url, job.contact, rid)))
    final = chat.sample()
    raw = (final.content or "").strip()
    if not raw:
        return RealtimeResult(request_id=rid, row=None, error="empty model response")
    try:
        row = ChannelResearchRow.model_validate_json(raw)
    except Exception as e:
        return RealtimeResult(request_id=rid, row=None, error=f"json parse: {e}")
    if row.match_id != rid:
        return RealtimeResult(
            request_id=rid,
            row=None,
            error=f"match_id mismatch: got {row.match_id!r} expected {rid!r}",
        )
    return RealtimeResult(request_id=rid, row=row, error=None)


def run_realtime(
    jobs: list[RealtimeJob],
    *,
    target_url: str,
    model: str,
    max_turns: int,
    concurrency: int,
    on_progress: Callable[[int, int], None] | None = None,
) -> tuple[dict[str, ChannelResearchRow], list[dict[str, str]]]:
    """
    Run enrichment jobs in parallel. Returns (rows_by_request_id, failures).
    """
    api_key = os.environ.get("XAI_API_KEY", "")
    if not api_key:
        raise SystemExit("Missing XAI_API_KEY")

    rows: dict[str, ChannelResearchRow] = {}
    failures: list[dict[str, str]] = []
    total = len(jobs)
    done = 0

    workers = max(1, min(concurrency, total or 1))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = {
            ex.submit(
                _run_single_contact,
                api_key=api_key,
                model=model,
                max_turns=max_turns,
                target_url=target_url,
                job=j,
            ): j
            for j in jobs
        }
        for fut in as_completed(futs):
            r = fut.result()
            done += 1
            if on_progress:
                on_progress(done, total)
            if r.error or r.row is None:
                failures.append({"request_id": r.request_id, "error": r.error or "no row"})
            else:
                rows[r.request_id] = r.row
    return rows, failures
