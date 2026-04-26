from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Callable

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
class BatchJob:
    contact: Contact


def _build_chat_for_batch(
    client: Any,
    *,
    model: str,
    max_turns: int,
    target_url: str,
    job: BatchJob,
    batch_request_id: str,
) -> Any:
    from xai_sdk.chat import system, user
    from xai_sdk.tools import web_search, x_search

    chat = client.chat.create(
        model=model,
        batch_request_id=batch_request_id,
        tools=[web_search(), x_search()],
        max_turns=max_turns,
        store_messages=True,
        response_format=ChannelResearchRow,
    )
    c = job.contact
    chat.append(system(build_system_prompt()))
    chat.append(user(build_user_wave_a(target_url, c, batch_request_id)))
    chat.append(user(build_user_wave_b(target_url, c, batch_request_id, missing_fields_hint(c))))
    chat.append(user(build_user_wave_c_final(target_url, c, batch_request_id)))
    return chat


def _parse_batch_chat_result(result: Any) -> tuple[str, ChannelResearchRow | None, str | None]:
    """Returns (request_id, row, error)."""
    rid = getattr(result, "batch_request_id", "") or ""
    try:
        content = (result.response.content or "").strip()
    except Exception as e:
        return rid, None, f"no response: {e}"
    if not content:
        return rid, None, "empty content"
    try:
        row = ChannelResearchRow.model_validate_json(content)
    except Exception as e:
        return rid, None, f"json: {e}"
    if row.match_id != rid:
        return rid, None, f"match_id mismatch got={row.match_id!r} want={rid!r}"
    return rid, row, None


def submit_and_drain_batch(
    jobs: list[BatchJob],
    *,
    target_url: str,
    model: str,
    max_turns: int,
    batch_name: str,
    add_chunk_size: int,
    on_page: Callable[[dict[str, ChannelResearchRow], list[dict[str, str]]], None] | None = None,
    poll_interval_sec: float = 5.0,
    max_wait_sec: float | None = None,
) -> tuple[dict[str, ChannelResearchRow], list[dict[str, str]], str]:
    """
    Submit all jobs to xAI Batch API, poll until complete, merge all result pages.
    on_page is called after each results page with (delta_rows, delta_failures).
    Returns (rows_by_request_id, failures, batch_id).
    """
    try:
        from xai_sdk import Client
    except ImportError as e:  # pragma: no cover
        raise SystemExit(f"xai_sdk import failed: {e}") from e

    api_key = os.environ.get("XAI_API_KEY", "")
    if not api_key:
        raise SystemExit("Missing XAI_API_KEY")

    client = Client(api_key=api_key)
    batch = client.batch.create(batch_name=batch_name)
    batch_id = batch.batch_id

    batch_requests: list[Any] = []
    for job in jobs:
        rid = request_id(job.contact)
        batch_requests.append(
            _build_chat_for_batch(
                client,
                model=model,
                max_turns=max_turns,
                target_url=target_url,
                job=job,
                batch_request_id=rid,
            )
        )

    for i in range(0, len(batch_requests), add_chunk_size):
        chunk = batch_requests[i : i + add_chunk_size]
        client.batch.add(batch_id=batch_id, batch_requests=chunk)

    rows: dict[str, ChannelResearchRow] = {}
    failures: list[dict[str, str]] = []
    failure_rids: set[str] = set()
    poll_started = time.monotonic()

    while True:
        b = client.batch.get(batch_id=batch_id)
        pending = b.state.num_pending
        if max_wait_sec is not None and max_wait_sec > 0 and (time.monotonic() - poll_started) > max_wait_sec:
            failures.append(
                {
                    "request_id": "__batch__",
                    "error": f"xAI batch polling exceeded max_wait_sec={max_wait_sec} (batch_id={batch_id}, pending={pending})",
                }
            )
            break

        pagination_token = None
        while True:
            page = client.batch.list_batch_results(
                batch_id=batch_id,
                limit=100,
                pagination_token=pagination_token,
            )
            delta: dict[str, ChannelResearchRow] = {}
            delta_fail: list[dict[str, str]] = []
            for item in page.succeeded:
                rid, row, err = _parse_batch_chat_result(item)
                if err or row is None:
                    rid_key = rid or "unknown"
                    if rid_key not in failure_rids:
                        failure_rids.add(rid_key)
                        rec = {"request_id": rid_key, "error": err or "unknown"}
                        delta_fail.append(rec)
                        failures.append(rec)
                elif rid not in rows:
                    delta[rid] = row
                    rows[rid] = row
            for item in page.failed:
                rid = item.batch_request_id
                if rid in failure_rids:
                    continue
                failure_rids.add(rid)
                msg = item.error_message or "batch item failed"
                rec = {"request_id": rid, "error": msg}
                delta_fail.append(rec)
                failures.append(rec)

            if on_page and (delta or delta_fail):
                on_page(delta, delta_fail)

            if page.pagination_token is None:
                break
            pagination_token = page.pagination_token

        if pending == 0:
            break
        time.sleep(poll_interval_sec)

    return rows, failures, batch_id
