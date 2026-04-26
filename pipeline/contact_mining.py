from __future__ import annotations

import re
import time
from typing import Any

from pipeline.config import PipelineConfig
from pipeline.exa_discovery import ExaClientProtocol, _item_to_source, _result_items, _search_with_optional_category
from pipeline.models import CandidateLead, ContactRoute, HotelOrg
from pipeline.telemetry import record_exa_stage, record_xai_stage

EMAIL_IN_TEXT = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
PHONE_IN_TEXT = re.compile(r"\+?\d[\d\s().-]{8,}\d")


def _eligible(c: CandidateLead) -> bool:
    if c.role_tier == 4:
        return False
    if c.role_tier in (1, 2):
        return bool(c.needs_contact_mining)
    if c.role_tier == 3 and c.role_family in ("sales_events", "reservations"):
        return bool(c.needs_contact_mining)
    return False


def _has_strong_contact(c: CandidateLead) -> bool:
    for r in c.contact_routes:
        if r.kind in ("email", "phone") and r.confidence == "high":
            return True
    return False


def _extract_routes_from_text(text: str, source_url: str) -> list[ContactRoute]:
    routes: list[ContactRoute] = []
    for m in EMAIL_IN_TEXT.finditer(text or ""):
        addr = m.group(0)
        routes.append(
            ContactRoute(
                kind="email",
                value=addr,
                confidence="medium",
                source_url=source_url,
                rationale="Found in page text",
            )
        )
    for m in PHONE_IN_TEXT.finditer(text or ""):
        routes.append(
            ContactRoute(
                kind="phone",
                value=m.group(0).strip(),
                confidence="medium",
                source_url=source_url,
                rationale="Found in page text",
            )
        )
    return routes[:5]


def mine_contacts(
    hotel: HotelOrg,
    candidates: list[CandidateLead],
    config: PipelineConfig,
    exa_client: ExaClientProtocol | None,
    xai_api_key: str | None,
    telemetry: Any,
) -> list[CandidateLead]:
    if config.skip_contact_mining:
        return candidates
    name_hint = hotel.property_name or hotel.canonical_name or ""
    updated: list[CandidateLead] = []
    for c in candidates:
        if not _eligible(c) or _has_strong_contact(c):
            updated.append(c)
            continue
        if exa_client is None:
            updated.append(c)
            continue
        t0 = time.perf_counter()
        q = f'"{c.full_name}" {name_hint} email OR phone'
        res = _search_with_optional_category(exa_client, q, num_results=5, category=None)
        new_routes: list[ContactRoute] = list(c.contact_routes)
        for it in _result_items(res):
            src = _item_to_source(it, q)
            blob = (src.snippet or "") + "\n" + (src.fetched_text or "")
            for r in _extract_routes_from_text(blob, src.url):
                new_routes.append(r)
        record_exa_stage(
            telemetry,
            stage=f"contact_mine:{c.full_name[:20]}",
            search_delta=1,
            fetch_delta=0,
            seconds=time.perf_counter() - t0,
        )
        still_need = not any(x.kind in ("email", "phone") for x in new_routes)
        updated.append(
            c.model_copy(
                update={
                    "contact_routes": new_routes,
                    "needs_contact_mining": still_need and c.role_tier in (1, 2, 3),
                }
            )
        )

    if config.use_xai_for_contact_mining and (xai_api_key or "").strip() and exa_client is not None:
        updated = _xai_contact_pass(hotel, updated, config, (xai_api_key or "").strip(), telemetry)
    return updated


def _dedupe_routes(routes: list[ContactRoute]) -> list[ContactRoute]:
    seen: set[tuple[str, str]] = set()
    out: list[ContactRoute] = []
    for r in routes:
        k = (r.kind, (r.value or "").strip().lower())
        if not k[1] or k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def mine_contacts_v4(
    hotel: HotelOrg,
    candidates: list[CandidateLead],
    config: PipelineConfig,
    exa_client: ExaClientProtocol | None,
    telemetry: Any,
) -> list[CandidateLead]:
    """Extract routes from evidence text; optional capped hotel-level site: Exa queries (no per-person search)."""
    if config.skip_contact_mining:
        return candidates

    updated: list[CandidateLead] = []
    for c in candidates:
        if (c.relationship_confidence or "") == "reject":
            updated.append(c)
            continue
        routes = list(c.contact_routes)
        for ev in c.evidence:
            blob = (ev.fetched_text or "") + "\n" + (ev.snippet or "")
            routes.extend(_extract_routes_from_text(blob, ev.url or ""))
        updated.append(c.model_copy(update={"contact_routes": _dedupe_routes(routes)}))

    dom = hotel.domains[0] if hotel.domains else ""
    if not dom or exa_client is None:
        return updated

    cap = config.max_contact_route_exa_searches
    queries = [
        f'site:{dom} "sales" "email"',
        f'site:{dom} "events" "email"',
        f'site:{dom} "reservations" "phone"',
        f"site:{dom} contact",
    ][:cap]

    shared_routes: list[ContactRoute] = []
    for q in queries:
        t0 = time.perf_counter()
        res = _search_with_optional_category(exa_client, q, num_results=5, category=None)
        record_exa_stage(
            telemetry,
            stage="contact_route_site",
            search_delta=1,
            fetch_delta=0,
            seconds=time.perf_counter() - t0,
        )
        for it in _result_items(res):
            src = _item_to_source(it, q)
            blob = (src.snippet or "") + "\n" + (src.fetched_text or "")
            shared_routes.extend(_extract_routes_from_text(blob, src.url or ""))
    shared_routes = _dedupe_routes(shared_routes)

    if not shared_routes:
        return updated

    out2: list[CandidateLead] = []
    for c in updated:
        if c.role_tier == 4 or (c.relationship_confidence or "") == "low":
            out2.append(c)
            continue
        if c.role_tier in (1, 2) or (
            c.role_tier == 3 and c.role_family in ("sales_events", "reservations")
        ):
            merged = _dedupe_routes(list(c.contact_routes) + shared_routes)
            out2.append(c.model_copy(update={"contact_routes": merged, "needs_contact_mining": False}))
        else:
            out2.append(c)
    return out2


def _xai_contact_pass(
    hotel: HotelOrg,
    candidates: list[CandidateLead],
    config: PipelineConfig,
    api_key: str,
    telemetry: Any,
) -> list[CandidateLead]:
    """Optional single batched xAI call for Tier1/2 still missing email/phone."""
    from pydantic import BaseModel, Field

    from pipeline.grok_validation import _usage_to_dict

    class ContactPassRow(BaseModel):
        candidate_id: str
        new_routes: list[ContactRoute] = Field(default_factory=list)

    class ContactPassOut(BaseModel):
        rows: list[ContactPassRow] = Field(default_factory=list)

    needy = [c for c in candidates if c.role_tier in (1, 2) and not any(r.kind in ("email", "phone") for r in c.contact_routes)]
    if not needy:
        return candidates

    import json

    from xai_sdk import Client
    from xai_sdk.chat import user

    payload = [{"candidate_id": c.candidate_id, "name": c.full_name, "title": c.title, "evidence_urls": [e.url for e in c.evidence]} for c in needy[:15]]
    prompt = (
        "Using ONLY plausible public patterns from the evidence URLs provided, suggest contact routes. "
        "Do not invent verified emails; use kind=pattern with low confidence if guessing. "
        "Return JSON ContactPassOut.\n"
        + json.dumps(payload, ensure_ascii=False)
    )
    t0 = time.perf_counter()
    client = Client(api_key=api_key)
    chat = client.chat.create(
        model=config.grok_validation_model,
        max_turns=4,
        store_messages=True,
        response_format=ContactPassOut,
    )
    chat.append(user(prompt))
    final = chat.sample()
    raw = (final.content or "").strip()
    usage = _usage_to_dict(getattr(final, "usage", None))
    record_xai_stage(telemetry, stage="xai_contact_mining", usages=[usage], seconds=time.perf_counter() - t0)
    try:
        out = ContactPassOut.model_validate_json(raw)
    except Exception:
        return candidates
    by_id = {c.candidate_id: c for c in candidates}
    for row in out.rows:
        c = by_id.get(row.candidate_id)
        if not c:
            continue
        merged = list(c.contact_routes)
        merged.extend(row.new_routes)
        by_id[row.candidate_id] = c.model_copy(update={"contact_routes": merged, "needs_contact_mining": False})
    return [by_id[c.candidate_id] for c in candidates]
