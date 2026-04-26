from __future__ import annotations

import time
from typing import Any, Protocol
from urllib.parse import urlparse

from pipeline.candidates import (
    candidate_from_linkedin_source,
    domain_from_url,
    initial_hotel_from_url,
)
from pipeline.config import PipelineConfig
from pipeline.models import CandidateLead, HotelOrg, SourceRef
from pipeline.telemetry import record_exa_stage


class ExaSearchItem(Protocol):
    url: str
    title: str | None
    text: str | None


class ExaSearchResult(Protocol):
    results: list[Any]


class ExaClientProtocol(Protocol):
    def search(self, query: str, num_results: int = 10, **kwargs: Any) -> Any: ...

    def get_contents(self, urls: list[str], **kwargs: Any) -> Any: ...


def _result_items(result: Any) -> list[Any]:
    return list(getattr(result, "results", []) or [])


def _item_to_source(item: Any, query: str) -> SourceRef:
    url = str(getattr(item, "url", "") or "").strip()
    title = getattr(item, "title", None)
    if title is not None:
        title = str(title).strip() or None
    snippet = getattr(item, "text", None) or getattr(item, "snippet", None)
    if snippet is not None:
        snippet = str(snippet).strip() or None
    score = getattr(item, "score", None)
    if isinstance(score, (int, float)):
        sc: float | None = float(score)
    else:
        sc = None
    return SourceRef(url=url, title=title, snippet=snippet, query=query, score=sc)


def _search_with_optional_category(
    client: ExaClientProtocol,
    query: str,
    *,
    num_results: int,
    category: str | None,
) -> Any:
    try:
        if category:
            return client.search(query, num_results=num_results, category=category)
    except TypeError:
        pass
    return client.search(query, num_results=num_results)


def org_resolution_queries(hotel: HotelOrg) -> list[str]:
    name = hotel.property_name or hotel.canonical_name or ""
    dom = hotel.domains[0] if hotel.domains else domain_from_url(hotel.input_url)
    q: list[str] = []
    if name:
        q.append(f'"{name}" hotel ownership')
        q.append(f'"{name}" "managed by"')
        q.append(f'"{name}" "owned by"')
        q.append(f'"{name}" "part of" hotel group')
    if dom:
        q.append(f"site:{dom} leadership team")
        q.append(f"site:{dom} contact")
    q.append(f'"{dom}" hotel management company')
    return q[:8]


def role_discovery_queries(hotel: HotelOrg) -> list[str]:
    name = hotel.property_name or hotel.canonical_name or dom_hint(hotel)
    if not name:
        name = dom_hint(hotel)
    queries = [
        f'"{name}" "general manager"',
        f'"{name}" "managing director"',
        f'"{name}" owner OR founder',
        f'"{name}" CEO',
        f'"{name}" "commercial director"',
        f'"{name}" "revenue manager" OR "revenue director"',
        f'"{name}" "director of sales"',
        f'"{name}" "groups" sales',
        f'"{name}" "reservations manager"',
        f'"{name}" "IT manager" OR "digital director"',
        f'"{name}" procurement OR "finance director"',
        f'"{name}" appointed "general manager"',
        f'"{name}" site:linkedin.com/in',
    ]
    return queries


def dom_hint(hotel: HotelOrg) -> str:
    if hotel.domains:
        return hotel.domains[0]
    return domain_from_url(hotel.input_url)


def discover(
    hotel_url: str,
    config: PipelineConfig,
    exa_client: ExaClientProtocol | None,
    telemetry: Any,
) -> tuple[HotelOrg, list[SourceRef], list[CandidateLead]]:
    """
    Run Exa searches until caps; return hotel org (best-effort), flat sources, LinkedIn-derived candidates.
    """
    hotel = initial_hotel_from_url(hotel_url)
    parsed = urlparse(hotel.input_url)
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    hotel.property_name = host.split(".")[0].replace("-", " ").title() if host else None

    all_sources: list[SourceRef] = []
    candidates: list[CandidateLead] = []

    if exa_client is None:
        return hotel, all_sources, candidates

    search_cap = config.exa_search_cap()
    searches_done = 0

    queries: list[str] = []
    queries.extend(org_resolution_queries(hotel))
    queries.extend(role_discovery_queries(hotel))

    for q in queries:
        if searches_done >= search_cap:
            break
        t_search = time.perf_counter()
        res = _search_with_optional_category(exa_client, q, num_results=6, category=None)
        searches_done += 1
        for it in _result_items(res):
            src = _item_to_source(it, q)
            if src.url:
                all_sources.append(src)
                cand = candidate_from_linkedin_source(src, hotel)
                if cand:
                    candidates.append(cand)
        record_exa_stage(
            telemetry,
            stage=f"exa_search:{q[:40]}",
            search_delta=1,
            fetch_delta=0,
            seconds=time.perf_counter() - t_search,
        )

    people_q = role_people_queries(hotel)
    for q in people_q:
        if searches_done >= search_cap:
            break
        t_search = time.perf_counter()
        res = _search_with_optional_category(exa_client, q, num_results=5, category="people")
        searches_done += 1
        for it in _result_items(res):
            src = _item_to_source(it, q)
            if src.url:
                all_sources.append(src)
                cand = candidate_from_linkedin_source(src, hotel)
                if cand:
                    candidates.append(cand)
        record_exa_stage(
            telemetry,
            stage=f"exa_people:{q[:36]}",
            search_delta=1,
            fetch_delta=0,
            seconds=time.perf_counter() - t_search,
        )

    return hotel, all_sources, candidates


def role_people_queries(hotel: HotelOrg) -> list[str]:
    name = hotel.property_name or hotel.canonical_name or dom_hint(hotel)
    return [
        f"{name} general manager",
        f"{name} director of sales",
        f"{name} revenue director",
    ]


def orphan_sources_for_pack(all_sources: list[SourceRef], candidates: list[CandidateLead]) -> list[SourceRef]:
    attached: set[str] = set()
    for c in candidates:
        for s in c.evidence:
            if s.url:
                attached.add(s.url)
    return [s for s in all_sources if s.url and s.url not in attached]
