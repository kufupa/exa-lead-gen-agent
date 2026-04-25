from __future__ import annotations

from urllib.parse import urlparse

from lead_aggregates.urls import canonical_hotel_url


def _expand_www_variants(netlocs: set[str]) -> frozenset[str]:
    """Also match apex vs www when data and filter disagree."""
    s = set(netlocs)
    for n in list(netlocs):
        if n.startswith("www."):
            s.add(n[4:])
        else:
            s.add("www." + n)
    return frozenset(s)


def netlocs_from_hotel_urls(urls: list[str]) -> frozenset[str]:
    """Lowercased netlocs from canonicalised hotel URLs (host-only match across paths)."""
    out: set[str] = set()
    for u in urls:
        s = (u or "").strip()
        if not s:
            continue
        out.add(urlparse(canonical_hotel_url(s)).netloc.lower())
    return _expand_www_variants(out)


def row_hotel_netloc(row: dict) -> str:
    return urlparse((row.get("hotel_canonical_url") or "").strip()).netloc.lower()


def row_matches_hotel_netlocs(row: dict, netlocs: frozenset[str]) -> bool:
    if not netlocs:
        return True
    return row_hotel_netloc(row) in netlocs
