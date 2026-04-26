from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import urlparse
import csv

from lead_aggregates.urls import canonical_hotel_url


def domain_from_website(raw: str) -> str:
    website = (raw or "").strip()
    if not website:
        return ""
    canonical = canonical_hotel_url(website)
    parsed = urlparse(canonical)
    host = (parsed.netloc or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return host


def read_csv_rows(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            rows.append(dict(row))
    return rows


def iter_candidates(csv_path: Path, blocked: frozenset[str]) -> list[dict[str, Any]]:
    blocked_set = set(blocked)
    seen_domains: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in read_csv_rows(csv_path):
        domain = domain_from_website((row.get("website") or "").strip())
        if not domain:
            continue
        if domain in blocked_set:
            continue
        if domain in seen_domains:
            continue
        seen_domains.add(domain)
        candidate = dict(row)
        candidate["domain"] = domain
        out.append(candidate)
    return out

