from __future__ import annotations

from typing import Any


def _row_hotels(row: dict[str, Any]) -> list[str]:
    hotels: list[str] = []
    primary = (row.get("hotel_canonical_url") or "").strip()
    if primary:
        hotels.append(primary)
    related = row.get("related_hotel_canonical_urls")
    if isinstance(related, list):
        hotels.extend(str(h).strip() for h in related if str(h).strip())
    return sorted(set(hotels))


def rebuild_by_hotel_index(by_id: dict[str, Any]) -> dict[str, list[str]]:
    by_hotel: dict[str, list[str]] = {}
    for oid, row in by_id.items():
        if not isinstance(row, dict):
            continue
        for hotel in _row_hotels(row):
            by_hotel.setdefault(hotel, []).append(oid)
    for hotel in by_hotel:
        by_hotel[hotel] = sorted(set(by_hotel[hotel]))
    return by_hotel


def rebuild_indexes(by_id: dict[str, Any]) -> dict[str, Any]:
    return {"by_hotel": rebuild_by_hotel_index(by_id)}
