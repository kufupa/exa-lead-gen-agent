from __future__ import annotations

from typing import Any


def rebuild_by_hotel_index(by_id: dict[str, Any]) -> dict[str, list[str]]:
    by_hotel: dict[str, list[str]] = {}
    for oid, row in by_id.items():
        if not isinstance(row, dict):
            continue
        hotel = (row.get("hotel_canonical_url") or "").strip()
        if not hotel:
            continue
        by_hotel.setdefault(hotel, []).append(oid)
    for hotel in by_hotel:
        by_hotel[hotel] = sorted(set(by_hotel[hotel]))
    return by_hotel


def rebuild_indexes(by_id: dict[str, Any]) -> dict[str, Any]:
    return {"by_hotel": rebuild_by_hotel_index(by_id)}
