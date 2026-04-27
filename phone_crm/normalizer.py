from __future__ import annotations

import hashlib
import json
from typing import Any

from urllib.parse import urlparse


def text(value: Any) -> str:
    return value.strip() if isinstance(value, str) else ""


def first_nonempty(*values: Any) -> str:
    for value in values:
        candidate = text(value)
        if candidate:
            return candidate
    return ""


def stable_json_hash(value: Any) -> str:
    raw = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _normalize_hotel_name(target_url: str, contact: dict[str, Any]) -> str:
    company = text(contact.get("company"))
    if company:
        return company
    raw_target = text(target_url)
    if raw_target:
        try:
            return urlparse(raw_target).hostname or raw_target
        except Exception:
            return raw_target
    return "Unknown hotel"


def normalize_contact_row(row: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(row, dict):
        raise ValueError("Each contact row must be an object")

    occurrence_id = text(row.get("occurrence_id"))
    if not occurrence_id:
        raise ValueError("Missing occurrence_id")

    contact = row.get("contact")
    if not isinstance(contact, dict):
        contact = {}

    target_url = text(row.get("target_url"))
    hotel_name = _normalize_hotel_name(target_url, contact)

    phone = text(contact.get("phone"))
    phone2 = text(contact.get("phone2"))
    email = text(contact.get("email"))
    email2 = text(contact.get("email2"))
    other = text(contact.get("other_contact_detail"))
    x_handle = text(contact.get("x_handle"))
    linkedin = text(contact.get("linkedin_url"))

    primary_handle = first_nonempty(
        phone,
        phone2,
        email,
        email2,
        other,
        linkedin,
        x_handle,
    )

    return {
        "occurrence_id": occurrence_id,
        "source_enriched_json": text(row.get("source_enriched_json")),
        "target_url": target_url,
        "hotel_name": hotel_name,
        "full_name": first_nonempty(contact.get("full_name"), "Unknown contact"),
        "title": text(contact.get("title")),
        "primary_handle": primary_handle,
        "phone": phone,
        "phone2": phone2,
        "email": email,
        "email2": email2,
        "linkedin_url": linkedin,
        "x_handle": x_handle,
        "other_contact_detail": other,
        "decision_maker_score": text(contact.get("decision_maker_score")),
        "intimacy_grade": text(contact.get("intimacy_grade")),
        "has_phone": bool(phone or phone2),
        "has_email": bool(email or email2),
        "has_contact_route": bool(phone or phone2 or email or email2 or other or linkedin or x_handle),
        "payload": row,
        "source_hash": stable_json_hash(row),
    }
