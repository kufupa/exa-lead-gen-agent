from __future__ import annotations

import hashlib
from typing import Any

from hotel_decision_maker_research import is_generic_functional_email
from lead_aggregates.urls import canonical_hotel_url


def primary_delivery_email(contact_row: dict[str, Any]) -> str | None:
    """First non-empty, non-generic email from email then email2 (lowercased)."""
    for key in ("email", "email2"):
        raw = (contact_row.get(key) or "").strip()
        if not raw:
            continue
        if is_generic_functional_email(raw):
            continue
        return raw.lower()
    return None


def target_url_from_intimate_row(contact_row: dict[str, Any]) -> str:
    top_level = (contact_row.get("target_url") or "").strip()
    if top_level:
        return top_level
    p1 = contact_row.get("phase1_research")
    if isinstance(p1, dict):
        return (p1.get("target_url") or "").strip()
    source_run = contact_row.get("source_run")
    if isinstance(source_run, dict):
        return (source_run.get("target_url") or "").strip()
    return ""


def compute_outreach_id(primary_email_lower: str, target_url: str) -> str:
    """Stable id per (normalized email + canonical hotel URL)."""
    canon = canonical_hotel_url(target_url) if target_url.strip() else ""
    payload = f"{primary_email_lower}\x1f{canon}"
    h = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"oh_{h}"
