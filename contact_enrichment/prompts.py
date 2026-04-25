from __future__ import annotations

import json
from typing import Any

from hotel_decision_maker_research import Contact


def build_system_prompt() -> str:
    return """You are a B2B contact-channel researcher for hotels and hospitality groups.

Rules:
- Use server-side web_search and x_search aggressively. Prefer official sites, press PDFs, event speaker bios, news, and verified X posts.
- Goal: find PUBLIC business email, phone, X handle, or LinkedIn for the exact person named.
- Never fabricate emails, phones, handles, or URLs. Use null when unknown after thorough search.
- Prefer property or employer-domain email over personal freemail unless clearly their public work address.
- Return ONLY JSON matching the response schema on your final structured turn.
- The match_id in your output MUST exactly equal the provided request match_id string."""


def _contact_brief(c: Contact) -> dict[str, Any]:
    return {
        "full_name": c.full_name,
        "title": c.title,
        "company": c.company,
        "linkedin_url": c.linkedin_url,
        "email": c.email,
        "email2": c.email2,
        "phone": c.phone,
        "phone2": c.phone2,
        "x_handle": c.x_handle,
        "other_contact_detail": c.other_contact_detail,
    }


def build_user_wave_a(target_url: str, contact: Contact, match_id: str) -> str:
    payload = {
        "anchor_hotel_url": target_url,
        "match_id": match_id,
        "person": _contact_brief(contact),
        "task": "Wave A — Discover any public business email, phone, X handle, or canonical LinkedIn. Search broadly (property, brand HQ, management company).",
    }
    return json.dumps(payload, ensure_ascii=False)


def build_user_wave_b(target_url: str, contact: Contact, match_id: str, missing_hint: str) -> str:
    payload = {
        "anchor_hotel_url": target_url,
        "match_id": match_id,
        "person": _contact_brief(contact),
        "task": (
            "Wave B — Fill gaps only. "
            f"Focus on still-missing: {missing_hint}. "
            "Mine PDFs, conference agendas, podcast show notes, press releases, site /team pages."
        ),
    }
    return json.dumps(payload, ensure_ascii=False)


def build_user_wave_c_final(target_url: str, contact: Contact, match_id: str) -> str:
    payload = {
        "anchor_hotel_url": target_url,
        "match_id": match_id,
        "person": _contact_brief(contact),
        "task": (
            "Wave C — FINAL structured output. "
            "Emit JSON matching the schema with match_id exactly as given. "
            "source_urls: up to 8 URLs you relied on (no long quotes). "
            "status: ok|partial|not_found|error. notes: one short sentence if needed."
        ),
    }
    return json.dumps(payload, ensure_ascii=False)


def missing_fields_hint(contact: Contact) -> str:
    parts: list[str] = []
    if not (contact.email and str(contact.email).strip()):
        parts.append("email")
    if not (contact.phone and str(contact.phone).strip()):
        parts.append("phone")
    if not (contact.x_handle and str(contact.x_handle).strip()):
        parts.append("x_handle")
    if not (contact.linkedin_url and str(contact.linkedin_url).strip()):
        parts.append("linkedin_url")
    return ", ".join(parts) if parts else "none (verify or upgrade existing values)"
