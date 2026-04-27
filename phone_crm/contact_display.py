from __future__ import annotations

from typing import Any

from phone_crm.models import ContactRow


def _coerce_map(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _coerce_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _clean_text(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    return text


def _first_non_empty(*values: Any) -> str:
    for value in values:
        text = _clean_text(value)
        if text:
            return text
    return ""


def _unique_contact_values(values: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for value in values:
        text = _clean_text(value)
        if not text:
            continue
        if text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def payload_remainder(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        key: value
        for key, value in payload.items()
        if key
        not in {
            "evidence",
            "fit_reason",
            "contact_fit_reason",
            "evidence_summary",
            "contact_evidence_summary",
            "decision_maker_score",
            "decision_score",
            "score",
            "intimacy",
            "intimacy_grade",
        }
    }


def normalize_payload_evidence(payload: Any) -> list[dict[str, str]]:
    output: list[dict[str, str]] = []
    for item in _coerce_list(payload):
        if isinstance(item, dict):
            output.append(
                {
                    "source_url": _clean_text(item.get("source_url") or item.get("source") or ""),
                    "quote_or_fact": _clean_text(item.get("quote_or_fact") or item.get("quote") or item.get("fact") or ""),
                }
            )
            continue
        text = _clean_text(item)
        if text:
            output.append({"source_url": "", "quote_or_fact": text})
    return output


def _coerce_payload_node_label(key: Any) -> str:
    return "None" if key is None else str(key)


def _coerce_payload_scalar(value: Any) -> str:
    if value is None:
        return "None"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    return str(value)


def _build_payload_fields(payload: dict[str, Any], max_depth: int = 4) -> list[dict[str, Any]]:
    return [_build_payload_node(key, value, max_depth=max_depth) for key, value in payload.items()]


def _build_payload_node(label: str, value: Any, max_depth: int = 4) -> dict[str, Any]:
    if max_depth <= 0 and isinstance(value, (dict, list)):
        return {
            "label": str(label),
            "value": "[max depth reached]",
            "children": [],
            "kind": "list" if isinstance(value, list) else "object",
        }

    if isinstance(value, dict):
        return {
            "label": str(label),
            "value": "",
            "children": [
                _build_payload_node(_coerce_payload_node_label(child_key), child_value, max_depth=max_depth - 1)
                for child_key, child_value in value.items()
            ],
            "kind": "object",
        }

    if isinstance(value, list):
        return {
            "label": str(label),
            "value": "",
            "children": [
                _build_payload_node(f"[{index}]", child_value, max_depth=max_depth - 1)
                for index, child_value in enumerate(value)
            ],
            "kind": "list",
        }

    return {
        "label": str(label),
        "value": _coerce_payload_scalar(value),
        "children": [],
        "kind": "scalar",
    }


def build_contact_display(contact: ContactRow | None) -> dict[str, Any]:
    if contact is None:
        return {
            "hero_name": "Unknown",
            "hero_title": "",
            "hero_hotel": "Unknown hotel",
            "hotel_url": "",
            "phones": [],
            "emails": [],
            "linkedin_url": "",
            "primary_handle": "—",
            "x_handle": "—",
            "other_contact_detail": "—",
            "tech": [],
            "enrichment": {
                "intimacy": "—",
                "decision_score": "—",
                "fit_reason": "—",
                "evidence_summary": "—",
            },
            "evidence": [],
            "payload": {},
            "payload_contact": {},
            "payload_remainder": {},
            "payload_fields": [],
            "has_phone": False,
            "has_email": False,
            "has_contact_route": False,
            "status": "pending",
            "notes": "",
        }

    payload_root = _coerce_map(contact.payload)
    payload_contact = _coerce_map(payload_root.get("contact"))

    phones = _unique_contact_values([contact.phone, contact.phone2])
    emails = _unique_contact_values([contact.email, contact.email2])
    intimacy = _first_non_empty(contact.intimacy_grade, payload_contact.get("intimacy"), payload_contact.get("intimacy_grade"), "—")
    decision_score = _first_non_empty(
        contact.decision_maker_score,
        payload_contact.get("decision_maker_score"),
        payload_contact.get("decision_score"),
    ) or "—"
    fit_reason = _first_non_empty(payload_contact.get("fit_reason"), payload_contact.get("contact_fit_reason"), "—")
    evidence_summary = _first_non_empty(
        payload_contact.get("evidence_summary"),
        payload_contact.get("contact_evidence_summary"),
        "—",
    )

    return {
        "hero_name": _first_non_empty(contact.full_name, "Unknown"),
        "hero_title": _first_non_empty(contact.title, "—"),
        "hero_hotel": _first_non_empty(contact.hotel_name, "Unknown hotel"),
        "hotel_url": _clean_text(contact.target_url),
        "phones": phones,
        "emails": emails,
        "linkedin_url": _clean_text(contact.linkedin_url),
        "primary_handle": _first_non_empty(contact.primary_handle, "—"),
        "x_handle": _first_non_empty(contact.x_handle, "—"),
        "other_contact_detail": _first_non_empty(contact.other_contact_detail, "—"),
        "tech": [
            ("Contact ID", contact.occurrence_id),
            ("Status", contact.status),
            ("Has phone", "yes" if contact.has_phone else "no"),
            ("Has email", "yes" if contact.has_email else "no"),
            ("Has contact route", "yes" if contact.has_contact_route else "no"),
        ],
        "enrichment": {
            "intimacy": intimacy or "—",
            "decision_score": decision_score,
            "fit_reason": fit_reason,
            "evidence_summary": evidence_summary,
        },
        "evidence": normalize_payload_evidence(payload_contact.get("evidence")),
        "payload": payload_root,
        "payload_contact": payload_contact,
        "payload_remainder": payload_remainder(payload_contact),
        "payload_fields": _build_payload_fields(payload_root),
        "has_phone": bool(contact.has_phone),
        "has_email": bool(contact.has_email),
        "has_contact_route": bool(contact.has_contact_route),
        "status": contact.status,
        "notes": _clean_text(contact.notes),
    }
