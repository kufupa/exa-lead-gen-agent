from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from lead_aggregates.builders import has_structured_phone
from lead_aggregates.urls import canonical_hotel_url

from outreach.ids import compute_outreach_id, primary_delivery_email, target_url_from_intimate_row
from outreach.indexes import rebuild_indexes
from outreach.schema import TRIAGE_PENDING, empty_state


def _row_hash(contact_row: dict[str, Any]) -> str:
    subset = {
        "full_name": contact_row.get("full_name"),
        "title": contact_row.get("title"),
        "company": contact_row.get("company"),
        "email": (contact_row.get("email") or "").strip().lower() or None,
        "email2": (contact_row.get("email2") or "").strip().lower() or None,
        "target_url": contact_row.get("target_url"),
        "phase1_research": contact_row.get("phase1_research"),
        "source_enriched_json": contact_row.get("source_enriched_json"),
        "occurrence_id": contact_row.get("occurrence_id"),
        "contact_key": contact_row.get("contact_key"),
        "email_key": contact_row.get("email_key"),
        "merged_occurrence_count": contact_row.get("merged_occurrence_count"),
        "merged_occurrence_ids": contact_row.get("merged_occurrence_ids"),
        "related_target_urls": contact_row.get("related_target_urls"),
        "related_hotel_canonical_urls": contact_row.get("related_hotel_canonical_urls"),
    }
    blob = json.dumps(subset, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:40]


def _snapshot(contact_row: dict[str, Any]) -> dict[str, Any]:
    p1 = contact_row.get("phase1_research") if isinstance(contact_row.get("phase1_research"), dict) else {}
    source_run = contact_row.get("source_run") if isinstance(contact_row.get("source_run"), dict) else {}
    return {
        "source_enriched_json": contact_row.get("source_enriched_json") or p1.get("source_enriched_json") or source_run.get("source_enriched_json"),
        "occurrence_id": contact_row.get("occurrence_id"),
        "full_name": contact_row.get("full_name"),
        "title": contact_row.get("title"),
        "company": contact_row.get("company"),
        "contact_key": contact_row.get("contact_key"),
        "email_key": contact_row.get("email_key"),
        "merged_occurrence_count": contact_row.get("merged_occurrence_count"),
        "merged_occurrence_ids": contact_row.get("merged_occurrence_ids"),
        "related_target_urls": contact_row.get("related_target_urls"),
        "related_hotel_canonical_urls": contact_row.get("related_hotel_canonical_urls"),
    }


def _new_row(
    *,
    outreach_id: str,
    primary_email: str,
    target_url: str,
    hotel_canonical: str,
    snapshot: dict[str, Any],
    row_hash: str,
    intimate_generated_at: str | None,
) -> dict[str, Any]:
    return {
        "outreach_id": outreach_id,
        "primary_email": primary_email,
        "target_url": target_url,
        "hotel_canonical_url": hotel_canonical,
        "related_target_urls": snapshot.get("related_target_urls") if isinstance(snapshot.get("related_target_urls"), list) else [],
        "related_hotel_canonical_urls": snapshot.get("related_hotel_canonical_urls")
        if isinstance(snapshot.get("related_hotel_canonical_urls"), list)
        else [],
        "intimate_snapshot": snapshot,
        "intimate_row_hash": row_hash,
        "intimate_doc_generated_at_utc": intimate_generated_at,
        "triage": {
            "status": TRIAGE_PENDING,
            "decided_at_utc": None,
            "note": None,
        },
        "generation": None,
        "send": None,
    }


def load_intimate_doc(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def merge_intimates_into_state(
    intimate_doc: dict[str, Any],
    state: dict[str, Any] | None,
    *,
    refresh_snapshots: bool = False,
    skip_structured_phone_contacts: bool = False,
) -> tuple[dict[str, Any], int, int]:
    """
    Merge contacts from intimate_email_contacts.json document into state.
    Returns (new_state, num_added, num_snapshot_refreshed).

    If skip_structured_phone_contacts is True, rows with non-empty phone or phone2
    are omitted (email-only outreach).
    """
    if state is None:
        state = empty_state()
    by_id: dict[str, Any] = dict(state.get("by_id") or {})
    intimate_generated_at = intimate_doc.get("generated_at_utc")
    if not isinstance(intimate_generated_at, str):
        intimate_generated_at = None

    added = 0
    refreshed = 0
    contacts = intimate_doc.get("contacts")
    if not isinstance(contacts, list):
        contacts = []

    for contact_row in contacts:
        if not isinstance(contact_row, dict):
            continue
        pem = primary_delivery_email(contact_row)
        if not pem:
            continue
        if skip_structured_phone_contacts and has_structured_phone(contact_row):
            continue
        target_url = target_url_from_intimate_row(contact_row)
        if not target_url:
            continue
        oid = compute_outreach_id(pem, target_url)
        row_hash = _row_hash(contact_row)
        snap = _snapshot(contact_row)
        hotel_canonical = canonical_hotel_url(target_url)

        existing = by_id.get(oid)
        if existing is None:
            by_id[oid] = _new_row(
                outreach_id=oid,
                primary_email=pem,
                target_url=target_url,
                hotel_canonical=hotel_canonical,
                snapshot=snap,
                row_hash=row_hash,
                intimate_generated_at=intimate_generated_at,
            )
            added += 1
            continue

        existing["intimate_doc_generated_at_utc"] = intimate_generated_at
        if refresh_snapshots or existing.get("intimate_row_hash") != row_hash:
            existing["intimate_snapshot"] = snap
            existing["intimate_row_hash"] = row_hash
            existing["primary_email"] = pem
            existing["target_url"] = target_url
            existing["hotel_canonical_url"] = hotel_canonical
            existing["related_target_urls"] = (
                snap.get("related_target_urls") if isinstance(snap.get("related_target_urls"), list) else []
            )
            existing["related_hotel_canonical_urls"] = (
                snap.get("related_hotel_canonical_urls")
                if isinstance(snap.get("related_hotel_canonical_urls"), list)
                else []
            )
            refreshed += 1

    state["version"] = state.get("version") or 1
    state["by_id"] = by_id
    state["updated_at_utc"] = datetime.now(timezone.utc).isoformat()
    state["indexes"] = rebuild_indexes(by_id)
    return state, added, refreshed
