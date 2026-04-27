from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ContactRow:
    occurrence_id: str
    source_enriched_json: str
    target_url: str
    hotel_name: str
    full_name: str
    title: str
    primary_handle: str
    phone: str
    phone2: str
    email: str
    email2: str
    linkedin_url: str
    x_handle: str
    other_contact_detail: str
    decision_maker_score: str
    intimacy_grade: str
    has_phone: bool
    has_email: bool
    has_contact_route: bool
    status: str
    notes: str
    payload: dict[str, Any]


@dataclass(frozen=True)
class HotelGroup:
    hotel_name: str
    target_url: str
    pending_count: int
    total_count: int
    contacts: list[ContactRow]


@dataclass(frozen=True)
class CrmSummary:
    total: int
    pending: int
    done: int
    skipped: int
