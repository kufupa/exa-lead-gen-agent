from __future__ import annotations

import json
from pathlib import Path

import pytest

from phone_crm.models import ContactRow
from phone_crm.normalizer import normalize_contact_row
from phone_crm.queries import build_groups, find_next_contact_id, normalize_rows_from_json
from phone_crm.sync import run_sync


def test_normalize_contact_row_generates_expected_flags() -> None:
    row = {
        "occurrence_id": "jsons/file.enriched.json::li:https://www.linkedin.com/in/person",
        "source_enriched_json": "jsons/file.enriched.json",
        "target_url": "https://hotel.example",
        "contact": {
            "full_name": "Alex Morgan",
            "title": "Sales Director",
            "phone": "+44 20 1234",
            "phone2": "",
            "email": None,
            "other_contact_detail": "",
            "linkedin_url": "https://www.linkedin.com/in/person",
            "x_handle": "",
            "decision_maker_score": "high",
            "intimacy_grade": "medium",
        },
    }

    normalized = normalize_contact_row(row)
    assert normalized["occurrence_id"] == row["occurrence_id"]
    assert normalized["hotel_name"] == "hotel.example"
    assert normalized["full_name"] == "Alex Morgan"
    assert normalized["has_phone"] is True
    assert normalized["has_email"] is False
    assert normalized["has_contact_route"] is True


def test_normalize_rows_from_json_skips_missing_payload_contact_list() -> None:
    assert normalize_rows_from_json({"no": "contacts"}) == []


def test_normalize_rows_from_json_requires_occurrence_id() -> None:
    payload = {
        "contacts": [
            {"source_enriched_json": "jsons/file.enriched.json", "contact": {"full_name": "Missing ID"}},
        ],
    }
    with pytest.raises(ValueError, match="Missing occurrence_id"):
        normalize_rows_from_json(payload)


def test_run_sync_dry_run_does_not_require_db(tmp_path: Path) -> None:
    source = {
        "contacts": [
            {
                "occurrence_id": "jsons/file.enriched.json::li:https://www.linkedin.com/in/person",
                "source_enriched_json": "jsons/file.enriched.json",
                "target_url": "https://hotel.example",
                "contact": {"full_name": "Alex Morgan"},
            },
        ]
    }
    input_path = tmp_path / "all_enriched_leads.json"
    input_path.write_text(json.dumps(source))

    seen, upserted = run_sync(str(input_path), dry_run=True)
    assert seen == 1
    assert upserted == 1


def _row(
    *,
    occurrence_id: str,
    hotel_name: str,
    full_name: str,
    status: str = "pending",
    has_contact_route: bool = True,
) -> ContactRow:
    return ContactRow(
        occurrence_id=occurrence_id,
        source_enriched_json="jsons/file.enriched.json",
        target_url="https://hotel.example",
        hotel_name=hotel_name,
        full_name=full_name,
        title="",
        primary_handle="",
        phone="",
        phone2="",
        email="",
        email2="",
        linkedin_url="",
        x_handle="",
        other_contact_detail="",
        decision_maker_score="",
        intimacy_grade="",
        has_phone=False,
        has_email=False,
        has_contact_route=has_contact_route,
        status=status,
        notes="",
        payload={},
    )


def test_build_groups_sorts_by_pending_count_then_name() -> None:
    rows = [
        _row(occurrence_id="a1", hotel_name="Hotel B", full_name="Zelda", status="done"),
        _row(occurrence_id="a2", hotel_name="Hotel B", full_name="Ana", status="pending"),
        _row(occurrence_id="a3", hotel_name="Hotel B", full_name="Ben", status="pending"),
        _row(occurrence_id="b1", hotel_name="Hotel A", full_name="Cara", status="pending"),
    ]

    groups = build_groups(rows)
    assert [g.hotel_name for g in groups] == ["Hotel B", "Hotel A"]
    assert groups[0].pending_count == 2
    assert [c.occurrence_id for c in groups[0].contacts] == ["a2", "a3", "a1"]


def test_find_next_contact_prefers_same_hotel_pending_then_global_next() -> None:
    rows = [
        _row(occurrence_id="a1", hotel_name="Hotel B", full_name="Zelda", status="done"),
        _row(occurrence_id="a2", hotel_name="Hotel B", full_name="Ana", status="pending"),
        _row(occurrence_id="a3", hotel_name="Hotel B", full_name="Ben", status="pending"),
        _row(occurrence_id="b1", hotel_name="Hotel A", full_name="Cara", status="pending"),
    ]

    assert find_next_contact_id(rows, "a2") == "a3"
    assert find_next_contact_id(rows, "a3") == "b1"
    assert find_next_contact_id(rows, None) == "a2"


def test_find_next_contact_returns_none_for_final_row() -> None:
    rows = [
        _row(occurrence_id="a1", hotel_name="Hotel A", full_name="Only", status="done", has_contact_route=False),
        _row(occurrence_id="a2", hotel_name="Hotel A", full_name="Done", status="done", has_contact_route=False),
    ]
    assert find_next_contact_id(rows, "a1") is None
