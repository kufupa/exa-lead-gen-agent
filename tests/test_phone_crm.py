from __future__ import annotations

import json
from pathlib import Path

import pytest

from phone_crm.models import ContactRow
from phone_crm.normalizer import normalize_contact_row
from phone_crm.queries import (
    build_groups,
    fetch_contacts,
    find_next_contact_id,
    filter_contacts_by_search,
    normalize_rows_from_json,
    update_notes_and_status,
    row_to_contact,
)
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


def test_row_to_contact_normalizes_phone_presence() -> None:
    row_with_phone_text = {
        "occurrence_id": "x1",
        "source_enriched_json": "jsons/file.enriched.json",
        "target_url": "https://hotel.example",
        "hotel_name": "hotel.example",
        "full_name": "Raw Contact",
        "title": "",
        "primary_handle": "",
        "phone": " +44 20 1111 ",
        "phone2": " ",
        "email": "",
        "email2": "",
        "linkedin_url": "",
        "x_handle": "",
        "other_contact_detail": "",
        "decision_maker_score": "",
        "intimacy_grade": "",
        "has_phone": False,
        "has_email": False,
        "has_contact_route": True,
        "status": "pending",
        "notes": "",
        "payload": {},
    }

    row_with_whitespace_phone = dict(row_with_phone_text, phone="   ", phone2="   ")

    assert row_to_contact(row_with_phone_text).has_phone is True
    assert row_to_contact(row_with_whitespace_phone).has_phone is False


def test_row_to_contact_derives_phone_flag_from_phone_text() -> None:
    row = {
        "occurrence_id": "jsons/file.enriched.json::li:https://www.linkedin.com/in/whitespace",
        "source_enriched_json": "jsons/file.enriched.json",
        "target_url": "https://hotel.example",
        "full_name": "Phone Contact",
        "phone": "  +44 20 1234  ",
        "phone2": "    ",
        "status": "pending",
        "has_phone": False,
        "has_contact_route": False,
    }
    contact = row_to_contact(row)
    assert contact.has_phone is True
    assert find_next_contact_id([contact], None, phones_only=True) == contact.occurrence_id
    contact_empty_phone = row_to_contact({**row, "phone": "   ", "phone2": "   "})
    assert contact_empty_phone.has_phone is False


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
    title: str = "",
    phone: str = "",
    phone2: str = "",
    primary_handle: str = "",
    target_url: str = "https://hotel.example",
    has_contact_route: bool = True,
    has_phone: bool = False,
) -> ContactRow:
    return ContactRow(
        occurrence_id=occurrence_id,
        source_enriched_json="jsons/file.enriched.json",
        target_url=target_url,
        hotel_name=hotel_name,
        full_name=full_name,
        title=title,
        primary_handle=primary_handle,
        phone=phone,
        phone2=phone2,
        email="",
        email2="",
        linkedin_url="",
        x_handle="",
        other_contact_detail="",
        decision_maker_score="",
        intimacy_grade="",
        has_phone=has_phone,
        has_email=False,
        has_contact_route=has_contact_route,
        status=status,
        notes="",
        payload={},
    )


def test_filter_contacts_by_search_blank_search_returns_original_rows() -> None:
    rows = [
        _row(occurrence_id="a1", hotel_name="Hotel One", full_name="Alice"),
        _row(occurrence_id="a2", hotel_name="Hotel Two", full_name="Bob"),
    ]

    assert filter_contacts_by_search(rows, None) == rows
    assert filter_contacts_by_search(rows, "") == rows
    assert filter_contacts_by_search(rows, "   ") == rows


def test_filter_contacts_by_search_matches_hotel_name() -> None:
    rows = [
        _row(occurrence_id="a1", hotel_name="Grand Mirage", full_name="Alice"),
        _row(occurrence_id="a2", hotel_name="Azure Bay", full_name="Bob"),
    ]

    assert filter_contacts_by_search(rows, "mirage") == [rows[0]]


def test_filter_contacts_by_search_matches_contact_name_or_title() -> None:
    rows = [
        _row(occurrence_id="a1", hotel_name="Hotel One", full_name="Alice Johnson"),
        _row(
            occurrence_id="a2",
            hotel_name="Hotel Two",
            full_name="Bob",
            title="Director of Sales",
        ),
    ]

    assert filter_contacts_by_search(rows, "alice") == [rows[0]]
    assert filter_contacts_by_search(rows, "director") == [rows[1]]


def test_filter_contacts_by_search_matches_phone_substring() -> None:
    rows = [
        _row(occurrence_id="a1", hotel_name="Hotel One", full_name="Alice", phone="+44 20 1234 5678"),
        _row(occurrence_id="a2", hotel_name="Hotel Two", full_name="Bob"),
    ]

    assert filter_contacts_by_search(rows, "1234") == [rows[0]]


def test_filter_contacts_by_search_no_match_returns_empty_list() -> None:
    rows = [
        _row(occurrence_id="a1", hotel_name="Hotel One", full_name="Alice"),
        _row(occurrence_id="a2", hotel_name="Hotel Two", full_name="Bob"),
    ]

    assert filter_contacts_by_search(rows, "nonexistent") == []


def test_build_groups_sorts_by_pending_count_then_name() -> None:
    rows = [
        _row(occurrence_id="a1", hotel_name="Hotel B", full_name="Zelda", status="done"),
        _row(occurrence_id="a2", hotel_name="Hotel B", full_name="Ana", status="pending", has_phone=True),
        _row(occurrence_id="a3", hotel_name="Hotel B", full_name="Ben", status="pending", has_phone=True),
        _row(occurrence_id="b1", hotel_name="Hotel A", full_name="Cara", status="pending", has_phone=True),
    ]

    groups = build_groups(rows, phones_only=False)
    assert [g.hotel_name for g in groups] == ["Hotel B", "Hotel A"]
    assert groups[0].pending_count == 2
    assert [c.occurrence_id for c in groups[0].contacts] == ["a2", "a3", "a1"]


def test_build_groups_respects_mode_specific_pending_counts() -> None:
    rows = [
        _row(occurrence_id="a1", hotel_name="Alpha Hotel", full_name="Ana", status="pending", has_phone=False),
        _row(occurrence_id="a2", hotel_name="Alpha Hotel", full_name="Ben", status="pending", has_phone=True),
        _row(occurrence_id="b1", hotel_name="Bravo Hotel", full_name="Cara", status="pending", has_phone=True),
        _row(occurrence_id="b2", hotel_name="Bravo Hotel", full_name="Drew", status="pending", has_phone=True),
        _row(occurrence_id="c1", hotel_name="Charlie Hotel", full_name="Eli", status="done", has_phone=True),
    ]

    default_mode = build_groups(rows)
    phone_mode = build_groups(rows, phones_only=True)

    assert [g.hotel_name for g in default_mode] == ["Alpha Hotel", "Bravo Hotel", "Charlie Hotel"]
    assert {group.hotel_name: group.pending_count for group in default_mode} == {
        "Alpha Hotel": 2,
        "Bravo Hotel": 2,
        "Charlie Hotel": 0,
    }

    assert [g.hotel_name for g in phone_mode] == ["Bravo Hotel", "Alpha Hotel", "Charlie Hotel"]
    assert {group.hotel_name: group.pending_count for group in phone_mode} == {
        "Bravo Hotel": 2,
        "Alpha Hotel": 1,
        "Charlie Hotel": 0,
    }


def test_build_groups_orders_by_status_when_phones_only() -> None:
    rows = [
        _row(occurrence_id="done", hotel_name="Hotel A", full_name="Done", status="done", has_phone=True),
        _row(occurrence_id="skipped", hotel_name="Hotel A", full_name="Skipped", status="skipped", has_phone=True),
        _row(
            occurrence_id="pending",
            hotel_name="Hotel A",
            full_name="Pending",
            status="pending",
            has_phone=True,
        ),
    ]

    groups = build_groups(rows, phones_only=True)
    assert len(groups) == 1
    assert [contact.occurrence_id for contact in groups[0].contacts] == [
        "pending",
        "done",
        "skipped",
    ]


def test_find_next_contact_prefers_same_hotel_pending_then_global_next() -> None:
    rows = [
        _row(occurrence_id="a1", hotel_name="Hotel B", full_name="Zelda", status="done"),
        _row(occurrence_id="a2", hotel_name="Hotel B", full_name="Ana", status="pending", has_phone=True),
        _row(occurrence_id="a3", hotel_name="Hotel B", full_name="Ben", status="pending", has_phone=True),
        _row(occurrence_id="b1", hotel_name="Hotel A", full_name="Cara", status="pending", has_phone=True),
    ]

    assert find_next_contact_id(rows, "a2", phones_only=False) == "a3"
    assert find_next_contact_id(rows, "a3", phones_only=False) == "b1"
    assert find_next_contact_id(rows, None, phones_only=False) == "a2"


def test_find_next_contact_returns_none_for_final_row() -> None:
    rows = [
        _row(occurrence_id="a1", hotel_name="Hotel A", full_name="Only", status="done", has_contact_route=False),
        _row(occurrence_id="a2", hotel_name="Hotel A", full_name="Done", status="done", has_contact_route=False),
    ]
    assert find_next_contact_id(rows, "a1", phones_only=False) is None


def test_find_next_contact_skips_non_actionable_rows_in_phone_mode() -> None:
    rows = [
        _row(
            occurrence_id="a1",
            hotel_name="Hotel A",
            full_name="Ana",
            status="pending",
            has_contact_route=True,
            has_phone=False,
        ),
        _row(
            occurrence_id="a2",
            hotel_name="Hotel A",
            full_name="Ben",
            status="pending",
            has_contact_route=False,
            has_phone=True,
        ),
        _row(
            occurrence_id="a3",
            hotel_name="Hotel A",
            full_name="Cara",
            status="done",
            has_contact_route=True,
            has_phone=True,
        ),
    ]

    assert find_next_contact_id(rows, "a1", phones_only=False) is None
    assert find_next_contact_id(rows, "a1", phones_only=True) == "a2"


def test_fetch_contacts_phones_only_filters_phone_routes() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.executed = []
            self.rows = []

        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, query: str, params: tuple[bool]) -> None:
            self.executed.append((query, params))

        def fetchall(self) -> list[dict[str, object]]:
            return self.rows

    class FakeConnection:
        def __init__(self, cursor: FakeCursor) -> None:
            self._cursor = cursor

        def cursor(self) -> FakeCursor:
            return self._cursor

    cursor = FakeCursor()
    conn = FakeConnection(cursor)

    fetch_contacts(conn, phones_only=True)

    assert len(cursor.executed) == 1
    query, params = cursor.executed[0]
    flattened = " ".join(query.split())
    assert "%s = false or has_phone = true or coalesce(btrim(phone), '') <> '' or coalesce(btrim(phone2), '') <> ''" in flattened
    assert params == (True,)


def test_update_notes_and_status_maps_row_and_writes_payload() -> None:
    class FakeCursor:
        def __init__(self) -> None:
            self.executed = []

        def __enter__(self) -> "FakeCursor":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def execute(self, query: str, params: tuple[str, str, str]) -> None:
            self.executed.append((query, params))

        def fetchone(self) -> dict[str, object]:
            return {
                "occurrence_id": "occ-1",
                "source_enriched_json": "jsons/file.enriched.json",
                "target_url": "https://hotel.example",
                "hotel_name": "hotel.example",
                "full_name": "Alex Morgan",
                "title": "Sales Director",
                "primary_handle": "@alex",
                "phone": " +44 20 1234 5678 ",
                "phone2": "   ",
                "email": "",
                "email2": "",
                "linkedin_url": "",
                "x_handle": "",
                "other_contact_detail": "",
                "decision_maker_score": "high",
                "intimacy_grade": "medium",
                "has_phone": False,
                "has_email": False,
                "has_contact_route": True,
                "status": "done",
                "notes": "called back tomorrow",
                "payload": '{"evidence": [{"source_url": "https://example.com"}]}',
            }

    class FakeConnection:
        def __init__(self, cursor: FakeCursor) -> None:
            self._cursor = cursor
            self.commits = 0

        def cursor(self) -> FakeCursor:
            return self._cursor

        def commit(self) -> None:
            self.commits += 1

    cursor = FakeCursor()
    conn = FakeConnection(cursor)
    updated = update_notes_and_status(conn, "occ-1", "called back tomorrow", "done")

    assert updated is not None
    assert updated.occurrence_id == "occ-1"
    assert updated.status == "done"
    assert updated.notes == "called back tomorrow"
    assert updated.has_phone is True
    assert updated.payload == {"evidence": [{"source_url": "https://example.com"}]}
    assert cursor.executed and "set notes = %s, status = %s" in " ".join(cursor.executed[0][0].split())
    assert cursor.executed[0][1] == ("called back tomorrow", "done", "occ-1")
    assert conn.commits == 1
