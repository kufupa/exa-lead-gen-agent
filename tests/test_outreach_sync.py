from __future__ import annotations

from outreach.ids import compute_outreach_id, primary_delivery_email, target_url_from_intimate_row
from outreach.indexes import rebuild_indexes
from outreach.schema import TRIAGE_APPROVED, TRIAGE_DECLINED, TRIAGE_PENDING, empty_state, validate_state
from outreach.sync import merge_intimates_into_state


def _minimal_intimate(generated_at: str = "2026-01-01T00:00:00+00:00") -> dict:
    return {
        "version": 1,
        "generated_at_utc": generated_at,
        "contacts": [
            {
                "full_name": "Test Person",
                "title": "GM",
                "company": "Ex Hotel",
                "email": "test.person@exhotel.com",
                "email2": None,
                "phase1_research": {
                    "source_enriched_json": "hotel_leads__ex.enriched.json",
                    "target_url": "https://www.exhotel.com/",
                },
            }
        ],
    }


def test_merge_adds_row_and_index() -> None:
    intimate = _minimal_intimate()
    state, added, refreshed = merge_intimates_into_state(intimate, None)
    assert added == 1
    assert refreshed == 0
    assert validate_state(state) == []
    pem = primary_delivery_email(intimate["contacts"][0])
    assert pem
    oid = compute_outreach_id(pem, target_url_from_intimate_row(intimate["contacts"][0]))
    assert oid in state["by_id"]
    hotel = state["by_id"][oid]["hotel_canonical_url"]
    assert oid in state["indexes"]["by_hotel"][hotel]


def test_merge_preserves_declined() -> None:
    intimate = _minimal_intimate()
    state = empty_state()
    pem = "test.person@exhotel.com"
    oid = compute_outreach_id(pem, "https://www.exhotel.com/")
    state["by_id"][oid] = {
        "outreach_id": oid,
        "primary_email": pem,
        "target_url": "https://www.exhotel.com/",
        "hotel_canonical_url": "https://www.exhotel.com",
        "intimate_snapshot": {},
        "intimate_row_hash": "x",
        "intimate_doc_generated_at_utc": None,
        "triage": {"status": TRIAGE_DECLINED, "decided_at_utc": "2026-01-02T00:00:00+00:00", "note": None},
        "generation": None,
        "send": None,
    }
    state["indexes"] = rebuild_indexes(state["by_id"])
    state2, added, _ = merge_intimates_into_state(intimate, state)
    assert added == 0
    assert state2["by_id"][oid]["triage"]["status"] == TRIAGE_DECLINED


def test_merge_skips_structured_phone_when_flag() -> None:
    doc = _minimal_intimate()
    doc["contacts"].append(
        {
            "full_name": "Phone Person",
            "title": "Sales",
            "company": "Ex Hotel",
            "email": "phone.person@exhotel.com",
            "phone": "+1 555 0100",
            "phase1_research": {
                "source_enriched_json": "hotel_leads__ex.enriched.json",
                "target_url": "https://www.exhotel.com/",
            },
        }
    )
    state, added, _ = merge_intimates_into_state(doc, None, skip_structured_phone_contacts=True)
    assert added == 1
    oids = list(state["by_id"].keys())
    assert len(oids) == 1


def test_merge_second_contact_different_hotel() -> None:
    intimate = _minimal_intimate()
    intimate["contacts"].append(
        {
            "full_name": "Other",
            "title": "Sales",
            "company": "Y",
            "email": "other@y.com",
            "phase1_research": {
                "source_enriched_json": "a.json",
                "target_url": "https://www.otherhotel.com/",
            },
        }
    )
    state, added, _ = merge_intimates_into_state(intimate, None)
    assert added == 2
    assert len(state["indexes"]["by_hotel"]) == 2


def test_validate_catches_orphan_index() -> None:
    doc = empty_state()
    oid = "oh_deadbeefcafecafe000000"
    doc["by_id"][oid] = {
        "outreach_id": oid,
        "primary_email": "a@b.co",
        "target_url": "https://x.com/",
        "hotel_canonical_url": "https://x.com",
        "intimate_snapshot": {},
        "intimate_row_hash": "h",
        "intimate_doc_generated_at_utc": None,
        "triage": {"status": TRIAGE_PENDING, "decided_at_utc": None, "note": None},
        "generation": None,
        "send": None,
    }
    doc["indexes"] = {"by_hotel": {"https://x.com": ["oh_missing"]}}
    errs = validate_state(doc)
    assert any("orphan" in e for e in errs)


def test_merge_snapshot_keeps_provenance_for_full_payload_rows() -> None:
    intimate = {
        "version": 2,
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "contacts": [
            {
                "full_name": "Alex Person",
                "title": "GM",
                "company": "Ex Hotel",
                "email": "alex.person@exhotel.com",
                "target_url": "https://www.exhotel.com/",
                "source_enriched_json": "jsons/ex.enriched.json",
                "occurrence_id": "jsons/ex.enriched.json::em:alex.person@exhotel.com",
                "contact_key": "em:alex.person@exhotel.com",
                "email_key": "alex.person@exhotel.com",
                "contact": {"full_name": "Alex Person", "custom": "full payload stays in intimate file"},
            }
        ],
    }

    state, added, refreshed = merge_intimates_into_state(intimate, None)

    assert added == 1
    assert refreshed == 0
    row = next(iter(state["by_id"].values()))
    snap = row["intimate_snapshot"]
    assert snap["source_enriched_json"] == "jsons/ex.enriched.json"
    assert snap["occurrence_id"] == "jsons/ex.enriched.json::em:alex.person@exhotel.com"
    assert snap["contact_key"] == "em:alex.person@exhotel.com"
    assert snap["email_key"] == "alex.person@exhotel.com"
    assert "triage" not in snap
    assert "generation" not in snap
    assert "contact" not in snap


def test_merge_snapshot_keeps_email_merge_summary() -> None:
    intimate = {
        "version": 3,
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "contacts": [
            {
                "full_name": "Alex Person",
                "title": "GM",
                "company": "Ex Hotel",
                "email": "alex.person@exhotel.com",
                "target_url": "https://hotel-a.example/",
                "hotel_canonical_url": "https://hotel-a.example",
                "source_enriched_json": "jsons/a.enriched.json",
                "occurrence_id": "jsons/a.enriched.json::em:alex.person@exhotel.com",
                "contact_key": "em:alex.person@exhotel.com",
                "email_key": "alex.person@exhotel.com",
                "merged_occurrence_count": 2,
                "merged_occurrence_ids": [
                    "jsons/a.enriched.json::em:alex.person@exhotel.com",
                    "jsons/b.enriched.json::em:alex.person@exhotel.com",
                ],
                "related_target_urls": ["https://hotel-a.example/", "https://hotel-b.example/"],
                "related_hotel_canonical_urls": ["https://hotel-a.example", "https://hotel-b.example"],
            }
        ],
    }

    state, added, refreshed = merge_intimates_into_state(intimate, None)

    assert added == 1
    assert refreshed == 0
    row = next(iter(state["by_id"].values()))
    snap = row["intimate_snapshot"]
    assert snap["merged_occurrence_count"] == 2
    assert snap["related_hotel_canonical_urls"] == ["https://hotel-a.example", "https://hotel-b.example"]
    oid = next(iter(state["by_id"]))
    assert oid in state["indexes"]["by_hotel"]["https://hotel-b.example"]
