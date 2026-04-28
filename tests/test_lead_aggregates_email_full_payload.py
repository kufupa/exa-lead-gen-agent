from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from lead_aggregates.builders import build_email_document


def _write_enriched(path: Path, *, target_url: str, contacts: list[dict[str, Any]]) -> None:
    doc = {
        "target_url": target_url,
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "model": "pipeline-v4",
        "agent_count": None,
        "max_turns": None,
        "max_turns_effective": None,
        "min_contacts": None,
        "target_contacts": None,
        "max_contacts": len(contacts),
        "extra_contact_pass": False,
        "strict_evidence": True,
        "allow_linkedin": True,
        "usage": {"prompt_tokens": 10},
        "contact_enrichment": {
            "version": 4,
            "enriched_at_utc": "2026-01-01T00:00:00+00:00",
            "mode": "pipeline-v4",
            "model": "pipeline-v4",
            "concurrency": None,
            "skipped_pre_enrichment": 0,
            "attempted": len(contacts),
            "succeeded": len(contacts),
            "failed": 0,
        },
        "pipeline_v4": {
            "input_url": target_url,
            "resolved_org": {"canonical_name": "Example Hotel", "domains": ["example.com"]},
            "candidates": [
                {
                    "candidate_id": "c_alex",
                    "full_name": "Alex Person",
                    "title": "General Manager",
                    "company": "Example Hotel",
                    "role_tier": 1,
                    "role_family": "gm_ops",
                    "current_role_confidence": "high",
                    "relationship_confidence": "high",
                    "linkedin_url": "https://www.linkedin.com/in/alex-person",
                    "contact_routes": [
                        {"kind": "email", "value": "alex.person@example.com", "confidence": "high"},
                    ],
                    "evidence": [
                        {
                            "url": "https://example.com/team",
                            "title": "Team",
                            "source_type": "hotel_site",
                            "snippet": "Alex is GM.",
                        },
                    ],
                    "notes": ["candidate-level note that legacy contact omits"],
                }
            ],
        },
        "contacts": contacts,
    }
    path.write_text(json.dumps(doc), encoding="utf-8")


def _contact(**overrides: Any) -> dict[str, Any]:
    base = {
        "full_name": "Alex Person",
        "title": "General Manager",
        "company": "Example Hotel",
        "linkedin_url": "https://www.linkedin.com/in/alex-person",
        "email": "alex.person@example.com",
        "email2": None,
        "phone": "+44 20 0000 0000",
        "phone2": None,
        "x_handle": None,
        "other_contact_detail": "VIP events buyer",
        "decision_maker_score": "high",
        "intimacy_grade": "high",
        "fit_reason": "Owns hotel operations.",
        "contact_evidence_summary": "Official team page.",
        "evidence": [{"source_url": "https://example.com/team", "source_type": "official_site", "quote_or_fact": "Alex is GM."}],
        "linkedin_profile": {"headline": "GM at Example Hotel"},
        "custom_enrichment_blob": {"personalization_angle": "mentions events ops"},
    }
    base.update(overrides)
    return base


def test_email_document_preserves_full_contact_and_source_payload(tmp_path: Path) -> None:
    jsons_dir = tmp_path / "jsons"
    jsons_dir.mkdir()
    _write_enriched(jsons_dir / "one.enriched.json", target_url="https://example.com/", contacts=[_contact()])

    doc = build_email_document(jsons_dir)

    assert doc["count"] == 1
    row = doc["contacts"][0]
    assert row["email"] == "alex.person@example.com"
    assert row["target_url"] == "https://example.com/"
    assert row["source_enriched_json"] == "jsons/one.enriched.json"
    assert row["occurrence_id"].startswith("jsons/one.enriched.json::")
    assert row["email_key"] == "alex.person@example.com"
    assert row["contact"]["custom_enrichment_blob"] == {"personalization_angle": "mentions events ops"}
    assert row["source_run"]["model"] == "pipeline-v4"
    assert row["source_run"]["contact_enrichment"]["version"] == 4
    assert row["pipeline_v4_candidate"]["candidate_id"] == "c_alex"
    assert row["pipeline_v4_candidate_match"]["status"] == "matched_linkedin"
    assert row["pipeline_v4_candidate"]["notes"] == ["candidate-level note that legacy contact omits"]
    assert "triage" not in row
    assert "generation" not in row
    assert "send" not in row


def test_email_document_dedupes_same_email_same_hotel_and_tracks_duplicate(tmp_path: Path) -> None:
    jsons_dir = tmp_path / "jsons"
    jsons_dir.mkdir()
    weaker = _contact(full_name="Alex Person", intimacy_grade="medium", phone=None)
    stronger = _contact(full_name="Alex Person", intimacy_grade="high", phone="+44 20 0000 0000")
    _write_enriched(jsons_dir / "a.enriched.json", target_url="https://example.com/", contacts=[weaker])
    _write_enriched(jsons_dir / "b.enriched.json", target_url="https://example.com/", contacts=[stronger])

    doc = build_email_document(jsons_dir)

    assert doc["count"] == 1
    row = doc["contacts"][0]
    assert row["phone"] == "+44 20 0000 0000"
    assert row["duplicate_occurrence_count"] == 1
    assert len(row["duplicate_occurrences"]) == 1
    assert row["duplicate_occurrences"][0]["source_enriched_json"] == "jsons/a.enriched.json"


def test_email_document_merges_same_email_across_different_hotels(tmp_path: Path) -> None:
    jsons_dir = tmp_path / "jsons"
    jsons_dir.mkdir()
    weak = _contact(company="Hotel A", phone=None, intimacy_grade="low")
    strong = _contact(company="Hotel B", phone="+44 20 0000 0000", intimacy_grade="high")
    _write_enriched(jsons_dir / "a.enriched.json", target_url="https://hotel-a.example/", contacts=[weak])
    _write_enriched(jsons_dir / "b.enriched.json", target_url="https://hotel-b.example/", contacts=[strong])

    doc = build_email_document(jsons_dir)

    assert doc["count"] == 1
    row = doc["contacts"][0]
    assert row["email_key"] == "alex.person@example.com"
    assert row["company"] == "Hotel B"
    assert row["merged_occurrence_count"] == 2
    assert row["duplicate_occurrence_count"] == 1
    assert set(row["related_target_urls"]) == {"https://hotel-a.example/", "https://hotel-b.example/"}
    assert len(row["merged_occurrences"]) == 2
    assert {occ["contact"]["company"] for occ in row["merged_occurrences"]} == {"Hotel A", "Hotel B"}


def test_email_document_merged_occurrences_keep_full_payload(tmp_path: Path) -> None:
    jsons_dir = tmp_path / "jsons"
    jsons_dir.mkdir()
    first = _contact(custom_enrichment_blob={"angle": "first hotel"})
    second = _contact(custom_enrichment_blob={"angle": "second hotel"})
    _write_enriched(jsons_dir / "a.enriched.json", target_url="https://hotel-a.example/", contacts=[first])
    _write_enriched(jsons_dir / "b.enriched.json", target_url="https://hotel-b.example/", contacts=[second])

    doc = build_email_document(jsons_dir)
    row = doc["contacts"][0]

    assert len(row["merged_occurrences"]) == 2
    for occ in row["merged_occurrences"]:
        assert occ["contact"]["custom_enrichment_blob"] in ({"angle": "first hotel"}, {"angle": "second hotel"})
        assert occ["source_run"]["model"] == "pipeline-v4"
        assert occ["pipeline_v4_candidate"]["candidate_id"] == "c_alex"
        assert occ["pipeline_v4_candidate_match"]["status"] == "matched_linkedin"


def test_email_document_does_not_attach_ambiguous_name_only_candidate(tmp_path: Path) -> None:
    jsons_dir = tmp_path / "jsons"
    jsons_dir.mkdir()
    contact = _contact(linkedin_url=None)
    doc = {
        "target_url": "https://example.com/",
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "model": "pipeline-v4",
        "contacts": [contact],
        "pipeline_v4": {
            "input_url": "https://example.com/",
            "candidates": [
                {"candidate_id": "c_1", "full_name": "Alex Person", "company": "Example Hotel", "contact_routes": []},
                {"candidate_id": "c_2", "full_name": "Alex Person", "company": "Example Hotel", "contact_routes": []},
            ],
        },
    }
    (jsons_dir / "ambiguous.enriched.json").write_text(json.dumps(doc), encoding="utf-8")

    out = build_email_document(jsons_dir)

    row = out["contacts"][0]
    assert row["pipeline_v4_candidate"] is None
    assert row["pipeline_v4_candidate_match"]["status"] == "ambiguous_name_company"
