from __future__ import annotations

from outreach.batch_cold_email import (
    ColdEmailResult,
    apply_generation_results,
    build_user_message,
    generation_candidates,
    user_prompt_hash,
)
from outreach.schema import TRIAGE_APPROVED, TRIAGE_PENDING


def test_user_prompt_hash_stable() -> None:
    a = user_prompt_hash("hello")
    b = user_prompt_hash("hello")
    assert a == b
    assert len(a) == 16


def test_build_user_message_contains_json() -> None:
    text = build_user_message(contact_json={"outreach_id": "oh_x", "email": "a@b.co"}, template_body="Do thing.")
    assert "outreach_id" in text
    assert "Do thing." in text


def test_generation_candidates() -> None:
    doc = {
        "version": 1,
        "by_id": {
            "a": {
                "outreach_id": "a",
                "triage": {"status": TRIAGE_APPROVED},
                "generation": None,
            },
            "b": {
                "outreach_id": "b",
                "triage": {"status": TRIAGE_APPROVED},
                "generation": {"body": "done"},
            },
            "c": {
                "outreach_id": "c",
                "triage": {"status": TRIAGE_PENDING},
                "generation": None,
            },
        },
    }
    ids = generation_candidates(doc)
    assert ids == ["a"]


def test_apply_generation_results_success() -> None:
    doc = {
        "version": 1,
        "by_id": {
            "oh_1": {
                "outreach_id": "oh_1",
                "triage": {"status": TRIAGE_APPROVED},
                "generation": None,
            }
        },
    }
    rows = {"oh_1": ColdEmailResult(match_id="oh_1", subject="Hi", body="Body text")}
    apply_generation_results(
        doc,
        batch_id="bid",
        model="grok-test",
        system_prompt_id="cold_email_system_ansh_v1",
        outreach_ids=["oh_1"],
        user_prompt_hashes={"oh_1": "abc"},
        rows_ok=rows,
        failures=[],
    )
    g = doc["by_id"]["oh_1"]["generation"]
    assert g["subject"] == "Hi"
    assert g["body"] == "Body text"
    assert g["batch_job_id"] == "bid"
    assert g["error"] is None


def test_build_user_message_includes_full_nested_contact_payload() -> None:
    payload = {
        "outreach_id": "oh_x",
        "email": "alex.person@example.com",
        "contact": {"custom_enrichment_blob": {"personalization_angle": "mentions events ops"}},
        "pipeline_v4_candidate": {"candidate_id": "c_alex", "notes": ["candidate-level note"]},
        "pipeline_v4_candidate_match": {"status": "matched_linkedin", "method": "linkedin"},
    }
    text = build_user_message(contact_json=payload, template_body="Draft email.")
    assert "custom_enrichment_blob" in text
    assert "personalization_angle" in text
    assert "pipeline_v4_candidate" in text
    assert "pipeline_v4_candidate_match" in text
    assert "matched_linkedin" in text
    assert "candidate-level note" in text
