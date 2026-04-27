"""Dedupe + intimate_unified aggregate."""

from __future__ import annotations

import json
from pathlib import Path

from lead_aggregates.builders import build_intimate_unified_document, dedupe_key
from lead_aggregates.store import AggregatesStore


def test_dedupe_key_normalizes_regional_linkedin() -> None:
    uk = {"linkedin_url": "https://uk.linkedin.com/in/some-one", "full_name": "X", "company": "Y"}
    www = {"linkedin_url": "https://www.linkedin.com/in/some-one", "full_name": "X", "company": "Y"}
    assert dedupe_key(uk) == dedupe_key(www)
    assert dedupe_key(uk).startswith("li:https://www.linkedin.com/in/some-one")


def test_intimate_unified_collapses_regional_linkedin_across_files(tmp_path: Path) -> None:
    jd = tmp_path / "jsons"
    fd = tmp_path / "fullJSONs"
    jd.mkdir(parents=True)
    fd.mkdir()
    base_contact = {
        "full_name": "Pat Smith",
        "title": "GM",
        "evidence": [],
        "decision_maker_score": "high",
        "fit_reason": "x",
        "contact_evidence_summary": "y",
    }
    a = {
        "target_url": "https://hotel-a.example/",
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "contacts": [
            {
                **base_contact,
                "intimacy_grade": "medium",
                "phone": "+44 20 0000 0001",
                "email": None,
                "linkedin_url": "https://uk.linkedin.com/in/pat-smith",
            }
        ],
    }
    b = {
        "target_url": "https://hotel-b.example/",
        "generated_at_utc": "2026-01-02T00:00:00+00:00",
        "contacts": [
            {
                **base_contact,
                "intimacy_grade": "high",
                "phone": "+44 20 0000 0001",
                "email": "pat.smith@lead.example",
                "linkedin_url": "https://www.linkedin.com/in/pat-smith",
            }
        ],
    }
    (jd / "a.enriched.json").write_text(json.dumps(a), encoding="utf-8")
    (jd / "b.enriched.json").write_text(json.dumps(b), encoding="utf-8")

    uni = build_intimate_unified_document(jd)
    assert uni["count"] == 1
    row = uni["contacts"][0]
    assert row["phone"] == "+44 20 0000 0001"
    assert row["email"] == "pat.smith@lead.example"
    assert row["linkedin_url"] == "https://www.linkedin.com/in/pat-smith"

    store = AggregatesStore(fd, jd, lock_timeout=60)
    store.rebuild_all()
    assert json.loads((fd / "intimate_unified_contacts.json").read_text(encoding="utf-8"))["count"] == 1
