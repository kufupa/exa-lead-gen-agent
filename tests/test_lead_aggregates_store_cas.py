"""Registry compare-and-swap (run_claim) behavior on commit_after_enrich."""

from __future__ import annotations

import json
from pathlib import Path

from lead_aggregates.store import AggregatesStore
from lead_aggregates.urls import canonical_hotel_url


def _write_min_enriched(jd: Path, stem: str, target_url: str) -> None:
    data = {
        "target_url": target_url,
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "contacts": [
            {
                "full_name": "Alex",
                "title": "RM",
                "email": "alex@example.com",
                "phone": "+1 555 0000",
                "linkedin_url": None,
                "evidence": [],
                "intimacy_grade": "high",
                "decision_maker_score": "high",
                "fit_reason": "x",
                "contact_evidence_summary": "y",
            }
        ],
    }
    (jd / f"{stem}.enriched.json").write_text(json.dumps(data), encoding="utf-8")


def test_commit_success_skips_registry_patch_on_claim_mismatch(tmp_path: Path) -> None:
    jd = tmp_path / "jsons"
    fd = tmp_path / "fullJSONs"
    jd.mkdir(parents=True)
    fd.mkdir()
    tu = "https://h.example/"
    _write_min_enriched(jd, "one", tu)
    key = canonical_hotel_url(tu)

    store = AggregatesStore(fd, jd, lock_timeout=60)
    store.mark_researching(
        canonical_url=key,
        research_json="jsons/one.json",
        enriched_json="jsons/one.enriched.json",
        run_claim="owner-a",
    )

    ok = store.commit_after_enrich(
        canonical_url=key,
        research_json="jsons/one.json",
        enriched_json="jsons/one.enriched.json",
        error=None,
        run_claim="owner-b",
    )
    assert ok is False

    reg = json.loads((fd / "url_registry.json").read_text(encoding="utf-8"))
    row = reg["urls"][key]
    assert row["status"] == "researching"
    assert row["claimed_by"] == "owner-a"
    assert (fd / "all_enriched_leads.json").exists()
