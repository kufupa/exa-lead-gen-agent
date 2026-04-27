import json
from pathlib import Path

from lead_aggregates.store import AggregatesStore


def test_rebuild_all_writes_five_files(tmp_path: Path) -> None:
    root = tmp_path
    jd = root / "jsons"
    jd.mkdir(parents=True)
    fd = root / "fullJSONs"
    data = {
        "target_url": "https://h.example/",
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "contacts": [
            {
                "full_name": "Alex",
                "title": "RM",
                "email": "alex.person@h.example",
                "phone": "+44 20 0000 0000",
                "linkedin_url": None,
                "evidence": [],
                "intimacy_grade": "high",
                "decision_maker_score": "high",
                "fit_reason": "x",
                "contact_evidence_summary": "y",
            }
        ],
    }
    (jd / "one.enriched.json").write_text(json.dumps(data), encoding="utf-8")

    store = AggregatesStore(fd, jd, lock_timeout=60)
    store.rebuild_all()

    assert (fd / "all_enriched_leads.json").exists()
    assert (fd / "intimate_phone_contacts.json").exists()
    assert (fd / "intimate_email_contacts.json").exists()
    assert (fd / "intimate_unified_contacts.json").exists()
    assert (fd / "url_registry.json").exists()
    master = json.loads((fd / "all_enriched_leads.json").read_text(encoding="utf-8"))
    assert master["count"] == 1
