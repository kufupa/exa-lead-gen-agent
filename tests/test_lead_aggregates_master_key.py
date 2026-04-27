import json
from pathlib import Path

from lead_aggregates.builders import build_master_document


def test_master_keeps_two_occurrences_same_person_two_hotels(tmp_path: Path) -> None:
    root = tmp_path
    jd = root / "jsons"
    jd.mkdir(parents=True)
    pat = {"full_name": "Pat Lee", "title": "GM", "linkedin_url": "https://linkedin.com/in/pat-lee"}
    a = {
        "target_url": "https://hotel-a.example/",
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "contacts": [pat],
    }
    b = {
        "target_url": "https://hotel-b.example/",
        "generated_at_utc": "2026-01-02T00:00:00+00:00",
        "contacts": [dict(pat)],
    }
    (jd / "a.enriched.json").write_text(json.dumps(a), encoding="utf-8")
    (jd / "b.enriched.json").write_text(json.dumps(b), encoding="utf-8")

    doc = build_master_document(jd)
    assert doc["count"] == 2
    ids = {row["occurrence_id"] for row in doc["contacts"]}
    assert len(ids) == 2
    assert all("::li:https://www.linkedin.com/in/pat-lee" in oid for oid in ids)
