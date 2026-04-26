from __future__ import annotations

from pipeline.grok_discovery import parse_grok_discovery_json, synthetic_grok_result_for_tests


def test_parse_grok_discovery_json_minimal() -> None:
    payload = {
        "hotel": {
            "input_url": "https://kayagnhlondon.com/",
            "canonical_name": "Kaya Great Northern Hotel",
            "property_name": "Kaya Great Northern Hotel",
            "domains": ["kayagnhlondon.com"],
            "evidence": [],
        },
        "aliases": [
            {
                "value": "Kaya Great Northern Hotel",
                "kind": "property",
                "confidence": "high",
                "source_url": "https://kayagnhlondon.com/",
                "quote": "Kaya",
            }
        ],
        "drafts": [
            {
                "full_name": "Alex Pat",
                "title": "General Manager",
                "company": "Kaya Great Northern Hotel",
                "evidence": [],
                "contact_routes": [],
                "confidence_hint": "high",
            }
        ],
    }
    r = parse_grok_discovery_json(payload)
    assert r.hotel.canonical_name == "Kaya Great Northern Hotel"
    assert any("Kaya Great Northern" in a.value for a in r.aliases)
    assert r.drafts[0].draft_id


def test_synthetic_kaya_fixture() -> None:
    r = synthetic_grok_result_for_tests()
    assert "Kaya Great Northern Hotel" in [a.value for a in r.aliases]
