"""Tests for hotel_decision_maker_research."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from hotel_decision_maker_research import (
    Contact,
    Evidence,
    LeadResearchResult,
    build_parser,
    build_round1_prompt,
    dedupe_and_rank,
    is_linkedin_only_evidence,
    process_contacts,
    recompute_intimacy,
    utility_score,
)


def test_invalid_intimacy_grade_rejected() -> None:
    with pytest.raises(ValidationError):
        Contact(
            full_name="Jane Doe",
            title="Director of Revenue",
            decision_maker_score="high",
            intimacy_grade="critical",  # type: ignore[arg-type]
            fit_reason="Owns distribution strategy.",
            contact_evidence_summary="Listed on corporate site.",
            evidence=[],
        )


def test_default_agent_count_is_16() -> None:
    args = build_parser().parse_args(["--url", "https://example.com"])
    assert args.agent_count == 16


def test_prompt_mentions_intimacy_grade_rules() -> None:
    prompt = build_round1_prompt("https://hotel.example", 25)
    assert "intimacy_grade" in prompt
    assert "direct public business email" in prompt


def test_intimacy_high_when_direct_email_on_contact() -> None:
    c = Contact(
        full_name="A",
        title="GM",
        email="a@hotel.com",
        decision_maker_score="medium",
        intimacy_grade="low",
        fit_reason="x",
        contact_evidence_summary="x",
        evidence=[],
    )
    assert recompute_intimacy(c) == "high"


def test_intimacy_high_when_email_in_evidence_quote() -> None:
    c = Contact(
        full_name="B",
        title="Reservations Manager",
        decision_maker_score="high",
        intimacy_grade="low",
        fit_reason="x",
        contact_evidence_summary="x",
        evidence=[
            Evidence(
                source_url="https://hotel.example/team",
                source_type="official_site",
                quote_or_fact="Email jane.smith@grandhotel.com for RFPs.",
            )
        ],
    )
    assert recompute_intimacy(c) == "high"


def test_intimacy_medium_for_functional_route_alias() -> None:
    c = Contact(
        full_name="C",
        title="Director of Sales",
        decision_maker_score="high",
        intimacy_grade="low",
        fit_reason="x",
        contact_evidence_summary="x",
        evidence=[
            Evidence(
                source_url="https://hotel.example/contact",
                source_type="official_site",
                quote_or_fact="Group sales inquiries: groupsales@hotel.com",
            )
        ],
    )
    assert recompute_intimacy(c) == "medium"


def test_dedupe_prefers_higher_utility() -> None:
    a = Contact(
        full_name="Dup",
        title="VP",
        linkedin_url="https://linkedin.com/in/dup",
        decision_maker_score="low",
        intimacy_grade="low",
        fit_reason="x",
        contact_evidence_summary="x",
        evidence=[],
    )
    b = Contact(
        full_name="Dup",
        title="VP",
        linkedin_url="https://linkedin.com/in/dup",
        email="dup@hotel.com",
        decision_maker_score="high",
        intimacy_grade="high",
        fit_reason="x",
        contact_evidence_summary="x",
        evidence=[],
    )
    out = dedupe_and_rank([a, b], max_contacts=10)
    assert len(out) == 1
    assert out[0].email == "dup@hotel.com"


def test_strict_evidence_drops_weak() -> None:
    weak = Contact(
        full_name="W",
        title="T",
        decision_maker_score="high",
        intimacy_grade="low",
        fit_reason="x",
        contact_evidence_summary="x",
        evidence=[
            Evidence(
                source_url="https://x.com/foo",
                source_type="x",
                quote_or_fact="Heard they work there.",
            )
        ],
    )
    strong = weak.model_copy(
        update={
            "email": "w@hotel.com",
            "evidence": [
                Evidence(
                    source_url="https://hotel.example/a",
                    source_type="official_site",
                    quote_or_fact="Bio",
                ),
                Evidence(
                    source_url="https://hotel.example/b",
                    source_type="news",
                    quote_or_fact="Interview",
                ),
            ],
        }
    )
    out = process_contacts(
        [weak, strong],
        max_contacts=25,
        strict_evidence=True,
        allow_linkedin=True,
    )
    assert len(out) == 1
    assert out[0].email == "w@hotel.com"


def test_linkedin_only_filtered_when_disallowed() -> None:
    c = Contact(
        full_name="L",
        title="RM",
        decision_maker_score="medium",
        intimacy_grade="low",
        fit_reason="x",
        contact_evidence_summary="x",
        evidence=[
            Evidence(
                source_url="https://linkedin.com/in/l",
                source_type="linkedin",
                quote_or_fact="Profile headline.",
            )
        ],
    )
    assert is_linkedin_only_evidence(c) is True
    out = process_contacts(
        [c],
        max_contacts=25,
        strict_evidence=False,
        allow_linkedin=False,
    )
    assert out == []


def test_normalize_caps_to_25() -> None:
    rows = [
        Contact(
            full_name=f"P{i}",
            title="T",
            decision_maker_score="high",
            intimacy_grade="high",
            fit_reason="x",
            contact_evidence_summary="x",
            email=f"p{i}@h.com",
            evidence=[
                Evidence(
                    source_url=f"https://h.com/{i}a",
                    source_type="official_site",
                    quote_or_fact="a",
                ),
                Evidence(
                    source_url=f"https://h.com/{i}b",
                    source_type="official_site",
                    quote_or_fact="b",
                ),
            ],
        )
        for i in range(30)
    ]
    out = process_contacts(
        rows,
        max_contacts=25,
        strict_evidence=True,
        allow_linkedin=True,
    )
    assert len(out) == 25


def test_utility_score_weights_intimacy() -> None:
    low = Contact(
        full_name="x",
        title="CEO",
        decision_maker_score="high",
        intimacy_grade="low",
        fit_reason="x",
        contact_evidence_summary="x",
        evidence=[],
    )
    high = low.model_copy(update={"intimacy_grade": "high", "decision_maker_score": "low"})
    assert utility_score(high) > utility_score(low)


def test_dry_run_writes_no_api(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("XAI_API_KEY", raising=False)
    import subprocess
    import sys

    script = Path(__file__).resolve().parents[1] / "hotel_decision_maker_research.py"
    r = subprocess.run(
        [sys.executable, str(script), "--url", "https://hotel.example", "--dry-run-prompt"],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert r.returncode == 0
    assert "intimacy_grade" in r.stdout
    assert "direct public business email" in r.stdout


def test_json_export_roundtrip(tmp_path: Path) -> None:
    payload = LeadResearchResult(
        contacts=[
            Contact(
                full_name="Z",
                title="T",
                decision_maker_score="medium",
                intimacy_grade="medium",
                fit_reason="f",
                contact_evidence_summary="s",
                evidence=[
                    Evidence(
                        source_url="https://example.com",
                        source_type="official_site",
                        quote_or_fact="q",
                    )
                ],
            )
        ]
    )
    path = tmp_path / "x.json"
    path.write_text(payload.model_dump_json(indent=2), encoding="utf-8")
    loaded = LeadResearchResult.model_validate_json(path.read_text(encoding="utf-8"))
    assert loaded.contacts[0].full_name == "Z"
