"""Tests for hotel_decision_maker_research."""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest
from pydantic import ValidationError

from hotel_decision_maker_research import (
    Contact,
    Evidence,
    LeadResearchResult,
    append_csv,
    build_parser,
    build_round1_prompt,
    build_round3_user_message,
    dedupe_and_rank,
    default_json_path_from_url,
    is_linkedin_only_evidence,
    normalize_contact_bounds,
    process_contacts,
    read_csv_contacts,
    recompute_intimacy,
    rewrite_csv_deduped,
    utility_score,
    utility_score_v2,
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
    normalize_contact_bounds(args)
    assert args.agent_count == 16
    assert args.min_contacts == 10
    assert args.target_contacts == 25
    assert args.max_contacts == 50
    assert args.out_json is None
    assert args.out_csv == "csv/hotel_leads.csv"
    assert args.no_csv is False


def test_default_json_path_from_url_pattern() -> None:
    p = default_json_path_from_url("https://WWW.Example.COM/hotel/stay")
    assert p.startswith("jsons/hotel_leads__")
    assert p.endswith(".json")
    assert "www_example_com" in p
    assert re.search(r"__[0-9a-f]{8}\.json$", p)


def test_default_json_path_differs_by_url() -> None:
    a = default_json_path_from_url("https://a.example.com/")
    b = default_json_path_from_url("https://b.example.com/")
    assert a != b


def test_append_csv_single_header(tmp_path: Path) -> None:
    path = str(tmp_path / "leads.csv")
    ev = [
        Evidence(
            source_url="https://h.com",
            source_type="official_site",
            quote_or_fact="Bio",
        ),
        Evidence(
            source_url="https://h.com/2",
            source_type="official_site",
            quote_or_fact="Bio2",
        ),
    ]
    c1 = Contact(
        full_name="N1",
        title="T1",
        decision_maker_score="high",
        intimacy_grade="high",
        fit_reason="f",
        contact_evidence_summary="s",
        evidence=ev,
    )
    c2 = c1.model_copy(update={"full_name": "N2", "title": "T2"})
    append_csv(path, [c1], source_target_url="https://h.com", generated_at_utc="2026-01-01T00:00:00+00:00")
    append_csv(path, [c2], source_target_url="https://h.com", generated_at_utc="2026-01-02T00:00:00+00:00")
    lines = Path(path).read_text(encoding="utf-8").strip().splitlines()
    assert sum(1 for line in lines if line.startswith("full_name,")) == 1
    assert len(lines) == 3


def test_rewrite_csv_deduped_keeps_best_utility(tmp_path: Path) -> None:
    path = str(tmp_path / "d.csv")
    ev = [
        Evidence(
            source_url="https://x.com/a",
            source_type="official_site",
            quote_or_fact="a",
        ),
        Evidence(
            source_url="https://x.com/b",
            source_type="official_site",
            quote_or_fact="b",
        ),
    ]
    low = Contact(
        full_name="Same",
        title="VP",
        linkedin_url="https://linkedin.com/in/same",
        decision_maker_score="low",
        intimacy_grade="low",
        fit_reason="f",
        contact_evidence_summary="s",
        evidence=ev,
    )
    high = low.model_copy(
        update={"decision_maker_score": "high", "intimacy_grade": "high", "title": "VP Sales"}
    )
    rewrite_csv_deduped(path, [low, high])
    rows = read_csv_contacts(path)
    assert len(rows) == 1
    assert rows[0].decision_maker_score == "high"


def test_prompt_mentions_intimacy_grade_rules() -> None:
    prompt = build_round1_prompt("https://hotel.example", 10, 25, 50)
    assert "intimacy_grade" in prompt
    assert "direct public business email" in prompt
    assert "10" in prompt
    assert "25" in prompt
    assert "50" in prompt


def test_round3_prompt_rejects_shrinkage_phrase() -> None:
    p = build_round3_user_message(10, 25, 50)
    assert "fewer is fine" not in p.lower()
    assert "at least 10" in p
    assert "target ~25" in p


def test_normalize_max_contacts_clamped() -> None:
    args = build_parser().parse_args(
        ["--url", "https://x.com", "--max-contacts", "99", "--target-contacts", "60", "--min-contacts", "5"]
    )
    normalize_contact_bounds(args)
    assert args.max_contacts == 50
    assert args.min_contacts == 5
    assert args.target_contacts == 50


def test_utility_score_v2_prefers_contact_fill() -> None:
    ev = [
        Evidence(
            source_url="https://o.com",
            source_type="official_site",
            quote_or_fact="Bio",
        ),
        Evidence(
            source_url="https://o.com/2",
            source_type="official_site",
            quote_or_fact="Bio2",
        ),
    ]
    bare = Contact(
        full_name="Exec",
        title="CEO",
        decision_maker_score="high",
        intimacy_grade="high",
        fit_reason="f",
        contact_evidence_summary="s",
        evidence=ev,
    )
    rich = bare.model_copy(
        update={
            "full_name": "Mgr",
            "title": "Mgr",
            "decision_maker_score": "high",
            "intimacy_grade": "high",
            "email": "mgr@hotel.com",
            "phone": "+44 20 7138 0000",
        }
    )
    assert utility_score_v2(rich) > utility_score_v2(bare)


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
    assert "out_json:" in r.stdout
    assert "hotel_leads__" in r.stdout
    assert "min=10" in r.stdout
    assert "target=25" in r.stdout
    assert "max=50" in r.stdout


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
