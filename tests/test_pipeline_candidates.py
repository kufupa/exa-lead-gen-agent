from __future__ import annotations

from pipeline.candidates import (
    classify_role_family,
    classify_role_tier,
    dedupe_candidates,
    infer_current_role_confidence_from_text,
    normalize_name,
    parse_linkedin_result_title,
)
from pipeline.models import CandidateLead, SourceRef


def test_normalize_name() -> None:
    assert normalize_name("  Jane   Doe  ") == "Jane Doe"


def test_classify_gm_tier1() -> None:
    assert classify_role_tier("General Manager") == 1
    assert classify_role_family("General Manager") == "gm_ops"


def test_classify_sales_tier2() -> None:
    assert classify_role_tier("Director of Sales") == 2
    assert classify_role_family("Director of Sales") == "sales_events"


def test_former_title_low_confidence() -> None:
    assert infer_current_role_confidence_from_text("GM", "Former General Manager at X") == "low"


def test_parse_linkedin_title() -> None:
    n, t = parse_linkedin_result_title("Jane Doe - General Manager | LinkedIn")
    assert n == "Jane Doe"
    assert t == "General Manager"


def test_dedupe_merges_linkedin() -> None:
    s = SourceRef(url="https://www.linkedin.com/in/jane", title="Jane - GM", snippet="x")
    a = CandidateLead(
        candidate_id="a",
        full_name="Jane",
        title="GM",
        role_tier=1,
        role_family="gm_ops",
        current_role_confidence="high",
        evidence=[s],
        linkedin_url="https://www.linkedin.com/in/jane",
    )
    b = CandidateLead(
        candidate_id="b",
        full_name="Jane",
        title="GM",
        role_tier=1,
        role_family="gm_ops",
        current_role_confidence="high",
        evidence=[s],
        linkedin_url="https://www.linkedin.com/in/jane",
    )
    out = dedupe_candidates([a, b])
    assert len(out) == 1
