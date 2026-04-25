"""Tests for contact_enrichment gates, identity, merge."""

from __future__ import annotations

import pytest

from hotel_decision_maker_research import Contact, Evidence

from contact_enrichment.gates import direct_channel_score, needs_enrichment
from contact_enrichment.identity import request_id
from contact_enrichment.merge import apply_row, merge_by_request_id
from contact_enrichment.types import ChannelResearchRow


def _contact(**kwargs: object) -> Contact:
    base = dict(
        full_name="Jane Doe",
        title="GM",
        company="Hotel X",
        decision_maker_score="high",
        intimacy_grade="low",
        fit_reason="r",
        contact_evidence_summary="s",
        evidence=[
            Evidence(
                source_url="https://x.com/a",
                source_type="official_site",
                quote_or_fact="Bio",
            ),
            Evidence(
                source_url="https://x.com/b",
                source_type="official_site",
                quote_or_fact="Bio2",
            ),
        ],
    )
    base.update(kwargs)
    return Contact(**base)


def test_request_id_stable() -> None:
    c = _contact(linkedin_url="https://linkedin.com/in/jane")
    assert request_id(c) == request_id(c)
    assert request_id(c).startswith("c_")


def test_needs_enrichment_respects_threshold() -> None:
    bare = _contact()
    rich = _contact(email="jane@hotelx.com", phone="+44 20 0000 0000")
    assert needs_enrichment(bare, 1.5) is True
    assert needs_enrichment(rich, 1.5) is False
    assert direct_channel_score(rich) >= 1.5


def test_apply_row_fill_only() -> None:
    c = _contact(email=None)
    row = ChannelResearchRow(
        match_id="c_test_match_id_01",
        email="jane@hotelx.com",
        phone=None,
        status="ok",
    )
    out = apply_row(c, row, overwrite=False)
    assert out.email == "jane@hotelx.com"


def test_apply_row_no_overwrite_by_default() -> None:
    c = _contact(email="old@hotelx.com")
    row = ChannelResearchRow(match_id="c_test_match_id_02", email="new@hotelx.com", status="ok")
    out = apply_row(c, row, overwrite=False)
    assert out.email == "old@hotelx.com"


def test_apply_row_overwrite() -> None:
    c = _contact(email="old@hotelx.com")
    row = ChannelResearchRow(match_id="c_test_match_id_03", email="new@hotelx.com", status="ok")
    out = apply_row(c, row, overwrite=True)
    assert out.email == "new@hotelx.com"


def test_merge_by_request_id() -> None:
    c = _contact(linkedin_url="https://linkedin.com/in/x")
    rid = request_id(c)
    rows = {rid: ChannelResearchRow(match_id=rid, x_handle="janedoe", status="ok")}
    out = merge_by_request_id([c], rows, request_id_fn=request_id, overwrite=False)
    assert out[0].x_handle == "janedoe"
