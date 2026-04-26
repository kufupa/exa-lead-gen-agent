from __future__ import annotations

from pipeline.models import CandidateLead, ContactRoute, HotelOrg
from pipeline.review_board import build_review_rows


def test_review_board_orders_tier1_before_tier3() -> None:
    hotel = HotelOrg(input_url="https://x.com", property_name="X")
    low = CandidateLead(
        candidate_id="t3",
        full_name="Sales Mgr",
        title="Sales Manager",
        role_tier=3,
        role_family="sales_events",
        current_role_confidence="high",
        contact_routes=[ContactRoute(kind="email", value="a@x.com", confidence="high", source_url="https://x")],
    )
    high = CandidateLead(
        candidate_id="t1",
        full_name="CEO",
        title="CEO",
        role_tier=1,
        role_family="owner_exec",
        current_role_confidence="high",
        contact_routes=[],
    )
    rows = build_review_rows(hotel, [low, high])
    assert rows[0].full_name == "CEO"
    assert rows[1].full_name == "Sales Mgr"
