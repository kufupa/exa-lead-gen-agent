from __future__ import annotations

import json

from pipeline.models import CandidateLead, HotelOrg, PipelineRunResult, PipelineTelemetry, ReviewRow, make_candidate_id


def test_make_candidate_id_stable() -> None:
    a = make_candidate_id("example.com", "Jane Doe", "GM")
    b = make_candidate_id("example.com", "Jane Doe", "GM")
    assert a == b
    assert a.startswith("c_")


def test_candidate_lead_json_roundtrip() -> None:
    c = CandidateLead(
        candidate_id="c_x",
        full_name="A",
        title="General Manager",
        role_tier=1,
        role_family="gm_ops",
        current_role_confidence="high",
    )
    s = c.model_dump_json()
    c2 = CandidateLead.model_validate_json(s)
    assert c2.full_name == "A"


def test_pipeline_run_result_dump() -> None:
    r = PipelineRunResult(
        hotel=HotelOrg(input_url="https://x.com"),
        candidates=[],
        review_rows=[],
        telemetry=PipelineTelemetry(),
    )
    d = r.model_dump()
    assert json.loads(json.dumps(d))["hotel"]["input_url"] == "https://x.com"


def test_review_row_fields() -> None:
    row = ReviewRow(
        hotel_name="H",
        hotel_url="https://h.com",
        candidate_id="c1",
        full_name="N",
        title="T",
        company=None,
        role_tier=2,
        role_family="sales_events",
        current_role_confidence="medium",
        best_email=None,
        best_phone=None,
        linkedin_url=None,
        other_routes=None,
        needs_human_review=False,
        needs_contact_mining=True,
        evidence_urls="",
        evidence_summary="",
        notes="",
    )
    assert row.role_tier == 2
