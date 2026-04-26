from __future__ import annotations

from pipeline.io import build_pipeline_ui_json
from pipeline.models import CandidateLead, HotelOrg, OrgAlias, PipelineTelemetry


def test_pipeline_ui_json_shape() -> None:
    tel = PipelineTelemetry()
    tel.exa_search_requests = 2
    tel.exa_content_pages = 1
    tel.stages = []
    hotel = HotelOrg(input_url="https://kayagnhlondon.com/", canonical_name="Kaya Great Northern Hotel")
    aliases = [OrgAlias(value="Kaya Great Northern Hotel", kind="property", confidence="high")]
    c = CandidateLead(
        candidate_id="c1",
        full_name="GM",
        title="General Manager",
        role_tier=1,
        role_family="gm_ops",
        current_role_confidence="high",
        relationship_confidence="high",
    )
    ui = build_pipeline_ui_json(
        input_url="https://kayagnhlondon.com/",
        resolved_org=hotel,
        aliases=aliases,
        candidates=[c],
        rejected_candidates=[],
        telemetry=tel,
        needs_manual_org_review=False,
    )
    d = ui.model_dump()
    assert d["input_url"].startswith("https://")
    assert "xai_usd" in d["provider_costs"]
    assert "exa_usd" in d["provider_costs"]
    assert d["quality_metrics"]["candidate_count"] == 1
    assert d["quality_metrics"]["tier1_count"] == 1
