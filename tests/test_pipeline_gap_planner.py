from __future__ import annotations

from pipeline.candidates import initial_hotel_from_url
from pipeline.gap_planner import plan_exa_jobs
from pipeline.grok_discovery import synthetic_grok_result_for_tests
from pipeline.models import GrokDiscoveryResult, OrgAlias


def test_gap_planner_kaya_not_manual() -> None:
    disc = synthetic_grok_result_for_tests()
    jobs, manual = plan_exa_jobs(disc, max_jobs=12)
    assert manual is False
    assert jobs
    kinds = {j.kind for j in jobs}
    assert "person_verify" in kinds
    # Synthetic draft already has General Manager title → missing_role job omitted


def test_gap_planner_weak_hostname_stops_exa() -> None:
    hotel = initial_hotel_from_url("https://Kayagnhlondon.com/")
    disc = GrokDiscoveryResult(
        hotel=hotel,
        aliases=[
            OrgAlias(value="Kayagnhlondon", kind="domain", confidence="medium", source_url=None, quote=None)
        ],
        drafts=[],
    )
    jobs, manual = plan_exa_jobs(disc, max_jobs=12)
    assert manual is True
    assert jobs == []


def test_gap_no_broad_hostname_query() -> None:
    disc = synthetic_grok_result_for_tests()
    jobs, _ = plan_exa_jobs(disc, max_jobs=12)
    for j in jobs:
        assert "Kayagnhlondon" not in j.query
