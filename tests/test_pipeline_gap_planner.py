from __future__ import annotations

from pipeline.candidates import initial_hotel_from_url, leads_from_people_gap_sources
from pipeline.gap_planner import plan_exa_jobs
from pipeline.grok_discovery import assign_draft_ids, synthetic_grok_result_for_tests
from pipeline.models import GrokDiscoveryResult, OrgAlias, SourceRef


def test_gap_planner_kaya_not_manual() -> None:
    disc = synthetic_grok_result_for_tests()
    jobs, manual = plan_exa_jobs(disc, max_jobs=22)
    assert manual is False
    assert jobs
    kinds = {j.kind for j in jobs}
    assert "person_verify" in kinds
    assert "people_gap" in kinds
    # Synthetic draft already has General Manager title → GM people_gap job omitted


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
    jobs, _ = plan_exa_jobs(disc, max_jobs=22)
    for j in jobs:
        assert "Kayagnhlondon" not in j.query


def test_people_gap_adds_general_manager_when_absent() -> None:
    base = synthetic_grok_result_for_tests()
    d0 = base.drafts[0].model_copy(update={"title": "Head of Revenue"})
    disc = assign_draft_ids(GrokDiscoveryResult(hotel=base.hotel, aliases=base.aliases, drafts=[d0]))
    jobs, _ = plan_exa_jobs(disc, max_jobs=22)
    gm_gap = [j for j in jobs if j.kind == "people_gap" and "general manager" in j.query.lower()]
    assert gm_gap
    assert gm_gap[0].category == "people"


def test_people_gap_skips_general_manager_when_title_present() -> None:
    disc = synthetic_grok_result_for_tests()
    jobs, _ = plan_exa_jobs(disc, max_jobs=22)
    gm_gap = [j for j in jobs if j.kind == "people_gap" and "general manager" in j.query.lower()]
    assert not gm_gap


def test_leads_from_people_gap_linkedin_and_email() -> None:
    disc = synthetic_grok_result_for_tests()
    hotel = disc.hotel
    aliases = list(disc.aliases)
    src_li = SourceRef(
        url="https://www.linkedin.com/in/example-exec",
        title="Alex Exec - Managing Director | LinkedIn",
        snippet="Reach alex@hotel.test",
        query="q",
    )
    leads = leads_from_people_gap_sources(hotel, aliases, [src_li])
    assert leads
    kinds = {r.kind for r in leads[0].contact_routes}
    assert "linkedin" in kinds
    assert "email" in kinds


def test_leads_from_people_gap_non_linkedin_email_route() -> None:
    disc = synthetic_grok_result_for_tests()
    hotel = disc.hotel
    aliases = list(disc.aliases)
    src = SourceRef(
        url="https://press.example.com/article",
        title="Sam Owner - Founder",
        snippet="Contact sam.owner@example.com for details.",
        query="q",
    )
    leads = leads_from_people_gap_sources(hotel, aliases, [src])
    assert leads
    assert any(r.kind == "email" for r in leads[0].contact_routes)
