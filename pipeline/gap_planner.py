from __future__ import annotations

import re
from typing import Iterable

from pipeline.grok_discovery import new_job_id
from pipeline.models import CandidateDraft, ExaJob, GrokDiscoveryResult, HotelOrg, OrgAlias


def _norm(s: str | None) -> str:
    return (s or "").strip()


def _alias_strings(hotel: HotelOrg, aliases: list[OrgAlias]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for a in aliases:
        if a.confidence not in ("high", "medium"):
            continue
        v = _norm(a.value)
        if len(v) < 3 or v.lower() in seen:
            continue
        seen.add(v.lower())
        out.append(v)
    for fld in (hotel.canonical_name, hotel.property_name, hotel.brand_name):
        v = _norm(fld)
        if len(v) >= 3 and v.lower() not in seen:
            seen.add(v.lower())
            out.append(v)
    return out


def _bare_hostname_alias(hotel: HotelOrg, aliases: list[OrgAlias]) -> bool:
    """True if best usable anchor looks like hostname token only (weak org resolution)."""
    pool = _alias_strings(hotel, aliases)
    if not pool:
        return True
    dom = (hotel.domains[0] if hotel.domains else "").lower().split(".")[0]
    if not dom:
        return False
    token = dom.replace("-", "").replace("_", "")
    for a in pool:
        al = re.sub(r"[^a-z0-9]+", "", a.lower())
        if al and al != token and len(al) > len(token) + 2:
            return False
    return True


def _draft_titles_lower(drafts: Iterable[CandidateDraft]) -> str:
    return " ".join((_norm(d.title) or "").lower() for d in drafts)


def plan_exa_jobs(
    discovery: GrokDiscoveryResult,
    *,
    max_jobs: int = 12,
) -> tuple[list[ExaJob], bool]:
    """
    Build capped Exa jobs from Grok discovery.
    Returns (jobs, needs_manual_org_review) when org anchor is hostname-weak.
    """
    hotel = discovery.hotel
    alias_list = _alias_strings(hotel, discovery.aliases)
    weak_org = _bare_hostname_alias(hotel, discovery.aliases)
    if weak_org or not alias_list:
        return [], True

    primary = alias_list[0]
    jobs: list[ExaJob] = []
    titles_blob = _draft_titles_lower(discovery.drafts)

    for d in discovery.drafts:
        if len(jobs) >= max_jobs:
            break
        did = d.draft_id
        if not did or not _norm(d.full_name):
            continue
        q = f'"{_norm(d.full_name)}" "{primary}"'
        jobs.append(
            ExaJob(
                job_id=new_job_id(),
                kind="person_verify",
                query=q,
                candidate_id=did,
                category=None,
                max_results=5,
            )
        )

    if "general manager" not in titles_blob and len(jobs) < max_jobs:
        jobs.append(
            ExaJob(
                job_id=new_job_id(),
                kind="missing_role",
                query=f'"{primary}" "general manager"',
                candidate_id=None,
                category=None,
                max_results=5,
            )
        )

    for d in discovery.drafts:
        if len(jobs) >= max_jobs:
            break
        if d.linkedin_url or not _norm(d.company):
            continue
        did = d.draft_id
        if not did:
            continue
        q = f'"{_norm(d.full_name)}" "{_norm(d.company)}"'
        jobs.append(
            ExaJob(
                job_id=new_job_id(),
                kind="profile_lookup",
                query=q,
                candidate_id=did,
                category="people",
                max_results=5,
            )
        )

    return jobs[:max_jobs], False
