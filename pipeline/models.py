from __future__ import annotations

import hashlib
import re
from typing import Literal

from pydantic import BaseModel, Field

SourceType = Literal["hotel_site", "linkedin", "press", "directory", "company_site", "social", "other"]
RoleFamily = Literal[
    "owner_exec",
    "gm_ops",
    "commercial_revenue",
    "sales_events",
    "reservations",
    "it_digital",
    "procurement_finance",
    "other",
]
RoleTier = Literal[1, 2, 3, 4]
RoleConfidence = Literal["high", "medium", "low", "conflict"]
RelationshipConfidence = Literal["high", "medium", "low", "reject"]
OrgAliasKind = Literal["property", "brand", "management", "ownership", "domain", "historical"]
AliasConfidence = Literal["high", "medium", "low"]
ExaJobKind = Literal["person_verify", "missing_role", "contact_route", "profile_lookup"]
ContactKind = Literal["email", "phone", "linkedin", "contact_form", "generic_email", "switchboard", "pattern", "unknown"]
ContactConfidence = Literal["high", "medium", "low"]


class SourceRef(BaseModel):
    url: str
    title: str | None = None
    source_type: SourceType = "other"
    snippet: str | None = None
    fetched_text: str | None = None
    published_date: str | None = None
    query: str | None = None
    score: float | None = None


class HotelOrg(BaseModel):
    input_url: str
    canonical_name: str | None = None
    property_name: str | None = None
    brand_name: str | None = None
    management_company: str | None = None
    ownership_group: str | None = None
    location_hint: str | None = None
    domains: list[str] = Field(default_factory=list)
    evidence: list[SourceRef] = Field(default_factory=list)


class ContactRoute(BaseModel):
    kind: ContactKind
    value: str
    confidence: ContactConfidence
    source_url: str | None = None
    rationale: str | None = None


class OrgAlias(BaseModel):
    value: str
    kind: OrgAliasKind
    confidence: AliasConfidence
    source_url: str | None = None
    quote: str | None = None


class CandidateDraft(BaseModel):
    """Grok discovery row before Exa verification."""

    draft_id: str | None = None
    full_name: str
    title: str | None = None
    company: str | None = None
    role_family_hint: str | None = None
    evidence: list[SourceRef] = Field(default_factory=list)
    contact_routes: list[ContactRoute] = Field(default_factory=list)
    linkedin_url: str | None = None
    confidence_hint: AliasConfidence = "medium"
    uncertainty: str | None = None


class GrokDiscoveryResult(BaseModel):
    hotel: HotelOrg
    aliases: list[OrgAlias] = Field(default_factory=list)
    drafts: list[CandidateDraft] = Field(default_factory=list)


class ExaJob(BaseModel):
    job_id: str
    kind: ExaJobKind
    query: str
    candidate_id: str | None = None
    category: Literal["people"] | None = None
    max_results: int = 5


class CandidateLead(BaseModel):
    candidate_id: str
    full_name: str
    title: str | None = None
    normalized_title: str | None = None
    company: str | None = None
    role_tier: RoleTier = 4
    role_family: RoleFamily = "other"
    current_role_confidence: RoleConfidence = "medium"
    relationship_confidence: RelationshipConfidence | None = None
    evidence: list[SourceRef] = Field(default_factory=list)
    contact_routes: list[ContactRoute] = Field(default_factory=list)
    linkedin_url: str | None = None
    needs_human_review: bool = False
    needs_contact_mining: bool = False
    reason_kept: str | None = None
    notes: list[str] = Field(default_factory=list)


class ReviewRow(BaseModel):
    hotel_name: str
    hotel_url: str
    candidate_id: str
    full_name: str
    title: str | None = None
    company: str | None = None
    role_tier: RoleTier
    role_family: RoleFamily
    current_role_confidence: RoleConfidence
    best_email: str | None = None
    best_phone: str | None = None
    linkedin_url: str | None = None
    other_routes: str | None = None
    needs_human_review: bool
    needs_contact_mining: bool
    evidence_urls: str
    evidence_summary: str
    notes: str


class StageTelemetry(BaseModel):
    stage: str
    provider: Literal["exa", "xai", "local"]
    calls: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    estimated_usd: float = 0.0
    seconds: float = 0.0
    notes: list[str] = Field(default_factory=list)


class PipelineTelemetry(BaseModel):
    stages: list[StageTelemetry] = Field(default_factory=list)
    exa_search_requests: int = 0
    exa_content_pages: int = 0
    errors: list[str] = Field(default_factory=list)


class PipelineRunResult(BaseModel):
    hotel: HotelOrg
    candidates: list[CandidateLead]
    review_rows: list[ReviewRow]
    telemetry: PipelineTelemetry
    source_pack_json: str | None = None


class PipelineUiJson(BaseModel):
    """Single UI-ready artifact for pipeline v4."""

    input_url: str
    resolved_org: HotelOrg
    aliases: list[OrgAlias]
    candidates: list[CandidateLead]
    rejected_candidates: list[CandidateDraft] = Field(default_factory=list)
    provider_costs: dict[str, float] = Field(default_factory=dict)
    quality_metrics: dict[str, int | float] = Field(default_factory=dict)
    telemetry: PipelineTelemetry
    needs_manual_org_review: bool = False


def _slug(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return re.sub(r"_+", "_", s).strip("_") or "x"


def make_candidate_id(hotel_key: str, full_name: str, title: str | None) -> str:
    """Stable id from hotel scope + normalized name + title."""
    parts = "|".join(
        [
            _slug(hotel_key),
            _slug(full_name or ""),
            _slug(title or ""),
        ]
    )
    h = hashlib.sha256(parts.encode("utf-8")).hexdigest()[:16]
    return f"c_{h}"


def hotel_key_from_org(hotel: HotelOrg) -> str:
    if hotel.domains:
        return hotel.domains[0]
    from urllib.parse import urlparse

    p = urlparse(hotel.input_url)
    return (p.netloc or "nohost").lower()
