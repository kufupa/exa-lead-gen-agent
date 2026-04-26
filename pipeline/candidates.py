from __future__ import annotations

import re
from typing import Iterable
from urllib.parse import urlparse

from pipeline.models import (
    CandidateDraft,
    CandidateLead,
    ContactRoute,
    GrokDiscoveryResult,
    HotelOrg,
    OrgAlias,
    RelationshipConfidence,
    RoleConfidence,
    RoleFamily,
    RoleTier,
    SourceRef,
    SourceType,
    hotel_key_from_org,
    make_candidate_id,
)


def normalize_name(name: str) -> str:
    s = (name or "").strip()
    s = re.sub(r"\s+", " ", s)
    return s


def normalize_title(title: str | None) -> str:
    if not title:
        return ""
    t = title.strip().lower()
    t = re.sub(r"\s+", " ", t)
    return t


_FORMER_MARKERS = re.compile(
    r"\b(former|ex-|ex\s|previous|previously|retired|was\s+the|past\s+)\b",
    re.I,
)


def infer_current_role_confidence_from_text(title: str | None, snippet: str | None) -> RoleConfidence:
    blob = f"{title or ''} {snippet or ''}"
    if _FORMER_MARKERS.search(blob):
        return "low"
    return "medium"


def classify_role_family(title: str | None) -> RoleFamily:
    t = normalize_title(title)
    if not t:
        return "other"
    if any(
        x in t
        for x in (
            "owner",
            "founder",
            "co-founder",
            "ceo",
            "chief executive",
            "managing director",
            "chair",
            "board",
        )
    ):
        return "owner_exec"
    if any(
        x in t
        for x in (
            "general manager",
            "hotel manager",
            "property manager",
            "operations director",
            "operations manager",
            "front office",
        )
    ):
        return "gm_ops"
    if any(
        x in t
        for x in (
            "commercial",
            "revenue",
            "yield",
            "distribution",
            "marketing director",
            "marketing manager",
        )
    ):
        return "commercial_revenue"
    if any(x in t for x in ("sales", "business development", "events", "groups", "mice", "catering")):
        return "sales_events"
    if "reservation" in t or "reservations" in t:
        return "reservations"
    if any(x in t for x in ("it ", " i.t.", "technology", "digital", "systems", "cio", "cto")):
        return "it_digital"
    if any(x in t for x in ("procurement", "finance director", "financial controller", "cfo")):
        return "procurement_finance"
    return "other"


def _alias_match_strings(hotel: HotelOrg, aliases: list[OrgAlias]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for a in aliases:
        v = (a.value or "").strip()
        if len(v) < 2 or v.lower() in seen:
            continue
        seen.add(v.lower())
        out.append(v)
    for fld in (hotel.canonical_name, hotel.property_name, hotel.brand_name, hotel.management_company):
        v = (fld or "").strip()
        if len(v) >= 2 and v.lower() not in seen:
            seen.add(v.lower())
            out.append(v)
    return out


def relationship_confidence_for_draft(
    draft: CandidateDraft,
    hotel: HotelOrg,
    aliases: list[OrgAlias],
    extra_sources: list[SourceRef] | None = None,
) -> RelationshipConfidence:
    """Local deterministic relationship tier using org aliases and evidence text."""
    alias_vals = _alias_match_strings(hotel, aliases)
    parts: list[str] = [draft.title or "", draft.company or ""]
    for src in list(draft.evidence) + list(extra_sources or []):
        parts.append(src.snippet or "")
        parts.append(src.fetched_text or "")
        parts.append(src.title or "")
    blob = " ".join(parts).lower()
    if not blob.strip():
        return "low"

    for a in alias_vals:
        al = a.lower()
        if len(al) >= 4 and al in blob:
            return "high"

    if draft.company:
        cl = draft.company.lower()
        for a in alias_vals:
            if len(a) >= 4 and (a.lower() in cl or cl in a.lower()):
                return "medium"

    if draft.linkedin_url and "linkedin.com" in draft.linkedin_url.lower():
        return "medium"

    # Hostname-only company matching property domain (weak)
    dom = (hotel.domains[0] if hotel.domains else "").split(".")[0].lower()
    name_tok = normalize_name(draft.full_name).lower().replace(" ", "")
    if dom and len(name_tok) <= 3:
        return "reject"
    if dom and name_tok == dom[: max(3, len(name_tok))]:
        return "reject"

    if classify_role_tier(draft.title) <= 2 and not alias_vals:
        return "low"
    return "low"


def classify_role_tier(title: str | None) -> RoleTier:
    t = normalize_title(title)
    if not t:
        return 4
    tier1 = (
        "owner",
        "founder",
        "co-founder",
        "chief executive",
        "ceo",
        "managing director",
        "general manager",
        "hotel manager",
        "property manager",
    )
    if any(x in t for x in tier1):
        return 1
    tier2 = (
        "commercial director",
        "revenue director",
        "revenue manager",
        "director of sales",
        "sales director",
        "marketing director",
        "events director",
        "groups",
        "reservations manager",
        "reservations director",
        "it director",
        "technology director",
        "digital director",
        "chief information",
        "procurement",
        "finance director",
    )
    if any(x in t for x in tier2):
        return 2
    tier3 = (
        "front office manager",
        "f&b",
        "food and beverage",
        "rooms division",
        "meetings",
        "sales manager",
        "events manager",
        "reservations supervisor",
        "assistant",
    )
    if any(x in t for x in tier3):
        return 3
    if "director" in t or "manager" in t or "head" in t:
        return 3
    return 4


def parse_linkedin_result_title(title: str | None) -> tuple[str, str | None]:
    """Best-effort: 'Name - Title | LinkedIn' -> (name, title)."""
    if not title:
        return ("Unknown", None)
    s = title.replace(" | LinkedIn", "").replace(" | linkedin", "").strip()
    if " - " in s:
        a, b = s.split(" - ", 1)
        return (normalize_name(a), b.strip() or None)
    if " – " in s:
        a, b = s.split(" – ", 1)
        return (normalize_name(a), b.strip() or None)
    return (normalize_name(s), None)


def source_type_from_url(url: str) -> SourceType:
    u = (url or "").lower()
    if "linkedin.com" in u:
        return "linkedin"
    if any(x in u for x in ("/press", "/news", "news.", "hospitalitynet", "boutiquehotel")):
        return "press"
    return "other"


def candidate_from_linkedin_source(ref: SourceRef, hotel: HotelOrg) -> CandidateLead | None:
    if "linkedin.com/in/" not in (ref.url or "").lower():
        return None
    name, title = parse_linkedin_result_title(ref.title)
    if name == "Unknown" and ref.snippet:
        name = normalize_name(ref.snippet.split("\n")[0][:80])
    rt = classify_role_tier(title)
    rf = classify_role_family(title)
    conf = infer_current_role_confidence_from_text(title, ref.snippet)
    key = hotel_key_from_org(hotel)
    cid = make_candidate_id(key, name, title)
    st = source_type_from_url(ref.url)
    ev = SourceRef(
        url=ref.url,
        title=ref.title,
        source_type=st,
        snippet=ref.snippet,
        fetched_text=ref.fetched_text,
        published_date=ref.published_date,
        query=ref.query,
        score=ref.score,
    )
    routes: list[ContactRoute] = []
    if ref.url:
        routes.append(
            ContactRoute(
                kind="linkedin",
                value=ref.url,
                confidence="medium",
                source_url=ref.url,
                rationale="LinkedIn profile URL from search result",
            )
        )
    return CandidateLead(
        candidate_id=cid,
        full_name=name,
        title=title,
        normalized_title=normalize_title(title) or None,
        company=None,
        role_tier=rt,
        role_family=rf,
        current_role_confidence=conf,
        evidence=[ev],
        contact_routes=routes,
        linkedin_url=ref.url,
        needs_human_review=conf in ("low", "conflict"),
        needs_contact_mining=True,
        notes=[],
    )


def dedupe_key_candidate(c: CandidateLead) -> str:
    if c.linkedin_url and c.linkedin_url.strip():
        return "li:" + c.linkedin_url.strip().lower()
    return "nm:" + normalize_name(c.full_name).lower() + "|" + normalize_title(c.title)


def merge_evidence(a: list[SourceRef], b: list[SourceRef]) -> list[SourceRef]:
    seen: set[str] = set()
    out: list[SourceRef] = []
    for s in a + b:
        u = (s.url or "").strip()
        if not u or u in seen:
            continue
        seen.add(u)
        out.append(s)
    return out


def promote_discovery_to_candidates(
    discovery: GrokDiscoveryResult,
    exa_by_draft: dict[str, list[SourceRef]],
) -> tuple[list[CandidateLead], list[CandidateDraft]]:
    """Turn drafts into CandidateLead rows or rejected drafts from local relationship checks."""
    hotel = discovery.hotel
    kept: list[CandidateLead] = []
    rejected: list[CandidateDraft] = []
    key = hotel_key_from_org(hotel)

    for d in discovery.drafts:
        did = d.draft_id or make_candidate_id(key, d.full_name, d.title)
        extra = exa_by_draft.get(did, [])
        rel = relationship_confidence_for_draft(d, hotel, discovery.aliases, extra)
        if rel == "reject":
            rejected.append(d.model_copy(update={"draft_id": did}))
            continue
        merged_ev = merge_evidence(list(d.evidence), list(extra))
        rt = classify_role_tier(d.title)
        rf = classify_role_family(d.title)
        conf = infer_current_role_confidence_from_text(
            d.title,
            " ".join(s.snippet or "" for s in merged_ev[:3]),
        )
        routes = list(d.contact_routes)
        notes: list[str] = []
        if d.uncertainty:
            notes.append(d.uncertainty)
        lead = CandidateLead(
            candidate_id=did,
            full_name=normalize_name(d.full_name),
            title=d.title,
            normalized_title=normalize_title(d.title) or None,
            company=d.company,
            role_tier=rt,
            role_family=rf,
            current_role_confidence=conf,
            relationship_confidence=rel,
            evidence=merged_ev,
            contact_routes=routes,
            linkedin_url=d.linkedin_url,
            needs_human_review=conf in ("low", "conflict") or rel == "low",
            needs_contact_mining=rt in (1, 2, 3),
            reason_kept=None,
            notes=notes,
        )
        kept.append(lead)
    return dedupe_candidates(kept), rejected


def dedupe_candidates(candidates: Iterable[CandidateLead]) -> list[CandidateLead]:
    by_key: dict[str, CandidateLead] = {}
    for c in candidates:
        k = dedupe_key_candidate(c)
        if k not in by_key:
            by_key[k] = c
            continue
        existing = by_key[k]
        merged_ev = merge_evidence(existing.evidence, c.evidence)
        routes = list(existing.contact_routes)
        seen_r = {(r.kind, r.value.lower()) for r in routes}
        for r in c.contact_routes:
            sig = (r.kind, r.value.lower())
            if sig not in seen_r:
                routes.append(r)
                seen_r.add(sig)
        notes = list(existing.notes)
        for n in c.notes:
            if n and n not in notes:
                notes.append(n)
        by_key[k] = existing.model_copy(
            update={
                "evidence": merged_ev,
                "contact_routes": routes,
                "notes": notes,
                "needs_human_review": existing.needs_human_review or c.needs_human_review,
                "needs_contact_mining": existing.needs_contact_mining or c.needs_contact_mining,
                "linkedin_url": existing.linkedin_url or c.linkedin_url,
                "title": existing.title or c.title,
                "company": existing.company or c.company,
            }
        )
    return list(by_key.values())


def domain_from_url(url: str) -> str:
    p = urlparse(url if url.startswith("http") else "https://" + url)
    return (p.netloc or "").lower().lstrip("www.")


def initial_hotel_from_url(input_url: str) -> HotelOrg:
    u = input_url.strip()
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    d = domain_from_url(u)
    return HotelOrg(
        input_url=u,
        property_name=None,
        domains=[d] if d else [],
        evidence=[],
    )
