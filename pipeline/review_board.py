from __future__ import annotations

from pipeline.models import CandidateLead, ContactRoute, HotelOrg, ReviewRow


def _best_email(routes: list[ContactRoute]) -> str | None:
    emails = [r for r in routes if r.kind == "email"]
    for conf in ("high", "medium", "low"):
        for r in emails:
            if r.confidence == conf:
                return r.value
    return None


def _best_phone(routes: list[ContactRoute]) -> str | None:
    phones = [r for r in routes if r.kind == "phone"]
    for conf in ("high", "medium", "low"):
        for r in phones:
            if r.confidence == conf:
                return r.value
    return None


def _other_routes(routes: list[ContactRoute]) -> str | None:
    parts: list[str] = []
    for r in routes:
        if r.kind in ("email", "phone"):
            continue
        parts.append(f"{r.kind}:{r.value}")
    return "; ".join(parts) if parts else None


def _evidence_summary(c: CandidateLead) -> str:
    bits: list[str] = []
    for e in c.evidence[:5]:
        q = (e.snippet or "")[:240].replace("\n", " ")
        bits.append(q)
    return " | ".join(bits) if bits else ""


def _tier_sort_key(c: CandidateLead) -> tuple[int, int, str]:
    """
    Sort key ascending = better rows first (human review board order).
    Bands: T1 strong, T2 strong, T1 weak, T3 commercial-ish, rest.
    """
    conf = c.current_role_confidence
    strong = conf in ("high", "medium")
    tier = int(c.role_tier)
    if tier == 1 and strong:
        band = 0
    elif tier == 2 and strong:
        band = 1
    elif tier == 1:
        band = 2
    elif tier == 3 and c.role_family in ("sales_events", "reservations", "commercial_revenue"):
        band = 3
    else:
        band = 4
    conf_order = {"high": 0, "medium": 1, "low": 2, "conflict": 3}
    # Deprioritize contact-fill vs role: only use name as stable tie-break
    return (band, conf_order.get(conf, 2), c.full_name.lower())


def build_review_rows(hotel: HotelOrg, candidates: list[CandidateLead]) -> list[ReviewRow]:
    hotel_name = hotel.canonical_name or hotel.property_name or (hotel.domains[0] if hotel.domains else hotel.input_url)
    sorted_cs = sorted(candidates, key=_tier_sort_key)
    rows: list[ReviewRow] = []
    for c in sorted_cs:
        ev_urls = ";".join(e.url for e in c.evidence if e.url)
        rows.append(
            ReviewRow(
                hotel_name=str(hotel_name),
                hotel_url=hotel.input_url,
                candidate_id=c.candidate_id,
                full_name=c.full_name,
                title=c.title,
                company=c.company,
                role_tier=c.role_tier,
                role_family=c.role_family,
                current_role_confidence=c.current_role_confidence,
                best_email=_best_email(c.contact_routes),
                best_phone=_best_phone(c.contact_routes),
                linkedin_url=c.linkedin_url,
                other_routes=_other_routes(c.contact_routes),
                needs_human_review=c.needs_human_review,
                needs_contact_mining=c.needs_contact_mining,
                evidence_urls=ev_urls,
                evidence_summary=_evidence_summary(c),
                notes="; ".join(c.notes) if c.notes else "",
            )
        )
    return rows
