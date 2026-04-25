from __future__ import annotations

from hotel_decision_maker_research import Contact, contact_fill_score


def direct_channel_score(contact: Contact) -> float:
    """
    Outreach-usable signal aligned with hotel_decision_maker_research.contact_fill_score.
    Higher = more direct channels (named email, phone, X, etc.).
    """
    return contact_fill_score(contact)


def needs_enrichment(contact: Contact, min_score: float) -> bool:
    """True when this row should be sent to xAI (below the skip threshold)."""
    return direct_channel_score(contact) < min_score


__all__ = ["direct_channel_score", "needs_enrichment"]
