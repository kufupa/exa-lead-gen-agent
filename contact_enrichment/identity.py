from __future__ import annotations

import hashlib

from hotel_decision_maker_research import Contact, dedupe_key


def request_id(contact: Contact) -> str:
    """Stable short id for batch_request_id and ChannelResearchRow.match_id."""
    key = dedupe_key(contact)
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
    return f"c_{h}"


__all__ = ["dedupe_key", "request_id"]
