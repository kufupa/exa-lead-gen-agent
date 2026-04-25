"""Outreach email state: sync from intimate contacts, triage, xAI cold-email batch."""

from outreach.ids import compute_outreach_id, primary_delivery_email
from outreach.schema import STATE_VERSION, empty_state, validate_state

__all__ = [
    "STATE_VERSION",
    "compute_outreach_id",
    "empty_state",
    "primary_delivery_email",
    "validate_state",
]
