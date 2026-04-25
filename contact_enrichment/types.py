from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field

RowStatus = Literal["ok", "partial", "not_found", "error"]


class ChannelResearchRow(BaseModel):
    """Structured channel output from the enrichment model (one row per contact)."""

    match_id: str = Field(min_length=3, max_length=64, description="Must equal request_id for this contact")
    email: str | None = None
    email2: str | None = None
    phone: str | None = None
    phone2: str | None = None
    x_handle: str | None = None
    linkedin_url: str | None = None
    other_contact_detail: str | None = None
    source_urls: list[str] = Field(default_factory=list, max_length=8)
    status: RowStatus = "ok"
    notes: str | None = Field(default=None, max_length=500)
