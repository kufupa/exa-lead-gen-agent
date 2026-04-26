from __future__ import annotations

from pydantic import BaseModel, Field


class PipelineConfig(BaseModel):
    """Runtime caps and feature flags (v4: Grok-led, capped Exa)."""

    max_candidates: int = Field(default=50, ge=1, le=200)
    max_exa_searches: int = Field(default=22, ge=0, le=200)
    max_exa_fetches: int = Field(default=8, ge=0, le=200)
    max_people_gap_searches: int = Field(default=10, ge=0, le=20)
    max_person_verify_searches: int = Field(default=10, ge=0, le=100)
    max_contact_route_exa_searches: int = Field(default=4, ge=0, le=12)
    max_source_chars_per_ref: int = Field(default=4000, ge=500, le=50_000)
    source_pack_max_candidates: int = Field(default=50, ge=5, le=200)
    grok_validation_model: str = "grok-4.20-reasoning"
    grok_max_turns: int = Field(default=8, ge=1, le=32)
    grok_chunk_size: int = Field(default=12, ge=4, le=30)
    skip_linkedin: bool = False
    skip_contact_mining: bool = False
    use_xai_for_contact_mining: bool = False

    def exa_search_cap(self) -> int:
        return self.max_exa_searches

    def exa_fetch_cap(self) -> int:
        return self.max_exa_fetches
