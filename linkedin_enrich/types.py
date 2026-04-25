from __future__ import annotations
from typing import Literal
from pydantic import BaseModel, Field


class ExperienceEntry(BaseModel):
    title: str | None = None
    organization: str | None = None
    date_range: str | None = None
    location: str | None = None
    description: str | None = None


class EducationEntry(BaseModel):
    school: str | None = None
    degree_or_field: str | None = None
    date_range: str | None = None


class LinkedInProfile(BaseModel):
    """Structured LinkedIn profile for lead enrichment. Produced by Grok from Exa-fetched markdown."""

    linkedin_url: str = Field(min_length=12)
    display_name: str | None = None
    headline: str | None = None
    location: str | None = None
    about: str | None = None
    experience: list[ExperienceEntry] = Field(default_factory=list)
    education: list[EducationEntry] = Field(default_factory=list)
    skills: list[str] = Field(default_factory=list)
    activity_themes: list[str] = Field(
        default_factory=list,
        max_length=10,
        description="Short bullets summarizing recent post/activity themes",
    )
    source_urls: list[str] = Field(default_factory=list, max_length=8)
    data_quality: Literal["strong", "partial", "weak"] = "partial"
    caveats: str | None = Field(default=None, max_length=800)
