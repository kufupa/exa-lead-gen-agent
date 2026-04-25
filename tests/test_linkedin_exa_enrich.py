# tests/test_linkedin_exa_enrich.py
"""Tests for linkedin_enrich types and logic."""
from __future__ import annotations
import pytest
from linkedin_enrich.types import LinkedInProfile, ExperienceEntry


def test_linkedin_profile_minimal():
    p = LinkedInProfile(
        linkedin_url="https://www.linkedin.com/in/test-person-123",
        display_name="Test Person",
    )
    assert p.headline is None
    assert p.experience == []
    assert p.data_quality == "partial"


def test_linkedin_profile_full():
    p = LinkedInProfile(
        linkedin_url="https://www.linkedin.com/in/test-person-123",
        display_name="Test Person",
        headline="CEO at TestCo",
        location="London, UK",
        about="Experienced leader.",
        experience=[
            ExperienceEntry(title="CEO", organization="TestCo", date_range="2020 - Present"),
            ExperienceEntry(title="VP", organization="OldCo", date_range="2015 - 2020"),
        ],
        data_quality="strong",
    )
    assert len(p.experience) == 2
    d = p.model_dump()
    assert d["experience"][0]["title"] == "CEO"
