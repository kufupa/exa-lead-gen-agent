# tests/test_linkedin_exa_enrich.py
"""Tests for linkedin_enrich types and logic."""
from __future__ import annotations
import pytest
from pydantic import ValidationError
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


def test_linkedin_profile_rejects_invalid_data_quality():
    with pytest.raises(ValidationError):
        LinkedInProfile(
            linkedin_url="https://www.linkedin.com/in/test-person-123",
            display_name="Test Person",
            data_quality="excellent",
        )


def test_linkedin_profile_rejects_short_linkedin_url():
    with pytest.raises(ValidationError):
        LinkedInProfile(linkedin_url="short-url")


def test_linkedin_profile_rejects_activity_themes_over_max():
    with pytest.raises(ValidationError):
        LinkedInProfile(
            linkedin_url="https://www.linkedin.com/in/test-person-123",
            activity_themes=[f"theme {i}" for i in range(11)],
        )


def test_linkedin_profile_rejects_source_urls_over_max():
    with pytest.raises(ValidationError):
        LinkedInProfile(
            linkedin_url="https://www.linkedin.com/in/test-person-123",
            source_urls=[f"https://example.com/{i}" for i in range(9)],
        )


def test_linkedin_profile_rejects_caveats_over_max():
    with pytest.raises(ValidationError):
        LinkedInProfile(
            linkedin_url="https://www.linkedin.com/in/test-person-123",
            caveats="x" * 801,
        )
