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


from unittest.mock import MagicMock
from linkedin_enrich.exa_fetch import discover_linkedin_urls, fetch_linkedin_profiles, normalize_linkedin_url


def test_normalize_linkedin_url():
    assert normalize_linkedin_url("https://www.linkedin.com/in/foo/") == "https://www.linkedin.com/in/foo"
    assert normalize_linkedin_url("https://uk.linkedin.com/in/foo") == "https://www.linkedin.com/in/foo"
    assert normalize_linkedin_url("http://linkedin.com/in/foo") == "https://www.linkedin.com/in/foo"


def test_fetch_deduplicates_urls():
    """Two contacts with same normalized URL should produce one Exa call."""
    mock_exa = MagicMock()
    mock_result = MagicMock()
    mock_result.results = [MagicMock(url="https://www.linkedin.com/in/foo", text="# Foo\nCEO at Bar")]
    mock_exa.get_contents.return_value = mock_result

    urls = [
        "https://www.linkedin.com/in/foo",
        "https://uk.linkedin.com/in/foo/",
        "https://www.linkedin.com/in/foo",
    ]
    result = fetch_linkedin_profiles(mock_exa, urls)
    assert mock_exa.get_contents.call_count == 1
    assert "https://www.linkedin.com/in/foo" in result
    assert "# Foo" in result["https://www.linkedin.com/in/foo"]


def test_discover_linkedin_urls_uses_full_name_contract():
    mock_exa = MagicMock()
    mock_result = MagicMock()
    mock_result.results = [
        MagicMock(url="https://www.linkedin.com/in/test-person/", text=""),
    ]
    mock_exa.search.return_value = mock_result

    contacts = [{"full_name": "Test Person", "company": "TestCo"}]
    discovered = discover_linkedin_urls(mock_exa, contacts)

    assert mock_exa.search.call_count == 1
    assert discovered["Test Person|TestCo"] == "https://www.linkedin.com/in/test-person"
