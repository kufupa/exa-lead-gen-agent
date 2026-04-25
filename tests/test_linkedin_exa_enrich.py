# tests/test_linkedin_exa_enrich.py
"""Tests for linkedin_enrich types and logic."""
from __future__ import annotations
import pytest
import types
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
from linkedin_enrich.grok_structure import build_structuring_prompt, structure_profile


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


def test_discover_linkedin_urls_does_not_fallback_to_name():
    mock_exa = MagicMock()
    mock_result = MagicMock()
    mock_result.results = [
        MagicMock(url="https://www.linkedin.com/in/no-fallback/", text=""),
    ]
    mock_exa.search.return_value = mock_result

    contacts = [{"name": "Fallback Name", "company": "TestCo"}]
    discovered = discover_linkedin_urls(mock_exa, contacts)

    assert mock_exa.search.call_count == 1
    called_query = mock_exa.search.call_args.args[0]
    assert "Fallback Name" not in called_query
    assert discovered["|TestCo"] == "https://www.linkedin.com/in/no-fallback"


def test_discover_linkedin_urls_skips_entry_without_linkedin_url():
    mock_exa = MagicMock()
    mock_result = MagicMock()
    mock_result.results = [
        MagicMock(url="https://example.com/person/test-person", text=""),
        MagicMock(url="https://example.org/about", text=""),
    ]
    mock_exa.search.return_value = mock_result

    contacts = [{"full_name": "Test Person", "company": "TestCo"}]
    discovered = discover_linkedin_urls(mock_exa, contacts)

    assert mock_exa.search.call_count == 1
    assert discovered == {}


def test_structuring_prompt_contains_url_and_markdown():
    prompt = build_structuring_prompt(
        linkedin_url="https://www.linkedin.com/in/foo",
        markdown="# Foo Bar\nCEO at TestCo\nLondon\n## Experience\n### CEO at TestCo\n2020 - Present",
    )
    assert "linkedin.com/in/foo" in prompt
    assert "CEO at TestCo" in prompt
    assert "Experience" in prompt


def _install_fake_structure_deps(monkeypatch, *, final_content, final_usage, usage_return):
    chat = MagicMock()
    final = types.SimpleNamespace(content=final_content, usage=final_usage)
    chat.sample.return_value = final

    client_instance = types.SimpleNamespace(chat=types.SimpleNamespace(create=MagicMock(return_value=chat)))
    client_ctor = MagicMock(return_value=client_instance)
    fake_xai_sdk = types.SimpleNamespace(Client=client_ctor)
    fake_xai_chat = types.SimpleNamespace(
        system=lambda text: ("system", text),
        user=lambda text: ("user", text),
    )
    fake_research = types.SimpleNamespace(usage_to_dict=MagicMock(return_value=usage_return))

    monkeypatch.setitem(__import__("sys").modules, "xai_sdk", fake_xai_sdk)
    monkeypatch.setitem(__import__("sys").modules, "xai_sdk.chat", fake_xai_chat)
    monkeypatch.setitem(__import__("sys").modules, "hotel_decision_maker_research", fake_research)

    return client_ctor, chat, fake_research


def test_structure_profile_success_parses_json(monkeypatch):
    usage_dict = {"input_tokens": 12, "output_tokens": 34}
    valid_json = (
        '{"linkedin_url":"https://www.linkedin.com/in/test-person-123","display_name":"Test Person",'
        '"headline":"CEO","experience":[{"title":"CEO","organization":"TestCo","date_range":"2020 - Present"}],'
        '"data_quality":"strong"}'
    )
    client_ctor, chat, fake_research = _install_fake_structure_deps(
        monkeypatch,
        final_content=valid_json,
        final_usage={"prompt_tokens": 12},
        usage_return=usage_dict,
    )

    profile, usage = structure_profile(
        api_key="test-key",
        model="grok-test",
        linkedin_url="https://www.linkedin.com/in/test-person-123",
        markdown="# Test Person",
    )

    assert isinstance(profile, LinkedInProfile)
    assert profile.display_name == "Test Person"
    assert usage == usage_dict
    fake_research.usage_to_dict.assert_called_once_with({"prompt_tokens": 12})
    client_ctor.assert_called_once_with(api_key="test-key")
    create_kwargs = client_ctor.return_value.chat.create.call_args.kwargs
    assert create_kwargs["response_format"] is LinkedInProfile
    assert create_kwargs["max_turns"] == 1
    assert "tools" not in create_kwargs
    assert chat.append.call_count == 2


def test_structure_profile_returns_none_on_invalid_json(monkeypatch):
    usage_dict = {"total_tokens": 99}
    client_ctor, _chat, fake_research = _install_fake_structure_deps(
        monkeypatch,
        final_content='{"display_name": "Missing closing brace"',
        final_usage={"completion_tokens": 9},
        usage_return=usage_dict,
    )

    profile, usage = structure_profile(
        api_key="test-key",
        model="grok-test",
        linkedin_url="https://www.linkedin.com/in/test-person-123",
        markdown="# Broken JSON",
    )

    assert profile is None
    assert usage == usage_dict
    fake_research.usage_to_dict.assert_called_once_with({"completion_tokens": 9})
    create_kwargs = client_ctor.return_value.chat.create.call_args.kwargs
    assert create_kwargs["response_format"] is LinkedInProfile
    assert create_kwargs["max_turns"] == 1
    assert "tools" not in create_kwargs


def test_structure_profile_returns_none_on_empty_content(monkeypatch):
    usage_dict = {"input_tokens": 2, "output_tokens": 0}
    client_ctor, _chat, fake_research = _install_fake_structure_deps(
        monkeypatch,
        final_content="   ",
        final_usage={"prompt_tokens": 2},
        usage_return=usage_dict,
    )

    profile, usage = structure_profile(
        api_key="test-key",
        model="grok-test",
        linkedin_url="https://www.linkedin.com/in/test-person-123",
        markdown="",
    )

    assert profile is None
    assert usage == usage_dict
    fake_research.usage_to_dict.assert_called_once_with({"prompt_tokens": 2})
    create_kwargs = client_ctor.return_value.chat.create.call_args.kwargs
    assert create_kwargs["response_format"] is LinkedInProfile
    assert create_kwargs["max_turns"] == 1
    assert "tools" not in create_kwargs


def test_structure_profile_chat_create_uses_response_format_and_no_tools(monkeypatch):
    client_ctor, _chat, _fake_research = _install_fake_structure_deps(
        monkeypatch,
        final_content="",
        final_usage=None,
        usage_return={"unused": True},
    )

    profile, usage = structure_profile(
        api_key="another-key",
        model="grok-check",
        linkedin_url="https://www.linkedin.com/in/test-person-123",
        markdown="profile text",
    )

    assert profile is None
    assert usage == {}
    create_kwargs = client_ctor.return_value.chat.create.call_args.kwargs
    assert create_kwargs["response_format"] is LinkedInProfile
    assert create_kwargs["max_turns"] == 1
    assert "tools" not in create_kwargs
