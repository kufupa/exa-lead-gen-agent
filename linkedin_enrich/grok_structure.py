"""Structure LinkedIn markdown into LinkedInProfile using Grok (no web tools)."""
from __future__ import annotations

from typing import Any

from linkedin_enrich.types import LinkedInProfile

SYSTEM_PROMPT = """You extract structured data from LinkedIn profile text.

Rules:
- Extract ONLY facts present in the provided text. Never fabricate.
- Include ALL experience entries (every distinct role), oldest to newest.
- Set data_quality: strong if headline + 2+ experience entries filled; partial if gaps; weak if only name/headline.
- If text is empty or clearly a login wall, set data_quality: weak with a caveat.
- Return JSON matching the schema. No markdown fences, no commentary outside JSON."""


def build_structuring_prompt(linkedin_url: str, markdown: str) -> str:
    # Cap markdown to avoid token blowout
    text = markdown[:10000] if len(markdown) > 10000 else markdown
    return f"""LinkedIn URL: {linkedin_url}

--- Profile text (fetched via Exa) ---
{text}
--- End profile text ---

Extract ALL structured fields from the text above into the JSON schema."""


def structure_profile(
    *,
    api_key: str,
    model: str,
    linkedin_url: str,
    markdown: str,
) -> tuple[LinkedInProfile | None, dict[str, Any]]:
    """Run Grok with no tools to structure LinkedIn markdown. Returns (profile, usage_dict)."""
    from xai_sdk import Client
    from xai_sdk.chat import system, user

    from hotel_decision_maker_research import usage_to_dict

    client = Client(api_key=api_key)
    chat = client.chat.create(
        model=model,
        max_turns=1,
        store_messages=True,
        response_format=LinkedInProfile,
    )
    chat.append(system(SYSTEM_PROMPT))
    chat.append(user(build_structuring_prompt(linkedin_url, markdown)))
    final = chat.sample()
    raw = (final.content or "").strip()
    raw_usage = getattr(final, "usage", None)
    usage = usage_to_dict(raw_usage) if raw_usage is not None else {}

    if not raw:
        return None, usage
    try:
        profile = LinkedInProfile.model_validate_json(raw)
    except Exception:
        return None, usage
    return (profile, usage)
