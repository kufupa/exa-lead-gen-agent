#!/usr/bin/env python3
"""Use xAI Grok + web_search to pull public LinkedIn-style facts for ONE profile URL (lead DB enrichment).

Needs XAI_API_KEY in repo-root `.env` or environment. Uses model `grok-4.20-reasoning` by default.

Example:
  python scripts/linkedin_profile_enrich_xai.py --linkedin-url https://www.linkedin.com/in/jonathan-raggett-aa787097
  python scripts/linkedin_profile_enrich_xai.py --out linkedin_xai_enrich.json
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from pydantic import BaseModel, Field
from scripts._repo_dotenv import load_repo_dotenv  # noqa: E402

from hotel_decision_maker_research import usage_to_dict  # noqa: E402

DEFAULT_LINKEDIN_URL = "https://www.linkedin.com/in/jonathan-raggett-aa787097"
DEFAULT_MODEL = "grok-4.20-reasoning"

# Default $ rates for `grok-4.20-reasoning` (verify on https://docs.x.ai/docs/models — console is source of truth).
DEFAULT_INPUT_USD_PER_MTOK = 2.0
DEFAULT_OUTPUT_USD_PER_MTOK = 6.0
# https://docs.x.ai/docs/models — Web Search row: $5 / 1k tool invocations
DEFAULT_WEB_SEARCH_USD_PER_1K_CALLS = 5.0

# xai_sdk proto: SERVER_SIDE_TOOL_WEB_SEARCH = 1
_SERVER_SIDE_TOOL_WEB_SEARCH = 1


class ExperienceEntry(BaseModel):
    title: str | None = None
    organization: str | None = None
    employment_type: str | None = None
    date_range: str | None = None
    location: str | None = None
    description: str | None = None


class EducationEntry(BaseModel):
    school: str | None = None
    degree_or_field: str | None = None
    date_range: str | None = None


class LinkedInLeadEnrichment(BaseModel):
    """Public profile facts for a single person — fill from web_search only; use null/empty if unknown."""

    linkedin_url: str = Field(min_length=12)
    profile_display_name: str | None = None
    headline: str | None = None
    location: str | None = None
    connections_or_followers_text: str | None = None
    about: str | None = None
    experience: list[ExperienceEntry] = Field(default_factory=list)
    education: list[EducationEntry] = Field(default_factory=list)
    skills: list[str] = Field(default_factory=list, max_length=80)
    languages: list[str] = Field(default_factory=list, max_length=20)
    certifications: list[str] = Field(default_factory=list, max_length=30)
    volunteer_or_causes: list[str] = Field(default_factory=list, max_length=20)
    publications_or_projects: list[str] = Field(default_factory=list, max_length=15)
    activity_highlights: list[str] = Field(
        default_factory=list,
        max_length=20,
        description="Short bullets from recent public activity if visible",
    )
    source_urls: list[str] = Field(
        default_factory=list,
        max_length=24,
        description="URLs web_search relied on (LinkedIn or mirrors)",
    )
    data_quality: Literal["strong", "partial", "weak"] = "partial"
    caveats: str | None = Field(
        default=None,
        max_length=1200,
        description="Paywalls, ambiguity, or conflicting sources",
    )


SYSTEM_PROMPT = """You enrich B2B lead records from the public web.

Rules:
- Use the web_search tool as needed. Prefer the exact LinkedIn profile URL when it is reachable; otherwise use reputable mirrors/snippets that clearly refer to the same person.
- Extract facts only for the ONE target person identified by the given LinkedIn URL. Do not mix in other people.
- Never fabricate contact info (no guessed emails/phones). If you did not see it on a retrieved page, leave fields null or empty.
- Populate source_urls with URLs whose content you actually used.
- Set data_quality: strong if headline+experience mostly filled from primary profile; partial if gaps; weak if little beyond name/headline.
"""


def _user_prompt(linkedin_url: str) -> str:
    return f"""Target LinkedIn profile (single person only):
{linkedin_url}

Task: Gather everything publicly useful for a lead-generation / CRM enrichment row for this individual only — roles, employers, tenure, location, education, skills, about/summary, visible activity themes, and any clear company context.

Return one JSON object matching the schema (no markdown fences, no commentary outside JSON)."""


def _usage_int(usage: dict[str, Any], key: str) -> int:
    v = usage.get(key)
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str) and v.lstrip("-").isdigit():
        return int(v)
    return 0


def _count_web_search_invocations(usage: dict[str, Any]) -> int:
    raw = usage.get("server_side_tools_used")
    if not isinstance(raw, list):
        return 0
    n = 0
    for x in raw:
        if x == _SERVER_SIDE_TOOL_WEB_SEARCH or x == "SERVER_SIDE_TOOL_WEB_SEARCH":
            n += 1
    return n


def estimate_run_cost_usd(
    usage: dict[str, Any],
    *,
    input_usd_per_mtok: float,
    output_usd_per_mtok: float,
    web_search_usd_per_1k: float,
) -> dict[str, Any]:
    """Rough invoice-style estimate from one `sample()` usage blob. Cached prompt discount not modeled."""
    prompt = _usage_int(usage, "prompt_tokens")
    cached = _usage_int(usage, "cached_prompt_text_tokens")
    completion = _usage_int(usage, "completion_tokens")
    reasoning = _usage_int(usage, "reasoning_tokens")
    total = _usage_int(usage, "total_tokens")
    # Bill output-like tokens at output $/M (reasoning + completion per xAI pricing narrative).
    output_like = max(0, completion + reasoning)
    input_like = max(0, prompt)
    token_usd = (input_like / 1_000_000) * input_usd_per_mtok + (output_like / 1_000_000) * output_usd_per_mtok
    n_ws = _count_web_search_invocations(usage)
    tool_usd = (n_ws / 1000.0) * web_search_usd_per_1k
    return {
        "approx_total_usd": round(token_usd + tool_usd, 6),
        "token_component_usd": round(token_usd, 6),
        "web_search_tool_component_usd": round(tool_usd, 6),
        "counts": {
            "prompt_tokens": prompt,
            "cached_prompt_text_tokens": cached,
            "completion_tokens": completion,
            "reasoning_tokens": reasoning,
            "total_tokens_reported": total,
            "web_search_invocations_estimated": n_ws,
            "num_sources_used": _usage_int(usage, "num_sources_used"),
        },
        "rates_used": {
            "input_usd_per_million_tokens": input_usd_per_mtok,
            "output_usd_per_million_tokens": output_usd_per_mtok,
            "web_search_usd_per_1000_invocations": web_search_usd_per_1k,
            "notes": (
                "Approximate. xAI bills tool calls separately from tokens "
                "(see https://docs.x.ai/docs/models Tools Pricing). "
                "Cached prompt tokens may be cheaper than full input — verify in console."
            ),
        },
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--linkedin-url", default=DEFAULT_LINKEDIN_URL, help="Public LinkedIn profile URL")
    p.add_argument("--model", default=DEFAULT_MODEL, help="xAI chat model id")
    p.add_argument("--max-turns", type=int, default=32)
    p.add_argument(
        "--out",
        type=Path,
        default=Path("linkedin_xai_enrich.json"),
        help="Write structured JSON here (default: ./linkedin_xai_enrich.json)",
    )
    p.add_argument("--pretty", action="store_true", help="Indent JSON")
    p.add_argument(
        "--input-usd-per-mtok",
        type=float,
        default=DEFAULT_INPUT_USD_PER_MTOK,
        help="$/1M prompt tokens (default Grok 4.20 reasoning tier — verify on xAI docs)",
    )
    p.add_argument(
        "--output-usd-per-mtok",
        type=float,
        default=DEFAULT_OUTPUT_USD_PER_MTOK,
        help="$/1M completion+reasoning tokens (default — verify on xAI docs)",
    )
    p.add_argument(
        "--web-search-usd-per-1k",
        type=float,
        default=DEFAULT_WEB_SEARCH_USD_PER_1K_CALLS,
        help="$ per 1,000 web_search invocations (xAI docs default 5)",
    )
    args = p.parse_args()

    load_repo_dotenv(_ROOT)
    api_key = (os.environ.get("XAI_API_KEY") or "").strip()
    if not api_key:
        print("Missing XAI_API_KEY (.env or environment).", file=sys.stderr)
        return 1

    from xai_sdk import Client
    from xai_sdk.chat import system, user
    from xai_sdk.tools import web_search

    client = Client(api_key=api_key)
    chat = client.chat.create(
        model=args.model,
        tools=[web_search()],
        max_turns=args.max_turns,
        store_messages=True,
        response_format=LinkedInLeadEnrichment,
    )
    chat.append(system(SYSTEM_PROMPT))
    chat.append(user(_user_prompt(args.linkedin_url.strip())))
    final = chat.sample()
    raw = (final.content or "").strip()
    if not raw:
        print("Empty model response.", file=sys.stderr)
        return 1
    try:
        payload = LinkedInLeadEnrichment.model_validate_json(raw)
    except Exception as e:
        print(f"Parse error: {e}\n--- raw ---\n{raw[:8000]}", file=sys.stderr)
        return 1

    raw_usage = getattr(final, "usage", None)
    usage: dict[str, Any] = usage_to_dict(raw_usage) if raw_usage is not None else {}
    cost = estimate_run_cost_usd(
        usage,
        input_usd_per_mtok=args.input_usd_per_mtok,
        output_usd_per_mtok=args.output_usd_per_mtok,
        web_search_usd_per_1k=args.web_search_usd_per_1k,
    )

    envelope = {
        "version": 1,
        "model": args.model,
        "max_turns": args.max_turns,
        "fetched_at_utc": datetime.now(timezone.utc).isoformat(),
        "linkedin_url_requested": args.linkedin_url.strip(),
        "usage": usage,
        "cost_estimate": cost,
        "enrichment": payload.model_dump(),
    }
    out_path = args.out
    if not out_path.is_absolute():
        out_path = (Path.cwd() / out_path).resolve()
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if args.pretty:
        txt = json.dumps(envelope, ensure_ascii=False, indent=2)
    else:
        txt = json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))
    out_path.write_text(txt, encoding="utf-8")
    print(f"Wrote {out_path}")
    ce = cost.get("counts", {})
    print(
        f"usage: prompt={ce.get('prompt_tokens')} completion={ce.get('completion_tokens')} "
        f"reasoning={ce.get('reasoning_tokens')} total={ce.get('total_tokens_reported')} "
        f"web_search_invocations~={ce.get('web_search_invocations_estimated')}"
    )
    print(
        f"cost_est_usd: total~=${cost.get('approx_total_usd')} "
        f"(tokens~=${cost.get('token_component_usd')} + web_search~=${cost.get('web_search_tool_component_usd')})"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
