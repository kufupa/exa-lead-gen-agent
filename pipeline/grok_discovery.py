from __future__ import annotations

import json
import time
import uuid
from typing import Any

from pipeline.candidates import hotel_key_from_org, make_candidate_id
from pipeline.models import GrokDiscoveryResult, HotelOrg
from pipeline.telemetry import record_xai_stage

try:
    from google.protobuf.json_format import MessageToDict
except ImportError:  # pragma: no cover
    MessageToDict = None  # type: ignore[misc, assignment]

GROK_DISCOVERY_MODEL = "grok-4.20-reasoning"


def _usage_to_dict(usage: Any) -> dict[str, Any]:
    if usage is None:
        return {}
    if MessageToDict is not None:
        try:
            return dict(MessageToDict(usage, preserving_proto_field_name=True))
        except TypeError:
            pass
    out: dict[str, Any] = {}
    for name in dir(usage):
        if name.startswith("_"):
            continue
        attr = getattr(usage, name, None)
        if callable(attr):
            continue
        if isinstance(attr, (int, float, str, bool)) or attr is None:
            out[name] = attr
    return out or {"repr": repr(usage)}


def build_grok_discovery_prompt(hotel_url: str) -> str:
    return f"""Hotel URL (only input): {hotel_url}

Tasks (use web_search and x_search; prefer official sites, press, company pages; LinkedIn allowed as evidence):

1) Resolve the operating hotel: canonical property name, brand, management company, ownership where public sources support it.
2) Emit aliases (property / brand / management / ownership / domain / historical) with confidence high|medium|low, optional source_url and short quote.
3) Discover 12–30 public decision-maker drafts (name + title + company when known). Include senior roles without direct contacts when evidence supports the role at this property.
4) For each draft: evidence as SourceRef-style entries (url required when claiming a fact). contact_routes only when explicitly present in source text (never invent email/phone). linkedin_url when clearly the same person. confidence_hint high|medium|low and optional uncertainty string.

Rules: never fabricate emails or phone numbers. If the best name you can anchor searches on is basically the bare hostname (e.g. Kayagnhlondon) and you cannot find a better commercial property name, still return best-effort hotel + aliases but mark uncertainty.

Return JSON matching the GrokDiscoveryResult schema (fields: hotel, aliases, drafts)."""


def assign_draft_ids(result: GrokDiscoveryResult) -> GrokDiscoveryResult:
    key = hotel_key_from_org(result.hotel)
    out_drafts = []
    for d in result.drafts:
        did = d.draft_id or make_candidate_id(key, d.full_name, d.title)
        out_drafts.append(d.model_copy(update={"draft_id": did}))
    return result.model_copy(update={"drafts": out_drafts})


def run_grok_discovery(
    hotel_url: str,
    api_key: str,
    telemetry: Any,
    *,
    max_turns: int = 24,
) -> tuple[GrokDiscoveryResult, dict[str, Any]]:
    """Single Grok 4.20 reasoning call: org resolution + draft candidates."""
    from xai_sdk import Client
    from xai_sdk.chat import user
    from xai_sdk.tools import web_search, x_search

    t0 = time.perf_counter()
    client = Client(api_key=api_key)
    chat = client.chat.create(
        model=GROK_DISCOVERY_MODEL,
        tools=[web_search(), x_search()],
        store_messages=True,
        max_turns=max_turns,
        response_format=GrokDiscoveryResult,
    )
    chat.append(user(build_grok_discovery_prompt(hotel_url.strip())))
    final = chat.sample()
    raw = (final.content or "").strip()
    if not raw:
        raise ValueError("Empty Grok discovery response")
    parsed = GrokDiscoveryResult.model_validate_json(raw)
    if not parsed.hotel.input_url:
        parsed = parsed.model_copy(update={"hotel": parsed.hotel.model_copy(update={"input_url": hotel_url.strip()})})
    parsed = assign_draft_ids(parsed)
    usage = _usage_to_dict(getattr(final, "usage", None))
    record_xai_stage(
        telemetry,
        stage="grok_discovery",
        usages=[usage],
        seconds=time.perf_counter() - t0,
        notes=["grok-4.20-reasoning discovery"],
    )
    return parsed, usage


def grok_discovery_dry_run_plan(hotel_url: str) -> dict[str, Any]:
    """Deterministic plan blob for CLI dry-run (no API keys)."""
    return {
        "pipeline_version": 4,
        "hotel_url": hotel_url,
        "stages": ["grok_discovery", "gap_planner", "exa_verify", "local_validation", "contact_routes"],
        "grok_model": GROK_DISCOVERY_MODEL,
        "exa_policy": "capped_jobs_only",
        "note": "Use XAI_API_KEY for live grok_discovery; EXA_API_KEY for Exa jobs.",
    }


def parse_grok_discovery_json(data: str | dict[str, Any]) -> GrokDiscoveryResult:
    """Test helper: parse JSON object or string into GrokDiscoveryResult."""
    obj = json.loads(data) if isinstance(data, str) else data
    return assign_draft_ids(GrokDiscoveryResult.model_validate(obj))


def synthetic_grok_result_for_tests(hotel_url: str = "https://kayagnhlondon.com/") -> GrokDiscoveryResult:
    """Minimal fixture: Kaya-style alias + one draft (unit tests)."""
    hotel = HotelOrg(
        input_url=hotel_url,
        canonical_name="Kaya Great Northern Hotel",
        property_name="Kaya Great Northern Hotel",
        brand_name=None,
        management_company=None,
        ownership_group=None,
        domains=["kayagnhlondon.com"],
        evidence=[],
    )
    from pipeline.models import CandidateDraft, OrgAlias

    aliases = [
        OrgAlias(
            value="Kaya Great Northern Hotel",
            kind="property",
            confidence="high",
            source_url="https://kayagnhlondon.com/",
            quote="Kaya Great Northern Hotel",
        ),
    ]
    drafts = [
        CandidateDraft(
            full_name="Example GM",
            title="General Manager",
            company="Kaya Great Northern Hotel",
            evidence=[],
            confidence_hint="medium",
            uncertainty=None,
        )
    ]
    return assign_draft_ids(GrokDiscoveryResult(hotel=hotel, aliases=aliases, drafts=drafts))


def new_job_id() -> str:
    return uuid.uuid4().hex[:12]
