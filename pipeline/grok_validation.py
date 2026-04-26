from __future__ import annotations

import json
import time
from typing import Any

from pydantic import BaseModel, Field

from pipeline.candidates import classify_role_family, classify_role_tier, hotel_key_from_org, make_candidate_id
from pipeline.config import PipelineConfig
from pipeline.models import (
    CandidateLead,
    ContactRoute,
    HotelOrg,
    RoleConfidence,
    RoleFamily,
    RoleTier,
    SourceRef,
)
from pipeline.telemetry import record_xai_stage

try:
    from google.protobuf.json_format import MessageToDict
except ImportError:  # pragma: no cover
    MessageToDict = None  # type: ignore[misc, assignment]


class GrokValidatedPerson(BaseModel):
    full_name: str = Field(..., min_length=1)
    title: str | None = None
    company: str | None = None
    role_tier: RoleTier = 4
    role_family: RoleFamily = "other"
    current_role_confidence: RoleConfidence = "medium"
    evidence_urls: list[str] = Field(default_factory=list)
    linkedin_url: str | None = None
    contact_routes: list[ContactRoute] = Field(default_factory=list)
    needs_human_review: bool = False
    needs_contact_mining: bool = False
    notes: list[str] = Field(default_factory=list)
    reject: bool = False


class GrokValidationOutput(BaseModel):
    candidates: list[GrokValidatedPerson] = Field(default_factory=list)


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


def _pool_by_url(sources: list[SourceRef]) -> dict[str, SourceRef]:
    m: dict[str, SourceRef] = {}
    for s in sources:
        u = (s.url or "").strip()
        if u:
            m[u] = s
    return m


def _merge_evidence_for_urls(urls: list[str], pool: dict[str, SourceRef]) -> list[SourceRef]:
    out: list[SourceRef] = []
    seen: set[str] = set()
    for u in urls:
        u = u.strip()
        if not u or u in seen:
            continue
        seen.add(u)
        if u in pool:
            out.append(pool[u])
    return out


def _needs_mining(routes: list[ContactRoute], tier: RoleTier) -> bool:
    if tier not in (1, 2):
        return False
    has_direct = any(
        r.kind in ("email", "phone") and r.confidence in ("high", "medium") for r in routes
    )
    has_linkedin = any(r.kind == "linkedin" for r in routes)
    if has_direct:
        return False
    if has_linkedin and tier == 1:
        return True
    return not has_direct


def build_validation_prompt(hotel: HotelOrg, chunk: dict[str, Any]) -> str:
    rules = """You validate hotel stakeholder evidence.
Use ONLY the supplied JSON evidence. Do not invent emails, phone numbers, employers, titles, or URLs.
If uncertain, set needs_human_review true and current_role_confidence low or medium.
Reject unrelated people, vendors, or clear ex-employees (set reject true).
Keep senior roles even when contact data is missing.
Return strict JSON matching the response schema (candidates array)."""
    body = json.dumps({"hotel": chunk.get("hotel"), "candidate_groups": chunk.get("candidate_groups", [])}, ensure_ascii=False)
    return f"{rules}\n\n--- EVIDENCE ---\n{body}"


def _chunk_pack(full: dict[str, Any], size: int) -> list[dict[str, Any]]:
    hotel = full.get("hotel", {})
    groups = list(full.get("candidate_groups", []))
    if not groups:
        return [{"hotel": hotel, "candidate_groups": []}]
    out: list[dict[str, Any]] = []
    for i in range(0, len(groups), size):
        out.append({"hotel": hotel, "candidate_groups": groups[i : i + size]})
    return out


def validate_with_grok(
    hotel: HotelOrg,
    source_pack: dict[str, Any],
    all_sources: list[SourceRef],
    config: PipelineConfig,
    xai_api_key: str | None,
    telemetry: Any,
) -> tuple[list[CandidateLead], list[dict[str, Any]]]:
    """
    Run capped Grok validation on source pack chunks.
    If xai_api_key is None/empty, returns heuristic candidates from pack groups only (no API).
    """
    pool = _pool_by_url(all_sources)
    usages: list[dict[str, Any]] = []

    if not (xai_api_key or "").strip():
        return _heuristic_from_pack(hotel, source_pack, pool), usages

    chunks = _chunk_pack(source_pack, config.grok_chunk_size)
    merged: list[CandidateLead] = []
    key = (xai_api_key or "").strip()

    for ch in chunks:
        t0 = time.perf_counter()
        prompt = build_validation_prompt(hotel, ch)
        chunk_usages: list[dict[str, Any]] = []
        raw, usage = _call_grok_json(prompt, config, key)
        chunk_usages.append(usage)
        try:
            out = GrokValidationOutput.model_validate_json(raw)
        except Exception:
            repair = (
                "Your previous output was invalid JSON or did not match the schema. "
                "Return ONLY valid JSON for GrokValidationOutput with a `candidates` array.\n"
                f"Parse error context (first 500 chars of raw): {raw[:500]!r}"
            )
            raw2, usage2 = _call_grok_json(repair + "\n\n" + prompt, config, key)
            chunk_usages.append(usage2)
            try:
                out = GrokValidationOutput.model_validate_json(raw2)
            except Exception:
                out = GrokValidationOutput(candidates=[])
        usages.extend(chunk_usages)
        record_xai_stage(
            telemetry,
            stage="grok_validation_chunk",
            usages=chunk_usages,
            seconds=time.perf_counter() - t0,
        )

        for p in out.candidates:
            if p.reject or not p.full_name.strip():
                continue
            ev = _merge_evidence_for_urls(p.evidence_urls, pool)
            hk = hotel_key_from_org(hotel)
            cid = make_candidate_id(hk, p.full_name, p.title)
            routes = list(p.contact_routes)
            mining = p.needs_contact_mining or _needs_mining(routes, p.role_tier)
            merged.append(
                CandidateLead(
                    candidate_id=cid,
                    full_name=p.full_name.strip(),
                    title=p.title,
                    normalized_title=(p.title or "").strip().lower() or None,
                    company=p.company,
                    role_tier=p.role_tier,
                    role_family=p.role_family,
                    current_role_confidence=p.current_role_confidence,
                    evidence=ev,
                    contact_routes=routes,
                    linkedin_url=p.linkedin_url,
                    needs_human_review=p.needs_human_review,
                    needs_contact_mining=mining,
                    notes=list(p.notes),
                )
            )

    if not merged:
        return _heuristic_from_pack(hotel, source_pack, pool), usages
    return merged, usages


def _call_grok_json(user_text: str, config: PipelineConfig, api_key: str) -> tuple[str, dict[str, Any]]:
    from xai_sdk import Client
    from xai_sdk.chat import user

    client = Client(api_key=api_key)
    chat = client.chat.create(
        model=config.grok_validation_model,
        max_turns=config.grok_max_turns,
        store_messages=True,
        response_format=GrokValidationOutput,
    )
    chat.append(user(user_text))
    final = chat.sample()
    raw = (final.content or "").strip()
    return raw, _usage_to_dict(getattr(final, "usage", None))


def _heuristic_from_pack(
    hotel: HotelOrg,
    pack: dict[str, Any],
    pool: dict[str, SourceRef],
) -> list[CandidateLead]:
    out: list[CandidateLead] = []
    hk = hotel_key_from_org(hotel)
    for g in pack.get("candidate_groups", []) or []:
        names = g.get("name_hints") or []
        titles = g.get("title_hints") or []
        name = (names[0] if names else "") or "Unknown"
        title = titles[0] if titles else None
        urls: list[str] = []
        for s in g.get("sources", []) or []:
            u = s.get("url")
            if isinstance(u, str) and u.strip():
                urls.append(u.strip())
        ev = _merge_evidence_for_urls(urls, pool)
        if name == "Unknown" and not ev:
            continue
        rt = classify_role_tier(title)
        rf = classify_role_family(title)
        cid = g.get("candidate_id") or make_candidate_id(hk, name, title)
        out.append(
            CandidateLead(
                candidate_id=str(cid),
                full_name=name,
                title=title,
                normalized_title=(title or "").lower() or None,
                company=None,
                role_tier=rt,
                role_family=rf,
                current_role_confidence="medium",
                evidence=ev,
                contact_routes=[],
                linkedin_url=next((u for u in urls if "linkedin.com/in/" in u.lower()), None),
                needs_human_review=True,
                needs_contact_mining=_needs_mining([], rt),
                notes=["heuristic_fallback_no_xai"],
            )
        )
    return out
