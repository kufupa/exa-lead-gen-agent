#!/usr/bin/env python3
"""
Hotel decision-maker research via xAI Grok multi-agent (web + X search).

Usage:
  export XAI_API_KEY=...
  python hotel_decision_maker_research.py --url https://example-hotel.com
  # JSON default: hotel_leads__<host>__...__<hash8>.json ; CSV default: hotel_leads.csv (append rows)

Dry-run (no API key, no network):
  python hotel_decision_maker_research.py --url https://example-hotel.com --dry-run-prompt
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlparse

from pydantic import BaseModel, Field

try:
    from google.protobuf.json_format import MessageToDict
except ImportError:  # pragma: no cover
    MessageToDict = None  # type: ignore[misc, assignment]

DecisionMakerScore = Literal["low", "medium", "high"]
IntimacyGrade = Literal["low", "medium", "high"]
SourceType = Literal["official_site", "linkedin", "news", "directory", "x", "other"]

EMAIL_IN_TEXT = re.compile(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}")
PHONE_IN_TEXT = re.compile(r"\+?\d[\d\s().-]{8,}\d")

# Local parts that indicate department / property inbox (MEDIUM intimacy), not a named person (HIGH).
_GENERIC_EMAIL_LOCALS = frozenset(
    {
        "info",
        "contact",
        "hello",
        "reservations",
        "groupsales",
        "groups",
        "sales",
        "events",
        "booking",
        "bookings",
        "frontdesk",
        "frontdesk0",
        "concierge",
        "inquiries",
        "welcome",
        "office",
        "admin",
    }
)


def _email_local_part(addr: str) -> str:
    addr = addr.strip().lower()
    if "@" not in addr:
        return ""
    return addr.split("@", 1)[0]


def is_generic_functional_email(addr: str) -> bool:
    """True for department / property inboxes (MEDIUM), false for likely named person (HIGH)."""
    local = _email_local_part(addr)
    if not local:
        return True
    if local in _GENERIC_EMAIL_LOCALS:
        return True
    if local.startswith("reservations") or "groupsales" in local or local.startswith("groupsales"):
        return True
    return False

FUNCTIONAL_ROUTE_MARKERS = (
    "reservations@",
    "groupsales@",
    "group.sales",
    "group sales",
    "commercial@",
    "corporate sales",
    "sales office",
    "revenue management",
    "central reservations",
    "reservations office",
)


class Evidence(BaseModel):
    source_url: str
    source_type: SourceType
    quote_or_fact: str = Field(min_length=1)


class Contact(BaseModel):
    full_name: str
    title: str
    company: str | None = None
    linkedin_url: str | None = None
    email: str | None = None
    email2: str | None = None
    phone: str | None = None
    phone2: str | None = None
    x_handle: str | None = None
    other_contact_detail: str | None = None
    decision_maker_score: DecisionMakerScore
    intimacy_grade: IntimacyGrade
    fit_reason: str
    contact_evidence_summary: str
    evidence: list[Evidence]


class LeadResearchResult(BaseModel):
    contacts: list[Contact]


def _slug_for_filename(raw: str, max_len: int) -> str:
    s = raw.lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    s = re.sub(r"_+", "_", s).strip("_")
    if len(s) > max_len:
        s = s[:max_len].rstrip("_")
    return s or "x"


def _normalize_url(url: str) -> str:
    u = url.strip()
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    return u


def default_json_path_from_url(url: str) -> str:
    """Filesystem-safe JSON path: hotel_leads__<host>__<path?>__<hash8>.json."""
    normalized = _normalize_url(url)
    parsed = urlparse(normalized)
    netloc = (parsed.netloc or "nohost").lower()
    path = (parsed.path or "").strip("/")
    slug_net = _slug_for_filename(netloc, 60)
    parts = [f"hotel_leads__{slug_net}"]
    if path:
        parts.append(_slug_for_filename(path.replace("/", "_"), 50))
    base = "__".join(parts)
    if len(base) > 100:
        base = base[:100].rstrip("_")
    h8 = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:8]
    return f"{base}__{h8}.json"


CSV_EXTRA_FIELDS = ("source_target_url", "generated_at_utc")


def csv_fieldnames() -> list[str]:
    return list(Contact.model_fields.keys()) + list(CSV_EXTRA_FIELDS)


def append_csv(
    path: str,
    contacts: list[Contact],
    *,
    source_target_url: str,
    generated_at_utc: str,
) -> None:
    """Append data rows; write header only if file is missing or empty."""
    if not contacts:
        return
    fieldnames = csv_fieldnames()
    p = Path(path)
    write_header = not p.exists() or p.stat().st_size == 0
    with p.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        if write_header:
            w.writeheader()
        for c in contacts:
            row = c.model_dump()
            row["evidence"] = json.dumps([e.model_dump() for e in c.evidence], ensure_ascii=False)
            row["source_target_url"] = source_target_url
            row["generated_at_utc"] = generated_at_utc
            w.writerow(row)


def read_csv_contacts(path: str) -> list[Contact]:
    """Load Contact rows from CSV (ignores extra columns like source_target_url)."""
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return []
    out: list[Contact] = []
    with p.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return []
        for row in reader:
            if not row:
                continue
            payload: dict[str, Any] = {}
            for k in Contact.model_fields:
                if k not in row:
                    continue
                v = row[k]
                if v == "":
                    continue
                payload[k] = v
            if "evidence" in payload and isinstance(payload["evidence"], str):
                payload["evidence"] = json.loads(payload["evidence"])
            try:
                out.append(Contact.model_validate(payload))
            except Exception:
                continue
    return out


def rewrite_csv_deduped(path: str, contacts: list[Contact]) -> None:
    """Rewrite CSV with one header; keep highest-utility row per dedupe_key."""
    merged = dedupe_and_rank(contacts, max_contacts=max(len(contacts), 1))
    fieldnames = csv_fieldnames()
    with Path(path).open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        w.writeheader()
        for c in merged:
            row = c.model_dump()
            row["evidence"] = json.dumps([e.model_dump() for e in c.evidence], ensure_ascii=False)
            row["source_target_url"] = ""
            row["generated_at_utc"] = ""
            w.writerow(row)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="Research hotel decision-makers for booking / guest / revenue tech."
    )
    p.add_argument("--url", required=True, help="Hotel website URL")
    p.add_argument(
        "--out-json",
        default=None,
        metavar="PATH",
        help="Output JSON path (default: hotel_leads__<url_slug>__<hash>.json)",
    )
    p.add_argument(
        "--out-csv",
        default="hotel_leads.csv",
        metavar="PATH",
        help="CSV path; rows are appended unless --csv-dedupe (default: hotel_leads.csv)",
    )
    p.add_argument(
        "--no-csv",
        action="store_true",
        help="Do not write CSV output",
    )
    p.add_argument(
        "--csv-dedupe",
        action="store_true",
        help="After append, read full CSV, merge rows by dedupe_key, rewrite with one header",
    )
    p.add_argument("--max-contacts", type=int, default=25, help="Max contacts to return")
    p.add_argument(
        "--agent-count",
        type=int,
        choices=[4, 16],
        default=16,
        help="Multi-agent depth (4 faster, 16 deeper)",
    )
    p.add_argument(
        "--max-turns",
        type=int,
        default=12,
        help="Server-side agentic tool turns cap per sample() call",
    )
    p.add_argument(
        "--strict-evidence",
        action="store_true",
        help="Drop contacts without 2+ evidence items unless direct email/phone present",
    )
    p.add_argument(
        "--allow-linkedin",
        action="store_true",
        help="Allow LinkedIn-only evidence rows (default: filter out)",
    )
    p.add_argument(
        "--dry-run-prompt",
        action="store_true",
        help="Print prompts and exit (no API calls)",
    )
    return p


def build_round1_prompt(url: str, max_contacts: int) -> str:
    return f"""You are a B2B prospecting research agent.

Hotel website (primary anchor): {url}

Goal: find up to {max_contacts} likely decision-makers for:
- AI booking automation
- reservations automation
- guest messaging
- revenue-tech adoption

Prioritize CONTACT USEFULNESS over raw seniority. A reachable manager beats an unreachable VP.

Research rules:
- Use web search and X search as needed. Triangulate with multiple independent public sources when possible.
- Prefer official hotel / brand / management company pages, press releases, conference bios.
- Never fabricate emails, phones, or LinkedIn URLs. Use null when unknown.

Scoring — decision_maker_score (low | medium | high):
- high: clear influence over reservations, distribution, revenue, digital guest journey, commercial strategy, or property systems tied to booking stack
- medium: plausible stakeholder (e.g. department lead) with partial influence or weaker role confirmation
- low: weak relevance OR no practical path to influence booking / guest messaging / revenue tech

Scoring — intimacy_grade (low | medium | high) from strongest PUBLIC contact evidence:
- high: direct public business email for that person OR direct public business phone/extension OR official profile page with business contact clearly tied to them
- medium: strong role confirmation plus department/team/company route likely to reach them (e.g. reservations office, group sales, commercial office, named office line, corporate alias tied to function)
- low: only LinkedIn / name+title / weak directories / generic hotel form with no role-specific route

Return JSON matching the response schema (contacts array only). Each contact must include:
- full_name, title, optional company, linkedin_url, email, email2, phone, phone2, x_handle, other_contact_detail
- decision_maker_score, intimacy_grade, fit_reason, contact_evidence_summary
- evidence: list of {{source_url, source_type, quote_or_fact}} with source_type one of official_site, linkedin, news, directory, x, other
"""


def build_round2_user_message() -> str:
    return """Second research pass on the same hotel target:
- Strengthen evidence for each remaining contact (prefer official pages + second independent source).
- Remove or merge duplicates and weak candidates.
- Fill optional contact channels only when publicly verifiable.
Return JSON matching the same schema (contacts array only)."""


def build_round3_user_message(max_contacts: int) -> str:
    return f"""Final pass:
- Rank by real-world outreach usefulness (weighted toward intimacy_grade).
- Cap at {max_contacts} contacts; fewer is fine if evidence is thin.
- Ensure every contact has non-empty evidence and honest scores.
Return JSON matching the same schema (contacts array only)."""


def _combined_contact_text(contact: Contact) -> str:
    parts = [
        contact.contact_evidence_summary or "",
        contact.fit_reason or "",
        contact.other_contact_detail or "",
    ]
    for ev in contact.evidence:
        parts.append(ev.quote_or_fact)
    return "\n".join(parts).lower()


def _emails_in_text(text: str) -> list[str]:
    return [m.group(0) for m in EMAIL_IN_TEXT.finditer(text)]


def has_personal_business_email_in_evidence(contact: Contact) -> bool:
    for ev in contact.evidence:
        for addr in _emails_in_text(ev.quote_or_fact):
            if not is_generic_functional_email(addr):
                return True
    return False


def has_personal_business_phone_in_evidence(contact: Contact) -> bool:
    return any(PHONE_IN_TEXT.search(ev.quote_or_fact) for ev in contact.evidence)


def has_functional_email_in_evidence(contact: Contact) -> bool:
    for ev in contact.evidence:
        for addr in _emails_in_text(ev.quote_or_fact):
            if is_generic_functional_email(addr):
                return True
    return False


def has_functional_route(contact: Contact) -> bool:
    blob = _combined_contact_text(contact)
    if any(m in blob for m in FUNCTIONAL_ROUTE_MARKERS):
        return True
    if EMAIL_IN_TEXT.search(blob) and any(
        k in blob for k in ("reservation", "group", "sales", "commercial", "revenue")
    ):
        return True
    return False


def recompute_intimacy(contact: Contact) -> IntimacyGrade:
    e1 = (contact.email or "").strip()
    e2 = (contact.email2 or "").strip()
    named_email_on_contact = bool(e1 and not is_generic_functional_email(e1)) or bool(
        e2 and not is_generic_functional_email(e2)
    )
    named_phone_on_contact = bool((contact.phone or "").strip()) or bool((contact.phone2 or "").strip())
    if named_phone_on_contact or named_email_on_contact:
        return "high"
    if has_personal_business_phone_in_evidence(contact) or has_personal_business_email_in_evidence(contact):
        return "high"
    if (
        (e1 and is_generic_functional_email(e1))
        or (e2 and is_generic_functional_email(e2))
        or has_functional_email_in_evidence(contact)
        or has_functional_route(contact)
    ):
        return "medium"
    return "low"


def utility_score(contact: Contact) -> float:
    rank = {"low": 1.0, "medium": 2.0, "high": 3.0}
    intimacy = rank[contact.intimacy_grade]
    decision = rank[contact.decision_maker_score]
    return 0.65 * intimacy + 0.35 * decision


def dedupe_key(contact: Contact) -> str:
    if contact.linkedin_url and contact.linkedin_url.strip():
        return "li:" + contact.linkedin_url.strip().lower()
    if contact.email and contact.email.strip():
        return "em:" + contact.email.strip().lower()
    if contact.email2 and contact.email2.strip():
        return "e2:" + contact.email2.strip().lower()
    company = (contact.company or "").strip().lower()
    return "nm:" + contact.full_name.strip().lower() + "|" + company


def dedupe_and_rank(contacts: list[Contact], max_contacts: int) -> list[Contact]:
    sorted_rows = sorted(contacts, key=utility_score, reverse=True)
    seen: set[str] = set()
    out: list[Contact] = []
    for c in sorted_rows:
        k = dedupe_key(c)
        if k in seen:
            continue
        seen.add(k)
        out.append(c)
        if len(out) >= max_contacts:
            break
    return out


def is_linkedin_only_evidence(contact: Contact) -> bool:
    if not contact.evidence:
        return True
    if any(ev.source_type != "linkedin" for ev in contact.evidence):
        return False
    return True


def passes_strict_evidence(contact: Contact) -> bool:
    if len(contact.evidence) >= 2:
        return True
    if contact.email or contact.email2 or contact.phone or contact.phone2:
        return True
    return False


def process_contacts(
    contacts: list[Contact],
    *,
    max_contacts: int,
    strict_evidence: bool,
    allow_linkedin: bool,
) -> list[Contact]:
    refreshed: list[Contact] = []
    for c in contacts:
        new_intimacy = recompute_intimacy(c)
        refreshed.append(c.model_copy(update={"intimacy_grade": new_intimacy}))

    filtered: list[Contact] = []
    for c in refreshed:
        if not allow_linkedin and is_linkedin_only_evidence(c):
            continue
        if strict_evidence and not passes_strict_evidence(c):
            continue
        filtered.append(c)

    return dedupe_and_rank(filtered, max_contacts=max_contacts)


def usage_to_dict(usage: Any) -> dict[str, Any]:
    if MessageToDict is not None:
        try:
            return MessageToDict(usage, preserving_proto_field_name=True)
        except TypeError:
            pass
    out: dict[str, Any] = {}
    for name in dir(usage):
        if name.startswith("_"):
            continue
        attr = getattr(usage, name, None)
        if callable(attr):
            continue
        try:
            if isinstance(attr, (int, float, str, bool)) or attr is None:
                out[name] = attr
        except Exception:
            continue
    return out or {"repr": repr(usage)}


def _run_research(args: argparse.Namespace) -> tuple[LeadResearchResult, dict[str, Any]]:
    from xai_sdk import Client
    from xai_sdk.chat import user
    from xai_sdk.tools import web_search, x_search

    api_key = os.getenv("XAI_API_KEY")
    if not api_key:
        raise SystemExit("Missing XAI_API_KEY")

    client = Client(api_key=api_key)
    chat = client.chat.create(
        model="grok-4.20-multi-agent",
        agent_count=args.agent_count,
        tools=[web_search(), x_search()],
        store_messages=True,
        max_turns=args.max_turns,
        response_format=LeadResearchResult,
    )

    chat.append(user(build_round1_prompt(args.url, args.max_contacts)))
    _ = chat.sample()

    chat.append(user(build_round2_user_message()))
    _ = chat.sample()

    chat.append(user(build_round3_user_message(args.max_contacts)))
    final = chat.sample()

    raw = (final.content or "").strip()
    if not raw:
        raise SystemExit("Empty model response; retry or increase --max-turns.")

    try:
        result = LeadResearchResult.model_validate_json(raw)
    except Exception as e:  # pragma: no cover
        raise SystemExit(f"Failed to parse structured JSON: {e}\nRaw (truncated): {raw[:2000]}") from e

    usage = usage_to_dict(final.usage)
    return result, usage


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    out_json = args.out_json or default_json_path_from_url(args.url)
    csv_path: str | None = None if args.no_csv else args.out_csv

    if args.dry_run_prompt:
        print("=== Round 1 system prompt ===\n")
        print(build_round1_prompt(args.url, args.max_contacts))
        print("\n=== Round 2 user message ===\n")
        print(build_round2_user_message())
        print("\n=== Round 3 user message ===\n")
        print(build_round3_user_message(args.max_contacts))
        print("\n=== Resolved default outputs (dry-run) ===\n")
        print(f"out_json: {out_json}")
        print(f"out_csv:  {csv_path or '(disabled)'}")
        return 0

    if not os.getenv("XAI_API_KEY"):
        raise SystemExit("Missing XAI_API_KEY")

    started = datetime.now(timezone.utc)
    generated_iso = started.isoformat()
    result, usage = _run_research(args)
    processed = process_contacts(
        result.contacts,
        max_contacts=args.max_contacts,
        strict_evidence=args.strict_evidence,
        allow_linkedin=args.allow_linkedin,
    )

    payload: dict[str, Any] = {
        "target_url": args.url,
        "generated_at_utc": started.isoformat(),
        "model": "grok-4.20-multi-agent",
        "agent_count": args.agent_count,
        "max_turns": args.max_turns,
        "strict_evidence": args.strict_evidence,
        "allow_linkedin": args.allow_linkedin,
        "usage": usage,
        "contacts": [c.model_dump() for c in processed],
    }

    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    if csv_path:
        if args.csv_dedupe:
            prior = read_csv_contacts(csv_path)
            rewrite_csv_deduped(csv_path, prior + processed)
        else:
            append_csv(
                csv_path,
                processed,
                source_target_url=args.url,
                generated_at_utc=generated_iso,
            )

    print(f"Wrote {len(processed)} contacts to {out_json}")
    if csv_path:
        msg = f"Appended CSV rows to {csv_path}"
        if args.csv_dedupe:
            msg += " (deduped rewrite)"
        print(msg)
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
