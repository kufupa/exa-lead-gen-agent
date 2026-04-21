#!/usr/bin/env python3
"""Merge Phase 1 + Phase 2 into jsons/intimate_phone_contacts.json (phone/phone2 required)."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def dedupe_key(c: dict[str, Any]) -> str:
    li = (c.get("linkedin_url") or "").strip().lower()
    if li:
        return "li:" + li
    e1 = (c.get("email") or "").strip().lower()
    if e1:
        return "em:" + e1
    e2 = (c.get("email2") or "").strip().lower()
    if e2:
        return "e2:" + e2
    company = (c.get("company") or "").strip().lower()
    return "nm:" + (c.get("full_name") or "").strip().lower() + "|" + company


def has_structured_phone(c: dict[str, Any]) -> bool:
    return bool((c.get("phone") or "").strip() or (c.get("phone2") or "").strip())


def score_for_pick(c: dict[str, Any]) -> tuple[int, int]:
    rank = {"low": 0, "medium": 1, "high": 2}
    ig = rank.get(str(c.get("intimacy_grade") or "low"), 0)
    extras = (1 if (c.get("phone") or "").strip() else 0) + (1 if (c.get("phone2") or "").strip() else 0)
    return (ig, extras)


def compact_usage(u: dict[str, Any] | None) -> dict[str, Any] | None:
    if not u:
        return None
    out: dict[str, Any] = {k: v for k, v in u.items() if k != "server_side_tools_used"}
    tools = u.get("server_side_tools_used")
    if isinstance(tools, list):
        out["server_side_tool_invocations"] = len(tools)
        out["server_side_tools_breakdown"] = dict(Counter(tools))
    return out


def phase1_run_meta(data: dict[str, Any], source_name: str) -> dict[str, Any]:
    return {
        "source_enriched_json": source_name,
        "target_url": (data.get("target_url") or "").strip() or None,
        "research_generated_at_utc": data.get("generated_at_utc"),
        "model": data.get("model"),
        "agent_count": data.get("agent_count"),
        "max_turns": data.get("max_turns"),
        "max_turns_effective": data.get("max_turns_effective"),
        "min_contacts": data.get("min_contacts"),
        "target_contacts": data.get("target_contacts"),
        "max_contacts": data.get("max_contacts"),
        "extra_contact_pass": data.get("extra_contact_pass"),
        "strict_evidence": data.get("strict_evidence"),
        "allow_linkedin": data.get("allow_linkedin"),
        "usage_summary": compact_usage(data.get("usage") if isinstance(data.get("usage"), dict) else None),
    }


def contact_enrichment_meta(data: dict[str, Any]) -> dict[str, Any] | None:
    ce = data.get("contact_enrichment")
    if not isinstance(ce, dict):
        return None
    return {
        "enriched_at_utc": ce.get("enriched_at_utc"),
        "mode": ce.get("mode"),
        "model": ce.get("model"),
        "skipped_pre_enrichment": ce.get("skipped_pre_enrichment"),
        "attempted": ce.get("attempted"),
        "succeeded": ce.get("succeeded"),
        "failed": ce.get("failed"),
    }


def build_row(
    c: dict[str, Any],
    *,
    phase1: dict[str, Any],
    phase2: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "full_name": c.get("full_name"),
        "title": c.get("title"),
        "company": c.get("company"),
        "phone": (c.get("phone") or "").strip() or None,
        "phone2": (c.get("phone2") or "").strip() or None,
        "email": (c.get("email") or "").strip() or None,
        "email2": (c.get("email2") or "").strip() or None,
        "linkedin_url": (c.get("linkedin_url") or "").strip() or None,
        "x_handle": (c.get("x_handle") or "").strip() or None,
        "intimacy_grade": c.get("intimacy_grade"),
        "decision_maker_score": c.get("decision_maker_score"),
        "other_contact_detail": (c.get("other_contact_detail") or "").strip() or None,
        "fit_reason": (c.get("fit_reason") or "").strip() or None,
        "contact_evidence_summary": (c.get("contact_evidence_summary") or "").strip() or None,
        "evidence": c.get("evidence") if isinstance(c.get("evidence"), list) else [],
        "phase1_research": phase1,
        "phase2_contact_enrichment": phase2,
    }


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--jsons-dir",
        type=Path,
        default=Path("jsons"),
        help="Directory containing *.enriched.json",
    )
    p.add_argument(
        "--out",
        type=Path,
        default=Path("jsons") / "intimate_phone_contacts.json",
        help="Output JSON path",
    )
    args = p.parse_args()
    jsons: Path = args.jsons_dir
    out: Path = args.out

    rows_by_key: dict[str, dict[str, Any]] = {}
    sources: list[str] = []

    for path in sorted(jsons.glob("*.enriched.json")):
        if path.resolve() == out.resolve():
            continue
        sources.append(path.name)
        data = json.loads(path.read_text(encoding="utf-8"))
        p1 = phase1_run_meta(data, path.name)
        p2 = contact_enrichment_meta(data)
        for c in data.get("contacts") or []:
            if not isinstance(c, dict) or not has_structured_phone(c):
                continue
            k = dedupe_key(c)
            row = build_row(c, phase1=p1, phase2=p2)
            prev = rows_by_key.get(k)
            if prev is None or score_for_pick(row) > score_for_pick(prev):
                rows_by_key[k] = row

    contacts = sorted(rows_by_key.values(), key=lambda r: (r.get("full_name") or "").lower())

    payload: dict[str, Any] = {
        "version": 2,
        "criteria": (
            "Contacts with non-empty phone or phone2; includes Phase 1 narrative (fit_reason, "
            "contact_evidence_summary, evidence) and Phase 1 run metadata per source file, plus "
            "Phase 2 contact_enrichment summary when present. Deduped globally (linkedin > email > "
            "name+company); tie-break by intimacy_grade then count of filled phone fields."
        ),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_enriched_files": sources,
        "count": len(contacts),
        "contacts": contacts,
    }

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {out} count={len(contacts)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
