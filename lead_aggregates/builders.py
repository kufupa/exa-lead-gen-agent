from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from hotel_decision_maker_research import is_generic_functional_email
from pipeline_metrics import count_xai_tools


def source_repo_rel(path: Path, jsons_dir: Path) -> str:
    """Path relative to jsons_dir.parent (repo root), e.g. jsons/foo.enriched.json."""
    return path.resolve().relative_to(jsons_dir.resolve().parent).as_posix()


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


def occurrence_id(source_rel: str, contact: dict[str, Any]) -> str:
    return f"{source_rel}::{dedupe_key(contact)}"


def has_structured_phone(c: dict[str, Any]) -> bool:
    return bool((c.get("phone") or "").strip() or (c.get("phone2") or "").strip())


def has_named_email(c: dict[str, Any]) -> bool:
    for k in ("email", "email2"):
        v = (c.get(k) or "").strip()
        if v and not is_generic_functional_email(v):
            return True
    return False


def score_for_pick(c: dict[str, Any]) -> tuple[int, int]:
    rank = {"low": 0, "medium": 1, "high": 2}
    ig = rank.get(str(c.get("intimacy_grade") or "low"), 0)
    extras = (1 if (c.get("phone") or "").strip() else 0) + (1 if (c.get("phone2") or "").strip() else 0)
    return (ig, extras)


def score_for_pick_email(c: dict[str, Any]) -> tuple[int, int]:
    rank = {"low": 0, "medium": 1, "high": 2}
    ig = rank.get(str(c.get("intimacy_grade") or "low"), 0)
    n = 0
    for k in ("email", "email2"):
        v = (c.get(k) or "").strip()
        if v and not is_generic_functional_email(v):
            n += 1
    return (ig, n)


def compact_usage(u: dict[str, Any] | None) -> dict[str, Any] | None:
    if not u:
        return None
    out: dict[str, Any] = {
        k: v for k, v in u.items() if k not in ("server_side_tools_used", "server_side_tool_usage")
    }
    tools_map = count_xai_tools(u)
    if tools_map:
        out["server_side_tool_invocations"] = sum(tools_map.values())
        out["server_side_tools_breakdown"] = dict(tools_map)
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
        "cost_estimate": data.get("cost_estimate") if isinstance(data.get("cost_estimate"), dict) else None,
    }


def contact_enrichment_meta(data: dict[str, Any]) -> dict[str, Any] | None:
    ce = data.get("contact_enrichment")
    if not isinstance(ce, dict):
        return None
    return {
        "version": ce.get("version"),
        "enriched_at_utc": ce.get("enriched_at_utc"),
        "mode": ce.get("mode"),
        "model": ce.get("model"),
        "concurrency": ce.get("concurrency"),
        "skipped_pre_enrichment": ce.get("skipped_pre_enrichment"),
        "attempted": ce.get("attempted"),
        "succeeded": ce.get("succeeded"),
        "failed": ce.get("failed"),
    }


def build_contact_row(
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
        "linkedin_profile": c.get("linkedin_profile"),
        "phase1_research": phase1,
        "phase2_contact_enrichment": phase2,
    }


def _iter_enriched_files(jsons_dir: Path) -> list[Path]:
    return sorted(jsons_dir.glob("*.enriched.json"))


def build_master_document(
    jsons_dir: Path,
    *,
    enriched_paths: Iterable[Path] | None = None,
) -> dict[str, Any]:
    paths = list(enriched_paths) if enriched_paths is not None else _iter_enriched_files(jsons_dir)
    runs: list[dict[str, Any]] = []
    contacts_out: list[dict[str, Any]] = []
    sources: list[str] = []

    for path in paths:
        if not path.is_file():
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        source_rel = source_repo_rel(path, jsons_dir)
        sources.append(source_rel)
        ce = data.get("contact_enrichment") if isinstance(data.get("contact_enrichment"), dict) else {}
        runs.append(
            {
                "source_file": source_rel,
                "target_url": (data.get("target_url") or "").strip(),
                "research_generated_at_utc": data.get("generated_at_utc"),
                "contact_enrichment": ce,
            }
        )
        target_url = (data.get("target_url") or "").strip()
        for c in data.get("contacts") or []:
            if not isinstance(c, dict):
                continue
            oid = occurrence_id(source_rel, c)
            contacts_out.append(
                {
                    "occurrence_id": oid,
                    "source_enriched_json": source_rel,
                    "target_url": target_url,
                    "contact": c,
                }
            )

    return {
        "version": 1,
        "criteria": "Warehouse: one row per contact occurrence per enriched file (occurrence_id = source_file::dedupe_key).",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_enriched_files": sorted(set(sources)),
        "count": len(contacts_out),
        "runs": runs,
        "contacts": contacts_out,
    }


def build_phone_document(jsons_dir: Path) -> dict[str, Any]:
    rows_by_key: dict[str, dict[str, Any]] = {}
    sources: list[str] = []

    for path in _iter_enriched_files(jsons_dir):
        source_rel = source_repo_rel(path, jsons_dir)
        sources.append(source_rel)
        data = json.loads(path.read_text(encoding="utf-8"))
        p1 = phase1_run_meta(data, path.name)
        p2 = contact_enrichment_meta(data)
        for c in data.get("contacts") or []:
            if not isinstance(c, dict) or not has_structured_phone(c):
                continue
            k = dedupe_key(c)
            row = build_contact_row(c, phase1=p1, phase2=p2)
            prev = rows_by_key.get(k)
            if prev is None or score_for_pick(row) > score_for_pick(prev):
                rows_by_key[k] = row

    contacts = sorted(rows_by_key.values(), key=lambda r: (r.get("full_name") or "").lower())
    return {
        "version": 3,
        "criteria": (
            "Contacts with non-empty phone or phone2; Phase 1 narrative + run metadata + Phase 2 summary. "
            "Globally deduped (linkedin > email > name+company); tie-break intimacy_grade then phone fields."
        ),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_enriched_files": sorted(set(sources)),
        "count": len(contacts),
        "contacts": contacts,
    }


def build_email_document(jsons_dir: Path) -> dict[str, Any]:
    rows_by_key: dict[str, dict[str, Any]] = {}
    sources: list[str] = []

    for path in _iter_enriched_files(jsons_dir):
        source_rel = source_repo_rel(path, jsons_dir)
        sources.append(source_rel)
        data = json.loads(path.read_text(encoding="utf-8"))
        p1 = phase1_run_meta(data, path.name)
        p2 = contact_enrichment_meta(data)
        for c in data.get("contacts") or []:
            if not isinstance(c, dict) or not has_named_email(c):
                continue
            k = dedupe_key(c)
            row = build_contact_row(c, phase1=p1, phase2=p2)
            row["contact_key"] = k
            prev = rows_by_key.get(k)
            if prev is None or score_for_pick_email(row) > score_for_pick_email(prev):
                rows_by_key[k] = row

    contacts = sorted(rows_by_key.values(), key=lambda r: (r.get("full_name") or "").lower())
    return {
        "version": 1,
        "criteria": (
            "Contacts with named non-generic email or email2 (see hotel_decision_maker_research.is_generic_functional_email). "
            "Globally deduped; tie-break intimacy_grade then count of named emails."
        ),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_enriched_files": sorted(set(sources)),
        "count": len(contacts),
        "contacts": contacts,
    }


def registry_from_enriched_scan(jsons_dir: Path) -> dict[str, Any]:
    """Rebuild url entries for every enriched file (status enriched when file exists)."""
    from lead_aggregates.urls import canonical_hotel_url

    urls: dict[str, Any] = {}
    for path in _iter_enriched_files(jsons_dir):
        data = json.loads(path.read_text(encoding="utf-8"))
        tu = (data.get("target_url") or "").strip()
        if not tu:
            continue
        key = canonical_hotel_url(tu)
        enriched_rel = source_repo_rel(path, jsons_dir)
        stem = path.name.removesuffix(".enriched.json")
        research_rel = source_repo_rel(path.parent / f"{stem}.json", jsons_dir)
        urls[key] = {
            "status": "enriched",
            "research_json": research_rel,
            "enriched_json": enriched_rel,
            "error": None,
            "claimed_by": None,
            "last_started_at_utc": None,
            "last_finished_at_utc": datetime.now(timezone.utc).isoformat(),
        }
    return {
        "version": 1,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "urls": urls,
    }
