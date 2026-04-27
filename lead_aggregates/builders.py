from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from hotel_decision_maker_research import is_generic_functional_email
from linkedin_enrich.exa_fetch import normalize_linkedin_url
from pipeline_metrics import count_xai_tools
from lead_aggregates.urls import canonical_hotel_url


def source_repo_rel(path: Path, jsons_dir: Path) -> str:
    """Path relative to jsons_dir.parent (repo root), e.g. jsons/foo.enriched.json."""
    return path.resolve().relative_to(jsons_dir.resolve().parent).as_posix()


def dedupe_key(c: dict[str, Any]) -> str:
    li_raw = (c.get("linkedin_url") or "").strip()
    if li_raw:
        canon = normalize_linkedin_url(li_raw)
        if canon:
            return "li:" + canon.lower()
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


def named_emails(c: dict[str, Any]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for k in ("email", "email2"):
        v = (c.get(k) or "").strip().lower()
        if not v or is_generic_functional_email(v) or v in seen:
            continue
        out.append(v)
        seen.add(v)
    return out


def primary_named_email(c: dict[str, Any]) -> str | None:
    emails = named_emails(c)
    return emails[0] if emails else None


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


def score_for_unified_pick(c: dict[str, Any]) -> tuple[int, int, int, int]:
    """Lexicographic tie-break: phone score tuple, then email score tuple."""
    p = score_for_pick(c)
    e = score_for_pick_email(c)
    return (p[0], p[1], e[0], e[1])


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


def _run_by_source(master_doc: dict[str, Any]) -> dict[str, dict[str, Any]]:
    runs = master_doc.get("runs") if isinstance(master_doc.get("runs"), list) else []
    return {str(r.get("source_file")): r for r in runs if isinstance(r, dict) and r.get("source_file")}


def _source_path(source_rel: str, jsons_dir: Path) -> Path:
    return jsons_dir.resolve().parent.joinpath(*source_rel.split("/"))


def _load_source_doc(
    source_rel: str,
    jsons_dir: Path,
    cache: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if source_rel in cache:
        return cache[source_rel]
    path = _source_path(source_rel, jsons_dir)
    if not path.is_file():
        cache[source_rel] = {}
        return cache[source_rel]
    loaded = json.loads(path.read_text(encoding="utf-8"))
    cache[source_rel] = loaded if isinstance(loaded, dict) else {}
    return cache[source_rel]


def _match_result(
    *,
    status: str,
    method: str | None,
    candidate_id: str | None,
    reason: str,
    candidate_count: int,
    ambiguous_candidate_ids: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "status": status,
        "method": method,
        "candidate_id": candidate_id,
        "reason": reason,
        "candidate_count": candidate_count,
        "ambiguous_candidate_ids": ambiguous_candidate_ids or [],
    }


def _candidate_id(candidate: dict[str, Any]) -> str | None:
    raw = candidate.get("candidate_id")
    return str(raw) if raw else None


def _email_route_values(candidate: dict[str, Any]) -> set[str]:
    values: set[str] = set()
    for route in candidate.get("contact_routes") or []:
        if not isinstance(route, dict):
            continue
        if route.get("kind") != "email":
            continue
        raw = (route.get("value") or "").strip().lower()
        if raw and not is_generic_functional_email(raw):
            values.add(raw)
    return values


def _match_pipeline_candidate(contact: dict[str, Any], source_doc: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    pipeline = source_doc.get("pipeline_v4") if isinstance(source_doc.get("pipeline_v4"), dict) else {}
    candidates = pipeline.get("candidates") if isinstance(pipeline.get("candidates"), list) else []
    candidate_count = len([c for c in candidates if isinstance(c, dict)])
    contact_li = normalize_linkedin_url((contact.get("linkedin_url") or "").strip() or "")
    contact_email_set = set(named_emails(contact))
    contact_name = (contact.get("full_name") or "").strip().lower()
    contact_company = (contact.get("company") or "").strip().lower()

    exact_id = (contact.get("pipeline_v4_candidate_id") or "").strip()
    if exact_id:
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            if _candidate_id(candidate) == exact_id:
                cid = _candidate_id(candidate)
                return candidate, _match_result(
                    status="matched_exact_id",
                    method="candidate_id",
                    candidate_id=cid,
                    reason="contact.pipeline_v4_candidate_id matched candidate.candidate_id",
                    candidate_count=candidate_count,
                )

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        cand_li = normalize_linkedin_url((candidate.get("linkedin_url") or "").strip() or "")
        if contact_li and cand_li and contact_li == cand_li:
            cid = _candidate_id(candidate)
            return candidate, _match_result(
                status="matched_linkedin",
                method="linkedin",
                candidate_id=cid,
                reason="normalized linkedin_url matched",
                candidate_count=candidate_count,
            )

    for candidate in candidates:
        if not isinstance(candidate, dict):
            continue
        if contact_email_set & _email_route_values(candidate):
            cid = _candidate_id(candidate)
            return candidate, _match_result(
                status="matched_email",
                method="email_route",
                candidate_id=cid,
                reason="named contact email matched candidate email route",
                candidate_count=candidate_count,
            )

    name_company_matches: list[dict[str, Any]] = []
    if contact_name and contact_company:
        for candidate in candidates:
            if not isinstance(candidate, dict):
                continue
            cand_name = (candidate.get("full_name") or "").strip().lower()
            cand_company = (candidate.get("company") or "").strip().lower()
            if cand_name == contact_name and cand_company == contact_company:
                name_company_matches.append(candidate)

    if len(name_company_matches) == 1:
        candidate = name_company_matches[0]
        cid = _candidate_id(candidate)
        return candidate, _match_result(
            status="matched_unique_name_company",
            method="unique_name_company",
            candidate_id=cid,
            reason="exact full_name + company matched exactly one candidate",
            candidate_count=candidate_count,
        )
    if len(name_company_matches) > 1:
        ids = [_candidate_id(c) or "" for c in name_company_matches]
        return None, _match_result(
            status="ambiguous_name_company",
            method="unique_name_company",
            candidate_id=None,
            reason="exact full_name + company matched multiple candidates",
            candidate_count=candidate_count,
            ambiguous_candidate_ids=[i for i in ids if i],
        )

    return None, _match_result(
        status="not_found",
        method=None,
        candidate_id=None,
        reason="no exact id, linkedin, email route, or unique name+company match",
        candidate_count=candidate_count,
    )


def _source_run_payload(
    source_rel: str,
    run: dict[str, Any] | None,
    source_doc: dict[str, Any],
) -> dict[str, Any]:
    run = run if isinstance(run, dict) else {}
    pipeline = source_doc.get("pipeline_v4") if isinstance(source_doc.get("pipeline_v4"), dict) else {}
    return {
        "source_enriched_json": source_rel,
        "target_url": (run.get("target_url") or source_doc.get("target_url") or "").strip() or None,
        "research_generated_at_utc": run.get("research_generated_at_utc") or source_doc.get("generated_at_utc"),
        "model": source_doc.get("model"),
        "contact_enrichment": run.get("contact_enrichment")
        if isinstance(run.get("contact_enrichment"), dict)
        else contact_enrichment_meta(source_doc),
        "phase1_research": phase1_run_meta(source_doc, Path(source_rel).name),
        "pipeline_v4_summary": {
            "input_url": pipeline.get("input_url"),
            "resolved_org": pipeline.get("resolved_org"),
            "aliases": pipeline.get("aliases") if isinstance(pipeline.get("aliases"), list) else [],
            "provider_costs": pipeline.get("provider_costs"),
            "quality_metrics": pipeline.get("quality_metrics"),
            "needs_manual_org_review": pipeline.get("needs_manual_org_review"),
        } if pipeline else None,
    }


def build_full_email_contact_row(
    warehouse_row: dict[str, Any],
    *,
    source_run: dict[str, Any] | None,
    source_doc: dict[str, Any],
) -> dict[str, Any]:
    contact = warehouse_row.get("contact") if isinstance(warehouse_row.get("contact"), dict) else {}
    source_rel = (warehouse_row.get("source_enriched_json") or "").strip()
    target_url = (warehouse_row.get("target_url") or "").strip()
    email_key = primary_named_email(contact)
    if not email_key:
        raise ValueError("build_full_email_contact_row called for contact without named email")

    phase1 = phase1_run_meta(source_doc, Path(source_rel).name) if source_doc else {}
    phase2 = contact_enrichment_meta(source_doc) if source_doc else None
    flat = build_contact_row(contact, phase1=phase1, phase2=phase2)
    pipeline_candidate, pipeline_match = _match_pipeline_candidate(contact, source_doc)
    flat.update(
        {
            "email_key": email_key,
            "named_emails": named_emails(contact),
            "contact_key": dedupe_key(contact),
            "occurrence_id": warehouse_row.get("occurrence_id"),
            "source_enriched_json": source_rel,
            "target_url": target_url,
            "hotel_canonical_url": canonical_hotel_url(target_url) if target_url else None,
            "contact": contact,
            "warehouse": {
                "occurrence_id": warehouse_row.get("occurrence_id"),
                "source_enriched_json": source_rel,
                "target_url": target_url,
            },
            "source_run": _source_run_payload(source_rel, source_run, source_doc),
            "pipeline_v4_candidate": pipeline_candidate,
            "pipeline_v4_candidate_match": pipeline_match,
            "duplicate_occurrence_count": 0,
            "duplicate_occurrences": [],
        }
    )
    return flat


def _duplicate_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "occurrence_id": row.get("occurrence_id"),
        "source_enriched_json": row.get("source_enriched_json"),
        "target_url": row.get("target_url"),
        "full_name": row.get("full_name"),
        "title": row.get("title"),
        "company": row.get("company"),
        "email_key": row.get("email_key"),
        "contact_key": row.get("contact_key"),
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
    master = build_master_document(jsons_dir)
    runs = _run_by_source(master)
    source_cache: dict[str, dict[str, Any]] = {}
    rows_by_key: dict[str, dict[str, Any]] = {}
    sources = list(master.get("source_enriched_files") or [])

    for warehouse_row in master.get("contacts") or []:
        if not isinstance(warehouse_row, dict):
            continue
        contact = warehouse_row.get("contact") if isinstance(warehouse_row.get("contact"), dict) else {}
        if not contact or not has_named_email(contact):
            continue
        source_rel = (warehouse_row.get("source_enriched_json") or "").strip()
        target_url = (warehouse_row.get("target_url") or "").strip()
        email_key = primary_named_email(contact)
        if not email_key or not target_url:
            continue
        dedupe_target = canonical_hotel_url(target_url)
        row_key = f"{email_key}\x1f{dedupe_target}"
        source_doc = _load_source_doc(source_rel, jsons_dir, source_cache)
        row = build_full_email_contact_row(
            warehouse_row,
            source_run=runs.get(source_rel),
            source_doc=source_doc,
        )
        prev = rows_by_key.get(row_key)
        if prev is None:
            rows_by_key[row_key] = row
            continue
        if score_for_pick_email(row) > score_for_pick_email(prev):
            row["duplicate_occurrences"] = [*prev.get("duplicate_occurrences", []), _duplicate_summary(prev)]
            row["duplicate_occurrence_count"] = len(row["duplicate_occurrences"])
            rows_by_key[row_key] = row
        else:
            prev.setdefault("duplicate_occurrences", []).append(_duplicate_summary(row))
            prev["duplicate_occurrence_count"] = len(prev["duplicate_occurrences"])

    contacts = sorted(
        rows_by_key.values(),
        key=lambda r: ((r.get("full_name") or "").lower(), r.get("email_key") or "", r.get("target_url") or ""),
    )
    return {
        "version": 2,
        "criteria": (
            "Contacts with named non-generic email or email2. One row per primary named email + canonical hotel URL. "
            "Rows preserve flat compatibility fields plus full contact/source payload for cold-email generation. "
            "Triage/generation/send state is intentionally excluded and remains in outreach_email_state.json."
        ),
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_enriched_files": sorted(set(sources)),
        "count": len(contacts),
        "contacts": contacts,
    }


def build_intimate_unified_document(jsons_dir: Path) -> dict[str, Any]:
    """One row per dedupe_key across contacts that qualify for phone and/or named-email lists."""
    rows_by_key: dict[str, dict[str, Any]] = {}
    sources: list[str] = []

    for path in _iter_enriched_files(jsons_dir):
        source_rel = source_repo_rel(path, jsons_dir)
        sources.append(source_rel)
        data = json.loads(path.read_text(encoding="utf-8"))
        p1 = phase1_run_meta(data, path.name)
        p2 = contact_enrichment_meta(data)
        for c in data.get("contacts") or []:
            if not isinstance(c, dict):
                continue
            if not (has_structured_phone(c) or has_named_email(c)):
                continue
            k = dedupe_key(c)
            row = build_contact_row(c, phase1=p1, phase2=p2)
            row["contact_key"] = k
            prev = rows_by_key.get(k)
            if prev is None or score_for_unified_pick(row) > score_for_unified_pick(prev):
                rows_by_key[k] = row

    contacts = sorted(rows_by_key.values(), key=lambda r: (r.get("full_name") or "").lower())
    return {
        "version": 1,
        "criteria": (
            "Contacts with structured phone and/or named non-generic email; globally deduped by "
            "linkedin (canonical www) then email then name+company. Tie-break: lexicographic "
            "(intimacy+phone extras, then intimacy+named-email count). Canonical single-row view "
            "for outreach; see also intimate_phone_contacts.json and intimate_email_contacts.json."
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
