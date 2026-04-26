from __future__ import annotations

import hashlib
import json
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field

from outreach.schema import TRIAGE_APPROVED


class ColdEmailResult(BaseModel):
    """Model response; match_id must equal outreach batch_request_id."""

    match_id: str = Field(..., description="outreach_id")
    subject: str = ""
    body: str = ""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _realtime_job_id() -> str:
    return "realtime-" + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def user_prompt_hash(user_text: str) -> str:
    return hashlib.sha256(user_text.encode("utf-8")).hexdigest()[:16]


def build_user_message(*, contact_json: dict[str, Any], template_body: str) -> str:
    ctx = json.dumps(contact_json, ensure_ascii=False, indent=2)
    return f"{template_body.strip()}\n\n--- Contact JSON ---\n{ctx}"


def _run_single_cold_email(
    *,
    api_key: str,
    model: str,
    max_turns: int,
    system_text: str,
    user_text: str,
    outreach_id: str,
) -> tuple[str, ColdEmailResult | None, str | None]:
    """Returns (outreach_id, row, error)."""
    try:
        from xai_sdk import Client
        from xai_sdk.chat import system, user
    except ImportError as e:  # pragma: no cover
        return outreach_id, None, f"xai_sdk import failed: {e}"

    client = Client(api_key=api_key)
    chat = client.chat.create(
        model=model,
        max_turns=max_turns,
        store_messages=True,
        response_format=ColdEmailResult,
    )
    chat.append(system(system_text.strip()))
    chat.append(user(user_text))
    final = chat.sample()
    raw = (final.content or "").strip()
    if not raw:
        return outreach_id, None, "empty model response"
    try:
        row = ColdEmailResult.model_validate_json(raw)
    except Exception as e:
        return outreach_id, None, f"json parse: {e}"
    if row.match_id != outreach_id:
        return outreach_id, None, f"match_id mismatch: got {row.match_id!r} expected {outreach_id!r}"
    return outreach_id, row, None


def run_cold_email_realtime(
    jobs: list[tuple[str, dict[str, Any]]],
    *,
    model: str,
    max_turns: int,
    system_prompt_text: str,
    user_template_text: str,
    concurrency: int,
) -> tuple[dict[str, ColdEmailResult], list[dict[str, str]], str]:
    """
    jobs: list of (outreach_id, contact_row_dict for JSON context).
    Returns (rows_by_outreach_id, failures, job_id) where job_id is a synthetic 'realtime-<utc>' tag.
    """
    api_key = os.environ.get("XAI_API_KEY", "")
    if not api_key:
        raise SystemExit("Missing XAI_API_KEY")

    job_id = _realtime_job_id()
    rows: dict[str, ColdEmailResult] = {}
    failures: list[dict[str, str]] = []

    if not jobs:
        return rows, failures, job_id

    workers = max(1, min(concurrency, len(jobs)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futs = []
        for outreach_id, contact_row in jobs:
            user_text = build_user_message(contact_json=contact_row, template_body=user_template_text)
            futs.append(
                ex.submit(
                    _run_single_cold_email,
                    api_key=api_key,
                    model=model,
                    max_turns=max_turns,
                    system_text=system_prompt_text,
                    user_text=user_text,
                    outreach_id=outreach_id,
                )
            )
        for fut in as_completed(futs):
            oid, row, err = fut.result()
            if err or row is None:
                failures.append({"request_id": oid or "unknown", "error": err or "unknown"})
            else:
                rows[oid] = row

    return rows, failures, job_id


def default_prompt_paths(repo_root: Path) -> tuple[Path, Path]:
    d = repo_root / "outreach" / "prompts"
    return d / "cold_email_system_v1.txt", d / "cold_email_user_template_v1.txt"


def load_prompt_pair(system_path: Path, user_template_path: Path) -> tuple[str, str]:
    return system_path.read_text(encoding="utf-8"), user_template_path.read_text(encoding="utf-8")


def build_intimate_index_by_outreach_id(
    intimate_doc: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    from outreach.ids import compute_outreach_id, primary_delivery_email, target_url_from_intimate_row

    out: dict[str, dict[str, Any]] = {}
    for row in intimate_doc.get("contacts") or []:
        if not isinstance(row, dict):
            continue
        pem = primary_delivery_email(row)
        if not pem:
            continue
        tu = target_url_from_intimate_row(row)
        if not tu:
            continue
        oid = compute_outreach_id(pem, tu)
        out[oid] = row
    return out


def generation_candidates(doc: dict[str, Any]) -> list[str]:
    """Outreach ids eligible for new submit (approved, no completed body)."""
    by_id = doc.get("by_id") or {}
    ids: list[str] = []
    if not isinstance(by_id, dict):
        return ids
    for oid, row in by_id.items():
        if not isinstance(row, dict):
            continue
        triage = row.get("triage") or {}
        if not isinstance(triage, dict):
            continue
        if triage.get("status") != TRIAGE_APPROVED:
            continue
        gen = row.get("generation")
        if isinstance(gen, dict) and (gen.get("body") or "").strip():
            continue
        ids.append(str(oid))
    return sorted(ids)


def apply_generation_results(
    doc: dict[str, Any],
    *,
    batch_id: str,
    model: str,
    system_prompt_id: str,
    outreach_ids: list[str],
    user_prompt_hashes: dict[str, str],
    rows_ok: dict[str, ColdEmailResult],
    failures: list[dict[str, str]],
) -> None:
    """Mutate doc.by_id in place for rows in outreach_ids.

    Param name `batch_id` is preserved for backward compat with tests/state JSON;
    the value may be a synthetic realtime id (e.g. "realtime-<utc>").
    """
    by_id = doc.setdefault("by_id", {})
    assert isinstance(by_id, dict)
    fail_by_oid = {str(f.get("request_id", "")): str(f.get("error", "") or "unknown") for f in failures}
    completed = _now()

    for oid in outreach_ids:
        row = by_id.get(oid)
        if not isinstance(row, dict):
            continue
        h = user_prompt_hashes.get(oid)
        prev = row.get("generation") if isinstance(row.get("generation"), dict) else {}
        req_at = prev.get("requested_at_utc") if isinstance(prev, dict) else None
        if not req_at:
            req_at = completed

        base: dict[str, Any] = {
            "provider": "xai",
            "model": model,
            "batch_job_id": batch_id,
            "system_prompt_id": system_prompt_id,
            "user_prompt_hash": h,
            "requested_at_utc": req_at,
            "completed_at_utc": completed,
            "subject": None,
            "body": None,
            "error": None,
        }

        if oid in rows_ok:
            r = rows_ok[oid]
            base["subject"] = r.subject.strip()
            base["body"] = r.body.strip()
            row["generation"] = base
        else:
            base["error"] = (fail_by_oid.get(oid) or "missing batch result")[:4000]
            row["generation"] = base

    doc["updated_at_utc"] = _now()
