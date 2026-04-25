from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TypeVar

from pydantic import BaseModel, Field

from outreach.schema import TRIAGE_APPROVED

try:
    import grpc
except ImportError:  # pragma: no cover
    grpc = None  # type: ignore[assignment]

_T = TypeVar("_T")

# region agent log
_DEBUG_LOG_PATH = Path(__file__).resolve().parents[1] / "debug-1b2584.log"
_DEBUG_SESSION = "1b2584"


def _agent_debug_log(
    *,
    hypothesis_id: str,
    location: str,
    message: str,
    data: dict[str, Any] | None = None,
) -> None:
    try:
        payload = {
            "sessionId": _DEBUG_SESSION,
            "timestamp": int(time.time() * 1000),
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data or {},
        }
        with _DEBUG_LOG_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        pass


# endregion


def _grpc_transient(exc: BaseException) -> bool:
    if grpc is None:
        return False
    if not isinstance(exc, grpc.RpcError):
        return False
    try:
        code = exc.code()
    except Exception:
        return False
    return code in (
        grpc.StatusCode.UNAVAILABLE,
        grpc.StatusCode.DEADLINE_EXCEEDED,
        grpc.StatusCode.RESOURCE_EXHAUSTED,
    )


def _retry_transient_xai_rpc(
    op_name: str,
    fn: Callable[[], _T],
    *,
    hypothesis_id: str,
    max_attempts: int = 8,
    base_sleep_sec: float = 1.5,
) -> _T:
    attempts = max(1, max_attempts)
    last: BaseException | None = None
    for attempt in range(1, attempts + 1):
        # region agent log
        _agent_debug_log(
            hypothesis_id=hypothesis_id,
            location="outreach/batch_cold_email.py:_retry_transient_xai_rpc",
            message="xai_rpc_attempt",
            data={"op": op_name, "attempt": attempt, "max_attempts": attempts},
        )
        # endregion
        try:
            out = fn()
            if attempt > 1:
                _agent_debug_log(
                    hypothesis_id=hypothesis_id,
                    location="outreach/batch_cold_email.py:_retry_transient_xai_rpc",
                    message="xai_rpc_recovered_after_retry",
                    data={"op": op_name, "attempt": attempt},
                )
            return out
        except BaseException as e:
            last = e
            if not _grpc_transient(e):
                _agent_debug_log(
                    hypothesis_id=hypothesis_id,
                    location="outreach/batch_cold_email.py:_retry_transient_xai_rpc",
                    message="xai_rpc_non_transient_abort",
                    data={
                        "op": op_name,
                        "attempt": attempt,
                        "exc_type": type(e).__name__,
                        "details_snip": (str(e) or "")[:240],
                    },
                )
                raise
            details_snip = ""
            if grpc is not None and isinstance(e, grpc.RpcError):
                try:
                    details_snip = (e.details() or "")[:240]
                except Exception:
                    details_snip = ""
            _agent_debug_log(
                hypothesis_id=hypothesis_id,
                location="outreach/batch_cold_email.py:_retry_transient_xai_rpc",
                message="xai_rpc_transient_backoff",
                data={
                    "op": op_name,
                    "attempt": attempt,
                    "details_snip": details_snip,
                    "will_retry": attempt < attempts,
                },
            )
            if attempt >= attempts:
                raise
            delay = min(60.0, base_sleep_sec * (2 ** (attempt - 1)))
            time.sleep(delay)
    if last is not None:
        raise last
    raise RuntimeError("_retry_transient_xai_rpc: empty loop")


class ColdEmailResult(BaseModel):
    """Model response; match_id must equal outreach batch_request_id."""

    match_id: str = Field(..., description="outreach_id")
    subject: str = ""
    body: str = ""


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def user_prompt_hash(user_text: str) -> str:
    return hashlib.sha256(user_text.encode("utf-8")).hexdigest()[:16]


def build_user_message(*, contact_json: dict[str, Any], template_body: str) -> str:
    ctx = json.dumps(contact_json, ensure_ascii=False, indent=2)
    return f"{template_body.strip()}\n\n--- Contact JSON ---\n{ctx}"


def _build_chat(
    client: Any,
    *,
    model: str,
    max_turns: int,
    system_text: str,
    user_text: str,
    outreach_id: str,
) -> Any:
    from xai_sdk.chat import system, user

    chat = client.chat.create(
        model=model,
        batch_request_id=outreach_id,
        max_turns=max_turns,
        store_messages=True,
        response_format=ColdEmailResult,
    )
    chat.append(system(system_text.strip()))
    chat.append(user(user_text))
    return chat


def _parse_result(item: Any) -> tuple[str, ColdEmailResult | None, str | None]:
    rid = getattr(item, "batch_request_id", "") or ""
    try:
        content = (item.response.content or "").strip()
    except Exception as e:
        return rid, None, f"no response: {e}"
    if not content:
        return rid, None, "empty content"
    try:
        row = ColdEmailResult.model_validate_json(content)
    except Exception as e:
        return rid, None, f"json: {e}"
    if row.match_id != rid:
        return rid, None, f"match_id mismatch got={row.match_id!r} want={rid!r}"
    return rid, row, None


def submit_and_drain_cold_email_batch(
    jobs: list[tuple[str, dict[str, Any]]],
    *,
    model: str,
    max_turns: int,
    batch_name: str,
    system_prompt_text: str,
    user_template_text: str,
    add_chunk_size: int,
    on_submitted_batch_id: Callable[[str], None] | None = None,
    poll_interval_sec: float = 5.0,
) -> tuple[dict[str, ColdEmailResult], list[dict[str, str]], str]:
    """
    jobs: list of (outreach_id, contact_row_dict for JSON context).
    Returns (rows_by_outreach_id, failures, batch_id).
    """
    try:
        from xai_sdk import Client
    except ImportError as e:  # pragma: no cover
        raise SystemExit(f"xai_sdk import failed: {e}") from e

    api_key = os.environ.get("XAI_API_KEY", "")
    if not api_key:
        raise SystemExit("Missing XAI_API_KEY")

    client = Client(api_key=api_key)
    batch = client.batch.create(batch_name=batch_name)
    batch_id = batch.batch_id

    batch_requests: list[Any] = []
    for outreach_id, contact_row in jobs:
        user_text = build_user_message(contact_json=contact_row, template_body=user_template_text)
        batch_requests.append(
            _build_chat(
                client,
                model=model,
                max_turns=max_turns,
                system_text=system_prompt_text,
                user_text=user_text,
                outreach_id=outreach_id,
            )
        )

    for i in range(0, len(batch_requests), max(1, add_chunk_size)):
        chunk = batch_requests[i : i + add_chunk_size]
        client.batch.add(batch_id=batch_id, batch_requests=chunk)

    if on_submitted_batch_id:
        on_submitted_batch_id(batch_id)

    rows: dict[str, ColdEmailResult] = {}
    failures: list[dict[str, str]] = []
    failure_oids: set[str] = set()

    while True:
        b = _retry_transient_xai_rpc(
            "batch.get",
            lambda: client.batch.get(batch_id=batch_id),
            hypothesis_id="H2",
        )
        pending = b.state.num_pending

        pagination_token = None
        while True:
            pt = pagination_token

            def _list_page_submit() -> Any:
                return client.batch.list_batch_results(
                    batch_id=batch_id,
                    limit=100,
                    pagination_token=pt,
                )

            page = _retry_transient_xai_rpc(
                "list_batch_results",
                _list_page_submit,
                hypothesis_id="H1",
            )
            for item in page.succeeded:
                rid, row, err = _parse_result(item)
                if err or row is None:
                    key = rid or "unknown"
                    if key not in failure_oids:
                        failure_oids.add(key)
                        failures.append({"request_id": key, "error": err or "unknown"})
                elif rid not in rows:
                    rows[rid] = row
            for item in page.failed:
                rid = item.batch_request_id
                if rid in failure_oids:
                    continue
                failure_oids.add(rid)
                failures.append({"request_id": rid, "error": item.error_message or "batch item failed"})

            if page.pagination_token is None:
                break
            pagination_token = page.pagination_token

        if pending == 0:
            break
        time.sleep(poll_interval_sec)

    return rows, failures, batch_id


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
    """Outreach ids eligible for new batch submit (approved, no completed body)."""
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
    """Mutate doc.by_id in place for rows in outreach_ids."""
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


def stamp_generation_requested(
    doc: dict[str, Any],
    *,
    outreach_ids: list[str],
    batch_id: str,
    model: str,
    system_prompt_id: str,
    user_prompt_hashes: dict[str, str],
) -> None:
    req = _now()
    by_id = doc.setdefault("by_id", {})
    for oid in outreach_ids:
        row = by_id.get(oid)
        if not isinstance(row, dict):
            continue
        row["generation"] = {
            "provider": "xai",
            "model": model,
            "batch_job_id": batch_id,
            "requested_at_utc": req,
            "completed_at_utc": None,
            "system_prompt_id": system_prompt_id,
            "user_prompt_hash": user_prompt_hashes.get(oid),
            "subject": None,
            "body": None,
            "error": None,
        }
    doc["updated_at_utc"] = _now()


def poll_batch_only(
    batch_id: str,
    *,
    poll_interval_sec: float = 5.0,
) -> tuple[dict[str, ColdEmailResult], list[dict[str, str]]]:
    """Poll an existing xAI batch until complete; return (rows_by_outreach_id, failures)."""
    try:
        from xai_sdk import Client
    except ImportError as e:  # pragma: no cover
        raise SystemExit(f"xai_sdk import failed: {e}") from e

    api_key = os.environ.get("XAI_API_KEY", "")
    if not api_key:
        raise SystemExit("Missing XAI_API_KEY")

    client = Client(api_key=api_key)
    rows: dict[str, ColdEmailResult] = {}
    failures: list[dict[str, str]] = []
    failure_oids: set[str] = set()

    while True:
        b = _retry_transient_xai_rpc(
            "batch.get",
            lambda: client.batch.get(batch_id=batch_id),
            hypothesis_id="H2",
        )
        pending = b.state.num_pending

        pagination_token = None
        while True:
            pt = pagination_token

            def _list_page_poll() -> Any:
                return client.batch.list_batch_results(
                    batch_id=batch_id,
                    limit=100,
                    pagination_token=pt,
                )

            page = _retry_transient_xai_rpc(
                "list_batch_results",
                _list_page_poll,
                hypothesis_id="H1",
            )
            for item in page.succeeded:
                rid, row, err = _parse_result(item)
                if err or row is None:
                    key = rid or "unknown"
                    if key not in failure_oids:
                        failure_oids.add(key)
                        failures.append({"request_id": key, "error": err or "unknown"})
                elif rid not in rows:
                    rows[rid] = row
            for item in page.failed:
                rid = item.batch_request_id
                if rid in failure_oids:
                    continue
                failure_oids.add(rid)
                failures.append({"request_id": rid, "error": item.error_message or "batch item failed"})

            if page.pagination_token is None:
                break
            pagination_token = page.pagination_token

        if pending == 0:
            break
        time.sleep(poll_interval_sec)

    return rows, failures
