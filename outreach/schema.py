from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

STATE_VERSION = 1

TRIAGE_PENDING = "pending"
TRIAGE_APPROVED = "approved_generate"
TRIAGE_DECLINED = "declined"

VALID_TRIAGE = frozenset({TRIAGE_PENDING, TRIAGE_APPROVED, TRIAGE_DECLINED})


def empty_state() -> dict[str, Any]:
    return {
        "version": STATE_VERSION,
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "by_id": {},
        "indexes": {"by_hotel": {}},
    }


def _validate_row(oid: str, row: Any) -> list[str]:
    errs: list[str] = []
    if not isinstance(row, dict):
        return [f"{oid}: row not object"]
    if row.get("outreach_id") != oid:
        errs.append(f"{oid}: outreach_id mismatch")
    triage = row.get("triage")
    if not isinstance(triage, dict):
        errs.append(f"{oid}: triage missing")
    else:
        st = triage.get("status")
        if st not in VALID_TRIAGE:
            errs.append(f"{oid}: bad triage.status {st!r}")
    return errs


def validate_state(doc: dict[str, Any]) -> list[str]:
    errs: list[str] = []
    if not isinstance(doc, dict):
        return ["root not object"]
    v = doc.get("version")
    if v != STATE_VERSION:
        errs.append(f"version: expected {STATE_VERSION}, got {v!r}")
    by_id = doc.get("by_id")
    if not isinstance(by_id, dict):
        errs.append("by_id not object")
        return errs
    for oid, row in by_id.items():
        errs.extend(_validate_row(str(oid), row))
    idx = doc.get("indexes")
    if idx is not None and not isinstance(idx, dict):
        errs.append("indexes not object")
    bh = (idx or {}).get("by_hotel") if isinstance(idx, dict) else None
    if bh is not None:
        if not isinstance(bh, dict):
            errs.append("indexes.by_hotel not object")
        else:
            for hotel, ids in bh.items():
                if not isinstance(ids, list):
                    errs.append(f"indexes.by_hotel[{hotel!r}] not array")
                    continue
                for x in ids:
                    if x not in by_id:
                        errs.append(f"indexes orphan id {x!r} under {hotel!r}")
    return errs
