from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def empty_registry() -> dict[str, Any]:
    return {"version": 1, "updated_at_utc": _now(), "urls": {}}


def apply_patch(reg: dict[str, Any], url: str, patch: dict[str, Any]) -> dict[str, Any]:
    reg = {**reg, "updated_at_utc": _now()}
    urls = dict(reg.get("urls") or {})
    cur = dict(urls.get(url) or {})
    cur.update(patch)
    urls[url] = cur
    reg["urls"] = urls
    return reg
