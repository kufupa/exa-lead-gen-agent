from __future__ import annotations

from pathlib import Path
import json
from typing import Any

from outreach.netloc_filter import netlocs_from_hotel_urls


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def _collect_urls_from_registry(payload: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for url in (payload.get("urls") or {}):
        if isinstance(url, str) and url.strip():
            out.add(url.strip())
    return out


def _collect_urls_from_enriched(payload: dict[str, Any]) -> set[str]:
    out: set[str] = set()
    for run in payload.get("runs") or []:
        if not isinstance(run, dict):
            continue
        url = (run.get("target_url") or "").strip()
        if url:
            out.add(url)
    return out


def load_blocked_netlocs(fulljsons_dir: Path) -> frozenset[str]:
    """Load blocked hosts from existing aggregate outputs in one normalized netloc set."""
    paths = (
        fulljsons_dir / "url_registry.json",
        fulljsons_dir / "all_enriched_leads.json",
    )
    urls: set[str] = set()
    for path in paths:
        payload = _read_json(path)
        if payload is None:
            continue
        if path.name == "url_registry.json":
            urls.update(_collect_urls_from_registry(payload))
        else:
            urls.update(_collect_urls_from_enriched(payload))

    return netlocs_from_hotel_urls(sorted(urls))

