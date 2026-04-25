from __future__ import annotations

from urllib.parse import urlparse, urlunparse

from hotel_decision_maker_research import _normalize_url


def canonical_hotel_url(url: str) -> str:
    """Single registry key per logical hotel URL (https, lower host, no fragment, no trailing slash on path)."""
    u = _normalize_url(url.strip())
    p = urlparse(u)
    netloc = (p.netloc or "").lower()
    path = (p.path or "").rstrip("/")
    path_part = "/" + path.lstrip("/") if path else ""
    return urlunparse(("https", netloc, path_part, "", "", "")).rstrip("/") if path_part else f"https://{netloc}"
