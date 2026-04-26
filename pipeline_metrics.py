"""Shared cost and usage estimation utilities for xAI and Exa."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class XaiRates:
    """Token + tool pricing knobs for xAI calls."""

    input_usd_per_mtok: float = 2.0
    output_usd_per_mtok: float = 6.0
    cached_input_usd_per_mtok: float = 0.20
    xai_web_search_usd_per_1k: float = 5.0
    xai_x_search_usd_per_1k: float = 5.0


@dataclass(frozen=True)
class ExaRates:
    """Rate knobs for Exa API calls."""

    search_usd_per_1k: float = 7.0
    contents_usd_per_1k: float = 1.0


def _int(v: Any) -> int:
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v)
    if isinstance(v, str):
        s = v.strip()
        if s.lstrip("-").isdigit():
            return int(s)
    return 0


def _canonical_tool_key(raw: Any) -> str:
    """Map xAI tool usage keys (ints, enums, categories) to web_search | x_search | other."""
    if raw == 1 or raw == "1" or raw == "SERVER_SIDE_TOOL_WEB_SEARCH" or str(raw) == "web_search":
        return "web_search"
    if raw == 2 or raw == "2" or raw == "SERVER_SIDE_TOOL_X_SEARCH" or str(raw) == "x_search":
        return "x_search"
    s = str(raw).strip()
    if "WEB_SEARCH" in s.upper() or s.lower() in ("web_search", "browse_page", "web_search_with_snippets"):
        return "web_search"
    if "X_SEARCH" in s.upper() or s.lower().startswith("x_"):
        return "x_search"
    return s if s else "other"


def _to_dict(usage: Any) -> dict[str, Any]:
    if usage is None:
        return {}
    if isinstance(usage, dict):
        return usage
    if hasattr(usage, "items"):
        try:
            return dict(usage.items())  # type: ignore[arg-type]
        except TypeError:
            pass

    out: dict[str, Any] = {}
    for name in dir(usage):
        if name.startswith("_"):
            continue
        try:
            val = getattr(usage, name)
        except Exception:
            continue
        if callable(val):
            continue
        if isinstance(val, (int, float, str, bool, list, dict, type(None))):
            out[name] = val
    return out


def count_xai_tools(usage: Any) -> dict[str, int]:
    """
    Return server-side tool invocation counts extracted from xAI usage metadata.

    Supports `server_side_tools_used` (list of enum ids/names) and
    `server_side_tool_usage` (already aggregated map).
    """

    u = _to_dict(usage)
    raw_usage = u.get("server_side_tool_usage")
    if isinstance(raw_usage, dict):
        out: dict[str, int] = {}
        for k, v in raw_usage.items():
            n = _int(v)
            if n <= 0:
                continue
            key = _canonical_tool_key(k)
            out[key] = out.get(key, 0) + n
        return out

    out: dict[str, int] = {}
    for item in u.get("server_side_tools_used", []) or []:
        key = _canonical_tool_key(item)
        out[key] = out.get(key, 0) + 1
    return out


def merge_xai_usage_dicts(*usages: dict[str, Any]) -> dict[str, Any]:
    """Sum numeric usage counters and merge tool lists/maps for multi-attempt runs."""
    merged: dict[str, Any] = {}
    tools_used: list[Any] = []
    tool_usage_maps: list[dict[str, Any]] = []

    for u in usages:
        if not u:
            continue
        for key in (
            "prompt_tokens",
            "cached_prompt_text_tokens",
            "completion_tokens",
            "reasoning_tokens",
            "output_tokens",
            "total_tokens",
        ):
            merged[key] = merged.get(key, 0) + _int(u.get(key))
        lst = u.get("server_side_tools_used")
        if isinstance(lst, list):
            tools_used.extend(lst)
        raw_map = u.get("server_side_tool_usage")
        if isinstance(raw_map, dict):
            tool_usage_maps.append(raw_map)

    if tools_used:
        merged["server_side_tools_used"] = tools_used
    if tool_usage_maps:
        combined: dict[str, int] = {}
        for m in tool_usage_maps:
            for k, v in m.items():
                ck = _canonical_tool_key(k)
                combined[ck] = combined.get(ck, 0) + _int(v)
        merged["server_side_tool_usage"] = combined
    return merged


def estimate_xai_cost(
    usage: Any,
    *,
    rates: XaiRates | None = None,
    token_price_multiplier: float = 1.0,
) -> dict[str, Any]:
    """
    Estimate local USD cost from a usage blob.

    Reasoning tokens are counted as output tokens.
    Cached prompt tokens use cached_input price if present.
    """
    rates = rates or XaiRates()
    u = _to_dict(usage)

    prompt_tokens = _int(u.get("prompt_tokens"))
    cached_prompt_tokens = _int(u.get("cached_prompt_text_tokens"))
    completion_tokens = _int(u.get("completion_tokens"))
    reasoning_tokens = _int(u.get("reasoning_tokens"))
    output_tokens = _int(u.get("output_tokens"))
    total_tokens = _int(u.get("total_tokens"))
    if completion_tokens == 0 and output_tokens:
        completion_tokens = output_tokens

    cached_prompt_tokens = min(cached_prompt_tokens, prompt_tokens)
    non_cached_prompt = max(0, prompt_tokens - cached_prompt_tokens)
    output_like_tokens = max(0, completion_tokens + reasoning_tokens)

    mult = max(0.0, float(token_price_multiplier))
    token_usd = (
        (non_cached_prompt / 1_000_000.0) * rates.input_usd_per_mtok
        + (cached_prompt_tokens / 1_000_000.0) * rates.cached_input_usd_per_mtok
        + (output_like_tokens / 1_000_000.0) * rates.output_usd_per_mtok
    ) * mult

    tools = count_xai_tools(u)
    web_search_count = tools.get("web_search", 0)
    x_search_count = tools.get("x_search", 0)
    tool_usd = (web_search_count / 1000.0) * rates.xai_web_search_usd_per_1k + (
        x_search_count / 1000.0
    ) * rates.xai_x_search_usd_per_1k

    total_usd = token_usd + tool_usd
    return {
        "approx_total_usd": round(total_usd, 6),
        "token_component_usd": round(token_usd, 6),
        "xai_tool_component_usd": round(tool_usd, 6),
        "token_price_multiplier": mult,
        "counts": {
            "prompt_tokens": prompt_tokens,
            "cached_prompt_text_tokens": cached_prompt_tokens,
            "completion_tokens": completion_tokens,
            "reasoning_tokens": reasoning_tokens,
            "total_tokens_reported": total_tokens,
            "output_like_tokens": output_like_tokens,
            "server_side_tool_invocations": sum(tools.values()),
            "server_side_tool_usage": tools,
            "web_search_invocations_estimated": web_search_count,
            "x_search_invocations_estimated": x_search_count,
        },
        "rates_used": {
            "input_usd_per_million_tokens": rates.input_usd_per_mtok,
            "output_usd_per_million_tokens": rates.output_usd_per_mtok,
            "cached_prompt_input_usd_per_million_tokens": rates.cached_input_usd_per_mtok,
            "web_search_usd_per_1000_invocations": rates.xai_web_search_usd_per_1k,
            "x_search_usd_per_1000_invocations": rates.xai_x_search_usd_per_1k,
            "token_price_multiplier": mult,
        },
    }


def estimate_exa_cost(
    *,
    search_requests: int = 0,
    content_pages: int = 0,
    rates: ExaRates | None = None,
) -> dict[str, Any]:
    """
    Estimate local USD cost for Exa API usage.
    """
    rates = rates or ExaRates()
    search_requests_i = max(0, search_requests)
    content_pages_i = max(0, content_pages)
    search_usd = (search_requests_i / 1000.0) * rates.search_usd_per_1k
    contents_usd = (content_pages_i / 1000.0) * rates.contents_usd_per_1k
    return {
        "approx_total_usd": round(search_usd + contents_usd, 6),
        "search_component_usd": round(search_usd, 6),
        "contents_component_usd": round(contents_usd, 6),
        "counts": {
            "search_requests": search_requests_i,
            "content_pages": content_pages_i,
        },
        "rates_used": {
            "search_usd_per_1000": rates.search_usd_per_1k,
            "contents_usd_per_1000": rates.contents_usd_per_1k,
        },
    }

