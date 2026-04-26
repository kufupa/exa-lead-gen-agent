"""Tests for aggregate builder usage compaction and phase1 metadata."""

from __future__ import annotations

from lead_aggregates.builders import compact_usage, phase1_run_meta


def test_compact_usage_normalizes_server_side_tool_usage_map() -> None:
    u: dict = {
        "prompt_tokens": 10,
        "server_side_tool_usage": {"1": 2, "2": 1},
    }
    c = compact_usage(u)
    assert c is not None
    assert c["server_side_tool_invocations"] == 3
    assert c["server_side_tools_breakdown"]["web_search"] == 2
    assert c["server_side_tools_breakdown"]["x_search"] == 1


def test_phase1_run_meta_includes_cost_estimate_when_present() -> None:
    data = {
        "target_url": "https://hotel.example/",
        "generated_at_utc": "2026-01-01T00:00:00+00:00",
        "model": "grok-test",
        "usage": {"prompt_tokens": 100},
        "cost_estimate": {"approx_total_usd": 0.042, "token_component_usd": 0.04},
    }
    meta = phase1_run_meta(data, "hotel.enriched.json")
    assert meta["cost_estimate"] == data["cost_estimate"]
    assert meta["usage_summary"] is not None
