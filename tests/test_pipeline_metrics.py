from __future__ import annotations

from pipeline_metrics import (
    ExaRates,
    XaiRates,
    count_xai_tools,
    estimate_exa_cost,
    estimate_xai_cost,
    merge_xai_usage_dicts,
)


def test_count_xai_tools_from_list_of_integers() -> None:
    usage = {"server_side_tools_used": [1, "SERVER_SIDE_TOOL_WEB_SEARCH", "x_search", 2, "other"]}
    counts = count_xai_tools(usage)
    assert counts["web_search"] == 2
    assert counts["x_search"] == 2


def test_count_xai_tools_from_usage_map() -> None:
    usage = {"server_side_tool_usage": {"web_search": 3, "x_search": 2}}
    counts = count_xai_tools(usage)
    assert counts["web_search"] == 3
    assert counts["x_search"] == 2


def test_xai_cost_treats_reasoning_as_output() -> None:
    usage = {
        "prompt_tokens": 1_000_000,
        "cached_prompt_text_tokens": 200_000,
        "completion_tokens": 100_000,
        "reasoning_tokens": 200_000,
        "server_side_tools_used": [1, 2],
    }
    cost = estimate_xai_cost(usage, rates=XaiRates(input_usd_per_mtok=2.0, output_usd_per_mtok=6.0, cached_input_usd_per_mtok=0.2))
    assert cost["counts"]["output_like_tokens"] == 300_000
    assert cost["rates_used"]["cached_prompt_input_usd_per_million_tokens"] == 0.2
    assert cost["counts"]["web_search_invocations_estimated"] == 1
    assert cost["counts"]["x_search_invocations_estimated"] == 1
    assert cost["approx_total_usd"] > 0


def test_xai_cost_uses_cached_prompt_price_not_full_prompt_price() -> None:
    usage = {
        "prompt_tokens": 1000,
        "cached_prompt_text_tokens": 1000,
        "completion_tokens": 0,
        "reasoning_tokens": 0,
        "server_side_tools_used": [],
    }
    cost = estimate_xai_cost(
        usage,
        rates=XaiRates(input_usd_per_mtok=2.0, output_usd_per_mtok=6.0, cached_input_usd_per_mtok=0.2),
    )
    assert cost["token_component_usd"] == round((1000 / 1_000_000) * 0.2, 6)


def test_exa_cost_formula() -> None:
    cost = estimate_exa_cost(search_requests=1400, content_pages=250, rates=ExaRates(search_usd_per_1k=7.0, contents_usd_per_1k=1.0))
    assert cost["counts"]["search_requests"] == 1400
    assert cost["counts"]["content_pages"] == 250
    assert cost["search_component_usd"] == 9.8
    assert cost["contents_component_usd"] == 0.25
    assert cost["approx_total_usd"] == 10.05


def test_count_xai_tools_canonicalizes_numeric_map_keys() -> None:
    usage = {"server_side_tool_usage": {"1": 2, "2": 1}}
    counts = count_xai_tools(usage)
    assert counts["web_search"] == 2
    assert counts["x_search"] == 1


def test_merge_xai_usage_dicts_sums_tokens_and_tools() -> None:
    a = {"prompt_tokens": 100, "completion_tokens": 10, "server_side_tools_used": [1]}
    b = {"prompt_tokens": 50, "reasoning_tokens": 5, "server_side_tool_usage": {"web_search": 2}}
    m = merge_xai_usage_dicts(a, b)
    assert m["prompt_tokens"] == 150
    assert m["completion_tokens"] == 10
    assert m["reasoning_tokens"] == 5

