from __future__ import annotations

import time
from typing import Any

from pipeline.models import PipelineTelemetry, StageTelemetry
from pipeline_metrics import ExaRates, XaiRates, estimate_exa_cost, estimate_xai_cost, merge_xai_usage_dicts


def new_telemetry() -> PipelineTelemetry:
    return PipelineTelemetry()


def record_exa_stage(
    tel: PipelineTelemetry,
    *,
    stage: str,
    search_delta: int,
    fetch_delta: int,
    seconds: float,
    notes: list[str] | None = None,
) -> None:
    tel.exa_search_requests += max(0, search_delta)
    tel.exa_content_pages += max(0, fetch_delta)
    stage_cost = estimate_exa_cost(
        search_requests=max(0, search_delta),
        content_pages=max(0, fetch_delta),
        rates=ExaRates(),
    )
    tel.stages.append(
        StageTelemetry(
            stage=stage,
            provider="exa",
            calls=max(0, search_delta) + max(0, fetch_delta),
            estimated_usd=float(stage_cost.get("approx_total_usd", 0.0)),
            seconds=seconds,
            notes=notes or [],
        )
    )


def finalize_exa_cost_on_telemetry(tel: PipelineTelemetry) -> dict[str, Any]:
    return estimate_exa_cost(
        search_requests=tel.exa_search_requests,
        content_pages=tel.exa_content_pages,
        rates=ExaRates(),
    )


def record_xai_stage(
    tel: PipelineTelemetry,
    *,
    stage: str,
    usages: list[dict[str, Any]],
    seconds: float,
    notes: list[str] | None = None,
) -> None:
    merged = merge_xai_usage_dicts(*usages) if usages else {}
    cost = estimate_xai_cost(merged, rates=XaiRates())
    tokens_in = int(merged.get("prompt_tokens", 0) or 0)
    tokens_out = int(merged.get("completion_tokens", 0) or 0) + int(merged.get("reasoning_tokens", 0) or 0)
    tel.stages.append(
        StageTelemetry(
            stage=stage,
            provider="xai",
            calls=len(usages),
            tokens_in=tokens_in,
            tokens_out=tokens_out,
            estimated_usd=float(cost.get("approx_total_usd", 0.0)),
            seconds=seconds,
            notes=notes or [],
        )
    )


def timed_stage(name: str):
    """Context manager style via generator for simple timing."""

    class _CM:
        def __init__(self) -> None:
            self.t0 = 0.0
            self.dt = 0.0

        def __enter__(self) -> "_CM":
            self.t0 = time.perf_counter()
            return self

        def __exit__(self, *args: object) -> None:
            self.dt = time.perf_counter() - self.t0

    return _CM()
