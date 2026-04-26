from __future__ import annotations

from types import SimpleNamespace

from pipeline.config import PipelineConfig
from pipeline.exa_discovery import discover
from pipeline.models import PipelineTelemetry


class FakeExa:
    def __init__(self) -> None:
        self.searches: list[tuple[str, dict]] = []

    def search(self, query: str, num_results: int = 10, **kwargs: object) -> object:
        self.searches.append((query, {"num_results": num_results, **kwargs}))
        item = SimpleNamespace(
            url="https://www.linkedin.com/in/demo",
            title="Alex Smith - General Manager | LinkedIn",
            text="snippet",
            score=0.9,
        )
        return SimpleNamespace(results=[item])

    def get_contents(self, urls: list[str], **kwargs: object) -> object:
        return SimpleNamespace(results=[])


def test_discover_emits_candidate_and_telemetry() -> None:
    tel = PipelineTelemetry()
    hotel, sources, cands = discover(
        "https://hotel.example.com/",
        PipelineConfig(max_exa_searches=5),
        FakeExa(),
        tel,
    )
    assert hotel.input_url.startswith("https://")
    assert sources
    assert cands
    assert tel.exa_search_requests >= 1
