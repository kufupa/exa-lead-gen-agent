from __future__ import annotations

from types import SimpleNamespace

from pipeline.exa_verify import run_exa_jobs
from pipeline.grok_discovery import new_job_id
from pipeline.models import ExaJob
from pipeline.telemetry import new_telemetry


class _Item:
    def __init__(self, url: str, title: str | None = None, text: str | None = None) -> None:
        self.url = url
        self.title = title
        self.text = text


class _SearchRes:
    def __init__(self, items: list[_Item]) -> None:
        self.results = items


class _FakeExa:
    def search(self, query: str, num_results: int = 10, **kwargs):  # noqa: ANN001
        return _SearchRes([_Item("https://example.com/p", "Example", "snippet")])

    def get_contents(self, urls: list[str], **kwargs):  # noqa: ANN001
        return SimpleNamespace(results=[SimpleNamespace(text="call us reservations@example.com")])


def test_run_exa_jobs_attaches_by_candidate_id() -> None:
    tel = new_telemetry()
    jid = new_job_id()
    jobs = [
        ExaJob(
            job_id=jid,
            kind="person_verify",
            query='"Pat" "Kaya Great Northern Hotel"',
            candidate_id="c_deadbeef",
            max_results=3,
        )
    ]
    out = run_exa_jobs(jobs, _FakeExa(), tel, max_searches=5, max_fetches=3)
    assert "c_deadbeef" in out
    assert out["c_deadbeef"][0].url.startswith("https://")
