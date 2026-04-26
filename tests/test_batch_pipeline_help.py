import subprocess
import sys
from pathlib import Path
from types import SimpleNamespace

import hotel_batch_pipeline as pipeline


def test_main_returns_nonzero_when_any_url_fails(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "jsons").mkdir()
    (tmp_path / "fullJSONs").mkdir()
    monkeypatch.chdir(tmp_path)

    def _fail(_url: str, **_: object) -> tuple[str, str]:
        return (_url, "failed research: 9")

    monkeypatch.setattr(pipeline, "_run_one", _fail)
    monkeypatch.setattr(sys, "argv", ["hotel_batch_pipeline.py", "--url", "https://bad.example/"])
    assert pipeline.main() == 1


def test_main_returns_zero_when_all_ok(tmp_path: Path, monkeypatch) -> None:
    (tmp_path / "jsons").mkdir()
    (tmp_path / "fullJSONs").mkdir()
    monkeypatch.chdir(tmp_path)

    def _ok(url: str, **_: object) -> tuple[str, str]:
        return (url, "ok")

    monkeypatch.setattr(pipeline, "_run_one", _ok)
    monkeypatch.setattr(sys, "argv", ["hotel_batch_pipeline.py", "--url", "https://good.example/"])
    assert pipeline.main() == 0


def test_hotel_batch_pipeline_help_exits_zero() -> None:
    root = Path(__file__).resolve().parent.parent
    r = subprocess.run(
        [sys.executable, str(root / "hotel_batch_pipeline.py"), "--help"],
        capture_output=True,
        text=True,
        cwd=str(root),
    )
    assert r.returncode == 0
    assert "skip-if-enriched" in (r.stdout + r.stderr).lower() or "skip" in (r.stdout + r.stderr).lower()


def _stub_phase3_script(tmp_path: Path) -> None:
    d = tmp_path / "scripts"
    d.mkdir(parents=True, exist_ok=True)
    (d / "linkedin_exa_enrich.py").write_text("# stub for tests\n", encoding="utf-8")


class _FakeStore:
    def enriched_entry_file_exists(self, _canon: str) -> bool:
        return False

    def mark_researching(self, **_kwargs: object) -> None:
        return None

    def commit_after_enrich(self, **_kwargs: object) -> bool:
        return True


def test_phase3_not_called_when_linkedin_enrich_flag_unset(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.delenv("LINKEDIN_ENRICH", raising=False)
    calls: list[list[str]] = []

    def _fake_run(cmd, **_kwargs):
        calls.append([str(part) for part in cmd])
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(pipeline.subprocess, "run", _fake_run)

    _, status = pipeline._run_one(
        "https://example.com/hotel",
        root=tmp_path,
        store=_FakeStore(),
        agent_count=1,
        skip_if_enriched=False,
    )

    assert status == "ok"
    assert len(calls) == 2
    assert not any("scripts/linkedin_exa_enrich.py" in " ".join(cmd) for cmd in calls)


def test_phase3_called_when_linkedin_enrich_flag_set(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("LINKEDIN_ENRICH", "1")
    _stub_phase3_script(tmp_path)
    calls: list[list[str]] = []

    def _fake_run(cmd, **_kwargs):
        calls.append([str(part) for part in cmd])
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(pipeline.subprocess, "run", _fake_run)

    _, status = pipeline._run_one(
        "https://example.com/hotel",
        root=tmp_path,
        store=_FakeStore(),
        agent_count=1,
        skip_if_enriched=False,
    )

    assert status == "ok"
    assert len(calls) == 3

    phase3 = calls[2]
    phase3_script = phase3[1].replace("\\", "/")
    assert phase3_script.endswith("scripts/linkedin_exa_enrich.py")
    assert "--in-json" in phase3
    assert "--out-json" in phase3
    assert "--discover-missing" in phase3
    assert "--pretty" in phase3


def test_phase3_failure_non_fatal(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("LINKEDIN_ENRICH", "true")
    _stub_phase3_script(tmp_path)
    calls: list[list[str]] = []

    def _fake_run(cmd, **_kwargs):
        calls.append([str(part) for part in cmd])
        if len(calls) == 3:
            return SimpleNamespace(returncode=7, stdout="", stderr="phase3 failed")
        return SimpleNamespace(returncode=0, stdout="", stderr="")

    monkeypatch.setattr(pipeline.subprocess, "run", _fake_run)

    _, status = pipeline._run_one(
        "https://example.com/hotel",
        root=tmp_path,
        store=_FakeStore(),
        agent_count=1,
        skip_if_enriched=False,
    )

    assert status == "ok"
    assert len(calls) == 3
    phase3_script = calls[2][1].replace("\\", "/")
    assert phase3_script.endswith("scripts/linkedin_exa_enrich.py")
