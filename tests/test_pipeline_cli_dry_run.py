from __future__ import annotations

from pathlib import Path

from pipeline.cli import run_pipeline
from pipeline.config import PipelineConfig


def test_run_pipeline_dry_run(tmp_path: Path) -> None:
    cfg = PipelineConfig()
    res = run_pipeline("https://www.grangehotels.com/", cfg, out_dir=tmp_path, dry_run=True)
    assert res.candidates == []
    assert res.source_pack_json
    assert "grangehotels" in res.source_pack_json.lower()
    assert "gap_jobs" in res.source_pack_json
    assert "pipeline_version" in res.source_pack_json
