"""Checkpoint resume: completed ids must have matching rows_json."""

from __future__ import annotations

import json
from pathlib import Path

from contact_enrichment.checkpoint import load_checkpoint
from contact_enrichment.types import ChannelResearchRow


def test_resume_drops_completed_without_row(tmp_path: Path) -> None:
    """completed_request_ids entries missing from rows_json are not treated as done."""
    ck = tmp_path / "ck.json"
    ck.write_text(
        json.dumps(
            {
                "version": 1,
                "completed_request_ids": ["c_orphan", "c_hasrow"],
                "rows_json": {
                    "c_hasrow": {
                        "match_id": "c_hasrow",
                        "email": "x@y.com",
                        "status": "ok",
                        "source_urls": [],
                    }
                },
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    data = load_checkpoint(str(ck))
    assert data
    ck_rows = {k: ChannelResearchRow.model_validate(v) for k, v in data["rows_json"].items()}
    raw_done = set(data.get("completed_request_ids", []))
    ck_completed = {rid for rid in raw_done if rid in ck_rows}
    assert ck_completed == {"c_hasrow"}
    assert "c_orphan" not in ck_completed
