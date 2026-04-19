"""Checkpoint atomic write tests."""

from __future__ import annotations

import json
from pathlib import Path

from contact_enrichment.checkpoint import load_checkpoint, save_checkpoint_atomic


def test_save_and_load_checkpoint(tmp_path: Path) -> None:
    p = tmp_path / "ck.json"
    data = {"version": 1, "completed_request_ids": ["c_abcd"], "rows_json": {}}
    save_checkpoint_atomic(p, data)
    loaded = load_checkpoint(p)
    assert loaded == data


def test_load_missing_returns_none(tmp_path: Path) -> None:
    assert load_checkpoint(tmp_path / "nope.json") is None


def test_atomic_replace(tmp_path: Path) -> None:
    p = tmp_path / "ck.json"
    save_checkpoint_atomic(p, {"a": 1})
    save_checkpoint_atomic(p, {"a": 2})
    assert json.loads(p.read_text(encoding="utf-8")) == {"a": 2}
