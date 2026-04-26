"""CLI dry-run smoke test."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_cli_dry_run(tmp_path: Path) -> None:
    script = Path(__file__).resolve().parents[1] / "hotel_contact_enrichment.py"
    dummy = tmp_path / "in.json"
    dummy.write_text('{"target_url": "https://h.com", "contacts": []}', encoding="utf-8")
    out = tmp_path / "out.json"
    r = subprocess.run(
        [
            sys.executable,
            str(script),
            "--in-json",
            str(dummy),
            "--out-json",
            str(out),
            "--dry-run",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert r.returncode == 0
    assert "contacts_total=0" in r.stdout
    assert "mode=realtime" in r.stdout


def test_cli_rejects_batch_chunk_size_below_one(tmp_path: Path) -> None:
    script = Path(__file__).resolve().parents[1] / "hotel_contact_enrichment.py"
    dummy = tmp_path / "in.json"
    dummy.write_text('{"target_url": "https://h.com", "contacts": []}', encoding="utf-8")
    out = tmp_path / "out.json"
    r = subprocess.run(
        [
            sys.executable,
            str(script),
            "--in-json",
            str(dummy),
            "--out-json",
            str(out),
            "--mode",
            "batch",
            "--batch-chunk-size",
            "0",
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert r.returncode == 1
    assert "batch-chunk-size" in (r.stdout + r.stderr).lower()
