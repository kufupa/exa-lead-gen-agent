from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


def test_print_generated_json_mode(tmp_path: Path) -> None:
    state = {
        "version": 1,
        "by_id": {
            "oh_test1": {
                "outreach_id": "oh_test1",
                "primary_email": "a@b.co",
                "hotel_canonical_url": "https://www.example.com",
                "intimate_snapshot": {"full_name": "Pat"},
                "triage": {"status": "approved_generate"},
                "generation": {"subject": "Hi", "body": "Hello there."},
            }
        },
        "indexes": {"by_hotel": {}},
    }
    p = tmp_path / "outreach_email_state.json"
    p.write_text(json.dumps(state), encoding="utf-8")
    root = Path(__file__).resolve().parent.parent
    r = subprocess.run(
        [
            sys.executable,
            str(root / "scripts" / "outreach_email_print_generated.py"),
            "--state-path",
            str(p),
            "--json",
        ],
        cwd=str(root),
        capture_output=True,
        text=True,
    )
    assert r.returncode == 0, r.stderr
    out = json.loads(r.stdout)
    assert len(out) == 1
    assert out[0]["subject"] == "Hi"
    assert out[0]["body"] == "Hello there."
