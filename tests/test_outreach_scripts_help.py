from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_outreach_scripts_help_exit_zero() -> None:
    root = Path(__file__).resolve().parent.parent
    for script in (
        "outreach_email_sync.py",
        "outreach_email_triage.py",
        "outreach_email_generate_xai.py",
        "outreach_email_flow.py",
        "outreach_email_print_generated.py",
        "browser_use_outlook_bootstrap.py",
    ):
        r = subprocess.run(
            [sys.executable, str(root / "scripts" / script), "--help"],
            cwd=str(root),
            capture_output=True,
            text=True,
        )
        assert r.returncode == 0, (script, r.stderr)
