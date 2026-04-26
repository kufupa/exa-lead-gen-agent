#!/usr/bin/env python3
"""Shim: `python legacy/hotel_contact_enrichment.py` → `contact_enrichment.__main__`."""

from __future__ import annotations

import sys
from pathlib import Path

_repo_root = Path(__file__).resolve().parents[1]
if str(_repo_root) not in sys.path:
    sys.path.insert(0, str(_repo_root))

from contact_enrichment.__main__ import main

if __name__ == "__main__":
    raise SystemExit(main())
