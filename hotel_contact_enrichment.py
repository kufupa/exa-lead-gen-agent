#!/usr/bin/env python3
"""Compatibility shim — implementation lives in `legacy/hotel_contact_enrichment.py`."""

from __future__ import annotations

import runpy
import sys
from pathlib import Path

_LEGACY = Path(__file__).resolve().parent / "legacy" / "hotel_contact_enrichment.py"

if __name__ == "__main__":
    sys.argv[0] = str(_LEGACY)
    runpy.run_path(str(_LEGACY), run_name="__main__")
else:
    from legacy.hotel_contact_enrichment import main

    __all__ = ["main"]
