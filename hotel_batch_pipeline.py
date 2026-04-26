#!/usr/bin/env python3
"""Compatibility shim — implementation lives in `legacy/hotel_batch_pipeline.py`."""

from __future__ import annotations

import sys
from pathlib import Path

if __name__ == "__main__":
    _root = Path(__file__).resolve().parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))
    from legacy.hotel_batch_pipeline import main

    raise SystemExit(main())

from legacy.hotel_batch_pipeline import *  # noqa: F403
