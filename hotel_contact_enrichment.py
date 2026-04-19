#!/usr/bin/env python3
"""Shim: `python hotel_contact_enrichment.py` → `contact_enrichment.__main__`."""

from __future__ import annotations

from contact_enrichment.__main__ import main

if __name__ == "__main__":
    raise SystemExit(main())
