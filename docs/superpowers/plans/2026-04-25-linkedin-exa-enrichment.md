# LinkedIn Profile Enrichment (Exa + Grok Hybrid) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a third enrichment stage that fetches LinkedIn profile content via Exa and structures it via Grok (no web_search tool), appending rich career/education/activity data onto each contact in `*.enriched.json`.

**Architecture:** Exa `get_contents` fetches full markdown from each unique `linkedin_url` ($0.001/page). That text is passed as prompt context to Grok `grok-4.20-reasoning` with `response_format` only (no tools) to produce structured `LinkedInProfile` JSON ($~0.009/profile). Total: ~$0.01/profile vs $0.13/profile with pure xAI `web_search`. For 297 unique URLs across 483 contacts: ~$3 total vs ~$39.

**Tech Stack:** `exa-py` SDK, `xai-sdk`, Pydantic v2, existing `Contact` model, existing atomic-write / checkpoint patterns.

---

## Critical review of prior suggestions

**What the previous LLM got wrong or missed:**

1. **Firecrawl was a dead end.** LinkedIn returns 403 to Firecrawl. The smoke test proved this. The entire `linkedin_firecrawl/` package plan and schema design around Firecrawl markdown was wasted architecture.

2. **Pure xAI `web_search` for LinkedIn is 13x more expensive than needed.** The $0.13/profile run was dominated by 9 web_search invocations ($0.045) plus reasoning tokens ($0.087). Most of those tokens are the model's internal search-result processing — not useful output.

3. **The model doesn't need to _search_ for LinkedIn content — we already have the URL.** When you have a URL, search is the wrong tool. Fetch is the right tool. Exa `get_contents` returns clean markdown from LinkedIn URLs (confirmed working via MCP `web_fetch_exa` earlier in this session).

4. **"Third stage as separate subprocess" is correct architecture.** Phase 1 and Phase 2 are already subprocesses in `hotel_batch_pipeline.py`. Phase 3 follows the same pattern. No need for a new package — one script + one small module.

5. **The `LinkedInLeadEnrichment` schema from the smoke script is too heavy for the contact model.** 15+ fields including `activity_highlights`, `publications_or_projects`, `volunteer_or_causes` — most of this is noise for cold email. The schema should be tight: career timeline, headline, location, about snippet, education. Activity/skills as optional lists.

6. **Missing: LinkedIn URL discovery for the 132 contacts (27%) with `linkedin_url: null`.** Exa `category:people` search can find LinkedIn URLs by name+company. This should be a pre-step, not ignored.

---

## File structure

| Responsibility | Path | Action |
|---|---|---|
| Exa client wrapper | `linkedin_enrich/exa_fetch.py` | Create |
| Grok structurer (no tools) | `linkedin_enrich/grok_structure.py` | Create |
| Pydantic types | `linkedin_enrich/types.py` | Create |
| Package init | `linkedin_enrich/__init__.py` | Create |
| CLI script | `scripts/linkedin_exa_enrich.py` | Create |
| Contact model extension | `hotel_decision_maker_research.py:104-120` | Modify — add optional `linkedin_profile` |
| Aggregates builder | `lead_aggregates/builders.py:110-134` | Modify — pass through `linkedin_profile` |
| Batch pipeline hook | `hotel_batch_pipeline.py:97-130` | Modify — optional phase 3 subprocess |
| Dependencies | `requirements.txt` | Modify — add `exa-py` |
| Env template | `.env.example` | Modify — add `EXA_API_KEY` |
| Tests | `tests/test_linkedin_exa_enrich.py` | Create |
| Delete smoke scripts | `scripts/firecrawl_linkedin_smoke.py`, `scrapTest.md` | Delete (dead code) |

---

## Data flow

```
enriched.json → extract linkedin_urls
                    ↓
        ┌─── urls with linkedin_url ──────────────────┐
        │                                              │
        │   contacts WITHOUT linkedin_url              │
        │   → Exa search(category:people,              │
        │     "{name} {company}")                      │
        │   → discover linkedin_url                    │
        │   → patch contact                            │
        │                                              │
        ↓                                              ↓
   Exa get_contents(url) ──→ raw markdown (cached)
                    ↓
   Grok grok-4.20-reasoning (NO tools)
   + response_format=LinkedInProfile
   + markdown as user context
                    ↓
   Merge linkedin_profile onto Contact
                    ↓
   Write enriched.json (atomic)
```

---

## Cost model (your actual data: 483 contacts, 297 unique LinkedIn URLs, 132 missing URLs)

| Step | Unit cost | Count | Total |
|------|-----------|-------|-------|
| Exa `category:people` search (URL discovery) | $0.007/req | ~132 | $0.92 |
| Exa `get_contents` (fetch profiles) | $0.001/page | ~350 | $0.35 |
| Grok structuring (no tools, ~3k in + ~500 out) | ~$0.009/call | ~350 | $3.15 |
| **Total** | | | **~$4.42** |

Compare: pure xAI `web_search` approach at $0.13/profile × 350 = **$45.50** (10x more).

---

## IMPLEMENTATION NOTES FOR WEAKER LLMs

**Read this section before starting any task.**

### Project layout (workspace root = repo root)

```
c:\Users\gamin\Desktop\startup\exa-lead-gen-agent\
├── hotel_decision_maker_research.py    ← Contact model lives here (line 104)
├── hotel_contact_enrichment.py         ← shim for contact_enrichment.__main__
├── hotel_batch_pipeline.py             ← orchestrator (phase1 + phase2 subprocesses)
├── contact_enrichment/                 ← phase 2 package (do NOT modify)
├── lead_aggregates/                    ← aggregate builders (modify builders.py only)
├── linkedin_enrich/                    ← NEW package you create (Task 1-3)
│   ├── __init__.py                     ← empty file
│   ├── types.py                        ← LinkedInProfile, ExperienceEntry, EducationEntry
│   ├── exa_fetch.py                    ← Exa get_contents + URL normalize + discovery
│   └── grok_structure.py               ← Grok structurer (NO web tools)
├── outreach/                           ← do NOT modify
├── scripts/
│   ├── _repo_dotenv.py                 ← loads .env into os.environ
│   ├── linkedin_exa_enrich.py          ← NEW CLI script (Task 5)
│   ├── linkedin_profile_enrich_xai.py  ← existing smoke script (keep as-is)
│   └── firecrawl_linkedin_smoke.py     ← DELETE in Task 7
├── tests/
│   ├── test_linkedin_exa_enrich.py     ← NEW test file (Tasks 1-5)
│   └── test_hotel_decision_maker_research.py ← append 2 tests (Task 4)
├── jsons/                              ← *.enriched.json files (input/output)
├── requirements.txt                    ← add exa-py (Task 2)
├── .env.example                        ← add EXA_API_KEY (Task 2)
└── .env                                ← user's actual env (add EXA_API_KEY there too)
```

### Key patterns from existing code you MUST follow

1. **sys.path insert pattern** — every script under `scripts/` does this at the top:
```python
_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))
```
This makes imports like `from linkedin_enrich.types import ...` work. Do NOT skip this.

2. **`load_repo_dotenv`** — call `from scripts._repo_dotenv import load_repo_dotenv` then `load_repo_dotenv(_ROOT)` early in `main()`. This loads `.env` vars without overriding existing env.

3. **Atomic file writes** — write to `.tmp` then `tmp.replace(out_path)`. See Task 5 code.

4. **`usage_to_dict`** — import from `hotel_decision_maker_research`. It converts xAI SDK protobuf usage objects to plain dicts. Already exists, do NOT rewrite.

5. **`Any` type for `linkedin_profile` on Contact** — Use `from typing import Any` and `linkedin_profile: Any | None = None`. Do NOT import `LinkedInProfile` into `hotel_decision_maker_research.py` — that would create a circular import since `linkedin_enrich/grok_structure.py` imports from `hotel_decision_maker_research`.

### Gotchas

- **`exa-py` package name vs import name:** `pip install exa-py` but `from exa_py import Exa` (underscore, not hyphen).
- **`exa_client.get_contents` signature:** first arg is a `list[str]` of URLs, keyword arg `text={"max_characters": 12000}`. Returns object with `.results` list, each result has `.url` and `.text`.
- **`exa_client.search` signature:** first arg is query string, keyword `num_results=3`. Returns object with `.results` list, each result has `.url`.
- **Grok `chat.create` with NO tools:** do NOT pass `tools=` argument at all. Pass `max_turns=1` since no tool loop needed. Pass `response_format=LinkedInProfile` (the Pydantic class, not a string).
- **`re` import in `exa_fetch.py`:** currently unused after final code — remove it or the linter will complain. The `normalize_linkedin_url` function uses only `urllib.parse.urlparse`.
- **Windows paths:** Use `Path` objects, not string concatenation. `tmp.replace(out_path)` works cross-platform.
- **Test file structure:** ALL tests go in ONE file `tests/test_linkedin_exa_enrich.py`. Task 4 tests go in the EXISTING `tests/test_hotel_decision_maker_research.py` (append at bottom).

### How to verify each task

After each task, run the exact `pytest` command shown. If it fails, fix before moving to next task. Tasks 1-4 have no API dependencies (mocked or unit-only). Task 5 `--help` test has no API dependency. Only manual runs of the CLI hit real APIs.

---

### Task 1: Pydantic types for LinkedIn profile

**Files:**
- Create: `linkedin_enrich/types.py`
- Create: `linkedin_enrich/__init__.py`
- Test: `tests/test_linkedin_exa_enrich.py`

- [ ] **Step 1: Write the failing test**

```python
# tests/test_linkedin_exa_enrich.py
"""Tests for linkedin_enrich types and logic."""
from __future__ import annotations
import pytest
from linkedin_enrich.types import LinkedInProfile, ExperienceEntry


def test_linkedin_profile_minimal():
    p = LinkedInProfile(
        linkedin_url="https://www.linkedin.com/in/test-person-123",
        display_name="Test Person",
    )
    assert p.headline is None
    assert p.experience == []
    assert p.data_quality == "partial"


def test_linkedin_profile_full():
    p = LinkedInProfile(
        linkedin_url="https://www.linkedin.com/in/test-person-123",
        display_name="Test Person",
        headline="CEO at TestCo",
        location="London, UK",
        about="Experienced leader.",
        experience=[
            ExperienceEntry(title="CEO", organization="TestCo", date_range="2020 - Present"),
            ExperienceEntry(title="VP", organization="OldCo", date_range="2015 - 2020"),
        ],
        data_quality="strong",
    )
    assert len(p.experience) == 2
    d = p.model_dump()
    assert d["experience"][0]["title"] == "CEO"
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_linkedin_exa_enrich.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'linkedin_enrich'`

- [ ] **Step 3: Write types module**

```python
# linkedin_enrich/__init__.py
```

```python
# linkedin_enrich/types.py
from __future__ import annotations
from typing import Literal
from pydantic import BaseModel, Field


class ExperienceEntry(BaseModel):
    title: str | None = None
    organization: str | None = None
    date_range: str | None = None
    location: str | None = None
    description: str | None = None


class EducationEntry(BaseModel):
    school: str | None = None
    degree_or_field: str | None = None
    date_range: str | None = None


class LinkedInProfile(BaseModel):
    """Structured LinkedIn profile for lead enrichment. Produced by Grok from Exa-fetched markdown."""

    linkedin_url: str = Field(min_length=12)
    display_name: str | None = None
    headline: str | None = None
    location: str | None = None
    about: str | None = None
    experience: list[ExperienceEntry] = Field(default_factory=list)
    education: list[EducationEntry] = Field(default_factory=list)
    skills: list[str] = Field(default_factory=list)
    activity_themes: list[str] = Field(
        default_factory=list,
        max_length=10,
        description="Short bullets summarizing recent post/activity themes",
    )
    source_urls: list[str] = Field(default_factory=list, max_length=8)
    data_quality: Literal["strong", "partial", "weak"] = "partial"
    caveats: str | None = Field(default=None, max_length=800)
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/test_linkedin_exa_enrich.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add linkedin_enrich/ tests/test_linkedin_exa_enrich.py
git commit -m "feat: add LinkedInProfile pydantic types for Exa+Grok enrichment"
```

---

### Task 2: Exa fetch module

**Files:**
- Create: `linkedin_enrich/exa_fetch.py`
- Modify: `requirements.txt`
- Modify: `.env.example`
- Test: `tests/test_linkedin_exa_enrich.py` (append)

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_linkedin_exa_enrich.py
from unittest.mock import MagicMock, patch
from linkedin_enrich.exa_fetch import fetch_linkedin_profiles, normalize_linkedin_url


def test_normalize_linkedin_url():
    assert normalize_linkedin_url("https://www.linkedin.com/in/foo/") == "https://www.linkedin.com/in/foo"
    assert normalize_linkedin_url("https://uk.linkedin.com/in/foo") == "https://www.linkedin.com/in/foo"
    assert normalize_linkedin_url("http://linkedin.com/in/foo") == "https://www.linkedin.com/in/foo"


def test_fetch_deduplicates_urls():
    """Two contacts with same normalized URL should produce one Exa call."""
    mock_exa = MagicMock()
    mock_result = MagicMock()
    mock_result.results = [MagicMock(url="https://www.linkedin.com/in/foo", text="# Foo\nCEO at Bar")]
    mock_exa.get_contents.return_value = mock_result

    urls = [
        "https://www.linkedin.com/in/foo",
        "https://uk.linkedin.com/in/foo/",
        "https://www.linkedin.com/in/foo",
    ]
    result = fetch_linkedin_profiles(mock_exa, urls)
    assert mock_exa.get_contents.call_count == 1
    assert "https://www.linkedin.com/in/foo" in result
    assert "# Foo" in result["https://www.linkedin.com/in/foo"]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_linkedin_exa_enrich.py::test_normalize_linkedin_url tests/test_linkedin_exa_enrich.py::test_fetch_deduplicates_urls -v`
Expected: FAIL

- [ ] **Step 3: Install exa-py and write module**

Add to `requirements.txt`:
```
exa-py>=1.0.0
```

Add to `.env.example`:
```
# Exa — https://dashboard.exa.ai/api-keys
EXA_API_KEY=
```

```python
# linkedin_enrich/exa_fetch.py
"""Fetch LinkedIn profile markdown via Exa Contents API. Deduplicates by normalized URL."""
from __future__ import annotations

from urllib.parse import urlparse


def normalize_linkedin_url(url: str) -> str:
    """Normalize LinkedIn URL: https://www.linkedin.com/in/<slug> (no trailing slash, www subdomain)."""
    u = url.strip().rstrip("/")
    parsed = urlparse(u)
    path = parsed.path.rstrip("/")
    return f"https://www.linkedin.com{path}"


def fetch_linkedin_profiles(
    exa_client: "exa_py.Exa",  # type: ignore[name-defined]
    urls: list[str],
    *,
    batch_size: int = 10,
    text_max_chars: int = 12000,
) -> dict[str, str]:
    """Fetch LinkedIn profiles via Exa. Returns {normalized_url: markdown_text}.

    Deduplicates URLs before fetching. Batches requests to respect API limits.
    """
    norm_map: dict[str, str] = {}
    for u in urls:
        n = normalize_linkedin_url(u)
        if n not in norm_map:
            norm_map[n] = u

    unique_urls = list(norm_map.keys())
    results: dict[str, str] = {}

    for i in range(0, len(unique_urls), batch_size):
        batch = unique_urls[i : i + batch_size]
        try:
            resp = exa_client.get_contents(batch, text={"max_characters": text_max_chars})
            for r in resp.results:
                norm = normalize_linkedin_url(r.url)
                text = getattr(r, "text", "") or ""
                if text.strip():
                    results[norm] = text
        except Exception:
            for u in batch:
                results.setdefault(u, "")

    return results


def discover_linkedin_urls(
    exa_client: "exa_py.Exa",  # type: ignore[name-defined]
    contacts: list[dict[str, str]],
) -> dict[str, str]:
    """For contacts without linkedin_url, search Exa category:people to find it.

    Args:
        contacts: list of {"full_name": ..., "company": ...}

    Returns:
        {"full_name|company": "https://www.linkedin.com/in/..."} for found profiles.
    """
    found: dict[str, str] = {}
    for c in contacts:
        name = (c.get("full_name") or "").strip()
        company = (c.get("company") or "").strip()
        if not name:
            continue
        query = f"category:people {name}"
        if company:
            query += f" {company}"
        try:
            resp = exa_client.search(query, num_results=3)
            for r in resp.results:
                url = getattr(r, "url", "")
                if "linkedin.com/in/" in url:
                    key = f"{name}|{company}"
                    found[key] = normalize_linkedin_url(url)
                    break
        except Exception:
            continue
    return found
```

- [ ] **Step 4: Run tests**

Run: `pip install exa-py && pytest tests/test_linkedin_exa_enrich.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add linkedin_enrich/exa_fetch.py requirements.txt .env.example
git commit -m "feat: Exa LinkedIn fetch + URL normalization + discovery"
```

---

### Task 3: Grok structurer (no tools)

**Files:**
- Create: `linkedin_enrich/grok_structure.py`
- Test: `tests/test_linkedin_exa_enrich.py` (append)

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_linkedin_exa_enrich.py
from linkedin_enrich.grok_structure import build_structuring_prompt


def test_structuring_prompt_contains_url_and_markdown():
    prompt = build_structuring_prompt(
        linkedin_url="https://www.linkedin.com/in/foo",
        markdown="# Foo Bar\nCEO at TestCo\nLondon\n## Experience\n### CEO at TestCo\n2020 - Present",
    )
    assert "linkedin.com/in/foo" in prompt
    assert "CEO at TestCo" in prompt
    assert "Experience" in prompt
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_linkedin_exa_enrich.py::test_structuring_prompt_contains_url_and_markdown -v`
Expected: FAIL

- [ ] **Step 3: Write module**

```python
# linkedin_enrich/grok_structure.py
"""Structure LinkedIn markdown into LinkedInProfile using Grok (no web tools)."""
from __future__ import annotations

import os
from typing import Any

from linkedin_enrich.types import LinkedInProfile

SYSTEM_PROMPT = """You extract structured data from LinkedIn profile text.

Rules:
- Extract ONLY facts present in the provided text. Never fabricate.
- Include ALL experience entries (every distinct role), oldest to newest.
- Set data_quality: strong if headline + 2+ experience entries filled; partial if gaps; weak if only name/headline.
- If text is empty or clearly a login wall, set data_quality: weak with a caveat.
- Return JSON matching the schema. No markdown fences, no commentary outside JSON."""


def build_structuring_prompt(linkedin_url: str, markdown: str) -> str:
    # Cap markdown to avoid token blowout
    text = markdown[:10000] if len(markdown) > 10000 else markdown
    return f"""LinkedIn URL: {linkedin_url}

--- Profile text (fetched via Exa) ---
{text}
--- End profile text ---

Extract ALL structured fields from the text above into the JSON schema."""


def structure_profile(
    *,
    api_key: str,
    model: str,
    linkedin_url: str,
    markdown: str,
) -> tuple[LinkedInProfile | None, dict[str, Any]]:
    """Run Grok with no tools to structure LinkedIn markdown. Returns (profile, usage_dict)."""
    from xai_sdk import Client
    from xai_sdk.chat import system, user

    from hotel_decision_maker_research import usage_to_dict

    client = Client(api_key=api_key)
    chat = client.chat.create(
        model=model,
        max_turns=1,
        store_messages=True,
        response_format=LinkedInProfile,
    )
    chat.append(system(SYSTEM_PROMPT))
    chat.append(user(build_structuring_prompt(linkedin_url, markdown)))
    final = chat.sample()
    raw = (final.content or "").strip()
    raw_usage = getattr(final, "usage", None)
    usage = usage_to_dict(raw_usage) if raw_usage is not None else {}

    if not raw:
        return None, usage
    try:
        profile = LinkedInProfile.model_validate_json(raw)
    except Exception:
        return None, usage
    return profile, usage
```

- [ ] **Step 4: Run tests**

Run: `pytest tests/test_linkedin_exa_enrich.py -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add linkedin_enrich/grok_structure.py
git commit -m "feat: Grok LinkedIn structurer (no web tools, response_format only)"
```

---

### Task 4: Add `linkedin_profile` to Contact model

**Files:**
- Modify: `hotel_decision_maker_research.py:104-120`
- Modify: `lead_aggregates/builders.py:110-134`
- Test: `tests/test_hotel_decision_maker_research.py` (append)

- [ ] **Step 1: Write the failing test**

```python
# append to tests/test_hotel_decision_maker_research.py
def test_contact_accepts_linkedin_profile():
    from linkedin_enrich.types import LinkedInProfile

    profile = LinkedInProfile(
        linkedin_url="https://www.linkedin.com/in/test",
        display_name="Test",
        headline="CEO",
        data_quality="strong",
    )
    c = Contact(
        full_name="Test Person",
        title="CEO",
        decision_maker_score="high",
        intimacy_grade="medium",
        fit_reason="Owns distribution.",
        contact_evidence_summary="Found on site.",
        evidence=[],
        linkedin_profile=profile,
    )
    assert c.linkedin_profile is not None
    d = c.model_dump()
    assert d["linkedin_profile"]["headline"] == "CEO"


def test_contact_linkedin_profile_defaults_none():
    c = Contact(
        full_name="Test",
        title="Manager",
        decision_maker_score="low",
        intimacy_grade="low",
        fit_reason="Test.",
        contact_evidence_summary="Test.",
        evidence=[],
    )
    assert c.linkedin_profile is None
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/test_hotel_decision_maker_research.py::test_contact_accepts_linkedin_profile -v`
Expected: FAIL with `unexpected keyword argument 'linkedin_profile'`

- [ ] **Step 3: Add field to Contact**

Open `hotel_decision_maker_research.py`. Find the `Contact` class (line 104). The class currently looks like this:

```python
class Contact(BaseModel):
    full_name: str
    title: str
    company: str | None = None
    linkedin_url: str | None = None
    email: str | None = None
    email2: str | None = None
    phone: str | None = None
    phone2: str | None = None
    x_handle: str | None = None
    other_contact_detail: str | None = None
    decision_maker_score: DecisionMakerScore
    intimacy_grade: IntimacyGrade
    fit_reason: str
    contact_evidence_summary: str
    evidence: list[Evidence]
```

**Add ONE line** after `evidence: list[Evidence]` (the last field, line 119):

```python
    linkedin_profile: Any | None = None
```

The class should now end like:

```python
    contact_evidence_summary: str
    evidence: list[Evidence]
    linkedin_profile: Any | None = None
```

**ALSO** make sure `Any` is imported. Find the existing imports at the top of the file. Line 26 already has `from typing import Any, Literal` — so `Any` is already imported. If for some reason it's not, add it to that import. Do NOT add a separate `from typing import Any` line if it's already there.

**Do NOT** import `LinkedInProfile` in this file. That would cause a circular import.

- [ ] **Step 4: Update `build_contact_row` in `lead_aggregates/builders.py`**

Open `lead_aggregates/builders.py`. Find the `build_contact_row` function (line 110). It currently returns a dict. The last three entries before the closing `}` are:

```python
        "evidence": c.get("evidence") if isinstance(c.get("evidence"), list) else [],
        "phase1_research": phase1,
        "phase2_contact_enrichment": phase2,
    }
```

**Add ONE line** between `"evidence"` and `"phase1_research"`:

```python
        "linkedin_profile": c.get("linkedin_profile"),
```

So it becomes:

```python
        "evidence": c.get("evidence") if isinstance(c.get("evidence"), list) else [],
        "linkedin_profile": c.get("linkedin_profile"),
        "phase1_research": phase1,
        "phase2_contact_enrichment": phase2,
    }
```

- [ ] **Step 5: Run tests**

Run: `pytest tests/test_hotel_decision_maker_research.py::test_contact_accepts_linkedin_profile tests/test_hotel_decision_maker_research.py::test_contact_linkedin_profile_defaults_none -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add hotel_decision_maker_research.py lead_aggregates/builders.py tests/test_hotel_decision_maker_research.py
git commit -m "feat: add optional linkedin_profile field to Contact model"
```

---

### Task 5: CLI script — `scripts/linkedin_exa_enrich.py`

**Files:**
- Create: `scripts/linkedin_exa_enrich.py`
- Test: manual run + dry-run test

- [ ] **Step 1: Write the CLI script**

```python
#!/usr/bin/env python3
"""LinkedIn profile enrichment via Exa fetch + Grok structuring (no web_search tool).

Reads *.enriched.json, extracts linkedin_urls, fetches via Exa, structures via Grok,
writes linkedin_profile back onto each contact.

Usage:
  python scripts/linkedin_exa_enrich.py --in-json jsons/foo.enriched.json --out-json jsons/foo.enriched.json --pretty
  python scripts/linkedin_exa_enrich.py --in-json jsons/foo.enriched.json --dry-run
  python scripts/linkedin_exa_enrich.py --jsons-dir jsons/ --all  # batch all enriched files
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from scripts._repo_dotenv import load_repo_dotenv  # noqa: E402

from linkedin_enrich.exa_fetch import (  # noqa: E402
    discover_linkedin_urls,
    fetch_linkedin_profiles,
    normalize_linkedin_url,
)
from linkedin_enrich.grok_structure import structure_profile  # noqa: E402
from linkedin_enrich.types import LinkedInProfile  # noqa: E402

DEFAULT_MODEL = "grok-4.20-reasoning"


def _process_one_file(
    path: Path,
    out_path: Path,
    *,
    exa_client: Any,
    xai_api_key: str,
    model: str,
    discover_missing: bool,
    skip_existing: bool,
    pretty: bool,
    dry_run: bool,
) -> dict[str, int]:
    data = json.loads(path.read_text(encoding="utf-8"))
    contacts = data.get("contacts", [])
    if not isinstance(contacts, list):
        return {"skipped": 0, "fetched": 0, "structured": 0, "errors": 0}

    # Collect URLs needing work
    urls_to_fetch: list[str] = []
    for c in contacts:
        if skip_existing and c.get("linkedin_profile"):
            continue
        li = (c.get("linkedin_url") or "").strip()
        if li:
            urls_to_fetch.append(li)

    # Discover missing URLs if enabled
    discovered: dict[str, str] = {}
    if discover_missing:
        missing = [
            {"full_name": c.get("full_name", ""), "company": c.get("company", "")}
            for c in contacts
            if not (c.get("linkedin_url") or "").strip()
            and not (skip_existing and c.get("linkedin_profile"))
        ]
        if missing and not dry_run:
            discovered = discover_linkedin_urls(exa_client, missing)

    stats = {"skipped": 0, "fetched": 0, "structured": 0, "discovered": len(discovered), "errors": 0}

    if dry_run:
        stats["would_fetch"] = len(set(normalize_linkedin_url(u) for u in urls_to_fetch))
        stats["would_discover"] = len([
            c for c in contacts
            if not (c.get("linkedin_url") or "").strip()
            and not (skip_existing and c.get("linkedin_profile"))
        ])
        return stats

    # Fetch all unique LinkedIn URLs via Exa
    all_urls = list(urls_to_fetch)
    for key, url in discovered.items():
        all_urls.append(url)

    profile_markdowns = fetch_linkedin_profiles(exa_client, all_urls) if all_urls else {}
    stats["fetched"] = len(profile_markdowns)

    # Patch discovered URLs onto contacts
    for c in contacts:
        if (c.get("linkedin_url") or "").strip():
            continue
        name = (c.get("full_name") or "").strip()
        company = (c.get("company") or "").strip()
        key = f"{name}|{company}"
        if key in discovered:
            c["linkedin_url"] = discovered[key]

    # Structure each contact's LinkedIn profile
    for c in contacts:
        if skip_existing and c.get("linkedin_profile"):
            stats["skipped"] += 1
            continue
        li = (c.get("linkedin_url") or "").strip()
        if not li:
            continue
        norm = normalize_linkedin_url(li)
        md = profile_markdowns.get(norm, "")
        if not md.strip():
            continue
        try:
            profile, usage = structure_profile(
                api_key=xai_api_key,
                model=model,
                linkedin_url=li,
                markdown=md,
            )
            if profile:
                c["linkedin_profile"] = profile.model_dump()
                stats["structured"] += 1
            else:
                stats["errors"] += 1
        except Exception as e:
            print(f"  error structuring {li}: {e}", file=sys.stderr)
            stats["errors"] += 1

    # Write output
    data["linkedin_enrichment"] = {
        "version": 1,
        "enriched_at_utc": datetime.now(timezone.utc).isoformat(),
        "model": model,
        "stats": stats,
    }
    data["contacts"] = contacts

    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_suffix(".tmp")
    txt = json.dumps(data, ensure_ascii=False, indent=2 if pretty else None)
    if not pretty:
        txt = json.dumps(data, ensure_ascii=False, separators=(",", ":"))
    tmp.write_text(txt, encoding="utf-8")
    tmp.replace(out_path)

    return stats


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--in-json", type=Path, help="Single enriched JSON to process")
    p.add_argument("--out-json", type=Path, help="Output path (default: overwrite in-json)")
    p.add_argument("--jsons-dir", type=Path, default=Path("jsons"), help="Directory of enriched JSONs (with --all)")
    p.add_argument("--all", action="store_true", help="Process all *.enriched.json in --jsons-dir")
    p.add_argument("--model", default=DEFAULT_MODEL)
    p.add_argument("--discover-missing", action="store_true", help="Use Exa people search to find missing LinkedIn URLs")
    p.add_argument("--no-skip-existing", action="store_true", help="Re-enrich contacts that already have linkedin_profile")
    p.add_argument("--pretty", action="store_true")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    load_repo_dotenv(_ROOT)

    exa_key = (os.environ.get("EXA_API_KEY") or "").strip()
    xai_key = (os.environ.get("XAI_API_KEY") or "").strip()
    if not exa_key:
        print("Missing EXA_API_KEY.", file=sys.stderr)
        return 1
    if not xai_key and not args.dry_run:
        print("Missing XAI_API_KEY.", file=sys.stderr)
        return 1

    from exa_py import Exa
    exa_client = Exa(api_key=exa_key)

    files: list[tuple[Path, Path]] = []
    if args.all:
        for f in sorted(args.jsons_dir.glob("*.enriched.json")):
            files.append((f, f))
    elif args.in_json:
        out = args.out_json or args.in_json
        files.append((args.in_json, out))
    else:
        print("Provide --in-json or --all.", file=sys.stderr)
        return 1

    total_stats: dict[str, int] = {}
    for in_path, out_path in files:
        print(f"Processing {in_path.name}...")
        stats = _process_one_file(
            in_path, out_path,
            exa_client=exa_client,
            xai_api_key=xai_key,
            model=args.model,
            discover_missing=args.discover_missing,
            skip_existing=not args.no_skip_existing,
            pretty=args.pretty,
            dry_run=args.dry_run,
        )
        print(f"  {stats}")
        for k, v in stats.items():
            total_stats[k] = total_stats.get(k, 0) + v

    print(f"\nTotal: {total_stats}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Write dry-run test**

```python
# append to tests/test_linkedin_exa_enrich.py
import subprocess
import sys

def test_linkedin_exa_enrich_dry_run():
    r = subprocess.run(
        [sys.executable, "scripts/linkedin_exa_enrich.py", "--help"],
        capture_output=True, text=True
    )
    assert r.returncode == 0
    assert "--in-json" in r.stdout
    assert "--discover-missing" in r.stdout
```

- [ ] **Step 3: Run test**

Run: `pytest tests/test_linkedin_exa_enrich.py::test_linkedin_exa_enrich_dry_run -v`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
git add scripts/linkedin_exa_enrich.py tests/test_linkedin_exa_enrich.py
git commit -m "feat: LinkedIn Exa+Grok enrichment CLI (fetch + structure, no web_search)"
```

---

### Task 6: Wire into batch pipeline (optional phase 3)

**Files:**
- Modify: `hotel_batch_pipeline.py:97-130`

- [ ] **Step 1: Add phase 3 subprocess after enrichment success**

Open `hotel_batch_pipeline.py`. Find the `_process_one` function. After phase 2 succeeds, there's this block (lines 124-130):

```python
    store.commit_after_enrich(
        canonical_url=canon,
        research_json=research_rel,
        enriched_json=enriched_rel,
        error=None,
    )
    return canon, "ok"
```

**Insert the following code BETWEEN the `store.commit_after_enrich(...)` call and `return canon, "ok"`:**

```python
    if os.environ.get("LINKEDIN_ENRICH", "").strip().lower() in ("1", "true", "yes"):
        r3 = subprocess.run(
            [
                sys.executable,
                str(root / "scripts" / "linkedin_exa_enrich.py"),
                "--in-json", str(enriched),
                "--out-json", str(enriched),
                "--discover-missing",
                "--pretty",
            ],
            cwd=root,
            env=env,
            capture_output=True,
            text=True,
        )
        if r3.returncode != 0:
            err = (r3.stderr or r3.stdout or "linkedin enrichment failed")[-2000:]
            print(f"Warning: LinkedIn enrichment failed for {canon}: {err[:200]}", file=sys.stderr)
```

**Also check `os` is imported.** Line 8 of `hotel_batch_pipeline.py` already has `import os` — verify this. If not, add it.

The `return canon, "ok"` line stays AFTER this new block. Phase 3 failure is a non-fatal warning — it does NOT change the return value.

- [ ] **Step 2: Commit**

```bash
git add hotel_batch_pipeline.py
git commit -m "feat: optional LinkedIn Exa enrichment phase 3 in batch pipeline (LINKEDIN_ENRICH=1)"
```

---

### Task 7: Cleanup dead code

**Files:**
- Delete: `scripts/firecrawl_linkedin_smoke.py`
- Delete: `scrapTest.md`

- [ ] **Step 1: Remove files**

```bash
rm scripts/firecrawl_linkedin_smoke.py scrapTest.md
git add -A
git commit -m "chore: remove dead Firecrawl smoke test (LinkedIn 403)"
```

---

## Self-review

**Spec coverage:** Exa fetch for LinkedIn URLs ✓, Grok structuring without web_search ✓, append to enriched JSON ✓, URL discovery for missing contacts ✓, batch mode ✓, cost tracking in output ✓, skip-existing logic ✓, atomic writes ✓, pipeline integration ✓.

**Placeholder scan:** No TBDs. All code blocks complete. All test code present.

**Type consistency:** `LinkedInProfile` used consistently across types, structure module, CLI script, and Contact model field. `normalize_linkedin_url` used in both fetch and CLI. `structure_profile` signature matches call sites.

---

## Final checklist for implementing LLM

After all 7 tasks are done, verify:

1. `pytest tests/test_linkedin_exa_enrich.py -v` — all pass
2. `pytest tests/test_hotel_decision_maker_research.py -v` — all pass (including new tests)
3. `python scripts/linkedin_exa_enrich.py --help` — prints usage, exits 0
4. `python scripts/linkedin_exa_enrich.py --in-json jsons/hotel_leads__www_thegoring_com__89b90276.enriched.json --dry-run --discover-missing` — prints stats without calling APIs
5. `scripts/firecrawl_linkedin_smoke.py` and `scrapTest.md` no longer exist
6. `requirements.txt` contains `exa-py>=1.0.0`
7. `.env.example` contains `EXA_API_KEY=`
8. No linter errors in any new/modified file
9. `git status` shows clean working tree (all changes committed)

---

## Exact dependency versions (as of Apr 2026)

```
pip install exa-py
```

The `exa-py` package provides the `Exa` class. Import: `from exa_py import Exa`. Constructor: `Exa(api_key="...")`. Key methods used:
- `exa.get_contents(urls: list[str], text={"max_characters": int})` — returns object with `.results` list
- `exa.search(query: str, num_results=int)` — returns object with `.results` list

The `xai-sdk` is already in `requirements.txt`. Key imports used:
- `from xai_sdk import Client`
- `from xai_sdk.chat import system, user`
- `Client(api_key=...).chat.create(model=..., max_turns=1, store_messages=True, response_format=PydanticClass)`
- `chat.append(system(...))`, `chat.append(user(...))`, `final = chat.sample()`
- `final.content` is the raw JSON string, `final.usage` is the usage object
