# Hotel phone CRM (FastAPI + HTMX + Supabase) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship password-protected internal web UI: hotels grouped left, contact detail (phone top, LinkedIn, evidence) right, notes bottom; data lives in Supabase Postgres; local pipeline still writes `jsons/` + `fullJSONs/` — a sync command pushes phone-intimate rows into Supabase idempotently.

**Architecture:** FastAPI serves server-rendered HTML + HTMX partial swaps. Browser never holds service keys. After Supabase Auth login, session uses JWT (HttpOnly cookie bridge or Authorization header pattern). Sync CLI reads `fullJSONs/intimate_phone_contacts.json`, maps each row to `crm_contacts` + `crm_contact_payload` (JSONB full row), upserts by stable `source_row_id`. Notes in `crm_notes` (append-only or latest-wins — Task 1 picks latest-wins MVP).

**Tech stack:** Python 3.11+, FastAPI, Jinja2, HTMX (CDN script tag), `httpx` or `PyJWT` + `cryptography` for JWT verify, `supabase-py` optional for admin only, `psycopg` or `asyncpg` for Postgres, Supabase Auth (email/password or magic link), deploy on Render or Railway (single web dyno).

**Context — repo files already reviewed (no code execution implied):**

| Path | Why it matters |
|------|----------------|
| [README.md](../../../README.md) | Documents `fullJSONs/`, rebuild scripts, hotel batch flow |
| [requirements.txt](../../../requirements.txt) | Current deps; new web stack adds here |
| [lead_aggregates/store.py](../../../lead_aggregates/store.py) | `AggregatesStore`, `commit_after_enrich`, paths for aggregates |
| [lead_aggregates/builders.py](../../../lead_aggregates/builders.py) | `build_phone_document`, `dedupe_key`, `build_contact_row`, `has_structured_phone` |
| [lead_aggregates/urls.py](../../../lead_aggregates/urls.py) | `canonical_hotel_url` — hotel grouping key |
| [lead_aggregates/atomic.py](../../../lead_aggregates/atomic.py) | Atomic JSON write pattern (sync script can mirror idempotency) |
| [outreach/ids.py](../../../outreach/ids.py) | `compute_outreach_id` — email-outreach IDs; phone CRM uses different stable key |
| [outreach/sync.py](../../../outreach/sync.py) | Pattern for merge state; reference for locked JSON updates |
| [outreach/store.py](../../../outreach/store.py) | FileLock pattern for JSON state |
| [hotel_batch_pipeline.py](../../../hotel_batch_pipeline.py) | After enrich, aggregates refresh |
| [scripts/rebuild_fulljsons.py](../../../scripts/rebuild_fulljsons.py) | Rebuild all `fullJSONs` from `jsons/*.enriched.json` |
| [scripts/build_intimate_phone_contacts.py](../../../scripts/build_intimate_phone_contacts.py) | Rebuild phone slice only |
| `fullJSONs/intimate_phone_contacts.json` | **Source for CRM sync** — `contacts[]` with `phase1_research.target_url`, phones, LinkedIn, evidence |
| [tests/test_lead_aggregates_rebuild.py](../../../tests/test_lead_aggregates_rebuild.py) | Fixture pattern for synthetic enriched JSON |

---

## File map (create / modify)

| Path | Responsibility |
|------|----------------|
| `supabase/migrations/000001_crm.sql` | Tables `crm_contacts`, `crm_contact_payload`, `crm_notes`; indexes |
| `crm_app/__init__.py` | Package marker (empty) |
| `crm_app/config.py` | Env: `DATABASE_URL`, `SUPABASE_JWT_SECRET`, `SUPABASE_URL`, `SESSION_SECRET`, `ALLOWED_EMAILS` |
| `crm_app/auth.py` | Verify Supabase JWT, extract `sub` + `email`, gate `ALLOWED_EMAILS` |
| `crm_app/db.py` | Connection pool, query helpers |
| `crm_app/main.py` | FastAPI app, mount static if needed, routes |
| `crm_app/routes_pages.py` | GET `/`, GET `/partials/contact/{id}` |
| `crm_app/routes_notes.py` | POST `/notes` (HTMX) |
| `crm_app/templates/base.html.j2` | Layout shell + HTMX script |
| `crm_app/templates/index.html.j2` | Left nav + empty state |
| `crm_app/templates/partials/contact.html.j2` | Phone, LinkedIn, fields |
| `crm_app/templates/partials/hotel_list.html.j2` | Accordion / list |
| `scripts/sync_phone_crm_to_supabase.py` | Read `intimate_phone_contacts.json`, upsert DB |
| `tests/test_crm_stable_id.py` | Stable id function matches fixtures |
| `tests/test_sync_phone_crm.py` | Upsert idempotency against temp Postgres or mocked |
| [requirements.txt](../../../requirements.txt) | Add `fastapi`, `uvicorn[standard]`, `jinja2`, `python-multipart`, `httpx`, `PyJWT[crypto]`, `psycopg[binary]`, `itsdangerous` |
| [README.md](../../../README.md) | Section: CRM app + sync + deploy |

---

## Stable identity (must match before Task 2)

Define in `crm_app/stable_id.py` (created Task 2):

```python
from __future__ import annotations

import hashlib

from lead_aggregates.builders import dedupe_key
from lead_aggregates.urls import canonical_hotel_url


def stable_crm_contact_id(contact_row: dict) -> str:
    """Deterministic id: same person + same hotel canonical URL -> same id."""
    p1 = contact_row.get("phase1_research") or {}
    if not isinstance(p1, dict):
        p1 = {}
    target = (p1.get("target_url") or "").strip()
    hotel = canonical_hotel_url(target) if target else ""
    dk = dedupe_key(contact_row) if isinstance(contact_row, dict) else ""
    blob = f"{hotel}\x1f{dk}".encode("utf-8")
    return "crm_" + hashlib.sha256(blob).hexdigest()[:24]
```

---

### Task 1: Supabase SQL schema (run in SQL editor or migration)

**Files:**

- Create: `supabase/migrations/000001_crm.sql`

- [ ] **Step 1: Create migration file with exact SQL**

```sql
-- crm_contacts: one row per (hotel_canonical, dedupe_key) via app stable id
CREATE TABLE IF NOT EXISTS crm_contacts (
    id TEXT PRIMARY KEY,
    hotel_canonical_url TEXT NOT NULL,
    target_url TEXT NOT NULL,
    full_name TEXT,
    title TEXT,
    company TEXT,
    phone TEXT,
    phone2 TEXT,
    linkedin_url TEXT,
    email TEXT,
    email2 TEXT,
    intimacy_grade TEXT,
    decision_maker_score TEXT,
    source_enriched_json TEXT,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_crm_contacts_hotel ON crm_contacts (hotel_canonical_url);
CREATE INDEX IF NOT EXISTS idx_crm_contacts_name ON crm_contacts (lower(full_name));

CREATE TABLE IF NOT EXISTS crm_contact_payload (
    contact_id TEXT PRIMARY KEY REFERENCES crm_contacts(id) ON DELETE CASCADE,
    payload JSONB NOT NULL,
    payload_version INT NOT NULL DEFAULT 1,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- MVP: latest note wins (single row per contact_id); swap to append-only later
CREATE TABLE IF NOT EXISTS crm_notes (
    contact_id TEXT PRIMARY KEY REFERENCES crm_contacts(id) ON DELETE CASCADE,
    body TEXT NOT NULL DEFAULT '',
    updated_by TEXT,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_crm_notes_updated ON crm_notes (updated_at DESC);
```

- [ ] **Step 2: Apply in Supabase**

Run SQL in Supabase Dashboard → SQL → New query → paste → Run.  
Expected: success, no error.

- [ ] **Step 3: Commit**

```bash
git add supabase/migrations/000001_crm.sql
git commit -m "feat(crm): add supabase tables for contacts payload notes"
```

---

### Task 2: `stable_crm_contact_id` + unit test (TDD)

**Files:**

- Create: `crm_app/stable_id.py` (full content = stable identity block above)
- Create: `tests/test_crm_stable_id.py`

- [ ] **Step 1: Write failing test**

```python
from __future__ import annotations

from crm_app.stable_id import stable_crm_contact_id


def test_stable_id_same_hotel_same_dedupe() -> None:
    row = {
        "full_name": "Ada",
        "title": "GM",
        "company": "X",
        "linkedin_url": "https://linkedin.com/in/ada",
        "phase1_research": {"target_url": "https://WWW.Hotel.COM/foo/"},
    }
    a = stable_crm_contact_id(row)
    b = stable_crm_contact_id(row)
    assert a == b
    assert a.startswith("crm_")


def test_stable_id_differs_across_hotels() -> None:
    base = {
        "full_name": "Ada",
        "title": "GM",
        "company": "X",
        "linkedin_url": "https://linkedin.com/in/ada",
    }
    r1 = {**base, "phase1_research": {"target_url": "https://a.com/"}}
    r2 = {**base, "phase1_research": {"target_url": "https://b.com/"}}
    assert stable_crm_contact_id(r1) != stable_crm_contact_id(r2)
```

- [ ] **Step 2: Run — expect fail**

```bash
cd /path/to/exa-lead-gen-agent
pytest tests/test_crm_stable_id.py -v
```

Expected: `ModuleNotFoundError` or `ImportError` for `crm_app.stable_id`.

- [ ] **Step 3: Add `crm_app/__init__.py` empty + implement `stable_id.py`** (content from Stable identity section).

- [ ] **Step 4: Run — expect pass**

```bash
pytest tests/test_crm_stable_id.py -v
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add crm_app/__init__.py crm_app/stable_id.py tests/test_crm_stable_id.py
git commit -m "feat(crm): stable contact id for hotel+dedupe"
```

---

### Task 3: Sync script `scripts/sync_phone_crm_to_supabase.py`

**Files:**

- Create: `scripts/sync_phone_crm_to_supabase.py`
- Modify: [requirements.txt](../../../requirements.txt) — add `psycopg[binary]>=3.2.0`

- [ ] **Step 1: Implement script (full body)**

```python
#!/usr/bin/env python3
"""Upsert phone-intimate contacts from fullJSONs into Supabase Postgres.

Usage:
  export DATABASE_URL=postgresql://...   # Supabase connection string (pooler ok)
  python scripts/sync_phone_crm_to_supabase.py --fulljsons-dir fullJSONs

Idempotent: re-run safe; updates synced_at + payload JSONB.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import psycopg  # noqa: E402

from crm_app.stable_id import stable_crm_contact_id  # noqa: E402
from lead_aggregates.urls import canonical_hotel_url  # noqa: E402


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--fulljsons-dir", type=Path, default=Path("fullJSONs"))
    p.add_argument("--intimate-phone", type=Path, default=None)
    args = p.parse_args()

    dsn = (os.environ.get("DATABASE_URL") or "").strip()
    if not dsn:
        print("DATABASE_URL missing", file=sys.stderr)
        return 2

    root = Path.cwd().resolve()
    fj = (root / args.fulljsons_dir).resolve()
    path = args.intimate_phone or (fj / "intimate_phone_contacts.json")
    path = path if path.is_absolute() else (root / path).resolve()
    if not path.is_file():
        print(f"Missing {path}", file=sys.stderr)
        return 2

    doc = json.loads(path.read_text(encoding="utf-8"))
    contacts = doc.get("contacts") or []
    if not isinstance(contacts, list):
        print("contacts not a list", file=sys.stderr)
        return 2

    upserted = 0
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            for c in contacts:
                if not isinstance(c, dict):
                    continue
                cid = stable_crm_contact_id(c)
                p1 = c.get("phase1_research") if isinstance(c.get("phase1_research"), dict) else {}
                target = (p1.get("target_url") or "").strip()
                hotel = canonical_hotel_url(target) if target else ""
                src = (p1.get("source_enriched_json") or "").strip() or None
                cur.execute(
                    """
                    INSERT INTO crm_contacts (
                        id, hotel_canonical_url, target_url, full_name, title, company,
                        phone, phone2, linkedin_url, email, email2,
                        intimacy_grade, decision_maker_score, source_enriched_json, synced_at, updated_at
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (id) DO UPDATE SET
                        hotel_canonical_url = EXCLUDED.hotel_canonical_url,
                        target_url = EXCLUDED.target_url,
                        full_name = EXCLUDED.full_name,
                        title = EXCLUDED.title,
                        company = EXCLUDED.company,
                        phone = EXCLUDED.phone,
                        phone2 = EXCLUDED.phone2,
                        linkedin_url = EXCLUDED.linkedin_url,
                        email = EXCLUDED.email,
                        email2 = EXCLUDED.email2,
                        intimacy_grade = EXCLUDED.intimacy_grade,
                        decision_maker_score = EXCLUDED.decision_maker_score,
                        source_enriched_json = EXCLUDED.source_enriched_json,
                        synced_at = EXCLUDED.synced_at,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (
                        cid,
                        hotel,
                        target,
                        c.get("full_name"),
                        c.get("title"),
                        c.get("company"),
                        (c.get("phone") or "").strip() or None,
                        (c.get("phone2") or "").strip() or None,
                        (c.get("linkedin_url") or "").strip() or None,
                        (c.get("email") or "").strip() or None,
                        (c.get("email2") or "").strip() or None,
                        c.get("intimacy_grade"),
                        c.get("decision_maker_score"),
                        src,
                        _now(),
                        _now(),
                    ),
                )
                cur.execute(
                    """
                    INSERT INTO crm_contact_payload (contact_id, payload, updated_at)
                    VALUES (%s, %s::jsonb, %s)
                    ON CONFLICT (contact_id) DO UPDATE SET
                        payload = EXCLUDED.payload,
                        updated_at = EXCLUDED.updated_at
                    """,
                    (cid, json.dumps(c, ensure_ascii=False), _now()),
                )
                upserted += 1
        conn.commit()
    print(f"synced contacts={upserted}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 2: Manual smoke (needs real DATABASE_URL)**

```bash
export DATABASE_URL='postgresql://...'
python scripts/sync_phone_crm_to_supabase.py --fulljsons-dir fullJSONs
```

Expected stdout: `synced contacts=N` where N = count of dict rows in file.

- [ ] **Step 3: Commit**

```bash
git add scripts/sync_phone_crm_to_supabase.py requirements.txt
git commit -m "feat(crm): sync intimate_phone_contacts to supabase"
```

---

### Task 4: FastAPI + Jinja + HTMX shell

**Files:**

- Create: `crm_app/config.py`
- Create: `crm_app/db.py`
- Create: `crm_app/main.py`
- Create: `crm_app/templates/base.html.j2`
- Create: `crm_app/templates/index.html.j2`
- Modify: [requirements.txt](../../../requirements.txt) — add `fastapi`, `uvicorn[standard]`, `jinja2`, `python-multipart`, `PyJWT[crypto]`, `itsdangerous`

`crm_app/config.py`:

```python
from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    supabase_jwt_secret: str
    supabase_url: str
    session_secret: str
    allowed_emails: frozenset[str]


def load_settings() -> Settings:
    raw = (os.environ.get("ALLOWED_EMAILS") or "").strip()
    emails = frozenset(x.strip().lower() for x in raw.split(",") if x.strip())
    return Settings(
        database_url=(os.environ.get("DATABASE_URL") or "").strip(),
        supabase_jwt_secret=(os.environ.get("SUPABASE_JWT_SECRET") or "").strip(),
        supabase_url=(os.environ.get("SUPABASE_URL") or "").strip(),
        session_secret=(os.environ.get("SESSION_SECRET") or "").strip(),
        allowed_emails=emails,
    )
```

`crm_app/db.py`:

```python
from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator

import psycopg
from psycopg.rows import dict_row

from crm_app.config import Settings


@contextmanager
def connection(settings: Settings) -> Iterator[psycopg.Connection]:
    conn = psycopg.connect(settings.database_url, row_factory=dict_row)
    try:
        yield conn
    finally:
        conn.close()


def list_hotels(conn: psycopg.Connection) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT hotel_canonical_url AS hotel, COUNT(*)::int AS n
            FROM crm_contacts
            GROUP BY 1
            ORDER BY 1
            """
        )
        return list(cur.fetchall())


def list_contacts_for_hotel(conn: psycopg.Connection, hotel: str) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, full_name, title, phone, phone2, linkedin_url
            FROM crm_contacts
            WHERE hotel_canonical_url = %s
            ORDER BY lower(full_name)
            """,
            (hotel,),
        )
        return list(cur.fetchall())


def get_contact(conn: psycopg.Connection, contact_id: str) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM crm_contacts WHERE id = %s", (contact_id,))
        row = cur.fetchone()
        if not row:
            return None
        cur.execute("SELECT body FROM crm_notes WHERE contact_id = %s", (contact_id,))
        n = cur.fetchone()
        out = dict(row)
        out["note_body"] = (n or {}).get("body") or ""
        cur.execute("SELECT payload FROM crm_contact_payload WHERE contact_id = %s", (contact_id,))
        pl = cur.fetchone()
        out["payload"] = (pl or {}).get("payload")
        return out
```

`crm_app/main.py` (MVP: protect routes in Task 5; here open for local dev only — **replace with dependency in Task 5**):

```python
from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from crm_app.config import load_settings
from crm_app.db import connection, get_contact, list_contacts_for_hotel, list_hotels

BASE = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE / "templates"))

app = FastAPI()
settings = load_settings()


@app.get("/", response_class=HTMLResponse)
def index(request: Request) -> HTMLResponse:
    with connection(settings) as conn:
        hotels = list_hotels(conn)
        first_hotel = hotels[0]["hotel"] if hotels else None
        contacts = list_contacts_for_hotel(conn, first_hotel) if first_hotel else []
    return templates.TemplateResponse(
        "index.html.j2",
        {
            "request": request,
            "hotels": hotels,
            "selected_hotel": first_hotel,
            "contacts": contacts,
        },
    )


@app.get("/partials/contacts", response_class=HTMLResponse)
def partial_contacts(request: Request, hotel: str) -> HTMLResponse:
    with connection(settings) as conn:
        contacts = list_contacts_for_hotel(conn, hotel)
    return templates.TemplateResponse(
        "partials/hotel_list.html.j2",
        {"request": request, "selected_hotel": hotel, "contacts": contacts},
    )


@app.get("/partials/contact/{contact_id}", response_class=HTMLResponse)
def partial_contact(request: Request, contact_id: str) -> HTMLResponse:
    with connection(settings) as conn:
        row = get_contact(conn, contact_id)
    return templates.TemplateResponse(
        "partials/contact.html.j2",
        {"request": request, "row": row},
    )
```

`crm_app/templates/base.html.j2`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8"/>
  <meta name="viewport" content="width=device-width, initial-scale=1"/>
  <title>Hotel CRM</title>
  <script src="https://unpkg.com/htmx.org@2.0.4"></script>
  <style>
    body { font-family: system-ui, sans-serif; margin: 0; display: flex; height: 100vh; }
    aside { width: 320px; border-right: 1px solid #ccc; overflow: auto; padding: 8px; }
    main { flex: 1; display: flex; flex-direction: column; min-width: 0; }
    #detail { flex: 1; overflow: auto; padding: 12px; }
    #notes { border-top: 1px solid #ccc; padding: 12px; min-height: 120px; }
    .phone { font-size: 1.4rem; font-weight: 700; }
    a { color: #06c; }
  </style>
</head>
<body>
{% block body %}{% endblock %}
</body>
</html>
```

`crm_app/templates/index.html.j2`:

```html
{% extends "base.html.j2" %}
{% block body %}
<aside>
  <h3>Hotels</h3>
  {% for h in hotels %}
  <div>
    <button hx-get="/partials/contacts?hotel={{ h.hotel | urlencode }}"
            hx-target="#contact-list"
            hx-swap="innerHTML">{{ h.hotel }} ({{ h.n }})</button>
  </div>
  {% endfor %}
  <hr/>
  <div id="contact-list">
    {% include "partials/hotel_list.html.j2" %}
  </div>
</aside>
<main>
  <div id="detail"><p>Select a contact.</p></div>
  <div id="notes"></div>
</main>
{% endblock %}
```

Create `crm_app/templates/partials/hotel_list.html.j2`:

```html
{% for c in contacts %}
<div>
  <a href="#"
     hx-get="/partials/contact/{{ c.id }}"
     hx-target="#detail"
     hx-swap="innerHTML">{{ c.full_name or "?" }} — {{ c.phone or c.phone2 or "no phone" }}</a>
</div>
{% endfor %}
```

Create `crm_app/templates/partials/contact.html.j2`:

```html
{% if not row %}
<p>Not found.</p>
{% else %}
<div class="phone">{{ row.phone or row.phone2 or "—" }}</div>
<p>{% if row.linkedin_url %}<a href="{{ row.linkedin_url }}" target="_blank" rel="noopener">LinkedIn</a>{% else %}No LinkedIn{% endif %}</p>
<p><strong>{{ row.full_name or "?" }}</strong> — {{ row.title or "" }} @ {{ row.company or "" }}</p>
<p>{{ row.target_url }}</p>
<pre style="white-space:pre-wrap;font-size:12px;">{{ row.payload | tojson(indent=2) if row.payload else "{}" }}</pre>
{% endif %}
```

- [ ] **Step 1: Run locally**

```bash
export DATABASE_URL='postgresql://...'
uvicorn crm_app.main:app --reload --host 0.0.0.0 --port 8000
```

Open `http://127.0.0.1:8000/`. Expected: hotel list, click contact loads partial into `#detail`.

- [ ] **Step 2: Commit**

```bash
git add crm_app/ requirements.txt
git commit -m "feat(crm): fastapi jinja htmx shell"
```

---

### Task 5: Supabase Auth JWT gate + notes POST

**Files:**

- Create: `crm_app/auth.py`
- Modify: `crm_app/main.py` — add `Depends` security on routes; notes route

`crm_app/auth.py`:

```python
from __future__ import annotations

import jwt
from fastapi import HTTPException, status

from crm_app.config import Settings


def require_user_email(settings: Settings, session: str | None) -> str:
    if not session:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    try:
        payload = jwt.decode(
            session,
            settings.session_secret,
            algorithms=["HS256"],
            audience="crm",
        )
    except jwt.PyJWTError:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED)
    email = str(payload.get("email") or "").lower()
    if not email or (settings.allowed_emails and email not in settings.allowed_emails):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN)
    return email
```

**Login flow MVP:** POST `/auth/exchange` JSON body `{ "access_token": "<supabase_jwt>" }`. Server verifies `access_token` with `SUPABASE_JWT_SECRET` (Supabase Dashboard → Project Settings → API → JWT Secret). Decode with `jwt.decode(access_token, SUPABASE_JWT_SECRET, algorithms=["HS256"], audience="authenticated")` — confirm `aud` claim matches your project (Supabase docs). Then mint app cookie: HS256 JWT `crm_session` signed with `SESSION_SECRET`, claims `{ "email": "<user email>", "aud": "crm" }`, `httponly=True`, `secure=True` in prod, `samesite=lax`.

Static page `crm_app/static/login.html` loads Supabase JS from CDN, calls `signInWithPassword` or magic link, sends returned `session.access_token` to `/auth/exchange`, browser stores `crm_session`.

`crm_app/main.py` — notes save (HTMX) + DI:

```python
from datetime import datetime, timezone
from typing import Annotated

from fastapi import Cookie, Depends, Form
from fastapi.responses import HTMLResponse

from crm_app.auth import require_user_email
from crm_app.config import Settings, load_settings
from crm_app.db import connection


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_settings() -> Settings:
    return load_settings()


def current_email(
    s: Annotated[Settings, Depends(get_settings)],
    session: Annotated[str | None, Cookie(alias="crm_session")] = None,
) -> str:
    return require_user_email(s, session)


@app.post("/notes/{contact_id}")
def save_note(
    contact_id: str,
    body: str = Form(""),
    email: str = Depends(current_email),
    s: Settings = Depends(get_settings),
):
    with connection(s) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO crm_notes (contact_id, body, updated_by, updated_at)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (contact_id) DO UPDATE SET
                    body = EXCLUDED.body,
                    updated_by = EXCLUDED.updated_by,
                    updated_at = EXCLUDED.updated_at
                """,
                (contact_id, body, email, _now()),
            )
        conn.commit()
    return HTMLResponse("<p>Saved.</p>")
```

Wire `contact.html.j2` form at bottom: `hx-post="/notes/{{ row.id }}"`, `hx-vals` or `<textarea name="body">`, `hx-target="#note-status"`, `hx-swap="innerHTML"`.

- [ ] **Step 1: Test auth with curl**

```bash
curl -i -X POST http://127.0.0.1:8000/auth/exchange -H "Content-Type: application/json" -d '{"access_token":"..."}'
```

Expected: `Set-Cookie: crm_session=...`

- [ ] **Step 2: Commit**

```bash
git add crm_app/auth.py crm_app/main.py crm_app/templates/
git commit -m "feat(crm): session gate and notes save"
```

---

### Task 6: README + `.env.example` (no real secrets)

**Files:**

- Modify: [README.md](../../../README.md)
- Modify: [.env.example](../../../.env.example) (create if missing)

Document:

- `DATABASE_URL`, `SUPABASE_URL`, `SUPABASE_JWT_SECRET`, `SESSION_SECRET` (random 32+ bytes hex), `ALLOWED_EMAILS=a@x.com,b@y.com`
- Run sync after `python scripts/rebuild_fulljsons.py`
- Deploy: Render start command `uvicorn crm_app.main:app --host 0.0.0.0 --port $PORT`

- [ ] **Commit**

```bash
git add README.md .env.example
git commit -m "docs(crm): env and runbook"
```

---

## Self-review

**Spec coverage:** Hotels left, detail right phone+LinkedIn+payload, notes bottom, Supabase DB, local sync after pipeline, HTMX — mapped to Tasks 1–6.

**Placeholder scan:** None intentional.

**Type consistency:** `crm_contacts.id` = output of `stable_crm_contact_id` everywhere.

---

## Execution handoff

**Plan saved to:** `docs/superpowers/plans/2026-04-26-hotel-crm-htmx-supabase.md`

**Two execution options:**

1. **Subagent-Driven (recommended)** — fresh subagent per task, review between tasks  
2. **Inline Execution** — same session, executing-plans checkpoints  

**Which approach?**

---

## Security warning (non-negotiable)

If any API key ever lived in a tracked `.env`, rotate those keys in provider dashboards before deploy. Never commit populated `.env`.
