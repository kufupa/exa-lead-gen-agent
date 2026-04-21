# fullJSONs pipeline + concurrent merge Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `fullJSONs/` as canonical store for merged aggregates (`all_enriched_leads.json`, `intimate_phone_contacts.json`, `intimate_email_contacts.json`, `url_registry.json`), keep per-hotel Phase 1+2 artifacts in `jsons/`, and guarantee safe concurrent updates when multiple URLs run in parallel or overlapping batches.

**Architecture:** Per-URL research/enrichment continues writing **only** unique paths under `jsons/` (already true when default filename includes URL hash). Any update to **shared** files under `fullJSONs/` goes through one **process-wide file lock** (`filelock.FileLock` on `fullJSONs/.merge.lock`), short critical section: read JSON → merge in memory → write temp → `Path.replace` atomic swap. URL registry records lifecycle (`queued` → `researching` → `enriched` / `failed`) so duplicate URL work can short-circuit or wait. A thin **batch CLI** wraps existing `hotel_decision_maker_research.py` + `hotel_contact_enrichment.py` with `ThreadPoolExecutor`, then calls merge after each job under the same lock contract.

**Tech stack:** Python 3.11+, existing `hotel_decision_maker_research.py` / `contact_enrichment`, new dep **`filelock`**, `pytest`, `pathlib`, `json`, `concurrent.futures`.

**Prerequisite (from brainstorming / superpowers):** Prefer implementing in a **git worktree** so main branch stays clean while multi-file work lands.

---

## File map (create / modify)

| Path | Responsibility |
|------|------------------|
| `requirements.txt` | Add `filelock>=3.13.0` |
| `fulljsons/__init__.py` | Package marker |
| `fulljsons/atomic.py` | `atomic_write_json(path, obj)` — write `*.tmp` then `replace` |
| `fulljsons/lock.py` | `merge_lock(fulljsons_dir: Path) -> FileLock` context helper, timeout configurable |
| `fulljsons/registry.py` | Load/merge/patch `url_registry.json` entries (pure functions + types) |
| `fulljsons/merge_all_enriched.py` | Scan `jsons/*.enriched.json`, build master payload, merge with existing master |
| `fulljsons/intimate_exports.py` | Shared dedupe/score helpers + `build_phone_rows` / `build_email_rows` from enriched files |
| `scripts/build_intimate_phone_contacts.py` | **Modify:** default `--out` → `fullJSONs/intimate_phone_contacts.json`; keep `--jsons-dir jsons` |
| `scripts/build_intimate_email_contacts.py` | **Create:** mirror phone script; filter = named non-generic email on `email`/`email2` (import `is_generic_functional_email` from `hotel_decision_maker_research.py`) |
| `scripts/rebuild_fulljsons.py` | **Create:** optional one-shot rebuild all four aggregates under lock |
| `hotel_batch_pipeline.py` (repo root) | **Create:** `--url` repeatable or `--urls-file`; `--workers N`; registry claim; subprocess or function calls to research+enrich; merge hooks |
| `tests/test_fulljsons_lock_merge.py` | **Create:** concurrent merge + atomic write + registry merge tests |
| `tests/test_intimate_email_export.py` | **Create:** filter rules on synthetic contacts |
| `README.md` | Document `fullJSONs/`, lock semantics, batch CLI |
| `jsons/intimate_phone_contacts.json` | **Delete** after migration (or leave; plan says move → regenerate under `fullJSONs/`) |

---

## Schemas (concrete)

### `fullJSONs/url_registry.json`

```json
{
  "version": 1,
  "updated_at_utc": "2026-04-21T12:00:00+00:00",
  "urls": {
    "https://www.example.com/": {
      "status": "enriched",
      "claimed_by": "pid:12345",
      "last_started_at_utc": null,
      "last_finished_at_utc": "2026-04-21T12:05:00+00:00",
      "research_json": "jsons/hotel_leads__www_example_com__abcd1234.json",
      "enriched_json": "jsons/hotel_leads__www_example_com__abcd1234.enriched.json",
      "error": null
    }
  }
}
```

**`status` values:** `queued` | `researching` | `enriched` | `failed`  
**Rule:** Before starting research for URL `U`, under merge lock: if `urls[U].status == researching` and `last_started_at_utc` younger than **stale threshold** (e.g. 6h) and PID not alive → treat stale, allow reclaim; if **enriched** and outputs exist on disk → skip re-run unless `--force`.

### `fullJSONs/all_enriched_leads.json`

```json
{
  "version": 1,
  "updated_at_utc": "...",
  "runs": [
    {
      "source_file": "jsons/foo.enriched.json",
      "target_url": "https://...",
      "research_generated_at_utc": "...",
      "contact_enrichment": { }
    }
  ],
  "contacts": [
    {
      "dedupe_key": "li:https://...",
      "target_url": "https://...",
      "source_enriched_json": "jsons/foo.enriched.json",
      "contact": { }
    }
  ]
}
```

`contact` = **full** contact dict from enriched file (all keys), unchanged, so nothing is lost vs individual JSON.

### `fullJSONs/intimate_phone_contacts.json` / `intimate_email_contacts.json`

Keep existing v2 shape for phone (version bump to **3** when adding `merge_meta` optional field). Email file: same structure, `criteria` string differs, filter on **named** email using existing `is_generic_functional_email` from `hotel_decision_maker_research.py`.

---

### Task 1: Add dependency

**Files:**
- Modify: `requirements.txt`

- [ ] **Step 1: Add line**

```text
filelock>=3.13.0
```

- [ ] **Step 2: Install**

Run: `pip install filelock>=3.13.0`  
Expected: installs without error.

- [ ] **Step 3: Commit**

```bash
git add requirements.txt
git commit -m "chore: add filelock for fullJSONs merge coordination"
```

---

### Task 2: `fulljsons/atomic.py` + unit test

**Files:**
- Create: `fulljsons/__init__.py` (empty)
- Create: `fulljsons/atomic.py`
- Create: `tests/test_fulljsons_atomic.py`

- [ ] **Step 1: Write failing test**

`tests/test_fulljsons_atomic.py`:

```python
import json
from pathlib import Path

from fulljsons.atomic import atomic_write_json


def test_atomic_write_json_creates_file(tmp_path: Path) -> None:
    p = tmp_path / "nested" / "a.json"
    atomic_write_json(p, {"x": 1})
    assert p.exists()
    assert json.loads(p.read_text(encoding="utf-8")) == {"x": 1}
```

- [ ] **Step 2: Run test — expect fail**

Run: `pytest tests/test_fulljsons_atomic.py::test_atomic_write_json_creates_file -v`  
Expected: `ImportError` or `ModuleNotFoundError` for `fulljsons.atomic`.

- [ ] **Step 3: Implement**

`fulljsons/atomic.py`:

```python
from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def atomic_write_json(path: Path, data: Any, *, indent: int = 2) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(data, ensure_ascii=False, indent=indent)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)
```

- [ ] **Step 4: Run test — expect pass**

Run: `pytest tests/test_fulljsons_atomic.py -v`  
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add fulljsons/ tests/test_fulljsons_atomic.py
git commit -m "feat(fulljsons): atomic JSON writer"
```

---

### Task 3: Merge lock helper + concurrent stress test

**Files:**
- Create: `fulljsons/lock.py`
- Create: `tests/test_fulljsons_lock_merge.py`

- [ ] **Step 1: Write failing test** (two threads increment same counter in JSON)

`tests/test_fulljsons_lock_merge.py`:

```python
import json
import threading
from pathlib import Path

from filelock import FileLock

from fulljsons.atomic import atomic_write_json
from fulljsons.lock import locked_merge_json


def test_locked_merge_json_serializes_writers(tmp_path: Path) -> None:
    data_path = tmp_path / "c.json"
    lock_path = tmp_path / "c.lock"
    atomic_write_json(data_path, {"n": 0})

    def bump() -> None:
        def merger(existing: dict) -> dict:
            existing = existing or {}
            existing["n"] = int(existing.get("n", 0)) + 1
            return existing

        locked_merge_json(data_path, lock_path, merger, timeout=30)

    threads = [threading.Thread(target=bump) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert json.loads(data_path.read_text(encoding="utf-8"))["n"] == 10
```

Adjust import to match actual API name you implement (`locked_merge_json`).

- [ ] **Step 2: Run test — fail**

Run: `pytest tests/test_fulljsons_lock_merge.py -v`  
Expected: import or attribute failure.

- [ ] **Step 3: Implement `fulljsons/lock.py`**

```python
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable

from filelock import FileLock

from fulljsons.atomic import atomic_write_json


def locked_merge_json(
    data_path: Path,
    lock_path: Path,
    merger: Callable[[dict[str, Any] | None], dict[str, Any]],
    *,
    timeout: float = 300,
) -> dict[str, Any]:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock = FileLock(str(lock_path), timeout=timeout)
    with lock:
        existing: dict[str, Any] | None = None
        if data_path.exists():
            raw = data_path.read_text(encoding="utf-8").strip()
            if raw:
                existing = json.loads(raw)
        merged = merger(existing)
        atomic_write_json(data_path, merged)
        return merged
```

- [ ] **Step 4: Run test — pass**

Run: `pytest tests/test_fulljsons_lock_merge.py -v`

- [ ] **Step 5: Commit**

```bash
git add fulljsons/lock.py tests/test_fulljsons_lock_merge.py
git commit -m "feat(fulljsons): file-lock guarded JSON merge"
```

---

### Task 4: URL registry merge logic (pure) + tests

**Files:**
- Create: `fulljsons/registry.py`
- Extend: `tests/test_fulljsons_lock_merge.py` (or new `tests/test_url_registry.py`)

- [ ] **Step 1: Write failing tests** for `claim_url`, `mark_enriched`

`tests/test_url_registry.py`:

```python
from fulljsons.registry import apply_patch, empty_registry


def test_apply_patch_creates_url_entry() -> None:
    reg = empty_registry()
    reg2 = apply_patch(
        reg,
        url="https://a.com/",
        patch={"status": "researching", "claimed_by": "pid:1"},
    )
    assert reg2["urls"]["https://a.com/"]["status"] == "researching"


def test_apply_patch_preserves_other_urls() -> None:
    reg = empty_registry()
    reg = apply_patch(reg, "https://a.com/", {"status": "enriched"})
    reg = apply_patch(reg, "https://b.com/", {"status": "queued"})
    assert set(reg["urls"]) == {"https://a.com/", "https://b.com/"}
```

- [ ] **Step 2: Run — fail**

- [ ] **Step 3: Implement `fulljsons/registry.py`**

```python
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def empty_registry() -> dict[str, Any]:
    return {"version": 1, "updated_at_utc": _now(), "urls": {}}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def apply_patch(reg: dict[str, Any], url: str, patch: dict[str, Any]) -> dict[str, Any]:
    reg = dict(reg)
    reg["updated_at_utc"] = _now()
    urls = dict(reg.get("urls") or {})
    cur = dict(urls.get(url) or {})
    cur.update(patch)
    urls[url] = cur
    reg["urls"] = urls
    return reg
```

- [ ] **Step 4: Run tests — pass**

- [ ] **Step 5: Commit**

---

### Task 5: Merge `all_enriched_leads.json` (locked) + test with two fake enriched files

**Depends on:** Task 6 first (`fulljsons/intimate_exports.py` must expose `dedupe_key` / `score_for_pick` used here).

**Files:**
- Create: `fulljsons/merge_all_enriched.py`
- Create: `tests/test_merge_all_enriched.py`

- [ ] **Step 1: Write test** that drops two minimal `*.enriched.json` into `tmp_path/jsons/`, runs merge, expects `contacts` length sum minus dedupe.

Use minimal valid enriched shape: `{"target_url":"https://x.com/","contacts":[{"full_name":"A","title":"t",...}]} ` — easiest: load real fixture copy from `jsons/` if present, else inline dict with required contact keys only for dedupe_key used in merge (import same `dedupe_key` function from `scripts/build_intimate_phone_contacts.py` or move `dedupe_key` to `fulljsons/intimate_exports.py` first in Task 6).

**Recommendation:** In Task 6, **move** `dedupe_key` / `score_for_pick` to `fulljsons/intimate_exports.py`; Task 5 imports `dedupe_key` from there for master merge.

- [ ] **Step 2: Implement `merge_all_enriched_incremental`**

Signature:

```python
def merge_all_enriched_incremental(
    *,
    fulljsons_dir: Path,
    jsons_dir: Path,
    enriched_path: Path,
    lock_path: Path | None = None,
) -> None:
    ...
```

Inside: `lock_path = lock_path or fulljsons_dir / ".merge.lock"` then `locked_merge_json(fulljsons_dir / "all_enriched_leads.json", lock_path, merger)`.

`merger(existing)`:
- Parse `existing` or start from `empty_master()`
- Read `enriched_path` JSON
- For each contact, append `{dedupe_key, target_url, source_enriched_json, contact: contact_dict}`
- Dedupe `contacts` list by `dedupe_key(contact_dict)` — keep higher `score_for_pick(contact_dict)` like phone export
- Update `runs` list: upsert by `source_file` = relative path `jsons/foo.enriched.json` as string posix

- [ ] **Step 3: Test + commit**

---

### Task 6: Refactor intimate phone build → `fulljsons/intimate_exports.py` + wire scripts

**Files:**
- Create: `fulljsons/intimate_exports.py` (move helpers from `scripts/build_intimate_phone_contacts.py`)
- Modify: `scripts/build_intimate_phone_contacts.py` — thin CLI calling `rebuild_intimate_phone(fulljsons_dir, jsons_dir)`
- Default `--out` to `fullJSONs/intimate_phone_contacts.json` (note folder name **fullJSONs** with capital J per user; Python module stays `fulljsons` lowercase — **document** that CLI paths use `fullJSONs/` on disk for user visibility)

**Naming decision:** Use directory name **`fullJSONs`** at repo root (matches user). Import package remains **`fulljsons`** (valid Python identifier).

- [ ] **Step 1: Move `dedupe_key`, `has_structured_phone`, `score_for_pick`, `build_row`-equivalent into `intimate_exports.py`**

- [ ] **Step 2: Add `rebuild_intimate_phone_json(...)`** calling `locked_merge_json` only if merging into aggregate — actually **full rebuild** from all `jsons/*.enriched.json` can be lock-short: load all sources, compute set, `atomic_write_json` output **inside** lock? Simpler v1: **always** `locked_merge_json` on output path with merger that **replaces entire document** built from scanning `jsons_dir` each time (O(n) files, OK for <100 hotels). Avoids partial-merge bugs.

Merger for intimate phone file:

```python
def merger(_existing):
    return build_intimate_phone_document(jsons_dir=jsons_dir)
```

Whole file rewritten each rebuild — still safe under lock.

- [ ] **Step 3: Update script defaults** `--out` default `fullJSONs/intimate_phone_contacts.json`, `--jsons-dir` default `jsons`

- [ ] **Step 4: pytest** on existing tests + new test `test_rebuild_intimate_phone_smoke` using tmp_path jsons with one fake enriched.

- [ ] **Step 5: Delete** `jsons/intimate_phone_contacts.json` from repo after regenerating into `fullJSONs/` (or run script once in CI step).

- [ ] **Step 6: Commit**

---

### Task 7: `intimate_email_contacts.json` + tests

**Files:**
- Create: `scripts/build_intimate_email_contacts.py`
- Create: `tests/test_intimate_email_export.py`

- [ ] **Step 1: Test** — contact with `email=reservations@x.com` (generic) excluded; `email=jane.doe@x.com` included.

Import:

```python
from hotel_decision_maker_research import is_generic_functional_email
```

```python
def has_named_email(c: dict) -> bool:
    for k in ("email", "email2"):
        v = (c.get(k) or "").strip()
        if v and not is_generic_functional_email(v):
            return True
    return False
```

- [ ] **Step 2: Implement script** mirroring phone rebuild under lock, output `fullJSONs/intimate_email_contacts.json`, version 1, same `phase1_research` / `phase2_contact_enrichment` embedding as phone v2.

- [ ] **Step 3: pytest + commit**

---

### Task 8: `hotel_batch_pipeline.py` (multi-URL, workers, registry, merge)

**Files:**
- Create: `hotel_batch_pipeline.py`

**CLI sketch:**

```text
python hotel_batch_pipeline.py \
  --url https://a.com/ --url https://b.com/ \
  --workers 4 \
  --jsons-dir jsons \
  --fulljsons-dir fullJSONs \
  --agent-count 4
```

**Behavior:**
1. Normalize URLs (reuse `_normalize_url` logic — import from `hotel_decision_maker_research` or duplicate minimal strip + https).
2. For each URL before subprocess: `locked_merge_json(registry_path, lock, merger_claim)` sets status `researching`, records `claimed_by` = `f"pid:{os.getpid()}"`, `last_started_at_utc`.
3. Run `subprocess.run([sys.executable, "hotel_decision_maker_research.py", "--url", url, "--out-json", out_json, "--out-csv", ...])` — **pass explicit `--out-json`** computed with `default_json_path_from_url` so path is deterministic inside `jsons/`.
4. On success: run enrichment subprocess with `--in-json` / `--out-json` paths.
5. On full success: under **same** `locked_merge_json` call sequence (single lock acquisition preferred — one `with lock:` block that: updates registry to `enriched`, calls `merge_all_enriched_incremental`, calls `rebuild_intimate_phone_json`, `rebuild_intimate_email_json` OR defer heavy rebuilds to end of batch — **user asked no bugs**: single lock section per URL completion that (a) registry patch (b) incremental master merge for that one file only (Task 5) (c) **skip** full rescan of intimates OR rescan all — rescan all is simpler code, longer hold. **Compromise:** hold lock only for **registry + merge_all one file**; intimates rebuild **without** scanning all in lock: build in temp under `fullJSONs/.tmp/` then `locked_merge_json` replace with merger returning prebuilt dict (fast). Merger then just assigns — still serialized.

**Refined:** Outside lock: `doc = build_intimate_phone_document(jsons_dir)`. Inside lock: `locked_merge_json(..., lambda _: doc)`.

6. On failure: registry `failed` + `error` message stderr tail truncated.

7. `--skip-if-enriched`: if registry says enriched and enriched json exists, skip.

- [ ] **Step 1: Write integration test** with `subprocess` mocking via `monkeypatch` env `HOTEL_BATCH_FAKE=1` — optional; if too heavy, smoke test only lock + registry in isolation.

Minimum: `tests/test_batch_pipeline_dry.py` that runs `python hotel_batch_pipeline.py --help` and parses.

- [ ] **Step 2: Implement pipeline file**

- [ ] **Step 3: pytest + manual dry run**

- [ ] **Step 4: Commit**

---

### Task 9: `scripts/rebuild_fulljsons.py` + README

**Files:**
- Create: `scripts/rebuild_fulljsons.py` — loads all `jsons/*.enriched.json`, rebuilds master + both intimates + leaves registry unchanged (or optional `--reset-registry` flag guarded).

- [ ] **Step 1: README section** "fullJSONs & concurrency" — explain lock file, `hotel_batch_pipeline.py`, rebuild script.

- [ ] **Step 2: Commit**

---

## Self-review

**1. Spec coverage**
- `fullJSONs/` folder with master + phone + email + registry: Tasks 5–8.
- Move intimate phone out of `jsons/`: Task 6.
- Concurrent writes safe: Tasks 3, 5, 8 (lock + atomic).
- Per-URL `jsons/` preserved: stated in Architecture; pipeline passes `--out-json` under `jsons/`.
- URL queue / skip duplicate: Task 8 registry + `--skip-if-enriched`.
- Wrapper for multi-URL + parallelism: Task 8.

**2. Placeholder scan** — no TBD strings in executable sections; adjust test fixture strategy if repo lacks `jsons/*.enriched.json` in CI (use tmp_path fixtures).

**3. Type consistency** — `dedupe_key` lives one place (`fulljsons/intimate_exports.py`); phone/email/master all import it.

**Gaps addressed later (optional phase 2):** Stale `researching` detection via PID alive check on Unix only; Windows stub = time-based stale only. Document in README.

---

## Execution handoff

**Plan complete and saved to `docs/superpowers/plans/2026-04-21-fulljsons-pipeline.md`. Two execution options:**

**1. Subagent-Driven (recommended)** — dispatch fresh subagent per task, review between tasks.

**2. Inline Execution** — execute tasks in this session using executing-plans with checkpoints.

**Which approach?**
