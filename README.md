# exa-lead-gen-agent

Generate enriched lead lists with structured company data using Exa deep search. ICP research, micro-vertical query expansion, parallel batch subagents, deduplication, ICP scoring, and CSV output — all from a single prompt.

## Run

```bash
npx @open-gitagent/gitagent run -r https://github.com/shreyas-lyzr/exa-lead-gen-agent
```

## Hotel leads (xAI)

1. **Research** — `hotel_decision_maker_research.py` writes JSON under **`jsons/`** and appends CSV under **`csv/`** by default (needs `XAI_API_KEY`).
2. **Contact enrichment** — `hotel_contact_enrichment.py` re-reads that JSON, runs `grok-4.20-reasoning` with web + X search per contact (skips rows that already score high on direct channels), merges email/phone/X/LinkedIn back in.

```bash
pip install -r requirements.txt
export XAI_API_KEY=...
python hotel_decision_maker_research.py --url https://example-hotel.com
python hotel_contact_enrichment.py --in-json jsons/hotel_leads__....json --out-json jsons/hotel_leads__....enriched.json --mode realtime
# Cheaper overnight: --mode batch --checkpoint .cache/enrich.json --resume
python hotel_contact_enrichment.py --in-json in.json --out-json out.json --dry-run
```

### fullJSONs aggregates (multi-URL, locked writes)

Python code lives in the **`lead_aggregates/`** package. Merged JSON outputs live under **`fullJSONs/`** (different name/case on purpose: on Windows, a package folder `fulljsons/` would collide with `fullJSONs/`).

| File | Purpose |
|------|---------|
| `fullJSONs/all_enriched_leads.json` | Warehouse: every contact from every `jsons/*.enriched.json` with `occurrence_id` = `source_file::dedupe_key` |
| `fullJSONs/intimate_phone_contacts.json` | Rows with structured `phone` / `phone2` (globally deduped) |
| `fullJSONs/intimate_email_contacts.json` | Rows with named non-generic `email` / `email2` |
| `fullJSONs/url_registry.json` | Canonical hotel URL → status, paths, errors |
| `fullJSONs/.merge.lock` | `filelock` coordination for all updates above |

**Rebuild everything from current `jsons/*.enriched.json`:**

```bash
python scripts/rebuild_fulljsons.py
```

**Rebuild only intimate slices:**

```bash
python scripts/build_intimate_phone_contacts.py
python scripts/build_intimate_email_contacts.py
```

**Many URLs in parallel** (run from repo root; uses `XAI_API_KEY`):

```bash
python hotel_batch_pipeline.py --url https://a.com/ --url https://b.com/ --workers 4 --skip-if-enriched
# or: --urls-file urls.txt
```

Each completed hotel triggers a locked refresh of all four `fullJSONs/` files (full rebuild from `jsons/` — simple and idempotent). If a run crashes mid-write, run `python scripts/rebuild_fulljsons.py` to heal aggregates from disk.

## Prerequisites

Requires an [Exa API key](https://dashboard.exa.ai/api-keys). Add the Exa MCP server before running:

```bash
claude mcp add --transport http exa "https://mcp.exa.ai/mcp?exaApiKey=YOUR_EXA_API_KEY&tools=web_search_advanced_exa"
```

## What It Can Do

- **ICP Research** — Automatically researches a target company to define its Ideal Customer Profile
- **Micro-Vertical Expansion** — Generates dozens of specific search queries for maximum coverage
- **Parallel Lead Generation** — Launches batch subagents that run Exa deep searches in parallel
- **Structured Enrichment** — Every lead comes with ICP fit score, reasoning, and custom enrichment fields
- **Deduplication & CSV** — Normalizes company names, dedupes, sorts by score, outputs clean CSV

## Example Usage

```
"Generate 500 leads for Lyzr"
"Find 200 companies that would buy our developer tools product"
"Build a prospect list of Series A-C SaaS companies using AI for customer support"
```

## Structure

```
exa-lead-gen-agent/
├── agent.yaml
├── csv/
├── fullJSONs/
├── jsons/
├── lead_aggregates/
├── SOUL.md
├── RULES.md
├── README.md
├── skills/
│   └── exa-lead-gen/
│       └── SKILL.md
│   └── people-research/
│       └── SKILL.md
└── knowledge/
    ├── index.yaml
    └── mcp-setup.md
```

## Built with

[gitagent](https://github.com/open-gitagent/gitagent) — a git-native, framework-agnostic open standard for AI agents.
