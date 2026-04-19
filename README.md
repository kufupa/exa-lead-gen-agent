# exa-lead-gen-agent

Generate enriched lead lists with structured company data using Exa deep search. ICP research, micro-vertical query expansion, parallel batch subagents, deduplication, ICP scoring, and CSV output — all from a single prompt.

## Run

```bash
npx @open-gitagent/gitagent run -r https://github.com/shreyas-lyzr/exa-lead-gen-agent
```

## Hotel leads (xAI)

1. **Research** — `hotel_decision_maker_research.py` writes a JSON + optional CSV (needs `XAI_API_KEY`).
2. **Contact enrichment** — `hotel_contact_enrichment.py` re-reads that JSON, runs `grok-4.20-reasoning` with web + X search per contact (skips rows that already score high on direct channels), merges email/phone/X/LinkedIn back in.

```bash
pip install -r requirements.txt
export XAI_API_KEY=...
python hotel_decision_maker_research.py --url https://example-hotel.com
python hotel_contact_enrichment.py --in-json hotel_leads__....json --out-json enriched.json --mode realtime
# Cheaper overnight: --mode batch --checkpoint .cache/enrich.json --resume
python hotel_contact_enrichment.py --in-json in.json --out-json out.json --dry-run
```

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
