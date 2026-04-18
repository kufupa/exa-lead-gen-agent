# exa-lead-gen-agent

Generate enriched lead lists with structured company data using Exa deep search. ICP research, micro-vertical query expansion, parallel batch subagents, deduplication, ICP scoring, and CSV output — all from a single prompt.

## Run

```bash
npx @open-gitagent/gitagent run -r https://github.com/shreyas-lyzr/exa-lead-gen-agent
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
└── knowledge/
    ├── index.yaml
    └── mcp-setup.md
```

## Built with

[gitagent](https://github.com/open-gitagent/gitagent) — a git-native, framework-agnostic open standard for AI agents.
