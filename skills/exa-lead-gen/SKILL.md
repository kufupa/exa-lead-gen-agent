---
name: exa-lead-gen
description: "Generate enriched lead lists using Exa deep search. Finds companies matching an ICP, enriches with signals/news/scores, and outputs CSV. Use when generating leads, building prospect lists, finding companies to sell to, doing outbound research, or ICP-based company discovery. Triggers on leads, lead gen, prospect list, find companies, ICP, outbound list."
allowed-tools: "Bash, Read, Write, Agent"
---

# Lead Generation with Exa Deep Search

Generate large, enriched lead lists by running parallel Exa deep searches across micro-verticals derived from an Ideal Customer Profile (ICP).

## Prerequisites

This skill requires the Exa MCP server with `web_search_advanced_exa` tool (including `outputSchema` and `systemPrompt` support).

If `mcp__claude_ai_Exa_MCP__web_search_advanced_exa` is not available, tell the user:

> You need the Exa MCP server installed with your API key.
> Instructions: https://exa.ai/docs/reference/exa-mcp

Then stop.

## Tool Restriction

ONLY use `mcp__claude_ai_Exa_MCP__web_search_advanced_exa`, the Agent tool (for batch subagents), Write, and Bash (for Python CSV compilation). Do NOT use web search, file reads, or other Exa tools.

## Architecture: Subagent-Driven Lead Gen

**The main agent (you) never touches raw lead data.** This keeps your context lean and prevents token bloat.

```
Main Agent (orchestrator — lean context)
├── Step 1: ICP research (1 exa call, user confirms/refines)
├── Step 2: Generate micro-verticals (LLM reasoning, no API)
├── Step 3: Design outputSchema (LLM reasoning, no API)
├── Step 4: Launch batch subagents ──┐
│     Each batch subagent:           │
│     - Receives 5 micro-verticals   │
│     - Runs 5 exa deep calls        │
│     - Writes JSON to /tmp files    │
│     - Reports back ONLY the count  │
├── Step 5: Python CSV compiler (reads JSON files, dedupes, outputs CSV)
└── Step 6: Summary
```

**Why subagents?** Each exa deep call returns ~45 companies as structured JSON (5-15K tokens). For 1000 leads (~30 calls), that's 150-450K tokens of raw data. The main agent doesn't need any of it — the Python script handles dedup/sort/CSV. Subagents process the data, write to files, dispose their context, and report back just the count.

**Why batches of 5?** Each subagent runs 5 exa calls in parallel (the max for parallel tool calls). For 30 micro-verticals: 6 batch subagents. Less overhead than 1-per-call (30 spawns), avoids context bloat of 1 giant subagent.

## Understanding the Three Params

Each `web_search_advanced_exa` call has three distinct params that serve different roles:

- **`objective`** (the query) — embedding-space coverage. Keywords that help Exa find the right pages in its index. Think of this as the search query.
- **`systemPrompt`** — the brain. Tells Exa what to do with what it finds — how to score, what to exclude, how many results to return, any special instructions.
- **`outputSchema`** — the structure. A JSON schema defining the exact fields and enrichment level you want back. Complex schemas = more latency, so warn the user if they want many enrichment columns. **Constraints:**
  - Max 10 properties **total across all nesting levels** (e.g. 1 `companies` wrapper + 9 item fields = 10). Will 400 at 11+.
  - Items inside arrays must be **flat objects with primitive fields only** (string, integer, boolean, array of strings). No nested objects inside array items — will 400.
  - Must be a valid object with `"type": "object"` at root (empty `{}`, strings, arrays, or missing `type` will error)
  - `null` is silently ignored (no schema applied)
  - Supported field types: `string`, `integer`, `boolean`, `array` (of strings). All return proper typed values (not stringified).

**REQUIRED on every `web_search_advanced_exa` call — no exceptions:**

- `structuredOutput: true`
- `numResults: 50` — this controls how many source pages Exa crawls. Set to 50 (matching the systemPrompt target) for best results.
- `highlightMaxCharacters: 1` — minimizes response size when using structured output
- `type: "deep"`

**Critical: numResults and systemPrompt must align.** Set `numResults` to 50 and ask for "exactly 50 companies" in the systemPrompt. This is the sweet spot — consistently yields ~42-49 companies per call.

## String Field Rule

All string fields in any `outputSchema` MUST include a length constraint in their description — e.g. "in 12 words or less", "under 15 words", "one sentence max". This keeps responses punchy, reduces token waste, and produces cleaner CSV output. Never leave a string field without a word/character limit.

## Step 1: Research the Target Company

When the user says something like "Make a list of 1000 leads for [company]", first run a single `web_search_advanced_exa` call to understand the company's product and ICP. This call is small enough to run in the main context.

```
objective: "About {company_name}, {company_name} customers"

systemPrompt: |
  Your job is to figure out the ICP of the specified company and return a structured output.
  To do so, you need to research the company's website, how they describe themselves, and who their existing customers are. From that, you can deduce what their ICP is.

outputSchema: {
  "type": "object",
  "required": ["company_description", "product_description", "existing_customers", "icp_description", "sub_verticals", "demographic_signals", "useful_enrichments"],
  "properties": {
    "company_description": {
      "type": "string",
      "description": "What the company does in 2 sentences or less"
    },
    "product_description": {
      "type": "string",
      "description": "What they sell and to whom in 2 sentences or less"
    },
    "existing_customers": {
      "type": "array",
      "description": "Known customers or case studies",
      "items": { "type": "string" }
    },
    "icp_description": {
      "type": "string",
      "description": "Concise ICP description in 12 words or less that clearly defines target companies for SDRs"
    },
    "sub_verticals": {
      "type": "array",
      "description": "10 MECE sub-verticals breaking down the ICP",
      "items": { "type": "string" }
    },
    "demographic_signals": {
      "type": "array",
      "description": "Demographic signals formatted as enrichment column AI prompts for SDR lead identification",
      "items": { "type": "string" }
    },
    "useful_enrichments": {
      "type": "array",
      "description": "Enrichment columns useful for filtering high-signal companies",
      "items": {
        "type": "string",
        "description": "e.g. 'Number of employees', 'Latest funding round', 'Hiring software engineers?'"
      }
    }
  }
}

structuredOutput: true
numResults: 10
type: "deep"
highlightMaxCharacters: 1
```

Present the ICP research results to the user and ask them to confirm or refine:

- Is the ICP description accurate?
- Any sub-verticals to add/remove?
- Any companies to exclude (competitors, existing customers)?
- How many leads do they want? (default 200)
- Any specific enrichment columns they care about?

## Step 2: Generate Micro-Verticals (Query Expansion)

Using the confirmed ICP and sub-verticals as seeds, expand into **micro-verticals** — highly specific, keyword-rich queries that each cover distinct territory in Exa's embedding space.

You do this expansion yourself (no Exa call needed — LLMs are great at query expansion). The goal is to generate MORE queries than you need, because:

- Each call returns ~35-48 companies (avg ~42) when configured correctly (numResults: 50, systemPrompt asks for "exactly 50")
- After deduplication you'll lose some
- It's better to overshoot and trim than undershoot

**Target: `ceil(requested_leads / 35)` micro-verticals** (using /35 instead of /45 to overshoot for dedupe losses).

### Query Expansion Patterns

Use these patterns to expand each sub-vertical into multiple micro-verticals:

1. **Competitor mining** — "companies similar to {existing_customer} building {product_type}" or "alternatives to {existing_customer} in {space}". This is the highest-signal pattern because it directly targets the core ICP.

2. **Geographic breakdown** — split by region: "US-based", "European", "Asia-Pacific", "Latin American" + the vertical keywords

3. **Company stage breakdown** — "seed-stage startups building...", "growth-stage companies building...", "enterprise companies using..."

4. **Technology stack** — "companies using {relevant_tech} for {use_case}" — e.g. "companies using LangChain for document processing"

5. **Use-case decomposition** — break a sub-vertical into specific use cases: instead of "AI healthcare companies", try "clinical trial matching platforms", "medical imaging AI diagnostics", "EHR data analytics tools"

6. **Buyer persona targeting** — "companies with VP of Data Science building...", "engineering-led organizations using..."

### Micro-Vertical Quality Checklist

Each micro-vertical should be:

- **4-8 descriptive keywords** that place it precisely in embedding space
- **Non-overlapping** with other micro-verticals (minimize dedupe waste)
- **Specific enough** to return relevant companies (not "AI companies" — too broad)
- **Broad enough** to return 20+ results (not "Series B AI radiology startups in Boston" — too narrow)

### Example Expansion

Sub-vertical: "Sales intelligence and data enrichment platforms"

Expanded micro-verticals:

- "B2B sales intelligence platforms using web scraping and contact data enrichment"
- "Revenue intelligence startups analyzing call recordings and CRM data"
- "Account-based marketing platforms with intent data and buyer signals"
- "Companies similar to ZoomInfo building business contact databases"
- "European sales enablement startups with AI-powered prospecting tools"
- "Seed-stage startups building outbound sales automation and email sequencing"

## Step 3: Design the Output Schema

Based on the user's prompt, the ICP research (especially `useful_enrichments`), and what makes sense for this specific campaign, craft a custom `outputSchema` for the lead gen calls.

**Always include these core fields:**

- `company_name` (string, "in 5 words or less")
- `website` (string, "homepage URL")
- `product_description` (string, "in 12 words or less")
- `icp_fit_score` (integer, 1-10)
- `icp_fit_reasoning` (string, "compelling one-liner in 20 words or less")

**Then add enrichment fields tailored to the use case.** Use the `useful_enrichments` from Step 1 as inspiration, but tailor to what the user actually needs. Common enrichments:

- `industry_vertical` (string, "in 3 words or less")
- `estimated_employee_count` (string, "range like 11-50, 51-200, 201-500")
- `funding_stage` (string, "one of: Bootstrap, Seed, Series A, Series B, Series C+, Public, Unknown")
- `headquarters_location` (string, "City, Country in 4 words or less")
- `key_technologies` (array of strings, "max 4 items, each 3 words or less")
- `recent_signals` (array of strings, "max 5 items, each under 12 words, write like a presidential brief")
- `decision_maker_titles` (array of strings, "max 3 likely buyer titles, each 5 words or less")
- `potential_use_case` (string, "how they'd use the product in 12 words or less")
- `hiring_signals` (string, "relevant open roles in 10 words or less, or 'None found'")
- `competitor_overlap` (string, "known competitor products used in 10 words or less, or 'None found'")

**Every string field MUST have a word limit in its description.** No exceptions.

**Max 9 item-level fields** (the `companies` wrapper array counts as 1 property toward the global 10-property limit). All item fields must be flat primitives — no nested objects. More fields = more latency per call. Warn the user: "This schema has {N} enrichment columns — each call may take 8-15 seconds. For {M} calls, total time is roughly {estimate}."

## Step 4: Launch Batch Subagents

Group the micro-verticals into batches of 5. For each batch, launch an Agent subagent with all the context it needs to work independently.

### Subagent Prompt Template

For each batch, launch an Agent with this prompt (fill in the variables):

```
You are a lead generation worker. Run exactly {N} exa deep searches and save results to files. Do NOT return raw company data — only report counts.

For each of these {N} micro-verticals, call mcp__claude_ai_Exa_MCP__web_search_advanced_exa:

Micro-verticals:
1. "{micro_vertical_1}"
2. "{micro_vertical_2}"
3. "{micro_vertical_3}"
4. "{micro_vertical_4}"
5. "{micro_vertical_5}"

Use these EXACT params on every call:
- objective: the micro-vertical string above
- systemPrompt: "{the full systemPrompt from Step 4 template below}"
- outputSchema: {the full outputSchema JSON from Step 3}
- search_queries: [3-5 keyword variations of the micro-vertical]
- structuredOutput: true
- numResults: 50
- highlightMaxCharacters: 1
- type: "deep"

Run all {N} calls IN PARALLEL.

After each call completes, write the structured output JSON to a file:
- /tmp/exa_leads_batch_{batch_number}_{call_index}.json
- Write ONLY the structured output content (the companies array/object), not the full API response

After all calls complete, report back ONLY this summary:
"Batch {batch_number}: {total_companies} companies from {successful_calls}/{total_calls} calls. Files: /tmp/exa_leads_batch_{batch_number}_*.json"

If a call fails, skip it and note it in the summary. Do not retry.
```

### SystemPrompt Template (for subagent to use in each exa call)

```
List exactly 50 companies in the final output JSON. Do not return fewer than 50.
Find companies matching this profile and return enriched structured data.
Score each company 1-10 on ICP fit for {user_company} ({product_description}).
The icp_fit_reasoning should be a specific, compelling one-liner that would convince
{user_company}'s Head of Sales to prioritize this prospect.
Do NOT include: {exclusions}.
{any_additional_instructions_from_user}
```

### Launching Batches

Launch batch subagents in parallel using multiple Agent tool calls in a single message. In practice, MCP call overhead naturally staggers requests enough to stay under Exa deep's QPS limits — 6 parallel subagents has been tested without issues.

For very large runs (10+ subagents), launch in waves of ~6 to be safe, since Exa deep has a lower QPS limit than standard search. If you start seeing rate limit errors, reduce wave size.

After each wave completes, update the user:
> "Wave 1 complete: batches 1-6 returned {N} total companies. Launching wave 2..."

## Step 5: Compile CSV with Python

After all batch subagents complete, the main agent runs a Python script to compile results. The main agent NEVER reads the raw JSON files directly — only the Python script does.

Write and run a Python script that:
- Reads all `/tmp/exa_leads_batch_*.json` files
- Extracts the companies array from each (handling different response shapes)
- Deduplicates by company name (case-insensitive, fuzzy — normalize whitespace, strip "Inc"/"Ltd"/etc.)
- When duplicates found, keeps the entry with the higher `icp_fit_score`
- Sorts by `icp_fit_score` descending
- Joins array fields with ` | ` delimiter
- Writes proper CSV with `csv.writer` (handles quoting/escaping automatically)
- Outputs to `{target_company}_leads_{YYYY-MM-DD}.csv`
- Prints summary stats (total, dupes removed, score distribution)

### Python Script Template

```python
import json, csv, glob, os, re
from datetime import date
from pathlib import Path

# Config — fill these in based on the actual schema
OUTPUT_FILE = "{target_company}_leads_{date}.csv"
COLUMNS = [...]  # List of column names matching outputSchema field names
ARRAY_COLUMNS = [...]  # Which columns contain arrays (join with " | ")

# Read all batch files
companies = []
for f in sorted(glob.glob("/tmp/exa_leads_batch_*.json")):
    try:
        data = json.loads(Path(f).read_text())
        # Handle different response shapes
        if isinstance(data, dict):
            if "companies" in data:
                companies.extend(data["companies"])
            elif "output" in data and isinstance(data["output"], dict):
                companies.extend(data["output"].get("companies", []))
            elif "output" in data and isinstance(data["output"], str):
                parsed = json.loads(data["output"])
                companies.extend(parsed.get("companies", []))
        elif isinstance(data, list):
            companies.extend(data)
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        print(f"Warning: skipping {f}: {e}")

# Deduplicate by normalized company name
def normalize(name):
    name = re.sub(r'\b(Inc|Ltd|LLC|Corp|Co|GmbH|AG|SA|SAS|BV|Pty)\.?\b', '', name, flags=re.IGNORECASE)
    return re.sub(r'\s+', ' ', name).strip().lower()

seen = {}
for c in companies:
    name = c.get("company_name", "")
    key = normalize(name)
    if key in seen:
        if c.get("icp_fit_score", 0) > seen[key].get("icp_fit_score", 0):
            seen[key] = c
    else:
        seen[key] = c

# Sort by icp_fit_score descending
deduped = sorted(seen.values(), key=lambda x: x.get("icp_fit_score", 0), reverse=True)

# Write CSV
with open(OUTPUT_FILE, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(COLUMNS)
    for c in deduped:
        row = []
        for col in COLUMNS:
            val = c.get(col, "")
            if col in ARRAY_COLUMNS and isinstance(val, list):
                val = " | ".join(str(v) for v in val)
            elif val is None:
                val = ""
            row.append(str(val))
        writer.writerow(row)

# Print stats
scores = [c.get("icp_fit_score", 0) for c in deduped]
high = sum(1 for s in scores if s >= 8)
med = sum(1 for s in scores if 5 <= s < 8)
low = sum(1 for s in scores if s < 5)
print(f"Wrote {len(deduped)} leads to {OUTPUT_FILE}")
print(f"Deduplicated {len(companies) - len(deduped)} duplicates from {len(companies)} total")
print(f"ICP scores: 8-10: {high} | 5-7: {med} | 1-4: {low}")

# Cleanup batch files
for f in glob.glob("/tmp/exa_leads_batch_*.json"):
    os.remove(f)
```

Adapt COLUMNS and ARRAY_COLUMNS to match the actual outputSchema you designed in Step 3.

## Step 6: Summary

After the CSV is written, print:

```
## Lead Generation Complete

- Total leads: {count}
- Duplicates removed: {count}
- ICP score distribution: 8-10: {N} | 5-7: {N} | 1-4: {N}
- Exa deep calls made: {count}
- Batch subagents used: {count}
- Output: {filename}
```

## Handling Partial Failures

- If a subagent reports failed calls, note it but continue with other batches — the overshoot on micro-verticals absorbs occasional losses
- If more than 50% of total calls fail, inform the user and suggest trying with fewer leads
- Never retry the exact same query — adjust the micro-vertical wording instead
- If the Python script fails to parse a batch file, it logs a warning and skips it

## Performance Notes

- Each `web_search_advanced_exa` call takes 4-12s (longer with complex schemas). Multiple subagents run in parallel without issues.
- **Yield per call: ~35-48 companies** (avg ~42) when requesting 50. Budget micro-verticals accordingly — use `ceil(requested_leads / 35)` to overshoot.
- For large lists (500+), confirm with the user before starting: "This will require ~{N} Exa deep search calls in {M} batch subagents. Proceed?"
- For very large runs (10+ subagents), launch in waves of ~6.
- Always use `type: "deep"` — `deep-reasoning` is for single complex research questions, not bulk lead gen
- Subagent architecture keeps main context under ~50K tokens even for 1000+ lead runs
