# Rules

## Must Always
- Research the target company's ICP before generating leads
- Confirm the ICP, exclusions, lead count, and enrichment columns with the user before launching
- Use `structuredOutput: true`, `numResults: 50`, `highlightMaxCharacters: 1`, and `type: "deep"` on every Exa call
- Include word limits on every string field in outputSchema
- Keep total outputSchema properties under 10 (including the wrapper array)
- Use flat primitives only inside array items — no nested objects
- Launch batch subagents for parallel execution — never process raw lead data in the main context
- Overshoot micro-verticals using `ceil(requested_leads / 35)` to absorb dedupe losses
- Deduplicate by normalized company name, keeping the higher ICP score
- Sort final output by icp_fit_score descending
- Clean up /tmp batch files after CSV compilation

## Must Never
- Return raw company JSON data in the main agent context
- Use `deep-reasoning` type — only `deep` for bulk lead gen
- Skip the ICP confirmation step
- Launch more than 6 batch subagents simultaneously
- Retry failed queries with the same wording — adjust the micro-vertical instead
- Leave string fields without word/character limits in outputSchema
- Use web search, file reads, or other Exa tools — only `web_search_advanced_exa`

## Output Constraints
- CSV output with proper quoting/escaping via csv.writer
- Array fields joined with " | " delimiter
- Filename format: `{company}_leads_{YYYY-MM-DD}.csv`
- Summary includes: total leads, dupes removed, score distribution, calls made, batches used

## Interaction Boundaries
- Only use Exa MCP web_search_advanced_exa, Agent tool, Write tool, and Bash for Python
- Stop and report if Exa MCP is not available
- Confirm with user before starting runs requiring 500+ leads
