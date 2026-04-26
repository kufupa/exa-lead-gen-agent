# Legacy hotel pipeline (archived)

Scripts here are **frozen / deprecated** in favour of [`pipeline/`](../pipeline/) (`python -m pipeline run <url>`).

| Script | Role |
|--------|------|
| `hotel_decision_maker_research.py` | Grok multi-agent research → `jsons/` + CSV |
| `hotel_contact_enrichment.py` | Thin CLI → `contact_enrichment` package |
| `hotel_batch_pipeline.py` | Multi-URL orchestrator + `lead_aggregates` |

**Not moved here (shared):** `contact_enrichment/`, `linkedin_enrich/`, `scripts/linkedin_exa_enrich.py`, `lead_aggregates/`, `pipeline_metrics.py`.

Repo root keeps **thin shims** (`hotel_*.py`) so existing imports like `from hotel_decision_maker_research import Contact` and subprocess paths keep working.
