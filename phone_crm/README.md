# Phone CRM

`phone_crm` is a minimal FastAPI + HTMX CRM for the enriched leads in
`fullJSONs/all_enriched_leads.json`.

## Quick start

1. Set environment variables:

```bash
export DATABASE_URL="postgresql://postgres:password@db.your.supabase.co:5432/postgres"
export CRM_USERNAME="admin"
export CRM_PASSWORD="change-me"
export CRM_JSON_PATH="fullJSONs/all_enriched_leads.json"
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Sync Supabase from the JSON warehouse:

```bash
python -m scripts.phone_crm_sync --json fullJSONs/all_enriched_leads.json
```

4. Run app:

```bash
uvicorn phone_crm.app:app --host 0.0.0.0 --port 8000
```

5. Open `http://localhost:8000/`, authenticate, and use the UI.

## Notes

- `sync` dry run is available:

```bash
python -m scripts.phone_crm_sync --json fullJSONs/all_enriched_leads.json --dry-run
```

- Database schema is in `phone_crm/schema.sql`; Supabase project is `phone-crm`.
