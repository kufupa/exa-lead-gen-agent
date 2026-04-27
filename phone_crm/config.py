from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str
    crm_username: str
    crm_password: str
    crm_json_path: str


def load_settings() -> Settings:
    database_url = (os.environ.get("DATABASE_URL") or "").strip()
    crm_password = (os.environ.get("CRM_PASSWORD") or "").strip()
    if not database_url:
        raise RuntimeError("DATABASE_URL is required")
    if not crm_password:
        raise RuntimeError("CRM_PASSWORD is required")
    crm_username = (os.environ.get("CRM_USERNAME") or "admin").strip() or "admin"
    crm_json_path = (os.environ.get("CRM_JSON_PATH") or "fullJSONs/all_enriched_leads.json").strip()
    if not crm_json_path:
        crm_json_path = "fullJSONs/all_enriched_leads.json"
    return Settings(
        database_url=database_url,
        crm_username=crm_username,
        crm_password=crm_password,
        crm_json_path=crm_json_path,
    )
