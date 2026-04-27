from __future__ import annotations

from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

from fastapi import Depends, FastAPI, Form, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from phone_crm.auth import require_user
from phone_crm.config import load_settings
from phone_crm.db import open_connection
from phone_crm.models import CrmSummary
from phone_crm.queries import (
    build_groups,
    build_summary,
    fetch_contact,
    fetch_contacts,
    find_next_contact_id,
    update_notes,
    update_status,
)

app = FastAPI()
BASE_DIR = Path(__file__).resolve().parent
TEMPLATE_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))
templates.env.filters["crm_url"] = lambda value: quote_plus((value or ""), safe="")


ALLOWED_STATUS = {"pending", "done", "skipped"}


def _pick_selected(groups: list, selected: str | None) -> str | None:
    if not groups:
        return None
    if selected:
        for group in groups:
            for contact in group.contacts:
                if contact.occurrence_id == selected:
                    return selected
    for group in groups:
        for contact in group.contacts:
            if contact.status == "pending" and contact.has_contact_route:
                return contact.occurrence_id
    return groups[0].contacts[0].occurrence_id if groups[0].contacts else None


def _render_main(request: Request, groups, summary: CrmSummary, phones_only: bool, selected_id: str | None):
    if selected_id:
        with open_connection(load_settings()) as conn:
            selected = fetch_contact(conn, selected_id)
    else:
        selected = None
    return templates.TemplateResponse(
        "_crm_main.html",
        {
            "request": request,
            "groups": groups,
            "summary": summary,
            "phones_only": phones_only,
            "selected_id": selected_id,
            "selected": selected,
        },
    )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request, _user: str = Depends(require_user)) -> HTMLResponse:
    return templates.TemplateResponse(
        "index.html",
        {"request": request},
    )


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/crm", response_class=HTMLResponse)
async def crm_list(
    request: Request,
    phones_only: bool = Query(default=False),
    selected: Optional[str] = Query(default=None),
    _user: str = Depends(require_user),
) -> HTMLResponse:
    with open_connection(load_settings()) as conn:
        contacts = fetch_contacts(conn, phones_only=phones_only)
    groups = build_groups(contacts)
    summary = build_summary(contacts)
    selected_id = _pick_selected(groups, selected)
    return _render_main(request, groups, summary, phones_only, selected_id)


@app.get("/contact", response_class=HTMLResponse)
async def contact_detail(
    request: Request,
    id: str = Query(...),
    _user: str = Depends(require_user),
) -> HTMLResponse:
    with open_connection(load_settings()) as conn:
        contact = fetch_contact(conn, id)
    return templates.TemplateResponse(
        "_contact_detail.html",
        {"request": request, "contact": contact},
    )


@app.post("/contact/notes", response_class=HTMLResponse)
async def save_notes(
    request: Request,
    id: str = Form(...),
    notes: str = Form(""),
    _user: str = Depends(require_user),
) -> HTMLResponse:
    with open_connection(load_settings()) as conn:
        updated = update_notes(conn, id, notes)
    return templates.TemplateResponse(
        "_contact_detail.html",
        {"request": request, "contact": updated},
    )


@app.post("/contact/status", response_class=HTMLResponse)
async def set_status(
    request: Request,
    id: str = Form(...),
    status: str = Form(...),
    phones_only: bool = Form(False),
    _user: str = Depends(require_user),
) -> HTMLResponse:
    if status not in ALLOWED_STATUS:
        return templates.TemplateResponse(
            "_crm_main.html",
            {
                "request": request,
                "groups": [],
                "summary": CrmSummary(total=0, pending=0, done=0, skipped=0),
                "phones_only": phones_only,
                "selected_id": None,
                "selected": None,
                "error": "Invalid status",
            },
            status_code=400,
        )

    with open_connection(load_settings()) as conn:
        updated = update_status(conn, id, status)
        contacts = fetch_contacts(conn, phones_only=phones_only)
    groups = build_groups(contacts)
    summary = build_summary(contacts)
    selected_id = id
    if updated and updated.status != "pending":
        selected_id = find_next_contact_id(contacts, id)
    if not selected_id:
        selected_id = _pick_selected(groups, None)

    with open_connection(load_settings()) as conn:
        selected = fetch_contact(conn, selected_id) if selected_id else None
    return templates.TemplateResponse(
        "_crm_main.html",
        {
            "request": request,
            "groups": groups,
            "summary": summary,
            "phones_only": phones_only,
            "selected_id": selected_id,
            "selected": selected,
        },
    )
