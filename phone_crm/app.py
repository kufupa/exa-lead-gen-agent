from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional
from urllib.parse import quote_plus

from fastapi import Depends, FastAPI, Form, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from phone_crm.auth import require_user
from phone_crm.config import load_settings
from phone_crm.db import open_connection
from phone_crm.models import CrmSummary
from phone_crm.contact_display import build_contact_display
from phone_crm.queries import (
    build_groups,
    build_summary,
    fetch_contact,
    fetch_contacts,
    filter_contacts_by_search,
    find_next_contact_id,
    update_notes,
    update_notes_and_status,
)

app = FastAPI()
BASE_DIR = Path(__file__).resolve().parent
TEMPLATE_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"
logger = logging.getLogger(__name__)

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))
templates.env.filters["crm_url"] = lambda value: quote_plus((value or ""), safe="")


ALLOWED_STATUS = {"pending", "done", "skipped"}


def _pick_selected(groups: list, selected: str | None, phones_only: bool = False) -> str | None:
    if not groups:
        return None
    if selected:
        for group in groups:
            for contact in group.contacts:
                if contact.occurrence_id == selected:
                    return selected
    for group in groups:
        for contact in group.contacts:
            if contact.status == "pending" and (contact.has_phone if phones_only else contact.has_contact_route):
                return contact.occurrence_id
    return groups[0].contacts[0].occurrence_id if groups[0].contacts else None


def _error_message(error: Exception) -> str:
    if isinstance(error, RuntimeError):
        return str(error)
    return "CRM backend error. Verify DATABASE_URL and Render environment configuration."


def _render_main(
    request: Request,
    groups: list,
    summary: CrmSummary,
    phones_only: bool,
    selected_id: str | None,
    selected_contact,
    search: str = "",
    error: str | None = None,
):
    return templates.TemplateResponse(
        request=request,
        name="_crm_main.html",
        context={
            "request": request,
            "groups": groups,
            "summary": summary,
            "phones_only": phones_only,
            "selected_id": selected_id,
            "contact": selected_contact,
            "search": search,
            "error": error,
            "contact_display": build_contact_display(selected_contact),
        },
    )


def _render_main_error(
    request: Request,
    phones_only: bool,
    error: str | None = None,
    search: str = "",
):
    return templates.TemplateResponse(
        request=request,
        name="_crm_main.html",
        context={
            "request": request,
            "groups": [],
            "summary": CrmSummary(total=0, pending=0, done=0, skipped=0),
            "phones_only": phones_only,
            "selected_id": None,
            "contact": None,
            "search": search,
            "error": error,
            "contact_display": build_contact_display(None),
        },
        status_code=500,
    )


def _render_contact_error(
    request: Request,
    error: str,
    phones_only: bool = True,
    search: str = "",
):
    return templates.TemplateResponse(
        request=request,
        name="_contact_detail.html",
        context={
            "request": request,
            "contact": None,
            "error": error,
            "phones_only": phones_only,
            "search": search,
            "contact_display": build_contact_display(None),
        },
        status_code=500,
    )


def _render_contact_detail(
    request: Request,
    contact,
    phones_only: bool = True,
    search: str = "",
):
    return templates.TemplateResponse(
        request=request,
        name="_contact_detail.html",
        context={
            "request": request,
            "contact": contact,
            "phones_only": phones_only,
            "search": search,
            "contact_display": build_contact_display(contact),
        },
    )


@app.get("/", response_class=HTMLResponse)
async def index(request: Request, _user: str = Depends(require_user)) -> HTMLResponse:
    return templates.TemplateResponse(request=request, name="index.html", context={"request": request})


@app.get("/health")
async def health(check_db: bool = False):
    if not check_db:
        return {"status": "ok"}
    try:
        with open_connection(load_settings()) as conn:
            with conn.cursor() as cur:
                cur.execute("select count(*) as crm_count from public.crm_contacts")
                row = cur.fetchone()
        return {"status": "ok", "crm_count": row["crm_count"] if row else 0}
    except Exception as error:
        logger.exception("Database health check failed")
        return JSONResponse(
            status_code=503,
            content={
                "status": "error",
                "error": "Cannot connect to CRM database.",
                "detail": str(error),
            },
        )


@app.get("/crm", response_class=HTMLResponse)
async def crm_list(
    request: Request,
    phones_only: bool = Query(default=True),
    selected: Optional[str] = Query(default=None),
    search: str = Query(default=""),
    _user: str = Depends(require_user),
) -> HTMLResponse:
    try:
        normalized_search = (search or "").strip()
        with open_connection(load_settings()) as conn:
            contacts = fetch_contacts(conn, phones_only=phones_only)
        filtered_contacts = filter_contacts_by_search(contacts, normalized_search)
        groups = build_groups(filtered_contacts, phones_only=phones_only)
        summary = build_summary(filtered_contacts)
        requested_selected = selected.strip() if selected else None
        selected_id = _pick_selected(groups, requested_selected, phones_only=phones_only)
        selected_contact = (
            next(
                (contact for group in groups for contact in group.contacts if contact.occurrence_id == selected_id),
                None,
            )
            if selected_id
            else None
        )
        return _render_main(
            request,
            groups,
            summary,
            phones_only,
            selected_id,
            selected_contact,
            search=normalized_search,
        )
    except Exception as error:
        logger.exception("Failed loading CRM list")
        return _render_main_error(
            request,
            phones_only,
            _error_message(error),
            search=normalized_search,
        )


@app.get("/contact", response_class=HTMLResponse)
async def contact_detail(
    request: Request,
    id: str = Query(...),
    phones_only: bool = Query(default=True),
    _user: str = Depends(require_user),
) -> HTMLResponse:
    try:
        with open_connection(load_settings()) as conn:
            contact = fetch_contact(conn, id)
        return _render_contact_detail(request, contact, phones_only=phones_only)
    except Exception as error:
        logger.exception("Failed loading contact detail")
        return _render_contact_error(request, _error_message(error))


@app.post("/contact/notes", response_class=HTMLResponse)
async def save_notes(
    request: Request,
    id: str = Form(...),
    notes: str = Form(""),
    search: str = Form(""),
    phones_only: bool = Form(True),
    _user: str = Depends(require_user),
) -> HTMLResponse:
    normalized_search = (search or "").strip()
    try:
        with open_connection(load_settings()) as conn:
            updated = update_notes(conn, id, notes)
            contacts = fetch_contacts(conn, phones_only=phones_only)
        groups = build_groups(contacts, phones_only=phones_only)
        summary = build_summary(contacts)
        requested_selected = updated.occurrence_id if updated else None
        selected_id = _pick_selected(groups, requested_selected, phones_only=phones_only)
        selected_contact = next(
            (contact for group in groups for contact in group.contacts if contact.occurrence_id == selected_id),
            updated,
        )
        if not selected_contact and selected_id:
            with open_connection(load_settings()) as conn:
                selected_contact = fetch_contact(conn, selected_id)
        return _render_contact_detail(
            request,
            selected_contact,
            phones_only=phones_only,
            search=normalized_search,
        )
    except Exception as error:
        logger.exception("Failed saving contact notes")
        return _render_contact_error(
            request,
            _error_message(error),
            phones_only=phones_only,
            search=normalized_search,
        )


@app.post("/contact/status", response_class=HTMLResponse)
async def set_status(
    request: Request,
    id: str = Form(...),
    status: str = Form(...),
    notes: str = Form(""),
    notes_mirror: str = Form(""),
    phones_only: bool = Form(True),
    search: str = Form(default=""),
    _user: str = Depends(require_user),
) -> HTMLResponse:
    normalized_search = (search or "").strip()
    if status not in ALLOWED_STATUS:
        return templates.TemplateResponse(
            request=request,
            name="_crm_main.html",
            context={
                "request": request,
                "groups": [],
                "summary": CrmSummary(total=0, pending=0, done=0, skipped=0),
                "phones_only": phones_only,
                "selected_id": None,
                "contact": None,
                "error": "Invalid status",
                "search": normalized_search,
                "contact_display": build_contact_display(None),
            },
            status_code=400,
        )

    normalized_notes = notes if notes else notes_mirror

    try:
        with open_connection(load_settings()) as conn:
            updated = update_notes_and_status(conn, id, normalized_notes, status)
            contacts = filter_contacts_by_search(
                fetch_contacts(conn, phones_only=phones_only),
                normalized_search,
            )
        groups = build_groups(contacts, phones_only=phones_only)
        summary = build_summary(contacts)
        selected_id = id
        if updated and updated.status != "pending":
            selected_id = find_next_contact_id(contacts, id, phones_only=phones_only)
        if not selected_id:
            selected_id = _pick_selected(groups, None, phones_only=phones_only)

        selected_contact = next(
            (contact for group in groups for contact in group.contacts if contact.occurrence_id == selected_id),
            None,
        )
        if not selected_contact and selected_id:
            with open_connection(load_settings()) as conn:
                selected_contact = fetch_contact(conn, selected_id)
        return _render_main(
            request,
            groups,
            summary,
            phones_only,
            selected_id,
            selected_contact,
            search=normalized_search,
        )
    except Exception as error:
        logger.exception("Failed updating contact status")
        return _render_main_error(request, phones_only, _error_message(error), search=normalized_search)
