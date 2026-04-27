from __future__ import annotations

from dataclasses import replace
from contextlib import contextmanager

from fastapi.testclient import TestClient
from starlette.requests import Request

from phone_crm.app import app
from phone_crm.auth import require_user
from phone_crm.config import Settings
from phone_crm.models import ContactRow, CrmSummary
from phone_crm import queries as crm_queries
import phone_crm.app as crm_app


def _sample_contact() -> ContactRow:
    return ContactRow(
        occurrence_id="occ-1",
        source_enriched_json="jsons/sample.enriched.json",
        target_url="https://hotel.example",
        hotel_name="Hotel Example",
        full_name="Alex Morgan",
        title="Sales Director",
        primary_handle="@alex",
        phone="+44 20 1234",
        phone2="",
        email="alex@hotel.example",
        email2="",
        linkedin_url="https://linkedin.com/in/alex",
        x_handle="",
        other_contact_detail="",
        decision_maker_score="high",
        intimacy_grade="medium",
        has_phone=True,
        has_email=True,
        has_contact_route=True,
        status="pending",
        notes="",
        payload={"contact": {"evidence": []}},
    )


def _sample_enriched_contact() -> ContactRow:
    return ContactRow(
        occurrence_id="occ-rich",
        source_enriched_json="jsons/sample.enriched.json",
        target_url="https://hotel.example",
        hotel_name="Example Hotel",
        full_name="Alex Morgan",
        title="Sales Director",
        primary_handle="@alex",
        phone="+44 20 1234 5678",
        phone2="+44 20 9999 0000",
        email="alex@hotel.example",
        email2="a.morgan@hotel.example",
        linkedin_url="https://www.linkedin.com/in/alex",
        x_handle="@alex",
        other_contact_detail="Conference room direct line",
        decision_maker_score="high",
        intimacy_grade="medium",
        has_phone=True,
        has_email=True,
        has_contact_route=True,
        status="pending",
        notes="existing note",
        payload={
            "contact": {
                "evidence": [
                    {"source_url": "https://source.example", "quote_or_fact": "met in person"},
                ],
                "fit_reason": "decision maker with direct phone route",
            }
        },
    )


@contextmanager
def _dummy_open_connection(_settings):
    yield object()


def _setup_mocks(monkeypatch):
    contact = _sample_contact()
    monkeypatch.setattr(
        crm_app,
        "load_settings",
        lambda: Settings(
            database_url="postgresql://example",
            crm_username="user",
            crm_password="pass",
            crm_json_path="fullJSONs/all_enriched_leads.json",
        ),
    )
    monkeypatch.setattr(crm_app, "open_connection", _dummy_open_connection)
    monkeypatch.setattr(crm_app, "fetch_contacts", lambda _conn, phones_only=False: [contact])
    monkeypatch.setattr(crm_app, "fetch_contact", lambda _conn, _id: contact)
    app.dependency_overrides[require_user] = lambda: "test-user"
    return contact


def _teardown_mocks():
    app.dependency_overrides.clear()


def test_index_route_renders_html(monkeypatch) -> None:
    _setup_mocks(monkeypatch)
    client = TestClient(app)
    response = client.get("/")
    _teardown_mocks()
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]


def test_crm_route_renders_html(monkeypatch) -> None:
    contact = _setup_mocks(monkeypatch)
    client = TestClient(app)
    response = client.get("/crm")
    _teardown_mocks()
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert contact.full_name in response.text


def test_crm_route_shows_header_mark_actions_for_selected(monkeypatch) -> None:
    contact = _sample_contact()
    _setup_mocks(monkeypatch)
    monkeypatch.setattr(crm_app, "fetch_contact", lambda _conn, _id: contact)
    client = TestClient(app)
    response = client.get("/crm?selected=occ-1")
    _teardown_mocks()
    assert response.status_code == 200
    assert "Mark done" in response.text
    assert "Skip" in response.text


def test_crm_route_shows_header_undo_for_non_pending_selected(monkeypatch) -> None:
    contact = replace(_sample_contact(), status="done")

    def fake_fetch_contacts(_conn, phones_only: bool) -> list[ContactRow]:
        return [contact]

    def fake_fetch_contact(_conn, _id: str) -> ContactRow:
        return contact

    _setup_mocks(monkeypatch)
    monkeypatch.setattr(crm_app, "fetch_contacts", fake_fetch_contacts)
    monkeypatch.setattr(crm_app, "fetch_contact", fake_fetch_contact)

    client = TestClient(app)
    response = client.get("/crm?selected=occ-1")
    _teardown_mocks()
    assert response.status_code == 200
    assert "Undo" in response.text
    assert "Mark done" not in response.text


def test_crm_route_shows_rich_contact_details(monkeypatch) -> None:
    contact = _sample_enriched_contact()

    def fake_fetch_contacts(_conn, phones_only: bool) -> list[ContactRow]:
        return [contact]

    def fake_fetch_contact(_conn, _id: str) -> ContactRow:
        return contact

    _setup_mocks(monkeypatch)
    monkeypatch.setattr(crm_app, "fetch_contacts", fake_fetch_contacts)
    monkeypatch.setattr(crm_app, "fetch_contact", fake_fetch_contact)

    client = TestClient(app)
    response = client.get("/crm?selected=occ-rich")
    _teardown_mocks()
    assert response.status_code == 200
    assert contact.hotel_name in response.text
    assert contact.linkedin_url in response.text
    assert contact.phone in response.text
    assert "+44 20 9999 0000" in response.text
    assert "met in person" in response.text
    assert "fit_reason" in response.text or "decision_maker_score" in response.text


def test_notes_route_posts_notes_and_rerenders(monkeypatch) -> None:
    contact = _sample_enriched_contact()

    def fake_update_notes(_conn, occurrence_id: str, notes: str) -> ContactRow:
        return replace(contact, notes=notes)

    _setup_mocks(monkeypatch)
    monkeypatch.setattr(crm_app, "update_notes", fake_update_notes)
    monkeypatch.setattr(crm_app, "fetch_contact", lambda _conn, _id: contact)

    client = TestClient(app)
    response = client.post("/contact/notes", data={"id": contact.occurrence_id, "notes": "new notes"})
    _teardown_mocks()
    assert response.status_code == 200
    assert "new notes" in response.text


def test_crm_main_renders_phone_only_rows(monkeypatch) -> None:
    contact = _sample_contact()
    fetch_calls: list[bool] = []

    def fake_fetch_contacts(_conn, phones_only: bool) -> list[ContactRow]:
        fetch_calls.append(phones_only)
        return [contact]

    _setup_mocks(monkeypatch)
    monkeypatch.setattr(crm_app, "fetch_contacts", fake_fetch_contacts)
    client = TestClient(app)
    response = client.get("/crm")
    _teardown_mocks()
    assert response.status_code == 200
    assert fetch_calls == [True]
    assert contact.full_name in response.text


def test_contact_route_renders_html(monkeypatch) -> None:
    contact = _setup_mocks(monkeypatch)
    client = TestClient(app)
    response = client.get("/contact", params={"id": contact.occurrence_id})
    _teardown_mocks()
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert contact.full_name in response.text


def test_status_update_uses_strict_phone_mode(monkeypatch) -> None:
    contact = _setup_mocks(monkeypatch)
    fetch_calls: list[bool] = []
    build_groups_calls: list[bool] = []
    find_next_calls: list[tuple[str, bool]] = []

    def fake_update_status(_conn, _occurrence_id: str, status: str) -> ContactRow:
        return replace(contact, status=status)

    def fake_fetch_contacts(_conn, phones_only: bool) -> list[ContactRow]:
        fetch_calls.append(phones_only)
        return [replace(contact, status="pending")]

    def fake_build_groups(rows: list[ContactRow], phones_only: bool = False):
        build_groups_calls.append(phones_only)
        return crm_queries.build_groups(rows, phones_only=phones_only)

    def fake_find_next_contact_id(rows: list[ContactRow], current_id: str | None, phones_only: bool = False):
        find_next_calls.append((current_id, phones_only))
        return crm_queries.find_next_contact_id(rows, current_id, phones_only=phones_only)

    monkeypatch.setattr(crm_app, "update_status", fake_update_status)
    monkeypatch.setattr(crm_app, "fetch_contacts", fake_fetch_contacts)
    monkeypatch.setattr(crm_app, "build_groups", fake_build_groups)
    monkeypatch.setattr(crm_app, "find_next_contact_id", fake_find_next_contact_id)

    client = TestClient(app)
    response = client.post(
        "/contact/status",
        data={"id": contact.occurrence_id, "status": "done"},
    )
    _teardown_mocks()
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert fetch_calls == [True]
    assert build_groups_calls == [True]
    assert find_next_calls == [(contact.occurrence_id, True)]


def test_crm_route_respects_phones_only_query_param(monkeypatch) -> None:
    contact = _sample_contact()
    fetch_calls: list[bool] = []

    def fake_fetch_contacts(_conn, phones_only: bool) -> list[ContactRow]:
        fetch_calls.append(phones_only)
        return [contact]

    def fake_fetch_contact(_conn, _id: str) -> ContactRow:
        return contact

    _setup_mocks(monkeypatch)
    monkeypatch.setattr(crm_app, "fetch_contacts", fake_fetch_contacts)
    monkeypatch.setattr(crm_app, "fetch_contact", fake_fetch_contact)
    client = TestClient(app)
    response = client.get("/crm?phones_only=false")
    _teardown_mocks()
    assert response.status_code == 200
    assert fetch_calls == [False]
    assert contact.full_name in response.text


def test_status_update_preserves_non_default_phones_only(monkeypatch) -> None:
    contact = _sample_contact()
    fetch_calls: list[bool] = []
    build_groups_calls: list[bool] = []
    find_next_calls: list[tuple[str, bool]] = []

    def fake_update_status(_conn, _occurrence_id: str, status: str) -> ContactRow:
        return replace(contact, status=status)

    def fake_fetch_contacts(_conn, phones_only: bool) -> list[ContactRow]:
        fetch_calls.append(phones_only)
        return [replace(contact, status="pending")]

    def fake_build_groups(rows: list[ContactRow], phones_only: bool = False):
        build_groups_calls.append(phones_only)
        return crm_queries.build_groups(rows, phones_only=phones_only)

    def fake_find_next_contact_id(rows: list[ContactRow], current_id: str | None, phones_only: bool = False):
        find_next_calls.append((current_id, phones_only))
        return crm_queries.find_next_contact_id(rows, current_id, phones_only=phones_only)

    monkeypatch.setattr(crm_app, "update_status", fake_update_status)
    monkeypatch.setattr(crm_app, "fetch_contacts", fake_fetch_contacts)
    monkeypatch.setattr(crm_app, "build_groups", fake_build_groups)
    monkeypatch.setattr(crm_app, "find_next_contact_id", fake_find_next_contact_id)

    client = TestClient(app)
    response = client.post(
        "/contact/status",
        data={"id": contact.occurrence_id, "status": "done", "phones_only": "false"},
    )
    _teardown_mocks()
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert fetch_calls == [False]
    assert build_groups_calls == [False]
    assert find_next_calls == [(contact.occurrence_id, False)]


def test_render_main_error_template_response_works() -> None:
    scope = {
        "type": "http",
        "http_version": "1.1",
        "method": "GET",
        "path": "/crm",
        "query_string": b"",
        "headers": [],
        "client": ("127.0.0.1", 0),
        "server": ("testserver", 80),
        "scheme": "http",
        "asgi": {"version": "3.0"},
        "app": app,
    }
    request = Request(scope)
    response = crm_app._render_main_error(
        request=request,
        phones_only=False,
        error="forced error",
    )
    assert response.status_code == 500
    assert "forced error" in response.body.decode()
    assert "All: 0" in response.body.decode()
    assert crm_app._render_main(
        request=request,
        groups=[],
        summary=CrmSummary(total=0, pending=0, done=0, skipped=0),
        phones_only=False,
        selected_id=None,
        selected=None,
    ).status_code == 200
