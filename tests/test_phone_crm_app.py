from __future__ import annotations

from dataclasses import replace
from contextlib import contextmanager

from fastapi.testclient import TestClient
from starlette.requests import Request

from phone_crm.app import app
from phone_crm.auth import require_user
from phone_crm.config import Settings
from phone_crm.models import ContactRow, CrmSummary
from phone_crm.contact_display import build_contact_display, payload_remainder
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
    assert 'id="crm-main"' in response.text
    assert 'hx-get="/crm?phones_only=true"' in response.text


def test_crm_route_renders_html(monkeypatch) -> None:
    contact = _setup_mocks(monkeypatch)
    client = TestClient(app)
    response = client.get("/crm")
    _teardown_mocks()
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert '<aside class="crm-left panel">' in response.text
    assert '<section class="crm-right panel">' in response.text
    assert "Choose a contact from the hotel list." not in response.text
    assert contact.full_name in response.text


def test_crm_route_auto_opens_first_contact_by_default(monkeypatch) -> None:
    _setup_mocks(monkeypatch)
    client = TestClient(app)
    response = client.get("/crm")
    _teardown_mocks()
    assert response.status_code == 200
    assert "occ-1" in response.text


def test_crm_route_shows_detail_mark_action_for_selected(monkeypatch) -> None:
    contact = _sample_contact()
    _setup_mocks(monkeypatch)
    monkeypatch.setattr(crm_app, "fetch_contact", lambda _conn, _id: contact)
    client = TestClient(app)
    response = client.get("/crm?selected=occ-1")
    _teardown_mocks()
    assert response.status_code == 200
    assert 'class="btn btn-primary btn-done ghost-btn ghost-btn-small mark-btn-done"' in response.text
    assert "Undo review" not in response.text
    assert "Undo skip" not in response.text
    assert 'class="btn btn-secondary btn-skip ghost-btn ghost-btn-small mark-btn-skip"' in response.text
    assert 'class="contact-block contact-detail-card"' in response.text
    assert f"<h2>{contact.full_name}</h2>" in response.text


def test_crm_route_shows_empty_state_when_no_contacts(monkeypatch) -> None:
    _setup_mocks(monkeypatch)

    def fake_fetch_contacts(_conn, phones_only: bool) -> list[ContactRow]:
        return []

    client = TestClient(app)
    monkeypatch.setattr(crm_app, "fetch_contacts", fake_fetch_contacts)
    response = client.get("/crm")
    _teardown_mocks()
    assert response.status_code == 200
    assert "Choose a contact from the hotel list." in response.text


def test_crm_route_shows_detail_undo_action_for_non_pending_selected(monkeypatch) -> None:
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
    assert "Undo review" in response.text
    assert "mark-btn-done" not in response.text
    assert f"<h2>{contact.full_name}</h2>" in response.text


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
    assert contact.occurrence_id in response.text
    assert "Call channels" in response.text
    assert contact.primary_handle in response.text
    assert contact.hotel_name in response.text
    assert contact.linkedin_url in response.text
    assert contact.phone in response.text
    assert "+44 20 9999 0000" in response.text
    assert "met in person" in response.text
    assert "View full JSON" in response.text
    assert "fit_reason" in response.text or "decision_maker_score" in response.text


def test_notes_route_posts_notes_and_rerenders(monkeypatch) -> None:
    contact = _sample_enriched_contact()

    def fake_update_notes(_conn, occurrence_id: str, notes: str) -> ContactRow:
        return replace(contact, notes=notes)

    _setup_mocks(monkeypatch)
    monkeypatch.setattr(crm_app, "update_notes", fake_update_notes)
    monkeypatch.setattr(crm_app, "fetch_contacts", lambda _conn, phones_only: [replace(contact, notes="new notes")])
    monkeypatch.setattr(crm_app, "fetch_contact", lambda _conn, _id: replace(contact, notes="new notes"))

    client = TestClient(app)
    response = client.post("/contact/notes", data={"id": contact.occurrence_id, "notes": "new notes"})
    _teardown_mocks()
    assert response.status_code == 200
    assert 'class="contact-block contact-detail-card"' in response.text
    assert 'id="crm-main"' not in response.text
    assert "new notes" in response.text


def test_contact_display_builds_fields():
    contact = _sample_enriched_contact()
    display = build_contact_display(contact)
    assert display["hero_name"] == contact.full_name
    assert display["hero_title"] == contact.title
    assert display["enrichment"]["decision_score"] == contact.decision_maker_score
    assert display["evidence"][0]["source_url"] == "https://source.example"


def test_payload_remainder_keeps_extra_fields():
    payload = {"contact": {"fit_reason": "x", "evidence": [], "extra_key": "hello", "decision_maker_score": "10"}}
    contact = _sample_enriched_contact()
    contact = replace(contact, payload=payload)
    remainder = payload_remainder(contact.payload["contact"])
    assert remainder["extra_key"] == "hello"
    assert "fit_reason" not in remainder


def test_crm_route_defaults_to_phone_only_contacts(monkeypatch) -> None:
    contact = _sample_contact()
    fetch_calls: list[bool] = []
    no_phone_contact = replace(
        _sample_contact(),
        occurrence_id="occ-no-phone",
        full_name="No Phone",
        has_phone=False,
        phone="",
        phone2="",
    )

    def fake_fetch_contacts(_conn, phones_only: bool) -> list[ContactRow]:
        fetch_calls.append(phones_only)
        contacts = [no_phone_contact, contact]
        return [entry for entry in contacts if not phones_only or entry.has_phone]

    _setup_mocks(monkeypatch)
    monkeypatch.setattr(crm_app, "fetch_contacts", fake_fetch_contacts)
    client = TestClient(app)
    response = client.get("/crm")
    _teardown_mocks()
    assert response.status_code == 200
    assert fetch_calls == [True]
    assert contact.full_name in response.text
    assert no_phone_contact.full_name not in response.text


def test_contact_route_renders_html(monkeypatch) -> None:
    contact = _setup_mocks(monkeypatch)
    client = TestClient(app)
    response = client.get("/contact", params={"id": contact.occurrence_id})
    _teardown_mocks()
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert '<section class="contact-block contact-detail-card" id="contactPanel"' in response.text
    assert contact.full_name in response.text


def test_status_update_uses_strict_phone_mode(monkeypatch) -> None:
    contact = _setup_mocks(monkeypatch)
    fetch_calls: list[bool] = []
    build_groups_calls: list[bool] = []
    find_next_calls: list[tuple[str, bool]] = []

    def fake_update_notes_and_status(_conn, _occurrence_id: str, notes: str, status: str) -> ContactRow:
        return replace(contact, notes=notes, status=status)

    def fake_fetch_contacts(_conn, phones_only: bool) -> list[ContactRow]:
        fetch_calls.append(phones_only)
        return [replace(contact, status="pending")]

    def fake_build_groups(rows: list[ContactRow], phones_only: bool = False):
        build_groups_calls.append(phones_only)
        return crm_queries.build_groups(rows, phones_only=phones_only)

    def fake_find_next_contact_id(rows: list[ContactRow], current_id: str | None, phones_only: bool = False):
        find_next_calls.append((current_id, phones_only))
        return crm_queries.find_next_contact_id(rows, current_id, phones_only=phones_only)

    monkeypatch.setattr(crm_app, "update_notes_and_status", fake_update_notes_and_status)
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

    _setup_mocks(monkeypatch)

    def fake_update_notes_and_status(_conn, _occurrence_id: str, notes: str, status: str) -> ContactRow:
        return replace(contact, notes=notes, status=status)

    def fake_fetch_contacts(_conn, phones_only: bool) -> list[ContactRow]:
        fetch_calls.append(phones_only)
        return [replace(contact, status="pending")]

    def fake_build_groups(rows: list[ContactRow], phones_only: bool = False):
        build_groups_calls.append(phones_only)
        return crm_queries.build_groups(rows, phones_only=phones_only)

    def fake_find_next_contact_id(rows: list[ContactRow], current_id: str | None, phones_only: bool = False):
        find_next_calls.append((current_id, phones_only))
        return crm_queries.find_next_contact_id(rows, current_id, phones_only=phones_only)

    monkeypatch.setattr(crm_app, "update_notes_and_status", fake_update_notes_and_status)
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
        selected_contact=None,
    ).status_code == 200


def test_status_update_updates_notes_and_status_together(monkeypatch) -> None:
    contact = _setup_mocks(monkeypatch)
    update_payload: dict[str, str] = {}
    status_calls: list[tuple[str, str, str]] = []

    def fake_update_notes_and_status(_conn, occurrence_id: str, notes: str, status: str) -> ContactRow:
        status_calls.append((occurrence_id, notes, status))
        update_payload["notes"] = notes
        update_payload["status"] = status
        return replace(contact, notes=notes, status=status)

    def fake_fetch_contacts(_conn, phones_only: bool) -> list[ContactRow]:
        notes = update_payload.get("notes", contact.notes)
        status = update_payload.get("status", contact.status)
        return [replace(contact, notes=notes, status=status)]

    monkeypatch.setattr(crm_app, "update_notes_and_status", fake_update_notes_and_status)
    monkeypatch.setattr(crm_app, "fetch_contacts", fake_fetch_contacts)
    client = TestClient(app)

    for status in ("done", "skipped"):
        status_calls.clear()
        update_payload.clear()
        response = client.post(
            "/contact/status",
            data={"id": contact.occurrence_id, "status": status, "notes": "left voicemail", "phones_only": "true"},
        )
        assert response.status_code == 200
        assert status_calls == [(contact.occurrence_id, "left voicemail", status)]
        assert "left voicemail" in response.text
        if status == "done":
            assert "Undo review" in response.text
        else:
            assert "Undo skip" in response.text

    _teardown_mocks()


def test_status_update_advances_within_same_hotel(monkeypatch) -> None:
    current = replace(
        _sample_contact(),
        occurrence_id="occ-1",
        full_name="Current",
        hotel_name="Hotel Alpha",
        has_phone=True,
    )
    next_contact = replace(
        _sample_contact(),
        occurrence_id="occ-2",
        full_name="Next in Hotel",
        hotel_name="Hotel Alpha",
        has_phone=True,
    )
    other_hotel = replace(
        _sample_contact(),
        occurrence_id="occ-3",
        full_name="Other Hotel Contact",
        hotel_name="Hotel Beta",
        has_phone=True,
    )

    _setup_mocks(monkeypatch)

    def fake_update_notes_and_status(_conn, occurrence_id: str, notes: str, status: str) -> ContactRow:
        return replace(current, status=status, notes=notes)

    def fake_fetch_contacts(_conn, phones_only: bool) -> list[ContactRow]:
        return [replace(current, status="pending"), replace(next_contact, status="pending"), replace(other_hotel, status="pending")]

    monkeypatch.setattr(crm_app, "update_notes_and_status", fake_update_notes_and_status)
    monkeypatch.setattr(crm_app, "fetch_contacts", fake_fetch_contacts)
    client = TestClient(app)
    response = client.post(
        "/contact/status",
        data={"id": current.occurrence_id, "status": "done", "notes": "spoke", "phones_only": "true"},
    )
    _teardown_mocks()

    assert response.status_code == 200
    assert '<h2>Next in Hotel</h2>' in response.text
    assert 'data-contact-id="occ-2"' in response.text


def test_status_update_rejects_invalid_status(monkeypatch) -> None:
    contact = _setup_mocks(monkeypatch)
    status_calls: list[tuple[str, str, str]] = []

    def fake_update_notes_and_status(_conn, occurrence_id: str, notes: str, status: str) -> ContactRow:
        status_calls.append((occurrence_id, notes, status))
        return replace(contact, notes=notes, status=status)

    monkeypatch.setattr(crm_app, "update_notes_and_status", fake_update_notes_and_status)
    client = TestClient(app)
    response = client.post(
        "/contact/status",
        data={"id": contact.occurrence_id, "status": "invalid", "notes": "nope", "phones_only": "true"},
    )
    _teardown_mocks()

    assert response.status_code == 400
    assert "Invalid status" in response.text
    assert status_calls == []
