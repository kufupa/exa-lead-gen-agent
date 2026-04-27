from __future__ import annotations

from contextlib import contextmanager

from fastapi.testclient import TestClient
from starlette.requests import Request

from phone_crm.app import app
from phone_crm.auth import require_user
from phone_crm.config import Settings
from phone_crm.models import ContactRow, CrmSummary
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


def test_contact_route_renders_html(monkeypatch) -> None:
    contact = _setup_mocks(monkeypatch)
    client = TestClient(app)
    response = client.get("/contact", params={"id": contact.occurrence_id})
    _teardown_mocks()
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert contact.full_name in response.text


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
