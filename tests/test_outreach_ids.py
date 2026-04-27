from __future__ import annotations

from outreach.ids import compute_outreach_id, primary_delivery_email, target_url_from_intimate_row


def test_primary_delivery_email_prefers_named() -> None:
    row = {"email": "info@hotel.com", "email2": "jane.smith@hotel.com"}
    assert primary_delivery_email(row) == "jane.smith@hotel.com"


def test_compute_outreach_id_stable() -> None:
    a = compute_outreach_id("jane@hotel.com", "https://WWW.Hotel.COM/foo/")
    b = compute_outreach_id("jane@hotel.com", "https://www.hotel.com/foo")
    assert a == b
    assert a.startswith("oh_")


def test_target_url_from_phase1() -> None:
    row = {"phase1_research": {"target_url": "https://ex.com/"}}
    assert target_url_from_intimate_row(row) == "https://ex.com/"


def test_target_url_from_intimate_row_prefers_top_level_target_url() -> None:
    row = {"target_url": "https://top.example/", "phase1_research": {"target_url": "https://phase1.example/"}}
    assert target_url_from_intimate_row(row) == "https://top.example/"
