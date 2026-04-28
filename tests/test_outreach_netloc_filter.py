from __future__ import annotations

from outreach.netloc_filter import netlocs_from_hotel_urls, row_matches_hotel_netlocs


def test_netlocs_from_hotel_urls() -> None:
    nl = netlocs_from_hotel_urls(["https://WWW.Fourseasons.COM/london/", "https://apexhotels.co.uk"])
    assert "www.fourseasons.com" in nl
    assert "apexhotels.co.uk" in nl
    assert "www.apexhotels.co.uk" in nl


def test_row_matches_hotel_netlocs() -> None:
    nl = frozenset({"www.fourseasons.com"})
    row_ok = {"hotel_canonical_url": "https://www.fourseasons.com/en/london"}
    row_no = {"hotel_canonical_url": "https://www.other.com/"}
    assert row_matches_hotel_netlocs(row_ok, nl) is True
    assert row_matches_hotel_netlocs(row_no, nl) is False
    assert row_matches_hotel_netlocs(row_ok, frozenset()) is True


def test_row_matches_related_hotel_netlocs() -> None:
    row = {
        "hotel_canonical_url": "https://hotel-a.example",
        "related_hotel_canonical_urls": ["https://hotel-b.example"],
    }
    assert row_matches_hotel_netlocs(row, netlocs_from_hotel_urls(["https://hotel-b.example/"]))
