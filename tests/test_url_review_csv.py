from pathlib import Path

from url_review.csv_model import domain_from_website, iter_candidates


def test_domain_from_website_normalizes() -> None:
    assert domain_from_website("https://www.Example.COM/rooms/") == "example.com"
    assert domain_from_website("example.com/path") == "example.com"


def test_iter_candidates_dedupes_and_filters(tmp_path: Path) -> None:
    csv_path = tmp_path / "leads.csv"
    csv_path.write_text(
        "company_name,website,hotel_type,estimated_rooms,london_area,icp_fit_level,icp_fit_reasoning\n"
        "Hotel A,https://www.dup.com/a,boutique,50,Area,High,Reason\n"
        "Hotel B,https://dup.com/b,boutique,50,Area,High,Reason\n"
        "Hotel C,,boutique,50,Area,High,Reason\n"
        "Hotel D,https://skip.me/,boutique,50,Area,High,Reason\n",
        encoding="utf-8",
    )

    blocked = frozenset({"skip.me"})
    rows = iter_candidates(csv_path, blocked)

    assert len(rows) == 1
    assert rows[0]["company_name"] == "Hotel A"
    assert rows[0]["website"] == "https://www.dup.com/a"
    assert rows[0]["domain"] == "dup.com"

