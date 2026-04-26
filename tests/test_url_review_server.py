from pathlib import Path

from scripts.url_review_server import CandidateSnapshot


def test_snapshot_filters_blocked_domains(tmp_path: Path) -> None:
    csv_path = tmp_path / "leads.csv"
    csv_path.write_text(
        "company_name,website,hotel_type,estimated_rooms,london_area,icp_fit_level,icp_fit_reasoning\n"
        "Hotel A,https://www.skip.com/path,boutique,100,West,High,Reason\n"
        "Hotel B,https://keep.com/path,boutique,80,West,High,Reason\n",
        encoding="utf-8",
    )

    fulljsons = tmp_path / "fullJSONs"
    fulljsons.mkdir()
    (fulljsons / "url_registry.json").write_text(
        '{"version":1,"urls":{"https://www.skip.com/home":{"status":"enriched"}}}',
        encoding="utf-8",
    )
    yes_path = tmp_path / "yes.txt"
    yes_path.write_text("other.com\n", encoding="utf-8")
    no_path = tmp_path / "no.txt"

    snapshot = CandidateSnapshot(
        csv_path=csv_path,
        fulljsons_dir=fulljsons,
        yes_path=yes_path,
        no_path=no_path,
    )
    payload = snapshot.payload()
    assert payload["remaining"] == 1
    assert payload["rows"][0]["domain"] == "keep.com"
    assert payload["columns"] == [
        "company_name",
        "website",
        "hotel_type",
        "estimated_rooms",
        "london_area",
        "icp_fit_level",
        "icp_fit_reasoning",
        "domain",
    ]


def test_snapshot_undo_last_removes_written_decision(tmp_path: Path) -> None:
    csv_path = tmp_path / "leads.csv"
    csv_path.write_text(
        "company_name,website,hotel_type,estimated_rooms,london_area,icp_fit_level,icp_fit_reasoning\n"
        "Hotel A,https://www.example.com/path,boutique,100,West,High,Reason\n"
        "Hotel B,https://skip.com/path,boutique,80,West,High,Reason\n",
        encoding="utf-8",
    )
    fulljsons = tmp_path / "fullJSONs"
    fulljsons.mkdir()
    yes_path = tmp_path / "yes.txt"
    no_path = tmp_path / "no.txt"

    snapshot = CandidateSnapshot(
        csv_path=csv_path,
        fulljsons_dir=fulljsons,
        yes_path=yes_path,
        no_path=no_path,
    )
    rows = snapshot.rows()
    assert rows[0]["domain"] == "example.com"
    assert snapshot.mark_decision(rows[0], "yes")
    assert yes_path.read_text(encoding="utf-8").strip() == "example.com"

    undone = snapshot.undo_last()
    assert undone is not None
    assert undone["domain"] == "example.com"
    assert undone["decision"] == "yes"
    assert yes_path.read_text(encoding="utf-8").strip() == ""
    refreshed = snapshot.rows()
    assert refreshed[0]["domain"] == "example.com"

