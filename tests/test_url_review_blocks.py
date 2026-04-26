from pathlib import Path

from url_review.blocks import load_blocked_netlocs


def test_load_blocked_netlocs_from_registry_and_all_enriched(tmp_path: Path) -> None:
    fulljsons = tmp_path / "fullJSONs"
    fulljsons.mkdir()
    (fulljsons / "url_registry.json").write_text(
        '{"version":1,"urls":{"https://www.foo.com/bar":{"status":"enriched"}}}',
        encoding="utf-8",
    )
    (fulljsons / "all_enriched_leads.json").write_text(
        '{"runs":[{"source_file":"jsons/x.json","target_url":"https://bar.com/rooms","other":1}]}',
        encoding="utf-8",
    )

    blocked = load_blocked_netlocs(fulljsons)
    assert "foo.com" in blocked
    assert "www.foo.com" in blocked
    assert "bar.com" in blocked
    assert "www.bar.com" in blocked


def test_load_blocked_netlocs_empty_if_missing(tmp_path: Path) -> None:
    fulljsons = tmp_path / "fulljsons_missing"
    assert load_blocked_netlocs(fulljsons) == frozenset()

