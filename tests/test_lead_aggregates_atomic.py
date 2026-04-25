import json
from pathlib import Path

from lead_aggregates.atomic import atomic_write_json


def test_atomic_write_json_creates_file(tmp_path: Path) -> None:
    p = tmp_path / "nested" / "a.json"
    atomic_write_json(p, {"x": 1})
    assert p.exists()
    assert json.loads(p.read_text(encoding="utf-8")) == {"x": 1}
