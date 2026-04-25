import json
import threading
from pathlib import Path

from lead_aggregates.atomic import atomic_write_json
from lead_aggregates.store import AggregatesStore


def test_run_locked_serializes_incrementers(tmp_path: Path) -> None:
    full = tmp_path / "fullJSONs"
    js = tmp_path / "jsons"
    js.mkdir(parents=True)
    counter = full / "_counter.json"
    atomic_write_json(counter, {"n": 0})
    store = AggregatesStore(full, js, lock_timeout=60)

    def bump() -> None:
        def inc() -> None:
            d = json.loads(counter.read_text(encoding="utf-8"))
            d["n"] = int(d["n"]) + 1
            atomic_write_json(counter, d)

        store.run_locked(inc)

    threads = [threading.Thread(target=bump) for _ in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert json.loads(counter.read_text(encoding="utf-8"))["n"] == 10
