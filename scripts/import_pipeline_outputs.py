#!/usr/bin/env python3
"""Import latest v4 `outputs/pipeline/*/pipeline_result.json` into `jsons/*.enriched.json` and rebuild fullJSONs."""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from lead_aggregates.store import AggregatesStore  # noqa: E402
from lead_aggregates.urls import canonical_hotel_url  # noqa: E402
from pipeline.legacy_export import load_pipeline_ui_json, write_pipeline_enriched_json  # noqa: E402


def _run_timestamp_key(path: Path) -> str:
    return path.parent.name.split("__", 1)[0]


def select_latest_pipeline_results(outputs_dir: Path) -> list[Path]:
    latest: dict[str, Path] = {}
    for path in sorted(outputs_dir.glob("*/pipeline_result.json")):
        ui = load_pipeline_ui_json(path)
        key = canonical_hotel_url(ui.input_url)
        prev = latest.get(key)
        if prev is None or _run_timestamp_key(path) > _run_timestamp_key(prev):
            latest[key] = path
    return sorted(latest.values(), key=lambda p: _run_timestamp_key(p))


def import_pipeline_outputs(
    outputs_dir: Path,
    jsons_dir: Path,
    fulljsons_dir: Path,
) -> list[Path]:
    written: list[Path] = []
    for result_path in select_latest_pipeline_results(outputs_dir):
        ui = load_pipeline_ui_json(result_path)
        generated_at = _run_timestamp_key(result_path)
        written.append(write_pipeline_enriched_json(ui, jsons_dir, generated_at_utc=generated_at))
    AggregatesStore(fulljsons_dir, jsons_dir).rebuild_all()
    return written


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Import latest v4 outputs/pipeline runs into jsons and rebuild fullJSONs.",
    )
    parser.add_argument("--outputs-dir", type=Path, default=Path("outputs/pipeline"))
    parser.add_argument("--jsons-dir", type=Path, default=Path("jsons"))
    parser.add_argument("--fulljsons-dir", type=Path, default=Path("fullJSONs"))
    args = parser.parse_args()

    written = import_pipeline_outputs(args.outputs_dir, args.jsons_dir, args.fulljsons_dir)
    print(f"imported={len(written)} jsons_dir={args.jsons_dir} fulljsons_dir={args.fulljsons_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
