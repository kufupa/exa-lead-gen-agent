from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hotel_decision_maker_research import Contact

from contact_enrichment.gates import needs_enrichment
from contact_enrichment.identity import request_id
from contact_enrichment.merge import merge_by_request_id
from contact_enrichment.realtime import RealtimeJob, run_realtime
from contact_enrichment.types import ChannelResearchRow


def _default_concurrency() -> int:
    cpu = os.cpu_count() or 4
    return min(12, max(4, cpu))


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Deep-enrich hotel lead JSON with xAI (web + X search).")
    p.add_argument("--in-json", required=True, help="Input JSON from hotel_decision_maker_research.py (e.g. jsons/hotel_leads__....json)")
    p.add_argument("--out-json", required=True, help="Output JSON path")
    p.add_argument("--model", default="grok-4.20-reasoning", help="xAI model id")
    p.add_argument("--max-turns", type=int, default=36)
    p.add_argument("--min-direct-channels", type=float, default=1.5, help="Skip contacts at or above this score")
    p.add_argument("--concurrency", type=int, default=None, help="Realtime worker threads (default: auto)")
    p.add_argument("--overwrite", action="store_true", help="Overwrite non-empty contact fields")
    p.add_argument("--pretty", action="store_true", help="Indent JSON output")
    p.add_argument("--dry-run", action="store_true", help="Print plan only; no API calls")
    return p


def _load_lead_file(path: str) -> tuple[dict[str, Any], list[Contact]]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    if "contacts" not in raw or not isinstance(raw["contacts"], list):
        raise SystemExit("Input JSON must contain a 'contacts' array")
    contacts = [Contact.model_validate(x) for x in raw["contacts"]]
    return raw, contacts


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    raw, contacts = _load_lead_file(args.in_json)
    target_url = str(raw.get("target_url", ""))

    gated = [c for c in contacts if needs_enrichment(c, args.min_direct_channels)]
    skipped = len(contacts) - len(gated)

    conc = args.concurrency if args.concurrency is not None else _default_concurrency()

    if args.dry_run:
        print(f"in_json={args.in_json}")
        print(f"out_json={args.out_json}")
        print(f"mode=realtime model={args.model} max_turns={args.max_turns}")
        print(f"contacts_total={len(contacts)} skipped_high_score={skipped} to_enrich={len(gated)}")
        print(f"pending={len(gated)} concurrency={conc}")
        return 0

    rows: dict[str, ChannelResearchRow] = {}
    failures: list[dict[str, str]] = []

    if gated:
        rjobs = [RealtimeJob(c) for c in gated]
        new_rows, fails = run_realtime(
            rjobs,
            target_url=target_url,
            model=args.model,
            max_turns=args.max_turns,
            concurrency=conc,
        )
        rows.update(new_rows)
        failures.extend(fails)

    merged = merge_by_request_id(contacts, rows, request_id_fn=request_id, overwrite=args.overwrite)
    raw["contacts"] = [c.model_dump() for c in merged]

    started = datetime.now(timezone.utc)
    raw["contact_enrichment"] = {
        "version": 1,
        "mode": "realtime",
        "model": args.model,
        "enriched_at_utc": started.isoformat(),
        "concurrency": conc,
        "skipped_pre_enrichment": skipped,
        "attempted": len(gated),
        "succeeded": len([c for c in gated if request_id(c) in rows]),
        "failed": failures,
    }

    Path(args.out_json).parent.mkdir(parents=True, exist_ok=True)
    if args.pretty:
        out_txt = json.dumps(raw, ensure_ascii=False, indent=2)
    else:
        out_txt = json.dumps(raw, ensure_ascii=False, separators=(",", ":"))
    Path(args.out_json).write_text(out_txt, encoding="utf-8")
    print(f"Wrote {args.out_json} (row keys={len(rows)}, failures={len(failures)})")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
