from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hotel_decision_maker_research import Contact

from contact_enrichment.batch import BatchJob, submit_and_drain_batch
from contact_enrichment.checkpoint import load_checkpoint, save_checkpoint_atomic
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
    p.add_argument("--in-json", required=True, help="Input JSON from hotel_decision_maker_research.py")
    p.add_argument("--out-json", required=True, help="Output JSON path")
    p.add_argument("--mode", choices=("realtime", "batch"), default="realtime")
    p.add_argument("--model", default="grok-4.20-reasoning", help="xAI model id")
    p.add_argument("--max-turns", type=int, default=36)
    p.add_argument("--min-direct-channels", type=float, default=1.5, help="Skip contacts at or above this score")
    p.add_argument("--concurrency", type=int, default=None, help="Realtime worker threads (default: auto)")
    p.add_argument("--batch-chunk-size", type=int, default=50, help="Requests per batch.add call")
    p.add_argument("--batch-name", default="hotel_contact_enrichment")
    p.add_argument("--checkpoint", default=None, help="Checkpoint JSON path (recommended for batch)")
    p.add_argument("--resume", action="store_true", help="Skip request_ids listed in checkpoint")
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


def _run_batch_with_checkpoint(
    jobs: list[BatchJob],
    *,
    target_url: str,
    model: str,
    max_turns: int,
    batch_name: str,
    add_chunk_size: int,
    checkpoint_path: str | None,
    rows_accumulator: dict[str, ChannelResearchRow],
    ck_completed: set[str],
) -> tuple[dict[str, ChannelResearchRow], list[dict[str, str]], str]:
    """Run batch drain; merge pages into rows_accumulator; optional incremental checkpoint."""

    def on_page(delta: dict[str, ChannelResearchRow], delta_fail: list[dict[str, str]]) -> None:
        rows_accumulator.update(delta)
        if not checkpoint_path:
            return
        done = set(ck_completed)
        done.update(rows_accumulator.keys())
        save_checkpoint_atomic(
            checkpoint_path,
            {
                "version": 1,
                "completed_request_ids": sorted(done),
                "rows_json": {k: v.model_dump() for k, v in rows_accumulator.items()},
            },
        )

    return submit_and_drain_batch(
        jobs,
        target_url=target_url,
        model=model,
        max_turns=max_turns,
        batch_name=batch_name,
        add_chunk_size=add_chunk_size,
        on_page=on_page if checkpoint_path else None,
    )


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    raw, contacts = _load_lead_file(args.in_json)
    target_url = str(raw.get("target_url", ""))

    gated = [c for c in contacts if needs_enrichment(c, args.min_direct_channels)]
    skipped = len(contacts) - len(gated)

    ck_rows: dict[str, ChannelResearchRow] = {}
    ck_completed: set[str] = set()
    if args.checkpoint and Path(args.checkpoint).exists():
        ck = load_checkpoint(args.checkpoint)
        if ck:
            for k, v in ck.get("rows_json", {}).items():
                ck_rows[k] = ChannelResearchRow.model_validate(v)
            if args.resume:
                raw_done = set(ck.get("completed_request_ids", []))
                ck_completed = {rid for rid in raw_done if rid in ck_rows}

    jobs_to_run = [c for c in gated if request_id(c) not in ck_completed]

    conc = args.concurrency if args.concurrency is not None else _default_concurrency()

    if args.dry_run:
        print(f"in_json={args.in_json}")
        print(f"out_json={args.out_json}")
        print(f"mode={args.mode} model={args.model} max_turns={args.max_turns}")
        print(f"contacts_total={len(contacts)} skipped_high_score={skipped} to_enrich={len(gated)}")
        print(f"after_resume_pending={len(jobs_to_run)} concurrency={conc}")
        print(f"checkpoint={args.checkpoint!r} resume={args.resume}")
        return 0

    rows: dict[str, ChannelResearchRow] = dict(ck_rows)
    failures: list[dict[str, str]] = []
    batch_id_out: str | None = None

    if jobs_to_run:
        if args.mode == "realtime":
            rjobs = [RealtimeJob(c) for c in jobs_to_run]
            new_rows, fails = run_realtime(
                rjobs,
                target_url=target_url,
                model=args.model,
                max_turns=args.max_turns,
                concurrency=conc,
            )
            rows.update(new_rows)
            failures.extend(fails)
        else:
            bjobs = [BatchJob(c) for c in jobs_to_run]
            new_rows, fails, batch_id_out = _run_batch_with_checkpoint(
                bjobs,
                target_url=target_url,
                model=args.model,
                max_turns=args.max_turns,
                batch_name=args.batch_name,
                add_chunk_size=args.batch_chunk_size,
                checkpoint_path=args.checkpoint,
                rows_accumulator=rows,
                ck_completed=ck_completed,
            )
            rows.update(new_rows)
            failures.extend(fails)
            if args.checkpoint and batch_id_out:
                save_checkpoint_atomic(
                    args.checkpoint,
                    {
                        "version": 1,
                        "batch_id": batch_id_out,
                        "completed_request_ids": sorted(rows.keys()),
                        "rows_json": {k: v.model_dump() for k, v in rows.items()},
                    },
                )

    merged = merge_by_request_id(contacts, rows, request_id_fn=request_id, overwrite=args.overwrite)
    raw["contacts"] = [c.model_dump() for c in merged]

    started = datetime.now(timezone.utc)
    raw["contact_enrichment"] = {
        "version": 1,
        "mode": args.mode,
        "model": args.model,
        "enriched_at_utc": started.isoformat(),
        "concurrency": conc if args.mode == "realtime" else None,
        "skipped_pre_enrichment": skipped,
        "attempted": len(jobs_to_run),
        "succeeded": len([c for c in jobs_to_run if request_id(c) in rows]),
        "failed": failures,
        "batch_id": batch_id_out,
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
