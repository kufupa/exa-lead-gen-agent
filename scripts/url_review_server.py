#!/usr/bin/env python3
"""Serve a local web UI for approving hotel URLs one-by-one."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from urllib.parse import urlparse

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from url_review.blocks import load_blocked_netlocs
from url_review.csv_model import iter_candidates
from url_review.decisions import append_domain, load_domains, remove_domain


def _read_file_utf8(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _json_error(msg: str, status: int) -> tuple[int, dict[str, str]]:
    return status, {"error": msg}


@dataclass
class CandidateSnapshot:
    csv_path: Path
    fulljsons_dir: Path
    yes_path: Path
    no_path: Path
    history: list[dict[str, Any]] = field(default_factory=list)

    def rows(self) -> list[dict[str, Any]]:
        blocked = set(load_blocked_netlocs(self.fulljsons_dir))
        blocked.update(load_domains(self.yes_path))
        blocked.update(load_domains(self.no_path))
        return iter_candidates(self.csv_path, frozenset(blocked))

    def payload(self) -> dict[str, Any]:
        rows = self.rows()
        return {
            "columns": list(rows[0].keys()) if rows else [],
            "rows": rows,
            "remaining": len(rows),
        }

    def mark_decision(self, row: dict[str, Any], decision: str) -> bool:
        d = decision.strip().lower()
        if d not in {"yes", "no"}:
            return False
        domain = (row.get("domain") or "").strip().lower()
        if not domain:
            return False
        target = self.yes_path if d == "yes" else self.no_path
        added = append_domain(target, domain)
        if added:
            self.history.append({"domain": domain, "decision": d, "row": row})
        return added

    def undo_last(self) -> dict[str, Any] | None:
        if not self.history:
            return None
        last = self.history.pop()
        target = self.yes_path if last["decision"] == "yes" else self.no_path
        if not remove_domain(target, last["domain"]):
            self.history.append(last)
            return None
        return last


def _resolve_path(raw: str, base: Path) -> Path:
    p = Path(raw)
    return p if p.is_absolute() else (base / p).resolve()


def _build_request_handler(snapshot: CandidateSnapshot, static_index_path: Path):
    class RequestHandler(BaseHTTPRequestHandler):
        def _send_json(self, status: int, payload: dict[str, Any]) -> None:
            body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _serve_static_index(self) -> None:
            if not static_index_path.is_file():
                self._send_json(500, _json_error("index file missing", 500))
                return
            body = _read_file_utf8(static_index_path).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _method_not_allowed(self, message: str = "method not allowed") -> None:
            self._send_json(405, {"error": message})

        def do_GET(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/":
                self._serve_static_index()
                return
            if parsed.path == "/api/candidates":
                self._send_json(200, snapshot.payload())
                return
            if parsed.path == "/favicon.ico":
                self.send_response(204)
                self.end_headers()
                return
            self._send_json(404, {"error": "not found"})

        def do_POST(self) -> None:
            parsed = urlparse(self.path)
            if parsed.path == "/api/decision":
                self._handle_decision()
                return
            if parsed.path == "/api/undo":
                self._handle_undo()
                return
            self._send_json(404, {"error": "not found"})
            return

        def _handle_decision(self) -> None:
            length = int(self.headers.get("Content-Length", "0") or 0)
            if length <= 0:
                self._send_json(*_json_error("missing body", 400))
                return

            raw = self.rfile.read(length).decode("utf-8")
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError:
                self._send_json(400, {"error": "invalid json"})
                return
            if not isinstance(payload, dict):
                self._send_json(400, {"error": "invalid payload"})
                return

            domain = str(payload.get("domain", "")).strip().lower()
            if not domain:
                self._send_json(400, {"error": "missing domain"})
                return
            decision = str(payload.get("decision", "")).strip().lower()
            if decision not in {"yes", "no"}:
                self._send_json(400, {"error": "invalid decision"})
                return

            candidates = snapshot.rows()
            row = next((r for r in candidates if (r.get("domain") or "").strip().lower() == domain), None)
            if row is None:
                self._send_json(409, {"error": "domain not in queue"})
                return

            added = snapshot.mark_decision(row, decision)
            self._send_json(200, {"ok": True, "already_recorded": not added})

        def _handle_undo(self) -> None:
            length = int(self.headers.get("Content-Length", "0") or 0)
            if length > 0:
                _ = self.rfile.read(length)
            undone = snapshot.undo_last()
            if not undone:
                self._send_json(409, {"error": "nothing_to_undo"})
                return
            self._send_json(200, {
                "ok": True,
                "row": undone["row"],
                "decision": undone["decision"],
                "domain": undone["domain"],
            })

    return RequestHandler


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Start URL review web UI.")
    p.add_argument("--csv", default="csv/Lobby_London_hotels_2026-04-17.csv")
    p.add_argument("--fulljsons-dir", default="fullJSONs")
    p.add_argument("--yes-file", default="yes.txt")
    p.add_argument("--no-file", default="no.txt")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8765)
    return p


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    root = Path.cwd().resolve()

    csv_path = _resolve_path(args.csv, root)
    fulljsons_dir = _resolve_path(args.fulljsons_dir, root)
    yes_path = _resolve_path(args.yes_file, root)
    no_path = _resolve_path(args.no_file, root)
    static_index = _ROOT / "url_review" / "static" / "index.html"

    snapshot = CandidateSnapshot(
        csv_path=csv_path,
        fulljsons_dir=fulljsons_dir,
        yes_path=yes_path,
        no_path=no_path,
    )

    handler_cls = _build_request_handler(snapshot, static_index)
    print(f"Serving on http://{args.host}:{args.port} (CSV={csv_path})")
    server = ThreadingHTTPServer((args.host, args.port), handler_cls)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        return 0
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
