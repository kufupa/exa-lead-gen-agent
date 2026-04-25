from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, TypeVar

from filelock import FileLock

from lead_aggregates.atomic import atomic_write_json
from lead_aggregates.builders import (
    build_email_document,
    build_master_document,
    build_phone_document,
    registry_from_enriched_scan,
)
from lead_aggregates.registry import apply_patch, empty_registry

T = TypeVar("T")

ALL_ENRICHED = "all_enriched_leads.json"
INTIMATE_PHONE = "intimate_phone_contacts.json"
INTIMATE_EMAIL = "intimate_email_contacts.json"
REGISTRY = "url_registry.json"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    raw = path.read_text(encoding="utf-8").strip()
    if not raw:
        return None
    return json.loads(raw)


class AggregatesStore:
    """Single `fullJSONs/.merge.lock` for all aggregate JSON writes."""

    @staticmethod
    def _repo_root_relative(rel: str, jsons_dir: Path) -> Path:
        return jsons_dir.parent.joinpath(*rel.split("/"))

    def __init__(
        self,
        fulljsons_dir: Path,
        jsons_dir: Path,
        *,
        lock_timeout: float = 600,
    ) -> None:
        self.fulljsons_dir = Path(fulljsons_dir)
        self.jsons_dir = Path(jsons_dir)
        self._lock_path = self.fulljsons_dir / ".merge.lock"
        self._timeout = lock_timeout

    def _path(self, name: str) -> Path:
        return self.fulljsons_dir / name

    def run_locked(self, fn: Callable[[], T]) -> T:
        self.fulljsons_dir.mkdir(parents=True, exist_ok=True)
        with FileLock(str(self._lock_path), timeout=self._timeout):
            return fn()

    def rebuild_all(self) -> None:
        master = build_master_document(self.jsons_dir)
        phone = build_phone_document(self.jsons_dir)
        email = build_email_document(self.jsons_dir)
        reg = registry_from_enriched_scan(self.jsons_dir)

        def write_all() -> None:
            atomic_write_json(self._path(ALL_ENRICHED), master)
            atomic_write_json(self._path(INTIMATE_PHONE), phone)
            atomic_write_json(self._path(INTIMATE_EMAIL), email)
            atomic_write_json(self._path(REGISTRY), reg)

        self.run_locked(write_all)

    def mark_researching(
        self,
        *,
        canonical_url: str,
        research_json: str,
        enriched_json: str,
    ) -> None:
        def op() -> None:
            reg = _read_json(self._path(REGISTRY)) or empty_registry()
            reg = apply_patch(
                reg,
                canonical_url,
                {
                    "status": "researching",
                    "research_json": research_json,
                    "enriched_json": enriched_json,
                    "claimed_by": f"pid:{os.getpid()}",
                    "last_started_at_utc": _now_iso(),
                    "error": None,
                },
            )
            atomic_write_json(self._path(REGISTRY), reg)

        self.run_locked(op)

    def commit_after_enrich(
        self,
        *,
        canonical_url: str,
        research_json: str,
        enriched_json: str,
        error: str | None,
    ) -> None:
        if error:

            def fail() -> None:
                reg = _read_json(self._path(REGISTRY)) or empty_registry()
                reg = apply_patch(
                    reg,
                    canonical_url,
                    {
                        "status": "failed",
                        "error": error,
                        "research_json": research_json,
                        "enriched_json": enriched_json,
                        "last_finished_at_utc": _now_iso(),
                    },
                )
                atomic_write_json(self._path(REGISTRY), reg)

            self.run_locked(fail)
            return

        master = build_master_document(self.jsons_dir)
        phone = build_phone_document(self.jsons_dir)
        email = build_email_document(self.jsons_dir)

        def success() -> None:
            atomic_write_json(self._path(ALL_ENRICHED), master)
            atomic_write_json(self._path(INTIMATE_PHONE), phone)
            atomic_write_json(self._path(INTIMATE_EMAIL), email)
            reg = _read_json(self._path(REGISTRY)) or empty_registry()
            reg = apply_patch(
                reg,
                canonical_url,
                {
                    "status": "enriched",
                    "research_json": research_json,
                    "enriched_json": enriched_json,
                    "error": None,
                    "last_finished_at_utc": _now_iso(),
                },
            )
            atomic_write_json(self._path(REGISTRY), reg)

        self.run_locked(success)

    def enriched_entry_file_exists(self, canonical_url: str) -> bool:
        """True if registry says enriched and enriched JSON path exists (repo-relative paths)."""
        reg = _read_json(self._path(REGISTRY))
        if not reg:
            return False
        u = (reg.get("urls") or {}).get(canonical_url)
        if not u or u.get("status") != "enriched":
            return False
        ej = u.get("enriched_json")
        if not ej or not isinstance(ej, str):
            return False
        return self._repo_root_relative(ej, self.jsons_dir).is_file()

    def rebuild_phone_document_only(self) -> None:
        doc = build_phone_document(self.jsons_dir)

        def w() -> None:
            atomic_write_json(self._path(INTIMATE_PHONE), doc)

        self.run_locked(w)

    def rebuild_email_document_only(self) -> None:
        doc = build_email_document(self.jsons_dir)

        def w() -> None:
            atomic_write_json(self._path(INTIMATE_EMAIL), doc)

        self.run_locked(w)
