from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Callable, TypeVar

from filelock import FileLock

from lead_aggregates.atomic import atomic_write_json

from outreach.schema import empty_state, validate_state

T = TypeVar("T")


DEFAULT_STATE_NAME = "outreach_email_state.json"
DEFAULT_LOCK_NAME = ".outreach_email_state.lock"


class OutreachStore:
    """Locked read-modify-write for outreach_email_state.json."""

    def __init__(
        self,
        state_path: Path,
        *,
        lock_timeout: float = 600.0,
    ) -> None:
        self.state_path = Path(state_path)
        self._lock_path = self.state_path.parent / DEFAULT_LOCK_NAME
        self._timeout = lock_timeout

    @classmethod
    def default_paths(cls, fulljsons_dir: Path) -> OutreachStore:
        return cls(fulljsons_dir / DEFAULT_STATE_NAME)

    def run_locked(self, fn: Callable[[], T]) -> T:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        with FileLock(str(self._lock_path), timeout=self._timeout):
            return fn()

    def load_unlocked(self) -> dict[str, Any]:
        if not self.state_path.is_file():
            return empty_state()
        raw = self.state_path.read_text(encoding="utf-8").strip()
        if not raw:
            return empty_state()
        return json.loads(raw)

    def load_validated(self) -> dict[str, Any]:
        doc = self.load_unlocked()
        problems = validate_state(doc)
        if problems:
            raise ValueError("invalid outreach state: " + "; ".join(problems[:20]))
        return doc

    def save(self, doc: dict[str, Any]) -> None:
        atomic_write_json(self.state_path, doc)

    def update_in_place(self, mutator: Callable[[dict[str, Any]], None]) -> None:
        def op() -> None:
            doc = self.load_validated()
            mutator(doc)
            self.save(doc)

        self.run_locked(op)
