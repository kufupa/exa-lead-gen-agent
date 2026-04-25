from __future__ import annotations

import json
import os
import tempfile
from pathlib import Path
from typing import Any


def load_checkpoint(path: str | Path) -> dict[str, Any] | None:
    p = Path(path)
    if not p.exists() or p.stat().st_size == 0:
        return None
    return json.loads(p.read_text(encoding="utf-8"))


def save_checkpoint_atomic(path: str | Path, data: dict[str, Any]) -> None:
    """Write JSON via temp file + os.replace for crash safety."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".ckpt_", suffix=".json", dir=str(p.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise
