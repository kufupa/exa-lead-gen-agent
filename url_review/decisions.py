from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

from filelock import FileLock


def normalize_domain(raw: str) -> str:
    raw = (raw or "").strip().lower()
    if not raw:
        return ""
    if "://" in raw:
        parsed = urlparse(raw)
        raw = (parsed.netloc or raw).strip()
    else:
        parsed = urlparse(f"https://{raw}")
        if parsed.netloc:
            raw = parsed.netloc.strip()
    if raw.startswith("www."):
        raw = raw[4:]
    return raw


def load_domains(path: Path) -> frozenset[str]:
    if not path.is_file():
        return frozenset()
    out: set[str] = set()
    for line in path.read_text(encoding="utf-8").splitlines():
        domain = normalize_domain(line)
        if domain:
            out.add(domain)
    return frozenset(out)


def append_domain(path: Path, domain: str, *, lock_path: Path | None = None) -> bool:
    normalized = normalize_domain(domain)
    if not normalized:
        return False

    lock_file = lock_path or (path.parent / "yes_no.lock")
    with FileLock(str(lock_file)):
        current = set(load_domains(path))
        if normalized in current:
            return False
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as f:
            f.write(f"{normalized}\n")
        return True


def remove_domain(path: Path, domain: str, *, lock_path: Path | None = None) -> bool:
    normalized = normalize_domain(domain)
    if not normalized:
        return False

    lock_file = lock_path or (path.parent / "yes_no.lock")
    with FileLock(str(lock_file)):
        if not path.is_file():
            return False
        lines = [line for line in path.read_text(encoding="utf-8").splitlines()]
        for idx in range(len(lines) - 1, -1, -1):
            if normalize_domain(lines[idx]) == normalized:
                del lines[idx]
                path.parent.mkdir(parents=True, exist_ok=True)
                text = "".join([f"{line}\n" for line in lines])
                path.write_text(text, encoding="utf-8")
                return True
        return False

