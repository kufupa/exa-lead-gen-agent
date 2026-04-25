from __future__ import annotations

from urllib.parse import urlsplit


def normalize_linkedin_url(url: str) -> str:
    if not url:
        return ""

    parsed = urlsplit(url.strip())
    path = parsed.path.rstrip("/")
    return f"https://www.linkedin.com{path}"


def fetch_linkedin_profiles(exa_client, urls, batch_size: int = 10, text_max_chars: int = 12000) -> dict[str, str]:
    unique_urls: list[str] = []
    seen: set[str] = set()
    for raw_url in urls:
        normalized = normalize_linkedin_url(raw_url)
        if normalized and normalized not in seen:
            seen.add(normalized)
            unique_urls.append(normalized)

    fetched: dict[str, str] = {url: "" for url in unique_urls}
    for start in range(0, len(unique_urls), batch_size):
        batch = unique_urls[start : start + batch_size]
        try:
            result = exa_client.get_contents(batch, text={"max_characters": text_max_chars})
        except Exception:
            for url in batch:
                fetched[url] = ""
            continue

        for item in getattr(result, "results", []):
            normalized = normalize_linkedin_url(getattr(item, "url", ""))
            if normalized in fetched:
                fetched[normalized] = getattr(item, "text", "") or ""

    return fetched


def discover_linkedin_urls(exa_client, contacts) -> dict[str, str]:
    discovered: dict[str, str] = {}

    for contact in contacts:
        name = str(contact.get("full_name", "")).strip()
        company = str(contact.get("company", "")).strip()
        key = f"{name}|{company}"

        query = f"category:people {name}"
        if company:
            query = f"{query} {company}"

        discovered_url = ""
        try:
            result = exa_client.search(query, num_results=3)
            for item in getattr(result, "results", []):
                normalized = normalize_linkedin_url(getattr(item, "url", ""))
                if "linkedin.com/in/" in normalized:
                    discovered_url = normalized
                    break
        except Exception:
            discovered_url = ""

        if discovered_url:
            discovered[key] = discovered_url

    return discovered
