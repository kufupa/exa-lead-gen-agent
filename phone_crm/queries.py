from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Iterable

import psycopg

from phone_crm.config import Settings, load_settings
from phone_crm.db import open_connection
from phone_crm.models import ContactRow, CrmSummary, HotelGroup
from phone_crm.normalizer import normalize_contact_row


def row_to_contact(row: Any) -> ContactRow:
    payload = row.get("payload")
    if isinstance(payload, str):
        payload = json.loads(payload)
    return ContactRow(
        occurrence_id=row["occurrence_id"] or "",
        source_enriched_json=row.get("source_enriched_json") or "",
        target_url=row.get("target_url") or "",
        hotel_name=row.get("hotel_name") or "Unknown hotel",
        full_name=row.get("full_name") or "",
        title=row.get("title") or "",
        primary_handle=row.get("primary_handle") or "",
        phone=row.get("phone") or "",
        phone2=row.get("phone2") or "",
        email=row.get("email") or "",
        email2=row.get("email2") or "",
        linkedin_url=row.get("linkedin_url") or "",
        x_handle=row.get("x_handle") or "",
        other_contact_detail=row.get("other_contact_detail") or "",
        decision_maker_score=row.get("decision_maker_score") or "",
        intimacy_grade=row.get("intimacy_grade") or "",
        has_phone=bool(row.get("has_phone")),
        has_email=bool(row.get("has_email")),
        has_contact_route=bool(row.get("has_contact_route")),
        status=row.get("status") or "pending",
        notes=row.get("notes") or "",
        payload=payload if isinstance(payload, dict) else {},
    )


def fetch_contacts(conn: psycopg.Connection, phones_only: bool) -> list[ContactRow]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select *
            from public.crm_contacts
            where (%s = false or has_phone = true or coalesce(phone, '') <> '' or coalesce(phone2, '') <> '')
            order by lower(hotel_name), lower(full_name), occurrence_id
            """,
            (phones_only,),
        )
        rows = cur.fetchall()
    return [row_to_contact(r) for r in rows]


def fetch_contact(conn: psycopg.Connection, occurrence_id: str) -> ContactRow | None:
    with conn.cursor() as cur:
        cur.execute("select * from public.crm_contacts where occurrence_id = %s", (occurrence_id,))
        row = cur.fetchone()
    if not row:
        return None
    return row_to_contact(row)


def update_notes(conn: psycopg.Connection, occurrence_id: str, notes: str) -> ContactRow | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.crm_contacts
            set notes = %s
            where occurrence_id = %s
            returning *
            """,
            (notes, occurrence_id),
        )
        row = cur.fetchone()
    if row is None:
        return None
    conn.commit()
    return row_to_contact(row)


def update_status(conn: psycopg.Connection, occurrence_id: str, status: str) -> ContactRow | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update public.crm_contacts
            set status = %s
            where occurrence_id = %s
            returning *
            """,
            (status, occurrence_id),
        )
        row = cur.fetchone()
    if row is None:
        return None
    conn.commit()
    return row_to_contact(row)


def _status_sort_rank(status: str) -> int:
    if status == "pending":
        return 0
    if status == "done":
        return 1
    if status == "skipped":
        return 2
    return 3


def _is_actionable_pending(row: ContactRow) -> bool:
    return row.status == "pending" and row.has_phone


def build_groups(rows: list[ContactRow]) -> list[HotelGroup]:
    by_hotel: dict[str, list[ContactRow]] = {}
    hotel_meta: dict[str, str] = {}
    for row in rows:
        key = row.hotel_name.lower()
        by_hotel.setdefault(key, []).append(row)
        hotel_meta.setdefault(key, row.target_url)
        if row.target_url:
            hotel_meta[key] = row.target_url

    groups: list[HotelGroup] = []
    for key, contacts in by_hotel.items():
        sorted_contacts = sorted(
            contacts,
            key=lambda c: (_status_sort_rank(c.status), (c.full_name or "").lower(), c.occurrence_id),
        )
        pending_count = sum(
            1
            for c in sorted_contacts
            if _is_actionable_pending(c)
        )
        groups.append(
            HotelGroup(
                hotel_name=sorted_contacts[0].hotel_name,
                target_url=hotel_meta.get(key, ""),
                pending_count=pending_count,
                total_count=len(sorted_contacts),
                contacts=sorted_contacts,
            )
        )

    return sorted(
        groups,
        key=lambda g: (-g.pending_count, g.hotel_name.lower()),
    )


def build_summary(rows: list[ContactRow]) -> CrmSummary:
    total = len(rows)
    pending = len([row for row in rows if row.status == "pending"])
    done = len([row for row in rows if row.status == "done"])
    skipped = len([row for row in rows if row.status == "skipped"])
    return CrmSummary(total=total, pending=pending, done=done, skipped=skipped)


def find_next_contact_id(rows: list[ContactRow], current_id: str | None) -> str | None:
    if not rows:
        return None
    groups = build_groups(rows)
    ordered: list[ContactRow] = []
    current_hotel = None
    current_index = None
    for group in groups:
        for row in group.contacts:
            ordered.append(row)
            if row.occurrence_id == current_id:
                current_hotel = group.hotel_name.lower()
                current_index = len(ordered) - 1

    if not current_id:
        for row in ordered:
            if _is_actionable_pending(row):
                return row.occurrence_id
        return None

    # prefer next pending contact in the same hotel first
    if current_hotel is not None:
        same_hotel: list[ContactRow] = []
        for group in groups:
            if group.hotel_name.lower() != current_hotel:
                continue
            same_hotel.extend(
                [
                    r
                    for r in group.contacts
                    if _is_actionable_pending(r)
                ]
            )
            break
        if same_hotel and current_index is not None:
            for idx, row in enumerate(same_hotel):
                if row.occurrence_id == current_id and idx + 1 < len(same_hotel):
                    return same_hotel[idx + 1].occurrence_id
                if row.occurrence_id == current_id and idx + 1 == len(same_hotel):
                    break

    if current_index is not None:
        for row in ordered[current_index + 1 :]:
            if _is_actionable_pending(row):
                return row.occurrence_id
    for row in ordered:
        if _is_actionable_pending(row) and row.occurrence_id != current_id:
            return row.occurrence_id
    return None


def normalize_rows_from_json(payload: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise ValueError("Expected JSON object payload.")
    raw_contacts = payload.get("contacts")
    if not isinstance(raw_contacts, list):
        return []
    rows: list[dict[str, Any]] = []
    for item in raw_contacts:
        rows.append(normalize_contact_row(item))
    return rows


def load_warehouse(path: str) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as handle:
        payload = json.load(handle)
    return normalize_rows_from_json(payload)


def sync_rows(conn: psycopg.Connection, rows: list[dict[str, Any]], *, chunk_size: int = 500) -> int:
    upserted = 0
    if not rows:
        return 0
    deduped_rows = {row["occurrence_id"]: row for row in rows if row.get("occurrence_id")}
    rows = list(deduped_rows.values())
    for start in range(0, len(rows), chunk_size):
        chunk = rows[start:start + chunk_size]
        query = "select public.crm_upsert_contacts(%s::jsonb) as count"
        with conn.cursor() as cur:
            cur.execute(query, (json.dumps(chunk),))
            result = cur.fetchone()
        if result and "count" in result:
            upserted += int(result["count"] or 0)
    conn.commit()
    return upserted


def sync_from_json_file(settings: Settings, json_path: str) -> tuple[int, int]:
    rows = load_warehouse(json_path)
    with open_connection(settings) as conn:
        with conn.cursor() as cur:
            # force table exists and check basic permission before doing heavy work
            cur.execute("select to_regclass('public.crm_contacts')")
            cur.fetchone()
        upserted = sync_rows(conn, rows)
    return len(rows), upserted
