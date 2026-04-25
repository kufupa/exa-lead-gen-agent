from __future__ import annotations

from collections.abc import Callable

from hotel_decision_maker_research import Contact, recompute_intimacy

from contact_enrichment.types import ChannelResearchRow


def _is_blank(v: str | None) -> bool:
    return v is None or not str(v).strip()


def _pick(dst: str | None, src: str | None, *, overwrite: bool) -> str | None:
    if src is None or not str(src).strip():
        return dst
    if overwrite or _is_blank(dst):
        return str(src).strip()
    return dst


def apply_row(contact: Contact, row: ChannelResearchRow, *, overwrite: bool) -> Contact:
    """Merge enrichment row into a Contact; recompute intimacy."""
    u = contact.model_copy(
        update={
            "email": _pick(contact.email, row.email, overwrite=overwrite),
            "email2": _pick(contact.email2, row.email2, overwrite=overwrite),
            "phone": _pick(contact.phone, row.phone, overwrite=overwrite),
            "phone2": _pick(contact.phone2, row.phone2, overwrite=overwrite),
            "x_handle": _pick(contact.x_handle, row.x_handle, overwrite=overwrite),
            "linkedin_url": _pick(contact.linkedin_url, row.linkedin_url, overwrite=overwrite),
            "other_contact_detail": _pick(
                contact.other_contact_detail, row.other_contact_detail, overwrite=overwrite
            ),
        }
    )
    return u.model_copy(update={"intimacy_grade": recompute_intimacy(u)})


def merge_by_request_id(
    contacts: list[Contact],
    rows_by_request_id: dict[str, ChannelResearchRow],
    *,
    request_id_fn: Callable[[Contact], str],
    overwrite: bool,
) -> list[Contact]:
    """Apply rows keyed by request_id to the matching contact (same list order)."""
    out: list[Contact] = []
    for c in contacts:
        rid = request_id_fn(c)
        row = rows_by_request_id.get(rid)
        if row is None:
            out.append(c.model_copy(update={"intimacy_grade": recompute_intimacy(c)}))
        else:
            out.append(apply_row(c, row, overwrite=overwrite))
    return out
