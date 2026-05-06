"""Altegio Records API client.

Endpoint used:
  GET /records/{company_id}

A visit is counted as attended when ``attendance == 1`` or
``visit_attendance == 1`` in the API response. This corresponds to
the 'Пришёл' status shown in the Altegio UI.

The local ``records`` table must NOT be used as source of truth for
visit-count decisions or for checking future appointments. It only
contains a partial sync of the client history: webhook events that
arrived before the bot was deployed are absent, causing systematic
under-counting and stale state.

Authorization: Bearer {partner_token},{user_token}
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from altegio_bot.settings import settings

logger = logging.getLogger(__name__)

_PAGE_SIZE = 200
_ALTEGIO_LOCAL_TZ = ZoneInfo("Europe/Belgrade")


class AmbiguousRecordError(Exception):
    """Raised when an active record's start time cannot be determined.

    A record is "active" when it is not deleted and not explicitly cancelled
    (``confirmed != 0``).  If its ``date`` / ``datetime`` fields are absent
    or unparseable, we cannot say whether it is in the future or the past.

    Callers that perform a pre-send guard (e.g. ``repeat_10d``) MUST treat
    this as "uncertain future appointment" and requeue rather than send.
    Silently falling back to False (= "no future appointment") is fail-open
    and could cause the bot to message a client who already has a booking.
    """


def _auth_header() -> str:
    return f"Bearer {settings.altegio_partner_token},{settings.altegio_user_token}"


def _headers() -> dict[str, str]:
    return {
        "Authorization": _auth_header(),
        "Accept": settings.altegio_api_accept,
        "Content-Type": "application/json",
    }


def _extract_records_data(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise ValueError(f"Unexpected Altegio response type: {type(payload)}")

    data = payload.get("data")
    if not isinstance(data, list):
        raise ValueError(f"Expected list in data, got: {type(data)}")

    rows: list[dict[str, Any]] = []
    for rec in data:
        if not isinstance(rec, dict):
            raise ValueError(f"Expected dict record, got: {type(rec)}")
        rows.append(rec)

    return rows


def _parse_record_starts_at(record_data: dict[str, Any]) -> datetime | None:
    """Return record start time in UTC.

    Altegio's ``datetime`` field may contain an incorrect UTC offset.
    To match the logic already used in ``inbox_worker``, prefer the
    naïve local ``date`` field and fall back to the first 19 chars of
    ``datetime`` while stripping its offset.

    If neither field can be parsed and at least one of them was
    non-empty, a warning is logged so the issue is visible in prod.
    Returns ``None`` in all failure cases; callers decide what to do.
    """
    raw_date = record_data.get("date")
    if isinstance(raw_date, str) and raw_date.strip():
        try:
            naive_dt = datetime.fromisoformat(raw_date.strip().replace(" ", "T"))
            return naive_dt.replace(tzinfo=_ALTEGIO_LOCAL_TZ).astimezone(timezone.utc)
        except ValueError:
            pass

    raw_datetime = record_data.get("datetime")
    if isinstance(raw_datetime, str) and len(raw_datetime) >= 19:
        try:
            naive_dt = datetime.fromisoformat(raw_datetime[:19])
            return naive_dt.replace(tzinfo=_ALTEGIO_LOCAL_TZ).astimezone(timezone.utc)
        except ValueError:
            pass

    # Log when we had non-empty date data but failed to parse it.
    # Silently returning None for records with no date at all is expected
    # (e.g. draft records), but a parse failure on real data is noteworthy.
    if raw_date or raw_datetime:
        logger.warning(
            "record id=%s: could not parse date=%r or datetime=%r — start time unknown",
            record_data.get("id"),
            raw_date,
            raw_datetime,
        )

    return None


def _record_is_future_active(
    record_data: dict[str, Any],
    *,
    now_dt: datetime,
) -> bool:
    """Return True when this record represents an active future appointment.

    A record is considered NOT active when:
    - ``deleted`` is truthy (physically removed), OR
    - ``confirmed == 0`` (appointment was cancelled in Altegio).

    Assumption: the ``confirmed`` field follows the same semantics as the
    local ``Record.confirmed`` column populated by the inbox_worker, where
    ``confirmed == 1`` (CONFIRMED_FLAG from campaigns/segment.py) means
    the booking is active and ``confirmed == 0`` means it was cancelled.
    If ``confirmed`` is absent/None we treat the record conservatively as
    potentially active — it is safer to suppress ``repeat_10d`` than to
    send it to a client who already has an upcoming visit.

    Raises:
        AmbiguousRecordError: when the record is active (not deleted, not
            cancelled) but its start time cannot be determined.  The caller
            must NOT treat this as "no future appointment" — it should
            requeue the job instead of sending.
    """
    if bool(record_data.get("deleted")):
        return False

    # confirmed == 0 → appointment was cancelled in Altegio.
    confirmed = record_data.get("confirmed")
    if confirmed is not None:
        try:
            if int(confirmed) == 0:
                return False
        except (ValueError, TypeError):
            pass

    starts_at = _parse_record_starts_at(record_data)
    if starts_at is None:
        # The record is active (not deleted, not cancelled) but we cannot
        # determine when it starts.  This is ambiguous — falling back to
        # False (= "no future appointment") would be fail-open and could
        # let repeat_10d through when the client actually has a booking.
        raise AmbiguousRecordError(
            f"record id={record_data.get('id')!r}: active record with no"
            f" parseable start time"
            f" (date={record_data.get('date')!r},"
            f" datetime={record_data.get('datetime')!r})"
        )

    return starts_at > now_dt


async def _iter_client_records(
    *,
    company_id: int,
    altegio_client_id: int,
    timeout_sec: float,
) -> AsyncIterator[dict[str, Any]]:
    base = settings.altegio_api_base_url.rstrip("/")
    url = f"{base}/records/{company_id}"
    page = 1

    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        while True:
            params: dict[str, Any] = {
                "client_id": altegio_client_id,
                "count": _PAGE_SIZE,
                "page": page,
            }
            resp = await client.get(url, headers=_headers(), params=params)
            resp.raise_for_status()

            data = _extract_records_data(resp.json())

            for rec in data:
                yield rec

            if len(data) < _PAGE_SIZE:
                break

            logger.debug(
                "iterate_client_records page=%d company_id=%d altegio_client_id=%d",
                page,
                company_id,
                altegio_client_id,
            )
            page += 1


async def count_attended_client_visits(
    *,
    company_id: int,
    altegio_client_id: int,
    timeout_sec: float = 15.0,
) -> int:
    """Return attended visit count for a client from the Altegio API.

    Paginates through all records for the client and counts those where
    ``attendance == 1`` or ``visit_attendance == 1``.

    Raises ``httpx.HTTPError`` on network or HTTP errors instead of
    returning a fallback of 0. Callers must handle the exception and
    choose the safe action (cancel job, fail for retry, etc.). Never
    treat an API failure as '0 visits' — that would wrongly qualify
    every unreachable client for a review request.
    """
    total = 0

    async for rec in _iter_client_records(
        company_id=company_id,
        altegio_client_id=altegio_client_id,
        timeout_sec=timeout_sec,
    ):
        attendance = rec.get("attendance") or 0
        visit_attendance = rec.get("visit_attendance") or 0
        if attendance == 1 or visit_attendance == 1:
            total += 1

    return total


async def client_has_any_future_record(
    *,
    company_id: int,
    altegio_client_id: int,
    now_dt: datetime | None = None,
    timeout_sec: float = 15.0,
) -> bool:
    """Return True when the client has any non-deleted future record (any booking status).

    Unlike client_has_future_appointments, this does NOT exclude cancelled
    appointments (confirmed == 0).  Used by the follow-up live guard to be
    maximally conservative — any non-deleted future record blocks the follow-up.

    Records with no date fields at all (draft/placeholder rows) are silently
    skipped, matching the behaviour of the local ``_has_future_record`` query
    which requires ``starts_at > now``.

    Raises:
        AmbiguousRecordError: when a non-deleted record has non-empty but
            unparseable date fields.  Callers must treat this as a transient
            error and requeue the job rather than sending.
        httpx.HTTPError: on network or HTTP errors.
    """
    current_dt = now_dt or datetime.now(timezone.utc)

    async for rec in _iter_client_records(
        company_id=company_id,
        altegio_client_id=altegio_client_id,
        timeout_sec=timeout_sec,
    ):
        if bool(rec.get("deleted")):
            continue

        raw_date = rec.get("date")
        raw_datetime = rec.get("datetime")
        starts_at = _parse_record_starts_at(rec)

        if starts_at is None:
            if raw_date or raw_datetime:
                # Non-empty but unparseable date on a non-deleted record — be conservative.
                raise AmbiguousRecordError(
                    f"record id={rec.get('id')!r}: non-deleted record with unparseable"
                    f" start time (date={raw_date!r}, datetime={raw_datetime!r})"
                )
            # No date at all (draft/placeholder) — cannot determine if future; skip.
            continue

        if starts_at > current_dt:
            return True

    return False


async def client_has_future_appointments(
    *,
    company_id: int,
    altegio_client_id: int,
    now_dt: datetime | None = None,
    timeout_sec: float = 15.0,
) -> bool:
    """Return True when the client has an active future appointment.

    Uses the Altegio API as the source of truth and ignores deleted
    and cancelled (confirmed == 0) records. This is used by the
    ``repeat_10d`` guard immediately before sending the message.

    Raises:
        AmbiguousRecordError: if an active record's date cannot be parsed.
            The caller should requeue the job rather than proceed with
            sending — see ``AmbiguousRecordError`` for rationale.
        httpx.HTTPError: on network or HTTP errors.
    """
    current_dt = now_dt or datetime.now(timezone.utc)

    async for rec in _iter_client_records(
        company_id=company_id,
        altegio_client_id=altegio_client_id,
        timeout_sec=timeout_sec,
    ):
        if _record_is_future_active(rec, now_dt=current_dt):
            return True

    return False
