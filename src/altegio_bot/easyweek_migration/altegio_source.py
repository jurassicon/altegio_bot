"""The SOURCE OF TRUTH for the cutover: the Altegio API (PR-11.1).

Not the local ``records`` table. ``altegio_records`` already says why, at the top
of the file, and it is the same reason here only louder: our tables hold a
*partial* sync — everything that happened before the bot was deployed, and
everything that arrived while a worker was down, is simply absent. Migrating from
them would silently leave real customers out of the new schedule, and the gap
would be invisible until somebody arrived to a salon that had no record of them.

So every mode reads Altegio live. The local database is used for exactly one
thing in this package — the migration ledger — and never as a list of what exists.

This module is read-only by construction: it issues ``GET /records/{company_id}``
and nothing else. It does not import the Altegio production modules' internals or
change them in any way; the six lines of header-building below are duplicated on
purpose, so that the cutover cannot alter the behaviour of the running bot.
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Final

import httpx

from altegio_bot.easyweek_migration.cutover import ALTEGIO_LOCAL_TZ
from altegio_bot.settings import settings

logger = logging.getLogger("easyweek_migration.source")

_PAGE_SIZE: Final = 200
# A hard stop so a malformed response that never shrinks below the page size
# cannot spin forever against a rate-limited API.
_MAX_PAGES: Final = 500
_DEFAULT_TIMEOUT_SEC: Final = 30.0

# Altegio attendance semantics. 1 (came) and -1 (no-show) are FINISHED visits and
# are never migrated; 0 (waiting) and 2 (client confirmed) are the live states of
# an appointment that has not happened yet.
ATTENDANCE_WAITING: Final = 0
ATTENDANCE_CONFIRMED: Final = 2
ACTIVE_ATTENDANCE: Final[frozenset[int]] = frozenset({ATTENDANCE_WAITING, ATTENDANCE_CONFIRMED})


class AltegioSourceError(RuntimeError):
    """The source could not be read. The run stops rather than migrating a subset.

    A partial source list is the one failure mode with no safe interpretation: it
    is indistinguishable from "those customers have no booking", and acting on it
    would leave them out of the new schedule with a green report.
    """


def _headers() -> dict[str, str]:
    """Altegio auth headers.

    Deliberately built here rather than imported from ``altegio_records``: PR-11.1
    must not modify or re-enter the Altegio production path, and importing a
    private helper is a modification in waiting. Tokens are read at call time and
    never logged.
    """
    return {
        "Authorization": f"Bearer {settings.altegio_partner_token},{settings.altegio_user_token}",
        "Accept": settings.altegio_api_accept,
        "Content-Type": "application/json",
    }


@dataclass(frozen=True)
class SourceWindow:
    """The inclusive local-date window handed to the Altegio API.

    Altegio filters by local calendar date, so the window is derived from the
    cutover instant in the salons' own zone and widened by a day on the left. The
    extra day costs one page and removes a whole class of off-by-one: a booking
    on the cutover date, minutes after the cutover, must not be excluded because
    the API's day boundary sits in a different zone than ours.
    """

    start_date: str
    end_date: str


def build_window(cutover_at: datetime, *, horizon_days: int) -> SourceWindow:
    if horizon_days < 1:
        raise ValueError("horizon_days must be >= 1")
    local = cutover_at.astimezone(ALTEGIO_LOCAL_TZ)
    start = (local - timedelta(days=1)).date()
    end = (local + timedelta(days=horizon_days)).date()
    return SourceWindow(start_date=start.isoformat(), end_date=end.isoformat())


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    """Pull the record list out of one page, refusing anything unexpected.

    An unrecognised envelope is an error, not an empty page: "we could not
    understand the response" and "this branch has no future bookings" must never
    collapse into the same outcome.
    """
    if not isinstance(payload, dict):
        raise AltegioSourceError("records response is not a JSON object")
    data = payload.get("data")
    if not isinstance(data, list):
        raise AltegioSourceError("records response has no data list")
    rows: list[dict[str, Any]] = []
    for row in data:
        if not isinstance(row, dict):
            raise AltegioSourceError("records response contains a non-object entry")
        rows.append(row)
    return rows


async def iter_company_records(
    *,
    company_id: int,
    window: SourceWindow,
    timeout_sec: float = _DEFAULT_TIMEOUT_SEC,
    client: httpx.AsyncClient | None = None,
) -> AsyncIterator[dict[str, Any]]:
    """Yield every Altegio record for *company_id* inside *window*.

    Paginates until a short page arrives. HTTP and transport failures raise
    :class:`AltegioSourceError` — they are never swallowed into a shorter list,
    for the reason given on that class.
    """
    base = settings.altegio_api_base_url.rstrip("/")
    url = f"{base}/records/{company_id}"

    owns_client = client is None
    http = client or httpx.AsyncClient(timeout=timeout_sec)
    try:
        for page in range(1, _MAX_PAGES + 1):
            params: dict[str, Any] = {
                "start_date": window.start_date,
                "end_date": window.end_date,
                "count": _PAGE_SIZE,
                "page": page,
            }
            try:
                response = await http.get(url, headers=_headers(), params=params)
                response.raise_for_status()
                payload = response.json()
            except httpx.HTTPStatusError as exc:
                # Status only. An Altegio error body can echo the request, and the
                # request carries the partner/user tokens in a header.
                raise AltegioSourceError(
                    f"altegio records request failed company_id={company_id} status={exc.response.status_code}"
                ) from None
            except httpx.HTTPError as exc:
                raise AltegioSourceError(
                    f"altegio records transport error company_id={company_id} error_type={type(exc).__name__}"
                ) from None
            except ValueError:
                raise AltegioSourceError(f"altegio records response is not JSON company_id={company_id}") from None

            rows = _extract_rows(payload)
            for row in rows:
                yield row

            logger.info(
                "easyweek_migration: source page company_id=%s page=%s rows=%s",
                company_id,
                page,
                len(rows),
            )
            if len(rows) < _PAGE_SIZE:
                return
        raise AltegioSourceError(f"altegio records pagination did not terminate company_id={company_id}")
    finally:
        if owns_client:
            await http.aclose()


async def fetch_company_records(
    *,
    company_id: int,
    window: SourceWindow,
    timeout_sec: float = _DEFAULT_TIMEOUT_SEC,
    client: httpx.AsyncClient | None = None,
) -> list[dict[str, Any]]:
    """Materialise :func:`iter_company_records`. The caller needs the whole set."""
    return [
        row
        async for row in iter_company_records(
            company_id=company_id,
            window=window,
            timeout_sec=timeout_sec,
            client=client,
        )
    ]
