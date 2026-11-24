"""The diagnostic's own, stricter view of one phone lookup.

Why a second, stricter contract exists
--------------------------------------
:func:`~altegio_bot.easyweek_migration.customer_api.lookup_customer_by_phone`
has a deliberate, production-proven allowance: the same customer UUID listed
twice is one customer. That allowance is right where it lives — the migration
read and the manual Add both depend on it, and both are left exactly as they
are — but it is an allowance about ROWS, and the diagnostic needs a guarantee
about COVERAGE.

The case that forced this apart: a lookup declaring ``total: 2``, handing back
the same UUID on both pages, with ``per_page`` changing between the two
responses. Under the shared contract that reads as "one customer, fully read",
because two rows were seen and two were promised. But the two facts do not fit
together: if the window really moved, one of the two promised rows was never
shown, and the row never shown may be a SECOND customer on that number. Reading
one person's booking history and showing it against the other's row is the one
mistake this whole path exists to prevent, so the diagnostic refuses to resolve
an identity from a listing it cannot prove it saw all of.

What it checks, beyond the shared contract
------------------------------------------
* the request parameters are the ones this lookup is for — this number, and the
  next page in sequence, never a page repeated or skipped;
* the page metadata is complete and internally consistent, and ``current_page``
  answers the page that was asked for;
* ``per_page`` is stable across every response, and every page before the last
  is actually full — a shrinking or growing window cannot be proven to cover
  the declared total;
* ``last_page`` and ``total`` are stable across every response;
* the rows handed over add up to exactly the declared total, and
* every row is a DISTINCT customer. A repeated UUID inside a declared total
  means a row was spent on a customer we had already seen, so the listing
  cannot account for the total it promised.

Nothing here is retryable: each one of these is an answer about the response,
not an outage, and "try again later" would be advice that cannot work.

Every refusal code is a fixed identifier. No number, name, UUID or response
body appears in one, nor in anything this module raises or records.
"""

from __future__ import annotations

from typing import Any, Final, Protocol

from altegio_bot.campaigns.easyweek_customer_history import CUSTOMER_BOOKINGS_PER_PAGE
from altegio_bot.easyweek_migration.manifest import canonical_uuid

# -- stable, PII-free refusal details --------------------------------------
LOOKUP_PARAMS_UNEXPECTED: Final = "zero_visit_lookup_params_unexpected"
LOOKUP_PAGE_OUT_OF_SEQUENCE: Final = "zero_visit_lookup_page_out_of_sequence"
LOOKUP_META_MALFORMED: Final = "zero_visit_lookup_meta_malformed"
LOOKUP_PAGE_SIZE_UNSTABLE: Final = "zero_visit_lookup_page_size_unstable"
LOOKUP_PAGE_UNDERFILLED: Final = "zero_visit_lookup_page_underfilled"
LOOKUP_TOTAL_UNSTABLE: Final = "zero_visit_lookup_total_unstable"
LOOKUP_COVERAGE_INCOMPLETE: Final = "zero_visit_lookup_coverage_incomplete"
LOOKUP_ROWS_NOT_DISTINCT: Final = "zero_visit_lookup_rows_not_distinct"


class DiagnosticLookupRefused(Exception):
    """A listing the diagnostic will not resolve an identity from.

    ``retryable`` is a class attribute and always ``False``: the shared identity
    proof reads exactly that attribute to decide whether to tell an operator to
    come back later, and nothing here gets better by being asked twice.
    """

    retryable = False

    def __init__(self, detail: str) -> None:
        super().__init__(detail)
        self.detail = detail


class _Reads(Protocol):
    async def get_booking(self, booking_uuid: str) -> dict[str, Any]: ...

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]: ...

    async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]: ...

    async def list_customer_bookings(self, customer_uuid: str, page: int, per_page: int = ...) -> dict[str, Any]: ...


class DiagnosticReader:
    """One operator's reads: counted, classified, and strictly paginated.

    Three jobs, all of them for the one check that wraps it:

    *Counting.* ``external_calls`` is how many reads were ATTEMPTED. The check's
    single deadline covers its local phase as well as its network phase, and
    the two failures are not the same thing to report: a deadline that expired
    with this counter still at zero never reached EasyWeek, and saying "EasyWeek
    did not answer" would blame a branch that was never asked.

    *Classifying.* ``failure`` keeps the first typed client exception, because
    the shared identity proof deliberately collapses every read failure into one
    refusal. The check needs the family back — temporary, refused, malformed —
    and the typed exception is where that lives.

    *Strictness.* ``refusal`` keeps the first coverage refusal, so the check can
    say which part of the listing could not be trusted.

    Pure delegation otherwise. It reads, never writes, and holds no state about
    a person: a page size, some counters, and a set of UUIDs seen during one
    lookup.
    """

    def __init__(self, reader: _Reads, *, phone: str | None = None) -> None:
        self._reader = reader
        self._phone = phone
        self.external_calls = 0
        self.failure: Exception | None = None
        self.refusal: str | None = None
        self._page = 0
        self._per_page: int | None = None
        self._expected: tuple[int, int] | None = None
        self._rows = 0
        self._seen: set[str] = set()

    def expect_phone(self, phone: str) -> None:
        """Bind the number this lookup is allowed to ask about.

        Set after the local phase, because the number is not known until the
        row's own evidence has been read. From then on a page requested for any
        other number is refused: an answer about a different number is not an
        answer about this row, however well formed it is.
        """
        self._phone = phone

    # -- bookkeeping -------------------------------------------------------
    def _note(self, exc: Exception) -> None:
        if self.failure is None:
            self.failure = exc

    def _refuse(self, detail: str) -> DiagnosticLookupRefused:
        if self.refusal is None:
            self.refusal = detail
        return DiagnosticLookupRefused(detail)

    # -- plain delegation --------------------------------------------------
    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        self.external_calls += 1
        try:
            return await self._reader.get_booking(booking_uuid)
        except Exception as exc:
            self._note(exc)
            raise

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        self.external_calls += 1
        try:
            return await self._reader.get_customer(customer_uuid)
        except Exception as exc:
            self._note(exc)
            raise

    async def list_customer_bookings(
        self, customer_uuid: str, page: int, per_page: int = CUSTOMER_BOOKINGS_PER_PAGE
    ) -> dict[str, Any]:
        self.external_calls += 1
        try:
            return await self._reader.list_customer_bookings(customer_uuid, page=page, per_page=per_page)
        except Exception as exc:
            self._note(exc)
            raise

    # -- the strict part ---------------------------------------------------
    async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
        """One page of the phone lookup, verified before it is handed on."""
        page = params.get("page")
        if self._phone is not None and params.get("phone") != self._phone:
            # The lookup asked about a different number than the one this check
            # proved locally. Nothing about the answer would be about this row.
            raise self._refuse(LOOKUP_PARAMS_UNEXPECTED)
        if type(page) is not int or page != self._page + 1:
            # Pages must be walked once each, in order. A repeat would
            # double-count towards the declared total and a skip would miss a
            # customer, and both look like a complete read from the outside.
            raise self._refuse(LOOKUP_PAGE_OUT_OF_SEQUENCE)
        self._page = page
        self.external_calls += 1
        try:
            payload = await self._reader.list_customers(params=params)
        except Exception as exc:
            self._note(exc)
            raise
        self._verify(payload, page=page)
        return payload

    def _verify(self, payload: object, *, page: int) -> None:
        if not isinstance(payload, dict) or not isinstance(payload.get("data"), list):
            raise self._refuse(LOOKUP_META_MALFORMED)
        meta = payload.get("meta")
        if not isinstance(meta, dict):
            raise self._refuse(LOOKUP_META_MALFORMED)
        current, last, total = (self._int(meta, key) for key in ("current_page", "last_page", "total"))
        if current is None or last is None or total is None or last < 1 or total < 0:
            raise self._refuse(LOOKUP_META_MALFORMED)
        if current != page:
            raise self._refuse(LOOKUP_PAGE_OUT_OF_SEQUENCE)
        if self._expected is not None and self._expected != (last, total):
            # The shape of the result set changed underneath the walk, so no
            # number of pages proves it was covered.
            raise self._refuse(LOOKUP_TOTAL_UNSTABLE)
        self._expected = (last, total)

        rows = payload["data"]
        per_page = self._int(meta, "per_page")
        if per_page is not None:
            if per_page < 1:
                raise self._refuse(LOOKUP_META_MALFORMED)
            if self._per_page is not None and per_page != self._per_page:
                raise self._refuse(LOOKUP_PAGE_SIZE_UNSTABLE)
            self._per_page = per_page
            if page < last and len(rows) != per_page:
                # A short page before the last one means the window moved: the
                # rows between the two windows were never handed over.
                raise self._refuse(LOOKUP_PAGE_UNDERFILLED)

        self._rows += len(rows)
        if self._rows > total:
            raise self._refuse(LOOKUP_COVERAGE_INCOMPLETE)
        for row in rows:
            identity = canonical_uuid(row.get("uuid") or row.get("id")) if isinstance(row, dict) else None
            if identity is None:
                raise self._refuse(LOOKUP_META_MALFORMED)
            if identity in self._seen:
                # A row spent on a customer already listed. The declared total
                # is then unaccounted for, and the unaccounted row may be a
                # second person on this number.
                raise self._refuse(LOOKUP_ROWS_NOT_DISTINCT)
            self._seen.add(identity)
        if page >= last and self._rows != total:
            raise self._refuse(LOOKUP_COVERAGE_INCOMPLETE)

    @staticmethod
    def _int(meta: dict[str, Any], key: str) -> int | None:
        value = meta.get(key)
        return value if type(value) is int else None


__all__ = [
    "LOOKUP_COVERAGE_INCOMPLETE",
    "LOOKUP_META_MALFORMED",
    "LOOKUP_PAGE_OUT_OF_SEQUENCE",
    "LOOKUP_PAGE_SIZE_UNSTABLE",
    "LOOKUP_PAGE_UNDERFILLED",
    "LOOKUP_PARAMS_UNEXPECTED",
    "LOOKUP_ROWS_NOT_DISTINCT",
    "LOOKUP_TOTAL_UNSTABLE",
    "DiagnosticLookupRefused",
    "DiagnosticReader",
]
