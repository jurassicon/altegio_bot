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
* the declared page size, page count and total agree with each other: a size of
  100 and a total of 1 is one page, and no page carries more rows than the size
  allows;
* ``last_page`` and ``total`` are stable across every response;
* no page is empty while the total says there is something left, and no page
  arrives after the rows have already added up to the total — an empty listing
  (``total: 0``) is a complete answer and passes;
* the rows handed over add up to exactly the declared total, and
* every row is a DISTINCT customer. A repeated UUID inside a declared total
  means a row was spent on a customer we had already seen, so the listing
  cannot account for the total it promised.

The client sends ``{"phone": ..., "page": n}`` and never a page size, so
``per_page`` is the server describing its own window. Where it is not an
integer — a string ``"100"``, say — it is treated as unknown rather than
refused: a legitimate answer may spell it that way, and the structural rules
above still have to hold without it. The direct card read that follows the
listing is checked too, and its two failures are kept apart: a 200 whose body is
not a readable card, and a readable card for a different customer.

Nothing here is retryable: each one of these is an answer about the response,
not an outage, and "try again later" would be advice that cannot work.

Every refusal code is a fixed identifier. No number, name, UUID or response
body appears in one, nor in anything this module raises or records.
"""

from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Final, Protocol

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
# The declared page size and the declared page count do not agree with the
# declared total: with a page size of 100 and a total of 1 there is exactly one
# page, and an answer that claims two has not described a listing that exists.
LOOKUP_PAGE_COUNT_UNEXPECTED: Final = "zero_visit_lookup_page_count_unexpected"
# A page that handed over nothing while the total says there is something. The
# rows it was supposed to carry were not refused, they were simply not there.
LOOKUP_PAGE_EMPTY: Final = "zero_visit_lookup_page_empty"
# More rows on one page than the declared page size allows.
LOOKUP_PAGE_OVERFILLED: Final = "zero_visit_lookup_page_overfilled"
# Every promised row was handed over before the last page. The pages after it
# are then either empty — see above — or rows the total never counted.
LOOKUP_TOTAL_REACHED_EARLY: Final = "zero_visit_lookup_total_reached_early"
# The direct card read, kept apart from the listing's codes: a 200 whose body is
# not a readable card, and a readable card for a DIFFERENT customer, are two
# different facts and only one of them is about an identity.
LOOKUP_CARD_MALFORMED: Final = "zero_visit_lookup_card_malformed"
LOOKUP_CARD_UUID_MISMATCH: Final = "zero_visit_lookup_card_uuid_mismatch"


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
        # True exactly while a call is in flight. The check's deadline covers
        # the local phase and the network phase together, and three outcomes
        # have to be told apart: nothing asked yet, EasyWeek not answering in
        # time, and our own phase running out of time AFTER EasyWeek answered.
        # Only this flag distinguishes the last two.
        self.awaiting = False
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

    async def _read(self, call: Awaitable[dict[str, Any]]) -> dict[str, Any]:
        """One counted, phase-tracked external read."""
        self.external_calls += 1
        self.awaiting = True
        try:
            payload = await call
        except asyncio.CancelledError:
            # The deadline expired INSIDE this call, so ``awaiting`` stays set
            # on purpose: it is the only evidence that EasyWeek, and not our own
            # phase, is what ran out of time.
            raise
        except Exception as exc:
            self.awaiting = False
            self._note(exc)
            raise
        self.awaiting = False
        return payload

    # -- plain delegation --------------------------------------------------
    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        return await self._read(self._reader.get_booking(booking_uuid))

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        payload = await self._read(self._reader.get_customer(customer_uuid))
        self._note_card(payload, customer_uuid=customer_uuid)
        return payload

    async def list_customer_bookings(
        self, customer_uuid: str, page: int, per_page: int = CUSTOMER_BOOKINGS_PER_PAGE
    ) -> dict[str, Any]:
        return await self._read(self._reader.list_customer_bookings(customer_uuid, page=page, per_page=per_page))

    def _note_card(self, payload: object, *, customer_uuid: str) -> None:
        """Record WHY a direct card read is unusable, without deciding anything.

        The shared proof already refuses an unreadable card and a card for the
        wrong customer with one code, which is right for a path that only needs
        "no". A diagnostic has to say which: a 200 whose body is not a card is a
        contract problem on our side of the conversation, and a readable card
        for a DIFFERENT customer is an answer about identity. Noted rather than
        raised, so the shared proof keeps producing the outcome and this only
        supplies the detail an operator can quote.
        """
        body: object = payload
        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            body = payload["data"]
        if not isinstance(body, dict):
            self._refuse(LOOKUP_CARD_MALFORMED)
            return
        identity = canonical_uuid(body.get("uuid") or body.get("id"))
        if identity is None:
            self._refuse(LOOKUP_CARD_MALFORMED)
            return
        if identity != canonical_uuid(customer_uuid):
            self._refuse(LOOKUP_CARD_UUID_MISMATCH)

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
        # The client asks for ``{"phone": ..., "page": n}`` and never sends a
        # page size, so ``per_page`` is the server describing its own window. A
        # value that is not an integer — a string "100", say — is not refused
        # outright, because a legitimate answer may well spell it that way; it
        # is simply not a size anything can be proven from, so the size-based
        # checks are skipped and the structural ones below still have to hold.
        per_page = self._int(meta, "per_page")
        if per_page is not None:
            if per_page < 1:
                raise self._refuse(LOOKUP_META_MALFORMED)
            if self._per_page is not None and per_page != self._per_page:
                raise self._refuse(LOOKUP_PAGE_SIZE_UNSTABLE)
            self._per_page = per_page
            if last != max(1, -(-total // per_page)):
                # This size and this total determine the page count exactly. An
                # answer that claims a different one is not describing a listing
                # that exists, and walking it proves nothing about coverage.
                raise self._refuse(LOOKUP_PAGE_COUNT_UNEXPECTED)
            if len(rows) > per_page:
                # More rows than the window it declared.
                raise self._refuse(LOOKUP_PAGE_OVERFILLED)
            if page < last and len(rows) != per_page:
                # A short page before the last one means the window moved: the
                # rows between the two windows were never handed over.
                raise self._refuse(LOOKUP_PAGE_UNDERFILLED)
        if not rows and total > 0:
            # A page that handed over nothing while the total says there is
            # something. Whatever it was supposed to carry was never shown, and
            # a customer never shown may be the second person on this number.
            # An empty listing — ``total: 0`` — is a complete answer and passes
            # here, as it must: "nobody has this number" is a fact.
            raise self._refuse(LOOKUP_PAGE_EMPTY)

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
        if page < last and self._rows >= total:
            # Every promised row has been handed over, and yet the answer says
            # there are more pages. Either they are empty — a listing that
            # cannot be reconciled with its own total — or they hold rows the
            # total never counted. Neither can be proven complete.
            raise self._refuse(LOOKUP_TOTAL_REACHED_EARLY)
        if page >= last and self._rows != total:
            raise self._refuse(LOOKUP_COVERAGE_INCOMPLETE)

    @staticmethod
    def _int(meta: dict[str, Any], key: str) -> int | None:
        value = meta.get(key)
        return value if type(value) is int else None


__all__ = [
    "LOOKUP_CARD_MALFORMED",
    "LOOKUP_CARD_UUID_MISMATCH",
    "LOOKUP_COVERAGE_INCOMPLETE",
    "LOOKUP_META_MALFORMED",
    "LOOKUP_PAGE_COUNT_UNEXPECTED",
    "LOOKUP_PAGE_EMPTY",
    "LOOKUP_PAGE_OUT_OF_SEQUENCE",
    "LOOKUP_PAGE_OVERFILLED",
    "LOOKUP_PAGE_SIZE_UNSTABLE",
    "LOOKUP_PAGE_UNDERFILLED",
    "LOOKUP_PARAMS_UNEXPECTED",
    "LOOKUP_ROWS_NOT_DISTINCT",
    "LOOKUP_TOTAL_REACHED_EARLY",
    "LOOKUP_TOTAL_UNSTABLE",
    "DiagnosticLookupRefused",
    "DiagnosticReader",
]
