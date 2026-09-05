"""The ONLY module in this project allowed to mutate EasyWeek (PR-11.1).

``easyweek_client.EasyWeekClient`` is GET-only by construction and stays that
way: plan §1.6 p.8 is a hard rule, the running bot depends on it, and "we added a
POST to the shared client, but only the migration calls it" is how a hard rule
stops being one. The cutover therefore gets its own client, in its own module,
with its own name — so the grep for "who can write to EasyWeek" has exactly one
answer.

It reuses the transport *policy* of the read client (pinned origin, no redirects,
typed errors, no bodies in logs) by importing those pieces rather than
re-deciding them.

The uncertain-result contract
-----------------------------
A ``POST`` that times out, or dies mid-flight, is **not** a failed POST. The
server may have created the booking and lost the response. Retrying it would
create a second appointment for a real person, and the customer would see two.

So this client separates three outcomes, and the caller must too:

``BookingCreated``      a 2xx we read back, carrying a booking UUID.
``EasyWeekUncertainMutation``  we do not know. No retry, ever, automatically.
                        The row goes to ``uncertain`` and waits for reconcile.
``EasyWeekError``       a definite failure — the request provably did not create
                        anything (a 4xx the server rejected before acting).

Retries exist only where they are provably safe, and that is **429 alone**: a
rate limiter refuses a request before the handler runs, so nothing was created.
Everything else — a timeout, a transport disconnect, any 5xx, a 2xx we cannot
read a UUID out of — leaves the outcome unknown and gets exactly one POST.

A 5xx is deliberately NOT retried. "The server answered, so it declined to act"
sounds right and is false: gateways, proxies and application handlers all return
5xx after a write has already landed, and EasyWeek publishes no idempotency key
for ``POST /bookings``. Without documented idempotency, a second POST is a coin
flip on whether a real customer gets two appointments.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Sequence
from types import TracebackType
from typing import Any, Awaitable, Callable, Final

import httpx

from altegio_bot.easyweek_client import (
    _DEFAULT_TIMEOUT,
    EasyWeekAuthError,
    EasyWeekConfigError,
    EasyWeekError,
    EasyWeekNotFoundError,
    EasyWeekPermanentError,
    EasyWeekProtocolError,
    EasyWeekRetryableError,
    _backoff_delay,
    _canonical_booking_uuid,
    _normalize_base_url,
    _parse_retry_after,
    _unwrap_secret,
)
from altegio_bot.settings import settings

logger = logging.getLogger("easyweek_migration.write")

_PATH_BOOKINGS: Final = "bookings"
_PATH_LOCATIONS: Final = "locations"
_PATH_SERVICES: Final = "services"
_PATH_CUSTOMERS: Final = "customers"

# `GET /customers` defaults to 15 per page and caps at 100. Asking for the cap
# keeps a workspace-wide directory to as few paged reads as the pacing allows.
_CUSTOMERS_PER_PAGE: Final = 100

# EasyWeek allows 60 requests/min per key (plan §1.1). The migration is the only
# thing that ever runs at volume against that budget, but it is not alone on the
# key: the shared outbox worker's reminder guard verifies every reminder against
# the same API. 40/min (one request every 1.5s) is the operator-chosen pace — it
# leaves a third of the budget for that worker rather than the two thirds the
# earlier 24 left. A 429 is still handled by the retry path; this constant only
# decides how often we walk into one.
DEFAULT_REQUESTS_PER_MINUTE: Final = 40

# A mutation gets fewer attempts than a read. Every extra attempt is another
# chance to time out in a way we cannot interpret.
_MAX_MUTATION_ATTEMPTS: Final = 2


class EasyWeekUncertainMutation(EasyWeekError):
    """The request was sent and its outcome is unknown. NEVER retried automatically.

    Deliberately not a subclass of the retryable error: the whole point is that
    no generic "retryable?" check can sweep it up. A caller has to name it.
    """

    retryable = False


class RateLimiter:
    """A simple, monotonic-clock request pacer.

    Not a token bucket with a burst: a burst is precisely what trips a
    60-per-minute limit at the start of a bulk apply, and being slightly slower
    costs a cutover nothing.
    """

    def __init__(
        self,
        *,
        requests_per_minute: int = DEFAULT_REQUESTS_PER_MINUTE,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if requests_per_minute < 1:
            raise ValueError("requests_per_minute must be >= 1")
        self._interval = 60.0 / requests_per_minute
        self._sleep = sleep or asyncio.sleep
        self._monotonic = monotonic
        self._next_allowed: float | None = None

    async def acquire(self) -> None:
        now = self._monotonic()
        if self._next_allowed is not None and now < self._next_allowed:
            await self._sleep(self._next_allowed - now)
            now = self._monotonic()
        self._next_allowed = now + self._interval


class BookingCreated:
    """A proven creation: EasyWeek answered 2xx and named the booking."""

    __slots__ = ("booking_uuid", "attempts")

    def __init__(self, *, booking_uuid: str, attempts: int) -> None:
        self.booking_uuid = booking_uuid
        self.attempts = attempts

    def __repr__(self) -> str:
        return f"<BookingCreated uuid={self.booking_uuid!r} attempts={self.attempts}>"


# The IANA zone these branches run on, as EasyWeek itself reports it on a live
# booking (`"timezone": "Europe/Berlin"`). Sent explicitly rather than left to a
# server default, and — because a constant is still an assumption — re-read from
# the booking and compared at readback, so a wrong value fails a canary instead
# of quietly shifting appointments.
#
# Not the same string as `cutover.ALTEGIO_LOCAL_TZ` ("Europe/Belgrade"), and
# deliberately so: that one is how the Altegio production path parses source
# timestamps, this one is what EasyWeek calls the destination's zone. The two
# share CET/CEST offsets and DST rules to the second, so the instant is identical
# either way; keeping them as separate names keeps the two contracts separate.
EASYWEEK_BOOKING_TIMEZONE: Final = "Europe/Berlin"

# Exactly the fields `POST /bookings` documents, and nothing else. Used to build
# the request and to allowlist field names in 422 diagnostics — a server naming
# a field outside this set is telling us something we must not paraphrase.
BOOKING_REQUEST_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "location_uuid",
        "service_uuid",
        "reserved_on",
        "customer_phone",
        "customer_first_name",
        "staffer_uuid",
        "booking_comment",
        "timezone",
    }
)


def build_booking_request(
    *,
    location_uuid: str,
    staffer_uuid: str,
    service_uuid: str,
    customer_phone: str,
    customer_first_name: str,
    reserved_on_utc_iso: str,
    comment: str,
    timezone_name: str = EASYWEEK_BOOKING_TIMEZONE,
) -> dict[str, Any]:
    """Build the ``POST /bookings`` body from the DOCUMENTED contract.

    The first version of this function was written against the endpoint alone —
    the plan confirmed the path but not the schema — and it guessed six field
    names out of seven. EasyWeek answered 422 and the canary died there. The
    published contract is:

    ``location_uuid``, ``service_uuid``, ``reserved_on``, ``customer_phone`` and
    ``customer_first_name`` are required; ``staffer_uuid``, ``booking_comment``
    and ``timezone`` are the optional fields this migration uses. There is no
    ``customer_uuid``, no ``duration`` and no ``price`` on this endpoint: the
    length and the money come from the catalogue service, which is exactly why
    the classifier refuses any booking whose price or duration differs from its
    catalogue baseline. Those refusals stopped being caution and became load
    bearing the moment this contract was confirmed.

    ``staffer_uuid`` is optional to EasyWeek and mandatory to us. Omitting it
    lets the server pick a master, and a migration that lets somebody else choose
    who serves the customer is not a migration.

    The customer is identified by phone because the API offers no other way, and
    the phone and first name come from the **EasyWeek card that was already
    matched** — never invented, never taken from Altegio to overwrite what
    EasyWeek holds. Support confirmed on 2026-08-31 that a phone number is a
    unique customer identifier and duplicates cannot be created, which is what
    makes sending one safe; it is not, however, idempotency for the booking
    itself, so the ledger's claim-before-write contract is unchanged.

    ``booking_comment`` carries the stable, PII-free marker (see
    :func:`altegio_bot.easyweek_migration.ledger.migration_marker`) that lets a
    migrated booking be recognised in the EasyWeek UI. It comes back as
    ``public_notes``.

    Undocumented Form Builder fields are never guessed. If a workspace requires
    one, the server says 422 and the run stops with that named refusal.
    """
    body = {
        "location_uuid": location_uuid,
        "service_uuid": service_uuid,
        "reserved_on": reserved_on_utc_iso,
        "customer_phone": customer_phone,
        "customer_first_name": customer_first_name,
        "staffer_uuid": staffer_uuid,
        "booking_comment": comment,
        "timezone": timezone_name,
    }
    # A body that grew a field the contract does not name is a body we cannot
    # reason about. Caught here rather than by the server.
    assert frozenset(body) == BOOKING_REQUEST_FIELDS
    return body


# The exact top-level keys of the cart body a live canary was answered 200 for
# (plan §30.12). An allowlist, not a starting point: the plain `/bookings`
# endpoint already cost one canary to a guessed schema, and this one is proven
# only in the shape below.
CART_REQUEST_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "location_uuid",
        "timezone",
        "customer_phone",
        "customer_first_name",
        "booking_comment",
        "items",
    }
)
CART_ITEM_FIELDS: Final[frozenset[str]] = frozenset({"datetime_start", "services"})
CART_SERVICE_FIELDS: Final[frozenset[str]] = frozenset({"service_uuid", "staffer_uuid"})

_PATH_CART: Final = "cart"
_PATH_STATUS: Final = "status"
_PATH_CANCEL: Final = "cancel"

# The exact cancel body a live canary was answered 2xx for. An allowlist, like
# every other request shape here: `internal_notes` is a fixed technical string
# and never carries a customer's name, phone, or anything read off the record —
# it is written into somebody else's CRM and stays there.
CANCEL_REQUEST_BODY: Final[dict[str, str]] = {
    "cancel_reason": "other",
    "internal_notes": "altegio migration rollback",
}
CANCEL_REQUEST_FIELDS: Final[frozenset[str]] = frozenset(CANCEL_REQUEST_BODY)


def build_cart_booking_request(
    *,
    location_uuid: str,
    customer_phone: str,
    customer_first_name: str,
    datetime_start_utc_iso: str,
    comment: str,
    services: Sequence[tuple[str, str]],
    timezone_name: str = EASYWEEK_BOOKING_TIMEZONE,
) -> dict[str, Any]:
    """Build the ``POST /bookings/cart`` body from the PROVEN canary shape.

    ``services`` is a sequence of ``(service_uuid, staffer_uuid)`` in the order
    the source lists them. That order is the request's order and is canonical
    everywhere else too — a booking whose two services swapped places is a
    different request, and a plan reviewed against the old one never authorised
    it.

    One item, always. The canary created one cart item holding both services and
    got back exactly one booking; several items is a shape with no evidence, and
    the `one Altegio record -> one EasyWeek booking` ledger relation depends on
    the one-to-one the single item gives.

    Field names are an allowlist rather than a starting point. The plain
    ``/bookings`` endpoint already cost a canary to a guessed schema (see
    :func:`build_booking_request`), and nothing here is sent that the successful
    body did not contain — no duration, no price, no customer uuid. The length
    and the money come from the catalogue services, which is exactly why the
    classifier refuses any booking whose price or duration differs from its
    reviewed baseline.
    """
    if len(services) != 2:
        # The proven contract is two. One goes through `build_booking_request`;
        # three or more has no evidence at all.
        raise EasyWeekPermanentError(
            "a cart booking carries exactly two services", operation="build_cart_booking_request"
        )
    if services[0][0] == services[1][0]:
        raise EasyWeekPermanentError(
            "a cart booking needs two different services", operation="build_cart_booking_request"
        )
    if services[0][1] != services[1][1]:
        # The canary proved one staffer across both lines and nothing else.
        raise EasyWeekPermanentError(
            "a cart booking needs one staffer for both services", operation="build_cart_booking_request"
        )

    item = {
        "datetime_start": datetime_start_utc_iso,
        "services": [
            {"service_uuid": service_uuid, "staffer_uuid": staffer_uuid} for service_uuid, staffer_uuid in services
        ],
    }
    body = {
        "location_uuid": location_uuid,
        "timezone": timezone_name,
        "customer_phone": customer_phone,
        "customer_first_name": customer_first_name,
        "booking_comment": comment,
        "items": [item],
    }
    # A body that grew a field the proven shape does not name is a body we
    # cannot reason about. Caught here rather than by the server.
    assert frozenset(body) == CART_REQUEST_FIELDS
    assert frozenset(item) == CART_ITEM_FIELDS
    for line in item["services"]:
        assert frozenset(line) == CART_SERVICE_FIELDS
    return body


def _cart_uuid_candidates(payload: object) -> list[str]:
    """Every booking uuid a cart response states, in the order it states them.

    The canary's response carried exactly one. This reader looks in the shapes a
    list-or-object API can answer with — a bare object, a ``data`` envelope, a
    list of bookings — and collects ALL of them rather than stopping at the
    first, because "how many bookings did this create?" is the question the
    caller has to be able to answer.
    """
    if isinstance(payload, dict):
        inner = payload.get("data")
        if isinstance(inner, (dict, list)):
            payload = inner

    rows: list[Any]
    if isinstance(payload, dict):
        # A single booking object, or an envelope naming its bookings.
        for key in ("bookings", "items"):
            nested = payload.get(key)
            if isinstance(nested, list):
                rows = nested
                break
        else:
            rows = [payload]
    elif isinstance(payload, list):
        rows = payload
    else:
        return []

    found: list[str] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key in ("uuid", "uid", "booking_uuid"):
            candidate = row.get(key)
            if isinstance(candidate, str) and candidate.strip():
                found.append(candidate)
                break
    return found


# Every field name either proven request shape can legitimately be complained
# about, including the ones only the cart body has. The allowlist is the whole
# mechanism: a validation body is written by the server and can carry anything —
# the submitted phone echoed back, a stack frame, an internal column name — so a
# key survives only if this migration itself put it in a request.
CART_ERROR_FIELDS: Final[frozenset[str]] = CART_REQUEST_FIELDS | CART_ITEM_FIELDS | CART_SERVICE_FIELDS


def _safe_validation_fields(
    response: httpx.Response,
    *,
    allowed: frozenset[str] = BOOKING_REQUEST_FIELDS,
) -> list[str]:
    """Field NAMES a 4xx complained about, filtered to ones we sent.

    Contract-aware, because the two request shapes have different fields. The
    plain booking allowlist knows nothing about ``items`` or ``datetime_start``,
    so filtering a cart rejection through it discarded every useful name and
    told the operator "no recognised field" for a body the server had named
    precisely.

    Laravel reports nested failures as dotted paths — ``items.0.services.1.
    service_uuid``. The path is walked and every segment that is a known field
    survives; numeric indices and anything unrecognised are dropped. So a
    caller learns WHICH field, and never learns what value was in it.

    A workspace that requires an undocumented Form Builder field still shows up
    as "no recognised field named", which is the correct answer: we cannot fix
    it by guessing, and the runbook says to ask the operator.
    """
    try:
        payload: Any = response.json()
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []

    keys: set[str] = set()
    # Laravel-shaped: {"errors": {"field": [...]}}, and the flat variant.
    for container in (payload.get("errors"), payload):
        if isinstance(container, dict):
            keys.update(key for key in container if isinstance(key, str))

    recognised: set[str] = set()
    for key in keys:
        # A dotted path names several fields; a plain key names one. Either way
        # only the segments in the allowlist survive, and the ORDER is dropped —
        # what an operator needs is which fields, not the index that failed.
        for segment in key.split("."):
            if segment in allowed:
                recognised.add(segment)
    return sorted(recognised)


class EasyWeekMigrationWriteClient:
    """Mutating EasyWeek client, scoped to the cutover and nothing else."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        workspace_slug: str | None = None,
        base_url: str | None = None,
        timeout: httpx.Timeout | None = None,
        transport: httpx.AsyncBaseTransport | None = None,
        http_client: httpx.AsyncClient | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        rate_limiter: RateLimiter | None = None,
        max_attempts: int = _MAX_MUTATION_ATTEMPTS,
    ) -> None:
        key = _unwrap_secret(api_key if api_key is not None else settings.easyweek_api_key)
        slug = _unwrap_secret(workspace_slug if workspace_slug is not None else settings.easyweek_workspace_slug)
        if not (isinstance(key, str) and key.strip()):
            raise EasyWeekConfigError("EASYWEEK_API_KEY is not configured")
        if not (isinstance(slug, str) and slug.strip()):
            raise EasyWeekConfigError("EASYWEEK_WORKSPACE_SLUG is not configured")

        self._api_key = key.strip()
        self._workspace_slug = slug.strip()
        self._base_url = _normalize_base_url(base_url if base_url is not None else settings.easyweek_api_base_url)
        self._max_attempts = max(1, int(max_attempts))
        self._sleep = sleep or asyncio.sleep
        self._limiter = rate_limiter or RateLimiter(sleep=self._sleep)

        if http_client is not None:
            self._client = http_client
            self._owns_client = False
        else:
            self._client = httpx.AsyncClient(
                timeout=timeout or _DEFAULT_TIMEOUT,
                follow_redirects=False,
                transport=transport,
            )
            self._owns_client = True

    async def aclose(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> EasyWeekMigrationWriteClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    def __repr__(self) -> str:
        return f"<EasyWeekMigrationWriteClient base_url={self._base_url!r}>"

    __str__ = __repr__

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Workspace": self._workspace_slug,
            "Accept": "application/json",
            "Content-Type": "application/json",
        }

    async def create_booking(self, body: dict[str, Any]) -> BookingCreated:
        """``POST /bookings``. Returns only on a PROVEN creation.

        Exactly one outcome per call, and only ONE of them permits another POST:

        =========================  ===========================================
        429                        bounded retry — the server explicitly
                                   declined to process the request
        5xx                        **uncertain**, one POST only
        timeout                    **uncertain**, one POST only
        transport disconnect       **uncertain**, one POST only
        2xx without usable uuid    **uncertain**
        permanent 4xx              definite failure, no retry
        =========================  ===========================================

        The 5xx row is a correction. An earlier version retried 5xx on the
        reasoning that "the server answered, so it declined to act". That
        reasoning is wrong: a 500 from a gateway, a proxy or an application
        handler is routinely returned *after* the write has landed, and EasyWeek
        publishes no idempotency key for ``POST /bookings``. Retrying it is a
        coin flip on whether a real customer gets two appointments.

        Only 429 keeps its retry, because "too many requests" is a refusal to
        process, stated by the rate limiter before the handler runs.
        """
        url = "/".join([self._base_url, _PATH_BOOKINGS])
        last_retryable: EasyWeekError | None = None

        for attempt in range(1, self._max_attempts + 1):
            await self._limiter.acquire()
            try:
                response = await self._client.post(url, headers=self._headers(), json=body)
            except httpx.TimeoutException:
                # The request left; no answer came back. Whether a booking exists
                # is genuinely unknown, and this is the single most important
                # branch in the file.
                logger.error("easyweek_migration: create_booking timeout attempt=%s — outcome UNKNOWN", attempt)
                raise EasyWeekUncertainMutation(
                    "mutation timed out; outcome unknown", operation="create_booking", attempts=attempt
                ) from None
            except httpx.HTTPError as exc:
                logger.error(
                    "easyweek_migration: create_booking transport failure attempt=%s error_type=%s — outcome UNKNOWN",
                    attempt,
                    type(exc).__name__,
                )
                raise EasyWeekUncertainMutation(
                    "mutation transport failure; outcome unknown",
                    operation="create_booking",
                    attempts=attempt,
                ) from None

            status = response.status_code
            logger.info("easyweek_migration: create_booking status=%s attempt=%s", status, attempt)

            if 200 <= status < 300:
                return BookingCreated(booking_uuid=self._booking_uuid_from(response), attempts=attempt)

            if 500 <= status < 600:
                # NOT retried. A 5xx does not prove the booking was not created:
                # gateways, proxies and application handlers all return one after
                # a write has already landed, and EasyWeek offers no idempotency
                # key that would make a second POST safe.
                logger.error(
                    "easyweek_migration: create_booking server error status=%s attempt=%s — outcome UNKNOWN",
                    status,
                    attempt,
                )
                raise EasyWeekUncertainMutation(
                    "server error; outcome unknown",
                    operation="create_booking",
                    status_code=status,
                    attempts=attempt,
                )

            if status == 429:
                # The only safe retry. A rate limiter refuses the request before
                # the handler runs, so nothing was created.
                last_retryable = EasyWeekRetryableError(
                    "rate limited",
                    operation="create_booking",
                    status_code=status,
                    attempts=attempt,
                )
                if attempt < self._max_attempts:
                    retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                    await self._sleep(retry_after if retry_after is not None else _backoff_delay(attempt))
                    continue
                break

            # Permanent 4xx: the request was rejected, nothing was created, and
            # repeating it cannot change the answer.
            if status in (401, 403):
                raise EasyWeekAuthError(
                    "authentication or authorization failed",
                    operation="create_booking",
                    status_code=status,
                    attempts=attempt,
                )
            if status == 404:
                raise EasyWeekNotFoundError(
                    "resource not found", operation="create_booking", status_code=status, attempts=attempt
                )
            if status == 422:
                # The one 4xx worth describing. A validation failure is an
                # operator's problem to fix, and "422" alone sent the first
                # canary into an afternoon of archaeology. What may be reported
                # is the NAME of a field we ourselves sent; never the server's
                # prose, never a value, never the body.
                fields = _safe_validation_fields(response)
                logger.error(
                    "easyweek_migration: create_booking rejected status=422 fields=%s",
                    ",".join(fields) if fields else "unnamed",
                )
                raise EasyWeekPermanentError(
                    "request rejected as invalid: " + (",".join(fields) if fields else "no recognised field named"),
                    operation="create_booking",
                    status_code=status,
                    attempts=attempt,
                )
            raise EasyWeekPermanentError(
                "permanent client error", operation="create_booking", status_code=status, attempts=attempt
            )

        assert last_retryable is not None
        raise last_retryable

    @staticmethod
    def _booking_uuid_from(response: httpx.Response) -> str:
        """Read the created booking's UUID out of a 2xx body.

        A 2xx we cannot read is NOT a success we can record: without a UUID there
        is nothing to reconcile against and nothing to roll back. It is reported
        as uncertain, because a booking probably was created.
        """
        try:
            payload: Any = response.json()
        except Exception:
            raise EasyWeekUncertainMutation(
                "mutation succeeded but the response was not JSON", operation="create_booking"
            ) from None

        if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
            payload = payload["data"]
        if not isinstance(payload, dict):
            raise EasyWeekUncertainMutation(
                "mutation succeeded but the response was not an object", operation="create_booking"
            )

        for key in ("uuid", "uid"):
            candidate = payload.get(key)
            if isinstance(candidate, str) and candidate.strip():
                try:
                    return _canonical_booking_uuid(candidate)
                except EasyWeekPermanentError:
                    break
        raise EasyWeekUncertainMutation(
            "mutation succeeded but the response carried no usable booking uuid",
            operation="create_booking",
        )

    async def create_cart_booking(self, body: dict[str, Any]) -> BookingCreated:
        """``POST /bookings/cart``. Returns only on a PROVEN single creation.

        The same outcome taxonomy as :meth:`create_booking`, deliberately: 429 is
        the only status that permits another POST, because a rate limiter refuses
        the request before the handler runs. A timeout, a transport failure and
        every 5xx are UNKNOWN — a write may already have landed, and EasyWeek
        publishes no idempotency key that would make a second attempt safe.

        One extra refusal this endpoint needs and the plain one does not: a cart
        could in principle answer with several bookings. The canary's single item
        produced exactly one, and the ledger's whole relation is
        `one Altegio record -> one EasyWeek booking uuid` — so more than one uuid
        in the response is uncertain rather than "take the first". Somebody has
        to look at what was actually created before anything else happens.
        """
        url = "/".join([self._base_url, _PATH_BOOKINGS, _PATH_CART])
        last_retryable: EasyWeekError | None = None

        for attempt in range(1, self._max_attempts + 1):
            await self._limiter.acquire()
            try:
                response = await self._client.post(url, headers=self._headers(), json=body)
            except httpx.TimeoutException:
                logger.error("easyweek_migration: create_cart_booking timeout attempt=%s — outcome UNKNOWN", attempt)
                raise EasyWeekUncertainMutation(
                    "cart mutation timed out; outcome unknown",
                    operation="create_cart_booking",
                    attempts=attempt,
                ) from None
            except httpx.HTTPError as exc:
                logger.error(
                    "easyweek_migration: create_cart_booking transport failure attempt=%s error_type=%s"
                    " — outcome UNKNOWN",
                    attempt,
                    type(exc).__name__,
                )
                raise EasyWeekUncertainMutation(
                    "cart mutation transport failure; outcome unknown",
                    operation="create_cart_booking",
                    attempts=attempt,
                ) from None

            status = response.status_code
            logger.info("easyweek_migration: create_cart_booking status=%s attempt=%s", status, attempt)

            if 200 <= status < 300:
                return BookingCreated(booking_uuid=self._cart_booking_uuid_from(response), attempts=attempt)

            if 500 <= status < 600:
                logger.error(
                    "easyweek_migration: create_cart_booking server error status=%s attempt=%s — outcome UNKNOWN",
                    status,
                    attempt,
                )
                raise EasyWeekUncertainMutation(
                    "cart server error; outcome unknown",
                    operation="create_cart_booking",
                    status_code=status,
                    attempts=attempt,
                )

            if status == 429:
                last_retryable = EasyWeekRetryableError(
                    "rate limited",
                    operation="create_cart_booking",
                    status_code=status,
                    attempts=attempt,
                )
                if attempt < self._max_attempts:
                    retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                    await self._sleep(retry_after if retry_after is not None else _backoff_delay(attempt))
                    continue
                break

            # Permanent 4xx. The request was rejected and nothing was created —
            # including the conflict a taken slot produces, which is how an
            # unavailable source time reaches the operator as a named refusal
            # rather than as a guess (plan §30.12.3).
            fields = _safe_validation_fields(response, allowed=CART_ERROR_FIELDS)
            detail = f"; fields: {', '.join(fields)}" if fields else ""
            raise EasyWeekPermanentError(
                f"cart mutation rejected{detail}",
                operation="create_cart_booking",
                status_code=status,
                attempts=attempt,
            )

        assert last_retryable is not None
        raise last_retryable

    @staticmethod
    def _cart_booking_uuid_from(response: httpx.Response) -> str:
        """The ONE booking uuid a cart response is allowed to carry.

        A 2xx we cannot read is not a success we can record: without a uuid there
        is nothing to reconcile against and nothing to roll back. Neither is a
        2xx naming several bookings — the ledger keys one source record to one
        target, and picking one of two would leave the other unrecorded and
        unrollbackable, which is worse than admitting we do not know.
        """
        try:
            payload: Any = response.json()
        except Exception:
            raise EasyWeekUncertainMutation(
                "cart mutation succeeded but the response was not JSON", operation="create_cart_booking"
            ) from None

        found: list[str] = []
        for candidate in _cart_uuid_candidates(payload):
            try:
                canonical = _canonical_booking_uuid(candidate)
            except EasyWeekPermanentError:
                continue
            if canonical not in found:
                found.append(canonical)

        if len(found) == 1:
            return found[0]
        if not found:
            raise EasyWeekUncertainMutation(
                "cart mutation succeeded but the response carried no usable booking uuid",
                operation="create_cart_booking",
            )
        raise EasyWeekUncertainMutation(
            f"cart mutation succeeded but the response named {len(found)} bookings",
            operation="create_cart_booking",
        )

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        """``GET /bookings/{uuid}`` — used by reconcile and rollback.

        Present on the write client so reconciliation can prove an uncertain
        outcome using the same key, pacing and error taxonomy as the mutation it
        is reconciling. It never logs the body, which carries customer PII.
        """
        canonical = _canonical_booking_uuid(booking_uuid)
        url = "/".join([self._base_url, _PATH_BOOKINGS, canonical])

        last_retryable: EasyWeekError | None = None
        for attempt in range(1, self._max_attempts + 1):
            await self._limiter.acquire()
            try:
                response = await self._client.get(url, headers=self._headers())
            except httpx.TimeoutException:
                last_retryable = EasyWeekRetryableError("request timed out", operation="get_booking", attempts=attempt)
            except httpx.HTTPError:
                last_retryable = EasyWeekRetryableError("transport error", operation="get_booking", attempts=attempt)
            else:
                status = response.status_code
                if 200 <= status < 300:
                    try:
                        payload: Any = response.json()
                    except Exception:
                        raise EasyWeekProtocolError(
                            "booking response is not valid JSON", operation="get_booking", status_code=status
                        ) from None
                    if isinstance(payload, dict) and isinstance(payload.get("data"), dict):
                        payload = payload["data"]
                    if not isinstance(payload, dict):
                        raise EasyWeekProtocolError("booking response is not a JSON object", operation="get_booking")
                    return payload
                if status == 404:
                    raise EasyWeekNotFoundError(
                        "resource not found", operation="get_booking", status_code=status, attempts=attempt
                    )
                if status in (401, 403):
                    raise EasyWeekAuthError(
                        "authentication or authorization failed",
                        operation="get_booking",
                        status_code=status,
                        attempts=attempt,
                    )
                if not (status == 429 or 500 <= status < 600):
                    raise EasyWeekPermanentError(
                        "permanent client error", operation="get_booking", status_code=status, attempts=attempt
                    )
                last_retryable = EasyWeekRetryableError(
                    "retryable response status", operation="get_booking", status_code=status, attempts=attempt
                )

            if attempt < self._max_attempts:
                await self._sleep(_backoff_delay(attempt))

        assert last_retryable is not None
        raise last_retryable

    async def _get_json(self, path: str, *, params: dict[str, Any], operation: str) -> dict[str, Any]:
        """One paced, retried GET returning a JSON object. Read-only.

        Shares `get_booking`'s error taxonomy and pacing so the reads that PROVE
        a booking cannot drift from the read that fetches it. Never logs a body:
        a bookings list carries customer names and phone numbers.
        """
        url = "/".join([self._base_url, path])
        last_retryable: EasyWeekError | None = None

        for attempt in range(1, self._max_attempts + 1):
            await self._limiter.acquire()
            try:
                response = await self._client.get(url, headers=self._headers(), params=params)
            except httpx.TimeoutException:
                last_retryable = EasyWeekRetryableError("request timed out", operation=operation, attempts=attempt)
            except httpx.HTTPError:
                last_retryable = EasyWeekRetryableError("transport error", operation=operation, attempts=attempt)
            else:
                status = response.status_code
                if 200 <= status < 300:
                    try:
                        payload: Any = response.json()
                    except Exception:
                        raise EasyWeekProtocolError(
                            "response is not valid JSON", operation=operation, status_code=status
                        ) from None
                    if not isinstance(payload, dict):
                        raise EasyWeekProtocolError("response is not a JSON object", operation=operation)
                    return payload
                if status == 404:
                    raise EasyWeekNotFoundError(
                        "resource not found", operation=operation, status_code=status, attempts=attempt
                    )
                if status in (401, 403):
                    raise EasyWeekAuthError(
                        "authentication or authorization failed",
                        operation=operation,
                        status_code=status,
                        attempts=attempt,
                    )
                if not (status == 429 or 500 <= status < 600):
                    raise EasyWeekPermanentError(
                        "permanent client error", operation=operation, status_code=status, attempts=attempt
                    )
                last_retryable = EasyWeekRetryableError(
                    "retryable response status", operation=operation, status_code=status, attempts=attempt
                )

            if attempt < self._max_attempts:
                await self._sleep(_backoff_delay(attempt))

        assert last_retryable is not None
        raise last_retryable

    async def list_location_services(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        """``GET /locations/{uuid}/services`` — one page of the catalogue.

        The catalogue is how the service on a booking is proven at all: the
        booking response carries an order-line uuid, not a catalogue one. See
        `service_catalog`.
        """
        canonical = _canonical_booking_uuid(location_uuid)
        return await self._get_json(
            "/".join([_PATH_LOCATIONS, canonical, _PATH_SERVICES]),
            params={"page": page},
            operation="list_location_services",
        )

    async def list_bookings(self, *, params: dict[str, Any]) -> dict[str, Any]:
        """``GET /bookings`` — the documented filtered list.

        The only way to prove which master a booking belongs to: the booking
        response itself names no staffer. An operator probe confirmed the filter
        discriminates — the test booking appears under its own master and is
        absent under a control master.
        """
        return await self._get_json(_PATH_BOOKINGS, params=dict(params), operation="list_bookings")

    async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]:
        """``GET /customers`` — one page of the documented filtered list.

        Customers belong to the WORKSPACE, not to a location: the response
        carries no location field, and the same person is reachable from every
        branch. So this is never scoped to a branch, and a lookup that found
        nothing in Durlach has not shown that the customer is new.

        ``phone`` is an exact-match filter and EasyWeek normalises it to E.164 —
        but it travels in the URL query string, which is the one place a phone
        number reaches an access log. Callers pace and mask; this method never
        logs the URL or the body.
        """
        merged = {"per_page": _CUSTOMERS_PER_PAGE, **dict(params)}
        return await self._get_json(_PATH_CUSTOMERS, params=merged, operation="list_customers")

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        """``GET /customers/{uuid}`` — the read that PROVES a creation.

        A ``POST`` response is what the server says it did; this is what the
        workspace actually holds. The two are compared before a created customer
        is written into the directory.
        """
        canonical = _canonical_booking_uuid(customer_uuid)
        return await self._get_json(
            "/".join([_PATH_CUSTOMERS, canonical]),
            params={},
            operation="get_customer",
        )

    async def create_customer(self, body: dict[str, Any]) -> dict[str, Any]:
        """``POST /customers``. Returns the raw response only on a 2xx.

        Deliberately shaped like :meth:`create_booking`, and for the same reason:
        EasyWeek publishes no idempotency key, so a timeout or a transport
        failure is an UNKNOWN outcome, never a failed one. Retrying it blindly is
        how one person becomes two cards — and a duplicate card is worse than a
        missing one, because the booking then lands on whichever of the two the
        next lookup happens to return.

        A 4xx that is not 429 is permanent: the workspace rejects a phone or
        e-mail that another customer already holds, and the answer to that is to
        look at who holds it, never to alter the contact details until the
        collision goes away.

        The caller proves the result with :meth:`get_customer`; this method does
        not decide that a customer exists.
        """
        url = "/".join([self._base_url, _PATH_CUSTOMERS])

        await self._limiter.acquire()
        try:
            response = await self._client.post(url, headers=self._headers(), json=dict(body))
        except httpx.TimeoutException:
            raise EasyWeekUncertainMutation(
                "customer create timed out; outcome unknown", operation="create_customer"
            ) from None
        except httpx.HTTPError:
            raise EasyWeekUncertainMutation(
                "customer create transport failure; outcome unknown", operation="create_customer"
            ) from None

        status = response.status_code
        # Status only. The request body is a person's name and phone number, and
        # the response echoes them back.
        logger.info("easyweek_migration: create_customer status=%s", status)
        if 200 <= status < 300:
            try:
                payload: Any = response.json()
            except Exception:
                # 2xx with an unreadable body: the customer may well exist, and
                # there is no uuid to prove it with. Uncertain, not failed.
                raise EasyWeekUncertainMutation(
                    "customer create returned an unreadable body; outcome unknown",
                    operation="create_customer",
                    status_code=status,
                ) from None
            if not isinstance(payload, dict):
                raise EasyWeekUncertainMutation(
                    "customer create returned a non-object body; outcome unknown",
                    operation="create_customer",
                    status_code=status,
                )
            return payload
        if status in (401, 403):
            raise EasyWeekAuthError(
                "authentication or authorization failed", operation="create_customer", status_code=status
            )
        if status == 429 or 500 <= status < 600:
            # 5xx after a POST is NOT a failed POST — the write may have landed.
            raise EasyWeekUncertainMutation(
                "customer create returned a retryable status; outcome unknown",
                operation="create_customer",
                status_code=status,
            )
        raise EasyWeekPermanentError("permanent client error", operation="create_customer", status_code=status)

    async def cancel_booking(self, booking_uuid: str) -> None:
        """Cancel one booking, and PROVE it. Reached ONLY by a confirmed rollback.

        The endpoint is ``PUT /bookings/{uuid}/status/cancel``, which a live
        canary exercised successfully. The previous one — ``POST
        /bookings/{uuid}/set-booking-cancel``, taken from the plan's early
        endpoint list — answered **404** against the real API. A rollback built
        on it could never have cancelled anything: it would have raised
        not-found for every booking and left the operator with a run that
        reported failures for appointments that were still live.

        Three properties, in order:

        **Already cancelled is not cancelled again.** A ``GET`` runs first. If
        the booking is already ``is_canceled``, this returns without issuing a
        mutation at all — a rollback re-run must be idempotent, and a second
        cancel on a cancelled booking is a request whose outcome nobody has
        proven.

        **An unknown outcome is never repeated.** A timeout, a transport failure
        and every 5xx leave it genuinely unknown whether the cancel landed, and
        EasyWeek publishes no idempotency key that would make a second PUT safe.
        They raise :class:`EasyWeekUncertainMutation`; a 401/403 and every
        deterministic 4xx are permanent refusals, because nothing was changed.

        **A 2xx is not proof.** After a successful status change the booking is
        read back and ``is_canceled`` has to be true. A 2xx we cannot confirm is
        reported as uncertain and sent to manual review rather than recorded as
        a rollback — the ledger's ``rolled_back`` is a claim about a real
        appointment, and it must not rest on a status code alone.
        """
        canonical = _canonical_booking_uuid(booking_uuid)

        # 1. Is it already cancelled? Proven by a read, before any mutation.
        if await self._booking_is_canceled(canonical):
            logger.info("easyweek_migration: cancel_booking already canceled — no mutation issued")
            return

        url = "/".join([self._base_url, _PATH_BOOKINGS, canonical, _PATH_STATUS, _PATH_CANCEL])

        await self._limiter.acquire()
        try:
            response = await self._client.put(url, headers=self._headers(), json=dict(CANCEL_REQUEST_BODY))
        except httpx.TimeoutException:
            raise EasyWeekUncertainMutation("cancel timed out; outcome unknown", operation="cancel_booking") from None
        except httpx.HTTPError:
            raise EasyWeekUncertainMutation(
                "cancel transport failure; outcome unknown", operation="cancel_booking"
            ) from None

        status = response.status_code
        logger.info("easyweek_migration: cancel_booking status=%s", status)

        if 500 <= status < 600:
            # NOT retryable. A 5xx does not prove the cancel did not land, and a
            # second PUT against a booking that may already be cancelled is a
            # mutation with an unproven outcome.
            raise EasyWeekUncertainMutation(
                "cancel server error; outcome unknown", operation="cancel_booking", status_code=status
            )
        if status == 429:
            # Refused before the handler ran, so nothing changed. The caller
            # decides whether to come back; this method does not loop.
            raise EasyWeekRetryableError("rate limited", operation="cancel_booking", status_code=status)
        if status in (401, 403):
            raise EasyWeekAuthError(
                "authentication or authorization failed", operation="cancel_booking", status_code=status
            )
        if status == 404:
            raise EasyWeekNotFoundError("resource not found", operation="cancel_booking", status_code=status)
        if not (200 <= status < 300):
            raise EasyWeekPermanentError("permanent client error", operation="cancel_booking", status_code=status)

        # 2. The status code said yes; the workspace has to agree.
        if not await self._booking_is_canceled(canonical):
            raise EasyWeekUncertainMutation(
                "cancel returned success but the booking does not read as canceled",
                operation="cancel_booking",
                status_code=status,
            )

    async def _booking_is_canceled(self, canonical_uuid: str) -> bool:
        """Read one booking and answer whether it is cancelled, or raise.

        ``is_canceled`` is read strictly: anything that is not a literal boolean
        is a shape we have not proven, and reading it loosely here would let an
        unparseable field pass as "already cancelled" and skip the mutation
        entirely.
        """
        payload = await self.get_booking(canonical_uuid)
        body = payload.get("data") if isinstance(payload.get("data"), dict) else payload
        if not isinstance(body, dict):
            raise EasyWeekProtocolError("booking response is not an object", operation="cancel_booking")
        flag = body.get("is_canceled")
        if not isinstance(flag, bool):
            raise EasyWeekProtocolError("booking is_canceled is not a boolean", operation="cancel_booking")
        return flag
