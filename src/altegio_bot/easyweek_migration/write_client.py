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

# EasyWeek allows 60 requests/min per key (plan §1.1). The migration is the only
# thing that ever runs at volume against that budget, and the shared outbox
# worker's reminder guard is using the same key at the same time — so the
# cutover deliberately takes well under half of it.
DEFAULT_REQUESTS_PER_MINUTE: Final = 24

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


def _safe_validation_fields(response: httpx.Response) -> list[str]:
    """Field NAMES a 422 complained about, filtered to ones we sent.

    Deliberately narrow. A validation body is written by the server and can carry
    anything — the submitted phone number echoed back, a stack frame, an internal
    column name. So nothing is read out of it except keys, and a key survives only
    if it is a field this migration itself put in the request.

    A workspace that requires an undocumented Form Builder field therefore shows
    up as "no recognised field named", which is the correct answer: we cannot fix
    it by guessing, and the runbook says to ask the operator.
    """
    try:
        payload: Any = response.json()
    except Exception:
        return []
    if not isinstance(payload, dict):
        return []

    candidates: set[str] = set()
    # Laravel-shaped: {"errors": {"field": [...]}}, and the flat variant.
    for container in (payload.get("errors"), payload):
        if isinstance(container, dict):
            candidates.update(key for key in container if isinstance(key, str))
    return sorted(candidates & BOOKING_REQUEST_FIELDS)


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

    async def cancel_booking(self, booking_uuid: str) -> None:
        """Cancel one booking. Reached ONLY by a confirmed rollback.

        Uses the plan's documented ``set-booking-cancel`` action (§1.1). Like
        ``create_booking``, an unknown outcome is raised as uncertain rather than
        retried: a cancel that may or may not have landed must be looked at, not
        repeated.
        """
        canonical = _canonical_booking_uuid(booking_uuid)
        url = "/".join([self._base_url, _PATH_BOOKINGS, canonical, "set-booking-cancel"])

        await self._limiter.acquire()
        try:
            response = await self._client.post(url, headers=self._headers(), json={})
        except httpx.TimeoutException:
            raise EasyWeekUncertainMutation("cancel timed out; outcome unknown", operation="cancel_booking") from None
        except httpx.HTTPError:
            raise EasyWeekUncertainMutation(
                "cancel transport failure; outcome unknown", operation="cancel_booking"
            ) from None

        status = response.status_code
        logger.info("easyweek_migration: cancel_booking status=%s", status)
        if 200 <= status < 300:
            return
        if status in (401, 403):
            raise EasyWeekAuthError(
                "authentication or authorization failed", operation="cancel_booking", status_code=status
            )
        if status == 404:
            raise EasyWeekNotFoundError("resource not found", operation="cancel_booking", status_code=status)
        if status == 429 or 500 <= status < 600:
            raise EasyWeekRetryableError("retryable cancel status", operation="cancel_booking", status_code=status)
        raise EasyWeekPermanentError("permanent client error", operation="cancel_booking", status_code=status)
