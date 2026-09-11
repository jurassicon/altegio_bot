"""Read-only client for the EasyWeek Public API v2 (INTEGRATION_PLAN §1.1, PR-2).

The EasyWeek integration is deliberately **GET-only** here: the plan's hard
rules forbid mutation calls, so this transport exposes only reviewed read
operations and no generic request escape hatch::

    GET /ping
    GET /locations
    GET /locations/{location_uuid}/services
    GET /bookings/{booking_uuid}
    GET /customers/{customer_uuid}
    GET /bookings?customer_uuid={customer_uuid}&page={page}&per_page=100
    GET /workspace
    GET /voucher-templates
    GET /voucher-templates/{voucher_template_uuid}
    GET /orders?location_uuid&customer_uuid&page&per_page=100
    GET /orders/{order_uuid}
    GET /locations/{location_uuid}/accounts
    GET /locations/{location_uuid}/staffers?page&per_page=100

Everything here is built around two threats:

* **Secret leakage.** The Bearer key and the ``Workspace`` header must never
  reach a log record, an exception message, a ``repr``, or the probe's stdout.
* **Customer PII leakage.** ``GET /bookings/{uuid}`` returns a ``customer``
  subtree plus notes and order totals. This client therefore never logs a
  response body, and its exceptions carry only metadata (operation, HTTP status,
  attempt count, retryable flag) — never bodies, headers, URLs or PII.

The last four were added for the §35 voucher-canary reconciliation and are
still reads: proving what a POS mutation did needs a way to look, and looking
must not require the mutation client. The mutation surface itself lives in
``easyweek_voucher_mutation`` and is not reachable from here.

Deliberately NOT here: EasyWeek domain modelling. PR-4 owns normalization; PR-2
only needs the transport, a safe JSON-shape check, and typed failures.
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
import uuid as uuid_module
from types import TracebackType
from typing import Any, Awaitable, Callable, Final, Mapping
from urllib.parse import urlsplit

import httpx

from altegio_bot.settings import settings

logger = logging.getLogger("easyweek_client")

# Relative paths this client is allowed to build. A caller can never pass a URL:
# every public method maps to one of these constants, so neither a hostile
# booking id nor a redirect can retarget the request at another host.
_PATH_PING = "ping"
_PATH_LOCATIONS = "locations"
_PATH_SERVICES = "services"
_PATH_BOOKINGS = "bookings"
_PATH_CUSTOMERS = "customers"
_PATH_WORKSPACE = "workspace"
_PATH_VOUCHER_TEMPLATES = "voucher-templates"
# Read-only POS surface, added for the §35 voucher-canary reconciliation. The
# mutation side lives in `easyweek_voucher_mutation`; these are GETs only.
_PATH_ORDERS = "orders"
_PATH_ACCOUNTS = "accounts"
_PATH_STAFFERS = "staffers"

# The ONE origin this client may ever talk to. A misconfigured base URL would
# otherwise send the Bearer key in clear text or to a third-party host, so the
# scheme, host, port and path are all pinned rather than merely "looks like a
# URL" (INTEGRATION_PLAN §1.1).
_ALLOWED_API_SCHEME = "https"
_ALLOWED_API_HOST = "my.easyweek.io"
_ALLOWED_API_PATH = "/api/public/v2"
_ALLOWED_API_PORTS = (None, 443)
CANONICAL_API_BASE_URL = f"{_ALLOWED_API_SCHEME}://{_ALLOWED_API_HOST}{_ALLOWED_API_PATH}"
CUSTOMER_BOOKINGS_PER_PAGE: Final = 100
# One fixed page size for every POS read. A caller that could choose it could
# also ask for a page so small that a complete walk silently truncates.
POS_PER_PAGE: Final = 100

# Bounded retry policy. EasyWeek allows 60 requests/min per key (§1.1), so a
# short, bounded backoff is enough; unbounded retries would only burn the quota.
_MAX_ATTEMPTS = 3
_BACKOFF_BASE_SEC = 0.5
_BACKOFF_MAX_SEC = 8.0
# Hard ceiling for a server-provided Retry-After, so a hostile or mistaken
# header can never park an operator probe (or a worker) for hours.
_RETRY_AFTER_MAX_SEC = 10.0

_DEFAULT_TIMEOUT = httpx.Timeout(connect=5.0, read=15.0, write=10.0, pool=5.0)


def _is_retryable_status(status: int) -> bool:
    """Rate limiting plus the WHOLE 5xx range is worth another attempt.

    Deliberately a range test, not a hand-picked allowlist: a server-side status
    we did not enumerate (505, 507, 599, a proxy's own 5xx) is still a server
    problem, and treating it as permanent would drop a recoverable request.
    """
    return status == 429 or 500 <= status < 600


# ---------------------------------------------------------------------------
# Typed errors
# ---------------------------------------------------------------------------


class EasyWeekError(Exception):
    """Base class for every EasyWeek client failure.

    The string form is intentionally metadata-only. Callers that want to explain
    a failure to a human must use these fields, never a captured response.
    """

    retryable = False

    def __init__(
        self,
        message: str,
        *,
        operation: str | None = None,
        status_code: int | None = None,
        attempts: int | None = None,
    ) -> None:
        self.operation = operation
        self.status_code = status_code
        self.attempts = attempts
        parts = [message]
        if operation:
            parts.append(f"operation={operation}")
        if status_code is not None:
            parts.append(f"status={status_code}")
        if attempts is not None:
            parts.append(f"attempts={attempts}")
        parts.append(f"retryable={self.retryable}")
        super().__init__(" ".join(parts))

    @property
    def safe_summary(self) -> dict[str, Any]:
        """Metadata-only description, safe to print or serialise."""
        return {
            "error": type(self).__name__,
            "operation": self.operation,
            "status": self.status_code,
            "attempts": self.attempts,
            "retryable": self.retryable,
        }


class EasyWeekConfigError(EasyWeekError):
    """API key / workspace slug / base URL is missing or unusable."""


class EasyWeekAuthError(EasyWeekError):
    """401 / 403 — the key or workspace is not accepted. Never retried."""


class EasyWeekNotFoundError(EasyWeekError):
    """404 — the resource does not exist for this workspace. Never retried."""


class EasyWeekPermanentError(EasyWeekError):
    """A permanent 4xx (400/422/…) response. Retrying cannot help."""


class EasyWeekRetryableError(EasyWeekError):
    """429 / 5xx / timeout / transport failure that survived every attempt."""

    retryable = True


class EasyWeekProtocolError(EasyWeekError):
    """A 2xx response whose body is not the JSON shape the endpoint promises.

    Deliberately NOT retryable: a well-formed HTTP 200 with the wrong shape is a
    contract problem, and repeating the call would only mask it.
    """


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _normalize_base_url(raw: object) -> str:
    """Pin the base URL to the one canonical EasyWeek Public API v2 origin.

    Every request carries the Bearer key, so a configuration slip must not be
    able to send it in clear text (``http://``) or to a host that merely looks
    like EasyWeek. Scheme, host, port and path are therefore all checked against
    fixed values instead of being accepted as "some absolute URL"; only a
    trailing slash is tolerated and normalised away.

    The rejected value is never echoed into the error: an operator could paste a
    URL that already carries a token in its query string, and that must not end
    up in a log or a ticket.
    """
    if not isinstance(raw, str) or not raw.strip():
        raise EasyWeekConfigError("EASYWEEK_API_BASE_URL is not configured")

    split = urlsplit(raw.strip())

    if split.scheme != _ALLOWED_API_SCHEME:
        raise EasyWeekConfigError("EASYWEEK_API_BASE_URL must use https")
    if split.username or split.password:
        raise EasyWeekConfigError("EASYWEEK_API_BASE_URL must not carry credentials")
    if split.query or split.fragment:
        raise EasyWeekConfigError("EASYWEEK_API_BASE_URL must not carry query or fragment")

    hostname = (split.hostname or "").lower()
    if hostname != _ALLOWED_API_HOST:
        raise EasyWeekConfigError("EASYWEEK_API_BASE_URL host is not the expected EasyWeek API host")

    try:
        port = split.port
    except ValueError:
        raise EasyWeekConfigError("EASYWEEK_API_BASE_URL has an invalid port") from None
    if port not in _ALLOWED_API_PORTS:
        raise EasyWeekConfigError("EASYWEEK_API_BASE_URL must use the default https port")

    # Only a trailing slash may differ; '/api/public/v2/../x' or any other path
    # is a different endpoint and is rejected rather than silently resolved.
    if split.path.rstrip("/") != _ALLOWED_API_PATH:
        raise EasyWeekConfigError("EASYWEEK_API_BASE_URL path is not the expected EasyWeek API v2 path")

    return CANONICAL_API_BASE_URL


def _unwrap_secret(value: object) -> object:
    """Return the plain string behind a ``SecretStr`` (or the value unchanged).

    Settings store the API key as ``SecretStr`` so it cannot leak through a model
    repr; the real value is only ever unwrapped here, at the point of building
    the Authorization header.
    """
    getter = getattr(value, "get_secret_value", None)
    if callable(getter):
        return getter()
    return value


def _canonical_booking_uuid(value: object) -> str:
    """Return the canonical UUID string for *value* or raise.

    Validated BEFORE any request is built: the booking id is the only
    caller-controlled part of a path, so anything that is not a real UUID
    (``../``, an absolute URL, a query string) must never reach the wire.
    """
    return _canonical_resource_uuid(value, operation="get_booking", label="booking_uuid")


def _canonical_resource_uuid(value: object, *, operation: str, label: str) -> str:
    """Validate a path UUID before construction, with operation-safe errors."""
    if not isinstance(value, str) or not value.strip():
        raise EasyWeekPermanentError(f"{label} must be a non-empty string", operation=operation)
    try:
        parsed = uuid_module.UUID(value.strip())
    except (ValueError, AttributeError, TypeError):
        raise EasyWeekPermanentError(f"{label} is not a valid UUID", operation=operation) from None
    return str(parsed)


def _canonical_customer_uuid(value: object, *, operation: str) -> str:
    """Return an exact canonical customer UUID without exposing the input."""
    if not isinstance(value, str):
        raise EasyWeekPermanentError("customer_uuid must be a canonical UUID", operation=operation)
    try:
        canonical = str(uuid_module.UUID(value))
    except (ValueError, AttributeError, TypeError):
        raise EasyWeekPermanentError("customer_uuid must be a canonical UUID", operation=operation) from None
    if value != canonical:
        raise EasyWeekPermanentError("customer_uuid must be a canonical UUID", operation=operation)
    return canonical


def _positive_page(value: object, *, operation: str = "list_customer_bookings") -> int:
    if type(value) is not int or value < 1:
        raise EasyWeekPermanentError("page must be a positive integer", operation=operation)
    return value


def _has_usable_timezone(value: object) -> bool:
    """True when *value* is a timezone this integration can act on.

    The live API returns an object — ``{"name": "...", "offset": "...",
    "short": "..."}`` — while the documented/legacy shape is a bare IANA string.
    Both are accepted; anything else is not.

    Only ``name`` is required. ``offset`` and ``short`` are presentation details
    that EasyWeek may add, drop or change without the branch becoming
    unidentifiable, so requiring them would turn an upstream cosmetic change into
    an outage. This is a *shape* check only — the value is neither normalised nor
    rewritten here; see :meth:`EasyWeekClient.list_locations`.
    """
    if isinstance(value, str):
        return bool(value.strip())
    if isinstance(value, dict):
        name = value.get("name")
        return isinstance(name, str) and bool(name.strip())
    return False


def _validated_location(item: object) -> dict[str, Any]:
    """Return *item* if it is a usable location entry, otherwise raise.

    The seed reads this list to verify registry UUIDs independently, so an
    entry is only usable when it can actually be identified AND referenced. A bare
    ``{}`` used to pass the "is a dict" check and reach the probe as
    ``{"uuid": null, "name": null, "timezone": null}`` — an operator could not act
    on that, and printing it as a success was worse than failing.

    ``uuid`` must parse as a real UUID (which also rules out a value carrying a
    query, a path traversal or free text), ``name`` must be a non-blank string,
    and ``timezone`` must satisfy :func:`_has_usable_timezone`. That is the whole
    contract here: timezone/domain normalization stays with PR-4, so the entry is
    returned exactly as received — an object timezone stays an object.

    The offending value is never logged nor put in the error message.
    """
    if not isinstance(item, dict):
        raise EasyWeekProtocolError("locations response contains a non-object entry", operation="list_locations")

    raw_uuid = item.get("uuid")
    if not isinstance(raw_uuid, str) or not raw_uuid.strip():
        raise EasyWeekProtocolError("location entry has no usable uuid", operation="list_locations")
    try:
        uuid_module.UUID(raw_uuid.strip())
    except (ValueError, AttributeError, TypeError):
        raise EasyWeekProtocolError("location entry uuid is not a valid UUID", operation="list_locations") from None

    name = item.get("name")
    if not isinstance(name, str) or not name.strip():
        # The field NAME is a fixed literal; the bad value is never included.
        raise EasyWeekProtocolError("location entry has no usable name", operation="list_locations")

    if not _has_usable_timezone(item.get("timezone")):
        raise EasyWeekProtocolError("location entry has no usable timezone", operation="list_locations")

    return item


def _parse_retry_after(raw: str | None) -> float | None:
    """Parse a ``Retry-After`` delay in seconds, clamped to a safe maximum.

    Only the numeric-seconds form is honoured; an HTTP-date form (or anything
    unparsable) simply falls back to the normal backoff. Negative values are
    ignored and large values are capped, so the header can never stall a caller.
    """
    if raw is None:
        return None
    try:
        seconds = float(raw.strip())
    except (ValueError, AttributeError):
        return None
    if seconds < 0:
        return None
    return min(seconds, _RETRY_AFTER_MAX_SEC)


def _backoff_delay(attempt: int) -> float:
    """Bounded exponential backoff with jitter for *attempt* (1-based)."""
    ceiling = min(_BACKOFF_BASE_SEC * (2 ** (attempt - 1)), _BACKOFF_MAX_SEC)
    return random.uniform(0.0, ceiling)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class EasyWeekClient:
    """GET-only async client for the EasyWeek Public API v2.

    Usage::

        async with EasyWeekClient() as client:
            await client.ping()

    ``transport``, ``http_client`` and ``sleep`` exist for dependency injection
    in tests: the unit suite drives a ``MockTransport`` and a recording sleep, so
    it never touches the network and never actually waits.
    """

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
        max_attempts: int = _MAX_ATTEMPTS,
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

        if http_client is not None:
            self._client = http_client
            self._owns_client = False
        else:
            self._client = httpx.AsyncClient(
                timeout=timeout or _DEFAULT_TIMEOUT,
                # A redirect could send the Authorization header to another host.
                follow_redirects=False,
                transport=transport,
            )
            self._owns_client = True

    # -- lifecycle ---------------------------------------------------------

    async def aclose(self) -> None:
        """Close the underlying HTTP client if this instance created it."""
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> EasyWeekClient:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    def __repr__(self) -> str:
        # No key, no slug, no headers: a repr lands in logs and tracebacks.
        return f"<EasyWeekClient base_url={self._base_url!r}>"

    __str__ = __repr__

    # -- internals ---------------------------------------------------------

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self._api_key}",
            "Workspace": self._workspace_slug,
            "Accept": "application/json",
        }

    async def _get_json(
        self,
        *path_segments: str,
        operation: str,
        params: Mapping[str, str | int] | None = None,
    ) -> Any:
        """Issue a bounded-retry GET against a known relative path.

        The URL is assembled here from vetted constants and an already-validated
        UUID; callers cannot supply a URL. Only 429/5xx/timeout/transport
        failures are retried — every permanent 4xx and every malformed 2xx fails
        immediately, because repeating them cannot change the answer.
        """
        url = "/".join([self._base_url, *path_segments])
        last_error: EasyWeekError | None = None

        for attempt in range(1, self._max_attempts + 1):
            started = time.monotonic()
            try:
                response = await self._client.get(url, headers=self._headers(), params=params)
            except httpx.TimeoutException:
                last_error = EasyWeekRetryableError("request timed out", operation=operation, attempts=attempt)
                logger.warning(
                    "easyweek: request timeout operation=%s attempt=%s/%s",
                    operation,
                    attempt,
                    self._max_attempts,
                )
            except httpx.HTTPError as exc:
                # Only the exception CLASS is logged: an httpx error message can
                # embed the full request URL.
                last_error = EasyWeekRetryableError("transport error", operation=operation, attempts=attempt)
                logger.warning(
                    "easyweek: transport error operation=%s attempt=%s/%s error_type=%s",
                    operation,
                    attempt,
                    self._max_attempts,
                    type(exc).__name__,
                )
            else:
                elapsed_ms = int((time.monotonic() - started) * 1000)
                status = response.status_code
                logger.info(
                    "easyweek: %s status=%s attempt=%s/%s elapsed_ms=%s",
                    operation,
                    status,
                    attempt,
                    self._max_attempts,
                    elapsed_ms,
                )

                if 200 <= status < 300:
                    return self._decode_json(response, operation=operation)

                self._raise_for_permanent_status(status, operation=operation, attempt=attempt)

                # Retryable status (429 / 5xx).
                last_error = EasyWeekRetryableError(
                    "retryable response status",
                    operation=operation,
                    status_code=status,
                    attempts=attempt,
                )
                retry_after = _parse_retry_after(response.headers.get("Retry-After"))
                if attempt < self._max_attempts:
                    await self._sleep(retry_after if retry_after is not None else _backoff_delay(attempt))
                    continue

            if attempt < self._max_attempts:
                await self._sleep(_backoff_delay(attempt))

        assert last_error is not None  # loop always sets it before exhausting
        logger.error(
            "easyweek: %s exhausted retries attempts=%s error_type=%s",
            operation,
            self._max_attempts,
            type(last_error).__name__,
        )
        raise last_error

    @staticmethod
    def _raise_for_permanent_status(status: int, *, operation: str, attempt: int) -> None:
        """Raise the typed permanent error for *status*, or return if retryable."""
        if _is_retryable_status(status):
            return
        if status in (401, 403):
            raise EasyWeekAuthError(
                "authentication or authorization failed",
                operation=operation,
                status_code=status,
                attempts=attempt,
            )
        if status == 404:
            raise EasyWeekNotFoundError("resource not found", operation=operation, status_code=status, attempts=attempt)
        if 400 <= status < 500:
            raise EasyWeekPermanentError(
                "permanent client error", operation=operation, status_code=status, attempts=attempt
            )
        # A non-retryable 5xx (e.g. 501) is still permanent for our purposes.
        raise EasyWeekPermanentError(
            "unexpected response status", operation=operation, status_code=status, attempts=attempt
        )

    @staticmethod
    def _decode_json(response: httpx.Response, *, operation: str) -> Any:
        """Parse a successful body as JSON without ever echoing it."""
        try:
            return response.json()
        except Exception:
            raise EasyWeekProtocolError(
                "response body is not valid JSON",
                operation=operation,
                status_code=response.status_code,
            ) from None

    # -- public GET-only API ----------------------------------------------

    async def ping(self) -> dict[str, Any]:
        """``GET /ping`` — verify that the API key and workspace slug work.

        A 200 alone proves nothing: a captive portal, a proxy error page or the
        wrong endpoint can all answer 200 with arbitrary JSON. The documented
        success marker ``{"ping": "pong"}`` must actually be present, otherwise
        the probe would report a healthy API that was never reached. Extra
        fields (``version``, …) are allowed.
        """
        payload = await self._get_json(_PATH_PING, operation="ping")
        if not isinstance(payload, dict):
            raise EasyWeekProtocolError("ping response is not a JSON object", operation="ping")
        if payload.get("ping") != "pong":
            raise EasyWeekProtocolError("ping response did not confirm the API", operation="ping")
        return payload

    async def list_locations(self) -> list[dict[str, Any]]:
        """``GET /locations`` — locations this API key can see.

        One key may legitimately see several locations (§1.6 p.5), which is why
        the seed matches every registry UUID to a human-readable API location name.
        A bare list and the documented ``{"data": [...]}`` envelope are both
        accepted.

        Malformed entries are NOT dropped silently: quietly discarding one would
        hide exactly the case where the operator then picks a UUID from an
        incomplete list. Any entry that is not a usable location — see
        :func:`_validated_location` for the required ``uuid`` / ``name`` /
        ``timezone`` contract — fails the whole call.

        Entries are returned verbatim. In particular a live ``timezone`` object is
        NOT collapsed to its name here: rewriting the transport payload would be
        domain normalization (PR-4), and it would hide the real API shape from
        anything reading this list. The operator probe does that projection for
        display instead. Envelope ``links``/``meta`` are ignored — pagination is
        out of scope for PR-2.
        """
        payload = await self._get_json(_PATH_LOCATIONS, operation="list_locations")

        if isinstance(payload, dict):
            if "data" not in payload:
                raise EasyWeekProtocolError("locations envelope has no data key", operation="list_locations")
            items: Any = payload["data"]
        elif isinstance(payload, list):
            items = payload
        else:
            raise EasyWeekProtocolError(
                "locations response is neither a list nor a data envelope", operation="list_locations"
            )

        if not isinstance(items, list):
            raise EasyWeekProtocolError("locations data is not a JSON list", operation="list_locations")
        return [_validated_location(item) for item in items]

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        """``GET /bookings/{uuid}`` — read one booking.

        The response contains customer PII, so the caller is responsible for
        projecting only safe fields; this client never logs the body.

        If a ``data`` key is present it MUST hold the booking object — falling
        back to the outer envelope when ``data`` is null/list/scalar would turn a
        broken response into a "successfully read" booking. A minimal identity
        check (a usable ``uuid``) keeps an arbitrary JSON object from passing as
        a booking; full domain validation belongs to PR-4, not here.
        """
        canonical = _canonical_booking_uuid(booking_uuid)
        payload = await self._get_json(_PATH_BOOKINGS, canonical, operation="get_booking")

        if isinstance(payload, dict) and "data" in payload:
            inner = payload["data"]
            if not isinstance(inner, dict):
                raise EasyWeekProtocolError("booking data is not a JSON object", operation="get_booking")
            payload = inner

        if not isinstance(payload, dict):
            raise EasyWeekProtocolError("booking response is not a JSON object", operation="get_booking")

        uid = payload.get("uuid")
        if not (isinstance(uid, str) and uid.strip()):
            raise EasyWeekProtocolError("booking response has no usable uuid", operation="get_booking")
        return payload

    async def list_location_services(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        """Read one page of a location's catalogue through the pinned GET path.

        Pagination completeness and row validation are deliberately owned by
        ``easyweek_migration.service_catalog.read_full_catalog_rows``.  Keeping
        this method transport-only lets normal runtime reuse that reviewed
        parser without importing the mutating migration client.
        """
        canonical = _canonical_resource_uuid(
            location_uuid,
            operation="list_location_services",
            label="location_uuid",
        )
        exact_page = _positive_page(page, operation="list_location_services")
        payload = await self._get_json(
            _PATH_LOCATIONS,
            canonical,
            _PATH_SERVICES,
            operation="list_location_services",
            params={"page": exact_page},
        )
        if not isinstance(payload, dict):
            raise EasyWeekProtocolError(
                "location services response is not a JSON object",
                operation="list_location_services",
            )
        return payload

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]:
        """``GET /customers/{uuid}`` with strict, pre-wire identity validation."""
        canonical = _canonical_customer_uuid(customer_uuid, operation="get_customer")
        payload = await self._get_json(_PATH_CUSTOMERS, canonical, operation="get_customer")
        if isinstance(payload, dict) and "data" in payload:
            payload = payload["data"]
        if not isinstance(payload, dict):
            raise EasyWeekProtocolError("customer response is not a JSON object", operation="get_customer")
        return payload

    async def list_customer_bookings(
        self,
        customer_uuid: str,
        page: int,
        per_page: int = CUSTOMER_BOOKINGS_PER_PAGE,
    ) -> dict[str, Any]:
        """Read one fixed-size page from ``GET /bookings?customer_uuid=...``."""
        canonical = _canonical_customer_uuid(customer_uuid, operation="list_customer_bookings")
        exact_page = _positive_page(page)
        if type(per_page) is not int or per_page != CUSTOMER_BOOKINGS_PER_PAGE:
            raise EasyWeekPermanentError(
                "per_page must equal the fixed customer history page size",
                operation="list_customer_bookings",
            )
        payload = await self._get_json(
            _PATH_BOOKINGS,
            operation="list_customer_bookings",
            params={"customer_uuid": canonical, "page": exact_page, "per_page": per_page},
        )
        if not isinstance(payload, dict):
            raise EasyWeekProtocolError(
                "customer bookings response is not a JSON object",
                operation="list_customer_bookings",
            )
        return payload

    async def get_workspace(self) -> dict[str, Any]:
        """``GET /workspace`` for provider-scoped readiness evidence."""
        payload = await self._get_json(_PATH_WORKSPACE, operation="get_workspace")
        if isinstance(payload, dict) and "data" in payload:
            payload = payload["data"]
        if not isinstance(payload, dict):
            raise EasyWeekProtocolError("workspace response is not a JSON object", operation="get_workspace")
        return payload

    async def list_voucher_templates(self) -> list[dict[str, Any]]:
        """``GET /voucher-templates`` without interpreting product semantics."""
        payload = await self._get_json(_PATH_VOUCHER_TEMPLATES, operation="list_voucher_templates")
        if isinstance(payload, dict):
            payload = payload.get("data")
        if not isinstance(payload, list) or any(not isinstance(item, dict) for item in payload):
            raise EasyWeekProtocolError(
                "voucher templates response is not a JSON list",
                operation="list_voucher_templates",
            )
        return payload

    async def get_voucher_template(self, voucher_template_uuid: str) -> dict[str, Any]:
        """Documented ``GET`` by API UUID; numeric dashboard ids are rejected."""
        try:
            canonical = str(uuid_module.UUID(voucher_template_uuid))
        except (ValueError, AttributeError, TypeError):
            raise EasyWeekProtocolError(
                "voucher template identity is not a UUID",
                operation="get_voucher_template",
            ) from None
        if canonical != voucher_template_uuid:
            raise EasyWeekProtocolError(
                "voucher template identity is not canonical",
                operation="get_voucher_template",
            )
        payload = await self._get_json(
            _PATH_VOUCHER_TEMPLATES,
            canonical,
            operation="get_voucher_template",
        )
        if isinstance(payload, dict) and "data" in payload:
            payload = payload["data"]
        if not isinstance(payload, dict):
            raise EasyWeekProtocolError(
                "voucher template response is not a JSON object",
                operation="get_voucher_template",
            )
        return payload

    # -- POS reads for the §35 voucher canary ------------------------------
    #
    # Reconciliation after an unknown mutation is the ONLY reason these exist.
    # They are GETs with a closed filter set: there is no parameter through
    # which a caller could pass a free-form query, a different page size, or an
    # arbitrary endpoint. Everything they return is handed to a caller that
    # projects safe facts out of it; this client still never logs a body.

    async def list_location_customer_orders(
        self,
        *,
        location_uuid: str,
        customer_uuid: str,
        page: int,
        per_page: int = POS_PER_PAGE,
    ) -> dict[str, Any]:
        """``GET /orders`` — one page, scoped to one branch and one customer.

        Exactly four query parameters leave this method: ``location_uuid``,
        ``customer_uuid``, ``page`` and the fixed ``per_page``. There is no way
        to add a fifth: no params mapping, no keyword passthrough, no caller
        URL.

        **No ``staffer_uuid``.** A production probe on 11.09.2026 ran the same
        listing twice against a real, confirmed-existing voucher order. With
        ``location_uuid`` + ``customer_uuid`` + ``staffer_uuid`` the walk
        completed and returned one unrelated row: the target was not in it. With
        ``location_uuid`` + ``customer_uuid`` alone the walk completed and
        returned the target, matching on marker, customer and the local window.
        Why the provider excludes it is not something the probe established, and
        nothing here guesses — what was proven is that adding the filter hides
        the order this canary must find, so the canary does not send it.

        The consequence is stated rather than hidden: this listing proves the
        branch and the customer, and it does **not** prove a remote staffer
        attribution. The staffer is still mandatory everywhere it is actually
        provable — runtime identity, live Karlsruhe membership, the CREATE
        request, the identity fingerprint and the durable ledger binding.

        **No server-side date filters.** Two probe calls passing
        ``created_at_from``/``created_at_to`` built from the ledger window with
        ``datetime.isoformat()`` were answered 422, consistently. That proves
        this date form is unusable here, and nothing more general — so the
        bounded create window is checked locally, against each row's own
        timezone-aware ``created_at``.

        Pagination completeness is the caller's contract, not this method's — it
        returns one page verbatim, ``meta`` included, so the reconciler can prove
        it walked all of them rather than guessing from an empty page.
        """
        canonical_location = _canonical_resource_uuid(
            location_uuid, operation="list_location_customer_orders", label="location_uuid"
        )
        canonical_customer = _canonical_customer_uuid(customer_uuid, operation="list_location_customer_orders")
        exact_page = _positive_page(page, operation="list_location_customer_orders")
        if type(per_page) is not int or per_page != POS_PER_PAGE:
            raise EasyWeekPermanentError(
                "per_page must equal the fixed POS page size",
                operation="list_location_customer_orders",
            )
        payload = await self._get_json(
            _PATH_ORDERS,
            operation="list_location_customer_orders",
            params={
                "location_uuid": canonical_location,
                "customer_uuid": canonical_customer,
                "page": exact_page,
                "per_page": per_page,
            },
        )
        if not isinstance(payload, dict):
            raise EasyWeekProtocolError(
                "orders response is not a JSON object",
                operation="list_location_customer_orders",
            )
        return payload

    async def get_order(self, order_uuid: str) -> dict[str, Any]:
        """``GET /orders/{uuid}`` for one exact POS order.

        The response may carry customer PII and, after a voucher sale, whatever
        an issued voucher looks like. Nothing is interpreted here beyond one
        thing: the body has to be the order that was asked for.

        That check belongs at the transport, not at the caller. Every later
        proof — the marker, the customer, the open state, the voucher line —
        reads whatever body came back, so a body for a different order would
        have all of those proofs answer about somebody else's order while the
        ledger, the claim and the payment still name ours.

        The refusal carries neither the requested nor the observed UUID: it is a
        transport error whose message ends up in logs.
        """
        canonical = _canonical_resource_uuid(order_uuid, operation="get_order", label="order_uuid")
        payload = await self._get_json(_PATH_ORDERS, canonical, operation="get_order")
        if isinstance(payload, dict) and "data" in payload:
            inner = payload["data"]
            if not isinstance(inner, dict):
                raise EasyWeekProtocolError("order data is not a JSON object", operation="get_order")
            payload = inner
        if not isinstance(payload, dict):
            raise EasyWeekProtocolError("order response is not a JSON object", operation="get_order")

        body_uuid = payload.get("uuid")
        if not isinstance(body_uuid, str) or not body_uuid:
            raise EasyWeekProtocolError("order response carries no uuid", operation="get_order")
        try:
            body_canonical = str(uuid_module.UUID(body_uuid))
        except (ValueError, AttributeError, TypeError):
            raise EasyWeekProtocolError("order response uuid is not a uuid", operation="get_order") from None
        if body_canonical != body_uuid:
            # An upper-case or otherwise non-canonical spelling is not this
            # order proven; it is a body we cannot compare reliably.
            raise EasyWeekProtocolError("order response uuid is not canonical", operation="get_order")
        if body_canonical != canonical:
            raise EasyWeekProtocolError("order response identifies a different order", operation="get_order")
        return payload

    async def list_location_accounts(self, location_uuid: str) -> Any:
        """``GET /locations/{location_uuid}/accounts`` — the branch's POS accounts.

        The documented path nests the account collection under the location;
        there is no workspace-wide ``/accounts?location_uuid=`` endpoint, and
        building one would have been a URL this API does not serve.

        Deliberately takes no page: the documented response is a plain
        collection, not a paginated one. Asking for a page it does not implement
        would either be ignored or change the meaning of the answer, and neither
        is something a payment pre-check should rely on. The payload is returned
        verbatim — bare list or ``data`` envelope — and the caller decides.
        """
        canonical = _canonical_resource_uuid(location_uuid, operation="list_location_accounts", label="location_uuid")
        payload = await self._get_json(
            _PATH_LOCATIONS,
            canonical,
            _PATH_ACCOUNTS,
            operation="list_location_accounts",
        )
        if not isinstance(payload, (dict, list)):
            raise EasyWeekProtocolError(
                "accounts response is neither a list nor an object",
                operation="list_location_accounts",
            )
        return payload

    async def list_location_staffers(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        """``GET /locations/{location_uuid}/staffers`` — one page of staffers.

        The documented path nests staffers under the location, and this response
        DOES carry pagination metadata. The page is returned verbatim, ``meta``
        included, so a caller can prove a complete walk from ``last_page``
        instead of inferring the end from one empty page.
        """
        canonical = _canonical_resource_uuid(location_uuid, operation="list_location_staffers", label="location_uuid")
        exact_page = _positive_page(page, operation="list_location_staffers")
        payload = await self._get_json(
            _PATH_LOCATIONS,
            canonical,
            _PATH_STAFFERS,
            operation="list_location_staffers",
            params={"page": exact_page, "per_page": POS_PER_PAGE},
        )
        if not isinstance(payload, dict):
            raise EasyWeekProtocolError(
                "staffers response is not a JSON object",
                operation="list_location_staffers",
            )
        return payload
