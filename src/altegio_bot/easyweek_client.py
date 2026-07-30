"""Read-only client for the EasyWeek Public API v2 (INTEGRATION_PLAN §1.1, PR-2).

Phase 1 of the EasyWeek integration is deliberately **GET-only**: the plan's hard
rules (§1.6 p.8) forbid any mutation call, so this module exposes exactly three
read operations and no way to issue anything else::

    GET /ping
    GET /locations
    GET /bookings/{booking_uuid}

Everything here is built around two threats:

* **Secret leakage.** The Bearer key and the ``Workspace`` header must never
  reach a log record, an exception message, a ``repr``, or the probe's stdout.
* **Customer PII leakage.** ``GET /bookings/{uuid}`` returns a ``customer``
  subtree plus notes and order totals. This client therefore never logs a
  response body, and its exceptions carry only metadata (operation, HTTP status,
  attempt count, retryable flag) — never bodies, headers, URLs or PII.

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
from typing import Any, Awaitable, Callable
from urllib.parse import urlsplit

import httpx

from altegio_bot.settings import settings

logger = logging.getLogger("easyweek_client")

# Relative paths this client is allowed to build. A caller can never pass a URL:
# every public method maps to one of these constants, so neither a hostile
# booking id nor a redirect can retarget the request at another host.
_PATH_PING = "ping"
_PATH_LOCATIONS = "locations"
_PATH_BOOKINGS = "bookings"

# The ONE origin this client may ever talk to. A misconfigured base URL would
# otherwise send the Bearer key in clear text or to a third-party host, so the
# scheme, host, port and path are all pinned rather than merely "looks like a
# URL" (INTEGRATION_PLAN §1.1).
_ALLOWED_API_SCHEME = "https"
_ALLOWED_API_HOST = "my.easyweek.io"
_ALLOWED_API_PATH = "/api/public/v2"
_ALLOWED_API_PORTS = (None, 443)
CANONICAL_API_BASE_URL = f"{_ALLOWED_API_SCHEME}://{_ALLOWED_API_HOST}{_ALLOWED_API_PATH}"

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
    if not isinstance(value, str) or not value.strip():
        raise EasyWeekPermanentError("booking_uuid must be a non-empty string", operation="get_booking")
    try:
        parsed = uuid_module.UUID(value.strip())
    except (ValueError, AttributeError, TypeError):
        raise EasyWeekPermanentError("booking_uuid is not a valid UUID", operation="get_booking") from None
    return str(parsed)


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

    async def _get_json(self, *path_segments: str, operation: str) -> Any:
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
                response = await self._client.get(url, headers=self._headers())
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
        the operator picks ``EASYWEEK_LOCATION_UUID`` from this list by hand.
        A bare list and the documented ``{"data": [...]}`` envelope are both
        accepted.

        Malformed entries are NOT dropped silently: quietly discarding one would
        hide exactly the case where the operator then picks a UUID from an
        incomplete list. Any non-object entry fails the whole call.
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
        for item in items:
            if not isinstance(item, dict):
                raise EasyWeekProtocolError(
                    "locations response contains a non-object entry", operation="list_locations"
                )
        return list(items)

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
