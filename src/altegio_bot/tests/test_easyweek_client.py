"""Tests for the GET-only EasyWeek Public API v2 client (PR-2).

Every HTTP interaction goes through ``httpx.MockTransport``: the suite never
touches the real EasyWeek API, and the retry tests inject a recording ``sleep``
so no test ever actually waits.

The hygiene tests use unique sentinel strings for the API key, the workspace
slug, and customer PII, then assert those sentinels appear in no log record, no
exception text, and no ``repr``.
"""

from __future__ import annotations

import logging
from typing import Any

import httpx
import pytest

from altegio_bot.easyweek_client import (
    CANONICAL_API_BASE_URL,
    EasyWeekAuthError,
    EasyWeekClient,
    EasyWeekConfigError,
    EasyWeekError,
    EasyWeekNotFoundError,
    EasyWeekPermanentError,
    EasyWeekProtocolError,
    EasyWeekRetryableError,
    _is_retryable_status,
    _parse_retry_after,
)

# ---------------------------------------------------------------------------
# Sentinels — must never appear in logs, exceptions or reprs
# ---------------------------------------------------------------------------

KEY = "SENTINEL_APIKEY_zzz111"
SLUG = "SENTINEL_SLUG_zzz222"
CUSTOMER_NAME = "SENTINEL_CUSTOMERNAME_zzz333"
CUSTOMER_PHONE = "SENTINEL_PHONE_zzz444"
CUSTOMER_EMAIL = "SENTINEL_EMAIL_zzz555"
BODY_MARKER = "SENTINEL_BODY_zzz666"
NOTES_MARKER = "SENTINEL_NOTES_zzz777"

ALL_SENTINELS = (
    KEY,
    SLUG,
    CUSTOMER_NAME,
    CUSTOMER_PHONE,
    CUSTOMER_EMAIL,
    BODY_MARKER,
    NOTES_MARKER,
)

# Only the canonical EasyWeek origin is accepted now; MockTransport still
# intercepts every request, so no real network call happens.
BASE = "https://my.easyweek.io/api/public/v2"
BOOKING_UUID = "123e4567-e89b-12d3-a456-426614174000"
VALID_UUID = "3f2a1b6c-0d4e-4f8a-9b1c-2d3e4f5a6b7c"

# A minimal location that satisfies the required uuid/name/timezone contract.
_LOCATION: dict[str, Any] = {"uuid": VALID_UUID, "name": "Durlach", "timezone": "Europe/Berlin"}
_LOCATION_2: dict[str, Any] = {
    "uuid": "9c8b7a65-4321-4abc-8def-0123456789ab",
    "name": "Zweite Filiale",
    "timezone": "Europe/Berlin",
}
# The shape the live API actually returns: timezone is an OBJECT, not a string.
# Confirmed by a read-only production probe against GET /locations.
_LOCATION_TZ_OBJECT: dict[str, Any] = {
    "uuid": VALID_UUID,
    "name": "Durlach",
    "timezone": {"name": "Europe/Berlin", "offset": "+02:00", "short": "CEST"},
}

_BOOKING_WITH_PII: dict[str, Any] = {
    "uuid": BOOKING_UUID,
    "location_uuid": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
    "start_time": "2026-08-01T10:00:00Z",
    "end_time": "2026-08-01T11:00:00Z",
    "timezone": "Europe/Berlin",
    "status": {"type": "CONFIRMED"},
    "is_canceled": False,
    "is_completed": False,
    "customer": {
        "name": CUSTOMER_NAME,
        "phone": CUSTOMER_PHONE,
        "email": CUSTOMER_EMAIL,
    },
    "notes": NOTES_MARKER,
    "order": {"total": 4200, "marker": BODY_MARKER},
    "ordered_services": [{"name": "Wimpernverlängerung", "price": 4200}],
}


class _Sleeps:
    """Recording stand-in for ``asyncio.sleep`` — never actually waits."""

    def __init__(self) -> None:
        self.delays: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.delays.append(delay)


def _client(handler, *, sleep=None, max_attempts: int = 3, **kwargs: Any) -> EasyWeekClient:
    return EasyWeekClient(
        api_key=KEY,
        workspace_slug=SLUG,
        base_url=BASE,
        transport=httpx.MockTransport(handler),
        sleep=sleep or _Sleeps(),
        max_attempts=max_attempts,
        **kwargs,
    )


# ===========================================================================
# Configuration
# ===========================================================================


def test_missing_api_key_is_typed_config_error() -> None:
    with pytest.raises(EasyWeekConfigError):
        EasyWeekClient(api_key="", workspace_slug=SLUG, base_url=BASE)


def test_blank_api_key_is_typed_config_error() -> None:
    with pytest.raises(EasyWeekConfigError):
        EasyWeekClient(api_key="   ", workspace_slug=SLUG, base_url=BASE)


def test_missing_workspace_slug_is_typed_config_error() -> None:
    with pytest.raises(EasyWeekConfigError):
        EasyWeekClient(api_key=KEY, workspace_slug="", base_url=BASE)


def test_importing_the_app_without_easyweek_config_does_not_break() -> None:
    """Missing EasyWeek settings must never break app/worker import.

    Run in a subprocess with the EasyWeek variables explicitly cleared: importing
    in-process could not prove anything (the module is already imported), and
    reloading it here would rebind the exception classes and break every other
    test in this file.
    """
    import os
    import subprocess
    import sys

    env = {k: v for k, v in os.environ.items() if not k.startswith("EASYWEEK_")}
    env["EASYWEEK_API_KEY"] = ""
    env["EASYWEEK_WORKSPACE_SLUG"] = ""
    code = (
        "import altegio_bot.easyweek_client as m;"
        "import altegio_bot.main;"
        "from altegio_bot.settings import settings;"
        # SecretStr, so the value is only reachable explicitly.
        "assert settings.easyweek_api_key.get_secret_value() == '';"
        "assert m.EasyWeekClient is not None;"
        "print('import-ok')"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stderr
    assert "import-ok" in result.stdout


@pytest.mark.parametrize(
    "raw",
    [
        "https://my.easyweek.io/api/public/v2",
        "https://my.easyweek.io/api/public/v2/",
        "https://my.easyweek.io/api/public/v2///",
        "  https://my.easyweek.io/api/public/v2/  ",
        "https://MY.EasyWeek.IO/api/public/v2",  # host comparison is case-insensitive
        "https://my.easyweek.io:443/api/public/v2",  # explicit default port is fine
    ],
)
def test_canonical_base_url_is_accepted_and_normalized(raw: str) -> None:
    client = EasyWeekClient(api_key=KEY, workspace_slug=SLUG, base_url=raw)
    assert client._base_url == CANONICAL_API_BASE_URL


@pytest.mark.parametrize(
    "bad",
    [
        # Explicitly required regression cases.
        "http://my.easyweek.io/api/public/v2",  # plaintext would expose the Bearer key
        "https://evil.example/api/public/v2",  # third-party host
        "https://my.easyweek.io/other",  # different path
        "https://user:password@my.easyweek.io/api/public/v2",
        "https://my.easyweek.io/api/public/v2?token=x",
        "https://my.easyweek.io/api/public/v2#fragment",
        # Additional bypass attempts.
        "",
        "   ",
        "not-a-url",
        "ftp://my.easyweek.io/api/public/v2",
        "https://my.easyweek.io:8443/api/public/v2",  # non-default port
        "https://my.easyweek.io.evil.example/api/public/v2",  # suffix look-alike
        "https://evil.example/my.easyweek.io/api/public/v2",  # path look-alike
        "https://my.easyweek.io/api/public/v1",
        "https://my.easyweek.io/api/public/v2/../admin",
        "https://my.easyweek.io",  # no path at all
    ],
)
def test_non_canonical_base_url_is_rejected(bad: str) -> None:
    with pytest.raises(EasyWeekConfigError):
        EasyWeekClient(api_key=KEY, workspace_slug=SLUG, base_url=bad)


def test_rejected_base_url_is_never_echoed_in_the_error() -> None:
    """A pasted URL may itself carry a token, so it must not reach the message."""
    hostile = "https://evil.example/api/public/v2?token=SENTINEL_URLTOKEN_zzz999"
    with pytest.raises(EasyWeekConfigError) as exc_info:
        EasyWeekClient(api_key=KEY, workspace_slug=SLUG, base_url=hostile)
    rendered = str(exc_info.value)
    assert "SENTINEL_URLTOKEN_zzz999" not in rendered
    assert hostile not in rendered
    assert "evil.example" not in rendered


# ===========================================================================
# HTTP contract
# ===========================================================================


@pytest.mark.asyncio
async def test_ping_issues_single_get_to_ping() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"ping": "pong", "version": "v12.32.3"})

    async with _client(handler) as client:
        payload = await client.ping()

    assert payload["ping"] == "pong"
    assert len(seen) == 1
    assert seen[0].method == "GET"
    assert str(seen[0].url) == f"{BASE}/ping"


@pytest.mark.asyncio
async def test_list_locations_issues_single_get_to_locations() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=[_LOCATION])

    async with _client(handler) as client:
        items = await client.list_locations()

    assert items == [_LOCATION]
    assert len(seen) == 1
    assert seen[0].method == "GET"
    assert str(seen[0].url) == f"{BASE}/locations"


@pytest.mark.asyncio
async def test_list_locations_accepts_data_envelope() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": [_LOCATION, _LOCATION_2]})

    async with _client(handler) as client:
        assert await client.list_locations() == [_LOCATION, _LOCATION_2]


@pytest.mark.asyncio
async def test_get_booking_issues_single_get_to_canonical_uuid_path() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=_BOOKING_WITH_PII)

    async with _client(handler) as client:
        # Upper-case input must be canonicalised before the request is built.
        await client.get_booking(BOOKING_UUID.upper())

    assert len(seen) == 1
    assert seen[0].method == "GET"
    assert str(seen[0].url) == f"{BASE}/bookings/{BOOKING_UUID}"


@pytest.mark.asyncio
async def test_auth_and_workspace_headers_are_sent() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"ping": "pong"})

    async with _client(handler) as client:
        await client.ping()

    assert seen[0].headers["Authorization"] == f"Bearer {KEY}"
    assert seen[0].headers["Workspace"] == SLUG
    assert seen[0].headers["Accept"] == "application/json"


@pytest.mark.asyncio
async def test_redirects_are_not_followed() -> None:
    """A redirect must never carry the Authorization header to another host."""
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(302, headers={"Location": "https://evil.test/steal"})

    async with _client(handler) as client:
        with pytest.raises(EasyWeekPermanentError) as exc_info:
            await client.ping()

    assert len(seen) == 1  # the redirect was not followed
    assert exc_info.value.status_code == 302


@pytest.mark.asyncio
async def test_only_get_is_ever_issued() -> None:
    methods: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        methods.append(request.method)
        if request.url.path.endswith("/locations"):
            return httpx.Response(200, json=[])
        if "/bookings/" in request.url.path:
            return httpx.Response(200, json=_BOOKING_WITH_PII)
        return httpx.Response(200, json={"ping": "pong"})

    async with _client(handler) as client:
        await client.ping()
        await client.list_locations()
        await client.get_booking(BOOKING_UUID)

    assert set(methods) == {"GET"}


@pytest.mark.asyncio
async def test_client_has_no_mutation_methods() -> None:
    """The class must not expose any write surface."""
    for forbidden in ("post", "put", "patch", "delete", "create_booking", "cancel_booking"):
        assert not hasattr(EasyWeekClient, forbidden)


@pytest.mark.parametrize(
    "bad_uuid",
    [
        "",
        "   ",
        "not-a-uuid",
        "../../etc/passwd",
        "123e4567-e89b-12d3-a456-42661417400",  # one char short
        "https://evil.test/x",
        f"{BOOKING_UUID}?x=1",
        None,
        12345,
    ],
)
@pytest.mark.asyncio
async def test_invalid_booking_uuid_is_rejected_before_any_request(bad_uuid: Any) -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover
        calls.append(request)
        return httpx.Response(200, json={})

    async with _client(handler) as client:
        with pytest.raises(EasyWeekPermanentError):
            await client.get_booking(bad_uuid)

    assert calls == []  # nothing reached the wire


@pytest.mark.asyncio
async def test_client_closes_owned_http_client() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ping": "pong"})

    client = _client(handler)
    await client.ping()
    await client.aclose()
    assert client._client.is_closed


@pytest.mark.asyncio
async def test_injected_http_client_is_not_closed_by_the_wrapper() -> None:
    """An injected client belongs to the caller, so the wrapper must not close it."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ping": "pong"})

    injected = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        client = EasyWeekClient(api_key=KEY, workspace_slug=SLUG, base_url=BASE, http_client=injected)
        async with client:
            await client.ping()
        assert not injected.is_closed
    finally:
        await injected.aclose()


# ===========================================================================
# Retry matrix
# ===========================================================================


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
@pytest.mark.asyncio
async def test_retryable_status_then_success(status: int) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(status)
        return httpx.Response(200, json={"ping": "pong"})

    sleeps = _Sleeps()
    async with _client(handler, sleep=sleeps) as client:
        assert (await client.ping())["ping"] == "pong"

    assert calls["n"] == 2
    assert len(sleeps.delays) == 1  # backed off exactly once, without waiting


@pytest.mark.asyncio
async def test_timeout_then_success() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ReadTimeout("timed out", request=request)
        return httpx.Response(200, json={"ping": "pong"})

    sleeps = _Sleeps()
    async with _client(handler, sleep=sleeps) as client:
        assert (await client.ping())["ping"] == "pong"

    assert calls["n"] == 2
    assert len(sleeps.delays) == 1


@pytest.mark.asyncio
async def test_network_error_then_success() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("connection refused", request=request)
        return httpx.Response(200, json={"ping": "pong"})

    sleeps = _Sleeps()
    async with _client(handler, sleep=sleeps) as client:
        assert (await client.ping())["ping"] == "pong"

    assert calls["n"] == 2


@pytest.mark.asyncio
async def test_retries_are_exhausted_after_max_attempts() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503)

    sleeps = _Sleeps()
    async with _client(handler, sleep=sleeps, max_attempts=3) as client:
        with pytest.raises(EasyWeekRetryableError) as exc_info:
            await client.ping()

    assert calls["n"] == 3  # bounded
    assert exc_info.value.attempts == 3
    assert exc_info.value.retryable is True
    assert len(sleeps.delays) == 2  # slept between attempts only


@pytest.mark.parametrize(
    "status,expected",
    [
        (400, EasyWeekPermanentError),
        (401, EasyWeekAuthError),
        (403, EasyWeekAuthError),
        (404, EasyWeekNotFoundError),
        (422, EasyWeekPermanentError),
    ],
)
@pytest.mark.asyncio
async def test_permanent_statuses_are_not_retried(status: int, expected: type[EasyWeekError]) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(status, json={"marker": BODY_MARKER})

    sleeps = _Sleeps()
    async with _client(handler, sleep=sleeps) as client:
        with pytest.raises(expected) as exc_info:
            await client.ping()

    assert calls["n"] == 1  # exactly one attempt
    assert sleeps.delays == []  # never backed off
    assert exc_info.value.status_code == status
    assert exc_info.value.retryable is False


@pytest.mark.asyncio
async def test_malformed_success_body_is_not_retried() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, content=b"<html>not json</html>")

    sleeps = _Sleeps()
    async with _client(handler, sleep=sleeps) as client:
        with pytest.raises(EasyWeekProtocolError):
            await client.ping()

    assert calls["n"] == 1
    assert sleeps.delays == []


@pytest.mark.asyncio
async def test_valid_json_of_wrong_shape_is_protocol_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=["not", "an", "object"])

    async with _client(handler) as client:
        with pytest.raises(EasyWeekProtocolError):
            await client.ping()


@pytest.mark.asyncio
async def test_backoff_is_bounded_and_never_really_sleeps() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    sleeps = _Sleeps()
    async with _client(handler, sleep=sleeps, max_attempts=3) as client:
        with pytest.raises(EasyWeekRetryableError):
            await client.ping()

    assert len(sleeps.delays) == 2
    for delay in sleeps.delays:
        assert 0.0 <= delay <= 8.0  # bounded by _BACKOFF_MAX_SEC


@pytest.mark.asyncio
async def test_retry_after_header_is_honoured_and_capped() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            # Hostile/mistaken value: must be clamped, not obeyed literally.
            return httpx.Response(429, headers={"Retry-After": "99999"})
        return httpx.Response(200, json={"ping": "pong"})

    sleeps = _Sleeps()
    async with _client(handler, sleep=sleeps) as client:
        await client.ping()

    assert sleeps.delays == [10.0]  # _RETRY_AFTER_MAX_SEC


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("1", 1.0),
        ("0", 0.0),
        ("2.5", 2.5),
        ("99999", 10.0),  # capped
        ("-5", None),  # negative ignored
        ("Wed, 21 Oct 2026 07:28:00 GMT", None),  # HTTP-date form falls back
        ("garbage", None),
        (None, None),
    ],
)
def test_parse_retry_after(raw: str | None, expected: float | None) -> None:
    assert _parse_retry_after(raw) == expected


# ===========================================================================
# Secret / PII hygiene
# ===========================================================================


@pytest.mark.asyncio
async def test_no_sentinel_leaks_on_success_path(caplog) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_BOOKING_WITH_PII)

    with caplog.at_level(logging.DEBUG):
        async with _client(handler) as client:
            booking = await client.get_booking(BOOKING_UUID)

    # The client returns the raw payload (the caller projects it) ...
    assert booking["customer"]["name"] == CUSTOMER_NAME
    # ... but nothing sensitive may appear in the logs.
    blob = "\n".join(r.getMessage() for r in caplog.records)
    for sentinel in ALL_SENTINELS:
        assert sentinel not in blob, f"{sentinel!r} leaked into logs"


@pytest.mark.asyncio
async def test_no_sentinel_leaks_on_error_paths(caplog) -> None:
    """Error bodies/headers must not reach logs or the exception text."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            500,
            json={"error": BODY_MARKER, "customer": {"name": CUSTOMER_NAME}},
            headers={"X-Leak": KEY},
        )

    with caplog.at_level(logging.DEBUG):
        async with _client(handler, max_attempts=2) as client:
            with pytest.raises(EasyWeekRetryableError) as exc_info:
                await client.get_booking(BOOKING_UUID)

    blob = "\n".join(r.getMessage() for r in caplog.records)
    text = str(exc_info.value)
    summary = repr(exc_info.value.safe_summary)
    for sentinel in ALL_SENTINELS:
        assert sentinel not in blob, f"{sentinel!r} leaked into logs"
        assert sentinel not in text, f"{sentinel!r} leaked into exception text"
        assert sentinel not in summary, f"{sentinel!r} leaked into safe_summary"


@pytest.mark.asyncio
async def test_transport_exception_text_never_reaches_logs(caplog) -> None:
    """httpx exception messages can embed the full URL — only the class is logged."""

    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError(f"failed connecting with key {KEY} to {BASE}", request=request)

    with caplog.at_level(logging.DEBUG):
        async with _client(handler, max_attempts=1) as client:
            with pytest.raises(EasyWeekRetryableError) as exc_info:
                await client.ping()

    blob = "\n".join(r.getMessage() for r in caplog.records)
    assert KEY not in blob
    assert KEY not in str(exc_info.value)
    assert "ConnectError" in blob  # the safe class marker IS present


def test_repr_and_str_never_expose_key_or_slug() -> None:
    client = EasyWeekClient(api_key=KEY, workspace_slug=SLUG, base_url=BASE)
    for rendered in (repr(client), str(client), f"{client}"):
        assert KEY not in rendered
        assert SLUG not in rendered


def test_config_error_text_never_contains_the_key() -> None:
    with pytest.raises(EasyWeekConfigError) as exc_info:
        EasyWeekClient(api_key=KEY, workspace_slug="", base_url=BASE)
    assert KEY not in str(exc_info.value)


# ===========================================================================
# Retryable status classification: 429 + the WHOLE 5xx range
# ===========================================================================


@pytest.mark.parametrize("status", [429, 500, 501, 502, 503, 504, 505, 507, 508, 511, 599])
def test_status_is_classified_retryable(status: int) -> None:
    assert _is_retryable_status(status) is True


@pytest.mark.parametrize("status", [200, 201, 204, 301, 302, 400, 401, 403, 404, 409, 422, 499, 600])
def test_status_is_classified_permanent(status: int) -> None:
    assert _is_retryable_status(status) is False


@pytest.mark.parametrize("status", [500, 501, 502, 503, 504, 505, 507, 599])
@pytest.mark.asyncio
async def test_every_5xx_is_retried_then_succeeds(status: int) -> None:
    """No 5xx may be treated as permanent — not even the ones we never enumerated."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(status)
        return httpx.Response(200, json={"ping": "pong"})

    sleeps = _Sleeps()
    async with _client(handler, sleep=sleeps) as client:
        assert (await client.ping())["ping"] == "pong"

    assert calls["n"] == 2
    assert len(sleeps.delays) == 1


@pytest.mark.parametrize("status", [500, 501, 502, 503, 504, 505, 507, 599])
@pytest.mark.asyncio
async def test_every_5xx_exhausts_into_retryable_error(status: int) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(status)

    sleeps = _Sleeps()
    async with _client(handler, sleep=sleeps, max_attempts=3) as client:
        with pytest.raises(EasyWeekRetryableError) as exc_info:
            await client.ping()

    assert calls["n"] == 3  # bounded at three attempts
    assert exc_info.value.status_code == status
    assert exc_info.value.retryable is True
    assert len(sleeps.delays) == 2
    for delay in sleeps.delays:
        assert 0.0 <= delay <= 8.0


# ===========================================================================
# Strict shape validation of a malformed 2xx
# ===========================================================================


@pytest.mark.parametrize(
    "body",
    [
        {},  # no ping marker at all
        {"ping": "nope"},  # wrong value
        {"ping": 1},  # wrong type
        {"ping": None},
        {"version": "v12.32.3"},  # only the optional field
        {"status": "ok"},  # a different API answering 200
    ],
)
@pytest.mark.asyncio
async def test_ping_without_the_success_marker_is_protocol_error(body: Any) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=body)

    sleeps = _Sleeps()
    async with _client(handler, sleep=sleeps) as client:
        with pytest.raises(EasyWeekProtocolError):
            await client.ping()

    assert calls["n"] == 1  # a malformed 2xx is never retried
    assert sleeps.delays == []


@pytest.mark.asyncio
async def test_ping_allows_extra_fields() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ping": "pong", "version": "v12.32.3", "extra": 1})

    async with _client(handler) as client:
        assert (await client.ping())["version"] == "v12.32.3"


@pytest.mark.parametrize(
    "body",
    [
        {"items": [_LOCATION]},  # envelope without the documented data key
        {"data": None},
        {"data": "junk"},
        {"data": 7},
        {"data": _LOCATION},  # object where a list is required
        [_LOCATION, "junk"],  # a non-object entry must fail the call
        [_LOCATION, None],
        [_LOCATION, 7],
        {"data": [_LOCATION, "junk"]},
        "not-a-container",
        42,
    ],
)
@pytest.mark.asyncio
async def test_malformed_locations_shape_is_protocol_error(body: Any) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=body)

    sleeps = _Sleeps()
    async with _client(handler, sleep=sleeps) as client:
        with pytest.raises(EasyWeekProtocolError):
            await client.list_locations()

    assert calls["n"] == 1
    assert sleeps.delays == []


@pytest.mark.asyncio
async def test_locations_never_silently_drops_entries() -> None:
    """A malformed entry must fail loudly, not shrink the operator's list."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[_LOCATION, "junk", _LOCATION_2])

    async with _client(handler) as client:
        with pytest.raises(EasyWeekProtocolError):
            await client.list_locations()


@pytest.mark.asyncio
async def test_empty_locations_list_is_returned_by_the_client() -> None:
    """The client itself may legitimately return []; the probe decides policy."""

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[])

    async with _client(handler) as client:
        assert await client.list_locations() == []


@pytest.mark.parametrize(
    "body",
    [
        {"data": None},  # present but null -> must NOT fall back to the envelope
        {"data": []},
        {"data": "junk"},
        {"data": 7},
        {"data": True},
        {"uuid": None},  # no usable identifier
        {"uuid": ""},
        {"uuid": 123},
        {},  # arbitrary object is not a booking
        {"something": "else"},
        [],
        "not-an-object",
    ],
)
@pytest.mark.asyncio
async def test_malformed_booking_shape_is_protocol_error(body: Any) -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=body)

    sleeps = _Sleeps()
    async with _client(handler, sleep=sleeps) as client:
        with pytest.raises(EasyWeekProtocolError):
            await client.get_booking(BOOKING_UUID)

    assert calls["n"] == 1
    assert sleeps.delays == []


@pytest.mark.asyncio
async def test_booking_data_envelope_is_unwrapped() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": _BOOKING_WITH_PII})

    async with _client(handler) as client:
        booking = await client.get_booking(BOOKING_UUID)

    assert booking["uuid"] == BOOKING_UUID


@pytest.mark.asyncio
async def test_bare_booking_object_is_accepted_when_no_data_key() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_BOOKING_WITH_PII)

    async with _client(handler) as client:
        assert (await client.get_booking(BOOKING_UUID))["uuid"] == BOOKING_UUID


@pytest.mark.asyncio
async def test_malformed_body_never_reaches_logs_or_exception(caplog) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": None, "leak": BODY_MARKER})

    with caplog.at_level(logging.DEBUG):
        async with _client(handler) as client:
            with pytest.raises(EasyWeekProtocolError) as exc_info:
                await client.get_booking(BOOKING_UUID)

    blob = "\n".join(r.getMessage() for r in caplog.records)
    assert BODY_MARKER not in blob
    assert BODY_MARKER not in str(exc_info.value)


# ===========================================================================
# The API key must not be reachable through Settings either
# ===========================================================================


def test_settings_never_expose_the_api_key_in_any_string_form() -> None:
    """repr(settings) lands in config dumps and tracebacks — a plain str would leak."""
    from altegio_bot.settings import Settings

    sentinel = "SENTINEL_SETTINGSKEY_zzz888"
    cfg = Settings(
        database_url="postgresql+asyncpg://x/y",
        altegio_webhook_secret="x",
        easyweek_api_key=sentinel,
        _env_file=None,
    )

    for rendered in (repr(cfg), str(cfg), f"{cfg}", repr(cfg.easyweek_api_key), str(cfg.easyweek_api_key)):
        assert sentinel not in rendered

    # ... while the real value is still retrievable explicitly.
    assert cfg.easyweek_api_key.get_secret_value() == sentinel


@pytest.mark.asyncio
async def test_secret_str_key_from_settings_still_builds_the_bearer_header(monkeypatch) -> None:
    """SecretStr must be unwrapped for the header, never stringified into it."""
    from pydantic import SecretStr

    from altegio_bot import settings as settings_module

    sentinel = "SENTINEL_HEADERKEY_zzz777"
    monkeypatch.setattr(settings_module.settings, "easyweek_api_key", SecretStr(sentinel))
    monkeypatch.setattr(settings_module.settings, "easyweek_workspace_slug", SLUG)

    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"ping": "pong"})

    client = EasyWeekClient(base_url=BASE, transport=httpx.MockTransport(handler))
    async with client:
        await client.ping()

    assert seen[0].headers["Authorization"] == f"Bearer {sentinel}"
    assert "**" not in seen[0].headers["Authorization"]
    assert sentinel not in repr(client)


def test_empty_secret_str_key_still_raises_config_error(monkeypatch) -> None:
    from pydantic import SecretStr

    from altegio_bot import settings as settings_module

    monkeypatch.setattr(settings_module.settings, "easyweek_api_key", SecretStr(""))
    monkeypatch.setattr(settings_module.settings, "easyweek_workspace_slug", SLUG)
    with pytest.raises(EasyWeekConfigError):
        EasyWeekClient(base_url=BASE)


# ===========================================================================
# Strict location contract: uuid + name + timezone are all required
# ===========================================================================

_MALFORMED_LOCATIONS: list[Any] = [
    # Missing fields entirely.
    [{}],
    [{"name": "Durlach", "timezone": "Europe/Berlin"}],  # no uuid
    [{"uuid": VALID_UUID, "timezone": "Europe/Berlin"}],  # no name
    [{"uuid": VALID_UUID, "name": "Durlach"}],  # no timezone
    # Blank / unusable uuid.
    [{"uuid": "", "name": "Durlach", "timezone": "Europe/Berlin"}],
    [{"uuid": "   ", "name": "Durlach", "timezone": "Europe/Berlin"}],
    [{"uuid": "not-a-uuid", "name": "Durlach", "timezone": "Europe/Berlin"}],
    # Blank name / timezone.
    [{"uuid": VALID_UUID, "name": "", "timezone": "Europe/Berlin"}],
    [{"uuid": VALID_UUID, "name": "   ", "timezone": "Europe/Berlin"}],
    [{"uuid": VALID_UUID, "name": "Durlach", "timezone": ""}],
    [{"uuid": VALID_UUID, "name": "Durlach", "timezone": "   "}],
    # Wrong types.
    [{"uuid": None, "name": "Durlach", "timezone": "Europe/Berlin"}],
    [{"uuid": 123, "name": "Durlach", "timezone": "Europe/Berlin"}],
    [{"uuid": VALID_UUID, "name": None, "timezone": "Europe/Berlin"}],
    [{"uuid": VALID_UUID, "name": [], "timezone": "Europe/Berlin"}],
    [{"uuid": VALID_UUID, "name": "Durlach", "timezone": None}],
    [{"uuid": VALID_UUID, "name": "Durlach", "timezone": {}}],
    # A uuid carrying extra junk must not slip through.
    [{"uuid": f"{VALID_UUID}?x=1", "name": "Durlach", "timezone": "Europe/Berlin"}],
    [{"uuid": "../../etc/passwd", "name": "Durlach", "timezone": "Europe/Berlin"}],
    # One good entry plus one broken entry must fail the WHOLE response.
    [_LOCATION, {}],
    [{}, _LOCATION],
    # Same, inside the documented envelope.
    {"data": [_LOCATION, {}]},
]


@pytest.mark.parametrize("body", _MALFORMED_LOCATIONS)
@pytest.mark.asyncio
async def test_location_without_required_identity_is_protocol_error(body: Any) -> None:
    """A location the operator cannot identify or reference is not a location.

    Accepting ``[{}]`` used to make the probe print
    ``{"uuid": null, "name": null, "timezone": null}`` with ok=true.
    """
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=body)

    sleeps = _Sleeps()
    async with _client(handler, sleep=sleeps) as client:
        with pytest.raises(EasyWeekProtocolError) as exc_info:
            await client.list_locations()

    assert exc_info.value.operation == "list_locations"
    assert calls["n"] == 1  # a malformed 2xx is never retried
    assert sleeps.delays == []


@pytest.mark.asyncio
async def test_malformed_location_value_never_reaches_logs_or_exception(caplog) -> None:
    """The offending uuid/name value must not be echoed anywhere."""
    marker = "SENTINEL_BADLOC_zzz555"

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[{"uuid": marker, "name": marker, "timezone": marker}],
        )

    with caplog.at_level(logging.DEBUG):
        async with _client(handler) as client:
            with pytest.raises(EasyWeekProtocolError) as exc_info:
                await client.list_locations()

    blob = "\n".join(r.getMessage() for r in caplog.records)
    assert marker not in blob
    assert marker not in str(exc_info.value)
    assert marker not in repr(exc_info.value.safe_summary)


@pytest.mark.parametrize(
    "body,expected",
    [
        ([_LOCATION], [_LOCATION]),  # bare list
        ({"data": [_LOCATION]}, [_LOCATION]),  # documented envelope
        ([_LOCATION, _LOCATION_2], [_LOCATION, _LOCATION_2]),  # several locations
        ({"data": [_LOCATION, _LOCATION_2]}, [_LOCATION, _LOCATION_2]),
    ],
)
@pytest.mark.asyncio
async def test_valid_locations_are_accepted(body: Any, expected: list[dict[str, Any]]) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    async with _client(handler) as client:
        assert await client.list_locations() == expected


@pytest.mark.asyncio
async def test_unknown_extra_location_fields_do_not_break_validation() -> None:
    """Upstream may add fields; only the required three are enforced."""
    enriched = {
        **_LOCATION,
        "address": {"city": "Karlsruhe"},
        "working_hours": [{"day": 1}],
        "brand_new_field": "whatever",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[enriched])

    async with _client(handler) as client:
        assert await client.list_locations() == [enriched]


@pytest.mark.asyncio
async def test_uppercase_location_uuid_is_accepted_as_is() -> None:
    """Validation must not rewrite the operator-visible value."""
    upper = {**_LOCATION, "uuid": VALID_UUID.upper()}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[upper])

    async with _client(handler) as client:
        items = await client.list_locations()

    assert items[0]["uuid"] == VALID_UUID.upper()


# ===========================================================================
# Timezone: the live object shape and the documented string shape
# ===========================================================================


@pytest.mark.parametrize(
    "timezone",
    [
        "Europe/Berlin",  # documented / legacy string
        {"name": "Europe/Berlin"},  # live object, name only
        {"name": "Europe/Berlin", "offset": "+02:00", "short": "CEST"},  # full live object
        {"name": "Europe/Berlin", "offset": None, "short": None},  # optionals nulled
        {"name": "Europe/Berlin", "offset": "+02:00", "brand_new": {"x": 1}},  # upstream growth
    ],
    ids=["string", "object-name-only", "object-full", "object-null-optionals", "object-extra-field"],
)
@pytest.mark.asyncio
async def test_usable_timezone_shapes_are_accepted(timezone: Any) -> None:
    """Only ``timezone.name`` is required; offset/short are presentation detail.

    Requiring them would turn a cosmetic upstream change into an outage.
    """
    location = {**_LOCATION, "timezone": timezone}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[location])

    async with _client(handler) as client:
        assert await client.list_locations() == [location]


@pytest.mark.asyncio
async def test_object_timezone_is_returned_verbatim_and_not_normalised() -> None:
    """The transport layer must not collapse the object to a string.

    Rewriting it here would be domain normalization (PR-4) and would hide the
    real API shape from every later consumer. Display-time projection belongs to
    the operator probe.
    """

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[_LOCATION_TZ_OBJECT])

    async with _client(handler) as client:
        items = await client.list_locations()

    assert items[0]["timezone"] == {"name": "Europe/Berlin", "offset": "+02:00", "short": "CEST"}
    # And the module-level fixture was not mutated in place.
    assert _LOCATION_TZ_OBJECT["timezone"]["name"] == "Europe/Berlin"


@pytest.mark.asyncio
async def test_live_envelope_with_links_and_meta_returns_only_data() -> None:
    """The real response carries ``links``/``meta``; PR-2 ignores both."""
    body = {"data": [_LOCATION_TZ_OBJECT], "links": {"next": None}, "meta": {"total": 1}}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    async with _client(handler) as client:
        items = await client.list_locations()

    assert items == [_LOCATION_TZ_OBJECT]


_UNUSABLE_TIMEZONES: list[Any] = [
    None,
    "",
    "   ",
    {},
    {"name": None},
    {"name": ""},
    {"name": "   "},
    {"name": 123},
    {"name": {"name": "Europe/Berlin"}},
    {"offset": "+02:00", "short": "CEST"},  # object without a name at all
    [],
    ["Europe/Berlin"],
    123,
    True,
]


@pytest.mark.parametrize("timezone", _UNUSABLE_TIMEZONES)
@pytest.mark.asyncio
async def test_unusable_timezone_is_protocol_error(timezone: Any) -> None:
    """Fail closed: an unidentifiable branch must not reach the operator."""
    location = {**_LOCATION, "timezone": timezone}
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json=[location])

    sleeps = _Sleeps()
    async with _client(handler, sleep=sleeps) as client:
        with pytest.raises(EasyWeekProtocolError) as exc_info:
            await client.list_locations()

    assert exc_info.value.operation == "list_locations"
    assert calls["n"] == 1  # a malformed 2xx is never retried
    assert sleeps.delays == []
    # A fixed literal message: the offending value is never interpolated.
    assert str(exc_info.value).startswith("location entry has no usable timezone")


@pytest.mark.asyncio
async def test_malformed_timezone_value_never_reaches_logs_or_exception(caplog) -> None:
    """A bad timezone name must not be echoed into the error or the logs."""
    marker = "SENTINEL_BADTZ_qqq777"
    location = {**_LOCATION, "timezone": {"name": "", "short": marker, "offset": marker}}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[location])

    with caplog.at_level(logging.DEBUG):
        async with _client(handler) as client:
            with pytest.raises(EasyWeekProtocolError) as exc_info:
                await client.list_locations()

    blob = "\n".join(r.getMessage() for r in caplog.records)
    assert marker not in blob
    assert marker not in str(exc_info.value)
    assert marker not in repr(exc_info.value.safe_summary)


@pytest.mark.asyncio
async def test_one_bad_timezone_fails_the_whole_list() -> None:
    """No partial list: the operator must not choose from filtered results."""
    body = {"data": [_LOCATION_TZ_OBJECT, {**_LOCATION_2, "timezone": {"offset": "+02:00"}}]}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    async with _client(handler) as client:
        with pytest.raises(EasyWeekProtocolError) as exc_info:
            await client.list_locations()

    assert exc_info.value.operation == "list_locations"
