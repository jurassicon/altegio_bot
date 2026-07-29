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
    EasyWeekAuthError,
    EasyWeekClient,
    EasyWeekConfigError,
    EasyWeekError,
    EasyWeekNotFoundError,
    EasyWeekPermanentError,
    EasyWeekProtocolError,
    EasyWeekRetryableError,
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

BASE = "https://api.example.test/api/public/v2"
BOOKING_UUID = "123e4567-e89b-12d3-a456-426614174000"

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
        "assert settings.easyweek_api_key == '';"
        "assert m.EasyWeekClient is not None;"
        "print('import-ok')"
    )
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, env=env)
    assert result.returncode == 0, result.stderr
    assert "import-ok" in result.stdout


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("https://h/api/public/v2", "https://h/api/public/v2"),
        ("https://h/api/public/v2/", "https://h/api/public/v2"),
        ("https://h/api/public/v2///", "https://h/api/public/v2"),
        ("  https://h/api/public/v2/  ", "https://h/api/public/v2"),
    ],
)
def test_base_url_is_normalized_without_double_slash(raw: str, expected: str) -> None:
    client = EasyWeekClient(api_key=KEY, workspace_slug=SLUG, base_url=raw)
    assert client._base_url == expected


@pytest.mark.parametrize(
    "bad",
    [
        "",
        "   ",
        "not-a-url",
        "ftp://h/api",
        "https://user:pw@h/api",
        "https://h/api?token=x",
        "https://h/api#frag",
    ],
)
def test_invalid_base_url_is_rejected(bad: str) -> None:
    with pytest.raises(EasyWeekConfigError):
        EasyWeekClient(api_key=KEY, workspace_slug=SLUG, base_url=bad)


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
        return httpx.Response(200, json=[{"uuid": "u1", "name": "Durlach"}])

    async with _client(handler) as client:
        items = await client.list_locations()

    assert items == [{"uuid": "u1", "name": "Durlach"}]
    assert len(seen) == 1
    assert seen[0].method == "GET"
    assert str(seen[0].url) == f"{BASE}/locations"


@pytest.mark.asyncio
async def test_list_locations_accepts_data_envelope_and_drops_non_objects() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"data": [{"uuid": "u1"}, "junk", 7, None]})

    async with _client(handler) as client:
        assert await client.list_locations() == [{"uuid": "u1"}]


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
