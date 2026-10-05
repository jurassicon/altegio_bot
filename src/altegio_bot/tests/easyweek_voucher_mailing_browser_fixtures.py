"""A real browser against the real Ops pages (review R8).

What this gives that the API tests could not
--------------------------------------------
The product of §43 is the interface. API tests prove the contracts behind it and
prove nothing about whether a click reaches the right handler, whether a value
typed into a field arrives, whether polling repaints, or whether navigation lands —
and every one of those is the operator path itself. The R3 defect is the proof: the
backend contract was right, the API tests were green, and the screen dead-ended
before an operator could enter the numbers.

So these tests drive Chromium. They click, they type, they wait for the page to
change, and they follow navigation. Nothing here posts to the API to move the
workflow along, and nothing reads a batch id out of the database to open the next
screen by URL — the point is the transitions.

How the app is served
---------------------
Uvicorn, on a loopback port, **in this process**. That matters: the suite's
``session_maker`` is installed by monkeypatching module globals, and a subprocess
would have its own database. In-process means the browser talks real HTTP to the
same app object the other tests exercise, against the same disposable PostgreSQL.

What is faked
-------------
EasyWeek and Meta, through the same stub the API tests install at the dispatch
seam, and the narrow synthetic issuer pin. Nothing else: the session cookie, the
CSRF derivation, the origin check, the approvals, the operations, the ledger and the
executor are all real.

Required, never skipped
-----------------------
``ALTEGIO_REQUIRE_BROWSER_TESTS=1`` turns a missing browser into a FAILURE instead
of a skip, and the required CI gate sets it. A green skip would be the worst
outcome: the gate would report success for the one layer this phase is judged on.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import socket
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio

import altegio_bot.campaigns.runner as campaign_runner_module
import altegio_bot.ops.campaigns_api as campaigns_api_module
import altegio_bot.ops.router as ops_router_module
import altegio_bot.ops.voucher_mailing as voucher_mailing_module
from altegio_bot.ops.auth import SESSION_COOKIE
from altegio_bot.tests.easyweek_voucher_mailing_ui_fixtures import OPS_USER, session_cookie

REQUIRE_BROWSER = os.getenv("ALTEGIO_REQUIRE_BROWSER_TESTS") == "1"

# How long a browser assertion waits for the page to catch up. Generous because the
# page polls on a timer and the executor runs between polls; short enough that a
# genuine hang fails the test rather than the suite's patience.
WAIT_MS = 15_000


def _free_port() -> int:
    with contextlib.closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def _unavailable(reason: str) -> None:
    """Fail when the browser runtime is required, skip only when it is not.

    The flag is what the required gate sets. Locally, a developer without Chromium
    still gets the rest of the suite.
    """
    if REQUIRE_BROWSER:
        raise AssertionError(
            f"the browser acceptance suite is required and cannot run: {reason}. "
            "Install it with `uv run playwright install chromium`."
        )
    pytest.skip(f"browser runtime unavailable: {reason}")


@pytest_asyncio.fixture
async def ops_server(session_maker, monkeypatch, ops_credentials) -> AsyncIterator[str]:
    """The real app on a loopback port, sharing this test's database."""
    import uvicorn

    from altegio_bot.main import app

    monkeypatch.setattr(voucher_mailing_module, "SessionLocal", session_maker)
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
    monkeypatch.setattr(campaign_runner_module, "SessionLocal", session_maker)

    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", access_log=False)
    server = uvicorn.Server(config)
    serving = asyncio.create_task(server.serve())
    for _ in range(200):
        if server.started:
            break
        await asyncio.sleep(0.05)
    if not server.started:
        server.should_exit = True
        await serving
        _unavailable("uvicorn did not start")
    try:
        yield f"http://127.0.0.1:{port}"
    finally:
        server.should_exit = True
        with contextlib.suppress(asyncio.CancelledError):
            await serving


@pytest_asyncio.fixture
async def page(ops_server: str) -> AsyncIterator[Any]:
    """A Chromium page already holding a valid operator session.

    The cookie is minted the way the login form mints it, so the session, the CSRF
    token the page embeds and the origin check are all the application's own.
    """
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:  # pragma: no cover - environment, not code
        _unavailable(f"playwright is not installed ({exc})")
        return

    try:
        manager = async_playwright()
        playwright = await manager.start()
    except Exception as exc:  # noqa: BLE001 - environment, not code
        _unavailable(f"playwright could not start ({exc})")
        return

    browser = None
    try:
        try:
            browser = await playwright.chromium.launch()
        except Exception as exc:  # noqa: BLE001 - a missing browser binary
            _unavailable(f"chromium is not available ({exc})")
            return
        context = await browser.new_context(base_url=ops_server)
        context.set_default_timeout(WAIT_MS)
        # Hermetic: nothing in this suite may depend on a CDN being reachable, and a
        # required gate must not go red because jsdelivr is slow. The Ops pages pull
        # Bootstrap's CSS and JS from one, and the operator workflow does not use it —
        # the page ships its own navbar fallback for exactly that reason.
        #
        # Blocked rather than allowed-and-ignored, so a test can assert on page errors
        # strictly: a third-party script that fails to load is not a defect in the
        # page under test, and filtering its noise after the fact would also hide ours.
        await context.route(
            lambda url: not url.startswith(ops_server),
            lambda route: asyncio.ensure_future(route.abort()),
        )
        await context.add_cookies(
            [
                {
                    "name": SESSION_COOKIE,
                    "value": session_cookie(OPS_USER),
                    "domain": "127.0.0.1",
                    "path": "/",
                }
            ]
        )
        opened = watch_for_errors(await context.new_page())
        yield opened
    finally:
        if browser is not None:
            await browser.close()
        await playwright.stop()


# Console noise that is not the page's own behaviour. Deliberately short, and
# deliberately about RESOURCES rather than about script errors: anything thrown by the
# page's own JavaScript must reach the assertion below.
_THIRD_PARTY_NOISE = (
    "favicon",
    # Third-party requests are blocked by the context route above; Chromium still
    # logs the refusal.
    "net::err_failed",
    "net::err_blocked",
    "failed to load resource",
    # The Ops page shell pins a Bootstrap bundle whose published SRI digest no longer
    # matches what the CDN serves, so Chromium blocks that script. Pre-existing, on
    # every Ops page, and harmless here: the pages carry their own navbar fallback and
    # the voucher workflow uses no Bootstrap JavaScript. Called out rather than hidden.
    "integrity",
)


def watch_for_errors(opened: Any) -> Any:
    """Collect this page's JavaScript errors, for :func:`assert_no_page_errors`.

    Applied to every page a test drives, not only the first one: a silent exception
    is exactly the class of defect this suite exists to catch, and a second tab is
    where several of the review findings actually showed up.
    """
    errors: list[str] = []
    opened.on("pageerror", lambda exc: errors.append(str(exc)))
    opened.on(
        "console",
        lambda message: errors.append(message.text) if message.type == "error" else None,
    )
    opened.collected_page_errors = errors
    return opened


async def tab_with_no_cache(opened: Any, ops_server: str) -> Any:
    """A page in a context that provably has nothing of this browser's own state.

    The point of review F2 is that the page restores itself from the SERVER, so a test
    of it must be able to say that the browser had nothing to restore from.
    ``storage_state`` carries the session cookie and deliberately not sessionStorage,
    and the init script empties it before any page script runs — so if the operation
    still appears, the server is the only place it can have come from.
    """
    state = await opened.context.storage_state()
    context = await opened.context.browser.new_context(base_url=ops_server, storage_state=state)
    context.set_default_timeout(WAIT_MS)
    await context.route(
        lambda url: not url.startswith(ops_server),
        lambda route: asyncio.ensure_future(route.abort()),
    )
    await context.add_init_script("try { window.sessionStorage.clear(); } catch (err) {}")
    return watch_for_errors(await context.new_page())


def assert_no_page_errors(opened: Any) -> None:
    """No uncaught JavaScript and no console error of the page's own making."""
    collected = getattr(opened, "collected_page_errors", None)
    assert collected is not None, "this page was not watched for errors: use watch_for_errors / tab_with_no_cache"
    errors = [message for message in collected if not any(noise in message.lower() for noise in _THIRD_PARTY_NOISE)]
    assert errors == [], f"the page reported JavaScript errors: {errors}"


__all__ = [
    "REQUIRE_BROWSER",
    "WAIT_MS",
    "assert_no_page_errors",
    "ops_server",
    "page",
    "tab_with_no_cache",
    "watch_for_errors",
]
