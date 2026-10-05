"""Fixtures for the §43 operator UI: a real session, a real app, fake transports.

What is real here, deliberately
------------------------------
The FastAPI app, the routes, the rendered pages, their JavaScript, the session
cookie, the CSRF derivation, the origin check, the approval and operation tables,
the per-item ledger and the executor loop. The tests drive the same endpoints a
browser drives, with a cookie a browser would hold.

The Ops auth dependency is **not** overridden. §43.7 is a contract about
authorisation, and a suite that replaced the dependency with ``lambda: None``
would be testing a different application than the one that ships: it could not
tell an unconfigured deployment from a configured one, could not see a query
token being refused, and could not fail if somebody wired the permissive fallback
into a money endpoint. So these fixtures mint a genuine session token with
``make_session_token`` and send it as a cookie.

What is faked is the outside world — EasyWeek and Meta — through the same
``FakeReader``/``FakeMutator``/``FakeSender`` §42's own suite uses, and the single
narrow issuer seam. No real staffer UUID, no real customer, no real code.
"""

from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from typing import Any

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import altegio_bot.campaigns.runner as campaign_runner_module
import altegio_bot.ops.campaigns_api as campaigns_api_module
import altegio_bot.ops.router as ops_router_module
import altegio_bot.ops.voucher_mailing as voucher_mailing_module
from altegio_bot.campaigns.easyweek_voucher_production import dispatch as dispatch_module
from altegio_bot.campaigns.easyweek_voucher_production import operations as operations_module
from altegio_bot.ops.auth import SESSION_COOKIE, csrf_token_for, make_session_token
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_voucher_production_fixtures import (
    FakeMutator,
    FakeReader,
    FakeSender,
)

OPS_USER = "ops-operator"
OPS_PASS = "synthetic-ops-password"
OPS_SECRET = "synthetic-ops-signing-secret"
# A second account, for the tests about one operator's approval not being
# another's to spend.
OTHER_OPS_USER = "ops-second-operator"

BASE_URL = "http://ops.test"
ORIGIN = BASE_URL


class StubTransports(dispatch_module.Transports):
    """The three transports a stage uses, supplied by the test.

    Subclasses :class:`dispatch.Transports` so the class the deployed code
    constructs is the one under test, and only what it yields is replaced.

    Mutable on purpose. A single instance is installed in place of
    ``dispatch.Transports`` for the whole test, and each stage swaps in the fake
    it needs through :meth:`use`. That matters because the API and the executor
    construct their transports independently — the plan opens a reader inside the
    HTTP request, the stage opens a mutator inside the worker — so a per-call
    argument could only ever reach one of them, and a test that injected into the
    executor alone would quietly let the API talk to the real EasyWeek.
    """

    def __init__(self) -> None:
        self.reader_obj: Any = None
        self.mutator_obj: Any = None
        self.sender_obj: Any = None

    def use(self, *, reader: Any = None, mutator: Any = None, sender: Any = None) -> "StubTransports":
        if reader is not None:
            self.reader_obj = reader
        # ``mutator`` and ``sender`` are reset to None when not given, so a stage
        # that opens a transport this test did not expect fails loudly instead of
        # reusing the previous stage's fake and recording its calls.
        self.mutator_obj = mutator
        self.sender_obj = sender
        return self

    @contextlib.asynccontextmanager
    async def reader(self) -> AsyncIterator[Any]:
        if self.reader_obj is None:
            raise AssertionError("this test did not install a read transport")
        yield self.reader_obj

    @contextlib.asynccontextmanager
    async def mutator(self) -> AsyncIterator[Any]:
        if self.mutator_obj is None:
            raise AssertionError("this test did not expect a mutation transport to be opened")
        yield self.mutator_obj

    @contextlib.asynccontextmanager
    async def sender(self) -> AsyncIterator[Any]:
        if self.sender_obj is None:
            raise AssertionError("this test did not expect a send transport to be opened")
        yield self.sender_obj


@pytest.fixture
def transports(monkeypatch: pytest.MonkeyPatch) -> StubTransports:
    """One stub, installed wherever production would build a real transport.

    Patched at ``dispatch.Transports`` rather than passed as an argument, so BOTH
    callers get it: the HTTP plan endpoint and the executor.
    """
    stub = StubTransports()
    monkeypatch.setattr(dispatch_module, "Transports", lambda: stub)
    return stub


@pytest.fixture
def ops_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    """A configured Ops cabinet. Without this, every new action must refuse."""
    monkeypatch.setattr(settings, "ops_user", OPS_USER, raising=False)
    monkeypatch.setattr(settings, "ops_pass", OPS_PASS, raising=False)
    monkeypatch.setattr(settings, "ops_secret", OPS_SECRET, raising=False)
    monkeypatch.setattr(settings, "ops_token", "", raising=False)


@pytest.fixture
def executor_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    """The deployment runs the dedicated executor."""
    monkeypatch.setattr(settings, "easyweek_voucher_production_executor_enabled", True, raising=False)


def session_cookie(user: str = OPS_USER) -> str:
    """A genuine signed session token, exactly as the login form would mint one."""
    return make_session_token(user, OPS_SECRET)


def csrf_for(cookie: str) -> str:
    return csrf_token_for(cookie)


def _app() -> Any:
    """The FastAPI app, imported LAZILY. Deliberately not at module scope.

    ``altegio_bot.main`` calls ``logging.basicConfig(level=INFO)`` when it is
    imported, which raises the ROOT logger's level for the whole process. These
    fixtures are re-exported from ``conftest.py``, so importing the app up here
    would do that to every test session — including suites that have nothing to do
    with the app and that read ``caplog.records`` expecting only their own logger's
    output. ``test_perf.py`` is exactly that: it parses every captured record as
    JSON, and the outbox worker's own plain-text "Outbox sent" INFO line would then
    be captured and fail to parse.

    Importing inside the fixture keeps the side effect scoped to the tests that
    actually drive the app.
    """
    from altegio_bot.main import app

    return app


@pytest_asyncio.fixture
async def ui_client(session_maker, monkeypatch, ops_credentials) -> AsyncIterator[AsyncClient]:
    """An HTTP client holding a valid operator session, against the real app."""
    monkeypatch.setattr(voucher_mailing_module, "SessionLocal", session_maker)
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
    monkeypatch.setattr(campaign_runner_module, "SessionLocal", session_maker)
    cookie = session_cookie()
    async with AsyncClient(transport=ASGITransport(app=_app()), base_url=BASE_URL) as client:
        client.cookies.set(SESSION_COOKIE, cookie)
        client.headers.update({"X-Ops-CSRF": csrf_for(cookie), "Origin": ORIGIN})
        yield client


@pytest_asyncio.fixture
async def anon_client(session_maker, monkeypatch) -> AsyncIterator[AsyncClient]:
    """A client with no session at all, for the refusal tests."""
    monkeypatch.setattr(voucher_mailing_module, "SessionLocal", session_maker)
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    async with AsyncClient(transport=ASGITransport(app=_app()), base_url=BASE_URL) as client:
        client.headers.update({"Origin": ORIGIN})
        yield client


def principal_of(user: str = OPS_USER) -> operations_module.OpsPrincipal:
    """The principal the server would resolve for this account's session."""
    from altegio_bot.ops.auth import session_fingerprint

    cookie = session_cookie(user)
    return operations_module.OpsPrincipal(account=user, session_fingerprint=session_fingerprint(cookie))


__all__ = [
    "BASE_URL",
    "ORIGIN",
    "OPS_PASS",
    "OPS_SECRET",
    "OPS_USER",
    "OTHER_OPS_USER",
    "FakeMutator",
    "FakeReader",
    "FakeSender",
    "StubTransports",
    "transports",
    "anon_client",
    "csrf_for",
    "executor_enabled",
    "ops_credentials",
    "principal_of",
    "session_cookie",
    "ui_client",
]
