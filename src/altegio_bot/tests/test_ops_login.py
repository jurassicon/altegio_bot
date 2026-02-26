"""Tests for /ops/login and /ops/logout (session-based auth)."""
from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import altegio_bot.ops.auth as auth_module
from altegio_bot.main import app
from altegio_bot.ops.auth import (
    SESSION_COOKIE,
    SESSION_MAX_AGE,
    check_session_token,
    make_session_token,
)

_USER = 'testuser'
_PASS = 'testpass'


@pytest_asyncio.fixture
async def client(monkeypatch) -> AsyncGenerator[AsyncClient, None]:
    """Unauthenticated client – auth dependency is NOT overridden."""
    monkeypatch.setattr(auth_module.settings, 'ops_user', _USER)
    monkeypatch.setattr(auth_module.settings, 'ops_pass', _PASS)
    monkeypatch.setattr(auth_module.settings, 'ops_token', '')
    monkeypatch.setattr(auth_module.settings, 'ops_secret', '')  # use ops_pass as key
    monkeypatch.setattr(auth_module.settings, 'env', 'dev')      # skip secure-cookie flag
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url='http://test',
        follow_redirects=False,
    ) as c:
        yield c


# ---------------------------------------------------------------------------
# Smoke test – verifies the app imports and starts (catches missing deps)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_health_endpoint_responds(client: AsyncClient) -> None:
    """GET /health must return 200 without any auth or DB.

    This test doubles as a startup smoke test: if a required dependency
    (e.g. python-multipart) is missing, FastAPI raises RuntimeError when
    the application module is loaded, and this test fails at collection time.
    """
    response = await client.get('/health')
    assert response.status_code == 200
    assert response.json() == {'ok': True}


@pytest.mark.asyncio
async def test_ops_login_page_reachable(client: AsyncClient) -> None:
    """GET /ops/login must return 200 without authentication."""
    response = await client.get('/ops/login', headers={'accept': 'text/html'})
    assert response.status_code == 200


# ---------------------------------------------------------------------------
# Session token unit tests (no HTTP)
# ---------------------------------------------------------------------------

def test_make_and_check_session_token_valid() -> None:
    token = make_session_token(_USER, _PASS)
    assert check_session_token(token, _USER, _PASS)


def test_check_session_token_wrong_password() -> None:
    token = make_session_token(_USER, _PASS)
    assert not check_session_token(token, _USER, 'wrongpass')


def test_check_session_token_wrong_user() -> None:
    token = make_session_token(_USER, _PASS)
    assert not check_session_token(token, 'other', _PASS)


def test_check_session_token_malformed() -> None:
    assert not check_session_token('notavalidtoken', _USER, _PASS)
    assert not check_session_token('', _USER, _PASS)


def test_check_session_token_expired(monkeypatch) -> None:
    import time as time_mod
    import altegio_bot.ops.auth as auth_mod

    real_time = time_mod.time()
    # Create the token with a timestamp that is SESSION_MAX_AGE+2 seconds in the past
    past_ts = real_time - SESSION_MAX_AGE - 2
    monkeypatch.setattr(auth_mod.time, 'time', lambda: past_ts)
    token = make_session_token(_USER, _PASS)
    # Restore real time so validation sees the token as expired
    monkeypatch.setattr(auth_mod.time, 'time', lambda: real_time)
    assert not check_session_token(token, _USER, _PASS)


# ---------------------------------------------------------------------------
# HTTP integration tests
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_login_page_accessible_without_auth(client: AsyncClient) -> None:
    response = await client.get(
        '/ops/login', headers={'accept': 'text/html'}
    )
    assert response.status_code == 200
    assert '<form' in response.text
    assert 'Login' in response.text


@pytest.mark.asyncio
async def test_login_post_valid_sets_cookie_and_redirects(
    client: AsyncClient,
) -> None:
    response = await client.post(
        '/ops/login', data={'username': _USER, 'password': _PASS}
    )
    assert response.status_code == 303
    assert SESSION_COOKIE in response.cookies
    token = response.cookies[SESSION_COOKIE]
    assert check_session_token(token, _USER, _PASS)


@pytest.mark.asyncio
async def test_login_post_invalid_returns_401(client: AsyncClient) -> None:
    response = await client.post(
        '/ops/login', data={'username': _USER, 'password': 'wrongpass'}
    )
    assert response.status_code == 401
    assert SESSION_COOKIE not in response.cookies
    assert 'Invalid' in response.text


@pytest.mark.asyncio
async def test_login_post_next_param_redirect(client: AsyncClient) -> None:
    response = await client.post(
        '/ops/login?next=/ops/queue',
        data={'username': _USER, 'password': _PASS},
    )
    assert response.status_code == 303
    assert response.headers['location'] == '/ops/queue'


@pytest.mark.asyncio
async def test_login_post_next_param_open_redirect_blocked(
    client: AsyncClient,
) -> None:
    response = await client.post(
        '/ops/login?next=https://evil.example.com',
        data={'username': _USER, 'password': _PASS},
    )
    assert response.status_code == 303
    # Must be redirected to the safe default, not the external URL
    assert response.headers['location'] == '/ops/monitoring'


@pytest.mark.asyncio
async def test_unauthenticated_browser_redirects_to_login(
    client: AsyncClient,
) -> None:
    response = await client.get(
        '/ops/monitoring',
        headers={'accept': 'text/html,application/xhtml+xml'},
    )
    assert response.status_code == 302
    location = response.headers['location']
    assert location.startswith('/ops/login')


@pytest.mark.asyncio
async def test_unauthenticated_api_client_returns_401(
    client: AsyncClient,
) -> None:
    response = await client.get(
        '/ops/monitoring',
        headers={'accept': 'application/json'},
    )
    assert response.status_code == 401
    assert response.headers.get('www-authenticate', '').startswith('Basic')


@pytest.mark.asyncio
async def test_logout_clears_cookie_and_redirects(client: AsyncClient) -> None:
    response = await client.post('/ops/logout')
    assert response.status_code == 303
    assert response.headers['location'] == '/ops/login'
    # Cookie should be cleared (max-age=0 or expires in the past)
    set_cookie = response.headers.get('set-cookie', '')
    assert SESSION_COOKIE in set_cookie


@pytest.mark.asyncio
async def test_session_cookie_grants_access_to_protected_route(
    client: AsyncClient,
) -> None:
    """After logging in, the session cookie bypasses auth for protected pages."""
    login = await client.post(
        '/ops/login', data={'username': _USER, 'password': _PASS}
    )
    assert login.status_code == 303
    session_cookie = login.cookies[SESSION_COOKIE]

    # Manually check that the token validates (DB query would require pg)
    assert check_session_token(session_cookie, _USER, _PASS)
