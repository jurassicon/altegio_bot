"""Tests for Facebook data-deletion endpoints."""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import altegio_bot.webhooks.facebook_data_deletion as fb_mod
from altegio_bot.main import app

_APP_SECRET = 'test-app-secret-1234'


def _make_signed_request(payload: dict, secret: str) -> str:
    payload_bytes = json.dumps(payload, separators=(',', ':')).encode('utf-8')
    payload_b64 = base64.urlsafe_b64encode(payload_bytes).rstrip(b'=').decode()
    sig = hmac.new(
        secret.encode('utf-8'), payload_bytes, hashlib.sha256
    ).digest()
    sig_b64 = base64.urlsafe_b64encode(sig).rstrip(b'=').decode()
    return f'{sig_b64}.{payload_b64}'


@pytest_asyncio.fixture
async def client(monkeypatch) -> AsyncGenerator[AsyncClient, None]:
    monkeypatch.setattr(fb_mod.settings, 'meta_app_secret', _APP_SECRET)
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url='http://test',
    ) as c:
        yield c


# ---------------------------------------------------------------------------
# GET /data-deletion
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_data_deletion_instructions_page(client: AsyncClient) -> None:
    response = await client.get('/data-deletion')
    assert response.status_code == 200
    assert 'text/html' in response.headers['content-type']
    assert 'www.kitilash.com/privacy-policy/' in response.text


# ---------------------------------------------------------------------------
# POST /webhook/facebook/data-deletion
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_data_deletion_callback_valid(client: AsyncClient) -> None:
    payload = {'user_id': '123456', 'algorithm': 'HMAC-SHA256', 'issued_at': 0}
    signed_request = _make_signed_request(payload, _APP_SECRET)

    response = await client.post(
        '/webhook/facebook/data-deletion',
        data={'signed_request': signed_request},
    )
    assert response.status_code == 200
    body = response.json()
    assert 'url' in body
    assert 'confirmation_code' in body
    assert body['confirmation_code'] in body['url']


@pytest.mark.asyncio
async def test_data_deletion_callback_bad_signature(client: AsyncClient) -> None:
    payload = {'user_id': '999', 'algorithm': 'HMAC-SHA256', 'issued_at': 0}
    signed_request = _make_signed_request(payload, 'wrong-secret')

    response = await client.post(
        '/webhook/facebook/data-deletion',
        data={'signed_request': signed_request},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_data_deletion_callback_malformed(client: AsyncClient) -> None:
    response = await client.post(
        '/webhook/facebook/data-deletion',
        data={'signed_request': 'notvalid'},
    )
    assert response.status_code == 400


@pytest.mark.asyncio
async def test_data_deletion_callback_no_secret(
    monkeypatch, client: AsyncClient
) -> None:
    monkeypatch.setattr(fb_mod.settings, 'meta_app_secret', '')
    payload = {'user_id': '1', 'algorithm': 'HMAC-SHA256', 'issued_at': 0}
    signed_request = _make_signed_request(payload, _APP_SECRET)

    response = await client.post(
        '/webhook/facebook/data-deletion',
        data={'signed_request': signed_request},
    )
    assert response.status_code == 500


# ---------------------------------------------------------------------------
# _parse_signed_request unit tests
# ---------------------------------------------------------------------------


def test_parse_signed_request_valid() -> None:
    payload = {'user_id': 'u1'}
    sr = _make_signed_request(payload, _APP_SECRET)
    result = fb_mod._parse_signed_request(sr, _APP_SECRET)
    assert result['user_id'] == 'u1'


def test_parse_signed_request_wrong_secret() -> None:
    sr = _make_signed_request({'user_id': 'u1'}, _APP_SECRET)
    with pytest.raises(ValueError, match='Signature'):
        fb_mod._parse_signed_request(sr, 'bad-secret')


def test_parse_signed_request_no_dot() -> None:
    with pytest.raises(ValueError):
        fb_mod._parse_signed_request('nodothere', _APP_SECRET)
