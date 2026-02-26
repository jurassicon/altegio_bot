"""Tests for the ops monitoring routes (/ops/*)."""
from __future__ import annotations

from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import altegio_bot.ops.router as ops_router_module
from altegio_bot.main import app


@pytest_asyncio.fixture
async def http_client(
    session_maker, monkeypatch
) -> AsyncGenerator[AsyncClient, None]:
    """AsyncClient wired to the ASGI app with a test DB session."""
    monkeypatch.setattr(ops_router_module, 'SessionLocal', session_maker)
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url='http://test'
    ) as client:
        yield client


@pytest.mark.asyncio
async def test_ops_monitoring_returns_200(http_client: AsyncClient) -> None:
    response = await http_client.get('/ops/monitoring')
    assert response.status_code == 200
    assert 'Monitoring' in response.text


@pytest.mark.asyncio
async def test_ops_queue_returns_200(http_client: AsyncClient) -> None:
    response = await http_client.get('/ops/queue')
    assert response.status_code == 200
    assert 'Queue' in response.text


@pytest.mark.asyncio
async def test_ops_history_returns_200(http_client: AsyncClient) -> None:
    response = await http_client.get('/ops/history')
    assert response.status_code == 200
    assert 'History' in response.text


@pytest.mark.asyncio
async def test_ops_wa_inbox_returns_200(http_client: AsyncClient) -> None:
    response = await http_client.get('/ops/whatsapp/inbox')
    assert response.status_code == 200
    assert 'WA Inbox' in response.text


@pytest.mark.asyncio
async def test_ops_optouts_returns_200(http_client: AsyncClient) -> None:
    response = await http_client.get('/ops/optouts')
    assert response.status_code == 200
    assert 'Opt-outs' in response.text
