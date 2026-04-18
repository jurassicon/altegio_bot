"""Tests for the ops monitoring routes (/ops/*)."""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from urllib.parse import urlencode

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient
from starlette.requests import Request as StarletteRequest

import altegio_bot.ops.router as ops_router_module
from altegio_bot.main import app
from altegio_bot.models.models import (
    AltegioEvent,
    Client,
    MessageJob,
    OutboxMessage,
    Record,
    WhatsAppEvent,
)
from altegio_bot.ops.auth import require_ops_auth
from altegio_bot.ops.router import _period_params


def _make_request(period: str | None = None, from_dt: str = "", to_dt: str = "") -> StarletteRequest:
    """Build a minimal Starlette Request with the given query params."""
    params: dict[str, str] = {}
    if period is not None:
        params["period"] = period
    if from_dt:
        params["from_dt"] = from_dt
    if to_dt:
        params["to_dt"] = to_dt
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/ops/history",
        "query_string": urlencode(params).encode(),
        "headers": [],
    }
    return StarletteRequest(scope)


@pytest_asyncio.fixture
async def http_client(session_maker, monkeypatch) -> AsyncGenerator[AsyncClient, None]:
    """AsyncClient wired to the ASGI app with a test DB session."""
    monkeypatch.setattr(ops_router_module, "SessionLocal", session_maker)
    monkeypatch.setitem(app.dependency_overrides, require_ops_auth, lambda: None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


@pytest.mark.asyncio
async def test_ops_monitoring_returns_200(http_client: AsyncClient) -> None:
    response = await http_client.get("/ops/monitoring")
    assert response.status_code == 200
    assert "Monitoring" in response.text


@pytest.mark.asyncio
async def test_ops_queue_returns_200(http_client: AsyncClient) -> None:
    response = await http_client.get("/ops/queue")
    assert response.status_code == 200
    assert "Queue" in response.text


@pytest.mark.asyncio
async def test_ops_history_returns_200(http_client: AsyncClient) -> None:
    response = await http_client.get("/ops/history")
    assert response.status_code == 200
    assert "History" in response.text


@pytest.mark.asyncio
async def test_ops_wa_inbox_returns_200(http_client: AsyncClient) -> None:
    response = await http_client.get("/ops/whatsapp/inbox")
    assert response.status_code == 200
    assert "WA Events" in response.text


@pytest.mark.asyncio
async def test_ops_wa_inbox_tab_inbox(http_client: AsyncClient) -> None:
    response = await http_client.get("/ops/whatsapp/inbox?tab=inbox")
    assert response.status_code == 200
    assert "Inbox" in response.text
    assert "Delivery" in response.text


@pytest.mark.asyncio
async def test_ops_wa_delivery_tab(http_client: AsyncClient) -> None:
    response = await http_client.get("/ops/whatsapp/inbox?tab=delivery")
    assert response.status_code == 200
    assert "Status Msg ID" in response.text
    assert "Delivery" in response.text


@pytest.mark.asyncio
async def test_ops_optouts_returns_200(http_client: AsyncClient) -> None:
    response = await http_client.get("/ops/optouts")
    assert response.status_code == 200
    assert "Opt-outs" in response.text


@pytest.mark.asyncio
async def test_ops_job_not_found(http_client: AsyncClient) -> None:
    response = await http_client.get("/ops/job/9999999")
    assert response.status_code == 200
    assert "Job not found" in response.text


@pytest.mark.asyncio
async def test_ops_record_not_found(http_client: AsyncClient) -> None:
    response = await http_client.get("/ops/record/9999999")
    assert response.status_code == 200
    assert "Record not found" in response.text


@pytest.mark.asyncio
async def test_ops_job_shows_record_history(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Job detail page shows altegio events and sibling jobs for the same record."""
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            record = Record(
                id=1,
                company_id=1,
                altegio_record_id=42,
                client_id=1,
                altegio_client_id=1,
                is_deleted=False,
                raw={},
            )
            session.add(record)
            await session.flush()

            event = AltegioEvent(
                dedupe_key="ev-42-create",
                company_id=1,
                resource="record",
                resource_id=42,
                event_status="create",
                status="processed",
                query={},
                headers={},
                payload={},
            )
            session.add(event)

            job = MessageJob(
                id=1,
                company_id=1,
                record_id=1,
                client_id=1,
                job_type="record_created",
                run_at=now,
                status="done",
                attempts=1,
                max_attempts=5,
                dedupe_key="job-1",
                payload={},
            )
            session.add(job)

    response = await http_client.get("/ops/job/1")
    assert response.status_code == 200
    assert "Altegio Events for this Record" in response.text
    assert "All Jobs for this Record" in response.text
    assert "altegio: 42" in response.text


@pytest.mark.asyncio
async def test_ops_record_shows_full_timeline(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Record page shows altegio events, jobs, and outbox messages."""
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            record = Record(
                id=2,
                company_id=1,
                altegio_record_id=99,
                client_id=1,
                altegio_client_id=1,
                is_deleted=True,
                raw={},
            )
            session.add(record)
            await session.flush()

            event = AltegioEvent(
                dedupe_key="ev-99-delete",
                company_id=1,
                resource="record",
                resource_id=99,
                event_status="delete",
                status="processed",
                query={},
                headers={},
                payload={},
            )
            session.add(event)

            job = MessageJob(
                id=2,
                company_id=1,
                record_id=2,
                client_id=1,
                job_type="comeback_3d",
                run_at=now,
                status="queued",
                attempts=0,
                max_attempts=5,
                dedupe_key="job-2",
                payload={},
            )
            session.add(job)

    response = await http_client.get("/ops/record/2")
    assert response.status_code == 200
    assert "Record #2" in response.text
    assert "Altegio Events" in response.text
    assert "Message Jobs" in response.text
    assert "comeback_3d" in response.text


@pytest.mark.asyncio
async def test_ops_history_phone_search_with_country_code(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """History phone filter finds messages even when stored phone lacks country code."""
    now = datetime.now(timezone.utc)
    async with session_maker() as session:
        async with session.begin():
            # phone stored in local format (without country code / without +)
            client = Client(
                id=50,
                company_id=1,
                altegio_client_id=50,
                display_name="Phone Test",
                phone_e164="736855823",
                raw={},
            )
            session.add(client)
            await session.flush()

            msg = OutboxMessage(
                company_id=1,
                client_id=50,
                phone_e164="736855823",
                template_code="record_created",
                language="de",
                body="Test",
                status="sent",
                scheduled_at=now,
                sent_at=now,
                meta={},
            )
            session.add(msg)

    # Search with country-code prefix: '491736855823' should find '736855823'
    response = await http_client.get("/ops/history?" + urlencode({"phone_e164": "491736855823", "period": "24h"}))
    assert response.status_code == 200
    assert "736855823" in response.text

    # Searching without country code should also work
    response2 = await http_client.get("/ops/history?" + urlencode({"phone_e164": "736855823", "period": "24h"}))
    assert response2.status_code == 200
    assert "736855823" in response2.text


@pytest.mark.asyncio
async def test_ops_optouts_deduplication(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Optouts page shows one row per (company, phone) even when multiple client
    records share the same phone."""
    opted_out_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    phone = "+381638400431"
    async with session_maker() as session:
        async with session.begin():
            # Three client records for the same company+phone
            for cid, acid in [(60, 60), (61, 61), (62, 62)]:
                session.add(
                    Client(
                        id=cid,
                        company_id=1,
                        altegio_client_id=acid,
                        display_name="Dup Test",
                        phone_e164=phone,
                        wa_opted_out=True,
                        wa_opted_out_at=opted_out_at,
                        wa_opt_out_reason="wa:stop",
                        raw={},
                    )
                )

    response = await http_client.get("/ops/optouts")
    assert response.status_code == 200
    # The phone should appear exactly once (deduplicated by company+phone)
    assert response.text.count(phone) == 1


# ---------------------------------------------------------------------------
# _period_params unit tests
# ---------------------------------------------------------------------------


def test_period_params_today() -> None:
    req = _make_request("today")
    from_dt, to_dt = _period_params(req)
    now = datetime.now(timezone.utc)
    assert from_dt.hour == 0 and from_dt.minute == 0 and from_dt.second == 0
    assert from_dt.date() == now.date()
    assert to_dt <= now + timedelta(seconds=1)


def test_period_params_yesterday() -> None:
    req = _make_request("yesterday")
    from_dt, to_dt = _period_params(req)
    now = datetime.now(timezone.utc)
    yesterday = now - timedelta(days=1)
    assert from_dt.date() == yesterday.date()
    assert from_dt.hour == 0 and from_dt.minute == 0 and from_dt.second == 0
    assert to_dt == from_dt + timedelta(days=1)


def test_period_params_last_7d() -> None:
    req = _make_request("last_7d")
    from_dt, to_dt = _period_params(req)
    now = datetime.now(timezone.utc)
    assert abs((to_dt - from_dt).days - 7) <= 1
    assert to_dt <= now + timedelta(seconds=1)


def test_period_params_last_30d() -> None:
    req = _make_request("last_30d")
    from_dt, to_dt = _period_params(req)
    now = datetime.now(timezone.utc)
    assert abs((to_dt - from_dt).days - 30) <= 1
    assert to_dt <= now + timedelta(seconds=1)


def test_period_params_this_week() -> None:
    req = _make_request("this_week")
    from_dt, to_dt = _period_params(req)
    now = datetime.now(timezone.utc)
    # from_dt should be Monday of this week at midnight
    assert from_dt.weekday() == 0
    assert from_dt.hour == 0 and from_dt.minute == 0 and from_dt.second == 0
    assert to_dt <= now + timedelta(seconds=1)


def test_period_params_last_week() -> None:
    req = _make_request("last_week")
    from_dt, to_dt = _period_params(req)
    # Both bounds should be Mondays
    assert from_dt.weekday() == 0
    assert to_dt.weekday() == 0
    # Exactly 7 days apart
    assert (to_dt - from_dt) == timedelta(days=7)


def test_period_params_this_month() -> None:
    req = _make_request("this_month")
    from_dt, to_dt = _period_params(req)
    now = datetime.now(timezone.utc)
    assert from_dt.day == 1
    assert from_dt.hour == 0 and from_dt.minute == 0 and from_dt.second == 0
    assert from_dt.month == now.month
    assert to_dt <= now + timedelta(seconds=1)


def test_period_params_last_month() -> None:
    req = _make_request("last_month")
    from_dt, to_dt = _period_params(req)
    now = datetime.now(timezone.utc)
    first_of_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    assert to_dt == first_of_this_month
    assert from_dt.day == 1
    assert from_dt.month != now.month or from_dt.year != now.year


def test_period_params_legacy_24h() -> None:
    req = _make_request("24h")
    from_dt, to_dt = _period_params(req)
    assert abs((to_dt - from_dt).total_seconds() - 86400) < 2


def test_period_params_legacy_7d() -> None:
    req = _make_request("7d")
    from_dt, to_dt = _period_params(req)
    assert abs((to_dt - from_dt).days - 7) <= 1


def test_period_params_default_no_period() -> None:
    """When no period query param is supplied, _period_params falls back to 24h."""
    req = _make_request()
    from_dt, to_dt = _period_params(req)
    assert abs((to_dt - from_dt).total_seconds() - 86400) < 2


@pytest.mark.asyncio
async def test_ops_history_default_period_is_today(http_client: AsyncClient) -> None:
    """The /ops/history page should default to period=today (not 24h)."""
    response = await http_client.get("/ops/history")
    assert response.status_code == 200
    # The period select should have 'today' selected by default
    assert 'value="today" selected' in response.text


def _inbound_payload(from_phone: str, body: str) -> dict:
    return {
        "entry": [
            {
                "changes": [
                    {
                        "value": {
                            "metadata": {"phone_number_id": "pni-test"},
                            "messages": [
                                {
                                    "id": "wamid.test-real",
                                    "from": from_phone,
                                    "type": "text",
                                    "text": {"body": body},
                                }
                            ],
                        }
                    }
                ]
            }
        ]
    }


@pytest.mark.asyncio
async def test_wa_inbox_excludes_chatwoot_origin_events(
    http_client: AsyncClient,
    session_maker,
) -> None:
    """Chatwoot-origin mirror rows (chatwoot_conversation_id IS NOT NULL)
    must not appear on the Inbox tab; real Meta events must remain visible."""
    async with session_maker() as session:
        async with session.begin():
            real_event = WhatsAppEvent(
                dedupe_key="wa:wamid.test-real",
                status="processed",
                payload=_inbound_payload("491meta000", "msg-from-meta"),
                query={},
                headers={},
                chatwoot_conversation_id=None,
            )
            session.add(real_event)
            cw_event = WhatsAppEvent(
                dedupe_key="chatwoot:230:1470",
                status="processed",
                payload=_inbound_payload("491cw000", "msg-from-chatwoot"),
                query={},
                headers={},
                chatwoot_conversation_id=230,
            )
            session.add(cw_event)

    response = await http_client.get("/ops/whatsapp/inbox?tab=inbox&period=24h")
    assert response.status_code == 200
    assert "491meta000" in response.text
    assert "491cw000" not in response.text
