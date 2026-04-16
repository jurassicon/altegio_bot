"""Тесты retry campaign message_job через Ops API.

Сценарии:
  1. retry сбрасывает упавший job корректно (outcome=retried).
  2. retry отклоняет job с успешным OutboxMessage (outcome=already_sent).
  3. retry отклоняет получателя без message_job_id (outcome=no_message_job).
  4. retry отклоняет job неправильного типа (outcome=wrong_job_type).
  5. endpoint возвращает 404 если recipient не принадлежит run_id.
  6. retry отклоняет job в статусе 'done' (outcome=not_retryable).
  7. retry допускает canceled job (outcome=retried).
  8. retry допускает queued+locked (застрявший) job (outcome=retried).
  9. retry записывает last_manual_retry_at в recipient.meta.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import datetime, timezone

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

import altegio_bot.campaigns.runner as runner_module
import altegio_bot.ops.campaigns_api as campaigns_api_module
from altegio_bot.main import app
from altegio_bot.models.models import (
    CampaignRecipient,
    CampaignRun,
    MessageJob,
    OutboxMessage,
)
from altegio_bot.ops.auth import require_ops_auth

COMPANY = 758285
PERIOD_START = datetime(2026, 1, 1, tzinfo=timezone.utc)
PERIOD_END = datetime(2026, 2, 1, tzinfo=timezone.utc)
CLIENT_ID = 1
PHONE = "+10000000001"

NEWSLETTER_JOB_TYPE = "newsletter_new_clients_monthly"
OTHER_JOB_TYPE = "campaign_execute_new_clients_monthly"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_run(session, *, status: str = "completed") -> CampaignRun:
    run = CampaignRun(
        campaign_code="new_clients_monthly",
        mode="send-real",
        company_ids=[COMPANY],
        location_id=1,
        card_type_id="type-1",
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        status=status,
        total_clients_seen=0,
        candidates_count=0,
        meta={},
    )
    session.add(run)
    return run


def _make_job(
    session,
    *,
    job_type: str = NEWSLETTER_JOB_TYPE,
    job_status: str = "failed",
    attempts: int = 5,
    locked_at: datetime | None = None,
    last_error: str | None = "Send failed: provider error",
    dedupe_key: str | None = None,
) -> MessageJob:
    job = MessageJob(
        company_id=COMPANY,
        client_id=CLIENT_ID,
        job_type=job_type,
        status=job_status,
        attempts=attempts,
        max_attempts=5,
        run_at=PERIOD_START,
        locked_at=locked_at,
        last_error=last_error,
        dedupe_key=dedupe_key or f"dk-{id(session)}",
        payload={"campaign_run_id": 1, "kind": job_type},
    )
    session.add(job)
    return job


def _make_recipient(
    session,
    run_id: int,
    *,
    message_job_id: int | None = None,
    status: str = "queued",
) -> CampaignRecipient:
    r = CampaignRecipient(
        campaign_run_id=run_id,
        company_id=COMPANY,
        client_id=CLIENT_ID,
        phone_e164=PHONE,
        status=status,
        message_job_id=message_job_id,
        meta={},
    )
    session.add(r)
    return r


def _make_outbox(
    session,
    job_id: int,
    *,
    outbox_status: str = "sent",
) -> OutboxMessage:
    msg = OutboxMessage(
        company_id=COMPANY,
        client_id=CLIENT_ID,
        job_id=job_id,
        phone_e164=PHONE,
        template_code=NEWSLETTER_JOB_TYPE,
        language="de",
        body="Hello",
        status=outbox_status,
        sent_at=datetime.now(timezone.utc),
        scheduled_at=PERIOD_START,
        meta={},
    )
    session.add(msg)
    return msg


# ---------------------------------------------------------------------------
# Fixture: http client + monkeypatched SessionLocal
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def http_client(session_maker, monkeypatch) -> AsyncGenerator[AsyncClient, None]:
    monkeypatch.setattr(campaigns_api_module, "SessionLocal", session_maker)
    monkeypatch.setattr(runner_module, "SessionLocal", session_maker)
    monkeypatch.setitem(app.dependency_overrides, require_ops_auth, lambda: None)
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        yield client


# ---------------------------------------------------------------------------
# Тест 1: retry сбрасывает failed job корректно
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_resets_failed_job(http_client: AsyncClient, session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            job = _make_job(
                session,
                job_status="failed",
                attempts=5,
                last_error="Send failed: timeout",
                dedupe_key="dk-test-retry-1",
            )
            await session.flush()
            r = _make_recipient(session, run.id, message_job_id=job.id)
            await session.flush()
            run_id, rec_id, job_id = run.id, r.id, job.id

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/{rec_id}/retry")
    assert resp.status_code == 200
    data = resp.json()
    assert data["outcome"] == "retried"
    assert data["job_id"] == job_id
    assert data["prev_status"] == "failed"
    assert data["prev_attempts"] == 5

    # Проверить что job реально сброшен в БД
    async with session_maker() as session:
        job_db = await session.get(MessageJob, job_id)
    assert job_db.status == "queued"
    assert job_db.locked_at is None
    assert job_db.last_error is None
    assert job_db.attempts == 0


# ---------------------------------------------------------------------------
# Тест 2: retry отклоняет job с успешным OutboxMessage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_rejects_already_sent_job(http_client: AsyncClient, session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            job = _make_job(
                session,
                job_status="done",
                attempts=1,
                last_error=None,
                dedupe_key="dk-test-retry-2",
            )
            await session.flush()
            _make_outbox(session, job.id, outbox_status="sent")
            r = _make_recipient(session, run.id, message_job_id=job.id)
            await session.flush()
            run_id, rec_id = run.id, r.id

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/{rec_id}/retry")
    assert resp.status_code == 200
    data = resp.json()
    assert data["outcome"] == "already_sent"
    assert "outbox_id" in data


# ---------------------------------------------------------------------------
# Тест 3: retry отклоняет получателя без message_job_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_rejects_no_message_job(http_client: AsyncClient, session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            r = _make_recipient(session, run.id, message_job_id=None, status="candidate")
            await session.flush()
            run_id, rec_id = run.id, r.id

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/{rec_id}/retry")
    assert resp.status_code == 200
    data = resp.json()
    assert data["outcome"] == "no_message_job"


# ---------------------------------------------------------------------------
# Тест 4: retry отклоняет job неправильного типа
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_rejects_wrong_job_type(http_client: AsyncClient, session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            job = _make_job(
                session,
                job_type=OTHER_JOB_TYPE,
                job_status="failed",
                dedupe_key="dk-test-retry-4",
            )
            await session.flush()
            r = _make_recipient(session, run.id, message_job_id=job.id)
            await session.flush()
            run_id, rec_id = run.id, r.id

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/{rec_id}/retry")
    assert resp.status_code == 200
    data = resp.json()
    assert data["outcome"] == "wrong_job_type"
    assert data["job_type"] == OTHER_JOB_TYPE


# ---------------------------------------------------------------------------
# Тест 5: endpoint возвращает 404 если recipient не принадлежит run_id
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_returns_404_wrong_run(http_client: AsyncClient, session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run_a = _make_run(session)
            run_b = _make_run(session)
            await session.flush()
            job = _make_job(
                session,
                job_status="failed",
                dedupe_key="dk-test-retry-5",
            )
            await session.flush()
            r = _make_recipient(session, run_a.id, message_job_id=job.id)
            await session.flush()
            run_b_id, rec_id = run_b.id, r.id

    resp = await http_client.post(f"/ops/campaigns/runs/{run_b_id}/recipients/{rec_id}/retry")
    assert resp.status_code == 404


# ---------------------------------------------------------------------------
# Тест 6: retry отклоняет job в статусе 'done' без OutboxMessage
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_rejects_done_job_without_outbox(http_client: AsyncClient, session_maker) -> None:
    """job.status='done' но OutboxMessage нет — not_retryable."""
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            job = _make_job(
                session,
                job_status="done",
                attempts=1,
                last_error=None,
                dedupe_key="dk-test-retry-6",
            )
            await session.flush()
            r = _make_recipient(session, run.id, message_job_id=job.id)
            await session.flush()
            run_id, rec_id = run.id, r.id

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/{rec_id}/retry")
    assert resp.status_code == 200
    data = resp.json()
    assert data["outcome"] == "not_retryable"
    assert data["job_status"] == "done"


# ---------------------------------------------------------------------------
# Тест 7: retry допускает canceled job
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_allows_canceled_job(http_client: AsyncClient, session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            job = _make_job(
                session,
                job_status="canceled",
                attempts=0,
                last_error="Skipped: client unsubscribed",
                dedupe_key="dk-test-retry-7",
            )
            await session.flush()
            r = _make_recipient(session, run.id, message_job_id=job.id)
            await session.flush()
            run_id, rec_id, job_id = run.id, r.id, job.id

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/{rec_id}/retry")
    assert resp.status_code == 200
    data = resp.json()
    assert data["outcome"] == "retried"
    assert data["prev_status"] == "canceled"

    async with session_maker() as session:
        job_db = await session.get(MessageJob, job_id)
    assert job_db.status == "queued"
    assert job_db.attempts == 0
    assert job_db.locked_at is None
    assert job_db.last_error is None


# ---------------------------------------------------------------------------
# Тест 8: retry очищает lock у queued+locked (застрявшего) job
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_allows_stuck_queued_job(http_client: AsyncClient, session_maker) -> None:
    stuck_at = datetime(2026, 1, 1, tzinfo=timezone.utc)

    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            job = _make_job(
                session,
                job_status="queued",
                attempts=2,
                locked_at=stuck_at,
                last_error="Send failed: previous attempt",
                dedupe_key="dk-test-retry-8",
            )
            await session.flush()
            r = _make_recipient(session, run.id, message_job_id=job.id)
            await session.flush()
            run_id, rec_id, job_id = run.id, r.id, job.id

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/{rec_id}/retry")
    assert resp.status_code == 200
    data = resp.json()
    assert data["outcome"] == "retried"
    assert data["prev_status"] == "queued"
    assert data["prev_attempts"] == 2

    async with session_maker() as session:
        job_db = await session.get(MessageJob, job_id)
    assert job_db.status == "queued"
    assert job_db.locked_at is None
    assert job_db.attempts == 0
    assert job_db.last_error is None


# ---------------------------------------------------------------------------
# Тест 9: retry записывает last_manual_retry_at в recipient.meta
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retry_records_meta_timestamp(http_client: AsyncClient, session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            run = _make_run(session)
            await session.flush()
            job = _make_job(
                session,
                job_status="failed",
                dedupe_key="dk-test-retry-9",
            )
            await session.flush()
            r = _make_recipient(session, run.id, message_job_id=job.id)
            await session.flush()
            run_id, rec_id = run.id, r.id

    resp = await http_client.post(f"/ops/campaigns/runs/{run_id}/recipients/{rec_id}/retry")
    assert resp.status_code == 200
    assert resp.json()["outcome"] == "retried"

    async with session_maker() as session:
        rec_db = await session.get(CampaignRecipient, rec_id)
    meta = rec_db.meta or {}
    assert "last_manual_retry_at" in meta
