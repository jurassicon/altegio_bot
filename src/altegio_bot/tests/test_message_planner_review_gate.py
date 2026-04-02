"""
Integration tests for the review_3d visit-limit gate in the planner.

The Altegio API call (``count_attended_client_visits``) is mocked so
these tests do not require network access.  A real DB session is used
for everything else to catch SQL regressions.

Every test creates a Client record with an altegio_client_id so the
planner can resolve it via session.get().
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from altegio_bot import message_planner as planner_mod
from altegio_bot.message_planner import MAX_VISITS_FOR_REVIEW, plan_jobs_for_record_event
from altegio_bot.models.models import Client, MessageJob, Record
from altegio_bot.workers.outbox_worker import utcnow

_ALTEGIO_CLIENT_ID = 9001


async def _make_client(session: Any, *, company_id: int) -> Client:
    client = Client(
        company_id=company_id,
        altegio_client_id=_ALTEGIO_CLIENT_ID,
        wa_opted_out=False,
    )
    session.add(client)
    await session.flush()
    return client


@pytest.mark.asyncio
async def test_new_visitor_gets_review_job(
    session_maker: Any,
    monkeypatch: Any,
) -> None:
    """Client with 1 attended visit (API) receives review_3d."""
    monkeypatch.setattr(
        planner_mod,
        "count_attended_client_visits",
        AsyncMock(return_value=1),
    )
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            client = await _make_client(session, company_id=1)
            record = Record(
                company_id=1,
                altegio_record_id=111,
                client_id=client.id,
                staff_name="Staff",
                starts_at=now + timedelta(hours=1),
            )
            session.add(record)
            await session.flush()

            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                event_status="create",
            )

        jobs = (await session.execute(select(MessageJob).order_by(MessageJob.id.asc()))).scalars().all()
        job_types = [j.job_type for j in jobs]

        assert "review_3d" in job_types


@pytest.mark.asyncio
async def test_visitor_at_max_threshold_gets_review_job(
    session_maker: Any,
    monkeypatch: Any,
) -> None:
    """Client with exactly MAX_VISITS_FOR_REVIEW (API) gets review_3d."""
    monkeypatch.setattr(
        planner_mod,
        "count_attended_client_visits",
        AsyncMock(return_value=MAX_VISITS_FOR_REVIEW),
    )
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            client = await _make_client(session, company_id=1)
            record = Record(
                company_id=1,
                altegio_record_id=9999,
                client_id=client.id,
                staff_name="Staff",
                starts_at=now + timedelta(hours=1),
            )
            session.add(record)
            await session.flush()

            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                event_status="create",
            )

        jobs = (await session.execute(select(MessageJob).order_by(MessageJob.id.asc()))).scalars().all()
        job_types = [j.job_type for j in jobs]

        assert "review_3d" in job_types


@pytest.mark.asyncio
async def test_experienced_visitor_does_not_get_review_job(
    session_maker: Any,
    monkeypatch: Any,
) -> None:
    """Client with more than MAX_VISITS_FOR_REVIEW (API) does not
    receive review_3d, even if the local DB has fewer records."""
    # API returns 19 visits; local DB has 0 -- API must win.
    monkeypatch.setattr(
        planner_mod,
        "count_attended_client_visits",
        AsyncMock(return_value=MAX_VISITS_FOR_REVIEW + 1),
    )
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            client = await _make_client(session, company_id=1)
            record = Record(
                company_id=1,
                altegio_record_id=9999,
                client_id=client.id,
                staff_name="Staff",
                starts_at=now + timedelta(hours=1),
            )
            session.add(record)
            await session.flush()

            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                event_status="create",
            )

        jobs = (await session.execute(select(MessageJob).order_by(MessageJob.id.asc()))).scalars().all()
        job_types = [j.job_type for j in jobs]

        assert "review_3d" not in job_types


@pytest.mark.asyncio
async def test_experienced_visitor_still_gets_repeat_10d(
    session_maker: Any,
    monkeypatch: Any,
) -> None:
    """Client above the review limit still receives repeat_10d."""
    monkeypatch.setattr(
        planner_mod,
        "count_attended_client_visits",
        AsyncMock(return_value=MAX_VISITS_FOR_REVIEW + 1),
    )
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            client = await _make_client(session, company_id=1)
            record = Record(
                company_id=1,
                altegio_record_id=9999,
                client_id=client.id,
                staff_name="Staff",
                starts_at=now + timedelta(hours=1),
            )
            session.add(record)
            await session.flush()

            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                event_status="create",
            )

        jobs = (await session.execute(select(MessageJob).order_by(MessageJob.id.asc()))).scalars().all()
        job_types = [j.job_type for j in jobs]

        assert "repeat_10d" in job_types


@pytest.mark.asyncio
async def test_no_client_id_skips_review(
    session_maker: Any,
    monkeypatch: Any,
) -> None:
    """Record without a client_id cannot be resolved to an Altegio
    client ID, so review_3d is not scheduled (safe default)."""
    api_mock = AsyncMock()
    monkeypatch.setattr(
        planner_mod,
        "count_attended_client_visits",
        api_mock,
    )
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=1,
                altegio_record_id=111,
                client_id=None,
                staff_name="Staff",
                starts_at=now + timedelta(hours=1),
            )
            session.add(record)
            await session.flush()

            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                event_status="create",
            )

        jobs = (await session.execute(select(MessageJob).order_by(MessageJob.id.asc()))).scalars().all()
        job_types = [j.job_type for j in jobs]

        api_mock.assert_not_called()
        assert "review_3d" not in job_types
