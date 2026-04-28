from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from altegio_bot import message_planner as planner_mod
from altegio_bot.message_planner import plan_jobs_for_record_event
from altegio_bot.models.models import MessageJob, Record
from altegio_bot.workers.outbox_worker import utcnow


@pytest.fixture(autouse=True)
def _mock_altegio_api(monkeypatch):
    """Mock the Altegio API call for all tests in this module.

    Tests here cover scheduling logic, not the visit-limit gate.
    Return 1 (new visitor) so review_3d is always eligible.
    """
    monkeypatch.setattr(
        planner_mod,
        "count_attended_client_visits",
        AsyncMock(return_value=1),
    )


@pytest.mark.asyncio
async def test_create_schedules_created_now(session_maker):
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=1,
                altegio_record_id=111,
                client_id=10,
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

        assert [j.job_type for j in jobs] == [
            "record_created",
            "review_3d",
            "repeat_10d",
        ]
        assert jobs[0].run_at <= now + timedelta(seconds=2)


@pytest.mark.asyncio
async def test_create_schedules_both_reminders_when_more_than_24h_left(
    session_maker,
):
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=1,
                altegio_record_id=111,
                client_id=10,
                staff_name="Staff",
                starts_at=now + timedelta(hours=25),
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

        assert [j.job_type for j in jobs] == [
            "record_created",
            "reminder_24h",
            "reminder_2h",
            "review_3d",
            "repeat_10d",
        ]
        reminder_24h = jobs[1]
        assert reminder_24h.job_type == "reminder_24h"
        assert reminder_24h.run_at == record.starts_at - timedelta(hours=24)

        reminder_2h = jobs[2]
        assert reminder_2h.job_type == "reminder_2h"
        assert reminder_2h.run_at == record.starts_at - timedelta(hours=2)


@pytest.mark.asyncio
async def test_create_schedules_reminder_2h_only_when_2h_to_24h_left(
    session_maker,
):
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=1,
                altegio_record_id=111,
                client_id=10,
                staff_name="Staff",
                starts_at=now + timedelta(hours=3),
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

        assert [j.job_type for j in jobs] == [
            "record_created",
            "reminder_2h",
            "review_3d",
            "repeat_10d",
        ]

        reminder = jobs[1]
        assert reminder.job_type == "reminder_2h"
        assert reminder.run_at == record.starts_at - timedelta(hours=2)


@pytest.mark.asyncio
async def test_update_reschedules_system_jobs(session_maker):
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=1,
                altegio_record_id=111,
                client_id=10,
                staff_name="Staff",
                starts_at=now + timedelta(hours=25),
            )
            session.add(record)
            await session.flush()

            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                event_status="create",
            )

        async with session.begin():
            record.starts_at = now + timedelta(hours=3)
            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                event_status="update",
            )

        jobs = (await session.execute(select(MessageJob).order_by(MessageJob.id.asc()))).scalars().all()

        canceled = [j.job_type for j in jobs if j.status == "canceled"]
        queued = [j.job_type for j in jobs if j.status == "queued"]

        assert sorted(canceled) == sorted(
            [
                "record_created",
                "reminder_24h",
                "reminder_2h",
                "review_3d",
                "repeat_10d",
            ]
        )

        assert sorted(queued) == sorted(
            [
                "record_updated",
                "reminder_2h",
                "review_3d",
                "repeat_10d",
            ]
        )


@pytest.mark.asyncio
async def test_delete_cancels_future_jobs_and_schedules_canceled_and_comeback(
    session_maker,
):
    now = utcnow()
    cancelled_at = datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=1,
                altegio_record_id=111,
                client_id=10,
                staff_name="Staff",
                starts_at=now + timedelta(hours=25),
            )
            session.add(record)
            await session.flush()

            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                event_status="create",
            )

        async with session.begin():
            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                event_status="delete",
                source_cancelled_at=cancelled_at,
            )

        jobs = (await session.execute(select(MessageJob).order_by(MessageJob.id.asc()))).scalars().all()

        canceled = [j.job_type for j in jobs if j.status == "canceled"]
        queued = [j.job_type for j in jobs if j.status == "queued"]

        assert sorted(canceled) == sorted(
            [
                "record_created",
                "reminder_24h",
                "reminder_2h",
                "review_3d",
                "repeat_10d",
            ]
        )

        assert sorted(queued) == sorted(
            [
                "record_canceled",
                "comeback_3d",
            ]
        )

        comeback = [j for j in jobs if j.job_type == "comeback_3d"][0]
        assert comeback.run_at == cancelled_at + timedelta(days=3)
        assert comeback.payload["source_cancelled_at"] == cancelled_at.isoformat()
