from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import select

from altegio_bot.message_planner import plan_jobs_for_record_event
from altegio_bot.models.models import MessageJob, Record
from altegio_bot.workers.outbox_worker import utcnow


@pytest.mark.asyncio
async def test_create_schedules_created_now(session_maker):
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=1,
                altegio_record_id=111,
                client_id=10,
                staff_name='Staff',
                starts_at=now + timedelta(hours=1),
            )
            session.add(record)
            await session.flush()

            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                status='create',
            )

        jobs = (
            await session.execute(
                select(MessageJob).order_by(MessageJob.id.asc())
            )
        ).scalars().all()

        assert [j.job_type for j in jobs] == [
            'record_created',
            'review_3d',
            'repeat_10d',
        ]
        assert jobs[0].run_at <= now + timedelta(seconds=2)


@pytest.mark.asyncio
async def test_create_schedules_reminder_24h_only_when_more_than_24h_left(
    session_maker,
):
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=1,
                altegio_record_id=111,
                client_id=10,
                staff_name='Staff',
                starts_at=now + timedelta(hours=25),
            )
            session.add(record)
            await session.flush()

            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                status='create',
            )

        jobs = (
            await session.execute(
                select(MessageJob).order_by(MessageJob.id.asc())
            )
        ).scalars().all()

        assert [j.job_type for j in jobs] == [
            'record_created',
            'reminder_24h',
            'review_3d',
            'repeat_10d',
        ]

        reminder = jobs[1]
        assert reminder.job_type == 'reminder_24h'
        assert reminder.run_at == record.starts_at - timedelta(hours=24)


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
                staff_name='Staff',
                starts_at=now + timedelta(hours=3),
            )
            session.add(record)
            await session.flush()

            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                status='create',
            )

        jobs = (
            await session.execute(
                select(MessageJob).order_by(MessageJob.id.asc())
            )
        ).scalars().all()

        assert [j.job_type for j in jobs] == [
            'record_created',
            'reminder_2h',
            'review_3d',
            'repeat_10d',
        ]

        reminder = jobs[1]
        assert reminder.job_type == 'reminder_2h'
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
                staff_name='Staff',
                starts_at=now + timedelta(hours=25),
            )
            session.add(record)
            await session.flush()

            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                status='create',
            )

        async with session.begin():
            record.starts_at = now + timedelta(hours=3)
            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                status='update',
            )

        jobs = (
            await session.execute(
                select(MessageJob).order_by(MessageJob.id.asc())
            )
        ).scalars().all()

        canceled = [j.job_type for j in jobs if j.status == 'canceled']
        queued = [j.job_type for j in jobs if j.status == 'queued']

        assert sorted(canceled) == sorted([
            'record_created',
            'reminder_24h',
            'review_3d',
            'repeat_10d',
        ])

        assert sorted(queued) == sorted([
            'record_updated',
            'reminder_2h',
            'review_3d',
            'repeat_10d',
        ])


@pytest.mark.asyncio
async def test_delete_cancels_future_jobs_and_schedules_canceled_and_comeback(
    session_maker,
):
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            record = Record(
                company_id=1,
                altegio_record_id=111,
                client_id=10,
                staff_name='Staff',
                starts_at=now + timedelta(hours=25),
            )
            session.add(record)
            await session.flush()

            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                status='create',
            )

        async with session.begin():
            await plan_jobs_for_record_event(
                session,
                company_id=record.company_id,
                record_id=record.id,
                status='delete',
            )

        jobs = (
            await session.execute(
                select(MessageJob).order_by(MessageJob.id.asc())
            )
        ).scalars().all()

        canceled = [j.job_type for j in jobs if j.status == 'canceled']
        queued = [j.job_type for j in jobs if j.status == 'queued']

        assert sorted(canceled) == sorted([
            'record_created',
            'reminder_24h',
            'review_3d',
            'repeat_10d',
        ])

        assert sorted(queued) == sorted([
            'record_canceled',
            'comeback_3d',
        ])

        comeback = [j for j in jobs if j.job_type == 'comeback_3d'][0]
        assert comeback.run_at >= now + timedelta(days=3)