"""
Тесты для проверки ограничения отправки запроса на отзыв (review_3d)
только новым посетителям (не более MAX_VISITS_FOR_REVIEW визитов).
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import select

from altegio_bot.message_planner import MAX_VISITS_FOR_REVIEW, plan_jobs_for_record_event
from altegio_bot.models.models import MessageJob, Record
from altegio_bot.workers.outbox_worker import utcnow


async def _create_past_records(session, *, company_id: int, client_id: int, count: int, now) -> None:
    """Создаёт `count` завершённых записей для клиента."""
    for i in range(count):
        record = Record(
            company_id=company_id,
            altegio_record_id=10000 + i,
            client_id=client_id,
            staff_name="Staff",
            starts_at=now - timedelta(days=30 + i),
        )
        session.add(record)
    await session.flush()


@pytest.mark.asyncio
async def test_new_visitor_gets_review_job(session_maker) -> None:
    """Клиент с 1 визитом должен получить задачу review_3d."""
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
        job_types = [j.job_type for j in jobs]

        assert "review_3d" in job_types


@pytest.mark.asyncio
async def test_visitor_at_max_threshold_gets_review_job(session_maker) -> None:
    """Клиент ровно с MAX_VISITS_FOR_REVIEW визитами должен получить review_3d."""
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            # Создаём MAX_VISITS_FOR_REVIEW - 1 предыдущих записей, затем текущую,
            # итого MAX_VISITS_FOR_REVIEW записей
            await _create_past_records(
                session,
                company_id=1,
                client_id=10,
                count=MAX_VISITS_FOR_REVIEW - 1,
                now=now,
            )

            record = Record(
                company_id=1,
                altegio_record_id=9999,
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
        job_types = [j.job_type for j in jobs]

        assert "review_3d" in job_types


@pytest.mark.asyncio
async def test_experienced_visitor_does_not_get_review_job(session_maker) -> None:
    """Клиент с более чем MAX_VISITS_FOR_REVIEW визитами НЕ должен получать review_3d."""
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            # Создаём MAX_VISITS_FOR_REVIEW + 1 предыдущих записей, итого > порога
            await _create_past_records(
                session,
                company_id=1,
                client_id=10,
                count=MAX_VISITS_FOR_REVIEW + 1,
                now=now,
            )

            record = Record(
                company_id=1,
                altegio_record_id=9999,
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
        job_types = [j.job_type for j in jobs]

        assert "review_3d" not in job_types


@pytest.mark.asyncio
async def test_experienced_visitor_still_gets_repeat_10d(session_maker) -> None:
    """Клиент с большим опытом всё равно должен получать repeat_10d (напоминание о повторной записи)."""
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            await _create_past_records(
                session,
                company_id=1,
                client_id=10,
                count=MAX_VISITS_FOR_REVIEW + 1,
                now=now,
            )

            record = Record(
                company_id=1,
                altegio_record_id=9999,
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
        job_types = [j.job_type for j in jobs]

        assert "repeat_10d" in job_types


@pytest.mark.asyncio
async def test_no_client_id_sends_review(session_maker) -> None:
    """Запись без client_id трактуется как новый визит и review_3d создаётся."""
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

        assert "review_3d" in job_types
