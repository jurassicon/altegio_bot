from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import select

from altegio_bot.message_planner import plan_jobs_for_record_event
from altegio_bot.models.models import Client, MessageJob, Record
from altegio_bot.workers.outbox_worker import utcnow


@pytest.mark.asyncio
async def test_opted_out_client_does_not_get_followups(session_maker) -> None:
    now = utcnow()

    async with session_maker() as session:
        async with session.begin():
            client = await session.get(Client, 10)
            assert client is not None
            client.wa_opted_out = True

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

        assert [j.job_type for j in jobs] == ["record_created"]
