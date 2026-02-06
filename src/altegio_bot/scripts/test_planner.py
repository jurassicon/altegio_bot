from __future__ import annotations

import asyncio

from sqlalchemy import select

from altegio_bot.db import SessionLocal
from altegio_bot.message_planner import plan_jobs_for_record_event
from altegio_bot.models.models import Client, MessageJob, Record

RECORD_ID = 2
EVENT = "update"  # "create" / "update" / "delete"


async def main() -> None:
    async with SessionLocal() as session:
        async with session.begin():
            record = await session.get(Record, RECORD_ID)
            if record is None:
                print("Record not found:", RECORD_ID)
                return

            client = None
            if record.client_id is not None:
                client = await session.get(Client, record.client_id)

            await plan_jobs_for_record_event(
                session=session,
                record=record,
                client=client,
                event_status=EVENT,
            )

        async with session.begin():
            stmt = (
                select(MessageJob.job_type, MessageJob.status,
                       MessageJob.run_at)
                .where(MessageJob.record_id == RECORD_ID)
                .order_by(MessageJob.run_at.asc())
            )
            res = await session.execute(stmt)
            rows = res.all()

    print("Jobs for record:", RECORD_ID)
    for job_type, status, run_at in rows:
        print(job_type, status, run_at)


if __name__ == "__main__":
    asyncio.run(main())
