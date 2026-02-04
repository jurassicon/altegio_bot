from __future__ import annotations

import asyncio

from sqlalchemy import select

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import ServiceSenderRule, WhatsAppSender


SENDERS = [
    {
        "company_id": 758285,
        "sender_code": "default",
        "phone_number_id": "TEST_PHONE_NUMBER_ID_1",
        "display_phone": "+491742310386",
    },
    {
        "company_id": 758285,
        "sender_code": "nails",
        "phone_number_id": "TEST_PHONE_NUMBER_ID_2",
        "display_phone": "+491700000000",
    },
]

RULES = [
    {
        "company_id": 758285,
        "service_id": 12859821,
        "sender_code": "nails",
    },
]


async def upsert_sender(data: dict) -> None:
    async with SessionLocal() as session:
        async with session.begin():
            stmt = (
                select(WhatsAppSender)
                .where(WhatsAppSender.company_id == data["company_id"])
                .where(WhatsAppSender.sender_code == data["sender_code"])
            )
            res = await session.execute(stmt)
            obj = res.scalar_one_or_none()

            if obj is None:
                session.add(WhatsAppSender(**data))
                return

            obj.phone_number_id = data["phone_number_id"]
            obj.display_phone = data.get("display_phone")
            obj.is_active = True


async def upsert_rule(data: dict) -> None:
    async with SessionLocal() as session:
        async with session.begin():
            stmt = (
                select(ServiceSenderRule)
                .where(ServiceSenderRule.company_id == data["company_id"])
                .where(ServiceSenderRule.service_id == data["service_id"])
            )
            res = await session.execute(stmt)
            obj = res.scalar_one_or_none()

            if obj is None:
                session.add(ServiceSenderRule(**data))
                return

            obj.sender_code = data["sender_code"]


async def main() -> None:
    for s in SENDERS:
        await upsert_sender(s)

    for r in RULES:
        await upsert_rule(r)


if __name__ == "__main__":
    asyncio.run(main())
