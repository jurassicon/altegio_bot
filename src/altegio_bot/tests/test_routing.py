import asyncio

from altegio_bot.db import SessionLocal
from altegio_bot.whatsapp_routing import pick_sender_id


async def main() -> None:
    async with SessionLocal() as session:
        sender_id = await pick_sender_id(
            session=session,
            company_id=758285,
            sender_code='nails',
        )
        print(sender_id)


if __name__ == "__main__":
    asyncio.run(main())
