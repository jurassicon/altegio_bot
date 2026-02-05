from __future__ import annotations

import asyncio

from sqlalchemy import select

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import MessageTemplate


TEMPLATES: dict[int, dict[str, str]] = {
    758285: {
        'record_created': (
            '*{client_name}, hallo! Ihre Terminbuchung wurde bestätigt:*\n\n'
            '*Ausgewählte Mitarbeiterin:* {staff_name}\n'
            '*Datum:* {date}\n'
            '*Zeit:* {time}\n'
            '*Service:*\n{services}\n'
            '*Summe:* {total_cost}€\n\n'
            '*KitiLash*\n'
            '76133 Karlsruhe, Kaiserstraße, 68\n'
            '☎ +491742310386\n\n'
            '📍https://goo.gl/maps/p7quWqbAqY9cusuRA\n'
            '📺 https://www.instagram.com/kitilash001\n'
            '_______________________\n'
            'Wenn die Links inaktiv sind, fügen Sie uns zur Kontaktliste hinzu.\n\n'
            'Newsletter abbestellen: {unsubscribe_link}\n'
        ),
        'reminder_24h': (
            '*{client_name}, hallo!* Erinnerung an Ihren Termin morgen.\n\n'
            '*Mitarbeiterin:* {staff_name}\n'
            '*Datum:* {date}\n'
            '*Zeit:* {time}\n'
            '*Service:*\n{services}\n'
            '*Summe:* {total_cost}€\n\n'
            '{short_link}\n'
        ),
        'reminder_2h': (
            '*{client_name}, hallo!* Erinnerung: Ihr Termin in 2 Stunden.\n\n'
            '*Zeit:* {time}\n'
            '*Service:*\n{services}\n\n'
            '{short_link}\n'
        ),
    }
}


async def upsert_one(
    company_id: int,
    code: str,
    body: str,
) -> None:
    async with SessionLocal() as session:
        async with session.begin():
            stmt = (
                select(MessageTemplate)
                .where(MessageTemplate.company_id == company_id)
                .where(MessageTemplate.code == code)
            )
            res = await session.execute(stmt)
            obj = res.scalar_one_or_none()

            if obj is None:
                obj = MessageTemplate(
                    company_id=company_id,
                    code=code,
                    body=body,
                    is_active=True,
                )
                session.add(obj)
                return

            obj.body = body
            obj.is_active = True


async def main() -> None:
    for company_id, items in TEMPLATES.items():
        for code, body in items.items():
            await upsert_one(company_id, code, body)


if __name__ == '__main__':
    asyncio.run(main())
