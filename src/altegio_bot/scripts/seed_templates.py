from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import MessageTemplate


@dataclass(frozen=True)
class _TemplateDef:
    code: str
    body_base: str


_COMPANY_FOOTERS_DE: dict[int, str] = {
    # 758285 = Karlsruhe
    758285: (
        '*KitiLash*\n'
        '76133 Karlsruhe, Kaiserstraße, 68\n'
        '☎ +491742310386\n\n'
        '📍https://goo.gl/maps/p7quWqbAqY9cusuRA\n'
        '📺 https://www.instagram.com/kitilash001\n'
        '_______________________\n'
        'Wenn die Links inaktiv sind, fügen Sie uns zur Kontaktliste hinzu.\n\n'
        'Newsletter abbestellen: {unsubscribe_link}'
    ),
    # 1271200 = Rastatt
    1271200: (
        '*KitiLash Rastatt*\n'
        '76437 Rastatt, Rathausstraße 5\n'
        '☎ +491742310386\n\n'
        '📍https://maps.app.goo.gl/xvYYbJbPaWcnp9Xv5\n'
        '📺 https://www.instagram.com/kitilash001\n'
        '_______________________\n'
        'Wenn die Links inaktiv sind, fügen Sie uns zur Kontaktliste hinzu.\n\n'
        'Newsletter abbestellen: {unsubscribe_link}'
    ),
}

_TEMPLATES_DE: tuple[_TemplateDef, ...] = (
    _TemplateDef(
        code='record_created',
        body_base=(
            '*{client_name}, hallo! Ihre Terminbuchung wurde bestätigt:*\n\n'
            '*Ausgewählte Mitarbeiterin:* {staff_name}\n'
            '*Datum:* {date}\n'
            '*Zeit:* {time}\n'
            '*Service:*\n'
            '{services}\n'
            '*Summe:* {total_cost}€\n\n'
            '{pre_appointment_notes}'
        ),
    ),
    _TemplateDef(
        code='record_updated',
        body_base=(
            '*{client_name}, hallo! Ihr Termin wurde geändert:*\n\n'
            '*Ausgewählte Mitarbeiterin:* {staff_name}\n'
            '*Neues Datum:* {date}\n'
            '*Neue Zeit:* {time}\n'
            '*Service:*\n'
            '{services}\n'
            '*Summe:* {total_cost}€'
        ),
    ),
    _TemplateDef(
        code='record_canceled',
        body_base=(
            '*{client_name}, hallo! Ihr Termin wurde storniert.*\n\n'
            'Wenn Sie einen neuen Termin wünschen, antworten Sie einfach '
            'auf diese Nachricht.'
        ),
    ),
    _TemplateDef(
        code='reminder_24h',
        body_base=(
            '*{client_name}, hallo!* Erinnerung an Ihren Termin morgen.\n\n'
            '*Ausgewählte Mitarbeiterin:* {staff_name}\n'
            '*Datum:* {date}\n'
            '*Zeit:* {time}\n'
            '*Service:*\n'
            '{services}'
        ),
    ),
    _TemplateDef(
        code='reminder_2h',
        body_base=(
            '*{client_name}, hallo!* Erinnerung: Ihr Termin in 2 Stunden.\n\n'
            '*Ausgewählte Mitarbeiterin:* {staff_name}\n'
            '*Datum:* {date}\n'
            '*Zeit:* {time}\n'
            '*Service:*\n'
            '{services}'
        ),
    ),
    _TemplateDef(
        code='comeback_3d',
        body_base=(
            '*{client_name}, hallo!* Wir vermissen Sie 😊\n\n'
            'Möchten Sie einen neuen Termin buchen? Antworten Sie einfach '
            'auf diese Nachricht.'
        ),
    ),
)


async def _upsert_one(
    session: AsyncSession,
    *,
    company_id: int,
    code: str,
    body: str,
) -> None:
    stmt = (
        select(MessageTemplate)
        .where(MessageTemplate.company_id == company_id)
        .where(MessageTemplate.code == code)
        .where(MessageTemplate.language == 'de')
    )
    res = await session.execute(stmt)
    tpl = res.scalar_one_or_none()

    if tpl is None:
        session.add(
            MessageTemplate(
                company_id=company_id,
                code=code,
                language='de',
                body=body,
                is_active=True,
            )
        )
        return

    tpl.body = body
    tpl.is_active = True


async def main() -> None:
    async with SessionLocal() as session:
        async with session.begin():
            for company_id, footer in _COMPANY_FOOTERS_DE.items():
                for t in _TEMPLATES_DE:
                    body = f'{t.body_base}\n\n{footer}'
                    await _upsert_one(
                        session,
                        company_id=company_id,
                        code=t.code,
                        body=body,
                    )

    print('seeded templates for:', ', '.join(map(str, _COMPANY_FOOTERS_DE.keys())))


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())