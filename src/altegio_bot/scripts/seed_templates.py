from __future__ import annotations

from sqlalchemy import delete, select

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import MessageTemplate


COMPANY_KARLSRUHE = 758285
COMPANY_RASTATT = 1271200

LANG = 'de'

CODES = (
    'record_created',
    'record_updated',
    'record_canceled',
    'reminder_24h',
    'reminder_2h',
    'comeback_3d',
)

FOOTER_KARLSRUHE = (
    '*KitiLash*\n'
    '76133 Karlsruhe, Kaiserstraße, 68\n'
    '☎ +491742310386\n\n'
    '📍https://goo.gl/maps/p7quWqbAqY9cusuRA\n'
    '📺 https://www.instagram.com/kitilash001\n'
)

FOOTER_RASTATT = (
    '*KitiLash Rastatt*\n'
    '76437 Rastatt, Rathausstraße 5\n'
    '☎ +491742310386\n\n'
    '📍https://maps.app.goo.gl/xvYYbJbPaWcnp9Xv5\n'
    '📺 https://www.instagram.com/kitilash001\n'
)

FOOTER_COMMON = (
    '_______________________\n'
    'Wenn die Links inaktiv sind, fügen Sie uns zur Kontaktliste hinzu.\n\n'
    'Newsletter abbestellen: {unsubscribe_link}'
)


def _tpl_record_created(footer: str) -> str:
    return (
        '*{client_name}, hallo! Ihre Terminbuchung wurde bestätigt:*\n\n'
        '*Ausgewählte Mitarbeiterin:* {staff_name}\n'
        '*Datum:* {date}\n'
        '*Zeit:* {time}\n'
        '*Service:*\n'
        '{services}\n'
        '*Summe:* {total_cost}€\n\n'
        '{pre_appointment_notes}\n\n'
        f'{footer}\n'
        f'{FOOTER_COMMON}'
    )


def _tpl_record_updated(footer: str) -> str:
    return (
        '*{client_name}, hallo! Ihr Termin wurde geändert:*\n\n'
        '*Ausgewählte Mitarbeiterin:* {staff_name}\n'
        '*Neues Datum:* {date}\n'
        '*Neue Zeit:* {time}\n'
        '*Service:*\n'
        '{services}\n'
        '*Summe:* {total_cost}€\n\n'
        f'{footer}\n'
        f'{FOOTER_COMMON}'
    )


def _tpl_record_canceled(footer: str) -> str:
    return (
        '*{client_name}, hallo! Ihr Termin wurde storniert.*\n\n'
        'Wenn Sie einen neuen Termin möchten, antworten Sie einfach auf diese '
        'Nachricht.\n\n'
        f'{footer}\n'
        f'{FOOTER_COMMON}'
    )


def _tpl_reminder_24h(footer: str) -> str:
    return (
        '*{client_name}, hallo!* Erinnerung an Ihren Termin morgen.\n\n'
        '*Mitarbeiterin:* {staff_name}\n'
        '*Datum:* {date}\n'
        '*Zeit:* {time}\n'
        '*Service:*\n'
        '{services}\n\n'
        f'{footer}\n'
        f'{FOOTER_COMMON}'
    )


def _tpl_reminder_2h(footer: str) -> str:
    return (
        '*{client_name}, hallo!* Erinnerung: Ihr Termin beginnt in 2 Stunden.\n\n'
        '*Mitarbeiterin:* {staff_name}\n'
        '*Datum:* {date}\n'
        '*Zeit:* {time}\n'
        '*Service:*\n'
        '{services}\n\n'
        '{pre_appointment_notes}\n\n'
        f'{footer}\n'
        f'{FOOTER_COMMON}'
    )


def _tpl_comeback_3d(footer: str) -> str:
    return (
        '*{client_name}, hallo!* Wir haben Sie vermisst 😊\n\n'
        'Wenn Sie einen neuen Termin buchen möchten, antworten Sie einfach auf '
        'diese Nachricht.\n\n'
        f'{footer}\n'
        f'{FOOTER_COMMON}'
    )


def _templates_for_company(company_id: int, footer: str) -> list[MessageTemplate]:
    bodies = {
        'record_created': _tpl_record_created(footer),
        'record_updated': _tpl_record_updated(footer),
        'record_canceled': _tpl_record_canceled(footer),
        'reminder_24h': _tpl_reminder_24h(footer),
        'reminder_2h': _tpl_reminder_2h(footer),
        'comeback_3d': _tpl_comeback_3d(footer),
    }

    items: list[MessageTemplate] = []
    for code in CODES:
        items.append(
            MessageTemplate(
                company_id=company_id,
                code=code,
                language=LANG,
                body=bodies[code],
                is_active=True,
            )
        )
    return items


async def main() -> None:
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(
                delete(MessageTemplate)
                .where(MessageTemplate.company_id.in_([COMPANY_KARLSRUHE, COMPANY_RASTATT]))
                .where(MessageTemplate.language == LANG)
                .where(MessageTemplate.code.in_(list(CODES)))
            )

            for tpl in _templates_for_company(COMPANY_KARLSRUHE, FOOTER_KARLSRUHE):
                session.add(tpl)

            for tpl in _templates_for_company(COMPANY_RASTATT, FOOTER_RASTATT):
                session.add(tpl)

    async with SessionLocal() as session:
        res = await session.execute(
            select(MessageTemplate.company_id, MessageTemplate.code, MessageTemplate.language)
            .where(MessageTemplate.company_id.in_([COMPANY_KARLSRUHE, COMPANY_RASTATT]))
            .order_by(MessageTemplate.company_id, MessageTemplate.code, MessageTemplate.language)
        )
        rows = res.all()

    print('seeded templates:', len(rows))
    for row in rows:
        print(row)


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())