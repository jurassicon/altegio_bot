from __future__ import annotations

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import MessageTemplate


TEMPLATES: dict[int, dict[str, str]] = {
    758285: {
        'brand': 'KitiLash',
        'address': '76133 Karlsruhe, Kaiserstraße, 68',
        'phone': '+491742310386',
        'map_link': 'https://goo.gl/maps/p7quWqbAqY9cusuRA',
        'instagram': 'https://www.instagram.com/kitilash001',
        'unsubscribe_link': 'https://example.com/unsubscribe/karlsruhe',
        'booking_link': 'https://example.com/book/karlsruhe',
        'review_link': 'https://example.com/review/karlsruhe',
    },
    1271200: {
        'brand': 'KitiLash Rastatt',
        'address': '76437 Rastatt, Rathausstraße 5',
        'phone': '+491742310386',
        'map_link': 'https://maps.app.goo.gl/xvYYbJbPaWcnp9Xv5',
        'instagram': 'https://www.instagram.com/kitilash001',
        'unsubscribe_link': 'https://example.com/unsubscribe/rastatt',
        'booking_link': 'https://example.com/book/rastatt',
        'review_link': 'https://example.com/review/rastatt',
    },
}

CODES = (
    'record_created',
    'record_updated',
    'record_canceled',
    'reminder_24h',
    'reminder_2h',
    'comeback_3d',
    'repeat_10d',
    'review_3d',
)

LANG = 'de'


def _footer(cfg: dict[str, str]) -> str:
    brand = cfg['brand']
    address = cfg['address']
    phone = cfg['phone']
    map_link = cfg['map_link']
    instagram = cfg['instagram']
    unsubscribe_link = cfg['unsubscribe_link']

    return (
        f'\n\n*{brand}*\n'
        f'{address}\n'
        f'☎ {phone}\n\n'
        f'📍{map_link}\n'
        f'📺 {instagram}\n'
        '_______________________\n'
        'Wenn die Links inaktiv sind, fügen Sie uns zur Kontaktliste hinzu.\n\n'
        f'Newsletter abbestellen: {unsubscribe_link}'
    )


def _body_record_created(cfg: dict[str, str]) -> str:
    return (
        '*{client_name}, hallo! Ihre Terminbuchung wurde bestätigt:*\n\n'
        '*Ausgewählte Mitarbeiterin:* {staff_name}\n'
        '*Datum:* {date}\n'
        '*Zeit:* {time}\n'
        '*Service:*\n'
        '{services}\n'
        '*Summe:* {total_cost}€'
        '{pre_appointment_notes}'
        + _footer(cfg)
    )


def _body_record_updated(cfg: dict[str, str]) -> str:
    return (
        '*{client_name}, hallo! Ihr Termin wurde geändert:*\n\n'
        '*Ausgewählte Mitarbeiterin:* {staff_name}\n'
        '*Neues Datum:* {date}\n'
        '*Neue Zeit:* {time}\n'
        '*Service:*\n'
        '{services}\n'
        '*Summe:* {total_cost}€'
        + _footer(cfg)
    )


def _body_record_canceled(cfg: dict[str, str]) -> str:
    booking_link = cfg['booking_link']
    return (
        '*{client_name}, hallo!*\n\n'
        'Ihr Termin wurde storniert.\n'
        f'Neuen Termin buchen: {booking_link}'
        + _footer(cfg)
    )


def _body_reminder_24h(cfg: dict[str, str]) -> str:
    return (
        '*{client_name}, hallo!* Erinnerung an Ihren Termin morgen.\n\n'
        '*Mitarbeiterin:* {staff_name}\n'
        '*Datum:* {date}\n'
        '*Zeit:* {time}\n'
        '*Service:*\n'
        '{services}\n'
        '*Summe:* {total_cost}€'
        + _footer(cfg)
    )


def _body_reminder_2h(cfg: dict[str, str]) -> str:
    return (
        '*{client_name}, hallo!* Erinnerung: Ihr Termin findet in ca. 2 Stunden statt.\n\n'
        '*Mitarbeiterin:* {staff_name}\n'
        '*Datum:* {date}\n'
        '*Zeit:* {time}\n'
        '*Service:*\n'
        '{services}\n'
        '*Summe:* {total_cost}€'
        + _footer(cfg)
    )


def _body_comeback_3d(cfg: dict[str, str]) -> str:
    booking_link = cfg['booking_link']
    return (
        '*{client_name}, hallo!*\n\n'
        'Schade, dass es diesmal nicht geklappt hat.\n'
        f'Neuen Termin buchen: {booking_link}'
        + _footer(cfg)
    )


def _body_repeat_10d(cfg: dict[str, str]) -> str:
    booking_link = cfg['booking_link']
    return (
        '*{client_name}, hallo!*\n\n'
        'Möchten Sie Ihren letzten Service erneut buchen?\n'
        '*Letzter Service:* {primary_service}\n\n'
        f'Termin buchen: {booking_link}'
        + _footer(cfg)
    )


def _body_review_3d(cfg: dict[str, str]) -> str:
    review_link = cfg['review_link']
    return (
        '*{client_name}, hallo!*\n\n'
        'Vielen Dank, dass Sie bei uns waren! '
        'Wenn Sie 30 Sekunden Zeit haben, freuen wir uns sehr über eine Bewertung:\n'
        f'{review_link}'
        + _footer(cfg)
    )


def _make_body(code: str, cfg: dict[str, str]) -> str:
    mapping = {
        'record_created': _body_record_created,
        'record_updated': _body_record_updated,
        'record_canceled': _body_record_canceled,
        'reminder_24h': _body_reminder_24h,
        'reminder_2h': _body_reminder_2h,
        'comeback_3d': _body_comeback_3d,
        'repeat_10d': _body_repeat_10d,
        'review_3d': _body_review_3d,
    }
    return mapping[code](cfg)


async def seed_company(
    session: AsyncSession,
    *,
    company_id: int,
) -> None:
    cfg = TEMPLATES[company_id]

    await session.execute(
        delete(MessageTemplate).where(
            MessageTemplate.company_id == company_id,
            MessageTemplate.language == LANG,
            MessageTemplate.code.in_(CODES),
        )
    )

    for code in CODES:
        body = _make_body(code, cfg)
        session.add(
            MessageTemplate(
                company_id=company_id,
                code=code,
                language=LANG,
                body=body,
                is_active=True,
            )
        )


async def main() -> None:
    async with SessionLocal() as session:
        async with session.begin():
            await seed_company(session, company_id=758285)
            await seed_company(session, company_id=1271200)


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())