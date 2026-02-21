from __future__ import annotations

import asyncio
from typing import Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import MessageTemplate


COMPANY_KARLSRUHE = 758285
COMPANY_RASTATT = 1271200

FOOTER_BY_COMPANY: dict[int, str] = {
    COMPANY_KARLSRUHE: (
        '*KitiLash*\n'
        '76133 Karlsruhe, Kaiserstraße, 68\n'
        '☎ +491742310386\n\n'
        '📍https://goo.gl/maps/p7quWqbAqY9cusuRA\n'
        '📺 https://www.instagram.com/kitilash001\n'
        '_______________________\n'
        'Wenn die Links inaktiv sind, fügen Sie uns zur Kontaktliste hinzu.\n\n'
        'Newsletter abbestellen: {unsubscribe_link}\n'
    ),
    COMPANY_RASTATT: (
        '*KitiLash Rastatt*\n'
        '76437 Rastatt, Rathausstraße 5\n'
        '☎ +491742310386\n\n'
        '📍https://maps.app.goo.gl/xvYYbJbPaWcnp9Xv5\n'
        '📺 https://www.instagram.com/kitilash001\n'
        '_______________________\n'
        'Wenn die Links inaktiv sind, fügen Sie uns zur Kontaktliste hinzu.\n\n'
        'Newsletter abbestellen: {unsubscribe_link}\n'
    ),
}


def _details_block(include_total: bool = True) -> str:
    block = (
        '*Ausgewählte Mitarbeiterin:* {staff_name}\n'
        '*Datum:* {date}\n'
        '*Zeit:* {time}\n'
        '*Service:*\n'
        '{services}\n'
    )
    if include_total:
        block += '*Summe:* {total_cost}€\n'
    return block


def _tmpl_record_created(footer: str) -> str:
    return (
        '*{client_name}, hallo! Ihre Terminbuchung wurde bestätigt:*\n\n'
        f'{_details_block(include_total=True)}\n'
        f'{footer}'
    )


def _tmpl_record_updated(footer: str) -> str:
    return (
        '*{client_name}, hallo! Ihr Termin wurde geändert:*\n\n'
        f'{_details_block(include_total=True)}\n'
        f'{footer}'
    )


def _tmpl_record_canceled(footer: str) -> str:
    return (
        '*{client_name}, hallo! Ihr Termin wurde storniert:*\n\n'
        f'{_details_block(include_total=True)}\n'
        f'{footer}'
    )


def _tmpl_reminder_24h(footer: str) -> str:
    return (
        '*{client_name}, hallo!* Erinnerung an Ihren Termin morgen.\n\n'
        f'{_details_block(include_total=True)}\n'
        f'{footer}'
    )


def _tmpl_reminder_2h(footer: str) -> str:
    return (
        '*{client_name}, hallo!* Erinnerung: Ihr Termin in 2 Stunden.\n\n'
        f'{_details_block(include_total=True)}\n'
        f'{footer}'
    )


def _tmpl_comeback_3d(footer: str) -> str:
    return (
        '*{client_name}, hallo!*\n'
        'Wir vermissen Sie 😊\n'
        'Wenn Sie einen neuen Termin möchten, antworten Sie einfach auf diese '
        'Nachricht.\n\n'
        f'{footer}'
    )


def _templates_for_company(company_id: int) -> dict[str, str]:
    footer = FOOTER_BY_COMPANY[company_id]
    return {
        'record_created': _tmpl_record_created(footer),
        'record_updated': _tmpl_record_updated(footer),
        'record_canceled': _tmpl_record_canceled(footer),
        'reminder_24h': _tmpl_reminder_24h(footer),
        'reminder_2h': _tmpl_reminder_2h(footer),
        'comeback_3d': _tmpl_comeback_3d(footer),
    }


async def _upsert_one(
    session: AsyncSession,
    *,
    company_id: int,
    code: str,
    body: str,
    language: str = 'de',
    is_active: bool = True,
) -> None:
    stmt = (
        select(MessageTemplate)
        .where(MessageTemplate.company_id == company_id)
        .where(MessageTemplate.code == code)
        .where(MessageTemplate.language == language)
        .limit(1)
    )
    res = await session.execute(stmt)
    existing = res.scalars().first()

    if existing is None:
        session.add(
            MessageTemplate(
                company_id=company_id,
                code=code,
                language=language,
                body=body,
                is_active=is_active,
            )
        )
        return

    existing.body = body
    existing.is_active = is_active


async def _seed_company(session: AsyncSession, company_id: int) -> None:
    templates = _templates_for_company(company_id)
    for code, body in templates.items():
        await _upsert_one(
            session,
            company_id=company_id,
            code=code,
            body=body,
            language='de',
            is_active=True,
        )


async def main(company_ids: Iterable[int]) -> None:
    async with SessionLocal() as session:
        async with session.begin():
            for cid in company_ids:
                if cid not in FOOTER_BY_COMPANY:
                    continue
                await _seed_company(session, cid)

    print('seeded templates for:', list(company_ids))


if __name__ == '__main__':
    asyncio.run(main([COMPANY_KARLSRUHE, COMPANY_RASTATT]))