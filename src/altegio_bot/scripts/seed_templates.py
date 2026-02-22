from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import delete

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import MessageTemplate


@dataclass(frozen=True)
class _CompanyCfg:
    company_id: int
    brand_line: str
    address_line: str
    phone_line: str
    maps_line: str
    instagram_line: str
    booking_link: str


COMPANIES: dict[int, _CompanyCfg] = {
    758285: _CompanyCfg(
        company_id=758285,
        brand_line='*KitiLash*',
        address_line='76133 Karlsruhe, Kaiserstraße, 68',
        phone_line='☎ +491742310386',
        maps_line='📍https://goo.gl/maps/p7quWqbAqY9cusuRA',
        instagram_line='📺 https://www.instagram.com/kitilash001',
        booking_link='https://n813709.alteg.io/',
    ),
    1271200: _CompanyCfg(
        company_id=1271200,
        brand_line='*KitiLash Rastatt*',
        address_line='76437 Rastatt, Rathausstraße 5',
        phone_line='☎ +491742310386',
        maps_line='📍https://maps.app.goo.gl/xvYYbJbPaWcnp9Xv5',
        instagram_line='📺 https://www.instagram.com/kitilash001',
        booking_link='https://n813709.alteg.io/',
    ),
}


def _footer(cfg: _CompanyCfg) -> str:
    return (
        f'\n\n{cfg.brand_line}\n'
        f'{cfg.address_line}\n'
        f'{cfg.phone_line}\n\n'
        f'{cfg.maps_line}\n'
        f'{cfg.instagram_line}\n'
        '_______________________\n'
        'Wenn die Links inaktiv sind, fügen Sie uns zur Kontaktliste hinzu.\n\n'
        'Newsletter abbestellen: {unsubscribe_link}'
    )


def _body_record_created(cfg: _CompanyCfg) -> str:
    return (
        '*{client_name}, hallo! Ihre Terminbuchung wurde bestätigt:*\n\n'
        '*Ausgewählte Mitarbeiterin:* {staff_name}\n'
        '*Datum:* {date}\n'
        '*Zeit:* {time}\n'
        '*Service:*\n'
        '{services}\n'
        '*Summe:* {total_cost}€\n'
        '{pre_appointment_notes}'
        f'{_footer(cfg)}'
    )


def _body_record_updated(cfg: _CompanyCfg) -> str:
    return (
        '*{client_name}, hallo! Ihr Termin wurde geändert:*\n\n'
        '*Ausgewählte Mitarbeiterin:* {staff_name}\n'
        '*Neues Datum:* {date}\n'
        '*Neue Zeit:* {time}\n'
        '*Service:*\n'
        '{services}\n'
        '*Summe:* {total_cost}€'
        f'{_footer(cfg)}'
    )


def _body_record_canceled(cfg: _CompanyCfg) -> str:
    return (
        '*{client_name}, hallo!*\n\n'
        'Ihr Termin wurde storniert.\n\n'
        'Wenn Sie einen neuen Termin vereinbaren möchten, buchen Sie hier:\n'
        '{booking_link}'
        f'{_footer(cfg)}'
    )


def _body_reminder_24h(cfg: _CompanyCfg) -> str:
    return (
        '*{client_name}, hallo!* Erinnerung an Ihren Termin morgen.\n\n'
        '*Mitarbeiterin:* {staff_name}\n'
        '*Datum:* {date}\n'
        '*Zeit:* {time}\n'
        '*Service:*\n'
        '{services}\n'
        '*Summe:* {total_cost}€'
        f'{_footer(cfg)}'
    )


def _body_reminder_2h(cfg: _CompanyCfg) -> str:
    return (
        '*{client_name}, hallo!*\n\n'
        'Erinnerung: Ihr Termin beginnt in ca. 2 Stunden.\n\n'
        '*Mitarbeiterin:* {staff_name}\n'
        '*Datum:* {date}\n'
        '*Zeit:* {time}\n'
        '*Service:*\n'
        '{services}\n'
        '*Summe:* {total_cost}€'
        f'{_footer(cfg)}'
    )


def _body_review_3d(cfg: _CompanyCfg) -> str:
    return (
        '*{client_name}, hallo!*\n\n'
        'Vielen Dank für Ihren Besuch bei KitiLash!\n'
        'Wenn Sie zufrieden waren, würden wir uns sehr über eine kurze '
        'Bewertung freuen.\n\n'
        'Link: {short_link}'
        f'{_footer(cfg)}'
    )


def _body_comeback_3d(cfg: _CompanyCfg) -> str:
    return (
        '*{client_name}, hallo!*\n\n'
        'Schade, dass es diesmal nicht geklappt hat.\n'
        'Wenn Sie einen neuen Termin möchten, buchen Sie hier:\n'
        '{booking_link}'
        f'{_footer(cfg)}'
    )


def _body_repeat_10d(cfg: _CompanyCfg) -> str:
    return (
        '*Hallo, {client_name}* 🙂\n\n'
        'Ich hoffe, dir geht es gut.\n\n'
        'Ich bin Julia vom Beautystudio KitiLash.\n'
        'Ich habe gesehen, dass du vor 10 Tagen bei uns für die '
        'Wimpernverlängerung *"{primary_service}"* da warst.\n\n'
        'Ich würde mich freuen, wenn du wiederkommst. Du kannst natürlich '
        'dieselbe Meisterin und die gleiche Behandlung wählen, oder du '
        'probierst etwas Neues aus. Wir haben eine tolle Auswahl an Looks.\n'
        'Denke an den Auffüllpreis bis 3 Wochen und buche deinen Termin '
        'rechtzeitig.\n\n'
        '*Wir warten auf dich im KitiLash: {booking_link}*\n\n'
        'Ich freue mich auf deine Antwort!\n\n'
        'Liebe Grüße, Julia'
        f'{_footer(cfg)}'
    )


def _templates_for_company(cfg: _CompanyCfg) -> list[MessageTemplate]:
    bodies = {
        'record_created': _body_record_created(cfg),
        'record_updated': _body_record_updated(cfg),
        'record_canceled': _body_record_canceled(cfg),
        'reminder_24h': _body_reminder_24h(cfg),
        'reminder_2h': _body_reminder_2h(cfg),
        'review_3d': _body_review_3d(cfg),
        'comeback_3d': _body_comeback_3d(cfg),
        'repeat_10d': _body_repeat_10d(cfg),
    }

    out: list[MessageTemplate] = []
    for code, body in bodies.items():
        out.append(
            MessageTemplate(
                company_id=cfg.company_id,
                code=code,
                language='de',
                body=body,
                is_active=True,
            )
        )
    return out


async def main() -> None:
    async with SessionLocal() as session:
        async with session.begin():
            await session.execute(
                delete(MessageTemplate).where(
                    MessageTemplate.company_id.in_(list(COMPANIES.keys()))
                )
            )

            for cfg in COMPANIES.values():
                for tmpl in _templates_for_company(cfg):
                    session.add(tmpl)

    print('seeded message_templates for:', ', '.join(map(str, COMPANIES)))


if __name__ == '__main__':
    import asyncio

    asyncio.run(main())