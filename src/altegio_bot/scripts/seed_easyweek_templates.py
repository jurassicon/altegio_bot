"""Seed the EasyWeek (Durlach) message templates and WhatsApp sender.

Separate from ``seed_templates.py`` on purpose. That script is Altegio-only and
starts by DELETEing every row for its own company ids; copying that pattern here
would be dangerous, because ``message_templates`` has no unique index and both
CRMs share one integer space for ``company_id``. Nothing here deletes anything.

Templates and sender live in ONE script because they are one activation unit:
PR-5 fails an EasyWeek job closed when either half is missing (no template ->
"Template not found", no sender -> "No active sender"), so seeding one without
the other only produces failed jobs. One operator step, one transaction.

Idempotent by construction — see :func:`_upsert_template` and
:func:`_upsert_sender`. Running it twice is a no-op.

Phase 1 seeds FOUR rows: the three lifecycle codes EasyWeek plans
(``easyweek_policy.EASYWEEK_LIFECYCLE_JOB_TYPES``) plus the first-time-customer
variant of ``record_created``. That fourth row is a TEMPLATE CODE, not a job
type — the job stays ``record_created`` and so does its seven-field Meta param
contract; only the row, and therefore ``meta_template_name``, differs. See
``_render_message``.

Reminders are phase 2.

This script does NOT enable notifications. ``EASYWEEK_NOTIFICATIONS_ENABLED``
stays an operator decision — see docs/easyweek/durlach_activation_runbook.md.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_policy import (
    RECORD_CANCELED,
    RECORD_CREATED,
    RECORD_CREATED_NEW_CLIENT,
    RECORD_UPDATED,
)
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageTemplate, WhatsAppSender
from altegio_bot.settings import settings
from altegio_bot.workers.outbox_worker import PRE_APPOINTMENT_NOTES_DE

# Approved Meta template names for the Durlach location, in the shared WABA.
# DB-first resolution (PR-5) reads these from `message_templates`; they are
# deliberately NOT added to META_TEMPLATE_MAP, which is keyed by Altegio company
# id and must not learn about another CRM.
META_TEMPLATE_NAMES: dict[str, str] = {
    RECORD_CREATED: "kitilash_du_record_created_v1",
    RECORD_UPDATED: "kitilash_du_record_updated_v1",
    RECORD_CANCELED: "kitilash_du_record_canceled_v1",
    RECORD_CREATED_NEW_CLIENT: "kitilash_du_record_created_new_client_v1",
}

# The confirmed numeric EasyWeek :location_id of the Durlach location.
#
# NOT YET CONFIRMED — read it from the production `easyweek.env`
# (EASYWEEK_LOCATION_ID) and fill it in before the first seed.
#
# `None` means unconfirmed, and the seed then refuses to run. Everything below
# is Durlach-specific content — its Meta template names, its address, its map
# pin. Binding that to whatever location id happens to be configured would
# silently give another location Durlach's messages, and nothing downstream
# would notice: the worker matches rows by company_id and would find them.
DURLACH_LOCATION_ID: int | None = None

# The templates are written in German and point at German Meta templates, so
# the configured language has to be German. `EASYWEEK_DEFAULT_LANGUAGE=en` would
# otherwise create rows with `language='en'` referring to `kitilash_du_*_v1`,
# and the mismatch would only surface as a Meta rejection at send time.
DURLACH_LANGUAGE = "de"

# Studio contact details. Public information, printed on the storefront.
BRAND_LINE = "*KitiLash Durlach*"
ADDRESS_LINE = "Pfinztalstraße 4, 76227 Karlsruhe-Durlach"
CONTACT_PHONE = "+491742310386"
MAPS_LINE = "📍https://maps.app.goo.gl/HnVPnHaJHf2DW3Nn8"
INSTAGRAM_LINE = "📺 https://www.instagram.com/kitilash001"

# One source for the sender's display phone and the footer line, so the number a
# customer reads can never drift from the number the row claims.
FOOTER = f"\n\n{BRAND_LINE}\n{ADDRESS_LINE}\n☎ {CONTACT_PHONE}\n\n{MAPS_LINE}\n{INSTAGRAM_LINE}"

# Bodies for the TEXT send inside the 24-hour customer-service window (and for
# `fallback_text`). Placeholders are filled by `final_body.format(**msg_ctx)`, so
# every name here must exist in the ctx `_render_message` builds — an unknown
# name would raise and be swallowed, leaving the raw `{placeholder}` in a live
# message. The set used below is a subset of that ctx, and matches the positional
# order of `meta_templates.LIFECYCLE_PARAM_FIELDS` for the same code.
#
# `{booking_link}` is the EFFECTIVE link the worker resolved, never a baked-in
# URL: a re-verified per-booking manage link for created/updated, and the static
# booking page for canceled (§1.6.4).
BODIES: dict[str, str] = {
    RECORD_CREATED: (
        "*{client_name}, hallo! Ihre Terminbuchung wurde bestätigt:*\n\n"
        "*Mitarbeiterin:* {staff_name}\n"
        "*Datum:* {date}\n"
        "*Zeit:* {time}\n"
        "*Service:*\n"
        "{services}\n"
        "*Summe:* {total_cost}€\n\n"
        "Termin verwalten: {booking_link}"
        f"{FOOTER}"
    ),
    RECORD_UPDATED: (
        "*{client_name}, hallo! Ihr Termin wurde geändert:*\n\n"
        "*Mitarbeiterin:* {staff_name}\n"
        "*Neues Datum:* {date}\n"
        "*Neue Zeit:* {time}\n"
        "*Service:*\n"
        "{services}\n"
        "*Summe:* {total_cost}€\n\n"
        "Termin verwalten: {booking_link}"
        f"{FOOTER}"
    ),
    RECORD_CANCELED: (
        "*{client_name}, hallo!*\n\n"
        "Ihr Termin am {date} um {time} Uhr wurde storniert.\n"
        "{services}\n\n"
        "Neuen Termin buchen: {booking_link}"
        f"{FOOTER}"
    ),
}

# The first-time-customer variant: the ordinary confirmation plus the
# "Wichtige Hinweise" block.
#
# Composed from PRE_APPOINTMENT_NOTES_DE rather than retyped, because the
# approved Meta template was cloned from `kitilash_ka_record_created_new_client_v1`
# and carries that exact block. If the text here drifted from it, a customer
# inside the 24h window would read one thing and a customer outside it another —
# and nothing would flag the divergence.
BODIES[RECORD_CREATED_NEW_CLIENT] = (
    "*{client_name}, hallo! Ihre Terminbuchung wurde bestätigt:*\n\n"
    "*Mitarbeiterin:* {staff_name}\n"
    "*Datum:* {date}\n"
    "*Zeit:* {time}\n"
    "*Service:*\n"
    "{services}\n"
    "*Summe:* {total_cost}€\n\n"
    "Termin verwalten: {booking_link}"
    f"{PRE_APPOINTMENT_NOTES_DE}"
    f"{FOOTER}"
)


class SeedConfigError(RuntimeError):
    """The environment is not configured well enough to seed safely."""


@dataclass(frozen=True)
class SeedResult:
    templates_created: int = 0
    templates_updated: int = 0
    template_duplicates: int = 0
    sender_created: bool = False
    sender_updated: bool = False


def _resolve_company_id() -> int:
    """The Durlach location id, or refuse.

    A positive number is not enough. This script writes Durlach's Meta template
    names, address and map pin; pointing them at a different location is not a
    configuration error the system can detect later, it is Durlach's messages
    being sent on another location's behalf.
    """
    company_id = int(getattr(settings, "easyweek_location_id", 0) or 0)
    if company_id <= 0:
        raise SeedConfigError(
            "EASYWEEK_LOCATION_ID is not configured; refusing to seed rows that no job could ever match."
        )
    if DURLACH_LOCATION_ID is None:
        raise SeedConfigError(
            "DURLACH_LOCATION_ID is not confirmed in this script. Fill it from the production "
            "easyweek.env before seeding, so Durlach content cannot be bound to another location."
        )
    if company_id != DURLACH_LOCATION_ID:
        raise SeedConfigError(
            "EASYWEEK_LOCATION_ID does not match the confirmed Durlach location id; "
            "this seed writes Durlach-specific content and refuses to bind it elsewhere."
        )
    return company_id


def _resolve_language() -> str:
    language = (getattr(settings, "easyweek_default_language", "") or "").strip()
    if not language:
        raise SeedConfigError("EASYWEEK_DEFAULT_LANGUAGE is empty; the template language must be explicit.")
    if language.lower() != DURLACH_LANGUAGE:
        raise SeedConfigError(
            f"EASYWEEK_DEFAULT_LANGUAGE must be {DURLACH_LANGUAGE!r} for Durlach; the bodies are German "
            "and the Meta templates they name are registered in German."
        )
    return DURLACH_LANGUAGE


def _resolve_phone_number_id() -> str:
    phone_number_id = (getattr(settings, "meta_wa_phone_number_id", "") or "").strip()
    if not phone_number_id:
        raise SeedConfigError(
            "META_WA_PHONE_NUMBER_ID is not configured; the sender row would point at no WhatsApp number."
        )
    return phone_number_id


async def _upsert_template(
    session: AsyncSession,
    *,
    company_id: int,
    language: str,
    code: str,
    result: SeedResult,
) -> SeedResult:
    """Insert or update one template, keyed by (provider, company_id, code, language).

    ``message_templates`` has only a NON-unique index on that tuple, so this is a
    read-then-write rather than ``ON CONFLICT``. Any extra rows already sharing
    the key are left untouched and merely counted: deleting rows this script did
    not create is exactly the ``seed_templates.py`` pattern that must not be
    repeated here. ``_load_template`` orders by id, so updating the lowest-id row
    updates the one the worker will actually pick.
    """
    stmt = (
        select(MessageTemplate)
        .where(MessageTemplate.provider == PROVIDER_EASYWEEK)
        .where(MessageTemplate.company_id == company_id)
        .where(MessageTemplate.code == code)
        .where(MessageTemplate.language == language)
        .order_by(MessageTemplate.id.asc())
    )
    existing = list((await session.execute(stmt)).scalars().all())

    if not existing:
        session.add(
            MessageTemplate(
                provider=PROVIDER_EASYWEEK,
                company_id=company_id,
                code=code,
                language=language,
                body=BODIES[code],
                meta_template_name=META_TEMPLATE_NAMES[code],
                is_active=True,
            )
        )
        return SeedResult(
            templates_created=result.templates_created + 1,
            templates_updated=result.templates_updated,
            template_duplicates=result.template_duplicates,
            sender_created=result.sender_created,
            sender_updated=result.sender_updated,
        )

    row = existing[0]
    row.body = BODIES[code]
    row.meta_template_name = META_TEMPLATE_NAMES[code]
    row.is_active = True
    return SeedResult(
        templates_created=result.templates_created,
        templates_updated=result.templates_updated + 1,
        template_duplicates=result.template_duplicates + (len(existing) - 1),
        sender_created=result.sender_created,
        sender_updated=result.sender_updated,
    )


async def _upsert_sender(
    session: AsyncSession,
    *,
    company_id: int,
    phone_number_id: str,
    result: SeedResult,
) -> SeedResult:
    """Insert or update the Durlach sender.

    ``whatsapp_senders`` DOES carry a unique constraint on
    (provider, company_id, sender_code), so this is an atomic ``ON CONFLICT``.

    One ``phone_number_id`` serving several ``company_id`` rows is normal and
    supported: ``pick_sender_id`` keys on (provider, company_id, sender_code) and
    never on the number, so each branch owns a row and they may all point at the
    shared bot number. Inbound is unaffected too — the webhook allowlist is keyed
    on ``phone_number_id`` alone, and the 24h window is a property of the
    (customer, business number) pair in Meta's model rather than of a branch.
    """
    existed = (
        await session.execute(
            select(WhatsAppSender.id)
            .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
            .where(WhatsAppSender.company_id == company_id)
            .where(WhatsAppSender.sender_code == "default")
        )
    ).scalar_one_or_none() is not None

    stmt = pg_insert(WhatsAppSender).values(
        provider=PROVIDER_EASYWEEK,
        company_id=company_id,
        sender_code="default",
        phone_number_id=phone_number_id,
        display_phone=CONTACT_PHONE,
        is_active=True,
    )
    stmt = stmt.on_conflict_do_update(
        constraint="uq_whatsapp_senders_provider_company_code",
        set_={
            "phone_number_id": phone_number_id,
            "display_phone": CONTACT_PHONE,
            "is_active": True,
        },
    )
    await session.execute(stmt)
    return SeedResult(
        templates_created=result.templates_created,
        templates_updated=result.templates_updated,
        template_duplicates=result.template_duplicates,
        sender_created=result.sender_created or not existed,
        sender_updated=result.sender_updated or existed,
    )


async def seed(session: AsyncSession) -> SeedResult:
    """Seed templates and sender into *session*. Caller owns the transaction."""
    company_id = _resolve_company_id()
    language = _resolve_language()
    phone_number_id = _resolve_phone_number_id()

    result = SeedResult()
    for code in (RECORD_CREATED, RECORD_CREATED_NEW_CLIENT, RECORD_UPDATED, RECORD_CANCELED):
        result = await _upsert_template(
            session,
            company_id=company_id,
            language=language,
            code=code,
            result=result,
        )
    result = await _upsert_sender(
        session,
        company_id=company_id,
        phone_number_id=phone_number_id,
        result=result,
    )
    return result


async def main() -> None:
    async with SessionLocal() as session:
        async with session.begin():
            result = await seed(session)

    # Ids and counts only — no bodies, no phone numbers.
    print(
        "seeded easyweek templates: created={} updated={} sender_created={} sender_updated={}".format(
            result.templates_created,
            result.templates_updated,
            result.sender_created,
            result.sender_updated,
        )
    )
    if result.template_duplicates:
        print(
            f"WARNING: {result.template_duplicates} extra template row(s) share a seeded key. "
            "Nothing was deleted; review them manually."
        )


if __name__ == "__main__":
    asyncio.run(main())
