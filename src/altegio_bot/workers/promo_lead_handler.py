"""WhatsApp promo lead handler for the secret-word discount funnel."""

from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING
from urllib.parse import quote

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.altegio_records import AltegioNewClientCheckError, check_client_has_any_altegio_record
from altegio_bot.models.models import Client, MessageJob, OutboxMessage, PromoLead, Record
from altegio_bot.promo_loyalty import AltegioLoyaltyError, issue_promo_loyalty_card
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.providers.dummy import safe_send
from altegio_bot.settings import settings
from altegio_bot.whatsapp_routing import pick_sender_id

if TYPE_CHECKING:
    from altegio_bot.models.models import WhatsAppEvent

logger = logging.getLogger("promo_lead_handler")
PROMO_ELIGIBILITY_CHECK_JOB_TYPE = "promo_eligibility_check"


# ---------------------------------------------------------------------------
# Safe informational reply (promo_lead_funnel_enabled = False)
# ---------------------------------------------------------------------------

_PROMO_INFO_TEXT = (
    "Danke für Ihr Interesse! 🎁\n\n"
    "Diese Aktion richtet sich an Neukunden beim ersten Besuch.\n\n"
    "Bitte buchen Sie Ihren Termin online – wir freuen uns auf Sie.\n\n"
    "Termin buchen:\n{booking_url}"
)
_PROMO_REFERRAL_BRAND_NAME = "KitiLash"


# ---------------------------------------------------------------------------
# Time helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def compute_expires_at(
    issued_at: datetime,
    mode: str,
    validity_days: int,
) -> datetime:
    """Return the expiry datetime for a newly issued promo lead.

    mode='issued_plus_days':
        expires_at = issued_at + validity_days days (UTC).

    mode='calendar_month':
        expires_at = midnight UTC on the first day of the next calendar month.
        e.g. issued 2026-05-07 → expires 2026-06-01 00:00:00 UTC.
        Customer display shows the last valid day (31.05.2026), not the
        exclusive boundary.
    """
    if mode == "calendar_month":
        year = issued_at.year
        month = issued_at.month
        if month == 12:
            return issued_at.replace(
                year=year + 1,
                month=1,
                day=1,
                hour=0,
                minute=0,
                second=0,
                microsecond=0,
            )
        return issued_at.replace(
            month=month + 1,
            day=1,
            hour=0,
            minute=0,
            second=0,
            microsecond=0,
        )

    # Default: issued_plus_days
    return issued_at + timedelta(days=validity_days)


# ---------------------------------------------------------------------------
# Reply builders (German customer-facing text)
# ---------------------------------------------------------------------------


def _format_discount(amount: Decimal, discount_type: str) -> str:
    """Format discount for display: '15 €' or '10 %'."""
    amt = int(amount) if amount == int(amount) else float(amount)
    if discount_type == "percent":
        return f"{amt} %"
    return f"{amt} €"


def _format_secret_code(secret_code: str) -> str:
    code = secret_code.strip()[:64]
    if not code:
        return "Aktionswort"
    if code.islower():
        return code[:1].upper() + code[1:]
    return code


def _normalize_promo_text(raw: str | None) -> str:
    if not raw:
        return ""

    text = raw.strip().lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip(" \t\n\r.,!?:;\"'()[]{}")


def _configured_promo_keywords() -> frozenset[str]:
    return frozenset(keyword.strip().lower() for keyword in settings.promo_secret_words.split(",") if keyword.strip())


def _extract_promo_keyword(text: str) -> str:
    normalized = _normalize_promo_text(text)
    if not normalized:
        return ""

    first_token = normalized.split(" ", 1)[0][:64]
    if first_token in _configured_promo_keywords():
        return first_token
    return first_token


def _build_referral_share_url(
    *,
    booking_url: str,
    secret_code: str,
    discount_amount: Decimal,
    discount_type: str,
) -> str:
    discount = _format_discount(discount_amount, discount_type)
    display_code = _format_secret_code(secret_code)
    share_text = (
        "Hallo 💙\n\n"
        f"Ich habe eine Aktion bei {_PROMO_REFERRAL_BRAND_NAME} gefunden:\n"
        f"Neukunden erhalten {discount} Rabatt beim ersten Besuch.\n\n"
        f"Schreib {_PROMO_REFERRAL_BRAND_NAME} einfach das Aktionswort: {display_code}\n\n"
        "Dann kann der Rabatt direkt mit deiner WhatsApp-Nummer verknüpft werden.\n\n"
        "Termin buchen:\n"
        f"{booking_url}"
    )
    return f"https://wa.me/?text={quote(share_text, safe='')}"


def _expires_display(expires_at: datetime) -> str:
    """Return display date string for customer-facing expiry.

    calendar_month boundaries fall on day=1 at midnight UTC.
    Show the last valid day (day before the exclusive boundary).
    """
    if (
        expires_at.day == 1
        and expires_at.hour == 0
        and expires_at.minute == 0
        and expires_at.second == 0
        and expires_at.microsecond == 0
    ):
        expires_at = expires_at - timedelta(days=1)
    return expires_at.strftime("%d.%m.%Y")


def build_reply_issued(
    expires_at: datetime,
    booking_url: str,
    discount_amount: Decimal,
    discount_type: str,
) -> str:
    discount = _format_discount(discount_amount, discount_type)
    exp = _expires_display(expires_at)
    return (
        f"Super! 🎁\n\n"
        f"Wir haben Ihren persönlichen Rabatt von {discount} für den ersten Besuch "
        f"mit Ihrer WhatsApp-Nummer verknüpft.\n\n"
        f"Wichtig: In der Online-Buchung werden die regulären Preise angezeigt. "
        f"Nach Ihrer Buchung erkennt unser System Ihre Nummer automatisch und "
        f"ordnet den Rabatt Ihrem ersten Besuch zu.\n\n"
        f"Der Rabatt gilt nur für Neukunden und ist bis {exp} gültig.\n\n"
        f"Termin buchen:\n{booking_url}"
    )


def build_reply_already_issued(expires_at: datetime, booking_url: str) -> str:
    exp = _expires_display(expires_at)
    return (
        f"Ihr persönlicher Rabatt ist bereits aktiv ✅\n\n"
        f"Er ist mit Ihrer WhatsApp-Nummer verknüpft und gilt bis {exp}.\n\n"
        f"In der Online-Buchung werden die regulären Preise angezeigt. "
        f"Nach Ihrer Buchung ordnen wir den Rabatt automatisch Ihrem ersten Besuch zu.\n\n"
        f"Termin buchen:\n{booking_url}"
    )


def build_reply_expired() -> str:
    return (
        "Ihr Aktions-Gutschein ist leider abgelaufen. 😔\n\n"
        "Bitte schreiben Sie uns, falls Sie Fragen zu aktuellen Angeboten haben."
    )


def build_reply_issued_with_card(
    expires_at: datetime,
    booking_url: str,
    discount_amount: Decimal,
    discount_type: str,
    card_number: str,
) -> str:
    discount = _format_discount(discount_amount, discount_type)
    exp = _expires_display(expires_at)
    return (
        f"Super! 🎁\n\n"
        f"Wir haben Ihren persönlichen Rabatt von {discount} für den ersten Besuch "
        f"mit Ihrer WhatsApp-Nummer verknüpft.\n\n"
        f"Ihre Rabattkarte: #{card_number}\n\n"
        f"Wichtig: In der Online-Buchung werden die regulären Preise angezeigt. "
        f"Nach Ihrer Buchung erkennt unser System Ihre Nummer automatisch und "
        f"ordnet den Rabatt Ihrem ersten Besuch zu.\n\n"
        f"Der Rabatt gilt nur für Neukunden und ist bis {exp} gültig.\n\n"
        f"Termin buchen:\n{booking_url}"
    )


def build_reply_rejected_not_new(
    booking_url: str,
    secret_code: str,
    discount_amount: Decimal,
    discount_type: str,
) -> str:
    display_code = _format_secret_code(secret_code)
    share_url = _build_referral_share_url(
        booking_url=booking_url,
        secret_code=display_code,
        discount_amount=discount_amount,
        discount_type=discount_type,
    )
    return (
        "Danke für Ihre Nachricht 💙\n\n"
        "Diese Aktion gilt nur für Neukunden beim ersten Besuch.\n\n"
        "Sie können den Rabatt aber gerne weiterempfehlen:\n"
        "Die neue Kundin soll uns das Aktionswort einfach direkt von ihrer eigenen "
        "WhatsApp-Nummer schreiben. Dann können wir den Rabatt korrekt mit ihrer "
        "Buchung verknüpfen.\n\n"
        f"Aktionswort: {display_code}\n\n"
        f"Freundin einladen:\n{share_url}\n\n"
        f"Termin buchen:\n{booking_url}"
    )


def build_reply_loyalty_card_failed() -> str:
    return (
        "Danke für Ihre Nachricht 💙\n\n"
        "Wir prüfen Ihre Aktion gerade manuell.\n"
        "Bitte schreiben Sie uns kurz, wenn Sie sofort einen Termin buchen möchten."
    )


def build_reply_new_client_check_failed() -> str:
    return (
        "Danke für Ihre Nachricht 💙\n\n"
        "Wir prüfen kurz, ob der Neukundenrabatt für Ihre Nummer verfügbar ist.\n"
        "Unser Team meldet sich bei Ihnen."
    )


def build_reply_checking_eligibility() -> str:
    return (
        "Danke für Ihre Nachricht 💙\n\n"
        "Wir prüfen kurz, ob der Neukundenrabatt für Ihre Nummer verfügbar ist.\n"
        "Sie erhalten gleich hier im Chat eine Rückmeldung.\n\n"
        "Bitte beachten Sie: Der Rabatt gilt nur für Neukunden beim ersten Besuch."
    )


def build_reply_checking_still_in_progress() -> str:
    return (
        "Danke für Ihre Nachricht 💙\n\n"
        "Wir prüfen den Neukundenrabatt für Ihre Nummer noch.\n"
        "Sie erhalten gleich hier im Chat eine Rückmeldung."
    )


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------


def _phone_variants(phone_e164: str) -> list[str]:
    digits = re.sub(r"\D+", "", phone_e164)
    variants = {phone_e164, digits, f"+{digits}"}
    return [v for v in variants if v]


async def _find_any_lead(
    session: AsyncSession,
    phone_e164: str,
    campaign_name: str,
) -> PromoLead | None:
    """Return the most recent PromoLead for this phone + campaign."""
    stmt = (
        select(PromoLead)
        .where(PromoLead.phone_e164 == phone_e164)
        .where(PromoLead.campaign_name == campaign_name)
        .order_by(PromoLead.created_at.desc())
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none()


async def _has_prior_visits(session: AsyncSession, phone_e164: str) -> bool:
    """Return True if this phone has at least one attended visit locally.

    Uses only locally synced records (Client + Record tables), so it may miss
    visits that have not yet been synced from Altegio.  The optional external
    CRM history check is handled separately by
    PROMO_CHECK_NEW_CLIENT_IN_ALTEGIO.

    An attended visit is indicated by attendance == 1 OR visit_attendance == 1,
    matching the same predicate used elsewhere in the project.
    """
    variants = _phone_variants(phone_e164)
    stmt = (
        select(Record.id)
        .join(Client, Client.id == Record.client_id)
        .where(Client.phone_e164.in_(variants))
        .where(Record.is_deleted.is_(False))
        .where(or_(Record.attendance == 1, Record.visit_attendance == 1))
        .limit(1)
    )
    result = await session.execute(stmt)
    return result.scalar_one_or_none() is not None


def _resolve_promo_location_id_for_company(company_id: int) -> int:
    try:
        location_map = json.loads(settings.promo_location_id_by_company or "{}")
    except (TypeError, ValueError) as exc:
        raise AltegioNewClientCheckError(f"invalid promo_location_id_by_company JSON: {exc}") from exc

    if not isinstance(location_map, dict):
        raise AltegioNewClientCheckError("invalid promo_location_id_by_company JSON: expected object")

    location_id_raw = location_map.get(str(company_id))
    if location_id_raw in (None, ""):
        raise AltegioNewClientCheckError(f"missing promo location_id for company_id={company_id}")

    try:
        location_id = int(location_id_raw)
    except (TypeError, ValueError) as exc:
        raise AltegioNewClientCheckError(
            f"invalid promo location_id for company_id={company_id}: {location_id_raw!r}"
        ) from exc

    if location_id <= 0:
        raise AltegioNewClientCheckError(f"invalid promo location_id for company_id={company_id}: {location_id}")

    return location_id


def _build_new_client_check_failed_lead(
    *,
    company_id: int,
    phone_e164: str,
    campaign_name: str,
    secret_code: str,
    discount_amount: Decimal,
    discount_type: str,
    now: datetime,
    error: str,
) -> PromoLead:
    return PromoLead(
        company_id=company_id,
        phone_e164=phone_e164,
        campaign_name=campaign_name,
        secret_code=secret_code[:64],
        discount_amount=discount_amount,
        discount_type=discount_type,
        status="cancelled",
        reject_reason="altegio_new_client_check_failed",
        issued_at=now,
        expires_at=now,
        meta={
            "altegio_new_client_check": "error",
            "altegio_new_client_check_error": error,
            "altegio_new_client_check_failed_at": now.isoformat(),
        },
    )


def _build_promo_eligibility_check_job(lead: PromoLead, *, now: datetime) -> MessageJob:
    return MessageJob(
        company_id=lead.company_id,
        record_id=None,
        client_id=None,
        job_type=PROMO_ELIGIBILITY_CHECK_JOB_TYPE,
        run_at=now,
        dedupe_key=f"{PROMO_ELIGIBILITY_CHECK_JOB_TYPE}:{lead.id}",
        payload={
            "promo_lead_id": lead.id,
            "phone_e164": lead.phone_e164,
            "company_id": lead.company_id,
            "campaign_name": lead.campaign_name,
            "secret_code": lead.secret_code,
        },
    )


def _mark_promo_check_done(lead: PromoLead, *, now: datetime, extra: dict | None = None) -> None:
    lead.meta = {
        **(lead.meta or {}),
        "promo_check": "done",
        "promo_check_completed_at": now.isoformat(),
        **(extra or {}),
    }


def _mark_promo_check_failed(lead: PromoLead, *, now: datetime, error: str) -> None:
    lead.meta = {
        **(lead.meta or {}),
        "promo_check": "failed",
        "promo_check_completed_at": now.isoformat(),
        "promo_check_error": error,
        "altegio_new_client_check": "error",
        "altegio_new_client_check_error": error,
    }


# ---------------------------------------------------------------------------
# Informational handler (funnel disabled)
# ---------------------------------------------------------------------------


async def handle_promo_info_command(
    session: AsyncSession,
    event: "WhatsAppEvent",
    phone_e164: str,
    text: str,
    sender_id: int,
    company_id: int | None,
    provider: WhatsAppProvider,
) -> None:
    """Send a safe informational promo reply when the funnel is disabled.

    No PromoLead is created.  The reply makes no promise of automatic
    discount assignment.  An OutboxMessage audit row is created on success.
    """
    if company_id is None:
        logger.warning("promo_info: missing company_id phone=%s sender_id=%s", phone_e164, sender_id)
        event.error = "promo_info: missing company_id"
        return

    cfg = settings
    reply = _PROMO_INFO_TEXT.format(booking_url=cfg.promo_booking_url)
    now = _utcnow()

    msg_id, err = await safe_send(
        provider=provider,
        sender_id=sender_id,
        phone=phone_e164,
        text=reply,
    )
    if err is not None:
        logger.warning(
            "promo_info: send failed phone=%s sender_id=%s err=%s",
            phone_e164,
            sender_id,
            err,
        )
        event.error = f"promo_info: send failed: {err}"
        return

    session.add(
        OutboxMessage(
            company_id=company_id,
            client_id=None,
            record_id=None,
            job_id=None,
            sender_id=sender_id,
            phone_e164=phone_e164,
            template_code="wa_promo_info",
            language="de",
            body=reply,
            status="sent",
            provider_message_id=msg_id,
            scheduled_at=now,
            sent_at=now,
            message_source="bot",
            meta={
                "source": "promo_lead",
                "command": "promo",
                "inbound_text": text,
                "whatsapp_event_id": event.id,
                "campaign_name": cfg.promo_campaign_name,
            },
        )
    )
    event.error = None
    logger.info(
        "promo_info: sent phone=%s sender_id=%s msg_id=%s",
        phone_e164,
        sender_id,
        msg_id,
    )


# ---------------------------------------------------------------------------
# Loyalty card issuance helper
# ---------------------------------------------------------------------------


async def _attempt_loyalty_card_issue(
    event: "WhatsAppEvent | None",
    lead: PromoLead,
    *,
    phone_e164: str,
    company_id: int,
) -> bool:
    """Attempt to issue an Altegio loyalty card for a newly issued PromoLead.

    Validates required settings before making any API call (fail-closed).
    Updates lead fields and meta on success.
    Sets event.error and lead.meta.loyalty_card_issued=False on any failure.

    Returns True if card was issued, False otherwise.
    """
    cfg = settings

    if not cfg.promo_loyalty_card_api_verified:
        err = "promo_loyalty: promo_loyalty_card_api_verified=False — card issuance blocked"
        lead.meta = {**(lead.meta or {}), "loyalty_card_issued": False, "loyalty_card_error": err}
        if event is not None:
            event.error = err
        logger.warning(
            "promo_loyalty: card issuance blocked (promo_loyalty_card_api_verified=False) phone=%s",
            phone_e164,
        )
        return False

    if not cfg.promo_loyalty_card_type_id:
        err = "promo_loyalty: missing promo_loyalty_card_type_id"
        lead.meta = {**(lead.meta or {}), "loyalty_card_issued": False, "loyalty_card_error": err}
        if event is not None:
            event.error = err
        logger.warning("promo_loyalty: missing promo_loyalty_card_type_id phone=%s", phone_e164)
        return False

    if not cfg.promo_discount_program_id:
        err = "promo_loyalty: missing promo_discount_program_id"
        lead.meta = {**(lead.meta or {}), "loyalty_card_issued": False, "loyalty_card_error": err}
        if event is not None:
            event.error = err
        logger.warning("promo_loyalty: missing promo_discount_program_id phone=%s", phone_e164)
        return False

    try:
        location_id = _resolve_promo_location_id_for_company(company_id)
    except AltegioNewClientCheckError as exc:
        err = f"promo_loyalty: {exc}"
        lead.meta = {**(lead.meta or {}), "loyalty_card_issued": False, "loyalty_card_error": err}
        if event is not None:
            event.error = err
        logger.warning("promo_loyalty: location_id resolution failed phone=%s: %s", phone_e164, exc)
        return False

    try:
        result = await issue_promo_loyalty_card(
            phone_e164=phone_e164,
            location_id=location_id,
            card_type_id=cfg.promo_loyalty_card_type_id,
        )
    except AltegioLoyaltyError as exc:
        err_str = str(exc)
        lead.meta = {**(lead.meta or {}), "loyalty_card_issued": False, "loyalty_card_error": err_str}
        if event is not None:
            event.error = f"promo_loyalty: {err_str}"
        logger.warning("promo_loyalty: card issue failed phone=%s: %s", phone_e164, exc)
        return False

    lead.altegio_client_id = result.altegio_client_id
    lead.loyalty_card_id = result.loyalty_card_id
    lead.loyalty_card_number = result.loyalty_card_number
    lead.card_type_id = result.card_type_id
    lead.discount_program_id = cfg.promo_discount_program_id
    lead.location_id = location_id
    lead.meta = {**(lead.meta or {}), "loyalty_card_issued": True}
    logger.info(
        "promo_loyalty: card issued card_id=%s card_number=%s phone=%s",
        result.loyalty_card_id,
        result.loyalty_card_number,
        phone_e164,
    )
    return True


async def process_promo_eligibility_check_job(
    session: AsyncSession,
    job: MessageJob,
    provider: WhatsAppProvider,
) -> None:
    """Resolve a pending async promo eligibility check and send the final reply."""
    now = _utcnow()
    payload = job.payload or {}
    promo_lead_id = payload.get("promo_lead_id")
    if promo_lead_id is None:
        job.status = "failed"
        job.locked_at = None
        job.last_error = "promo_eligibility_check: missing promo_lead_id"
        return

    stmt = select(PromoLead).where(PromoLead.id == int(promo_lead_id)).with_for_update()
    result = await session.execute(stmt)
    lead = result.scalar_one_or_none()
    if lead is None:
        job.status = "failed"
        job.locked_at = None
        job.last_error = f"promo_eligibility_check: PromoLead not found id={promo_lead_id}"
        return

    if lead.status != "pending_check":
        job.status = "done"
        job.locked_at = None
        job.last_error = None
        return

    cfg = settings
    phone_e164 = lead.phone_e164
    discount_amount = Decimal(str(cfg.promo_discount_amount))
    discount_type = cfg.promo_discount_type
    template_code: str
    reply: str
    card_issue_failed = False

    if await _has_prior_visits(session, phone_e164):
        lead.status = "rejected_not_new"
        lead.reject_reason = "has_prior_visits"
        _mark_promo_check_done(lead, now=now)
        reply = build_reply_rejected_not_new(
            cfg.promo_booking_url,
            lead.secret_code,
            discount_amount,
            discount_type,
        )
        template_code = "wa_promo_lead_rejected_not_new"
    else:
        try:
            if cfg.promo_check_new_client_in_altegio:
                location_id = _resolve_promo_location_id_for_company(lead.company_id)
                has_altegio_records = await check_client_has_any_altegio_record(
                    phone_e164=phone_e164,
                    location_id=location_id,
                )
            else:
                has_altegio_records = False
        except AltegioNewClientCheckError as exc:
            err = str(exc)
            lead.status = "cancelled"
            lead.reject_reason = "altegio_new_client_check_failed"
            _mark_promo_check_failed(lead, now=now, error=err)
            reply = build_reply_new_client_check_failed()
            template_code = "wa_promo_lead_manual_check"
        else:
            if has_altegio_records:
                lead.status = "rejected_not_new"
                lead.reject_reason = "has_altegio_records"
                _mark_promo_check_done(
                    lead,
                    now=now,
                    extra={"altegio_new_client_check": "records_found"},
                )
                reply = build_reply_rejected_not_new(
                    cfg.promo_booking_url,
                    lead.secret_code,
                    discount_amount,
                    discount_type,
                )
                template_code = "wa_promo_lead_rejected_not_new"
            else:
                lead.status = "issued"
                lead.discount_amount = discount_amount
                lead.discount_type = discount_type
                _mark_promo_check_done(
                    lead,
                    now=now,
                    extra={
                        "altegio_new_client_check": (
                            "no_records" if cfg.promo_check_new_client_in_altegio else "disabled"
                        )
                    },
                )
                if cfg.promo_issue_loyalty_card_enabled:
                    if await _attempt_loyalty_card_issue(
                        None,
                        lead,
                        phone_e164=phone_e164,
                        company_id=lead.company_id,
                    ):
                        reply = build_reply_issued_with_card(
                            lead.expires_at,
                            cfg.promo_booking_url,
                            discount_amount,
                            discount_type,
                            lead.loyalty_card_number,
                        )
                        template_code = "wa_promo_loyalty_card_issued"
                    else:
                        card_issue_failed = True
                        reply = build_reply_loyalty_card_failed()
                        template_code = "wa_promo_loyalty_card_issue_failed"
                else:
                    reply = build_reply_issued(
                        lead.expires_at,
                        cfg.promo_booking_url,
                        discount_amount,
                        discount_type,
                    )
                    template_code = "wa_promo_lead_issued"

    sender_id = await pick_sender_id(
        session=session,
        company_id=lead.company_id,
        sender_code="default",
    )
    if sender_id is None:
        job.status = "failed"
        job.locked_at = None
        job.last_error = "promo_eligibility_check: no active sender for company"
        lead.meta = {
            **(lead.meta or {}),
            "promo_check_final_reply_sent": False,
            "promo_check_final_reply_error": job.last_error,
        }
        return

    attempts = getattr(job, "attempts", 0) + 1
    job.attempts = attempts
    msg_id, err = await safe_send(
        provider=provider,
        sender_id=sender_id,
        phone=phone_e164,
        text=reply,
        company_id=lead.company_id,
    )
    if err is not None:
        session.add(
            OutboxMessage(
                company_id=lead.company_id,
                client_id=None,
                record_id=None,
                job_id=job.id,
                sender_id=sender_id,
                phone_e164=phone_e164,
                template_code=template_code,
                language="de",
                body=reply,
                status="failed",
                error=err,
                provider_message_id=msg_id,
                scheduled_at=job.run_at,
                sent_at=now,
                message_source="bot",
                meta={"source": PROMO_ELIGIBILITY_CHECK_JOB_TYPE, "promo_lead_id": lead.id},
            )
        )
        job.status = "failed"
        job.locked_at = None
        job.last_error = f"promo_eligibility_check: send failed: {err}"
        lead.meta = {
            **(lead.meta or {}),
            "promo_check_final_reply_sent": False,
            "promo_check_final_reply_error": err,
        }
        return

    session.add(
        OutboxMessage(
            company_id=lead.company_id,
            client_id=None,
            record_id=None,
            job_id=job.id,
            sender_id=sender_id,
            phone_e164=phone_e164,
            template_code=template_code,
            language="de",
            body=reply,
            status="sent",
            error=None,
            provider_message_id=msg_id,
            scheduled_at=job.run_at,
            sent_at=now,
            message_source="bot",
            meta={
                "source": PROMO_ELIGIBILITY_CHECK_JOB_TYPE,
                "promo_lead_id": lead.id,
                "card_issue_failed": card_issue_failed,
            },
        )
    )
    lead.meta = {
        **(lead.meta or {}),
        "promo_check_final_reply_sent": True,
        "promo_check_final_reply_provider_message_id": msg_id,
    }
    job.status = "done"
    job.locked_at = None
    job.last_error = None


# ---------------------------------------------------------------------------
# Main funnel handler (funnel enabled)
# ---------------------------------------------------------------------------


async def handle_promo_command(
    session: AsyncSession,
    event: "WhatsAppEvent",
    phone_e164: str,
    text: str,
    sender_id: int,
    company_id: int | None,
    provider: WhatsAppProvider,
) -> None:
    """Handle an inbound WhatsApp promo / secret-word command.

    Flow:
    1.  Look for the most recent PromoLead for this phone + campaign.
    2a. Lead is active (issued/booked/applied) AND not expired
            → reply 'already active', no new lead.
    2b. Lead is active AND past expires_at
            → mark expired after send, reply 'expired'.
    2c. Lead exists but is already closed (expired/cancelled/rejected_*)
            → reply 'expired', no new lead.
    3.  No lead exists: build candidate, persist via savepoint.
        Savepoint won → build reply from persisted lead.
        Savepoint lost (UniqueConstraint race) → savepoint auto-rolled back;
            outer transaction stays clean; re-read winner; send already-active reply.
    4.  Send free-form WhatsApp reply.
    5.  Send failure for a newly persisted lead: mark meta.reply_sent=False,
            meta.reply_send_error; set event.error; no OutboxMessage.
    6.  Send success: mark meta.reply_sent=True; create OutboxMessage audit row.
    7.  Mark expired lead status after successful send.

    Sends a free-form WhatsApp text reply (NOT a Meta template).
    Creates an OutboxMessage audit row on success.
    """
    if company_id is None:
        logger.warning("promo_lead: missing company_id phone=%s sender_id=%s", phone_e164, sender_id)
        event.error = "promo_lead: missing company_id"
        return

    now = _utcnow()
    cfg = settings
    discount_amount = Decimal(str(cfg.promo_discount_amount))
    promo_keyword = _extract_promo_keyword(text)

    # ── 1. Look up most recent lead ──────────────────────────────────────────
    lead = await _find_any_lead(session, phone_e164, cfg.promo_campaign_name)

    new_lead: PromoLead | None = None
    repair_lead: PromoLead | None = None
    mark_lead_expired: bool = False
    card_issue_failed: bool = False
    reply: str
    template_code: str

    if lead is not None and lead.status in ("issued", "booked", "applied"):
        if lead.expires_at <= now:
            # Active status but validity elapsed → will mark expired after send.
            mark_lead_expired = True
            reply = build_reply_expired()
            template_code = "wa_promo_lead_expired"
        elif cfg.promo_issue_loyalty_card_enabled and lead.loyalty_card_number:
            # Card already issued — resend with card number.
            # Set repair_lead so post-send meta update clears card_message_pending.
            repair_lead = lead
            reply = build_reply_issued_with_card(
                lead.expires_at,
                cfg.promo_booking_url,
                lead.discount_amount,
                lead.discount_type,
                lead.loyalty_card_number,
            )
            template_code = "wa_promo_loyalty_card_issued"
        elif cfg.promo_issue_loyalty_card_enabled and not lead.loyalty_card_id and lead.status == "issued":
            # Repair path: card was never issued for this lead — attempt now.
            repair_lead = lead
            if await _attempt_loyalty_card_issue(event, lead, phone_e164=phone_e164, company_id=company_id):
                reply = build_reply_issued_with_card(
                    lead.expires_at,
                    cfg.promo_booking_url,
                    lead.discount_amount,
                    lead.discount_type,
                    lead.loyalty_card_number,
                )
                template_code = "wa_promo_loyalty_card_issued"
            else:
                card_issue_failed = True
                reply = build_reply_loyalty_card_failed()
                template_code = "wa_promo_loyalty_card_issue_failed"
        else:
            # Still active → resend confirmation.
            reply = build_reply_already_issued(lead.expires_at, cfg.promo_booking_url)
            template_code = "wa_promo_lead_already_issued"

    elif lead is not None and lead.status == "rejected_not_new":
        # Client was already rejected; resend the rejection reply.
        reply = build_reply_rejected_not_new(
            cfg.promo_booking_url,
            promo_keyword,
            discount_amount,
            cfg.promo_discount_type,
        )
        template_code = "wa_promo_lead_rejected_not_new"

    elif lead is not None and lead.status == "pending_check":
        # Eligibility is already queued; keep the user informed without duplicating jobs.
        reply = build_reply_checking_still_in_progress()
        template_code = "wa_promo_lead_checking_still_in_progress"

    elif lead is not None and lead.status == "cancelled" and lead.reject_reason == "altegio_new_client_check_failed":
        # External eligibility check failed earlier; keep the conversation in manual-review mode.
        reply = build_reply_new_client_check_failed()
        template_code = "wa_promo_lead_manual_check"

    elif lead is not None:
        # Other terminal states: expired, cancelled, apply_failed, rejected_service_not_allowed.
        reply = build_reply_expired()
        template_code = "wa_promo_lead_expired"

    else:
        # ── 2. No existing lead: build candidate ─────────────────────────────
        if cfg.promo_async_eligibility_check_enabled:
            expires_at = compute_expires_at(now, cfg.promo_validity_mode, cfg.promo_validity_days)
            candidate = PromoLead(
                company_id=company_id,
                phone_e164=phone_e164,
                campaign_name=cfg.promo_campaign_name,
                secret_code=promo_keyword,
                discount_amount=discount_amount,
                discount_type=cfg.promo_discount_type,
                status="pending_check",
                issued_at=now,
                expires_at=expires_at,
                meta={
                    "promo_check": "pending",
                    "promo_check_pending_at": now.isoformat(),
                    "altegio_new_client_check": "pending",
                },
            )
        elif await _has_prior_visits(session, phone_e164):
            candidate = PromoLead(
                company_id=company_id,
                phone_e164=phone_e164,
                campaign_name=cfg.promo_campaign_name,
                secret_code=promo_keyword,
                discount_amount=discount_amount,
                discount_type=cfg.promo_discount_type,
                status="rejected_not_new",
                reject_reason="has_prior_visits",
                issued_at=now,
                expires_at=now,
            )
        elif cfg.promo_check_new_client_in_altegio:
            try:
                location_id = _resolve_promo_location_id_for_company(company_id)
                has_altegio_records = await check_client_has_any_altegio_record(
                    phone_e164=phone_e164,
                    location_id=location_id,
                )
            except AltegioNewClientCheckError as exc:
                err = str(exc)
                logger.warning(
                    "promo_lead: Altegio new-client check failed phone=%s company_id=%s: %s",
                    phone_e164,
                    company_id,
                    err,
                )
                candidate = _build_new_client_check_failed_lead(
                    company_id=company_id,
                    phone_e164=phone_e164,
                    campaign_name=cfg.promo_campaign_name,
                    secret_code=promo_keyword,
                    discount_amount=discount_amount,
                    discount_type=cfg.promo_discount_type,
                    now=now,
                    error=err,
                )
            else:
                if has_altegio_records:
                    candidate = PromoLead(
                        company_id=company_id,
                        phone_e164=phone_e164,
                        campaign_name=cfg.promo_campaign_name,
                        secret_code=promo_keyword,
                        discount_amount=discount_amount,
                        discount_type=cfg.promo_discount_type,
                        status="rejected_not_new",
                        reject_reason="has_altegio_records",
                        issued_at=now,
                        expires_at=now,
                        meta={"altegio_new_client_check": "records_found"},
                    )
                else:
                    expires_at = compute_expires_at(now, cfg.promo_validity_mode, cfg.promo_validity_days)
                    candidate = PromoLead(
                        company_id=company_id,
                        phone_e164=phone_e164,
                        campaign_name=cfg.promo_campaign_name,
                        secret_code=text[:64],
                        discount_amount=discount_amount,
                        discount_type=cfg.promo_discount_type,
                        status="issued",
                        issued_at=now,
                        expires_at=expires_at,
                        meta={"altegio_new_client_check": "no_records"},
                    )
        else:
            expires_at = compute_expires_at(now, cfg.promo_validity_mode, cfg.promo_validity_days)
            candidate = PromoLead(
                company_id=company_id,
                phone_e164=phone_e164,
                campaign_name=cfg.promo_campaign_name,
                secret_code=text[:64],
                discount_amount=discount_amount,
                discount_type=cfg.promo_discount_type,
                status="issued",
                issued_at=now,
                expires_at=expires_at,
                meta={"altegio_new_client_check": "disabled"},
            )

        # ── 3. Persist via savepoint — safe concurrent-insert handling ────────
        try:
            async with session.begin_nested():
                session.add(candidate)
                await session.flush()
            # Savepoint committed: we won the race.
            new_lead = candidate
            if new_lead.status == "pending_check":
                session.add(_build_promo_eligibility_check_job(new_lead, now=now))
        except IntegrityError:
            # UniqueConstraint violation: a concurrent worker won the race.
            # The savepoint is auto-rolled back; the outer transaction is clean.
            logger.warning(
                "promo_lead: concurrent insert race phone=%s campaign=%s — reading winner",
                phone_e164,
                cfg.promo_campaign_name,
            )
            lead = await _find_any_lead(session, phone_e164, cfg.promo_campaign_name)

        # ── Attempt loyalty card issuance for new issued leads ────────────────
        if new_lead is not None and new_lead.status == "issued" and cfg.promo_issue_loyalty_card_enabled:
            if not await _attempt_loyalty_card_issue(
                event,
                new_lead,
                phone_e164=phone_e164,
                company_id=company_id,
            ):
                card_issue_failed = True

        # Determine reply from the actual DB outcome.
        if new_lead is not None:
            if new_lead.status == "pending_check":
                reply = build_reply_checking_eligibility()
                template_code = "wa_promo_lead_checking_eligibility"
            elif new_lead.status == "rejected_not_new":
                reply = build_reply_rejected_not_new(
                    cfg.promo_booking_url,
                    promo_keyword,
                    discount_amount,
                    cfg.promo_discount_type,
                )
                template_code = "wa_promo_lead_rejected_not_new"
            elif new_lead.status == "cancelled" and new_lead.reject_reason == "altegio_new_client_check_failed":
                reply = build_reply_new_client_check_failed()
                template_code = "wa_promo_lead_manual_check"
            elif not card_issue_failed and cfg.promo_issue_loyalty_card_enabled and new_lead.loyalty_card_number:
                reply = build_reply_issued_with_card(
                    new_lead.expires_at,
                    cfg.promo_booking_url,
                    discount_amount,
                    cfg.promo_discount_type,
                    new_lead.loyalty_card_number,
                )
                template_code = "wa_promo_loyalty_card_issued"
            elif card_issue_failed:
                reply = build_reply_loyalty_card_failed()
                template_code = "wa_promo_loyalty_card_issue_failed"
            else:
                reply = build_reply_issued(
                    new_lead.expires_at, cfg.promo_booking_url, discount_amount, cfg.promo_discount_type
                )
                template_code = "wa_promo_lead_issued"
        else:
            # Race lost: reply based on the winner we just re-read.
            if lead is not None and lead.status in ("issued", "booked", "applied") and lead.expires_at > now:
                reply = build_reply_already_issued(lead.expires_at, cfg.promo_booking_url)
                template_code = "wa_promo_lead_already_issued"
            elif lead is not None and lead.status == "pending_check":
                reply = build_reply_checking_still_in_progress()
                template_code = "wa_promo_lead_checking_still_in_progress"
            elif lead is not None and lead.status == "rejected_not_new":
                reply = build_reply_rejected_not_new(
                    cfg.promo_booking_url,
                    promo_keyword,
                    discount_amount,
                    cfg.promo_discount_type,
                )
                template_code = "wa_promo_lead_rejected_not_new"
            elif (
                lead is not None
                and lead.status == "cancelled"
                and lead.reject_reason == "altegio_new_client_check_failed"
            ):
                reply = build_reply_new_client_check_failed()
                template_code = "wa_promo_lead_manual_check"
            else:
                reply = build_reply_expired()
                template_code = "wa_promo_lead_expired"

    # ── 4. Send free-form reply ──────────────────────────────────────────────
    logger.info(
        "promo_lead: phone=%s template=%s campaign=%s event_id=%s",
        phone_e164,
        template_code,
        cfg.promo_campaign_name,
        event.id,
    )

    msg_id, err = await safe_send(
        provider=provider,
        sender_id=sender_id,
        phone=phone_e164,
        text=reply,
    )

    if err is not None:
        logger.warning(
            "promo_lead: send failed phone=%s sender_id=%s err=%s",
            phone_e164,
            sender_id,
            err,
        )
        lead_to_update = new_lead or repair_lead
        if lead_to_update is not None:
            meta_update: dict = {"reply_sent": False, "reply_send_error": str(err)}
            if lead_to_update.loyalty_card_id:
                # Card was issued but message delivery failed — flag for ops retry.
                meta_update["card_message_pending"] = True
            lead_to_update.meta = {**(lead_to_update.meta or {}), **meta_update}
        event.error = f"promo_lead: send failed: {err}"
        return

    # ── 5. Post-send mutations ───────────────────────────────────────────────
    if mark_lead_expired:
        lead.status = "expired"

    lead_to_update = new_lead or repair_lead
    if lead_to_update is not None:
        meta_after = {**(lead_to_update.meta or {}), "reply_sent": True}
        if meta_after.get("card_message_pending"):
            meta_after["card_message_pending"] = False
        lead_to_update.meta = meta_after

    # ── 6. Audit OutboxMessage ───────────────────────────────────────────────
    session.add(
        OutboxMessage(
            company_id=company_id,
            client_id=None,
            record_id=None,
            job_id=None,
            sender_id=sender_id,
            phone_e164=phone_e164,
            template_code=template_code,
            language="de",
            body=reply,
            status="sent",
            provider_message_id=msg_id,
            scheduled_at=now,
            sent_at=now,
            message_source="bot",
            meta={
                "source": "promo_lead",
                "command": "promo",
                "inbound_text": text,
                "whatsapp_event_id": event.id,
                "campaign_name": cfg.promo_campaign_name,
            },
        )
    )

    if not card_issue_failed:
        event.error = None
    logger.info(
        "promo_lead: sent phone=%s sender_id=%s msg_id=%s template=%s",
        phone_e164,
        sender_id,
        msg_id,
        template_code,
    )
