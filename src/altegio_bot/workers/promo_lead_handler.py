"""WhatsApp promo lead handler.

Manages the PromoLead lifecycle triggered by secret-word WhatsApp messages.

This PR (first promo funnel PR) implements:
  - Creating a PromoLead with status 'issued'.
  - Detecting and resending 'already active' replies.
  - Marking expired leads and replying with an expiry message.
  - Local-only 'not new client' check → status 'rejected_not_new'.
  - Sending free-form WhatsApp text replies (NOT Meta templates).
  - Creating OutboxMessage audit rows.

NOT in this PR (deferred to future PRs):
  - Altegio loyalty card API (issuing / applying discount programs).
  - Full Altegio CRM API history check for new-client validation.
  - Applying the discount to a record/visit_id.
  - Ops dashboard metrics for PromoLead.
"""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import TYPE_CHECKING

from sqlalchemy import or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.models.models import Client, OutboxMessage, PromoLead, Record
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.providers.dummy import safe_send
from altegio_bot.settings import settings

if TYPE_CHECKING:
    from altegio_bot.models.models import WhatsAppEvent

logger = logging.getLogger("promo_lead_handler")


# ---------------------------------------------------------------------------
# Safe informational reply (promo_lead_funnel_enabled = False)
# ---------------------------------------------------------------------------

_PROMO_INFO_TEXT = (
    "Danke für Ihr Interesse! 🎁\n\n"
    "Diese Aktion richtet sich an Neukunden beim ersten Besuch.\n\n"
    "Bitte buchen Sie Ihren Termin online – wir freuen uns auf Sie.\n\n"
    "Termin buchen:\n{booking_url}"
)


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


def build_reply_rejected_not_new() -> str:
    return (
        "Danke für Ihre Nachricht 💙\n\n"
        "Diese Aktion gilt nur für Neukunden beim ersten Besuch.\n"
        "Wir helfen Ihnen aber gerne, einen passenden Termin oder eine aktuelle "
        "Aktion zu finden."
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

    Uses only locally synced records (Client + Record tables).  This may
    miss visits that have not yet been synced from Altegio.  A full Altegio
    CRM API check is deferred to a future PR.

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

    # ── 1. Look up most recent lead ──────────────────────────────────────────
    lead = await _find_any_lead(session, phone_e164, cfg.promo_campaign_name)

    new_lead: PromoLead | None = None
    mark_lead_expired: bool = False
    reply: str
    template_code: str

    if lead is not None and lead.status in ("issued", "booked", "applied"):
        if lead.expires_at <= now:
            # Active status but validity elapsed → will mark expired after send.
            mark_lead_expired = True
            reply = build_reply_expired()
            template_code = "wa_promo_lead_expired"
        else:
            # Still active → resend confirmation.
            reply = build_reply_already_issued(lead.expires_at, cfg.promo_booking_url)
            template_code = "wa_promo_lead_already_issued"

    elif lead is not None and lead.status == "rejected_not_new":
        # Client was already rejected; resend the rejection reply.
        reply = build_reply_rejected_not_new()
        template_code = "wa_promo_lead_rejected_not_new"

    elif lead is not None:
        # Other terminal states: expired, cancelled, apply_failed, rejected_service_not_allowed.
        reply = build_reply_expired()
        template_code = "wa_promo_lead_expired"

    else:
        # ── 2. No existing lead: build candidate ─────────────────────────────
        if await _has_prior_visits(session, phone_e164):
            candidate = PromoLead(
                company_id=company_id,
                phone_e164=phone_e164,
                campaign_name=cfg.promo_campaign_name,
                secret_code=text[:64],
                discount_amount=discount_amount,
                discount_type=cfg.promo_discount_type,
                status="rejected_not_new",
                reject_reason="has_prior_visits",
                issued_at=now,
                expires_at=now,
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
            )

        # ── 3. Persist via savepoint — safe concurrent-insert handling ────────
        try:
            async with session.begin_nested():
                session.add(candidate)
                await session.flush()
            # Savepoint committed: we won the race.
            new_lead = candidate
        except IntegrityError:
            # UniqueConstraint violation: a concurrent worker won the race.
            # The savepoint is auto-rolled back; the outer transaction is clean.
            logger.warning(
                "promo_lead: concurrent insert race phone=%s campaign=%s — reading winner",
                phone_e164,
                cfg.promo_campaign_name,
            )
            lead = await _find_any_lead(session, phone_e164, cfg.promo_campaign_name)

        # Determine reply from the actual DB outcome.
        if new_lead is not None:
            if new_lead.status == "rejected_not_new":
                reply = build_reply_rejected_not_new()
                template_code = "wa_promo_lead_rejected_not_new"
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
        if new_lead is not None:
            # Lead persisted but reply not delivered — mark for ops awareness / retry.
            new_lead.meta = {**(new_lead.meta or {}), "reply_sent": False, "reply_send_error": str(err)}
        event.error = f"promo_lead: send failed: {err}"
        return

    # ── 5. Post-send mutations ───────────────────────────────────────────────
    if mark_lead_expired:
        lead.status = "expired"

    if new_lead is not None:
        new_lead.meta = {**(new_lead.meta or {}), "reply_sent": True}

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

    event.error = None
    logger.info(
        "promo_lead: sent phone=%s sender_id=%s msg_id=%s template=%s",
        phone_e164,
        sender_id,
        msg_id,
        template_code,
    )
