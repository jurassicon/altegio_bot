"""Enqueue promo card booking reminder jobs for eligible sommer_2026 leads.

Selects leads that have an ACTIVE issued promo card (card identifiers present,
loyalty_card_issued=true) but no suitable booking and have not yet been sent
a booking reminder via WhatsApp.

Usage (dry-run — no DB writes):
    uv run python -m altegio_bot.scripts.enqueue_promo_booking_reminders --dry-run

Usage (apply — creates MessageJob rows and updates PromoLead.meta):
    uv run python -m altegio_bot.scripts.enqueue_promo_booking_reminders --apply
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import re
import sys
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

CAMPAIGN_NAME = "sommer_2026"
_PROMO_REMINDER_TEMPLATE = "kitilash_ka_promo_card_booking_reminder_v1"


def _phone_digits(phone: str | None) -> str:
    """Return only digits from *phone* for normalized opt-out comparison."""
    if not phone:
        return ""
    return re.sub(r"\D", "", phone)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--dry-run",
        action="store_true",
        help="Print eligible leads without writing to DB",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        help="Create MessageJob rows for eligible leads",
    )
    return parser.parse_args()


async def _fetch_eligible_leads(session):  # type: ignore[no-untyped-def]
    """Return PromoLead rows eligible for a booking reminder.

    Applies all active-card guards in SQL (loyalty_card_id, loyalty_card_number,
    location_id, discount_program_id, meta.loyalty_card_issued=true,
    meta.card_issue_failed not true) plus status/date/meta filters.
    Opted-out phones are filtered in Python using normalized digit comparison.
    """
    from sqlalchemy import select, text

    from altegio_bot.models.models import Client, PromoLead

    now = datetime.now(timezone.utc)

    stmt = (
        select(PromoLead)
        .where(PromoLead.campaign_name == CAMPAIGN_NAME)
        .where(PromoLead.status == "issued")
        .where(PromoLead.issued_at.is_not(None))
        .where(PromoLead.expires_at > now)
        .where(PromoLead.applied_at.is_(None))
        .where(PromoLead.used_at.is_(None))
        .where(PromoLead.cancelled_at.is_(None))
        # Active card identifiers must be present
        .where(PromoLead.loyalty_card_id.is_not(None))
        .where(PromoLead.loyalty_card_number.is_not(None))
        .where(PromoLead.location_id.is_not(None))
        .where(PromoLead.discount_program_id.is_not(None))
        # Card must have been successfully issued
        .where(text("(meta ->> 'loyalty_card_issued') = 'true'"))
        # Card issue must not have failed
        .where(text("COALESCE((meta ->> 'card_issue_failed')::boolean, false) IS NOT TRUE"))
        # Reminder not already sent
        .where(text("(meta ->> 'booking_reminder_sent_at') IS NULL"))
        # Not flagged for manual review
        .where(text("COALESCE((meta ->> 'manual_review_required')::boolean, false) IS NOT TRUE"))
        .order_by(PromoLead.id)
    )
    result = await session.execute(stmt)
    leads = result.scalars().all()

    # Normalized opt-out: compare digit-only phone strings to catch formatting variants
    opted_out_stmt = select(Client.phone_e164).where(Client.wa_opted_out.is_(True))
    opted_out_result = await session.execute(opted_out_stmt)
    opted_out_digits: set[str] = {_phone_digits(row) for (row,) in opted_out_result.all() if row}

    return [lead for lead in leads if _phone_digits(lead.phone_e164) not in opted_out_digits]


async def _enqueue_one(session, lead, now: datetime) -> bool:  # type: ignore[no-untyped-def]
    """Create a MessageJob for *lead*. Returns True if a new job was created.

    Fix 6: on success, write booking_reminder_job_id / booking_reminder_queued_at /
    booking_reminder_template to lead.meta.  If job already exists, backfill
    missing meta fields without creating a duplicate.
    """
    from sqlalchemy import select
    from sqlalchemy.exc import IntegrityError

    from altegio_bot.models.models import MessageJob
    from altegio_bot.workers.promo_lead_handler import PROMO_CARD_BOOKING_REMINDER_JOB_TYPE

    dedupe_key = f"{PROMO_CARD_BOOKING_REMINDER_JOB_TYPE}:{lead.id}"

    existing = (
        await session.execute(select(MessageJob).where(MessageJob.dedupe_key == dedupe_key))
    ).scalar_one_or_none()
    if existing is not None:
        logger.info("lead_id=%s already has job_id=%s (dedupe), skipping", lead.id, existing.id)
        # Backfill meta if not already set
        meta = lead.meta or {}
        if not meta.get("booking_reminder_job_id"):
            lead.meta = {
                **meta,
                "booking_reminder_job_id": existing.id,
                "booking_reminder_queued_at": now.isoformat(),
                "booking_reminder_template": _PROMO_REMINDER_TEMPLATE,
            }
        return False

    job: MessageJob | None = None
    try:
        async with session.begin_nested():
            job = MessageJob(
                company_id=lead.company_id,
                record_id=None,
                client_id=None,
                job_type=PROMO_CARD_BOOKING_REMINDER_JOB_TYPE,
                run_at=now,
                dedupe_key=dedupe_key,
                max_attempts=3,
                payload={"promo_lead_id": lead.id},
            )
            session.add(job)
            await session.flush()
            logger.info("lead_id=%s enqueued job_id=%s phone=%s", lead.id, job.id, lead.phone_e164)
    except IntegrityError:
        logger.info("lead_id=%s race condition on insert, dedupe key already exists", lead.id)
        job = (
            await session.execute(select(MessageJob).where(MessageJob.dedupe_key == dedupe_key))
        ).scalar_one_or_none()
        if job is not None:
            meta = lead.meta or {}
            if not meta.get("booking_reminder_job_id"):
                lead.meta = {
                    **meta,
                    "booking_reminder_job_id": job.id,
                    "booking_reminder_queued_at": now.isoformat(),
                    "booking_reminder_template": _PROMO_REMINDER_TEMPLATE,
                }
        return False

    if job is not None:
        meta = lead.meta or {}
        lead.meta = {
            **meta,
            "booking_reminder_job_id": job.id,
            "booking_reminder_queued_at": now.isoformat(),
            "booking_reminder_template": _PROMO_REMINDER_TEMPLATE,
        }

    return True


async def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    args = _parse_args()

    from altegio_bot.db import SessionLocal

    now = datetime.now(timezone.utc)

    async with SessionLocal() as session:
        async with session.begin():
            eligible = await _fetch_eligible_leads(session)

            print(f"Eligible leads: {len(eligible)}")
            for lead in eligible:
                print(
                    f"  lead_id={lead.id} phone={lead.phone_e164} "
                    f"company_id={lead.company_id} "
                    f"discount={lead.discount_amount} "
                    f"expires_at={lead.expires_at}"
                )

            if args.dry_run:
                print("Dry-run mode — no changes written.")
                return 0

            enqueued = 0
            skipped = 0
            for lead in eligible:
                created = await _enqueue_one(session, lead, now)
                if created:
                    enqueued += 1
                else:
                    skipped += 1

    print(f"enqueued={enqueued} skipped={skipped}")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
