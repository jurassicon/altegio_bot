"""Monthly newsletter script: "new clients" for the previous calendar month.

A client qualifies for the newsletter if, within the previous calendar month:
  - total number of records  <= 1, AND
  - no record has attendance == 1 (arrived), AND
  - client.wa_opted_out is False.

For each qualifying client the script:
  1. Issues a loyalty card via the Altegio API.
  2. Queues a ``newsletter_new_clients_monthly`` MessageJob containing
     ``loyalty_card_text`` in the payload.

Usage::

    python -m altegio_bot.scripts.run_newsletter_new_clients_monthly \
        --company-id 758285 \
        --location-id 758285 \
        --card-type-id <id>   # optional; first available type is used when omitted

Environment variables required:
    DATABASE_URL
    ALTEGIO_PARTNER_TOKEN
    ALTEGIO_USER_TOKEN
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.altegio_loyalty import AltegioLoyaltyClient
from altegio_bot.db import SessionLocal
from altegio_bot.message_planner import add_job
from altegio_bot.models.models import Client, Record
from altegio_bot.utils import utcnow

logger = logging.getLogger(__name__)

NEWSLETTER_JOB_TYPE = "newsletter_new_clients_monthly"
_LOYALTY_CARD_PREFIX = "Kundenkarte #"


def _prev_month_range(
    ref: datetime | None = None,
) -> tuple[datetime, datetime]:
    """Return (start_inclusive, end_exclusive) for the previous calendar month.

    Both datetimes are UTC midnight.
    """
    now = ref or utcnow()
    year, month = now.year, now.month
    if month == 1:
        year -= 1
        month = 12
    else:
        month -= 1
    start = datetime(year, month, 1, tzinfo=timezone.utc)
    # end exclusive = first second of the following month
    next_month = month + 1 if month < 12 else 1
    next_year = year if month < 12 else year + 1
    end_exclusive = datetime(next_year, next_month, 1, tzinfo=timezone.utc)
    return start, end_exclusive


async def _fetch_new_clients(
    session: AsyncSession,
    *,
    company_id: int,
    period_start: datetime,
    period_end: datetime,
) -> list[Client]:
    """Return clients qualifying as 'new' for the given period."""

    # Sub-query: clients who have ANY arrived record in the period.
    arrived_subq = (
        select(Record.client_id)
        .where(Record.company_id == company_id)
        .where(Record.client_id.is_not(None))
        .where(Record.starts_at >= period_start)
        .where(Record.starts_at < period_end)
        .where((Record.attendance == 1) | (Record.visit_attendance == 1))
        .distinct()
        .subquery()
    )

    # Sub-query: clients with MORE THAN 1 record in the period.
    multi_subq = (
        select(Record.client_id)
        .where(Record.company_id == company_id)
        .where(Record.client_id.is_not(None))
        .where(Record.starts_at >= period_start)
        .where(Record.starts_at < period_end)
        .group_by(Record.client_id)
        .having(func.count(Record.id) > 1)
        .subquery()
    )

    # Clients who HAVE at least 1 record in period…
    has_record_subq = (
        select(Record.client_id)
        .where(Record.company_id == company_id)
        .where(Record.client_id.is_not(None))
        .where(Record.starts_at >= period_start)
        .where(Record.starts_at < period_end)
        .distinct()
        .subquery()
    )

    stmt = (
        select(Client)
        .where(Client.company_id == company_id)
        .where(Client.wa_opted_out.is_(False))
        .where(Client.id.in_(select(has_record_subq.c.client_id)))
        .where(Client.id.not_in(select(arrived_subq.c.client_id)))
        .where(Client.id.not_in(select(multi_subq.c.client_id)))
        .order_by(Client.id.asc())
    )

    res = await session.execute(stmt)
    return list(res.scalars().all())


def _format_card_text(card_number: str) -> str:
    """Format a card number as displayed in the template."""
    return f"{_LOYALTY_CARD_PREFIX}{card_number}"


async def run_newsletter(
    *,
    company_id: int,
    location_id: int,
    card_type_id: str | None = None,
    dry_run: bool = False,
    ref_date: datetime | None = None,
) -> dict[str, Any]:
    """Main entry-point for the monthly new-client newsletter run.

    Returns a dict with ``sent``, ``skipped``, and ``errors`` counts.
    """
    period_start, period_end = _prev_month_range(ref_date)
    logger.info(
        "Newsletter run company=%s location=%s period=[%s, %s) dry_run=%s",
        company_id,
        location_id,
        period_start.date(),
        period_end.date(),
        dry_run,
    )

    loyalty = AltegioLoyaltyClient()
    try:
        # Resolve card_type_id if not provided.
        if card_type_id is None:
            card_types = await loyalty.get_card_types(location_id)
            if not card_types:
                raise RuntimeError(f"No loyalty card types found for location_id={location_id}")
            card_type_id = str(card_types[0].get("id") or card_types[0].get("loyalty_card_type_id"))
            logger.info("Using card_type_id=%s", card_type_id)

        sent = 0
        skipped = 0
        errors = 0

        async with SessionLocal() as session:
            clients = await _fetch_new_clients(
                session,
                company_id=company_id,
                period_start=period_start,
                period_end=period_end,
            )
            logger.info("Qualifying new clients: %d", len(clients))

        for client in clients:
            if not client.phone_e164:
                logger.warning("Skip client_id=%s: no phone", client.id)
                skipped += 1
                continue

            # Strip '+' to get numeric phone for Altegio API.
            phone_num = int(client.phone_e164.lstrip("+"))
            card_number_str = client.phone_e164.lstrip("+").zfill(16)
            loyalty_card_text = _format_card_text(card_number_str)

            if dry_run:
                logger.info(
                    "[dry_run] Would issue card + queue job for client_id=%s phone=%s card_text=%s",
                    client.id,
                    client.phone_e164,
                    loyalty_card_text,
                )
                sent += 1
                continue

            try:
                card = await loyalty.issue_card(
                    location_id,
                    loyalty_card_number=card_number_str,
                    loyalty_card_type_id=card_type_id,
                    phone=phone_num,
                )
                issued_number = str(card.get("loyalty_card_number") or card_number_str)
                loyalty_card_text = _format_card_text(issued_number)
                logger.info(
                    "Issued card client_id=%s card_number=%s",
                    client.id,
                    issued_number,
                )
            except Exception as exc:
                logger.error(
                    "Failed to issue card for client_id=%s: %s",
                    client.id,
                    exc,
                )
                errors += 1
                continue

            try:
                async with SessionLocal() as session:
                    async with session.begin():
                        await add_job(
                            session,
                            company_id=company_id,
                            record_id=None,
                            client_id=client.id,
                            job_type=NEWSLETTER_JOB_TYPE,
                            run_at=utcnow(),
                            payload={
                                "kind": NEWSLETTER_JOB_TYPE,
                                "loyalty_card_text": loyalty_card_text,
                            },
                        )
                sent += 1
                logger.info("Queued newsletter job client_id=%s", client.id)
            except Exception as exc:
                logger.error(
                    "Failed to queue job for client_id=%s: %s",
                    client.id,
                    exc,
                )
                errors += 1

    finally:
        await loyalty.aclose()

    summary = {"sent": sent, "skipped": skipped, "errors": errors}
    logger.info("Newsletter run complete: %s", summary)
    return summary


async def main() -> None:
    import argparse

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    parser = argparse.ArgumentParser(description="Run monthly newsletter for new clients.")
    parser.add_argument("--company-id", type=int, required=True)
    parser.add_argument("--location-id", type=int, required=True)
    parser.add_argument("--card-type-id", type=str, default=None)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Log actions without issuing cards or queuing jobs.",
    )
    args = parser.parse_args()

    await run_newsletter(
        company_id=args.company_id,
        location_id=args.location_id,
        card_type_id=args.card_type_id,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    asyncio.run(main())
