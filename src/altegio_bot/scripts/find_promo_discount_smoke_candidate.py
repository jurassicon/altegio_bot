"""Find a PromoLead + Record candidate for manual promo discount smoke testing.

Read-only: makes no Altegio API calls and performs no DB writes.

Usage (local):
    uv run python -m altegio_bot.scripts.find_promo_discount_smoke_candidate
    uv run python -m altegio_bot.scripts.find_promo_discount_smoke_candidate --company-id 1
    uv run python -m altegio_bot.scripts.find_promo_discount_smoke_candidate --phone +49...

Usage (Docker):
    docker compose exec -T altegio-api python -m altegio_bot.scripts.find_promo_discount_smoke_candidate

The script prints the dry-run command for smoke_apply_promo_discount.py.
The --yes-apply command is intentionally NOT printed: Record has no created_at,
so the helper cannot prove a booking was created after the promo lead.

It does not apply any discount and does not require PROMO_APPLY_DISCOUNT_ENABLED
or PROMO_APPLY_DISCOUNT_API_VERIFIED to be set.

Exit codes:
    0  — completed (candidates found or not)
    1  — unexpected error
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from sqlalchemy import select

from altegio_bot.models.models import Client, PromoLead, Record, RecordService
from altegio_bot.promo_discount_apply import get_promo_allowed_service_ids
from altegio_bot.settings import settings

if TYPE_CHECKING:
    from collections.abc import Callable

    from sqlalchemy.ext.asyncio import AsyncSession

_PAGE_SIZE_MIN = 30
_MAX_SCAN_MULTIPLIER = 20
_MAX_SCAN_ABS = 200


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class SmokeCandidate:
    lead: PromoLead
    record: Record
    services: list[RecordService] = field(default_factory=list)


async def find_smoke_candidates(
    session: AsyncSession,
    *,
    company_id: int | None = None,
    phone: str | None = None,
    campaign_name: str,
    now: datetime,
    limit: int = 10,
) -> list[SmokeCandidate]:
    """Return PromoLeads that have all IDs needed to smoke-test the apply endpoint.

    Filters applied to PromoLead:
    - campaign_name matches
    - status in ('issued', 'booked')
    - expires_at > now
    - loyalty_card_id, location_id, discount_program_id all not null
    - meta.loyalty_card_issued == 'true'
    - meta.promo_card_deleted_at is null (card not cleaned up yet)

    For each qualifying lead, the most recent non-deleted Record with a valid
    altegio_record_id is located via the Client phone. Leads without a matching
    Record are excluded.

    Uses a paginated scan with a hard cap of max(limit*20, 200) rows so that
    leads without matching Records near the top of the result set do not hide
    valid candidates further down.

    NOTE: Record has no created_at column. The record may predate the promo
    issuance. Callers must not construct --yes-apply commands without manual
    verification in Altegio.

    Read-only: no DB writes, no Altegio API calls.
    """
    page_size = max(limit * 3, _PAGE_SIZE_MIN)
    max_scanned = max(limit * _MAX_SCAN_MULTIPLIER, _MAX_SCAN_ABS)

    base_stmt = (
        select(PromoLead)
        .where(PromoLead.campaign_name == campaign_name)
        .where(PromoLead.status.in_(["issued", "booked"]))
        .where(PromoLead.expires_at > now)
        .where(PromoLead.loyalty_card_id.is_not(None))
        .where(PromoLead.location_id.is_not(None))
        .where(PromoLead.discount_program_id.is_not(None))
        .where(PromoLead.meta["loyalty_card_issued"].astext == "true")
        .where(PromoLead.meta["promo_card_deleted_at"].astext.is_(None))
        .order_by(PromoLead.created_at.desc())
    )

    if company_id is not None:
        base_stmt = base_stmt.where(PromoLead.company_id == company_id)
    if phone is not None:
        base_stmt = base_stmt.where(PromoLead.phone_e164 == phone)

    candidates: list[SmokeCandidate] = []
    offset = 0
    total_scanned = 0

    while len(candidates) < limit and total_scanned < max_scanned:
        leads = list((await session.execute(base_stmt.limit(page_size).offset(offset))).scalars().all())
        if not leads:
            break

        for lead in leads:
            if len(candidates) >= limit or total_scanned >= max_scanned:
                break
            total_scanned += 1

            record_stmt = (
                select(Record)
                .join(Client, Client.id == Record.client_id)
                .where(Client.phone_e164 == lead.phone_e164)
                .where(Client.company_id == lead.company_id)
                .where(Record.company_id == lead.company_id)
                .where(Record.is_deleted.is_(False))
                .where(Record.altegio_record_id.is_not(None))
                .order_by(Record.starts_at.desc())
                .limit(1)
            )
            record = (await session.execute(record_stmt)).scalar_one_or_none()
            if record is None:
                continue

            svc_stmt = select(RecordService).where(RecordService.record_id == record.id)
            services = list((await session.execute(svc_stmt)).scalars().all())

            candidates.append(SmokeCandidate(lead=lead, record=record, services=services))

        if len(leads) < page_size:
            break
        offset += len(leads)

    return candidates


def _dry_run_cmd(c: SmokeCandidate) -> str:
    return (
        "docker compose exec -T altegio-api"
        " python -m altegio_bot.scripts.smoke_apply_promo_discount"
        f" --location-id {c.lead.location_id}"
        f" --card-id {c.lead.loyalty_card_id}"
        f" --program-id {c.lead.discount_program_id}"
        f" --record-id {c.record.altegio_record_id}"
    )


def _print_candidate(c: SmokeCandidate, index: int) -> None:
    lead = c.lead
    record = c.record
    services_str = ", ".join(f"{s.service_id}:{s.title or '?'}" for s in c.services) if c.services else "(none)"
    starts_at_str = record.starts_at.isoformat() if record.starts_at else "(none)"

    allowed_ids = get_promo_allowed_service_ids()
    candidate_service_ids = {s.service_id for s in c.services}

    print(f"--- Candidate {index} ---")
    print(f"promo_lead_id={lead.id}")
    print(f"company_id={lead.company_id}")
    print(f"phone_e164={lead.phone_e164}")
    print(f"status={lead.status}")
    print(f"expires_at={lead.expires_at.isoformat()}")
    print(f"loyalty_card_id={lead.loyalty_card_id}")
    print(f"loyalty_card_number={lead.loyalty_card_number}")
    print(f"location_id={lead.location_id}")
    print(f"discount_program_id={lead.discount_program_id}")
    print(f"record_id={record.id}")
    print(f"altegio_record_id={record.altegio_record_id}")
    print(f"record_starts_at={starts_at_str}")
    print(f"services={services_str}")
    print(f"allowed_service_ids={sorted(allowed_ids) if allowed_ids else '(not_configured)'}")
    print(f"candidate_service_ids={sorted(candidate_service_ids) if candidate_service_ids else '(none)'}")

    if not allowed_ids:
        print("allowed_service_match=not_configured")
        print("WARNING: PROMO_ALLOWED_SERVICE_IDS is empty — automatic apply would skip this record.")
    elif candidate_service_ids & allowed_ids:
        print("allowed_service_match=yes")
    else:
        print("allowed_service_match=no")
        print("WARNING: This record has no services from PROMO_ALLOWED_SERVICE_IDS.")

    print()
    print("DRY-RUN COMMAND (no API call):")
    print(_dry_run_cmd(c))
    print()
    print("REAL APPLY COMMAND is intentionally not printed.")
    print()
    print(
        "Reason:\n"
        "Record.created_at is not available, so this helper cannot prove that the\n"
        "booking was created after the promo lead.\n"
        "\n"
        "Manually verify in Altegio that this booking was created after the promo\n"
        "lead was issued before running smoke_apply_promo_discount.py with the\n"
        "apply flag."
    )
    print()


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Find a PromoLead + Record pair suitable for manual smoke testing "
            "of the promo discount apply endpoint. "
            "Read-only: no API calls, no DB writes."
        )
    )
    parser.add_argument("--company-id", type=int, default=None, metavar="ID")
    parser.add_argument("--phone", type=str, default=None)
    parser.add_argument(
        "--campaign-name",
        type=str,
        default=None,
        help="Default: settings.promo_campaign_name",
    )
    parser.add_argument("--limit", type=int, default=10)
    return parser


async def _run(args: argparse.Namespace, *, session_factory: Callable | None = None) -> int:
    campaign_name = args.campaign_name or settings.promo_campaign_name

    if session_factory is None:
        from altegio_bot.db import SessionLocal

        session_factory = SessionLocal

    now = _utcnow()
    async with session_factory() as session:
        async with session.begin():
            candidates = await find_smoke_candidates(
                session,
                company_id=args.company_id,
                phone=args.phone,
                campaign_name=campaign_name,
                now=now,
                limit=args.limit,
            )

    if not candidates:
        print("No promo discount smoke candidates found.")
        return 0

    print(f"Found {len(candidates)} candidate(s).")
    print()
    for i, c in enumerate(candidates, start=1):
        _print_candidate(c, i)

    return 0


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()
    sys.exit(asyncio.run(_run(args)))


if __name__ == "__main__":
    main()
