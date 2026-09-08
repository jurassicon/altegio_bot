"""Read-only PR-14 re-proof for one EasyWeek campaign preview snapshot."""

from __future__ import annotations

import argparse
import asyncio
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Final

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.easyweek_eligibility import BookingReader, evaluate_recipient
from altegio_bot.db import SessionLocal
from altegio_bot.easyweek_client import EasyWeekClient
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.models.models import PROVIDER_EASYWEEK, CampaignRecipient, CampaignRun
from altegio_bot.settings import settings
from altegio_bot.utils import utcnow

DEFAULT_LIMIT: Final = 100
DEFAULT_PAUSE_SEC: Final = 1.0


@dataclass
class CampaignPreflightReport:
    run_id: int
    total_recipient_count: int = 0
    candidate_count: int = 0
    checked_count: int = 0
    local_eligible_count: int = 0
    source_booking_current_count: int = 0
    send_ready_count: int = 0
    retryable_uncertainty_count: int = 0
    truncated: bool = False
    reasons: Counter[str] = field(default_factory=Counter)

    @property
    def ready_for_send(self) -> bool:
        return False

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "mode": "read-only",
            "run_id": self.run_id,
            "total_recipient_count": self.total_recipient_count,
            "candidate_count": self.candidate_count,
            "checked_count": self.checked_count,
            "local_eligible_count": self.local_eligible_count,
            "source_booking_current_count": self.source_booking_current_count,
            "send_ready_count": self.send_ready_count,
            "retryable_uncertainty_count": self.retryable_uncertainty_count,
            "truncated": self.truncated,
            "reasons": dict(sorted(self.reasons.items())),
            "ready_for_send": False,
        }


async def run_preflight(
    session: AsyncSession,
    *,
    run_id: int,
    client: BookingReader,
    limit: int = DEFAULT_LIMIT,
    pause_sec: float = DEFAULT_PAUSE_SEC,
    sleep: Any = None,
) -> CampaignPreflightReport:
    """Select and evaluate without flushing, committing or mutating ORM rows."""
    if limit < 1:
        raise ValueError("limit_must_be_positive")
    run = await session.get(CampaignRun, run_id)
    if run is None or run.provider != PROVIDER_EASYWEEK or run.mode != "preview" or run.status != "completed":
        raise ValueError("easyweek_campaign_preview_unavailable")

    total = int(
        await session.scalar(
            select(func.count())
            .select_from(CampaignRecipient)
            .where(CampaignRecipient.campaign_run_id == run_id)
            .where(CampaignRecipient.provider == PROVIDER_EASYWEEK)
        )
        or 0
    )
    candidate_total = int(
        await session.scalar(
            select(func.count())
            .select_from(CampaignRecipient)
            .where(CampaignRecipient.campaign_run_id == run_id)
            .where(CampaignRecipient.provider == PROVIDER_EASYWEEK)
            .where(CampaignRecipient.status == "candidate")
        )
        or 0
    )
    stmt = (
        select(CampaignRecipient)
        .where(CampaignRecipient.campaign_run_id == run_id)
        .where(CampaignRecipient.provider == PROVIDER_EASYWEEK)
        .where(CampaignRecipient.status == "candidate")
        .order_by(CampaignRecipient.id.asc())
        .limit(limit + 1)
    )
    rows = list((await session.execute(stmt)).scalars().all())
    truncated = candidate_total > limit
    rows = rows[:limit]
    report = CampaignPreflightReport(
        run_id=run_id,
        total_recipient_count=total,
        candidate_count=candidate_total,
        truncated=truncated,
    )
    registry = configured_easyweek_locations()
    pause = sleep if sleep is not None else asyncio.sleep

    for index, recipient in enumerate(rows):
        if index:
            await pause(pause_sec)
        result = await evaluate_recipient(
            session,
            run=run,
            recipient=recipient,
            registry=registry,
            allowed_categories_raw=settings.easyweek_allowed_service_categories,
            client_reader=client,
            now=utcnow(),
        )
        report.checked_count += 1
        report.local_eligible_count += int(result.local_eligible)
        report.source_booking_current_count += int(result.source_booking_current)
        report.send_ready_count += int(result.send_ready)
        report.retryable_uncertainty_count += int(result.retryable_uncertainty)
        report.reasons.update(result.reasons)
    return report


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Read-only EasyWeek campaign preview preflight")
    parser.add_argument("preview_run_id", type=int)
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    parser.add_argument("--pause-sec", type=float, default=DEFAULT_PAUSE_SEC)
    args = parser.parse_args(argv)
    if args.preview_run_id < 1:
        parser.error("preview_run_id must be positive")
    if args.limit < 1:
        parser.error("--limit must be at least 1")
    if args.pause_sec < 0:
        parser.error("--pause-sec must not be negative")
    return args


async def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    client = EasyWeekClient()
    try:
        async with SessionLocal() as session:
            report = await run_preflight(
                session,
                run_id=args.preview_run_id,
                client=client,
                limit=args.limit,
                pause_sec=args.pause_sec,
            )
    finally:
        await client.aclose()
    print(report.as_safe_dict())
    # PR-14 is diagnostic only.  A successful inspection is still not a send
    # permission, so the command deliberately remains non-zero.
    return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
