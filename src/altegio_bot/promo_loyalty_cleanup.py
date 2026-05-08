"""Cleanup of expired promo loyalty cards via Altegio API.

Selects PromoLead rows that are eligible for card deletion:
  - expires_at <= now (UTC)
  - status in ('issued', 'booked', 'applied')
  - loyalty_card_id IS NOT NULL
  - location_id IS NOT NULL
  - meta.loyalty_card_issued == True   (only promo-flow cards)
  - meta.promo_card_deleted_at absent  (idempotency guard)

Per-row behaviour
-----------------
Success:
  lead.status          → 'expired'
  meta.promo_card_deleted_at         = now.isoformat()
  meta.promo_card_delete_attempted_at = now.isoformat()
  meta.promo_card_delete_result       = 'deleted'
  meta.promo_card_delete_error        = None

Failure (delete_card raised):
  status unchanged
  meta.promo_card_delete_attempted_at = now.isoformat()
  meta.promo_card_delete_error        = str(exc)
  meta.promo_card_delete_result       = 'failed'
  (retryable on next run)

Out of scope
------------
- status='used'    — deletion policy TBD
- status='cancelled' — TODO: separate decision
- Non-promo cards  — only rows with meta.loyalty_card_issued=True are touched
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.altegio_loyalty import AltegioLoyaltyClient
from altegio_bot.models.models import PromoLead

logger = logging.getLogger(__name__)

_CLEANUP_STATUSES = ("issued", "booked", "applied")


@dataclass
class PromoLoyaltyCleanupResult:
    found: int = field(default=0)
    deleted: int = field(default=0)
    failed: int = field(default=0)
    skipped: int = field(default=0)


async def cleanup_expired_promo_loyalty_cards(
    session: AsyncSession,
    *,
    now: datetime | None = None,
    limit: int = 100,
) -> PromoLoyaltyCleanupResult:
    """Delete Altegio loyalty cards for expired promo leads.

    Idempotent: rows with meta.promo_card_deleted_at already set are skipped.
    Only processes rows created by the promo funnel (meta.loyalty_card_issued=True).

    The caller is responsible for committing the session after this returns.

    Returns a PromoLoyaltyCleanupResult summary.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    stmt = (
        select(PromoLead)
        .where(
            PromoLead.expires_at <= now,
            PromoLead.status.in_(_CLEANUP_STATUSES),
            PromoLead.loyalty_card_id.is_not(None),
            PromoLead.location_id.is_not(None),
        )
        .order_by(PromoLead.expires_at)
        .limit(limit)
    )
    rows = (await session.execute(stmt)).scalars().all()

    result = PromoLoyaltyCleanupResult()
    loyalty = AltegioLoyaltyClient()
    try:
        for lead in rows:
            result.found += 1
            meta = lead.meta or {}

            if meta.get("loyalty_card_issued") is not True:
                logger.debug(
                    "promo_cleanup: skip lead_id=%d — loyalty_card_issued not True",
                    lead.id,
                )
                result.skipped += 1
                continue

            if meta.get("promo_card_deleted_at") is not None:
                logger.debug(
                    "promo_cleanup: skip lead_id=%d — already deleted at %s",
                    lead.id,
                    meta["promo_card_deleted_at"],
                )
                result.skipped += 1
                continue

            try:
                card_id = int(lead.loyalty_card_id)  # type: ignore[arg-type]
            except (ValueError, TypeError) as exc:
                err = f"invalid loyalty_card_id {lead.loyalty_card_id!r}: {exc}"
                lead.meta = {
                    **meta,
                    "promo_card_delete_attempted_at": now.isoformat(),
                    "promo_card_delete_error": err,
                    "promo_card_delete_result": "failed",
                }
                result.failed += 1
                logger.warning("promo_cleanup: lead_id=%d %s", lead.id, err)
                continue

            try:
                await loyalty.delete_card(lead.location_id, card_id)  # type: ignore[arg-type]
                lead.status = "expired"
                lead.meta = {
                    **meta,
                    "promo_card_deleted_at": now.isoformat(),
                    "promo_card_delete_attempted_at": now.isoformat(),
                    "promo_card_delete_result": "deleted",
                    "promo_card_delete_error": None,
                }
                result.deleted += 1
                logger.info(
                    "promo_cleanup: deleted card_id=%s location=%d lead_id=%d",
                    lead.loyalty_card_id,
                    lead.location_id,
                    lead.id,
                )
            except Exception as exc:
                lead.meta = {
                    **meta,
                    "promo_card_delete_attempted_at": now.isoformat(),
                    "promo_card_delete_error": str(exc),
                    "promo_card_delete_result": "failed",
                }
                result.failed += 1
                logger.warning(
                    "promo_cleanup: delete_card failed lead_id=%d card_id=%s: %s",
                    lead.id,
                    lead.loyalty_card_id,
                    exc,
                )
    finally:
        await loyalty.aclose()

    return result
