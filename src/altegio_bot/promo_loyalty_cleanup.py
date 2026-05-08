"""Cleanup of expired promo loyalty cards via Altegio API.

Selects PromoLead rows that are eligible for card deletion:
  - status == 'issued'  (leads that expired without ever booking)
  - expires_at <= now (UTC)
  - loyalty_card_id IS NOT NULL
  - location_id IS NOT NULL
  - meta->>'loyalty_card_issued' == 'true'   (only promo-flow cards)
  - meta->>'promo_card_deleted_at' IS NULL   (idempotency guard)

All guards are applied in SQL so that `limit` reliably caps actionable rows
and already-processed rows cannot permanently crowd them out.

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
- status='booked'    — client already has a booking; deletion policy TBD
- status='applied'   — discount already linked to a booking; deletion policy TBD
- status='used'      — deletion policy TBD
- status='cancelled' — TODO: separate decision
- Non-promo cards    — only rows with meta.loyalty_card_issued=True are touched
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
    """Delete Altegio loyalty cards for expired promo leads with status='issued'.

    Only touches leads that expired without a booking (status='issued').
    Leads with status 'booked', 'applied', or 'used' are intentionally excluded —
    their card deletion policy is defined separately.

    All eligibility guards (including JSONB meta checks) are applied in SQL so
    that `limit` reliably caps actionable rows and already-processed rows cannot
    permanently crowd them out.

    Idempotent: rows with meta->>'promo_card_deleted_at' already set are excluded
    by the query. Failed rows (result='failed') are retried on the next run because
    promo_card_deleted_at is only written on success.

    The caller is responsible for committing the session after this returns.

    Returns a PromoLoyaltyCleanupResult summary.
    """
    if now is None:
        now = datetime.now(timezone.utc)

    stmt = (
        select(PromoLead)
        .where(
            PromoLead.status == "issued",
            PromoLead.expires_at <= now,
            PromoLead.loyalty_card_id.is_not(None),
            PromoLead.location_id.is_not(None),
            PromoLead.meta["loyalty_card_issued"].astext == "true",
            PromoLead.meta["promo_card_deleted_at"].astext.is_(None),
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
