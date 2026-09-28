"""Is this EasyWeek preview frozen by a voucher canary, batch or mailing?

Four things can attach themselves to a preview run: the §36 delivery canary,
which acts on an earned or test recipient; the §37.2 manual-basis canary, which
acts on somebody an operator chose by hand; the §41 snapshot batch, which acts
on up to five of them at once; and the §42 production mailing, which acts on
however many an operator curated. They keep separate ledgers on purpose — the
identities behind them are not the same kind of thing — but the consequence of
any of them attaching is identical, and it is the reason this one question has
one answer.

Each of them addresses its recipients by run id and recipient id, and re-proves
them live before every external step. An operator who edits or discards the
preview after CREATE or PAY therefore undoes nothing: they make the DELIVER and
the REFUND unprovable, leaving real €15 orders with no way to finish them and no
way to take them back. The larger the frozen list, the more that costs.

Asked in the UI as a courtesy; enforced here, under the caller's transaction, so
the check happens under the same lock as the edit it guards.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.easyweek_manual_voucher.ledger import preview_is_locked_by_manual_canary
from altegio_bot.campaigns.easyweek_voucher_batch.ledger import preview_is_locked_by_voucher_batch
from altegio_bot.campaigns.easyweek_voucher_delivery.ledger import preview_is_locked_by_canary
from altegio_bot.campaigns.easyweek_voucher_production.ledger import preview_is_locked_by_voucher_production


async def preview_is_locked_by_any_canary(session: AsyncSession, *, campaign_run_id: int) -> bool:
    """True if ANY of the four has attached itself to this preview run.

    Asked in this order for no reason other than the order the phases were
    built in: the ledgers are separate tables and a preview belongs to at most
    one of them, so the first ``True`` is the answer.
    """
    if await preview_is_locked_by_canary(session, campaign_run_id=campaign_run_id):
        return True
    if await preview_is_locked_by_manual_canary(session, campaign_run_id=campaign_run_id):
        return True
    if await preview_is_locked_by_voucher_batch(session, campaign_run_id=campaign_run_id):
        return True
    return await preview_is_locked_by_voucher_production(session, campaign_run_id=campaign_run_id)


__all__ = ["preview_is_locked_by_any_canary"]
