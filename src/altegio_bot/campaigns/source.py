"""Explicit source-CRM dispatch for campaigns.

This module intentionally has no module-level import of ``segment`` or
``altegio_crm``.  An EasyWeek request is refused before Python loads the Altegio
implementation, which makes the CRM boundary observable in tests and review.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

from altegio_bot.campaigns.provider import require_campaign_execution_provider
from altegio_bot.models.models import PROVIDER_ALTEGIO

if TYPE_CHECKING:
    from altegio_bot.campaigns.segment import ClientCandidate


async def find_candidates_for_provider(
    *,
    provider: str,
    company_id: int,
    period_start: datetime,
    period_end: datetime,
) -> list[ClientCandidate]:
    """Dispatch candidate discovery by explicit provider."""
    exact = require_campaign_execution_provider(provider)
    if exact == PROVIDER_ALTEGIO:
        from altegio_bot.campaigns.segment import find_candidates

        return await find_candidates(
            company_id=company_id,
            period_start=period_start,
            period_end=period_end,
        )
    raise AssertionError("unreachable campaign provider")
