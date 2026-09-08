"""Provider-scoped campaign transport/readiness resolution (PR-13)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.easyweek_segment import SEGMENT_SOURCE
from altegio_bot.campaigns.provider import (
    CAMPAIGN_JOB_TYPES,
    CAMPAIGN_LIVE_GUARD_UNPROVEN,
    validate_campaign_provider,
)
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_policy import validate_static_booking_page
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    MessageTemplate,
)
from altegio_bot.whatsapp_routing import pick_sender_id_by_code

CAMPAIGN_LOCATION_UNPROVEN: Final = "campaign_location_unproven"
CAMPAIGN_BOOKING_PAGE_UNPROVEN: Final = "campaign_booking_page_unproven"
CAMPAIGN_SENDER_UNPROVEN: Final = "campaign_sender_unproven"
CAMPAIGN_TEMPLATE_UNPROVEN: Final = "campaign_template_unproven"


@dataclass(frozen=True)
class CampaignReadiness:
    provider: str
    company_id: int
    booking_page_url: str | None
    sender_id: int | None
    template_id: int | None
    meta_template_name: str | None
    language: str
    segment_source: str | None
    live_guard: str | None
    supported_job_types: tuple[str, ...]
    reasons: tuple[str, ...]

    @property
    def ready_for_send(self) -> bool:
        return not self.reasons


async def resolve_campaign_readiness(
    session: AsyncSession,
    *,
    provider: str,
    company_id: int,
    sender_code: str,
    template_code: str,
    language: str = "de",
) -> CampaignReadiness:
    """Resolve one campaign configuration without widening provider scope."""
    exact = validate_campaign_provider(provider)
    reasons: list[str] = []
    booking_page_url: str | None = None

    if exact == PROVIDER_EASYWEEK:
        registry = configured_easyweek_locations()
        location = registry.locations.get(company_id) if registry.ready else None
        if location is None:
            reasons.append(CAMPAIGN_LOCATION_UNPROVEN)
        else:
            booking_page_url = validate_static_booking_page(location.booking_page_url)
            if booking_page_url is None:
                reasons.append(CAMPAIGN_BOOKING_PAGE_UNPROVEN)
        # PR-14 proves only a local, incomplete subset and a partial per-booking
        # GET re-proof.  Neither is a customer-level live send guard.
        segment_source = SEGMENT_SOURCE
        live_guard = "easyweek_booking_partial_reproof"
        supported_job_types: tuple[str, ...] = ()
        reasons.append(CAMPAIGN_LIVE_GUARD_UNPROVEN)
    else:
        # Preserve the one existing source of Altegio campaign booking links.
        from altegio_bot.workers.outbox_worker import BOOKING_LINKS

        booking_page_url = BOOKING_LINKS.get(company_id)
        if not booking_page_url:
            reasons.append(CAMPAIGN_BOOKING_PAGE_UNPROVEN)
        segment_source = "altegio_crm"
        live_guard = "altegio_campaign_guard"
        supported_job_types = tuple(sorted(CAMPAIGN_JOB_TYPES))

    sender_id = await pick_sender_id_by_code(
        session,
        company_id,
        sender_code,
        provider=exact,
    )
    if sender_id is None:
        reasons.append(CAMPAIGN_SENDER_UNPROVEN)

    template = await session.scalar(
        select(MessageTemplate)
        .where(
            MessageTemplate.provider == exact,
            MessageTemplate.company_id == company_id,
            MessageTemplate.code == template_code,
            MessageTemplate.language == language,
            MessageTemplate.is_active.is_(True),
        )
        .order_by(MessageTemplate.id.asc())
        .limit(1)
    )
    if template is None or not template.meta_template_name:
        reasons.append(CAMPAIGN_TEMPLATE_UNPROVEN)

    return CampaignReadiness(
        provider=exact,
        company_id=company_id,
        booking_page_url=booking_page_url,
        sender_id=sender_id,
        template_id=template.id if template is not None else None,
        meta_template_name=template.meta_template_name if template is not None else None,
        language=language,
        segment_source=segment_source,
        live_guard=live_guard,
        supported_job_types=supported_job_types,
        reasons=tuple(dict.fromkeys(reasons)),
    )
