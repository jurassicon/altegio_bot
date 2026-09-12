"""Can this canary deliver at all, and may it run at all (§36).

Answered before the voucher exists
----------------------------------
Every check here runs before CREATE, not merely before DELIVER. Issuing and
paying for a voucher that cannot legally be delivered would leave a real €15
with only two exits — a refund, or an unhappy manual message — so the question
"is there an approved message, an active sender, a key and a fence?" is answered
while the answer is still free.

A narrow readiness, and only a narrow one
-----------------------------------------
This model describes ONE recipient in ONE canary. It deliberately reports
``campaign_send_authorized=false``, ``bulk_delivery_authorized=false`` and
``global_ready_for_send=false`` in the same breath as any green result, because
a proven canary is a research outcome and never a campaign permission. It also
does not touch the global gift-card blockers: this canary sends a proven code to
one proven person, which says nothing about public sale, expiry, redemption
rules or anybody else.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract
from altegio_bot.campaigns.easyweek_voucher_delivery.binding import binding_key_reason
from altegio_bot.campaigns.easyweek_voucher_delivery.identity import (
    CANARY_DISABLED,
    SENDER_UNPROVEN,
    TEMPLATE_UNPROVEN,
    VOUCHER_TEMPLATE_CODE,
)
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageTemplate, WhatsAppSender
from altegio_bot.settings import settings


@dataclass(frozen=True)
class DeliveryPrerequisites:
    """The things that must be true before a voucher is worth creating."""

    fence_open: bool
    key_reason: str | None
    template_reason: str | None
    sender_reason: str | None
    # Needed to send, never printed: the WhatsApp line this message would leave.
    sender_id: int | None = None
    phone_number_id: str | None = None
    meta_template_name: str | None = None
    template_language: str | None = None

    @property
    def reasons(self) -> tuple[str, ...]:
        found = [
            None if self.fence_open else CANARY_DISABLED,
            self.key_reason,
            self.template_reason,
            self.sender_reason,
        ]
        return tuple(dict.fromkeys([reason for reason in found if reason is not None]))

    @property
    def ready(self) -> bool:
        return not self.reasons

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "canary_fence_open": self.fence_open,
            "hmac_key_usable": self.key_reason is None,
            "template_proven": self.template_reason is None,
            "sender_proven": self.sender_reason is None,
            "template_code": VOUCHER_TEMPLATE_CODE,
            "meta_template_name": self.meta_template_name,
            "template_language": self.template_language,
            # Presence only: a phone-number id identifies a real line.
            "sender_recorded": self.sender_id is not None,
            "reasons": list(self.reasons),
        }


async def prove_prerequisites(
    session: AsyncSession,
    *,
    company_id: int,
    sender_code: str,
    enabled: bool | None = None,
) -> DeliveryPrerequisites:
    """Fence, key, stored template and sender — all four, or a refusal.

    The stored template row is the DB-first contract the send path resolves
    through. It is proven against the source-owned contract here; whether META
    approved that text is proven separately by the reconciler, which is the only
    thing allowed to write this row.
    """
    fence_open = settings.easyweek_voucher_delivery_canary_enabled if enabled is None else enabled
    key_reason = binding_key_reason()

    rows = list(
        (
            await session.execute(
                select(MessageTemplate)
                .where(MessageTemplate.provider == PROVIDER_EASYWEEK)
                .where(MessageTemplate.company_id == company_id)
                .where(MessageTemplate.code == VOUCHER_TEMPLATE_CODE)
                .where(MessageTemplate.language == template_contract.VOUCHER_TEMPLATE_LANGUAGE)
            )
        )
        .scalars()
        .all()
    )
    active = [row for row in rows if row.is_active]
    if len(active) != 1:
        # Zero is nothing to send with; two is an ambiguity about which text a
        # customer would receive.
        template_reason: str | None = TEMPLATE_UNPROVEN
    else:
        template_reason = template_contract.db_row_blocker(active[0], company_id=company_id)

    sender = (
        await session.execute(
            select(WhatsAppSender)
            .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
            .where(WhatsAppSender.company_id == company_id)
            .where(WhatsAppSender.sender_code == sender_code)
        )
    ).scalar_one_or_none()
    sender_ok = sender is not None and sender.is_active and bool((sender.phone_number_id or "").strip())

    return DeliveryPrerequisites(
        fence_open=bool(fence_open),
        key_reason=key_reason,
        template_reason=template_reason,
        sender_reason=None if sender_ok else SENDER_UNPROVEN,
        sender_id=sender.id if sender_ok and sender is not None else None,
        phone_number_id=(sender.phone_number_id or "").strip() if sender_ok and sender is not None else None,
        meta_template_name=template_contract.VOUCHER_META_TEMPLATE_NAME,
        template_language=template_contract.VOUCHER_TEMPLATE_LANGUAGE,
    )


__all__ = ["DeliveryPrerequisites", "prove_prerequisites"]
