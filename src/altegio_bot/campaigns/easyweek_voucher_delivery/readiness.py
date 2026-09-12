"""Can this canary deliver at all, and may it run at all (§36).

Answered before the voucher exists
----------------------------------
The delivery prerequisites run before CREATE, not merely before DELIVER.
Issuing and paying for a voucher that cannot legally be delivered would leave a
real €15 with only two exits — a refund, or an unhappy manual message — so the
question "is there an approved message, an active sender and a key?" is answered
while the answer is still free.

The refund is the exception, on purpose
---------------------------------------
A refund sends nothing to anybody. Asking it to prove an approved template, an
active sender or a usable MAC key would mean the cleanup path stops working at
exactly the moments it is most needed: a template paused by Meta, a sender
switched off, a key rotated between stages. Every one of those is a reason to
GET THE MONEY BACK, not a reason to leave it out there.

So the refund proves the fence, the ledger identity, the exact order and the
live guard — and deliberately not the things that only matter to a message it
will never send. The report says so in those words rather than reporting a
comfortable ``true`` for a check nobody ran.

A narrow readiness, and only a narrow one
-----------------------------------------
This model describes ONE recipient in ONE canary. It reports
``campaign_send_authorized=false``, ``bulk_delivery_authorized=false`` and
``global_ready_for_send=false`` in the same breath as any green result, and it
does not touch the global gift-card blockers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract
from altegio_bot.campaigns.easyweek_voucher_delivery.binding import binding_key_reason
from altegio_bot.campaigns.easyweek_voucher_delivery.identity import (
    CANARY_DISABLED,
    SENDER_UNPROVEN,
    STAGE_REFUND,
    TEMPLATE_UNPROVEN,
    VOUCHER_TEMPLATE_CODE,
)
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageTemplate, WhatsAppSender
from altegio_bot.settings import settings

# What a refund's report prints where a delivery report prints a boolean. Not
# ``false`` — that would read as "we checked and it failed" — and not ``true``,
# which would be a lie about a check nobody ran.
NOT_REQUIRED_FOR_REFUND: Final = "not_required_for_refund"


@dataclass(frozen=True)
class DeliveryPrerequisites:
    """The things that must be true before this stage may act."""

    stage: str
    fence_open: bool
    # ``None`` means proven; a string is the blocker. All three are ``None`` and
    # unchecked when ``delivery_checks_applied`` is false.
    key_reason: str | None = None
    template_reason: str | None = None
    sender_reason: str | None = None
    delivery_checks_applied: bool = True
    # Needed to send, never printed: the WhatsApp line this message would leave.
    sender_id: int | None = None
    phone_number_id: str | None = None
    meta_template_name: str | None = None
    template_language: str | None = None

    @property
    def reasons(self) -> tuple[str, ...]:
        found: list[str | None] = [None if self.fence_open else CANARY_DISABLED]
        if self.delivery_checks_applied:
            found.extend([self.key_reason, self.template_reason, self.sender_reason])
        return tuple(dict.fromkeys([reason for reason in found if reason is not None]))

    @property
    def ready(self) -> bool:
        return not self.reasons

    def as_safe_dict(self) -> dict[str, Any]:
        def applicable(reason: str | None) -> Any:
            return (reason is None) if self.delivery_checks_applied else NOT_REQUIRED_FOR_REFUND

        return {
            "stage": self.stage,
            "canary_fence_open": self.fence_open,
            "delivery_checks_applied": self.delivery_checks_applied,
            "hmac_key_usable": applicable(self.key_reason),
            "template_proven": applicable(self.template_reason),
            "sender_proven": applicable(self.sender_reason),
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
    stage: str,
    company_id: int,
    sender_code: str,
    enabled: bool | None = None,
) -> DeliveryPrerequisites:
    """The prerequisites THIS stage actually depends on.

    For create, pay and deliver: the fence, the key, the stored template row and
    the sender — all four. For refund: the fence alone, because a refund reaches
    no customer and must not be held hostage by the machinery of a message it
    will never send. The other checks are not merely skipped, they are not run,
    so a missing key or a deleted template row cannot raise on the way past.
    """
    fence_open = settings.easyweek_voucher_delivery_canary_enabled if enabled is None else enabled

    if stage == STAGE_REFUND:
        return DeliveryPrerequisites(
            stage=stage,
            fence_open=bool(fence_open),
            delivery_checks_applied=False,
        )

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
        stage=stage,
        fence_open=bool(fence_open),
        key_reason=key_reason,
        template_reason=template_reason,
        sender_reason=None if sender_ok else SENDER_UNPROVEN,
        delivery_checks_applied=True,
        sender_id=sender.id if sender_ok and sender is not None else None,
        phone_number_id=(sender.phone_number_id or "").strip() if sender_ok and sender is not None else None,
        meta_template_name=template_contract.VOUCHER_META_TEMPLATE_NAME,
        template_language=template_contract.VOUCHER_TEMPLATE_LANGUAGE,
    )


__all__ = ["NOT_REQUIRED_FOR_REFUND", "DeliveryPrerequisites", "prove_prerequisites"]
