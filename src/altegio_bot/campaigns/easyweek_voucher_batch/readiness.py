"""May this batch act at all, and could it deliver what it is about to buy (§41).

Answered before any voucher exists
----------------------------------
The delivery prerequisites run before the first CREATE, not merely before
DELIVER. Issuing and paying for five vouchers that cannot legally be delivered
would leave €75 with only two exits — five refunds, or five unhappy manual
messages — so "is there an approved message, an active sender, a key and the two
EasyWeek identities this sale needs?" is answered while the answer is still free.

The refund is the exception, on purpose
---------------------------------------
A refund sends nothing to anybody. Asking it to prove an approved template, an
active sender or a usable MAC key would mean the cleanup path stops working at
exactly the moments it is most needed: a template paused by Meta, a sender
switched off, a key rotated between stages. Every one of those is a reason to
GET THE MONEY BACK, not a reason to leave it out there.

The freeze is the other exception
---------------------------------
Freezing writes a composition and sends nothing. It still proves the whole
delivery surface, and deliberately so: the entire point of freezing before
buying is to find out, while it is still free, that the batch could not have
been delivered.

A narrow readiness, and only a narrow one
-----------------------------------------
Every report here repeats ``campaign_send_authorized=false``,
``bulk_delivery_authorized=false`` and ``global_ready_for_send=false`` in the
same breath as any green result, and touches none of the global gift-card
blockers.
"""

from __future__ import annotations

import uuid as uuid_module
from dataclasses import dataclass
from typing import Any, Final

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.easyweek_voucher_batch.identity import (
    ACCOUNT_UNCONFIGURED,
    BATCH_DISABLED,
    BOOKING_LINK_UNPROVEN,
    MAX_EXPOSURE_MINOR,
    MAX_RECIPIENTS,
    SENDER_UNPROVEN,
    STAFFER_UNCONFIGURED,
    STAGE_REFUND,
    TEMPLATE_UNPROVEN,
    UNIT_PRICE_MINOR,
    VOUCHER_TEMPLATE_CODE,
)
from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract
from altegio_bot.campaigns.easyweek_voucher_delivery.binding import binding_key_reason
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageTemplate, WhatsAppSender
from altegio_bot.settings import settings

# What a refund's report prints where a delivery report prints a boolean. Not
# ``false`` — that would read as "we checked and it failed" — and not ``true``,
# which would be a lie about a check nobody ran.
NOT_REQUIRED_FOR_REFUND: Final = "not_required_for_refund"


def _canonical(value: str | None) -> str | None:
    """A configured UUID, or None if it is absent or not one.

    An unset variable and a typo are the same answer here: this batch refuses
    rather than guessing which staffer sold or which account is charged.
    """
    text = (value or "").strip()
    if not text:
        return None
    try:
        return str(uuid_module.UUID(text))
    except ValueError:
        return None


@dataclass(frozen=True)
class BatchPrerequisites:
    """The things that must be true before this stage may act."""

    stage: str
    fence_open: bool
    # ``None`` means proven; a string is the blocker.
    staffer_reason: str | None = None
    account_reason: str | None = None
    key_reason: str | None = None
    template_reason: str | None = None
    sender_reason: str | None = None
    booking_link_reason: str | None = None
    delivery_checks_applied: bool = True
    # Needed to act, never printed.
    staffer_uuid: str | None = None
    payment_account_uuid: str | None = None
    sender_id: int | None = None
    phone_number_id: str | None = None
    meta_template_name: str | None = None
    template_language: str | None = None
    # The third template parameter. Server-resolved from the reviewed registry;
    # a browser never supplies it and it is never defaulted to an empty string.
    booking_link: str | None = None

    @property
    def reasons(self) -> tuple[str, ...]:
        found: list[str | None] = [
            None if self.fence_open else BATCH_DISABLED,
            self.account_reason,
        ]
        if self.delivery_checks_applied:
            found.extend(
                [
                    self.staffer_reason,
                    self.key_reason,
                    self.template_reason,
                    self.sender_reason,
                    self.booking_link_reason,
                ]
            )
        return tuple(dict.fromkeys([reason for reason in found if reason is not None]))

    @property
    def ready(self) -> bool:
        return not self.reasons

    def as_safe_dict(self) -> dict[str, Any]:
        def applicable(reason: str | None) -> Any:
            return (reason is None) if self.delivery_checks_applied else NOT_REQUIRED_FOR_REFUND

        return {
            "stage": self.stage,
            "batch_fence_open": self.fence_open,
            "delivery_checks_applied": self.delivery_checks_applied,
            # The account is proven on every stage, refund included: it is the
            # account the money comes back to.
            "payment_account_configured": self.account_reason is None,
            "staffer_configured": applicable(self.staffer_reason),
            "hmac_key_usable": applicable(self.key_reason),
            "template_proven": applicable(self.template_reason),
            "sender_proven": applicable(self.sender_reason),
            # Presence only: the link is a real public URL, and a report is
            # pasted into tickets.
            "booking_link_proven": applicable(self.booking_link_reason),
            "template_parameter_count": template_contract.VOUCHER_TEMPLATE_ARITY,
            "template_code": VOUCHER_TEMPLATE_CODE,
            "meta_template_name": self.meta_template_name,
            "template_language": self.template_language,
            # Presence only: a phone-number id identifies a real line.
            "sender_recorded": self.sender_id is not None,
            # Repeated on every report so a green stage can never read as a
            # raised ceiling.
            "max_recipients": MAX_RECIPIENTS,
            "voucher_unit_price_minor": UNIT_PRICE_MINOR,
            "max_exposure_minor": MAX_EXPOSURE_MINOR,
            "reasons": list(self.reasons),
        }


async def prove_prerequisites(
    session: AsyncSession,
    *,
    stage: str,
    company_id: int,
    sender_code: str,
    enabled: bool | None = None,
) -> BatchPrerequisites:
    """The prerequisites THIS stage actually depends on.

    For freeze, create, pay and deliver: the fence, both EasyWeek identities,
    the key, the stored template row, the sender and the booking link. For
    refund: the fence and the payment account alone, because a refund reaches no
    customer and must not be held hostage by the machinery of a message it will
    never send. The other checks are not merely skipped — they are not run, so a
    missing key or a deleted template row cannot raise on the way past.
    """
    fence_open = settings.easyweek_voucher_snapshot_batch_enabled if enabled is None else enabled
    account_uuid = _canonical(settings.easyweek_voucher_snapshot_batch_account_uuid)
    account_reason = None if account_uuid else ACCOUNT_UNCONFIGURED

    if stage == STAGE_REFUND:
        return BatchPrerequisites(
            stage=stage,
            fence_open=bool(fence_open),
            account_reason=account_reason,
            payment_account_uuid=account_uuid,
            delivery_checks_applied=False,
        )

    staffer_uuid = _canonical(settings.easyweek_voucher_snapshot_batch_staffer_uuid)
    key_reason = binding_key_reason()

    # The third parameter of the approved template. It comes from the same
    # server-side registry the preview branch came from — never from a request,
    # never a constant in this module, and never an empty string standing in for
    # a link that is not configured.
    registry = configured_easyweek_locations()
    location = registry.locations.get(company_id) if registry.ready else None
    booking_link = (location.booking_page_url or "").strip() if location is not None else ""
    booking_link_reason = None if booking_link.startswith("https://") else BOOKING_LINK_UNPROVEN

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
        # The shared contract answers with §36's vocabulary. This phase has its
        # own closed one, so the answer is mapped rather than echoed: an operator
        # reading a §41 report should never see a §36 reason code.
        template_reason = (
            TEMPLATE_UNPROVEN
            if template_contract.db_row_blocker(active[0], company_id=company_id) is not None
            else None
        )

    sender = (
        await session.execute(
            select(WhatsAppSender)
            .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
            .where(WhatsAppSender.company_id == company_id)
            .where(WhatsAppSender.sender_code == sender_code)
        )
    ).scalar_one_or_none()
    sender_ok = sender is not None and sender.is_active and bool((sender.phone_number_id or "").strip())

    return BatchPrerequisites(
        stage=stage,
        fence_open=bool(fence_open),
        staffer_reason=None if staffer_uuid else STAFFER_UNCONFIGURED,
        account_reason=account_reason,
        key_reason=key_reason,
        template_reason=template_reason,
        sender_reason=None if sender_ok else SENDER_UNPROVEN,
        booking_link_reason=booking_link_reason,
        booking_link=booking_link or None,
        delivery_checks_applied=True,
        staffer_uuid=staffer_uuid,
        payment_account_uuid=account_uuid,
        sender_id=sender.id if sender_ok and sender is not None else None,
        phone_number_id=(sender.phone_number_id or "").strip() if sender_ok and sender is not None else None,
        meta_template_name=template_contract.VOUCHER_META_TEMPLATE_NAME,
        template_language=template_contract.VOUCHER_TEMPLATE_LANGUAGE,
    )


__all__ = ["NOT_REQUIRED_FOR_REFUND", "BatchPrerequisites", "prove_prerequisites"]
