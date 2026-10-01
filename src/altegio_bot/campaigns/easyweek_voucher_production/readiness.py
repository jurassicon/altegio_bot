"""May this mailing act at all, and could it deliver what it is about to buy (§42).

Answered before the first voucher exists
----------------------------------------
The delivery prerequisites run before the first CREATE, not merely before
DELIVER. Issuing and paying for a list of vouchers that cannot legally be
delivered would leave real money with only two exits — a refund per slot, or a
manual message per slot — and the larger the list the worse that trade gets. So
"is there an approved message, an active sender, a key and the two EasyWeek
identities this sale needs?" is answered while the answer is still free.

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

Its own fence
-------------
:data:`~altegio_bot.settings.Settings.easyweek_voucher_production_mailing_enabled`
is this phase's own fence, false by default. Opening §36, §37.2 or §41 does not
open this one and opening this one does not reopen any of them.

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

from altegio_bot.campaigns.easyweek_voucher_delivery import template_contract
from altegio_bot.campaigns.easyweek_voucher_delivery.binding import binding_key_reason
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    ACCOUNT_UNCONFIGURED,
    APPROVAL_ARITHMETIC,
    BOOKING_LINK_UNPROVEN,
    ISSUER_MEMBERSHIP_INCOMPLETE,
    PRODUCTION_DISABLED,
    SENDER_UNPROVEN,
    STAGE_REFUND,
    TEMPLATE_UNPROVEN,
    UNIT_PRICE_MINOR,
    VOUCHER_TEMPLATE_CODE,
)
from altegio_bot.campaigns.easyweek_voucher_production.issuer import (
    IssuerMembership,
    PinnedIssuer,
    pinned_issuer,
)
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageTemplate, WhatsAppSender
from altegio_bot.settings import settings

# What a refund's report prints where a delivery report prints a boolean. Not
# ``false`` — that would read as "we checked and it failed" — and not ``true``,
# which would be a lie about a check nobody ran.
NOT_REQUIRED_FOR_REFUND: Final = "not_required_for_refund"


def _canonical(value: str | None) -> str | None:
    """A configured UUID, or None if it is absent or not one.

    An unset variable and a typo are the same answer here: this phase refuses
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
class ProductionPrerequisites:
    """The things that must be true before this stage may act."""

    stage: str
    fence_open: bool
    # ``None`` means proven; a string is the blocker.
    staffer_reason: str | None = None
    account_reason: str | None = None
    # §43.9. The approved issuer, and whether they still provably belong to this
    # location. ``None`` membership means the question was not asked, which is
    # the refund case and never a silent pass.
    issuer: PinnedIssuer | None = None
    issuer_membership: IssuerMembership | None = None
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
            None if self.fence_open else PRODUCTION_DISABLED,
            self.account_reason,
        ]
        if self.delivery_checks_applied:
            found.extend(
                [
                    self.staffer_reason,
                    # The pin and the live membership, in that order: "this is
                    # not the approved staffer" is a more useful answer than
                    # "some staffer could not be found in the catalogue".
                    self.issuer.reason if self.issuer is not None else None,
                    self.issuer_membership.reason if self.issuer_membership is not None else None,
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
            "production_fence_open": self.fence_open,
            "delivery_checks_applied": self.delivery_checks_applied,
            # The account is proven on every stage, refund included: it is the
            # account the money comes back to.
            "payment_account_configured": self.account_reason is None,
            "staffer_configured": applicable(self.staffer_reason),
            # §43.9, as booleans. The display name is for a human to recognise;
            # nothing in this phase resolves a staffer by it.
            **(
                {
                    **(self.issuer.as_safe_dict() if self.issuer is not None else {}),
                    **(self.issuer_membership.as_safe_dict() if self.issuer_membership is not None else {}),
                }
                if self.delivery_checks_applied
                else {
                    "issuer_pinned": NOT_REQUIRED_FOR_REFUND,
                    "issuer_membership_proven": NOT_REQUIRED_FOR_REFUND,
                }
            ),
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
            # campaign permission. There is no recipient ceiling in this phase;
            # what bounds the money is the arithmetic the operator approved.
            "voucher_unit_price_minor": UNIT_PRICE_MINOR,
            "approval_arithmetic": APPROVAL_ARITHMETIC,
            "reasons": list(self.reasons),
        }


async def prove_prerequisites(
    session: AsyncSession,
    *,
    stage: str,
    company_id: int,
    sender_code: str,
    enabled: bool | None = None,
    issuer_membership: IssuerMembership | None = None,
    require_membership: bool = True,
) -> ProductionPrerequisites:
    """The prerequisites THIS stage actually depends on.

    For freeze, create, pay and deliver: the fence, both EasyWeek identities,
    the key, the stored template row, the sender and the booking link. For
    refund: the fence and the payment account alone, because a refund reaches no
    customer and must not be held hostage by the machinery of a message it will
    never send. The other checks are not merely skipped — they are not run, so a
    missing key or a deleted template row cannot raise on the way past.
    """
    fence_open = settings.easyweek_voucher_production_mailing_enabled if enabled is None else enabled
    account_uuid = _canonical(settings.easyweek_voucher_production_mailing_account_uuid)
    account_reason = None if account_uuid else ACCOUNT_UNCONFIGURED

    if stage == STAGE_REFUND:
        return ProductionPrerequisites(
            stage=stage,
            fence_open=bool(fence_open),
            account_reason=account_reason,
            payment_account_uuid=account_uuid,
            delivery_checks_applied=False,
        )

    # §43.9: not "is this a UUID" but "is this THE approved issuer". A valid
    # UUID of one of the other seven people at this branch fails here, which is
    # the whole reason the pin exists.
    #
    # ``issuer_membership`` is proven by the caller, which is the only place that
    # holds a read-only EasyWeek client, and is asked once per stage plan rather
    # than once per recipient. ``None`` means the walk was not run; the plan
    # supplies it for every acting stage, so a missing proof shows up as an
    # unproven membership rather than as a pass.
    issuer = pinned_issuer(settings.easyweek_voucher_production_mailing_staffer_uuid)
    staffer_uuid = issuer.uuid
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
        # reading a §42 report should never see a §36 reason code.
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

    return ProductionPrerequisites(
        stage=stage,
        fence_open=bool(fence_open),
        # The pin answers both "configured?" and "the approved one?", so there is
        # no separate staffer reason left to report: a second code derived from
        # the same value could only ever disagree with the first.
        staffer_reason=None,
        issuer=issuer,
        # Three states, not two, and the difference is load bearing.
        #
        # A caller that WILL act must have asked: a plan with no membership proof
        # is an unproven membership, never a pass, which is why ``None`` becomes
        # `incomplete` here. A caller that is only RENDERING A PAGE has not asked
        # and must not be made to — the proof costs a live EasyWeek walk, and a
        # readiness panel that reported `incomplete` on every page load would
        # show every operator a blocker that is not one.
        #
        # So `require_membership=False` leaves it ``None``, which contributes no
        # reason and no field, and the page says the check happens when a step is
        # prepared.
        issuer_membership=(
            issuer_membership
            if issuer_membership is not None
            else (IssuerMembership(reason=ISSUER_MEMBERSHIP_INCOMPLETE) if require_membership else None)
        ),
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


__all__ = ["NOT_REQUIRED_FOR_REFUND", "ProductionPrerequisites", "prove_prerequisites"]
