"""Proving that this exact person may still be given this voucher (§36).

Run twice, not once
-------------------
The same proof runs before the first EasyWeek mutation and again immediately
before the Meta send. That is not belt-and-braces: between paying for a voucher
and delivering it, the customer can book again, opt out, change their number, or
have the source booking cancelled — and every one of those turns a welcome gift
into an unwelcome message. The second proof is what the database means by
``live_guard_reproven_at``, and a send whose guard is older than its payment is
refused by a CHECK constraint.

Reused, not reimplemented
-------------------------
The eligibility contract itself is §33/§34 code: ``evaluate_recipient`` reads
the durable proof, re-normalises the source event, re-reads the booking and
walks the customer's whole history. This module adds only what a DELIVERY needs
on top of it: that the run and the recipient are the exact ones the operator
named, that the campaign is the one the entitlement belongs to, that the
addressee is the number the preview recorded, and which EasyWeek customer the
voucher must be issued to.

Nothing here decides anything by itself. Every refusal is one of §36's closed
reason codes, and a refusal means zero external calls.
"""

from __future__ import annotations

import uuid as uuid_module
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.easyweek_eligibility import BookingReader, evaluate_recipient
from altegio_bot.campaigns.easyweek_voucher_delivery.identity import (
    ACTIVE_FUTURE_BOOKING_PRESENT,
    CUSTOMER_HISTORY_INCOMPLETE,
    CUSTOMER_IDENTITY_NOT_CURRENT,
    FIRST_VISIT_NOT_CURRENT,
    LIVE_GUARD_UNCERTAIN,
    NEW_CLIENT_CAMPAIGN_CODE,
    RECIPIENT_IDENTITY_UNPROVEN,
    SOURCE_BOOKING_NOT_CURRENT,
    TEST_BINDING_MISMATCH,
    TEST_CUSTOMER_UNCONFIGURED,
    TEST_CUSTOMER_UNPROVEN,
    TEST_RECIPIENT_DISABLED,
)
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_migration.customer_api import read_customer_card
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    VOUCHER_DELIVERY_BASIS_EARNED,
    VOUCHER_DELIVERY_BASIS_TEST,
    CampaignRecipient,
    CampaignRun,
    Client,
)
from altegio_bot.settings import settings
from altegio_bot.webhooks.common import normalize_phone_candidate


@dataclass(frozen=True)
class RecipientProof:
    """One recipient, re-proven live. Booleans and reason codes only."""

    proven: bool
    reasons: tuple[str, ...]
    # Which contract proved this: an earned first visit, or the owner's approved
    # test account. Printed, because a report that hides it would let a test run
    # read exactly like an entitlement somebody earned.
    recipient_basis: str = VOUCHER_DELIVERY_BASIS_EARNED
    proven_at: datetime | None = None
    # Needed to act, never printed: the CREATE request and the ledger binding.
    easyweek_customer_uuid: str | None = None
    source_booking_uuid: str | None = None
    company_id: int | None = None
    # The addressee, held in memory for one send and never stored by this canary.
    destination_phone: str | None = None
    client_display_name: str | None = None
    # Per-check booleans, safe to print.
    checks: dict[str, bool] | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        # Deliberately without ``proven_at``. This dict is signed into the stage
        # digest, and the digest has to be reproducible when the apply command
        # rebuilds the same plan seconds later. WHEN the proof was taken is
        # already recorded — and already signed — as the plan's own
        # ``plan_issued_at``; repeating it here would make every plan hash
        # differently from itself.
        return {
            "recipient_proven": self.proven,
            "reasons": list(self.reasons),
            "recipient_basis": self.recipient_basis,
            "company_id": self.company_id,
            "checks": dict(self.checks or {}),
            # Presence, never the value.
            "destination_recorded": self.destination_phone is not None,
            "easyweek_customer_recorded": self.easyweek_customer_uuid is not None,
        }


def _canonical(value: object) -> str | None:
    if isinstance(value, uuid_module.UUID):
        return str(value)
    if not isinstance(value, str) or not value:
        return None
    try:
        return str(uuid_module.UUID(value))
    except (ValueError, AttributeError, TypeError):
        return None


def _booking_customer_uuid(payload: object, *, expected_booking_uuid: uuid_module.UUID) -> str | None:
    """The customer this booking names, or ``None``.

    A minimal, strict projection on top of an already-proven booking: the body
    must be the booking we asked for, and it must carry a canonical customer
    uuid. The heavy proof — location, cancellation, service count, whole
    history — has already happened in ``evaluate_recipient``; this only answers
    *which customer the voucher belongs to*, and refuses to guess.
    """
    if not isinstance(payload, dict):
        return None
    inner = payload.get("data")
    booking = inner if isinstance(inner, dict) else payload
    if _canonical(booking.get("uuid")) != str(expected_booking_uuid):
        return None
    customer = booking.get("customer")
    if not isinstance(customer, dict):
        return None
    return _canonical(customer.get("uuid"))


@dataclass(frozen=True)
class TestCustomerProof:
    """The one configured test account, read live and compared, or a refusal."""

    proven: bool
    reason: str | None = None
    # Needed to act, never printed.
    customer_uuid: str | None = None
    phone: str | None = None
    first_name: str | None = None


def configured_test_customer_uuid() -> str | None:
    """The canonical UUID of the approved test account, or ``None``.

    Read from the server's own configuration and nowhere else. The Ops UI sends
    a phone number; a screen that could name any customer UUID would be a way to
    point a real €15 voucher at any real person.
    """
    return _canonical((settings.easyweek_voucher_delivery_test_customer_uuid or "").strip())


def test_recipient_fences_reason(*, enabled: bool | None = None) -> str | None:
    """Why the test-recipient path is closed, or ``None`` if it is open.

    TWO fences, both of which must be open. The canary fence says a canary may
    run at all; this one says the owner's test account may stand in for an
    earned entitlement. Neither implies the other, and turning on the canary
    must never be what turns on the substitution.
    """
    canary_open = settings.easyweek_voucher_delivery_canary_enabled if enabled is None else enabled
    if not canary_open or not settings.easyweek_voucher_delivery_test_recipient_enabled:
        return TEST_RECIPIENT_DISABLED
    if configured_test_customer_uuid() is None:
        # Empty, or something that is not a UUID. Either way there is no account
        # to prove, and guessing one is not an option.
        return TEST_CUSTOMER_UNCONFIGURED
    return None


async def prove_test_customer(
    client_reader: BookingReader,
    *,
    expected_phone: str,
    enabled: bool | None = None,
) -> TestCustomerProof:
    """Read the ONE configured customer and prove it is still that customer.

    Read-only, and strict in both directions: the answer must carry the exact
    UUID we asked about, and the number on that card must be the number the
    operator typed. A 200 is not proof that the row belongs to this account, and
    a card whose number has moved on belongs to a different phone, whoever
    answers it.

    Every failure — 404, auth, timeout, 429, 5xx, a malformed body, a mismatch —
    lands on the same fail-closed answer with a stable reason. None of them are
    "probably fine".
    """
    fence_reason = test_recipient_fences_reason(enabled=enabled)
    if fence_reason is not None:
        return TestCustomerProof(False, fence_reason)
    configured = configured_test_customer_uuid()
    assert configured is not None  # the fence check proved it

    try:
        payload = await client_reader.get_customer(configured)
        card = read_customer_card(payload, expected_phone=expected_phone)
    except Exception:  # noqa: BLE001 - every read failure is the same refusal
        return TestCustomerProof(False, TEST_CUSTOMER_UNPROVEN)

    if card.uuid != configured or card.phone != expected_phone:
        return TestCustomerProof(False, TEST_CUSTOMER_UNPROVEN)
    return TestCustomerProof(
        proven=True,
        customer_uuid=card.uuid,
        phone=card.phone,
        first_name=card.first_name,
    )


async def prove_recipient(
    session: AsyncSession,
    *,
    preview_run_id: int,
    campaign_recipient_id: int,
    expected_company_id: int,
    client_reader: BookingReader,
    now: datetime,
    enabled: bool | None = None,
) -> RecipientProof:
    """Re-prove one exact recipient, live, from durable evidence outwards.

    The order is deliberate: everything answerable locally is answered before a
    single EasyWeek request is made, so a mistyped id or a foreign run costs
    nothing and proves nothing.
    """
    checks: dict[str, bool] = {}

    run = await session.get(CampaignRun, preview_run_id)
    recipient = await session.get(CampaignRecipient, campaign_recipient_id)
    if run is None or recipient is None:
        return RecipientProof(False, (RECIPIENT_IDENTITY_UNPROVEN,), checks=checks)

    # Which contract this recipient is under is decided by the durable typed
    # binding, not by a flag a caller passes in: the column's presence IS the
    # basis, and a CHECK constraint keeps it from coexisting with earned proof.
    test_binding = _canonical(recipient.easyweek_test_customer_uuid)
    is_test = recipient.easyweek_test_customer_uuid is not None

    # -- the run, the recipient and their relationship ----------------------
    identity_ok = (
        run.provider == PROVIDER_EASYWEEK
        and recipient.provider == PROVIDER_EASYWEEK
        and run.mode == "preview"
        and run.status == "completed"
        and run.campaign_code == NEW_CLIENT_CAMPAIGN_CODE
        and recipient.campaign_run_id == run.id
        and recipient.company_id == expected_company_id
        and expected_company_id in (run.company_ids or [])
        and recipient.status == "candidate"
        and not recipient.is_opted_out
        # An earned row names the visit it was earned by; a test row names none.
        # Reading the column as "optional" in both directions is what would let
        # one basis quietly borrow the other's evidence.
        and (recipient.source_booking_uuid is None if is_test else recipient.source_booking_uuid is not None)
        and (test_binding is not None if is_test else True)
        and recipient.client_id is not None
    )
    checks["recipient_identity"] = identity_ok
    if not identity_ok:
        return RecipientProof(False, (RECIPIENT_IDENTITY_UNPROVEN,), checks=checks)

    client = await session.get(Client, recipient.client_id)
    if client is None or client.provider != PROVIDER_EASYWEEK:
        checks["recipient_identity"] = False
        return RecipientProof(False, (RECIPIENT_IDENTITY_UNPROVEN,), checks=checks)

    # The addressee is the number the preview recorded AND the number the client
    # has now. A number that changed between preview and send belongs to a
    # different phone, whoever answers it.
    destination = normalize_phone_candidate(client.phone_e164)
    snapshot_phone = normalize_phone_candidate(recipient.phone_e164)
    phone_ok = bool(destination) and destination == snapshot_phone and client.wa_opted_out is False
    checks["destination_current"] = phone_ok
    if not phone_ok:
        return RecipientProof(False, (RECIPIENT_IDENTITY_UNPROVEN,), checks=checks)

    if is_test:
        # The owner's test account. Its history is not evidence of anything —
        # it cannot be cleared, which is the whole reason this basis exists — so
        # the first-visit contract is NOT run here and NOT reported as passed.
        # What is proven instead is that this is still the exact configured
        # account, reachable at the exact number the preview recorded, with both
        # fences still open. That proof is re-taken on every stage, including
        # the one immediately before the Meta send.
        checks["first_visit_proof_applicable"] = False
        assert test_binding is not None  # identity_ok proved it
        proof = await prove_test_customer(client_reader, enabled=enabled, expected_phone=destination)
        checks["test_customer_proven"] = proof.proven
        if not proof.proven:
            return RecipientProof(
                False,
                (proof.reason or TEST_CUSTOMER_UNPROVEN,),
                recipient_basis=VOUCHER_DELIVERY_BASIS_TEST,
                checks=checks,
            )
        # A configured UUID that no longer matches the binding this recipient
        # was added under is a rotation, not a match. Refusing here is what
        # makes a rotated environment stop the canary instead of quietly
        # pointing it at a different account.
        if proof.customer_uuid != test_binding:
            checks["test_binding_current"] = False
            return RecipientProof(
                False,
                (TEST_BINDING_MISMATCH,),
                recipient_basis=VOUCHER_DELIVERY_BASIS_TEST,
                checks=checks,
            )
        checks["test_binding_current"] = True
        return RecipientProof(
            proven=True,
            reasons=(),
            recipient_basis=VOUCHER_DELIVERY_BASIS_TEST,
            proven_at=now,
            easyweek_customer_uuid=proof.customer_uuid,
            # No booking, deliberately. There is no visit to name, and naming
            # one would be the lie this whole basis exists to avoid.
            source_booking_uuid=None,
            company_id=recipient.company_id,
            destination_phone=destination,
            client_display_name=(
                (client.display_name or recipient.display_name or proof.first_name or "").strip() or None
            ),
            checks=checks,
        )

    # -- the live guard, unchanged from the campaign contract ---------------
    registry = configured_easyweek_locations()
    result = await evaluate_recipient(
        session,
        run=run,
        recipient=recipient,
        registry=registry,
        allowed_categories_raw=settings.easyweek_allowed_service_categories,
        client_reader=client_reader,
        now=now,
    )
    checks["local_eligible"] = result.local_eligible
    checks["source_booking_current"] = result.source_booking_current
    checks["customer_identity_current"] = result.customer_identity_current
    checks["history_complete"] = result.history_complete
    checks["first_visit_current"] = result.first_visit_current
    checks["no_active_future_booking"] = result.no_active_future_booking
    checks["live_guard_ready"] = result.live_guard_ready

    reasons: list[str] = []
    if result.retryable_uncertainty:
        # Uncertainty is not permission to proceed and not proof of refusal.
        reasons.append(LIVE_GUARD_UNCERTAIN)
    if not result.local_eligible:
        reasons.append(RECIPIENT_IDENTITY_UNPROVEN)
    if not result.source_booking_current:
        reasons.append(SOURCE_BOOKING_NOT_CURRENT)
    if not result.customer_identity_current:
        reasons.append(CUSTOMER_IDENTITY_NOT_CURRENT)
    if not result.history_complete:
        reasons.append(CUSTOMER_HISTORY_INCOMPLETE)
    if not result.first_visit_current:
        reasons.append(FIRST_VISIT_NOT_CURRENT)
    if not result.no_active_future_booking:
        reasons.append(ACTIVE_FUTURE_BOOKING_PRESENT)
    if not result.live_guard_ready and not reasons:
        reasons.append(LIVE_GUARD_UNCERTAIN)
    if reasons:
        return RecipientProof(False, tuple(dict.fromkeys(reasons)), checks=checks)

    # -- which EasyWeek customer the voucher belongs to ---------------------
    booking_uuid = recipient.source_booking_uuid
    assert booking_uuid is not None
    try:
        payload = await client_reader.get_booking(str(booking_uuid))
    except Exception:  # noqa: BLE001 - a read failure is uncertainty, not a verdict
        checks["customer_uuid_proven"] = False
        return RecipientProof(False, (LIVE_GUARD_UNCERTAIN,), checks=checks)

    customer_uuid = _booking_customer_uuid(payload, expected_booking_uuid=booking_uuid)
    checks["customer_uuid_proven"] = customer_uuid is not None
    if customer_uuid is None:
        return RecipientProof(False, (CUSTOMER_IDENTITY_NOT_CURRENT,), checks=checks)

    return RecipientProof(
        proven=True,
        reasons=(),
        recipient_basis=VOUCHER_DELIVERY_BASIS_EARNED,
        proven_at=now,
        easyweek_customer_uuid=customer_uuid,
        source_booking_uuid=str(booking_uuid),
        company_id=recipient.company_id,
        destination_phone=destination,
        client_display_name=(client.display_name or recipient.display_name or "").strip() or None,
        checks=checks,
    )


__all__ = [
    "RecipientProof",
    "TestCustomerProof",
    "configured_test_customer_uuid",
    "prove_recipient",
    "prove_test_customer",
    "test_recipient_fences_reason",
]
