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
)
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.models.models import PROVIDER_EASYWEEK, CampaignRecipient, CampaignRun, Client
from altegio_bot.settings import settings
from altegio_bot.webhooks.common import normalize_phone_candidate


@dataclass(frozen=True)
class RecipientProof:
    """One recipient, re-proven live. Booleans and reason codes only."""

    proven: bool
    reasons: tuple[str, ...]
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


async def prove_recipient(
    session: AsyncSession,
    *,
    preview_run_id: int,
    campaign_recipient_id: int,
    expected_company_id: int,
    client_reader: BookingReader,
    now: datetime,
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
        and recipient.source_booking_uuid is not None
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
        proven_at=now,
        easyweek_customer_uuid=customer_uuid,
        source_booking_uuid=str(booking_uuid),
        company_id=recipient.company_id,
        destination_phone=destination,
        client_display_name=(client.display_name or recipient.display_name or "").strip() or None,
        checks=checks,
    )


__all__ = ["RecipientProof", "prove_recipient"]
