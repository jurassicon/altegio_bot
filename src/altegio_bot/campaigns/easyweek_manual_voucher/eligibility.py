"""Proving that this exact manually selected person may be given this voucher.

What is proven, and what is honestly not
----------------------------------------
§36 proves an entitlement: a first visit happened, the booking that earned it is
still current, and the history behind it is complete. None of that applies here.
An operator looked at a preview, typed a phone number, and decided — and this
module must neither pretend that decision is a proven first visit nor treat the
missing proof as a defect.

So the first-visit contract is not run at all. It is reported as
``not_applicable``: never a quiet ``true``, and never a refusal either. What IS
proven, every time and from scratch, is the part that decides whether a real
€15 code reaches the right real phone:

* the run is a completed EasyWeek preview of the approved branch and campaign;
* the recipient belongs to that run, is still a candidate, and is on the manual
  basis — a typed column, not an inference;
* the stored EasyWeek customer UUID exists, and exactly one local EasyWeek
  ``Client`` stands behind the row;
* the number the preview recorded is still the number that client has;
* nobody has opted out;
* and, live, the EasyWeek customer with that exact UUID answers with that exact
  normalised number and a non-empty name — the last because the approved Meta
  template has a name parameter, and a message addressed to nobody is not one we
  send.

Run twice, not once
-------------------
The same proof runs before the first EasyWeek mutation and again immediately
before the Meta send. Between paying for a voucher and delivering it a customer
can opt out or change their number, and either turns a gift into an unwelcome
message. The second proof is what the database means by
``live_guard_reproven_at``, and a send whose guard is older than its payment is
refused by a CHECK constraint.

Every refusal is one of §37.2's closed reason codes, and a refusal means zero
external writes.
"""

from __future__ import annotations

import uuid as uuid_module
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.easyweek_eligibility import BookingReader
from altegio_bot.campaigns.easyweek_manual_voucher.identity import (
    CUSTOMER_IDENTITY_NOT_CURRENT,
    CUSTOMER_NAME_MISSING,
    CUSTOMER_PHONE_NOT_CURRENT,
    CUSTOMER_UUID_MISSING,
    KARLSRUHE_COMPANY_ID,
    LIVE_GUARD_UNCERTAIN,
    LOCAL_CLIENT_UNPROVEN,
    NEW_CLIENT_CAMPAIGN_CODE,
    RECIPIENT_BASIS_UNSUPPORTED,
    RECIPIENT_NOT_CANDIDATE,
    RECIPIENT_OPTED_OUT,
    RECIPIENT_UNPROVEN,
    RUN_UNPROVEN,
)
from altegio_bot.easyweek_migration.customer_api import read_customer_card
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_MANUAL,
    CampaignRecipient,
    CampaignRun,
    Client,
)
from altegio_bot.webhooks.common import normalize_phone_candidate

# The three answers the report may give about a first visit. "Not applicable" is
# a first-class answer here, not a polite way of saying no: this basis is a
# person's decision, and there is no visit for the contract to be about.
FIRST_VISIT_NOT_APPLICABLE = "not_applicable"


@dataclass(frozen=True)
class ManualRecipientProof:
    """One manually selected recipient, re-proven live.

    Booleans, reason codes and the two values an action needs. The values are
    held in memory for one request and are never printed, stored or hashed.
    """

    proven: bool
    reasons: tuple[str, ...] = ()
    proven_at: datetime | None = None
    # Needed to act, never printed.
    easyweek_customer_uuid: str | None = None
    company_id: int | None = None
    campaign_run_id: int | None = None
    campaign_recipient_id: int | None = None
    # The campaign wave this selection belongs to: half of the entitlement key.
    campaign_period_start: datetime | None = None
    campaign_period_end: datetime | None = None
    # The addressee and the name the template needs, held for one send.
    destination_phone: str | None = None
    client_display_name: str | None = None
    # Per-check booleans, safe to print.
    checks: dict[str, bool] | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        # Deliberately without ``proven_at``: this dict is signed into the stage
        # digest, which must rebuild identically seconds later. WHEN the proof
        # was taken is already signed, as the plan's own ``plan_issued_at``.
        return {
            "recipient_proven": self.proven,
            "reasons": list(self.reasons),
            # Stated on every answer, including refusals. A report that hid it
            # would let a manual selection read like an earned entitlement.
            "recipient_basis": RECIPIENT_BASIS_MANUAL,
            "first_visit_proof": FIRST_VISIT_NOT_APPLICABLE,
            "company_id": self.company_id,
            "campaign_run_id": self.campaign_run_id,
            "campaign_recipient_id": self.campaign_recipient_id,
            "checks": dict(self.checks or {}),
            # Presence, never the value.
            "destination_recorded": self.destination_phone is not None,
            "customer_name_recorded": bool(self.client_display_name),
            "easyweek_customer_recorded": self.easyweek_customer_uuid is not None,
        }


def _canonical(value: object) -> str | None:
    if value is None:
        return None
    try:
        return str(uuid_module.UUID(str(value)))
    except (ValueError, AttributeError, TypeError):
        return None


async def prove_manual_recipient(
    session: AsyncSession,
    *,
    preview_run_id: int,
    campaign_recipient_id: int,
    client_reader: BookingReader,
    now: datetime,
) -> ManualRecipientProof:
    """Re-prove one exact manually selected recipient, live.

    The order is deliberate: everything answerable from the database is answered
    before a single EasyWeek request is made, so a mistyped id or a foreign run
    costs nothing and proves nothing.
    """
    checks: dict[str, bool] = {}

    run = await session.get(CampaignRun, preview_run_id)
    if (
        run is None
        or run.provider != PROVIDER_EASYWEEK
        or run.mode != "preview"
        or run.status != "completed"
        or run.campaign_code != NEW_CLIENT_CAMPAIGN_CODE
        or KARLSRUHE_COMPANY_ID not in (run.company_ids or [])
        or run.period_start is None
        or run.period_end is None
    ):
        checks["run_proven"] = False
        return ManualRecipientProof(False, (RUN_UNPROVEN,), checks=checks)
    checks["run_proven"] = True

    recipient = await session.get(CampaignRecipient, campaign_recipient_id)
    if (
        recipient is None
        or recipient.provider != PROVIDER_EASYWEEK
        or recipient.campaign_run_id != run.id
        or recipient.company_id != KARLSRUHE_COMPANY_ID
    ):
        checks["recipient_belongs_to_run"] = False
        return ManualRecipientProof(False, (RECIPIENT_UNPROVEN,), checks=checks)
    checks["recipient_belongs_to_run"] = True

    # The basis is a typed column, and this canary serves exactly one value of
    # it. An earned or test recipient belongs to §36 and is refused here for the
    # same reason a manual one is refused there: the two contracts prove
    # different things and must not borrow each other's ledgers.
    if (recipient.recipient_basis or "") != RECIPIENT_BASIS_MANUAL:
        checks["recipient_basis_supported"] = False
        return ManualRecipientProof(False, (RECIPIENT_BASIS_UNSUPPORTED,), checks=checks)
    checks["recipient_basis_supported"] = True

    if recipient.status != "candidate":
        checks["recipient_is_candidate"] = False
        return ManualRecipientProof(False, (RECIPIENT_NOT_CANDIDATE,), checks=checks)
    checks["recipient_is_candidate"] = True

    customer_uuid = _canonical(recipient.easyweek_customer_uuid)
    if customer_uuid is None:
        # §37.1 stores this when an operator adds somebody by hand, and a CHECK
        # ties it to the manual basis in both directions. Missing here means the
        # row is not one this canary can address at all.
        checks["customer_uuid_recorded"] = False
        return ManualRecipientProof(False, (CUSTOMER_UUID_MISSING,), checks=checks)
    checks["customer_uuid_recorded"] = True

    if recipient.is_opted_out:
        checks["not_opted_out"] = False
        return ManualRecipientProof(False, (RECIPIENT_OPTED_OUT,), checks=checks)

    # Exactly one local EasyWeek client for this branch and this number. Counted
    # rather than fetched by id alone: two rows for one number is an ambiguity
    # about who we would be messaging, not a detail to resolve by picking one.
    if recipient.client_id is None:
        checks["local_client_proven"] = False
        return ManualRecipientProof(False, (LOCAL_CLIENT_UNPROVEN,), checks=checks)
    client = await session.get(Client, recipient.client_id)
    snapshot_phone = normalize_phone_candidate(recipient.phone_e164)
    if (
        client is None
        or client.provider != PROVIDER_EASYWEEK
        or client.company_id != KARLSRUHE_COMPANY_ID
        or snapshot_phone is None
    ):
        checks["local_client_proven"] = False
        return ManualRecipientProof(False, (LOCAL_CLIENT_UNPROVEN,), checks=checks)
    duplicates = await session.scalar(
        select(func.count())
        .select_from(Client)
        .where(
            Client.provider == PROVIDER_EASYWEEK,
            Client.company_id == KARLSRUHE_COMPANY_ID,
            Client.phone_e164 == client.phone_e164,
        )
    )
    if int(duplicates or 0) != 1:
        checks["local_client_proven"] = False
        return ManualRecipientProof(False, (LOCAL_CLIENT_UNPROVEN,), checks=checks)
    checks["local_client_proven"] = True

    # The addressee is the number the preview recorded AND the number the client
    # has now. A number that changed in between belongs to a different phone,
    # whoever answers it.
    destination = normalize_phone_candidate(client.phone_e164)
    if not destination or destination != snapshot_phone:
        checks["destination_current"] = False
        return ManualRecipientProof(False, (CUSTOMER_PHONE_NOT_CURRENT,), checks=checks)
    checks["destination_current"] = True

    if client.wa_opted_out is not False:
        checks["not_opted_out"] = False
        return ManualRecipientProof(False, (RECIPIENT_OPTED_OUT,), checks=checks)
    checks["not_opted_out"] = True

    # -- and only now, the live read -----------------------------------------
    # Every failure mode of this call — 404, auth, timeout, 429, 5xx, malformed
    # body, a phone that does not match — is the same answer: we do not know who
    # this is, so nothing external happens.
    try:
        payload = await client_reader.get_customer(customer_uuid)
    except Exception:  # noqa: BLE001 - every read failure is one refusal
        checks["customer_live_proven"] = False
        return ManualRecipientProof(False, (LIVE_GUARD_UNCERTAIN,), checks=checks)

    try:
        card = read_customer_card(payload, expected_phone=destination)
    except Exception:  # noqa: BLE001 - an unreadable card proves nothing
        checks["customer_live_proven"] = False
        return ManualRecipientProof(False, (CUSTOMER_IDENTITY_NOT_CURRENT,), checks=checks)

    if card.uuid != customer_uuid:
        # A 200 is not proof the row belongs to the customer we asked about.
        checks["customer_live_proven"] = False
        return ManualRecipientProof(False, (CUSTOMER_IDENTITY_NOT_CURRENT,), checks=checks)
    if card.phone != destination:
        checks["customer_live_proven"] = False
        return ManualRecipientProof(False, (CUSTOMER_PHONE_NOT_CURRENT,), checks=checks)
    display_name = (card.first_name or "").strip()
    if not display_name:
        # The approved template's first parameter is the customer's name. There
        # is no safe placeholder for it: a voucher addressed to nobody is a
        # message we do not send.
        checks["customer_live_proven"] = False
        return ManualRecipientProof(False, (CUSTOMER_NAME_MISSING,), checks=checks)
    checks["customer_live_proven"] = True

    return ManualRecipientProof(
        proven=True,
        proven_at=now,
        easyweek_customer_uuid=customer_uuid,
        company_id=KARLSRUHE_COMPANY_ID,
        campaign_run_id=run.id,
        campaign_recipient_id=recipient.id,
        campaign_period_start=run.period_start,
        campaign_period_end=run.period_end,
        destination_phone=destination,
        client_display_name=display_name,
        checks=checks,
    )


__all__ = [
    "FIRST_VISIT_NOT_APPLICABLE",
    "ManualRecipientProof",
    "prove_manual_recipient",
]
