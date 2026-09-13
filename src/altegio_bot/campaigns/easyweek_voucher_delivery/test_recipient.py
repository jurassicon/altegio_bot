"""Adding the ONE owner-approved test account to a preview snapshot (§36.11).

Why this exists at all
----------------------
The delivery canary has to send a real message to a real phone. The owner's test
account cannot pass the first-visit proof — its history cannot be cleared, and
creating a fresh account for every attempt is not a workable way to test — so
the owner approved that one pre-configured account may stand in for an earned
recipient, for this canary and nothing else.

That is a test identity, not an entitlement. Nothing here says the account
earned a voucher, and the row it writes is marked as exactly what it is.

What the operator may choose, and what they may not
---------------------------------------------------
They may choose to add the test recipient, and they type the phone number so
that a mistake is caught rather than assumed. They may NOT choose WHICH account:
the customer UUID comes from the server's own configuration, never from the
request. An Ops screen able to name any customer UUID would be a way to point a
real €15 voucher at any real person.

Everything is proven before anything is written
-----------------------------------------------
Both fences, the configured UUID, the run, the branch, the local client, and a
read-only live GET of that exact customer whose UUID and phone number must both
match. The live read happens BEFORE the write transaction opens, so a slow or
failing EasyWeek never holds a lock on a campaign run. Then the run is locked,
every fact is re-checked under that lock, the row is written or reactivated, and
the snapshot counters are recounted in the same transaction. A failure anywhere
leaves the snapshot exactly as it was.

No refusal here carries a phone number, a name or a customer UUID. Every one of
them is a stable code an operator can look up and a wrapper can branch on.
"""

from __future__ import annotations

import uuid as uuid_module
from dataclasses import dataclass
from typing import Any, Final

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_eligibility import BookingReader
from altegio_bot.campaigns.easyweek_voucher_delivery.eligibility import (
    configured_test_customer_uuid,
    prove_test_customer,
    test_recipient_fences_reason,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.identity import NEW_CLIENT_CAMPAIGN_CODE
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_voucher_identity import KARLSRUHE_LOCATION_UUID
from altegio_bot.models.models import PROVIDER_EASYWEEK, CampaignRecipient, CampaignRun, Client
from altegio_bot.utils import utcnow
from altegio_bot.webhooks.common import normalize_phone_candidate

# -- refusals ---------------------------------------------------------------
# Stable, PII-free, and specific enough to act on. "It did not work" is not an
# operator instruction; "this preview is locked by the canary" is.
PHONE_UNUSABLE: Final = "test_recipient_phone_unusable"
RUN_NOT_EDITABLE: Final = "test_recipient_run_not_editable"
RUN_NOT_SUPPORTED: Final = "test_recipient_run_not_supported"
CLIENT_UNRESOLVED: Final = "test_recipient_client_unresolved"
CLIENT_OPTED_OUT: Final = "test_recipient_client_opted_out"
NAME_MISSING: Final = "test_recipient_display_name_missing"
AMBIGUOUS_ROWS: Final = "test_recipient_rows_ambiguous"
LOCKED_BY_CANARY: Final = "test_recipient_preview_locked_by_canary"
CLIENT_ID_NOT_ACCEPTED: Final = "test_recipient_altegio_client_id_not_accepted"

ACTION_CREATED: Final = "created"
ACTION_REACTIVATED: Final = "reactivated"
ACTION_UNCHANGED: Final = "unchanged"


@dataclass(frozen=True)
class TestRecipientOutcome:
    """What happened, in terms an operator screen can render verbatim."""

    ok: bool
    reason: str | None = None
    recipient_id: int | None = None
    action: str | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "reason": self.reason,
            "recipient_id": self.recipient_id,
            "action": self.action,
            "recipient_basis": "owner_test_account",
            # Repeated on every answer, success included: this adds a row to a
            # preview and authorises nothing.
            "campaign_send_authorized": False,
            "bulk_delivery_authorized": False,
            "global_ready_for_send": False,
        }


def _supported_branch(company_id: int) -> bool:
    """Is this the one branch §36 is pinned to?

    The canary's location, staffer, payment account and voucher template are all
    constants in this repository. A preview from another branch cannot be the
    one this canary acts on, and adding a test recipient to it would only create
    a row nothing can use.
    """
    registry = configured_easyweek_locations()
    if not registry.ready:
        return False
    location = registry.locations.get(company_id)
    return location is not None and location.location_uuid == KARLSRUHE_LOCATION_UUID


async def _preview_usable(session: AsyncSession, run: CampaignRun) -> str | None:
    """Why this run may not receive a test recipient, or ``None``."""
    if run.provider != PROVIDER_EASYWEEK:
        return RUN_NOT_SUPPORTED
    if run.mode != "preview" or run.status != "completed":
        return RUN_NOT_EDITABLE
    if run.campaign_code != NEW_CLIENT_CAMPAIGN_CODE:
        return RUN_NOT_SUPPORTED
    company_ids = list(run.company_ids or [])
    if len(company_ids) != 1 or not _supported_branch(int(company_ids[0])):
        return RUN_NOT_SUPPORTED
    used_as_source = await session.scalar(
        select(func.count())
        .select_from(CampaignRun)
        .where(CampaignRun.source_preview_run_id == run.id)
        .where(CampaignRun.provider == run.provider)
        .where(CampaignRun.mode == "send-real")
    )
    if int(used_as_source or 0) > 0:
        return RUN_NOT_EDITABLE
    return None


async def _one_local_client(session: AsyncSession, *, company_id: int, phone: str) -> tuple[Client | None, str | None]:
    """Exactly one EasyWeek client for this number in this branch, or a refusal.

    Zero is nothing to address; two is an ambiguity about WHO would receive a
    real voucher, and a script does not get to pick between two people.
    """
    rows = list(
        (
            await session.execute(
                select(Client)
                .where(Client.provider == PROVIDER_EASYWEEK)
                .where(Client.company_id == company_id)
                .where(Client.phone_e164 == phone)
            )
        )
        .scalars()
        .all()
    )
    if len(rows) != 1:
        return None, CLIENT_UNRESOLVED
    client = rows[0]
    if client.wa_opted_out:
        return None, CLIENT_OPTED_OUT
    return client, None


def _display_name(client: Client) -> str | None:
    """The one template parameter this canary takes from the client row."""
    return (client.display_name or "").strip() or None


async def add_test_recipient_to_preview(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
    phone: str,
    client_reader: BookingReader,
    altegio_client_id: int | None = None,
    enabled: bool | None = None,
) -> TestRecipientOutcome:
    """Add — or safely reactivate — the one approved test recipient.

    Idempotent by construction: an exact test candidate that is already there is
    left alone, and a single skipped row for the same client is reactivated in
    place rather than duplicated. Two matching rows are an ambiguity, and this
    refuses rather than choosing.
    """
    # An Altegio client id is not merely unused here — accepting one would mean
    # this path can be steered by something other than the configured identity.
    if altegio_client_id is not None:
        return TestRecipientOutcome(False, CLIENT_ID_NOT_ACCEPTED)

    destination = normalize_phone_candidate(phone)
    if not destination:
        return TestRecipientOutcome(False, PHONE_UNUSABLE)

    fence_reason = test_recipient_fences_reason(enabled=enabled)
    if fence_reason is not None:
        # Before anything is read, local or remote. A closed fence must cost
        # nothing and touch nothing.
        return TestRecipientOutcome(False, fence_reason)
    configured = configured_test_customer_uuid()
    assert configured is not None  # proven by the fence check

    # -- everything answerable locally, before the network ------------------
    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            return TestRecipientOutcome(False, RUN_NOT_EDITABLE)
        blocker = await _preview_usable(session, run)
        if blocker is not None:
            return TestRecipientOutcome(False, blocker)
        company_id = int(list(run.company_ids or [])[0])
        client, client_blocker = await _one_local_client(session, company_id=company_id, phone=destination)
        if client is None:
            assert client_blocker is not None
            return TestRecipientOutcome(False, client_blocker)
        if _display_name(client) is None:
            # The template has a name slot. An empty one would either fail at
            # Meta or reach the customer as a blank, and both are worse than
            # stopping here.
            return TestRecipientOutcome(False, NAME_MISSING)

    # -- one read-only live call, outside any transaction -------------------
    proof = await prove_test_customer(client_reader, expected_phone=destination, enabled=enabled)
    if not proof.proven or proof.customer_uuid != configured:
        return TestRecipientOutcome(False, proof.reason or fence_reason or RUN_NOT_SUPPORTED)

    # -- the write, with everything re-checked under the lock ---------------
    from altegio_bot.campaigns.runner import lock_editable_preview, recompute_snapshot_counters

    async with session_maker() as session:
        async with session.begin():
            try:
                run = await lock_editable_preview(session, run_id)
            except ValueError:
                # `lock_editable_preview` also refuses a preview the canary has
                # attached itself to. Which of its reasons fired is not worth a
                # personal detail in the answer; the two operator-visible cases
                # are "not editable" and "locked".
                async with session_maker() as probe:
                    from altegio_bot.campaigns.easyweek_voucher_delivery.ledger import preview_is_locked_by_canary

                    locked = await preview_is_locked_by_canary(probe, campaign_run_id=run_id)
                return TestRecipientOutcome(False, LOCKED_BY_CANARY if locked else RUN_NOT_EDITABLE)

            blocker = await _preview_usable(session, run)
            if blocker is not None:
                return TestRecipientOutcome(False, blocker)

            client, client_blocker = await _one_local_client(session, company_id=company_id, phone=destination)
            if client is None:
                assert client_blocker is not None
                return TestRecipientOutcome(False, client_blocker)
            name = _display_name(client)
            if name is None:
                return TestRecipientOutcome(False, NAME_MISSING)

            existing = list(
                (
                    await session.execute(
                        select(CampaignRecipient)
                        .where(CampaignRecipient.campaign_run_id == run_id)
                        .where(CampaignRecipient.provider == PROVIDER_EASYWEEK)
                        .where(CampaignRecipient.client_id == client.id)
                        .order_by(CampaignRecipient.id)
                    )
                )
                .scalars()
                .all()
            )
            if len(existing) > 1:
                # Which of them is the one to reactivate is not a question a
                # script may answer by picking the first.
                return TestRecipientOutcome(False, AMBIGUOUS_ROWS)

            customer_uuid = uuid_module.UUID(configured)
            if existing:
                row = existing[0]
                if row.source_booking_uuid is not None or (
                    row.easyweek_test_customer_uuid is not None and row.easyweek_test_customer_uuid != customer_uuid
                ):
                    # An earned row, or a test row bound to a different account.
                    # Overwriting either would rewrite what somebody approved.
                    return TestRecipientOutcome(False, AMBIGUOUS_ROWS)
                if row.status == "candidate" and row.easyweek_test_customer_uuid == customer_uuid:
                    # Already exactly what was asked for. Doing it again is not
                    # a second recipient.
                    return TestRecipientOutcome(True, None, recipient_id=row.id, action=ACTION_UNCHANGED)
                row.easyweek_test_customer_uuid = customer_uuid
                row.status = "candidate"
                row.excluded_reason = None
                row.is_opted_out = False
                row.phone_e164 = destination
                row.display_name = name
                row.local_client_found = True
                meta = dict(row.meta or {})
                # A timestamp and a basis. No phone, no name, no UUID: `meta` is
                # untyped JSON that ends up in reports and exports.
                meta["test_recipient_reactivated_at"] = utcnow().isoformat()
                row.meta = meta
                await recompute_snapshot_counters(session, run)
                await session.flush()
                return TestRecipientOutcome(True, None, recipient_id=row.id, action=ACTION_REACTIVATED)

            duplicate = await session.scalar(
                select(func.count())
                .select_from(CampaignRecipient)
                .where(CampaignRecipient.campaign_run_id == run_id)
                .where(CampaignRecipient.provider == PROVIDER_EASYWEEK)
                .where(CampaignRecipient.phone_e164 == destination)
            )
            if int(duplicate or 0) > 0:
                return TestRecipientOutcome(False, AMBIGUOUS_ROWS)

            row = CampaignRecipient(
                provider=PROVIDER_EASYWEEK,
                campaign_run_id=run_id,
                company_id=company_id,
                client_id=client.id,
                # No Altegio identity, and no Altegio CRM was consulted to
                # build this row: that API has nothing to say about an EasyWeek
                # customer, and calling it would be the bug this path avoids.
                altegio_client_id=None,
                phone_e164=destination,
                display_name=name,
                local_client_found=True,
                is_opted_out=False,
                status="candidate",
                excluded_reason=None,
                # The typed basis. Every `source_*` column stays NULL, and a
                # CHECK constraint keeps it that way: this row must never be
                # able to look like proof of a first visit nobody made.
                easyweek_test_customer_uuid=customer_uuid,
                meta={"test_recipient_added_at": utcnow().isoformat()},
            )
            session.add(row)
            await session.flush()
            recipient_id = row.id
            await recompute_snapshot_counters(session, run)
            return TestRecipientOutcome(True, None, recipient_id=recipient_id, action=ACTION_CREATED)


__all__ = [
    "AMBIGUOUS_ROWS",
    "CLIENT_ID_NOT_ACCEPTED",
    "CLIENT_OPTED_OUT",
    "CLIENT_UNRESOLVED",
    "LOCKED_BY_CANARY",
    "NAME_MISSING",
    "PHONE_UNUSABLE",
    "RUN_NOT_EDITABLE",
    "RUN_NOT_SUPPORTED",
    "TestRecipientOutcome",
    "add_test_recipient_to_preview",
]
