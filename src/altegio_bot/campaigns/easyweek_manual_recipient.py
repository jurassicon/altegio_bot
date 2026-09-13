"""Adding a recipient to an EasyWeek preview by hand (§37.1).

Why an operator types phone numbers at all
------------------------------------------
EasyWeek only started on 1 September, so the transitional August list cannot be
proven from EasyWeek data — there is none. The owner's decision is that one list
is assembled by a person, and every later list comes from the segmenter as
before. No Altegio recipient is copied across: an Altegio candidate was proven
against Altegio's CRM, and moving the row would move a proof that does not exist
on this side.

A decision, not a proof
-----------------------
The row this writes carries ``operator_manual_selection``. It is NOT evidence of
a first visit, and nothing here pretends otherwise: no source event, no record,
no booking UUID, no ``visits_total``, no borrowed booking from somebody else's
visit. A database CHECK forbids those columns on this basis outright, so no
later code path can promote a decision into a proof.

What the browser may say, and what it may not
---------------------------------------------
It may say a phone number. It may not say a customer UUID, an Altegio client id,
or anything that would let the caller declare a person already verified. The
identity is resolved on the server, twice: a workspace-wide phone lookup that
reads every page it is told exists and accepts exactly one customer, and then a
direct read of that customer whose UUID and number must both still match.

Everything is proven before anything is written
-----------------------------------------------
The live reads happen BEFORE the write transaction opens, so a slow or failing
EasyWeek never holds a lock on a campaign run. Then the run is locked, every
fact is re-checked under that lock, the row is written or reactivated, and the
snapshot counters are recounted in the same transaction. A failure anywhere
leaves the snapshot exactly as it was.

Nothing external is ever created. There is no POST here: an absent customer is a
stable refusal, not an invitation to create one.

No refusal carries a phone number, a name or a customer UUID.
"""

from __future__ import annotations

import uuid as uuid_module
from dataclasses import dataclass
from typing import Any, Final, Protocol

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_migration.customer_api import (
    LOOKUP_ABSENT,
    LOOKUP_AMBIGUOUS,
    LOOKUP_FIRST_NAME_MISSING,
    LOOKUP_FOUND,
    LOOKUP_PHONE_UNUSABLE,
    lookup_customer_by_phone,
    read_customer_card,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_EARNED,
    RECIPIENT_BASIS_MANUAL,
    RECIPIENT_BASIS_TEST,
    CampaignRecipient,
    CampaignRun,
    Client,
)
from altegio_bot.utils import utcnow
from altegio_bot.webhooks.common import normalize_phone_candidate

# -- refusals ---------------------------------------------------------------
# Stable, PII-free, and specific enough for an operator to act on. Every one of
# them is rendered as a sentence in the UI; none of them carries a number.
PHONE_UNUSABLE: Final = "manual_recipient_phone_unusable"
RUN_NOT_FOUND: Final = "manual_recipient_run_not_found"
RUN_NOT_EASYWEEK: Final = "manual_recipient_run_not_easyweek"
RUN_NOT_EDITABLE: Final = "manual_recipient_run_not_editable"
RUN_BRANCH_UNKNOWN: Final = "manual_recipient_branch_unknown"
PREVIEW_FROZEN: Final = "manual_recipient_preview_frozen"
CUSTOMER_ABSENT: Final = "manual_recipient_customer_absent"
CUSTOMER_AMBIGUOUS: Final = "manual_recipient_customer_ambiguous"
CUSTOMER_UNPROVEN: Final = "manual_recipient_customer_unproven"
CUSTOMER_NAME_MISSING: Final = "manual_recipient_customer_name_missing"
CLIENT_ABSENT: Final = "manual_recipient_local_client_absent"
CLIENT_AMBIGUOUS: Final = "manual_recipient_local_client_ambiguous"
CLIENT_OPTED_OUT: Final = "manual_recipient_opted_out"
ROWS_AMBIGUOUS: Final = "manual_recipient_rows_ambiguous"
IDENTITY_NOT_ACCEPTED: Final = "manual_recipient_identity_not_accepted"

ACTION_CREATED: Final = "created"
ACTION_REACTIVATED: Final = "reactivated"
ACTION_INCLUDED: Final = "included"
ACTION_UNCHANGED: Final = "unchanged"

MANUAL_REMOVED: Final = "manual_removed"


class CustomerReader(Protocol):
    """The read-only slice of the EasyWeek client this module needs."""

    async def list_customers(self, *, params: dict[str, Any]) -> dict[str, Any]: ...

    async def get_customer(self, customer_uuid: str) -> dict[str, Any]: ...


@dataclass(frozen=True)
class ManualRecipientOutcome:
    """What happened, in terms an operator screen can render verbatim."""

    ok: bool
    reason: str | None = None
    recipient_id: int | None = None
    action: str | None = None
    # The segmenter's original verdict, when an operator overrode one.
    overrode_auto_reason: str | None = None

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "reason": self.reason,
            "recipient_id": self.recipient_id,
            "action": self.action,
            "recipient_basis": RECIPIENT_BASIS_MANUAL,
            "overrode_auto_reason": self.overrode_auto_reason,
            # Repeated on every answer, success included. §37.1 opens the editor
            # and nothing else.
            "campaign_send_authorized": False,
            "bulk_delivery_authorized": False,
            "global_ready_for_send": False,
        }


@dataclass(frozen=True)
class ProvenCustomer:
    """One EasyWeek customer, resolved and then re-read. Never printed."""

    uuid: str
    phone: str
    first_name: str


def known_branch(company_id: int) -> bool:
    """Is this company one of the branches the server itself configured?

    The browser sends a company id; it does not get to define what that id
    means. The registry is the only mapping, and a preview for a branch nobody
    configured is a preview nothing can act on.
    """
    registry = configured_easyweek_locations()
    return registry.ready and company_id in registry.locations


async def _preview_blocker(session: AsyncSession, run: CampaignRun) -> str | None:
    """Why this run may not be edited, or ``None``."""
    if run.provider != PROVIDER_EASYWEEK:
        return RUN_NOT_EASYWEEK
    if run.mode != "preview" or run.status != "completed":
        return RUN_NOT_EDITABLE
    company_ids = list(run.company_ids or [])
    if len(company_ids) != 1:
        return RUN_NOT_EDITABLE
    if not known_branch(int(company_ids[0])):
        return RUN_BRANCH_UNKNOWN
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


async def prove_customer(reader: CustomerReader, *, phone: str) -> tuple[ProvenCustomer | None, str | None]:
    """Resolve a number to exactly one customer, then read that customer back.

    Two reads, deliberately. The listing answers "who has this number?" across
    the whole workspace, reading every page it claims to have — an unfinished
    read is not an absence, and the row never seen may be the customer. The
    direct read then answers "is that still this customer, at this number?",
    because a filtered listing row is a claim and a 200 on a UUID is a fact.

    Returns ``(None, reason)`` for every failure. There is no partial success:
    ambiguity, absence, a mismatch, a malformed body, an incomplete walk, a
    timeout, a 429, a 5xx and an auth failure all stop here, before the caller
    opens a transaction.
    """
    lookup = await lookup_customer_by_phone(reader, phone)
    if lookup.outcome == LOOKUP_PHONE_UNUSABLE:
        return None, PHONE_UNUSABLE
    if lookup.outcome == LOOKUP_ABSENT:
        # No customer, and this path does not create one: a POST here would
        # invent a person in a production workspace on the strength of a typo.
        return None, CUSTOMER_ABSENT
    if lookup.outcome == LOOKUP_AMBIGUOUS:
        # One number, two customers — a couple, a family phone, a duplicated
        # import. Picking the first would pick a person.
        return None, CUSTOMER_AMBIGUOUS
    if lookup.outcome == LOOKUP_FIRST_NAME_MISSING:
        return None, CUSTOMER_NAME_MISSING
    if lookup.outcome != LOOKUP_FOUND or lookup.uuid is None or lookup.phone is None:
        # Undetermined: transport, auth, malformed, incomplete pagination. The
        # workspace was not read, so nothing about it is known.
        return None, CUSTOMER_UNPROVEN

    try:
        payload = await reader.get_customer(lookup.uuid)
        card = read_customer_card(payload, expected_phone=lookup.phone)
    except Exception:  # noqa: BLE001 - every read failure is the same refusal
        return None, CUSTOMER_UNPROVEN

    if card.uuid != lookup.uuid or card.phone != lookup.phone:
        return None, CUSTOMER_UNPROVEN
    first_name = (card.first_name or "").strip()
    if not first_name:
        # The delivery template has a name slot. An empty one either fails at
        # Meta or reaches a customer as a blank.
        return None, CUSTOMER_NAME_MISSING
    return ProvenCustomer(uuid=card.uuid, phone=card.phone, first_name=first_name), None


async def _one_local_client(session: AsyncSession, *, company_id: int, phone: str) -> tuple[Client | None, str | None]:
    """Exactly one EasyWeek client for this number in this branch, or a refusal."""
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
    if not rows:
        return None, CLIENT_ABSENT
    if len(rows) > 1:
        return None, CLIENT_AMBIGUOUS
    client = rows[0]
    if client.wa_opted_out:
        return None, CLIENT_OPTED_OUT
    return client, None


def _active(row: CampaignRecipient) -> bool:
    """Is this row still a recipient, as opposed to an excluded one?"""
    return row.status == "candidate" and not row.excluded_reason


async def add_manual_recipient(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    run_id: int,
    phone: str,
    reader: CustomerReader,
    customer_uuid: object = None,
    altegio_client_id: object = None,
) -> ManualRecipientOutcome:
    """Add — or reactivate, or explicitly include — one recipient by hand.

    Deterministic about every shape the snapshot can already be in, because
    "what happens if the person is already there" is exactly where an editor
    quietly creates duplicates:

    * an active earned candidate stays earned and reports ``unchanged`` — an
      operator asking for somebody the segmenter already proved does not
      downgrade that proof to a decision;
    * an active manual candidate reports ``unchanged``;
    * a manually removed row comes back on ITS OWN original basis;
    * an automatically excluded row can be included, and the segmenter's verdict
      is kept in ``auto_excluded_reason`` rather than erased;
    * anything ambiguous is refused without picking a winner.
    """
    # Identity is the server's to establish. Accepting either of these would
    # make the browser a party to deciding who receives a real message.
    if customer_uuid is not None or altegio_client_id is not None:
        return ManualRecipientOutcome(False, IDENTITY_NOT_ACCEPTED)

    destination = normalize_phone_candidate(phone)
    if not destination:
        return ManualRecipientOutcome(False, PHONE_UNUSABLE)

    # -- everything answerable locally, before the network ------------------
    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        if run is None:
            return ManualRecipientOutcome(False, RUN_NOT_FOUND)
        blocker = await _preview_blocker(session, run)
        if blocker is not None:
            return ManualRecipientOutcome(False, blocker)
        company_id = int(list(run.company_ids or [])[0])
        if await _frozen(session, run_id):
            return ManualRecipientOutcome(False, PREVIEW_FROZEN)
        _client, client_blocker = await _one_local_client(session, company_id=company_id, phone=destination)
        if client_blocker is not None:
            return ManualRecipientOutcome(False, client_blocker)

    # -- the live reads, outside any transaction ----------------------------
    proven, reason = await prove_customer(reader, phone=destination)
    if proven is None:
        assert reason is not None
        return ManualRecipientOutcome(False, reason)

    # -- the write, with everything re-checked under the lock ---------------
    from altegio_bot.campaigns.runner import lock_editable_preview, recompute_snapshot_counters

    async with session_maker() as session:
        async with session.begin():
            try:
                run = await lock_editable_preview(session, run_id)
            except ValueError:
                return ManualRecipientOutcome(False, await _locked_reason(session_maker, run_id))

            blocker = await _preview_blocker(session, run)
            if blocker is not None:
                return ManualRecipientOutcome(False, blocker)

            client, client_blocker = await _one_local_client(session, company_id=company_id, phone=destination)
            if client is None:
                assert client_blocker is not None
                return ManualRecipientOutcome(False, client_blocker)

            proven_uuid = uuid_module.UUID(proven.uuid)
            existing = list(
                (
                    await session.execute(
                        select(CampaignRecipient)
                        .where(CampaignRecipient.campaign_run_id == run_id)
                        .where(CampaignRecipient.provider == PROVIDER_EASYWEEK)
                        .where(
                            (CampaignRecipient.client_id == client.id)
                            | (CampaignRecipient.easyweek_customer_uuid == proven_uuid)
                        )
                        .order_by(CampaignRecipient.id)
                    )
                )
                .scalars()
                .all()
            )
            if len(existing) > 1:
                # Two rows for one person. Which one an operator meant is not a
                # question a script answers by taking the first.
                return ManualRecipientOutcome(False, ROWS_AMBIGUOUS)

            if existing:
                outcome = _reuse(existing[0], proven_uuid=proven_uuid, name=proven.first_name, phone=destination)
                if outcome.ok:
                    await recompute_snapshot_counters(session, run)
                return outcome

            row = CampaignRecipient(
                provider=PROVIDER_EASYWEEK,
                campaign_run_id=run_id,
                company_id=company_id,
                client_id=client.id,
                # No Altegio identity: this path never asked the Altegio CRM,
                # which has nothing to say about an EasyWeek customer.
                altegio_client_id=None,
                phone_e164=destination,
                display_name=proven.first_name,
                local_client_found=True,
                is_opted_out=False,
                status="candidate",
                excluded_reason=None,
                recipient_basis=RECIPIENT_BASIS_MANUAL,
                easyweek_customer_uuid=proven_uuid,
                meta={"manually_added_at": utcnow().isoformat()},
            )
            session.add(row)
            await session.flush()
            recipient_id = row.id
            await recompute_snapshot_counters(session, run)
            return ManualRecipientOutcome(True, None, recipient_id=recipient_id, action=ACTION_CREATED)


def _reuse(
    row: CampaignRecipient,
    *,
    proven_uuid: uuid_module.UUID,
    name: str,
    phone: str,
) -> ManualRecipientOutcome:
    """Decide what an existing row becomes. One rule per shape, no defaults.

    The basis a row already has is the basis it keeps. Reactivating an earned
    candidate as `manual` would quietly discard a proof; reactivating a manual
    one as `earned` would quietly manufacture one.
    """
    if row.recipient_basis == RECIPIENT_BASIS_TEST:
        # The canary's account, which has its own contract and its own endpoint.
        return ManualRecipientOutcome(False, ROWS_AMBIGUOUS)
    if row.easyweek_customer_uuid is not None and row.easyweek_customer_uuid != proven_uuid:
        # This row is bound to a different customer than the number resolved to.
        return ManualRecipientOutcome(False, ROWS_AMBIGUOUS)

    if _active(row):
        # Already a recipient, on whatever basis it earned. Asking twice is not
        # two recipients, and it does not change what the first one was.
        return ManualRecipientOutcome(True, None, recipient_id=row.id, action=ACTION_UNCHANGED)

    was_manual_removal = row.excluded_reason == MANUAL_REMOVED
    original_auto_reason = row.auto_excluded_reason
    if not was_manual_removal and row.excluded_reason:
        # The segmenter excluded this person and an operator is including them
        # anyway. Keep WHY it excluded them: an override that erases what it
        # overrode leaves nobody able to say what was decided.
        original_auto_reason = original_auto_reason or row.excluded_reason

    row.status = "candidate"
    row.excluded_reason = None
    row.is_opted_out = False
    row.auto_excluded_reason = original_auto_reason
    meta = dict(row.meta or {})
    # A timestamp and nothing else: `meta` is untyped JSON that reaches reports.
    meta["manually_included_at" if not was_manual_removal else "manually_restored_at"] = utcnow().isoformat()
    row.meta = meta

    if row.recipient_basis == RECIPIENT_BASIS_EARNED:
        # Restored as what it was. Its source proof is untouched.
        return ManualRecipientOutcome(
            True,
            None,
            recipient_id=row.id,
            action=ACTION_REACTIVATED if was_manual_removal else ACTION_INCLUDED,
            overrode_auto_reason=None if was_manual_removal else original_auto_reason,
        )

    row.recipient_basis = RECIPIENT_BASIS_MANUAL
    row.easyweek_customer_uuid = proven_uuid
    row.phone_e164 = phone
    row.display_name = name
    row.local_client_found = True
    return ManualRecipientOutcome(
        True,
        None,
        recipient_id=row.id,
        action=ACTION_REACTIVATED if was_manual_removal else ACTION_INCLUDED,
        overrode_auto_reason=None if was_manual_removal else original_auto_reason,
    )


async def _frozen(session: AsyncSession, run_id: int) -> bool:
    """Has a voucher canary attached itself to this preview?"""
    from altegio_bot.campaigns.easyweek_voucher_delivery.ledger import preview_is_locked_by_canary

    return await preview_is_locked_by_canary(session, campaign_run_id=run_id)


async def _locked_reason(session_maker: async_sessionmaker[AsyncSession], run_id: int) -> str:
    """Which of `lock_editable_preview`'s refusals an operator is looking at."""
    async with session_maker() as session:
        if await _frozen(session, run_id):
            return PREVIEW_FROZEN
    return RUN_NOT_EDITABLE


__all__ = [
    "ACTION_CREATED",
    "ACTION_INCLUDED",
    "ACTION_REACTIVATED",
    "ACTION_UNCHANGED",
    "CLIENT_ABSENT",
    "CLIENT_AMBIGUOUS",
    "CLIENT_OPTED_OUT",
    "CUSTOMER_ABSENT",
    "CUSTOMER_AMBIGUOUS",
    "CUSTOMER_NAME_MISSING",
    "CUSTOMER_UNPROVEN",
    "IDENTITY_NOT_ACCEPTED",
    "PHONE_UNUSABLE",
    "PREVIEW_FROZEN",
    "ROWS_AMBIGUOUS",
    "RUN_BRANCH_UNKNOWN",
    "RUN_NOT_EASYWEEK",
    "RUN_NOT_EDITABLE",
    "RUN_NOT_FOUND",
    "ManualRecipientOutcome",
    "ProvenCustomer",
    "add_manual_recipient",
    "known_branch",
    "prove_customer",
]
