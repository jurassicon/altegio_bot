"""Which people this batch is for, in which order, and why no others (§41).

§37.2 addressed one person by a pair of ids an operator typed. A batch cannot
work that way: a list typed on a command line is a list that can be retyped
differently for the next stage, and "which five" would then be a fact about the
last invocation rather than about anything approved.

So the composition is read, never accepted
------------------------------------------
The operator curates the snapshot in the existing preview editor — adding and
removing by hand, which is what §37.1 exists for — and this module then reads
what that editor left behind:

    every ACTIVE recipient of one completed preview, ordered by row id.

Nothing is filtered. An extra person is not silently dropped, and a sixth
person is not silently truncated to five: both refuse the whole composition and
send the operator back to the editor. A tool that quietly took the first five
would be deciding who gets €15 and who does not.

One basis, one branch, one campaign, one period
-----------------------------------------------
Only ``operator_manual_selection`` is served here. An earned candidate belongs
to §36 and would arrive carrying a first-visit proof this phase has no business
spending; the owner's test account belongs to §36.11. Either sitting in the
snapshot refuses the batch rather than being skipped — a mixed snapshot is a
decision for the operator to take again.

Live, from scratch, every time
------------------------------
Each member is re-proven by §37.2's own ``prove_manual_recipient``: the local
client, the current number, the absence of an opt-out, and a live EasyWeek read
of that exact customer. A manual basis has no visit to prove and says so —
``first_visit_proof=not_applicable``, never a quiet ``true``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.easyweek_eligibility import BookingReader
from altegio_bot.campaigns.easyweek_manual_voucher import identity as manual_identity
from altegio_bot.campaigns.easyweek_manual_voucher.eligibility import (
    FIRST_VISIT_NOT_APPLICABLE,
    ManualRecipientProof,
    prove_manual_recipient,
)
from altegio_bot.campaigns.easyweek_voucher_batch.identity import (
    BATCH_SCHEMA_VERSION,
    BATCH_SCOPE,
    COMPOSITION_DUPLICATE_CUSTOMER,
    COMPOSITION_EMPTY,
    COMPOSITION_MIXED_BASIS,
    COMPOSITION_TOO_LARGE,
    CUSTOMER_AMBIGUOUS,
    CUSTOMER_IDENTITY_NOT_CURRENT,
    CUSTOMER_LOOKUP_UNDETERMINED,
    CUSTOMER_NAME_MISSING,
    CUSTOMER_PHONE_NOT_CURRENT,
    CUSTOMER_UUID_MISSING,
    ENTITLEMENT_ALREADY_EXISTS,
    KARLSRUHE_COMPANY_ID,
    LIVE_GUARD_UNCERTAIN,
    LOCAL_CLIENT_UNPROVEN,
    MAX_RECIPIENTS,
    NEW_CLIENT_CAMPAIGN_CODE,
    PREVIEW_ALREADY_CONSUMED,
    RECIPIENT_BASIS_UNSUPPORTED,
    RECIPIENT_NOT_CANDIDATE,
    RECIPIENT_OPTED_OUT,
    RECIPIENT_UNPROVEN,
    RUN_UNPROVEN,
    UNIT_PRICE_MINOR,
    batch_marker,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_MANUAL,
    CampaignRecipient,
    CampaignRun,
    EasyWeekCampaignVoucherDeliveryLedger,
    EasyWeekManualVoucherDeliveryLedger,
    EasyWeekVoucherSnapshotBatchItem,
)

# The recipient statuses that count as "in this snapshot". A row an operator
# removed carries `status='skipped'` and is deliberately not one of them; a row
# that was never a candidate never was.
ACTIVE_RECIPIENT_STATUS = "candidate"

# §37.2's proof is reused whole — it is the one that was run in production — but
# its ANSWERS are translated into this phase's own closed vocabulary before they
# reach a report. An operator reading a §41 refusal must never have to work out
# which phase a `manual_voucher_*` code came from, and a wrapper acting on these
# strings must not start depending on another phase's spelling.
#
# Mapped explicitly rather than by rewriting a prefix: a code §37.2 adds later
# that this phase has not considered should arrive as an unmapped string a
# reviewer notices, not as a §41 code nobody defined.
_REASON_TRANSLATION: dict[str, str] = {
    manual_identity.RUN_UNPROVEN: RUN_UNPROVEN,
    manual_identity.RECIPIENT_UNPROVEN: RECIPIENT_UNPROVEN,
    manual_identity.RECIPIENT_BASIS_UNSUPPORTED: RECIPIENT_BASIS_UNSUPPORTED,
    manual_identity.RECIPIENT_NOT_CANDIDATE: RECIPIENT_NOT_CANDIDATE,
    manual_identity.CUSTOMER_UUID_MISSING: CUSTOMER_UUID_MISSING,
    manual_identity.CUSTOMER_IDENTITY_NOT_CURRENT: CUSTOMER_IDENTITY_NOT_CURRENT,
    manual_identity.CUSTOMER_PHONE_NOT_CURRENT: CUSTOMER_PHONE_NOT_CURRENT,
    manual_identity.CUSTOMER_NAME_MISSING: CUSTOMER_NAME_MISSING,
    manual_identity.LOCAL_CLIENT_UNPROVEN: LOCAL_CLIENT_UNPROVEN,
    manual_identity.CUSTOMER_AMBIGUOUS: CUSTOMER_AMBIGUOUS,
    manual_identity.CUSTOMER_LOOKUP_UNDETERMINED: CUSTOMER_LOOKUP_UNDETERMINED,
    manual_identity.RECIPIENT_OPTED_OUT: RECIPIENT_OPTED_OUT,
    manual_identity.LIVE_GUARD_UNCERTAIN: LIVE_GUARD_UNCERTAIN,
}


def translate_reason(reason: str) -> str:
    """One §37.2 reason in this phase's vocabulary, or the string unchanged."""
    return _REASON_TRANSLATION.get(reason, reason)


@dataclass(frozen=True)
class BatchMember:
    """One slot of a proposed composition, with its live proof attached."""

    slot: int
    campaign_recipient_id: int
    proof: ManualRecipientProof

    @property
    def easyweek_customer_uuid(self) -> str | None:
        return self.proof.easyweek_customer_uuid

    def marker(self, *, preview_run_id: int) -> str:
        return batch_marker(
            preview_run_id=preview_run_id,
            campaign_recipient_id=self.campaign_recipient_id,
            slot=self.slot,
        )

    def as_safe_dict(self, *, preview_run_id: int) -> dict[str, Any]:
        """Slot, row id, booleans and reason codes. Never a person."""
        proof = dict(self.proof.as_safe_dict())
        # §37.2's answers, in this phase's vocabulary. See `translate_reason`.
        proof["reasons"] = [translate_reason(reason) for reason in proof.get("reasons", [])]
        return {
            "slot": self.slot,
            "campaign_recipient_id": self.campaign_recipient_id,
            "reconciliation_marker": self.marker(preview_run_id=preview_run_id),
            "voucher_value_minor": UNIT_PRICE_MINOR,
            "voucher_quantity": 1,
            **proof,
        }


@dataclass(frozen=True)
class BatchComposition:
    """The ordered composition a freeze would write, or the reasons it would not."""

    proven: bool
    preview_run_id: int
    reasons: tuple[str, ...] = ()
    members: tuple[BatchMember, ...] = ()
    campaign_period_start: datetime | None = None
    campaign_period_end: datetime | None = None
    # How many ACTIVE rows the snapshot held, whatever happened afterwards. An
    # operator staring at a refusal needs to know whether the answer is "six" or
    # "none", and a composition that refused carries no members to count.
    observed_active: int = 0

    @property
    def recipient_count(self) -> int:
        return len(self.members)

    @property
    def total_exposure_minor(self) -> int:
        return UNIT_PRICE_MINOR * self.recipient_count

    def digest(self) -> str:
        """The immutable fingerprint of this exact composition.

        Signed over the ordered slots and the money, so a later stage can prove
        it is acting on the batch that was approved rather than on one that was
        re-derived. Customer UUIDs carry 122 bits of entropy, so naming them in
        a digest is a fingerprint rather than a reversible record of who they
        are — the same reasoning §35 applied, and the same reason a voucher code
        is never hashed anywhere in this codebase.
        """
        material = {
            "batch_scope": BATCH_SCOPE,
            "schema_version": BATCH_SCHEMA_VERSION,
            "provider": PROVIDER_EASYWEEK,
            "company_id": KARLSRUHE_COMPANY_ID,
            "campaign_code": NEW_CLIENT_CAMPAIGN_CODE,
            "recipient_basis": RECIPIENT_BASIS_MANUAL,
            "campaign_run_id": self.preview_run_id,
            "campaign_period_start": self.campaign_period_start.isoformat()
            if self.campaign_period_start is not None
            else None,
            "campaign_period_end": self.campaign_period_end.isoformat()
            if self.campaign_period_end is not None
            else None,
            "recipient_count": self.recipient_count,
            "voucher_unit_price_minor": UNIT_PRICE_MINOR,
            "total_exposure_minor": self.total_exposure_minor,
            "slots": [
                {
                    "slot": member.slot,
                    "campaign_recipient_id": member.campaign_recipient_id,
                    "easyweek_customer_uuid": member.easyweek_customer_uuid,
                    "reconciliation_marker": member.marker(preview_run_id=self.preview_run_id),
                }
                for member in self.members
            ],
        }
        return hashlib.sha256(json.dumps(material, sort_keys=True).encode("utf-8")).hexdigest()

    @property
    def period_label(self) -> str | None:
        """The wave an operator is approving, in one glance: ``2026-08-01..2026-08-31``.

        Dates only, because that is the question being asked — WHICH monthly
        wave is this — and a timestamp with a timezone offset invites a reader
        to skim past it. The exact bounds stay beside it in full ISO, and the
        digest signs those, not this label.
        """
        if self.campaign_period_start is None or self.campaign_period_end is None:
            return None
        return f"{self.campaign_period_start.date().isoformat()}..{self.campaign_period_end.date().isoformat()}"

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "composition_proven": self.proven,
            "reasons": list(self.reasons),
            "preview_run_id": self.preview_run_id,
            # WHICH wave these people are entitled to, shown before the freeze
            # and not only after it.
            #
            # It is the entitlement key, not a send date: a transitional August
            # audience mailed in October is still an AUGUST entitlement, and an
            # operator approving the wrong period would be approving a second
            # €15 for people the August batch already served. Never inferred
            # from today; always read from the run being frozen.
            "campaign_period": self.period_label,
            "campaign_period_start": self.campaign_period_start.isoformat()
            if self.campaign_period_start is not None
            else None,
            "campaign_period_end": self.campaign_period_end.isoformat()
            if self.campaign_period_end is not None
            else None,
            "recipient_basis": RECIPIENT_BASIS_MANUAL,
            "first_visit_proof": FIRST_VISIT_NOT_APPLICABLE,
            "observed_active_recipients": self.observed_active,
            "recipient_count": self.recipient_count,
            "max_recipients": MAX_RECIPIENTS,
            "voucher_unit_price_minor": UNIT_PRICE_MINOR,
            "total_exposure_minor": self.total_exposure_minor,
            "frozen_digest": self.digest() if self.proven else None,
            "slots": [member.as_safe_dict(preview_run_id=self.preview_run_id) for member in self.members],
        }


async def _historically_consumed(
    session: AsyncSession,
    *,
    preview_run_id: int,
    recipient_ids: list[int],
    customer_uuids: list[str],
) -> bool:
    """Has §36 or §37.2 already spent this preview, row or person?

    The canary previews and the canary recipients are history. Reusing one would
    mean a batch acting on a snapshot that a different ledger is still the owner
    of — and, for a person, a second €15 for the same campaign wave.
    """
    for model in (EasyWeekCampaignVoucherDeliveryLedger, EasyWeekManualVoucherDeliveryLedger):
        clauses = [model.campaign_run_id == preview_run_id]
        if recipient_ids:
            clauses.append(model.campaign_recipient_id.in_(recipient_ids))
        if customer_uuids:
            clauses.append(model.easyweek_customer_uuid.in_(customer_uuids))
        found = await session.scalar(select(model.id).where(or_(*clauses)).limit(1))
        if found is not None:
            return True
    return False


async def _entitlement_already_taken(
    session: AsyncSession,
    *,
    customer_uuids: list[str],
    period_start: datetime,
    period_end: datetime,
    exclude_batch_id: int | None,
) -> bool:
    """Does SOME OTHER batch item already hold this person's entitlement?

    Checked here as well as by the unique constraint, so a refusal reads as a
    named reason rather than as an IntegrityError an operator has to decode.

    ``exclude_batch_id`` is what makes this usable after the freeze. Once the
    batch exists, its own slots hold exactly these entitlements — that is what
    freezing means — and counting them would make every later stage report the
    batch as a conflict with itself.
    """
    if not customer_uuids:
        return False
    statement = (
        select(EasyWeekVoucherSnapshotBatchItem.id)
        .where(EasyWeekVoucherSnapshotBatchItem.provider == PROVIDER_EASYWEEK)
        .where(EasyWeekVoucherSnapshotBatchItem.company_id == KARLSRUHE_COMPANY_ID)
        .where(EasyWeekVoucherSnapshotBatchItem.campaign_code == NEW_CLIENT_CAMPAIGN_CODE)
        .where(EasyWeekVoucherSnapshotBatchItem.easyweek_customer_uuid.in_(customer_uuids))
        .where(EasyWeekVoucherSnapshotBatchItem.campaign_period_start == period_start)
        .where(EasyWeekVoucherSnapshotBatchItem.campaign_period_end == period_end)
        .limit(1)
    )
    if exclude_batch_id is not None:
        statement = statement.where(EasyWeekVoucherSnapshotBatchItem.batch_id != exclude_batch_id)
    return (await session.scalar(statement)) is not None


async def active_recipient_ids(session: AsyncSession, *, preview_run_id: int) -> tuple[list[int], list[str]]:
    """The active rows of this preview, in slot order, and the bases they carry.

    Ordered by row id: deterministic, stable across processes, and derived from
    the order the operator actually added people in. Nothing here filters by
    basis — reporting the bases found is what lets the caller refuse a mixed
    snapshot rather than quietly serve part of it.
    """
    rows = list(
        (
            await session.execute(
                select(CampaignRecipient.id, CampaignRecipient.recipient_basis)
                .where(CampaignRecipient.campaign_run_id == preview_run_id)
                .where(CampaignRecipient.provider == PROVIDER_EASYWEEK)
                .where(CampaignRecipient.status == ACTIVE_RECIPIENT_STATUS)
                .order_by(CampaignRecipient.id.asc())
            )
        ).all()
    )
    return [int(row[0]) for row in rows], [str(row[1] or "") for row in rows]


async def prove_batch_composition(
    session: AsyncSession,
    *,
    preview_run_id: int,
    client_reader: BookingReader,
    now: datetime,
    exclude_batch_id: int | None = None,
) -> BatchComposition:
    """Read one preview's active snapshot and prove every member of it, live.

    Everything answerable from the database is answered before a single EasyWeek
    request is made, so a foreign run, an empty snapshot or a sixth recipient
    costs nothing and proves nothing.

    ``exclude_batch_id`` names the batch whose own slots must not count as a
    conflict. Before a freeze there is none; afterwards it is the frozen batch,
    which is re-proven through this same function on every later stage.
    """
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
        return BatchComposition(False, preview_run_id, (RUN_UNPROVEN,))

    recipient_ids, bases = await active_recipient_ids(session, preview_run_id=preview_run_id)
    observed = len(recipient_ids)

    if any(basis != RECIPIENT_BASIS_MANUAL for basis in bases):
        # An earned or owner-test row is sitting in the snapshot. Refused whole:
        # serving "the manual ones" would be this tool deciding which of the
        # operator's rows counted.
        return BatchComposition(False, preview_run_id, (COMPOSITION_MIXED_BASIS,), observed_active=observed)
    if observed == 0:
        return BatchComposition(False, preview_run_id, (COMPOSITION_EMPTY,), observed_active=observed)
    if observed > MAX_RECIPIENTS:
        return BatchComposition(False, preview_run_id, (COMPOSITION_TOO_LARGE,), observed_active=observed)

    # -- and only now, the live reads: one per member, in slot order ---------
    members: list[BatchMember] = []
    reasons: list[str] = []
    for slot, recipient_id in enumerate(recipient_ids, start=1):
        proof = await prove_manual_recipient(
            session,
            preview_run_id=preview_run_id,
            campaign_recipient_id=recipient_id,
            client_reader=client_reader,
            now=now,
        )
        members.append(BatchMember(slot=slot, campaign_recipient_id=recipient_id, proof=proof))
        reasons.extend(translate_reason(reason) for reason in proof.reasons)

    proposal = BatchComposition(
        proven=False,
        preview_run_id=preview_run_id,
        members=tuple(members),
        campaign_period_start=run.period_start,
        campaign_period_end=run.period_end,
        observed_active=observed,
    )
    if reasons:
        # At least one member did not prove out. The batch is refused whole: a
        # partial composition is a different approval from the one an operator
        # would be looking at.
        return BatchComposition(
            proven=False,
            preview_run_id=preview_run_id,
            reasons=tuple(dict.fromkeys(reasons)),
            members=tuple(members),
            campaign_period_start=run.period_start,
            campaign_period_end=run.period_end,
            observed_active=observed,
        )

    customer_uuids = [member.easyweek_customer_uuid or "" for member in members]
    if len(set(customer_uuids)) != len(customer_uuids):
        # Two rows, one human. The unique index would catch it at freeze; saying
        # so here means the operator reads a reason instead of a constraint name.
        reasons.append(COMPOSITION_DUPLICATE_CUSTOMER)
    if await _historically_consumed(
        session,
        preview_run_id=preview_run_id,
        recipient_ids=recipient_ids,
        customer_uuids=customer_uuids,
    ):
        # The historical canary ledgers are separate tables that this phase
        # never writes to, so this stays true before and after the freeze.
        reasons.append(PREVIEW_ALREADY_CONSUMED)
    if await _entitlement_already_taken(
        session,
        customer_uuids=customer_uuids,
        period_start=run.period_start,
        period_end=run.period_end,
        exclude_batch_id=exclude_batch_id,
    ):
        reasons.append(ENTITLEMENT_ALREADY_EXISTS)

    unique = tuple(dict.fromkeys(reasons))
    return BatchComposition(
        proven=not unique,
        preview_run_id=preview_run_id,
        reasons=unique,
        members=proposal.members,
        campaign_period_start=run.period_start,
        campaign_period_end=run.period_end,
        observed_active=observed,
    )


__all__ = [
    "ACTIVE_RECIPIENT_STATUS",
    "BatchComposition",
    "BatchMember",
    "active_recipient_ids",
    "prove_batch_composition",
    "translate_reason",
]
