"""Which people a production batch is for, how many, and what it costs (§42).

The composition is read, never accepted
---------------------------------------
The operator curates the snapshot in the existing preview editor — adding and
removing by hand, which is what §37.1 exists for — and this module then reads
what that editor left behind:

    every ACTIVE recipient of one completed preview, ordered by row id.

Nothing is filtered, exactly as in §41. An extra person is not silently
dropped, an inconvenient one is not skipped, and the list is never truncated to
a comfortable length: any problem refuses the whole composition and sends the
operator back to the editor. A tool that quietly took a subset would be
deciding who gets €15 and who does not.

What §42 adds: the operator has to say the size out loud
--------------------------------------------------------
§41 could get away with reading the size off the snapshot, because its size was
capped at five by the schema and a human could hold the whole thing in their
head. Production cannot: "every active recipient" might be four people or
forty, and the difference between those two is €540.

So a freeze here requires two numbers the operator states BEFORE anything is
frozen — ``expected_recipient_count`` and ``approved_exposure_minor`` — and both
must describe the full active snapshot exactly:

* the count must be greater than zero;
* the count must equal the number of active recipients actually found;
* the exposure must equal ``count * contract.face_value_minor`` — the NOMINAL,
  which for every paid contract is also the money.

A missing number and a wrong number are different refusals, deliberately: one
means the operator has not told us, the other means what they told us does not
match what is there. Neither is resolved by this module picking a number.

This is not a ceiling, and it is not a substitute for one. §41's five was a real
limit; inventing a new limit here would be this code deciding how large a real
campaign may be. What is enforced instead is that nobody can freeze a batch
without knowing — and recording — how many people it reaches and what it costs.

Two bases, one branch, one campaign, one period
----------------------------------------------
PR-21 serves earned first visits and operator selections together. Each member
retains its own proof and policy; a batch never claims one shared first visit.
Unknown and owner-test bases refuse the whole composition. Version 1 batches
remain manual-only and retain their original frozen digest.

The earned proof reuses the source-event, record, booking, customer, complete
history, service, period and consent guards. Manual selections reuse their
identity proof; only the explicitly attested no-EasyWeek-bookings policy adds
a complete zero-history requirement. Refunds deliberately require neither.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from altegio_bot.campaigns.easyweek_eligibility import BookingReader
from altegio_bot.campaigns.easyweek_manual_recipient import prove_customer
from altegio_bot.campaigns.easyweek_manual_voucher import identity as manual_identity
from altegio_bot.campaigns.easyweek_manual_voucher.eligibility import (
    FIRST_VISIT_NOT_APPLICABLE,
    ManualRecipientProof,
    prove_manual_recipient,
)
from altegio_bot.campaigns.easyweek_voucher_delivery.eligibility import RecipientProof, prove_recipient
from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    APPROVAL_COUNT_MISMATCH,
    APPROVAL_COUNT_MISSING,
    APPROVAL_EXPOSURE_MISMATCH,
    APPROVAL_EXPOSURE_MISSING,
    COMPOSITION_DUPLICATE_CUSTOMER,
    COMPOSITION_EMPTY,
    COMPOSITION_MIXED_BASIS,
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
    NEW_CLIENT_CAMPAIGN_CODE,
    PREVIEW_ALREADY_CONSUMED,
    PRODUCTION_SCHEMA_VERSION,
    PRODUCTION_SCOPE,
    RECIPIENT_BASIS_UNSUPPORTED,
    RECIPIENT_NOT_CANDIDATE,
    RECIPIENT_OPTED_OUT,
    RECIPIENT_UNPROVEN,
    RUN_UNPROVEN,
    production_marker,
)
from altegio_bot.campaigns.easyweek_voucher_production.read_sessions import release_reads_before_http
from altegio_bot.easyweek_voucher_production_contract import (
    is_current_fixed_contract,
    production_contract,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_EARNED,
    RECIPIENT_BASIS_MANUAL,
    CampaignRecipient,
    CampaignRun,
    EasyWeekCampaignVoucherDeliveryLedger,
    EasyWeekManualVoucherDeliveryLedger,
    EasyWeekVoucherProductionBatchItem,
    EasyWeekVoucherSnapshotBatchItem,
)

# The recipient statuses that count as "in this snapshot". A row an operator
# removed carries `status='skipped'` and is deliberately not one of them; a row
# that was never a candidate never was.
ACTIVE_RECIPIENT_STATUS = "candidate"

# §37.2's proof is reused whole — it is the one that was run in production — but
# its ANSWERS are translated into this phase's own closed vocabulary before they
# reach a report. An operator reading a §42 refusal must never have to work out
# which phase a `manual_voucher_*` code came from, and a wrapper acting on these
# strings must not start depending on another phase's spelling.
#
# Mapped explicitly rather than by rewriting a prefix: a code §37.2 adds later
# that this phase has not considered should arrive as an unmapped string a
# reviewer notices, not as a §42 code nobody defined.
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
class BatchApproval:
    """The size and the money an operator stated before the freeze.

    Both are ``None`` until an operator supplies them, and ``None`` is a refusal
    rather than a default. There is no number this class could pick that would
    not be it deciding what a human is agreeing to.
    """

    expected_recipient_count: int | None = None
    approved_exposure_minor: int | None = None

    @property
    def supplied(self) -> bool:
        return self.expected_recipient_count is not None and self.approved_exposure_minor is not None

    def reasons_against(
        self, observed_count: int, *, schema_version: str = PRODUCTION_SCHEMA_VERSION
    ) -> tuple[str, ...]:
        """Why these two numbers do NOT describe a snapshot of *observed_count*.

        An empty tuple means the operator's count matches what is really there
        and their money is exactly that count times the selected product price.

        A zero or negative count is reported as *missing* rather than as a
        mismatch: "minus one recipients" is not a claim about a snapshot that
        happens to be wrong, it is an absent approval.
        """
        reasons: list[str] = []
        count = self.expected_recipient_count
        exposure = self.approved_exposure_minor

        if count is None or count <= 0:
            reasons.append(APPROVAL_COUNT_MISSING)
        elif count != observed_count:
            reasons.append(APPROVAL_COUNT_MISMATCH)

        if exposure is None or exposure <= 0:
            reasons.append(APPROVAL_EXPOSURE_MISSING)
        elif count is None or count <= 0 or exposure != count * production_contract(schema_version).face_value_minor:
            # Compared against what the OPERATOR said, not against what was
            # observed. Two wrong numbers that are consistent with each other
            # are still caught, because the count itself is compared above.
            reasons.append(APPROVAL_EXPOSURE_MISMATCH)

        return tuple(dict.fromkeys(reasons))

    def as_safe_dict(self, *, schema_version: str = PRODUCTION_SCHEMA_VERSION) -> dict[str, Any]:
        return {
            "expected_recipient_count": self.expected_recipient_count,
            "approved_exposure_minor": self.approved_exposure_minor,
            "approval_supplied": self.supplied,
            "approval_arithmetic": (
                "approved_exposure_minor = expected_recipient_count * "
                f"{production_contract(schema_version).face_value_minor}"
            ),
            # What the same approval will actually charge. Zero for a free issue,
            # and equal to the nominal for every paid contract — reported beside it
            # so no screen and no report can show one as the other.
            **(
                {
                    "approved_issue_price_minor": (
                        None
                        if self.expected_recipient_count is None
                        else self.expected_recipient_count * production_contract(schema_version).issue_price_minor
                    )
                }
                if production_contract(schema_version).free_issue
                else {}
            ),
        }


@dataclass(frozen=True)
class ProductionMember:
    """One slot of a proposed composition, with its live proof attached."""

    slot: int
    campaign_recipient_id: int
    proof: ManualRecipientProof | RecipientProof
    recipient_basis: str = RECIPIENT_BASIS_MANUAL
    client_id: int | None = None
    manual_policy: str | None = None
    manual_policy_checked_at: str | None = None
    manual_operator_attested_at: str | None = None
    source_booking_uuid: str | None = None
    source_proof_digest: str | None = None

    def immutable_proof(self) -> dict[str, Any]:
        return {
            "recipient_basis": self.recipient_basis,
            "client_id": self.client_id,
            "manual_policy": self.manual_policy,
            "manual_policy_checked_at": self.manual_policy_checked_at,
            "manual_operator_attested_at": self.manual_operator_attested_at,
            "source_booking_uuid": self.source_booking_uuid,
            "source_proof_digest": self.source_proof_digest,
        }

    @property
    def easyweek_customer_uuid(self) -> str | None:
        return self.proof.easyweek_customer_uuid

    def marker(self, *, preview_run_id: int) -> str:
        return production_marker(
            preview_run_id=preview_run_id,
            campaign_recipient_id=self.campaign_recipient_id,
            slot=self.slot,
        )

    def as_safe_dict(self, *, preview_run_id: int, schema_version: str = PRODUCTION_SCHEMA_VERSION) -> dict[str, Any]:
        """Slot, row id, booleans and reason codes. Never a person."""
        proof = dict(self.proof.as_safe_dict())
        # §37.2's answers, in this phase's vocabulary. See `translate_reason`.
        proof["reasons"] = [translate_reason(reason) for reason in proof.get("reasons", [])]
        proof["recipient_basis"] = self.recipient_basis
        proof["first_visit_proof"] = (
            "earned_first_visit" if self.recipient_basis == RECIPIENT_BASIS_EARNED else FIRST_VISIT_NOT_APPLICABLE
        )
        proof["manual_policy"] = self.manual_policy
        proof["operator_previous_altegio_visit_attested"] = self.manual_operator_attested_at is not None
        proof["source_proof_digest"] = self.source_proof_digest
        return {
            "slot": self.slot,
            "campaign_recipient_id": self.campaign_recipient_id,
            "reconciliation_marker": self.marker(preview_run_id=preview_run_id),
            "voucher_value_minor": production_contract(schema_version).face_value_minor,
            **(
                {"voucher_issue_price_minor": production_contract(schema_version).issue_price_minor}
                if production_contract(schema_version).free_issue
                else {}
            ),
            "voucher_quantity": 1,
            **proof,
        }


@dataclass(frozen=True)
class ProductionComposition:
    """The ordered composition a freeze would write, or the reasons it would not."""

    proven: bool
    preview_run_id: int
    reasons: tuple[str, ...] = ()
    members: tuple[ProductionMember, ...] = ()
    campaign_period_start: datetime | None = None
    campaign_period_end: datetime | None = None
    # How many ACTIVE rows the snapshot held, whatever happened afterwards. An
    # operator staring at a refusal needs to know whether the answer is "forty"
    # or "none", and a composition that refused carries no members to count.
    observed_active: int = 0
    approval: BatchApproval = BatchApproval()
    schema_version: str = PRODUCTION_SCHEMA_VERSION

    @property
    def recipient_basis(self) -> str:
        bases = {member.recipient_basis for member in self.members}
        return next(iter(bases)) if len(bases) == 1 else "mixed"

    @property
    def first_visit_proof(self) -> str:
        if self.recipient_basis == RECIPIENT_BASIS_MANUAL:
            return FIRST_VISIT_NOT_APPLICABLE
        if self.recipient_basis == RECIPIENT_BASIS_EARNED:
            return "earned_first_visit"
        return "per_recipient"

    @property
    def recipient_count(self) -> int:
        return len(self.members)

    @property
    def total_exposure_minor(self) -> int:
        """The NOMINAL total: what this audience is worth to the people in it."""
        return production_contract(self.schema_version).face_value_minor * self.recipient_count

    @property
    def total_issue_price_minor(self) -> int:
        """What issuing all of them costs. Zero for the free gift certificate."""
        return production_contract(self.schema_version).issue_price_minor * self.recipient_count

    def digest(self) -> str:
        """The immutable fingerprint of this exact composition.

        Signed over the ordered slots and the money, so a later stage can prove
        it is acting on the batch that was approved rather than on one that was
        re-derived. Customer UUIDs carry 122 bits of entropy, so naming them in
        a digest is a fingerprint rather than a reversible record of who they
        are — the same reasoning §35 applied, and the same reason a voucher code
        is never hashed anywhere in this codebase.

        The APPROVED numbers are signed too, not only the observed ones. They
        are equal by the time a freeze is allowed, so including them changes no
        outcome — what it changes is what the digest MEANS: an approval covers
        the operator's own statement of the size and the cost, and a plan built
        for forty people cannot be presented as one built for four.
        """
        material = {
            "batch_scope": PRODUCTION_SCOPE,
            "schema_version": self.schema_version,
            "provider": PROVIDER_EASYWEEK,
            "company_id": KARLSRUHE_COMPANY_ID,
            "campaign_code": NEW_CLIENT_CAMPAIGN_CODE,
            "recipient_basis": self.recipient_basis,
            "campaign_run_id": self.preview_run_id,
            "campaign_period_start": self.campaign_period_start.isoformat()
            if self.campaign_period_start is not None
            else None,
            "campaign_period_end": self.campaign_period_end.isoformat()
            if self.campaign_period_end is not None
            else None,
            "recipient_count": self.recipient_count,
            "voucher_unit_price_minor": production_contract(self.schema_version).face_value_minor,
            "total_exposure_minor": self.total_exposure_minor,
            "approved_recipient_count": self.approval.expected_recipient_count,
            "approved_exposure_minor": self.approval.approved_exposure_minor,
            "slots": [
                {
                    "slot": member.slot,
                    "campaign_recipient_id": member.campaign_recipient_id,
                    "easyweek_customer_uuid": member.easyweek_customer_uuid,
                    "reconciliation_marker": member.marker(preview_run_id=self.preview_run_id),
                    **(member.immutable_proof() if self.schema_version != "1" else {}),
                }
                for member in self.members
            ],
        }
        if is_current_fixed_contract(self.schema_version):
            material["product_contract"] = production_contract(self.schema_version).digest_material()
        return hashlib.sha256(json.dumps(material, sort_keys=True).encode("utf-8")).hexdigest()

    def composition_digest(self) -> str:
        """The fingerprint of the PEOPLE alone, without the operator's approval.

        This is what a post-freeze stage compares against, and it has to exclude
        the approved numbers for a plain reason: after the freeze those numbers
        live in the database, and the later stages do not ask an operator to
        retype them. Re-deriving :meth:`digest` there would require reconstructing
        an approval, which is exactly the kind of assembled-from-defaults
        identity §41 learned not to build.

        Drift detection loses nothing by this. The approved numbers are pinned to
        the composition by CHECK constraints — they cannot describe a different
        set of people — so a change to the people always changes this digest.
        """
        material = {
            "batch_scope": PRODUCTION_SCOPE,
            "schema_version": self.schema_version,
            "provider": PROVIDER_EASYWEEK,
            "company_id": KARLSRUHE_COMPANY_ID,
            "campaign_code": NEW_CLIENT_CAMPAIGN_CODE,
            "recipient_basis": self.recipient_basis,
            "campaign_run_id": self.preview_run_id,
            "campaign_period_start": self.campaign_period_start.isoformat()
            if self.campaign_period_start is not None
            else None,
            "campaign_period_end": self.campaign_period_end.isoformat()
            if self.campaign_period_end is not None
            else None,
            "recipient_count": self.recipient_count,
            "voucher_unit_price_minor": production_contract(self.schema_version).face_value_minor,
            "total_exposure_minor": self.total_exposure_minor,
            "slots": [
                {
                    "slot": member.slot,
                    "campaign_recipient_id": member.campaign_recipient_id,
                    "easyweek_customer_uuid": member.easyweek_customer_uuid,
                    "reconciliation_marker": member.marker(preview_run_id=self.preview_run_id),
                    **(member.immutable_proof() if self.schema_version != "1" else {}),
                }
                for member in self.members
            ],
        }
        if is_current_fixed_contract(self.schema_version):
            material["product_contract"] = production_contract(self.schema_version).digest_material()
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
            "recipient_basis": self.recipient_basis,
            "first_visit_proof": self.first_visit_proof,
            "observed_active_recipients": self.observed_active,
            "recipient_count": self.recipient_count,
            "earned_recipient_count": sum(member.recipient_basis == RECIPIENT_BASIS_EARNED for member in self.members),
            "manual_recipient_count": sum(member.recipient_basis == RECIPIENT_BASIS_MANUAL for member in self.members),
            # The two sums, side by side and never one standing for the other: the
            # nominal is what this audience is worth, the issue price is what
            # putting it in their hands costs.
            "voucher_unit_price_minor": production_contract(self.schema_version).face_value_minor,
            "total_exposure_minor": self.total_exposure_minor,
            **(
                {
                    "voucher_issue_price_minor": production_contract(self.schema_version).issue_price_minor,
                    "total_issue_price_minor": self.total_issue_price_minor,
                }
                if production_contract(self.schema_version).free_issue
                else {}
            ),
            # Stated rather than implied: this phase has no recipient ceiling,
            # and a report must not leave a reader guessing whether one applied.
            "max_recipients": None,
            "recipient_ceiling_applies": False,
            **self.approval.as_safe_dict(schema_version=self.schema_version),
            **(
                {"product_contract": production_contract(self.schema_version).digest_material()}
                if is_current_fixed_contract(self.schema_version)
                else {}
            ),
            "frozen_digest": self.composition_digest() if self.proven else None,
            "approval_digest": self.digest() if self.proven else None,
            "slots": [
                member.as_safe_dict(preview_run_id=self.preview_run_id, schema_version=self.schema_version)
                for member in self.members
            ],
        }


class _ConsistentEarnedBookingReader:
    """Bind all source reads in one earned proof to the same observed payload.

    The shared earned guard verifies history, then reads the booking again to
    obtain its customer UUID. Production must never spend customer A's history
    on a customer B observed only by that last read. Reads remain live; the
    digest only rejects drift, and is neither persisted nor exposed in errors.
    """

    def __init__(self, reader: Any) -> None:
        self.reader = reader
        self.observed: dict[str, str] = {}

    def __getattr__(self, name: str) -> Any:
        return getattr(self.reader, name)

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        payload = await self.reader.get_booking(booking_uuid)
        try:
            fingerprint = hashlib.sha256(
                json.dumps(payload, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
            ).hexdigest()
        except (TypeError, ValueError):
            raise ValueError("voucher_production_source_booking_unproven") from None
        previous = self.observed.setdefault(booking_uuid, fingerprint)
        if previous != fingerprint:
            raise ValueError("voucher_production_source_booking_drift")
        return payload


def source_proof_digest(recipient: CampaignRecipient) -> str:
    """Fingerprint durable earned evidence; never manufacture visit evidence."""
    material = {
        "client_id": recipient.client_id,
        "source_easyweek_event_id": recipient.source_easyweek_event_id,
        "source_record_id": recipient.source_record_id,
        "source_booking_uuid": str(recipient.source_booking_uuid) if recipient.source_booking_uuid else None,
        "source_visits_total": recipient.source_visits_total,
        "source_visits_total_updated_at": recipient.source_visits_total_updated_at.isoformat()
        if recipient.source_visits_total_updated_at
        else None,
    }
    return hashlib.sha256(json.dumps(material, sort_keys=True).encode()).hexdigest()


async def _historically_consumed(
    session: AsyncSession,
    *,
    preview_run_id: int,
    recipient_ids: list[int],
    customer_uuids: list[str],
) -> bool:
    """Has §36, §37.2 or §41 already spent this preview, row or person?

    The canary previews, the canary recipients and the §41 batch are history.
    Reusing one would mean a production batch acting on a snapshot that a
    different ledger is still the owner of — and, for a person, a second €15 for
    a campaign they have already been served by.

    Deliberately BLANKET rather than period-scoped, which is stricter than this
    phase's own entitlement rule and is meant to be. Those ledgers are the
    controlled experiments: a handful of named people who already hold a real
    €15 code from this exact campaign. A period-scoped rule would hand one of
    them a second voucher next month on the grounds that the wave had changed,
    and "the old batch is completed", "it was a different preview" and "we are
    sending in a different month" are none of them reasons to do that.

    §41's own conservative exclusion of §36 and §37.2 is preserved unchanged and
    extended to §41 itself by the same reasoning.
    """
    singleton_models = (
        EasyWeekCampaignVoucherDeliveryLedger,
        EasyWeekManualVoucherDeliveryLedger,
        EasyWeekVoucherSnapshotBatchItem,
    )
    for model in singleton_models:
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
    """Does SOME OTHER production batch already hold this person's entitlement?

    Checked here as well as by the unique constraint, so a refusal reads as a
    named reason rather than as an IntegrityError an operator has to decode.
    The constraint remains the arbiter: two operators freezing two previews at
    the same moment race, and only one of them can win.

    ``exclude_batch_id`` is what makes this usable after the freeze. Once a
    batch exists, its own slots hold exactly these entitlements — that is what
    freezing means — and counting them would make every later stage report the
    batch as a conflict with itself.

    Scoped to the campaign PERIOD, unlike the historical check above: inside
    this phase, a new wave is a new entitlement, which is the whole point of a
    monthly campaign.
    """
    if not customer_uuids:
        return False
    statement = (
        select(EasyWeekVoucherProductionBatchItem.id)
        .where(EasyWeekVoucherProductionBatchItem.provider == PROVIDER_EASYWEEK)
        .where(EasyWeekVoucherProductionBatchItem.company_id == KARLSRUHE_COMPANY_ID)
        .where(EasyWeekVoucherProductionBatchItem.campaign_code == NEW_CLIENT_CAMPAIGN_CODE)
        .where(EasyWeekVoucherProductionBatchItem.easyweek_customer_uuid.in_(customer_uuids))
        .where(EasyWeekVoucherProductionBatchItem.campaign_period_start == period_start)
        .where(EasyWeekVoucherProductionBatchItem.campaign_period_end == period_end)
        .limit(1)
    )
    if exclude_batch_id is not None:
        statement = statement.where(EasyWeekVoucherProductionBatchItem.batch_id != exclude_batch_id)
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


@release_reads_before_http("client_reader")
async def prove_production_composition(
    session: AsyncSession,
    *,
    preview_run_id: int,
    client_reader: BookingReader,
    now: datetime,
    approval: BatchApproval | None = None,
    exclude_batch_id: int | None = None,
    schema_version: str = PRODUCTION_SCHEMA_VERSION,
) -> ProductionComposition:
    """Read one preview's active snapshot and prove every member of it, live.

    Everything answerable from the database is answered before a single EasyWeek
    request is made, so a foreign run, an empty snapshot or a mixed basis costs
    nothing and proves nothing.

    ``approval`` is the operator's stated size and cost. It is checked against
    the snapshot that was actually found, and a mismatch refuses the whole
    composition. Passing ``None`` — which every post-freeze stage does — means
    "do not ask": those stages read the approved numbers from the frozen row
    instead of asking an operator to retype them.

    ``exclude_batch_id`` names the batch whose own slots must not count as a
    conflict. Before a freeze there is none; afterwards it is the frozen batch,
    which is re-proven through this same function on every later stage.
    """
    supplied = approval or BatchApproval()
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
        return ProductionComposition(
            False, preview_run_id, (RUN_UNPROVEN,), approval=supplied, schema_version=schema_version
        )

    recipient_ids, bases = await active_recipient_ids(session, preview_run_id=preview_run_id)
    observed = len(recipient_ids)

    if any(
        basis
        not in ({RECIPIENT_BASIS_MANUAL} if schema_version == "1" else {RECIPIENT_BASIS_MANUAL, RECIPIENT_BASIS_EARNED})
        for basis in bases
    ):
        # Unknown/test bases refuse the full composition. Legacy batches never
        # acquire earned eligibility merely because a newer app is deployed.
        return ProductionComposition(
            False,
            preview_run_id,
            (COMPOSITION_MIXED_BASIS,),
            observed_active=observed,
            approval=supplied,
            schema_version=schema_version,
        )
    if observed == 0:
        return ProductionComposition(
            False,
            preview_run_id,
            (COMPOSITION_EMPTY,),
            observed_active=observed,
            approval=supplied,
            schema_version=schema_version,
        )

    # The operator's stated size and cost, against what is really there. Checked
    # BEFORE the live reads: a wrong count is answerable from the database, and
    # an operator who approved four people for a snapshot of forty should find
    # that out without forty EasyWeek requests being spent on it.
    #
    # Only when an approval was asked for at all. Post-freeze stages pass none.
    approval_reasons: tuple[str, ...] = ()
    if approval is not None:
        approval_reasons = supplied.reasons_against(observed, schema_version=schema_version)
        if approval_reasons:
            return ProductionComposition(
                False,
                preview_run_id,
                approval_reasons,
                observed_active=observed,
                approval=supplied,
                schema_version=schema_version,
            )

    # -- and only now, the live reads: one per member, in slot order ---------
    members: list[ProductionMember] = []
    reasons: list[str] = []
    for slot, recipient_id in enumerate(recipient_ids, start=1):
        recipient = await session.get(CampaignRecipient, recipient_id)
        assert recipient is not None
        basis = recipient.recipient_basis
        policy = recipient.manual_policy
        if basis == RECIPIENT_BASIS_EARNED:
            proof = await prove_recipient(
                session,
                preview_run_id=preview_run_id,
                campaign_recipient_id=recipient_id,
                expected_company_id=KARLSRUHE_COMPANY_ID,
                client_reader=_ConsistentEarnedBookingReader(client_reader),
                now=now,
            )
            if proof.proven:
                # Source/history UUIDs alone do not establish the destination.
                # Legacy Clients may still have no UUID column populated, so
                # local phone equality cannot substitute for this live bridge.
                customer, _reason = await prove_customer(
                    client_reader, phone=proof.destination_phone or "", require_name=False
                )
                destination_current = customer is not None and customer.uuid == proof.easyweek_customer_uuid
                proof = replace(
                    proof,
                    proven=destination_current,
                    reasons=() if destination_current else (CUSTOMER_IDENTITY_NOT_CURRENT,),
                    checks={**(proof.checks or {}), "workspace_customer_destination_current": destination_current},
                )
        else:
            proof = await prove_manual_recipient(
                session,
                preview_run_id=preview_run_id,
                campaign_recipient_id=recipient_id,
                client_reader=client_reader,
                now=now,
            )
        if proof.proven:
            from altegio_bot.campaigns.easyweek_manual_identity import local_identity

            _client, identity_reason = await local_identity(
                session,
                company_id=KARLSRUHE_COMPANY_ID,
                phone=proof.destination_phone or "",
                customer_uuid=proof.easyweek_customer_uuid,
            )
            if identity_reason or _client is None or _client.id != recipient.client_id:
                proof = replace(proof, proven=False, reasons=(identity_reason or LOCAL_CLIENT_UNPROVEN,))
        if proof.proven and not proof.client_display_name:
            proof = replace(proof, proven=False, reasons=(CUSTOMER_NAME_MISSING,))
        if policy is not None:
            from altegio_bot.campaigns.easyweek_manual_batch import prove_zero_booking_history

            if (
                schema_version == "1"
                or basis != RECIPIENT_BASIS_MANUAL
                or policy != "altegio_visit_zero_easyweek_bookings"
                or recipient.manual_policy_checked_at is None
                or recipient.manual_operator_attested_at is None
            ):
                proof = replace(proof, proven=False, reasons=(RECIPIENT_BASIS_UNSUPPORTED,))
            elif proof.proven:
                history_reason = await prove_zero_booking_history(
                    client_reader,
                    customer_uuid=proof.easyweek_customer_uuid or "",
                )
                checks = {**(proof.checks or {}), "zero_easyweek_bookings": history_reason is None}
                proof = replace(
                    proof,
                    proven=history_reason is None,
                    reasons=(history_reason,) if history_reason else (),
                    checks=checks,
                )
        member = ProductionMember(
            slot=slot,
            campaign_recipient_id=recipient_id,
            proof=proof,
            recipient_basis=basis,
            client_id=recipient.client_id,
            manual_policy=policy,
            manual_policy_checked_at=recipient.manual_policy_checked_at.isoformat()
            if recipient.manual_policy_checked_at
            else None,
            manual_operator_attested_at=recipient.manual_operator_attested_at.isoformat()
            if recipient.manual_operator_attested_at
            else None,
            source_booking_uuid=str(recipient.source_booking_uuid) if recipient.source_booking_uuid else None,
            source_proof_digest=source_proof_digest(recipient) if basis == RECIPIENT_BASIS_EARNED else None,
        )
        members.append(member)
        reasons.extend(translate_reason(reason) for reason in proof.reasons)

    if reasons:
        # At least one member did not prove out. The batch is refused whole: a
        # partial composition is a different approval from the one an operator
        # would be looking at.
        return ProductionComposition(
            proven=False,
            preview_run_id=preview_run_id,
            reasons=tuple(dict.fromkeys(reasons)),
            members=tuple(members),
            campaign_period_start=run.period_start,
            campaign_period_end=run.period_end,
            observed_active=observed,
            approval=supplied,
            schema_version=schema_version,
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
        # The historical canary ledgers and the §41 batch are separate tables
        # that this phase never writes to, so this stays true before and after
        # the freeze.
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
    return ProductionComposition(
        proven=not unique,
        preview_run_id=preview_run_id,
        reasons=unique,
        members=tuple(members),
        campaign_period_start=run.period_start,
        campaign_period_end=run.period_end,
        observed_active=observed,
        approval=supplied,
        schema_version=schema_version,
    )


__all__ = [
    "ACTIVE_RECIPIENT_STATUS",
    "BatchApproval",
    "ProductionComposition",
    "ProductionMember",
    "active_recipient_ids",
    "prove_production_composition",
    "translate_reason",
]
