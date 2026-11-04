"""Build and store one immutable follow-up preview version (§46.2, §46.6).

The three phases, and why they are three
----------------------------------------
**Local.** One short read transaction: the batch header, its slots, their single
attempts, the confirmed failure facts, consent, replies, entitlements. Most rows
are decided here, for nothing, and no lock is taken that outlives the read.

**External.** No transaction at all. The booking proof is the only network step
in the whole preview, and it is spent only on rows that would otherwise be
included. §46.5 forbids holding a row lock or an advisory lock across it, and
the simplest way to obey that is to hold no session at all.

**Save.** One short write transaction: re-read the mutable local facts, apply the
dedupe contract, insert the snapshot. Consent, pending inbound and the batch's
terminal stop can all have moved while the network was busy, so each is asked
again — and a row whose consent moved is dropped rather than stored as included
on evidence that is no longer true.

Immutability
------------
A stored version is never rewritten. Re-viewing one changes nothing; an explicit
recalculation inserts the NEXT version beside it, and the earlier one keeps its
groups, its interval, its template contract, its policy version, its counts and
its digest exactly as they were. Nothing marks an old version as superseded,
because nothing has to: the current one is simply the highest version, and a
column saying otherwise would be a write into an immutable row.

Preview is not permission
-------------------------
Nothing here spends, reserves or claims a send attempt; no POST to Meta, no job,
no Outbox row, no voucher mutation, no change to any §45 counter. Creating a
preview twice is allowed and harmless, and it may not block anybody permanently:
the dedupe contract is scoped to ONE version, so the second preview of the same
batch starts from the same candidates as the first.

What the client cannot do
-------------------------
The composition comes from the server's own read of the ledger. A caller may
choose a batch, a set of groups and an interval, and nothing else: no list of
allowed recipients, no bindings, no digest, no counts. A digest from an older
version therefore cannot describe a different composition, because the digest is
computed from the rows this server just read.
"""

from __future__ import annotations

import hashlib
import json
import logging
import uuid as uuid_module
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any, Callable, Final, Sequence

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.campaigns.easyweek_customer_history import (
    CustomerBookingSet,
    CustomerHistoryReader,
)
from altegio_bot.campaigns.easyweek_customer_history import (
    read_customer_bookings as _read_customer_bookings,
)
from altegio_bot.campaigns.easyweek_voucher_followup import followup_template_contract as message
from altegio_bot.campaigns.easyweek_voucher_followup.artifact_proof import (
    prove_source_artifact as _prove_source_artifact,
)
from altegio_bot.campaigns.easyweek_voucher_followup.booking_evidence import (
    CapturedCreations,
    load_creation_evidence,
)
from altegio_bot.campaigns.easyweek_voucher_followup.eligibility import (
    BookingVerdict,
    RowFacts,
    booking_verdict,
    consent_snapshot,
    entitlement_verdict,
    failure_policy_reason,
    has_replied_since,
    interval_elapsed,
    resolve_identity,
    source_entitlement_digest,
)
from altegio_bot.campaigns.easyweek_voucher_followup.failure_policy import (
    CURRENT_FAILURE_POLICY,
    FailureFact,
    FailurePolicy,
)
from altegio_bot.campaigns.easyweek_voucher_followup.groups import SourceEvidence, resolve_group
from altegio_bot.campaigns.easyweek_voucher_followup.historical_consent import (
    MAX_SCANNED_EVENTS as HISTORICAL_SCAN_LIMIT,
)
from altegio_bot.campaigns.easyweek_voucher_followup.historical_consent import (
    HistoricalConsentScan,
    scan_historical_refusals,
)
from altegio_bot.campaigns.easyweek_voucher_followup.identity import (
    BATCH_NOT_FOUND,
    BATCH_NOT_FROZEN,
    BATCH_SCHEMA_UNSUPPORTED,
    BATCH_SCOPE_UNSUPPORTED,
    BATCH_TERMINAL_STOP,
    BOOKING_CAPTURE_UNPROVEN,
    CONTRACT_VERSION,
    DUPLICATE_SOURCE,
    FOLLOWUP_ROW_EXCLUDED,
    FOLLOWUP_ROW_INCLUDED,
    FOLLOWUP_ROW_UNKNOWN,
    GROUP_MOVED_OUT_OF_SELECTION,
    GROUPS_REQUIRED,
    HISTORICAL_CONSENT_UNPROVEN,
    HISTORICAL_STOP_UNACCOUNTED,
    HISTORY_UNPROVEN,
    IDENTITY_CONFLICT,
    INTERVAL_NOT_ELAPSED,
    INTERVAL_REQUIRED,
    PREVIEW_VALIDITY_SECONDS,
    SENDER_UNPROVEN,
    SOURCE_ATTEMPT_UNPROVEN,
    STATUS_EVIDENCE_UNPROVEN,
    TEMPLATE_UNPROVEN,
    canonical_groups,
)
from altegio_bot.campaigns.easyweek_voucher_production import ledger as production_ledger
from altegio_bot.easyweek_voucher_production_contract import is_current_fixed_contract
from altegio_bot.models.models import (
    DELIVERY_FAILURE_MATCH_CONFIRMED,
    DELIVERY_RECEIPT_KIND_DELIVERED,
    DELIVERY_RECEIPT_KIND_READ,
    FOLLOWUP_GROUP_UNPROVEN,
    PROVIDER_EASYWEEK,
    RECIPIENT_BASIS_EARNED,
    RECIPIENT_BASIS_MANUAL,
    CampaignRecipient,
    Client,
    EasyWeekVoucherFollowupPreview,
    EasyWeekVoucherFollowupPreviewRow,
    EasyWeekVoucherProductionBatch,
    EasyWeekVoucherProductionBatchAttempt,
    EasyWeekVoucherProductionBatchItem,
    EasyWeekVoucherProductionDeliveryFailure,
    EasyWeekVoucherProductionDeliveryReceipt,
)
from altegio_bot.utils import utcnow
from altegio_bot.whatsapp_consent import normalize_wa_address

logger = logging.getLogger("easyweek_voucher_followup_preview")

# What a production header stores when its composition carries both bases. The
# value lives in the §42 composition read; named here so neither this module nor
# a test repeats the literal.
RECIPIENT_BASIS_PRODUCTION_MIXED: Final = "mixed"

__all__ = [
    "BatchGate",
    "PreviewOutcome",
    "PreviewRowView",
    "PreviewSpec",
    "StoredPreview",
    "SnapshotCurrency",
    "assess_snapshot",
    "batch_gate",
    "create_preview",
    "latest_preview",
    "local_template_row_proven",
    "prove_meta_followup_template",
]

# The composition bases a follow-up preview supports, at the BATCH header.
#
# All three a production batch can carry. §46.2 keeps the first version to the
# fixed €10 batches of schema 3/4 "с доказанными собственными bindings" and
# puts schema 1/2, canary and snapshot ledgers out of scope; it does not narrow
# the audience to one composition basis, and a manually selected recipient of a
# production mailing received the same approved message from the same sender as
# an earned one. What differs is which link proves their identity, and that is
# proven per slot by ``resolve_identity`` against the row's own basis.
#
# A canary, a snapshot ledger, an owner test and an Altegio campaign are other
# kinds of mailing with their own tables, and the batch gate refuses them by
# provider and schema rather than by basis.
SUPPORTED_RECIPIENT_BASES = frozenset(
    {RECIPIENT_BASIS_EARNED, RECIPIENT_BASIS_MANUAL, RECIPIENT_BASIS_PRODUCTION_MIXED}
)


# ---------------------------------------------------------------------------
# The source batch gate
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class BatchGate:
    """May a follow-up preview be built from this batch at all?"""

    allowed: bool
    reason: str | None = None
    batch_id: int | None = None
    campaign_run_id: int | None = None
    company_id: int | None = None
    location_uuid: uuid_module.UUID | None = None
    schema_version: str = ""
    frozen_digest: str = ""
    recipient_basis: str = ""
    status: str = ""
    stop_generation: int = 0
    terminal_stop: bool = False
    recipient_count: int = 0
    # The digest of the branch sender a repeat message would go out from.
    sender_digest: str | None = None


async def batch_gate(session: AsyncSession, *, batch_id: int) -> BatchGate:
    """§46.1's source bounds, re-proven on the server for every request.

    All of it is checked here rather than relied upon from a page, because a
    page cannot be trusted to have been rendered recently or at all. A direct
    API call reaches exactly this function.

    * the batch must exist and be an EasyWeek production batch;
    * its request schema must be one of the fixed owner-approved 10 EUR
      products. Schemas 1 and 2 are the historical 15 EUR contract and keep
      their own older rules, and nothing here reinterprets them;
    * its recipient basis must be the earned one. A canary, a snapshot ledger,
      a manually composed batch or an Altegio campaign is refused by NAME, with
      a reason an operator can read, rather than quietly filtered to nothing;
    * it must carry a frozen composition digest, which is what makes "the
      original mailing" a fixed thing to refer to;
    * the branch's own WhatsApp sender must still be configured and active, and
      its Meta identifier must digest to the route the batch's attempts froze.
      §46.1 keeps the repeat message on the original send geography — the
      Karlsruhe line and its confirmed sender — so a deployment repointed at
      another number cannot inherit this audience;
    * and a §45.4 terminal stop forbids the preview outright. That check is the
      reason this is a function and not a template condition: §46.1 says in so
      many words that hiding the button is not a check.
    """
    header = await session.get(EasyWeekVoucherProductionBatch, int(batch_id))
    if header is None:
        return BatchGate(False, BATCH_NOT_FOUND)
    if header.provider != PROVIDER_EASYWEEK:
        return BatchGate(False, BATCH_SCOPE_UNSUPPORTED)

    schema_version = str(header.request_schema_version or "")
    basis = str(header.recipient_basis or "")
    frozen_digest = str(header.frozen_digest or "")
    stop_active, generation = await production_ledger.stop_facts_in_session(session, batch_id=int(batch_id))
    terminal = stop_active and await production_ledger.terminal_stop_active_in_session(session, batch_id=int(batch_id))
    facts = {
        "batch_id": int(batch_id),
        "campaign_run_id": int(header.campaign_run_id or 0),
        "company_id": int(header.company_id or 0),
        "location_uuid": header.location_uuid,
        "schema_version": schema_version,
        "frozen_digest": frozen_digest,
        "recipient_basis": basis,
        "status": str(header.status or ""),
        "stop_generation": int(generation),
        "terminal_stop": bool(terminal),
        "recipient_count": int(header.recipient_count or 0),
    }

    if not is_current_fixed_contract(schema_version):
        return BatchGate(False, BATCH_SCHEMA_UNSUPPORTED, **facts)
    if basis not in SUPPORTED_RECIPIENT_BASES:
        return BatchGate(False, BATCH_SCOPE_UNSUPPORTED, **facts)
    if not frozen_digest:
        return BatchGate(False, BATCH_NOT_FROZEN, **facts)
    if terminal:
        return BatchGate(False, BATCH_TERMINAL_STOP, **facts)
    sender_digest = await _confirmed_sender_digest(session, company_id=int(header.company_id or 0))
    if sender_digest is None:
        return BatchGate(False, SENDER_UNPROVEN, **facts)
    return BatchGate(True, None, sender_digest=sender_digest, **facts)


async def _confirmed_sender_digest(session: AsyncSession, *, company_id: int) -> str | None:
    """The digest of this branch's configured, active WhatsApp sender.

    The DIGEST and never the identifier: a frozen attempt stored a one-way
    digest of the Meta sender id precisely so that nothing downstream has to
    hold the value, and this comparison keeps that property.

    ``None`` when the branch has no active sender or its identifier is empty,
    which is a missing prerequisite rather than a reason to carry on with
    whatever the settings happen to name.
    """
    from altegio_bot.campaigns.easyweek_voucher_production.dispatch import SENDER_CODE
    from altegio_bot.models.models import WhatsAppSender
    from altegio_bot.whatsapp_consent import sender_route_digest

    row = (
        await session.execute(
            select(WhatsAppSender)
            .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
            .where(WhatsAppSender.company_id == int(company_id))
            .where(WhatsAppSender.sender_code == SENDER_CODE)
        )
    ).scalar_one_or_none()
    if row is None or not row.is_active:
        return None
    return sender_route_digest((row.phone_number_id or "").strip() or None)


# ---------------------------------------------------------------------------
# The request and its result
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PreviewSpec:
    """Everything a caller is allowed to choose. Deliberately four fields."""

    batch_id: int
    groups: tuple[str, ...]
    min_interval_seconds: int
    operator_principal: str


@dataclass(frozen=True)
class PreviewRowView:
    """One row as a page may print it. No phone, no name, no code, no uuid."""

    slot: int
    group: str | None
    decision: str
    reason: str | None


@dataclass(frozen=True)
class PreviewOutcome:
    """What one explicit recalculation produced."""

    created: bool
    reason: str | None = None
    preview_id: int | None = None
    version: int | None = None
    composition_digest: str = ""
    included: int = 0
    excluded: int = 0
    unknown: int = 0
    checked_at: datetime | None = None
    valid_until: datetime | None = None
    rows: tuple[PreviewRowView, ...] = ()
    template: message.FollowupTemplateProof | None = None
    booking_reads: int = 0
    retryable_uncertainty: bool = False


@dataclass(frozen=True)
class StoredPreview:
    """A stored version, read back for display. Never modified by reading it."""

    preview_id: int
    batch_id: int
    version: int
    selected_groups: tuple[str, ...]
    min_interval_seconds: int
    template_code: str
    meta_template_name: str
    template_language: str
    template_category: str
    template_parameter_order: tuple[str, ...]
    template_body_digest: str
    template_meta_verified: bool
    contract_version: str
    failure_policy_version: str
    stop_generation: int
    checked_at: datetime
    valid_until: datetime
    included_count: int
    excluded_count: int
    unknown_count: int
    composition_digest: str
    operator_principal: str
    # The source batch's frozen state, as the snapshot recorded it.
    source_frozen_digest: str = ""
    evidence: dict[str, Any] = field(default_factory=dict)
    rows: tuple[PreviewRowView, ...] = ()

    def is_fresh(self, *, now: datetime | None = None) -> bool:
        """Is this snapshot still inside the validity window it recorded?

        A stale snapshot is not wrong, it is OLD — the checks behind it were
        true when they were taken. §46.6 asks the page to say which, so a check
        failure can never leave an old preview looking current.
        """
        moment = now or utcnow()
        return moment < self.valid_until


# ---------------------------------------------------------------------------
# Is a stored snapshot still the current answer? (§46.6)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class SnapshotCurrency:
    """How much of a stored preview may still be presented as current.

    Three separate things, kept apart because an operator acts on them
    differently and §46.6 asks the page to distinguish them:

    * ``fresh`` — the snapshot is inside the validity window it recorded. That
      is a statement about AGE and nothing else;
    * ``contract_matches`` — the message contract, the failure policy, the
      preview contract version and the source batch's frozen state are still
      the ones the snapshot was computed under. A snapshot computed under a
      different text is a historical record, however young it is;
    * ``blocked_now`` — something forbids a follow-up for this batch right now:
      a terminal stop, a sender that is gone, a template that is no longer
      proven. The old counts stay true as history and say nothing about today.

    ``current`` is the conjunction, and only that may be shown as "these checks
    hold". Everything else is shown as a past result that needs recomputing.
    """

    fresh: bool
    contract_matches: bool
    template_proven_now: bool
    blocked_now: str | None = None
    # Why the contract no longer matches. Safe labels, for the screen.
    drift: tuple[str, ...] = ()

    @property
    def current(self) -> bool:
        return self.fresh and self.contract_matches and self.template_proven_now and self.blocked_now is None

    @property
    def text_restorable(self) -> bool:
        """May today's text be shown AS the text of this snapshot?

        Only when the body digest still matches. A snapshot stores the digest
        and not the body — a body is personal once rendered and large once
        stored — so when the digest has moved the old text cannot be shown at
        all, and §46.6 requires saying so rather than substituting the new one.
        """
        return "template_body" not in self.drift

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "fresh": self.fresh,
            "contract_matches": self.contract_matches,
            "template_proven_now": self.template_proven_now,
            "blocked_now": self.blocked_now,
            "drift": list(self.drift),
            "current": self.current,
            "text_restorable": self.text_restorable,
        }


def assess_snapshot(
    stored: StoredPreview,
    *,
    gate: BatchGate,
    template_proven_now: bool,
    policy: FailurePolicy = CURRENT_FAILURE_POLICY,
    now: datetime | None = None,
) -> SnapshotCurrency:
    """Compare one stored snapshot with what is true right now.

    A pure comparison: it reads nothing and writes nothing, and it never
    modifies the snapshot — §46.5 keeps a stored version immutable, so this
    answers a question ABOUT it instead.
    """
    drift: list[str] = []
    if stored.template_body_digest != message.body_digest():
        drift.append("template_body")
    if stored.template_code != message.FOLLOWUP_TEMPLATE_CODE:
        drift.append("template_code")
    if stored.meta_template_name != message.FOLLOWUP_META_TEMPLATE_NAME:
        drift.append("template_name")
    if stored.template_language != message.FOLLOWUP_TEMPLATE_LANGUAGE:
        drift.append("template_language")
    if stored.template_category != message.FOLLOWUP_TEMPLATE_CATEGORY:
        drift.append("template_category")
    if tuple(stored.template_parameter_order) != message.FOLLOWUP_TEMPLATE_FIELDS:
        drift.append("template_parameters")
    if stored.contract_version != CONTRACT_VERSION:
        drift.append("contract_version")
    if stored.failure_policy_version != policy.version:
        drift.append("failure_policy")
    if gate.frozen_digest and stored.source_frozen_digest != gate.frozen_digest:
        drift.append("source_composition")
    if int(stored.stop_generation) != int(gate.stop_generation):
        # The original batch has been stopped (or stopped again) since. The
        # composition was computed in knowledge of a different stop.
        drift.append("source_stop_generation")

    return SnapshotCurrency(
        fresh=stored.is_fresh(now=now),
        contract_matches=not drift,
        template_proven_now=bool(template_proven_now),
        blocked_now=None if gate.allowed else gate.reason,
        drift=tuple(drift),
    )


# ---------------------------------------------------------------------------
# The message contract
# ---------------------------------------------------------------------------


async def local_template_row_proven(session: AsyncSession, *, company_id: int) -> bool:
    """Does this branch hold exactly one local row matching the contract?

    The local half on its own, so the Meta read can run with no database
    session open. A row that merely exists is not a contract: provider, company,
    code, language, the Meta name and the body all have to agree, and the row
    has to be active.
    """
    from altegio_bot.models.models import MessageTemplate

    rows = list(
        (
            await session.execute(
                select(MessageTemplate)
                .where(MessageTemplate.provider == PROVIDER_EASYWEEK)
                .where(MessageTemplate.company_id == int(company_id))
                .where(MessageTemplate.code == message.FOLLOWUP_TEMPLATE_CODE)
                .where(MessageTemplate.language == message.FOLLOWUP_TEMPLATE_LANGUAGE)
                .limit(2)
            )
        ).scalars()
    )
    return len(rows) == 1 and message.db_row_blocker(rows[0], company_id=int(company_id)) is None


async def prove_meta_followup_template(reader: Any | None = None) -> message.FollowupTemplateProof:
    """The live Meta half: is this exact template APPROVED right now?

    Required in THIS evaluation, because a template can be paused or disabled
    between two page loads and a previous proof is not evidence.

    Read-only by construction. The listing client has no create or modify
    capability, and §46.4 forbids this phase from creating or changing a Meta
    template under any circumstances. A transport failure is normalised to "not
    proven in this evaluation" rather than escaping as a 500, and nothing from
    the exception — no url, no header, no token, no response text — survives.
    """
    import httpx

    from altegio_bot.scripts.clone_meta_templates_for_location import MetaTemplateClient, ScriptError
    from altegio_bot.settings import settings

    try:
        if reader is not None and callable(getattr(reader, "list_meta_templates", None)):
            templates = await reader.list_meta_templates()
        else:
            token = settings.whatsapp_access_token.strip()
            waba_id = (settings.meta_waba_id or "").strip()
            if not token or not waba_id:
                return message.prove_meta_templates([])
            async with MetaTemplateClient(
                token=token,
                waba_id=waba_id,
                graph_url=settings.whatsapp_graph_url,
                api_version=settings.whatsapp_api_version,
                timeout_seconds=20.0,
            ) as client:
                templates = await client.list_templates()
        return message.prove_meta_templates(templates)
    except (ScriptError, httpx.HTTPError):
        return message.prove_meta_templates([])


# ---------------------------------------------------------------------------
# The composition digest
# ---------------------------------------------------------------------------


def composition_digest(
    *,
    batch_id: int,
    frozen_digest: str,
    groups: Sequence[str],
    min_interval_seconds: int,
    contract_version: str,
    failure_policy_version: str,
    template_body_digest: str,
    included: Sequence[tuple[int, str]],
) -> str:
    """A deterministic handle over WHO is in this version and under WHAT terms.

    Both halves are inside it on purpose. Two previews of the same people under
    a different interval, a different group selection, a different template or a
    different policy version are different proposals, and §46.5 requires each of
    them to be its own snapshot with its own digest.

    Deterministic across machines and runs: the included rows are sorted by slot
    and the material is one canonical JSON document with sorted keys. The rows
    contribute their slot and their entitlement digest — never a phone number, a
    name, a uuid or a MAC — so the digest can be logged and shown.
    """
    material = {
        "contract": contract_version,
        "failure_policy": failure_policy_version,
        "batch_id": int(batch_id),
        "source_frozen_digest": frozen_digest,
        "groups": list(groups),
        "min_interval_seconds": int(min_interval_seconds),
        "template_body_digest": template_body_digest,
        "included": [[int(slot), str(digest)] for slot, digest in sorted(included)],
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Phase 1 — the local read
# ---------------------------------------------------------------------------


async def _confirmed_failures(session: AsyncSession, *, batch_id: int) -> dict[int, tuple[FailureFact, ...]]:
    """Every CONFIRMED failure fact of this batch, by item id.

    Only ``confirmed``. An ``unmatched`` fact is about an identifier nobody owns
    yet, a ``conflicting`` one contradicts the slot it names, and an ``unproven``
    one is missing a proof §46.3 asks for. None of them says a slot's send
    failed, so none of them may put somebody in the third group.
    """
    rows = (
        await session.execute(
            select(EasyWeekVoucherProductionDeliveryFailure)
            .where(EasyWeekVoucherProductionDeliveryFailure.batch_id == int(batch_id))
            .where(EasyWeekVoucherProductionDeliveryFailure.match_state == DELIVERY_FAILURE_MATCH_CONFIRMED)
            .order_by(EasyWeekVoucherProductionDeliveryFailure.id.asc())
        )
    ).scalars()
    found: dict[int, list[FailureFact]] = {}
    for row in rows:
        if row.item_id is None:
            continue
        found.setdefault(int(row.item_id), []).append(
            FailureFact(
                match_state=str(row.match_state),
                failure_class=str(row.failure_class),
                meta_error_code=(int(row.meta_error_code) if row.meta_error_code is not None else None),
                reason_code=str(row.reason_code),
            )
        )
    return {item_id: tuple(facts) for item_id, facts in found.items()}


async def _proven_receipts(session: AsyncSession, *, batch_id: int) -> dict[int, set[str]]:
    """Which positive statuses of this batch carry §46.3's proof, by item id.

    A stored receipt IS the proof — the database requires the verified
    signature and both halves of a matching route — so reading the rows is
    enough and no verdict is recomputed here.
    """
    rows = (
        await session.execute(
            select(
                EasyWeekVoucherProductionDeliveryReceipt.item_id,
                EasyWeekVoucherProductionDeliveryReceipt.kind,
            ).where(EasyWeekVoucherProductionDeliveryReceipt.batch_id == int(batch_id))
        )
    ).all()
    proven: dict[int, set[str]] = {}
    for item_id, kind in rows:
        proven.setdefault(int(item_id), set()).add(str(kind))
    return proven


async def _attempts(session: AsyncSession, *, batch_id: int) -> dict[int, EasyWeekVoucherProductionBatchAttempt]:
    """The one attempt per slot, by item id.

    A slot may hold at most one send attempt by §45's own CHECK, so this picks
    the earliest claim and would be a single row in every correct case; the
    ordering makes the choice deterministic rather than incidental if a
    historical row ever broke that rule.
    """
    rows = (
        await session.execute(
            select(EasyWeekVoucherProductionBatchAttempt)
            .where(EasyWeekVoucherProductionBatchAttempt.batch_id == int(batch_id))
            .order_by(EasyWeekVoucherProductionBatchAttempt.id.asc())
        )
    ).scalars()
    found: dict[int, EasyWeekVoucherProductionBatchAttempt] = {}
    for row in rows:
        found.setdefault(int(row.item_id), row)
    return found


async def _batch_items(session: AsyncSession, *, batch_id: int) -> list[EasyWeekVoucherProductionBatchItem]:
    """Every slot of one batch, in slot order. One reader, so two callers agree."""
    return list(
        (
            await session.execute(
                select(EasyWeekVoucherProductionBatchItem)
                .where(EasyWeekVoucherProductionBatchItem.batch_id == int(batch_id))
                .order_by(EasyWeekVoucherProductionBatchItem.slot.asc())
            )
        ).scalars()
    )


async def _candidate_addresses(session: AsyncSession, *, batch_id: int) -> set[str]:
    """Every number this batch's recipients are known by, normalised.

    Both sides of the identity, because the §46.4.4 backlog is about PEOPLE and
    a number can have moved since the mailing: the campaign row's number, and
    the number on the EasyWeek card of each slot's proven uuid. A refusal sent
    from either is this person's refusal.

    One query pair for the whole batch. The result is the question the bounded
    stored-event scan is given, which is what keeps that scan narrow.
    """
    rows = list(
        (
            await session.execute(
                select(CampaignRecipient.phone_e164, EasyWeekVoucherProductionBatchItem.easyweek_customer_uuid)
                .join(
                    EasyWeekVoucherProductionBatchItem,
                    EasyWeekVoucherProductionBatchItem.campaign_recipient_id == CampaignRecipient.id,
                )
                .where(EasyWeekVoucherProductionBatchItem.batch_id == int(batch_id))
            )
        ).all()
    )
    addresses: set[str] = set()
    uuids: set[uuid_module.UUID] = set()
    for phone, customer_uuid in rows:
        normalised = normalize_wa_address(phone)
        if normalised is not None:
            addresses.add(normalised)
        if customer_uuid is not None:
            uuids.add(customer_uuid)
    if uuids:
        cards = (
            await session.execute(
                select(Client.phone_e164)
                .where(Client.provider == PROVIDER_EASYWEEK)
                .where(Client.easyweek_customer_uuid.in_(sorted(uuids)))
            )
        ).scalars()
        for phone in cards:
            normalised = normalize_wa_address(phone)
            if normalised is not None:
                addresses.add(normalised)
    return addresses


async def _local_phase(
    session: AsyncSession,
    *,
    gate: BatchGate,
    spec: PreviewSpec,
    policy: FailurePolicy,
    now: datetime,
    configured_key_id: str | None,
    historical: HistoricalConsentScan,
    creations: CapturedCreations,
) -> list[RowFacts]:
    """Decide everything that can be decided without a network request.

    ``historical`` is the one bounded pass over the stored-event backlog
    (§46.4.4), taken once for the whole preview rather than per row.
    """
    items = await _batch_items(session, batch_id=int(spec.batch_id))
    attempts = await _attempts(session, batch_id=int(spec.batch_id))
    failures = await _confirmed_failures(session, batch_id=int(spec.batch_id))
    receipts = await _proven_receipts(session, batch_id=int(spec.batch_id))

    decided: list[RowFacts] = []
    for item in items:
        attempt = attempts.get(int(item.id))
        evidence = SourceEvidence(
            item_id=int(item.id),
            slot=int(item.slot),
            status=str(item.status or ""),
            send_attempt_count=int(item.send_attempt_count or 0),
            send_attempted_at=item.send_attempted_at,
            delivered_at=item.delivered_at,
            read_at=item.read_at,
            item_message_id=item.provider_message_id,
            attempt_message_id=(attempt.provider_message_id if attempt is not None else None),
            attempt_sender_digest=(attempt.sender_phone_number_id_digest if attempt is not None else None),
            attempt_recipient_digest=(attempt.recipient_digest if attempt is not None else None),
            confirmed_failures=failures.get(int(item.id), ()),
            delivered_receipt_proven=DELIVERY_RECEIPT_KIND_DELIVERED in receipts.get(int(item.id), set()),
            read_receipt_proven=DELIVERY_RECEIPT_KIND_READ in receipts.get(int(item.id), set()),
        )
        group = resolve_group(evidence, expected_sender_digest=gate.sender_digest)
        facts = RowFacts(
            item_id=int(item.id),
            slot=int(item.slot),
            campaign_recipient_id=int(item.campaign_recipient_id),
            easyweek_customer_uuid=item.easyweek_customer_uuid,
            source_attempted_at=item.send_attempted_at,
            evidence=evidence,
            group=group,
            # Computed for every row, including the ones nothing is proven
            # about: it is a pure function of local ledger fields, and a stored
            # row has to carry a real handle rather than a placeholder.
            entitlement_digest=source_entitlement_digest(
                batch_id=int(spec.batch_id),
                slot=int(item.slot),
                voucher_code_hmac=item.voucher_code_hmac,
                hmac_key_id=item.hmac_key_id,
                target_order_uuid=item.target_order_uuid,
            ),
            recipient_digest=evidence.attempt_recipient_digest,
        )

        # A slot with no provable group is reported as unknown, whatever the
        # operator selected: §46.6 asks the page to account for the whole
        # original mailing rather than to show a shorter list than it has.
        if not group.proven:
            facts.unknown(group.reason or HISTORY_UNPROVEN)
            decided.append(facts)
            continue
        # A proven group the operator did not select is simply out of scope for
        # this preview and is not part of it. An empty selection is refused
        # before this function runs, so "nothing selected" never arrives here as
        # "everything selected".
        if group.group not in spec.groups:
            continue

        entitlement = entitlement_verdict(
            batch_id=int(spec.batch_id),
            slot=int(item.slot),
            status=str(item.status or ""),
            voucher_code_hmac=item.voucher_code_hmac,
            hmac_key_id=item.hmac_key_id,
            target_order_uuid=item.target_order_uuid,
            configured_key_id=configured_key_id,
        )
        assert entitlement.digest == facts.entitlement_digest

        identity = await resolve_identity(
            session,
            campaign_recipient_id=int(item.campaign_recipient_id),
            company_id=int(gate.company_id or 0),
            campaign_run_id=int(gate.campaign_run_id or 0),
            # The SLOT's own basis, not the header's: a mixed batch has both,
            # and each row is proven against the link its basis actually has.
            recipient_basis=str(item.recipient_basis or ""),
            expected_customer_uuid=item.easyweek_customer_uuid,
            expected_source_booking_uuid=item.source_booking_uuid,
            attempt_recipient_digest=evidence.attempt_recipient_digest,
        )
        if not identity.proven:
            facts.unknown(identity.reason or HISTORY_UNPROVEN)
            decided.append(facts)
            continue

        if not interval_elapsed(
            attempted_at=item.send_attempted_at,
            min_interval_seconds=int(spec.min_interval_seconds),
            now=now,
        ):
            facts.exclude(INTERVAL_NOT_ELAPSED)
            decided.append(facts)
            continue

        policy_refusal = failure_policy_reason(facts, policy=policy)
        if policy_refusal is not None:
            facts.exclude(policy_refusal, failure_policy=policy.version)
            decided.append(facts)
            continue

        if not entitlement.proven:
            assert entitlement.reason is not None
            facts.exclude(entitlement.reason)
            decided.append(facts)
            continue

        consent = await consent_snapshot(
            session,
            phone=identity.address,
            easyweek_customer_uuid=identity.customer_uuid,
        )
        facts.notes["consent_fingerprint"] = consent.fingerprint
        if not consent.allowed:
            assert consent.reason is not None
            facts.exclude(consent.reason, consent_sources=list(consent.sources), consent_gaps=list(consent.gaps))
            decided.append(facts)
            continue

        # §46.4.4: the historical backlog, before the audience is opened. The
        # aggregate above reads the ledger, the cards and the recipient
        # snapshot, and every one of those is silent about a refusal that is
        # still sitting unaccounted in a stored event.
        if not historical.complete:
            # Not established for anybody, so not permission for anybody.
            facts.unknown(HISTORICAL_CONSENT_UNPROVEN, historical_consent=list(historical.gaps))
            decided.append(facts)
            continue
        if historical.blocks(identity.address):
            facts.exclude(HISTORICAL_STOP_UNACCOUNTED)
            decided.append(facts)
            continue

        assert item.send_attempted_at is not None
        if await has_replied_since(
            session,
            address=str(identity.address),
            easyweek_customer_uuid=identity.customer_uuid,
            since=item.send_attempted_at,
        ):
            from altegio_bot.campaigns.easyweek_voucher_followup.identity import CUSTOMER_REPLIED

            facts.exclude(CUSTOMER_REPLIED)
            decided.append(facts)
            continue

        # Everything local is satisfied. The booking proof is the one thing
        # left, and it is the only reason this row costs a request.
        facts.notes["pending_booking_proof"] = True
        facts.notes["address"] = identity.address
        # What the external artifact proof needs, read here so that phase holds
        # no database handle. The schema is the SLOT's own: §46.6 forbids
        # applying a newer contract's conditions to a historical one.
        facts.notes["batch_id"] = int(spec.batch_id)
        facts.notes["schema_version"] = gate.schema_version
        facts.notes["target_order_uuid"] = str(item.target_order_uuid) if item.target_order_uuid else None
        facts.notes["reconciliation_marker"] = str(item.reconciliation_marker or "")
        # The local half of the booking question, resolved now and carried: the
        # creation capture is a database read and must not be taken while the
        # external read is in flight.
        facts.creations = creations.for_slot(int(item.slot), reason=BOOKING_CAPTURE_UNPROVEN)
        decided.append(facts)

    return decided


# ---------------------------------------------------------------------------
# Phase 2 — the one external step
# ---------------------------------------------------------------------------


async def _external_phase(
    candidates: Sequence[RowFacts],
    *,
    reader: CustomerHistoryReader | None,
    order_reader: Any | None,
    session_maker: async_sessionmaker[AsyncSession],
    now: datetime,
    read_bookings: Callable[..., Any] = _read_customer_bookings,
    prove_artifact: Callable[..., Any] = _prove_source_artifact,
) -> tuple[int, bool]:
    """The two external proofs, for every row that is still a candidate.

    The artifact first (§46.6) and the bookings second (§46.5), because a slot
    whose original voucher is gone needs no booking read at all.

    No transaction and no lock is held across either request. The MAC check
    inside the artifact proof opens its own short read session once the order
    has already come back, which is why ``session_maker`` is passed rather than
    a session.

    A missing reader is never an empty answer. Without a configured EasyWeek
    read every candidate becomes ``unknown``, which is what §46.5 and §46.6
    both demand: an API failure may not turn into "this person has no bookings"
    or into "this voucher is still fine".
    """
    reads = 0
    retryable = False
    for facts in candidates:
        if not facts.notes.get("pending_booking_proof"):
            continue
        # §46.6: the ORIGINAL artifact, read and proven rather than inferred
        # from the presence of its binding columns.
        artifact = await prove_artifact(
            order_reader,
            session_maker=session_maker,
            batch_id=int(facts.notes["batch_id"]),
            slot=int(facts.slot),
            schema_version=str(facts.notes["schema_version"]),
            target_order_uuid=facts.notes.get("target_order_uuid"),
            reconciliation_marker=str(facts.notes.get("reconciliation_marker") or ""),
            easyweek_customer_uuid=facts.easyweek_customer_uuid,
            now=now,
        )
        facts.notes["artifact"] = dict(artifact.observations)
        if not artifact.proven:
            assert artifact.reason is not None
            if artifact.unknown:
                facts.unknown(artifact.reason)
            else:
                facts.exclude(artifact.reason)
            continue

        if reader is None:
            facts.unknown(HISTORY_UNPROVEN, booking_proof="reader_unavailable")
            continue
        # Every candidate reached here through a proven attempt, so the anchor
        # the booking rules measure against exists. Asserted rather than
        # defaulted: a missing anchor would silently turn "booked since" into
        # "booked ever".
        assert facts.source_attempted_at is not None
        bookings: CustomerBookingSet = await read_bookings(
            reader,
            customer_uuid=facts.easyweek_customer_uuid,
        )
        reads += 1
        verdict: BookingVerdict = booking_verdict(
            bookings,
            creations=facts.creations,
            attempted_at=facts.source_attempted_at,
            now=now,
        )
        facts.notes["booking_pages_read"] = verdict.pages_read
        if verdict.proven:
            facts.include(booking_count=verdict.booking_count)
            continue
        assert verdict.reason is not None
        if verdict.unknown:
            retryable = retryable or bookings.retryable_uncertainty
            # The reason is the verdict's own: "the history could not be read"
            # and "this booking's creation is not proven" are different missing
            # facts, and an operator acts on them differently.
            facts.unknown(verdict.reason, booking_reason=verdict.detail or verdict.reason)
        else:
            facts.exclude(verdict.reason)
    return reads, retryable


# ---------------------------------------------------------------------------
# Phase 3 — the save
# ---------------------------------------------------------------------------


async def _recheck_and_dedupe(
    session: AsyncSession,
    *,
    gate: BatchGate,
    decided: Sequence[RowFacts],
    selected_groups: Sequence[str],
) -> str | None:
    """Re-prove the mutable local facts, then apply the dedupe contract.

    Everything checked here can have changed while the booking reads were in
    flight, and each one is a case §46.5 names:

    * a terminal stop on the original batch now forbids the preview entirely,
      so the whole save is refused rather than stored with a note;
    * consent may have moved — a STOP arrived, a receipt landed unresolved — so
      every included row's aggregate is asked again and compared against the
      fingerprint the first pass recorded. A row whose consent moved at all is
      demoted, even if the new answer still reads "allowed": the preview would
      otherwise carry a fingerprint that no longer describes it, and PR-22.3's
      locked guard compares exactly that;
    * a reply may have arrived after the first pass looked;
    * the SOURCE FACTS of the attempt may have moved. A ``read`` callback can
      land while the booking read is in flight, and a row proven ``delivered``
      a moment ago is then in a different group. §46.5 allows a move between
      two groups the operator ALREADY selected, provided the approved message
      contract is the same and the remaining conditions hold, and requires a
      new preview otherwise — so the actual group is recorded and a move out of
      the selection is an exclusion with a reason, never a quiet relabelling;
    * identity may have moved: a number reassigned, a card adopted by another
      uuid, a basis changed. Each one means this row is about somebody else now;
    * the SENDER and the local TEMPLATE row may have changed or been disabled.
      Those are contract facts shared by the whole preview rather than one row,
      so an incompatible change refuses the save instead of quietly storing a
      composition under a contract that no longer exists.

    A row that loses eligibility here is excluded with a reason and its place is
    not filled: §46.5 forbids adding anybody in its stead, and the stored
    composition may therefore be smaller than the first pass expected.

    Then one version's dedupe: at most one included row per source slot, per
    canonical customer uuid and per original entitlement. The partial unique
    indexes enforce the last two in the database, and this loop decides WHICH
    row keeps the place — the lowest slot, deterministically — and gives the
    other one a reason instead of letting an insert fail.

    Scoped to this version and no further. A preview must not be able to block
    somebody permanently, so nothing here looks at what an earlier version
    included.
    """
    if await production_ledger.terminal_stop_active_in_session(session, batch_id=int(gate.batch_id or 0)):
        return BATCH_TERMINAL_STOP

    # The contract facts the whole preview rests on, re-proven once. A change
    # here is not a per-row exclusion: the composition would be stored under a
    # message contract or a sender that no longer holds, so the save is refused
    # and an explicit recalculation is required.
    if not await local_template_row_proven(session, company_id=int(gate.company_id or 0)):
        return TEMPLATE_UNPROVEN
    sender_now = await _confirmed_sender_digest(session, company_id=int(gate.company_id or 0))
    if sender_now is None or sender_now != gate.sender_digest:
        return SENDER_UNPROVEN

    # The source facts, re-read for the whole batch in one pass rather than per
    # row: a late callback is the case this exists for, and it moves rows.
    items = {int(item.id): item for item in await _batch_items(session, batch_id=int(gate.batch_id or 0))}
    attempts = await _attempts(session, batch_id=int(gate.batch_id or 0))
    failures = await _confirmed_failures(session, batch_id=int(gate.batch_id or 0))
    receipts = await _proven_receipts(session, batch_id=int(gate.batch_id or 0))

    seen_customers: set[uuid_module.UUID] = set()
    seen_entitlements: set[str] = set()
    for facts in sorted(decided, key=lambda row: row.slot):
        if not facts.included:
            continue

        item = items.get(facts.item_id)
        if item is None:
            facts.unknown(IDENTITY_CONFLICT, recheck="source_row_missing")
            continue

        attempt = attempts.get(facts.item_id)
        evidence = SourceEvidence(
            item_id=facts.item_id,
            slot=facts.slot,
            status=str(item.status or ""),
            send_attempt_count=int(item.send_attempt_count or 0),
            send_attempted_at=item.send_attempted_at,
            delivered_at=item.delivered_at,
            read_at=item.read_at,
            item_message_id=item.provider_message_id,
            attempt_message_id=(attempt.provider_message_id if attempt is not None else None),
            attempt_sender_digest=(attempt.sender_phone_number_id_digest if attempt is not None else None),
            attempt_recipient_digest=(attempt.recipient_digest if attempt is not None else None),
            confirmed_failures=failures.get(facts.item_id, ()),
            delivered_receipt_proven=DELIVERY_RECEIPT_KIND_DELIVERED in receipts.get(facts.item_id, set()),
            read_receipt_proven=DELIVERY_RECEIPT_KIND_READ in receipts.get(facts.item_id, set()),
        )
        group_now = resolve_group(evidence, expected_sender_digest=gate.sender_digest)
        if not group_now.proven:
            facts.unknown(group_now.reason or STATUS_EVIDENCE_UNPROVEN, recheck="source_facts_changed")
            continue
        if group_now.group != facts.group.group:
            if group_now.group not in selected_groups:
                # Moved out of what the operator chose. Not this preview's
                # audience any more, and not silently relabelled either.
                facts.exclude(GROUP_MOVED_OUT_OF_SELECTION, recheck="source_facts_changed")
                continue
            if message.template_code_for_groups(sorted({group_now.group, facts.group.group})) is None:
                # Two selected groups whose approved texts differ. §46.5 allows
                # the move only under one contract, so the save is refused.
                return TEMPLATE_UNPROVEN
            # Allowed, and the ACTUAL group is what gets stored.
            facts.group = group_now
        if int(item.send_attempt_count or 0) != int(facts.evidence.send_attempt_count or 0) or (
            item.send_attempted_at != facts.source_attempted_at
        ):
            facts.unknown(SOURCE_ATTEMPT_UNPROVEN, recheck="source_attempt_changed")
            continue

        identity = await resolve_identity(
            session,
            campaign_recipient_id=facts.campaign_recipient_id,
            company_id=int(gate.company_id or 0),
            campaign_run_id=int(gate.campaign_run_id or 0),
            recipient_basis=str(item.recipient_basis or ""),
            expected_customer_uuid=item.easyweek_customer_uuid,
            expected_source_booking_uuid=item.source_booking_uuid,
            attempt_recipient_digest=evidence.attempt_recipient_digest,
        )
        if not identity.proven or identity.address != facts.notes.get("address"):
            facts.unknown(identity.reason or IDENTITY_CONFLICT, recheck="identity_changed")
            continue

        address = facts.notes.get("address")
        consent = await consent_snapshot(
            session,
            phone=address,
            easyweek_customer_uuid=facts.easyweek_customer_uuid,
        )
        if not consent.allowed:
            assert consent.reason is not None
            facts.exclude(consent.reason, recheck="after_external_reads")
            continue
        if consent.fingerprint != facts.notes.get("consent_fingerprint"):
            from altegio_bot.campaigns.easyweek_voucher_followup.identity import CONSENT_UNKNOWN

            facts.unknown(CONSENT_UNKNOWN, recheck="consent_state_changed")
            continue

        assert facts.source_attempted_at is not None
        if await has_replied_since(
            session,
            address=str(address),
            easyweek_customer_uuid=facts.easyweek_customer_uuid,
            since=facts.source_attempted_at,
        ):
            from altegio_bot.campaigns.easyweek_voucher_followup.identity import CUSTOMER_REPLIED

            facts.exclude(CUSTOMER_REPLIED, recheck="after_external_reads")
            continue

        if facts.easyweek_customer_uuid in seen_customers or facts.entitlement_digest in seen_entitlements:
            facts.exclude(DUPLICATE_SOURCE)
            continue
        seen_customers.add(facts.easyweek_customer_uuid)
        seen_entitlements.add(facts.entitlement_digest)

    return None


async def _next_version(session: AsyncSession, *, batch_id: int) -> int:
    """The next version number for this batch. Existing rows are never touched."""
    current = (
        await session.execute(
            select(func.max(EasyWeekVoucherFollowupPreview.version)).where(
                EasyWeekVoucherFollowupPreview.batch_id == int(batch_id)
            )
        )
    ).scalar()
    return int(current or 0) + 1


def _row_view(facts: RowFacts) -> PreviewRowView:
    return PreviewRowView(
        slot=facts.slot,
        group=facts.group.group,
        decision=facts.decision,
        reason=facts.reason,
    )


# ---------------------------------------------------------------------------
# The orchestrator
# ---------------------------------------------------------------------------


def spec_blocker(spec: PreviewSpec) -> str | None:
    """Why this request cannot be served, from its own parameters alone.

    Both refusals are §46.3's, and both are mandatory rather than defaults:

    * nothing is selected by default, an empty selection is NOT "all three",
      and a selection naming only groups that do not exist is empty;
    * the minimum interval has to be given explicitly, in seconds, positive. No
      three days, no seven days, no "whatever the last preview used": §46.3
      forbids inventing a waiting time, and an absent, negative or unparseable
      one forbids the preview rather than falling back to a number nobody chose.
    """
    if not spec.groups or canonical_groups(spec.groups) != spec.groups:
        return GROUPS_REQUIRED
    if int(spec.min_interval_seconds) <= 0:
        return INTERVAL_REQUIRED
    return None


async def create_preview(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    spec: PreviewSpec,
    booking_reader: CustomerHistoryReader | None,
    order_reader: Any | None = None,
    template_reader: Any | None = None,
    policy: FailurePolicy = CURRENT_FAILURE_POLICY,
    now: datetime | None = None,
    read_bookings: Callable[..., Any] = _read_customer_bookings,
    prove_artifact: Callable[..., Any] = _prove_source_artifact,
    historical_event_limit: int = HISTORICAL_SCAN_LIMIT,
) -> PreviewOutcome:
    """Compute one new immutable preview version, or say why there is none.

    The explicit, protected action of §46.6. It reads the original mailing,
    judges every candidate, and stores what it found. It sends nothing, claims
    nothing and spends nothing, and running it again produces another version
    beside this one rather than changing it.
    """
    moment = now or utcnow()
    blocker = spec_blocker(spec)
    if blocker is not None:
        return PreviewOutcome(False, blocker)

    # Phase 1: one read transaction, released before anything external.
    async with session_maker() as session:
        gate = await batch_gate(session, batch_id=int(spec.batch_id))
        if not gate.allowed:
            return PreviewOutcome(False, gate.reason)
        local_template = await local_template_row_proven(session, company_id=int(gate.company_id or 0))
        configured_key_id = _configured_binding_key_id()
        # §46.4.4, once for the whole preview: one bounded, read-only pass over
        # the stored events nothing has accounted for, asked only about the
        # numbers this batch's recipients are known by. It writes nothing and
        # runs no reconciliation.
        historical = await scan_historical_refusals(
            session,
            addresses=await _candidate_addresses(session, batch_id=int(spec.batch_id)),
            limit=int(historical_event_limit),
            now=moment,
        )
        # The booking CREATION capture (§46.5), also local and also once for
        # the whole batch. Separate from the live read on purpose: creation
        # times come from stored deliveries, appointment times from the live
        # history, and collapsing the two is the defect this closes.
        creations = await load_creation_evidence(
            session,
            items=await _batch_items(session, batch_id=int(spec.batch_id)),
            now=moment,
            unproven_reason=BOOKING_CAPTURE_UNPROVEN,
        )
        decided = await _local_phase(
            session,
            gate=gate,
            spec=spec,
            policy=policy,
            now=moment,
            configured_key_id=configured_key_id,
            historical=historical,
            creations=creations,
        )

    # An unproven message contract blocks the whole preview. §46.4 forbids
    # substituting free text for an approved template, so there is nothing to
    # propose and the composition is not computed into a stored version.
    if not local_template:
        return PreviewOutcome(
            False,
            TEMPLATE_UNPROVEN,
            template=message.FollowupTemplateProof(proven=False, reason=TEMPLATE_UNPROVEN),
        )
    # The Meta read is external, so it happens here with no session open.
    template = await prove_meta_followup_template(template_reader)
    if not template.proven:
        return PreviewOutcome(False, TEMPLATE_UNPROVEN, template=template)

    # Phase 2: the only network step, with no session open.
    reads, retryable = await _external_phase(
        decided,
        reader=booking_reader,
        order_reader=order_reader if order_reader is not None else booking_reader,
        session_maker=session_maker,
        now=moment,
        read_bookings=read_bookings,
        prove_artifact=prove_artifact,
    )

    # Phase 3: one write transaction.
    async with session_maker() as session:
        async with session.begin():
            refusal = await _recheck_and_dedupe(session, gate=gate, decided=decided, selected_groups=spec.groups)
            if refusal is not None:
                return PreviewOutcome(False, refusal, template=template)

            included = [(facts.slot, facts.entitlement_digest) for facts in decided if facts.included]
            digest = composition_digest(
                batch_id=int(spec.batch_id),
                frozen_digest=gate.frozen_digest,
                groups=spec.groups,
                min_interval_seconds=int(spec.min_interval_seconds),
                contract_version=CONTRACT_VERSION,
                failure_policy_version=policy.version,
                template_body_digest=message.body_digest(),
                included=included,
            )
            counts = {
                FOLLOWUP_ROW_INCLUDED: 0,
                FOLLOWUP_ROW_EXCLUDED: 0,
                FOLLOWUP_ROW_UNKNOWN: 0,
            }
            for facts in decided:
                counts[facts.decision] = counts.get(facts.decision, 0) + 1

            version = await _next_version(session, batch_id=int(spec.batch_id))
            snapshot = EasyWeekVoucherFollowupPreview(
                batch_id=int(spec.batch_id),
                campaign_run_id=int(gate.campaign_run_id or 0),
                company_id=int(gate.company_id or 0),
                location_uuid=gate.location_uuid,
                source_request_schema_version=gate.schema_version,
                source_frozen_digest=gate.frozen_digest,
                stop_generation=int(gate.stop_generation),
                version=version,
                selected_groups=list(spec.groups),
                min_interval_seconds=int(spec.min_interval_seconds),
                template_code=template.template_code,
                meta_template_name=template.meta_template_name,
                template_language=template.language,
                template_category=template.category,
                template_parameter_order=list(message.FOLLOWUP_TEMPLATE_FIELDS),
                template_body_digest=message.body_digest(),
                template_meta_verified=bool(template.meta_verified),
                contract_version=CONTRACT_VERSION,
                failure_policy_version=policy.version,
                checked_at=moment,
                valid_until=moment + timedelta(seconds=PREVIEW_VALIDITY_SECONDS),
                included_count=counts[FOLLOWUP_ROW_INCLUDED],
                excluded_count=counts[FOLLOWUP_ROW_EXCLUDED],
                unknown_count=counts[FOLLOWUP_ROW_UNKNOWN],
                composition_digest=digest,
                operator_principal=str(spec.operator_principal or "")[:128],
                evidence={
                    "historical_consent": historical.as_safe_dict(),
                    "booking_creation_capture": creations.as_safe_dict(),
                    "booking_reads": int(reads),
                    "retryable_uncertainty": bool(retryable),
                    "source_status": gate.status,
                    "recipient_basis": gate.recipient_basis,
                    "recipient_count": int(gate.recipient_count),
                },
            )
            session.add(snapshot)
            await session.flush()

            for facts in decided:
                session.add(
                    EasyWeekVoucherFollowupPreviewRow(
                        preview_id=int(snapshot.id),
                        source_batch_id=int(spec.batch_id),
                        source_item_id=facts.item_id,
                        source_slot=facts.slot,
                        campaign_recipient_id=facts.campaign_recipient_id,
                        easyweek_customer_uuid=facts.easyweek_customer_uuid,
                        source_entitlement_digest=facts.entitlement_digest,
                        recipient_digest=facts.recipient_digest,
                        source_attempted_at=facts.source_attempted_at,
                        group_code=facts.group.group or FOLLOWUP_GROUP_UNPROVEN,
                        decision=facts.decision,
                        reason_code=facts.reason,
                        evidence=_safe_notes(facts),
                    )
                )
            await session.flush()
            outcome = PreviewOutcome(
                created=True,
                preview_id=int(snapshot.id),
                version=version,
                composition_digest=digest,
                included=counts[FOLLOWUP_ROW_INCLUDED],
                excluded=counts[FOLLOWUP_ROW_EXCLUDED],
                unknown=counts[FOLLOWUP_ROW_UNKNOWN],
                checked_at=moment,
                valid_until=snapshot.valid_until,
                rows=tuple(_row_view(facts) for facts in sorted(decided, key=lambda row: row.slot)),
                template=template,
                booking_reads=int(reads),
                retryable_uncertainty=bool(retryable),
            )
    logger.info(
        "followup_preview: batch=%s version=%s included=%s excluded=%s unknown=%s digest=%s",
        int(spec.batch_id),
        version,
        outcome.included,
        outcome.excluded,
        outcome.unknown,
        digest[:12],
    )
    return outcome


_SAFE_NOTE_KEYS = frozenset(
    {
        "booking_count",
        "booking_pages_read",
        "booking_proof",
        "booking_reason",
        "artifact",
        "consent_gaps",
        "consent_sources",
        "failure_policy",
        "recheck",
    }
)


def _safe_notes(facts: RowFacts) -> dict[str, Any]:
    """Only labels and shapes reach the stored row.

    The address and the consent fingerprint are working values of the
    computation, not evidence to keep: one is personal data and the other is a
    handle over it. Both are dropped here rather than filtered at the far end,
    so no later reader of this table has to know they ever existed.
    """
    return {key: value for key, value in facts.notes.items() if key in _SAFE_NOTE_KEYS}


def _configured_binding_key_id() -> str | None:
    """The voucher MAC key id this deployment currently holds, or ``None``.

    Only the identifier. The secret itself is never read here: a preview has
    nothing to sign and nothing to verify, and it only needs to know whether the
    key that bound an artifact is still the configured one.
    """
    from altegio_bot.campaigns.easyweek_voucher_delivery.binding import (
        VoucherBindingKeyError,
        load_binding_key,
    )

    try:
        identifier, _secret = load_binding_key()
    except VoucherBindingKeyError:
        return None
    return identifier


# ---------------------------------------------------------------------------
# Reading a stored version back
# ---------------------------------------------------------------------------


async def latest_preview(
    session: AsyncSession,
    *,
    batch_id: int,
    version: int | None = None,
) -> StoredPreview | None:
    """Read one stored version. Reading it changes nothing about it.

    The current version is the highest one; an explicit ``version`` reads an
    older proposal exactly as it was computed, which is the point of storing
    them rather than overwriting.
    """
    query = select(EasyWeekVoucherFollowupPreview).where(EasyWeekVoucherFollowupPreview.batch_id == int(batch_id))
    if version is not None:
        query = query.where(EasyWeekVoucherFollowupPreview.version == int(version))
    row = (
        await session.execute(query.order_by(EasyWeekVoucherFollowupPreview.version.desc()).limit(1))
    ).scalar_one_or_none()
    if row is None:
        return None
    rows = (
        await session.execute(
            select(EasyWeekVoucherFollowupPreviewRow)
            .where(EasyWeekVoucherFollowupPreviewRow.preview_id == int(row.id))
            .order_by(EasyWeekVoucherFollowupPreviewRow.source_slot.asc())
        )
    ).scalars()
    return StoredPreview(
        preview_id=int(row.id),
        batch_id=int(row.batch_id),
        version=int(row.version),
        selected_groups=tuple(row.selected_groups or ()),
        min_interval_seconds=int(row.min_interval_seconds),
        template_code=str(row.template_code),
        meta_template_name=str(row.meta_template_name),
        template_language=str(row.template_language),
        template_category=str(row.template_category),
        template_parameter_order=tuple(row.template_parameter_order or ()),
        template_body_digest=str(row.template_body_digest),
        template_meta_verified=bool(row.template_meta_verified),
        contract_version=str(row.contract_version),
        failure_policy_version=str(row.failure_policy_version),
        stop_generation=int(row.stop_generation or 0),
        checked_at=row.checked_at,
        valid_until=row.valid_until,
        included_count=int(row.included_count),
        excluded_count=int(row.excluded_count),
        unknown_count=int(row.unknown_count),
        composition_digest=str(row.composition_digest),
        operator_principal=str(row.operator_principal or ""),
        source_frozen_digest=str(row.source_frozen_digest or ""),
        evidence=dict(row.evidence or {}),
        rows=tuple(
            PreviewRowView(
                slot=int(item.source_slot),
                group=str(item.group_code),
                decision=str(item.decision),
                reason=item.reason_code,
            )
            for item in rows
        ),
    )
