"""Preparing a migration wave: collect, propose, confirm, create. Then stop.

The problem
-----------
Migrating one master's future bookings took an afternoon, and almost none of it
was the migration. It was the preparation: finding which services the bookings
use, finding the matching EasyWeek services by eye, regenerating a customer
export, working out which customers are missing, creating them by hand, and
carrying identifiers between commands in a text editor. The migrator itself —
inventory, dry-run, canary, apply, reconcile — worked. Everything in front of it
was manual.

This module is that front half, and nothing else. It produces exactly the
artefacts the existing migrator already reads:

* a **manifest** with the confirmed service mappings merged in, additively;
* a **customer directory** built from the live workspace rather than a
  spreadsheet;
* a **report** that says, separately, what is ready and what is not;
* a **verified dry-run id** it obtained itself, by running the dry-run and taking
  the digest off the report object — never by picking the newest file in a
  directory.

It does not migrate a booking, and it contains no second migrator. The canary,
the apply gate, the ledger, the reproof, the notification attestations and the
cumulative-wave contract are untouched and still ahead of every write.

Four commands, three of which cannot change anything
----------------------------------------------------
``prepare``          reads Altegio, reads the EasyWeek catalogue, reads the
                     workspace's customers, writes local files. No CRM writes.
``confirm``          records what a person agreed to. Local files only.
``create-customers`` the ONLY command in this module that mutates EasyWeek, and
                     the only one that needs its own permission.
``verify-dry-run``   runs the existing dry-run and hands back its digest.

The split is the permission boundary. Creating customer cards and migrating
bookings are different powers and are granted separately: this module's write
command cannot create a booking, and the migrator's ``--apply`` cannot create a
customer.

Two kinds of output
-------------------
The machine report holds ids, UUIDs, counts and stable codes — no names, no
phone numbers, no e-mail addresses, no response bodies. The operator report holds
the names, numbers, exact service names and local times a person needs in order
to decide anything at all. They are different files, the second is written 0600
into a directory that is not the repository, and neither is committed.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
from collections import defaultdict
from dataclasses import dataclass, field, replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Final
from zoneinfo import ZoneInfo

from altegio_bot.easyweek_migration.altegio_source import build_window, fetch_company_records
from altegio_bot.easyweek_migration.branch_identity import verify_branch_identity
from altegio_bot.easyweek_migration.classify import (
    BLOCK_CUSTOM_DURATION,
    BLOCK_CUSTOM_PRICE,
    BLOCK_DURATION_UNKNOWN,
    BLOCK_MULTI_SERVICE,
    BLOCK_NO_RECORD_ID,
    BLOCK_NO_SERVICES,
    BLOCK_PRICE_BASELINE_MISSING,
    BLOCK_PRICE_MALFORMED,
    BLOCK_SERVICE_ID_INVALID,
    BLOCK_SERVICE_MAPPING_MISSING,
    BLOCK_STAFF_MAPPING_MISSING,
    BLOCK_STATUS_UNRECOGNISED,
    BLOCKED,
    READY,
    SKIPPED,
    classify_record,
)
from altegio_bot.easyweek_migration.customer_api import (
    LOOKUP_ABSENT,
    LOOKUP_AMBIGUOUS,
    LOOKUP_FIRST_NAME_MISSING,
    LOOKUP_FOUND,
    LOOKUP_PHONE_UNUSABLE,
    LOOKUP_UNDETERMINED,
    CustomerLookup,
    CustomerLookupUndetermined,
    lookup_customer_by_phone,
    phone_fingerprint,
    silence_http_request_logs,
    verify_customer,
)
from altegio_bot.easyweek_migration.customer_decisions import (
    STATE_BLOCKED,
    STATE_CONFIRMED,
    STATE_CREATED,
    STATE_IN_FLIGHT,
    STATE_PENDING,
    STATE_SKIPPED,
    CustomerDecision,
    CustomerDecisionStore,
    DecisionSet,
)
from altegio_bot.easyweek_migration.customer_overrides import (
    OVERRIDE_STALE_IDENTITY,
    CustomerOverride,
    CustomerOverrideStore,
    source_identity_digest,
)
from altegio_bot.easyweek_migration.customers import (
    CUSTOMER_AMBIGUOUS,
    CUSTOMER_FIRST_NAME_MISSING,
    CUSTOMER_NOT_FOUND,
    CUSTOMER_PHONE_UNUSABLE,
    CustomerDirectory,
    normalized_international_phone,
)
from altegio_bot.easyweek_migration.cutover import Cutover, LocalTimeError, parse_altegio_local_to_utc
from altegio_bot.easyweek_migration.manifest import BranchMapping, MigrationManifest
from altegio_bot.easyweek_migration.mapping_proposal import (
    MappingAgreement,
    ServiceProposal,
    collect_source_services,
    manifest_service_patch,
    merge_manifest_services,
    proposal_digest,
    propose_service_mapping,
    read_service_staff_uuids,
    whole_minutes,
)
from altegio_bot.easyweek_migration.service_catalog import (
    CatalogSnapshot,
    ServiceEvidenceError,
    read_full_catalog_rows,
)
from altegio_bot.easyweek_migration.write_client import (
    EasyWeekAuthError,
    EasyWeekError,
    EasyWeekPermanentError,
    EasyWeekUncertainMutation,
)

logger = logging.getLogger("easyweek_migration.prepare")

MODE_PREPARE: Final = "prepare"
MODE_CONFIRM: Final = "confirm"
MODE_CREATE_CUSTOMERS: Final = "create-customers"
MODE_VERIFY_DRY_RUN: Final = "verify-dry-run"

BERLIN: Final = ZoneInfo("Europe/Berlin")

# File names inside the state directory. All of them are operator artefacts and
# none of them is committed; the runbook says where the directory lives.
FILE_OPERATOR_REVIEW: Final = "operator_review.json"
FILE_MACHINE_REPORT: Final = "prepare_report.json"
FILE_MAPPING_AGREEMENT: Final = "mapping_agreement.json"
FILE_CUSTOMER_DIRECTORY: Final = "customer_directory.json"
FILE_MANIFEST_PROPOSED: Final = "manifest.proposed.json"

FILE_MODE: Final = 0o600
DIR_MODE: Final = 0o700

# Blocks the preparation stage exists to clear. Anything else blocking a row is
# a per-booking difference — a stretched slot, a discount, two services on one
# appointment — that this stage must not touch and must report as manual work.
PREPARABLE_BLOCKS: Final = frozenset(
    {
        BLOCK_SERVICE_MAPPING_MISSING,
        BLOCK_STAFF_MAPPING_MISSING,
        CUSTOMER_NOT_FOUND,
        CUSTOMER_AMBIGUOUS,
        CUSTOMER_PHONE_UNUSABLE,
        CUSTOMER_FIRST_NAME_MISSING,
    }
)

# Why a customer cannot be proposed for creation. Stable, safe to print.
BLOCK_NAME_NOT_SPLIT: Final = "source_name_not_split"
BLOCK_NAME_MISSING: Final = "source_name_missing"
BLOCK_SHARED_PHONE: Final = "source_customers_share_phone"
BLOCK_LOOKUP_UNDETERMINED: Final = "lookup_undetermined"
BLOCK_ALREADY_EXISTS: Final = "customer_already_exists"
BLOCK_CREATE_UNCERTAIN: Final = "create_outcome_unknown"
BLOCK_CREATE_REJECTED: Final = "create_rejected_by_workspace"
# Distinct from a rejected customer: nothing is wrong with the person's data,
# the key cannot write. Conflating them would send an operator off to edit a
# name that was never the problem.
BLOCK_CREATE_ACCESS_DENIED: Final = "create_access_denied"
BLOCK_CREATE_UNVERIFIED: Final = "create_not_verified"


class PrepareError(Exception):
    """The stage refuses to continue. Message is a code, never a payload."""


# ---------------------------------------------------------------------------
# Inputs
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PrepareInputs:
    """Everything a preparation command needs, already validated by the CLI."""

    mode: str
    run_id: str
    state_dir: Path
    manifest: MigrationManifest
    manifest_json: dict[str, Any]
    altegio_company_id: int
    cutover: Cutover
    horizon_days: int
    create_allowed: bool = False


@dataclass
class SourceCustomer:
    """One person, as the in-scope Altegio bookings describe them."""

    phone: str
    first_name: str | None = None
    last_name: str | None = None
    full_name: str | None = None
    email: str | None = None
    record_ids: list[int] = field(default_factory=list)
    source_client_ids: set[Any] = field(default_factory=set)

    @property
    def linked_record_count(self) -> int:
        return len(self.record_ids)

    @property
    def shares_phone(self) -> bool:
        """Two different Altegio customers on one number.

        Never merged: a shared family phone is one number and two people, and
        booking both onto one card puts one person's appointments into the
        other's history.
        """
        return len(self.source_client_ids) > 1


# ---------------------------------------------------------------------------
# Reading the source
# ---------------------------------------------------------------------------


def _client_of(record: dict[str, Any]) -> dict[str, Any]:
    client = record.get("client")
    return client if isinstance(client, dict) else {}


def _text(raw: object) -> str | None:
    if not isinstance(raw, str):
        return None
    collapsed = " ".join(raw.split())
    return collapsed or None


def collect_source_customers(records: list[dict[str, Any]]) -> dict[str, SourceCustomer]:
    """Group the in-scope bookings by customer, once per person.

    Keyed by the normalised international number, which is also what the lookup
    and the creation are keyed by — so one person is asked about once and every
    booking of theirs is linked to that one answer, however many they have.

    Names are read exactly as Altegio wrote them and are never derived. A source
    that gives only a full name yields ``full_name`` and no first name; splitting
    it here is how "Anna Maria" becomes "Anna" and a double surname becomes
    nonsense, and it would be written into a customer card as fact.
    """
    grouped: dict[str, SourceCustomer] = {}
    for record in records:
        client = _client_of(record)
        phone = normalized_international_phone(client.get("phone"))
        if phone is None:
            continue
        entry = grouped.get(phone)
        if entry is None:
            entry = SourceCustomer(phone=phone)
            grouped[phone] = entry

        first = _text(client.get("first_name") or client.get("name_first"))
        last = _text(client.get("last_name") or client.get("surname"))
        full = _text(client.get("name"))
        if first and not entry.first_name:
            entry.first_name = first
        if last and not entry.last_name:
            entry.last_name = last
        if full and not entry.full_name:
            entry.full_name = full
        email = _text(client.get("email"))
        if email and not entry.email:
            entry.email = email

        record_id = record.get("id")
        if type(record_id) is int and record_id not in entry.record_ids:
            entry.record_ids.append(record_id)
        client_id = client.get("id")
        if client_id is not None:
            entry.source_client_ids.add(client_id)
    return grouped


# How a booking would have to be handled, and WHY. The distinction the vocabulary
# exists to keep: "the EasyWeek API has no proven way to express this" is a
# different fact from "the source data cannot be proven", and collapsing them
# sends an operator to fix the wrong thing.
#
# None of these except `automatic` is an automatic path. Nothing here claims the
# EasyWeek API supports a custom duration, a custom price or a multi-service
# cart: no canary has proven any of those, so `cart_candidate` and
# `manual_adjustment_candidate` name what a PERSON would have to do, not what
# the tool will do.
CLASS_AUTOMATIC: Final = "automatic"
CLASS_CART_CANDIDATE: Final = "cart_candidate"
CLASS_MANUAL_ADJUSTMENT: Final = "manual_adjustment_candidate"
CLASS_FULLY_MANUAL: Final = "fully_manual"
CLASS_BLOCKED_UNPROVEN: Final = "blocked_unproven"

# The API cannot express it. A person recreates the booking in EasyWeek by hand.
_API_LIMIT_CLASSES: Final = {
    BLOCK_MULTI_SERVICE: CLASS_CART_CANDIDATE,
    BLOCK_CUSTOM_DURATION: CLASS_MANUAL_ADJUSTMENT,
    BLOCK_CUSTOM_PRICE: CLASS_MANUAL_ADJUSTMENT,
}
# The DATA cannot be proven. A person fixes the source, and it may then migrate
# automatically after all.
_UNPROVEN_DATA_BLOCKS: Final = frozenset(
    {
        BLOCK_DURATION_UNKNOWN,
        BLOCK_PRICE_MALFORMED,
        BLOCK_PRICE_BASELINE_MISSING,
        BLOCK_NO_SERVICES,
        BLOCK_SERVICE_ID_INVALID,
        BLOCK_STATUS_UNRECOGNISED,
        BLOCK_NO_RECORD_ID,
    }
)


def handling_class(block_reason: str | None) -> str:
    """Which of the five kinds of work this booking needs.

    Reporting only. It changes no decision and authorises no path; it exists so
    an operator reading "12 records need work" can tell which of them need a
    data fix and which need a person in the EasyWeek interface.
    """
    if block_reason is None:
        return CLASS_AUTOMATIC
    if block_reason in _API_LIMIT_CLASSES:
        return _API_LIMIT_CLASSES[block_reason]
    if block_reason in _UNPROVEN_DATA_BLOCKS:
        return CLASS_BLOCKED_UNPROVEN
    if block_reason in PREPARABLE_BLOCKS:
        # This stage's own job — a mapping or a customer it is about to resolve.
        return CLASS_AUTOMATIC
    return CLASS_FULLY_MANUAL


def operator_record_row(record: dict[str, Any], *, block_reason: str | None) -> dict[str, Any]:
    """One booking as the operator has to check it against Altegio.

    Local wall-clock in Europe/Berlin — the only clock the salon and the customer
    ever use — alongside the UTC instant the migration actually writes. Both,
    because a runbook read at 02:00 CET during a DST change with only one of them
    is a runbook that cannot be checked.

    A time that cannot be resolved to a single instant (the autumn fold, the
    spring gap) is reported as unreadable rather than approximated: the row would
    be blocked by the classifier for the same reason.
    """
    services = record.get("services")
    service = services[0] if isinstance(services, list) and services and isinstance(services[0], dict) else {}
    # THE APPOINTMENT's own length, from the top-level field. The service line's
    # length is a property of the catalogue entry and is reported next to it, not
    # instead of it: a slot hand-stretched to 90 minutes states its real length
    # only here, and reading the line would show the standard hour.
    minutes = whole_minutes(record.get("seance_length"))
    service_minutes = whole_minutes(service.get("seance_length"))

    starts_local: str | None = None
    ends_local: str | None = None
    starts_utc: str | None = None
    try:
        started = parse_altegio_local_to_utc(record.get("date") or record.get("datetime"))
    except LocalTimeError:
        started = None
    if started is not None:
        starts_utc = started.isoformat().replace("+00:00", "Z")
        starts_local = _local(started)
        if minutes is not None:
            ends_local = _local(started + timedelta(minutes=minutes))

    return {
        "altegio_record_id": record.get("id"),
        "altegio_staff_id": _staff_id_of_record(record),
        "starts_at_local": starts_local,
        "ends_at_local": ends_local,
        "starts_at_utc": starts_utc,
        # The booking's actual length, and separately the service line's own —
        # a disagreement between them IS the per-booking override.
        "duration_minutes": minutes,
        "service_line_duration_minutes": service_minutes,
        "handling": handling_class(block_reason),
        "altegio_service_id": service.get("id"),
        "altegio_service_name": _text(service.get("title")),
        "price": _text(str(service.get("cost"))) if service.get("cost") is not None else None,
        "price_to_pay": _text(str(service.get("cost_to_pay"))) if service.get("cost_to_pay") is not None else None,
        "customer_phone": normalized_international_phone(_client_of(record).get("phone")),
        "block_reason": block_reason,
    }


def _staff_id_of_record(record: dict[str, Any]) -> Any:
    flat = record.get("staff_id")
    if flat is not None:
        return flat
    staff = record.get("staff")
    return staff.get("id") if isinstance(staff, dict) else None


def source_identity_of(source: SourceCustomer) -> str:
    """The proven evidence about one source customer, as a digest.

    What a manual correction is bound to. Not the name — a name is exactly what
    an operator may be correcting, and two people in one salon share one often
    enough that binding to it would write one person's surname onto the other.
    """
    return source_identity_digest(
        phone=source.phone,
        source_client_ids=source.source_client_ids,
        record_ids=source.record_ids,
    )


def _proposal_from_source(
    source: SourceCustomer,
    lookup: CustomerLookup,
    *,
    override: CustomerOverride | None = None,
) -> CustomerDecision:
    """Turn one person plus one lookup into a decision record.

    The only path to ``pending`` — a record a person may later confirm — is a
    proven absence with a usable name. Everything else is written down as blocked
    with the reason, because an operator has to see WHY a customer will not be
    created, not merely that they were not.

    ``override`` is a correction a person entered earlier. It is applied ON TOP
    of the fresh source data, before the blocking rules are evaluated and before
    the digest is computed — so a customer whose source still has only a full
    name becomes confirmable on the strength of the split the operator supplied,
    and the digest they confirm covers the corrected values they can see.

    An override for a DIFFERENT source identity never reaches this function; the
    caller checks that first and blocks the customer as stale instead. Applying
    one silently is how a correction ends up on somebody else's card.
    """
    first_name = source.first_name
    last_name = source.last_name
    email = source.email
    if override is not None:
        first_name = override.first_name or first_name
        last_name = override.last_name or last_name
        email = override.email or email

    blocked: str | None = None
    if lookup.outcome != LOOKUP_ABSENT:
        blocked = {
            LOOKUP_FOUND: BLOCK_ALREADY_EXISTS,
            LOOKUP_FIRST_NAME_MISSING: CUSTOMER_FIRST_NAME_MISSING,
            LOOKUP_AMBIGUOUS: CUSTOMER_AMBIGUOUS,
            LOOKUP_PHONE_UNUSABLE: CUSTOMER_PHONE_UNUSABLE,
            LOOKUP_UNDETERMINED: BLOCK_LOOKUP_UNDETERMINED,
        }.get(lookup.outcome, BLOCK_LOOKUP_UNDETERMINED)
    elif source.shares_phone:
        blocked = BLOCK_SHARED_PHONE
    elif not first_name:
        # `POST /customers` needs a real given name. A full name is not one, and
        # is not split automatically — the operator supplies the split, and once
        # they have, the override above has already put it here.
        blocked = BLOCK_NAME_NOT_SPLIT if source.full_name else BLOCK_NAME_MISSING

    decision = CustomerDecision(
        phone=source.phone,
        first_name=first_name,
        last_name=last_name,
        email=email,
        linked_record_count=source.linked_record_count,
        source_label=source.full_name or "",
        state=STATE_BLOCKED if blocked else STATE_PENDING,
        customer_uuid=lookup.uuid,
        blocked_reason=blocked,
    )
    return decision.with_digest()


def _stale_correction(source: SourceCustomer, lookup: CustomerLookup) -> CustomerDecision:
    """A customer whose stored correction no longer describes them.

    Never silently applied, never silently dropped. The correction was evidence
    about one person as the source then described them; the source has since
    changed, so a person has to look again and either re-enter it or accept the
    new data.
    """
    return replace(
        _proposal_from_source(source, lookup),
        state=STATE_BLOCKED,
        blocked_reason=OVERRIDE_STALE_IDENTITY,
    ).with_digest()


# ---------------------------------------------------------------------------
# Digests binding a batch confirmation to the list that was shown
# ---------------------------------------------------------------------------


def pending_digest(decisions: DecisionSet) -> str:
    """Digest of the full set of customers a batch confirmation would cover.

    A batch "yes" is only meaningful if it is a yes to a specific list. The
    digest is printed with the list; the confirm command demands it back and
    refuses when it has moved, so a list that grew between the review and the
    confirmation cannot be waved through by a flag typed earlier.
    """
    entries = sorted(record.shown_digest for record in decisions.in_state(STATE_PENDING))
    blob = json.dumps(entries, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def mapping_pending_digest(proposals: list[ServiceProposal], agreement: MappingAgreement) -> str:
    """The same binding, for the service mapping an operator was shown."""
    entries = sorted(
        proposal_digest(proposal) for proposal in proposals if proposal.actionable and not agreement.agreed(proposal)
    )
    blob = json.dumps(entries, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Artefacts
# ---------------------------------------------------------------------------


def _write_private_json(path: Path, payload: Any) -> None:
    """Write one artefact 0600, atomically.

    Every file this stage writes is either PII or an input the migrator trusts.
    Neither should ever be half-written, and neither should be world-readable on
    a shared host.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    os.chmod(path.parent, DIR_MODE)
    tmp = path.with_suffix(path.suffix + ".tmp")
    fd = os.open(tmp, os.O_CREAT | os.O_TRUNC | os.O_WRONLY, FILE_MODE)
    try:
        os.write(fd, json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8"))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, path)
    os.chmod(path, FILE_MODE)


def build_customer_directory_payload(decisions: DecisionSet) -> list[dict[str, Any]]:
    """The customer directory the existing migrator reads, from proven cards only.

    A row appears only for a customer whose UUID this stage actually saw — found
    in the workspace, or created and then read back by UUID. A pending, blocked
    or merely-confirmed record contributes nothing: the directory is what the
    apply path resolves bookings against, and a speculative row there is a
    booking placed on a card that may not exist.
    """
    # The two blocked reasons that still carry a PROVEN uuid. The second one
    # matters: a card we found but cannot address must reach the directory, or
    # the migrator would report "this customer is not in EasyWeek" for somebody
    # who is — and those two call for opposite operator actions.
    directory_reasons = (BLOCK_ALREADY_EXISTS, CUSTOMER_FIRST_NAME_MISSING)

    rows: list[dict[str, Any]] = []
    for record in sorted(decisions.records.values(), key=lambda item: item.phone):
        if record.customer_uuid is None:
            continue
        if record.state not in (STATE_CREATED, STATE_BLOCKED):
            continue
        if record.state == STATE_BLOCKED and record.blocked_reason not in directory_reasons:
            continue
        rows.append(
            {
                "uuid": record.customer_uuid,
                "phone": record.phone,
                "first_name": record.first_name,
            }
        )
    return rows


def _local(instant: datetime) -> str:
    return instant.astimezone(BERLIN).isoformat()


@dataclass
class PrepareResult:
    """Everything one preparation command produced."""

    machine: dict[str, Any]
    operator: dict[str, Any]
    blocked: bool = False

    @property
    def exit_code(self) -> int:
        return 1 if self.blocked else 0


# ---------------------------------------------------------------------------
# The one live read every command shares
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class PreparationSnapshot:
    """One complete, read-only observation of a wave. Built once per command.

    Why this exists as a type rather than as steps inside ``prepare``: the
    confirm path used to rebuild proposals its own way. It skipped the
    classifier, so it collected services from every fetched booking rather than
    from the ones actually in the wave; it re-read the catalogue without the raw
    rows, so it judged staff availability against nothing; and it did not
    re-verify branch identity. The two paths could therefore disagree about the
    same unchanged data, and a service the catalogue withholds from the master
    who books it could become confirmable at the moment of confirmation.

    So there is one builder, and both paths call it. The properties it
    guarantees, in order:

    * branch identity is proven against the runtime registry, every time;
    * the window and the fetch are derived from the same cutover and horizon;
    * ``classify_record`` runs with the same manifest, cutover, empty customer
      directory and absent ledger, so scope is decided identically;
    * skipped rows and rows blocked for reasons this stage must not touch are
      excluded before anything is proposed;
    * services are collected from in-scope bookings only;
    * the catalogue is read ONCE, in full, and both the snapshot and the staff
      availability come from those same rows;
    * everything is ordered deterministically before it is digested.

    Anything that cannot be proven raises, so no caller reaches a decision file
    with a half-built picture.
    """

    branch: BranchMapping
    records: tuple[dict[str, Any], ...]
    in_scope: tuple[dict[str, Any], ...]
    operator_records: tuple[dict[str, Any], ...]
    manual: dict[str, int]
    ready_now: int
    catalog: CatalogSnapshot
    catalog_staff: dict[str, frozenset[str] | None]
    proposals: tuple[ServiceProposal, ...]
    customer_sources: dict[str, SourceCustomer]
    customer_lookups: dict[str, CustomerLookup]
    # Freshly derived from the live lookups, already digested. The confirm path
    # compares these against the stored decisions rather than trusting the file.
    customer_proposals: dict[str, CustomerDecision]
    # Phones whose proposal carries a manual correction applied on top of the
    # fresh source, and phones whose stored correction no longer describes the
    # source it was made about. Disjoint by construction.
    corrected_customers: frozenset[str] = frozenset()
    stale_corrections: frozenset[str] = frozenset()

    def proposal_for(self, altegio_service_id: int) -> ServiceProposal | None:
        for proposal in self.proposals:
            if proposal.source.altegio_service_id == altegio_service_id:
                return proposal
        return None


async def build_preparation_snapshot(
    inputs: PrepareInputs,
    *,
    write_client: Any,
    http_client: Any | None = None,
) -> PreparationSnapshot:
    """Read Altegio, the catalogue and the workspace's customers. Writes nothing.

    ``GET`` only, along every branch of this function. The write client is used
    for catalogue pages and customer lookups; nothing here can reach
    ``create_customer``.
    """
    silence_http_request_logs()

    branch = inputs.manifest.branch(inputs.altegio_company_id)
    if branch is None:
        raise PrepareError("manifest has no entry for that Altegio company id")

    identity = verify_branch_identity(inputs.manifest)
    if not identity.proven:
        # The manifest says which EasyWeek location a branch maps to; only the
        # runtime registry can say whether that location IS that branch. A wave
        # prepared against the wrong location proposes the wrong services — and
        # a confirmation recorded against it would be a decision about somebody
        # else's catalogue.
        raise PrepareError(f"branch identity unproven ({', '.join(identity.failures)})")

    window = build_window(inputs.cutover.at, horizon_days=inputs.horizon_days)
    records = await fetch_company_records(inputs.altegio_company_id, window, http_client=http_client)

    # The classifier decides scope. An empty-but-valid directory is passed on
    # purpose: customers are this stage's job, so their absence must not change
    # which bookings count as in scope.
    empty_directory = CustomerDirectory(valid=True, by_phone={})
    in_scope: list[dict[str, Any]] = []
    operator_records: list[dict[str, Any]] = []
    manual: dict[str, int] = defaultdict(int)
    ready_now = 0
    for record in records:
        decision = classify_record(
            record,
            company_id=inputs.altegio_company_id,
            manifest=inputs.manifest,
            directory=empty_directory,
            cutover=inputs.cutover,
            ledger=None,
        )
        if decision.outcome == SKIPPED:
            continue
        if decision.outcome == BLOCKED and decision.reason not in PREPARABLE_BLOCKS:
            # A per-booking difference this stage must not paper over.
            manual[decision.reason or "unknown"] += 1
            operator_records.append(operator_record_row(record, block_reason=decision.reason))
            continue
        if decision.outcome == READY:
            ready_now += 1
        operator_records.append(operator_record_row(record, block_reason=decision.reason))
        in_scope.append(record)

    # -- services -----------------------------------------------------------
    staff_ids = set(branch.selected_staff_ids)
    source_services = collect_source_services(in_scope, staff_ids=staff_ids or None)

    try:
        catalog, catalog_rows = await read_full_catalog_rows(write_client, location_uuid=branch.easyweek_location_uuid)
    except ServiceEvidenceError as error:
        # An unreadable catalogue proves nothing about any mapping, so no
        # command continues on one.
        raise PrepareError(f"catalogue unreadable ({error.reason})") from None

    catalog_staff = _catalog_staff_map(catalog_rows)
    proposals = propose_service_mapping(
        altegio_company_id=inputs.altegio_company_id,
        source_services=source_services,
        catalog=catalog,
        catalog_staff=catalog_staff,
        branch=branch,
    )

    # -- customers ----------------------------------------------------------
    # Corrections a person entered earlier, applied on top of the fresh source.
    # Loaded before the lookups so a rebuild cannot discard them (plan §30.9).
    overrides = CustomerOverrideStore(inputs.state_dir).load()

    sources = collect_source_customers(in_scope)
    lookups: dict[str, CustomerLookup] = {}
    customer_proposals: dict[str, CustomerDecision] = {}
    corrected: set[str] = set()
    stale_corrections: set[str] = set()
    for phone in sorted(sources):
        source = sources[phone]
        lookups[phone] = await lookup_customer_by_phone(write_client, phone)
        override = overrides.get(phone)
        if override is None:
            customer_proposals[phone] = _proposal_from_source(source, lookups[phone])
            continue
        if override.applies_to(source_identity_of(source)):
            customer_proposals[phone] = _proposal_from_source(source, lookups[phone], override=override)
            corrected.add(phone)
        else:
            # The evidence the correction was made about has moved. Applying it
            # anyway would put a person's decision onto data they never saw.
            customer_proposals[phone] = _stale_correction(source, lookups[phone])
            stale_corrections.add(phone)

    return PreparationSnapshot(
        branch=branch,
        records=tuple(records),
        in_scope=tuple(in_scope),
        operator_records=tuple(operator_records),
        manual=dict(manual),
        ready_now=ready_now,
        catalog=catalog,
        catalog_staff=catalog_staff,
        proposals=tuple(proposals),
        customer_sources=sources,
        customer_lookups=lookups,
        customer_proposals=customer_proposals,
        corrected_customers=frozenset(corrected),
        stale_corrections=frozenset(stale_corrections),
    )


# ---------------------------------------------------------------------------
# The read-only preparation pass
# ---------------------------------------------------------------------------


async def run_prepare(
    inputs: PrepareInputs,
    *,
    write_client: Any,
    http_client: Any | None = None,
    snapshot: PreparationSnapshot | None = None,
) -> PrepareResult:
    """Collect, propose and look up. Writes local files; mutates no CRM.

    All the reading lives in :func:`build_preparation_snapshot`, which the
    confirm path calls too — so what an operator is shown here and what a
    confirmation is checked against are produced by the same code over the same
    live data.
    """
    if snapshot is None:
        snapshot = await build_preparation_snapshot(inputs, write_client=write_client, http_client=http_client)

    store = CustomerDecisionStore(inputs.state_dir)
    with store:
        decisions = store.load()
        for phone in sorted(snapshot.customer_proposals):
            decisions.upsert_proposal(snapshot.customer_proposals[phone])
        store.save(decisions)

        agreement = _load_agreement(inputs.state_dir)
        _write_artefacts(
            inputs,
            decisions,
            list(snapshot.proposals),
            agreement,
            catalog_digest=snapshot.catalog.digest,
        )

        machine = _machine_report(
            inputs,
            decisions=decisions,
            proposals=list(snapshot.proposals),
            agreement=agreement,
            lookups=snapshot.customer_lookups,
            corrections_applied=len(snapshot.corrected_customers),
            corrections_stale=len(snapshot.stale_corrections),
            manual=snapshot.manual,
            ready_now=snapshot.ready_now,
            in_scope=len(snapshot.in_scope),
            source_records=len(snapshot.records),
            catalog_digest=snapshot.catalog.digest,
        )
        operator = _operator_report(
            inputs,
            decisions=decisions,
            proposals=list(snapshot.proposals),
            sources=snapshot.customer_sources,
            records=list(snapshot.operator_records),
            corrected=snapshot.corrected_customers,
            stale=snapshot.stale_corrections,
        )
        _write_private_json(inputs.state_dir / FILE_MACHINE_REPORT, machine)
        _write_private_json(inputs.state_dir / FILE_OPERATOR_REVIEW, operator)

    return PrepareResult(machine=machine, operator=operator, blocked=not machine["ready"]["all_clear"])


def _catalog_staff_map(rows: list[Any]) -> dict[str, frozenset[str] | None]:
    """Per-service staff availability, if the catalogue rows state any.

    Read from the very pages the snapshot came from, and read honestly: a
    catalogue that carries no staff field yields ``None`` for every service, and
    the proposal then says the availability could not be proven rather than
    claiming it was fine. There is no documented per-employee availability field
    on this endpoint, and inventing one is the failure PR-9 already paid for.
    """
    staff: dict[str, frozenset[str] | None] = {}
    for row in rows:
        if isinstance(row, dict) and isinstance(row.get("uuid"), str):
            staff[row["uuid"]] = read_service_staff_uuids(row)
    return staff


def _load_agreement(state_dir: Path) -> MappingAgreement:
    path = state_dir / FILE_MAPPING_AGREEMENT
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return MappingAgreement()
    except OSError as error:
        raise PrepareError(f"cannot read the mapping agreement: {error.strerror}") from None
    try:
        return MappingAgreement.from_json(json.loads(raw))
    except Exception:
        raise PrepareError("the mapping agreement file is unusable") from None


def _write_artefacts(
    inputs: PrepareInputs,
    decisions: DecisionSet,
    proposals: list[ServiceProposal],
    agreement: MappingAgreement,
    *,
    catalog_digest: str,
) -> None:
    """The three files the existing migrator consumes."""
    patch = manifest_service_patch(proposals, agreement)
    merged = merge_manifest_services(
        inputs.manifest_json,
        altegio_company_id=inputs.altegio_company_id,
        patch=patch,
    )
    _write_private_json(inputs.state_dir / FILE_MANIFEST_PROPOSED, merged)
    _write_private_json(inputs.state_dir / FILE_CUSTOMER_DIRECTORY, build_customer_directory_payload(decisions))
    _write_private_json(inputs.state_dir / FILE_MAPPING_AGREEMENT, agreement.to_json())
    logger.info(
        "easyweek_migration.prepare: artefacts written services_patched=%d catalog_digest=%s",
        len(patch),
        catalog_digest[:12],
    )


def _machine_report(
    inputs: PrepareInputs,
    *,
    decisions: DecisionSet,
    proposals: list[ServiceProposal],
    agreement: MappingAgreement,
    lookups: dict[str, CustomerLookup],
    manual: dict[str, int],
    ready_now: int,
    in_scope: int,
    source_records: int,
    catalog_digest: str,
    corrections_applied: int = 0,
    corrections_stale: int = 0,
) -> dict[str, Any]:
    """Counts and codes only. Never a name, a number, an address or a body.

    Deliberately reports readiness in FIVE separate lines rather than one word.
    "Ready" collapsed five different situations into one, and an operator acting
    on it could not tell a wave that is finished from a wave whose customer
    lookups all failed on a network error.
    """
    unresolved_lookups = sum(1 for lookup in lookups.values() if lookup.outcome == LOOKUP_UNDETERMINED)
    # Outstanding = anything still needing a person. `settled` is the ONLY free
    # pass, and it means an existing mapping whose live catalogue entry still
    # matches the reviewed baseline. Drift used to hide here as `already_mapped`
    # and report a wave ready while its override baseline had gone stale.
    mapping_outstanding = [
        proposal.as_safe_dict() for proposal in proposals if not proposal.settled and not agreement.agreed(proposal)
    ]
    mapping_drift = [proposal.as_safe_dict() for proposal in proposals if proposal.drift_fields]
    customers_ready = len(build_customer_directory_payload(decisions))
    customers_pending = len(decisions.in_state(STATE_PENDING))
    customers_confirmed = len(decisions.in_state(STATE_CONFIRMED))
    customers_blocked = [
        record.as_safe_dict()
        for record in decisions.in_state(STATE_BLOCKED)
        if record.blocked_reason != BLOCK_ALREADY_EXISTS
    ]
    in_flight = [record.as_safe_dict() for record in decisions.in_state(STATE_IN_FLIGHT)]

    return {
        "mode": inputs.mode,
        "run_id": inputs.run_id,
        "altegio_company_id": inputs.altegio_company_id,
        "cutover_at": inputs.cutover.iso,
        "horizon_days": inputs.horizon_days,
        "catalog_digest": catalog_digest,
        "source": {
            "records_fetched": source_records,
            "records_in_scope": in_scope,
            "records_ready_now": ready_now,
            "records_needing_manual_work": manual,
        },
        "mapping": {
            "proposals": [proposal.as_safe_dict() for proposal in proposals],
            "outstanding": mapping_outstanding,
            "agreed": len(agreement.entries),
            "drift": mapping_drift,
            "pending_digest": mapping_pending_digest(proposals, agreement),
        },
        "customers": {
            "ready": customers_ready,
            "pending_confirmation": customers_pending,
            "confirmed_not_yet_created": customers_confirmed,
            "blocked": customers_blocked,
            "in_flight_needs_reconciliation": in_flight,
            "lookups_undetermined": unresolved_lookups,
            # Counts only. WHICH customer was corrected, and to what, lives in
            # the operator review; a machine report never carries a name.
            "manual_corrections_applied": corrections_applied,
            "manual_corrections_stale": corrections_stale,
            "pending_digest": pending_digest(decisions),
            "states": decisions.summary(),
        },
        # Five separate answers, on purpose. Not one "READY".
        "ready": {
            "customers_ready": customers_ready,
            "mapping_ready": not mapping_outstanding,
            "records_ready": ready_now,
            "records_needing_manual_work": sum(manual.values()),
            "blocked_by_technical_error": unresolved_lookups + len(in_flight),
            "manual_corrections_stale": corrections_stale,
            "all_clear": (
                not mapping_outstanding
                and customers_pending == 0
                and customers_confirmed == 0
                and not customers_blocked
                and unresolved_lookups == 0
                and not in_flight
                and corrections_stale == 0
            ),
        },
        "artefacts": {
            "manifest": str(inputs.state_dir / FILE_MANIFEST_PROPOSED),
            "customer_directory": str(inputs.state_dir / FILE_CUSTOMER_DIRECTORY),
            "operator_review": str(inputs.state_dir / FILE_OPERATOR_REVIEW),
        },
    }


def _operator_report(
    inputs: PrepareInputs,
    *,
    decisions: DecisionSet,
    proposals: list[ServiceProposal],
    sources: dict[str, SourceCustomer],
    records: list[dict[str, Any]],
    corrected: frozenset[str] = frozenset(),
    stale: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    """The reviewable document. HOLDS PII, by design and by necessity.

    A person cannot confirm the creation of a customer they are shown as
    ``a7f3…``. So this file carries the name and the number, plus the exact
    service names and the local times the decision rests on — and it is written
    0600, outside the repository, and is never committed.

    It also states, next to every customer, what creating a card does NOT do:
    it does not bring the visit history across. That sentence is here because a
    card created empty makes a ten-year regular look like a first-timer to the
    retention messages, and the person clicking "confirm" is the one who needs to
    know it.
    """
    customers: list[dict[str, Any]] = []
    for record in sorted(decisions.records.values(), key=lambda item: item.phone):
        source = sources.get(record.phone)
        customers.append(
            {
                "phone": record.phone,
                "first_name": record.first_name,
                "last_name": record.last_name,
                "full_name_from_source": record.source_label or None,
                "email": record.email,
                "linked_records": record.linked_record_count,
                "source_record_ids": list(source.record_ids) if source else [],
                "state": record.state,
                "blocked_reason": record.blocked_reason,
                "easyweek_customer_uuid": record.customer_uuid,
                # Copy this into `--confirm-customer 'PHONE=DIGEST'`. It covers
                # every field printed above, so a confirmation cannot be aimed
                # at data the person never read.
                "review_digest": record.shown_digest,
                # Whether the values above include a correction a person
                # entered, and whether a stored correction stopped applying
                # because the source moved under it.
                "manually_corrected": record.phone in corrected,
                "correction_stale": record.phone in stale,
                "note": (
                    "Creating a card does NOT transfer visit history: EasyWeek will "
                    "show this customer with no previous visits."
                ),
            }
        )

    return {
        "warning": "CONTAINS PERSONAL DATA. Do not commit, do not paste into a ticket.",
        "run_id": inputs.run_id,
        "altegio_company_id": inputs.altegio_company_id,
        "cutover_at_local": _local(inputs.cutover.at),
        "timezone": "Europe/Berlin",
        "customer_pending_digest": pending_digest(decisions),
        "customers": customers,
        "service_mapping": [proposal.as_operator_dict() for proposal in proposals],
        # Every in-scope booking, with the times, duration and prices an operator
        # compares against the Altegio screen — plus the reason each blocked row
        # is blocked, which is what turns "12 records need work" into a list.
        "records": sorted(records, key=lambda row: (row["starts_at_utc"] or "", row["altegio_record_id"] or 0)),
    }


# ---------------------------------------------------------------------------
# Recording what a person agreed to. Local files only; no CRM request at all.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ConfirmTarget:
    """One thing an operator confirmed, and the digest they saw next to it.

    Both halves are required. An identifier alone says "confirm whatever is
    under this number today", which is not what a person reviewing a list means
    — and it is how a proposal that changed between the review and the command
    could be agreed to unseen.
    """

    identifier: str
    review_digest: str


@dataclass(frozen=True)
class ConfirmRequest:
    """One confirmation command, exactly as the operator spelled it out.

    Nothing is read from stdin, here or anywhere else in this module. An EOF, a
    closed stdin or a Docker run without a TTY is not consent, and the way to
    make that impossible is to have no code that could mistake it for consent.
    """

    confirm_customers: tuple[ConfirmTarget, ...] = ()
    skip_customers: tuple[str, ...] = ()
    confirm_all_pending: bool = False
    expected_pending_digest: str | None = None
    # Corrections. Applied to ONE customer, and they reset the confirmation:
    # the digest changes, so the record returns to pending and has to be agreed
    # to again with the new values visible. No digest is required to correct
    # something — a correction is not an approval, and demanding the digest of
    # data the operator is about to replace would be theatre.
    correct_phone: str | None = None
    correct_first_name: str | None = None
    correct_last_name: str | None = None
    correct_email: str | None = None
    # Mapping.
    confirm_services: tuple[ConfirmTarget, ...] = ()
    confirm_all_services: bool = False
    expected_mapping_digest: str | None = None


def _refuse(reason: str, detail: str = "") -> PrepareError:
    """A fail-closed stop. The message is a code plus operator instructions."""
    suffix = f" ({detail})" if detail else ""
    return PrepareError(
        f"{reason}{suffix}: nothing was changed. Re-run prepare, read the review output, "
        "and confirm with the digest printed next to the item."
    )


def apply_confirmations(
    inputs: PrepareInputs,
    request: ConfirmRequest,
    *,
    snapshot: PreparationSnapshot,
) -> dict[str, Any]:
    """Record confirmations, skips and corrections against FRESH data.

    Three things have to agree before a single confirmation is honoured:

    1. the digest the operator typed, from the review they read;
    2. the digest of the proposal rebuilt from live data just now;
    3. the stored decision's own internal consistency.

    Checking only (3) was the hole: ``matches_shown`` compares a record's fields
    against a digest stored beside those same fields, so a second ``prepare``
    that replaced both left it happily consistent — and the operator could then
    confirm a proposal they had never seen.

    Every check runs before any mutation. A refusal leaves the decision store,
    the mapping agreement and the proposed manifest exactly as they were: a
    half-applied confirmation is worse than none, because the half that landed
    looks reviewed.
    """
    store = CustomerDecisionStore(inputs.state_dir)
    outcome: dict[str, Any] = {"confirmed": [], "skipped": [], "corrected": [], "refused": []}

    with store:
        decisions = store.load()

        # What `prepare` last wrote, captured BEFORE the live data is folded in.
        # The three checks below have to be independent, and an upsert-first
        # order would quietly make two of them the same check: it rewrites a
        # pending record to match the live read, after which "the store agrees
        # with itself" and "the store agrees with live" are both true by
        # construction, however far the data had actually moved.
        stored = dict(decisions.records)

        # Re-derive from live data, exactly as `prepare` does. A record whose
        # source moved loses its confirmation here, before anything in this
        # command is allowed to act on it.
        for phone in sorted(snapshot.customer_proposals):
            decisions.upsert_proposal(snapshot.customer_proposals[phone])

        planned = _plan_customer_changes(decisions, stored, request, snapshot)
        mapping_plan = _plan_mapping_changes(inputs, request, snapshot)

        # -- everything validated; now, and only now, mutate -----------------
        if request.correct_phone is not None:
            corrected = _apply_correction(inputs, decisions, request, snapshot)
            outcome["corrected"].append(phone_fingerprint(corrected))

        for phone, state in planned:
            decisions.set_state(phone, state, blocked_reason=None)
            outcome["skipped" if state == STATE_SKIPPED else "confirmed"].append(phone_fingerprint(phone))

        store.save(decisions)
        outcome["customer_states"] = decisions.summary()
        outcome["pending_digest"] = pending_digest(decisions)

        # Inside the lock: the mapping agreement and the proposed manifest live
        # in the same state directory, and a second run rewriting them while
        # this one is halfway through would leave the two disagreeing.
        if mapping_plan is not None:
            outcome["mapping"] = _commit_mapping(inputs, snapshot, mapping_plan)
    return outcome


def _apply_correction(
    inputs: PrepareInputs,
    decisions: DecisionSet,
    request: ConfirmRequest,
    snapshot: PreparationSnapshot,
) -> str:
    """Record a correction durably, then reflect it in this run's decision.

    The durable half is the point. Writing only the in-memory decision was the
    defect: the next command rebuilds proposals from live data and
    ``upsert_proposal`` replaces a pending record with the fresh one, so the
    correction vanished and the customer blocked again — forever, however many
    times it was retyped.

    The override is bound to the PROVEN source identity, never to a name, and it
    is written before the decision so a crash between the two leaves the
    correction recorded rather than lost.
    """
    phone = normalized_international_phone(request.correct_phone)
    if phone is None:
        raise _refuse("the phone number to correct is not a usable international number")
    record = decisions.get(phone)
    if record is None:
        raise _refuse("no customer decision for that number")
    if record.state in (STATE_CREATED, STATE_IN_FLIGHT):
        # Terminal and in-flight states are untouchable: a created card exists,
        # and an in-flight one may. Editing either would either rewrite a real
        # customer or authorise a second POST for one.
        raise _refuse("that customer has already been created or is mid-creation", record.state)

    source = snapshot.customer_sources.get(phone)
    if source is None:
        raise _refuse("that customer is no longer part of this wave")

    first_name = _text(request.correct_first_name)
    last_name = _text(request.correct_last_name)
    email = _text(request.correct_email)
    if not (first_name or last_name or email):
        raise _refuse("a correction must set at least one of --first-name, --last-name, --email")

    store = CustomerOverrideStore(inputs.state_dir)
    overrides = store.load()
    merged = replace(
        overrides.merged(phone, first_name=first_name, last_name=last_name, email=email),
        identity_digest=source_identity_of(source),
        base_review_digest=record.shown_digest,
    )
    overrides.put(merged)
    store.save(overrides)

    corrected = replace(
        record,
        first_name=merged.first_name or record.first_name,
        last_name=merged.last_name or record.last_name,
        email=merged.email or record.email,
        # A correction always returns the record to pending: the person who
        # confirms it must see the corrected values, and its digest changes with
        # them, so the old agreement cannot survive the edit.
        state=STATE_PENDING,
        blocked_reason=None,
    ).with_digest()
    decisions.records[phone] = corrected
    return phone


def _plan_customer_changes(
    decisions: DecisionSet,
    stored: dict[str, CustomerDecision],
    request: ConfirmRequest,
    snapshot: PreparationSnapshot,
) -> list[tuple[str, str]]:
    """Validate every customer instruction. Returns the changes, or raises.

    Nothing is written from here. The caller applies the returned list only if
    this function returned at all.
    """
    planned: list[tuple[str, str]] = []
    # A command that both corrects a customer and decides about the same one is
    # self-contradictory: the correction resets them to pending, so whichever
    # order it ran in, one half of the instruction would be silently discarded.
    correcting = normalized_international_phone(request.correct_phone) if request.correct_phone else None

    for raw in request.skip_customers:
        phone = normalized_international_phone(raw)
        if phone is None:
            raise _refuse("a phone number to skip is not a usable international number")
        if decisions.get(phone) is None:
            raise _refuse("no customer decision for a number given to --skip-customer")
        if phone == correcting:
            raise _refuse("the same customer cannot be corrected and skipped in one command")
        planned.append((phone, STATE_SKIPPED))

    targets: list[tuple[str, str | None]] = []
    if request.confirm_all_pending:
        current = pending_digest(decisions)
        if request.expected_pending_digest != current:
            # The list moved between the review and the confirmation. A batch
            # "yes" is a yes to one specific set, and this is not that set.
            raise _refuse(
                "the pending customer list has changed since it was printed",
                f"expected {current[:12]}",
            )
        targets = [(record.phone, None) for record in decisions.in_state(STATE_PENDING)]
    else:
        for target in request.confirm_customers:
            phone = normalized_international_phone(target.identifier)
            if phone is None:
                raise _refuse("a phone number to confirm is not a usable international number")
            targets.append((phone, target.review_digest))

    for phone, supplied in targets:
        if phone == correcting:
            raise _refuse("the same customer cannot be corrected and confirmed in one command")
        record = decisions.get(phone)
        if record is None:
            raise _refuse("no customer decision for a number given to --confirm-customer")
        fresh = snapshot.customer_proposals.get(phone)
        if fresh is None:
            # The customer is no longer in the wave at all — a booking moved,
            # was cancelled, or fell outside the window.
            raise _refuse("that customer is no longer part of this wave")
        if record.state != STATE_PENDING:
            raise _refuse("that customer is not awaiting confirmation", record.state)

        # (3) the file `prepare` wrote is internally consistent,
        reviewed = stored.get(phone)
        if reviewed is None or not reviewed.matches_shown():
            raise _refuse("the stored decision does not match its own digest")
        # (2) the workspace still says what that file recorded,
        if fresh.shown_digest != reviewed.shown_digest:
            raise _refuse("the live data no longer matches the reviewed decision")
        # (1) and the operator typed the digest of that very item.
        if supplied is not None and supplied != fresh.shown_digest:
            raise _refuse("the supplied review digest does not match this customer")
        planned.append((phone, STATE_CONFIRMED))

    return planned


@dataclass(frozen=True)
class _MappingPlan:
    """Service ids to confirm, validated but not yet written."""

    agreement: MappingAgreement
    confirmed: tuple[int, ...]


def _plan_mapping_changes(
    inputs: PrepareInputs,
    request: ConfirmRequest,
    snapshot: PreparationSnapshot,
) -> _MappingPlan | None:
    """Validate every service instruction against the freshly built proposals."""
    if not (request.confirm_services or request.confirm_all_services):
        return None

    agreement = _load_agreement(inputs.state_dir)
    proposals = list(snapshot.proposals)

    if request.confirm_all_services:
        current = mapping_pending_digest(proposals, agreement)
        if request.expected_mapping_digest != current:
            raise _refuse(
                "the proposed service mapping has changed since it was printed",
                f"expected {current[:12]}",
            )
        chosen = [proposal for proposal in proposals if proposal.actionable and not agreement.agreed(proposal)]
    else:
        chosen = []
        for target in request.confirm_services:
            try:
                service_id = int(target.identifier)
            except ValueError:
                raise _refuse("a service id to confirm is not an integer") from None
            proposal = snapshot.proposal_for(service_id)
            if proposal is None:
                raise _refuse("that service is not part of this wave")
            if not proposal.actionable:
                # Ambiguous, absent from the catalogue, drifted from its
                # baseline, or a service the catalogue withholds from the master
                # who books it. None of those is something a confirmation fixes.
                raise _refuse("that service proposal cannot be confirmed", proposal.status)
            if target.review_digest != proposal_digest(proposal):
                raise _refuse("the supplied review digest does not match this service")
            chosen.append(proposal)

    for proposal in chosen:
        agreement.confirm(proposal)
    return _MappingPlan(
        agreement=agreement,
        confirmed=tuple(sorted(proposal.source.altegio_service_id for proposal in chosen)),
    )


def _commit_mapping(inputs: PrepareInputs, snapshot: PreparationSnapshot, plan: _MappingPlan) -> dict[str, Any]:
    """Write the validated agreement and the manifest it produces."""
    proposals = list(snapshot.proposals)
    _write_private_json(inputs.state_dir / FILE_MAPPING_AGREEMENT, plan.agreement.to_json())
    patch = manifest_service_patch(proposals, plan.agreement)
    merged = merge_manifest_services(
        inputs.manifest_json,
        altegio_company_id=inputs.altegio_company_id,
        patch=patch,
    )
    _write_private_json(inputs.state_dir / FILE_MANIFEST_PROPOSED, merged)
    return {
        "confirmed_service_ids": list(plan.confirmed),
        "manifest_entries_written": len(patch),
        "pending_digest": mapping_pending_digest(proposals, plan.agreement),
    }


# ---------------------------------------------------------------------------
# The only command in this module that writes to EasyWeek
# ---------------------------------------------------------------------------


def build_customer_request(record: CustomerDecision) -> dict[str, Any]:
    """The minimal ``POST /customers`` body: a real number and a real given name.

    Nothing optional is sent unless the source genuinely had it. An invented
    e-mail is a channel EasyWeek may notify a stranger on; an invented surname is
    a wrong name on a card that outlives this migration. Neither is improved by
    being plausible.
    """
    if not record.phone or not record.first_name:
        raise PrepareError("a customer request needs a real phone number and a real first name")
    body: dict[str, Any] = {"phone": record.phone, "first_name": record.first_name}
    if record.last_name:
        body["last_name"] = record.last_name
    if record.email:
        body["email"] = record.email
    return body


async def reconcile_in_flight(
    write_client: Any,
    decisions: DecisionSet,
    store: CustomerDecisionStore,
) -> list[dict[str, Any]]:
    """Read-only first pass over records whose creation outcome is unknown.

    A process that died between writing the marker and reading the response left
    a record saying "a POST may have gone out". The answer is never another POST.
    It is a lookup: if the workspace now holds a card for that number, the
    creation landed and the record becomes ``created``; if it does not, the
    record goes back to ``confirmed`` and may be attempted once more; if the
    lookup itself cannot say, the record stays in flight and the run stops.
    """
    resolved: list[dict[str, Any]] = []
    for record in list(decisions.in_state(STATE_IN_FLIGHT)):
        lookup = await lookup_customer_by_phone(write_client, record.phone)
        if lookup.outcome in (LOOKUP_FOUND, LOOKUP_FIRST_NAME_MISSING) and lookup.uuid:
            decisions.set_state(record.phone, STATE_CREATED, customer_uuid=lookup.uuid, blocked_reason=None)
        elif lookup.outcome == LOOKUP_ABSENT:
            # The POST did not land. Back to confirmed — the confirmation itself
            # is still good, and its digest still binds it to the same data.
            decisions.set_state(record.phone, STATE_CONFIRMED, blocked_reason=None)
        elif lookup.outcome == LOOKUP_AMBIGUOUS:
            # Two cards on the number now. Possibly a duplicate this migration
            # created. A person looks at it; nothing here picks a winner.
            decisions.set_state(record.phone, STATE_BLOCKED, blocked_reason=CUSTOMER_AMBIGUOUS)
        else:
            decisions.set_state(record.phone, STATE_IN_FLIGHT, blocked_reason=BLOCK_LOOKUP_UNDETERMINED)
        resolved.append(decisions.records[record.phone].as_safe_dict())
        store.save(decisions)
    return resolved


async def run_create_customers(inputs: PrepareInputs, *, write_client: Any) -> PrepareResult:
    """Create exactly the customers a person confirmed, one at a time.

    The sequence per customer is fixed and every step of it is load-bearing:

    1. **re-check** — a lookup, now. A confirmation is old by the time it runs,
       and the customer may have been created in the EasyWeek UI five minutes
       ago. A ``POST`` without a fresh absence check is how a duplicate is made.
    2. **mark in flight and fsync** — before the request, not after. A crash
       between the write and the response must leave evidence that a ``POST``
       may exist.
    3. **one POST** — never retried. A timeout, a transport failure, a 5xx or a
       2xx without a readable uuid are all *unknown*, not failed.
    4. **verify by GET** — the workspace, not the POST's own account of itself,
       and the card must carry the number that was asked for.
    5. **record and re-save** — then, and only then, the customer counts as
       created and enters the directory.

    A single unknown outcome stops the whole run. There is no partial-failure
    mode worth continuing into: the next customer's creation would be decided
    against a workspace nobody has looked at yet.
    """
    if not inputs.create_allowed:
        # Distinct from the migrator's `--apply`, on purpose: creating customer
        # cards and creating bookings are separate powers.
        raise PrepareError("customer creation was not authorised for this run")

    silence_http_request_logs()
    store = CustomerDecisionStore(inputs.state_dir)
    created: list[dict[str, Any]] = []
    halted: str | None = None

    with store:
        decisions = store.load()
        recovered = await reconcile_in_flight(write_client, decisions, store)
        if decisions.in_state(STATE_IN_FLIGHT):
            # Still unknown after the read-only pass. Creating anything now would
            # be creating it next to a card that may already exist.
            halted = BLOCK_CREATE_UNCERTAIN

        if halted is None:
            for record in sorted(decisions.in_state(STATE_CONFIRMED), key=lambda item: item.phone):
                if not record.creatable:
                    decisions.set_state(record.phone, STATE_BLOCKED, blocked_reason=BLOCK_NAME_MISSING)
                    store.save(decisions)
                    continue
                if not record.matches_shown():
                    decisions.set_state(record.phone, STATE_PENDING, blocked_reason=None)
                    store.save(decisions)
                    continue

                lookup = await lookup_customer_by_phone(write_client, record.phone)
                if lookup.outcome != LOOKUP_ABSENT:
                    reason = (
                        BLOCK_ALREADY_EXISTS
                        if lookup.outcome in (LOOKUP_FOUND, LOOKUP_FIRST_NAME_MISSING)
                        else BLOCK_LOOKUP_UNDETERMINED
                    )
                    decisions.set_state(
                        record.phone,
                        STATE_BLOCKED,
                        blocked_reason=reason,
                        customer_uuid=lookup.uuid or record.customer_uuid,
                    )
                    store.save(decisions)
                    if reason == BLOCK_LOOKUP_UNDETERMINED:
                        halted = BLOCK_LOOKUP_UNDETERMINED
                        break
                    continue

                attempt = os.urandom(8).hex()
                decisions.set_state(record.phone, STATE_IN_FLIGHT, attempt_id=attempt, blocked_reason=None)
                store.save(decisions)

                try:
                    payload = await write_client.create_customer(build_customer_request(record))
                except EasyWeekUncertainMutation:
                    # The record stays in flight. That is the correct state: it
                    # says a POST may have landed, and the next run reconciles it
                    # by reading rather than by posting again.
                    halted = BLOCK_CREATE_UNCERTAIN
                    break
                except EasyWeekAuthError:
                    # A definitive 401/403: the server refused before doing
                    # anything, so the confirmation is still good and the record
                    # goes back to `confirmed` rather than staying in flight.
                    decisions.set_state(record.phone, STATE_CONFIRMED, blocked_reason=None)
                    store.save(decisions)
                    halted = BLOCK_CREATE_ACCESS_DENIED
                    break
                except EasyWeekPermanentError:
                    # A rejected phone or e-mail: another customer in the
                    # workspace already holds it. The answer is to look at who,
                    # never to alter the contact details until it goes through.
                    decisions.set_state(record.phone, STATE_BLOCKED, blocked_reason=BLOCK_CREATE_REJECTED)
                    store.save(decisions)
                    continue
                except EasyWeekError:
                    decisions.set_state(record.phone, STATE_IN_FLIGHT, blocked_reason=BLOCK_CREATE_UNCERTAIN)
                    store.save(decisions)
                    halted = BLOCK_CREATE_UNCERTAIN
                    break

                body = payload.get("data") if isinstance(payload.get("data"), dict) else payload
                new_uuid = body.get("uuid") if isinstance(body, dict) else None
                if not isinstance(new_uuid, str):
                    decisions.set_state(record.phone, STATE_IN_FLIGHT, blocked_reason=BLOCK_CREATE_UNVERIFIED)
                    store.save(decisions)
                    halted = BLOCK_CREATE_UNVERIFIED
                    break

                try:
                    card = await verify_customer(write_client, new_uuid, expected_phone=record.phone)
                except CustomerLookupUndetermined:
                    # Created, probably — but not proven, and an unproven uuid
                    # must not enter the directory the apply path books against.
                    decisions.set_state(record.phone, STATE_IN_FLIGHT, blocked_reason=BLOCK_CREATE_UNVERIFIED)
                    store.save(decisions)
                    halted = BLOCK_CREATE_UNVERIFIED
                    break

                decisions.set_state(record.phone, STATE_CREATED, customer_uuid=card.uuid, blocked_reason=None)
                store.save(decisions)
                created.append(decisions.records[record.phone].as_safe_dict())

        _write_private_json(inputs.state_dir / FILE_CUSTOMER_DIRECTORY, build_customer_directory_payload(decisions))
        machine = {
            "mode": inputs.mode,
            "run_id": inputs.run_id,
            "created": created,
            "recovered_in_flight": recovered,
            "halted": halted,
            "customer_states": decisions.summary(),
            "customers_ready": len(build_customer_directory_payload(decisions)),
            "artefacts": {"customer_directory": str(inputs.state_dir / FILE_CUSTOMER_DIRECTORY)},
        }
        _write_private_json(inputs.state_dir / FILE_MACHINE_REPORT, machine)

    return PrepareResult(machine=machine, operator={}, blocked=halted is not None)
