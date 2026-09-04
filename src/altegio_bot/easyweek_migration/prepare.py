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
    BLOCK_SERVICE_MAPPING_MISSING,
    BLOCK_STAFF_MAPPING_MISSING,
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
from altegio_bot.easyweek_migration.customers import (
    CUSTOMER_AMBIGUOUS,
    CUSTOMER_FIRST_NAME_MISSING,
    CUSTOMER_NOT_FOUND,
    CUSTOMER_PHONE_UNUSABLE,
    CustomerDirectory,
    normalized_international_phone,
)
from altegio_bot.easyweek_migration.cutover import Cutover, LocalTimeError, parse_altegio_local_to_utc
from altegio_bot.easyweek_migration.manifest import MigrationManifest
from altegio_bot.easyweek_migration.mapping_proposal import (
    PROPOSAL_ALREADY_MAPPED,
    MappingAgreement,
    ServiceProposal,
    collect_source_services,
    manifest_service_patch,
    merge_manifest_services,
    proposal_digest,
    propose_service_mapping,
    read_service_staff_uuids,
)
from altegio_bot.easyweek_migration.service_catalog import (
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
    # The EasyWeek staff UUIDs of the masters in this wave, taken from the
    # manifest's own staff mapping. Used only to ask the catalogue whether it
    # says the service is available to them.
    selected_staff_uuids: frozenset[str] = frozenset()
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
    seance = record.get("seance_length")
    minutes = seance // 60 if type(seance) is int and seance > 0 and seance % 60 == 0 else None

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
        "duration_minutes": minutes,
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


def _proposal_from_source(source: SourceCustomer, lookup: CustomerLookup) -> CustomerDecision:
    """Turn one person plus one lookup into a decision record.

    The only path to ``pending`` — a record a person may later confirm — is a
    proven absence with a usable name. Everything else is written down as blocked
    with the reason, because an operator has to see WHY a customer will not be
    created, not merely that they were not.
    """
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
    elif not source.first_name:
        # `POST /customers` needs a real given name. A full name is not one, and
        # is not split automatically — the operator supplies the split.
        blocked = BLOCK_NAME_NOT_SPLIT if source.full_name else BLOCK_NAME_MISSING

    decision = CustomerDecision(
        phone=source.phone,
        first_name=source.first_name,
        last_name=source.last_name,
        email=source.email,
        linked_record_count=source.linked_record_count,
        source_label=source.full_name or "",
        state=STATE_BLOCKED if blocked else STATE_PENDING,
        customer_uuid=lookup.uuid,
        blocked_reason=blocked,
    )
    return decision.with_digest()


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
# The read-only preparation pass
# ---------------------------------------------------------------------------


async def run_prepare(
    inputs: PrepareInputs,
    *,
    write_client: Any,
    http_client: Any | None = None,
) -> PrepareResult:
    """Collect, propose and look up. Writes local files; mutates no CRM.

    The EasyWeek client is used for ``GET`` only along this path — the catalogue
    pages and the customer lookups. Nothing here can reach ``create_customer``.
    """
    silence_http_request_logs()

    branch = inputs.manifest.branch(inputs.altegio_company_id)
    if branch is None:
        raise PrepareError("manifest has no entry for that Altegio company id")

    identity = verify_branch_identity(inputs.manifest)
    if not identity.proven:
        # The manifest says which EasyWeek location a branch maps to; only the
        # runtime registry can say whether that location IS that branch. A wave
        # prepared against the wrong location proposes the wrong services.
        raise PrepareError(f"branch identity unproven ({', '.join(identity.failures)})")

    window = build_window(inputs.cutover.at, horizon_days=inputs.horizon_days)
    records = await fetch_company_records(inputs.altegio_company_id, window, http_client=http_client)

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
        raise PrepareError(f"catalogue unreadable ({error.reason})") from None

    catalog_staff = _catalog_staff_map(catalog_rows)
    proposals = propose_service_mapping(
        altegio_company_id=inputs.altegio_company_id,
        source_services=source_services,
        catalog=catalog,
        catalog_staff=catalog_staff,
        selected_staff_uuids=set(inputs.selected_staff_uuids),
        branch=branch,
    )

    # -- customers ----------------------------------------------------------
    sources = collect_source_customers(in_scope)
    lookups: dict[str, CustomerLookup] = {}
    for phone in sorted(sources):
        lookups[phone] = await lookup_customer_by_phone(write_client, phone)

    store = CustomerDecisionStore(inputs.state_dir)
    with store:
        decisions = store.load()
        for phone in sorted(sources):
            decisions.upsert_proposal(_proposal_from_source(sources[phone], lookups[phone]))
        store.save(decisions)

        agreement = _load_agreement(inputs.state_dir)
        _write_artefacts(inputs, decisions, proposals, agreement, catalog_digest=catalog.digest)

        machine = _machine_report(
            inputs,
            decisions=decisions,
            proposals=proposals,
            agreement=agreement,
            lookups=lookups,
            manual=dict(manual),
            ready_now=ready_now,
            in_scope=len(in_scope),
            source_records=len(records),
            catalog_digest=catalog.digest,
        )
        operator = _operator_report(
            inputs,
            decisions=decisions,
            proposals=proposals,
            sources=sources,
            records=operator_records,
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
) -> dict[str, Any]:
    """Counts and codes only. Never a name, a number, an address or a body.

    Deliberately reports readiness in FIVE separate lines rather than one word.
    "Ready" collapsed five different situations into one, and an operator acting
    on it could not tell a wave that is finished from a wave whose customer
    lookups all failed on a network error.
    """
    unresolved_lookups = sum(1 for lookup in lookups.values() if lookup.outcome == LOOKUP_UNDETERMINED)
    mapping_outstanding = [
        proposal.as_safe_dict()
        for proposal in proposals
        if proposal.status not in (PROPOSAL_ALREADY_MAPPED,) and not agreement.agreed(proposal)
    ]
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
            "pending_digest": mapping_pending_digest(proposals, agreement),
        },
        "customers": {
            "ready": customers_ready,
            "pending_confirmation": customers_pending,
            "confirmed_not_yet_created": customers_confirmed,
            "blocked": customers_blocked,
            "in_flight_needs_reconciliation": in_flight,
            "lookups_undetermined": unresolved_lookups,
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
            "all_clear": (
                not mapping_outstanding
                and customers_pending == 0
                and customers_confirmed == 0
                and not customers_blocked
                and unresolved_lookups == 0
                and not in_flight
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
class ConfirmRequest:
    """One confirmation command, exactly as the operator spelled it out.

    Nothing is read from stdin, here or anywhere else in this module. An EOF, a
    closed stdin or a Docker run without a TTY is not consent, and the way to
    make that impossible is to have no code that could mistake it for consent.
    """

    confirm_customers: tuple[str, ...] = ()
    skip_customers: tuple[str, ...] = ()
    confirm_all_pending: bool = False
    expected_pending_digest: str | None = None
    # Corrections. Applied to ONE customer, and they reset the confirmation:
    # the digest changes, so the record returns to pending and has to be agreed
    # to again with the new values visible.
    correct_phone: str | None = None
    correct_first_name: str | None = None
    correct_last_name: str | None = None
    correct_email: str | None = None
    # Mapping.
    confirm_services: tuple[int, ...] = ()
    confirm_all_services: bool = False
    expected_mapping_digest: str | None = None


def apply_confirmations(
    inputs: PrepareInputs,
    request: ConfirmRequest,
    *,
    proposals: list[ServiceProposal] | None = None,
) -> dict[str, Any]:
    """Record confirmations, skips and corrections. Touches no network.

    A batch confirmation is accepted only against the digest of the list that
    was printed. That is what makes "confirm everything" safe to offer at all:
    it is not a standing permission, it is a yes to one specific set, and a set
    that changed by so much as one customer no longer matches.
    """
    store = CustomerDecisionStore(inputs.state_dir)
    outcome: dict[str, Any] = {"confirmed": [], "skipped": [], "corrected": [], "refused": []}

    with store:
        decisions = store.load()

        if request.correct_phone is not None:
            phone = normalized_international_phone(request.correct_phone)
            if phone is None:
                raise PrepareError("the phone number to correct is not a usable international number")
            record = decisions.get(phone)
            if record is None:
                raise PrepareError("no customer decision for that number")
            if record.state in (STATE_CREATED, STATE_IN_FLIGHT):
                raise PrepareError("that customer has already been created or is mid-creation")
            corrected = replace(
                record,
                first_name=_text(request.correct_first_name) or record.first_name,
                last_name=_text(request.correct_last_name) or record.last_name,
                email=_text(request.correct_email) or record.email,
                # A correction always returns the record to pending: the person
                # who confirms it must see the corrected values, not the old ones.
                state=STATE_PENDING,
                blocked_reason=None,
            ).with_digest()
            decisions.records[phone] = corrected
            outcome["corrected"].append(phone_fingerprint(phone))

        for raw in request.skip_customers:
            phone = normalized_international_phone(raw)
            if phone is None or decisions.get(phone) is None:
                outcome["refused"].append({"reason": "unknown_customer"})
                continue
            decisions.set_state(phone, STATE_SKIPPED)
            outcome["skipped"].append(phone_fingerprint(phone))

        targets: list[str] = []
        if request.confirm_all_pending:
            current = pending_digest(decisions)
            if request.expected_pending_digest != current:
                # The list moved between the review and the confirmation.
                raise PrepareError(
                    "the pending customer list has changed since it was printed; "
                    "re-run prepare, read the new list, and confirm against the new digest"
                )
            targets = [record.phone for record in decisions.in_state(STATE_PENDING)]
        else:
            for raw in request.confirm_customers:
                phone = normalized_international_phone(raw)
                if phone is None:
                    outcome["refused"].append({"reason": "phone_unusable"})
                    continue
                targets.append(phone)

        for phone in targets:
            record = decisions.get(phone)
            if record is None:
                outcome["refused"].append({"reason": "unknown_customer"})
                continue
            if record.state != STATE_PENDING:
                outcome["refused"].append({"state": record.state, "reason": "not_pending"})
                continue
            if not record.matches_shown():
                # The data moved under a confirmation aimed at the old values.
                outcome["refused"].append({"reason": "decision_stale"})
                continue
            decisions.set_state(phone, STATE_CONFIRMED)
            outcome["confirmed"].append(phone_fingerprint(phone))

        store.save(decisions)
        outcome["customer_states"] = decisions.summary()
        outcome["pending_digest"] = pending_digest(decisions)

        # Inside the lock: the mapping agreement and the proposed manifest live
        # in the same state directory, and a second run rewriting them while
        # this one is halfway through would leave the two disagreeing.
        if proposals is not None and (request.confirm_services or request.confirm_all_services):
            outcome["mapping"] = _confirm_mapping(inputs, request, proposals)
    return outcome


def _confirm_mapping(
    inputs: PrepareInputs, request: ConfirmRequest, proposals: list[ServiceProposal]
) -> dict[str, Any]:
    agreement = _load_agreement(inputs.state_dir)
    if request.confirm_all_services:
        current = mapping_pending_digest(proposals, agreement)
        if request.expected_mapping_digest != current:
            raise PrepareError(
                "the proposed service mapping has changed since it was printed; "
                "re-run prepare and confirm against the new digest"
            )

    wanted = set(request.confirm_services)
    confirmed: list[int] = []
    refused: list[dict[str, Any]] = []
    for proposal in proposals:
        service_id = proposal.source.altegio_service_id
        if not (request.confirm_all_services or service_id in wanted):
            continue
        if not proposal.actionable:
            # Ambiguous, absent from the catalogue, or a service the catalogue
            # says the chosen master cannot perform. None of those is something
            # a confirmation can fix.
            refused.append({"altegio_service_id": service_id, "status": proposal.status})
            continue
        agreement.confirm(proposal)
        confirmed.append(service_id)

    _write_private_json(inputs.state_dir / FILE_MAPPING_AGREEMENT, agreement.to_json())
    patch = manifest_service_patch(proposals, agreement)
    merged = merge_manifest_services(
        inputs.manifest_json,
        altegio_company_id=inputs.altegio_company_id,
        patch=patch,
    )
    _write_private_json(inputs.state_dir / FILE_MANIFEST_PROPOSED, merged)
    return {
        "confirmed_service_ids": sorted(confirmed),
        "refused": refused,
        "manifest_entries_written": len(patch),
        "pending_digest": mapping_pending_digest(proposals, agreement),
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
