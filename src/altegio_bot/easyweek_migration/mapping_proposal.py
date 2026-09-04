"""Proposing the Altegio → EasyWeek service mapping, without ever deciding it.

The mapping is the part of a migration that used to be done by eye: open Altegio
in one window, EasyWeek in the other, copy a UUID, hope the two "Wimpern 1:1"
entries are the same one. This module does the collecting — which services the
future bookings actually use, what the live catalogue holds, and which pairs are
even plausible — and then stops. A proposal is an argument put in front of a
person; the manifest is still written from a decision, not from a similarity.

What is allowed to appear in a proposal, and what is allowed to survive it
-------------------------------------------------------------------------
A proposal may be argued from names, because a person reading it needs to see
"Wimpernverlängerung 2D" next to "Wimpernverlängerung 2D" to judge it at all.
Nothing downstream may be argued from names: the accepted mapping is a UUID
written into the manifest, and the apply path proves that UUID against the
catalogue exactly as it did before this module existed.

There is no fuzzy matching anywhere. The only automatic pairing is equality of
the canonical name — the same normalisation the manifest parser and the service
proof already use, and no other. Everything else is presented as a candidate
list, or as no candidate at all. A near-miss is not a match; "one letter off" in
a salon catalogue is usually a different service at a different price.

Cost and duration are shown for every candidate because they are the fields that
catch the plausible-but-wrong pair, and because they are the manifest's own
baseline: an operator who accepts a proposal is accepting those numbers as the
values a per-booking override will later be detected against.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass, field
from typing import Any, Final

from altegio_bot.easyweek_migration.manifest import BranchMapping
from altegio_bot.easyweek_migration.money import AmountError, from_minor_units, read_amount
from altegio_bot.easyweek_migration.service_catalog import (
    CatalogService,
    CatalogSnapshot,
    normalize_service_name,
)

# What a proposal is. Only ACCEPTED_UNIQUE may be written into a manifest without
# the operator choosing between candidates, and even that needs a confirmation.
PROPOSAL_UNIQUE_NAME: Final = "unique_name_match"
PROPOSAL_AMBIGUOUS: Final = "ambiguous_candidates"
PROPOSAL_NO_CANDIDATE: Final = "no_candidate"
PROPOSAL_ALREADY_MAPPED: Final = "already_mapped"
PROPOSAL_CONFLICTS_WITH_MANIFEST: Final = "conflicts_with_manifest"
PROPOSAL_SOURCE_NAME_UNUSABLE: Final = "source_name_unusable"

# Whether the catalogue said this service can be booked with the chosen master.
STAFF_AVAILABILITY_PROVEN: Final = "available_for_selected_staff"
STAFF_AVAILABILITY_ABSENT: Final = "not_available_for_selected_staff"
# The catalogue row carried no staff list at all. Not a failure and NOT a pass:
# the confirmed catalogue response documents no per-employee availability field,
# so the honest answer is that this stage cannot prove it, and the operator has
# to confirm it in the EasyWeek interface. Inventing the field would be exactly
# the PR-9 failure again.
STAFF_AVAILABILITY_UNSTATED: Final = "not_stated_by_catalogue"

_STAFF_KEYS: Final = ("employees", "staff", "employee_uuids", "staff_uuids")


def _exact_price(raw: object) -> str | None:
    """The source price as an exact decimal string, for display only.

    No conversion to minor units: that needs a currency, Altegio does not state
    one on the service row, and assuming EUR here would be an assumption printed
    next to a number an operator is about to confirm.
    """
    try:
        amount = read_amount(raw)
    except AmountError:
        return None
    if not amount.present or amount.value is None:
        return None
    return str(amount.value)


def read_service_staff_uuids(row: object) -> frozenset[str] | None:
    """The catalogue row's own staff list, or ``None`` when it states none.

    ``None`` and ``frozenset()`` are deliberately different answers: the first is
    "the catalogue did not say", the second is "the catalogue said nobody". They
    lead to different operator instructions, and collapsing them would turn a
    silence into a claim.
    """
    if not isinstance(row, dict):
        return None
    for key in _STAFF_KEYS:
        if key not in row:
            continue
        value = row[key]
        if not isinstance(value, list):
            return None
        uuids: set[str] = set()
        for item in value:
            if isinstance(item, str):
                uuids.add(item)
            elif isinstance(item, dict) and isinstance(item.get("uuid"), str):
                uuids.add(item["uuid"])
            else:
                # A shape we do not recognise. Reading half of it would be worse
                # than reading none of it.
                return None
        return frozenset(uuids)
    return None


@dataclass(frozen=True)
class SourceService:
    """One Altegio service as the future bookings actually use it."""

    altegio_service_id: int
    name: str
    booking_count: int
    # Every distinct price and duration the source showed, exactly as written.
    # More than one means the bookings disagree about the service's own
    # baseline, which the operator has to see before choosing one.
    observed_prices: tuple[str, ...] = ()
    observed_durations: tuple[int, ...] = ()

    @property
    def normalized_name(self) -> str | None:
        return normalize_service_name(self.name)


@dataclass(frozen=True)
class CandidateService:
    """One EasyWeek catalogue entry offered as a target, with its numbers."""

    easyweek_service_uuid: str
    name: str
    normalized_name: str
    currency: str
    price_minor: int
    duration_minutes: int
    staff_availability: str

    @property
    def price_text(self) -> str:
        """The catalogue price as an exact decimal, or its minor units verbatim.

        A currency this migration cannot express in minor units is shown as the
        integer the API sent, labelled as such. Dividing it by 100 anyway would
        print a plausible wrong price next to a confirmation button.
        """
        try:
            return str(from_minor_units(self.price_minor, currency=self.currency))
        except AmountError:
            return f"{self.price_minor} (minor units)"

    def as_operator_dict(self) -> dict[str, Any]:
        return {
            "easyweek_service_uuid": self.easyweek_service_uuid,
            "easyweek_service_name": self.name,
            "currency": self.currency,
            "price": self.price_text,
            "price_minor_units": self.price_minor,
            "duration_minutes": self.duration_minutes,
            "staff_availability": self.staff_availability,
        }


@dataclass(frozen=True)
class ServiceProposal:
    """What the stage would map, why, and what it refuses to decide alone."""

    altegio_company_id: int
    source: SourceService
    status: str
    candidates: tuple[CandidateService, ...] = ()
    existing_uuid: str | None = None

    @property
    def chosen(self) -> CandidateService | None:
        """The single candidate a confirmation would accept, or nothing."""
        if self.status != PROPOSAL_UNIQUE_NAME or len(self.candidates) != 1:
            return None
        return self.candidates[0]

    @property
    def actionable(self) -> bool:
        """Can this proposal become a manifest entry once confirmed?"""
        candidate = self.chosen
        # A service the catalogue says the chosen master cannot perform is never
        # actionable, however exactly the names agree.
        return candidate is not None and candidate.staff_availability != STAFF_AVAILABILITY_ABSENT

    def presentation(self) -> dict[str, Any]:
        """Exactly what a confirmation is about. Digested; see the decision store."""
        candidate = self.chosen
        return {
            "altegio_company_id": self.altegio_company_id,
            "altegio_service_id": self.source.altegio_service_id,
            "altegio_service_name": self.source.name,
            "booking_count": self.source.booking_count,
            "status": self.status,
            "target": candidate.as_operator_dict() if candidate else None,
        }

    def as_operator_dict(self) -> dict[str, Any]:
        """The reviewable form. Carries service names, so it is operator-only."""
        payload = self.presentation()
        payload["candidates"] = [candidate.as_operator_dict() for candidate in self.candidates]
        payload["existing_manifest_uuid"] = self.existing_uuid
        payload["observed_source_prices"] = list(self.source.observed_prices)
        payload["observed_source_durations_minutes"] = list(self.source.observed_durations)
        return payload

    def as_safe_dict(self) -> dict[str, Any]:
        """The machine form. Ids, UUIDs, counts and codes — never a name."""
        candidate = self.chosen
        return {
            "altegio_company_id": self.altegio_company_id,
            "altegio_service_id": self.source.altegio_service_id,
            "booking_count": self.source.booking_count,
            "status": self.status,
            "candidate_count": len(self.candidates),
            "easyweek_service_uuid": candidate.easyweek_service_uuid if candidate else None,
            "staff_availability": candidate.staff_availability if candidate else None,
            "actionable": self.actionable,
        }


def collect_source_services(records: list[dict[str, Any]], *, staff_ids: set[int] | None = None) -> list[SourceService]:
    """Tally the services the given records use, keeping the exact source name.

    ``staff_ids`` narrows to the masters in this wave, because a service only
    another master performs is not this wave's problem and asking an operator to
    map it is how the preparation grew from minutes into an afternoon.
    """
    counts: Counter[int] = Counter()
    names: dict[int, str] = {}
    prices: dict[int, set[str]] = {}
    durations: dict[int, set[int]] = {}

    for record in records:
        if staff_ids is not None and _staff_id_of(record) not in staff_ids:
            continue
        raw_services = record.get("services")
        if not isinstance(raw_services, list):
            continue
        for item in raw_services:
            if not isinstance(item, dict) or type(item.get("id")) is not int:
                continue
            service_id = item["id"]
            counts[service_id] += 1
            title = item.get("title")
            if isinstance(title, str) and title.strip() and service_id not in names:
                names[service_id] = title.strip()
            price = _exact_price(item.get("cost"))
            if price is not None:
                prices.setdefault(service_id, set()).add(price)
            seance = item.get("seance_length")
            if type(seance) is int and seance > 0 and seance % 60 == 0:
                durations.setdefault(service_id, set()).add(seance // 60)

    return [
        SourceService(
            altegio_service_id=service_id,
            name=names.get(service_id, ""),
            booking_count=count,
            observed_prices=tuple(sorted(prices.get(service_id, ()))),
            observed_durations=tuple(sorted(durations.get(service_id, ()))),
        )
        for service_id, count in sorted(counts.items())
    ]


def _staff_id_of(record: dict[str, Any]) -> object:
    flat = record.get("staff_id")
    if flat is not None:
        return flat
    staff = record.get("staff")
    if isinstance(staff, dict):
        return staff.get("id")
    return None


def _availability(service_uuid: str, staff_uuids: dict[str, frozenset[str] | None], selected: set[str]) -> str:
    stated = staff_uuids.get(service_uuid)
    if stated is None:
        return STAFF_AVAILABILITY_UNSTATED
    if not selected:
        return STAFF_AVAILABILITY_UNSTATED
    return STAFF_AVAILABILITY_PROVEN if selected & stated else STAFF_AVAILABILITY_ABSENT


def propose_service_mapping(
    *,
    altegio_company_id: int,
    source_services: list[SourceService],
    catalog: CatalogSnapshot,
    catalog_staff: dict[str, frozenset[str] | None],
    selected_staff_uuids: set[str],
    branch: BranchMapping | None,
) -> list[ServiceProposal]:
    """One proposal per source service. Decides nothing.

    A service the manifest already maps is reported as such and left alone — the
    mapping is cumulative, and re-proposing a mapping an earlier wave reviewed
    would ask the operator to confirm the same thing every wave. It is only
    flagged when the live catalogue no longer holds the mapped UUID, which is the
    one case where "already mapped" has stopped being true.
    """
    by_name: dict[str, list[CatalogService]] = {}
    for service in catalog.services:
        by_name.setdefault(service.normalized_name, []).append(service)

    def _candidate(service: CatalogService) -> CandidateService:
        return CandidateService(
            # The catalogue's own spelling for the person reading the proposal,
            # falling back to the normalised form when the row had none.
            easyweek_service_uuid=service.uuid,
            name=service.display_name or service.normalized_name,
            normalized_name=service.normalized_name,
            currency=service.currency,
            price_minor=service.price_minor,
            duration_minutes=service.duration_minutes,
            staff_availability=_availability(service.uuid, catalog_staff, selected_staff_uuids),
        )

    proposals: list[ServiceProposal] = []
    for source in source_services:
        existing = branch.service(source.altegio_service_id) if branch is not None else None
        if existing is not None:
            live = catalog.by_uuid(existing.easyweek_service_uuid)
            status = PROPOSAL_ALREADY_MAPPED if live is not None else PROPOSAL_CONFLICTS_WITH_MANIFEST
            proposals.append(
                ServiceProposal(
                    altegio_company_id=altegio_company_id,
                    source=source,
                    status=status,
                    candidates=(_candidate(live),) if live is not None else (),
                    existing_uuid=existing.easyweek_service_uuid,
                )
            )
            continue

        normalized = source.normalized_name
        if normalized is None:
            # No usable name in the source. There is nothing to argue from, and
            # a service id alone matches nothing in EasyWeek.
            proposals.append(
                ServiceProposal(
                    altegio_company_id=altegio_company_id,
                    source=source,
                    status=PROPOSAL_SOURCE_NAME_UNUSABLE,
                )
            )
            continue

        matches = by_name.get(normalized, [])
        candidates = tuple(_candidate(service) for service in matches)
        if len(candidates) == 1:
            status = PROPOSAL_UNIQUE_NAME
        elif candidates:
            # Two catalogue entries fold to the same name. Picking one by price
            # or by position is guessing, and a guess here books somebody into
            # the wrong service at the right price.
            status = PROPOSAL_AMBIGUOUS
        else:
            status = PROPOSAL_NO_CANDIDATE
        proposals.append(
            ServiceProposal(
                altegio_company_id=altegio_company_id,
                source=source,
                status=status,
                candidates=candidates,
            )
        )
    return proposals


# ---------------------------------------------------------------------------
# Persisting an agreed mapping
# ---------------------------------------------------------------------------


def proposal_digest(proposal: ServiceProposal) -> str:
    """Digest of exactly what an operator was shown about one service."""
    blob = json.dumps(proposal.presentation(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


@dataclass
class MappingAgreement:
    """Confirmed service mappings, keyed by ``company_id:service_id``.

    Persisted next to the customer decisions and reused: an operator who agreed
    that Altegio 12345 is EasyWeek ``d3f1…`` has agreed to it for good, and a
    later run that finds the same proposal must not ask again. A proposal whose
    digest has MOVED — a different target, a different price, a renamed source
    service — loses the agreement, because the agreement was about those values.
    """

    entries: dict[str, str] = field(default_factory=dict)

    @staticmethod
    def key(company_id: int, service_id: int) -> str:
        return f"{company_id}:{service_id}"

    def agreed(self, proposal: ServiceProposal) -> bool:
        stored = self.entries.get(self.key(proposal.altegio_company_id, proposal.source.altegio_service_id))
        return stored is not None and stored == proposal_digest(proposal)

    def confirm(self, proposal: ServiceProposal) -> None:
        if not proposal.actionable:
            raise ValueError("only an actionable proposal can be confirmed")
        self.entries[self.key(proposal.altegio_company_id, proposal.source.altegio_service_id)] = proposal_digest(
            proposal
        )

    def to_json(self) -> dict[str, Any]:
        return {"version": 1, "agreed": dict(sorted(self.entries.items()))}

    @classmethod
    def from_json(cls, payload: object) -> MappingAgreement:
        if not isinstance(payload, dict) or payload.get("version") != 1:
            raise ValueError("mapping agreement has an unexpected version")
        agreed = payload.get("agreed")
        if not isinstance(agreed, dict) or not all(
            isinstance(k, str) and isinstance(v, str) for k, v in agreed.items()
        ):
            raise ValueError("mapping agreement has an unexpected shape")
        return cls(entries=dict(agreed))


def manifest_service_patch(proposals: list[ServiceProposal], agreement: MappingAgreement) -> dict[str, dict[str, Any]]:
    """The manifest ``services`` entries the CONFIRMED proposals would add.

    All four catalogue fields, because the manifest parser requires them for any
    writing mode and because they are the baseline a per-booking override is
    detected against. Only confirmed, actionable proposals appear; nothing else
    reaches a file the apply path reads.
    """
    patch: dict[str, dict[str, Any]] = {}
    for proposal in proposals:
        candidate = proposal.chosen
        if candidate is None or not proposal.actionable or not agreement.agreed(proposal):
            continue
        # A currency with no exact minor-unit form has no manifest price, and a
        # manifest entry without one would be refused by the parser anyway.
        # Better to leave the service unmapped and say so than to write a number
        # nothing verified.
        try:
            catalog_price = str(from_minor_units(candidate.price_minor, currency=candidate.currency))
        except AmountError:
            continue
        patch[str(proposal.source.altegio_service_id)] = {
            "easyweek_service_uuid": candidate.easyweek_service_uuid,
            "catalog_duration_minutes": candidate.duration_minutes,
            "catalog_price": catalog_price,
            # The manifest baseline is compared under the shared normalisation,
            # so it is the normalised name that is written — not the display one.
            "catalog_service_name": candidate.normalized_name,
            "catalog_currency": candidate.currency,
        }
    return patch


def merge_manifest_services(
    manifest_json: dict[str, Any],
    *,
    altegio_company_id: int,
    patch: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    """Return a copy of the manifest with the patch merged into one branch.

    Additive only. An entry the manifest already holds is never overwritten,
    however confident a proposal is: the earlier value is one an operator
    reviewed, and a wave that silently re-pointed a previous wave's service is a
    wave whose earlier bookings can no longer be reconciled.
    """
    merged = json.loads(json.dumps(manifest_json))
    branches = merged.setdefault("branches", {})
    if not isinstance(branches, dict):
        raise ValueError("manifest has no branches object")
    branch = branches.setdefault(str(altegio_company_id), {})
    if not isinstance(branch, dict):
        raise ValueError("manifest branch is not an object")
    services = branch.setdefault("services", {})
    if not isinstance(services, dict):
        raise ValueError("manifest branch has no services object")
    for service_id, entry in sorted(patch.items()):
        if service_id in services:
            continue
        services[service_id] = entry
    return merged
