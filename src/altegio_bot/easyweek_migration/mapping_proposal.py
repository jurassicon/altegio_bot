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

from altegio_bot.easyweek_migration.manifest import BranchMapping, ServiceMapping, canonical_service_name
from altegio_bot.easyweek_migration.money import AmountError, from_minor_units, read_amount, to_minor_units
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
# The manifest's UUID is still in the catalogue, but the service behind it has
# moved: renamed, repriced, re-timed, or re-denominated. A matching UUID is not
# a matching service, and reporting this as `already_mapped` told an operator a
# wave was ready while the baseline every override check compares against had
# quietly gone stale.
PROPOSAL_BASELINE_DRIFT: Final = "existing_mapping_drift"
# The manifest maps the service but never recorded the reviewed identity, so
# there is no baseline to compare the live catalogue against. Not drift — we
# cannot tell — and for that reason not readiness either.
PROPOSAL_BASELINE_INCOMPLETE: Final = "existing_mapping_baseline_incomplete"
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

# The baseline fields a manifest entry freezes, and the order they are reported
# in. One tuple so the comparison, the report and the tests cannot drift apart.
BASELINE_FIELDS: Final = (
    "easyweek_service_uuid",
    "catalog_service_name",
    "catalog_currency",
    "catalog_price",
    "catalog_duration_minutes",
)


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
    # The Altegio staff ids that ACTUALLY perform this service in the in-scope
    # bookings — not the wave's staff list. The distinction is the whole point:
    # availability was judged against the union of every selected master, so a
    # service master A books, which the catalogue offers only to master B,
    # counted as available because B happened to be in the same wave.
    altegio_staff_ids: tuple[int, ...] = ()

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
    # The evidence the availability verdict was reached on, so a reader — and
    # the digest — can see WHY it says what it says. Required is the set of
    # EasyWeek staff uuids the masters who actually book this service map to;
    # stated is what the catalogue row named, or `None` when it named nothing.
    required_staff_uuids: tuple[str, ...] = ()
    stated_staff_uuids: tuple[str, ...] | None = None
    # Masters who use the service but whose EasyWeek uuid the manifest does not
    # give. Availability cannot be proven for somebody we cannot name.
    unmapped_staff_ids: tuple[int, ...] = ()

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
        """The canonical, deterministically ordered view of one candidate.

        Feeds both the operator review and the proposal digest, so a field a
        person was shown cannot change without the agreement lapsing.
        """
        return {
            "easyweek_service_uuid": self.easyweek_service_uuid,
            "easyweek_service_name": self.name,
            "easyweek_service_normalized_name": self.normalized_name,
            "currency": self.currency,
            "price": self.price_text,
            "price_minor_units": self.price_minor,
            "duration_minutes": self.duration_minutes,
            "staff_availability": self.staff_availability,
            "required_staff_uuids": sorted(self.required_staff_uuids),
            "stated_staff_uuids": sorted(self.stated_staff_uuids) if self.stated_staff_uuids is not None else None,
            "unmapped_altegio_staff_ids": sorted(self.unmapped_staff_ids),
        }


@dataclass(frozen=True)
class ServiceProposal:
    """What the stage would map, why, and what it refuses to decide alone."""

    altegio_company_id: int
    source: SourceService
    status: str
    candidates: tuple[CandidateService, ...] = ()
    existing_uuid: str | None = None
    # The manifest baseline this proposal was compared against, and which of its
    # fields disagree with the live catalogue. Empty when there is nothing
    # mapped yet, or when the mapping still matches.
    existing_baseline: tuple[tuple[str, str], ...] = ()
    drift_fields: tuple[str, ...] = ()

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
        # A service the catalogue says the master who actually books it cannot
        # perform is never actionable, however exactly the names agree.
        return candidate is not None and candidate.staff_availability != STAFF_AVAILABILITY_ABSENT

    @property
    def settled(self) -> bool:
        """Is this service done — mapped, matching, and needing no decision?

        Only an unchanged existing mapping qualifies. Drift and an unverifiable
        baseline both mean the wave is NOT ready, which is the correction: a
        matching UUID over a moved service used to read as readiness.
        """
        return self.status == PROPOSAL_ALREADY_MAPPED

    def review_payload(self) -> dict[str, Any]:
        """THE canonical view of one proposal: shown to a person, and digested.

        One structure for both jobs on purpose. When the review output and the
        digest were built separately, a field could be displayed without being
        covered — the observed source prices and durations were exactly that, so
        a service whose bookings changed price kept an agreement made about the
        old one.

        Every collection here is sorted by a stable key, so two runs over the
        same data digest identically however the API happened to order its rows.
        Nothing time-, sequence- or transport-derived goes in.
        """
        candidate = self.chosen
        return {
            "altegio_company_id": self.altegio_company_id,
            "altegio_service_id": self.source.altegio_service_id,
            "altegio_service_name": self.source.name,
            "altegio_service_normalized_name": self.source.normalized_name,
            "booking_count": self.source.booking_count,
            "observed_source_prices": sorted(self.source.observed_prices),
            "observed_source_durations_minutes": sorted(self.source.observed_durations),
            "altegio_staff_ids": sorted(self.source.altegio_staff_ids),
            "status": self.status,
            "actionable": self.actionable,
            "target": candidate.as_operator_dict() if candidate else None,
            "candidates": [
                item.as_operator_dict() for item in sorted(self.candidates, key=lambda c: c.easyweek_service_uuid)
            ],
            "existing_manifest_uuid": self.existing_uuid,
            "existing_manifest_baseline": {key: value for key, value in sorted(self.existing_baseline)},
            "drift_fields": sorted(self.drift_fields),
        }

    def as_operator_dict(self) -> dict[str, Any]:
        """The reviewable form. Carries service names, so it is operator-only."""
        payload = self.review_payload()
        payload["review_digest"] = proposal_digest(self)
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
            "settled": self.settled,
            "drift_fields": sorted(self.drift_fields),
            "review_digest": proposal_digest(self),
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
    performed_by: dict[int, set[int]] = {}

    for record in records:
        staff_id = _staff_id_of(record)
        if staff_ids is not None and staff_id not in staff_ids:
            continue
        raw_services = record.get("services")
        if not isinstance(raw_services, list):
            continue
        for item in raw_services:
            if not isinstance(item, dict) or type(item.get("id")) is not int:
                continue
            service_id = item["id"]
            counts[service_id] += 1
            if type(staff_id) is int:
                performed_by.setdefault(service_id, set()).add(staff_id)
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
            altegio_staff_ids=tuple(sorted(performed_by.get(service_id, ()))),
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


def _availability(stated: frozenset[str] | None, required: frozenset[str], *, unmapped: bool) -> str:
    """Is this service available to every master who actually books it?

    The correction. The previous rule asked whether the catalogue offered the
    service to ANY selected master, so a wave containing masters A and B proved
    a service that only B may perform even when only A ever books it — and the
    apply would then place A's bookings on a service A cannot deliver.

    ``required`` is now the set of EasyWeek staff uuids belonging to the masters
    the in-scope bookings show performing this service. Every one of them has to
    be covered; a stranger's coverage proves nothing.

    A catalogue that states nothing, and a master we cannot name in EasyWeek,
    both yield ``UNSTATED`` — the honest "we cannot tell", which the runbook
    sends to a person rather than treating as a pass.
    """
    if stated is None:
        return STAFF_AVAILABILITY_UNSTATED
    if not required or unmapped:
        # Nobody to check against, or somebody we cannot check. Either way the
        # catalogue's list cannot be turned into a verdict.
        return STAFF_AVAILABILITY_UNSTATED
    return STAFF_AVAILABILITY_PROVEN if required <= stated else STAFF_AVAILABILITY_ABSENT


def _baseline_drift(
    existing: ServiceMapping, live: CatalogService
) -> tuple[tuple[tuple[str, str], ...], tuple[str, ...]]:
    """The manifest's frozen identity, and which of its fields the catalogue no
    longer agrees with.

    A UUID that is still in the catalogue used to be enough to call a mapping
    ``already_mapped``. It is not: the manifest's four catalogue fields are the
    baseline every per-booking override is detected against, so a service that
    was renamed, repriced or re-timed under a stable UUID silently invalidates
    that baseline while the report says the wave is ready.

    Returns the baseline as displayable text plus the disagreeing field names.
    """
    baseline: list[tuple[str, str]] = [("easyweek_service_uuid", existing.easyweek_service_uuid)]
    drift: list[str] = []

    expected_name = canonical_service_name(existing.catalog_service_name)
    baseline.append(("catalog_service_name", expected_name or ""))
    if expected_name != live.normalized_name:
        drift.append("catalog_service_name")

    expected_currency = (existing.catalog_currency or "").strip().upper()
    baseline.append(("catalog_currency", expected_currency))
    if expected_currency != live.currency:
        drift.append("catalog_currency")

    # Price is compared in minor units under the MANIFEST's currency, which is
    # the value an operator reviewed. A currency mismatch is already reported
    # above; an amount we cannot express exactly is a disagreement, not a pass.
    try:
        expected_minor = to_minor_units(existing.catalog_price, currency=expected_currency)
    except AmountError:
        expected_minor = None
    baseline.append(
        ("catalog_price", str(existing.catalog_price.value) if existing.catalog_price.value is not None else "")
    )
    if expected_minor is None or expected_minor != live.price_minor:
        drift.append("catalog_price")

    expected_minutes = existing.catalog_duration.minutes
    baseline.append(("catalog_duration_minutes", str(expected_minutes) if expected_minutes is not None else ""))
    if expected_minutes != live.duration_minutes:
        drift.append("catalog_duration_minutes")

    return tuple(baseline), tuple(drift)


def propose_service_mapping(
    *,
    altegio_company_id: int,
    source_services: list[SourceService],
    catalog: CatalogSnapshot,
    catalog_staff: dict[str, frozenset[str] | None],
    branch: BranchMapping | None,
) -> list[ServiceProposal]:
    """One proposal per source service. Decides nothing.

    A service the manifest already maps and whose live catalogue entry still
    matches the reviewed baseline is reported as ``already_mapped`` and left
    alone — the mapping is cumulative, and re-proposing it every wave is exactly
    what this stage exists to stop. It stops being "already mapped" in two ways:
    the UUID is gone from the catalogue, or the service behind that UUID has
    moved. Both need a person, and neither is readiness.

    Availability is judged per service against the masters who actually book it,
    which is why ``branch`` supplies the Altegio-id → EasyWeek-uuid mapping
    rather than the caller passing a flat set of the wave's staff.
    """
    by_name: dict[str, list[CatalogService]] = {}
    for service in catalog.services:
        by_name.setdefault(service.normalized_name, []).append(service)

    def _candidate(service: CatalogService, source: SourceService) -> CandidateService:
        required: set[str] = set()
        unmapped: list[int] = []
        for staff_id in source.altegio_staff_ids:
            target = branch.staff_uuid(staff_id) if branch is not None else None
            if target is None:
                unmapped.append(staff_id)
            else:
                required.add(target)
        return CandidateService(
            # The catalogue's own spelling for the person reading the proposal,
            # falling back to the normalised form when the row had none.
            easyweek_service_uuid=service.uuid,
            name=service.display_name or service.normalized_name,
            normalized_name=service.normalized_name,
            currency=service.currency,
            price_minor=service.price_minor,
            duration_minutes=service.duration_minutes,
            staff_availability=_availability(
                catalog_staff.get(service.uuid),
                frozenset(required),
                unmapped=bool(unmapped),
            ),
            required_staff_uuids=tuple(sorted(required)),
            stated_staff_uuids=(
                tuple(sorted(catalog_staff[service.uuid])) if catalog_staff.get(service.uuid) is not None else None
            ),
            unmapped_staff_ids=tuple(sorted(unmapped)),
        )

    proposals: list[ServiceProposal] = []
    for source in source_services:
        existing = branch.service(source.altegio_service_id) if branch is not None else None
        if existing is not None:
            live = catalog.by_uuid(existing.easyweek_service_uuid)
            baseline: tuple[tuple[str, str], ...] = ()
            drift: tuple[str, ...] = ()
            if live is None:
                # Includes the re-created-service case: the mapped UUID is not
                # in the catalogue, and a new UUID is never adopted by name.
                status = PROPOSAL_CONFLICTS_WITH_MANIFEST
            elif not existing.identity_complete:
                # Mapped, present, and nothing recorded to compare it against.
                # Not drift; not readiness either.
                status = PROPOSAL_BASELINE_INCOMPLETE
            else:
                baseline, drift = _baseline_drift(existing, live)
                status = PROPOSAL_BASELINE_DRIFT if drift else PROPOSAL_ALREADY_MAPPED
            proposals.append(
                ServiceProposal(
                    altegio_company_id=altegio_company_id,
                    source=source,
                    status=status,
                    candidates=(_candidate(live, source),) if live is not None else (),
                    existing_uuid=existing.easyweek_service_uuid,
                    existing_baseline=baseline,
                    drift_fields=drift,
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
        candidates = tuple(_candidate(service, source) for service in matches)
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
    """Digest of exactly what an operator was shown about one service.

    Computed from :meth:`ServiceProposal.review_payload` — the same structure the
    review prints — so "shown" and "digested" cannot come apart. Sorted keys and
    canonically ordered collections, so the digest depends on the data and not on
    the order the API happened to return its rows in.
    """
    blob = json.dumps(proposal.review_payload(), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
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
