"""Turning one Altegio record into a migration decision (PR-11.1).

Every source booking lands in exactly one of four buckets:

``skipped``           it is not ours to migrate at all — wrong branch, in the
                      past, cancelled, finished. Not a problem; not reported as
                      one.
``already_migrated``  the ledger already holds a proven target for it.
``blocked``           it *should* migrate but something is missing or ambiguous,
                      and guessing would put a real appointment in the wrong
                      place. An operator fixes it by hand.
``ready``             everything resolved to exactly one value and it can be
                      created.

The rule that shapes all of it: **anything not proven is blocked, never
approximated.** A booking an operator moves by hand costs five minutes. A booking
we place with the wrong master, at the wrong time, or on someone else's profile
costs a customer, and it is discovered by the customer.

Blocking is per-row. One unmapped master does not stop the run — the other
bookings are independent and keep going, which is what makes the blocked list
short enough for a human to work through.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final

from altegio_bot.easyweek_migration.altegio_source import ACTIVE_ATTENDANCE
from altegio_bot.easyweek_migration.bindings import (
    MUTATION_CART_TWO,
    MUTATION_SINGLE,
    PROVEN_SERVICE_AMOUNT,
    SUPPORTED_MUTATION_KINDS,
    BindingError,
    ServiceBinding,
    total_duration_minutes,
    validate_bindings,
)
from altegio_bot.easyweek_migration.customers import CustomerDirectory
from altegio_bot.easyweek_migration.cutover import Cutover, LocalTimeError, parse_altegio_local_to_utc
from altegio_bot.easyweek_migration.manifest import (
    STAFF_DEFERRED,
    STAFF_UNKNOWN,
    BranchMapping,
    MigrationManifest,
)
from altegio_bot.easyweek_migration.money import (
    AmountError,
    DurationError,
    amounts_differ,
    read_amount,
    read_duration_seconds,
    to_minor_units,
)

# -- outcomes ---------------------------------------------------------------
READY: Final = "ready"
ALREADY_MIGRATED: Final = "already_migrated"
BLOCKED: Final = "blocked"
SKIPPED: Final = "skipped"

# -- skip reasons (expected, not problems) ----------------------------------
SKIP_FOREIGN_COMPANY: Final = "foreign_company"
SKIP_PAST: Final = "starts_before_cutover"
SKIP_DELETED: Final = "source_deleted"
SKIP_CANCELED: Final = "source_canceled"
SKIP_COMPLETED: Final = "source_completed"
# Not an error and not a gap: this master is migrating in a LATER wave, and
# the manifest says so out loud.
SKIP_STAFF_DEFERRED: Final = "staff_deferred_to_later_wave"
# A booking with a PROVEN empty service list. The masters use these as breaks in
# their own calendar, and the owner confirmed on 2026-08-31 that they must not be
# recreated in EasyWeek as customer bookings. Deliberately a skip and not a
# block: a block is a row somebody has to fix, and there is nothing here to fix.
#
# Narrow on purpose. Only `services: []` — an explicit, well-formed, empty list —
# qualifies. Missing, null, not-a-list and corrupt entries stay data errors
# (`source_has_no_service`), because "the field is broken" and "the master
# blocked out an hour" are different facts and only one of them is safe to skip.
SKIP_EMPTY_SERVICES: Final = "source_empty_services_excluded"

# -- block reasons (need a human) -------------------------------------------
BLOCK_NO_RECORD_ID: Final = "source_record_id_invalid"
BLOCK_STATUS_UNRECOGNISED: Final = "source_status_unrecognised"
BLOCK_STAFF_MAPPING_MISSING: Final = "staff_mapping_missing"
# A master the wave selector never classified. Deliberately NOT reported as
# `staff_mapping_missing`: "we forgot to map her" and "we chose to defer her"
# must stay distinguishable, or a forgotten master reads as an intended one.
BLOCK_STAFF_NOT_IN_WAVE: Final = "staff_not_in_wave_scope"
BLOCK_SERVICE_MAPPING_MISSING: Final = "service_mapping_missing"
BLOCK_SERVICE_ID_INVALID: Final = "service_id_invalid"
BLOCK_NO_SERVICES: Final = "source_has_no_service"
BLOCK_MULTI_SERVICE: Final = "multi_service_unsupported"
# A two-service booking whose shape the cart canary did not prove: the same
# service twice, two different masters, or two currencies.
BLOCK_CART_UNSUPPORTED: Final = "cart_shape_unsupported"
# The source line books more (or fewer, or an unreadable number) than one unit
# of its service. No request shape this migration can send carries a quantity,
# so anything but an exact integer 1 is a booking it cannot express.
BLOCK_SERVICE_QUANTITY: Final = "source_service_quantity_unsupported"
# The booking's shape maps to a mutation contract this build cannot yet write
# end to end. Named separately from every data problem: nothing is wrong with
# the booking, and no operator action on the source will change it.
BLOCK_CONTRACT_UNSUPPORTED: Final = "mutation_contract_unsupported"

# The widest booking this migration can write. Two, and only because a real
# canary created one and read it back (plan §30.12); three has no evidence.
MAX_CART_SERVICES: Final = 2
BLOCK_CUSTOM_DURATION: Final = "custom_duration_unsupported"
BLOCK_CUSTOM_PRICE: Final = "custom_price_unsupported"
BLOCK_DURATION_UNKNOWN: Final = "duration_unknown"
BLOCK_PRICE_MALFORMED: Final = "price_malformed"
BLOCK_PRICE_BASELINE_MISSING: Final = "price_baseline_missing"
BLOCK_LEDGER_UNCERTAIN: Final = "ledger_uncertain_needs_reconcile"
BLOCK_SOURCE_CHANGED: Final = "source_changed_since_ledger"

# ``LocalTimeError`` reasons and the customer reasons pass through unchanged;
# they are already stable codes owned by their own modules.

# Ledger statuses that mean "a booking may exist and we cannot say". Spelled as
# literals rather than imported from :mod:`ledger`, which imports this module.
# The two lists are pinned together by a test.
LEDGER_UNRESOLVED_STATUSES: Final = frozenset({"uncertain", "pending"})


@dataclass(frozen=True)
class Decision:
    """One source booking's outcome, carrying only what a report may print."""

    outcome: str
    reason: str | None
    source_company_id: int
    source_record_id: int | None
    # Populated only for ``ready``: everything the writer needs, all proven.
    starts_at_utc: datetime | None = None
    easyweek_location_uuid: str | None = None
    easyweek_staff_uuid: str | None = None
    easyweek_customer_uuid: str | None = None
    source_fingerprint: str | None = None
    # Which contract writes this booking, and what it is made of. One binding
    # for `single`, two for `cart_two` — see `bindings`. The sequence is the
    # source's own order and is canonical everywhere downstream.
    mutation_kind: str = MUTATION_SINGLE
    bindings: tuple[ServiceBinding, ...] = ()
    # Set when the ledger already knew this row.
    target_booking_uuid: str | None = None

    @property
    def easyweek_service_uuid(self) -> str | None:
        """The single service's target uuid, or a refusal for a cart booking.

        A convenience for the single-service write path, computed from the
        bindings rather than stored beside them so the two can never disagree.
        It REFUSES for `cart_two` rather than returning the first of two: a
        caller reaching for "the service uuid" of a two-service booking is a
        caller that has not been taught about carts, and quietly handing it one
        of the pair is how half a booking gets written.
        """
        if not self.bindings:
            return None
        if self.mutation_kind != MUTATION_SINGLE:
            raise BindingError("a cart booking has no single service uuid; read `bindings`")
        return self.bindings[0].easyweek_service_uuid

    @property
    def duration_minutes(self) -> int | None:
        """Total booked minutes: one service's, or the sum of the cart's two.

        Safe for both kinds because a total is meaningful for both — unlike a
        service uuid, of which a cart has two and no single answer.
        """
        if not self.bindings:
            return None
        return total_duration_minutes(self.bindings)

    def as_safe_dict(self) -> dict[str, Any]:
        """Ids, codes and a UTC instant. No phone, no name, no payload."""
        return {
            "outcome": self.outcome,
            "reason": self.reason,
            "source_company_id": self.source_company_id,
            "source_record_id": self.source_record_id,
            "starts_at_utc": self.starts_at_utc.isoformat().replace("+00:00", "Z")
            if self.starts_at_utc is not None
            else None,
            "mutation_kind": self.mutation_kind,
            "services": [item.as_safe_dict() for item in self.bindings],
            "target_booking_uuid": self.target_booking_uuid,
        }


def _exact_int(value: object) -> int | None:
    """Exact ``int``. ``bool`` is not an id and ``"3"`` is not a number."""
    return value if type(value) is int else None


def _record_id(record: dict[str, Any]) -> int | None:
    raw = record.get("id")
    value = _exact_int(raw)
    if value is None or value <= 0:
        return None
    return value


def _staff_id(record: dict[str, Any]) -> object:
    """Altegio reports the master either flat or nested. Both are read; neither
    is invented — an absent id stays absent and the row blocks."""
    flat = record.get("staff_id")
    if flat is not None:
        return flat
    staff = record.get("staff")
    if isinstance(staff, dict):
        return staff.get("id")
    return None


def _services(record: dict[str, Any]) -> list[dict[str, Any]] | None:
    raw = record.get("services")
    if not isinstance(raw, list):
        return None
    if any(not isinstance(item, dict) for item in raw):
        return None
    return [item for item in raw if isinstance(item, dict)]


def _prove_service(
    service: dict[str, Any],
    *,
    branch: BranchMapping,
    staff_id: object,
) -> ServiceBinding | str:
    """One service line, proven against the manifest, or a block reason.

    Extracted so a cart booking's second service goes through exactly the same
    checks as its first — and as a single-service booking's only one. A second
    copy of these rules is how one of two services would quietly migrate at a
    price nobody reviewed.
    """
    service_id = _exact_int(service.get("id"))
    if service_id is None or service_id <= 0:
        return BLOCK_SERVICE_ID_INVALID

    # How many units of this service the booking carries.
    #
    # Neither request body has a quantity field — not `POST /bookings`, not the
    # proven cart shape — so the only amount this migration can express is one.
    # A source line saying `amount: 2` would be sent once and migrate half of
    # what the customer booked, at half the price and half the length.
    #
    # Strict `int` on purpose: `True` is not a quantity even though `True == 1`,
    # `"1"` is a string somebody typed, `1.0` is a float whose companions are
    # `1.5`, and a MISSING field is not evidence of one either. Each of them
    # blocks with the same named reason rather than being coerced.
    amount = service.get("amount")
    if type(amount) is not int or amount != PROVEN_SERVICE_AMOUNT:
        return BLOCK_SERVICE_QUANTITY

    # Mapping first: price and duration are checked against a baseline an
    # operator wrote down and verified. Without the mapping there is no
    # baseline, so the override checks below would have nothing to compare
    # against — and "nothing to compare against" must never read as "no
    # override".
    mapping = branch.service(service_id)
    if mapping is None:
        return BLOCK_SERVICE_MAPPING_MISSING
    if not mapping.identity_complete:
        # A writing mode needs the reviewed name and currency; without them
        # there is nothing for a readback to compare a service line against.
        return BLOCK_SERVICE_MAPPING_MISSING

    # A per-booking price override has no proven EasyWeek equivalent. Migrating
    # it as the catalogue price would quietly change what the customer was
    # promised, so the row goes to a human instead.
    #
    # Every read below distinguishes ABSENT from ZERO. `cost=90, cost_to_pay=0`
    # is a full discount, not a missing field, and the earlier "positive numbers
    # only" helper silently dropped exactly that case — a booking promised free
    # would have migrated at 90 EUR.
    try:
        price_to_pay = read_amount(service.get("cost_to_pay"))
        listed_price = read_amount(service.get("cost"))
        first_price = read_amount(service.get("first_cost"))
        discount = read_amount(service.get("discount"))
    except AmountError:
        # NaN, infinity, a boolean, a negative or unparseable amount. Not a
        # price we can reason about, so not a booking we migrate.
        return BLOCK_PRICE_MALFORMED

    catalog_price = mapping.catalog_price

    # The booking must state a price at all: with none, there is nothing to
    # compare to the catalogue and an override would be invisible.
    if not price_to_pay.present and not listed_price.present:
        return BLOCK_PRICE_BASELINE_MISSING

    # Exact Decimal comparison throughout — a cent of difference IS the override.
    for left, right in (
        (price_to_pay, listed_price),
        (first_price, listed_price),
        (price_to_pay, catalog_price),
        (listed_price, catalog_price),
        (first_price, catalog_price),
    ):
        if amounts_differ(left, right):
            return BLOCK_CUSTOM_PRICE
    # A discount of zero is not a discount; any other stated discount is.
    if discount.present and not discount.is_zero:
        return BLOCK_CUSTOM_PRICE

    # When Altegio DOES state the service's own length, it must equal the
    # reviewed catalogue length — otherwise the manifest baseline has gone stale.
    catalog_duration = mapping.catalog_duration
    if catalog_duration.minutes is None:
        return BLOCK_SERVICE_MAPPING_MISSING
    try:
        line_duration = read_duration_seconds(service.get("seance_length"))
    except DurationError:
        return BLOCK_CUSTOM_DURATION
    if line_duration.present and line_duration.minutes != catalog_duration.minutes:
        return BLOCK_CUSTOM_DURATION

    staff_uuid = branch.staff_uuid(staff_id)
    if staff_uuid is None:
        return BLOCK_STAFF_MAPPING_MISSING

    try:
        price_minor = to_minor_units(catalog_price, currency=mapping.catalog_currency or "")
    except AmountError:
        # A currency this migration cannot express exactly. Comparing a readback
        # against it would compare against a rounded number.
        return BLOCK_PRICE_MALFORMED

    assert mapping.catalog_service_name is not None
    return ServiceBinding(
        altegio_service_id=service_id,
        easyweek_service_uuid=mapping.easyweek_service_uuid,
        normalized_name=mapping.catalog_service_name,
        currency=(mapping.catalog_currency or "").strip().upper(),
        catalog_price_minor=price_minor,
        catalog_duration_minutes=catalog_duration.minutes,
        staffer_uuid=staff_uuid,
        source_amount=amount,
    )


def source_fingerprint(
    *,
    company_id: int,
    record_id: int,
    starts_at_utc: datetime,
    staff_uuid: str,
    customer_uuid: str,
    mutation_kind: str,
    bindings: tuple[ServiceBinding, ...],
    booked_duration_minutes: int,
) -> str:
    """Digest of the schedule identity this row was migrated as.

    Compared on a later run to answer "is the Altegio side still what we created
    in EasyWeek?". Nothing reversible to a person goes in — the customer appears
    as their EasyWeek UUID, which is an identifier we already store, not contact
    data.

    Everything that decides WHAT would be written is folded in: the mutation
    kind, every binding in the source's own order, and the booking's actual
    length. Two services swapping places is a different request body, and a
    catalogue price moving under an unchanged uuid is a different booking — so
    both have to produce a different fingerprint, or a plan reviewed against the
    old values would still look current.
    """
    parts = [
        mutation_kind,
        str(company_id),
        str(record_id),
        starts_at_utc.isoformat(),
        staff_uuid,
        str(booked_duration_minutes),
        customer_uuid,
    ]
    for binding in bindings:
        parts.extend(binding.digest_material())
    return hashlib.sha256("|".join(parts).encode("utf-8")).hexdigest()


def legacy_source_fingerprint(
    *,
    company_id: int,
    record_id: int,
    starts_at_utc: datetime,
    staff_uuid: str,
    service_uuid: str,
    duration_minutes: int,
    customer_uuid: str,
) -> str:
    """The fingerprint format every ledger row written before bindings carries.

    This is not a re-derivation or an approximation: it is the exact algorithm
    that ran in production from the first migration wave until `ServiceBinding`
    replaced the loose scalars, copied field for field and in the same order.
    It is frozen. Changing it would not "improve" anything — it would silently
    stop recognising rows that already exist in PostgreSQL.

    It knows only one service, because the contract it was written for could
    only ever migrate one, and it carries no quantity, because the code that
    produced it never read `amount`. Both facts constrain when it may be
    trusted; see `_legacy_candidate`.
    """
    blob = "|".join(
        [
            str(company_id),
            str(record_id),
            starts_at_utc.isoformat(),
            staff_uuid,
            service_uuid,
            str(duration_minutes),
            customer_uuid,
        ]
    )
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _legacy_candidate(
    *,
    mutation_kind: str,
    bindings: tuple[ServiceBinding, ...],
    company_id: int,
    record_id: int,
    starts_at_utc: datetime | None,
    staff_uuid: str | None,
    customer_uuid: str | None,
    booked_duration_minutes: int | None,
) -> str | None:
    """The legacy fingerprint THIS source booking would have produced, or None.

    `None` means "no legacy value can honestly be computed here", and the caller
    must then treat a mismatch as a mismatch. The conditions are the exact
    boundary of what the legacy format ever described:

    * `single` only — the legacy contract had no cart, so a cart row's stored
      hash can never legitimately be a legacy hash of anything;
    * exactly one binding, for the same reason;
    * `amount` exactly `1`, as an `int` and never a `bool`. The old code never
      read the quantity, so a booking that is TWO units today would recompute to
      the same legacy hash as the one unit that was migrated. Requiring the
      quantity to be one now is what stops `amount=2` from hiding behind the old
      format; and
    * every field the legacy blob needs actually present.

    Everything else about the booking has already been proven by the modern
    classifier before this is reached — this only decides whether the stored
    hash may be COMPARED against the old shape.
    """
    if mutation_kind != MUTATION_SINGLE or len(bindings) != 1:
        return None
    binding = bindings[0]
    if type(binding.source_amount) is not int or binding.source_amount != PROVEN_SERVICE_AMOUNT:
        return None
    if starts_at_utc is None or staff_uuid is None or customer_uuid is None or booked_duration_minutes is None:
        return None
    return legacy_source_fingerprint(
        company_id=company_id,
        record_id=record_id,
        starts_at_utc=starts_at_utc,
        staff_uuid=staff_uuid,
        service_uuid=binding.easyweek_service_uuid,
        duration_minutes=booked_duration_minutes,
        customer_uuid=customer_uuid,
    )


def _matches(stored: str | None, *, current: str, legacy: str | None) -> bool:
    """The ONE place a stored fingerprint is compared against anything.

    Two accepted values and no third: the fingerprint this build computes, and —
    only when `_legacy_candidate` was able to compute one — the exact hash the
    pre-binding production code would have written for the very same booking.

    Centralised on purpose. The comparison used to be a bare `!=` in four
    modules; adding a second accepted format there would have meant four chances
    to write `or` slightly too generously, in code paths that cancel real
    appointments. Everything fails closed here: an empty stored value never
    matches, and a legacy value that could not be computed is not a licence.
    """
    if not stored:
        return False
    if hmac.compare_digest(stored, current):
        return True
    if legacy is None:
        return False
    return hmac.compare_digest(stored, legacy)


def fingerprint_matches_decision(stored: str | None, decision: Decision) -> bool:
    """Does a stored ledger fingerprint describe THIS freshly classified booking?

    The decision must already be a modern, fully proven one — this answers only
    the format question, never the "is the source still valid" question, which
    every caller has asked before it gets here.
    """
    if decision.source_fingerprint is None or decision.source_record_id is None:
        return False
    return _matches(
        stored,
        current=decision.source_fingerprint,
        legacy=_legacy_candidate(
            mutation_kind=decision.mutation_kind,
            bindings=decision.bindings,
            company_id=decision.source_company_id,
            record_id=decision.source_record_id,
            starts_at_utc=decision.starts_at_utc,
            staff_uuid=decision.easyweek_staff_uuid,
            customer_uuid=decision.easyweek_customer_uuid,
            booked_duration_minutes=decision.duration_minutes,
        ),
    )


@dataclass(frozen=True)
class LedgerView:
    """What the ledger already knows about one source booking."""

    status: str
    target_booking_uuid: str | None
    source_fingerprint: str


@dataclass(frozen=True)
class SourceLiveness:
    """Is this Altegio record still a live booking in the future?

    Extracted so that there is exactly **one** implementation of "cancelled,
    deleted, finished, in the past". Two callers ask it, and they ask for
    different reasons:

    * :func:`classify_record`, deciding whether a booking migrates at all; and
    * the lifecycle re-proof, deciding what became of a booking that was already
      migrated — including one whose branch the current manifest no longer
      names, where the classifier's own first question ("is this company in the
      manifest?") would otherwise hide the answer.

    A second copy of these rules in the second caller would be a second way of
    being wrong about whether a customer still has an appointment.

    ``outcome`` is ``None`` exactly when the booking is live and starts at or
    after the cutover; then ``starts_at_utc`` carries the parsed instant.
    ``BLOCKED`` means the record could not be read well enough to say — an
    unrecognised status, an unparseable time — and is never "still alive".
    """

    outcome: str | None = None
    reason: str | None = None
    starts_at_utc: datetime | None = None

    @property
    def alive(self) -> bool:
        return self.outcome is None


def classify_source_liveness(record: dict[str, Any], *, cutover: Cutover) -> SourceLiveness:
    """Whether one source record is still a future, uncancelled, unfinished booking.

    Pure; performs no I/O and consults neither the manifest nor the wave
    selector. Deliberately independent of both: a master moved to a later wave,
    or a branch left out of this wave's manifest, has not thereby cancelled
    anybody's appointment.
    """
    if bool(record.get("deleted")):
        return SourceLiveness(outcome=SKIPPED, reason=SKIP_DELETED)

    confirmed = record.get("confirmed")
    if confirmed is not None:
        confirmed_int = _exact_int(confirmed)
        if confirmed_int is None:
            return SourceLiveness(outcome=BLOCKED, reason=BLOCK_STATUS_UNRECOGNISED)
        if confirmed_int == 0:
            return SourceLiveness(outcome=SKIPPED, reason=SKIP_CANCELED)

    # ``attendance`` is absent on a plain future booking and present once the
    # visit resolves. Present-and-terminal is skipped; present-and-unrecognised
    # is BLOCKED rather than assumed live — an unknown status is exactly the case
    # where guessing creates a booking for a visit that already happened.
    attendance = record.get("attendance")
    if attendance is not None:
        attendance_int = _exact_int(attendance)
        if attendance_int is None:
            return SourceLiveness(outcome=BLOCKED, reason=BLOCK_STATUS_UNRECOGNISED)
        if attendance_int not in ACTIVE_ATTENDANCE:
            return SourceLiveness(outcome=SKIPPED, reason=SKIP_COMPLETED)

    visit_attendance = record.get("visit_attendance")
    if visit_attendance is not None:
        visit_int = _exact_int(visit_attendance)
        if visit_int is None:
            return SourceLiveness(outcome=BLOCKED, reason=BLOCK_STATUS_UNRECOGNISED)
        if visit_int not in ACTIVE_ATTENDANCE:
            return SourceLiveness(outcome=SKIPPED, reason=SKIP_COMPLETED)

    raw_start = record.get("date") if record.get("date") else record.get("datetime")
    try:
        starts_at = parse_altegio_local_to_utc(raw_start)
    except LocalTimeError as exc:
        return SourceLiveness(outcome=BLOCKED, reason=exc.reason)

    if starts_at < cutover.at:
        return SourceLiveness(outcome=SKIPPED, reason=SKIP_PAST)

    return SourceLiveness(starts_at_utc=starts_at)


def classify_record(
    record: dict[str, Any],
    *,
    company_id: int,
    manifest: MigrationManifest,
    directory: CustomerDirectory,
    cutover: Cutover,
    ledger: LedgerView | None,
    ignore_wave_scope: bool = False,
) -> Decision:
    """Decide what happens to one Altegio record. Pure; performs no I/O.

    The order of the checks is part of the contract, and it runs cheapest-and-
    most-exclusionary first: a booking that is not ours, or is in the past, is
    ``skipped`` before we ever ask whether its master is mapped. That keeps the
    blocked list free of rows nobody was ever going to migrate.
    """
    branch: BranchMapping | None = manifest.branch(company_id)
    if branch is None:
        # Not in the manifest, so not part of this cutover. Durlach reaches this
        # branch only in the sense that it can never reach it at all: it has no
        # Altegio company_id to be fetched under.
        return Decision(
            outcome=SKIPPED, reason=SKIP_FOREIGN_COMPANY, source_company_id=company_id, source_record_id=None
        )

    record_id = _record_id(record)
    if record_id is None:
        return Decision(outcome=BLOCKED, reason=BLOCK_NO_RECORD_ID, source_company_id=company_id, source_record_id=None)

    def _skip(reason: str) -> Decision:
        return Decision(outcome=SKIPPED, reason=reason, source_company_id=company_id, source_record_id=record_id)

    def _block(reason: str) -> Decision:
        return Decision(outcome=BLOCKED, reason=reason, source_company_id=company_id, source_record_id=record_id)

    # -- 1-2. is the source booking still alive, and still in the future? ----
    # The rules themselves live in `classify_source_liveness`, because the
    # lifecycle re-proof needs the same answer to a different question.
    liveness = classify_source_liveness(record, cutover=cutover)
    if not liveness.alive:
        assert liveness.reason is not None
        return _block(liveness.reason) if liveness.outcome == BLOCKED else _skip(liveness.reason)
    assert liveness.starts_at_utc is not None
    starts_at = liveness.starts_at_utc

    # -- 2a. is this master part of THIS wave? -----------------------------
    # Asked before anything about the service, the price or the mapping, because
    # a deferred master's booking is not a problem to be reported — it is simply
    # somebody else's wave, and it should not appear in the blocked list an
    # operator has to work through.
    staff_id = _staff_id(record)
    scope = branch.staff_scope(staff_id)
    # `ignore_wave_scope` is for the ONE caller that is asking a different
    # question: not "does this booking migrate in this wave?" but "is this
    # booking still alive and still the booking we migrated?". The wave selector
    # answers the first and says nothing about the second — a master moved to a
    # later wave has not thereby cancelled her customers' appointments. See
    # `reclassify_source_lifecycle`.
    #
    # It is never set by planning, apply, canary or the pre-POST re-proof: for
    # them a deferred master IS out of scope, and an unknown one still blocks.
    if not ignore_wave_scope:
        if scope == STAFF_DEFERRED:
            return _skip(SKIP_STAFF_DEFERRED)
        if scope == STAFF_UNKNOWN:
            # A master nobody classified. Fail closed: an unlisted master is the
            # one case where "not migrating" and "we missed her" look identical,
            # and only a human can tell them apart.
            return _block(BLOCK_STAFF_NOT_IN_WAVE)

    # -- 3. exactly one service, no overrides ------------------------------
    services = _services(record)
    if services is None:
        # Missing, null, not a list, or an entry that is not an object. A data
        # error, and never read as a break: see `SKIP_EMPTY_SERVICES`.
        return _block(BLOCK_NO_SERVICES)
    if not services:
        # Reached only after liveness, the cutover window and the wave selector
        # have all been decided, so an unknown master's break still blocks and a
        # deferred master's break is still somebody else's wave. The exclusion
        # cannot be used to launder a row past those checks.
        return _skip(SKIP_EMPTY_SERVICES)
    if len(services) > MAX_CART_SERVICES:
        # Three or more. The cart canary proved a two-service booking and
        # nothing wider, so anything larger is still refused outright rather
        # than flattened to "the first service".
        return _block(BLOCK_MULTI_SERVICE)

    # -- 3a. every service, proven against its own reviewed baseline --------
    # One binding per service, in the SOURCE's order. That order is canonical
    # from here on: it is what the request body sends, what the fingerprint
    # covers and what a readback compares against.
    kind = MUTATION_SINGLE if len(services) == 1 else MUTATION_CART_TWO
    bindings: list[ServiceBinding] = []
    for service in services:
        proven = _prove_service(service, branch=branch, staff_id=staff_id)
        if isinstance(proven, str):
            return _block(proven)
        bindings.append(proven)

    # -- 4. duration: the BOOKING's length against the catalogue total ------
    # The booking's own length, and the catalogue length it must equal. The
    # manifest always supplies the second one; Altegio does not always repeat it
    # on the booking row, and treating that silence as "no override" is how a
    # hand-stretched slot used to pass.
    #
    # For a cart booking the comparison is against the SUM: the canary's 180
    # minutes were two standard services back to back, and a source slot that
    # does not add up to them is a hand-adjusted booking, not a cart.
    try:
        booking_duration = read_duration_seconds(record.get("seance_length"))
    except DurationError:
        # Zero, negative, fractional-second, non-finite or malformed.
        return _block(BLOCK_CUSTOM_DURATION)

    if not booking_duration.present:
        return _block(BLOCK_DURATION_UNKNOWN)
    assert booking_duration.minutes is not None
    duration_minutes = booking_duration.minutes

    if duration_minutes != total_duration_minutes(tuple(bindings)):
        # A slot that does not match its services' catalogue length. EasyWeek's
        # custom-duration representation is not proven, so it is not guessed.
        return _block(BLOCK_CUSTOM_DURATION)

    try:
        validate_bindings(kind, tuple(bindings))
    except BindingError:
        # Two lines on one catalogue entry, two different masters, or two
        # currencies. Each is a shape the canary did not prove.
        return _block(BLOCK_CART_UNSUPPORTED)

    # -- 5. staff mapping: exact, or blocked -------------------------------
    # A selected master is guaranteed a mapping by the manifest parser; this stays
    # fail-closed anyway, because the classifier must not depend on a validation
    # that lives in another module.
    staff_uuid = branch.staff_uuid(staff_id)
    if staff_uuid is None:
        return _block(BLOCK_STAFF_MAPPING_MISSING)

    # -- 6. customer: exactly one --------------------------------------
    client = record.get("client")
    raw_phone = client.get("phone") if isinstance(client, dict) else None
    match = directory.resolve(raw_phone)
    if not match.resolved:
        assert match.reason is not None
        return _block(match.reason)
    customer_uuid = match.uuid
    assert customer_uuid is not None

    fingerprint = source_fingerprint(
        company_id=company_id,
        record_id=record_id,
        starts_at_utc=starts_at,
        staff_uuid=staff_uuid,
        customer_uuid=customer_uuid,
        mutation_kind=kind,
        bindings=tuple(bindings),
        booked_duration_minutes=duration_minutes,
    )

    # -- 7. has this already been migrated? --------------------------------
    if ledger is not None:
        if ledger.status in LEDGER_UNRESOLVED_STATUSES:
            # A mutation whose outcome we never learned. `uncertain` says so
            # explicitly; `pending` says it by omission — some process claimed
            # this booking and never came back, and it may well have sent the
            # POST before it died. Retrying either blind is exactly the
            # double-booking this whole design exists to prevent; both need
            # `reconcile`, not another attempt.
            return Decision(
                outcome=BLOCKED,
                reason=BLOCK_LEDGER_UNCERTAIN,
                source_company_id=company_id,
                source_record_id=record_id,
                target_booking_uuid=ledger.target_booking_uuid,
            )
        if ledger.status == "created":
            # Both formats, one comparison. A row migrated before the binding
            # model exists in PostgreSQL with the old hash and cannot be
            # recomputed into the new one without re-proving the source and the
            # target — so it is recognised, not rewritten. `_matches` decides,
            # and it only offers the legacy value for a booking that is still a
            # single service of exactly one unit.
            stored_matches = _matches(
                ledger.source_fingerprint,
                current=fingerprint,
                legacy=_legacy_candidate(
                    mutation_kind=kind,
                    bindings=tuple(bindings),
                    company_id=company_id,
                    record_id=record_id,
                    starts_at_utc=starts_at,
                    staff_uuid=staff_uuid,
                    customer_uuid=customer_uuid,
                    booked_duration_minutes=duration_minutes,
                ),
            )
            if not stored_matches:
                # It was migrated, and then Altegio changed underneath. Creating
                # a second booking would double-book; silently accepting the old
                # one would leave the customer with a stale appointment. Human.
                return Decision(
                    outcome=BLOCKED,
                    reason=BLOCK_SOURCE_CHANGED,
                    source_company_id=company_id,
                    source_record_id=record_id,
                    target_booking_uuid=ledger.target_booking_uuid,
                )
            # Carries the full resolution, not just the target uuid. An
            # already-migrated row is exactly the row a reconciliation has to
            # PROVE, and proving it needs the master, the service and the
            # customer this booking should have — the master especially, since
            # the EasyWeek booking payload names none and the expected one has
            # to come from somewhere before it can be checked against the
            # filtered list.
            return Decision(
                outcome=ALREADY_MIGRATED,
                reason=None,
                source_company_id=company_id,
                source_record_id=record_id,
                starts_at_utc=starts_at,
                easyweek_location_uuid=branch.easyweek_location_uuid,
                easyweek_staff_uuid=staff_uuid,
                easyweek_customer_uuid=customer_uuid,
                mutation_kind=kind,
                bindings=tuple(bindings),
                target_booking_uuid=ledger.target_booking_uuid,
                source_fingerprint=fingerprint,
            )
        # `blocked` / `failed` ledger rows carry no target, so the row is simply
        # re-evaluated from scratch and may become ready once the cause is fixed.

    # -- 8. can this build actually WRITE this contract? -------------------
    # Last, and deliberately so. Everything above has been proven: the shape,
    # the mapping, the prices, the durations, the customer. What is missing is
    # nothing about this booking — it is a complete write path for its contract.
    #
    # Refused HERE rather than at apply time so a dry-run and an apply say the
    # same thing about the same booking. A row that reviewed as `ready` and then
    # blocked mid-write would be a surprise at the worst possible moment, and
    # the operator would have approved a plan the tool never meant to execute.
    #
    # The decision still carries its full shape — kind, bindings, fingerprint —
    # so the candidate remains visible in the report. Visible is not migratable:
    # the outcome is `blocked`, so no gate, no ledger claim and no POST can ever
    # reach it while the write path is missing.
    contract_ready = kind in SUPPORTED_MUTATION_KINDS

    return Decision(
        outcome=READY if contract_ready else BLOCKED,
        reason=None if contract_ready else BLOCK_CONTRACT_UNSUPPORTED,
        source_company_id=company_id,
        source_record_id=record_id,
        starts_at_utc=starts_at,
        easyweek_location_uuid=branch.easyweek_location_uuid,
        easyweek_staff_uuid=staff_uuid,
        easyweek_customer_uuid=customer_uuid,
        mutation_kind=kind,
        bindings=tuple(bindings),
        source_fingerprint=fingerprint,
    )
