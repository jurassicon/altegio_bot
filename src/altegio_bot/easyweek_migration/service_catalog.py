"""Proving which catalogue service a booking carries, when the API will not say.

The problem this exists for
---------------------------
``POST /bookings`` takes a catalogue ``service_uuid``. ``GET /bookings/{uuid}``
does **not** give it back. What comes back is ``ordered_services[]``, whose
``uuid`` is the order line's own identifier: the published
``replace-booking-service`` example sends one UUID and returns a different one,
and an operator GET against a live booking confirmed it — ``/services/{that
uuid}`` answered 404. There is no documented endpoint, field or expansion that
links a booking back to its catalogue service.

So the field we would normally compare simply is not there, and the migration
cannot prove by identifier that the booking it created carries the service it
asked for.

What the owner authorised, and what it is not
---------------------------------------------
Plan revision 22 (§28) authorises one narrow substitute: prove the service by
its **exact attributes**, on the condition that those attributes identify it
uniquely in the location's full catalogue. The method is named
:data:`SERVICE_PROOF_METHOD` and travels in the report so nothing downstream can
mistake it for a vendor UUID link.

It is weaker than a UUID link, in ways worth stating plainly:

* uniqueness is proven only among the services the catalogue endpoint returns at
  the moment it is read — a hidden, historical or deleted service is outside it;
* two reads are two observations, not a lock on the catalogue;
* it proves *an* identical-looking service, and only the catalogue's own
  uniqueness makes "identical-looking" mean "the same one".

Hence the rules below are strict rather than best-effort. Every page of the
catalogue is read; the expectation is pinned before the write and re-proved
before each later check; exactly one candidate must carry the expected UUID; and
anything else — an unreadable page, an ambiguous pair, a changed price, a
currency we cannot express in minor units — fails closed.

If EasyWeek later exposes a real link, that link wins: a direct UUID that
disagrees is a mismatch, and must never be rescued by an attribute match.

Nothing here writes. It reads the catalogue and compares numbers.
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Any, Final, Protocol

from altegio_bot.easyweek_client import EasyWeekError
from altegio_bot.easyweek_migration.manifest import (
    ServiceMapping,
    canonical_service_name,
    canonical_uuid,
)
from altegio_bot.easyweek_migration.money import (
    AmountError,
    to_minor_units,
)

# The method's name and version. Travels into the report and into
# `REQUEST_SCHEMA_VERSION`, so a proof produced under one method can never
# license a run using another.
SERVICE_PROOF_METHOD: Final = "catalog_attributes_unique_v1"
# The same fact, short enough to live inside the canary proof's
# `request_schema_version` column (varchar(16)) alongside the request version.
# Kept next to the full name so the two are changed together: the long form is
# what a report says, the short form is what a stored proof is bound to, and a
# new method must change BOTH or an old proof would license it.
SERVICE_PROOF_TAG: Final = "cat-attr-v1"
# The version of the STORED expectation. A baseline written under another version
# is not read as evidence for this one — it is refused, so that changing what a
# baseline means can never be mistaken for the old baseline still holding.
SERVICE_BASELINE_VERSION: Final = "v1"

# Stable, PII-free refusals. A service name is not personal data, but it is free
# text an operator typed, so it never appears in these codes either.
CATALOG_UNREADABLE: Final = "service_catalog_unreadable"
CATALOG_MALFORMED: Final = "service_catalog_malformed"
CATALOG_PAGINATION_INCOMPLETE: Final = "service_catalog_pagination_incomplete"
CATALOG_CHANGED_DURING_READ: Final = "service_catalog_changed_during_read"
SERVICE_NOT_IN_CATALOG: Final = "service_not_in_catalog"
SERVICE_ATTRIBUTES_AMBIGUOUS: Final = "service_attributes_ambiguous"
SERVICE_ATTRIBUTES_CHANGED: Final = "service_attributes_changed"
SERVICE_FORMAT_UNSUPPORTED: Final = "service_format_unsupported"
# No stored expectation for this service at all, or one written under a method or
# version this build cannot read. Never recovered from the current catalogue:
# rebuilding a lost baseline out of whatever the catalogue says today is exactly
# the circular check this module exists to stop.
SERVICE_BASELINE_MISSING: Final = "service_baseline_missing"
SERVICE_BASELINE_VERSION_UNSUPPORTED: Final = "service_baseline_version_unsupported"
# The manifest does not state this service's reviewed name or currency, so there
# is no approved expectation to prove anything against. Never filled in from the
# live catalogue: that is what made the check circular in the first place.
SERVICE_EXPECTATION_INCOMPLETE: Final = "service_expectation_incomplete"
# The stored baseline and the reviewed manifest disagree. Neither wins
# automatically — an operator decides which is right.
SERVICE_BASELINE_CONFLICTS_WITH_PLAN: Final = "service_baseline_conflicts_with_plan"
# Readback-side codes.
ORDERED_SERVICE_MISSING: Final = "ordered_service_missing"
ORDERED_SERVICE_NOT_SINGLE: Final = "ordered_service_not_single"
ORDERED_SERVICE_QUANTITY_UNSUPPORTED: Final = "ordered_service_quantity_unsupported"
ORDERED_SERVICE_MISMATCH: Final = "ordered_service_mismatch"
ORDERED_SERVICE_UUID_CONFLICT: Final = "ordered_service_uuid_conflict"

# A booking the migration writes always covers exactly one service, once.
SUPPORTED_QUANTITY: Final = 1

_MAX_CATALOG_PAGES: Final = 50


class ServiceEvidenceError(ValueError):
    """The service could not be pinned or proven. Carries a stable reason."""

    def __init__(self, reason: str, detail: str | None = None) -> None:
        self.reason = reason
        self.detail = detail
        super().__init__(reason if detail is None else f"{reason}:{detail}")


def normalize_service_name(raw: object) -> str | None:
    """Canonical form of a catalogue service name, or ``None`` if unusable.

    Unicode NFC, collapsed internal whitespace, case-folded. German catalogues
    are full of umlauts that two systems can encode differently
    (``Wimpernverlängerung`` composed vs decomposed), and a comparison that broke
    on the encoding would be a comparison that fails for the wrong reason.

    Case-folding is deliberate and it *widens* what counts as the same name,
    which is safe here only because uniqueness is verified under exactly this
    same normalisation — two names that fold together are reported as ambiguous
    rather than silently treated as one.

    One implementation, shared with the manifest parser, so the reviewed file and
    the live catalogue cannot disagree about what the same name is.
    """
    return canonical_service_name(raw)


@dataclass(frozen=True)
class CatalogService:
    """One catalogue entry, reduced to what identity is argued from."""

    uuid: str
    normalized_name: str
    currency: str
    price_minor: int
    duration_minutes: int
    # The name exactly as the catalogue wrote it. Never compared and never part
    # of `attributes` or the snapshot digest — identity is judged on the
    # normalised form, and always has been. It exists so an operator reviewing a
    # mapping sees the catalogue's own capitalisation instead of a case-folded
    # rendering of it, which is harder to recognise and easy to mistake for a
    # different entry.
    display_name: str = ""

    @property
    def attributes(self) -> tuple[str, str, int, int]:
        """The tuple uniqueness is judged on."""
        return (self.normalized_name, self.currency, self.price_minor, self.duration_minutes)


@dataclass(frozen=True)
class CatalogSnapshot:
    """Every service the location endpoint returned, in one read."""

    location_uuid: str
    services: tuple[CatalogService, ...]

    @property
    def digest(self) -> str:
        """Stable digest of the whole snapshot — a cheap catalogue-drift detector."""
        blob = "|".join(
            f"{service.uuid}:{service.normalized_name}:{service.currency}:"
            f"{service.price_minor}:{service.duration_minutes}"
            for service in sorted(self.services, key=lambda item: item.uuid)
        )
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def by_uuid(self, service_uuid: str) -> CatalogService | None:
        for service in self.services:
            if service.uuid == service_uuid:
                return service
        return None

    def matching(self, attributes: tuple[str, str, int, int]) -> list[CatalogService]:
        return [service for service in self.services if service.attributes == attributes]

    def as_safe_dict(self) -> dict[str, Any]:
        return {"location_uuid": self.location_uuid, "services": len(self.services), "digest": self.digest}


@dataclass(frozen=True)
class ServiceBaseline:
    """The service a wave was reviewed against. Immutable once written.

    The distinction between this and a :class:`CatalogSnapshot` is the whole
    correction. A snapshot is an **observation** — what the catalogue said at one
    moment. A baseline is an **expectation** — what an operator reviewed and what
    every later run must still find. Collapsing the two is what made the first
    version circular: each run re-derived its expectation from the current
    catalogue, so a renamed service produced a new expectation that the new
    catalogue satisfied by construction, and the check proved nothing.

    So this is established once, before the first booking for the service, and
    afterwards only ever verified. Never recomputed, never silently widened, and
    never rebuilt from the catalogue when the stored row is missing.
    """

    easyweek_location_uuid: str
    easyweek_service_uuid: str
    normalized_name: str
    currency: str
    price_minor: int
    duration_minutes: int
    method: str = SERVICE_PROOF_METHOD
    version: str = SERVICE_BASELINE_VERSION

    @property
    def attributes(self) -> tuple[str, str, int, int]:
        return (self.normalized_name, self.currency, self.price_minor, self.duration_minutes)

    @property
    def digest(self) -> str:
        """Stable identity of this expectation, for reports and comparisons."""
        blob = "|".join(
            [
                self.version,
                self.method,
                self.easyweek_location_uuid,
                self.easyweek_service_uuid,
                self.normalized_name,
                self.currency,
                str(self.price_minor),
                str(self.duration_minutes),
            ]
        )
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def as_safe_dict(self) -> dict[str, Any]:
        """Identifiers, numbers and the method. The name is a digest, not text."""
        return {
            "method": self.method,
            "version": self.version,
            "easyweek_location_uuid": self.easyweek_location_uuid,
            "easyweek_service_uuid": self.easyweek_service_uuid,
            "service_name_digest": hashlib.sha256(self.normalized_name.encode("utf-8")).hexdigest()[:16],
            "currency": self.currency,
            "price_minor_units": self.price_minor,
            "duration_minutes": self.duration_minutes,
            "baseline_digest": self.digest[:16],
            "limitations": [
                "uniqueness holds only among services the catalogue endpoint returned at read time",
                "hidden, historical or deleted services are not covered",
                "reading the catalogue is an observation, not an atomic lock",
                "this is not a vendor-provided booking-to-catalogue UUID link",
            ],
        }


def read_catalog_service(row: object) -> CatalogService:
    """Project one catalogue row, or raise. Every field is required."""
    if not isinstance(row, dict):
        raise ServiceEvidenceError(CATALOG_MALFORMED, "row")

    service_uuid = canonical_uuid(row.get("uuid"))
    if service_uuid is None:
        raise ServiceEvidenceError(CATALOG_MALFORMED, "uuid")

    name = normalize_service_name(row.get("name"))
    if name is None:
        raise ServiceEvidenceError(CATALOG_MALFORMED, "name")

    currency = row.get("currency")
    if not isinstance(currency, str) or not currency.strip():
        raise ServiceEvidenceError(CATALOG_MALFORMED, "currency")
    currency = currency.strip().upper()

    # EasyWeek states catalogue prices as an integer of minor units. Anything
    # else — a float, a string, a null — is a shape we have not proven, and
    # guessing its unit is exactly what §28.2 forbids.
    price = row.get("price")
    if type(price) is not int or price < 0:
        raise ServiceEvidenceError(SERVICE_FORMAT_UNSUPPORTED, "price")

    duration = row.get("duration")
    if not isinstance(duration, dict):
        raise ServiceEvidenceError(CATALOG_MALFORMED, "duration")
    minutes = _duration_minutes(duration)

    raw_name = row.get("name")
    return CatalogService(
        uuid=service_uuid,
        normalized_name=name,
        currency=currency,
        price_minor=price,
        duration_minutes=minutes,
        display_name=" ".join(raw_name.split()) if isinstance(raw_name, str) else "",
    )


def _duration_minutes(duration: dict[str, Any]) -> int:
    """Whole minutes from EasyWeek's duration object, or raise.

    The unit is read from the payload's own ``label`` rather than assumed: a
    ``value`` of 90 means something very different in minutes and in seconds, and
    the field that says which is right there in the response.
    """
    value = duration.get("value")
    label = duration.get("label")
    if type(value) is not int or value <= 0:
        raise ServiceEvidenceError(SERVICE_FORMAT_UNSUPPORTED, "duration_value")
    if not isinstance(label, str) or label.strip().lower() != "minutes":
        raise ServiceEvidenceError(SERVICE_FORMAT_UNSUPPORTED, "duration_label")
    return value


def build_catalog_snapshot(location_uuid: str, rows: list[Any]) -> CatalogSnapshot:
    """Project a full page set into a snapshot, refusing repeated UUIDs."""
    services: list[CatalogService] = []
    seen: set[str] = set()
    for row in rows:
        service = read_catalog_service(row)
        if service.uuid in seen:
            raise ServiceEvidenceError(CATALOG_MALFORMED, "duplicate_uuid")
        seen.add(service.uuid)
        services.append(service)
    return CatalogSnapshot(location_uuid=location_uuid, services=tuple(services))


class SupportsCatalogReads(Protocol):
    """The one method a catalogue read needs. Kept structural so this module
    stays free of the write client (and of the import cycle that would create)."""

    async def list_location_services(self, location_uuid: str, *, page: int) -> dict[str, Any]: ...


async def read_full_catalog(client: SupportsCatalogReads, *, location_uuid: str) -> CatalogSnapshot:
    """Every page of one location's catalogue, read now, or raise."""
    snapshot, _rows = await read_full_catalog_rows(client, location_uuid=location_uuid)
    return snapshot


async def read_full_catalog_rows(
    client: SupportsCatalogReads, *, location_uuid: str
) -> tuple[CatalogSnapshot, list[Any]]:
    """The snapshot AND the raw rows it was projected from, in one walk.

    The single implementation of the page walk: the cutover's service proof and
    the preparation stage's mapping proposals must never disagree about what
    "the catalogue" is, or a mapping could be proposed against a catalogue the
    apply then reads differently.

    The raw rows come back because a caller may need a field the projection
    deliberately drops — the preparation stage reads whatever staff list a row
    happens to carry. Walking the pages a second time for it would both double
    the rate budget and let the two reads disagree about the same catalogue.

    Never filtered — not by category, not by the wave's services, not by master.
    Uniqueness is judged over the whole returned catalogue, and a filter would
    hide exactly the look-alike that makes an attribute match ambiguous.
    """
    rows: list[Any] = []
    page = 1
    while True:
        try:
            payload = await client.list_location_services(location_uuid, page=page)
        except EasyWeekError:
            # Timeout, 5xx, auth, protocol — every one of them is one catalogue
            # we did not read, and none of them is a catalogue we may reason on.
            raise ServiceEvidenceError(CATALOG_UNREADABLE) from None
        data = payload.get("data")
        if not isinstance(data, list):
            raise ServiceEvidenceError(CATALOG_UNREADABLE)
        last_page, total = read_pagination(payload.get("meta"), page=page)
        rows.extend(data)
        if page >= last_page:
            if len(rows) != total:
                # A page set that does not add up to the stated total is a
                # partial catalogue, and a partial catalogue cannot prove that
                # anything in it is unique.
                raise ServiceEvidenceError(CATALOG_UNREADABLE)
            break
        page += 1

    return build_catalog_snapshot(location_uuid, rows), rows


def _unique_entry(catalog: CatalogSnapshot, easyweek_service_uuid: str) -> CatalogService:
    """The catalogue entry for this uuid, proven to be identifiable by attributes.

    Shared by establishing a baseline and by verifying one, so the two can never
    disagree about what "unique" means. Uniqueness is judged across the WHOLE
    returned catalogue — never narrowed to the wave's services, a category or a
    master, because a look-alike outside that narrowing is exactly the one that
    would make the attributes ambiguous.
    """
    service = catalog.by_uuid(easyweek_service_uuid)
    if service is None:
        # Includes the re-created-service case: a new UUID is never adopted by
        # name, however well the name matches.
        raise ServiceEvidenceError(SERVICE_NOT_IN_CATALOG)

    candidates = catalog.matching(service.attributes)
    if len(candidates) != 1 or candidates[0].uuid != service.uuid:
        # Either two services look identical, or the one that does is not ours.
        # Both mean the attributes cannot stand in for the identifier.
        raise ServiceEvidenceError(SERVICE_ATTRIBUTES_AMBIGUOUS)
    return service


def expectation_from_manifest(
    location_uuid: str,
    *,
    easyweek_service_uuid: str,
    mapping: ServiceMapping,
) -> ServiceBaseline:
    """The reviewed expectation for one service, built from the manifest alone.

    Every value comes from the file an operator checked and the plan digest
    covers. Nothing is read from the live catalogue here, and that is the whole
    correction: the previous version took the name and the currency from
    whatever the catalogue said at write time, so a service renamed after the
    canary supplied its own new "expectation" and satisfied it by construction.

    Raises when the manifest does not state the identity — never fills it in.
    """
    if not mapping.identity_complete:
        raise ServiceEvidenceError(SERVICE_EXPECTATION_INCOMPLETE)
    assert mapping.catalog_service_name is not None
    assert mapping.catalog_currency is not None

    if mapping.catalog_duration.minutes is None:
        raise ServiceEvidenceError(SERVICE_FORMAT_UNSUPPORTED, "manifest_duration")
    try:
        price_minor = to_minor_units(mapping.catalog_price, currency=mapping.catalog_currency)
    except AmountError:
        raise ServiceEvidenceError(SERVICE_FORMAT_UNSUPPORTED, "manifest_price") from None

    return ServiceBaseline(
        easyweek_location_uuid=location_uuid,
        easyweek_service_uuid=easyweek_service_uuid,
        normalized_name=mapping.catalog_service_name,
        currency=mapping.catalog_currency,
        price_minor=price_minor,
        duration_minutes=mapping.catalog_duration.minutes,
    )


def establish_baseline(
    catalog: CatalogSnapshot,
    *,
    easyweek_service_uuid: str,
    mapping: ServiceMapping,
) -> ServiceBaseline:
    """The reviewed expectation, confirmed against the catalogue. Or raise.

    Two steps, and the order carries the meaning: the expectation is built from
    the manifest, then the live catalogue is asked whether it still holds. The
    catalogue is an observation that can agree or disagree — it never supplies a
    value. If it has moved since the operator reviewed the file, this refuses
    rather than writing down what the catalogue happens to say now.
    """
    expected = expectation_from_manifest(
        catalog.location_uuid, easyweek_service_uuid=easyweek_service_uuid, mapping=mapping
    )
    verify_baseline(catalog, expected)
    return expected


def verify_baseline(catalog: CatalogSnapshot, baseline: ServiceBaseline) -> None:
    """Raise unless a FRESH catalogue still satisfies a stored expectation.

    The only direction this function works in. It can agree with the baseline or
    fail closed; it can never update it. A renamed, repriced, re-timed, deleted,
    re-created or newly-ambiguous service all end here as a refusal, and fixing
    any of them is an explicit operator act with a new reviewed plan — not
    something a run does on its way to a POST.
    """
    if baseline.version != SERVICE_BASELINE_VERSION or baseline.method != SERVICE_PROOF_METHOD:
        # Written under a different contract. Adapting to it would mean guessing
        # what the old version meant, which is how a weaker check gets inherited.
        raise ServiceEvidenceError(SERVICE_BASELINE_VERSION_UNSUPPORTED)
    if baseline.easyweek_location_uuid != catalog.location_uuid:
        # A baseline for another branch proves nothing here, and a look-alike in
        # a foreign catalogue must never count towards this location's uniqueness.
        raise ServiceEvidenceError(SERVICE_BASELINE_MISSING, "location")

    service = _unique_entry(catalog, baseline.easyweek_service_uuid)
    if service.attributes != baseline.attributes:
        raise ServiceEvidenceError(SERVICE_ATTRIBUTES_CHANGED)


@dataclass(frozen=True)
class OrderedService:
    """The single ordered line a migrated booking must carry."""

    line_uuid: str | None
    direct_service_uuid: str | None
    normalized_name: str
    currency: str
    price_minor: int
    duration_minutes: int


def read_ordered_service(payload: dict[str, Any]) -> OrderedService:
    """Project ``ordered_services`` from a live booking, or raise.

    Exactly one line, quantity one. A booking that came back with two lines is
    not the booking this migration writes, so it is a refusal rather than
    something to take the first element of.
    """
    items = payload.get("ordered_services")
    if items is None or not isinstance(items, list):
        raise ServiceEvidenceError(ORDERED_SERVICE_MISSING)
    if len(items) != 1 or not isinstance(items[0], dict):
        raise ServiceEvidenceError(ORDERED_SERVICE_NOT_SINGLE)
    item = items[0]

    quantity = item.get("quantity")
    if quantity is not None and (type(quantity) is not int or quantity != SUPPORTED_QUANTITY):
        raise ServiceEvidenceError(ORDERED_SERVICE_QUANTITY_UNSUPPORTED)

    name = normalize_service_name(item.get("name"))
    if name is None:
        raise ServiceEvidenceError(ORDERED_SERVICE_MISMATCH, "name")

    # `currency` sits on the booking in the documented example and on the line in
    # others. Both are read; neither is invented.
    currency = item.get("currency") or payload.get("currency")
    if not isinstance(currency, str) or not currency.strip():
        raise ServiceEvidenceError(ORDERED_SERVICE_MISMATCH, "currency")

    # The ACTUAL charged price and length, never `original_*`: the originals are
    # the catalogue values echoed back, so comparing them would prove the
    # catalogue against itself and miss a per-booking override entirely.
    price = item.get("price")
    if type(price) is not int or price < 0:
        raise ServiceEvidenceError(SERVICE_FORMAT_UNSUPPORTED, "ordered_price")

    duration = item.get("duration")
    if not isinstance(duration, dict):
        raise ServiceEvidenceError(SERVICE_FORMAT_UNSUPPORTED, "ordered_duration")

    return OrderedService(
        line_uuid=canonical_uuid(item.get("uuid")),
        # If EasyWeek ever starts returning a real catalogue link, it is read and
        # it wins. Until then this is `None` for every real response.
        direct_service_uuid=canonical_uuid(item.get("service_uuid")),
        normalized_name=name,
        currency=currency.strip().upper(),
        price_minor=price,
        duration_minutes=_duration_minutes(duration),
    )


def prove_ordered_service(ordered: OrderedService, expectation: ServiceBaseline) -> None:
    """Raise unless the live line matches what was pinned before the write.

    A direct catalogue UUID, if the API ever supplies one, is authoritative in
    both directions: it proves the service when it agrees, and it is a hard
    conflict when it disagrees. An attribute match must never rescue that — the
    whole point of §28.2 is that attributes stand in for an absent identifier,
    not for a present one that says something else.
    """
    if ordered.direct_service_uuid is not None:
        if ordered.direct_service_uuid != expectation.easyweek_service_uuid:
            raise ServiceEvidenceError(ORDERED_SERVICE_UUID_CONFLICT)
        return

    if ordered.normalized_name != expectation.normalized_name:
        raise ServiceEvidenceError(ORDERED_SERVICE_MISMATCH, "name")
    if ordered.currency != expectation.currency:
        raise ServiceEvidenceError(ORDERED_SERVICE_MISMATCH, "currency")
    if ordered.price_minor != expectation.price_minor:
        raise ServiceEvidenceError(ORDERED_SERVICE_MISMATCH, "price")
    if ordered.duration_minutes != expectation.duration_minutes:
        raise ServiceEvidenceError(ORDERED_SERVICE_MISMATCH, "duration")


def read_pagination(meta: object, *, page: int) -> tuple[int, int]:
    """``(last_page, total)`` from a list response's ``meta``, or raise.

    A partial list is the failure mode this guards: a service missing from page
    three reads exactly like a service that is not in the catalogue, and one of
    those is a reason to refuse while the other is a reason to migrate.
    """
    if not isinstance(meta, dict):
        raise ServiceEvidenceError(CATALOG_PAGINATION_INCOMPLETE, "meta")
    current = meta.get("current_page")
    last = meta.get("last_page")
    total = meta.get("total")
    if type(current) is not int or current != page:
        raise ServiceEvidenceError(CATALOG_PAGINATION_INCOMPLETE, "current_page")
    if type(last) is not int or not 1 <= last <= _MAX_CATALOG_PAGES:
        raise ServiceEvidenceError(CATALOG_PAGINATION_INCOMPLETE, "last_page")
    if type(total) is not int or total < 0:
        raise ServiceEvidenceError(CATALOG_PAGINATION_INCOMPLETE, "total")
    return last, total
