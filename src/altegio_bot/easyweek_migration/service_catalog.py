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
import unicodedata
from dataclasses import dataclass
from typing import Any, Final

from altegio_bot.easyweek_migration.manifest import ServiceMapping, canonical_uuid
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
    """
    if not isinstance(raw, str):
        return None
    collapsed = " ".join(raw.split())
    if not collapsed:
        return None
    return unicodedata.normalize("NFC", collapsed).casefold()


@dataclass(frozen=True)
class CatalogService:
    """One catalogue entry, reduced to what identity is argued from."""

    uuid: str
    normalized_name: str
    currency: str
    price_minor: int
    duration_minutes: int

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
class ServiceExpectation:
    """What the booking's service must look like, pinned before the write.

    Pinned from the live catalogue rather than from the manifest alone: the
    manifest states the price and duration an operator verified, and this records
    that the catalogue still agrees with them *and* that nothing else in the
    catalogue looks the same.
    """

    method: str
    easyweek_service_uuid: str
    normalized_name: str
    currency: str
    price_minor: int
    duration_minutes: int
    catalog_digest: str

    def as_safe_dict(self) -> dict[str, Any]:
        """Identifiers, numbers and the method. The name is a digest, not text."""
        return {
            "method": self.method,
            "easyweek_service_uuid": self.easyweek_service_uuid,
            "service_name_digest": hashlib.sha256(self.normalized_name.encode("utf-8")).hexdigest()[:16],
            "currency": self.currency,
            "price_minor_units": self.price_minor,
            "duration_minutes": self.duration_minutes,
            "catalog_digest": self.catalog_digest,
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

    return CatalogService(
        uuid=service_uuid,
        normalized_name=name,
        currency=currency,
        price_minor=price,
        duration_minutes=minutes,
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


def pin_service_expectation(
    catalog: CatalogSnapshot,
    *,
    easyweek_service_uuid: str,
    mapping: ServiceMapping,
) -> ServiceExpectation:
    """Pin what the booking's service must look like, or raise fail-closed.

    Three things must hold, and all three are checked against the catalogue that
    was just read rather than against a stored audit:

    1. the expected UUID is in the catalogue;
    2. its price and duration still equal the manifest's verified baseline —
       a catalogue edit since the operator checked is a changed expectation, and
       §28.2 requires a new reviewed plan for that, not a silent re-baseline;
    3. no other catalogue entry shares its exact attributes, so the attributes
       identify it.
    """
    service = catalog.by_uuid(easyweek_service_uuid)
    if service is None:
        # Includes the re-created-service case: a new UUID is never adopted by
        # name, however well the name matches.
        raise ServiceEvidenceError(SERVICE_NOT_IN_CATALOG)

    try:
        expected_price_minor = to_minor_units(mapping.catalog_price, currency=service.currency)
    except AmountError:
        raise ServiceEvidenceError(SERVICE_FORMAT_UNSUPPORTED, "manifest_price") from None

    if mapping.catalog_duration.minutes is None:
        raise ServiceEvidenceError(SERVICE_FORMAT_UNSUPPORTED, "manifest_duration")

    if service.price_minor != expected_price_minor or service.duration_minutes != mapping.catalog_duration.minutes:
        raise ServiceEvidenceError(SERVICE_ATTRIBUTES_CHANGED)

    candidates = catalog.matching(service.attributes)
    if len(candidates) != 1 or candidates[0].uuid != service.uuid:
        # Either two services look identical, or the one that does is not ours.
        # Both mean the attributes cannot stand in for the identifier.
        raise ServiceEvidenceError(SERVICE_ATTRIBUTES_AMBIGUOUS)

    return ServiceExpectation(
        method=SERVICE_PROOF_METHOD,
        easyweek_service_uuid=service.uuid,
        normalized_name=service.normalized_name,
        currency=service.currency,
        price_minor=service.price_minor,
        duration_minutes=service.duration_minutes,
        catalog_digest=catalog.digest,
    )


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


def prove_ordered_service(ordered: OrderedService, expectation: ServiceExpectation) -> None:
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
