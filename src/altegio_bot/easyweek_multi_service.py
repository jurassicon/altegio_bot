"""Fail-closed proof for the PR-7.4 exactly-two EasyWeek contract.

The webhook proves which two names the customer selected.  The live booking
proves the two actual order lines, prices and durations.  The full location
catalogue proves each name's category.  None of those sources is sufficient on
its own, so this module accepts a notification only when all three agree.

Only a minimal, versioned projection is persisted in ``Record.raw``.  The live
booking response and its customer subtree never leave this function.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import unicodedata
import uuid
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Any, Final, Protocol

from altegio_bot.easyweek_client import EasyWeekError
from altegio_bot.easyweek_migration.service_catalog import (
    ServiceEvidenceError,
    read_full_catalog_rows,
)
from altegio_bot.easyweek_service_category import (
    ALLOWED,
    EASYWEEK_RAW_NAMESPACE,
    ServiceCategoryEligibility,
    evaluate_service_category,
    normalize_service_category,
    parse_allowed_service_categories,
    services_count_from_record_raw,
)

MULTI_SERVICE_SNAPSHOT_KEY: Final = "multi_service_snapshot"
MULTI_SERVICE_SNAPSHOT_VERSION: Final = 1
MULTI_SERVICE_JOB_DIGEST_KEY: Final = "multi_service_snapshot_digest"
MULTI_SERVICE_JOB_VERSION_KEY: Final = "multi_service_snapshot_version"
MULTI_SERVICE_JOB_SNAPSHOT_KEY: Final = "multi_service_snapshot"
PROVIDER: Final = "easyweek"

MULTI_SERVICE_DISABLED: Final = "multi_service_disabled"
MULTI_SERVICE_SEND_DISABLED: Final = "multi_service_send_disabled"
MULTI_SERVICE_WEBHOOK_SHAPE_UNPROVEN: Final = "multi_service_webhook_shape_unproven"
MULTI_SERVICE_RELATED_MISSING: Final = "multi_service_related_missing"
MULTI_SERVICE_NAMES_NOT_DISTINCT: Final = "multi_service_names_not_distinct"
MULTI_SERVICE_DESCRIPTION_MISMATCH: Final = "multi_service_description_mismatch"
MULTI_SERVICE_API_UNAVAILABLE: Final = "multi_service_api_unavailable"
MULTI_SERVICE_ORDERED_SERVICES_MALFORMED: Final = "multi_service_ordered_services_malformed"
MULTI_SERVICE_QUANTITY_UNSUPPORTED: Final = "multi_service_quantity_unsupported"
MULTI_SERVICE_DISCOUNT_UNSUPPORTED: Final = "multi_service_discount_unsupported"
MULTI_SERVICE_CUSTOM_PRICE_UNSUPPORTED: Final = "multi_service_custom_price_unsupported"
MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED: Final = "multi_service_custom_duration_unsupported"
MULTI_SERVICE_DUPLICATE_AMBIGUOUS: Final = "multi_service_duplicate_ambiguous"
MULTI_SERVICE_BUSINESS_COUNT_MISMATCH: Final = "multi_service_business_count_mismatch"
MULTI_SERVICE_TOTAL_MISMATCH: Final = "multi_service_total_mismatch"
MULTI_SERVICE_CURRENCY_MISMATCH: Final = "multi_service_currency_mismatch"
MULTI_SERVICE_CATALOG_UNAVAILABLE: Final = "multi_service_catalog_unavailable"
MULTI_SERVICE_CATALOG_MATCH_MISSING: Final = "multi_service_catalog_match_missing"
MULTI_SERVICE_CATEGORY_AMBIGUOUS: Final = "multi_service_category_ambiguous"
MULTI_SERVICE_CATEGORY_NOT_ALLOWED: Final = "multi_service_category_not_allowed"
MULTI_SERVICE_SNAPSHOT_MISSING: Final = "multi_service_snapshot_missing"
MULTI_SERVICE_SNAPSHOT_VERSION_UNSUPPORTED: Final = "multi_service_snapshot_version_unsupported"
MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH: Final = "multi_service_snapshot_digest_mismatch"

_CATALOG_TTL_SECONDS: Final = 300.0
_HEX_DIGEST_LENGTH: Final = 64


class ServiceEligibilityPurpose(str, Enum):
    """Closed purposes; callers cannot opt into multi-service with a bool."""

    SINGLE_SERVICE_ONLY = "single_service_only"
    LIFECYCLE_REMINDER = "lifecycle_reminder"


class MultiServiceReader(Protocol):
    async def get_booking(self, booking_uuid: str) -> dict[str, Any]: ...

    async def list_location_services(self, location_uuid: str, *, page: int) -> dict[str, Any]: ...


class MultiServiceProofError(ValueError):
    """A stable refusal which never includes a service/customer value."""

    def __init__(self, reason: str, *, recoverable: bool = False) -> None:
        self.reason = reason
        self.recoverable = recoverable
        super().__init__(reason)


@dataclass(frozen=True)
class WebhookServicePair:
    booking_uuid: uuid.UUID
    location_uuid: str
    service_name: object
    service_related: object
    services_description: object
    services_count: object
    quantity: object
    booking_currency: object
    total_cost: Decimal | None


@dataclass(frozen=True)
class MultiServiceLine:
    display_name: str
    normalized_name: str
    currency: str
    actual_price_minor: int
    actual_duration_minutes: int
    original_duration_minutes: int
    category: str
    business_signature_digest: str

    def as_dict(self) -> dict[str, object]:
        return {
            "display_name": self.display_name,
            "normalized_name": self.normalized_name,
            "currency": self.currency,
            "actual_price_minor": self.actual_price_minor,
            "actual_duration_minutes": self.actual_duration_minutes,
            "original_duration_minutes": self.original_duration_minutes,
            "category": self.category,
            "business_signature_digest": self.business_signature_digest,
        }


@dataclass(frozen=True)
class MultiServiceSnapshot:
    booking_uuid: str
    location_uuid: str
    lines: tuple[MultiServiceLine, MultiServiceLine]
    digest: str
    version: int = MULTI_SERVICE_SNAPSHOT_VERSION
    provider: str = PROVIDER

    def unsigned_dict(self) -> dict[str, object]:
        return {
            "version": self.version,
            "provider": self.provider,
            "booking_uuid": self.booking_uuid,
            "location_uuid": self.location_uuid,
            "services_count": 2,
            "lines": [line.as_dict() for line in self.lines],
        }

    def as_dict(self) -> dict[str, object]:
        return {**self.unsigned_dict(), "digest": self.digest}

    @property
    def total_minor(self) -> int:
        return sum(line.actual_price_minor for line in self.lines)


def _sha256_json(value: object) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _canonical_uuid(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        return str(uuid.UUID(value.strip()))
    except (ValueError, AttributeError, TypeError):
        return None


def _normalized_name(value: object) -> str | None:
    if not isinstance(value, str) or len(value) > 512:
        return None
    if any(unicodedata.category(char).startswith("C") for char in value):
        return None
    collapsed = " ".join(unicodedata.normalize("NFKC", value).split())
    if not collapsed:
        return None
    return unicodedata.normalize("NFKC", collapsed.casefold())


def _display_name(value: object) -> str | None:
    normalized = _normalized_name(value)
    if normalized is None or not isinstance(value, str):
        return None
    return " ".join(unicodedata.normalize("NFKC", value).split())


def _currency(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    candidate = value.strip().upper()
    if not candidate or len(candidate) > 8 or not candidate.isascii() or not candidate.isalpha():
        return None
    return candidate


def _duration_minutes(value: object) -> int | None:
    if not isinstance(value, Mapping):
        return None
    amount = value.get("value")
    label = value.get("label")
    if type(amount) is not int or amount <= 0:
        return None
    if not isinstance(label, str) or label.strip().casefold() != "minutes":
        return None
    return amount


def _minor_from_decimal(value: Decimal | None) -> int | None:
    if value is None or value < 0:
        return None
    minor = value * Decimal(100)
    integral = minor.to_integral_value()
    if minor != integral:
        return None
    return int(integral)


def webhook_pair_is_touched(present_fields: object) -> bool:
    if not isinstance(present_fields, (set, frozenset)):
        return False
    return bool(
        {
            "service_name",
            "service_related",
            "services_description",
            "services_count",
            "service_quantity",
            "total_cost",
            "booking_currency",
        }
        & present_fields
    )


def _validate_webhook(pair: WebhookServicePair) -> tuple[str, str]:
    if type(pair.services_count) is not int or pair.services_count != 2:
        raise MultiServiceProofError(MULTI_SERVICE_WEBHOOK_SHAPE_UNPROVEN)
    if type(pair.quantity) is not int or pair.quantity != 2:
        raise MultiServiceProofError(MULTI_SERVICE_WEBHOOK_SHAPE_UNPROVEN)

    first = _normalized_name(pair.service_name)
    second = _normalized_name(pair.service_related)
    if first is None:
        raise MultiServiceProofError(MULTI_SERVICE_WEBHOOK_SHAPE_UNPROVEN)
    if second is None:
        raise MultiServiceProofError(MULTI_SERVICE_RELATED_MISSING)
    if first == second:
        raise MultiServiceProofError(MULTI_SERVICE_NAMES_NOT_DISTINCT)

    description = _normalized_name(pair.services_description)
    # This is an exact whole-set representation, not a split.  A comma inside a
    # service name therefore remains part of that service instead of becoming a
    # third guessed line.
    expected = _normalized_name(f"{_display_name(pair.service_name)}, {_display_name(pair.service_related)}")
    if description is None or expected is None or description != expected:
        raise MultiServiceProofError(MULTI_SERVICE_DESCRIPTION_MISMATCH)
    if _currency(pair.booking_currency) is None or _minor_from_decimal(pair.total_cost) is None:
        raise MultiServiceProofError(MULTI_SERVICE_WEBHOOK_SHAPE_UNPROVEN)
    return first, second


@dataclass(frozen=True)
class _OrderedLine:
    display_name: str
    normalized_name: str
    currency: str
    price: int
    duration: int
    original_duration: int
    signature_digest: str


def _ordered_line(value: object) -> _OrderedLine:
    if not isinstance(value, Mapping):
        raise MultiServiceProofError(MULTI_SERVICE_ORDERED_SERVICES_MALFORMED)
    name = _display_name(value.get("name"))
    normalized = _normalized_name(value.get("name"))
    if name is None or normalized is None:
        raise MultiServiceProofError(MULTI_SERVICE_ORDERED_SERVICES_MALFORMED)

    quantity = value.get("quantity")
    if type(quantity) is not int or quantity != 1:
        raise MultiServiceProofError(MULTI_SERVICE_QUANTITY_UNSUPPORTED)
    discount = value.get("discount")
    if type(discount) is not int or discount != 0:
        raise MultiServiceProofError(MULTI_SERVICE_DISCOUNT_UNSUPPORTED)

    currency = _currency(value.get("currency"))
    price = value.get("price")
    original_price = value.get("original_price")
    if currency is None or type(price) is not int or price < 0 or type(original_price) is not int or original_price < 0:
        raise MultiServiceProofError(MULTI_SERVICE_ORDERED_SERVICES_MALFORMED)
    if price != original_price:
        raise MultiServiceProofError(MULTI_SERVICE_CUSTOM_PRICE_UNSUPPORTED)

    duration = _duration_minutes(value.get("duration"))
    original_duration = _duration_minutes(value.get("original_duration"))
    if duration is None or original_duration is None:
        raise MultiServiceProofError(MULTI_SERVICE_ORDERED_SERVICES_MALFORMED)
    if duration != original_duration:
        raise MultiServiceProofError(MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED)

    # Every service-line field can affect whether two rows are the same
    # business line.  Only the observed order-line UUID is excluded.  The
    # snapshot stores the digest, never the raw API row.
    signature_source = {key: item for key, item in value.items() if key != "uuid"}
    return _OrderedLine(
        display_name=name,
        normalized_name=normalized,
        currency=currency,
        price=price,
        duration=duration,
        original_duration=original_duration,
        signature_digest=_sha256_json(signature_source),
    )


def _business_lines(payload: Mapping[str, Any]) -> tuple[_OrderedLine, _OrderedLine]:
    raw = payload.get("ordered_services")
    if not isinstance(raw, list) or len(raw) not in (2, 3):
        raise MultiServiceProofError(MULTI_SERVICE_BUSINESS_COUNT_MISMATCH)
    lines = [_ordered_line(item) for item in raw]
    counts = Counter(line.signature_digest for line in lines)

    if len(lines) == 2:
        if len(counts) != 2:
            # Two identical returned lines might be two real identical services;
            # the observed resource-shadow contract does not prove otherwise.
            raise MultiServiceProofError(MULTI_SERVICE_DUPLICATE_AMBIGUOUS)
        return lines[0], lines[1]

    if sorted(counts.values()) != [1, 2]:
        raise MultiServiceProofError(MULTI_SERVICE_DUPLICATE_AMBIGUOUS)
    seen: set[str] = set()
    representatives: list[_OrderedLine] = []
    for line in lines:
        if line.signature_digest not in seen:
            seen.add(line.signature_digest)
            representatives.append(line)
    if len(representatives) != 2:
        raise MultiServiceProofError(MULTI_SERVICE_BUSINESS_COUNT_MISMATCH)
    return representatives[0], representatives[1]


def _category_for_name(normalized_name: str, catalog_rows: Sequence[object]) -> str:
    categories: list[str] = []
    candidates = 0
    for row in catalog_rows:
        if not isinstance(row, Mapping) or _normalized_name(row.get("name")) != normalized_name:
            continue
        candidates += 1
        category = row.get("category")
        category_name = category.get("name") if isinstance(category, Mapping) else None
        normalized = normalize_service_category(category_name)
        if normalized is None:
            raise MultiServiceProofError(MULTI_SERVICE_CATEGORY_AMBIGUOUS)
        categories.append(normalized.value)
    if candidates == 0:
        raise MultiServiceProofError(MULTI_SERVICE_CATALOG_MATCH_MISSING)
    keys = {normalize_service_category(item).key for item in categories if normalize_service_category(item) is not None}
    if len(keys) != 1:
        raise MultiServiceProofError(MULTI_SERVICE_CATEGORY_AMBIGUOUS)
    return categories[0]


def prove_exactly_two_service_snapshot(
    *,
    webhook: WebhookServicePair,
    booking_payload: object,
    catalog_rows: Sequence[object],
) -> MultiServiceSnapshot:
    """Prove and project exactly two distinct business services."""
    webhook_names = _validate_webhook(webhook)
    if not isinstance(booking_payload, Mapping):
        raise MultiServiceProofError(MULTI_SERVICE_ORDERED_SERVICES_MALFORMED)
    if _canonical_uuid(booking_payload.get("uuid")) != str(webhook.booking_uuid):
        raise MultiServiceProofError(MULTI_SERVICE_WEBHOOK_SHAPE_UNPROVEN)
    location_uuid = _canonical_uuid(webhook.location_uuid)
    if location_uuid is None or _canonical_uuid(booking_payload.get("location_uuid")) != location_uuid:
        raise MultiServiceProofError(MULTI_SERVICE_WEBHOOK_SHAPE_UNPROVEN)

    lines = _business_lines(booking_payload)
    if tuple(line.normalized_name for line in lines) != webhook_names:
        raise MultiServiceProofError(MULTI_SERVICE_BUSINESS_COUNT_MISMATCH)
    if lines[0].normalized_name == lines[1].normalized_name:
        raise MultiServiceProofError(MULTI_SERVICE_NAMES_NOT_DISTINCT)

    booking_currency = _currency(booking_payload.get("currency"))
    webhook_currency = _currency(webhook.booking_currency)
    currencies = {line.currency for line in lines}
    if (
        booking_currency is None
        or webhook_currency is None
        or currencies != {booking_currency}
        or booking_currency != webhook_currency
    ):
        raise MultiServiceProofError(MULTI_SERVICE_CURRENCY_MISMATCH)

    order = booking_payload.get("order")
    if not isinstance(order, Mapping):
        raise MultiServiceProofError(MULTI_SERVICE_TOTAL_MISMATCH)
    subtotal = order.get("subtotal")
    total = order.get("total")
    line_total = sum(line.price for line in lines)
    record_total = _minor_from_decimal(webhook.total_cost)
    if type(subtotal) is not int or type(total) is not int or min(subtotal, total) < 0:
        raise MultiServiceProofError(MULTI_SERVICE_TOTAL_MISMATCH)
    if line_total != subtotal or line_total != total or line_total != record_total:
        raise MultiServiceProofError(MULTI_SERVICE_TOTAL_MISMATCH)

    projected: list[MultiServiceLine] = []
    for line in lines:
        projected.append(
            MultiServiceLine(
                display_name=line.display_name,
                normalized_name=line.normalized_name,
                currency=line.currency,
                actual_price_minor=line.price,
                actual_duration_minutes=line.duration,
                original_duration_minutes=line.original_duration,
                category=_category_for_name(line.normalized_name, catalog_rows),
                business_signature_digest=line.signature_digest,
            )
        )

    unsigned = {
        "version": MULTI_SERVICE_SNAPSHOT_VERSION,
        "provider": PROVIDER,
        "booking_uuid": str(webhook.booking_uuid),
        "location_uuid": location_uuid,
        "services_count": 2,
        "lines": [line.as_dict() for line in projected],
    }
    return MultiServiceSnapshot(
        booking_uuid=str(webhook.booking_uuid),
        location_uuid=location_uuid,
        lines=(projected[0], projected[1]),
        digest=_sha256_json(unsigned),
    )


@dataclass
class _CatalogEntry:
    expires_at: float
    rows: tuple[object, ...]


_catalog_entries: dict[str, _CatalogEntry] = {}
_catalog_locks: dict[str, asyncio.Lock] = {}


def clear_multi_service_catalog_cache() -> None:
    """Test/operator helper; no production decision depends on stale entries."""
    _catalog_entries.clear()
    _catalog_locks.clear()


async def read_catalog_rows_cached(client: MultiServiceReader, *, location_uuid: str) -> tuple[object, ...]:
    canonical = _canonical_uuid(location_uuid)
    if canonical is None:
        raise MultiServiceProofError(MULTI_SERVICE_CATALOG_UNAVAILABLE, recoverable=True)
    now = time.monotonic()
    cached = _catalog_entries.get(canonical)
    if cached is not None and cached.expires_at > now:
        return cached.rows

    lock = _catalog_locks.setdefault(canonical, asyncio.Lock())
    async with lock:
        now = time.monotonic()
        cached = _catalog_entries.get(canonical)
        if cached is not None and cached.expires_at > now:
            return cached.rows
        try:
            _snapshot, rows = await read_full_catalog_rows(client, location_uuid=canonical)
        except ServiceEvidenceError:
            raise MultiServiceProofError(MULTI_SERVICE_CATALOG_UNAVAILABLE, recoverable=True) from None
        result = tuple(rows)
        _catalog_entries[canonical] = _CatalogEntry(expires_at=now + _CATALOG_TTL_SECONDS, rows=result)
        return result


async def fetch_and_prove_exactly_two_service_snapshot(
    *,
    client: MultiServiceReader,
    webhook: WebhookServicePair,
) -> MultiServiceSnapshot:
    """Read the booking/catalogue and apply the same pure proof as preflight."""
    _validate_webhook(webhook)
    try:
        booking_payload = await client.get_booking(str(webhook.booking_uuid))
    except EasyWeekError:
        raise MultiServiceProofError(MULTI_SERVICE_API_UNAVAILABLE, recoverable=True) from None
    catalog_rows = await read_catalog_rows_cached(client, location_uuid=webhook.location_uuid)
    return prove_exactly_two_service_snapshot(
        webhook=webhook,
        booking_payload=booking_payload,
        catalog_rows=catalog_rows,
    )


def record_raw_with_multi_service_snapshot(raw: object, snapshot: MultiServiceSnapshot | None) -> dict:
    updated = dict(raw) if isinstance(raw, Mapping) else {}
    namespace_raw = updated.get(EASYWEEK_RAW_NAMESPACE)
    namespace = dict(namespace_raw) if isinstance(namespace_raw, Mapping) else {}
    if snapshot is None:
        namespace.pop(MULTI_SERVICE_SNAPSHOT_KEY, None)
    else:
        namespace[MULTI_SERVICE_SNAPSHOT_KEY] = snapshot.as_dict()
    if namespace:
        updated[EASYWEEK_RAW_NAMESPACE] = namespace
    else:
        updated.pop(EASYWEEK_RAW_NAMESPACE, None)
    return updated


def _valid_digest(value: object) -> bool:
    return (
        isinstance(value, str)
        and len(value) == _HEX_DIGEST_LENGTH
        and all(char in "0123456789abcdef" for char in value)
    )


def multi_service_snapshot_from_record_raw(raw: object) -> tuple[MultiServiceSnapshot | None, str | None]:
    if not isinstance(raw, Mapping):
        return None, MULTI_SERVICE_SNAPSHOT_MISSING
    namespace = raw.get(EASYWEEK_RAW_NAMESPACE)
    if not isinstance(namespace, Mapping):
        return None, MULTI_SERVICE_SNAPSHOT_MISSING
    value = namespace.get(MULTI_SERVICE_SNAPSHOT_KEY)
    if not isinstance(value, Mapping):
        return None, MULTI_SERVICE_SNAPSHOT_MISSING
    if value.get("version") != MULTI_SERVICE_SNAPSHOT_VERSION:
        return None, MULTI_SERVICE_SNAPSHOT_VERSION_UNSUPPORTED
    if value.get("provider") != PROVIDER or value.get("services_count") != 2:
        return None, MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
    booking_uuid = _canonical_uuid(value.get("booking_uuid"))
    location_uuid = _canonical_uuid(value.get("location_uuid"))
    rows = value.get("lines")
    digest = value.get("digest")
    if (
        booking_uuid is None
        or location_uuid is None
        or not isinstance(rows, list)
        or len(rows) != 2
        or not _valid_digest(digest)
    ):
        return None, MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH

    lines: list[MultiServiceLine] = []
    for row in rows:
        if not isinstance(row, Mapping):
            return None, MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
        display_name = _display_name(row.get("display_name"))
        normalized_name = _normalized_name(row.get("normalized_name"))
        currency = _currency(row.get("currency"))
        category = normalize_service_category(row.get("category"))
        price = row.get("actual_price_minor")
        duration = row.get("actual_duration_minutes")
        original_duration = row.get("original_duration_minutes")
        signature = row.get("business_signature_digest")
        if (
            display_name is None
            or normalized_name is None
            or normalized_name != _normalized_name(display_name)
            or currency is None
            or category is None
            or type(price) is not int
            or price < 0
            or type(duration) is not int
            or duration <= 0
            or type(original_duration) is not int
            or original_duration <= 0
            or duration != original_duration
            or not _valid_digest(signature)
        ):
            return None, MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
        lines.append(
            MultiServiceLine(
                display_name=display_name,
                normalized_name=normalized_name,
                currency=currency,
                actual_price_minor=price,
                actual_duration_minutes=duration,
                original_duration_minutes=original_duration,
                category=category.value,
                business_signature_digest=signature,
            )
        )
    if lines[0].normalized_name == lines[1].normalized_name or lines[0].currency != lines[1].currency:
        return None, MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
    snapshot = MultiServiceSnapshot(
        booking_uuid=booking_uuid,
        location_uuid=location_uuid,
        lines=(lines[0], lines[1]),
        digest=digest,
    )
    if _sha256_json(snapshot.unsigned_dict()) != digest:
        return None, MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
    return snapshot, None


def evaluate_service_eligibility(
    *,
    record_raw: object,
    allowed_categories_raw: object,
    purpose: ServiceEligibilityPurpose,
) -> ServiceCategoryEligibility:
    """Single service stays PR-7.1; only the closed lifecycle purpose expands."""
    count = services_count_from_record_raw(record_raw)
    if count != 2 or purpose is ServiceEligibilityPurpose.SINGLE_SERVICE_ONLY:
        return evaluate_service_category(
            record_raw=record_raw,
            allowed_categories_raw=allowed_categories_raw,
        )
    allowed = parse_allowed_service_categories(allowed_categories_raw)
    if reason := allowed.unavailable_reason:
        return ServiceCategoryEligibility(False, reason, recoverable_configuration=True)
    snapshot, error = multi_service_snapshot_from_record_raw(record_raw)
    if snapshot is None:
        return ServiceCategoryEligibility(False, error or MULTI_SERVICE_SNAPSHOT_MISSING)
    for line in snapshot.lines:
        category = normalize_service_category(line.category)
        if category is None:
            return ServiceCategoryEligibility(False, MULTI_SERVICE_CATEGORY_AMBIGUOUS)
        if category.key not in allowed.keys:
            return ServiceCategoryEligibility(False, MULTI_SERVICE_CATEGORY_NOT_ALLOWED)
    return ServiceCategoryEligibility(True, ALLOWED)


def multi_service_job_payload(
    snapshot: MultiServiceSnapshot | None,
    *,
    include_snapshot: bool = False,
) -> dict[str, object]:
    if snapshot is None:
        return {}
    payload: dict[str, object] = {
        MULTI_SERVICE_JOB_VERSION_KEY: snapshot.version,
        MULTI_SERVICE_JOB_DIGEST_KEY: snapshot.digest,
    }
    # Normal webhook planning stores the projection on Record.raw.  Recovery is
    # expressly forbidden to mutate an existing Record, so its MessageJob must
    # carry the same bounded, PII-free projection.  The digest below protects
    # every byte that renderer and live guard consume.
    if include_snapshot:
        payload[MULTI_SERVICE_JOB_SNAPSHOT_KEY] = snapshot.as_dict()
    return payload


def multi_service_job_digest(payload: object) -> str | None:
    if not isinstance(payload, Mapping):
        return None
    value = payload.get(MULTI_SERVICE_JOB_DIGEST_KEY)
    return value if _valid_digest(value) else None


def multi_service_snapshot_from_job_payload(
    payload: object,
) -> tuple[MultiServiceSnapshot | None, str | None]:
    """Read an embedded recovery projection with the ordinary snapshot parser."""
    if not isinstance(payload, Mapping):
        return None, MULTI_SERVICE_SNAPSHOT_MISSING
    value = payload.get(MULTI_SERVICE_JOB_SNAPSHOT_KEY)
    if value is None:
        return None, MULTI_SERVICE_SNAPSHOT_MISSING
    return multi_service_snapshot_from_record_raw({EASYWEEK_RAW_NAMESPACE: {MULTI_SERVICE_SNAPSHOT_KEY: value}})


def multi_service_snapshot_for_job(
    *,
    record_raw: object,
    job_payload: object,
) -> tuple[MultiServiceSnapshot | None, str | None]:
    """Resolve the durable projection, refusing disagreement between sources."""
    stored, stored_error = multi_service_snapshot_from_record_raw(record_raw)
    embedded, embedded_error = multi_service_snapshot_from_job_payload(job_payload)
    if stored is None and stored_error != MULTI_SERVICE_SNAPSHOT_MISSING:
        return None, stored_error
    if embedded is None and embedded_error != MULTI_SERVICE_SNAPSHOT_MISSING:
        return None, embedded_error
    if stored is not None and embedded is not None and stored.digest != embedded.digest:
        return None, MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
    if stored is not None:
        return stored, None
    if embedded is not None:
        return embedded, None
    return None, embedded_error or stored_error or MULTI_SERVICE_SNAPSHOT_MISSING


def multi_service_send_guard(
    *,
    record_raw: object,
    job_payload: object,
    record_total_cost: Decimal | None,
    expected_booking_uuid: object,
    expected_location_uuid: object,
) -> str | None:
    """Re-prove the persisted pair and immutable job digest before rendering."""
    snapshot, error = multi_service_snapshot_for_job(
        record_raw=record_raw,
        job_payload=job_payload,
    )
    if snapshot is None:
        return error or MULTI_SERVICE_SNAPSHOT_MISSING
    expected_booking = (
        str(expected_booking_uuid)
        if isinstance(expected_booking_uuid, uuid.UUID)
        else _canonical_uuid(expected_booking_uuid)
    )
    expected_location = _canonical_uuid(expected_location_uuid)
    if snapshot.booking_uuid != expected_booking or snapshot.location_uuid != expected_location:
        return MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
    if not isinstance(job_payload, Mapping) or job_payload.get(MULTI_SERVICE_JOB_VERSION_KEY) != snapshot.version:
        return MULTI_SERVICE_SNAPSHOT_VERSION_UNSUPPORTED
    if multi_service_job_digest(job_payload) != snapshot.digest:
        return MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
    if _minor_from_decimal(record_total_cost) != snapshot.total_minor:
        return MULTI_SERVICE_TOTAL_MISMATCH
    return None


__all__ = [name for name in globals() if name.startswith("MULTI_SERVICE_")] + [
    "MultiServiceLine",
    "MultiServiceProofError",
    "MultiServiceReader",
    "MultiServiceSnapshot",
    "ServiceEligibilityPurpose",
    "WebhookServicePair",
    "clear_multi_service_catalog_cache",
    "evaluate_service_eligibility",
    "fetch_and_prove_exactly_two_service_snapshot",
    "multi_service_job_digest",
    "multi_service_job_payload",
    "multi_service_send_guard",
    "multi_service_snapshot_for_job",
    "multi_service_snapshot_from_job_payload",
    "multi_service_snapshot_from_record_raw",
    "prove_exactly_two_service_snapshot",
    "read_catalog_rows_cached",
    "record_raw_with_multi_service_snapshot",
    "webhook_pair_is_touched",
]
