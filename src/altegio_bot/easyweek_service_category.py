"""Fail-closed EasyWeek service-category eligibility contract (PR-7.1).

The webhook category is the only approved service eligibility proof.  This
module deliberately knows nothing about service names, descriptions or ids, so
neither the planner nor the outbox can grow an accidental fallback.
"""

from __future__ import annotations

import json
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

MAX_ALLOWED_SERVICE_CATEGORIES: Final = 32
MAX_SERVICE_CATEGORY_LENGTH: Final = 128

EASYWEEK_RAW_NAMESPACE: Final = "easyweek"
SERVICE_CATEGORY_SNAPSHOT_KEY: Final = "service_category"
SERVICES_COUNT_SNAPSHOT_KEY: Final = "services_count"

ALLOWED: Final = "allowed"
CATEGORY_MISSING: Final = "category_missing"
CATEGORY_NOT_ALLOWED: Final = "category_not_allowed"
CATEGORY_AMBIGUOUS_MULTI_SERVICE: Final = "category_ambiguous_multi_service"
SERVICE_COUNT_UNPROVEN: Final = "service_count_unproven"
ALLOWED_CATEGORIES_UNCONFIGURED: Final = "allowed_categories_unconfigured"
ALLOWED_CATEGORIES_INVALID: Final = "allowed_categories_invalid"


@dataclass(frozen=True)
class NormalizedServiceCategory:
    """Bounded display value and its exact-match comparison key."""

    value: str
    key: str


@dataclass(frozen=True)
class AllowedServiceCategories:
    """Total parse result; invalid input never degrades to a partial list."""

    configured: bool
    valid: bool
    keys: frozenset[str] = frozenset()

    @property
    def ready(self) -> bool:
        return self.configured and self.valid and bool(self.keys)

    @property
    def unavailable_reason(self) -> str | None:
        """Stable reason for an unusable configuration, if any."""
        if not self.configured:
            return ALLOWED_CATEGORIES_UNCONFIGURED
        if not self.valid:
            return ALLOWED_CATEGORIES_INVALID
        return None


@dataclass(frozen=True)
class ServiceCategoryEligibility:
    """One stable, PII-safe decision shared by planning and sending."""

    allowed: bool
    reason: str
    recoverable_configuration: bool = False

    @property
    def terminal_business_suppression(self) -> bool:
        return not self.allowed and not self.recoverable_configuration


def normalize_service_category(value: object) -> NormalizedServiceCategory | None:
    """Normalize one category for storage and exact matching, or reject it.

    Compatibility normalization is intentional: operators should not have to
    distinguish canonically equivalent Unicode or presentation forms. Control
    and format characters are rejected before and after normalization; they
    must never be silently collapsed into a value that looks harmless.
    """
    if not isinstance(value, str) or len(value) > MAX_SERVICE_CATEGORY_LENGTH:
        return None
    if any(unicodedata.category(char).startswith("C") for char in value):
        return None

    normalized = unicodedata.normalize("NFKC", value)
    if any(unicodedata.category(char).startswith("C") for char in normalized):
        return None
    collapsed = " ".join(normalized.split())
    if not collapsed or len(collapsed) > MAX_SERVICE_CATEGORY_LENGTH:
        return None

    key = unicodedata.normalize("NFKC", collapsed.casefold())
    if not key or len(key) > MAX_SERVICE_CATEGORY_LENGTH:
        return None
    return NormalizedServiceCategory(value=collapsed, key=key)


def parse_allowed_service_categories(raw: object) -> AllowedServiceCategories:
    """Parse the JSON allowlist without coercion, exceptions or partial use."""
    if not isinstance(raw, str):
        return AllowedServiceCategories(configured=True, valid=False)

    stripped = raw.strip()
    if not stripped:
        return AllowedServiceCategories(configured=False, valid=True)

    try:
        parsed = json.loads(stripped)
    except Exception:
        return AllowedServiceCategories(configured=True, valid=False)

    if not isinstance(parsed, list):
        return AllowedServiceCategories(configured=True, valid=False)
    if not parsed:
        return AllowedServiceCategories(configured=False, valid=True)
    if len(parsed) > MAX_ALLOWED_SERVICE_CATEGORIES:
        return AllowedServiceCategories(configured=True, valid=False)

    keys: set[str] = set()
    for item in parsed:
        normalized = normalize_service_category(item)
        if normalized is None or normalized.key in keys:
            return AllowedServiceCategories(configured=True, valid=False)
        keys.add(normalized.key)

    return AllowedServiceCategories(configured=True, valid=True, keys=frozenset(keys))


def record_raw_with_service_category(raw: object, category: str | None) -> dict:
    """Return a new JSONB object with the minimal EasyWeek category snapshot.

    Existing top-level keys and unrelated keys inside the EasyWeek namespace
    survive. ``None`` removes only the category proof, which is the required
    explicit-clear semantics.
    """
    updated = dict(raw) if isinstance(raw, Mapping) else {}
    namespace_raw = updated.get(EASYWEEK_RAW_NAMESPACE)
    namespace = dict(namespace_raw) if isinstance(namespace_raw, Mapping) else {}

    if category is None:
        namespace.pop(SERVICE_CATEGORY_SNAPSHOT_KEY, None)
    else:
        namespace[SERVICE_CATEGORY_SNAPSHOT_KEY] = category

    if namespace:
        updated[EASYWEEK_RAW_NAMESPACE] = namespace
    else:
        updated.pop(EASYWEEK_RAW_NAMESPACE, None)
    return updated


def record_raw_with_services_count(raw: object, services_count: int | None) -> dict:
    """Return a new JSONB object with the minimal service-count proof.

    Only a positive, non-bool integer is persisted. Explicit null, zero,
    negative or malformed input removes the proof; callers use presence
    tracking to distinguish that clear from an absent patch field.
    """
    updated = dict(raw) if isinstance(raw, Mapping) else {}
    namespace_raw = updated.get(EASYWEEK_RAW_NAMESPACE)
    namespace = dict(namespace_raw) if isinstance(namespace_raw, Mapping) else {}

    if isinstance(services_count, bool) or not isinstance(services_count, int) or services_count <= 0:
        namespace.pop(SERVICES_COUNT_SNAPSHOT_KEY, None)
    else:
        namespace[SERVICES_COUNT_SNAPSHOT_KEY] = services_count

    if namespace:
        updated[EASYWEEK_RAW_NAMESPACE] = namespace
    else:
        updated.pop(EASYWEEK_RAW_NAMESPACE, None)
    return updated


def service_category_from_record_raw(raw: object) -> str | None:
    """Read and revalidate the persisted category proof without guessing."""
    if not isinstance(raw, Mapping):
        return None
    namespace = raw.get(EASYWEEK_RAW_NAMESPACE)
    if not isinstance(namespace, Mapping):
        return None
    normalized = normalize_service_category(namespace.get(SERVICE_CATEGORY_SNAPSHOT_KEY))
    return normalized.value if normalized is not None else None


def services_count_from_record_raw(raw: object) -> int | None:
    """Read a positive persisted count without coercing corrupted JSONB."""
    if not isinstance(raw, Mapping):
        return None
    namespace = raw.get(EASYWEEK_RAW_NAMESPACE)
    if not isinstance(namespace, Mapping):
        return None
    value = namespace.get(SERVICES_COUNT_SNAPSHOT_KEY)
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    return value


def evaluate_service_category(
    *,
    record_raw: object,
    allowed_categories_raw: object,
) -> ServiceCategoryEligibility:
    """Evaluate persisted category proof against the configured exact allowlist."""
    allowed_categories = parse_allowed_service_categories(allowed_categories_raw)
    if unavailable_reason := allowed_categories.unavailable_reason:
        return ServiceCategoryEligibility(
            False,
            unavailable_reason,
            recoverable_configuration=True,
        )

    services_count = services_count_from_record_raw(record_raw)
    if services_count is None:
        return ServiceCategoryEligibility(False, SERVICE_COUNT_UNPROVEN)
    if services_count != 1:
        return ServiceCategoryEligibility(False, CATEGORY_AMBIGUOUS_MULTI_SERVICE)

    category = service_category_from_record_raw(record_raw)
    normalized = normalize_service_category(category)
    if normalized is None:
        return ServiceCategoryEligibility(False, CATEGORY_MISSING)
    if normalized.key not in allowed_categories.keys:
        return ServiceCategoryEligibility(False, CATEGORY_NOT_ALLOWED)
    return ServiceCategoryEligibility(True, ALLOWED)


__all__ = [
    "ALLOWED",
    "ALLOWED_CATEGORIES_INVALID",
    "ALLOWED_CATEGORIES_UNCONFIGURED",
    "CATEGORY_AMBIGUOUS_MULTI_SERVICE",
    "CATEGORY_MISSING",
    "CATEGORY_NOT_ALLOWED",
    "EASYWEEK_RAW_NAMESPACE",
    "MAX_ALLOWED_SERVICE_CATEGORIES",
    "MAX_SERVICE_CATEGORY_LENGTH",
    "SERVICE_CATEGORY_SNAPSHOT_KEY",
    "SERVICES_COUNT_SNAPSHOT_KEY",
    "SERVICE_COUNT_UNPROVEN",
    "AllowedServiceCategories",
    "NormalizedServiceCategory",
    "ServiceCategoryEligibility",
    "evaluate_service_category",
    "normalize_service_category",
    "parse_allowed_service_categories",
    "record_raw_with_service_category",
    "record_raw_with_services_count",
    "service_category_from_record_raw",
    "services_count_from_record_raw",
]
