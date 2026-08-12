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

ALLOWED: Final = "allowed"
CATEGORY_MISSING: Final = "category_missing"
CATEGORY_NOT_ALLOWED: Final = "category_not_allowed"
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


@dataclass(frozen=True)
class ServiceCategoryEligibility:
    """One stable, PII-safe decision shared by planning and sending."""

    allowed: bool
    reason: str


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


def service_category_from_record_raw(raw: object) -> str | None:
    """Read and revalidate the persisted category proof without guessing."""
    if not isinstance(raw, Mapping):
        return None
    namespace = raw.get(EASYWEEK_RAW_NAMESPACE)
    if not isinstance(namespace, Mapping):
        return None
    normalized = normalize_service_category(namespace.get(SERVICE_CATEGORY_SNAPSHOT_KEY))
    return normalized.value if normalized is not None else None


def evaluate_service_category(
    *,
    record_raw: object,
    allowed_categories_raw: object,
) -> ServiceCategoryEligibility:
    """Evaluate persisted category proof against the configured exact allowlist."""
    allowed_categories = parse_allowed_service_categories(allowed_categories_raw)
    if not allowed_categories.configured:
        return ServiceCategoryEligibility(False, ALLOWED_CATEGORIES_UNCONFIGURED)
    if not allowed_categories.valid:
        return ServiceCategoryEligibility(False, ALLOWED_CATEGORIES_INVALID)

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
    "CATEGORY_MISSING",
    "CATEGORY_NOT_ALLOWED",
    "EASYWEEK_RAW_NAMESPACE",
    "MAX_ALLOWED_SERVICE_CATEGORIES",
    "MAX_SERVICE_CATEGORY_LENGTH",
    "SERVICE_CATEGORY_SNAPSHOT_KEY",
    "AllowedServiceCategories",
    "NormalizedServiceCategory",
    "ServiceCategoryEligibility",
    "evaluate_service_category",
    "normalize_service_category",
    "parse_allowed_service_categories",
    "record_raw_with_service_category",
    "service_category_from_record_raw",
]
