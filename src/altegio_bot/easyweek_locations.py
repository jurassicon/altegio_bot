"""Strict runtime registry for EasyWeek locations (PR-7).

``EASYWEEK_LOCATION_MAP`` is a tenant-routing boundary.  A malformed value must
never be confused with an empty registry: the former is a broken configuration,
the latter is an intentionally unconfigured deployment, and both keep
processing off.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass, field

PG_INT_MAX = 2_147_483_647

_LOCATION_NAME_RE = re.compile(r"[a-z][a-z0-9_-]{0,63}")
_META_PREFIX_RE = re.compile(r"[a-z][a-z0-9]{1,7}")
_ENTRY_FIELDS = frozenset(
    {
        "location_id",
        "location_uuid",
        "meta_template_prefix",
        "booking_page_url",
    }
)


@dataclass(frozen=True)
class EasyWeekLocation:
    """One configured branch, keyed by its numeric webhook location id."""

    name: str
    location_id: int
    location_uuid: str
    meta_template_prefix: str
    booking_page_url: str

    @property
    def company_id(self) -> int:
        """Provider-scoped domain company id for this EasyWeek location."""
        return self.location_id


@dataclass(frozen=True)
class EasyWeekLocationRegistry:
    """Total parser result; invalid configuration never raises or degrades."""

    configured: bool
    valid: bool
    locations: dict[int, EasyWeekLocation] = field(default_factory=dict)

    @property
    def ready(self) -> bool:
        return self.configured and self.valid and bool(self.locations)


class _DuplicateJSONKey(Exception):
    pass


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    keys = [key for key, _value in pairs]
    if len(keys) != len(set(keys)):
        raise _DuplicateJSONKey
    return dict(pairs)


def parse_easyweek_location_map(raw: object) -> EasyWeekLocationRegistry:
    """Parse ``EASYWEEK_LOCATION_MAP`` without coercion or partial acceptance.

    Canonical JSON shape::

        {"branch-slug": {"location_id": 999001, "location_uuid": "...",
                         "meta_template_prefix": "xx",
                         "booking_page_url": "https://..."}}

    The function is total and never exposes the raw configuration in an error.
    Any invalid entry rejects the whole registry so a typo cannot silently drop
    one branch and turn its events into ordinary ``foreign_location`` traffic.
    """
    if not isinstance(raw, str):
        return EasyWeekLocationRegistry(configured=True, valid=False)

    stripped = raw.strip()
    if not stripped:
        return EasyWeekLocationRegistry(configured=False, valid=True)

    try:
        parsed = json.loads(stripped, object_pairs_hook=_reject_duplicate_keys)
    except Exception:
        return EasyWeekLocationRegistry(configured=True, valid=False)

    if not isinstance(parsed, dict):
        return EasyWeekLocationRegistry(configured=True, valid=False)
    if not parsed:
        return EasyWeekLocationRegistry(configured=False, valid=True)

    locations: dict[int, EasyWeekLocation] = {}
    seen_uuids: set[str] = set()
    seen_prefixes: set[str] = set()

    for name, entry in parsed.items():
        if not isinstance(name, str) or not _LOCATION_NAME_RE.fullmatch(name):
            return EasyWeekLocationRegistry(configured=True, valid=False)
        if not isinstance(entry, dict) or frozenset(entry) != _ENTRY_FIELDS:
            return EasyWeekLocationRegistry(configured=True, valid=False)

        location_id = entry.get("location_id")
        if type(location_id) is not int or not (0 < location_id <= PG_INT_MAX):
            return EasyWeekLocationRegistry(configured=True, valid=False)
        if location_id in locations:
            return EasyWeekLocationRegistry(configured=True, valid=False)

        raw_uuid = entry.get("location_uuid")
        if not isinstance(raw_uuid, str):
            return EasyWeekLocationRegistry(configured=True, valid=False)
        try:
            location_uuid = str(uuid.UUID(raw_uuid))
        except (ValueError, AttributeError, TypeError):
            return EasyWeekLocationRegistry(configured=True, valid=False)
        if location_uuid != raw_uuid or location_uuid in seen_uuids:
            return EasyWeekLocationRegistry(configured=True, valid=False)

        prefix = entry.get("meta_template_prefix")
        if not isinstance(prefix, str) or not _META_PREFIX_RE.fullmatch(prefix) or prefix in seen_prefixes:
            return EasyWeekLocationRegistry(configured=True, valid=False)

        booking_page_url = entry.get("booking_page_url")
        if (
            not isinstance(booking_page_url, str)
            or not booking_page_url
            or booking_page_url != booking_page_url.strip()
            or len(booking_page_url) > 2048
        ):
            return EasyWeekLocationRegistry(configured=True, valid=False)

        location = EasyWeekLocation(
            name=name,
            location_id=location_id,
            location_uuid=location_uuid,
            meta_template_prefix=prefix,
            booking_page_url=booking_page_url,
        )
        locations[location_id] = location
        seen_uuids.add(location_uuid)
        seen_prefixes.add(prefix)

    return EasyWeekLocationRegistry(configured=True, valid=True, locations=locations)


def configured_easyweek_locations() -> EasyWeekLocationRegistry:
    """Parse the current process setting at the point of use."""
    from altegio_bot.settings import settings

    return parse_easyweek_location_map(getattr(settings, "easyweek_location_map", "{}"))
