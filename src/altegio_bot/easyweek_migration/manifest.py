"""The explicit, verifiable Altegio → EasyWeek mapping (PR-11.1).

Three identifier spaces have to line up before a single booking can be created:

    Altegio company_id  →  EasyWeek location uuid  (+ its numeric location id)
    Altegio staff id    →  EasyWeek staff uuid
    Altegio service id  →  EasyWeek service uuid

None of them can be derived. Names look tempting — "Anna" is "Anna", "Refill" is
"Refill" — and that is exactly the trap: a fuzzy match that is right 95% of the
time books 1 customer in 20 with the wrong person, at the wrong branch, and the
mistake is only visible when somebody turns up. So the mapping is a file an
operator writes and checks, and this parser accepts nothing it did not write.

Design mirrors :mod:`altegio_bot.easyweek_locations`, which guards the same class
of boundary:

* **Total.** Parsing never raises and never half-succeeds; an invalid manifest
  produces an invalid registry that no mode will act on.
* **All-or-nothing.** One bad entry rejects the whole file. Dropping it silently
  would turn a typo into "that master simply has no bookings today".
* **Scoped.** Staff and service ids are keyed *inside* a company. Altegio numeric
  ids are only unique per company, so a flat map would let Karlsruhe's staff 42
  answer for Rastatt's staff 42.
* **Quiet.** The offending value never reaches an error message or a log. A
  manifest is not secret, but errors from it end up in tickets, and the habit of
  echoing configuration is how tokens leak.

The manifest holds ids only. It carries no customer data of any kind, which is
why it is the one migration input that may safely live in Git.
"""

from __future__ import annotations

import hashlib
import json
import uuid as uuid_module
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

from altegio_bot.easyweek_locations import PG_INT_MAX

# The two Altegio branches that migrate. Durlach is absent from Altegio entirely
# and is therefore not expressible here: there is no company_id to write down.
KARLSRUHE_COMPANY_ID: Final = 758285
RASTATT_COMPANY_ID: Final = 1271200
MIGRATABLE_COMPANY_IDS: Final[frozenset[int]] = frozenset({KARLSRUHE_COMPANY_ID, RASTATT_COMPANY_ID})

PG_BIGINT_MAX: Final = 9_223_372_036_854_775_807

_BRANCH_FIELDS: Final = frozenset(
    {
        "altegio_company_id",
        "easyweek_location_id",
        "easyweek_location_uuid",
        "staff",
        "services",
    }
)

_TOP_LEVEL_FIELDS: Final = frozenset({"manifest_id", "branches"})

# A manifest id is an operator-chosen label that ends up in every report and in
# the apply gate. Kept to a boring closed alphabet so it cannot smuggle newlines
# into a report or shell metacharacters into a runbook copy-paste.
_MANIFEST_ID_CHARS: Final = frozenset("abcdefghijklmnopqrstuvwxyz0123456789-_.")
_MANIFEST_ID_MAX_LEN: Final = 64

# Stable technical reasons. Deliberately coarse: an operator fixes the file and
# re-runs, and a finer taxonomy would only tempt us to print the bad value.
INVALID_NOT_A_FILE: Final = "manifest_not_readable"
INVALID_NOT_JSON: Final = "manifest_not_json"
INVALID_SHAPE: Final = "manifest_shape_invalid"
INVALID_EMPTY: Final = "manifest_empty"
INVALID_UNKNOWN_COMPANY: Final = "manifest_unknown_company"


class _DuplicateJSONKey(Exception):
    """A JSON object listed the same key twice — the later one would win silently."""


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    keys = [key for key, _value in pairs]
    if len(keys) != len(set(keys)):
        raise _DuplicateJSONKey
    return dict(pairs)


@dataclass(frozen=True)
class BranchMapping:
    """Everything needed to place one Altegio branch's booking into EasyWeek."""

    altegio_company_id: int
    easyweek_location_id: int
    easyweek_location_uuid: str
    # Altegio staff id → EasyWeek staff uuid, scoped to THIS company.
    staff: dict[int, str]
    # Altegio service id → EasyWeek service uuid, scoped to THIS company.
    services: dict[int, str]

    def staff_uuid(self, altegio_staff_id: object) -> str | None:
        """Exact lookup only. An unmapped or non-integer id resolves to nothing."""
        if type(altegio_staff_id) is not int:
            return None
        return self.staff.get(altegio_staff_id)

    def service_uuid(self, altegio_service_id: object) -> str | None:
        """Exact lookup only. An unmapped or non-integer id resolves to nothing."""
        if type(altegio_service_id) is not int:
            return None
        return self.services.get(altegio_service_id)


@dataclass(frozen=True)
class MigrationManifest:
    """Total parse result. ``valid`` is the only thing callers may branch on."""

    valid: bool
    reason: str | None = None
    manifest_id: str = ""
    branches: dict[int, BranchMapping] = field(default_factory=dict)
    # Digest of the CANONICAL content, not of the file bytes. Reformatting the
    # JSON must not invalidate an operator's verified dry-run; changing a single
    # uuid must.
    digest: str = ""

    @property
    def company_ids(self) -> tuple[int, ...]:
        return tuple(sorted(self.branches))

    def branch(self, altegio_company_id: int) -> BranchMapping | None:
        return self.branches.get(altegio_company_id)

    def as_safe_dict(self) -> dict[str, Any]:
        """Counts and ids — never the mapping itself, which is long and noisy."""
        return {
            "valid": self.valid,
            "reason": self.reason,
            "manifest_id": self.manifest_id,
            "manifest_digest": self.digest,
            "branches": [
                {
                    "altegio_company_id": branch.altegio_company_id,
                    "easyweek_location_id": branch.easyweek_location_id,
                    "easyweek_location_uuid": branch.easyweek_location_uuid,
                    "staff_mappings": len(branch.staff),
                    "service_mappings": len(branch.services),
                }
                for _company_id, branch in sorted(self.branches.items())
            ],
        }


def _invalid(reason: str) -> MigrationManifest:
    return MigrationManifest(valid=False, reason=reason)


def canonical_uuid(raw: object) -> str | None:
    """Return the canonical form of *raw*, and only if it was already canonical.

    An operator who pastes ``{ABC...}`` or an uppercase UUID has pasted something
    other than what EasyWeek returned, and the difference is worth surfacing at
    parse time rather than at the first 404 in the middle of a bulk apply.
    """
    if not isinstance(raw, str):
        return None
    try:
        canonical = str(uuid_module.UUID(raw))
    except (ValueError, AttributeError, TypeError):
        return None
    return canonical if canonical == raw else None


def _positive_id(value: object, *, maximum: int) -> int | None:
    """Exact positive int. ``bool`` is not an id; ``"42"`` is not an id either."""
    if type(value) is not int:
        return None
    if not (0 < value <= maximum):
        return None
    return value


def _parse_id_to_uuid_map(raw: object) -> dict[int, str] | None:
    """Parse a ``{"<altegio numeric id>": "<easyweek uuid>"}`` object.

    JSON object keys are strings, so the numeric id arrives as text and is
    converted here — strictly: ``"07"``, ``"+7"``, ``" 7"`` and ``"7.0"`` all
    round-trip to something other than themselves and are rejected, because an
    id an operator cannot read back out of the file is an id they cannot verify.

    Two Altegio ids may legitimately point at ONE EasyWeek uuid (a merged
    service), so target uuids are not required to be distinct. Two identical
    source ids are impossible — duplicate JSON keys were already rejected.
    """
    if not isinstance(raw, dict):
        return None

    result: dict[int, str] = {}
    for key, value in raw.items():
        if not isinstance(key, str) or not key.isdigit():
            return None
        source_id = int(key)
        if str(source_id) != key or not (0 < source_id <= PG_BIGINT_MAX):
            return None
        target_uuid = canonical_uuid(value)
        if target_uuid is None:
            return None
        result[source_id] = target_uuid
    return result


def _canonical_digest(manifest_id: str, branches: dict[int, BranchMapping]) -> str:
    """Digest over the SEMANTIC content, in a fixed order.

    This value is half of the apply gate's "the plan you verified is the plan you
    are about to run". Sorting everything means whitespace, key order and
    re-exports do not move it; the actual identifiers do.
    """
    canonical = {
        "manifest_id": manifest_id,
        "branches": [
            {
                "altegio_company_id": branch.altegio_company_id,
                "easyweek_location_id": branch.easyweek_location_id,
                "easyweek_location_uuid": branch.easyweek_location_uuid,
                "staff": sorted(branch.staff.items()),
                "services": sorted(branch.services.items()),
            }
            for _company_id, branch in sorted(branches.items())
        ],
    }
    blob = json.dumps(canonical, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def parse_manifest(raw: object) -> MigrationManifest:
    """Parse manifest JSON text without coercion or partial acceptance.

    Canonical shape (placeholder values — the real EasyWeek location identity is
    read from ``GET /locations`` and never hardcoded in Python; plan §10 showed
    those ids are not stable, and a guard test enforces their absence here)::

        {
          "manifest_id": "cutover-2026-09-01",
          "branches": {
            "758285": {
              "altegio_company_id": 758285,
              "easyweek_location_id": 100001,
              "easyweek_location_uuid": "<easyweek location uuid>",
              "staff":    {"111": "<easyweek staff uuid>"},
              "services": {"222": "<easyweek service uuid>"}
            }
          }
        }

    The company id is written **twice** on purpose: once as the key and once
    inside the entry, and they must agree. It is the same class of check that
    caught the Durlach/Rastatt swap in plan §10 — one value can be typed wrong
    and look plausible, two values that must match cannot.
    """
    if not isinstance(raw, str) or not raw.strip():
        return _invalid(INVALID_NOT_JSON)

    try:
        parsed = json.loads(raw, object_pairs_hook=_reject_duplicate_keys)
    except Exception:
        return _invalid(INVALID_NOT_JSON)

    if not isinstance(parsed, dict) or frozenset(parsed) != _TOP_LEVEL_FIELDS:
        return _invalid(INVALID_SHAPE)

    manifest_id = parsed.get("manifest_id")
    if (
        not isinstance(manifest_id, str)
        or not manifest_id
        or len(manifest_id) > _MANIFEST_ID_MAX_LEN
        or not set(manifest_id) <= _MANIFEST_ID_CHARS
    ):
        return _invalid(INVALID_SHAPE)

    raw_branches = parsed.get("branches")
    if not isinstance(raw_branches, dict):
        return _invalid(INVALID_SHAPE)
    if not raw_branches:
        return _invalid(INVALID_EMPTY)

    branches: dict[int, BranchMapping] = {}
    seen_location_ids: set[int] = set()
    seen_location_uuids: set[str] = set()

    for key, entry in raw_branches.items():
        if not isinstance(key, str) or not key.isdigit():
            return _invalid(INVALID_SHAPE)
        keyed_company_id = int(key)
        if str(keyed_company_id) != key:
            return _invalid(INVALID_SHAPE)

        if not isinstance(entry, dict) or frozenset(entry) != _BRANCH_FIELDS:
            return _invalid(INVALID_SHAPE)

        company_id = _positive_id(entry.get("altegio_company_id"), maximum=PG_BIGINT_MAX)
        if company_id is None or company_id != keyed_company_id:
            return _invalid(INVALID_SHAPE)
        # Durlach has no Altegio company_id, so it cannot be named here; any
        # other company is simply not part of this cutover.
        if company_id not in MIGRATABLE_COMPANY_IDS:
            return _invalid(INVALID_UNKNOWN_COMPANY)
        if company_id in branches:
            return _invalid(INVALID_SHAPE)

        location_id = _positive_id(entry.get("easyweek_location_id"), maximum=PG_INT_MAX)
        if location_id is None or location_id in seen_location_ids:
            return _invalid(INVALID_SHAPE)

        location_uuid = canonical_uuid(entry.get("easyweek_location_uuid"))
        if location_uuid is None or location_uuid in seen_location_uuids:
            return _invalid(INVALID_SHAPE)

        staff = _parse_id_to_uuid_map(entry.get("staff"))
        services = _parse_id_to_uuid_map(entry.get("services"))
        if staff is None or services is None:
            return _invalid(INVALID_SHAPE)
        # An empty staff or service map is not a configuration, it is an
        # unfinished one: every booking in that branch would block, and the
        # operator would read a report full of `mapping_missing` instead of a
        # single clear "you have not filled the manifest in yet".
        if not staff or not services:
            return _invalid(INVALID_EMPTY)

        branches[company_id] = BranchMapping(
            altegio_company_id=company_id,
            easyweek_location_id=location_id,
            easyweek_location_uuid=location_uuid,
            staff=staff,
            services=services,
        )
        seen_location_ids.add(location_id)
        seen_location_uuids.add(location_uuid)

    return MigrationManifest(
        valid=True,
        manifest_id=manifest_id,
        branches=branches,
        digest=_canonical_digest(manifest_id, branches),
    )


def load_manifest(path: str | Path) -> MigrationManifest:
    """Read and parse a manifest file. Unreadable is invalid, never an exception."""
    try:
        raw = Path(path).read_text(encoding="utf-8")
    except OSError:
        return _invalid(INVALID_NOT_A_FILE)
    except UnicodeDecodeError:
        return _invalid(INVALID_NOT_JSON)
    return parse_manifest(raw)
