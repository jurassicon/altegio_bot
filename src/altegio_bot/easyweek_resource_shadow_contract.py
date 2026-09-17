"""Immutable Karlsruhe service contract for the PR-7.5 resource-shadow proof.

Why a code-side table exists at all
-----------------------------------
EasyWeek represents a pedicure and the technical chair it occupies as *two*
``ordered_services`` rows carrying the same service name, while the webhook
reports one business service.  The two rows agree on every business field and
differ only in technical API fields we do not model, so the PR-7.4 full-row
signature digest cannot recognise them as one business line and the booking
fails closed as ``multi_service_duplicate_ambiguous``.

Deciding that a repeated name is a resource shadow rather than two genuinely
identical services requires knowing *which* services are resource-backed.  That
knowledge is not derivable from the API: it is an owner-controlled property of
one branch's catalogue.  Hence an exact, versioned, provider/company/location
scoped table, and nothing wider.

Provenance
----------
The exact numeric ids, exact display names and the three resource-backed names
are the owner-approved table recorded in canonical plan §38.6.  They were first
proved against the full live Karlsruhe catalogue in the sibling Irida
deployment's reviewed resource-aware ownership hotfix (its commits ``1dc5466``,
``721618a``, ``aae5667``, ``a122a34``, ``496152e``).  Nothing here imports,
calls or depends on that deployment at runtime: only the verified constants are
restated, and this module re-proves every one of them against this bot's own
live catalogue read before the contract may be used.

Rules this table lives under
----------------------------
* Only ``provider=easyweek``, only company ``322579``, only the one location
  UUID below.  No other branch may borrow it, and Durlach/Rastatt/Altegio are
  untouched.
* Catalogue service UUIDs are deliberately NOT pinned.  A UUID and a numeric
  webhook ``service_id`` are different identifiers; each exact name is resolved
  afresh, and must resolve uniquely, in the full live catalogue.
* A rename is a code change, a review and a redeploy — never a runtime lookup,
  a substring match, a prefix match, a transliteration or a fuzzy guess.
* The table carries an explicit revision and a canonical digest so a durable
  snapshot can be tied to the exact contract that produced it.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Final

PROVIDER: Final = "easyweek"

KARLSRUHE_COMPANY_ID: Final = 322579
KARLSRUHE_LOCATION_UUID: Final = "8395fab6-7ee8-4702-88d9-fd78f92539c1"
# Every service in the table belongs to this one catalogue category.  A drifted
# category closes the contract instead of quietly proving a different service.
KARLSRUHE_SERVICE_CATEGORY: Final = "Nagelservice"
KARLSRUHE_CONTRACT_REVISION: Final = 1

# Exact numeric webhook service_id -> exact catalogue display name.
KARLSRUHE_NUMERIC_SERVICE_NAMES: Final[dict[int, str]] = {
    1030228: "Hygienische Maniküre für Damen",
    1030231: "Hygienische Maniküre für Herren",
    1030234: "Maniküre mit Gel-Lack / Shellac",
    1030237: "Maniküre mit French/Design",
    1030240: "Hygienische Pediküre für Damen",
    1030243: "Pediküre mit Gel-Lack",
    1030246: "Pediküre Mit French",
}

# The ONLY names whose repeated ordered row may be a technical resource copy.
# A repeated manicure is two real services and stays fail-closed.
KARLSRUHE_RESOURCE_BACKED_SERVICE_NAMES: Final[frozenset[str]] = frozenset(
    {
        "Hygienische Pediküre für Damen",
        "Pediküre mit Gel-Lack",
        "Pediküre Mit French",
    }
)

RESOURCE_SHADOW_PROOF_KIND: Final = "karlsruhe_resource_shadow"


@dataclass(frozen=True)
class ResourceShadowContract:
    """One immutable provider/company/location scoped service table."""

    provider: str
    company_id: int
    location_uuid: str
    revision: int
    category: str
    numeric_service_names: Mapping[int, str]
    resource_backed_service_names: frozenset[str]
    digest: str

    @property
    def service_names(self) -> frozenset[str]:
        return frozenset(self.numeric_service_names.values())


def _canonical_location_uuid(value: object) -> str | None:
    if not isinstance(value, str):
        return None
    try:
        return str(uuid.UUID(value.strip()))
    except (ValueError, AttributeError, TypeError):
        return None


def _contract_digest(
    *,
    provider: str,
    company_id: int,
    location_uuid: str,
    revision: int,
    category: str,
    numeric_service_names: Mapping[int, str],
    resource_backed_service_names: frozenset[str],
) -> str:
    """Digest every byte a snapshot promises the contract still contains."""
    canonical = json.dumps(
        {
            "provider": provider,
            "company_id": company_id,
            "location_uuid": location_uuid,
            "revision": revision,
            "category": category,
            "numeric_service_names": {str(key): value for key, value in sorted(numeric_service_names.items())},
            "resource_backed_service_names": sorted(resource_backed_service_names),
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _table_is_self_consistent() -> bool:
    """An edited-wrong table closes the feature instead of proving nonsense."""
    names = list(KARLSRUHE_NUMERIC_SERVICE_NAMES.values())
    if not names or len(set(names)) != len(names):
        return False
    if any(type(key) is not int or key <= 0 for key in KARLSRUHE_NUMERIC_SERVICE_NAMES):
        return False
    if any(not isinstance(name, str) or name != name.strip() or not name for name in names):
        return False
    if not KARLSRUHE_RESOURCE_BACKED_SERVICE_NAMES:
        return False
    return KARLSRUHE_RESOURCE_BACKED_SERVICE_NAMES <= set(names)


_KARLSRUHE_CONTRACT: Final[ResourceShadowContract | None] = (
    ResourceShadowContract(
        provider=PROVIDER,
        company_id=KARLSRUHE_COMPANY_ID,
        location_uuid=KARLSRUHE_LOCATION_UUID,
        revision=KARLSRUHE_CONTRACT_REVISION,
        category=KARLSRUHE_SERVICE_CATEGORY,
        numeric_service_names=dict(KARLSRUHE_NUMERIC_SERVICE_NAMES),
        resource_backed_service_names=KARLSRUHE_RESOURCE_BACKED_SERVICE_NAMES,
        digest=_contract_digest(
            provider=PROVIDER,
            company_id=KARLSRUHE_COMPANY_ID,
            location_uuid=KARLSRUHE_LOCATION_UUID,
            revision=KARLSRUHE_CONTRACT_REVISION,
            category=KARLSRUHE_SERVICE_CATEGORY,
            numeric_service_names=KARLSRUHE_NUMERIC_SERVICE_NAMES,
            resource_backed_service_names=KARLSRUHE_RESOURCE_BACKED_SERVICE_NAMES,
        ),
    )
    if _table_is_self_consistent()
    else None
)


def resolve_resource_shadow_contract(
    *,
    provider: object,
    company_id: object,
    location_uuid: object,
) -> ResourceShadowContract | None:
    """The one contract for this exact identity triple, or ``None``.

    ``None`` is the normal answer for every other provider, company and
    location: the caller then keeps the unchanged PR-7.4 behaviour.  There is
    deliberately no lookup by name, prefix or partial identity, so no branch can
    borrow another branch's table.
    """
    contract = _KARLSRUHE_CONTRACT
    if contract is None:
        return None
    if provider != contract.provider:
        return None
    if type(company_id) is not int or company_id != contract.company_id:
        return None
    if _canonical_location_uuid(location_uuid) != contract.location_uuid:
        return None
    return contract


__all__ = [
    "KARLSRUHE_COMPANY_ID",
    "KARLSRUHE_CONTRACT_REVISION",
    "KARLSRUHE_LOCATION_UUID",
    "KARLSRUHE_NUMERIC_SERVICE_NAMES",
    "KARLSRUHE_RESOURCE_BACKED_SERVICE_NAMES",
    "KARLSRUHE_SERVICE_CATEGORY",
    "PROVIDER",
    "RESOURCE_SHADOW_PROOF_KIND",
    "ResourceShadowContract",
    "resolve_resource_shadow_contract",
]
