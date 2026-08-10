"""Source-controlled EasyWeek branch profiles (PR-7).

A branch identity is **indivisible**. The production defect behind Revision 3
(§10 of the canonical plan) was that each half of it was checked separately:
the numeric ``location_id`` was compared, the UUID was not, and nothing tied
either of them to the *content* that would be sent. A structurally valid
registry could therefore point Rastatt's real ids at Durlach's prefix and
Durlach's footer, and every individual check still passed.

This module is the missing binding. One profile per branch, keyed by a stable
slug, fixes four things together:

* the slug used as the top-level key of ``EASYWEEK_LOCATION_MAP``;
* the human-readable name the EasyWeek API returns for that branch;
* the Meta template prefix (``du`` / ``ra``) — and therefore the template names;
* the storefront content (brand line, address, maps link) in the message footer.

Anything the operator supplies is checked *against* a profile; nothing is
derived *from* operator input. In particular the seed no longer picks content by
the ``meta_template_prefix`` an operator typed, and no longer merely prints the
API name for a human to eyeball.

Deliberately NOT here: numeric location ids and UUIDs. §10 established that
those are not stable — a location present in early captures no longer exists —
so they stay in ``EASYWEEK_LOCATION_MAP`` where an operator can correct them
without a code change. What lives in source is only what an operator must not be
able to contradict.

This is a production module, imported by both the CLI seed and the outbox
worker. It imports nothing from either, so there is no cycle and the worker
never pulls in a CLI entry point.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Final

# Codes seeded per branch. Kept here so the runtime guard can recognise a
# well-formed EasyWeek template name without importing the seed CLI.
RECORD_CREATED: Final = "record_created"
RECORD_CREATED_NEW_CLIENT: Final = "record_created_new_client"
RECORD_UPDATED: Final = "record_updated"
RECORD_CANCELED: Final = "record_canceled"

BRANCH_TEMPLATE_CODES: Final = (
    RECORD_CREATED,
    RECORD_CREATED_NEW_CLIENT,
    RECORD_UPDATED,
    RECORD_CANCELED,
)

_WHITESPACE_RE = re.compile(r"\s+")


@dataclass(frozen=True)
class BranchContent:
    """Public storefront content that appears in the message footer."""

    brand_line: str
    address_line: str
    contact_phone: str
    maps_line: str
    instagram_line: str


@dataclass(frozen=True)
class BranchProfile:
    """The indivisible identity of one branch.

    ``api_name`` is what ``GET /locations`` must report for the UUID configured
    under this slug. It is the independent third party in the check: the
    registry says "this UUID is Durlach", and only EasyWeek itself can confirm
    that the UUID really is the branch whose content we are about to seed.
    """

    slug: str
    api_name: str
    meta_template_prefix: str
    content: BranchContent


# Public storefront content only. Numeric EasyWeek ids and UUIDs deliberately
# never live in source; the registry is the sole source for those identities.
BRANCH_PROFILES: Final[dict[str, BranchProfile]] = {
    "durlach": BranchProfile(
        slug="durlach",
        api_name="KitiLash Durlach",
        meta_template_prefix="du",
        content=BranchContent(
            brand_line="*KitiLash Durlach*",
            address_line="Pfinztalstraße 4, 76227 Karlsruhe-Durlach",
            contact_phone="+491742310386",
            maps_line="📍https://maps.app.goo.gl/HnVPnHaJHf2DW3Nn8",
            instagram_line="📺 https://www.instagram.com/kitilash001",
        ),
    ),
    "rastatt": BranchProfile(
        slug="rastatt",
        api_name="KitiLash Rastatt",
        meta_template_prefix="ra",
        content=BranchContent(
            brand_line="*KitiLash Rastatt*",
            address_line="76437 Rastatt, Rathausstraße 5",
            contact_phone="+491742310386",
            maps_line="📍https://maps.app.goo.gl/xvYYbJbPaWcnp9Xv5",
            instagram_line="📺 https://www.instagram.com/kitilash001",
        ),
    ),
}


def normalize_api_name(value: object) -> str:
    """Fold an API branch name to a comparable form.

    Only differences that are certainly insignificant are folded away: leading
    and trailing space, runs of internal whitespace, and letter case. Anything
    else — a different word, an extra token, a different branch entirely — still
    compares unequal, which is the whole point of the check.
    """
    if not isinstance(value, str):
        return ""
    return _WHITESPACE_RE.sub(" ", value).strip().casefold()


def branch_profile_for_slug(slug: object) -> BranchProfile | None:
    """The profile registered under a registry top-level key, if it is known."""
    if not isinstance(slug, str):
        return None
    return BRANCH_PROFILES.get(slug.strip().casefold())


def meta_template_name(prefix: str, code: str) -> str:
    """Approved WABA template name for one branch and lifecycle code."""
    return f"kitilash_{prefix}_{code}_v1"


def branch_template_name_error(
    *,
    profile: BranchProfile,
    template_code: str,
    resolved_name: object,
) -> str | None:
    """Fail-closed check that a DB template row belongs to this branch.

    DB-first resolution is preserved: the name still comes from
    ``message_templates.meta_template_name``. This only proves the row that was
    selected is the row this branch may send — a ``du_*`` name resolved for a
    Rastatt job means the seed or the registry is crossed, and sending it would
    put Durlach's address in front of a Rastatt customer.

    ``record_created_new_client`` is validated as its own selected code, because
    it is a genuinely different approved template, not a variant of
    ``record_created``.

    The message names the branch and the code only — never the message body.
    """
    if not isinstance(resolved_name, str) or not resolved_name.strip():
        return f"EasyWeek template has no meta_template_name for branch {profile.slug} code {template_code}"

    name = resolved_name.strip()

    expected = meta_template_name(profile.meta_template_prefix, template_code)
    if name != expected:
        return (
            f"EasyWeek template does not belong to the selected code and branch {profile.slug}: "
            f"expected {expected} for code {template_code}, resolved {name}"
        )
    return None
