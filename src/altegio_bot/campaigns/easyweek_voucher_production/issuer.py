"""The one approved issuer of every production voucher (§43.9).

The owner's decision, 30.09.2026: every new production voucher is sold by ONE
EasyWeek staffer — the owner of all the salons — whoever the client is, whoever
served them, and whoever happens to be logged into Ops. So the staffer is not a
per-mailing choice and not a field: it is a deployment setting, pinned here, and
the browser cannot name one at all.

Why a pin rather than "a valid UUID"
------------------------------------
``EASYWEEK_VOUCHER_PRODUCTION_MAILING_STAFFER_UUID`` already existed, and
checking only that it parses would accept *any* employee of the branch. That is
exactly the mistake this module exists to prevent: eight people work at
Karlsruhe, and seven of them are a wrong answer that passes every other check in
this phase — the branch proof, the account proof, the template proof and the
live staffer listing all stay green while the vouchers are issued by somebody
the owner did not approve.

So the configured value is canonicalised and fingerprinted, and the fingerprint
is compared against :data:`APPROVED_ISSUER_FINGERPRINT`, a constant of this
business scope. One value on earth satisfies it.

That property is what lets every report here be a BOOLEAN. "The configured
staffer is the approved one" cannot be true of two different UUIDs, so a plan
that signs the boolean has signed the identity, and an approval taken while the
configuration matched cannot be replayed once somebody changes it.

What the fingerprint is not
---------------------------
It is not a secret: it is a SHA-256 of a domain string and a UUID, printed in
the plan document and in the source. It is not authorisation — it says which
staffer, never whether an action may happen. It is not the voucher HMAC, which
binds a code to its batch and slot with a real key. And it is deliberately not a
second freely editable environment value: an expected fingerprint an operator
could set is a pin that pins nothing.

The raw UUID stays in the server's configuration and in the owner's local
evidence. It is not in this file, not in the examples, not in the fixtures, not
in a log line and not in any report.

Membership is a separate question, asked live
---------------------------------------------
Being the approved identity and still working at this branch are two different
facts. :func:`prove_issuer_membership` answers the second one against the
location-scoped staffer catalogue through the existing read-only client, with
the pagination contract §35 earned: an incomplete walk is UNKNOWN, never
"absent". It never searches by name, never takes a position in a list, and never
falls back to another staffer — it is handed one UUID and answers about that one.

It is asked once per stage plan, not once per recipient. A mailing of forty does
one catalogue walk, which is the same linear discipline the rest of the phase
keeps.
"""

from __future__ import annotations

import hashlib
import uuid as uuid_module
from dataclasses import dataclass
from typing import Any, Final, Protocol

from altegio_bot.campaigns.easyweek_voucher_production.identity import (
    ISSUER_MEMBERSHIP_AMBIGUOUS,
    ISSUER_MEMBERSHIP_INCOMPLETE,
    ISSUER_MEMBERSHIP_MISSING,
    ISSUER_NOT_APPROVED,
    STAFFER_UNCONFIGURED,
)
from altegio_bot.easyweek_client import POS_PER_PAGE, EasyWeekError
from altegio_bot.easyweek_voucher_canary.orders import walk_pages

# The domain separator of the fingerprint. Versioned, so a later phase that
# pins a different issuer cannot be satisfied by this one's digest.
ISSUER_FINGERPRINT_DOMAIN: Final = "easyweek-voucher-production-issuer-v1:"

# SHA-256 of ISSUER_FINGERPRINT_DOMAIN + str(UUID(configured_value)) for the
# staffer the owner approved on 30.09.2026 and identified on 01.10.2026 from a
# complete location-scoped catalogue read of Karlsruhe.
#
# A constant of this business scope, not configuration. Changing the approved
# issuer is a decision the owner takes and a line somebody changes here, in a
# diff a reviewer reads — not an environment variable a deploy can move.
APPROVED_ISSUER_FINGERPRINT: Final = "ecf2ec1770e2fc7c7d1665c9f9e9749f1ae08f1a027f98295cc3591b2cad6935"

# What the UI says. A name for a human to recognise, never the thing runtime
# matches on: nothing in this phase resolves a staffer by name, by position in a
# listing or by a phone number.
APPROVED_ISSUER_DISPLAY_NAME: Final = "Юлия Мюллер"

# The same name in the genitive, for the one sentence §43.9 asks the interface to
# say: «Ваучеры оформляются от Юлии Мюллер». A separate constant rather than
# string surgery on the one above, because Russian declension is not something a
# template should be guessing at.
APPROVED_ISSUER_DISPLAY_NAME_GENITIVE: Final = "Юлии Мюллер"

# How many catalogue pages one membership proof will walk before calling the
# walk incomplete. Generous for a branch with eight staffers, bounded so a
# misbehaving listing cannot spin.
_MAX_STAFFER_PAGES: Final = 20


class StafferReader(Protocol):
    """The one read-only call this module makes. Nothing wider."""

    async def list_location_staffers(self, location_uuid: str, *, page: int) -> dict[str, Any]: ...


def expected_issuer_fingerprint() -> str:
    """The fingerprint the configured issuer must have.

    A function rather than a bare constant read, and the ONLY seam in this
    module. Tests pin a synthetic staffer by replacing what this answers, which
    keeps the production constant out of fixtures without introducing a bypass:
    there is no value it can return that makes the comparison pass for an
    arbitrary UUID, and no environment variable and no request can reach it.
    """
    return APPROVED_ISSUER_FINGERPRINT


def issuer_fingerprint(configured: str | None) -> str | None:
    """The fingerprint of a configured staffer value, or ``None``.

    ``None`` means the value is absent or is not a UUID — the two cases this
    phase refuses rather than guesses about. Canonicalised through
    :class:`uuid.UUID` first, so the same identity written in upper case, in
    braces or without hyphens fingerprints identically and a cosmetic rewrite of
    the server's configuration is not a drift.
    """
    text = (configured or "").strip()
    if not text:
        return None
    try:
        canonical = str(uuid_module.UUID(text))
    except ValueError:
        return None
    return hashlib.sha256((ISSUER_FINGERPRINT_DOMAIN + canonical).encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class PinnedIssuer:
    """Is the configured staffer the approved one? With the UUID, for acting."""

    # Needed to act; never printed by :meth:`as_safe_dict`.
    uuid: str | None
    reason: str | None

    @property
    def pinned(self) -> bool:
        return self.reason is None and self.uuid is not None

    def as_safe_dict(self) -> dict[str, Any]:
        """Booleans and a display name. No UUID and no fingerprint.

        The boolean is enough BECAUSE the pin admits exactly one value: there is
        no second UUID for which ``issuer_pinned`` could be true, so signing the
        boolean into a plan digest signs the identity itself.
        """
        return {
            "issuer_pinned": self.pinned,
            "issuer_display_name": APPROVED_ISSUER_DISPLAY_NAME,
            "issuer_selectable_by_operator": False,
            "issuer_reason": self.reason,
        }


def pinned_issuer(configured: str | None) -> PinnedIssuer:
    """The approved issuer, or the reason this configuration is not it."""
    if not (configured or "").strip():
        # Nothing configured at all. An empty setting is never read as "any
        # staffer will do"; it is the administrator's step that has not happened.
        return PinnedIssuer(uuid=None, reason=STAFFER_UNCONFIGURED)
    fingerprint = issuer_fingerprint(configured)
    if fingerprint is None:
        # Set, but not a UUID. Reported as "not the approved issuer" rather than
        # as "unconfigured", because the two send an administrator to different
        # places: one line is missing, versus one line is wrong.
        return PinnedIssuer(uuid=None, reason=ISSUER_NOT_APPROVED)
    if fingerprint != expected_issuer_fingerprint():
        # A valid UUID of some employee. Seven of the eight people at this
        # branch land here, and so does every drift after a deploy.
        return PinnedIssuer(uuid=None, reason=ISSUER_NOT_APPROVED)
    return PinnedIssuer(uuid=str(uuid_module.UUID((configured or "").strip())), reason=None)


@dataclass(frozen=True)
class IssuerMembership:
    """Does the approved issuer still belong to this location, provably?"""

    reason: str | None
    pagination_complete: bool = False
    pages_read: int = 0
    # How many rows of the catalogue carried this exact UUID: 0, 1, or more.
    matches: int = 0

    @property
    def proven(self) -> bool:
        return self.reason is None

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "issuer_membership_proven": self.proven,
            "issuer_membership_pagination_complete": self.pagination_complete,
            "issuer_membership_pages_read": self.pages_read,
            "issuer_membership_matches": self.matches,
            "issuer_membership_reason": self.reason,
        }


async def prove_issuer_membership(
    reader: StafferReader,
    *,
    location_uuid: str,
    issuer_uuid: str,
) -> IssuerMembership:
    """Walk the location's staffer catalogue and look for exactly this UUID.

    Three distinguishable failures, deliberately:

    ``missing``
        The walk was PROVEN complete and this UUID was not in it. The approved
        issuer no longer belongs to this branch.
    ``ambiguous``
        Two rows carried it. Nothing here resolves that by taking the first.
    ``incomplete``
        The walk could not be proven complete — missing or contradictory
        pagination metadata, a list that moved underneath it, the page ceiling,
        or the API failing. Unknown, and unknown is not "absent": treating it as
        absence would block a correct mailing, and treating it as presence would
        let an unprovable identity sell vouchers.

    No name is compared, no position in the listing is used and there is no
    fallback to another staffer. The UUID goes in and an answer about that UUID
    comes out.
    """
    try:
        canonical = str(uuid_module.UUID(issuer_uuid.strip()))
    except (AttributeError, ValueError):
        return IssuerMembership(reason=ISSUER_NOT_APPROVED)

    pages = 0

    async def fetch(page: int) -> Any:
        nonlocal pages
        pages = page
        return await reader.list_location_staffers(location_uuid, page=page)

    try:
        walk = await walk_pages(fetch, max_pages=_MAX_STAFFER_PAGES, expected_per_page=POS_PER_PAGE)
    except EasyWeekError:
        # An unreadable catalogue is an unknown membership, never a missing one.
        return IssuerMembership(reason=ISSUER_MEMBERSHIP_INCOMPLETE, pages_read=pages)

    matches = 0
    for row in walk.rows:
        if not isinstance(row, dict):
            continue
        value = row.get("uuid")
        if not isinstance(value, str):
            continue
        try:
            if str(uuid_module.UUID(value.strip())) == canonical:
                matches += 1
        except ValueError:
            continue

    if not walk.complete:
        return IssuerMembership(
            reason=ISSUER_MEMBERSHIP_INCOMPLETE,
            pagination_complete=False,
            pages_read=pages,
            matches=matches,
        )
    if matches > 1:
        return IssuerMembership(
            reason=ISSUER_MEMBERSHIP_AMBIGUOUS,
            pagination_complete=True,
            pages_read=pages,
            matches=matches,
        )
    if matches == 0:
        return IssuerMembership(
            reason=ISSUER_MEMBERSHIP_MISSING,
            pagination_complete=True,
            pages_read=pages,
            matches=0,
        )
    return IssuerMembership(reason=None, pagination_complete=True, pages_read=pages, matches=1)


__all__ = [
    "APPROVED_ISSUER_DISPLAY_NAME",
    "APPROVED_ISSUER_DISPLAY_NAME_GENITIVE",
    "APPROVED_ISSUER_FINGERPRINT",
    "ISSUER_FINGERPRINT_DOMAIN",
    "IssuerMembership",
    "PinnedIssuer",
    "StafferReader",
    "expected_issuer_fingerprint",
    "issuer_fingerprint",
    "pinned_issuer",
    "prove_issuer_membership",
]
