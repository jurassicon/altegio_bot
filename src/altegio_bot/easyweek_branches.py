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
# PR-8. Seeded per branch exactly like the lifecycle codes, so a reminder is
# bound to one branch's Meta name, body and footer by the same contract.
REMINDER_24H: Final = "reminder_24h"
REMINDER_2H: Final = "reminder_2h"

BRANCH_TEMPLATE_CODES: Final = (
    RECORD_CREATED,
    RECORD_CREATED_NEW_CLIENT,
    RECORD_UPDATED,
    RECORD_CANCELED,
    REMINDER_24H,
    REMINDER_2H,
)

PRE_APPOINTMENT_NOTES_DE: Final = (
    "\n\nWichtige Hinweise vor dem Termin:\n"
    "• Bitte pünktlich kommen — ab 15 Min. Verspätung können wir "
    "nicht garantieren, dass der Termin stattfindet.\n"
    "• Wimpern bitte sauber: ohne Mascara, ohne geklebte Wimpern.\n"
    "• Falls Sie schon eine Kundenkarte haben, bitte mitbringen.\n"
    "• Auffüllen: ab 3. Woche 60 €, ab 4. Woche 70 €, ab 5. Woche "
    "keine Auffüllung (Neuauflage).\n"
    "• Zahlung: bar oder mit Karte.\n"
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


@dataclass(frozen=True)
class BranchTemplateContract:
    """Canonical source-owned row expected for one branch and selected code."""

    profile: BranchProfile
    template_code: str
    meta_template_name: str
    raw_body: str


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


def _footer(content: BranchContent) -> str:
    return (
        f"\n\n{content.brand_line}\n"
        f"{content.address_line}\n"
        f"☎ {content.contact_phone}\n\n"
        f"{content.maps_line}\n"
        f"{content.instagram_line}"
    )


def branch_template_contract(profile: BranchProfile, template_code: str) -> BranchTemplateContract | None:
    """Bind a trusted profile to its approved Meta name and canonical DB body."""
    if template_code not in BRANCH_TEMPLATE_CODES:
        return None

    footer = _footer(profile.content)
    created = (
        "*{client_name}, hallo! Ihre Terminbuchung wurde bestätigt:*\n\n"
        "*Mitarbeiterin:* {staff_name}\n"
        "*Datum:* {date}\n"
        "*Zeit:* {time}\n"
        "*Service:*\n"
        "{services}\n"
        "*Summe:* {total_cost}€\n\n"
        "Termin verwalten: {booking_link}"
    )
    bodies = {
        RECORD_CREATED: f"{created}{footer}",
        RECORD_CREATED_NEW_CLIENT: f"{created}{PRE_APPOINTMENT_NOTES_DE}{footer}",
        RECORD_UPDATED: (
            "*{client_name}, hallo! Ihr Termin wurde geändert:*\n\n"
            "*Mitarbeiterin:* {staff_name}\n"
            "*Neues Datum:* {date}\n"
            "*Neue Zeit:* {time}\n"
            "*Service:*\n"
            "{services}\n"
            "*Summe:* {total_cost}€\n\n"
            f"Termin verwalten: {{booking_link}}{footer}"
        ),
        RECORD_CANCELED: (
            "*{client_name}, hallo!*\n\n"
            "Ihr Termin am {date} um {time} Uhr wurde storniert.\n"
            "{services}\n\n"
            f"Neuen Termin buchen: {{booking_link}}{footer}"
        ),
        # Six positional parameters, in the order the approved Meta reminder
        # templates use: client_name, staff_name, date, time, services, link.
        REMINDER_24H: (
            "*{client_name}, hallo! Wir erinnern an Ihren Termin morgen:*\n\n"
            "*Mitarbeiterin:* {staff_name}\n"
            "*Datum:* {date}\n"
            "*Zeit:* {time}\n"
            "*Service:*\n"
            "{services}\n\n"
            f"Termin verwalten: {{booking_link}}{footer}"
        ),
        REMINDER_2H: (
            "*{client_name}, hallo! Ihr Termin ist in 2 Stunden:*\n\n"
            "*Mitarbeiterin:* {staff_name}\n"
            "*Datum:* {date}\n"
            "*Zeit:* {time}\n"
            "*Service:*\n"
            "{services}\n\n"
            f"Termin verwalten: {{booking_link}}{footer}"
        ),
    }
    return BranchTemplateContract(
        profile=profile,
        template_code=template_code,
        meta_template_name=meta_template_name(profile.meta_template_prefix, template_code),
        raw_body=bodies[template_code],
    )


def branch_template_contract_error(
    *,
    profile: BranchProfile,
    template_code: str,
    resolved_name: object,
    resolved_body: object,
) -> str | None:
    """Fail closed unless a DB row exactly matches its source-owned contract.

    DB-first resolution is preserved: the name still comes from
    ``message_templates.meta_template_name`` and the raw body still comes from
    ``message_templates.body``. The source contract proves that both values
    belong to the selected branch and code before either can reach a provider.

    ``record_created_new_client`` is validated as its own selected code, because
    it is a genuinely different approved template, not a variant of
    ``record_created``.

    Errors name only the branch, code and failed field. They never echo either
    DB value, so a corrupted body cannot leak customer data into logs.
    """
    contract = branch_template_contract(profile, template_code)
    if contract is None:
        return f"EasyWeek template contract violation: branch={profile.slug} code={template_code} unsupported code"
    if not isinstance(resolved_name, str) or not resolved_name.strip():
        return (
            f"EasyWeek template contract violation: branch={profile.slug} "
            f"code={template_code} meta_template_name missing"
        )
    if resolved_name.strip() != contract.meta_template_name:
        return (
            f"EasyWeek template does not belong to the selected code and branch {profile.slug}: "
            f"code={template_code} meta_template_name mismatch"
        )
    if not isinstance(resolved_body, str) or resolved_body != contract.raw_body:
        return (
            f"EasyWeek template does not belong to the selected code and branch {profile.slug}: "
            f"code={template_code} body mismatch"
        )
    return None
