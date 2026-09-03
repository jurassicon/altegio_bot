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
* the Meta template prefix (``du`` / ``ka`` / ``ra``) — and therefore the names;
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
# PR-9: the review request earned by a proven booking-succeeded.
REVIEW_3D: Final = "review_3d"
# PR-12: the two retention messages. Seeded per branch exactly like every code
# above, so each is bound to one branch's Meta name, body and footer by the same
# contract — a retention message signed by the wrong salon is the failure this
# binding exists to make impossible.
REPEAT_10D: Final = "repeat_10d"
COMEBACK_3D: Final = "comeback_3d"

BRANCH_TEMPLATE_CODES: Final = (
    RECORD_CREATED,
    RECORD_CREATED_NEW_CLIENT,
    RECORD_UPDATED,
    RECORD_CANCELED,
    REMINDER_24H,
    REMINDER_2H,
    REVIEW_3D,
    REPEAT_10D,
    COMEBACK_3D,
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

# ---------------------------------------------------------------------------
# Approved Meta bodies for the three marketing codes
# ---------------------------------------------------------------------------
#
# These three are transcribed from the APPROVED Meta templates and must match
# them byte for byte. A read-only production audit (2026-09-02) found the
# opposite: all seven approved marketing templates across Durlach, Rastatt and
# Karlsruhe disagreed with what this module declared, so the runtime
# body-equality guard refused every one of them and the two review rows that did
# exist were wrong in the same way.
#
# Three rules apply to every character below, and each has cost something:
#
# 1. **Do not "improve" the German.** "konnten aber ihn nicht wahrnehmen" is what
#    Meta approved; correcting the word order would make the row stop matching
#    the template it is bound to, and a template edit needs a new Meta review.
# 2. **The double space after "uns," in comeback_3d is intentional.** It is in
#    the approved text. A formatter or a well-meaning cleanup that collapses it
#    breaks the equality check, which is exactly why it is called out here.
# 3. **No address footer.** Unlike the lifecycle and reminder codes, these three
#    are neutral: the approved bodies name no branch. That is a property of the
#    approved content, not a decision taken here.
#
# Neutral TEXT is not neutral OWNERSHIP. Each branch still resolves its own
# `kitilash_<prefix>_<code>_v1` Meta name and its own `message_templates` row;
# there is no shared row and no fallback to the Karlsruhe template for another
# branch. Two branches agreeing on the body is allowed; a branch borrowing
# another's name or row is not.
#
# Placeholders are written in the NAMED form this codebase uses everywhere. The
# positional `{{1}}`/`{{2}}`/`{{3}}` form Meta stores is derived from these by
# `meta_positional_body` below, in the one fixed order the code's param contract
# declares — never by matching text.

APPROVED_REVIEW_3D_BODY: Final = (
    "Hallo {client_name}!\n"
    "Danke für Ihren Besuch bei KitiLash.\n"
    "\n"
    "Wenn Sie kurz Zeit haben, freuen wir uns über eine Bewertung:\n"
    "{review_url}\n"
    "\n"
    "Oder antworten Sie mit STOP, um keine Nachrichten mehr zu erhalten.\n"
    "Danke."
)

APPROVED_REPEAT_10D_BODY: Final = (
    "Hallo, {client_name} 🙂\n"
    "\n"
    "Ich bin Julia vom Beautystudio KitiLash.\n"
    "Vor 10 Tagen waren Sie bei uns für: {primary_service}.\n"
    "\n"
    "Bitte beachten Sie, dass der Auffüllpreis nur bis zu 3 Wochen nach der Behandlung gilt.\n"
    "\n"
    "Wenn Sie Auffüllen planen, buchen Sie bitte rechtzeitig:\n"
    "{booking_link}\n"
    "\n"
    "Liebe Grüße, Julia\n"
    "\n"
    "Oder antworten Sie mit STOP, um keine Nachrichten mehr zu erhalten.\n"
    "Danke."
)

APPROVED_COMEBACK_3D_BODY: Final = (
    "Hallo, {client_name} 🙂\n"
    "\n"
    # Two spaces after "uns," — approved text, not a typo. See rule 2 above.
    "Sie haben einen Termin bei uns,  KitiLash, gehabt, konnten aber ihn nicht "
    "wahrnehmen. Möchten Sie einen neuen Termin vereinbaren? Wir würden uns "
    "freuen, Sie zu sehen! 😊\n"
    "\n"
    "Sie können denselben Meister auswählen und die Behandlung buchen oder "
    "etwas Neues ausprobieren.\n"
    "\n"
    "*Wir warten auf dich im KitiLash: {booking_link}*\n"
    "\n"
    "Oder antworten Sie mit STOP, um keine Nachrichten mehr zu erhalten.\n"
    "Danke."
)


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
    # PR-11.1 migrated Karlsruhe's future bookings to EasyWeek, so the branch is
    # in the production `EASYWEEK_LOCATION_MAP` and needs a source-controlled
    # profile like every other one: without it the seed, the preflight and the
    # send path all refuse the branch as unapproved.
    #
    # The storefront content is copied VERBATIM from the existing
    # source-controlled Karlsruhe values in `scripts/seed_templates.py` — the
    # same address, phone, maps link and Instagram handle Altegio has been
    # sending for this salon. Nothing here is invented: an address or a maps URL
    # guessed to look plausible is a customer sent to the wrong door.
    #
    # `brand_line` is `*KitiLash*` rather than `*KitiLash Karlsruhe*` for the
    # same reason: that is the line the salon already uses, and "improving" it
    # would be inventing branding.
    "karlsruhe": BranchProfile(
        slug="karlsruhe",
        api_name="KitiLash Karlsruhe",
        meta_template_prefix="ka",
        content=BranchContent(
            brand_line="*KitiLash*",
            address_line="76133 Karlsruhe, Kaiserstraße, 68",
            contact_phone="+491742310386",
            maps_line="📍https://goo.gl/maps/p7quWqbAqY9cusuRA",
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
        # Two positional parameters only: the name and the proven review link.
        #
        # The approved Meta body carries NO branch footer — see the approved
        # constants above. It is shared verbatim by all three branches, while the
        # Meta NAME and the `message_templates` row stay per branch.
        REVIEW_3D: APPROVED_REVIEW_3D_BODY,
        REMINDER_2H: (
            "*{client_name}, hallo! Ihr Termin ist in 2 Stunden:*\n\n"
            "*Mitarbeiterin:* {staff_name}\n"
            "*Datum:* {date}\n"
            "*Zeit:* {time}\n"
            "*Service:*\n"
            "{services}\n\n"
            f"Termin verwalten: {{booking_link}}{footer}"
        ),
        # PR-12. Three positional parameters: the name, the ONE service this
        # booking was for, and the branch's booking page.
        REPEAT_10D: APPROVED_REPEAT_10D_BODY,
        # Two positional parameters only: the name and the branch's booking page.
        COMEBACK_3D: APPROVED_COMEBACK_3D_BODY,
    }
    return BranchTemplateContract(
        profile=profile,
        template_code=template_code,
        meta_template_name=meta_template_name(profile.meta_template_prefix, template_code),
        raw_body=bodies[template_code],
    )


def meta_positional_body(profile: BranchProfile, template_code: str) -> str | None:
    """The source-owned body in Meta's positional form, or ``None``.

    Meta stores ``{{1}}`` / ``{{2}}`` / ``{{3}}``; this codebase writes
    ``{client_name}`` / ``{review_url}`` / ``{booking_link}``. Comparing the two
    needs a conversion, and the direction matters: the NAMED body is the
    contract, and it is rendered INTO positional form. Going the other way —
    parsing Meta's text and guessing which field each number is — would make the
    remote content define the contract, which is exactly what must not happen.

    The mapping is not inferred from the text. It is
    ``LIFECYCLE_PARAM_FIELDS[code]``, the same fixed order the send path uses to
    build the parameter list, so a body and the parameters that fill it cannot
    disagree about which slot is which.

    Substitution is positional and total: every declared field must appear
    exactly once, and nothing else is touched. A body whose placeholders do not
    match its declared fields returns ``None`` rather than a half-converted
    string — an unconvertible body is an unusable one, not one to approximate.
    """
    from altegio_bot.meta_templates import LIFECYCLE_PARAM_FIELDS

    contract = branch_template_contract(profile, template_code)
    if contract is None:
        return None
    fields = LIFECYCLE_PARAM_FIELDS.get(template_code)
    if not fields:
        return None

    body = contract.raw_body
    for index, field in enumerate(fields, start=1):
        token = "{" + field + "}"
        if body.count(token) != 1:
            # Declared but absent, or present twice: either way the positional
            # form would be ambiguous, and ambiguity here is a wrong parameter
            # in a customer's message.
            return None
        body = body.replace(token, "{{" + str(index) + "}}")

    if re.search(r"(?<!\{)\{(?!\{)[a-z_]+\}", body):
        # A named placeholder the param contract never declared. Converting the
        # rest and leaving this one behind would produce a body that looks
        # complete and renders a literal brace to a customer.
        return None
    return body


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
