"""The follow-up message's OWN contract (§46.4). Never the first message's.

Why a second contract rather than a flag on the first one
---------------------------------------------------------
The original §45 message hands somebody a voucher code. It is approved, in use,
and its body participates in the durable product digest that every issued MAC
was bound under — so it is not a text to extend "just a little" for a different
purpose. A repeat message says something else entirely to somebody who may
never have seen the first one, and it must be approved on its own terms. This
module therefore declares a separate template, and
:mod:`...easyweek_voucher_production.template_contract` is not touched.

One text for all three groups, and why it can be
------------------------------------------------
§46.4 allows either one explicitly suitable neutral contract or an unambiguous
group-to-contract mapping, and forbids an implicit fallback. This is the first
option, declared explicitly in :data:`GROUP_TEMPLATE_CODES`: all three groups
map to the same code, on purpose, because the body below is written to be true
for every one of them.

The constraint that shapes it is the failure group. Those people may never have
received anything, so the text may not say "as we wrote to you", "you received"
or "you have not opened it yet". It therefore states a fact about our own
records — a voucher is on file for this person — and invites a booking. That
sentence is equally true for somebody who read the first message and for
somebody whose first message Meta refused.

What the repeat message deliberately does not carry
---------------------------------------------------
The voucher code. Not because it would be awkward to render, but because this
application does not have it: §45 stores a keyed MAC and never the code itself,
which is exactly the property that makes the ledger safe to read. A follow-up
that printed a code would therefore have to mint or look up a new one, and §46.4
forbids a new voucher, a replacement code, an extended term and any promise of a
balance this process has not verified. So the repeat message points at the
original entitlement and at the booking link, and nothing else.

Unsubscribing is part of the text
---------------------------------
The outgoing German message asks recipients to reply with Latin STOP.
Inbound consent recognises both Latin and Cyrillic commands.

Nothing here creates or changes a Meta template
-----------------------------------------------
There is no API call in this file. The template must be created and approved by
its owner in Meta's own interface, and a local row must be added for it. Until
both exist the proof below returns ``followup_template_unproven``, the preview
reports that as a missing prerequisite, and free WhatsApp text is never
substituted for an approved template.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final, Sequence

from altegio_bot.campaigns.easyweek_voucher_followup.identity import (
    FOLLOWUP_GROUPS,
    TEMPLATE_UNPROVEN,
)

__all__ = [
    "FOLLOWUP_META_TEMPLATE_NAME",
    "FOLLOWUP_TEMPLATE_ARITY",
    "FOLLOWUP_TEMPLATE_BODY",
    "FOLLOWUP_TEMPLATE_CATEGORY",
    "FOLLOWUP_TEMPLATE_CODE",
    "FOLLOWUP_TEMPLATE_FIELDS",
    "FOLLOWUP_TEMPLATE_LANGUAGE",
    "FOLLOWUP_TEMPLATE_PARAMETER_FORMAT",
    "GROUP_TEMPLATE_CODES",
    "FollowupTemplateProof",
    "body_digest",
    "db_row_blocker",
    "meta_template_blocker",
    "positional_body",
    "prove_meta_templates",
    "safe_example",
    "template_code_for_groups",
]

FOLLOWUP_TEMPLATE_CODE: Final = "new_client_voucher_followup_v1"

# Per-branch by construction, exactly as the original is: a Durlach or Rastatt
# approval must never be able to answer for the Karlsruhe line.
FOLLOWUP_META_TEMPLATE_NAME: Final = "kitilash_ka_new_client_voucher_followup_v1"
FOLLOWUP_TEMPLATE_LANGUAGE: Final = "de"
FOLLOWUP_TEMPLATE_CATEGORY: Final = "MARKETING"
FOLLOWUP_TEMPLATE_PARAMETER_FORMAT: Final = "POSITIONAL"

# The fixed parameter order, declared once. No voucher code: see the module
# docstring. Both parameters are things this application already prints
# elsewhere, and neither is a secret.
FOLLOWUP_TEMPLATE_FIELDS: Final = ("client_name", "booking_link")
FOLLOWUP_TEMPLATE_ARITY: Final = len(FOLLOWUP_TEMPLATE_FIELDS)

# The NAMED body is the contract. It is rendered into Meta's positional form
# below, never parsed back out of it: letting the remote text decide which
# number is which field would put the contract on Meta's side.
#
# Deliberately in the mailing's own language, deliberately without any claim
# about the first message, and deliberately carrying the opt-out sentence.
FOLLOWUP_TEMPLATE_BODY: Final = (
    "Hallo {client_name}, für Sie ist bei uns ein Gutschein über 10 € hinterlegt. "
    "Termin buchen: {booking_link}. \n"
    "Wenn Sie keine Nachrichten mehr von uns möchten, antworten Sie mit STOP."
)

# The explicit group mapping §46.4 asks for. All three names are present, so a
# new group cannot inherit a text by accident, and a missing one is an error
# rather than a silent default.
GROUP_TEMPLATE_CODES: Final[dict[str, str]] = {group: FOLLOWUP_TEMPLATE_CODE for group in FOLLOWUP_GROUPS}


def positional_body() -> str:
    """The same body in Meta's ``{{n}}`` form."""
    body = FOLLOWUP_TEMPLATE_BODY
    for index, name in enumerate(FOLLOWUP_TEMPLATE_FIELDS, start=1):
        placeholder = "{" + name + "}"
        if body.count(placeholder) != 1:  # pragma: no cover - guarded by a test
            raise ValueError("follow-up template body does not declare each field exactly once")
        body = body.replace(placeholder, "{{" + str(index) + "}}")
    return body


def body_digest() -> str:
    """A handle over the exact approved text, for the stored snapshot.

    The snapshot keeps the digest rather than the body: a preview is shown and
    logged, and the body is the one part of the contract that a later version
    could change without changing its name.
    """
    import hashlib

    material = "\x1f".join(
        (
            "altegio_bot/easyweek_voucher_followup/template/v1",
            FOLLOWUP_TEMPLATE_CODE,
            FOLLOWUP_META_TEMPLATE_NAME,
            FOLLOWUP_TEMPLATE_LANGUAGE,
            FOLLOWUP_TEMPLATE_CATEGORY,
            FOLLOWUP_TEMPLATE_PARAMETER_FORMAT,
            ",".join(FOLLOWUP_TEMPLATE_FIELDS),
            FOLLOWUP_TEMPLATE_BODY,
        )
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


# What an operator sees instead of a rendered message. Square brackets rather
# than braces, so nothing on the page can be mistaken for a value that was
# actually substituted, and no real name or link is involved.
_EXAMPLE_VALUES: Final[dict[str, str]] = {
    "client_name": "[Vorname]",
    "booking_link": "[Buchungslink]",
}


def safe_example() -> str:
    """The body with placeholders, never with anybody's data.

    §46.4 and the existing Ops privacy contract both require this: a preview
    shows the SHAPE of the message. A real name, a real link, a real code or a
    real number would turn a page an operator may screenshot into an export of
    personal data.
    """
    body = FOLLOWUP_TEMPLATE_BODY
    for name in FOLLOWUP_TEMPLATE_FIELDS:
        body = body.replace("{" + name + "}", _EXAMPLE_VALUES[name])
    return body


def template_code_for_groups(groups: Sequence[str]) -> str | None:
    """The one template these groups share, or ``None`` if they do not.

    ``None`` is a blocker, never a cue to pick one. With a single neutral
    contract this always answers; it exists so that adding a second, group
    specific contract later is a data change here instead of new reasoning at
    the call site.
    """
    if not groups:
        return None
    codes = {GROUP_TEMPLATE_CODES.get(group) for group in groups}
    if len(codes) != 1:
        return None
    code = codes.pop()
    return code if isinstance(code, str) else None


@dataclass(frozen=True)
class FollowupTemplateProof:
    """Whether a follow-up has an approved message it would be allowed to send."""

    proven: bool
    reason: str | None
    template_code: str = FOLLOWUP_TEMPLATE_CODE
    meta_template_name: str = FOLLOWUP_META_TEMPLATE_NAME
    language: str = FOLLOWUP_TEMPLATE_LANGUAGE
    category: str = FOLLOWUP_TEMPLATE_CATEGORY
    # True only when a live Meta read proved the approval in THIS evaluation.
    # A previous proof is not evidence: an approved template can be paused or
    # disabled between two page loads.
    meta_verified: bool = False

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "template_code": self.template_code,
            "meta_template_name": self.meta_template_name,
            "language": self.language,
            "category": self.category,
            "parameter_order": list(FOLLOWUP_TEMPLATE_FIELDS),
            "body_digest": body_digest(),
            "template_proven": self.proven,
            "meta_verified": self.meta_verified,
            "reason": self.reason,
        }


def _components(template: dict[str, Any]) -> list[dict[str, Any]]:
    raw = template.get("components")
    return [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []


def meta_template_blocker(template: dict[str, Any]) -> str | None:
    """Why this Meta template may not carry the follow-up, or ``None``.

    Everything positively required, as for the original: APPROVED, the declared
    category, a present and positional ``parameter_format``, the declared
    language, exactly one component which is the BODY, that body exactly equal
    to ours, and the placeholder arity proven from the approved text itself
    rather than assumed from our side.

    A HEADER, a FOOTER, a BUTTONS block or a named-variable template is a
    refusal and not something to tolerate: each of them would put text or an
    action in front of a customer that nobody reviewed here.
    """
    if template.get("status") != "APPROVED":
        return TEMPLATE_UNPROVEN
    if template.get("category") != FOLLOWUP_TEMPLATE_CATEGORY:
        return TEMPLATE_UNPROVEN
    if template.get("parameter_format") != FOLLOWUP_TEMPLATE_PARAMETER_FORMAT:
        return TEMPLATE_UNPROVEN
    if template.get("language") != FOLLOWUP_TEMPLATE_LANGUAGE:
        return TEMPLATE_UNPROVEN

    raw_components = template.get("components")
    if not isinstance(raw_components, list) or len(raw_components) != 1:
        return TEMPLATE_UNPROVEN
    components = _components(template)
    bodies = [item for item in components if item.get("type") == "BODY"]
    if len(components) != 1 or len(bodies) != 1:
        return TEMPLATE_UNPROVEN

    text = bodies[0].get("text")
    if not isinstance(text, str) or text != positional_body():
        return TEMPLATE_UNPROVEN
    for index in range(1, FOLLOWUP_TEMPLATE_ARITY + 1):
        if text.count("{{" + str(index) + "}}") != 1:
            return TEMPLATE_UNPROVEN
    if "{{" + str(FOLLOWUP_TEMPLATE_ARITY + 1) + "}}" in text:
        return TEMPLATE_UNPROVEN
    return None


def select_meta_templates(
    templates: Sequence[dict[str, Any]],
    *,
    name: str = FOLLOWUP_META_TEMPLATE_NAME,
    language: str = FOLLOWUP_TEMPLATE_LANGUAGE,
) -> list[dict[str, Any]]:
    """Every Meta row for this exact name and language. Exact, never a prefix."""
    return [
        item
        for item in templates
        if isinstance(item, dict) and item.get("name") == name and item.get("language") == language
    ]


def prove_meta_templates(templates: Sequence[dict[str, Any]]) -> FollowupTemplateProof:
    """Prove the approval from a live Meta listing, or say why not."""
    matches = select_meta_templates(templates)
    if len(matches) != 1:
        # Zero is nothing to send with; two is an ambiguity about which text a
        # customer would actually receive.
        return FollowupTemplateProof(proven=False, reason=TEMPLATE_UNPROVEN)
    blocker = meta_template_blocker(matches[0])
    return FollowupTemplateProof(
        proven=blocker is None,
        reason=blocker,
        meta_verified=blocker is None,
    )


def db_row_blocker(row: object, *, company_id: int) -> str | None:
    """Why the stored template row may not be used, or ``None``.

    A row that merely exists is not a contract; it is a value somebody typed.
    So it has to agree with this module exactly — provider, company, code,
    language, the Meta name and the body — and it must be active.
    """
    from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageTemplate

    if not isinstance(row, MessageTemplate):
        return TEMPLATE_UNPROVEN
    if not row.is_active:
        return TEMPLATE_UNPROVEN
    if row.provider != PROVIDER_EASYWEEK or row.company_id != company_id:
        return TEMPLATE_UNPROVEN
    if row.code != FOLLOWUP_TEMPLATE_CODE or row.language != FOLLOWUP_TEMPLATE_LANGUAGE:
        return TEMPLATE_UNPROVEN
    if (row.meta_template_name or "").strip() != FOLLOWUP_META_TEMPLATE_NAME:
        return TEMPLATE_UNPROVEN
    if row.body != FOLLOWUP_TEMPLATE_BODY:
        return TEMPLATE_UNPROVEN
    return None
