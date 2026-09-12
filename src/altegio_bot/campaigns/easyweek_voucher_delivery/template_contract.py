"""The one message this canary may send, as a source-owned contract (§36).

Why the old newsletter template cannot be reused
------------------------------------------------
``newsletter_new_clients_monthly`` promises ten percent and a Kundenkarte. This
canary gives a €15 voucher code. Sending the old template would tell a real
customer something untrue, and sending the new offer through a template approved
for the old text would be a template violation on top of it. So the voucher gets
its own code, its own Meta name and its own approval.

Proven before the voucher exists, not before the send
-----------------------------------------------------
The template check runs before CREATE, not merely before DELIVER. Issuing and
paying for a voucher we then cannot legally deliver would leave a real €15 in a
state whose only exits are a refund or an unhappy manual message — so the
question "could this be delivered at all?" is answered while the answer is still
free.

The text is a development contract, not an approval
---------------------------------------------------
The body below is what this repository will compare Meta against. It is NOT
evidence that Meta approved it, and it is not the owner's final wording: the
message text and the voucher's terms of use are approved separately, and this
constant has to be updated — and the Meta template re-approved — if that wording
changes. Nothing here creates a Meta template; that is a human action in the
Business Manager.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final, Sequence

from altegio_bot.campaigns.easyweek_voucher_delivery.identity import (
    TEMPLATE_MISMATCH,
    TEMPLATE_UNPROVEN,
    VOUCHER_TEMPLATE_CODE,
)

# The Meta template name for the Karlsruhe line. Per-branch by construction, so
# a Durlach or Rastatt approval can never answer for this one.
VOUCHER_META_TEMPLATE_NAME: Final = "kitilash_ka_new_client_voucher_v1"
VOUCHER_TEMPLATE_LANGUAGE: Final = "de"
VOUCHER_TEMPLATE_CATEGORY: Final = "MARKETING"
VOUCHER_TEMPLATE_PARAMETER_FORMAT: Final = "POSITIONAL"

# The fixed parameter order. This is the single declaration both the body
# conversion and the send path read, so a body and the parameters that fill it
# cannot disagree about which slot is which.
VOUCHER_TEMPLATE_FIELDS: Final = ("client_name", "voucher_code", "booking_link")
VOUCHER_TEMPLATE_ARITY: Final = len(VOUCHER_TEMPLATE_FIELDS)

# The named body this repository owns and stores in `message_templates.body`.
VOUCHER_TEMPLATE_BODY: Final = (
    "Hallo {client_name}!\n"
    "\n"
    "Als Dankeschön für Ihren ersten Besuch erhalten Sie einen KitiLash-Gutschein "
    "im Wert von 15 €.\n"
    "\n"
    "Ihr Gutscheincode: {voucher_code}\n"
    "\n"
    "Termin buchen:\n"
    "{booking_link}\n"
    "\n"
    "Bitte zeigen Sie den Gutscheincode bei Ihrem nächsten Besuch vor.\n"
    "\n"
    "Wenn Sie keine weiteren Nachrichten erhalten möchten, antworten Sie mit STOP."
)


def positional_body() -> str:
    """The same body in Meta's ``{{n}}`` form.

    The direction matters: the NAMED body is the contract and is rendered into
    positional form. Parsing Meta's text and guessing which number is which
    field would let the remote content define the contract, which is exactly
    what this module exists to prevent.
    """
    body = VOUCHER_TEMPLATE_BODY
    for index, field in enumerate(VOUCHER_TEMPLATE_FIELDS, start=1):
        placeholder = "{" + field + "}"
        if body.count(placeholder) != 1:  # pragma: no cover - guarded by a test
            raise ValueError("voucher template body does not declare each field exactly once")
        body = body.replace(placeholder, "{{" + str(index) + "}}")
    return body


@dataclass(frozen=True)
class TemplateProof:
    """Whether this canary has a message it is allowed to send."""

    proven: bool
    reason: str | None
    meta_template_name: str
    language: str
    # True only when a live Meta read proved the approval in THIS evaluation.
    meta_verified: bool = False

    def as_safe_dict(self) -> dict[str, Any]:
        return {
            "template_code": VOUCHER_TEMPLATE_CODE,
            "meta_template_name": self.meta_template_name,
            "language": self.language,
            "template_proven": self.proven,
            "meta_verified": self.meta_verified,
            "reason": self.reason,
        }


def _components(template: dict[str, Any]) -> list[dict[str, Any]]:
    raw = template.get("components")
    return [item for item in raw if isinstance(item, dict)] if isinstance(raw, list) else []


def meta_template_blocker(template: dict[str, Any]) -> str | None:
    """Why this Meta template may not carry the voucher, or ``None``.

    Everything is required positively. The voucher message is a marketing
    template with exactly one BODY and nothing else: a HEADER we do not render,
    a FOOTER printing text nobody reviewed here, a BUTTONS block or a
    named-variable template are all refusals rather than things to tolerate.

    Stricter than the lifecycle audit in one way on purpose: ``parameter_format``
    must be present AND positional. An absent field is treated as unproven
    rather than as "probably positional", because the parameter this canary
    fills slot two with is a bearer secret.
    """
    if str(template.get("status", "")).upper() != "APPROVED":
        return TEMPLATE_MISMATCH
    if str(template.get("category", "")).upper() != VOUCHER_TEMPLATE_CATEGORY:
        return TEMPLATE_MISMATCH
    if str(template.get("parameter_format", "")).upper() != VOUCHER_TEMPLATE_PARAMETER_FORMAT:
        return TEMPLATE_MISMATCH
    if str(template.get("language", "")) != VOUCHER_TEMPLATE_LANGUAGE:
        return TEMPLATE_MISMATCH

    components = _components(template)
    bodies = [item for item in components if str(item.get("type", "")).upper() == "BODY"]
    if len(components) != 1 or len(bodies) != 1:
        return TEMPLATE_MISMATCH

    text = bodies[0].get("text")
    if not isinstance(text, str) or text != positional_body():
        return TEMPLATE_MISMATCH

    # Arity is proven from the approved text itself, not assumed from our own
    # contract: a template whose body somehow carries a fourth placeholder would
    # render an empty slot to a customer.
    for index in range(1, VOUCHER_TEMPLATE_ARITY + 1):
        if text.count("{{" + str(index) + "}}") != 1:
            return TEMPLATE_MISMATCH
    if "{{" + str(VOUCHER_TEMPLATE_ARITY + 1) + "}}" in text:
        return TEMPLATE_MISMATCH
    return None


def select_meta_templates(
    templates: Sequence[dict[str, Any]],
    *,
    name: str = VOUCHER_META_TEMPLATE_NAME,
    language: str = VOUCHER_TEMPLATE_LANGUAGE,
) -> list[dict[str, Any]]:
    """Every Meta row for this exact name and language.

    Exact, never a prefix or a case-insensitive match: a Durlach template must
    not be able to answer for the Karlsruhe one, which is the whole reason the
    name carries a branch.
    """
    return [
        item
        for item in templates
        if isinstance(item, dict) and item.get("name") == name and item.get("language") == language
    ]


def prove_meta_templates(templates: Sequence[dict[str, Any]]) -> TemplateProof:
    """Prove the approval from a live Meta listing, or say why not."""
    matches = select_meta_templates(templates)
    if len(matches) != 1:
        # Zero is nothing to send with; two is an ambiguity about which text a
        # customer would actually receive.
        return TemplateProof(
            proven=False,
            reason=TEMPLATE_UNPROVEN,
            meta_template_name=VOUCHER_META_TEMPLATE_NAME,
            language=VOUCHER_TEMPLATE_LANGUAGE,
        )
    blocker = meta_template_blocker(matches[0])
    return TemplateProof(
        proven=blocker is None,
        reason=blocker,
        meta_template_name=VOUCHER_META_TEMPLATE_NAME,
        language=VOUCHER_TEMPLATE_LANGUAGE,
        meta_verified=blocker is None,
    )


def db_row_blocker(row: object, *, company_id: int) -> str | None:
    """Why the stored template row may not be used, or ``None``.

    The send path resolves DB-first, so the row has to agree with the contract
    exactly — provider, company, code, language, the Meta name and the body.
    A row that merely exists is not a contract; it is a value somebody typed.
    """
    from altegio_bot.models.models import PROVIDER_EASYWEEK, MessageTemplate

    if not isinstance(row, MessageTemplate):
        return TEMPLATE_UNPROVEN
    if not row.is_active:
        return TEMPLATE_UNPROVEN
    if row.provider != PROVIDER_EASYWEEK or row.company_id != company_id:
        return TEMPLATE_MISMATCH
    if row.code != VOUCHER_TEMPLATE_CODE or row.language != VOUCHER_TEMPLATE_LANGUAGE:
        return TEMPLATE_MISMATCH
    if (row.meta_template_name or "").strip() != VOUCHER_META_TEMPLATE_NAME:
        return TEMPLATE_MISMATCH
    if row.body != VOUCHER_TEMPLATE_BODY:
        return TEMPLATE_MISMATCH
    return None


__all__ = [
    "TemplateProof",
    "VOUCHER_META_TEMPLATE_NAME",
    "VOUCHER_TEMPLATE_ARITY",
    "VOUCHER_TEMPLATE_BODY",
    "VOUCHER_TEMPLATE_CATEGORY",
    "VOUCHER_TEMPLATE_FIELDS",
    "VOUCHER_TEMPLATE_LANGUAGE",
    "db_row_blocker",
    "meta_template_blocker",
    "positional_body",
    "prove_meta_templates",
    "select_meta_templates",
]
