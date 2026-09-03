"""Meta-shaped fixtures for the three approved EasyWeek marketing templates.

These bodies are transcribed INDEPENDENTLY of the production module they check.
That is the whole point: an expected value produced by the same helper as the
value under test proves only that the helper is self-consistent, and this is a
contract with an external system that has already drifted once.

So the text below is written in Meta's own positional form, character for
character, including the parts that look like mistakes:

* ``konnten aber ihn nicht wahrnehmen`` — the approved word order;
* the DOUBLE space after ``uns,`` in ``comeback_3d``;
* the emoji, the ``*`` emphasis and the trailing ``Danke.`` with no newline.

If a test using these fails, the question is which side changed — never "let us
normalise both sides until they agree".
"""

from __future__ import annotations

from typing import Any, Final

# `{{1}}` = client_name
# `{{2}}` = review_url
APPROVED_META_REVIEW_3D_BODY: Final = (
    "Hallo {{1}}!\n"
    "Danke für Ihren Besuch bei KitiLash.\n"
    "\n"
    "Wenn Sie kurz Zeit haben, freuen wir uns über eine Bewertung:\n"
    "{{2}}\n"
    "\n"
    "Oder antworten Sie mit STOP, um keine Nachrichten mehr zu erhalten.\n"
    "Danke."
)

# `{{1}}` = client_name, `{{2}}` = primary_service, `{{3}}` = booking_link
APPROVED_META_REPEAT_10D_BODY: Final = (
    "Hallo, {{1}} 🙂\n"
    "\n"
    "Ich bin Julia vom Beautystudio KitiLash.\n"
    "Vor 10 Tagen waren Sie bei uns für: {{2}}.\n"
    "\n"
    "Bitte beachten Sie, dass der Auffüllpreis nur bis zu 3 Wochen nach der Behandlung gilt.\n"
    "\n"
    "Wenn Sie Auffüllen planen, buchen Sie bitte rechtzeitig:\n"
    "{{3}}\n"
    "\n"
    "Liebe Grüße, Julia\n"
    "\n"
    "Oder antworten Sie mit STOP, um keine Nachrichten mehr zu erhalten.\n"
    "Danke."
)

# `{{1}}` = client_name, `{{2}}` = booking_link
APPROVED_META_COMEBACK_3D_BODY: Final = (
    "Hallo, {{1}} 🙂\n"
    "\n"
    "Sie haben einen Termin bei uns,  KitiLash, gehabt, konnten aber ihn nicht wahrnehmen. "
    "Möchten Sie einen neuen Termin vereinbaren? Wir würden uns freuen, Sie zu sehen! 😊\n"
    "\n"
    "Sie können denselben Meister auswählen und die Behandlung buchen oder etwas Neues ausprobieren.\n"
    "\n"
    "*Wir warten auf dich im KitiLash: {{2}}*\n"
    "\n"
    "Oder antworten Sie mit STOP, um keine Nachrichten mehr zu erhalten.\n"
    "Danke."
)

APPROVED_META_BODIES: Final[dict[str, str]] = {
    "review_3d": APPROVED_META_REVIEW_3D_BODY,
    "repeat_10d": APPROVED_META_REPEAT_10D_BODY,
    "comeback_3d": APPROVED_META_COMEBACK_3D_BODY,
}

# The parameter order each body relies on, written out here rather than imported
# so a reordered production contract cannot silently reorder the expectation too.
APPROVED_PARAM_ORDER: Final[dict[str, tuple[str, ...]]] = {
    "review_3d": ("client_name", "review_url"),
    "repeat_10d": ("client_name", "primary_service", "booking_link"),
    "comeback_3d": ("client_name", "booking_link"),
}


def meta_template(
    *,
    name: str,
    code: str,
    language: str = "de",
    status: str = "APPROVED",
    category: str = "MARKETING",
    body: str | None = None,
    components: list[dict[str, Any]] | None = None,
    parameter_format: str | None = "POSITIONAL",
) -> dict[str, Any]:
    """One Meta ``message_templates`` row, in the shape the Graph API returns."""
    template: dict[str, Any] = {
        "id": f"meta-{name}",
        "name": name,
        "language": language,
        "status": status,
        "category": category,
        "components": (
            components
            if components is not None
            else [{"type": "BODY", "text": body if body is not None else APPROVED_META_BODIES[code]}]
        ),
    }
    if parameter_format is not None:
        template["parameter_format"] = parameter_format
    return template


def approved_set(prefix: str, codes: tuple[str, ...] = ("review_3d", "repeat_10d", "comeback_3d")) -> list[dict]:
    """The approved templates one branch prefix owns."""
    return [meta_template(name=f"kitilash_{prefix}_{code}_v1", code=code) for code in codes]
