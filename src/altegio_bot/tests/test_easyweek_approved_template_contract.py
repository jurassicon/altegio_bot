"""The three EasyWeek marketing bodies must equal the APPROVED Meta content.

A read-only production audit compared the approved Meta content with this
codebase's contract and found all seven marketing templates disagreeing. The
runtime guard is a different comparison — the selected database row against the
same contract, never against Meta — so rows written by the older code passed it
while still differing from Meta. These tests pin the corrected contract against
fixtures transcribed independently of the module they check.

Two properties carry the file:

* **Neutral text, branch-owned name.** The approved bodies name no branch, so
  all three branches legitimately share the text — while each still resolves its
  own ``kitilash_<prefix>_<code>_v1`` and its own row. Sharing a body is allowed;
  borrowing a name or a row is not.
* **Only an unambiguous conversion.** ``{{1}}`` ↔ ``{client_name}`` is decided by
  the code's fixed parameter order, never by matching text, and never by
  normalising whitespace until two strings agree.
"""

from __future__ import annotations

import re

import pytest

from altegio_bot.easyweek_branches import (
    APPROVED_COMEBACK_3D_BODY,
    APPROVED_REPEAT_10D_BODY,
    APPROVED_REVIEW_3D_BODY,
    BRANCH_PROFILES,
    BRANCH_TEMPLATE_CODES,
    branch_template_contract,
    branch_template_contract_error,
    meta_positional_body,
)
from altegio_bot.meta_templates import LIFECYCLE_PARAM_FIELDS
from altegio_bot.template_validation import validate_lifecycle_template_params
from altegio_bot.tests.easyweek_approved_meta_fixtures import (
    APPROVED_META_BODIES,
    APPROVED_PARAM_ORDER,
)

MARKETING_CODES = ("review_3d", "repeat_10d", "comeback_3d")
BRANCH_SPECIFIC_CODES = tuple(code for code in BRANCH_TEMPLATE_CODES if code not in MARKETING_CODES)
PREFIX_BY_SLUG = {"durlach": "du", "karlsruhe": "ka", "rastatt": "ra"}


# ---------------------------------------------------------------------------
# The bodies themselves
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("slug", sorted(PREFIX_BY_SLUG))
@pytest.mark.parametrize("code", MARKETING_CODES)
def test_the_positional_body_equals_the_approved_meta_body(slug: str, code: str) -> None:
    """Byte for byte, for every branch — the check the runtime guard performs."""
    assert meta_positional_body(BRANCH_PROFILES[slug], code) == APPROVED_META_BODIES[code]


def test_the_approved_comeback_body_keeps_its_double_space() -> None:
    """It is in the approved text. A cleanup that collapses it breaks the match."""
    assert "bei uns,  KitiLash," in APPROVED_COMEBACK_3D_BODY
    assert "bei uns, KitiLash," not in APPROVED_COMEBACK_3D_BODY


def test_the_approved_bodies_keep_the_details_a_reviewer_would_want_to_fix() -> None:
    """German wording, the named sender and the STOP line are all approved text."""
    assert "konnten aber ihn nicht wahrnehmen" in APPROVED_COMEBACK_3D_BODY
    assert "Ich bin Julia vom Beautystudio KitiLash." in APPROVED_REPEAT_10D_BODY
    assert "Liebe Grüße, Julia" in APPROVED_REPEAT_10D_BODY
    for body in (APPROVED_REVIEW_3D_BODY, APPROVED_REPEAT_10D_BODY, APPROVED_COMEBACK_3D_BODY):
        assert body.endswith("Oder antworten Sie mit STOP, um keine Nachrichten mehr zu erhalten.\nDanke.")


@pytest.mark.parametrize("code", MARKETING_CODES)
def test_the_approved_bodies_carry_no_branch_footer(code: str) -> None:
    """Neutral is a property of the APPROVED content, not a choice made here."""
    for slug in PREFIX_BY_SLUG:
        body = branch_template_contract(BRANCH_PROFILES[slug], code).raw_body
        for branded in ("Pfinztalstraße", "Rathausstraße", "Kaiserstraße", "maps.app.goo.gl", "instagram.com"):
            assert branded not in body, f"{slug}/{code} must not carry a branch footer"


@pytest.mark.parametrize("code", MARKETING_CODES)
def test_every_branch_shares_the_neutral_body_but_not_the_name(code: str) -> None:
    """Sharing text is allowed. Sharing identity is the failure this prevents."""
    contracts = {slug: branch_template_contract(BRANCH_PROFILES[slug], code) for slug in PREFIX_BY_SLUG}

    assert len({contract.raw_body for contract in contracts.values()}) == 1, "the approved text is neutral"
    names = {slug: contract.meta_template_name for slug, contract in contracts.items()}
    assert len(set(names.values())) == len(names), "each branch keeps its own Meta name"
    for slug, name in names.items():
        assert name == f"kitilash_{PREFIX_BY_SLUG[slug]}_{code}_v1"


# ---------------------------------------------------------------------------
# The positional ↔ named conversion
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("code", MARKETING_CODES)
def test_the_conversion_changes_only_the_placeholders(code: str) -> None:
    """Same text, different placeholder notation — nothing else moves."""
    named = branch_template_contract(BRANCH_PROFILES["durlach"], code).raw_body
    positional = meta_positional_body(BRANCH_PROFILES["durlach"], code)

    fields = APPROVED_PARAM_ORDER[code]
    rebuilt = positional
    for index, name in enumerate(fields, start=1):
        rebuilt = rebuilt.replace("{{" + str(index) + "}}", "{" + name + "}")
    assert rebuilt == named, "the conversion must be reversible without touching the prose"


@pytest.mark.parametrize("code", MARKETING_CODES)
def test_the_conversion_uses_the_declared_parameter_order(code: str) -> None:
    """The mapping comes from the param contract, not from reading the text."""
    assert LIFECYCLE_PARAM_FIELDS[code] == APPROVED_PARAM_ORDER[code]

    positional = meta_positional_body(BRANCH_PROFILES["durlach"], code)
    order = [int(match) for match in re.findall(r"\{\{(\d)\}\}", positional)]
    assert order == sorted(order), "slots appear in ascending order in the approved layout"
    assert set(order) == set(range(1, len(APPROVED_PARAM_ORDER[code]) + 1))


def test_a_body_with_an_undeclared_placeholder_is_not_convertible(monkeypatch) -> None:
    """Half a conversion would render a literal brace to a customer."""
    import altegio_bot.easyweek_branches as branches

    real = branches.branch_template_contract

    def _with_extra(profile, template_code):
        contract = real(profile, template_code)
        if contract is None or template_code != "review_3d":
            return contract
        return branches.BranchTemplateContract(
            profile=contract.profile,
            template_code=contract.template_code,
            meta_template_name=contract.meta_template_name,
            raw_body=contract.raw_body + "\n{unexpected_field}",
        )

    monkeypatch.setattr(branches, "branch_template_contract", _with_extra)
    assert branches.meta_positional_body(BRANCH_PROFILES["durlach"], "review_3d") is None


def test_a_body_missing_a_declared_placeholder_is_not_convertible(monkeypatch) -> None:
    import altegio_bot.easyweek_branches as branches

    real = branches.branch_template_contract

    def _without_link(profile, template_code):
        contract = real(profile, template_code)
        if contract is None or template_code != "comeback_3d":
            return contract
        return branches.BranchTemplateContract(
            profile=contract.profile,
            template_code=contract.template_code,
            meta_template_name=contract.meta_template_name,
            raw_body=contract.raw_body.replace("{booking_link}", "https://example.invalid"),
        )

    monkeypatch.setattr(branches, "branch_template_contract", _without_link)
    assert branches.meta_positional_body(BRANCH_PROFILES["durlach"], "comeback_3d") is None


# ---------------------------------------------------------------------------
# What must NOT have changed
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("code", BRANCH_SPECIFIC_CODES)
def test_the_other_six_codes_still_carry_their_branch_footer(code: str) -> None:
    """This PR touched three codes. The other six are proven untouched here."""
    durlach = branch_template_contract(BRANCH_PROFILES["durlach"], code).raw_body
    rastatt = branch_template_contract(BRANCH_PROFILES["rastatt"], code).raw_body

    assert "*KitiLash Durlach*" in durlach and "Pfinztalstraße 4, 76227 Karlsruhe-Durlach" in durlach
    assert "*KitiLash Rastatt*" in rastatt and "76437 Rastatt, Rathausstraße 5" in rastatt
    assert durlach != rastatt, "branch-specific codes stay branch-specific"


@pytest.mark.parametrize("code", BRANCH_SPECIFIC_CODES)
def test_the_other_six_bodies_are_not_the_approved_marketing_text(code: str) -> None:
    body = branch_template_contract(BRANCH_PROFILES["durlach"], code).raw_body
    assert body not in {APPROVED_REVIEW_3D_BODY, APPROVED_REPEAT_10D_BODY, APPROVED_COMEBACK_3D_BODY}


# ---------------------------------------------------------------------------
# The runtime guard still refuses everything it refused before
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("code", MARKETING_CODES)
def test_the_contract_guard_accepts_only_the_exact_pair(code: str) -> None:
    profile = BRANCH_PROFILES["durlach"]
    contract = branch_template_contract(profile, code)

    assert (
        branch_template_contract_error(
            profile=profile,
            template_code=code,
            resolved_name=contract.meta_template_name,
            resolved_body=contract.raw_body,
        )
        is None
    )


@pytest.mark.parametrize("code", MARKETING_CODES)
def test_another_branchs_meta_name_is_still_refused(code: str) -> None:
    """Neutral text does not make a Karlsruhe template usable for Rastatt."""
    profile = BRANCH_PROFILES["rastatt"]
    contract = branch_template_contract(profile, code)
    karlsruhe = branch_template_contract(BRANCH_PROFILES["karlsruhe"], code)

    assert contract.raw_body == karlsruhe.raw_body, "the bodies really are identical"
    error = branch_template_contract_error(
        profile=profile,
        template_code=code,
        resolved_name=karlsruhe.meta_template_name,
        resolved_body=karlsruhe.raw_body,
    )
    assert error is not None and "meta_template_name mismatch" in error


@pytest.mark.parametrize("code", MARKETING_CODES)
def test_a_body_that_merely_looks_right_is_refused(code: str) -> None:
    """No approximate match, no normalisation, no "close enough"."""
    profile = BRANCH_PROFILES["durlach"]
    contract = branch_template_contract(profile, code)

    for mutation in (
        contract.raw_body.replace("Danke.", "Danke!"),
        contract.raw_body.rstrip() + " ",
        contract.raw_body.replace("KitiLash", "Kitilash"),
        contract.raw_body.replace("\n\n", "\n"),
    ):
        if mutation == contract.raw_body:  # pragma: no cover - defensive
            continue
        error = branch_template_contract_error(
            profile=profile,
            template_code=code,
            resolved_name=contract.meta_template_name,
            resolved_body=mutation,
        )
        assert error is not None and "body mismatch" in error


def test_the_comeback_double_space_is_load_bearing_for_the_guard() -> None:
    """Collapsing it is exactly the "harmless" edit that would break production."""
    profile = BRANCH_PROFILES["durlach"]
    contract = branch_template_contract(profile, "comeback_3d")
    collapsed = contract.raw_body.replace("bei uns,  KitiLash", "bei uns, KitiLash")

    assert collapsed != contract.raw_body
    assert (
        branch_template_contract_error(
            profile=profile,
            template_code="comeback_3d",
            resolved_name=contract.meta_template_name,
            resolved_body=collapsed,
        )
        is not None
    )


# ---------------------------------------------------------------------------
# What a customer would actually receive
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("code", MARKETING_CODES)
def test_the_rendered_text_matches_meta_after_the_same_substitutions(code: str) -> None:
    """The stored/mirrored text and the Meta template must agree once filled in.

    The row body is what Chatwoot mirrors and what an operator reads back; the
    Meta template is what the customer receives. Rendering both from the same
    values is the only way to prove they say the same thing.
    """
    values = {
        "client_name": "Anna",
        "review_url": "https://g.page/r/CaV0vSmrSYkdEAE/review",
        "primary_service": "Wimpernverlängerung",
        "booking_link": "https://example.invalid/book",
    }
    fields = APPROVED_PARAM_ORDER[code]

    rendered_row = branch_template_contract(BRANCH_PROFILES["durlach"], code).raw_body.format(**values)

    rendered_meta = APPROVED_META_BODIES[code]
    for index, name in enumerate(fields, start=1):
        rendered_meta = rendered_meta.replace("{{" + str(index) + "}}", values[name])

    assert rendered_row == rendered_meta


@pytest.mark.parametrize("code", MARKETING_CODES)
def test_the_param_contract_still_validates_the_declared_arity(code: str) -> None:
    """The preflight that runs before every Meta attempt is unchanged."""
    fields = APPROVED_PARAM_ORDER[code]
    good = ["value"] * len(fields)

    assert validate_lifecycle_template_params(code, good) is None
    assert validate_lifecycle_template_params(code, good[:-1]) is not None
    assert validate_lifecycle_template_params(code, [*good, "extra"]) is not None
    assert validate_lifecycle_template_params(code, ["", *good[1:]]) is not None
