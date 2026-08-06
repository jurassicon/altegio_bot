"""Tests: clone_meta_templates_for_location CLI script.

The script submits templates to Meta, where a mistake costs a new template
version and another multi-week approval round. Everything below drives the pure
classification/planning functions plus ``main()`` against a fake Meta client, so
no network call is made.

Covers:
1.  real Karlsruhe footer (taken from seed_templates) -> address and map link
    rewritten, component order preserved, brand line reported but untouched;
2.  address spelled differently while the Karlsruhe map link is present -> NOT
    location-neutral but an explicit error;
3.  branch-specific template whose map link was not replaced -> BLOCKED, never
    prepared for submission;
4.  genuinely location-neutral template (no address, no map) -> skipped quietly;
5.  an expected branch-specific template missing from the sources -> non-zero
    exit code and nothing submitted;
6.  target template already exists -> skipped, no second submission;
7.  ``_normalize_api_version`` on valid and garbage values;
8.  the confirmation word follows --target-location.
"""

from __future__ import annotations

import sys
from typing import Any

import pytest

from altegio_bot.scripts import clone_meta_templates_for_location as cloner
from altegio_bot.scripts.clone_meta_templates_for_location import (
    DEFAULT_TARGET_ADDRESS,
    DEFAULT_TARGET_MAPS_URL,
    ReplacementStats,
    ScriptError,
    TemplateStatus,
    _confirmation_word,
    _normalize_api_version,
    build_plan,
    expected_branch_templates,
    plan_blockers,
    prepare_template,
    select_sources,
)
from altegio_bot.scripts.seed_templates import COMPANIES, _footer

_KA_COMPANY = 758285
_RA_COMPANY = 1271200
_KA_CFG = COMPANIES[_KA_COMPANY]
_RA_CFG = COMPANIES[_RA_COMPANY]

# The real Karlsruhe footer. Importing it (instead of copying the strings) makes
# these tests fail the moment seed_templates changes the address or map line
# without SOURCE_ADDRESS_PATTERNS / SOURCE_MAP_URLS following along.
_KA_FOOTER = _footer(_KA_CFG)

_SOURCE_PREFIX = "kitilash_ka_"
_TARGET_PREFIX = "kitilash_du_"
_LANGUAGE = "de"

_BODY = "*{{1}}, hallo! Ihre Terminbuchung wurde bestätigt:*\n\n*Datum:* {{2}}"

_PREPARE_KWARGS: dict[str, Any] = {
    "source_prefix": _SOURCE_PREFIX,
    "target_prefix": _TARGET_PREFIX,
    "address": DEFAULT_TARGET_ADDRESS,
    "maps_url": DEFAULT_TARGET_MAPS_URL,
}


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _template(name: str, *, body: str, language: str = _LANGUAGE) -> dict[str, Any]:
    """A Meta ``GET /message_templates`` entry, trimmed to what the script reads."""
    return {
        "id": f"id-{name}",
        "name": name,
        "status": "APPROVED",
        "language": language,
        "category": "UTILITY",
        "components": [
            {"type": "BODY", "text": body},
            {"type": "BUTTONS", "buttons": [{"type": "URL", "text": "Termin", "url": "https://n813709.alteg.io/"}]},
        ],
    }


def _body_text(payload: dict[str, Any]) -> str:
    return next(component["text"] for component in payload["components"] if component["type"] == "BODY")


def _ka_sources(*, exclude: str | None = None) -> list[dict[str, Any]]:
    """One APPROVED source per expected branch-specific Karlsruhe template."""
    return [
        _template(name, body=_BODY + _KA_FOOTER)
        for name in sorted(expected_branch_templates(source_prefix=_SOURCE_PREFIX))
        if name != exclude
    ]


class _FakeMetaClient:
    """Stand-in for MetaTemplateClient: no sockets, records what would be sent."""

    def __init__(self, templates: list[dict[str, Any]]) -> None:
        self._templates = templates
        self.created: list[dict[str, Any]] = []

    async def __aenter__(self) -> "_FakeMetaClient":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def list_templates(self) -> list[dict[str, Any]]:
        return list(self._templates)

    async def create_template(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.created.append(payload)
        return {"id": f"new-{payload['name']}", "status": "PENDING", "category": payload.get("category")}


def _run_script(
    monkeypatch: pytest.MonkeyPatch,
    templates: list[dict[str, Any]],
    argv: list[str],
) -> tuple[int, _FakeMetaClient]:
    fake = _FakeMetaClient(templates)
    monkeypatch.setattr(cloner, "MetaTemplateClient", lambda **_kwargs: fake)
    monkeypatch.setenv("WHATSAPP_ACCESS_TOKEN", "test-token")
    monkeypatch.setenv("META_WABA_ID", "test-waba")
    monkeypatch.setattr(sys, "argv", ["clone_meta_templates_for_location.py", *argv])
    return cloner.main(), fake


# ---------------------------------------------------------------------------
# 1. happy path on the real Karlsruhe footer
# ---------------------------------------------------------------------------


def test_real_karlsruhe_footer_is_rewritten_for_durlach() -> None:
    source = _template("kitilash_ka_record_created_v1", body=_BODY + _KA_FOOTER)

    outcome = prepare_template(source, **_PREPARE_KWARGS)

    assert outcome.status is TemplateStatus.READY
    assert outcome.replacements == ReplacementStats(address=1, maps_url=1)
    assert outcome.target_name == "kitilash_du_record_created_v1"

    text = _body_text(outcome.payload)
    assert DEFAULT_TARGET_ADDRESS in text
    assert DEFAULT_TARGET_MAPS_URL in text
    assert _KA_CFG.address_line not in text
    assert _KA_CFG.maps_line not in text


def test_shared_footer_lines_are_not_touched() -> None:
    """Phone and Instagram are one WABA-wide pair: cloning must leave them alone."""
    source = _template("kitilash_ka_reminder_24h_v1", body=_BODY + _KA_FOOTER)

    text = _body_text(prepare_template(source, **_PREPARE_KWARGS).payload)

    assert _KA_CFG.phone_line in text
    assert _KA_CFG.instagram_line in text


def test_component_order_is_preserved() -> None:
    """LIFECYCLE_PARAM_FIELDS is positional: reordering components breaks sends."""
    source = _template("kitilash_ka_record_created_v1", body=_BODY + _KA_FOOTER)

    outcome = prepare_template(source, **_PREPARE_KWARGS)

    assert [c["type"] for c in outcome.payload["components"]] == [c["type"] for c in source["components"]]


def test_brand_line_is_reported_but_not_replaced() -> None:
    """Durlach inherits Karlsruhe's brand line; the operator has to see that."""
    source = _template("kitilash_ka_record_created_v1", body=_BODY + _KA_FOOTER)

    outcome = prepare_template(source, **_PREPARE_KWARGS)

    assert outcome.brand_line == _KA_CFG.brand_line
    assert _KA_CFG.brand_line in _body_text(outcome.payload)


def test_default_maps_url_is_not_another_branch_link() -> None:
    """The Durlach map link, verified by the operator, must not drift back to KA/RA."""
    assert DEFAULT_TARGET_MAPS_URL == "https://maps.app.goo.gl/HnVPnHaJHf2DW3Nn8"
    assert DEFAULT_TARGET_MAPS_URL not in _KA_CFG.maps_line
    assert DEFAULT_TARGET_MAPS_URL not in _RA_CFG.maps_line


# ---------------------------------------------------------------------------
# 2. unrecognised address spelling is an error, not neutrality
# ---------------------------------------------------------------------------


def test_unrecognised_address_with_karlsruhe_map_is_an_error() -> None:
    odd_footer = _KA_FOOTER.replace(_KA_CFG.address_line, "76133 Karlsruhe, Kaiserstr. 68")
    source = _template("kitilash_ka_record_created_v1", body=_BODY + odd_footer)

    outcome = prepare_template(source, **_PREPARE_KWARGS)

    assert outcome.replacements == ReplacementStats(address=0, maps_url=1)
    assert outcome.status is TemplateStatus.ADDRESS_UNRECOGNIZED


def test_unrecognised_address_blocks_the_run_and_is_not_prepared() -> None:
    odd_footer = _KA_FOOTER.replace(_KA_CFG.address_line, "76133 Karlsruhe, Kaiserstr. 68")
    sources = _ka_sources(exclude="kitilash_ka_record_created_v1")
    sources.append(_template("kitilash_ka_record_created_v1", body=_BODY + odd_footer))

    plan = build_plan(
        sources,
        source_prefix=_SOURCE_PREFIX,
        target_prefix=_TARGET_PREFIX,
        address=DEFAULT_TARGET_ADDRESS,
        maps_url=DEFAULT_TARGET_MAPS_URL,
        language=_LANGUAGE,
        existing=(),
        expected=expected_branch_templates(source_prefix=_SOURCE_PREFIX),
    )

    assert [item.source_name for item in plan.unrecognized] == ["kitilash_ka_record_created_v1"]
    assert "kitilash_ka_record_created_v1" not in plan.neutral
    assert "kitilash_ka_record_created_v1" not in [item.source_name for item in plan.prepared]
    blockers = plan_blockers(plan)
    assert len(blockers) == 1
    assert "kitilash_ka_record_created_v1" in blockers[0]


# ---------------------------------------------------------------------------
# 3. a branch template without a replaced map link is blocked
# ---------------------------------------------------------------------------


def test_missing_map_replacement_blocks_the_template() -> None:
    no_map_footer = _KA_FOOTER.replace(_KA_CFG.maps_line, "📍https://maps.app.goo.gl/UnknownKarlsruheLink")
    source = _template("kitilash_ka_reminder_2h_v1", body=_BODY + no_map_footer)

    outcome = prepare_template(source, **_PREPARE_KWARGS)

    assert outcome.replacements == ReplacementStats(address=1, maps_url=0)
    assert outcome.status is TemplateStatus.BLOCKED_NO_MAPS


def test_blocked_template_is_never_submitted(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    no_map_footer = _KA_FOOTER.replace(_KA_CFG.maps_line, "📍https://maps.app.goo.gl/UnknownKarlsruheLink")
    templates = _ka_sources(exclude="kitilash_ka_reminder_2h_v1")
    templates.append(_template("kitilash_ka_reminder_2h_v1", body=_BODY + no_map_footer))

    code, fake = _run_script(monkeypatch, templates, ["--apply", "--yes"])

    assert code == 1
    assert fake.created == []
    output = capsys.readouterr()
    assert "BLOCKED kitilash_ka_reminder_2h_v1" in output.out
    assert "kitilash_ka_reminder_2h_v1" in output.err


# ---------------------------------------------------------------------------
# 4. genuinely location-neutral templates stay skipped
# ---------------------------------------------------------------------------


def test_template_without_address_and_map_is_neutral() -> None:
    source = _template("kitilash_ka_promo_card_booking_reminder_v1", body="*Ihr Rabatt:* {{1}}\n{{2}}")

    outcome = prepare_template(source, **_PREPARE_KWARGS)

    assert outcome.status is TemplateStatus.NEUTRAL
    assert outcome.replacements == ReplacementStats(address=0, maps_url=0)
    assert outcome.brand_line is None


def test_neutral_template_is_skipped_without_blocking_the_run() -> None:
    sources = [
        *_ka_sources(),
        _template("kitilash_ka_promo_card_booking_reminder_v1", body="*Ihr Rabatt:* {{1}}\n{{2}}"),
    ]

    plan = build_plan(
        sources,
        source_prefix=_SOURCE_PREFIX,
        target_prefix=_TARGET_PREFIX,
        address=DEFAULT_TARGET_ADDRESS,
        maps_url=DEFAULT_TARGET_MAPS_URL,
        language=_LANGUAGE,
        existing=(),
        expected=expected_branch_templates(source_prefix=_SOURCE_PREFIX),
    )

    assert plan.neutral == ("kitilash_ka_promo_card_booking_reminder_v1",)
    assert plan.neutral_expected == ()
    assert plan_blockers(plan) == []
    assert len(plan.prepared) == len(expected_branch_templates(source_prefix=_SOURCE_PREFIX))


def test_expected_branch_template_classified_as_neutral_blocks_the_run() -> None:
    """A branch template stripped of both address and map is still not neutral."""
    sources = _ka_sources(exclude="kitilash_ka_record_canceled_v1")
    sources.append(_template("kitilash_ka_record_canceled_v1", body="*{{1}}, hallo!*\nIhr Termin wurde storniert."))

    plan = build_plan(
        sources,
        source_prefix=_SOURCE_PREFIX,
        target_prefix=_TARGET_PREFIX,
        address=DEFAULT_TARGET_ADDRESS,
        maps_url=DEFAULT_TARGET_MAPS_URL,
        language=_LANGUAGE,
        existing=(),
        expected=expected_branch_templates(source_prefix=_SOURCE_PREFIX),
    )

    assert plan.neutral_expected == ("kitilash_ka_record_canceled_v1",)
    assert any("kitilash_ka_record_canceled_v1" in blocker for blocker in plan_blockers(plan))


# ---------------------------------------------------------------------------
# 5. expected branch coverage
# ---------------------------------------------------------------------------


def test_expected_branch_templates_are_derived_from_meta_templates() -> None:
    expected = expected_branch_templates(source_prefix=_SOURCE_PREFIX)

    assert expected == frozenset(
        {
            "kitilash_ka_record_created_v1",
            "kitilash_ka_record_created_new_client_v1",
            "kitilash_ka_record_updated_v1",
            "kitilash_ka_record_canceled_v1",
            "kitilash_ka_reminder_24h_v1",
            "kitilash_ka_reminder_2h_v1",
        }
    )
    assert all(name.startswith(_SOURCE_PREFIX) for name in expected)
    # Universal templates must never be demanded for the new location.
    assert "kitilash_ka_review_3d_v1" not in expected
    assert "kitilash_ka_newsletter_new_clients_monthly_v1" not in expected


def test_missing_expected_template_exits_non_zero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    templates = _ka_sources(exclude="kitilash_ka_record_created_new_client_v1")

    code, fake = _run_script(monkeypatch, templates, ["--apply", "--yes"])

    assert code == 1
    assert fake.created == []
    output = capsys.readouterr()
    assert "kitilash_ka_record_created_new_client_v1" in output.out
    assert "kitilash_ka_record_created_new_client_v1" in output.err


def test_pending_source_is_reported_as_missing() -> None:
    """Only APPROVED sources count; a pending one leaves the new location uncovered."""
    templates = _ka_sources()
    templates[0]["status"] = "PENDING"
    missing_name = templates[0]["name"]

    sources = select_sources(templates, source_prefix=_SOURCE_PREFIX, language=_LANGUAGE)
    plan = build_plan(
        sources,
        source_prefix=_SOURCE_PREFIX,
        target_prefix=_TARGET_PREFIX,
        address=DEFAULT_TARGET_ADDRESS,
        maps_url=DEFAULT_TARGET_MAPS_URL,
        language=_LANGUAGE,
        existing=(),
        expected=expected_branch_templates(source_prefix=_SOURCE_PREFIX),
    )

    assert plan.missing_expected == (missing_name,)
    assert any(missing_name in blocker for blocker in plan_blockers(plan))


# ---------------------------------------------------------------------------
# 6. existing targets
# ---------------------------------------------------------------------------


def test_existing_target_is_skipped_without_resubmitting(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    sources = _ka_sources()
    already_cloned = [
        _template(name.replace(_SOURCE_PREFIX, _TARGET_PREFIX), body=_BODY + _KA_FOOTER)
        for name in sorted(expected_branch_templates(source_prefix=_SOURCE_PREFIX))
    ]

    code, fake = _run_script(monkeypatch, [*sources, *already_cloned], ["--apply", "--yes"])

    assert code == 0
    assert fake.created == []
    output = capsys.readouterr().out
    assert "SKIP    target already exists: kitilash_du_record_created_v1" in output
    assert "Nothing to submit." in output


def test_only_the_missing_target_is_submitted(monkeypatch: pytest.MonkeyPatch) -> None:
    sources = _ka_sources()
    already_cloned = [
        _template(name.replace(_SOURCE_PREFIX, _TARGET_PREFIX), body=_BODY + _KA_FOOTER)
        for name in sorted(expected_branch_templates(source_prefix=_SOURCE_PREFIX))
        if name != "kitilash_ka_reminder_24h_v1"
    ]

    code, fake = _run_script(monkeypatch, [*sources, *already_cloned], ["--apply", "--yes"])

    assert code == 0
    assert [payload["name"] for payload in fake.created] == ["kitilash_du_reminder_24h_v1"]
    assert DEFAULT_TARGET_ADDRESS in _body_text(fake.created[0])


def test_dry_run_submits_nothing(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    code, fake = _run_script(monkeypatch, _ka_sources(), [])

    assert code == 0
    assert fake.created == []
    output = capsys.readouterr().out
    assert "Mode: DRY-RUN" in output
    assert "Nothing submitted. Re-run with --apply after reviewing this plan." in output
    assert f"brand line (kept as is): {_KA_CFG.brand_line}" in output


# ---------------------------------------------------------------------------
# 7. _normalize_api_version
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("v25.0", "v25.0"),
        ("V25.0", "v25.0"),
        ("25.0", "v25.0"),
        ("  v23.0  ", "v23.0"),
        ("v9.5", "v9.5"),
    ],
)
def test_normalize_api_version_accepts_valid_values(raw: str, expected: str) -> None:
    assert _normalize_api_version(raw) == expected


@pytest.mark.parametrize("raw", ["", "   ", "v25", "25", "v25.0.1", "latest", "v-1.0", "vv25.0", "v25,0"])
def test_normalize_api_version_rejects_garbage(raw: str) -> None:
    with pytest.raises(ScriptError):
        _normalize_api_version(raw)


# ---------------------------------------------------------------------------
# 8. confirmation word
# ---------------------------------------------------------------------------


def test_confirmation_word_follows_the_target_location() -> None:
    assert _confirmation_word("du") == "DU"
    assert _confirmation_word("ra") == "RA"
