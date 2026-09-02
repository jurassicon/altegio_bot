"""Tests: clone_meta_templates_for_location CLI script.

The script submits templates to Meta, where a mistake costs a new template
version and another multi-week approval round, and where the name of a rejected
template can never be reused. Everything below drives the pure
classification/planning functions, the hardened Meta client (through respx) and
``main()`` against a fake client, so no network call is made.

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
6.  existing target: APPROVED/PENDING are skips, REJECTED/PAUSED/unknown block
    the run instead of reporting "Nothing to submit";
7.  address and map link inside buttons[].url are rewritten too;
8.  any source marker left anywhere in the finished payload blocks the WHOLE
    run before the first POST;
9.  the {{n}} placeholder signature must survive the rewrite;
10. --graph-url is pinned to graph.facebook.com and rejected before any request;
11. paging follows cursors.after against our own endpoint; a repeating cursor
    raises instead of looping;
12. non-JSON 2xx bodies raise ScriptError on both list and create;
13. a create response without an id is reported as indeterminate, never SENT;
14. --apply refuses the dry-run defaults; a wrong confirmation sends nothing;
15. an empty/garbled/source-copied --address or --maps-url is refused before the
    first GET — every other guard watches the source side and passes;
16. --apply prints the full payload BEFORE asking for the confirmation;
17. the access token never appears in any line of output;
18. _normalize_api_version on valid and garbage values.
"""

from __future__ import annotations

import itertools
import re
import sys
from copy import deepcopy
from typing import Any
from unittest.mock import Mock

import httpx
import pytest
import respx

from altegio_bot.easyweek_branches import BRANCH_PROFILES
from altegio_bot.scripts import clone_meta_templates_for_location as cloner
from altegio_bot.scripts.clone_meta_templates_for_location import (
    DEFAULT_TARGET_ADDRESS,
    DEFAULT_TARGET_MAPS_URL,
    MetaTemplateClient,
    ReplacementStats,
    ScriptError,
    TemplateStatus,
    _confirmation_word,
    _normalize_api_version,
    build_plan,
    expected_branch_templates,
    index_existing,
    normalize_graph_url,
    placeholder_signature,
    plan_blockers,
    prepare_template,
    residual_source_markers,
    select_sources,
    validate_target_address,
    validate_target_maps_url,
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
_KA_MAPS_URL = "https://goo.gl/maps/p7quWqbAqY9cusuRA"

_SOURCE_PREFIX = "kitilash_ka_"
_TARGET_PREFIX = "kitilash_du_"
_LANGUAGE = "de"
_TOKEN = "EAAsecret-access-token-do-not-print"

_BODY = "*{{1}}, hallo! Ihre Terminbuchung wurde bestätigt:*\n\n*Datum:* {{2}}"

_PREPARE_KWARGS: dict[str, Any] = {
    "source_prefix": _SOURCE_PREFIX,
    "target_prefix": _TARGET_PREFIX,
    "address": DEFAULT_TARGET_ADDRESS,
    "maps_url": DEFAULT_TARGET_MAPS_URL,
}


# --apply no longer inherits the dry-run defaults, so every applying test states
# the target it means to submit.
def _apply_args(
    *,
    address: str = DEFAULT_TARGET_ADDRESS,
    maps_url: str = DEFAULT_TARGET_MAPS_URL,
    yes: bool = True,
) -> list[str]:
    return [
        "--apply",
        *(["--yes"] if yes else []),
        "--target-location",
        "du",
        "--address",
        address,
        "--maps-url",
        maps_url,
    ]


_APPLY_ARGS = _apply_args()


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _template(
    name: str,
    *,
    body: str,
    language: str = _LANGUAGE,
    status: str = "APPROVED",
    button_url: str = "https://n813709.alteg.io/",
) -> dict[str, Any]:
    """A Meta ``GET /message_templates`` entry, trimmed to what the script reads."""
    return {
        "id": f"id-{name}",
        "name": name,
        "status": status,
        "language": language,
        "category": "UTILITY",
        "components": [
            {"type": "BODY", "text": body},
            {"type": "BUTTONS", "buttons": [{"type": "URL", "text": "Termin", "url": button_url}]},
        ],
    }


def _body_text(payload: dict[str, Any]) -> str:
    return next(component["text"] for component in payload["components"] if component["type"] == "BODY")


def _expected() -> frozenset[str]:
    return expected_branch_templates(source_prefix=_SOURCE_PREFIX)


def _ka_sources(*, exclude: str | None = None) -> list[dict[str, Any]]:
    """One APPROVED source per expected branch-specific Karlsruhe template."""
    return [_template(name, body=_BODY + _KA_FOOTER) for name in sorted(_expected()) if name != exclude]


def _du_targets(*, status: str = "APPROVED", only: str | None = None) -> list[dict[str, Any]]:
    """The already-cloned Durlach counterparts, in the given Meta status."""
    return [
        _template(name.replace(_SOURCE_PREFIX, _TARGET_PREFIX), body=_BODY + _KA_FOOTER, status=status)
        for name in sorted(_expected())
        if only is None or name == only
    ]


def _plan(sources: list[dict[str, Any]], *, existing: list[dict[str, Any]] | None = None) -> cloner.ClonePlan:
    return build_plan(
        sources,
        source_prefix=_SOURCE_PREFIX,
        target_prefix=_TARGET_PREFIX,
        address=DEFAULT_TARGET_ADDRESS,
        maps_url=DEFAULT_TARGET_MAPS_URL,
        language=_LANGUAGE,
        existing=index_existing(existing or []),
        expected=_expected(),
    )


class _FakeMetaClient:
    """Stand-in for MetaTemplateClient: no sockets, records what would be sent."""

    def __init__(self, templates: list[dict[str, Any]], *, create_result: dict[str, Any] | None = None) -> None:
        self._templates = templates
        self._create_result = create_result
        self.created: list[dict[str, Any]] = []
        self.list_calls = 0

    async def __aenter__(self) -> "_FakeMetaClient":
        return self

    async def __aexit__(self, *_exc: object) -> None:
        return None

    async def list_templates(self) -> list[dict[str, Any]]:
        self.list_calls += 1
        return list(self._templates)

    async def create_template(self, payload: dict[str, Any]) -> dict[str, Any]:
        self.created.append(payload)
        if self._create_result is not None:
            return self._create_result
        return {"id": f"new-{payload['name']}", "status": "PENDING", "category": payload.get("category")}


def _run_script(
    monkeypatch: pytest.MonkeyPatch,
    templates: list[dict[str, Any]],
    argv: list[str],
    *,
    create_result: dict[str, Any] | None = None,
) -> tuple[int, _FakeMetaClient]:
    fake = _FakeMetaClient(templates, create_result=create_result)
    monkeypatch.setattr(cloner, "MetaTemplateClient", lambda **_kwargs: fake)
    monkeypatch.setenv("WHATSAPP_ACCESS_TOKEN", _TOKEN)
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
    assert outcome.source_status == "APPROVED"

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

    plan = _plan(sources)

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

    code, fake = _run_script(monkeypatch, templates, _APPLY_ARGS)

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

    plan = _plan(sources)

    assert plan.neutral == ("kitilash_ka_promo_card_booking_reminder_v1",)
    assert plan.neutral_expected == ()
    assert plan_blockers(plan) == []
    assert len(plan.prepared) == len(_expected())


_NEUTRAL_BODIES = {
    "review_3d": "Hallo {{1}}!\nDanke für Ihren Besuch bei KitiLash.\nBewertung: {{2}}\nAntworten Sie mit STOP.",
    "comeback_3d": "Hallo, {{1}} 🙂\nMöchten Sie einen neuen Termin vereinbaren?\n{{2}}\nAntworten Sie mit STOP.",
    "repeat_10d": (
        "Hallo, {{1}} 🙂\nIch bin Julia vom Beautystudio KitiLash.\n"
        "Vor 10 Tagen waren Sie bei uns für: {{2}}.\n{{3}}\nAntworten Sie mit STOP."
    ),
}


def _neutral_sources() -> list[dict[str, Any]]:
    return [
        {
            "id": f"existing-{code}",
            "name": f"kitilash_ka_{code}_v1",
            "language": "de",
            "status": "APPROVED",
            "category": "MARKETING",
            "parameter_format": "POSITIONAL",
            "components": [
                {
                    "type": "BODY",
                    "text": body,
                    "example": {
                        "body_text": [["Test", *(["Beispiel"] if code == "repeat_10d" else []), "https://example.com"]]
                    },
                }
            ],
        }
        for code, body in _NEUTRAL_BODIES.items()
    ]


def _neutral_args(*, include_neutral: bool = True) -> list[str]:
    return [
        *(["--include-neutral"] if include_neutral else []),
        *(arg for code in _NEUTRAL_BODIES for arg in ("--template-name", f"kitilash_ka_{code}_v1")),
    ]


@pytest.mark.parametrize("source_args", [[], ["--source-location", "ka"]], ids=["default-ka", "explicit-ka"])
def test_targeted_neutral_dry_run_needs_no_other_branch_templates(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture, source_args: list[str]
) -> None:
    code, fake = _run_script(monkeypatch, _neutral_sources(), [*source_args, *_neutral_args()])

    assert code == 0
    assert fake.created == []
    output = capsys.readouterr().out
    assert "Mode: DRY-RUN" in output
    assert "ready=3" in output
    assert "neutral-included=3" in output
    assert "neutral copy: content unchanged; only the template name changes" in output
    assert "Antworten Sie mit STOP." in output
    assert _TOKEN not in output


@pytest.mark.parametrize("source_args", [[], ["--source-location", "ka"]], ids=["default-ka", "explicit-ka"])
def test_targeted_apply_copies_only_named_neutral_templates_without_content_changes(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture, source_args: list[str]
) -> None:
    sources = _neutral_sources()
    original = deepcopy(sources)
    unrelated = _template("kitilash_ka_newsletter_new_clients_monthly_v1", body="Newsletter {{1}}")

    code, fake = _run_script(
        monkeypatch, [*sources, unrelated, *_ka_sources()], [*_APPLY_ARGS, *source_args, *_neutral_args()]
    )

    assert code == 0
    assert len(fake.created) == 3
    assert sources == original
    for source, payload in zip(sorted(sources, key=lambda row: row["name"]), fake.created, strict=True):
        assert payload == {key: source[key] for key in ("language", "category", "parameter_format", "components")} | {
            "name": source["name"].replace(_SOURCE_PREFIX, _TARGET_PREFIX, 1)
        }
    output = capsys.readouterr().out
    assert output.index("--- POST payload ---") < output.index("SENT    ")
    assert "status=PENDING" in output
    assert _TOKEN not in output


def test_neutral_inclusion_requires_exact_selection_before_reading_meta(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    code, fake = _run_script(monkeypatch, _neutral_sources(), [*_APPLY_ARGS, "--include-neutral"])

    assert code == 1
    assert fake.list_calls == 0
    assert fake.created == []
    assert "requires explicit --template-name" in capsys.readouterr().err


@pytest.mark.parametrize("source_location", ["du", "ra", "xx"])
@pytest.mark.parametrize("apply", [False, True], ids=["dry-run", "apply-yes"])
@pytest.mark.parametrize("credentials_present", [False, True], ids=["no-credentials", "fake-credentials"])
def test_neutral_inclusion_rejects_non_karlsruhe_before_credentials_or_meta(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
    source_location: str,
    apply: bool,
    credentials_present: bool,
) -> None:
    name = f"kitilash_{source_location}_reminder_24h_v1"
    body = _BODY
    if source_location == "du":
        # Regression: this real Durlach footer used to be classified as neutral
        # and submitted unchanged under a Rastatt name with --include-neutral.
        content = BRANCH_PROFILES["durlach"].content
        body += "\n" + "\n".join((content.brand_line, content.address_line, content.maps_line))
    fake = _FakeMetaClient([_template(name, body=body)])
    factory = Mock(return_value=fake)
    monkeypatch.setattr(cloner, "MetaTemplateClient", factory)
    for key, value in (("WHATSAPP_ACCESS_TOKEN", _TOKEN), ("META_WABA_ID", "test-waba")):
        if credentials_present:
            monkeypatch.setenv(key, value)
        else:
            monkeypatch.delenv(key, raising=False)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "clone_meta_templates_for_location.py",
            "--source-location",
            source_location,
            "--target-location",
            "ra" if source_location == "du" else "du",
            "--address",
            _RA_CFG.address_line if source_location == "du" else DEFAULT_TARGET_ADDRESS,
            "--maps-url",
            "https://maps.app.goo.gl/xvYYbJbPaWcnp9Xv5" if source_location == "du" else DEFAULT_TARGET_MAPS_URL,
            "--include-neutral",
            "--template-name",
            name,
            *(["--apply", "--yes"] if apply else []),
        ],
    )

    assert cloner.main() == 1
    assert "--include-neutral requires --source-location ka" in capsys.readouterr().err
    factory.assert_not_called()
    assert fake.list_calls == 0
    assert fake.created == []


@pytest.mark.parametrize("source_location", ["du", "ra", "xx"])
def test_non_karlsruhe_without_neutral_opt_in_keeps_existing_source_checks(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture, source_location: str
) -> None:
    name = f"kitilash_{source_location}_review_3d_v1"
    code, fake = _run_script(
        monkeypatch,
        [_template(name, body=_NEUTRAL_BODIES["review_3d"])],
        ["--source-location", source_location, "--target-location", "ka", "--template-name", name],
    )

    assert code == 1
    assert fake.list_calls == 1
    assert fake.created == []
    assert "SOURCE_ADDRESS_PATTERNS / SOURCE_MAP_URLS do not cover" in capsys.readouterr().err


def test_exact_selection_does_not_itself_authorize_neutral_copying(monkeypatch: pytest.MonkeyPatch) -> None:
    code, fake = _run_script(monkeypatch, _neutral_sources(), [*_APPLY_ARGS, *_neutral_args(include_neutral=False)])

    assert code == 1
    assert fake.created == []


@pytest.mark.parametrize("status", ["PENDING", "REJECTED", "PAUSED"])
def test_one_unapproved_selected_source_blocks_the_entire_apply(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture, status: str
) -> None:
    sources = _neutral_sources()
    sources[0]["status"] = status

    code, fake = _run_script(monkeypatch, sources, [*_APPLY_ARGS, *_neutral_args()])

    assert code == 1
    assert fake.created == []
    assert sources[0]["name"] in capsys.readouterr().err


@pytest.mark.parametrize("source_count", [0, 2])
def test_missing_selected_sources_block_apply(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture, source_count: int
) -> None:
    code, fake = _run_script(monkeypatch, _neutral_sources()[:source_count], [*_APPLY_ARGS, *_neutral_args()])

    assert code == 1
    assert fake.created == []
    assert "expected branch-specific template not found" in capsys.readouterr().err


def test_wrong_language_selected_source_is_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    sources = _neutral_sources()
    sources[0]["language"] = "en"

    code, fake = _run_script(monkeypatch, sources, [*_APPLY_ARGS, *_neutral_args()])

    assert code == 1
    assert fake.created == []


@pytest.mark.parametrize("status", ["APPROVED", "PENDING"])
def test_existing_neutral_targets_are_skipped_even_when_the_whole_plan_has_zero_replacements(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture, status: str
) -> None:
    sources = _neutral_sources()
    targets = [
        dict(source, name=source["name"].replace(_SOURCE_PREFIX, _TARGET_PREFIX), status=status) for source in sources
    ]

    code, fake = _run_script(monkeypatch, [*sources, *targets], [*_APPLY_ARGS, *_neutral_args()])

    assert code == 0
    assert fake.created == []
    assert "Nothing to submit." in capsys.readouterr().out


@pytest.mark.parametrize("status", ["REJECTED", "PAUSED", "DISABLED", "UNKNOWN"])
def test_unusable_neutral_target_blocks_other_creates(monkeypatch: pytest.MonkeyPatch, status: str) -> None:
    sources = _neutral_sources()
    target = dict(sources[0], name=sources[0]["name"].replace(_SOURCE_PREFIX, _TARGET_PREFIX), status=status)

    code, fake = _run_script(monkeypatch, [*sources, target], [*_APPLY_ARGS, *_neutral_args()])

    assert code == 1
    assert fake.created == []


@pytest.mark.parametrize("name", ["kitilash_ra_review_3d_v1", "kitilash_ka_*", "", "kitilash_ka_review_3d_v1\n"])
def test_invalid_exact_selection_is_rejected_before_reading_meta(monkeypatch: pytest.MonkeyPatch, name: str) -> None:
    code, fake = _run_script(monkeypatch, _neutral_sources(), [*_APPLY_ARGS, "--template-name", name])

    assert code == 1
    assert fake.list_calls == 0
    assert fake.created == []


def test_duplicate_selected_names_are_rejected_before_reading_meta(monkeypatch: pytest.MonkeyPatch) -> None:
    code, fake = _run_script(
        monkeypatch,
        _neutral_sources(),
        [*_APPLY_ARGS, *_neutral_args(), "--template-name", "kitilash_ka_review_3d_v1"],
    )

    assert code == 1
    assert fake.list_calls == 0
    assert fake.created == []


def test_duplicate_selected_sources_cannot_produce_duplicate_posts(monkeypatch: pytest.MonkeyPatch) -> None:
    sources = _neutral_sources()
    code, fake = _run_script(monkeypatch, [*sources, sources[0]], [*_APPLY_ARGS, *_neutral_args()])

    assert code == 1
    assert fake.created == []


def test_known_branch_template_missing_its_footer_stays_blocked_with_neutral_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    name = "kitilash_ka_record_canceled_v1"
    source = _template(name, body="Hallo {{1}}")
    code, fake = _run_script(monkeypatch, [source], [*_APPLY_ARGS, "--include-neutral", "--template-name", name])

    assert code == 1
    assert fake.created == []


@pytest.mark.parametrize("body", [_BODY + "\n" + _KA_CFG.address_line, _BODY + "\n" + _KA_MAPS_URL])
def test_neutral_opt_in_does_not_bypass_partial_footer_guards(monkeypatch: pytest.MonkeyPatch, body: str) -> None:
    sources = _neutral_sources()
    sources[0]["components"][0]["text"] = body

    code, fake = _run_script(monkeypatch, sources, [*_APPLY_ARGS, *_neutral_args()])

    assert code == 1
    assert fake.created == []


def test_targeted_branch_copy_does_not_require_unselected_branch_templates(monkeypatch: pytest.MonkeyPatch) -> None:
    source = _ka_sources()[0]
    code, fake = _run_script(monkeypatch, [source], [*_APPLY_ARGS, "--template-name", source["name"]])

    assert code == 0
    assert len(fake.created) == 1
    assert DEFAULT_TARGET_ADDRESS in _body_text(fake.created[0])


def test_neutral_apply_requires_confirmation_and_shows_unchanged_body_first(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture
) -> None:
    def refuse(prompt: str) -> str:
        assert "CREATE:DU:3" in prompt
        preview = capsys.readouterr().out
        assert "neutral copy: content unchanged" in preview
        assert "Antworten Sie mit STOP." in preview
        return "NO"

    monkeypatch.setattr("builtins.input", refuse)
    code, fake = _run_script(monkeypatch, _neutral_sources(), [*_apply_args(yes=False), *_neutral_args()])

    assert code == 1
    assert fake.created == []


def test_expected_branch_template_classified_as_neutral_blocks_the_run() -> None:
    """A branch template stripped of both address and map is still not neutral."""
    sources = _ka_sources(exclude="kitilash_ka_record_canceled_v1")
    sources.append(_template("kitilash_ka_record_canceled_v1", body="*{{1}}, hallo!*\nIhr Termin wurde storniert."))

    plan = _plan(sources)

    assert plan.neutral_expected == ("kitilash_ka_record_canceled_v1",)
    assert any("kitilash_ka_record_canceled_v1" in blocker for blocker in plan_blockers(plan))


# ---------------------------------------------------------------------------
# 5. expected branch coverage
# ---------------------------------------------------------------------------


def test_expected_branch_templates_are_derived_from_meta_templates() -> None:
    expected = _expected()

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


def test_new_client_variant_is_flagged_as_altegio_only(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """EasyWeek locations take the name from message_templates; the operator decides."""
    assert cloner.is_altegio_only("kitilash_ka_record_created_new_client_v1")
    assert not cloner.is_altegio_only("kitilash_ka_record_created_v1")

    _run_script(monkeypatch, _ka_sources(), [])

    output = capsys.readouterr().out
    note_lines = [line for line in output.splitlines() if "Altegio-only" in line]
    assert note_lines
    assert all("record_created_new_client" in line for line in note_lines)


def test_missing_expected_template_exits_non_zero(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    templates = _ka_sources(exclude="kitilash_ka_record_created_new_client_v1")

    code, fake = _run_script(monkeypatch, templates, _APPLY_ARGS)

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

    plan = _plan(select_sources(templates, source_prefix=_SOURCE_PREFIX, language=_LANGUAGE))

    assert plan.missing_expected == (missing_name,)
    assert any(missing_name in blocker for blocker in plan_blockers(plan))


def test_source_prefix_without_any_marker_is_named_explicitly() -> None:
    """A --source-location the patterns do not cover must say so, not just 'neutral'."""
    sources = [_template("kitilash_ka_record_created_v1", body="*{{1}}, hallo!*\nBis bald.")]

    blockers = plan_blockers(_plan(sources))

    assert any("SOURCE_ADDRESS_PATTERNS / SOURCE_MAP_URLS do not cover" in blocker for blocker in blockers)


# ---------------------------------------------------------------------------
# 6. existing targets and their Meta status
# ---------------------------------------------------------------------------


def test_approved_target_is_skipped_without_resubmitting(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    code, fake = _run_script(monkeypatch, [*_ka_sources(), *_du_targets(status="APPROVED")], _APPLY_ARGS)

    assert code == 0
    assert fake.created == []
    output = capsys.readouterr().out
    assert "SKIP    APPROVED target already exists: kitilash_du_record_created_v1" in output
    assert "Nothing to submit." in output


def test_pending_target_is_skipped_without_resubmitting(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    code, fake = _run_script(monkeypatch, [*_ka_sources(), *_du_targets(status="PENDING")], _APPLY_ARGS)

    assert code == 0
    assert fake.created == []
    output = capsys.readouterr().out
    assert "SKIP    PENDING target already awaiting review: kitilash_du_record_created_v1" in output
    assert "Nothing to submit." in output


def test_rejected_target_blocks_the_run_instead_of_reporting_success(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """All six targets REJECTED used to print 'Nothing to submit' and exit 0."""
    code, fake = _run_script(monkeypatch, [*_ka_sources(), *_du_targets(status="REJECTED")], _APPLY_ARGS)

    assert code == 1
    assert fake.created == []
    output = capsys.readouterr()
    assert "Nothing to submit." not in output.out
    assert "BLOCKED target exists with status REJECTED: kitilash_du_record_created_v1" in output.out
    assert "_v2" in output.err


@pytest.mark.parametrize("status", ["PAUSED", "DISABLED", "SOME_FUTURE_STATE"])
def test_unusable_target_status_blocks_the_run(status: str) -> None:
    plan = _plan(_ka_sources(), existing=_du_targets(status=status, only="kitilash_ka_reminder_2h_v1"))

    assert [target.status for target in plan.existing_unusable] == [status]
    assert any("kitilash_du_reminder_2h_v1" in blocker for blocker in plan_blockers(plan))


def test_only_the_missing_target_is_submitted(monkeypatch: pytest.MonkeyPatch) -> None:
    already_cloned = [
        template for template in _du_targets(status="APPROVED") if template["name"] != "kitilash_du_reminder_24h_v1"
    ]

    code, fake = _run_script(monkeypatch, [*_ka_sources(), *already_cloned], _APPLY_ARGS)

    assert code == 0
    assert [payload["name"] for payload in fake.created] == ["kitilash_du_reminder_24h_v1"]
    assert DEFAULT_TARGET_ADDRESS in _body_text(fake.created[0])


# ---------------------------------------------------------------------------
# 7-8. recursive replacement and the residual scan
# ---------------------------------------------------------------------------


def test_address_and_map_inside_button_url_are_rewritten() -> None:
    source = _template("kitilash_ka_record_created_v1", body=_BODY + _KA_FOOTER, button_url=_KA_MAPS_URL)
    source["components"][1]["buttons"].append({"type": "QUICK_REPLY", "text": f"Adresse: {_KA_CFG.address_line}"})

    outcome = prepare_template(source, **_PREPARE_KWARGS)

    buttons = outcome.payload["components"][1]["buttons"]
    assert buttons[0]["url"] == DEFAULT_TARGET_MAPS_URL
    assert DEFAULT_TARGET_ADDRESS in buttons[1]["text"]
    assert residual_source_markers(outcome.payload) == []
    assert outcome.status is TemplateStatus.READY
    assert outcome.replacements == ReplacementStats(address=2, maps_url=2)


def test_example_values_are_rewritten_too() -> None:
    source = _template("kitilash_ka_record_updated_v1", body=_BODY + _KA_FOOTER)
    source["components"][0]["example"] = {"body_text": [["Anna", _KA_CFG.address_line]]}

    outcome = prepare_template(source, **_PREPARE_KWARGS)

    assert outcome.payload["components"][0]["example"]["body_text"][0][1] == DEFAULT_TARGET_ADDRESS
    assert residual_source_markers(outcome.payload) == []


def test_residual_marker_outside_the_rewritten_fields_blocks_the_template() -> None:
    """The scan covers the whole payload, not only the keys the rewrite touches."""
    source = _template("kitilash_ka_record_created_v1", body=_BODY + _KA_FOOTER)
    # "format" is copied verbatim (it is not in _REPLACEABLE_COMPONENT_KEYS), so
    # a marker here stands in for any field a future Meta version might add.
    source["components"][0]["format"] = f"TEXT {_KA_MAPS_URL}"

    outcome = prepare_template(source, **_PREPARE_KWARGS)

    assert outcome.status is TemplateStatus.RESIDUAL_SOURCE
    assert any("components[0].format" in residual for residual in outcome.residuals)


def test_residual_marker_blocks_the_whole_run_before_any_post(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    templates = _ka_sources()
    templates[0]["components"][0]["format"] = f"TEXT {_KA_MAPS_URL}"

    code, fake = _run_script(monkeypatch, templates, _APPLY_ARGS)

    assert code == 1
    assert fake.created == []
    output = capsys.readouterr()
    assert "source marker left in the finished payload" in output.out
    assert "source marker left in the finished payload" in output.err


# ---------------------------------------------------------------------------
# 9. placeholder signature
# ---------------------------------------------------------------------------


def test_placeholder_signature_survives_the_rewrite() -> None:
    source = _template("kitilash_ka_record_created_v1", body=_BODY + _KA_FOOTER)

    outcome = prepare_template(source, **_PREPARE_KWARGS)

    assert outcome.placeholders_before == outcome.placeholders_after
    assert outcome.placeholders_after == (("BODY", ("{{1}}", "{{2}}")), ("BUTTONS", ()))
    assert placeholder_signature(outcome.payload["components"]) == outcome.placeholders_after


def test_rewrite_that_eats_a_placeholder_is_blocked(monkeypatch: pytest.MonkeyPatch) -> None:
    """Artificial divergence: a pattern that swallows {{2}} must not reach Meta."""
    monkeypatch.setattr(cloner, "SOURCE_ADDRESS_PATTERNS", (re.compile(r"\{\{2\}\}"),))
    source = _template("kitilash_ka_record_created_v1", body=_BODY + _KA_FOOTER)

    outcome = prepare_template(source, **_PREPARE_KWARGS)

    assert outcome.status is TemplateStatus.PLACEHOLDER_MISMATCH
    assert outcome.placeholders_before != outcome.placeholders_after

    plan = _plan([source])
    assert [item.source_name for item in plan.placeholder_mismatch] == ["kitilash_ka_record_created_v1"]
    assert any("binds template parameters by position" in blocker for blocker in plan_blockers(plan))


# ---------------------------------------------------------------------------
# 10. the Graph host is pinned
# ---------------------------------------------------------------------------


def test_normalize_graph_url_accepts_the_meta_host() -> None:
    assert normalize_graph_url("https://graph.facebook.com") == "https://graph.facebook.com"
    assert normalize_graph_url("  https://graph.facebook.com/  ") == "https://graph.facebook.com"


@pytest.mark.parametrize(
    "raw",
    [
        "http://graph.facebook.com",
        "https://graph.facebook.com.evil.example",
        "https://evil.example.com",
        "https://user:pass@graph.facebook.com",
        "https://graph.facebook.com:8443",
        "https://graph.facebook.com/v25.0",
        "https://graph.facebook.com?token=leak",
        "https://graph.facebook.com#frag",
        "graph.facebook.com",
        "",
    ],
)
def test_normalize_graph_url_rejects_everything_else(raw: str) -> None:
    with pytest.raises(ScriptError):
        normalize_graph_url(raw)


def test_foreign_graph_url_fails_before_any_request(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    code, fake = _run_script(
        monkeypatch,
        _ka_sources(),
        [*_APPLY_ARGS, "--graph-url", "https://evil.example.com"],
    )

    assert code == 1
    assert fake.list_calls == 0
    assert fake.created == []
    assert "graph.facebook.com" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# 11-12. the hardened Meta client
# ---------------------------------------------------------------------------

_ENDPOINT = "https://graph.facebook.com/v25.0/test-waba/message_templates"


def _client() -> MetaTemplateClient:
    return MetaTemplateClient(
        token=_TOKEN,
        waba_id="test-waba",
        graph_url="https://graph.facebook.com",
        api_version="v25.0",
        timeout_seconds=5.0,
    )


@respx.mock
async def test_paging_uses_cursors_after_against_our_own_endpoint() -> None:
    route = respx.get(_ENDPOINT).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "data": [{"name": "kitilash_ka_a"}],
                    # A hostile "next" must never be followed with the token attached.
                    "paging": {"next": "https://evil.example.com/steal", "cursors": {"after": "CURSOR1"}},
                },
            ),
            httpx.Response(200, json={"data": [{"name": "kitilash_ka_b"}], "paging": {"cursors": {}}}),
        ]
    )

    async with _client() as client:
        templates = await client.list_templates()

    assert [item["name"] for item in templates] == ["kitilash_ka_a", "kitilash_ka_b"]
    assert len(route.calls) == 2
    assert all(call.request.url.host == "graph.facebook.com" for call in route.calls)
    assert "after" not in route.calls[0].request.url.params
    assert route.calls[1].request.url.params["after"] == "CURSOR1"


@respx.mock
async def test_repeating_paging_cursor_raises() -> None:
    page = httpx.Response(
        200,
        json={"data": [{"name": "kitilash_ka_a"}], "paging": {"next": "x", "cursors": {"after": "SAME"}}},
    )
    route = respx.get(_ENDPOINT).mock(side_effect=[page, page])

    async with _client() as client:
        with pytest.raises(ScriptError, match="cursor repeated"):
            await client.list_templates()

    assert len(route.calls) == 2


@respx.mock
async def test_paging_stops_at_the_page_limit() -> None:
    """A cursor that keeps advancing must still not spin forever."""
    pages = itertools.count()

    def _next_page(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={"data": [], "paging": {"next": "more", "cursors": {"after": f"CURSOR{next(pages)}"}}},
        )

    respx.get(_ENDPOINT).mock(side_effect=_next_page)

    async with _client() as client:
        with pytest.raises(ScriptError, match="more than 50 template pages"):
            await client.list_templates()


@respx.mock
async def test_non_json_body_on_list_raises_script_error() -> None:
    respx.get(_ENDPOINT).mock(return_value=httpx.Response(200, text="<html>maintenance</html>"))

    async with _client() as client:
        with pytest.raises(ScriptError, match="non-JSON body"):
            await client.list_templates()


@respx.mock
async def test_non_json_body_on_create_raises_script_error() -> None:
    respx.post(_ENDPOINT).mock(return_value=httpx.Response(200, text="not json"))

    async with _client() as client:
        with pytest.raises(ScriptError, match="non-JSON body"):
            await client.create_template({"name": "kitilash_du_record_created_v1"})


@respx.mock
async def test_json_array_response_raises_script_error() -> None:
    respx.get(_ENDPOINT).mock(return_value=httpx.Response(200, json=[1, 2, 3]))

    async with _client() as client:
        with pytest.raises(ScriptError, match="expected an object"):
            await client.list_templates()


# ---------------------------------------------------------------------------
# 13. an indeterminate create result is not a success
# ---------------------------------------------------------------------------


def test_create_response_without_id_is_not_reported_as_sent(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    code, fake = _run_script(monkeypatch, _ka_sources(), _APPLY_ARGS, create_result={})

    assert code == 1
    assert len(fake.created) == len(_expected())
    output = capsys.readouterr()
    assert "SENT" not in output.out
    assert "UNKNOWN kitilash_du_record_created_v1" in output.err
    assert "indeterminate result(s)" in output.err


def test_create_response_with_id_is_reported_as_sent(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    code, _fake = _run_script(monkeypatch, _ka_sources(), _APPLY_ARGS)

    assert code == 0
    assert "SENT    kitilash_du_record_created_v1 id=new-kitilash_du_record_created_v1" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# 14. apply-time footguns
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "argv",
    [
        ["--apply", "--yes"],
        ["--apply", "--yes", "--target-location", "du"],
        ["--apply", "--yes", "--target-location", "du", "--address", DEFAULT_TARGET_ADDRESS],
        ["--apply", "--yes", "--address", DEFAULT_TARGET_ADDRESS, "--maps-url", DEFAULT_TARGET_MAPS_URL],
    ],
)
def test_apply_requires_explicit_target(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
    argv: list[str],
) -> None:
    code, fake = _run_script(monkeypatch, _ka_sources(), argv)

    assert code == 1
    assert fake.list_calls == 0
    assert fake.created == []
    assert "--apply requires" in capsys.readouterr().err


def test_dry_run_still_uses_the_durlach_defaults(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    code, fake = _run_script(monkeypatch, _ka_sources(), [])

    assert code == 0
    assert fake.created == []
    output = capsys.readouterr().out
    assert "Mode: DRY-RUN" in output
    assert f"Target: {_TARGET_PREFIX}*" in output
    assert f"Address: {DEFAULT_TARGET_ADDRESS}" in output
    assert f"Maps: {DEFAULT_TARGET_MAPS_URL}" in output
    assert "Nothing submitted. Re-run with --apply after reviewing this plan." in output


def test_dry_run_prints_the_full_preview(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    _run_script(monkeypatch, _ka_sources(), [])

    output = capsys.readouterr().out
    assert "READY   kitilash_ka_record_created_v1 -> kitilash_du_record_created_v1" in output
    assert "source status: APPROVED   language: de   category: UTILITY" in output
    assert "placeholders: BODY {{1}},{{2}} | BUTTONS -" in output
    assert f"brand line (kept as is): {_KA_CFG.brand_line}" in output
    assert "--- BODY ---" in output
    assert "--- BUTTONS ---" in output
    assert "[URL] Termin -> https://n813709.alteg.io/" in output
    assert "--- POST payload ---" in output
    # the whole rewritten footer, not just a summary
    assert DEFAULT_TARGET_ADDRESS in output
    assert DEFAULT_TARGET_MAPS_URL in output
    assert _KA_CFG.phone_line in output


@pytest.mark.parametrize(
    ("address", "reason"),
    [
        ("", "empty"),
        ("   ", "empty"),
        ("Pfinztalstraße 4,\x00 76227 Karlsruhe-Durlach", "control character"),
        ("Pfinztalstraße 4,\r\n76227 Karlsruhe-Durlach", "control character"),
        ("Pfinztalstraße {{1}}, 76227 Karlsruhe-Durlach", "Meta placeholder"),
        ("76133 Karlsruhe, Kaiserstraße, 68", "SOURCE address"),
        ("Kaiserstraße, 68, 76133 Karlsruhe", "SOURCE address"),
    ],
)
def test_bad_address_is_refused_before_any_meta_call(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
    address: str,
    reason: str,
) -> None:
    """Every SOURCE-side guard passes for these: only the value itself catches them."""
    code, fake = _run_script(monkeypatch, _ka_sources(), _apply_args(address=address))

    assert code == 1
    assert fake.list_calls == 0
    assert fake.created == []
    error = capsys.readouterr().err
    assert "--address" in error
    assert reason in error


@pytest.mark.parametrize(
    ("maps_url", "reason"),
    [
        ("", "empty"),
        ("   ", "empty"),
        ("abc", "absolute https URL"),
        ("maps.app.goo.gl/HnVPnHaJHf2DW3Nn8", "absolute https URL"),
        ("http://maps.app.goo.gl/HnVPnHaJHf2DW3Nn8", "absolute https URL"),
        ("https:///HnVPnHaJHf2DW3Nn8", "no hostname"),
        ("https://user:pass@maps.app.goo.gl/HnVPnHaJHf2DW3Nn8", "credentials"),
        ("https://maps.app.goo.gl/HnVPnHaJHf2DW3Nn8#pin", "fragment"),
        ("https://maps.app.goo.gl/HnVP\x07naJHf2DW3Nn8", "control character"),
        (_KA_MAPS_URL, "SOURCE map links"),
        ("https://maps.app.goo.gl/p7quWqbAqY9cusuRA", "SOURCE map links"),
    ],
)
def test_bad_maps_url_is_refused_before_any_meta_call(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
    maps_url: str,
    reason: str,
) -> None:
    code, fake = _run_script(monkeypatch, _ka_sources(), _apply_args(maps_url=maps_url))

    assert code == 1
    assert fake.list_calls == 0
    assert fake.created == []
    error = capsys.readouterr().err
    assert "--maps-url" in error
    assert reason in error


def test_empty_address_would_otherwise_pass_every_other_guard() -> None:
    """Why the value check exists: an empty address leaves nothing to detect."""
    source = _template("kitilash_ka_record_created_v1", body=_BODY + _KA_FOOTER)

    outcome = prepare_template(
        source,
        source_prefix=_SOURCE_PREFIX,
        target_prefix=_TARGET_PREFIX,
        address="",
        maps_url=DEFAULT_TARGET_MAPS_URL,
    )

    assert outcome.status is TemplateStatus.READY
    assert outcome.replacements == ReplacementStats(address=1, maps_url=1)
    assert outcome.residuals == ()
    assert outcome.placeholders_before == outcome.placeholders_after
    assert validate_target_address(DEFAULT_TARGET_ADDRESS) == DEFAULT_TARGET_ADDRESS
    with pytest.raises(ScriptError):
        validate_target_address("")


def test_validators_accept_the_shipped_defaults() -> None:
    assert validate_target_address(f"  {DEFAULT_TARGET_ADDRESS}  ") == DEFAULT_TARGET_ADDRESS
    assert validate_target_maps_url(f"  {DEFAULT_TARGET_MAPS_URL}  ") == DEFAULT_TARGET_MAPS_URL


def test_dry_run_also_refuses_a_bad_target_value(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """A plan built on an empty address is not worth reviewing either."""
    code, fake = _run_script(monkeypatch, _ka_sources(), ["--address", ""])

    assert code == 1
    assert fake.list_calls == 0
    assert "--address is empty" in capsys.readouterr().err


def test_full_preview_is_printed_before_the_confirmation(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    """--apply may run without a prior dry-run, and the word only encodes a count."""
    at_prompt: dict[str, str] = {}

    def _fake_input(prompt: str = "") -> str:
        # readouterr() drains what was printed so far: everything asserted below
        # therefore reached stdout BEFORE the operator was asked to confirm.
        at_prompt["stdout"] = capsys.readouterr().out
        at_prompt["prompt"] = prompt
        return "CREATE:DU:6"

    monkeypatch.setattr("builtins.input", _fake_input)

    code, fake = _run_script(monkeypatch, _ka_sources(), _apply_args(yes=False))

    assert code == 0
    assert [payload["name"] for payload in fake.created] == sorted(
        name.replace(_SOURCE_PREFIX, _TARGET_PREFIX) for name in _expected()
    )
    printed = at_prompt["stdout"]
    assert "About to submit 6 template(s) to Meta for review:" in printed
    assert "--- POST payload ---" in printed
    assert "--- BODY ---" in printed
    assert "placeholders: BODY {{1}},{{2}} | BUTTONS -" in printed
    assert DEFAULT_TARGET_ADDRESS in printed
    assert DEFAULT_TARGET_MAPS_URL in printed
    assert _KA_CFG.address_line not in printed
    assert "CREATE:DU:6" in at_prompt["prompt"]


def test_preview_is_printed_with_yes_as_the_run_transcript(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    code, fake = _run_script(monkeypatch, _ka_sources(), _APPLY_ARGS)

    assert code == 0
    assert len(fake.created) == 6
    output = capsys.readouterr().out
    assert "--- POST payload ---" in output
    assert DEFAULT_TARGET_MAPS_URL in output
    assert output.index("--- POST payload ---") < output.index("SENT    ")


def test_wrong_confirmation_still_sees_the_preview_but_sends_nothing(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
) -> None:
    monkeypatch.setattr("builtins.input", lambda *_args: "CREATE:DU:5")

    code, fake = _run_script(monkeypatch, _ka_sources(), _apply_args(yes=False))

    assert code == 1
    assert fake.created == []
    output = capsys.readouterr()
    assert "--- POST payload ---" in output.out
    assert "SENT" not in output.out
    assert "confirmation did not match" in output.err


def test_confirmation_word_carries_the_plan(monkeypatch: pytest.MonkeyPatch) -> None:
    assert _confirmation_word("du", 6) == "CREATE:DU:6"
    assert _confirmation_word("ra", 1) == "CREATE:RA:1"

    prompts: list[str] = []

    def _fake_input(prompt: str = "") -> str:
        prompts.append(prompt)
        return "CREATE:DU:6"

    monkeypatch.setattr("builtins.input", _fake_input)
    argv = [arg for arg in _APPLY_ARGS if arg != "--yes"]

    code, fake = _run_script(monkeypatch, _ka_sources(), argv)

    assert code == 0
    assert len(fake.created) == 6
    assert "CREATE:DU:6" in prompts[0]


def test_wrong_confirmation_submits_nothing(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture) -> None:
    monkeypatch.setattr("builtins.input", lambda *_args: "DURLACH")
    argv = [arg for arg in _APPLY_ARGS if arg != "--yes"]

    code, fake = _run_script(monkeypatch, _ka_sources(), argv)

    assert code == 1
    assert fake.created == []
    assert "confirmation did not match" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# 15. the token never reaches the output
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("argv", [[], _APPLY_ARGS])
def test_access_token_is_never_printed(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture,
    argv: list[str],
) -> None:
    _run_script(monkeypatch, _ka_sources(), argv)

    output = capsys.readouterr()
    assert _TOKEN not in output.out
    assert _TOKEN not in output.err


# ---------------------------------------------------------------------------
# 16. _normalize_api_version
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
