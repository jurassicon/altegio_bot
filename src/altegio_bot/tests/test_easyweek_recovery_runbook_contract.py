"""PR-7.1: the runbook's recovery path must not destroy captured backlog.

The runtime is fail-closed and recoverable; the danger lives in the instruction.
With `EASYWEEK_PROCESSING_ENABLED=true` and `EASYWEEK_NOTIFICATIONS_ENABLED=false`
the inbox worker DOES claim captured events, updates the domain snapshot, skips
`plan_lifecycle_job` on its first line, and marks the event terminal
`processed` — with no lifecycle job and no automatic replay afterwards. A runbook
that opens recovery with "turn notifications off and drain the backlog" quietly
destroys exactly the notifications the operator is trying to rescue.

These tests bind the document to that runtime fact, so neither can drift alone.
They check headings, env-var tokens, fence/restart ORDER and the safety
invariants — deliberately not the full prose.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from altegio_bot.settings import settings
from altegio_bot.workers.easyweek_inbox_worker import processing_is_configured

REPO_ROOT = Path(__file__).resolve().parents[3]
ACTIVATION_RUNBOOK = REPO_ROOT / "docs/easyweek/durlach_activation_runbook.md"

VALID_LOCATION_MAP = (
    '{"durlach": {"location_id": 999501, '
    '"location_uuid": "dddddddd-eeee-4fff-8000-000000000001", '
    '"meta_template_prefix": "du", '
    '"booking_page_url": "https://book.durlach.invalid/durlach"}}'
)

RECOVERY_HEADING = "### Восстановление после invalid/unconfigured allowlist"
NORMAL_HEADING = "#### Штатное восстановление"
STAGED_HEADING = "#### Staged fence, если inbox worker нужно остановить"
DRAIN_HEADING = "#### Domain-only drain"
# The recovery region ends where the pre-existing §6 prose resumes. Bounding it
# explicitly keeps the leak assertions off unrelated text — notably the §6
# log-hygiene grep, which legitimately SEARCHES for leak markers.
RECOVERY_REGION_END = "Отдельный production follow-up"


def _runbook() -> str:
    """The recovery region only — everything this contract governs."""
    text = ACTIVATION_RUNBOOK.read_text()
    start = text.index(RECOVERY_HEADING)
    return text[start : text.index(RECOVERY_REGION_END, start)]


def _code_blocks(section: str) -> list[str]:
    """Fenced blocks only — the parts an operator copies and runs.

    Prose that FORBIDS a setting necessarily names it; a config block that
    contains it is an instruction to apply it. Only the second is a defect, so
    the two are checked separately.
    """
    parts = section.split("```")
    return parts[1::2]


def _section(text: str, heading: str) -> str:
    """The slice from *heading* to the next heading of the SAME or higher level.

    Level matters: a `###` section has to include its own `####` subsections,
    otherwise the recovery-wide invariants would be checked against an empty
    string and pass for the wrong reason.
    """
    level = len(heading) - len(heading.lstrip("#"))
    start = text.index(heading) + len(heading)
    rest = text[start:]
    stoppers = ["\n" + "#" * n + " " for n in range(2, level + 1)]
    ends = [rest.index(m) for m in stoppers if m in rest]
    return rest[: min(ends)] if ends else rest


# ---------------------------------------------------------------------------
# The runtime fact the whole recovery path rests on
# ---------------------------------------------------------------------------


@pytest.fixture
def _invalid_allowlist(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_location_map", VALID_LOCATION_MAP, raising=False)
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", "not-json", raising=False)


def test_an_invalid_allowlist_fences_claiming_only_while_notifications_are_on(
    _invalid_allowlist: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The asymmetry that makes "turn notifications off" destructive.

    If this ever stops holding, the runbook's reasoning is void and both must be
    revisited together — which is why the assertion lives next to the doc tests.
    """
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    assert processing_is_configured() is False, "notifications on + invalid allowlist must fence the claim"

    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)
    assert processing_is_configured() is True, (
        "notifications off lets the worker claim and terminalize captured events without jobs — "
        "this is what the runbook must never present as safe recovery"
    )


def test_processing_disabled_is_a_hard_fence_regardless_of_notifications(
    _invalid_allowlist: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Why the staged variant uses processing=false as its write fence."""
    monkeypatch.setattr(settings, "easyweek_processing_enabled", False, raising=False)
    for notifications in (True, False):
        monkeypatch.setattr(settings, "easyweek_notifications_enabled", notifications, raising=False)
        assert processing_is_configured() is False


# ---------------------------------------------------------------------------
# Normal recovery
# ---------------------------------------------------------------------------


def test_the_runbook_has_a_recovery_section_with_all_three_variants() -> None:
    text = ACTIVATION_RUNBOOK.read_text()

    for heading in (RECOVERY_HEADING, NORMAL_HEADING, STAGED_HEADING, DRAIN_HEADING):
        assert heading in text, f"missing recovery heading: {heading}"
    # Ordered: explain the trap, then the safe path, then the escape hatches.
    assert (
        text.index(RECOVERY_HEADING)
        < text.index(NORMAL_HEADING)
        < text.index(STAGED_HEADING)
        < text.index(DRAIN_HEADING)
    )


def test_normal_recovery_never_turns_notifications_off() -> None:
    """The regression this file exists for."""
    normal = _section(_runbook(), NORMAL_HEADING)

    for block in _code_blocks(normal):
        assert "EASYWEEK_NOTIFICATIONS_ENABLED=false" not in block, (
            "normal recovery must never instruct the operator to disable notifications"
        )
    assert "notifications_enabled=true" in normal, "the config gate must require notifications to stay on"


def test_normal_recovery_does_not_recreate_a_running_worker_before_the_fix() -> None:
    """Force-recreate comes only after the allowlist is fixed AND validated."""
    normal = _section(_runbook(), NORMAL_HEADING)

    fix = normal.index('EASYWEEK_ALLOWED_SERVICE_CATEGORIES=["Wimpernverlängerung"]')
    validate = normal.index("--entrypoint")
    recreate = normal.index("up -d --force-recreate")

    assert fix < validate < recreate, "order must be: fix allowlist -> validate one-shot -> recreate consumers"


def test_normal_recovery_preserves_captured_backlog() -> None:
    normal = _section(_runbook(), NORMAL_HEADING)

    assert "FROM easyweek_events" in normal
    assert "'queued', 'processing'" in normal
    # No destructive recovery: the backlog is evidence, not something to clear.
    for forbidden in ("UPDATE easyweek_events", "DELETE FROM easyweek_events", "DELETE FROM message_jobs"):
        assert forbidden not in normal
    assert "processed_without_job" in normal, "recovery must prove jobs appeared, not just that events drained"


def test_the_one_shot_validation_does_not_start_the_inbox_loop() -> None:
    normal = _section(_runbook(), NORMAL_HEADING)

    assert "run --rm --no-deps" in normal
    assert "--entrypoint" in normal
    assert "run_easyweek_inbox_worker" not in normal, "config validation must not invoke the worker entrypoint"


# ---------------------------------------------------------------------------
# Staged fence
# ---------------------------------------------------------------------------


def test_staged_variant_fences_with_processing_not_notifications() -> None:
    staged = _section(_runbook(), STAGED_HEADING)

    fence = staged.index("EASYWEEK_PROCESSING_ENABLED=false")
    recreate = staged.index("up -d --force-recreate altegio-easyweek-inbox-worker")
    restore = staged.index("EASYWEEK_PROCESSING_ENABLED=true")

    assert fence < recreate < restore, "order must be: processing=false -> recreate inbox -> processing=true"
    for block in _code_blocks(staged):
        assert "EASYWEEK_NOTIFICATIONS_ENABLED" not in block, (
            "the staged fence is processing=false; notifications must not be touched here"
        )
    assert "остаётся `true`" in staged


def test_the_runbook_forbids_the_destructive_flag_pair_outright() -> None:
    recovery = _section(_runbook(), RECOVERY_HEADING)

    assert "не должно" in recovery and "EASYWEEK_NOTIFICATIONS_ENABLED=false" in recovery, (
        "the runbook must state the invariant that processing=true + notifications=false "
        "is forbidden while captured backlog exists"
    )


# ---------------------------------------------------------------------------
# Domain-only drain
# ---------------------------------------------------------------------------


def test_domain_only_drain_is_marked_irreversible_and_not_recovery() -> None:
    text = _runbook()
    heading_line = next(line for line in text.splitlines() if line.startswith(DRAIN_HEADING))
    drain = _section(text, DRAIN_HEADING)

    assert "НЕОБРАТИМ" in heading_line.upper()
    assert "не восстановление" in heading_line.lower() or "не восстановление" in drain.lower()
    assert "автоматического replay" in drain
    assert "captured_backlog" in drain, "the operator must count what is being discarded first"
    assert "подтвердить отказ" in drain


# ---------------------------------------------------------------------------
# Diagnostics stay clean
# ---------------------------------------------------------------------------


def test_recovery_diagnostics_print_no_secrets_or_payload() -> None:
    recovery = _runbook()

    for leaked in (
        "easyweek_api_key",
        "easyweek_webhook_secret",
        "whatsapp_access_token",
        "cat easyweek.env",
        "printenv",
        "settings.easyweek_allowed_service_categories}",
        "payload->>",
        "customer_phone",
        "customer_email",
    ):
        assert leaked not in recovery, f"recovery diagnostics must not surface {leaked}"

    # Only booleans and counts leave the one-shot probe.
    assert "service_categories_count" in recovery
    assert "len(c.keys)" in recovery


def test_recovery_uses_real_compose_service_names() -> None:
    import yaml

    recovery = _section(_runbook(), RECOVERY_HEADING)
    services = set(yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())["services"])

    for named in ("altegio-easyweek-inbox-worker", "altegio-outbox-worker"):
        assert named in recovery
        assert named in services, f"{named} is not a real compose service"
    assert "$COMPOSE" in recovery, "recovery must use the production file set variable"
