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

import uuid
from pathlib import Path

import pytest

from altegio_bot.easyweek_normalizer import easyweek_job_dedupe_key
from altegio_bot.scripts.easyweek_recovery_audit import expected_job_type
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
LEVEL_A_HEADING = "##### Уровень A"
LEVEL_B_HEADING = "##### Уровень B"
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


def test_the_one_shot_validation_does_not_start_the_inbox_loop() -> None:
    normal = _section(_runbook(), NORMAL_HEADING)

    assert "run --rm --no-deps" in normal
    assert "--entrypoint" in normal
    assert "run_easyweek_inbox_worker" not in normal, "config validation must not invoke the worker entrypoint"


def test_recovery_does_not_require_jobs_to_stay_queued_or_processing() -> None:
    """After recovery, `done`, a retry or a deadline-driven terminal is normal.

    The old wording ("queued/processing must survive to the end") would have an
    operator escalate on a successfully delivered notification.
    """
    normal = _section(_runbook(), NORMAL_HEADING)

    assert "обязаны сохраниться до конца" not in normal
    assert "существование job и объяснимый terminal/retry исход" in normal
    for status in ("done", "canceled"):
        assert status in normal, "the doc must name the terminal statuses that are legitimate after recovery"


# ---------------------------------------------------------------------------
# Post-recovery audit: per delivery, never per booking
# ---------------------------------------------------------------------------


def test_recovery_has_no_global_processed_without_job_invariant() -> None:
    """The regression this second contract exists for.

    `processed_without_job = 0` was never a production invariant: it is false
    green when a stale job covers a dropped delivery, and false alarm for every
    legitimately suppressed one.
    """
    recovery = _runbook()

    assert "processed_without_job" not in recovery
    for claim in ("Ожидается `0`", "должно быть 0", "равно нулю"):
        assert claim not in recovery


def test_the_audit_never_links_an_event_to_a_job_through_the_record() -> None:
    """Forbidden as proof: record_id, booking_uuid, company_id, job_type, time.

    Any of them can be satisfied by an unrelated older job for the same booking.
    """
    recovery = _runbook()

    for broad_join in (
        "j.record_id = r.id",
        "j.record_id=r.id",
        "ON j.record_id",
        "AND j.record_id",
        "easyweek_booking_uuid = e.booking_uuid",
    ):
        assert broad_join not in recovery, f"a job may not be matched to an event by {broad_join}"


def test_the_audit_uses_the_production_dedupe_key_helper() -> None:
    """Never a second SHA-256 implementation — it would rot out of sync."""
    audit = _section(_runbook(), LEVEL_A_HEADING) + _section(_runbook(), LEVEL_B_HEADING)

    assert "easyweek_job_dedupe_key" in audit
    assert "altegio_bot.scripts.easyweek_recovery_audit" in audit
    for block in _code_blocks(audit):
        assert "sha256" not in block.lower(), "the audit must import the key function, not restate the digest"


def test_a_stale_job_for_the_same_record_cannot_answer_for_a_new_delivery() -> None:
    """Bound to runtime: the key separates what the record_id join conflated."""
    booking = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-000000000009")

    created = easyweek_job_dedupe_key(
        event_hint="booking-created", booking_uuid=booking, payload_hash="hash-a", job_type="record_created"
    )
    updated = easyweek_job_dedupe_key(
        event_hint="booking-updated", booking_uuid=booking, payload_hash="hash-b", job_type="record_updated"
    )

    assert created != updated, "an old record_created job must never satisfy a booking-updated check"


def test_the_documented_mapping_matches_the_runtime_mapping() -> None:
    """booking-updated and booking-rescheduled share a job type, not an identity."""
    assert expected_job_type("booking-created") == "record_created"
    assert expected_job_type("booking-updated") == "record_updated"
    assert expected_job_type("booking-rescheduled") == "record_updated"
    assert expected_job_type("booking-canceled") == "record_canceled"
    assert expected_job_type("booking-succeeded") is None, "booking-succeeded owes no lifecycle job"

    # Same job_type, different payload -> different expected key, still separate.
    booking = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-000000000010")
    keys = {
        easyweek_job_dedupe_key(
            event_hint=hint, booking_uuid=booking, payload_hash=payload_hash, job_type="record_updated"
        )
        for hint, payload_hash in (("booking-updated", "h1"), ("booking-rescheduled", "h2"))
    }
    assert len(keys) == 2


def test_resend_is_documented_as_deduplication_not_loss() -> None:
    """Byte-identical deliveries share one key and are owed exactly one job."""
    level_a = _section(_runbook(), LEVEL_A_HEADING)

    assert "resend_groups" in level_a
    assert "Resend" in level_a
    assert "дедупликация" in level_a

    # Runtime: identical hint + uuid + payload hash collapse to one key.
    booking = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-000000000011")
    kwargs = {
        "event_hint": "booking-created",
        "booking_uuid": booking,
        "payload_hash": "same-hash",
        "job_type": "record_created",
    }
    assert easyweek_job_dedupe_key(**kwargs) == easyweek_job_dedupe_key(**kwargs)


def test_booking_succeeded_is_documented_as_owing_no_job() -> None:
    level_a = _section(_runbook(), LEVEL_A_HEADING)

    assert "non_lifecycle_event_ids" in level_a
    assert "booking-succeeded" in level_a


def test_a_missing_job_is_unclassified_and_not_declared_lost() -> None:
    """Suppression, no-op, replay and real loss are indistinguishable here.

    `Record.raw` cannot settle it either: a later delivery may have overwritten
    the snapshot the decision was made against.
    """
    level_a = _section(_runbook(), LEVEL_A_HEADING)

    assert "no_event_specific_job_unclassified" in level_a
    assert "не означает «потеряно»" in level_a
    for legitimate in ("category_not_allowed", "post-cancel no-op", "already_applied"):
        assert legitimate in level_a
    assert "Record.raw" in level_a and "нельзя" in level_a


def test_controlled_smoke_proves_both_directions() -> None:
    """Level B is the only positive proof that the pipeline creates jobs again."""
    level_b = _section(_runbook(), LEVEL_B_HEADING)

    assert "<allowed_event_id>" in level_b
    assert "<disallowed_event_id>" in level_b
    assert "easyweek_job_dedupe_key" in level_b
    assert "exact_jobs=0" in level_b, "the disallowed smoke must require the ABSENCE of an exact job"
    assert "ровно одна job" in level_b
    for field in ("MessageJob.provider", "MessageJob.company_id", "MessageJob.record_id", "MessageJob.job_type"):
        assert field in level_b


def test_the_audit_command_is_read_only_and_leaks_nothing() -> None:
    audit = _section(_runbook(), LEVEL_A_HEADING) + _section(_runbook(), LEVEL_B_HEADING)

    for block in _code_blocks(audit):
        assert "run --rm --no-deps" in block or "select(" in block or block.strip().startswith("|")
        for mutation in ("UPDATE ", "DELETE ", "INSERT ", "replay"):
            assert mutation not in block, f"the audit must stay read-only, found {mutation}"
    assert "run_easyweek_inbox_worker" not in audit, "the audit must not start the inbox loop"
    assert "read-only" in audit


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
