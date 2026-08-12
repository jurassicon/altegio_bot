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

import inspect
import uuid
from pathlib import Path

import pytest

from altegio_bot.easyweek_normalizer import easyweek_job_dedupe_key
from altegio_bot.scripts.easyweek_recovery_audit import (
    OUTBOX_DELIVERED_STATUSES,
    OUTBOX_PENDING_STATUSES,
    expected_job_type,
    verify_controlled_smoke,
)
from altegio_bot.settings import settings
from altegio_bot.workers.easyweek_inbox_worker import processing_is_configured

REPO_ROOT = Path(__file__).resolve().parents[3]
ACTIVATION_RUNBOOK = REPO_ROOT / "docs/easyweek/durlach_activation_runbook.md"

VALID_ALLOWLIST = '["Wimpernverlängerung"]'

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

# The active PR-7.1 rollout: everything from its heading up to the recovery
# subsection. The forbidden flag pair has to be absent from the rollout itself,
# not only from the recovery text that explains why it is forbidden.
ROLLOUT_HEADING = "## 6A. Обязательный rollout PR-7.1: service-category filter"
SMOKE_HEADING = "### Контролируемый smoke rollout"


def _rollout() -> str:
    """The PR-7.1 rollout region, excluding the recovery subsection."""
    text = ACTIVATION_RUNBOOK.read_text()
    start = text.index(ROLLOUT_HEADING)
    return text[start : text.index(RECOVERY_HEADING, start)]


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


def _first_instruction_offset(section: str, token: str) -> int:
    """Offset of the first FENCED block containing *token*.

    Prose necessarily names the flags it forbids, so ordering assertions have to
    look at what the operator actually applies, not at the warning above it.
    """
    offset = 0
    for index, part in enumerate(section.split("```")):
        if index % 2 == 1 and token in part:
            return offset
        offset += len(part) + 3
    raise AssertionError(f"no fenced block contains {token}")


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
    assert "exact_jobs=0" in level_b, "the disallowed smoke must require the ABSENCE of an exact job"
    assert "exact_jobs=1" in level_b, "the allowed smoke must require exactly one event-specific job"
    # The job must belong to the right record and company, not merely exist.
    for field in ("job_type_matches_event", "job_company_matches_record", "job_record_matches_booking"):
        assert field in level_b


def test_the_audit_command_is_read_only_and_leaks_nothing() -> None:
    audit = _section(_runbook(), LEVEL_A_HEADING) + _section(_runbook(), LEVEL_B_HEADING)

    for block in _code_blocks(audit):
        for mutation in ("UPDATE ", "DELETE ", "INSERT ", "replay"):
            assert mutation not in block, f"the audit must stay read-only, found {mutation}"
    assert any("run --rm --no-deps" in block for block in _code_blocks(audit)), (
        "the audit must run in a one-shot container"
    )
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


# ---------------------------------------------------------------------------
# The PR-7.1 rollout itself must not open the destructive window
# ---------------------------------------------------------------------------


def test_the_rollout_never_instructs_the_operator_to_disable_notifications() -> None:
    """The recovery section forbade this pair; the rollout used to prescribe it.

    With a live webhook feed even a short deploy window is enough: the worker
    claims new captured events, terminalizes them `processed` without a job, and
    nothing replays them afterwards.
    """
    rollout = _rollout()

    for block in _code_blocks(rollout):
        assert "EASYWEEK_NOTIFICATIONS_ENABLED=false" not in block, (
            "the PR-7.1 rollout must never carry an executable notifications=false step"
        )
    assert "EASYWEEK_NOTIFICATIONS_ENABLED` остаётся `true`" in rollout
    assert "Запрещённая пара" in rollout


def test_the_rollout_fences_with_processing_before_touching_the_inbox_worker() -> None:
    rollout = _rollout()

    fence = _first_instruction_offset(rollout, "EASYWEEK_PROCESSING_ENABLED=false")
    first_recreate = _first_instruction_offset(rollout, "up -d --force-recreate")

    assert fence < first_recreate, "the write fence must be set before the first worker recreate"


def test_processing_returns_to_true_only_after_deploy_config_and_consumers() -> None:
    """Order is the whole safety property, so it is asserted as an order."""
    rollout = _rollout()

    allowlist = _first_instruction_offset(rollout, 'EASYWEEK_ALLOWED_SERVICE_CATEGORIES=["Wimpernverlängerung"]')
    deploy = rollout.index("Развернуть новый код")
    probe = rollout.index("effective-конфигурацию одноразовым контейнером")
    outbox = _first_instruction_offset(rollout, "up -d --force-recreate altegio-outbox-worker")
    unfence = _first_instruction_offset(rollout, "EASYWEEK_PROCESSING_ENABLED=true")

    assert allowlist < deploy < probe < outbox < unfence, (
        "processing may only return to true after allowlist, deploy, probe and consumer recreate"
    )


def test_the_rollout_does_not_route_the_operator_into_the_domain_only_drain() -> None:
    rollout = _rollout()

    # It may NAME the drain to say it is out of scope, but must not prescribe it.
    assert "не ссылается" in rollout or "не является ни rollout" in rollout


def test_the_forbidden_pair_is_exactly_the_mode_that_claims(
    _invalid_allowlist: None,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Runtime-bound: why the rollout order above is not merely stylistic."""
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", VALID_ALLOWLIST, raising=False)

    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)
    assert processing_is_configured() is True, (
        "processing=true + notifications=false lets the worker claim and terminalize captured "
        "events without jobs — the mode the rollout must never enter"
    )

    monkeypatch.setattr(settings, "easyweek_processing_enabled", False, raising=False)
    for notifications in (True, False):
        monkeypatch.setattr(settings, "easyweek_notifications_enabled", notifications, raising=False)
        assert processing_is_configured() is False, "processing=false is the fence, regardless of notifications"


# ---------------------------------------------------------------------------
# Level B: a Resend can never be the positive smoke
# ---------------------------------------------------------------------------


def test_level_a_still_treats_a_resend_as_successful_deduplication() -> None:
    """Historical grouping is correct and must survive the Level B tightening."""
    level_a = _section(_runbook(), LEVEL_A_HEADING)

    assert "resend_groups" in level_a
    assert "дедупликация" in level_a

    booking = uuid.UUID("aaaaaaaa-bbbb-4ccc-8ddd-000000000021")
    kwargs = {
        "event_hint": "booking-created",
        "booking_uuid": booking,
        "payload_hash": "identical",
        "job_type": "record_created",
    }
    assert easyweek_job_dedupe_key(**kwargs) == easyweek_job_dedupe_key(**kwargs), (
        "byte-identical deliveries share one key — one job for the group is correct"
    )


def test_level_b_forbids_resend_as_the_positive_smoke() -> None:
    """Exactly why: the same key resolves to a job created before the outage."""
    level_b = _section(_runbook(), LEVEL_B_HEADING)

    assert "Resend запрещён" in level_b
    for block in _code_blocks(level_b):
        assert "Resend" not in block, "no smoke step may instruct the operator to Resend"
    # The prohibition has to hold in the steps too, not only in the warning:
    # an "or Resend it" aside would reopen the false green in plain prose.
    for offer in ("сделать Resend", "или Resend", "либо Resend"):
        assert offer not in level_b, f"Level B must not offer Resend as an option: {offer}"


def test_level_b_requires_a_brand_new_booking_identity() -> None:
    level_b = _section(_runbook(), LEVEL_B_HEADING)

    assert "новый `booking_uuid`" in level_b
    assert "distinct_bookings=2" in level_b, "allowed and disallowed must use different new bookings"
    assert "booking_first_seen_here=true" in level_b, "the freshness axis that a Resend cannot satisfy"


def test_level_b_records_a_baseline_before_the_smoke() -> None:
    level_b = _section(_runbook(), LEVEL_B_HEADING)

    assert "MAX(id)" in level_b and "FROM easyweek_events" in level_b
    assert "smoke_event_id_baseline" in level_b
    assert "newer_than_baseline=true" in level_b


def test_level_b_positive_proof_is_a_job_created_after_smoke_start() -> None:
    level_b = _section(_runbook(), LEVEL_B_HEADING)

    assert "job_created_after_smoke_start=true" in level_b
    assert "smoke-start" in level_b, "the command must pass the recorded instant"
    assert "exact_jobs=1" in level_b


def test_level_b_disallowed_smoke_requires_no_exact_job() -> None:
    level_b = _section(_runbook(), LEVEL_B_HEADING)

    assert "exact_jobs=0" in level_b
    assert "outbox_rows=0" in level_b


def test_level_b_gates_on_specific_event_ids_not_on_the_aggregate() -> None:
    """Live traffic lands in the same window; the aggregate proves nothing."""
    level_b = _section(_runbook(), LEVEL_B_HEADING)

    assert "<allowed_event_id>" in level_b and "<disallowed_event_id>" in level_b
    assert "groups_with_exact_job" in level_b and "главным gate быть не может" in level_b


def test_the_smoke_verifier_is_bound_to_the_production_key_and_fails_closed() -> None:
    """The documented mode has to exist in code, with the documented fields."""
    level_b = _section(_runbook(), LEVEL_B_HEADING)

    assert "--smoke-event-id" in level_b
    assert "--baseline-event-id" in level_b
    assert "altegio_bot.scripts.easyweek_recovery_audit" in level_b

    signature = inspect.signature(verify_controlled_smoke)
    assert {"event_ids", "baseline_event_id", "smoke_start"} <= set(signature.parameters)

    source = inspect.getsource(verify_controlled_smoke)
    assert "easyweek_job_dedupe_key(" in source, "the smoke must reuse the production key, not a copy"
    assert "sha256" not in source.lower()
    for guard in ("not found", "no booking uuid", "no lifecycle hint"):
        assert guard in source, f"fail-closed guard missing: {guard}"


# ---------------------------------------------------------------------------
# The rollout must close the SECOND producer, not just the planner
# ---------------------------------------------------------------------------


def test_the_rollout_names_delivery_retry_as_the_second_producer() -> None:
    rollout = _rollout()

    assert "_handle_failed_delivery_status" in rollout
    assert "altegio-whatsapp-inbox-worker" in rollout
    assert "OUTBOX_DELIVERY_RETRY_ENABLED" in rollout
    for job_type in ("record_created", "record_updated", "record_canceled"):
        assert job_type in rollout, f"the affected retry scope must name {job_type}"


def test_the_retry_producer_is_stopped_before_the_final_queue_gate() -> None:
    """A queue read while the producer runs is a snapshot, not a fence."""
    rollout = _rollout()

    stop = _first_instruction_offset(rollout, "stop altegio-whatsapp-inbox-worker")
    final_gate = rollout.index("Только теперь — финальный queue gate")

    assert stop < final_gate, "the producer must be closed before the queue is declared empty"


def test_the_new_outbox_starts_before_the_retry_producer_returns() -> None:
    """Late callbacks must land in the guarded image, never the old one."""
    rollout = _rollout()

    new_outbox = _first_instruction_offset(rollout, "up -d --force-recreate altegio-outbox-worker")
    producer_back = _first_instruction_offset(rollout, "up -d --force-recreate altegio-whatsapp-inbox-worker")

    assert new_outbox < producer_back


def test_processing_returns_to_true_only_after_the_producer_is_restored() -> None:
    rollout = _rollout()

    producer_back = _first_instruction_offset(rollout, "up -d --force-recreate altegio-whatsapp-inbox-worker")
    unfence = _first_instruction_offset(rollout, "EASYWEEK_PROCESSING_ENABLED=true")

    assert producer_back < unfence


def test_the_controlled_smoke_runs_only_after_the_fence_is_lifted() -> None:
    rollout = _rollout()

    unfence = _first_instruction_offset(rollout, "EASYWEEK_PROCESSING_ENABLED=true")
    smoke = rollout.index(SMOKE_HEADING)

    assert unfence < smoke


def test_the_rollout_admits_the_shared_worker_pause() -> None:
    """The WhatsApp worker is shared, so Altegio is affected — say so."""
    rollout = _rollout()

    assert "Altegio" in rollout
    assert "whatsapp_events" in rollout, "the doc must state that webhooks are still persisted"


def test_the_rollout_mutates_no_production_rows() -> None:
    rollout = _rollout()

    for block in _code_blocks(rollout):
        for mutation in ("UPDATE ", "DELETE ", "INSERT "):
            assert mutation not in block, f"the rollout must not mutate production data: {mutation}"
    assert "replay" not in rollout.lower() or "не" in rollout


def test_the_rollout_uses_real_compose_services_and_the_production_file_set() -> None:
    import yaml

    rollout = _rollout()
    services = set(yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())["services"])

    for named in ("altegio-easyweek-inbox-worker", "altegio-outbox-worker", "altegio-whatsapp-inbox-worker"):
        assert named in rollout
        assert named in services, f"{named} is not a real compose service"
    assert "$COMPOSE" in rollout


# ---------------------------------------------------------------------------
# Outbox presence is not proof of sending
# ---------------------------------------------------------------------------


def test_the_smoke_gates_require_proven_delivery_not_a_row_count() -> None:
    for section in (_section(_rollout(), SMOKE_HEADING), _section(_runbook(), LEVEL_B_HEADING)):
        assert "outbox_delivery_proven" in section
        assert "outbox_rows=1" in section or "| `outbox_rows` | ровно `1` |" in section
        for landed in ("sent", "delivered", "read"):
            assert landed in section, f"the successful progression must list {landed}"
        for pending in ("queued", "sending"):
            assert pending in section
        for not_green in ("failed", "unknown"):
            assert not_green in section
        assert "STOP" in section


def test_no_smoke_gate_calls_a_bare_row_count_proof_of_sending() -> None:
    """The exact false claim: "one row means it reached sent"."""
    for section in (_section(_rollout(), SMOKE_HEADING), _section(_runbook(), LEVEL_B_HEADING)):
        assert "одна доставка, дошедшая до `sent`" not in section
        assert "доказательством отправки не" in section or "разные доказательства" in section


def test_the_disallowed_gate_stays_absence_based() -> None:
    for section in (_section(_rollout(), SMOKE_HEADING), _section(_runbook(), LEVEL_B_HEADING)):
        assert "exact_jobs=0" in section or "| `exact_jobs` | `0` |" in section
        assert "outbox_rows=0" in section or "| `outbox_rows` | `0` |" in section
        assert "none" in section, "absence must have neutral semantics, not a failure"


def test_the_delivered_statuses_are_taken_from_the_audit_module() -> None:
    """Document and code must not drift on what counts as delivered."""
    assert OUTBOX_DELIVERED_STATUSES == frozenset({"sent", "delivered", "read"})
    assert OUTBOX_PENDING_STATUSES == frozenset({"queued", "sending"})
    assert not (OUTBOX_DELIVERED_STATUSES & OUTBOX_PENDING_STATUSES)
