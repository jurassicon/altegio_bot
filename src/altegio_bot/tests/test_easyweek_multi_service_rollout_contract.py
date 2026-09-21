"""§38.9: the source-controlled rollout contract — plan, runbook and wiring.

The defect this part guards against is not a wrong value, it is a runbook that
looks like it changes one. A dotenv block followed by ``docker compose up -d``
recreates the worker from a file nobody edited, so the flag never moves and the
verification that follows describes the old configuration.
"""

from __future__ import annotations

import inspect
from pathlib import Path

import pytest

from altegio_bot.easyweek_multi_service_rollout import parse_multi_service_canary_job_id
from altegio_bot.scripts import easyweek_env_set
from altegio_bot.scripts import easyweek_multi_service_release_audit as audit
from altegio_bot.scripts import easyweek_multi_service_release_verify as verify
from altegio_bot.settings import Settings
from altegio_bot.workers import outbox_worker as ow

ROOT = Path(__file__).resolve().parents[3]
PLAN = ROOT / "docs/easyweek/INTEGRATION_PLAN.md"
RUNBOOK = ROOT / "docs/easyweek/pr7_4_two_service_notifications_runbook.md"
ENV_EXAMPLE = ROOT / "easyweek.env.example"
SOURCE_ROOT = ROOT / "src" / "altegio_bot"

_PLAN_PRESENT = pytest.mark.skipif(not PLAN.exists(), reason="INTEGRATION_PLAN.md is untracked (.gitignore)")

SECTION_TITLE = "# §38.9 — управляемое открытие общего send fence (шаги 43–59)"
STEPS = (
    "## 43. (A) Deploy при закрытом send fence и пустом canary",
    "## 44. Аудит фактической конфигурации обоих workers",
    "## 45. Structural multi-service preflight",
    "## 46. Общий reminder preflight",
    "## 47. Полный release audit всей очереди",
    "## 48. Выбор ровно одной canary job",
    "## 49. Dry-run проверка canary readiness",
    "## 50. (B) Установка send=true и точного canary ID",
    "## 51. Пересоздание только outbox worker",
    "## 52. Проверка effective значений внутри пересозданного контейнера",
    "## 53. Проверка исхода canary",
    "## 54. (C) Немедленный возврат send=false и подтверждение rollback",
    "## 55. Повторный полный release audit",
    "## 56. Явное решение по каждому оставшемуся provider candidate",
    "## 57. (D) Bulk открытие после утверждённого inventory",
    "## 58. Post-open verification фактических исходов",
    "## 59. (E) Аварийный rollback",
)


def _section() -> str:
    text = RUNBOOK.read_text(encoding="utf-8")
    assert SECTION_TITLE in text, "the runbook has no §38.9 rollout part"
    return text.split(SECTION_TITLE, 1)[1]


def _fenced(text: str) -> list[str]:
    blocks: list[str] = []
    collecting = False
    current: list[str] = []
    for line in text.splitlines():
        if line.startswith("```"):
            if collecting:
                blocks.append("\n".join(current))
                current = []
            collecting = not collecting
            continue
        if collecting:
            current.append(line)
    return blocks


# ===========================================================================
# The canonical plan
# ===========================================================================


@_PLAN_PRESENT
def test_the_plan_records_the_owner_approval_and_every_narrow_rule() -> None:
    text = PLAN.read_text(encoding="utf-8")
    # Flattened: the plan is hard-wrapped, so a required phrase legitimately
    # spans two lines.
    section = " ".join(text.split("### 38.9 Ревизия 41", 1)[1].split())
    for required in (
        "20.09.2026",
        "default-deny",
        "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID",
        "structural preflight",
        "release audit",
        "удалённых",
        "не гарантируют ровно одну provider attempt",
        "claim predicate",
        "race guard",
        "не деградирует",
        "отдельное решение об открытии всей очереди",
        "автоматическую отмену",
        "решение принимает оператор",
        "не является командой",
        "printenv",
        "canary ID не является аварийным стопом",
        "В runtime-код они не попадают",
    ):
        assert required in section, required


@_PLAN_PRESENT
def test_the_plan_does_not_weaken_the_existing_proof() -> None:
    section = " ".join(PLAN.read_text(encoding="utf-8").split("### 38.9 Ревизия 41", 1)[1].split())
    assert "не ослабляется" in section
    for forbidden in ("Nagelservice в allowlist", "отключить live proof", "digest можно игнорировать"):
        assert forbidden not in section


# ===========================================================================
# The runbook sequence
# ===========================================================================


def test_the_runbook_pins_all_seventeen_steps_in_order() -> None:
    section = _section()
    positions = []
    for step in STEPS:
        assert step in section, f"missing rollout step: {step}"
        positions.append(section.index(step))
    assert positions == sorted(positions), "the rollout steps are out of order"
    assert len(STEPS) == 17


def test_every_opening_and_rollback_actually_edits_the_env_file() -> None:
    """The defect this whole section exists for."""
    section = _section()
    for step in ("## 43.", "## 50.", "## 54.", "## 57.", "## 59."):
        block = section.split(step, 1)[1].split("\n## ", 1)[0]
        assert "easyweek_env_set.py" in block, f"{step} recreates a container without editing easyweek.env"
        assert "--env-file /opt/altegio_bot/easyweek.env" in block, step


def test_every_recreate_is_followed_by_an_effective_printenv_check() -> None:
    section = _section()
    for step in ("## 52.", "## 54.", "## 57.", "## 59."):
        block = section.split(step, 1)[1].split("\n## ", 1)[0]
        assert "printenv EASYWEEK_MULTI_SERVICE_SEND_ENABLED" in block, step


def test_the_canary_phase_comes_before_the_bulk_phase() -> None:
    section = _section()
    assert section.index("## 50.") < section.index("## 57.")
    canary_block = section.split("## 50.", 1)[1].split("\n## ", 1)[0]
    assert "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=CANARY_JOB_ID" in canary_block
    bulk_block = section.split("## 57.", 1)[1].split("\n## ", 1)[0]
    assert "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=" in bulk_block
    assert "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=CANARY_JOB_ID" not in bulk_block


def test_a_full_release_audit_precedes_both_openings() -> None:
    section = _section()
    assert section.index("## 47.") < section.index("## 50.")
    assert section.index("## 55.") < section.index("## 57.")
    for step in ("## 47.", "## 55.", "## 58."):
        block = section.split(step, 1)[1].split("\n## ", 1)[0]
        assert "easyweek_multi_service_release_audit" in block, step


def test_the_emergency_rollback_is_the_send_flag_and_not_the_canary_id() -> None:
    block = _section().split("## 59.", 1)[1]
    assert "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false" in block
    assert "не является" in block and "аварийным стопом" in block
    assert "--set EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID" not in block


def test_the_runbook_never_promises_one_attempt_after_the_restriction_is_removed() -> None:
    bulk = _section().split("## 57.", 1)[1].split("\n## ", 1)[0]
    flattened = " ".join(bulk.split())
    assert "«ровно одна provider attempt» **недействительно**" in flattened
    assert "ровно один Outbox" not in flattened


def test_every_compose_command_names_both_production_files() -> None:
    for block in _fenced(_section()):
        for line in block.splitlines():
            if "docker compose" in line:
                assert "-f docker-compose.yml" in line, line
                assert "-f docker-compose.chatwoot-internal.yml" in line, line


def test_command_blocks_carry_commands_only() -> None:
    for block in _fenced(_section()):
        for line in block.splitlines():
            stripped = line.strip()
            if stripped:
                assert not stripped.startswith("#"), f"a comment in a paste-clean block: {stripped}"


def test_the_mandatory_flag_audit_covers_both_workers() -> None:
    block = _section().split("## 44.", 1)[1].split("\n## ", 1)[0]
    assert "altegio-easyweek-inbox-worker" in block
    assert "altegio-outbox-worker" in block
    for flag in (
        "EASYWEEK_NOTIFICATIONS_ENABLED",
        "EASYWEEK_REMINDERS_ENABLED",
        "EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED",
        "EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED",
        "EASYWEEK_REMINDER_API_GUARD_ENABLED",
        "EASYWEEK_MULTI_SERVICE_SEND_ENABLED",
        "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID",
    ):
        assert flag in block, flag


def test_the_superseded_sections_point_at_the_controlled_sequence() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    superseded = text.split("## 40. Открытие send fence", 1)[1].split(SECTION_TITLE, 1)[0]
    assert "43–59" in superseded
    assert "шагу 54" in superseded and "шагу 59" in superseded


# ===========================================================================
# Wiring
# ===========================================================================


def test_the_new_setting_ships_unrestricted_and_typed_as_a_string() -> None:
    field = Settings.model_fields["easyweek_multi_service_canary_job_id"]
    assert field.annotation is str
    assert field.default == ""
    assert ENV_EXAMPLE.read_text(encoding="utf-8").count("EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=\n") == 1


def test_the_canary_variable_is_parsed_in_exactly_one_place() -> None:
    """Claim and send cannot disagree if only one module reads the variable."""
    readers = []
    for path in SOURCE_ROOT.rglob("*.py"):
        if "tests" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        if "easyweek_multi_service_canary_job_id" in text:
            readers.append(path.relative_to(SOURCE_ROOT).as_posix())
    assert sorted(readers) == ["easyweek_multi_service_rollout.py", "settings.py"]


def test_the_claim_predicate_and_the_race_guard_share_the_resolver() -> None:
    claim = inspect.getsource(ow._lock_next_jobs)
    assert "multi_service_canary()" in claim
    assert "MessageJob.id == _multi_canary.job_id" in claim

    per_job = inspect.getsource(ow._easyweek_multi_service_job_blocked)
    assert "_easyweek_multi_service_fence_reason()" in per_job
    assert "multi_service_canary()" in per_job

    logic = inspect.getsource(ow._run_job_logic)
    assert "_easyweek_multi_service_job_blocked(job)" in logic


def test_a_malformed_canary_holds_the_queue_at_the_fence_level() -> None:
    fence = inspect.getsource(ow._easyweek_multi_service_fence_reason)
    assert "unavailable_reason" in fence
    assert parse_multi_service_canary_job_id("1,2").unavailable_reason is not None


def test_the_deleted_record_allowlist_has_exactly_one_definition() -> None:
    assert ow.DELETED_RECORD_ALLOWED_JOB_TYPES == frozenset({"record_canceled", "comeback_3d"})
    guard = inspect.getsource(ow._run_job_logic)
    assert "job.job_type in DELETED_RECORD_ALLOWED_JOB_TYPES" in guard
    assert "DELETED_RECORD_ALLOWED_JOB_TYPES" in inspect.getsource(audit._classify)


def test_the_release_audit_has_no_write_or_send_capability() -> None:
    source = inspect.getsource(audit)
    assert "select(" in source
    for forbidden in (
        ".commit(",
        ".add(",
        ".delete(",
        "insert(",
        "safe_send",
        "ChatwootClient(",
        "get_booking",
        "list_location_services",
        "cancel_booking",
    ):
        assert forbidden not in source, forbidden


def test_the_release_audit_selects_jobs_without_any_record_side_filter() -> None:
    source = inspect.getsource(audit.select_open_pair_jobs)
    assert "Record.is_deleted" not in source
    assert "Record.starts_at" not in source
    assert "MessageJob.payload.op" in source


def test_the_backup_and_temp_files_can_never_be_committed() -> None:
    """The backup is a FULL copy of the secrets file, next to the original.

    ``.gitignore`` ignores ``easyweek.env`` by exact name, which does not
    cover ``easyweek.env.bak.<timestamp>`` — so without these two patterns the
    first operator to run the helper on the production host would find a
    world-visible copy of the API key waiting in ``git status``.
    """
    rules = (ROOT / ".gitignore").read_text(encoding="utf-8").splitlines()
    assert "easyweek.env.bak.*" in rules
    assert ".easyweek.env.*" in rules
    assert "easyweek.env" in rules
    assert "!easyweek.env.example" not in rules, "the template is not matched by those patterns"


def test_the_env_helper_cannot_touch_a_secret() -> None:
    assert "EASYWEEK_API_KEY" not in easyweek_env_set.ALLOWED_KEYS
    assert "EASYWEEK_WEBHOOK_SECRET" not in easyweek_env_set.ALLOWED_KEYS
    source = inspect.getsource(easyweek_env_set)
    assert "os.replace(" in source, "the write must be atomic"
    assert "S_IMODE" in source, "the original file mode must survive"


# ===========================================================================
# §38.9 follow-up: bootstrap, due-only canary, and the producer boundary
# ===========================================================================

BOUNDARY_STEPS = (
    "## 56a. Приостановка producer: EasyWeek inbox worker",
    "## 56b. Финальный release audit при остановленном producer",
    "## 57. (D) Bulk открытие после утверждённого inventory",
    "## 58. Post-open verification фактических исходов",
    "## 58a. Возврат EasyWeek inbox worker в работу",
    "## 59. (E) Аварийный rollback",
)


def test_step_43_bootstraps_the_canary_key_before_it_sets_anything() -> None:
    """The production file predates the key, so --set alone would always refuse."""
    block = _section().split("## 43.", 1)[1].split("\n## ", 1)[0]
    assert "--bootstrap-canary-key" in block
    assert block.index("--bootstrap-canary-key") < block.index("--dry-run")
    assert block.index("--dry-run") < block.index("--set EASYWEEK_NOTIFICATIONS_ENABLED=true")
    assert "already present" in block
    assert "expected zero or one" in block
    # The verification half of the step is still mandatory.
    assert "шаг 44" in block


def test_step_49_names_the_not_due_stop_condition() -> None:
    block = _section().split("## 49.", 1)[1].split("\n## ", 1)[0]
    for code in (
        "multi_service_canary_job_not_found",
        "multi_service_canary_job_not_due",
        "multi_service_canary_job_mismatch",
    ):
        assert code in block, code


def test_the_producer_is_paused_and_restarted_in_order() -> None:
    section = _section()
    positions = [section.index(step) for step in BOUNDARY_STEPS]
    assert positions == sorted(positions), "the producer boundary steps are out of order"

    pause = section.split("## 56a.", 1)[1].split("\n## ", 1)[0]
    assert "stop altegio-easyweek-inbox-worker" in pause
    assert "ps -a altegio-easyweek-inbox-worker altegio-outbox-worker altegio-api" in pause

    resume = section.split("## 58a.", 1)[1].split("\n## ", 1)[0]
    assert "start altegio-easyweek-inbox-worker" in resume


def test_the_final_audit_is_bound_to_the_approved_digest() -> None:
    section = _section()
    assert "release_set_digest" in section.split("## 47.", 1)[1].split("\n## ", 1)[0]
    final = section.split("## 56b.", 1)[1].split("\n## ", 1)[0]
    assert "--expect-release-digest RELEASE_DIGEST" in final
    assert "multi_service_release_set_changed" in final
    # It runs BEFORE the bulk opening, and after the producer is paused.
    assert section.index("## 56a.") < section.index("## 56b.") < section.index("## 57.")


def test_the_rollback_always_returns_the_producer_to_a_running_state() -> None:
    """A paused inbox worker must not outlive the incident."""
    rollback = _section().split("## 59.", 1)[1]
    assert "start altegio-easyweek-inbox-worker" in rollback
    assert "ps -a altegio-easyweek-inbox-worker altegio-outbox-worker altegio-api" in rollback
    resume = _section().split("## 58a.", 1)[1].split("\n## ", 1)[0]
    assert "включая аварийный выход" in resume


def test_the_runbook_states_why_pausing_the_producer_loses_no_delivery() -> None:
    pause = " ".join(_section().split("## 56a.", 1)[1].split("\n## ", 1)[0].split())
    assert "altegio-api" in pause
    assert "captured" in pause
    assert "SIGTERM" in pause
    assert "не теряет доставки" in pause


def test_the_runbook_still_forbids_touching_jobs_by_hand() -> None:
    section = _section()
    flattened = " ".join(section.split())
    assert "Jobs при этом не удалять" in flattened
    assert "`attempts`, `run_at` и payload не править" in flattened


def test_the_bootstrap_is_one_key_one_value_and_never_a_generic_append() -> None:
    assert easyweek_env_set.BOOTSTRAP_KEY == "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID"
    assert easyweek_env_set.BOOTSTRAP_VALUE == ""
    assert easyweek_env_set.BOOTSTRAP_KEY in easyweek_env_set.ALLOWED_KEYS
    signature = inspect.signature(easyweek_env_set.bootstrap_canary_key)
    assert list(signature.parameters) == ["path", "dry_run"]
    source = inspect.getsource(easyweek_env_set.bootstrap_canary_key)
    assert "_backup(" in source and "_atomic_write(" in source and "S_IMODE" in source


def test_the_due_only_canary_rule_lives_in_the_audit() -> None:
    source = inspect.getsource(audit._canary_error)
    assert "future_provider_candidate_job_ids" in source
    assert "MULTI_SERVICE_CANARY_JOB_NOT_DUE" in source
    assert source.index("MULTI_SERVICE_CANARY_JOB_NOT_DUE") < source.index("MULTI_SERVICE_CANARY_JOB_NOT_FOUND")


def test_the_release_digest_covers_the_job_ids_and_nothing_else() -> None:
    source = inspect.getsource(audit.ReleaseAuditReport.release_set_digest.fget)
    assert "sha256" in source
    assert "provider_candidate_job_ids" in source and "future_provider_candidate_job_ids" in source
    # Classification and due/future split must NOT enter the digest: both move
    # on their own and would make an unchanged set look changed.
    assert "classifications" not in source
    assert "phase" not in source


def test_the_capture_endpoint_is_independent_of_the_inbox_worker() -> None:
    """What makes pausing the producer a safe boundary rather than data loss."""
    from altegio_bot.webhooks import easyweek as capture

    source = inspect.getsource(capture)
    assert "easyweek_inbox_worker" not in source
    assert "session.commit()" in source
    assert "status_code=503" in source


# ===========================================================================
# §38.9 step 58: post-open verification of what actually went out
# ===========================================================================


def test_step_58_runs_the_verifier_and_not_the_pre_open_audit_as_proof() -> None:
    block = _section().split("## 58. ", 1)[1].split("\n## ", 1)[0]
    assert "easyweek_multi_service_release_verify" in block
    assert "--due-job-ids DUE_IDS" in block
    assert "--future-job-ids FUTURE_IDS" in block
    assert "--opened-at OPENED_AT" in block
    assert "--settle-sec" in block

    flattened = " ".join(block.split())
    assert "Пустой open release set **не является** доказательством отправки" in flattened
    # The audit may still be run, but only as a look at what is left.
    assert "вспомогательная проверка остатка, а не доказательство отправки" in flattened


def test_step_58_requires_a_proven_outbox_row_for_every_approved_due_job() -> None:
    flattened = " ".join(_section().split("## 58. ", 1)[1].split("\n## ", 1)[0].split())
    assert "due_outcomes.succeeded == approved_due" in flattened
    assert "sent`/`delivered`/`read`" in flattened
    assert "unapproved_sent_job_ids=[]" in flattened
    assert "future_problem_job_ids=[]" in flattened


def test_step_58_treats_a_first_in_flight_look_as_normal() -> None:
    """The false red that used to start rollbacks mid-delivery."""
    flattened = " ".join(_section().split("## 58. ", 1)[1].split("\n## ", 1)[0].split())
    assert "in_progress" in flattened
    assert "Это **не** rollout failure" in flattened
    assert "batch" in flattened
    # The timeout is a decision, not a verdict.
    assert "наблюдать дальше" in flattened and "закрыть fence" in flattened


def test_step_58_lists_every_unsuccessful_outcome_as_a_stop_condition() -> None:
    flattened = " ".join(_section().split("## 58. ", 1)[1].split("\n## ", 1)[0].split())
    for outcome in (
        "done_without_proven_outbox_send",
        "canceled_without_provider_send",
        "unknown_or_indeterminate",
        "missing",
        "identity_mismatch",
        "future_sent_early",
        "scan_truncated",
    ):
        assert outcome in flattened, outcome


def test_the_opening_records_its_time_marker_before_the_flag_changes() -> None:
    block = _section().split("## 57. ", 1)[1].split("\n## ", 1)[0]
    assert "date -u +%Y-%m-%dT%H:%M:%SZ" in block
    assert block.index("date -u") < block.index("EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true")


def test_the_producer_returns_only_after_a_verified_release() -> None:
    resume = _section().split("## 58a.", 1)[1].split("\n## ", 1)[0]
    assert "verified=true" in resume


def test_the_rollback_closes_the_fence_before_it_resumes_the_producer() -> None:
    rollback = " ".join(_section().split("## 59.", 1)[1].split())
    assert "сначала закрыть send fence" in rollback
    assert rollback.index("закрыть send fence") < rollback.index("вернуть inbox worker")


def test_the_verifier_is_read_only_and_cannot_send_or_roll_back() -> None:
    source = inspect.getsource(verify)
    assert "select(" in source
    for forbidden in (
        ".commit(",
        ".add(",
        ".delete(",
        "insert(",
        "safe_send",
        "ChatwootClient(",
        "easyweek_env_set",
        "get_booking",
    ):
        assert forbidden not in source, forbidden
    assert "await session.rollback()" in source


def test_the_verifier_reuses_the_project_success_statuses() -> None:
    """A second opinion about what "sent" means is how a rollout lies."""
    from altegio_bot.workers.outbox_worker import SUCCESS_OUTBOX_STATUSES

    assert verify.SUCCESS_OUTBOX_STATUSES is SUCCESS_OUTBOX_STATUSES
    assert set(SUCCESS_OUTBOX_STATUSES) == {"sent", "delivered", "read"}
    source = inspect.getsource(verify)
    assert '("sent", "delivered", "read")' not in source, "restated instead of imported"


def test_the_verifier_settle_loop_is_bounded() -> None:
    source = inspect.getsource(verify.verify_release)
    assert "while True" not in source
    assert "max_polls" in source
    assert verify.MAX_SETTLE_SEC <= 1800


def test_the_bulk_audit_tolerates_in_flight_rows_and_nothing_else() -> None:
    source = inspect.getsource(audit.ReleaseAuditReport.audit_sound.fget)
    assert "RolloutPhase.BULK.value" in source
    assert "in_flight" in source
    assert "UNSAFE_OR_UNPROVEN" in source, "an unprovable job still blocks every phase"


# ===========================================================================
# §38.9 step 58: the all-future inventory and the future lifecycle
# ===========================================================================


def _step_58() -> str:
    return _section().split("## 58. ", 1)[1].split("\n## ", 1)[0]


def test_step_58_offers_a_variant_for_an_inventory_with_no_due_jobs() -> None:
    """After a successful canary the remaining inventory is legitimately all-future."""
    block = _step_58()
    assert "**Вариант A — в inventory есть due jobs.**" in block
    assert "**Вариант B — due jobs нет, есть только future jobs.**" in block
    assert block.index("Вариант A") < block.index("Вариант B")

    variant_a = block.split("Вариант A", 1)[1].split("Вариант B", 1)[0]
    command_a = variant_a.split("```", 2)[1]
    assert "--due-job-ids DUE_IDS" in command_a
    assert "--allow-empty-due" not in command_a
    # And the prose says so explicitly, so it is not left to inference.
    assert "`--allow-empty-due` в варианте A не используется" in " ".join(variant_a.split())

    variant_b = block.split("Вариант B", 1)[1]
    command_b = variant_b.split("```", 2)[1]
    assert "--future-job-ids FUTURE_IDS" in command_b
    assert "--allow-empty-due" in command_b
    assert "--due-job-ids" not in command_b, "an empty placeholder is never passed"
    assert "config_error=null" in variant_b
    assert "empty_due_allowed=true" in variant_b


def test_step_58_stops_when_both_approved_lists_are_empty() -> None:
    flattened = " ".join(_step_58().split())
    assert "Если и due, и future списки пусты — STOP" in flattened
    assert "approved_inventory_empty" in flattened
    assert "Пустой inventory не является доказательством успешного rollout" in flattened


def test_the_runbook_uses_no_angle_bracket_placeholder_in_a_shell_block() -> None:
    """`<FOO>` in a shell block is a redirection, not a placeholder."""
    for block in _fenced(_section()):
        for line in block.splitlines():
            assert "<" not in line and ">" not in line, line


def test_step_58_names_every_red_future_outcome() -> None:
    flattened = " ".join(_step_58().split())
    for outcome in (
        "future_sent_early",
        "future_unexpected_state",
        "future_indeterminate",
        "future_matured_pending",
        "future_maturing_job_ids",
        "inventory_proven",
    ):
        assert outcome in flattened, outcome


def test_the_verifier_checks_the_effective_bulk_configuration_through_the_shared_resolver() -> None:
    source = inspect.getsource(verify.verify_release)
    assert "multi_service_configuration_error(RolloutPhase.BULK)" in source
    # Not a second copy of the flag rules.
    for forbidden in (
        "easyweek_multi_service_send_enabled",
        "easyweek_notifications_enabled",
        "easyweek_reminder_api_guard_enabled",
    ):
        assert forbidden not in inspect.getsource(verify), forbidden


def test_an_empty_inventory_can_never_be_proven() -> None:
    source = inspect.getsource(verify.ReleaseVerifyReport.inventory_proven.fget)
    assert "approved_due == 0 and self.approved_future == 0" in source
    assert "empty_due_allowed" in source
    assert "due_all_succeeded" in source and "future_all_accepted" in source


def test_the_future_classification_states_the_approved_state_positively() -> None:
    """A check that only looked for a success row called everything pending."""
    source = inspect.getsource(verify.classify_future_job)
    for expected in (
        "FUTURE_UNEXPECTED_STATE",
        "FUTURE_INDETERMINATE",
        "FUTURE_MATURED_PENDING",
        "REASON_FUTURE_CLAIMED_BEFORE_RUN_AT",
        "REASON_JOB_FAILED",
        "REASON_JOB_CANCELED",
        "REASON_DONE_WITHOUT_PROVEN_SEND",
        "REASON_UNRECOGNISED_JOB_STATUS",
    ):
        assert expected in source, expected
    assert verify.FUTURE_ACCEPTED == (verify.FUTURE_PENDING, verify.FUTURE_RELEASED_ON_SCHEDULE)


def test_future_and_due_outcomes_keep_separate_vocabularies_and_lists() -> None:
    fields = verify.ReleaseVerifyReport.__dataclass_fields__
    for name in ("due_outcomes", "future_outcomes", "pending_job_ids", "future_maturing_job_ids"):
        assert name in fields, name
    # The future vocabulary never borrows a due outcome name beyond the two
    # identity-level ones that mean the same thing on both sides.
    shared = set(verify.DUE_OUTCOMES) & set(verify.FUTURE_OUTCOMES)
    assert shared == {verify.MISSING, verify.IDENTITY_MISMATCH}
