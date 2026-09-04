"""PR-12: the operator contract, checked for meaning rather than for keywords.

A rollout document is executable only if it says the right things in the right
order. The dangerous mistakes it can encode are specific: opening the send fence
before the preflight is green, using `restart` (which does not re-read env_file)
where `--force-recreate` is required, rolling back planning before sending, or
treating an empty candidate set as permission.

So these tests assert ORDER and CONTENT, not the presence of a word: where two
instructions must be sequenced, the assertion is on their relative position in
the document.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from altegio_bot.easyweek_retention import RETENTION_HOLD_REASONS, RETENTION_SEND_REFUSAL_REASONS
from altegio_bot.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[3]
ENV_EXAMPLE = REPO_ROOT / "easyweek.env.example"
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
RUNBOOK = REPO_ROOT / "docs" / "easyweek" / "durlach_activation_runbook.md"

PLANNING_FLAG = "EASYWEEK_RETENTION_ENABLED"
SEND_FLAG = "EASYWEEK_RETENTION_SEND_ENABLED"
COUNTER_FLAG = "EASYWEEK_VISIT_COUNTER_ENABLED"
CANARY_FLAG = "EASYWEEK_RETENTION_CANARY_JOB_ID"
MASTER_FLAG = "EASYWEEK_NOTIFICATIONS_ENABLED"
INBOX_SERVICE = "altegio-easyweek-inbox-worker"
OUTBOX_SERVICE = "altegio-outbox-worker"
PREFLIGHT_MODULE = "altegio_bot.scripts.easyweek_retention_preflight"


def prose(text: str) -> str:
    """Strip comment markers and line wrapping so assertions read sentences."""
    unwrapped = [re.sub(r"^\s*#\s?", "", line) for line in text.splitlines()]
    return re.sub(r"\s+", " ", " ".join(unwrapped)).strip()


def position(text: str, needle: str) -> int:
    """Index of `needle`, with a readable failure when the step is missing."""
    assert needle in text, f"the document no longer contains: {needle}"
    return text.index(needle)


def bash_blocks(text: str) -> list[str]:
    """Contents of every ```bash fence — the lines an operator actually runs."""
    return [block.strip() for block in re.findall(r"```bash\n(.*?)```", text, flags=re.S)]


@pytest.fixture(scope="module")
def env_example() -> str:
    return ENV_EXAMPLE.read_text()


@pytest.fixture(scope="module")
def runbook() -> str:
    return RUNBOOK.read_text()


@pytest.fixture(scope="module")
def pr12_section(runbook: str) -> str:
    """Only the PR-12 chapter, bounded at BOTH ends.

    Slicing to the end of the file would be correct only while PR-12 happens to
    be the last chapter: the next chapter appended after it would silently be
    read as PR-12 prose.
    """
    start = runbook.index("## 16. PR-12")
    following = re.search(r"^## \d+\. ", runbook[start + 1 :], flags=re.M)
    end = start + 1 + following.start() if following else len(runbook)
    return runbook[start:end]


@pytest.fixture(scope="module")
def master_flag_shutdown(pr12_section: str) -> str:
    """Only the "turning the master flag OFF" procedure in §16.1.

    Bounded at both ends: the section that follows it is deliberately about
    ENABLING, which must not inherit this ordering, and an unbounded slice would
    let assertions about the shutdown pass on the enabling prose.
    """
    start = pr12_section.index("#### Выключение мастер-флага")
    end = pr12_section.index("#### Обратное включение")
    return pr12_section[start:end]


@pytest.fixture(scope="module")
def master_flag_resume(pr12_section: str) -> str:
    """Only the "resuming after a master-flag pause" procedure in §16.1.

    Bounded at both ends. Every other part of the chapter also talks about
    fences, preflights and recreates, so an unbounded slice would let these
    assertions pass on the shutdown procedure or on the §16.2 rollout — which is
    exactly the mistake that made the old enabling advice look safe.
    """
    start = pr12_section.index("#### Возобновление после паузы мастер-флага")
    end = pr12_section.index("Что PR-12 **не** добавляет")
    return pr12_section[start:end]


@pytest.fixture(scope="module")
def rollout_steps(pr12_section: str) -> str:
    """Only §16.2 — the numbered steps an operator executes in order.

    The surrounding sections explain WHY (16.1) and what to do when something
    goes wrong (16.3+); sequencing assertions belong to the steps themselves.
    """
    start = pr12_section.index("### 16.2")
    end = pr12_section.index("### 16.3")
    return pr12_section[start:end]


# ---------------------------------------------------------------------------
# The two flags
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("flag", [PLANNING_FLAG, SEND_FLAG])
def test_both_flags_default_to_false_in_settings(flag: str) -> None:
    """Production default is off for both; the code is the source of truth."""
    assert getattr(Settings.model_fields[flag.lower()], "default") is False


@pytest.mark.parametrize("flag", [PLANNING_FLAG, SEND_FLAG])
def test_each_flag_is_declared_exactly_once_and_false(env_example: str, flag: str) -> None:
    assignments = re.findall(rf"^{flag}=(.*)$", env_example, flags=re.M)
    assert assignments == ["false"], f"{flag} must be declared once, as false"


def test_the_example_documents_which_worker_reads_which_flag_at_runtime(env_example: str) -> None:
    """Recreating the wrong container is the classic way to flip nothing."""
    section = env_example[env_example.index("--- PR-12") :]
    planning_para = prose(section[: section.index(f"{PLANNING_FLAG}=false")])
    send_para = prose(section[section.index(f"{PLANNING_FLAG}=false") : section.index(f"{SEND_FLAG}=false")])

    assert INBOX_SERVICE in planning_para and OUTBOX_SERVICE not in planning_para
    assert OUTBOX_SERVICE in send_para and INBOX_SERVICE not in send_para
    for para in (planning_para, send_para):
        assert "runtime" in para.lower() or "long-running" in para.lower(), (
            "ownership is a runtime claim; say so, or the preflight looks like a contradiction"
        )


def test_the_example_keeps_the_flag_narrow_rather_than_calling_it_marketing(env_example: str) -> None:
    """A campaign-shaped name would authorise job types nobody reviewed."""
    section = prose(env_example[env_example.index("--- PR-12") :])

    assert "repeat_10d" in section and "comeback_3d" in section
    for deferred in ("newsletter", "promo", "campaign"):
        assert deferred in section.lower(), f"the example must say {deferred} stays out"
    assert not re.search(r"^EASYWEEK_(MARKETING|CAMPAIGNS?)_", env_example, flags=re.M), (
        "a general marketing flag would let a future job type inherit this authorisation"
    )


def test_the_example_names_the_counter_as_a_precondition(env_example: str) -> None:
    """Without PR-11 the repeat has no baseline and is never created."""
    section = prose(env_example[env_example.index("--- PR-12") :])

    assert COUNTER_FLAG in section
    assert "visits_total" in section


def test_the_example_separates_planning_from_sending(env_example: str) -> None:
    section = prose(env_example[env_example.index("--- PR-12") :])

    assert "send fence" in section.lower()
    # Both directions of independence are stated, not just one.
    assert "Закрытие fence НЕ отменяет уже созданные jobs" in section
    assert "выключение planning НЕ открывает fence" in section


def test_the_example_says_the_fence_costs_no_attempts(env_example: str) -> None:
    section = prose(env_example[env_example.index("--- PR-12") :])
    assert "не тратят attempts" in section


def test_the_example_says_altegio_is_not_governed_by_this_fence(env_example: str) -> None:
    """Same job type, different provider — and that has to be written down."""
    section = prose(env_example[env_example.index("--- PR-12") :])
    assert "Altegio repeat_10d / comeback_3d этим fence НЕ управляются" in section


def test_the_example_warns_that_exec_would_read_a_stale_environment(env_example: str) -> None:
    section = prose(env_example[env_example.index("--- PR-12") :])
    assert "нельзя запускать через `docker compose exec`" in section
    assert "config_error `retention_planning_disabled`" in section
    assert "свежий one-off контейнер" in section


def test_the_example_carries_no_real_secret(env_example: str) -> None:
    for pattern in (r"EASYWEEK_API_KEY=\S", r"EASYWEEK_WEBHOOK_SECRET=\S", r"token=[A-Za-z0-9]{8,}"):
        assert not re.search(pattern, env_example), pattern


# ---------------------------------------------------------------------------
# Compose wiring
# ---------------------------------------------------------------------------


def test_no_new_service_and_no_new_secret_distribution() -> None:
    """PR-12 needs neither: both workers already read easyweek.env."""
    services = yaml.safe_load(COMPOSE_FILE.read_text())["services"]

    def _reads_easyweek_env(name: str) -> bool:
        return any("easyweek.env" in str(entry) for entry in services[name].get("env_file", []) or [])

    assert _reads_easyweek_env(INBOX_SERVICE)
    assert _reads_easyweek_env(OUTBOX_SERVICE)

    with_easyweek_env = {name for name in services if _reads_easyweek_env(name)}
    assert with_easyweek_env <= {
        "altegio-api",
        INBOX_SERVICE,
        OUTBOX_SERVICE,
        "easyweek-legacy-retire",
        "easyweek-booking-migration",
        # PR-11.2 (plan §29). The preparation stage reads the EasyWeek catalogue
        # and the workspace's customers, so it needs the same key. Same narrowest
        # carrier: `ops` profile, `restart: "no"`, one command per container,
        # never started by `docker compose up -d`. It is admitted deliberately
        # and separately from the migrator, because it holds a different power —
        # it can create a CUSTOMER and cannot create a booking.
        "easyweek-migration-prepare",
    }, f"easyweek.env reached an unexpected service: {with_easyweek_env}"


def test_the_compose_comments_name_the_right_owner_for_each_flag() -> None:
    text = COMPOSE_FILE.read_text()
    inbox_block = text[text.index(f"  {INBOX_SERVICE}:") - 2000 : text.index(f"  {INBOX_SERVICE}:")]
    outbox_block = text[text.index(f"  {OUTBOX_SERVICE}:") - 2000 : text.index(f"  {OUTBOX_SERVICE}:")]

    assert PLANNING_FLAG in inbox_block
    assert SEND_FLAG in outbox_block


# ---------------------------------------------------------------------------
# Rollout order
# ---------------------------------------------------------------------------


# These four sequence the §16.2 ROLLOUT steps, so they read `rollout_steps`
# rather than the whole chapter. §16.1 legitimately mentions the same flags
# while describing the shutdown and the resume, and a chapter-wide `index()`
# would match those first and assert about the wrong procedure.


def test_planning_is_enabled_before_the_send_fence(rollout_steps: str) -> None:
    assert position(rollout_steps, f"{PLANNING_FLAG}=true") < position(rollout_steps, f"{SEND_FLAG}=true")


def test_the_counter_is_confirmed_before_planning_is_enabled(rollout_steps: str) -> None:
    """A repeat planned without a proven baseline is never created at all."""
    assert position(rollout_steps, COUNTER_FLAG) < position(rollout_steps, f"{PLANNING_FLAG}=true")


def test_the_preflight_runs_between_planning_and_the_send_fence(rollout_steps: str) -> None:
    assert (
        position(rollout_steps, f"{PLANNING_FLAG}=true")
        < position(rollout_steps, PREFLIGHT_MODULE)
        < position(rollout_steps, f"{SEND_FLAG}=true")
    )


def test_the_fence_may_only_open_on_a_green_preflight(pr12_section: str) -> None:
    text = prose(pr12_section)
    assert "ready=true" in text and "exit code 0" in text
    assert "STOP" in text


def test_an_empty_candidate_set_is_not_permission(pr12_section: str) -> None:
    text = prose(pr12_section)
    assert "candidate_count=0" in text
    assert "Пустое множество кандидатов разрешением на rollout не является" in text


def test_a_truncated_result_is_a_stop(pr12_section: str) -> None:
    assert "truncated=true" in prose(pr12_section)


def test_every_preflight_invocation_is_a_fresh_one_off_container(pr12_section: str) -> None:
    """The rollout runs it three times — before the canary, under the canary, and
    over the whole queue once the restriction is gone — and each must be a
    one-off container.

    A single canonical form is what stops an operator picking the wrong one, so
    the assertion is that every invocation is byte-identical rather than that
    there is only one.
    """
    commands = [block for block in bash_blocks(pr12_section) if PREFLIGHT_MODULE in block]
    assert len(commands) == 3, "queue audit, canary audit, and the full audit after the canary"
    assert len(set(commands)) == 1, "all three must be the same canonical command"
    words = commands[0].split()
    assert "run" in words and "--rm" in words and "--no-deps" in words
    assert "exec" not in words, "exec reuses a container created with the pre-rollout environment"
    assert "restart" not in words


def test_each_flag_change_is_followed_by_force_recreate_of_its_own_service(
    pr12_section: str, rollout_steps: str
) -> None:
    planning = rollout_steps[position(rollout_steps, f"{PLANNING_FLAG}=true") :]
    sending = rollout_steps[position(rollout_steps, f"{SEND_FLAG}=true") :]

    assert f"--force-recreate {INBOX_SERVICE}" in planning[:1200]
    assert f"--force-recreate {OUTBOX_SERVICE}" in sending[:1200]
    # Checked on the COMMANDS, not the prose: the section deliberately mentions
    # `docker compose restart` in order to say it is not enough.
    assert all("restart" not in block.split() for block in bash_blocks(pr12_section)), (
        "restart does not re-read env_file"
    )


def test_the_outbox_worker_is_not_recreated_before_the_preflight(rollout_steps: str) -> None:
    """Recreating it early would open the fence before the audit.

    Scoped to the ROLLOUT STEPS rather than the whole chapter. §16.1 documents
    the master flag's own recreate order, which names the outbox worker on
    purpose and is not a rollout step; asserting over the whole chapter would
    read that reference as a premature fence opening.
    """
    preflight_at = position(rollout_steps, PREFLIGHT_MODULE)
    earlier = rollout_steps[:preflight_at]
    assert f"--force-recreate {OUTBOX_SERVICE}" not in earlier


def test_a_byte_identical_resend_is_rejected_as_positive_proof(pr12_section: str) -> None:
    text = prose(pr12_section)
    assert "Байт-идентичный Resend старого события доказательством не является" in text


def test_the_canary_is_a_technical_restriction_not_a_promise(pr12_section: str) -> None:
    """ "Pick one job and hope the queue holds only it" is not a canary.

    The queue can grow between the preflight and the fence opening, so the
    restriction has to be mechanical — a named job id the worker enforces.
    """
    text = prose(pr12_section)
    assert CANARY_FLAG in text
    assert "message_jobs.id" in text
    assert "claim'ить и отправлять **только** эту job" in text
    assert "Остальные EasyWeek retention jobs остаются `queued`" in text


def test_the_canary_sequence_is_in_the_only_workable_order(rollout_steps: str) -> None:
    """canary preflight -> canary fence -> verify -> close -> unset -> preflight -> bulk.

    Every one of those steps is load-bearing, and the order is the whole safety
    argument: releasing the bulk queue before the restriction is removed and the
    full audit is green is exactly the blast the canary exists to prevent.
    """
    set_canary = position(rollout_steps, f"{CANARY_FLAG}=<message_jobs.id")
    # The audit that belongs to the canary is the one AFTER the restriction is
    # set; the earlier one audits the queue before a canary is even chosen.
    canary_preflight = rollout_steps.index(PREFLIGHT_MODULE, set_canary)
    open_for_canary = position(rollout_steps, f"{SEND_FLAG}=true")
    # Searched FORWARD from the canary send: `...=false` also appears in step 1,
    # where both flags are deployed shut, and that occurrence is not this step.
    close_again = rollout_steps.index(f"{SEND_FLAG}=false", open_for_canary)
    unset_canary = rollout_steps.index(f"`{CANARY_FLAG}=` (пусто)", close_again)
    full_preflight = rollout_steps.index(PREFLIGHT_MODULE, unset_canary)
    bulk = rollout_steps.index(f"{SEND_FLAG}=true", full_preflight)

    assert set_canary < canary_preflight < open_for_canary
    assert open_for_canary < close_again < unset_canary
    assert unset_canary < full_preflight < bulk
    assert canary_preflight < full_preflight, "two distinct audits, not one"


def test_an_invalid_canary_is_documented_as_fail_closed(pr12_section: str) -> None:
    text = prose(pr12_section)
    assert "retention_canary_job_id_invalid" in text
    assert "fail-closed" in text.lower()


def test_the_master_fence_is_documented_as_stopping_retention_sends_only(pr12_section: str) -> None:
    """It gates SENDING as well as planning — but only for retention.

    Both halves are load-bearing and they fail in opposite directions. Omitting
    the send half lets an operator recreate the wrong container and keep sending;
    overstating it as a general stop lets them believe EasyWeek is silent while
    queued lifecycle and reminder jobs are still going out.
    """
    text = prose(pr12_section)
    assert "Мастер-флаг останавливает и отправку — но только retention" in text
    assert "EASYWEEK_NOTIFICATIONS_ENABLED=false" in text
    assert "Это узкое расширение, а не глобальный send fence" in text
    for untouched in ("lifecycle", "reminder_24h", "review_3d", "Altegio"):
        assert untouched in text, f"the narrow scope must name {untouched} as unaffected"


def test_the_deadline_deadlock_has_a_documented_way_out(pr12_section: str) -> None:
    """Waiting for the bounded cleanup — never opening the fence or editing SQL."""
    text = prose(pr12_section)
    assert "deadline_expired" in text
    assert "bounded cleanup" in text.lower()
    assert "Открывать fence при `deadline_expired` и править строки SQL-командами запрещено" in text


def test_the_hold_codes_are_documented_as_holds_rather_than_refusals(pr12_section: str) -> None:
    """A held job is not a cancelled one, and an operator must not read it as one."""
    table = pr12_section[position(pr12_section, "### 16.5") :]
    for reason in RETENTION_HOLD_REASONS:
        assert f"`{reason}`" in table, f"undocumented hold reason: {reason}"
    assert "**не** отменяют job" in prose(table)


def test_the_frozen_service_identity_is_documented(pr12_section: str) -> None:
    text = prose(pr12_section)
    assert "retention_service_changed" in text
    assert "service_id" in text
    assert "Сравнивается только id" in text


def test_the_two_obligation_markers_are_documented_as_independent(pr12_section: str) -> None:
    text = prose(pr12_section)
    assert "Отметки раздельные" in text
    assert "задним числом сообщение не получает" in text


def test_sent_is_not_treated_as_final_delivery(pr12_section: str) -> None:
    text = prose(pr12_section)
    assert "delivered" in text and "read" in text
    assert "Статус `sent` финальным доказательством не считается" in text


def test_the_runbook_forbids_editing_production_rows_to_speed_up_the_smoke(pr12_section: str) -> None:
    text = prose(pr12_section)
    assert "не менять его SQL-командой" in text
    assert "не редактировать payload" in text


def test_the_runbook_says_old_events_are_not_replayed(pr12_section: str) -> None:
    text = prose(pr12_section)
    assert "Старые processed events автоматически не replay'ятся" in text
    assert "backfill" in text.lower()


# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------


def test_rollback_closes_sending_before_planning(pr12_section: str) -> None:
    rollback = pr12_section[position(pr12_section, "### 16.3") :]
    assert position(rollback, f"{SEND_FLAG}=false") < position(rollback, f"{PLANNING_FLAG}=false")


def test_rollback_never_deletes_or_bulk_updates(pr12_section: str) -> None:
    rollback = prose(pr12_section[position(pr12_section, "### 16.3") :])
    assert "Ничего не удалять" in rollback
    assert "массовый `DELETE`" in rollback or "массовый DELETE" in rollback


def test_rollback_keeps_the_proven_counter(pr12_section: str) -> None:
    rollback = prose(pr12_section[position(pr12_section, "### 16.3") :])
    assert COUNTER_FLAG in rollback
    assert "не удалять" in rollback


def test_rollback_leaves_the_master_notification_gate_alone(pr12_section: str) -> None:
    rollback = prose(pr12_section[position(pr12_section, "### 16.3") :])
    assert "`EASYWEEK_NOTIFICATIONS_ENABLED` **не** выключать" in rollback


def test_rollback_states_what_keeps_working(pr12_section: str) -> None:
    rollback = prose(pr12_section[position(pr12_section, "### 16.3") :])
    for survivor in ("capture", "lifecycle", "reminders", "Altegio"):
        assert survivor in rollback


def test_queued_jobs_keep_their_attempts_after_the_fence_closes(pr12_section: str) -> None:
    rollback = prose(pr12_section[position(pr12_section, "### 16.3") :])
    assert "`attempts` не растёт" in rollback


# ---------------------------------------------------------------------------
# Scope, and the refusal vocabulary
# ---------------------------------------------------------------------------


def test_the_section_does_not_introduce_deferred_phase_work(pr12_section: str) -> None:
    text = prose(pr12_section)
    assert "Что PR-12 **не** добавляет" in text or "PR-12 **не** добавляет" in text
    for deferred in ("newsletters", "promo", "campaign runner", "backfill"):
        assert deferred in text.lower()


def test_the_section_states_that_altegio_is_unchanged(pr12_section: str) -> None:
    text = prose(pr12_section)
    assert "Altegio-путь не меняется" in text


def test_the_runbook_lists_the_complete_send_time_refusal_vocabulary(pr12_section: str) -> None:
    """Every code the worker can write has a documented operator action."""
    table = pr12_section[position(pr12_section, "### 16.4") :]
    for reason in RETENTION_SEND_REFUSAL_REASONS:
        assert f"`{reason}`" in table, f"undocumented send-time refusal: {reason}"


def test_no_command_prints_a_payload_or_customer_data(pr12_section: str) -> None:
    for block in bash_blocks(pr12_section):
        lowered = block.lower()
        for forbidden in ("phone_e164", "display_name", "payload", "customer_phone", "booking_uuid"):
            assert forbidden not in lowered, f"a runbook command must not print {forbidden}"


# ---------------------------------------------------------------------------
# The master notification flag has TWO runtime readers
#
# Since PR-12, EASYWEEK_NOTIFICATIONS_ENABLED gates planning in the inbox worker
# AND claim/send in the outbox worker. An operator who sets it to false and, on
# the strength of an ownership table naming one service, recreates only the inbox
# worker leaves the outbox worker running with the stale value `true` — and it
# keeps delivering repeat_10d / comeback_3d jobs that were already queued. The
# pause never happens, and nothing in the system says so.
# ---------------------------------------------------------------------------


def _ownership_table(env_example: str) -> str:
    """The "which service to recreate" table at the head of the env example."""
    start = env_example.index("Какой сервис пересоздавать после правки")
    end = env_example.index("EASYWEEK_ENABLED=false")
    return env_example[start:end]


def test_the_ownership_table_names_both_services_for_the_master_flag(env_example: str) -> None:
    table = prose(_ownership_table(env_example))

    assert MASTER_FLAG in table, "the master flag must appear in the ownership table"
    master_entry = table[table.index(MASTER_FLAG) :]
    # Both services, before the next flag's entry begins.
    entry = master_entry[: master_entry.index("EASYWEEK_ENABLED,")]
    assert INBOX_SERVICE in entry
    assert OUTBOX_SERVICE in entry


def test_the_ownership_table_says_one_service_is_not_enough(env_example: str) -> None:
    """The wrong model is refuted in words, not merely left unstated.

    A table that simply lists two services still reads as "pick the one you
    changed" to an operator in a hurry. The failure mode has to be named.
    """
    table = prose(_ownership_table(env_example))

    assert "ОБА сервиса" in table
    assert "НЕДОСТАТОЧНО" in table
    assert "продолжит отправлять" in table


def test_the_master_flag_is_not_listed_as_inbox_only(env_example: str) -> None:
    """The regression itself: the flag must not sit in the inbox-only group.

    This is what the table said before the fix, and it is what an operator
    followed. Pinning it means a future edit cannot quietly restore it.
    """
    table = _ownership_table(env_example)
    inbox_group_start = table.index("EASYWEEK_PROCESSING_ENABLED")
    inbox_group = table[inbox_group_start : table.index(f"-> {INBOX_SERVICE}", inbox_group_start)]

    assert MASTER_FLAG not in inbox_group, (
        "the master flag must not be grouped with the inbox-worker-only settings: "
        "recreating that service alone leaves the outbox worker sending"
    )


def test_the_master_flag_paragraph_names_both_readers(env_example: str) -> None:
    """Its own declaration paragraph, not only the table at the top."""
    # The ASSIGNMENT, at the start of a line: the ownership table above quotes
    # the same text inside prose, and slicing on the first occurrence would cut
    # the file before the declaration this test is about.
    assignment = re.search(rf"^{MASTER_FLAG}=false$", env_example, flags=re.M)
    assert assignment is not None, "the master flag must be declared in the example"
    paragraph = prose(env_example[: assignment.start()])
    paragraph = paragraph[paragraph.rindex("Создание EasyWeek MessageJob") :]

    assert INBOX_SERVICE in paragraph
    assert OUTBOX_SERVICE in paragraph
    assert "ОБОИХ" in paragraph


def test_the_shutdown_uses_two_separate_force_recreate_commands(master_flag_shutdown: str) -> None:
    """Ordering comes from separate commands, never from argument order.

    `docker compose up -d --force-recreate A B` orders services by the dependency
    graph. These two workers do not depend on each other, so Compose may recreate
    them in parallel or in the opposite order — and the window where the old
    outbox is still running with notifications=true is exactly when a due
    repeat_10d or comeback_3d gets claimed and sent.
    """
    recreates = [block for block in bash_blocks(master_flag_shutdown) if "--force-recreate" in block]

    assert len(recreates) == 2, "one command per service, so the sequence is the operator's, not Compose's"
    assert sum(OUTBOX_SERVICE in block for block in recreates) == 1
    assert sum(INBOX_SERVICE in block for block in recreates) == 1


def test_no_shutdown_command_recreates_both_workers_at_once(master_flag_shutdown: str) -> None:
    """The refuted form must not come back as a shortcut.

    A single command naming both services looks like the same thing and is not:
    it hands the ordering back to Compose, which does not provide one here.
    """
    for block in bash_blocks(master_flag_shutdown):
        if "--force-recreate" not in block:
            continue
        assert not (OUTBOX_SERVICE in block and INBOX_SERVICE in block), (
            f"a combined force-recreate is not an ordering guarantee: {block}"
        )


def test_the_sending_worker_is_recreated_before_the_planning_one(master_flag_shutdown: str) -> None:
    """Whoever sends goes quiet first."""
    outbox_at = position(master_flag_shutdown, f"--force-recreate {OUTBOX_SERVICE}")
    inbox_at = position(master_flag_shutdown, f"--force-recreate {INBOX_SERVICE}")

    assert outbox_at < inbox_at


def test_a_verified_outbox_gates_the_inbox_recreate(master_flag_shutdown: str) -> None:
    """The check between them is a GATE, not a formality.

    Without it the sequence is only a hope: the operator would move on to the
    inbox worker while the sending one may still hold the old value.
    """
    outbox_at = position(master_flag_shutdown, f"--force-recreate {OUTBOX_SERVICE}")
    inbox_at = position(master_flag_shutdown, f"--force-recreate {INBOX_SERVICE}")
    between = master_flag_shutdown[outbox_at:inbox_at]

    checks = [
        block
        for block in bash_blocks(between)
        if OUTBOX_SERVICE in block and "settings.easyweek_notifications_enabled" in block
    ]
    assert checks, "an effective-config read inside the outbox worker must sit between the two recreates"
    assert "exec" in checks[0].split(), "the check is about THIS running container"


def test_a_failed_outbox_check_stops_the_procedure(master_flag_shutdown: str) -> None:
    """ "Could not verify" must never read as "messaging is paused"."""
    text = prose(master_flag_shutdown)

    assert "остановиться здесь" in text.lower()
    assert "Не" in master_flag_shutdown and "пересоздавать inbox-worker" in text
    assert "не считать retention sends остановленными" in text


def test_the_shutdown_does_not_ask_for_a_dependency_between_the_workers(master_flag_shutdown: str) -> None:
    """The fix is an operator sequence, not a topology change.

    `depends_on` would make one consumer wait on the other for every deploy and
    restart, which is a production topology change PR-12 has no reason to make.
    """
    assert "depends_on" not in master_flag_shutdown

    services = yaml.safe_load(COMPOSE_FILE.read_text())["services"]
    for name, other in ((INBOX_SERVICE, OUTBOX_SERVICE), (OUTBOX_SERVICE, INBOX_SERVICE)):
        depends = services[name].get("depends_on") or {}
        assert other not in depends, f"{name} must stay independent of {other}"


def test_the_runbook_refutes_the_combined_command_as_an_ordering_claim(pr12_section: str) -> None:
    """The wrong model is named and refuted, not merely dropped."""
    text = prose(pr12_section)

    assert "читают ДВА long-running сервиса" in text
    assert "Одна Compose-команда с двумя сервисами порядка **не** даёт" in text
    assert "dependency graph" in text
    assert "друг от друга не зависят" in text


def test_turning_the_master_flag_off_is_never_documented_as_one_service(pr12_section: str) -> None:
    """Wherever the operator may close the master flag, the sequence is named."""
    text = prose(pr12_section)
    rollback = text[text.index("16.3 Rollback") :]

    assert MASTER_FLAG in rollback
    master_note = rollback[rollback.index(MASTER_FLAG) :]
    assert "отдельными командами" in master_note, "the rollback note must not imply one command is enough"
    assert "16.1" in master_note, "and it must point at the single verified sequence"


def test_enabling_the_master_flag_gets_no_universal_ordering_advice(pr12_section: str) -> None:
    """The shutdown order must not become a general "outbox first" rule.

    Enabling is not the mirror of disabling: opening the master flag permits
    nothing on its own, because retention answers to its own fences, its
    preflight and its canary. A blanket ordering rule would read as a shortcut
    past all three.
    """
    text = prose(pr12_section)

    assert "Обратное включение — это НЕ зеркало выключения" in text
    assert "Универсального правила «всегда пересоздавать outbox первым» не существует" in text
    for gate in ("EASYWEEK_RETENTION_ENABLED", "EASYWEEK_RETENTION_SEND_ENABLED", "preflight", "canary"):
        assert gate in text, f"the enabling path must still name {gate}"


def test_the_outbox_runtime_check_shows_the_master_flag_with_the_retention_gates(
    pr12_section: str,
) -> None:
    """A report of `retention_send` alone cannot distinguish two states.

    "Sending is fenced off" and "the outbox worker is still running with the old
    environment where the master flag was true" look identical unless the check
    prints the master flag from inside that container.
    """
    checks = [
        block
        for block in bash_blocks(pr12_section)
        if OUTBOX_SERVICE in block and "settings.easyweek_retention_send_enabled" in block
    ]
    assert checks, "the rollout must read the outbox worker's effective config"
    for block in checks:
        assert "settings.easyweek_notifications_enabled" in block, (
            "the outbox effective-config check must print the master flag too"
        )
        assert "settings.easyweek_retention_canary_job_id" in block
        assert "exec" in block.split(), "an effective-config read is about THIS running container"


def test_the_runbook_explains_why_the_outbox_check_prints_the_master_flag(pr12_section: str) -> None:
    text = prose(pr12_section)
    assert "читают **оба** long-running сервиса" in text
    assert "со старым окружением" in text


def test_the_env_example_points_at_the_master_flag_section_not_the_rollout(env_example: str) -> None:
    """The canonical shutdown instruction lives in §16.1, not §16.2.

    §16.2 is the ENABLING rollout. An operator following a pointer to it while
    trying to pause messaging would read the canary and preflight steps and find
    nothing about shutting the flag down.
    """
    table = prose(_ownership_table(env_example))
    entry = table[table.index(MASTER_FLAG) : table.index("EASYWEEK_ENABLED,")]

    assert "раздел 16.1 runbook" in entry, "the shutdown sequence is documented in 16.1"
    shutdown_at = entry.index("ВЫКЛЮЧЕНИЕ")
    enabling_at = entry.index("ВКЛЮЧЕНИЕ")
    assert shutdown_at < entry.index("раздел 16.1 runbook") < enabling_at, (
        "the 16.1 pointer must belong to the shutdown half, not to the enabling half"
    )


def test_the_env_example_refutes_the_combined_command_ordering(env_example: str) -> None:
    table = prose(_ownership_table(env_example))
    entry = table[table.index(MASTER_FLAG) : table.index("EASYWEEK_ENABLED,")]

    assert "НЕ аргументами одной Compose-команды" in entry
    assert "dependency graph" in entry
    assert "ОТДЕЛЬНЫЕ последовательные" in entry
    assert "notifications == false" in entry


def test_the_env_example_separates_shutdown_from_enabling(env_example: str) -> None:
    """Two different procedures, and the file must not blur them."""
    table = prose(_ownership_table(env_example))
    entry = table[table.index(MASTER_FLAG) : table.index("EASYWEEK_ENABLED,")]

    assert "ВКЛЮЧЕНИЕ — не зеркало выключения" in entry
    assert "раздела 16.2" in entry, "enabling points at the rollout with its fences"
    assert "«всегда outbox первым» не существует" in entry


# ---------------------------------------------------------------------------
# The master flag is a RETENTION send fence, not a general one
#
# On the send side EASYWEEK_NOTIFICATIONS_ENABLED gates exactly repeat_10d and
# comeback_3d. Queued EasyWeek lifecycle, reminder_24h/reminder_2h and review_3d
# keep their own send-time contracts and can still go out. A runbook that
# promises "messaging is stopped" after the shutdown sequence tells an operator
# the outage is contained when it is not — and they stop looking.
# ---------------------------------------------------------------------------

_UNAFFECTED_JOB_TYPES = ("record_created", "reminder_24h", "reminder_2h", "review_3d")


def test_the_shutdown_promises_retention_sends_only(master_flag_shutdown: str) -> None:
    text = prose(master_flag_shutdown)

    assert "PR-12 retention jobs" in text
    assert "`repeat_10d` / `comeback_3d`" in text
    assert "inbox больше не планирует новые EasyWeek jobs" in text


def test_the_shutdown_warns_that_queued_non_retention_jobs_still_send(master_flag_shutdown: str) -> None:
    """The warning has to name the job types, not gesture at "other messages"."""
    text = prose(master_flag_shutdown)

    assert "НЕ доказано" in text
    assert "могут быть отправлены" in text
    for job_type in _UNAFFECTED_JOB_TYPES:
        assert job_type in text, f"the warning must name {job_type} as still sendable"


def test_the_shutdown_points_at_the_existing_hard_stop(master_flag_shutdown: str) -> None:
    """One hard-stop procedure, referenced — never a second one invented here."""
    text = prose(master_flag_shutdown)

    assert "не аварийная остановка всего EasyWeek outbox" in text
    assert "§8.2" in text
    assert "Второй процедуры hard-stop здесь нет" in text
    # A copied hard-stop would show up as its own commands.
    for block in bash_blocks(master_flag_shutdown):
        assert "stop" not in block.split(), f"the hard-stop must be referenced, not duplicated: {block}"
        assert "UPDATE" not in block.upper()


def test_no_general_claim_that_all_messaging_is_stopped(pr12_section: str) -> None:
    """The exact phrasings the two reviewers flagged, refused by name."""
    text = prose(pr12_section)

    for overclaim in (
        "рассылка остановлена",
        "клиентские отправки не остановлены",
        "не считать рассылку остановленной",
        "общая пауза рассылки",
    ):
        assert overclaim not in text, f"a global shutdown promise came back: {overclaim!r}"


def test_the_rollback_calls_it_retention_and_planning_not_a_general_pause(pr12_section: str) -> None:
    text = prose(pr12_section)
    rollback = text[text.index("16.3 Rollback") :]
    master_note = rollback[rollback.index(MASTER_FLAG) :]

    assert "остановка retention sends и нового EasyWeek planning" in master_note
    assert "§8.2" in master_note, "a full stop must point at the one hard-stop procedure"


def test_the_env_example_states_the_asymmetry_between_the_two_readers(env_example: str) -> None:
    """Same flag, two services, two different meanings."""
    table = prose(_ownership_table(env_example))
    entry = table[table.index(MASTER_FLAG) : table.index("EASYWEEK_ENABLED,")]

    assert "ПЛАНИРОВАНИЕ" in entry and "НИ ОДНОГО типа" in entry
    assert "SEND только для PR-12 retention" in entry
    assert "И БОЛЬШЕ НИЧЕГО" in entry
    assert "Это НЕ глобальный send fence" in entry
    assert "8.2" in entry, "the env example must point at the real emergency stop"


def test_the_settings_comment_does_not_call_the_flag_a_global_send_fence() -> None:
    """The declaration an engineer reads first must not overstate the flag."""
    settings_source = (REPO_ROOT / "src" / "altegio_bot" / "settings.py").read_text()
    raw = settings_source[settings_source.index("easyweek_enabled                ->") :]
    raw = raw[: raw.index("easyweek_processing_enabled: bool")]
    # Unwrapped: these are wrapped comment lines, and an assertion that depended
    # on where the author happened to break a line would be accidental matching.
    block = prose(raw)

    assert "ONLY creating EasyWeek MessageJob rows" not in block, (
        "that absolute stopped being true when PR-12 added a retention send gate"
    )
    assert "the send side of retention only" in block
    assert "NOT a send fence" in block
    for untouched in ("reminder_24h", "review_3d", "Altegio"):
        assert untouched in block, f"the comment must name {untouched} as unaffected"


def test_the_review_comment_does_not_read_as_a_send_side_gate() -> None:
    """PR-9's "master gate above both" is about planning, and must say so."""
    settings_source = (REPO_ROOT / "src" / "altegio_bot" / "settings.py").read_text()
    raw = settings_source[settings_source.index("--- PR-9: review_3d") :]
    block = prose(raw[: raw.index("easyweek_reviews_enabled: bool")])

    assert "master gate above both — over PLANNING" in block
    assert "not a second send fence for review" in block


def test_the_outbox_gate_docstring_is_scoped_to_retention() -> None:
    """`master fence over every customer message` was too wide to be safe."""
    worker_source = (REPO_ROOT / "src" / "altegio_bot" / "workers" / "outbox_worker.py").read_text()
    start = worker_source.index("def easyweek_retention_send_blocked")
    raw = worker_source[start : worker_source.index("def easyweek_retention_job_blocked")]
    doc = " ".join(raw.split())

    assert "master fence over every PR-12 RETENTION customer message" in doc
    assert "master fence over every customer message" not in doc, "the too-wide phrasing came back"
    assert "Scoped to retention here, and only retention" in doc
    for untouched in ("lifecycle", "reminder_24h", "review_3d", "Altegio"):
        assert untouched in doc, f"the docstring must name {untouched} as unaffected"


# ---------------------------------------------------------------------------
# Resuming after a master-flag pause
#
# "Opening the master flag permits nothing on its own" is true for a FIRST
# rollout, where the send fence has been shut since deploy. It is false for a
# retention that was already running and was merely paused: the fence is still
# true in easyweek.env, the canary is empty, and `easyweek_retention_send_blocked`
# consults no stored preflight approval. The moment the recreated outbox reads
# notifications=true, the whole backlog that accumulated during the pause is
# claimable — with no preflight and no canary in between.
# ---------------------------------------------------------------------------


def test_the_chapter_separates_a_first_rollout_from_a_resume(pr12_section: str) -> None:
    """Two different situations; conflating them is what made the advice unsafe."""
    text = prose(pr12_section)

    assert "первый rollout" in text
    assert "возобновление после паузы мастер-флага" in text.lower()


def test_the_resume_names_the_danger_it_closes(master_flag_resume: str) -> None:
    """The failure has to be stated, not merely designed around."""
    text = prose(master_flag_resume)

    assert "EASYWEEK_RETENTION_SEND_ENABLED=true" in text
    assert "claimable" in text
    assert "без preflight и без canary" in text


def test_the_resume_says_planning_is_not_a_send_side_protection(master_flag_resume: str) -> None:
    """`EASYWEEK_RETENTION_ENABLED=false` does not hold a queue that already exists."""
    text = prose(master_flag_resume)

    assert f"{PLANNING_FLAG}=false" in text
    assert "флаг **планирования**" in text
    assert "уже созданную очередь не держит" in text
    assert f"Единственный send-side тормоз — `{SEND_FLAG}`" in text


def test_the_resume_proves_the_starting_pause_before_anything_else(master_flag_resume: str) -> None:
    """Step A: both workers must be shown to be at notifications=false."""
    text = prose(master_flag_resume)
    first_gate = master_flag_resume[: position(master_flag_resume, f"{SEND_FLAG}=false")]

    assert "Шаг A" in text
    checks = [b for b in bash_blocks(first_gate) if "easyweek_notifications_enabled" in b]
    assert len(checks) == 2, "the starting pause is proven in BOTH workers before it is trusted"
    assert any(OUTBOX_SERVICE in b for b in checks)
    assert any(INBOX_SERVICE in b for b in checks)
    assert "пауза **не доказана**" in text
    assert "Не объявлять её состоявшейся" in text


def test_the_send_fence_is_closed_before_the_master_flag_is_restored(master_flag_resume: str) -> None:
    """THE ordering property: the fence closes while the master is still false."""
    close_fence = position(master_flag_resume, f"{SEND_FLAG}=false")
    restore_master = position(master_flag_resume, f"{MASTER_FLAG}=true")

    assert close_fence < restore_master


def test_the_outbox_is_recreated_and_verified_before_the_master_flag(master_flag_resume: str) -> None:
    """Closing the fence in the file is not closing it in the running worker."""
    close_fence = position(master_flag_resume, f"{SEND_FLAG}=false")
    restore_master = position(master_flag_resume, f"{MASTER_FLAG}=true")
    between = master_flag_resume[close_fence:restore_master]

    recreates = [b for b in bash_blocks(between) if "--force-recreate" in b]
    assert recreates, "the outbox must be recreated before the master flag is restored"
    assert all(OUTBOX_SERVICE in b for b in recreates), "and only the outbox at this point"
    assert all(INBOX_SERVICE not in b for b in recreates)

    gates = [
        b
        for b in bash_blocks(between)
        if "easyweek_notifications_enabled" in b and "easyweek_retention_send_enabled" in b
    ]
    assert gates, "an effective-config gate must sit between the recreate and the master flag"
    gate = gates[-1]
    assert "sys.exit" in gate, "the gate must fail with a non-zero exit code"
    assert "exec" in gate.split()


def test_the_gate_is_mandatory_before_restoring_the_master_flag(master_flag_resume: str) -> None:
    text = prose(master_flag_resume)

    assert "Обязательный gate" in text
    assert "единственное разрешение идти дальше" in text
    assert "запрещает" in text


def test_the_send_fence_stays_closed_after_the_master_flag_is_restored(master_flag_resume: str) -> None:
    """Restoring the master flag must not be the moment the queue is released."""
    text = prose(master_flag_resume)
    after = master_flag_resume[position(master_flag_resume, f"{MASTER_FLAG}=true") :]

    assert f"`{SEND_FLAG}` остаётся `false`" in text
    verifications = [
        b
        for b in bash_blocks(after)
        if OUTBOX_SERVICE in b and "easyweek_retention_send_enabled" in b and "sys.exit" in b
    ]
    assert verifications, "the post-restore outbox check must re-prove the fence is still shut"
    assert "retention-отправки по-прежнему закрыты" in text


def test_the_resume_recreates_both_workers_separately(master_flag_resume: str) -> None:
    """No combined command, and no dependency between the workers."""
    for block in bash_blocks(master_flag_resume):
        if "--force-recreate" not in block:
            continue
        assert not (OUTBOX_SERVICE in block and INBOX_SERVICE in block), (
            f"a combined force-recreate is not an ordering guarantee: {block}"
        )
    assert "depends_on" not in master_flag_resume


def test_the_resume_hands_over_to_the_existing_preflight_and_canary(master_flag_resume: str) -> None:
    """It continues §16.2 by step number; it does not replay the first deploy."""
    text = prose(master_flag_resume)

    assert "шаг 8" in text, "the read-only preflight over the real queue"
    assert "шаги 10–14" in text, "the controlled single-job canary"
    assert "шаги 15–17" in text, "close the fence again and unset the canary"
    assert "шаг 18" in text and "шаг 19" in text, "full audit, then bulk"
    assert "Шаги 1–7" in text and "не повторяются" in text
    assert "автоматически не очищается" in text, "the canary restriction is not cleared for the operator"


def test_the_resume_does_not_claim_a_general_stop(master_flag_resume: str) -> None:
    """It holds retention, not every customer message."""
    text = prose(master_flag_resume)

    assert "удерживает **retention**, а не все клиентские сообщения" in text
    for job_type in ("reminder_24h", "review_3d"):
        assert job_type in text, f"the warning must name {job_type} as still sendable"
    assert "§8.2" in text
    assert "preflight работающий outbox не закрывает" in text


def test_the_old_unsafe_enabling_claim_is_gone(pr12_section: str) -> None:
    """The exact sentence that made the resume unsafe, refused by name.

    It read as an unconditional property of the master flag. It is only true
    while the send fence is shut — which is precisely NOT the state a paused
    retention is left in.
    """
    text = prose(pr12_section)

    assert "открытие мастер-флага само по себе ничего не разрешает" not in text
    assert "Включение мастер-флага не открывает retention" not in text


def test_the_env_example_no_longer_promises_that_the_master_flag_is_inert(env_example: str) -> None:
    table = prose(_ownership_table(env_example))
    entry = table[table.index(MASTER_FLAG) : table.index("EASYWEEK_ENABLED,")]

    assert "ВЕРНО ТОЛЬКО для первого rollout" in entry
    assert "СРАЗУ освобождает" in entry
    assert f"{PLANNING_FLAG}=false от этого не спасает" in entry
    assert "Возобновление после паузы мастер-флага" in entry, "and it points at the canonical procedure"
    assert "16.1" in entry
