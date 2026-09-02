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

from altegio_bot.easyweek_retention import RETENTION_SEND_REFUSAL_REASONS
from altegio_bot.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[3]
ENV_EXAMPLE = REPO_ROOT / "easyweek.env.example"
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
RUNBOOK = REPO_ROOT / "docs" / "easyweek" / "durlach_activation_runbook.md"

PLANNING_FLAG = "EASYWEEK_RETENTION_ENABLED"
SEND_FLAG = "EASYWEEK_RETENTION_SEND_ENABLED"
COUNTER_FLAG = "EASYWEEK_VISIT_COUNTER_ENABLED"
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


def test_planning_is_enabled_before_the_send_fence(pr12_section: str) -> None:
    assert position(pr12_section, f"{PLANNING_FLAG}=true") < position(pr12_section, f"{SEND_FLAG}=true")


def test_the_counter_is_confirmed_before_planning_is_enabled(pr12_section: str) -> None:
    """A repeat planned without a proven baseline is never created at all."""
    assert position(pr12_section, COUNTER_FLAG) < position(pr12_section, f"{PLANNING_FLAG}=true")


def test_the_preflight_runs_between_planning_and_the_send_fence(pr12_section: str) -> None:
    assert (
        position(pr12_section, f"{PLANNING_FLAG}=true")
        < position(pr12_section, PREFLIGHT_MODULE)
        < position(pr12_section, f"{SEND_FLAG}=true")
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


def test_exactly_one_command_runs_the_preflight_and_it_is_a_fresh_container(pr12_section: str) -> None:
    commands = [block for block in bash_blocks(pr12_section) if PREFLIGHT_MODULE in block]
    assert len(commands) == 1, "one canonical invocation, or an operator picks the wrong one"
    command = commands[0]
    words = command.split()
    assert "run" in words and "--rm" in words and "--no-deps" in words
    assert "exec" not in words, "exec reuses a container created with the pre-rollout environment"
    assert "restart" not in words


def test_each_flag_change_is_followed_by_force_recreate_of_its_own_service(pr12_section: str) -> None:
    planning = pr12_section[position(pr12_section, f"{PLANNING_FLAG}=true") :]
    sending = pr12_section[position(pr12_section, f"{SEND_FLAG}=true") :]

    assert f"--force-recreate {INBOX_SERVICE}" in planning[:1200]
    assert f"--force-recreate {OUTBOX_SERVICE}" in sending[:1200]
    # Checked on the COMMANDS, not the prose: the section deliberately mentions
    # `docker compose restart` in order to say it is not enough.
    assert all("restart" not in block.split() for block in bash_blocks(pr12_section)), (
        "restart does not re-read env_file"
    )


def test_the_outbox_worker_is_not_recreated_before_the_preflight(pr12_section: str) -> None:
    """Recreating it early would open the fence before the audit."""
    preflight_at = position(pr12_section, PREFLIGHT_MODULE)
    earlier = pr12_section[:preflight_at]
    assert f"--force-recreate {OUTBOX_SERVICE}" not in earlier


def test_a_byte_identical_resend_is_rejected_as_positive_proof(pr12_section: str) -> None:
    text = prose(pr12_section)
    assert "Байт-идентичный Resend старого события доказательством не является" in text


def test_the_canary_is_one_job_and_not_a_blast(pr12_section: str) -> None:
    text = prose(pr12_section)
    assert "Одна заранее выбранная job" in text
    assert "Массовой отправки на этом шаге быть не должно" in text


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
