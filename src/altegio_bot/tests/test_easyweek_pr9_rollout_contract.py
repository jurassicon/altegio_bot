"""PR-9: the operator contract, checked for meaning rather than for keywords.

A rollout document is executable only if it says the right things in the right
order. The dangerous mistakes it can encode are specific: opening the send fence
before the preflight is green, using `restart` (which does not re-read env_file)
where `--force-recreate` is required, rolling back planning before sending, or
offering a byte-identical Resend as proof that a new path works.

So these tests assert ORDER and CONTENT, not the presence of a word: where two
instructions must be sequenced, the assertion is on their relative position in
the document.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest
import yaml

from altegio_bot.easyweek_review import REVIEW_SEND_REFUSAL_REASONS
from altegio_bot.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[3]
ENV_EXAMPLE = REPO_ROOT / "easyweek.env.example"
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
RUNBOOK = REPO_ROOT / "docs" / "easyweek" / "durlach_activation_runbook.md"

PLANNING_FLAG = "EASYWEEK_REVIEWS_ENABLED"
SEND_FLAG = "EASYWEEK_REVIEW_SEND_ENABLED"
INBOX_SERVICE = "altegio-easyweek-inbox-worker"
OUTBOX_SERVICE = "altegio-outbox-worker"
PREFLIGHT_MODULE = "altegio_bot.scripts.easyweek_review_preflight"
PYTHON_ENTRYPOINT = "/app/.venv/bin/python"


def prose(text: str) -> str:
    """Strip comment markers and line wrapping so assertions read sentences.

    Without this, a test passes or fails on where the author happened to wrap a
    line, which is exactly the accidental-string matching we are avoiding.
    """
    unwrapped = [re.sub(r"^\s*#\s?", "", line) for line in text.splitlines()]
    return re.sub(r"\s+", " ", " ".join(unwrapped)).strip()


def position(text: str, needle: str) -> int:
    """Index of `needle`, with a readable failure when the step is missing."""
    assert needle in text, f"the document no longer contains: {needle}"
    return text.index(needle)


def bash_blocks(text: str) -> list[str]:
    """Contents of every ```bash fence — the lines an operator actually runs.

    Prose that merely mentions a command must not be mistaken for an
    instruction, so only fenced blocks count.
    """
    return [block.strip() for block in re.findall(r"```bash\n(.*?)```", text, flags=re.S)]


def preflight_commands(text: str) -> list[str]:
    return [block for block in bash_blocks(text) if PREFLIGHT_MODULE in block]


def preflight_violations(command: str) -> list[str]:
    """Everything wrong with a proposed preflight command, in plain words.

    Shared by the positive contract and the negative regressions, so the two can
    never drift apart: whatever this accepts is exactly what the runbook may say.
    """
    words = command.split()
    problems: list[str] = []

    if "exec" in words:
        problems.append("uses `exec`, reusing a container created with the pre-rollout environment")
    if "restart" in words:
        problems.append("uses `restart`, which does not re-read env_file")
    if "run" not in words:
        problems.append("does not use `docker compose run`, so it never re-reads easyweek.env")
    if "--rm" not in words:
        problems.append("omits `--rm` and leaves a one-off container behind")
    if "--no-deps" not in words:
        problems.append("omits `--no-deps` and may restart production dependencies")
    if "-e" in words or any(word.startswith("--env") for word in words):
        problems.append("hand-forces environment values instead of reading easyweek.env")
    if OUTBOX_SERVICE not in words:
        problems.append(f"does not run as the {OUTBOX_SERVICE} service")
    if "--entrypoint" not in words or PYTHON_ENTRYPOINT not in words:
        problems.append(f"does not override the entrypoint to {PYTHON_ENTRYPOINT}")
    if "-m" not in words or PREFLIGHT_MODULE not in words:
        problems.append(f"does not run `-m {PREFLIGHT_MODULE}`")

    return problems


@pytest.fixture(scope="module")
def env_example() -> str:
    return ENV_EXAMPLE.read_text()


@pytest.fixture(scope="module")
def runbook() -> str:
    return RUNBOOK.read_text()


@pytest.fixture(scope="module")
def pr9_section(runbook: str) -> str:
    """Only the PR-9 chapter, so an assertion cannot pass on PR-8 prose."""
    start = runbook.index("## 13. PR-9")
    return runbook[start:]


# ---------------------------------------------------------------------------
# The two flags
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("flag", [PLANNING_FLAG, SEND_FLAG])
def test_both_flags_default_to_false_in_settings(flag: str) -> None:
    """Production default is off for both; the code is the source of truth."""
    field = flag.lower()
    assert getattr(Settings.model_fields[field], "default") is False


@pytest.mark.parametrize("flag", [PLANNING_FLAG, SEND_FLAG])
def test_each_flag_is_declared_exactly_once_and_false(env_example: str, flag: str) -> None:
    assignments = re.findall(rf"^{flag}=(.*)$", env_example, flags=re.M)
    assert assignments == ["false"], f"{flag} must be declared once, as false"


def test_the_example_documents_which_worker_reads_which_flag_at_runtime(env_example: str) -> None:
    """Recreating the wrong container is the classic way to flip nothing.

    Each flag's own paragraph — not the file header — must name its worker, and
    must scope the claim to RUNTIME: one-off processes read more than one flag.
    """
    section = env_example[env_example.index("--- PR-9") :]
    planning_para = prose(section[: section.index(f"{PLANNING_FLAG}=false")])
    send_para = prose(section[section.index(f"{PLANNING_FLAG}=false") : section.index(f"{SEND_FLAG}=false")])

    assert INBOX_SERVICE in planning_para and OUTBOX_SERVICE not in planning_para
    assert OUTBOX_SERVICE in send_para and INBOX_SERVICE not in send_para
    for para in (planning_para, send_para):
        assert "runtime" in para.lower() or "long-running" in para.lower(), (
            "ownership is a runtime claim; say so, or the preflight looks like a contradiction"
        )


def test_the_example_says_the_one_off_preflight_reads_all_three_flags(env_example: str) -> None:
    """Runtime ownership is split; the audit process is not.

    The preflight's whole statement is "notifications on, planning on, fence
    still shut", so it must see all three. A document claiming the outbox side
    never reads EASYWEEK_REVIEWS_ENABLED would make the P1 blocker invisible.
    """
    section = prose(env_example[env_example.index("--- PR-9") :])

    assert "Один процесс читает ОБА флага сразу: one-off review preflight" in section
    assert f"создаётся из сервиса {OUTBOX_SERVICE}" in section
    for flag in ("EASYWEEK_NOTIFICATIONS_ENABLED", PLANNING_FLAG, SEND_FLAG):
        assert flag in section, flag
    # The wrong model is named and refuted, not merely left unstated.
    assert f"«{PLANNING_FLAG} вообще не читается ничем на базе outbox service» — неверно" in section


def test_the_example_warns_that_exec_would_read_a_stale_environment(env_example: str) -> None:
    section = prose(env_example[env_example.index("--- PR-9") :])
    assert "нельзя запускать через `docker compose exec`" in section
    assert "config_error `review_planning_disabled`" in section
    assert "свежий one-off контейнер" in section


def test_the_example_says_restart_is_not_enough(env_example: str) -> None:
    assert "docker compose restart" in env_example
    assert "force-recreate" in env_example


def test_the_example_separates_planning_from_sending(env_example: str) -> None:
    section = prose(env_example[env_example.index("--- PR-9") :])
    assert "send fence" in section.lower()
    # Both directions of independence are stated, not just one.
    assert "Закрытие fence НЕ отменяет уже созданные jobs" in section
    assert "выключение planning НЕ открывает fence" in section


def test_the_example_carries_no_real_secret(env_example: str) -> None:
    for pattern in (r"EASYWEEK_API_KEY=\S", r"EASYWEEK_WEBHOOK_SECRET=\S", r"token=[A-Za-z0-9]{8,}"):
        assert not re.search(pattern, env_example), pattern


def test_the_master_notification_gate_is_still_documented(env_example: str) -> None:
    """PR-9 adds gates; it does not replace the one above them."""
    assert "EASYWEEK_NOTIFICATIONS_ENABLED" in env_example


# ---------------------------------------------------------------------------
# Compose wiring
# ---------------------------------------------------------------------------


def test_both_workers_still_read_easyweek_env_and_nothing_new_was_added() -> None:
    """PR-9 needs no new service and no new secret distribution."""
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
    }, f"easyweek.env reached an unexpected service: {with_easyweek_env}"


def test_the_compose_comments_name_the_right_owner_for_each_flag() -> None:
    text = COMPOSE_FILE.read_text()
    inbox_block = text[text.index(f"  {INBOX_SERVICE}:") - 2000 : text.index(f"  {INBOX_SERVICE}:")]
    outbox_block = text[text.index(f"  {OUTBOX_SERVICE}:") - 2000 : text.index(f"  {OUTBOX_SERVICE}:")]

    assert PLANNING_FLAG in inbox_block
    assert SEND_FLAG in outbox_block


# ---------------------------------------------------------------------------
# The fifth webhook
# ---------------------------------------------------------------------------


def test_the_runbook_lists_exactly_the_five_supported_triggers(pr9_section: str) -> None:
    for trigger in (
        "booking-created",
        "booking-updated",
        "booking-rescheduled",
        "booking-canceled",
        "booking-succeeded",
    ):
        assert trigger in pr9_section, trigger
    # Phase 2 triggers must not have crept in.
    for absent in ("booking-review", "visits_total", "repeat_10d", "comeback_3d"):
        assert absent not in pr9_section, absent


def test_the_webhook_url_carries_a_placeholder_not_a_secret(pr9_section: str) -> None:
    assert "event=booking-succeeded&token=<EASYWEEK_WEBHOOK_SECRET>" in pr9_section
    assert not re.search(r"token=[A-Za-z0-9]{8,}", pr9_section), "a real token must never be written down"


def test_the_runbook_reuses_the_existing_endpoint(pr9_section: str) -> None:
    """No new HTTP route: the event hint is a query parameter."""
    assert "/webhooks/easyweek?event=" in pr9_section
    assert "тот же** endpoint и **тот же** secret, отдельного маршрута не создаётся" in prose(pr9_section)


def test_the_runbook_corrects_the_old_no_side_effects_claim(pr9_section: str) -> None:
    """booking-succeeded is now evidence of a finished visit, not a no-op."""
    text = prose(pr9_section)
    assert "может создать **один** `review_3d`" in text
    assert "не создаёт lifecycle-уведомление и не переписывает snapshot" in text


# ---------------------------------------------------------------------------
# Rollout order — the part that must not be got wrong
# ---------------------------------------------------------------------------


def test_planning_is_enabled_before_the_send_fence(pr9_section: str) -> None:
    planning = position(pr9_section, f"{PLANNING_FLAG}=true")
    send = position(pr9_section, f"{SEND_FLAG}=true")
    assert planning < send, "planning must open first, so a queue exists to audit"


def test_the_preflight_runs_between_planning_and_the_send_fence(pr9_section: str) -> None:
    planning = position(pr9_section, f"{PLANNING_FLAG}=true")
    preflight = position(pr9_section, "easyweek_review_preflight")
    send = position(pr9_section, f"{SEND_FLAG}=true")
    assert planning < preflight < send, "the queue must be audited before it can be sent"


def test_the_fence_may_only_open_on_a_green_preflight(pr9_section: str) -> None:
    assert "ready=true" in pr9_section
    assert "exit code 0" in pr9_section
    # And the failing conditions are named as STOP rather than left implicit.
    assert "truncated=true" in pr9_section
    assert "STOP" in pr9_section


def test_each_flag_change_is_followed_by_force_recreate_of_its_own_service(pr9_section: str) -> None:
    """`restart` does not re-read env_file — the classic silent no-op."""
    assert f"up -d --force-recreate {INBOX_SERVICE}" in pr9_section
    assert f"up -d --force-recreate {OUTBOX_SERVICE}" in pr9_section
    assert "restart` не перечитывает" in pr9_section


# ---------------------------------------------------------------------------
# The preflight must run in a process that re-read easyweek.env
# ---------------------------------------------------------------------------
#
# The rollout enables planning and force-recreates ONLY the inbox worker, so the
# long-running outbox container still holds the environment it was created with,
# where EASYWEEK_REVIEWS_ENABLED was false. A preflight `exec`d into it reports
# config_error=review_planning_disabled and exits 1 — the documented rollout
# could never reach a green preflight, and so could never open the fence.


def test_exactly_one_command_runs_the_preflight(pr9_section: str) -> None:
    """Two competing commands would let an operator pick the broken one."""
    assert len(preflight_commands(pr9_section)) == 1


def test_the_preflight_command_runs_in_a_fresh_one_off_container(pr9_section: str) -> None:
    command = preflight_commands(pr9_section)[0]
    assert preflight_violations(command) == [], command


def test_the_preflight_command_matches_the_production_compose_invocation(pr9_section: str) -> None:
    """A different project or file set is a different network and a different env."""
    command = preflight_commands(pr9_section)[0]
    assert "-p altegio_bot" in command
    assert "-f docker-compose.yml" in command
    assert "-f docker-compose.chatwoot-internal.yml" in command


def test_the_old_exec_form_is_rejected_by_the_same_contract() -> None:
    """Negative regression: restoring the pre-fix command must fail the check.

    This is the command the reviewed commit shipped. If a future edit puts it
    back, `test_the_preflight_command_runs_in_a_fresh_one_off_container` has to
    fail — which it only does if the checker actually rejects this string.
    """
    old = (
        "docker compose -p altegio_bot -f docker-compose.yml "
        "-f docker-compose.chatwoot-internal.yml exec -T altegio-outbox-worker "
        "/app/.venv/bin/python -m altegio_bot.scripts.easyweek_review_preflight"
    )
    violations = preflight_violations(old)

    assert any("exec" in problem for problem in violations), violations
    assert any("run" in problem for problem in violations), violations


@pytest.mark.parametrize(
    "command",
    [
        pytest.param(
            "docker compose -p altegio_bot -f docker-compose.yml "
            "-f docker-compose.chatwoot-internal.yml restart altegio-outbox-worker",
            id="restart-does-not-reread-env_file",
        ),
        pytest.param(
            "docker compose -p altegio_bot -f docker-compose.yml "
            "-f docker-compose.chatwoot-internal.yml run --rm --no-deps "
            "--entrypoint /app/.venv/bin/python altegio-easyweek-inbox-worker "
            "-m altegio_bot.scripts.easyweek_review_preflight",
            id="wrong-service",
        ),
        pytest.param(
            "docker compose -p altegio_bot -f docker-compose.yml "
            "-f docker-compose.chatwoot-internal.yml run --rm --no-deps "
            "-e EASYWEEK_REVIEWS_ENABLED=true --entrypoint /app/.venv/bin/python "
            "altegio-outbox-worker -m altegio_bot.scripts.easyweek_review_preflight",
            id="hand-forced-flag",
        ),
        pytest.param(
            "docker compose -p altegio_bot -f docker-compose.yml "
            "-f docker-compose.chatwoot-internal.yml run --entrypoint "
            "/app/.venv/bin/python altegio-outbox-worker "
            "-m altegio_bot.scripts.easyweek_review_preflight",
            id="leaks-a-container-and-restarts-dependencies",
        ),
    ],
)
def test_the_checker_rejects_the_near_misses(command: str) -> None:
    """Each of these would look plausible in a review and still be wrong."""
    assert preflight_violations(command) != []


def test_the_runbook_explains_why_exec_is_forbidden_here(pr9_section: str) -> None:
    """The reason has to survive: otherwise the next editor 'simplifies' it back."""
    text = prose(pr9_section)
    assert "создан на шаге 1" in text
    assert "review_planning_disabled" in text
    assert "не обходите это через `exec -e`" in text.lower()


def test_the_outbox_worker_is_not_recreated_before_the_preflight(pr9_section: str) -> None:
    """Recreating it early would open the fence before the queue was audited.

    Checked per command, not by substring: `--force-recreate inbox outbox` and a
    bare `--force-recreate` (which recreates everything) both recreate the outbox
    worker while reading like they only touch the inbox one.
    """
    window = pr9_section[position(pr9_section, f"{PLANNING_FLAG}=true") : position(pr9_section, PREFLIGHT_MODULE)]
    assert f"{SEND_FLAG}=true" not in window

    for block in bash_blocks(window):
        if "--force-recreate" not in block:
            continue
        services = block.split("--force-recreate", 1)[1].split()
        assert services == [INBOX_SERVICE], (
            f"before the preflight, only {INBOX_SERVICE} may be recreated; got {services or 'every service'}"
        )


def test_the_full_rollout_sequence_is_in_the_only_workable_order(pr9_section: str) -> None:
    steps = [
        f"{PLANNING_FLAG}=true",
        f"up -d --force-recreate {INBOX_SERVICE}",
        PREFLIGHT_MODULE,
        f"{SEND_FLAG}=true",
        f"up -d --force-recreate {OUTBOX_SERVICE}",
    ]
    positions = [position(pr9_section, step) for step in steps]
    assert positions == sorted(positions), f"rollout steps out of order: {steps}"


def test_the_runbook_says_step_ten_is_the_first_outbox_recreate(pr9_section: str) -> None:
    text = prose(pr9_section)
    assert "первое пересоздание outbox worker с шага 1" in text
    assert "Раньше шага 10 пересоздавать его нельзя" in text


def test_a_byte_identical_resend_is_rejected_as_positive_proof(pr9_section: str) -> None:
    assert "Resend" in pr9_section
    assert "доказательством не является" in pr9_section


def test_validation_requires_a_new_controlled_booking(pr9_section: str) -> None:
    assert "новую controlled запись" in pr9_section
    assert "дождаться реального `booking-succeeded`" in pr9_section


def test_sent_is_not_treated_as_final_delivery(pr9_section: str) -> None:
    assert "delivered" in pr9_section and "read" in pr9_section
    assert "финальным доказательством не считается" in pr9_section


def test_the_runbook_forbids_editing_production_rows_to_speed_up_the_smoke(pr9_section: str) -> None:
    assert "не менять его SQL-командой" in pr9_section
    assert "не подделывать событие" in pr9_section


# ---------------------------------------------------------------------------
# Rollback
# ---------------------------------------------------------------------------


def test_rollback_closes_sending_before_planning(pr9_section: str) -> None:
    rollback = pr9_section[pr9_section.index("### 13.3") :]
    send_off = position(rollback, f"{SEND_FLAG}=false")
    planning_off = position(rollback, f"{PLANNING_FLAG}=false")
    assert send_off < planning_off, "stop sending first; planning may keep the queue"
    # And each switch recreates its own worker, in the same order.
    assert position(rollback, f"--force-recreate {OUTBOX_SERVICE}") < position(
        rollback, f"--force-recreate {INBOX_SERVICE}"
    )


def test_rollback_never_deletes_or_bulk_updates(pr9_section: str) -> None:
    rollback = prose(pr9_section[pr9_section.index("### 13.3") :])
    assert "Ничего не удалять: ни events, ни jobs, ни Outbox" in rollback
    assert "Не выполнять массовых `UPDATE` по production-строкам" in rollback
    assert "не переигрывать delivery callbacks вручную" in rollback


def test_rollback_leaves_the_master_notification_gate_alone(pr9_section: str) -> None:
    rollback = pr9_section[pr9_section.index("### 13.3") :]
    assert "EASYWEEK_NOTIFICATIONS_ENABLED" in rollback
    assert "не** выключать" in rollback


def test_rollback_states_what_keeps_working(pr9_section: str) -> None:
    rollback = pr9_section[pr9_section.index("### 13.3") :]
    for survivor in ("lifecycle", "reminders", "Altegio"):
        assert survivor in rollback, survivor


# ---------------------------------------------------------------------------
# Scope and hygiene
# ---------------------------------------------------------------------------


def test_the_section_does_not_introduce_phase_two(pr9_section: str) -> None:
    for out_of_scope in ("repeat_10d", "comeback_3d", "visits_total", "campaign", "newsletter"):
        assert out_of_scope not in pr9_section, out_of_scope


def test_the_section_does_not_change_pr8_reminder_flags(pr9_section: str) -> None:
    for reminder_flag in ("EASYWEEK_REMINDERS_ENABLED", "EASYWEEK_REMINDER_API_GUARD_ENABLED"):
        assert reminder_flag not in pr9_section, reminder_flag


def test_the_section_promises_no_rolling_or_multi_replica_rollout(pr9_section: str) -> None:
    for absent in ("rolling", "replica", "blue-green"):
        assert absent not in pr9_section.lower(), absent


def test_no_command_prints_a_payload_or_customer_data(pr9_section: str) -> None:
    """Every SQL snippet selects ids, statuses and times — never content."""
    for forbidden in ("SELECT payload", "payload,", "phone_e164", "display_name", "review_url", "body"):
        assert forbidden not in pr9_section, forbidden


# ---------------------------------------------------------------------------
# PR-10: the link source, as documented
# ---------------------------------------------------------------------------


def test_the_link_map_is_its_own_variable_in_the_env_example() -> None:
    text = (REPO_ROOT / "easyweek.env.example").read_text()

    assert "EASYWEEK_GOOGLE_REVIEW_LINKS=" in text
    assert "REPLACE_WITH_REAL_TOKEN" in text, "the example must be a placeholder"
    # A real branch link is a production value and does not belong in a
    # committed example.
    assert "CaV0vSmrSYkdEAE" not in text


def test_the_env_example_names_both_consuming_services() -> None:
    text = (REPO_ROOT / "easyweek.env.example").read_text()
    block = text[text.index("EASYWEEK_GOOGLE_REVIEW_LINKS") - 2000 : text.index("EASYWEEK_GOOGLE_REVIEW_LINKS") + 200]

    assert "altegio-easyweek-inbox-worker" in block, "planning reads it"
    assert "altegio-outbox-worker" in block, "send-time re-proof reads it"
    assert "restart" in block, "a plain restart does not re-read env_file"


def test_the_link_map_is_kept_out_of_the_location_registry() -> None:
    """A typo in a review link must not take lifecycle and reminders down."""
    text = (REPO_ROOT / "easyweek.env.example").read_text()
    registry_line = next(
        (line for line in text.splitlines() if line.startswith("EASYWEEK_LOCATION_MAP=")),
        None,
    )
    assert registry_line is not None, "easyweek.env.example must still ship EASYWEEK_LOCATION_MAP"

    assert "g.page" not in registry_line
    assert "review" not in registry_line.lower()


def test_the_runbook_explains_that_the_link_is_ours() -> None:
    text = (REPO_ROOT / "docs/easyweek/durlach_activation_runbook.md").read_text()
    section = text[text.index("### 13.0") : text.index("### 13.1")]

    assert "EASYWEEK_GOOGLE_REVIEW_LINKS" in section
    assert "g.page/r/" in section
    assert "fail-closed" in section
    assert "parse_google_review_links" in section, "step 1 needs a read-only check"
    assert "review_link_changed" in section
    # And it must say plainly that the payload is not the source.
    assert "не payload" in section or "а не payload" in section


def test_the_runbook_check_prints_no_links() -> None:
    """The operator command reports counts and branches, never the URLs."""
    text = (REPO_ROOT / "docs/easyweek/durlach_activation_runbook.md").read_text()
    section = text[text.index("### 13.0") : text.index("### 13.1")]
    command = section[section.index("```bash") : section.index("```", section.index("```bash") + 7)]

    assert "sorted(m.links)" in command, "keys only"
    assert "m.links.values" not in command
    assert "print(settings.easyweek_google_review_links" not in command


def test_the_runbook_map_check_survives_an_env_edit() -> None:
    """`exec` reads the environment the container was CREATED with.

    The most likely action after a STOP is fixing `easyweek.env` and checking
    again — and `exec` would show the old broken map. Same trap PR-9's step 8
    already removed.
    """
    text = (REPO_ROOT / "docs/easyweek/durlach_activation_runbook.md").read_text()
    section = text[text.index("### 13.0") : text.index("### 13.1")]
    command = section[section.index("```bash") : section.index("```", section.index("```bash") + 7)]

    assert "run --rm --no-deps" in command
    assert "exec -T" not in command, "a re-check after an env edit must not use exec"
    assert "--entrypoint /app/.venv/bin/python" in command
    # And the reason has to be written down, not just implied by the command.
    assert "exec" in section and "созда" in section


def test_the_runbook_lists_the_complete_send_time_refusal_vocabulary(pr9_section: str) -> None:
    section = pr9_section[pr9_section.index("### 13.4") :]
    documented = set(re.findall(r"^\| `([^`]+)` \|", section, flags=re.M))

    assert documented == REVIEW_SEND_REFUSAL_REASONS
    assert "http://" not in section
    assert "https://" not in section
    assert "g.page" not in section
