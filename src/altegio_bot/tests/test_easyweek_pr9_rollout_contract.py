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

from altegio_bot.settings import Settings

REPO_ROOT = Path(__file__).resolve().parents[3]
ENV_EXAMPLE = REPO_ROOT / "easyweek.env.example"
COMPOSE_FILE = REPO_ROOT / "docker-compose.yml"
RUNBOOK = REPO_ROOT / "docs" / "easyweek" / "durlach_activation_runbook.md"

PLANNING_FLAG = "EASYWEEK_REVIEWS_ENABLED"
SEND_FLAG = "EASYWEEK_REVIEW_SEND_ENABLED"
INBOX_SERVICE = "altegio-easyweek-inbox-worker"
OUTBOX_SERVICE = "altegio-outbox-worker"


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


def test_the_example_documents_which_service_reads_which_flag(env_example: str) -> None:
    """Recreating the wrong container is the classic way to flip nothing.

    Each flag's own paragraph — not the file header — must name its worker.
    """
    section = env_example[env_example.index("--- PR-9") :]
    planning_para = prose(section[: section.index(f"{PLANNING_FLAG}=false")])
    send_para = prose(section[section.index(f"{PLANNING_FLAG}=false") : section.index(f"{SEND_FLAG}=false")])

    assert INBOX_SERVICE in planning_para and OUTBOX_SERVICE not in planning_para
    assert OUTBOX_SERVICE in send_para and INBOX_SERVICE not in send_para


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
