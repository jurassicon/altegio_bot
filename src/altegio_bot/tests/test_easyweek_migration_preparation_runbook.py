"""The preparation runbook has to be executable, not merely plausible.

Every command in it is parsed with the parser it would actually reach. A runbook
whose flags do not exist is worse than no runbook: it is read at the point where
somebody is about to touch customer data, and it fails there.
"""

from __future__ import annotations

import re
import shlex
from pathlib import Path

import pytest

from altegio_bot.scripts import easyweek_migration as migrator
from altegio_bot.scripts import easyweek_migration_prepare as prep

REPO_ROOT = Path(__file__).resolve().parents[3]
RUNBOOK = REPO_ROOT / "docs" / "easyweek" / "migration_preparation_runbook.md"
PREP_MODULE = "altegio_bot.scripts.easyweek_migration_prepare"
COMPOSE = REPO_ROOT / "docker-compose.yml"


def prose(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def bash_blocks(text: str) -> list[str]:
    return [block.strip() for block in re.findall(r"```bash\n(.*?)```", text, flags=re.S)]


def command_args(line: str) -> list[str]:
    """The argv the documented invocation would pass to the CLI parser."""
    assert "$(" not in line, "a new shell substitution needs its own fixture"
    tokens = shlex.split(line)
    if "-m" in tokens:
        return tokens[tokens.index("-m") + 2 :]
    # A compose invocation: everything after the service name is the CLI's argv.
    return tokens[tokens.index("easyweek-migration-prepare") + 1 :]


@pytest.fixture(scope="module")
def runbook() -> str:
    return RUNBOOK.read_text()


# The handover runs from its own one-off service whose name STARTS with the
# preparation one. Matching on a prefix would drag its commands into the
# preparation fixtures and parse them with the wrong parser.
HANDOVER_SERVICE_NAME = "easyweek-migration-prepare-handover"


@pytest.fixture(scope="module")
def commands(runbook: str) -> list[str]:
    lines = []
    for block in bash_blocks(runbook):
        for line in block.splitlines():
            if HANDOVER_SERVICE_NAME in line:
                continue
            if PREP_MODULE in line or "easyweek-migration-prepare" in line:
                lines.append(line)
    assert lines, "the runbook has no preparation commands"
    return lines


def test_every_documented_command_parses(commands: list[str]) -> None:
    parser = prep.build_parser()
    for line in commands:
        args = parser.parse_args(command_args(line))
        assert args.company_id in (758285, 1271200), line
        assert args.cutover_at, line


def test_every_mode_is_covered_and_none_is_invented(commands: list[str]) -> None:
    parser = prep.build_parser()
    modes = {parser.parse_args(command_args(line)).mode for line in commands}

    assert modes == set(prep.MODES)


def test_only_the_creation_command_carries_the_creation_permission(commands: list[str]) -> None:
    """A flag left in a saved command line must not turn a read into a write."""
    parser = prep.build_parser()
    for line in commands:
        args = parser.parse_args(command_args(line))
        if args.mode == prep.MODE_CREATE_CUSTOMERS:
            assert args.authorise_customer_create is True, line
            assert prep.CREATE_ENV_FLAG in line, line
        else:
            assert args.authorise_customer_create is False, line
            assert prep.CREATE_ENV_FLAG not in line, line


def test_batch_confirmations_always_carry_their_digest(commands: list[str]) -> None:
    parser = prep.build_parser()
    for line in commands:
        args = parser.parse_args(command_args(line))
        if args.confirm_all_pending_customers:
            assert args.pending_digest, line
        if args.confirm_all_services:
            assert args.mapping_digest, line


def test_the_preparation_commands_never_write_a_booking(commands: list[str]) -> None:
    """The preparation stage cannot migrate a booking, and must not appear to."""
    for line in commands:
        for forbidden in ("--apply", "--confirm-easyweek-native-notifications-disabled", "--canary-record-id"):
            assert forbidden not in line, line


def test_no_command_in_this_runbook_migrates_a_booking(runbook: str) -> None:
    """The handover has an `--apply` of its own; it authorises reminder JOBS.

    What must appear nowhere in this document is the migrator's booking-write
    vocabulary — the notification attestation and the canary — because neither
    the preparation stage nor the handover may ever create an appointment.
    """
    for line in [ln for block in bash_blocks(runbook) for ln in block.splitlines()]:
        for forbidden in (
            "--confirm-easyweek-native-notifications-disabled",
            "--canary-record-id",
            "--canary-company-id",
        ):
            assert forbidden not in line, line


def test_the_verify_step_uses_the_proposed_manifest(commands: list[str]) -> None:
    parser = prep.build_parser()
    verify = [line for line in commands if parser.parse_args(command_args(line)).mode == prep.MODE_VERIFY_DRY_RUN]
    assert verify, "the runbook must show how the verified dry-run id is obtained"
    for line in verify:
        assert "manifest.proposed.json" in parser.parse_args(command_args(line)).manifest, line


def test_the_compose_paths_match_the_container_mounts(commands: list[str]) -> None:
    compose = COMPOSE.read_text()
    assert "/migration/state" in compose
    assert "easyweek-migration-prepare:" in compose

    parser = prep.build_parser()
    for line in commands:
        if "docker compose" not in line:
            continue
        args = parser.parse_args(command_args(line))
        assert args.manifest.startswith("/migration/"), line
        assert "--profile ops" in line, line
        assert "--rm" in line, line


def test_the_compose_service_does_not_pre_authorise_customer_creation() -> None:
    """The permission is typed per command, never baked into the service."""
    compose = COMPOSE.read_text()
    service = compose[compose.index("easyweek-migration-prepare:") :]
    service = service[: service.index("\nvolumes:")]

    assert f"{prep.CREATE_ENV_FLAG}: " not in service
    assert f"{prep.CREATE_ENV_FLAG}=true" not in service


def test_the_runbook_says_read_only_preparation_needs_no_shutdown(runbook: str) -> None:
    text = prose(runbook)
    assert "Read-only часть бота не останавливает" in text


def test_the_runbook_states_the_five_separate_readiness_answers(runbook: str) -> None:
    text = prose(runbook)
    for field in (
        "customers_ready",
        "mapping_ready",
        "records_ready",
        "records_needing_manual_work",
        "blocked_by_technical_error",
    ):
        assert field in text, field
    assert "пять отдельных ответов, а не одно слово" in text


def test_the_runbook_warns_that_a_new_card_has_no_history(runbook: str) -> None:
    text = prose(runbook)
    assert "не переносит историю визитов" in text
    assert "пришедший впервые" in text


def test_the_runbook_says_an_unreadable_lookup_is_not_an_absence(runbook: str) -> None:
    text = prose(runbook)
    assert "это **не** «клиентов нет»" in text
    assert "Повторный `POST` вслепую" in text


def test_the_runbook_points_back_at_the_unchanged_cutover_process(runbook: str) -> None:
    text = prose(runbook)
    assert "pr11_1_cutover_runbook.md" in text
    assert "не даёт права пропустить" in text


def test_the_runbook_says_stdin_is_never_consent(runbook: str) -> None:
    text = prose(runbook)
    assert "не читает stdin" in text
    assert "EOF согласием не являются" in text


def test_the_state_directory_is_named_as_personal_data(runbook: str) -> None:
    text = prose(runbook)
    assert "содержит персональные данные" in text
    assert "0600" in text and "0700" in text
    assert (REPO_ROOT / ".gitignore").read_text().count("easyweek_migration_prepare") >= 1


def test_the_migrator_runbook_commands_are_not_duplicated_here(runbook: str) -> None:
    """One process, not a second migrator: the apply steps stay where they are."""
    parser = migrator.build_parser()
    assert parser.prog == "easyweek_migration"
    for line in [ln for block in bash_blocks(runbook) for ln in block.splitlines()]:
        assert "scripts.easyweek_migration " not in line, line


# ---------------------------------------------------------------------------
# Single confirmations are documented WITH their digests
# ---------------------------------------------------------------------------


def test_every_documented_single_confirmation_carries_a_digest(commands: list[str]) -> None:
    """A bare identifier in the runbook would teach the unbound form back in."""
    parser = prep.build_parser()
    seen = 0
    for line in commands:
        args = parser.parse_args(command_args(line))
        for raw in list(args.confirm_customer) + list(args.confirm_service):
            seen += 1
            target = prep._parse_confirm_target(raw, what="customer")
            assert target.review_digest, line
    assert seen >= 3, "the runbook must show confirming a customer and a service by digest"


def test_the_runbook_states_the_digest_workflow(runbook: str) -> None:
    text = prose(runbook)

    assert "review_digest" in text
    assert "сначала review из шага 2, потом команда" in text
    assert "Голый идентификатор командой отклоняется" in text


def test_the_runbook_says_confirm_rechecks_live_data(runbook: str) -> None:
    text = prose(runbook)

    assert "не верит сохранённому файлу" in text
    assert "заново проверяет branch identity" in text
    assert "сверяет три вещи" in text
    assert "ничего** не меняется" in text


def test_the_runbook_names_drift_and_per_master_availability(runbook: str) -> None:
    text = prose(runbook)

    assert "existing_mapping_drift" in text
    assert "drift_fields" in text
    assert "Не считает совпадение UUID достаточным" in text
    assert "Покрыты должны быть все мастера" in text


def test_the_runbook_promises_prepare_and_confirm_agree(runbook: str) -> None:
    text = prose(runbook)
    assert "одинаковые предложения и одинаковые дайджесты" in text


# ---------------------------------------------------------------------------
# The reminder handover section (plan §30.8)
# ---------------------------------------------------------------------------


HANDOVER_SERVICE = "easyweek-migration-prepare-handover"
HANDOVER_MODULE = "altegio_bot.scripts.easyweek_reminder_handover"


@pytest.fixture(scope="module")
def handover(runbook: str) -> str:
    start = runbook.index("## 6b. Передача напоминаний")
    end = runbook.index("## 7. Что этот этап не делает")
    return runbook[start:end]


@pytest.fixture(scope="module")
def handover_commands(handover: str) -> list[str]:
    lines = [line for block in bash_blocks(handover) for line in block.splitlines() if HANDOVER_SERVICE in line]
    assert lines, "the runbook documents no handover commands"
    return lines


def handover_args(line: str) -> list[str]:
    tokens = shlex.split(line)
    return tokens[tokens.index(HANDOVER_SERVICE) + 1 :]


def test_every_handover_command_parses_with_the_real_parser(handover_commands: list[str]) -> None:
    from altegio_bot.scripts import easyweek_reminder_handover as tool

    parser = tool.build_parser()
    for line in handover_commands:
        args = parser.parse_args(handover_args(line))
        assert args.company_id == [758285], line
        assert args.manifest.startswith("/migration/"), line
        assert args.snapshot.startswith("/migration/state/"), line


def test_all_three_handover_modes_are_documented(handover_commands: list[str]) -> None:
    from altegio_bot.scripts import easyweek_reminder_handover as tool

    parser = tool.build_parser()
    assert {parser.parse_args(handover_args(line)).mode for line in handover_commands} == set(tool.MODES)


def test_only_the_apply_command_can_write(handover_commands: list[str]) -> None:
    """A flag surviving in a saved command line must not turn a read into a write."""
    from altegio_bot.scripts import easyweek_reminder_handover as tool

    parser = tool.build_parser()
    for line in handover_commands:
        args = parser.parse_args(handover_args(line))
        if args.mode == "apply":
            assert args.apply is True, line
            assert args.plan_digest, line
            assert args.confirm, line
            assert tool.APPLY_ENV_FLAG in line, line
        else:
            assert args.apply is False, line
            assert tool.APPLY_ENV_FLAG not in line, line


def test_the_documented_confirmation_matches_the_real_phrase(handover_commands: list[str]) -> None:
    from altegio_bot.easyweek_migration.reminder_handover import confirmation_phrase
    from altegio_bot.scripts import easyweek_reminder_handover as tool

    parser = tool.build_parser()
    applies = [line for line in handover_commands if parser.parse_args(handover_args(line)).mode == "apply"]
    assert len(applies) == 1

    args = parser.parse_args(handover_args(applies[0]))
    assert args.confirm == confirmation_phrase(args.plan_digest)


def test_the_apply_command_restores_the_outbox_on_any_exit(handover: str) -> None:
    """The stop must be survivable: an error must not leave the worker down."""
    applies = [line for block in bash_blocks(handover) for line in block.splitlines() if "apply --manifest" in line]
    assert len(applies) == 1
    line = applies[0]

    assert "trap 'docker compose up -d altegio-outbox-worker' EXIT INT TERM" in line
    assert line.index("trap") < line.index("docker compose stop altegio-outbox-worker"), (
        "the trap has to be armed BEFORE the worker goes down"
    )


def test_only_the_outbox_worker_is_stopped(handover: str) -> None:
    stops = [line for block in bash_blocks(handover) for line in block.splitlines() if "compose stop" in line]
    assert stops
    for line in stops:
        assert "altegio-outbox-worker" in line, line
        for untouched in ("inbox", "capture", "postgres"):
            assert f"stop {untouched}" not in line, line


def test_the_runbook_checks_the_outbox_came_back(handover: str) -> None:
    assert "docker compose ps altegio-outbox-worker" in handover


def test_the_runbook_states_the_three_readiness_questions(handover: str) -> None:
    text = prose(handover)

    for field in ("guard_ready", "coverage_ready", "cutover_ready"):
        assert field in text, field
    assert "Пустая очередь EasyWeek даёт `guard_ready=true` тривиально" in text


def test_the_runbook_does_not_promise_the_notifications_flag_stops_altegio(handover: str) -> None:
    text = prose(handover)

    assert "общим Altegio send fence не является" in text
    assert "не полагайтесь на него" in text


def test_the_runbook_does_not_ask_for_inbox_or_capture_to_stop(handover: str) -> None:
    text = prose(handover)
    assert "Inbox и capture при этом **не** останавливаются" in text


def test_the_observed_production_numbers_are_not_a_contract(handover: str) -> None:
    text = prose(handover)

    assert "56, 84, 223" in text
    assert "не контракт" in text or "а не контракт" in text
    assert "считает всё заново" in text


def test_the_runbook_says_dry_run_changes_nothing(handover: str) -> None:
    text = prose(handover)

    assert "**не меняет ничего**" in text
    assert "Meta и Chatwoot не вызываются" in text


def test_the_runbook_explains_why_the_outbox_stop_is_short(handover: str) -> None:
    text = prose(handover)

    assert "весь обход API — пока outbox работает" in text
    assert "только на время транзакции" in text


def test_the_runbook_states_the_create_before_cancel_order(handover: str) -> None:
    text = prose(handover)

    assert "сначала создаются все недостающие" in text
    assert "оставляет клиенту то напоминание, которое у него уже было" in text


def test_the_runbook_hands_rescheduling_back_to_the_webhooks(handover: str) -> None:
    text = prose(handover)

    assert "целиком за обычными EasyWeek-вебхуками" in text
    assert "Никакого фонового синхронизатора" in text


def test_the_handover_service_does_not_pre_authorise_the_write() -> None:
    from altegio_bot.scripts import easyweek_reminder_handover as tool

    compose = COMPOSE.read_text()
    service = compose[compose.index(f"{HANDOVER_SERVICE}:") :]
    service = service[: service.index("\nvolumes:")]

    assert f"{tool.APPLY_ENV_FLAG}: " not in service
    assert f"{tool.APPLY_ENV_FLAG}=true" not in service
    assert "profiles:" in service and "ops" in service


def test_the_handover_command_cannot_migrate_a_booking_or_create_a_customer() -> None:
    from altegio_bot.scripts import easyweek_reminder_handover as tool

    actions = {action.dest for action in tool.build_parser()._actions}

    assert "authorise_customer_create" not in actions
    assert "verified_dry_run_id" not in actions
    assert "canary_record_id" not in actions


def test_the_runbook_says_a_correction_survives_a_rebuild(runbook: str) -> None:
    text = prose(runbook)

    assert "переживает пересборку" in text
    assert "correction_source_identity_changed" in text
    assert "а не к имени" in text


def test_the_runbook_names_the_inherited_mapping_refusal(runbook: str) -> None:
    text = prose(runbook)

    assert "existing_mapping_staff_unavailable" in text
    assert "Не считает mapping из прошлой волны разрешением" in text


def test_the_runbook_claims_no_custom_duration_support(runbook: str) -> None:
    text = prose(runbook)

    assert "Не утверждает, что EasyWeek API принимает индивидуальную длительность" in text
    assert "manual_adjustment_candidate" in text
