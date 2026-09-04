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


@pytest.fixture(scope="module")
def commands(runbook: str) -> list[str]:
    lines = []
    for block in bash_blocks(runbook):
        for line in block.splitlines():
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


def test_the_runbook_never_documents_a_booking_write(runbook: str) -> None:
    """This stage cannot migrate a booking, and must not appear to."""
    for forbidden in ("--apply", "--confirm-easyweek-native-notifications-disabled", "--canary-record-id"):
        for line in [ln for block in bash_blocks(runbook) for ln in block.splitlines()]:
            if PREP_MODULE in line or "easyweek-migration-prepare" in line:
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
