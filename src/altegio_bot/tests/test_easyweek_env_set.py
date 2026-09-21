"""§38.9: the helper that actually changes easyweek.env, and what it refuses.

Every test runs against a throwaway fixture file. The property that matters
most is the boring one: when the helper refuses, the file it was pointed at is
byte-for-byte what it was — because the runbook step right after it recreates
a production worker from that file.
"""

from __future__ import annotations

import ast
import os
import stat
import sys
from pathlib import Path

import pytest

from altegio_bot.scripts.easyweek_env_set import (
    ALLOWED_KEYS,
    BOOTSTRAP_ALREADY_PRESENT,
    BOOTSTRAP_CREATED,
    BOOTSTRAP_KEY,
    BOOTSTRAP_VALUE,
    EnvEditError,
    apply_assignments,
    bootstrap_canary_key,
    count_assignments,
    edit_env_file,
    main,
    parse_assignments,
)

SECRET = "sk-live-must-never-be-printed"

_FIXTURE = f"""# EasyWeek rollout flags
EASYWEEK_NOTIFICATIONS_ENABLED=true
EASYWEEK_REMINDERS_ENABLED=true
EASYWEEK_REMINDER_API_GUARD_ENABLED=true
EASYWEEK_MULTI_SERVICE_NOTIFICATIONS_ENABLED=true
EASYWEEK_RESOURCE_SHADOW_PROOF_ENABLED=true
EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false
EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=
EASYWEEK_API_KEY={SECRET}
"""


@pytest.fixture
def env_file(tmp_path: Path) -> Path:
    path = tmp_path / "easyweek.env"
    path.write_text(_FIXTURE, encoding="utf-8")
    path.chmod(0o640)
    return path


def _backups(path: Path) -> list[Path]:
    return sorted(path.parent.glob(f"{path.name}.bak.*"))


def _temp_leftovers(path: Path) -> list[Path]:
    return sorted(path.parent.glob(f".{path.name}.*"))


# ===========================================================================
# The allowlist
# ===========================================================================


def test_only_rollout_flags_are_editable() -> None:
    assert "EASYWEEK_API_KEY" not in ALLOWED_KEYS
    assert "EASYWEEK_WEBHOOK_SECRET" not in ALLOWED_KEYS
    assert "DATABASE_URL" not in ALLOWED_KEYS
    assert "EASYWEEK_MULTI_SERVICE_SEND_ENABLED" in ALLOWED_KEYS
    assert "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID" in ALLOWED_KEYS


def test_a_secret_key_is_refused_and_the_file_is_untouched(env_file: Path) -> None:
    before = env_file.read_bytes()
    with pytest.raises(EnvEditError):
        parse_assignments([f"EASYWEEK_API_KEY={SECRET}"])
    assert env_file.read_bytes() == before
    assert _backups(env_file) == []


@pytest.mark.parametrize("value", ["True", "1", "yes", "", "TRUE"])
def test_a_bool_flag_takes_exactly_true_or_false(value: str) -> None:
    with pytest.raises(EnvEditError):
        parse_assignments([f"EASYWEEK_MULTI_SERVICE_SEND_ENABLED={value}"])


@pytest.mark.parametrize("value", ["0", "-1", "1,2", "1 2", "true", "+1", "1.0", "١٤"])
def test_the_canary_value_mirrors_the_runtime_parser(value: str) -> None:
    with pytest.raises(EnvEditError):
        parse_assignments([f"EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID={value}"])


def test_the_canary_accepts_empty_and_one_positive_id() -> None:
    assert parse_assignments(["EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID="]) == {"EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID": ""}
    assert parse_assignments(["EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=14211"]) == {
        "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID": "14211"
    }


# ===========================================================================
# Exactly one assignment, or nothing happens
# ===========================================================================


def test_a_duplicate_assignment_refuses_before_any_write(env_file: Path) -> None:
    env_file.write_text(
        env_file.read_text(encoding="utf-8") + "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false\n",
        encoding="utf-8",
    )
    before = env_file.read_bytes()

    with pytest.raises(EnvEditError, match="expected exactly one"):
        edit_env_file(env_file, {"EASYWEEK_MULTI_SERVICE_SEND_ENABLED": "true"})

    assert env_file.read_bytes() == before, "a refused edit must leave the file untouched"
    assert _backups(env_file) == []
    assert _temp_leftovers(env_file) == []


def test_a_missing_assignment_refuses_before_any_write(tmp_path: Path) -> None:
    path = tmp_path / "easyweek.env"
    path.write_text("EASYWEEK_NOTIFICATIONS_ENABLED=true\n", encoding="utf-8")
    before = path.read_bytes()

    with pytest.raises(EnvEditError, match="no assignment found"):
        edit_env_file(path, {"EASYWEEK_MULTI_SERVICE_SEND_ENABLED": "true"})

    assert path.read_bytes() == before
    assert _backups(path) == []


def test_a_second_bad_key_does_not_leave_the_first_one_changed(env_file: Path) -> None:
    """The count check runs over every key before a single line is rewritten."""
    env_file.write_text(
        env_file.read_text(encoding="utf-8") + "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=5\n",
        encoding="utf-8",
    )
    before = env_file.read_bytes()

    with pytest.raises(EnvEditError):
        edit_env_file(
            env_file,
            {
                "EASYWEEK_MULTI_SERVICE_SEND_ENABLED": "true",
                "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID": "7",
            },
        )

    assert env_file.read_bytes() == before


def test_a_commented_out_line_is_not_an_assignment(tmp_path: Path) -> None:
    path = tmp_path / "easyweek.env"
    path.write_text(
        "# EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true\nEASYWEEK_MULTI_SERVICE_SEND_ENABLED=false\n",
        encoding="utf-8",
    )
    edit_env_file(path, {"EASYWEEK_MULTI_SERVICE_SEND_ENABLED": "true"})
    assert path.read_text(encoding="utf-8") == (
        "# EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true\nEASYWEEK_MULTI_SERVICE_SEND_ENABLED=true\n"
    )


def test_apply_is_pure_and_reports_only_what_changed() -> None:
    lines = ["EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false\n", "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=\n"]
    updated, changed = apply_assignments(
        list(lines),
        {"EASYWEEK_MULTI_SERVICE_SEND_ENABLED": "false", "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID": "9"},
    )
    assert changed == {"EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID": "9"}
    assert updated[0] == lines[0]


# ===========================================================================
# The write itself
# ===========================================================================


def test_one_flag_changes_and_everything_else_is_byte_identical(env_file: Path) -> None:
    before = env_file.read_text(encoding="utf-8").splitlines()

    changed = edit_env_file(env_file, {"EASYWEEK_MULTI_SERVICE_SEND_ENABLED": "true"})

    assert changed == {"EASYWEEK_MULTI_SERVICE_SEND_ENABLED": "true"}
    after = env_file.read_text(encoding="utf-8").splitlines()
    differences = [(left, right) for left, right in zip(before, after, strict=True) if left != right]
    assert differences == [("EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false", "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true")]
    assert f"EASYWEEK_API_KEY={SECRET}" in env_file.read_text(encoding="utf-8")


def test_the_file_mode_survives_the_atomic_replace(env_file: Path) -> None:
    edit_env_file(env_file, {"EASYWEEK_MULTI_SERVICE_SEND_ENABLED": "true"})
    assert stat.S_IMODE(env_file.stat().st_mode) == 0o640
    assert _temp_leftovers(env_file) == []


def test_the_backup_is_the_previous_content_with_closed_permissions(env_file: Path) -> None:
    original = env_file.read_text(encoding="utf-8")

    edit_env_file(env_file, {"EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID": "14211"})

    backups = _backups(env_file)
    assert len(backups) == 1
    assert backups[0].read_text(encoding="utf-8") == original
    assert stat.S_IMODE(backups[0].stat().st_mode) == 0o600


def test_setting_the_value_it_already_has_writes_nothing(env_file: Path) -> None:
    before = env_file.read_bytes()
    assert edit_env_file(env_file, {"EASYWEEK_MULTI_SERVICE_SEND_ENABLED": "false"}) == {}
    assert env_file.read_bytes() == before
    assert _backups(env_file) == []


def test_a_dry_run_reports_the_change_without_touching_the_file(env_file: Path) -> None:
    before = env_file.read_bytes()
    changed = edit_env_file(env_file, {"EASYWEEK_MULTI_SERVICE_SEND_ENABLED": "true"}, dry_run=True)
    assert changed == {"EASYWEEK_MULTI_SERVICE_SEND_ENABLED": "true"}
    assert env_file.read_bytes() == before
    assert _backups(env_file) == []


# ===========================================================================
# The command line
# ===========================================================================


def test_stdout_shows_only_the_changed_keys_and_never_a_secret(
    env_file: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = main(
        [
            "--env-file",
            str(env_file),
            "--set",
            "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true",
            "--set",
            "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=14211",
        ]
    )
    captured = capsys.readouterr()

    assert code == 0
    assert captured.out.splitlines() == [
        "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=14211",
        "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true",
    ]
    assert SECRET not in captured.out
    assert SECRET not in captured.err
    assert "EASYWEEK_NOTIFICATIONS_ENABLED" not in captured.out


def test_a_refusal_exits_non_zero_and_names_the_key_only(
    env_file: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    before = env_file.read_bytes()
    code = main(["--env-file", str(env_file), "--set", f"EASYWEEK_API_KEY={SECRET}"])
    captured = capsys.readouterr()

    assert code == 1
    assert captured.out == ""
    assert "not an editable rollout key" in captured.err
    assert SECRET not in captured.err
    assert env_file.read_bytes() == before


def test_a_missing_file_is_refused_rather_than_created(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    missing = tmp_path / "nope.env"
    code = main(["--env-file", str(missing), "--set", "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true"])
    assert code == 1
    assert not missing.exists()
    assert "refused" in capsys.readouterr().err


def test_the_helper_needs_no_project_import_and_no_third_party_package() -> None:
    """It runs with the host's bare python3, so it must import only stdlib.

    Asserted over the parsed import statements rather than the file text: the
    module docstring legitimately names the package it must not import.
    """
    source = Path(__file__).resolve().parents[1] / "scripts" / "easyweek_env_set.py"
    tree = ast.parse(source.read_text(encoding="utf-8"))
    imported: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, "a relative import would need the package on sys.path"
            imported.add((node.module or "").split(".", 1)[0])
    assert imported, "the parser found no imports at all"
    assert imported <= sys.stdlib_module_names, sorted(imported - sys.stdlib_module_names)


def test_no_temp_file_survives_a_successful_run(env_file: Path) -> None:
    main(["--env-file", str(env_file), "--set", "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true"])
    assert _temp_leftovers(env_file) == []
    assert os.path.isfile(env_file)


# ===========================================================================
# §38.9: creating the canary key on a file that predates it
# ===========================================================================


_PRE_38_9_FIXTURE = f"""# EasyWeek rollout flags
EASYWEEK_NOTIFICATIONS_ENABLED=true
EASYWEEK_MULTI_SERVICE_SEND_ENABLED=false
EASYWEEK_API_KEY={SECRET}
"""


@pytest.fixture
def legacy_env_file(tmp_path: Path) -> Path:
    """The production file as it exists before §38.9: no canary key at all."""
    path = tmp_path / "easyweek.env"
    path.write_text(_PRE_38_9_FIXTURE, encoding="utf-8")
    path.chmod(0o640)
    return path


def test_a_plain_set_of_the_missing_key_still_refuses(legacy_env_file: Path) -> None:
    """The strictness bootstrap exists to work around must stay intact."""
    before = legacy_env_file.read_bytes()
    with pytest.raises(EnvEditError, match="no assignment found"):
        edit_env_file(legacy_env_file, {"EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID": ""})
    assert legacy_env_file.read_bytes() == before


def test_bootstrap_creates_the_missing_key_empty(legacy_env_file: Path) -> None:
    outcome = bootstrap_canary_key(legacy_env_file)

    assert outcome == BOOTSTRAP_CREATED
    lines = legacy_env_file.read_text(encoding="utf-8").splitlines()
    assert lines[-1] == "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID="
    # Everything that was there before is byte-for-byte where it was.
    assert lines[:-1] == _PRE_38_9_FIXTURE.splitlines()
    assert f"EASYWEEK_API_KEY={SECRET}" in legacy_env_file.read_text(encoding="utf-8")


def test_bootstrap_is_idempotent_and_never_duplicates(legacy_env_file: Path) -> None:
    assert bootstrap_canary_key(legacy_env_file) == BOOTSTRAP_CREATED
    after_first = legacy_env_file.read_bytes()

    assert bootstrap_canary_key(legacy_env_file) == BOOTSTRAP_ALREADY_PRESENT

    assert legacy_env_file.read_bytes() == after_first
    assert count_assignments(legacy_env_file.read_text(encoding="utf-8").splitlines(True), BOOTSTRAP_KEY) == 1
    assert len(_backups(legacy_env_file)) == 1, "the no-op run creates no second backup"


def test_bootstrap_leaves_an_existing_value_alone(env_file: Path) -> None:
    """A key already set to a real canary must not be reset to empty."""
    edit_env_file(env_file, {"EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID": "14211"})
    before = env_file.read_bytes()

    assert bootstrap_canary_key(env_file) == BOOTSTRAP_ALREADY_PRESENT

    assert env_file.read_bytes() == before
    assert "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=14211" in env_file.read_text(encoding="utf-8")


def test_bootstrap_refuses_duplicate_assignments_without_writing(legacy_env_file: Path) -> None:
    legacy_env_file.write_text(
        legacy_env_file.read_text(encoding="utf-8")
        + "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=\nEASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=7\n",
        encoding="utf-8",
    )
    before = legacy_env_file.read_bytes()

    with pytest.raises(EnvEditError, match="expected zero or one"):
        bootstrap_canary_key(legacy_env_file)

    assert legacy_env_file.read_bytes() == before
    assert _backups(legacy_env_file) == []
    assert _temp_leftovers(legacy_env_file) == []


def test_bootstrap_treats_a_commented_key_as_absent(legacy_env_file: Path) -> None:
    legacy_env_file.write_text(
        legacy_env_file.read_text(encoding="utf-8") + "# EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=14211\n",
        encoding="utf-8",
    )

    assert bootstrap_canary_key(legacy_env_file) == BOOTSTRAP_CREATED

    text = legacy_env_file.read_text(encoding="utf-8")
    assert "# EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=14211" in text, "the comment is preserved as written"
    assert count_assignments(text.splitlines(True), BOOTSTRAP_KEY) == 1


def test_bootstrap_dry_run_writes_nothing_and_makes_no_backup(legacy_env_file: Path) -> None:
    before = legacy_env_file.read_bytes()

    assert bootstrap_canary_key(legacy_env_file, dry_run=True) == BOOTSTRAP_CREATED

    assert legacy_env_file.read_bytes() == before
    assert _backups(legacy_env_file) == []
    assert _temp_leftovers(legacy_env_file) == []


def test_bootstrap_keeps_the_file_mode_and_closes_the_backup(legacy_env_file: Path) -> None:
    original = legacy_env_file.read_text(encoding="utf-8")

    bootstrap_canary_key(legacy_env_file)

    assert stat.S_IMODE(legacy_env_file.stat().st_mode) == 0o640
    backups = _backups(legacy_env_file)
    assert len(backups) == 1
    assert backups[0].read_text(encoding="utf-8") == original
    assert stat.S_IMODE(backups[0].stat().st_mode) == 0o600
    assert _temp_leftovers(legacy_env_file) == []


def test_bootstrap_adds_a_newline_before_appending_to_an_unterminated_file(tmp_path: Path) -> None:
    path = tmp_path / "easyweek.env"
    path.write_text("EASYWEEK_NOTIFICATIONS_ENABLED=true", encoding="utf-8")

    bootstrap_canary_key(path)

    assert path.read_text(encoding="utf-8") == (
        "EASYWEEK_NOTIFICATIONS_ENABLED=true\nEASYWEEK_MULTI_SERVICE_CANARY_JOB_ID=\n"
    )


def test_the_cli_bootstrap_prints_only_the_created_key(
    legacy_env_file: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    code = main(["--env-file", str(legacy_env_file), "--bootstrap-canary-key"])
    captured = capsys.readouterr()

    assert code == 0
    assert captured.out.splitlines() == ["EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID="]
    assert SECRET not in captured.out
    assert SECRET not in captured.err


def test_the_cli_bootstrap_reports_an_existing_key_without_writing(
    legacy_env_file: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    main(["--env-file", str(legacy_env_file), "--bootstrap-canary-key"])
    capsys.readouterr()
    before = legacy_env_file.read_bytes()

    code = main(["--env-file", str(legacy_env_file), "--bootstrap-canary-key"])
    captured = capsys.readouterr()

    assert code == 0
    assert captured.out == ""
    assert "already present" in captured.err
    assert legacy_env_file.read_bytes() == before


def test_the_cli_bootstrap_cannot_be_combined_with_set(
    legacy_env_file: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    before = legacy_env_file.read_bytes()
    code = main(
        [
            "--env-file",
            str(legacy_env_file),
            "--bootstrap-canary-key",
            "--set",
            "EASYWEEK_MULTI_SERVICE_SEND_ENABLED=true",
        ]
    )
    assert code == 1
    assert "cannot be combined with --set" in capsys.readouterr().err
    assert legacy_env_file.read_bytes() == before


def test_bootstrap_can_only_ever_create_that_one_key_empty() -> None:
    """The narrowness is the point: it is not a general append mode."""
    import inspect

    source = inspect.getsource(bootstrap_canary_key)
    assert "BOOTSTRAP_KEY" in source
    assert "BOOTSTRAP_VALUE" in source
    assert BOOTSTRAP_KEY == "EASYWEEK_MULTI_SERVICE_CANARY_JOB_ID"
    assert BOOTSTRAP_VALUE == ""
    signature = inspect.signature(bootstrap_canary_key)
    assert list(signature.parameters) == ["path", "dry_run"], "no key or value argument exists"
