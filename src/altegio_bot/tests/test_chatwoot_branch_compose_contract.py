"""Deployment contract for PR-7 branch-scoped Chatwoot routing."""

from __future__ import annotations

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

from altegio_bot.scripts.verify_pre_hotfix_env_backup import verify_backup_map
from altegio_bot.webhooks.common import (
    parse_chatwoot_inbox_company_map,
    positive_int,
    resolve_chatwoot_general_inbox,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
BASE_COMPOSE = REPO_ROOT / "docker-compose.yml"
CHATWOOT_OVERRIDE = REPO_ROOT / "docker-compose.chatwoot-internal.yml"
ACTIVATION_RUNBOOK = REPO_ROOT / "docs/easyweek/durlach_activation_runbook.md"
ENV_EXAMPLE = REPO_ROOT / ".env.example"
CHATWOOT_CONSUMERS = ("altegio-outbox-worker", "altegio-whatsapp-inbox-worker")


def test_base_compose_does_not_require_external_chatwoot_network() -> None:
    config = yaml.safe_load(BASE_COMPOSE.read_text())

    assert "chatwoot_internal" not in config.get("networks", {})
    for service_name in CHATWOOT_CONSUMERS:
        networks = config["services"][service_name].get("networks", [])
        assert "chatwoot_internal" not in networks


def test_production_override_attaches_both_workers_to_both_networks() -> None:
    config = yaml.safe_load(CHATWOOT_OVERRIDE.read_text())

    assert config["networks"]["chatwoot_internal"]["external"] is True
    for service_name in CHATWOOT_CONSUMERS:
        assert config["services"][service_name]["networks"] == ["default", "chatwoot_internal"]


def test_activation_runbook_preserves_production_compose_file_set() -> None:
    text = ACTIVATION_RUNBOOK.read_text()

    assert (
        'COMPOSE="docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml"'
    ) in text
    assert "$COMPOSE up -d --force-recreate \\\n  altegio-outbox-worker altegio-whatsapp-inbox-worker" in text
    assert "$COMPOSE up -d --force-recreate \\\n  altegio-easyweek-inbox-worker altegio-outbox-worker" in text
    assert "$COMPOSE stop altegio-outbox-worker" in text
    assert "$COMPOSE up -d altegio-outbox-worker" in text
    assert "$COMPOSE up -d --force-recreate altegio-whatsapp-inbox-worker" in text


def test_activation_runbook_checks_both_worker_networks_and_api() -> None:
    text = ACTIVATION_RUNBOOK.read_text()

    worker_loop = "for CHATWOOT_SERVICE in altegio-outbox-worker altegio-whatsapp-inbox-worker; do"
    assert text.count(worker_loop) >= 3
    assert text.count("docker inspect") >= 2
    assert ".NetworkSettings.Networks" in text
    assert "socket.getaddrinfo" in text
    assert '"api_access_token": settings.chatwoot_api_token' in text
    assert '"api_status": response.status_code' in text
    assert "response.text" not in text


def test_activation_runbook_gates_isolated_general_inbox() -> None:
    text = ACTIVATION_RUNBOOK.read_text()

    assert '"global_general_inbox_configured": configured_general_id is not None' in text
    assert '"global_general_inbox_distinct_from_branches": general_inbox_id is not None' in text
    assert "resolve_chatwoot_general_inbox" in text
    assert "general_inbox_overlaps_branch" in text
    assert "**STOP**" in text


def test_activation_runbook_documents_only_the_explicit_identityless_general_routes() -> None:
    text = ACTIVATION_RUNBOOK.read_text()

    assert "STOP ACK, START ACK" in text
    assert "synchronous promo info" in text
    assert "synchronous promo funnel" in text
    assert "Это не EasyWeek promo и не филиальная маршрутизация" in text
    assert "обычный lifecycle send" in text
    assert "неявный fallback в General" in text


def test_official_env_example_uses_provider_scoped_branches_and_separate_general() -> None:
    lines = ENV_EXAMPLE.read_text().splitlines()
    general_id = int(next(line.split("=", 1)[1] for line in lines if line.startswith("CHATWOOT_INBOX_ID=")))
    example_prefix = "# Example: CHATWOOT_INBOX_COMPANY_MAP="
    raw_map = next(line.removeprefix(example_prefix) for line in lines if line.startswith(example_prefix))

    parsed = parse_chatwoot_inbox_company_map(raw_map)
    assert parsed.configured is True
    assert parsed.valid is True
    assert parsed.provider_scoped is True
    assert general_id not in parsed.mapping
    assert resolve_chatwoot_general_inbox(parsed, general_id) == (general_id, None)


# ---------------------------------------------------------------------------
# PR-7.4 single-inbox hotfix: the rollout has to be executable, not indicative
# ---------------------------------------------------------------------------
#
# Two earlier drafts of §14 were unsafe in ways that only show up on the day it
# is actually run:
#
#   * bare `KEY=value` lines set shell variables and vanish — `.env` untouched,
#     Compose none the wiser, and the operator believing the rollback is armed;
#   * `cat "$ENV_TMP" > .env` truncates the live production `.env` FIRST, so an
#     interrupted write leaves it empty or half-written, and `grep ... || true`
#     swallowed real read errors on the way there — after which the temp file
#     held three Chatwoot keys and every other production secret was gone.
#
# These tests do not paraphrase the runbook. They lift its own bash blocks out
# of the markdown and run them against a throwaway `.env`, so "the documented
# procedure is safe" is a fact rather than a claim.

SINGLE_INBOX_KEYS = (
    "CHATWOOT_INBOX_COMPANY_MAP",
    "CHATWOOT_INBOUND_ROUTING_MODE",
    "CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID",
)
RECREATE_BOTH_WORKERS = "$COMPOSE up -d --force-recreate \\\n  altegio-outbox-worker altegio-whatsapp-inbox-worker"

# The confirmed production topology, as the runbook states it. Restoring
# anything else is not a rollback.
PRODUCTION_BRANCH_MAP = (
    '{"9":{"provider":"altegio","company_id":758285},'
    '"10":{"provider":"easyweek","company_id":315607},'
    '"11":{"provider":"easyweek","company_id":308697}}'
)
# Deliberately fake: no test may read or print a real production secret.
FIXTURE_ENV_LINES = (
    "APP_NAME=altegio_bot",
    "WHATSAPP_ACCESS_TOKEN=NOT_A_REAL_TOKEN_FIXTURE",
    "CHATWOOT_INBOX_ID=8",
    f"CHATWOOT_INBOX_COMPANY_MAP={PRODUCTION_BRANCH_MAP}",
    "CHATWOOT_INBOUND_ROUTING_MODE=affinity",
    "# trailing comment",
    "",
)


def _single_inbox_section() -> str:
    """Only the PR-7.4 chapter, bounded at both ends like every other one."""
    text = ACTIVATION_RUNBOOK.read_text()
    start = text.index("## 14. PR-7.4")
    following = re.search(r"^## \d+\. ", text[start + 1 :], flags=re.M)
    end = start + 1 + following.start() if following else len(text)
    return text[start:end]


def _runbook_block(label: str) -> str:
    """The body of one `bash <<'LABEL'` block, verbatim from the runbook."""
    match = re.search(rf"bash <<'{label}'\n(.*?)\n{label}\n", _single_inbox_section(), flags=re.S)
    assert match is not None, f"runbook block {label} is missing"
    return match.group(1)


def _run_block(
    label: str,
    *,
    env_file,
    handoff_file,
    extra_env: dict[str, str] | None = None,
    script: str | None = None,
) -> subprocess.CompletedProcess[str]:
    """Execute a runbook block against a throwaway .env, never the real one."""
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "ENV_FILE": str(env_file),
        "HANDOFF_FILE": str(handoff_file),
    }
    env.update(extra_env or {})
    return subprocess.run(
        ["bash", "-c", script if script is not None else _runbook_block(label)],
        capture_output=True,
        text=True,
        env=env,
    )


@pytest.fixture
def env_fixture(tmp_path):
    """A stand-in `.env` plus a handoff path, both inside tmp_path."""
    env_file = tmp_path / ".env"
    env_file.write_text("\n".join(FIXTURE_ENV_LINES), encoding="utf-8")
    return env_file, tmp_path / "hotfix.handoff"


# --- static contract --------------------------------------------------------


def test_single_inbox_rollout_edits_the_real_env_file() -> None:
    section = _single_inbox_section()

    assert "cd /opt/altegio_bot" in section
    assert 'cp -p "$ENV_FILE" "$BACKUP_FILE"' in section, "the backup must preserve mode and owner"

    for key in SINGLE_INBOX_KEYS:
        # A bare assignment line is a shell variable that Compose never sees.
        assert not re.search(rf"^{key}=", section, flags=re.M), f"{key} is set as a shell variable"


def test_the_env_edit_is_never_an_in_place_truncation() -> None:
    """The exact defect: `cat "$TMP" > .env` empties production before writing."""
    section = _single_inbox_section()

    assert 'cat "$ENV_TMP" > .env' not in section
    assert 'cat "$ENV_TMP" > "$ENV_FILE"' not in section
    assert not re.search(r"^\s*(cat|printf|echo)[^\n]*>\s*\"?\$?ENV_FILE", section, flags=re.M)
    assert not re.search(r">\s*\"\$ENV_FILE\"", section), "nothing may redirect into the live .env"


def test_the_env_edit_uses_a_sibling_temp_file_and_an_atomic_rename() -> None:
    section = _single_inbox_section()

    # Same filesystem as .env, so the rename is atomic and cannot cross devices.
    assert section.count('ENV_TMP="$(mktemp "${ENV_FILE}.rollout.XXXXXX")"') == 1
    assert section.count('ENV_TMP="$(mktemp "${ENV_FILE}.rollback.XXXXXX")"') == 1
    assert not re.search(r"mktemp\s*\)", section), "mktemp must always be given a directory"
    assert not re.search(r"mktemp\s*\n", section)
    assert section.count('mv -f "$ENV_TMP" "$ENV_FILE"') == 2, "rollout and rollback both rename atomically"
    assert section.count("""trap 'rm -f "$ENV_TMP"' EXIT""") == 2, "both blocks clean their temp up"
    assert section.count("set -euo pipefail") >= 4, "every mutating block fails fast"


def test_a_read_error_is_never_masked_by_true() -> None:
    section = _single_inbox_section()

    assert "|| true" not in section, "|| true hides a real read error as well as the benign exit 1"
    # Only the documented benign code is tolerated, and only explicitly.
    assert section.count('grep -Ev "$MANAGED_RE" "$ENV_FILE" > "$ENV_TMP" || KEPT_RC=$?') == 2
    assert section.count('test "$KEPT_RC" -le 1') == 2


def test_rollout_and_rollback_share_one_write_contract() -> None:
    section = _single_inbox_section()
    verification = (
        "for KEY in CHATWOOT_INBOX_COMPANY_MAP CHATWOOT_INBOUND_ROUTING_MODE "
        "CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID; do\n"
        '  test "$(grep -Ec "^${KEY}=" "$ENV_TMP")" = "1"\n'
        "done"
    )

    assert section.count(verification) == 2, "both directions verify the temp file before replacing"
    assert section.count('diff <(grep -Ev "$MANAGED_RE" "$ENV_FILE") <(grep -Ev "$MANAGED_RE" "$ENV_TMP")') == 2
    assert section.count('cp -p "$ENV_FILE" "$ENV_TMP"') == 2, "the temp inherits owner/group/mode"


def test_single_inbox_rollout_validates_the_sender_id_before_writing_it() -> None:
    section = _single_inbox_section()

    assert "grep -Eq '^[1-9][0-9]*$'" in section, "only a positive integer may be written"


def test_single_inbox_rollout_never_prints_env_contents_or_secrets() -> None:
    section = _single_inbox_section()

    for leak in ("cat .env", 'cat "$ENV_FILE"', "grep CHATWOOT_API_TOKEN", "printenv"):
        assert leak not in section, leak
    assert "chatwoot_api_token" not in section
    assert "> /dev/null" in section, "the diff must not print .env content on mismatch"


def test_single_inbox_hotfix_does_not_touch_easyweek_env() -> None:
    section = _single_inbox_section()

    assert "easyweek.env" in section, "the runbook must say the file is NOT involved"
    assert "не трогает и править его не нужно" in section


def test_single_inbox_rollout_and_rollback_recreate_both_chatwoot_consumers() -> None:
    section = _single_inbox_section()

    assert section.count(RECREATE_BOTH_WORKERS) == 2, "both rollout and rollback recreate both workers"
    assert "restart" not in section.replace("`restart`", ""), "a plain restart does not re-read .env"
    for service_name in CHATWOOT_CONSUMERS:
        assert service_name in section


def test_single_inbox_rollback_restores_all_three_parameters() -> None:
    section = _single_inbox_section()
    rollback = section[section.index("### 14.7") :]

    assert 'printf \'%s\\n\' "$BACKUP_MAP_LINE" >> "$ENV_TMP"' in rollback, "the exact old map line comes back"
    assert "CHATWOOT_INBOUND_ROUTING_MODE=affinity" in rollback
    assert "CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID=0" in rollback
    assert "ambiguous_sender" in rollback, "removing only the sender id is explicitly not a rollback"


def test_single_inbox_rollout_and_rollback_both_preflight_both_workers() -> None:
    section = _single_inbox_section()
    worker_loop = "for CHATWOOT_SERVICE in altegio-outbox-worker altegio-whatsapp-inbox-worker; do"

    # Support gate, rollout preflight, rollback preflight.
    assert section.count(worker_loop) >= 3
    assert section.count('"single_inbox_sender_supported"') >= 3
    assert section.count('"branch_map_provider_scoped": parsed.provider_scoped') == 2
    assert section.count('"mode": settings.chatwoot_inbound_routing_mode') == 2


def test_single_inbox_stop_conditions_cover_split_worker_configuration() -> None:
    section = _single_inbox_section()

    assert "single_inbox_sender_supported: False" in section
    assert "worker видят разные значения" in section
    assert "один worker остался в `general`" in section
    assert "продолжает видеть пустую карту" in section
    assert "маршрутизируется в филиал технического" in section
    assert "зеркала" in section


# --- General inbox validity is proved, not inferred from an empty map -------


def test_the_preflight_proves_a_positive_general_inbox_id() -> None:
    section = _single_inbox_section()

    assert section.count('"general_inbox_id_valid": positive_int(settings.chatwoot_inbox_id) is not None') == 2
    # The old expression called an unset General inbox "isolated" purely because
    # the branch map was empty.
    assert "validated_general_id is not None or not parsed.configured" not in section
    assert (
        section.count('"general_inbox_isolated": (validated_general_id is not None) if parsed.configured else None')
        == 2
    )


def test_both_gates_require_a_valid_general_inbox_id() -> None:
    section = _single_inbox_section()

    assert section.count("`general_inbox_id_valid: True`") == 2, "rollout and rollback gates both demand it"
    assert "`general_inbox_id: 8`" in section, "the confirmed production General inbox"
    assert "general_inbox_id_valid: False" in section, "and it is a STOP condition"


@pytest.mark.parametrize(
    "inbox_id, expected",
    [
        pytest.param(0, False, id="unset"),
        pytest.param(-1, False, id="negative"),
        pytest.param("8", False, id="string"),
        pytest.param(True, False, id="bool"),
        pytest.param(8, True, id="production_general_inbox"),
    ],
)
def test_general_inbox_validity_never_comes_from_an_empty_map(inbox_id: object, expected: bool) -> None:
    """The preflight expression itself, evaluated the way the runbook does."""
    parsed = parse_chatwoot_inbox_company_map("{}")

    assert parsed.configured is False
    # The central resolver is deliberately unchanged: unconfigured map means
    # "legacy single-inbox mode", which is NOT proof of a usable General id.
    assert resolve_chatwoot_general_inbox(parsed, inbox_id) == (None, None)
    assert (positive_int(inbox_id) is not None) is expected


def test_the_central_general_inbox_resolver_is_untouched() -> None:
    """Its two documented contracts still hold; the fix lives in the runbook."""
    configured = parse_chatwoot_inbox_company_map(PRODUCTION_BRANCH_MAP)

    assert resolve_chatwoot_general_inbox(configured, 8) == (8, None)
    assert resolve_chatwoot_general_inbox(configured, 9) == (None, "general_inbox_overlaps_branch")
    assert resolve_chatwoot_general_inbox(configured, 0) == (None, "invalid_general_inbox_id")


# --- the rollback source is a proven backup, not the newest filename ---------


def test_the_rollback_never_picks_a_backup_by_filename() -> None:
    section = _single_inbox_section()
    rollback = section[section.index("### 14.7") :]

    assert "ls -1 .env.bak.*" not in section, "a newer backup may already hold the emptied map"
    assert "tail -1" not in section
    assert "PRE_HOTFIX_ENV_BACKUP=" in rollback
    assert rollback.count("grep -E '^PRE_HOTFIX_ENV_BACKUP=' \"$HANDOFF_FILE\"") == 2
    assert rollback.count('case "$PRE_HOTFIX_ENV_BACKUP" in "${ENV_FILE}.bak."*)') == 2
    assert rollback.count('test ! -L "$PRE_HOTFIX_ENV_BACKUP"') == 2


def test_the_rollout_writes_the_handoff_once_and_never_silently_replaces_it() -> None:
    section = _single_inbox_section()

    assert 'if [ -e "$HANDOFF_FILE" ]; then' in section
    assert 'mv -n "$HANDOFF_TMP" "$HANDOFF_FILE"' in section, "-n so a second run cannot overwrite it"
    assert "Handoff и pre-hotfix backup удаляются только после успешного post-rollback" in section


def test_the_rollback_proves_the_backup_map_before_touching_env() -> None:
    section = _single_inbox_section()
    rollback = section[section.index("### 14.7") :]

    assert "python -m altegio_bot.scripts.verify_pre_hotfix_env_backup" in rollback
    assert f"EXPECTED_BRANCH_MAP='{PRODUCTION_BRANCH_MAP}'" in rollback
    # The gate is stated, and it precedes the block that writes .env.
    assert rollback.index("backup_ok: True") < rollback.index("ROLLBACK_ENV")
    assert "post_hotfix_backup: True" in rollback


@pytest.mark.parametrize(
    "backup_map, expected_reason",
    [
        pytest.param("{}", "backup_map_unconfigured", id="taken_after_the_hotfix_armed"),
        pytest.param("", "backup_map_unconfigured", id="predates_the_branch_map"),
        pytest.param('{"9":758285,"10":315607,"11":308697}', "backup_map_not_provider_scoped", id="legacy_integer_map"),
        pytest.param(
            '{"9":{"provider":"altegio","company_id":758285},"9":{"provider":"easyweek","company_id":315607}}',
            "backup_map_invalid",
            id="duplicate_key",
        ),
        pytest.param("{not json", "backup_map_invalid", id="malformed"),
        pytest.param(
            '{"9":{"provider":"altegio","company_id":758285}}',
            "backup_map_identity_mismatch",
            id="incomplete_topology",
        ),
        pytest.param(
            '{"9":{"provider":"easyweek","company_id":758285},'
            '"10":{"provider":"easyweek","company_id":315607},'
            '"11":{"provider":"easyweek","company_id":308697}}',
            "backup_map_identity_mismatch",
            id="foreign_provider",
        ),
    ],
)
def test_an_unusable_backup_map_is_rejected(backup_map: str, expected_reason: str) -> None:
    verdict = verify_backup_map(backup_map, PRODUCTION_BRANCH_MAP)

    assert verdict.ok is False
    assert verdict.reason == expected_reason
    assert backup_map not in str(verdict.as_safe_dict()) or backup_map == ""


def test_the_expected_pre_hotfix_backup_is_accepted() -> None:
    verdict = verify_backup_map(PRODUCTION_BRANCH_MAP, PRODUCTION_BRANCH_MAP)

    assert verdict.ok is True
    assert verdict.reason == "backup_map_proven"
    assert verdict.branch_identities == [
        (9, "altegio", 758285),
        (10, "easyweek", 315607),
        (11, "easyweek", 308697),
    ]
    assert verdict.as_safe_dict()["identities_match"] is True


def test_a_broken_expectation_cannot_wave_a_bad_backup_through() -> None:
    assert verify_backup_map(PRODUCTION_BRANCH_MAP, "{}").reason == "expected_map_unusable"
    assert verify_backup_map("{}", "{}").reason == "expected_map_unusable"


# --- the blocks, actually executed ------------------------------------------


def test_the_documented_rollout_replaces_only_the_three_keys(env_fixture) -> None:
    env_file, handoff = env_fixture

    backup = _run_block("PREHOTFIX_BACKUP", env_file=env_file, handoff_file=handoff)
    upsert = _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )
    after = env_file.read_text().splitlines()

    assert backup.returncode == 0, backup.stderr
    assert upsert.returncode == 0, upsert.stderr
    assert handoff.exists()
    # Untouched lines survive verbatim, in their original order.
    assert [line for line in after if not line.startswith(SINGLE_INBOX_KEYS)] == [
        "APP_NAME=altegio_bot",
        "WHATSAPP_ACCESS_TOKEN=NOT_A_REAL_TOKEN_FIXTURE",
        "CHATWOOT_INBOX_ID=8",
        "# trailing comment",
    ]
    for key in SINGLE_INBOX_KEYS:
        assert len([line for line in after if line.startswith(f"{key}=")]) == 1
    assert "CHATWOOT_INBOX_COMPANY_MAP={}" in after
    assert "CHATWOOT_INBOUND_ROUTING_MODE=general" in after
    assert "CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID=3" in after


def test_the_documented_rollback_restores_the_proven_map(env_fixture) -> None:
    env_file, handoff = env_fixture

    _run_block("PREHOTFIX_BACKUP", env_file=env_file, handoff_file=handoff)
    _run_block("ROLLOUT_ENV", env_file=env_file, handoff_file=handoff, extra_env={"SINGLE_INBOX_SENDER_ID": "3"})
    restore = _run_block("ROLLBACK_ENV", env_file=env_file, handoff_file=handoff)
    after = env_file.read_text().splitlines()

    assert restore.returncode == 0, restore.stderr
    assert f"CHATWOOT_INBOX_COMPANY_MAP={PRODUCTION_BRANCH_MAP}" in after
    assert "CHATWOOT_INBOUND_ROUTING_MODE=affinity" in after
    assert "CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID=0" in after
    assert "WHATSAPP_ACCESS_TOKEN=NOT_A_REAL_TOKEN_FIXTURE" in after


def test_a_second_rollout_run_keeps_the_original_handoff(env_fixture) -> None:
    env_file, handoff = env_fixture

    _run_block("PREHOTFIX_BACKUP", env_file=env_file, handoff_file=handoff)
    first = handoff.read_text()
    _run_block("ROLLOUT_ENV", env_file=env_file, handoff_file=handoff, extra_env={"SINGLE_INBOX_SENDER_ID": "3"})
    second = _run_block("PREHOTFIX_BACKUP", env_file=env_file, handoff_file=handoff)

    assert second.returncode == 0, second.stderr
    assert handoff.read_text() == first, "a re-run must not repoint at a post-hotfix backup"


def test_a_newer_backup_cannot_become_the_rollback_source(env_fixture, tmp_path) -> None:
    env_file, handoff = env_fixture

    _run_block("PREHOTFIX_BACKUP", env_file=env_file, handoff_file=handoff)
    proven = handoff.read_text().split("=", 1)[1].strip()
    _run_block("ROLLOUT_ENV", env_file=env_file, handoff_file=handoff, extra_env={"SINGLE_INBOX_SENDER_ID": "3"})
    # Someone takes another backup — now holding the EMPTY map.
    newer = tmp_path / ".env.bak.29990101T000000Z"
    shutil.copy(env_file, newer)

    resolved = _run_block("ROLLBACK_RESOLVE", env_file=env_file, handoff_file=handoff)
    _run_block("ROLLBACK_ENV", env_file=env_file, handoff_file=handoff)
    after = env_file.read_text()

    assert resolved.returncode == 0, resolved.stderr
    assert resolved.stdout.strip() == f"PRE_HOTFIX_ENV_BACKUP={proven}"
    assert str(newer) not in resolved.stdout
    assert f"CHATWOOT_INBOX_COMPANY_MAP={PRODUCTION_BRANCH_MAP}" in after


def test_a_rollback_from_an_emptied_backup_is_refused(env_fixture) -> None:
    """Belt to the container-side identity gate, on the host, before any write."""
    env_file, handoff = env_fixture
    env_file.write_text("\n".join(FIXTURE_ENV_LINES).replace(PRODUCTION_BRANCH_MAP, "{}"), encoding="utf-8")

    _run_block("PREHOTFIX_BACKUP", env_file=env_file, handoff_file=handoff)
    before = env_file.read_bytes()
    refused = _run_block("ROLLBACK_ENV", env_file=env_file, handoff_file=handoff)

    assert refused.returncode != 0
    assert env_file.read_bytes() == before, "a refused rollback leaves .env byte for byte"


def test_a_read_error_stops_before_env_is_touched(env_fixture) -> None:
    """`grep` exit 2 must abort, not be swallowed as the benign exit 1."""
    env_file, handoff = env_fixture
    before = env_file.read_bytes()
    env_file.chmod(0o000)
    try:
        blocked = _run_block(
            "ROLLOUT_ENV",
            env_file=env_file,
            handoff_file=handoff,
            extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
        )
    finally:
        env_file.chmod(0o600)

    assert blocked.returncode != 0
    assert env_file.read_bytes() == before


def test_a_failure_just_before_the_rename_leaves_env_untouched(env_fixture, tmp_path) -> None:
    """The whole point of the temp-then-rename shape, proved rather than argued."""
    env_file, handoff = env_fixture
    before = env_file.read_bytes()
    sabotaged = _runbook_block("ROLLOUT_ENV").replace(
        'mv -f "$ENV_TMP" "$ENV_FILE"',
        'false\nmv -f "$ENV_TMP" "$ENV_FILE"',
    )

    failed = _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
        script=sabotaged,
    )

    assert failed.returncode != 0
    assert env_file.read_bytes() == before, "the live .env is only ever written by the rename"
    assert list(tmp_path.glob(".env.rollout.*")) == [], "the trap cleans the temp file up"


def test_an_invalid_sender_id_never_reaches_the_env_file(env_fixture) -> None:
    env_file, handoff = env_fixture
    before = env_file.read_bytes()

    for bad in ("0", "-1", "3a", "", " 3"):
        rejected = _run_block(
            "ROLLOUT_ENV",
            env_file=env_file,
            handoff_file=handoff,
            extra_env={"SINGLE_INBOX_SENDER_ID": bad},
        )
        assert rejected.returncode != 0, bad
        assert env_file.read_bytes() == before, bad
