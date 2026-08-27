"""Deployment contract for PR-7 branch-scoped Chatwoot routing."""

from __future__ import annotations

import hashlib
import os
import re
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

from altegio_bot.scripts.verify_pre_hotfix_env_backup import (
    extract_map_value,
    map_fingerprint,
    snapshot_branch_map,
)
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
# PR-7.4 single-inbox hotfix: no rollout without a proven rollback
# ---------------------------------------------------------------------------
#
# Three drafts of §14 were unsafe, each in a way that only shows up on the day
# it is run:
#
#   * bare `KEY=value` lines set shell variables and vanish — `.env` untouched,
#     Compose none the wiser, the operator believing the rollback is armed;
#   * `cat "$ENV_TMP" > .env` truncated the live production file first, and
#     `grep ... || true` swallowed real read errors on the way there;
#   * the handoff was accepted on sight ("keeping the pre-hotfix backup it
#     already names") and only checked at some future rollback — so the rollout
#     could empty `CHATWOOT_INBOX_COMPANY_MAP` while the backup it was supposed
#     to restore was missing, tampered with, or a snapshot of a different
#     topology entirely.
#
# The gate is now cryptographic and it runs inside the block that does the
# writing: handoff fields, backup SHA256, and a fingerprint of the normalised
# branch map captured immediately before the rollout. Nothing here knows what
# the topology *should* be — plan §10 records an EasyWeek location id that
# stopped existing, so a numeric id pinned in this repository would one day
# block a rollback that has to succeed.
#
# These tests do not paraphrase the runbook. They lift its own bash blocks out
# of the markdown and run them against a throwaway `.env`.

SINGLE_INBOX_KEYS = (
    "CHATWOOT_INBOX_COMPANY_MAP",
    "CHATWOOT_INBOUND_ROUTING_MODE",
    "CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID",
)
RECREATE_BOTH_WORKERS = "$COMPOSE up -d --force-recreate \\\n  altegio-outbox-worker altegio-whatsapp-inbox-worker"
HANDOFF_FIELDS = ("PRE_HOTFIX_ENV_BACKUP", "PRE_HOTFIX_BACKUP_SHA256", "PRE_HOTFIX_MAP_FINGERPRINT")

# Deliberately NOT the current production ids. The procedure must work for
# whatever topology is live when it runs, and these tests must keep passing the
# day production changes.
SYNTHETIC_BRANCH_MAP = (
    '{"9":{"provider":"altegio","company_id":700001},'
    '"10":{"provider":"easyweek","company_id":700002},'
    '"11":{"provider":"easyweek","company_id":700003}}'
)
FIXTURE_ENV_LINES = (
    "APP_NAME=altegio_bot",
    "WHATSAPP_ACCESS_TOKEN=NOT_A_REAL_TOKEN_FIXTURE",
    "CHATWOOT_INBOX_ID=8",
    f"CHATWOOT_INBOX_COMPANY_MAP={SYNTHETIC_BRANCH_MAP}",
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
    """Execute a runbook block against a throwaway .env, never the real one.

    The verifier normally runs inside a worker container; here it runs in this
    interpreter, which is the same code the container would execute.
    """
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "ENV_FILE": str(env_file),
        "HANDOFF_FILE": str(handoff_file),
        "VERIFY_BACKUP_CMD": f"{sys.executable} -m altegio_bot.scripts.verify_pre_hotfix_env_backup",
        "PYTHONPATH": str(REPO_ROOT / "src"),
    }
    env.update(extra_env or {})
    return subprocess.run(
        ["bash", "-c", script if script is not None else _runbook_block(label)],
        capture_output=True,
        text=True,
        env=env,
    )


def _handoff_fields(handoff_file) -> dict[str, str]:
    return dict(line.split("=", 1) for line in handoff_file.read_text().splitlines() if "=" in line)


def _sha256_of(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


@pytest.fixture
def env_fixture(tmp_path):
    """A stand-in `.env` in a valid pre-hotfix state, plus a handoff path."""
    env_file = tmp_path / ".env"
    env_file.write_text("\n".join(FIXTURE_ENV_LINES), encoding="utf-8")
    return env_file, tmp_path / "hotfix.handoff"


def _arm(env_fixture, sender_id: str = "3"):
    """Walk the documented rollout: prove, snapshot, then write."""
    env_file, handoff = env_fixture
    proved = _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    written = _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": sender_id},
    )
    return proved, written


# --- static contract --------------------------------------------------------


def test_single_inbox_rollout_edits_the_real_env_file() -> None:
    section = _single_inbox_section()

    assert "cd /opt/altegio_bot" in section
    assert 'cp -p "$ENV_FILE" "$BACKUP_FILE"' in section, "the backup must preserve mode and owner"

    for key in SINGLE_INBOX_KEYS:
        # A bare assignment line is a shell variable that Compose never sees.
        assert not re.search(rf"^{key}=", section, flags=re.M), f"{key} is set as a shell variable"


def test_the_env_edit_is_never_an_in_place_truncation() -> None:
    section = _single_inbox_section()

    assert 'cat "$ENV_TMP" > .env' not in section
    assert 'cat "$ENV_TMP" > "$ENV_FILE"' not in section
    assert not re.search(r"^\s*(cat|printf|echo)[^\n]*>\s*\"?\$?ENV_FILE", section, flags=re.M)
    assert not re.search(r">\s*\"\$ENV_FILE\"", section), "nothing may redirect into the live .env"


def test_the_env_edit_uses_a_sibling_temp_file_and_an_atomic_rename() -> None:
    section = _single_inbox_section()

    assert section.count('ENV_TMP="$(mktemp "${ENV_FILE}.rollout.XXXXXX")"') == 1
    assert section.count('ENV_TMP="$(mktemp "${ENV_FILE}.rollback.XXXXXX")"') == 1
    assert not re.search(r"mktemp\s*\)", section), "mktemp must always be given a directory"
    assert section.count('mv -f "$ENV_TMP" "$ENV_FILE"') == 2, "rollout and rollback both rename atomically"
    assert section.count("""trap 'rm -f "$ENV_TMP"' EXIT""") == 2
    assert section.count("set -euo pipefail") >= 4, "every mutating block fails fast"


def test_a_read_error_is_never_masked_by_true() -> None:
    section = _single_inbox_section()

    assert "|| true" not in section, "|| true hides a real read error as well as the benign exit 1"
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
    assert "grep -Eq '^[1-9][0-9]*$'" in _single_inbox_section()


def test_single_inbox_rollout_never_prints_env_contents_or_secrets() -> None:
    section = _single_inbox_section()

    for leak in ("cat .env", 'cat "$ENV_FILE"', "grep CHATWOOT_API_TOKEN", "printenv"):
        assert leak not in section, leak
    assert "chatwoot_api_token" not in section
    assert "> /dev/null" in section, "the diff must not print .env content on mismatch"


def test_single_inbox_hotfix_does_not_touch_easyweek_env() -> None:
    section = _single_inbox_section()

    assert "easyweek.env" in section
    assert "не трогает и править его не нужно" in section


def test_single_inbox_rollout_and_rollback_recreate_both_chatwoot_consumers() -> None:
    section = _single_inbox_section()

    assert section.count(RECREATE_BOTH_WORKERS) == 2
    assert "restart" not in section.replace("`restart`", "")
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


# --- General inbox validity is proved, not inferred from an empty map -------


def test_the_preflight_proves_a_positive_general_inbox_id() -> None:
    section = _single_inbox_section()

    assert section.count('"general_inbox_id_valid": positive_int(settings.chatwoot_inbox_id) is not None') == 2
    assert "validated_general_id is not None or not parsed.configured" not in section
    assert (
        section.count('"general_inbox_isolated": (validated_general_id is not None) if parsed.configured else None')
        == 2
    )


def test_both_gates_require_a_valid_general_inbox_id() -> None:
    section = _single_inbox_section()

    assert section.count("`general_inbox_id_valid: True`") == 2
    assert "general_inbox_id_valid: False" in section, "and it is a STOP condition"


@pytest.mark.parametrize(
    "inbox_id, expected",
    [
        pytest.param(0, False, id="unset"),
        pytest.param(-1, False, id="negative"),
        pytest.param("8", False, id="string"),
        pytest.param(True, False, id="bool"),
        pytest.param(8, True, id="positive"),
    ],
)
def test_general_inbox_validity_never_comes_from_an_empty_map(inbox_id: object, expected: bool) -> None:
    parsed = parse_chatwoot_inbox_company_map("{}")

    assert parsed.configured is False
    # The central resolver is deliberately unchanged: unconfigured map means
    # "legacy single-inbox mode", which is NOT proof of a usable General id.
    assert resolve_chatwoot_general_inbox(parsed, inbox_id) == (None, None)
    assert (positive_int(inbox_id) is not None) is expected


def test_the_central_general_inbox_resolver_is_untouched() -> None:
    configured = parse_chatwoot_inbox_company_map(SYNTHETIC_BRANCH_MAP)

    assert resolve_chatwoot_general_inbox(configured, 8) == (8, None)
    assert resolve_chatwoot_general_inbox(configured, 9) == (None, "general_inbox_overlaps_branch")
    assert resolve_chatwoot_general_inbox(configured, 0) == (None, "invalid_general_inbox_id")


# --- no topology is pinned in the repository --------------------------------


def test_no_expected_branch_map_is_hardcoded_anywhere() -> None:
    """Plan §10: numeric location ids are not eternal constants."""
    section = _single_inbox_section()
    verifier = (REPO_ROOT / "src/altegio_bot/scripts/verify_pre_hotfix_env_backup.py").read_text()

    assert "EXPECTED_BRANCH_MAP" not in section
    assert "EXPECTED_BRANCH_MAP" not in verifier
    assert "identities_match" not in section
    # No production branch identity may act as a permanent gate.
    for production_id in ("315607", "308697", "758285"):
        assert production_id not in section, production_id
        assert production_id not in verifier, production_id


def test_the_fingerprint_is_the_only_source_of_expected_identity() -> None:
    section = _single_inbox_section()

    # Four blocks carry the gate, and each proves the backup and, while the
    # live .env is still pre-hotfix, the live map against the same digest.
    assert section.count("$VERIFY_BACKUP_CMD verify --expect-fingerprint") == 8
    assert section.count("$VERIFY_BACKUP_CMD snapshot") == 1, "the snapshot is taken once, before arming"


# --- the verifier itself ----------------------------------------------------


def test_the_fingerprint_ignores_formatting_but_not_identity() -> None:
    spaced = (
        '{ "11" : {"provider":"easyweek","company_id":700003} ,'
        ' "9": {"provider":"altegio","company_id":700001},'
        ' "10":{"provider":"easyweek","company_id":700002} }'
    )
    baseline = snapshot_branch_map(f"CHATWOOT_INBOX_COMPANY_MAP={SYNTHETIC_BRANCH_MAP}")
    reordered = snapshot_branch_map(f"CHATWOOT_INBOX_COMPANY_MAP={spaced}")
    changed = snapshot_branch_map("CHATWOOT_INBOX_COMPANY_MAP=" + SYNTHETIC_BRANCH_MAP.replace("700003", "700009"))

    assert baseline.ok and reordered.ok and changed.ok
    assert baseline.fingerprint == reordered.fingerprint, "key order and spacing are not identity"
    assert baseline.fingerprint != changed.fingerprint, "a different company id is"
    assert baseline.fingerprint == map_fingerprint(
        [(9, "altegio", 700001), (10, "easyweek", 700002), (11, "easyweek", 700003)]
    )


@pytest.mark.parametrize(
    "map_value, expected_reason",
    [
        pytest.param("{}", "map_unconfigured", id="emptied_by_the_hotfix"),
        pytest.param("", "map_unconfigured", id="predates_the_branch_map"),
        pytest.param('{"9":700001,"10":700002}', "map_not_provider_scoped", id="legacy_integer_map"),
        pytest.param(
            '{"9":{"provider":"altegio","company_id":700001},"9":{"provider":"easyweek","company_id":700002}}',
            "map_invalid",
            id="duplicate_json_key",
        ),
        pytest.param("{not json", "map_invalid", id="malformed"),
    ],
)
def test_an_unusable_map_is_refused(map_value: str, expected_reason: str) -> None:
    verdict = snapshot_branch_map(f"CHATWOOT_INBOX_COMPANY_MAP={map_value}")

    assert verdict.ok is False
    assert verdict.reason == expected_reason
    assert verdict.fingerprint == ""


def test_a_missing_or_duplicated_map_line_is_refused() -> None:
    assert snapshot_branch_map("APP_NAME=x").reason == "map_line_missing"
    doubled = f"CHATWOOT_INBOX_COMPANY_MAP={SYNTHETIC_BRANCH_MAP}\nCHATWOOT_INBOX_COMPANY_MAP={{}}"
    assert snapshot_branch_map(doubled).reason == "map_line_not_unique"
    assert extract_map_value(doubled) == (None, "map_line_not_unique")


def test_a_fingerprint_mismatch_is_refused_even_for_a_valid_map() -> None:
    other = snapshot_branch_map("CHATWOOT_INBOX_COMPANY_MAP=" + SYNTHETIC_BRANCH_MAP.replace("700003", "700009"))
    verdict = snapshot_branch_map(
        f"CHATWOOT_INBOX_COMPANY_MAP={SYNTHETIC_BRANCH_MAP}",
        expected_fingerprint=other.fingerprint,
    )

    assert verdict.ok is False
    assert verdict.reason == "map_fingerprint_mismatch"


def test_the_verifier_output_never_carries_the_raw_map() -> None:
    verdict = snapshot_branch_map(f"CHATWOOT_INBOX_COMPANY_MAP={SYNTHETIC_BRANCH_MAP}")
    printed = str(verdict.as_safe_dict())

    assert verdict.ok is True
    # Identities are normalised tuples; the configuration string itself never
    # leaves, in whole or in part.
    assert SYNTHETIC_BRANCH_MAP not in printed
    assert '"provider"' not in printed and "company_id" not in printed
    assert "NOT_A_REAL_TOKEN_FIXTURE" not in printed


# --- the blocks, actually executed ------------------------------------------


def test_a_valid_pre_hotfix_env_arms_and_records_a_bound_snapshot(env_fixture) -> None:
    env_file, handoff = env_fixture

    proved, written = _arm(env_fixture)
    fields = _handoff_fields(handoff)
    after = env_file.read_text().splitlines()

    assert proved.returncode == 0, proved.stderr
    assert written.returncode == 0, written.stderr
    assert set(fields) == set(HANDOFF_FIELDS)
    assert oct(handoff.stat().st_mode)[-3:] == "600", "the handoff is root-only"
    assert fields["PRE_HOTFIX_BACKUP_SHA256"] == _sha256_of(fields["PRE_HOTFIX_ENV_BACKUP"])
    assert fields["PRE_HOTFIX_MAP_FINGERPRINT"] == map_fingerprint(
        [(9, "altegio", 700001), (10, "easyweek", 700002), (11, "easyweek", 700003)]
    )
    # The handoff carries a digest, never the configuration itself.
    assert SYNTHETIC_BRANCH_MAP not in handoff.read_text()
    assert "NOT_A_REAL_TOKEN_FIXTURE" not in handoff.read_text()

    assert [line for line in after if not line.startswith(SINGLE_INBOX_KEYS)] == [
        "APP_NAME=altegio_bot",
        "WHATSAPP_ACCESS_TOKEN=NOT_A_REAL_TOKEN_FIXTURE",
        "CHATWOOT_INBOX_ID=8",
        "# trailing comment",
    ]
    assert "CHATWOOT_INBOX_COMPANY_MAP={}" in after
    assert "CHATWOOT_INBOUND_ROUTING_MODE=general" in after
    assert "CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID=3" in after


def test_the_rollback_restores_the_exact_captured_map(env_fixture) -> None:
    env_file, handoff = env_fixture

    _arm(env_fixture)
    restored = _run_block("ROLLBACK_ENV", env_file=env_file, handoff_file=handoff)
    after = env_file.read_text().splitlines()

    assert restored.returncode == 0, restored.stderr
    assert f"CHATWOOT_INBOX_COMPANY_MAP={SYNTHETIC_BRANCH_MAP}" in after
    assert "CHATWOOT_INBOUND_ROUTING_MODE=affinity" in after
    assert "CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID=0" in after
    assert "WHATSAPP_ACCESS_TOKEN=NOT_A_REAL_TOKEN_FIXTURE" in after


@pytest.mark.parametrize(
    "live_map",
    [
        pytest.param("{}", id="already_empty"),
        pytest.param('{"9":700001}', id="legacy_integer_map"),
        pytest.param("{not json", id="malformed"),
        pytest.param("", id="unset"),
    ],
)
def test_an_unusable_live_map_can_never_arm_the_hotfix(tmp_path, live_map: str) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("\n".join(FIXTURE_ENV_LINES).replace(SYNTHETIC_BRANCH_MAP, live_map), encoding="utf-8")
    handoff = tmp_path / "hotfix.handoff"
    before = env_file.read_bytes()

    proved = _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    written = _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )

    assert proved.returncode != 0
    assert written.returncode != 0, "no handoff, so the write gate cannot pass either"
    assert not handoff.exists(), "an unprovable pre-hotfix state must not produce a handoff"
    assert env_file.read_bytes() == before


def test_a_duplicated_map_line_can_never_arm_the_hotfix(tmp_path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text(
        "\n".join(FIXTURE_ENV_LINES) + f"CHATWOOT_INBOX_COMPANY_MAP={SYNTHETIC_BRANCH_MAP}\n",
        encoding="utf-8",
    )
    handoff = tmp_path / "hotfix.handoff"
    before = env_file.read_bytes()

    proved = _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)

    assert proved.returncode != 0, "last-wins on a duplicated key is not a state anyone proved"
    assert not handoff.exists()
    assert env_file.read_bytes() == before


def test_a_missing_backup_stops_the_rollout(env_fixture) -> None:
    env_file, handoff = env_fixture

    _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    os.remove(_handoff_fields(handoff)["PRE_HOTFIX_ENV_BACKUP"])
    before = env_file.read_bytes()

    reproved = _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    written = _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )

    assert reproved.returncode != 0, "an existing handoff is verified, never taken on trust"
    assert written.returncode != 0
    assert env_file.read_bytes() == before


def test_a_tampered_backup_stops_the_rollout(env_fixture) -> None:
    env_file, handoff = env_fixture

    _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    backup = Path(_handoff_fields(handoff)["PRE_HOTFIX_ENV_BACKUP"])
    backup.write_text(backup.read_text() + "SNEAKY=1\n", encoding="utf-8")
    before = env_file.read_bytes()

    written = _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )
    rolled_back = _run_block("ROLLBACK_ENV", env_file=env_file, handoff_file=handoff)

    assert written.returncode != 0, "SHA256 binds the handoff to that exact file"
    assert rolled_back.returncode != 0
    assert env_file.read_bytes() == before


@pytest.mark.parametrize("target", ["handoff", "backup"])
def test_a_symlinked_handoff_or_backup_is_refused(env_fixture, target: str) -> None:
    env_file, handoff = env_fixture

    _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    if target == "handoff":
        real = handoff.with_suffix(".real")
        shutil.move(handoff, real)
        handoff.symlink_to(real)
    else:
        backup = Path(_handoff_fields(handoff)["PRE_HOTFIX_ENV_BACKUP"])
        real = backup.with_suffix(".real")
        shutil.move(backup, real)
        backup.symlink_to(real)
    before = env_file.read_bytes()

    written = _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )

    assert written.returncode != 0
    assert env_file.read_bytes() == before


def test_a_handoff_whose_snapshot_no_longer_matches_the_live_env_is_stale(env_fixture) -> None:
    """The map legitimately changed after the handoff was taken — stop."""
    env_file, handoff = env_fixture

    _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    env_file.write_text(
        "\n".join(FIXTURE_ENV_LINES).replace("700003", "700009"),
        encoding="utf-8",
    )
    before = env_file.read_bytes()

    reproved = _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    written = _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )

    assert reproved.returncode != 0, "restoring the old snapshot would undo a real change"
    assert written.returncode != 0
    assert env_file.read_bytes() == before


def test_a_missing_handoff_field_is_refused(env_fixture) -> None:
    env_file, handoff = env_fixture

    _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    kept = [line for line in handoff.read_text().splitlines() if not line.startswith("PRE_HOTFIX_BACKUP_SHA256=")]
    handoff.write_text("\n".join(kept) + "\n", encoding="utf-8")
    before = env_file.read_bytes()

    written = _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )

    assert written.returncode != 0
    assert env_file.read_bytes() == before


def test_a_duplicated_handoff_field_is_refused(env_fixture) -> None:
    env_file, handoff = env_fixture

    _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    fields = _handoff_fields(handoff)
    handoff.write_text(
        handoff.read_text() + f"PRE_HOTFIX_ENV_BACKUP={fields['PRE_HOTFIX_ENV_BACKUP']}\n",
        encoding="utf-8",
    )
    before = env_file.read_bytes()

    written = _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )

    assert written.returncode != 0
    assert env_file.read_bytes() == before


def test_a_backup_path_outside_the_allowed_pattern_is_refused(env_fixture, tmp_path) -> None:
    env_file, handoff = env_fixture

    _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    fields = _handoff_fields(handoff)
    elsewhere = tmp_path / "somewhere-else.env"
    shutil.copy(fields["PRE_HOTFIX_ENV_BACKUP"], elsewhere)
    handoff.write_text(
        handoff.read_text().replace(fields["PRE_HOTFIX_ENV_BACKUP"], str(elsewhere)),
        encoding="utf-8",
    )
    before = env_file.read_bytes()

    written = _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )

    assert written.returncode != 0
    assert env_file.read_bytes() == before


def test_a_second_rollout_run_after_arming_is_idempotent(env_fixture) -> None:
    """Already armed, empty map: accept the original handoff, take no new backup."""
    env_file, handoff = env_fixture

    _arm(env_fixture)
    first = handoff.read_text()
    reproved = _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)

    assert reproved.returncode == 0, reproved.stderr
    assert handoff.read_text() == first, "a re-run must not repoint at a post-hotfix backup"
    assert len(list(env_file.parent.glob(".env.bak.*"))) == 1, "and must not take a second backup"


def test_a_second_rollout_run_still_refuses_a_broken_handoff(env_fixture) -> None:
    env_file, handoff = env_fixture

    _arm(env_fixture)
    os.remove(_handoff_fields(handoff)["PRE_HOTFIX_ENV_BACKUP"])
    before = env_file.read_bytes()

    reproved = _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)

    assert reproved.returncode != 0, "idempotent only for a handoff that is still fully valid"
    assert env_file.read_bytes() == before


def test_a_newer_stray_backup_cannot_become_the_rollback_source(env_fixture) -> None:
    env_file, handoff = env_fixture

    _arm(env_fixture)
    proven = _handoff_fields(handoff)["PRE_HOTFIX_ENV_BACKUP"]
    stray = env_file.parent / ".env.bak.29990101T000000Z"
    shutil.copy(env_file, stray)  # holds the EMPTY map

    resolved = _run_block("ROLLBACK_RESOLVE", env_file=env_file, handoff_file=handoff)
    restored = _run_block("ROLLBACK_ENV", env_file=env_file, handoff_file=handoff)

    assert resolved.returncode == 0, resolved.stderr
    assert resolved.stdout.strip() == f"PRE_HOTFIX_ENV_BACKUP={proven}"
    assert str(stray) not in resolved.stdout
    assert restored.returncode == 0, restored.stderr
    assert f"CHATWOOT_INBOX_COMPANY_MAP={SYNTHETIC_BRANCH_MAP}" in env_file.read_text()


def test_a_read_error_stops_before_env_is_touched(env_fixture) -> None:
    env_file, handoff = env_fixture

    _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
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
    env_file, handoff = env_fixture

    _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
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

    _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
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


def test_no_block_output_ever_carries_the_map_or_a_secret(env_fixture) -> None:
    env_file, handoff = env_fixture

    proved, written = _arm(env_fixture)
    resolved = _run_block("ROLLBACK_RESOLVE", env_file=env_file, handoff_file=handoff)
    restored = _run_block("ROLLBACK_ENV", env_file=env_file, handoff_file=handoff)

    for result in (proved, written, resolved, restored):
        assert result.returncode == 0, result.stderr
        printed = result.stdout + result.stderr
        assert SYNTHETIC_BRANCH_MAP not in printed
        assert "NOT_A_REAL_TOKEN_FIXTURE" not in printed
        assert "700001" not in printed


# ---------------------------------------------------------------------------
# The supported pre-hotfix baseline is a precondition, not a hope
# ---------------------------------------------------------------------------
#
# §14.7 does not restore an arbitrary Chatwoot configuration. It restores ONE
# confirmed production baseline: the captured provider-scoped branch map,
# `affinity`, and the sender switched off. The gate used to prove only the map,
# so a `.env` that had drifted to `context` could still arm the hotfix — and the
# rollback would then "restore" a mode that was never live.
#
# `context` and `general` are perfectly valid application modes. They are simply
# not a supported starting point for this one-off rollout, so the procedure
# stops instead of quietly widening what rollback means.
#
# A positive sender alongside a non-empty map is not a second legal baseline
# either: the runtime calls that combination `single_inbox_config_invalid`, so
# it is a configuration to fix before the rollout, never one to capture.

BASELINE_ENV_LINES = (
    "APP_NAME=altegio_bot",
    "WHATSAPP_ACCESS_TOKEN=NOT_A_REAL_TOKEN_FIXTURE",
    "CHATWOOT_INBOX_ID=8",
    f"CHATWOOT_INBOX_COMPANY_MAP={SYNTHETIC_BRANCH_MAP}",
    "CHATWOOT_INBOUND_ROUTING_MODE=affinity",
)


def _env_text(*, mode: str = "affinity", sender: str | None = None, extra: tuple[str, ...] = ()) -> str:
    lines = [line for line in BASELINE_ENV_LINES if not line.startswith("CHATWOOT_INBOUND_ROUTING_MODE=")]
    lines.append(f"CHATWOOT_INBOUND_ROUTING_MODE={mode}")
    if sender is not None:
        lines.append(f"CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID={sender}")
    lines.extend(extra)
    lines.append("# trailing comment")
    return "\n".join(lines) + "\n"


def _env_at(tmp_path, text: str):
    env_file = tmp_path / ".env"
    env_file.write_text(text, encoding="utf-8")
    return env_file, tmp_path / "hotfix.handoff"


def _rollout_is_refused(env_file, handoff) -> None:
    """Neither half of the rollout may proceed, and `.env` must not move."""
    before = env_file.read_bytes()

    proved = _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    written = _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )

    assert proved.returncode != 0, proved.stdout
    assert written.returncode != 0, written.stdout
    assert env_file.read_bytes() == before, "a refused rollout leaves .env byte for byte"


@pytest.mark.parametrize(
    "sender",
    [
        pytest.param(None, id="sender_line_absent"),
        pytest.param("0", id="sender_explicitly_off"),
    ],
)
def test_the_supported_baseline_arms_and_rolls_back(tmp_path, sender: str | None) -> None:
    """Absent and `0` are the same runtime state: the feature is off."""
    env_file, handoff = _env_at(tmp_path, _env_text(sender=sender))

    proved = _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    armed = _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )
    after_rollout = env_file.read_text().splitlines()
    restored = _run_block("ROLLBACK_ENV", env_file=env_file, handoff_file=handoff)
    after_rollback = env_file.read_text().splitlines()

    assert proved.returncode == 0, proved.stderr
    assert armed.returncode == 0, armed.stderr
    assert restored.returncode == 0, restored.stderr
    assert "CHATWOOT_INBOX_COMPANY_MAP={}" in after_rollout
    assert "CHATWOOT_INBOUND_ROUTING_MODE=general" in after_rollout
    assert "CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID=3" in after_rollout
    # The rollback writes `0` rather than reproducing a missing line: the two
    # are runtime-equivalent, and byte-exact absence buys nothing.
    assert f"CHATWOOT_INBOX_COMPANY_MAP={SYNTHETIC_BRANCH_MAP}" in after_rollback
    assert "CHATWOOT_INBOUND_ROUTING_MODE=affinity" in after_rollback
    assert "CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID=0" in after_rollback
    assert "WHATSAPP_ACCESS_TOKEN=NOT_A_REAL_TOKEN_FIXTURE" in after_rollback


@pytest.mark.parametrize("mode", ["context", "general"])
def test_an_unsupported_starting_mode_stops_the_rollout(tmp_path, mode: str) -> None:
    """Valid modes, but not the baseline §14.7 knows how to give back."""
    env_file, handoff = _env_at(tmp_path, _env_text(mode=mode))

    _rollout_is_refused(env_file, handoff)
    assert not handoff.exists(), "an unprovable baseline must not be captured"


@pytest.mark.parametrize(
    "sender",
    [
        pytest.param("3", id="positive"),
        pytest.param("-1", id="negative"),
        pytest.param("abc", id="malformed"),
        pytest.param("00", id="non_canonical_zero"),
        pytest.param("", id="empty_value"),
    ],
)
def test_a_sender_that_is_not_off_stops_the_rollout(tmp_path, sender: str) -> None:
    """A positive sender with a non-empty map is `single_inbox_config_invalid`."""
    env_file, handoff = _env_at(tmp_path, _env_text(sender=sender))

    _rollout_is_refused(env_file, handoff)
    assert not handoff.exists()


@pytest.mark.parametrize(
    "extra",
    [
        pytest.param(("CHATWOOT_INBOUND_ROUTING_MODE=affinity",), id="duplicate_mode"),
        pytest.param(("CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID=0",), id="duplicate_sender"),
    ],
)
def test_a_duplicated_baseline_key_stops_the_rollout(tmp_path, extra: tuple[str, ...]) -> None:
    """Last-wins on a duplicated key is not a state anybody proved."""
    sender = "0" if extra[0].startswith("CHATWOOT_SINGLE_INBOX") else None
    env_file, handoff = _env_at(tmp_path, _env_text(sender=sender, extra=extra))

    _rollout_is_refused(env_file, handoff)
    assert not handoff.exists()


def test_a_missing_routing_mode_stops_the_rollout(tmp_path) -> None:
    env_file, handoff = _env_at(
        tmp_path,
        f"APP_NAME=altegio_bot\nCHATWOOT_INBOX_COMPANY_MAP={SYNTHETIC_BRANCH_MAP}\n",
    )

    _rollout_is_refused(env_file, handoff)
    assert not handoff.exists()


def test_a_backup_that_is_not_the_baseline_is_refused_even_when_it_hashes(tmp_path) -> None:
    """SHA256 and map fingerprint can both be right and the backup still wrong."""
    env_file, handoff = _env_at(tmp_path, _env_text())

    _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    fields = _handoff_fields(handoff)
    backup = Path(fields["PRE_HOTFIX_ENV_BACKUP"])
    backup.write_text(_env_text(mode="context"), encoding="utf-8")
    # Re-bind the digest so ONLY the baseline check can refuse this backup.
    handoff.write_text(
        handoff.read_text().replace(fields["PRE_HOTFIX_BACKUP_SHA256"], _sha256_of(backup)),
        encoding="utf-8",
    )
    before = env_file.read_bytes()

    reproved = _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    written = _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )
    resolved = _run_block("ROLLBACK_RESOLVE", env_file=env_file, handoff_file=handoff)
    restored = _run_block("ROLLBACK_ENV", env_file=env_file, handoff_file=handoff)

    assert _sha256_of(backup) == _handoff_fields(handoff)["PRE_HOTFIX_BACKUP_SHA256"]
    for result in (reproved, written, resolved, restored):
        assert result.returncode != 0, result.stdout
    assert env_file.read_bytes() == before


def test_the_armed_state_itself_is_checked_on_a_repeat_rollout(tmp_path) -> None:
    """Empty map is not enough: mode and sender must match what §14.4 wrote."""
    env_file, handoff = _env_at(tmp_path, _env_text())
    _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )
    armed = env_file.read_text()

    for broken in (
        armed.replace("CHATWOOT_INBOUND_ROUTING_MODE=general", "CHATWOOT_INBOUND_ROUTING_MODE=affinity"),
        armed.replace("CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID=3", "CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID=0"),
    ):
        env_file.write_text(broken, encoding="utf-8")
        before = env_file.read_bytes()

        reproved = _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)

        assert reproved.returncode != 0, reproved.stdout
        assert env_file.read_bytes() == before


def test_a_repeat_rollout_after_arming_stays_idempotent(tmp_path) -> None:
    env_file, handoff = _env_at(tmp_path, _env_text())
    _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    first = handoff.read_text()
    _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )
    armed = env_file.read_bytes()

    reproved = _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    rewritten = _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )

    assert reproved.returncode == 0, reproved.stderr
    assert rewritten.returncode == 0, rewritten.stderr
    assert handoff.read_text() == first, "no repointing"
    assert env_file.read_bytes() == armed, "and no drift"
    assert len(list(env_file.parent.glob(".env.bak.*"))) == 1, "no second backup"


def test_the_rollback_stays_idempotent_after_it_lands(tmp_path) -> None:
    """Once restored, the live file IS the baseline again — so it re-verifies."""
    env_file, handoff = _env_at(tmp_path, _env_text())
    _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    _run_block(
        "ROLLOUT_ENV",
        env_file=env_file,
        handoff_file=handoff,
        extra_env={"SINGLE_INBOX_SENDER_ID": "3"},
    )
    _run_block("ROLLBACK_ENV", env_file=env_file, handoff_file=handoff)
    restored = env_file.read_bytes()

    again = _run_block("ROLLBACK_ENV", env_file=env_file, handoff_file=handoff)

    assert again.returncode == 0, again.stderr
    assert env_file.read_bytes() == restored


def test_the_baseline_gate_runs_in_every_block_before_any_write() -> None:
    section = _single_inbox_section()

    # Four blocks share the prelude, so each defines both assertions...
    assert section.count("assert_pre_hotfix_baseline() {") == 4
    assert section.count("assert_armed_state() {") == 4
    # ...proves the backup once per gate, plus the one-off live check in §14.4.
    assert section.count('assert_pre_hotfix_baseline "$PRE_HOTFIX_ENV_BACKUP"') == 4
    assert section.count('assert_pre_hotfix_baseline "$ENV_FILE"') == 5
    assert section.count('assert_armed_state "$ENV_FILE"') == 4
    # The rollout may only capture a baseline it has already proven.
    handoff_block = _runbook_block("PREHOTFIX_HANDOFF")
    assert handoff_block.index('assert_pre_hotfix_baseline "$ENV_FILE"') < handoff_block.index("cp -p")


def test_the_handoff_stores_no_copy_of_mode_or_sender(tmp_path) -> None:
    """SHA256 already binds the backup; a second copy would only drift."""
    env_file, handoff = _env_at(tmp_path, _env_text(sender="0"))

    _run_block("PREHOTFIX_HANDOFF", env_file=env_file, handoff_file=handoff)
    stored = handoff.read_text()

    assert set(_handoff_fields(handoff)) == set(HANDOFF_FIELDS)
    assert "CHATWOOT_INBOUND_ROUTING_MODE" not in stored
    assert "CHATWOOT_SINGLE_INBOX_OPERATOR_SENDER_ID" not in stored
    assert "affinity" not in stored


def test_the_rollback_stop_conditions_name_the_real_failures() -> None:
    section = _single_inbox_section()

    # The old verifier printed these; the current one does not.
    assert "backup_ok" not in section
    assert "post_hotfix_backup" not in section
    assert "map_fingerprint_mismatch" in section
    assert "несовпадение SHA256" in section
    assert "baseline `affinity` + sender off" in section
