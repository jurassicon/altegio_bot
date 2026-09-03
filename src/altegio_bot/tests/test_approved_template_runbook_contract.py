"""The approved-template runbook has to be executable, not merely plausible.

Four defects made it unrunnable, and each is pinned here:

* the Rastatt cloning step used a `--only` flag the cloner does not have, so the
  commands are now checked against the REAL parser;
* the rollback told the operator to deploy the previous code first — which takes
  away both of the commands the rollback needs;
* re-opening review skipped the mandatory PR-9 preflight;
* the prose claimed the runtime had rejected all seven templates for disagreeing
  with Meta, which is not what the runtime compares.
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from altegio_bot.scripts import clone_meta_templates_for_location as cloner

REPO_ROOT = Path(__file__).resolve().parents[3]
RUNBOOK = REPO_ROOT / "docs" / "easyweek" / "approved_template_contract_runbook.md"

RECONCILE_MODULE = "altegio_bot.scripts.reconcile_easyweek_templates"
RESTORE_MODULE = "altegio_bot.scripts.restore_easyweek_templates"
REVIEW_PREFLIGHT_MODULE = "altegio_bot.scripts.easyweek_review_preflight"
CLONER_MODULE = "altegio_bot.scripts.clone_meta_templates_for_location"


def prose(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def bash_blocks(text: str) -> list[str]:
    return [block.strip() for block in re.findall(r"```bash\n(.*?)```", text, flags=re.S)]


def command_args(block: str, module: str) -> list[str]:
    """The argv a `-m <module>` invocation would pass, as the operator typed it."""
    line = next(line for line in block.splitlines() if module in line)
    tokens = re.findall(r'"[^"]*"|\S+', line)
    tokens = [token.strip('"') for token in tokens]
    return tokens[tokens.index("-m") + 2 :]


@pytest.fixture(scope="module")
def runbook() -> str:
    return RUNBOOK.read_text()


def section(runbook: str, heading: str) -> str:
    start = runbook.index(heading)
    following = re.search(r"^## ", runbook[start + len(heading) :], flags=re.M)
    end = start + len(heading) + following.start() if following else len(runbook)
    return runbook[start:end]


# ---------------------------------------------------------------------------
# 1. The Rastatt cloning commands
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def clone_section(runbook: str) -> str:
    return section(runbook, "## 4. External prerequisite")


def test_the_removed_only_flag_is_gone(clone_section: str) -> None:
    """`--only` never existed; the commands silently failed on it.

    Checked on the COMMANDS, not the prose: the section deliberately names the
    flag in order to say the cloner does not have it.
    """
    for block in bash_blocks(clone_section):
        assert "--only" not in block, block
    assert "there is no `--only`" in prose(clone_section), "and it says so"


def test_every_clone_command_parses_with_the_real_parser(clone_section: str) -> None:
    """Checked against the cloner's own parser, not against a copy of it."""
    blocks = [block for block in bash_blocks(clone_section) if CLONER_MODULE in block]
    assert len(blocks) == 3, "dry-run, apply, and the read-only confirmation"

    parser = cloner.build_parser()
    for block in blocks:
        args = parser.parse_args(command_args(block, CLONER_MODULE))
        assert args.source_location == "ka"
        assert args.target_location == "ra"
        assert args.language == "de"
        assert args.include_neutral is True
        assert args.template_name == ["kitilash_ka_repeat_10d_v1", "kitilash_ka_comeback_3d_v1"]
        assert args.yes is False, "the confirmation must not be skipped"


def test_only_the_two_missing_templates_are_selected(clone_section: str) -> None:
    """Rastatt's review_3d is already approved and must not be touched."""
    assert "kitilash_ka_review_3d_v1" not in clone_section


def test_the_apply_command_resolves_rastatts_own_targets(clone_section: str) -> None:
    """`--apply` refuses the dry-run defaults, and Durlach's must not slip in."""
    parser = cloner.build_parser()
    applies = [block for block in bash_blocks(clone_section) if CLONER_MODULE in block and "--apply" in block]
    assert len(applies) == 1

    args = parser.parse_args(command_args(applies[0], CLONER_MODULE))
    target, address, maps_url = cloner.resolve_targets(args)

    assert target == "ra"
    assert address == "76437 Rastatt, Rathausstraße 5"
    assert maps_url == "https://maps.app.goo.gl/xvYYbJbPaWcnp9Xv5"
    assert address != cloner.DEFAULT_TARGET_ADDRESS
    assert maps_url != cloner.DEFAULT_TARGET_MAPS_URL


def test_the_dry_run_commands_do_not_apply(clone_section: str) -> None:
    parser = cloner.build_parser()
    blocks = [block for block in bash_blocks(clone_section) if CLONER_MODULE in block]
    assert sum(parser.parse_args(command_args(b, CLONER_MODULE)).apply for b in blocks) == 1


def test_the_documented_confirmation_phrase_is_the_real_one(clone_section: str) -> None:
    """A wrong phrase would leave the operator unable to confirm."""
    assert cloner._confirmation_word("ra", 2) in clone_section


def test_pending_is_not_treated_as_readiness(clone_section: str) -> None:
    text = prose(clone_section)
    assert "PENDING" in text
    assert "не readiness" in text or "is not readiness" in text


# ---------------------------------------------------------------------------
# 2. The rollback
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def rollback(runbook: str) -> str:
    return section(runbook, "## 9. Rollback")


def test_the_rows_are_restored_before_the_code_is_reverted(rollback: str) -> None:
    """The previous version has neither command, so the order is load-bearing."""
    restore_at = rollback.index(RESTORE_MODULE)
    revert_at = rollback.index("previous code version")

    assert restore_at < revert_at
    text = prose(rollback)
    assert "exist only in *this* version" in text or "exist only in this version" in text


def test_the_rollback_no_longer_uses_the_ordinary_apply(rollback: str) -> None:
    """An apply writes the CURRENT contract; it restores nothing."""
    assert RECONCILE_MODULE not in rollback
    assert prose(rollback).count("writes the current contract") >= 1


def test_the_rollback_runs_a_dry_run_before_applying(rollback: str) -> None:
    blocks = [block for block in bash_blocks(rollback) if RESTORE_MODULE in block]
    assert len(blocks) == 2
    assert "--apply" not in blocks[0]
    assert "--apply" in blocks[1]


def test_the_snapshot_path_matches_the_mount_in_every_command(runbook: str) -> None:
    """A path the container cannot see would fail at the worst moment."""
    blocks = [
        block
        for block in bash_blocks(runbook)
        if RESTORE_MODULE in block or (RECONCILE_MODULE in block and "--apply" in block)
    ]
    assert blocks
    for block in blocks:
        assert "-v /opt/altegio_bot/outputs:/app/outputs" in block, block
        assert "--snapshot /app/outputs/" in block, block


def test_the_apply_step_writes_and_keeps_a_snapshot(runbook: str) -> None:
    reconcile = section(runbook, "## 5. Reconcile the database rows")
    text = prose(reconcile)

    assert "--snapshot" in reconcile
    assert "snapshot_written" in text
    assert "cannot be reconstructed afterwards" in text


def test_the_rollback_states_what_it_does_not_prove(rollback: str) -> None:
    text = prose(rollback)

    assert "not** evidence that it matches what Meta has approved today" in text
    assert "Leave the send fences closed" in text
    assert "never activates, deactivates or re-points a sender" in text


def test_the_rollback_still_forbids_the_blunt_instruments(rollback: str) -> None:
    text = prose(rollback)
    assert "Do **not** restore a database backup, delete queues" in text


# ---------------------------------------------------------------------------
# 3 / 4. Re-opening review goes through the PR-9 preflight
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def reopening(runbook: str) -> str:
    return section(runbook, "## 8. Re-opening sends")


def test_the_review_preflight_runs_before_the_fence_opens(reopening: str) -> None:
    preflight_at = reopening.index(REVIEW_PREFLIGHT_MODULE)
    open_at = reopening.index("EASYWEEK_REVIEW_SEND_ENABLED=true")

    assert preflight_at < open_at


def test_the_review_preflight_is_a_fresh_one_off_container(reopening: str) -> None:
    blocks = [block for block in bash_blocks(reopening) if REVIEW_PREFLIGHT_MODULE in block]
    assert len(blocks) == 1
    words = blocks[0].split()

    assert "run" in words and "--rm" in words and "--no-deps" in words
    assert "exec" not in words
    assert "-e" not in words, "exec -e workarounds are refused explicitly"


def test_the_preflight_acceptance_criteria_are_complete(reopening: str) -> None:
    text = prose(reopening)
    for requirement in ("ready=true", "exit code `0`", "candidate_count > 0", "truncated=false", "config_error"):
        assert requirement in text, requirement


def test_the_global_review_fence_is_named_as_global(reopening: str) -> None:
    """A green branch is not permission to release every other branch."""
    text = prose(reopening)

    assert "**global**, not per branch" in text
    assert "not permission to open it" in text


def test_a_missing_sender_is_a_stop_not_a_workaround(reopening: str) -> None:
    text = prose(reopening)

    assert "sender_missing_or_inactive" in text
    assert "STOP with the fence still closed" in text
    assert "do not invent a per-branch send gate" in text
    assert "removing the branch from `EASYWEEK_LOCATION_MAP`" in text


def test_review_and_retention_are_prepared_independently(reopening: str) -> None:
    text = prose(reopening)

    assert "--code review_3d" in text
    assert "without waiting for its missing retention pair" in text
    assert "while another live branch still holds the old review body" in text


def test_the_retention_sequence_is_unchanged(reopening: str) -> None:
    text = prose(reopening)
    for step in ("preflight", "canary", "close the fence", "bulk"):
        assert step in text, step
    assert "Never edit `run_at`" in text


def test_the_reopening_does_not_replay_the_first_rollout(reopening: str) -> None:
    text = prose(reopening)

    assert "no second broad seed" in text
    assert "no webhook re-creation" in text
    assert "do not switch off planning that is already running" in text


# ---------------------------------------------------------------------------
# 5. The corrected description
# ---------------------------------------------------------------------------


def test_the_runbook_does_not_claim_the_runtime_compared_against_meta(runbook: str) -> None:
    """The runtime compares the DB row against the source contract, not Meta."""
    text = prose(runbook)

    assert "the runtime body-equality guard refused every one of them" not in text
    assert "it does not read Meta" in text
    assert "could pass the runtime guard" in text
