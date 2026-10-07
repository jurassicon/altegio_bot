"""The authorship cleanup tool: what it removes, what it refuses, what it proves.

Every test here builds its own throwaway git repository under ``tmp_path`` and
runs the real tool against it. Nothing touches this repository's own history,
and nothing pushes: a rewrite is only ever performed on a bare clone the test
made seconds earlier.

The synthetic history deliberately mixes the three real shapes this repository
contains — an agent trailer next to a human co-author, a bot author with a
forge committer, and an agent branch name quoted in a merge subject — plus the
cases a careless rule would damage: a paging cursor in prose, a dependency line,
a licence header and a ``.gitignore`` rule.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from pathlib import Path

import pytest

from altegio_bot.scripts import clean_git_authorship as tool

pytestmark = pytest.mark.skipif(
    shutil.which("git") is None or shutil.which("git-filter-repo") is None,
    reason="git and git-filter-repo are required to exercise a real rewrite",
)

HUMAN = ("Human One", "human@example.invalid")
OTHER_HUMAN = ("jurassicon", "196919322+jurassicon@users.noreply.invalid")
BOT = ("copilot-swe-agent[bot]", "198982749+Copilot@users.noreply.invalid")
FORGE = ("GitHub", "noreply@github.invalid")


def git(repo: Path, *args: str, **env: str) -> str:
    environment = {
        "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_CONFIG_SYSTEM": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0",
        "PATH": __import__("os").environ.get("PATH", ""),
        "HOME": str(repo),
        **env,
    }
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        env=environment,
        check=True,
    )
    return result.stdout


def commit(
    repo: Path,
    message: str,
    *,
    author: tuple[str, str] = HUMAN,
    committer: tuple[str, str] = HUMAN,
    content: str | None = None,
    when: str = "2026-01-01T12:00:00+02:00",
) -> str:
    """One commit with exactly the identities and the date the test states."""
    if content is not None:
        (repo / "file.txt").write_text(content, encoding="utf-8")
        git(repo, "add", "file.txt")
    git(
        repo,
        "-c",
        f"user.name={author[0]}",
        "-c",
        f"user.email={author[1]}",
        "commit",
        "--allow-empty",
        "-m",
        message,
        GIT_AUTHOR_NAME=author[0],
        GIT_AUTHOR_EMAIL=author[1],
        GIT_AUTHOR_DATE=when,
        GIT_COMMITTER_NAME=committer[0],
        GIT_COMMITTER_EMAIL=committer[1],
        GIT_COMMITTER_DATE=when,
    )
    return git(repo, "rev-parse", "HEAD").strip()


AGENT_TRAILER_MESSAGE = (
    "refactor: regex-based brand prefix stripping\n"
    "\n"
    "The cursor is the thing that must not move: Chatwoot pages by id < before,\n"
    "so a replayed page is refused rather than retried.\n"
    "\n"
    "Agent-Logs-Url: https://example.invalid/agents/sessions/42\n"
    "\n"
    f"Co-authored-by: {OTHER_HUMAN[0]} <{OTHER_HUMAN[1]}>\n"
)

CLAUDE_TRAILER_MESSAGE = (
    "fix(easyweek): align marketing templates with approved Meta content\n"
    "\n"
    "Transcribed from the approved templates, character for character.\n"
    "\n"
    "Co-Authored-By: Claude Opus 5 <noreply@anthropic.invalid>\n"
)

HUMAN_ONLY_MESSAGE = (
    "docs: record the cursor contract\n"
    "\n"
    "Anthropic and OpenAI are named here as vendors in a comparison table, and\n"
    "the paragraph must survive a cleanup untouched.\n"
    "\n"
    f"Co-authored-by: {OTHER_HUMAN[0]} <{OTHER_HUMAN[1]}>\n"
)

MERGE_MESSAGE = "Merge pull request #42 from jurassicon/copilot/fix-contacts-without-names\n"

TRACKED_CONTENT = (
    "# SPDX-License-Identifier: MIT\n"
    "requests==2.32.3  # dependency, not an attribution\n"
    "CURSOR_PAGE_SIZE = 50\n"
    "# .gitignore in this project lists CLAUDE.md and .claude/ on purpose\n"
)


@pytest.fixture
def history(tmp_path: Path) -> Path:
    """A repository shaped like the real one, with every case side by side."""
    repo = tmp_path / "origin"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main", ".")
    commit(repo, "feat: initial import\n", content=TRACKED_CONTENT)
    commit(repo, AGENT_TRAILER_MESSAGE, author=BOT, committer=FORGE, content=TRACKED_CONTENT + "a\n")
    commit(repo, CLAUDE_TRAILER_MESSAGE, content=TRACKED_CONTENT + "b\n")
    commit(repo, HUMAN_ONLY_MESSAGE, content=TRACKED_CONTENT + "c\n")
    # A real merge, so the parent structure has something to preserve.
    git(repo, "checkout", "-q", "-b", "side")
    commit(repo, "chore: side work\n", author=BOT, committer=BOT, content=TRACKED_CONTENT + "d\n")
    git(repo, "checkout", "-q", "main")
    git(
        repo,
        "-c",
        f"user.name={HUMAN[0]}",
        "-c",
        f"user.email={HUMAN[1]}",
        "merge",
        "--no-ff",
        "-m",
        MERGE_MESSAGE,
        "side",
    )
    git(repo, "tag", "release/one")
    return repo


@pytest.fixture
def clone(history: Path, tmp_path: Path) -> Path:
    """The disposable bare clone a rewrite is allowed to happen in."""
    target = tmp_path / "mirror.git"
    subprocess.run(["git", "clone", "-q", "--mirror", str(history), str(target)], check=True)
    return target


# ===========================================================================
# The rule layer, without git
# ===========================================================================


def test_confirmed_ai_trailers_go_and_the_human_co_author_stays():
    rules = tool.message_rules()
    out = tool.apply_message_rules(AGENT_TRAILER_MESSAGE, rules)
    assert "Agent-Logs-Url" not in out
    assert f"Co-authored-by: {OTHER_HUMAN[0]}" in out
    # The prose, including the word a careless rule would chase, is untouched.
    assert "The cursor is the thing that must not move" in out
    # And the hole the removed trailer left is closed.
    assert "\n\n\n" not in out


def test_a_claude_trailer_goes_and_the_body_ends_on_its_last_substantive_line():
    out = tool.apply_message_rules(CLAUDE_TRAILER_MESSAGE, tool.message_rules())
    assert "Claude" not in out and "anthropic" not in out
    assert out.rstrip().endswith("character for character.")


def test_a_message_with_only_human_attribution_is_returned_unchanged():
    """The vendor names in prose are the trap, and they must survive."""
    out = tool.apply_message_rules(HUMAN_ONLY_MESSAGE, tool.message_rules())
    assert out == HUMAN_ONLY_MESSAGE
    assert "Anthropic and OpenAI are named here as vendors" in out


def test_the_merge_slug_rule_is_opt_in_and_keeps_the_pr_number():
    unchanged = tool.apply_message_rules(MERGE_MESSAGE, tool.message_rules())
    assert unchanged == MERGE_MESSAGE, "the slug rule must not fire without being asked for"
    changed = tool.apply_message_rules(MERGE_MESSAGE, tool.message_rules(merge_slug_prefixes=["copilot"]))
    assert changed.startswith("Merge pull request #42 from jurassicon/fix-contacts-without-names")
    assert "copilot" not in changed


@pytest.mark.parametrize(
    ("name", "email", "expected"),
    [
        (*BOT, tool.MACHINE),
        ("Claude Opus 5", "noreply@anthropic.invalid", tool.MACHINE),
        ("Cursor Agent", "agent@cursor.invalid", tool.MACHINE),
        (*FORGE, tool.FORGE),
        (*HUMAN, tool.HUMAN),
        (*OTHER_HUMAN, tool.HUMAN),
        # The word on its own is a paging cursor, not an agent.
        ("cursor", "cursor@example.invalid", tool.HUMAN),
    ],
)
def test_identities_are_classified_before_anything_may_be_replaced(name, email, expected):
    assert tool.classify_identity(name, email) == expected


def test_every_rule_survives_the_line_based_file_and_compiles_as_bytes():
    """filter-repo reads the file line by line and compiles bytes patterns.

    A rule with a literal newline would split into two broken rules, and a
    ``\\U`` escape — valid in a str pattern, invalid in a bytes one — would
    abort the whole rewrite on the rule file.
    """
    import re as regex

    for rule in tool.message_rules(merge_slug_prefixes=["copilot"]):
        line = f"regex:{rule.pattern}==>{rule.replacement}"
        assert "\n" not in line
        regex.compile(rule.pattern)
        regex.compile(rule.pattern.encode())
    assert len(tool.rules_file_lines(tool.message_rules())) == len(tool.message_rules())


# ===========================================================================
# The identity map: explicit, or nothing happens
# ===========================================================================


def test_a_bot_identity_is_reported_but_never_replaced_without_a_map(history: Path):
    report = tool.audit(history, ["refs/heads/main"], count_signatures=False)
    bot = report.identities[f"{BOT[0]} <{BOT[1]}>"]
    assert bot["class"] == tool.MACHINE and bot["agent"] == "copilot"
    assert bot["appearances"] >= 1 and "author" in bot["roles"]
    assert report.refs[0].machine_authors >= 1
    # Audit mode reports and changes nothing.
    assert tool.main(["--repo", str(history), "--refs", "refs/heads/main", "--no-signature-scan"]) == 0
    assert git(history, "log", "-1", "--format=%an", "refs/heads/main").strip() == HUMAN[0]


def test_an_identity_map_naming_a_human_on_the_left_is_refused(tmp_path: Path):
    path = tmp_path / "map.json"
    path.write_text(json.dumps({f"{HUMAN[0]} <{HUMAN[1]}>": {"name": "X", "email": "x@y.invalid"}}), encoding="utf-8")
    with pytest.raises(ValueError, match="not a machine identity"):
        tool.load_identity_map(path)


def test_an_identity_map_must_name_both_a_name_and_an_email(tmp_path: Path):
    path = tmp_path / "map.json"
    path.write_text(json.dumps({f"{BOT[0]} <{BOT[1]}>": {"name": "X"}}), encoding="utf-8")
    with pytest.raises(ValueError, match="needs both a name and an email"):
        tool.load_identity_map(path)


def test_an_accepted_map_becomes_a_four_field_mailmap(tmp_path: Path):
    path = tmp_path / "map.json"
    path.write_text(
        json.dumps({f"{BOT[0]} <{BOT[1]}>": {"name": HUMAN[0], "email": HUMAN[1]}}),
        encoding="utf-8",
    )
    lines = tool.mailmap_lines(tool.load_identity_map(path))
    assert lines == [f"{HUMAN[0]} <{HUMAN[1]}> {BOT[0]} <{BOT[1]}>"]


# ===========================================================================
# Refusals
# ===========================================================================


def test_a_working_checkout_is_refused(history: Path, tmp_path: Path):
    with pytest.raises(tool.RefusedError, match="working checkout"):
        tool.assert_disposable_clone(history, own_repo=tmp_path / "nowhere")


def test_the_tools_own_repository_is_refused(clone: Path):
    """Pointing it at the repository it came from is the accident to close."""
    with pytest.raises(tool.RefusedError, match="loaded from"):
        tool.assert_disposable_clone(clone, own_repo=clone)


def test_apply_against_a_working_checkout_exits_two_and_changes_nothing(history: Path):
    before = git(history, "rev-parse", "refs/heads/main").strip()
    assert tool.main(["--repo", str(history), "--refs", "refs/heads/main", "--apply"]) == 2
    assert git(history, "rev-parse", "refs/heads/main").strip() == before


# ===========================================================================
# The rewrite, end to end, in a disposable clone
# ===========================================================================


def rewrite_clone(clone: Path, tmp_path: Path, *, identity_map=None, slugs=()) -> dict:
    return tool.rewrite(
        clone,
        refs=[],
        rules=tool.message_rules(merge_slug_prefixes=slugs),
        identity_map=identity_map or {},
        workdir=tmp_path / "inputs",
        own_repo=tmp_path / "nowhere",
    )


def test_a_rewrite_removes_only_ai_attribution_across_branches_and_tags(clone: Path, tmp_path: Path, history: Path):
    before = int(git(history, "rev-list", "--count", "refs/heads/main").strip())
    rewrite_clone(clone, tmp_path, slugs=["copilot"])

    main_log = git(clone, "log", "--format=%B", "refs/heads/main")
    assert "Agent-Logs-Url" not in main_log
    assert "Claude" not in main_log and "anthropic" not in main_log
    assert "jurassicon" in main_log, "the human co-author must survive"
    assert "The cursor is the thing that must not move" in main_log
    assert "Merge pull request #42 from jurassicon/fix-contacts-without-names" in main_log
    # Both other refs were rewritten too, not just the default branch.
    assert "Agent-Logs-Url" not in git(clone, "log", "--format=%B", "refs/heads/side")
    assert "Claude" not in git(clone, "log", "--format=%B", "refs/tags/release/one")
    # Nothing was added or dropped.
    assert int(git(clone, "rev-list", "--count", "refs/heads/main").strip()) == before


def test_tracked_file_contents_are_not_touched_by_a_message_rewrite(clone: Path, tmp_path: Path):
    rewrite_clone(clone, tmp_path, slugs=["copilot"])
    blob = git(clone, "show", "refs/heads/main:file.txt")
    assert "SPDX-License-Identifier: MIT" in blob
    assert "requests==2.32.3" in blob
    assert "CURSOR_PAGE_SIZE" in blob
    assert "CLAUDE.md and .claude/" in blob


def test_the_bot_identity_moves_only_when_the_map_says_so(clone: Path, tmp_path: Path):
    identity_map = {f"{BOT[0]} <{BOT[1]}>": {"name": HUMAN[0], "email": HUMAN[1]}}
    rewrite_clone(clone, tmp_path, identity_map=identity_map)
    identities = git(clone, "log", "--format=%an <%ae>|%cn <%ce>", "--all")
    assert BOT[0] not in identities
    assert f"{HUMAN[0]} <{HUMAN[1]}>" in identities
    # The forge committer is a real committer and is left alone.
    assert FORGE[0] in identities


def test_without_a_map_the_bot_identity_survives_the_message_cleanup(clone: Path, tmp_path: Path):
    rewrite_clone(clone, tmp_path)
    identities = git(clone, "log", "--format=%an <%ae>", "--all")
    assert BOT[0] in identities, "identity replacement must not happen by inference"
    assert "Agent-Logs-Url" not in git(clone, "log", "--format=%B", "--all")


# ===========================================================================
# Verification of the rewrite
# ===========================================================================


def test_verification_proves_trees_dates_parents_and_counts(history: Path, clone: Path, tmp_path: Path):
    rewrite_clone(clone, tmp_path, slugs=["copilot"])
    report = tool.verify(history, clone, refs=["refs/heads/main", "refs/heads/side", "refs/tags/release/one"])
    assert report.pairs > 0
    assert report.dropped == []
    assert report.tree_mismatch == []
    assert report.author_date_mismatch == []
    assert report.committer_date_mismatch == []
    assert report.parent_mismatch == []
    assert report.ref_count_mismatch == []
    assert report.ok
    # The merge is still a merge, with both parents mapped.
    parents = git(clone, "log", "-1", "--format=%P", "refs/heads/main").strip().split()
    assert len(parents) == 2


def test_verification_reports_lost_signatures_rather_than_asserting_they_survive(
    history: Path, tmp_path: Path, clone: Path
):
    """A rewrite cannot re-sign, so the honest report is a count, not a pass."""
    rewrite_clone(clone, tmp_path, slugs=["copilot"])
    report = tool.verify(history, clone, refs=[])
    assert report.signatures_lost == [], "this synthetic history is unsigned"
    assert "signatures no longer present" in report.as_text()


def test_the_cli_verify_mode_reports_residual_findings_and_exits_zero(history: Path, clone: Path, tmp_path: Path):
    rewrite_clone(clone, tmp_path, slugs=["copilot"])
    assert (
        tool.main(
            [
                "--repo",
                str(clone),
                "--verify-against",
                str(history),
                "--refs",
                "refs/heads/main",
                "--merge-slug-prefix",
                "copilot",
            ]
        )
        == 0
    )


def test_a_residual_finding_is_counted_when_a_rule_was_not_asked_for(history: Path, clone: Path, tmp_path: Path):
    """Rewriting without the opt-in slug rule leaves the slug, and says so."""
    rewrite_clone(clone, tmp_path)
    report = tool.verify(history, clone, refs=["refs/heads/main"])
    assert report.ok
    residual = tool.audit(clone, ["refs/heads/main"], merge_slug_prefixes=["copilot"], count_signatures=False)
    assert residual.merge_slugs.get("copilot") == 1
    assert residual.findings > 0


# ===========================================================================
# Running it twice
# ===========================================================================


def test_a_second_rewrite_finds_nothing_left_to_do(clone: Path, tmp_path: Path, history: Path):
    rewrite_clone(clone, tmp_path, slugs=["copilot"])
    first = git(clone, "rev-parse", "refs/heads/main").strip()
    first_log = git(clone, "log", "--format=%B%an%cn", "--all")

    second = tmp_path / "mirror2.git"
    subprocess.run(["git", "clone", "-q", "--mirror", str(clone), str(second)], check=True)
    tool.rewrite(
        second,
        refs=[],
        rules=tool.message_rules(merge_slug_prefixes=["copilot"]),
        identity_map={},
        workdir=tmp_path / "inputs2",
        own_repo=tmp_path / "nowhere",
    )
    assert git(second, "rev-parse", "refs/heads/main").strip() == first
    assert git(second, "log", "--format=%B%an%cn", "--all") == first_log


def test_the_audit_of_a_clean_history_finds_nothing(clone: Path, tmp_path: Path):
    rewrite_clone(clone, tmp_path, slugs=["copilot"])
    report = tool.audit(
        clone,
        ["refs/heads/main", "refs/heads/side"],
        merge_slug_prefixes=["copilot"],
        count_signatures=False,
    )
    assert report.trailer_lines == {}
    assert report.merge_slugs == {}
    assert sum(entry.message_total for entry in report.refs) == 0
    # The bot identity is still there, because nobody supplied a map for it.
    assert sum(entry.machine_authors for entry in report.refs) > 0


# ===========================================================================
# The report an operator reads
# ===========================================================================


def test_the_audit_names_what_it_could_not_check(history: Path):
    report = tool.audit(history, ["refs/heads/main"], count_signatures=False)
    text = report.as_text()
    assert "NOT verified by this tool" in text
    assert any("refs/pull" in item for item in report.unverified)
    assert any("pull request titles" in item for item in report.unverified)
    assert any("fork" in item for item in report.unverified)


def test_the_audit_is_machine_readable_and_quotes_what_it_found(history: Path):
    report = tool.audit(history, ["refs/heads/main"], count_signatures=False)
    payload = json.loads(json.dumps(report.as_dict()))
    assert payload["refs"][0]["ref"] == "refs/heads/main"
    assert payload["refs"][0]["message_hits"]["agent-logs-url-trailer"] == 1
    assert payload["refs"][0]["message_hits"]["ai-co-author-trailer"] == 1
    assert payload["previews"], "an operator must be able to see a before/after"


def test_the_published_command_is_the_one_the_runbook_documents(tmp_path: Path):
    rules = tmp_path / "replace-message.txt"
    mailmap = tmp_path / "mailmap.txt"
    command = tool.filter_repo_command(rules_path=rules, mailmap_path=mailmap, refs=["refs/heads/main"])
    assert command[:2] == ["git-filter-repo", "--force"]
    assert "--replace-message" in command and str(rules) in command
    assert "--mailmap" in command and str(mailmap) in command
    assert command[-2:] == ["--refs", "refs/heads/main"]
    # Nothing in this tool ever offers to push.
    assert not any(part.startswith("push") or part == "--mirror" for part in command)


# ===========================================================================
# The runbook and the tool have to agree
# ===========================================================================

RUNBOOK = Path(__file__).resolve().parents[3] / "docs" / "ops" / "git_authorship_cleanup.md"


def test_the_runbook_never_offers_a_bare_force_or_a_mirror_push():
    """The publication step is the one place a mistake is unrecoverable.

    ``--mirror`` would delete every remote ref the clone happens not to have,
    and a bare ``--force`` would overwrite a branch somebody moved after the
    rehearsal. So no push may carry either, and every push that overwrites
    history must state the SHA it expects to find. A create and a delete carry
    no force at all and need no lease — git already refuses a create that would
    clobber something.
    """
    text = RUNBOOK.read_text(encoding="utf-8")
    pushes = [line for line in text.splitlines() if "git push" in line]
    assert pushes, "the runbook must show how to publish"
    overwriting = 0
    for line in pushes:
        assert "--mirror" not in line, line
        assert " --force " not in f" {line} ", line
        assert "--force " not in line.replace("--force-with-lease", ""), line
        if "--force" in line:
            assert "--force-with-lease=" in line, line
            overwriting += 1
    assert overwriting >= 2, "the force-with-lease form has to be shown, not just described"


def test_the_runbook_states_the_limits_it_cannot_overcome():
    text = RUNBOOK.read_text(encoding="utf-8")
    for promise in ("refs/pull", "signature", "protect", "fork"):
        assert promise.lower() in text.lower(), promise
    # It must not claim a complete erasure anywhere on the forge.
    assert "cannot deliver it" in text


def test_the_runbook_documents_the_module_that_actually_exists():
    text = RUNBOOK.read_text(encoding="utf-8")
    assert "altegio_bot.scripts.clean_git_authorship" in text
    assert "src/altegio_bot/scripts/clean_git_authorship.py" in text
    assert Path(tool.__file__).exists()
    # Audit is the documented default, and --apply the documented exception.
    assert "--apply" in text
    parser = tool.build_parser()
    flags = {action.option_strings[0] for action in parser._actions if action.option_strings}
    assert {"--apply", "--identity-map", "--merge-slug-prefix", "--verify-against"} <= flags


def test_the_runbook_reports_counts_rather_than_pasting_the_audit_into_the_tree():
    """The audit quotes the old attribution verbatim, so it stays local.

    Documenting the SHAPE of a trailer is what makes the runbook usable; what
    may not land in a tracked file is the audit's content — a real agent
    session id, or a per-commit dump of messages and SHAs. The only full SHAs
    allowed here are the leases the publication step cannot be written without.
    """
    import re as regex

    text = RUNBOOK.read_text(encoding="utf-8")
    assert not regex.search(r"sessions/[0-9a-f]{8}-[0-9a-f]{4}", text), "a real agent session id is in the document"
    full_shas = set(regex.findall(r"\b[0-9a-f]{40}\b", text))
    assert len(full_shas) <= 2, f"the runbook should carry leases, not a commit dump: {sorted(full_shas)}"
    assert "Keep the output **outside the repository**" in text


def test_a_ref_whose_own_name_carries_an_agent_prefix_is_reported_not_rewritten(history: Path):
    """No message rule can move a ref, so the audit has to say so out loud."""
    git(history, "branch", "copilot/fix-something", "refs/heads/main")
    report = tool.audit(
        history,
        ["refs/heads/main", "refs/heads/copilot/fix-something"],
        count_signatures=False,
    )
    assert report.agent_named_refs == ["refs/heads/copilot/fix-something"]
    assert "a rename is a separate decision" in report.as_text()
