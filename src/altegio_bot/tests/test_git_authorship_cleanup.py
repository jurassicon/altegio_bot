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
import os
import re
import subprocess
from pathlib import Path

import pytest

from altegio_bot.scripts import clean_git_authorship as tool

# No module-level skip, deliberately. `git-filter-repo` is a locked dev
# dependency of this project, so `uv sync --frozen` provides it here and on
# every required CI runner; a missing one is a loud failure rather than a green
# run with 37 skips, which is how these regressions silently left the gate.
# The pure-function tests below never touch the executable at all.


def test_the_rewrite_engine_is_provided_by_the_project_not_by_the_machine():
    """The dependency is pinned, so neither a laptop nor a runner image decides.

    This is the regression for a module-level skipif that turned the absence of
    a Homebrew install into `37 skipped, exit code 0` — a required gate that
    reported success while verifying nothing.
    """
    assert tool.filter_repo_executable(), "git-filter-repo must be resolvable"
    assert "git-filter-repo" in (Path(__file__).resolve().parents[3] / "pyproject.toml").read_text(encoding="utf-8")
    locked = (Path(__file__).resolve().parents[3] / "uv.lock").read_text(encoding="utf-8")
    assert 'name = "git-filter-repo"' in locked


def test_a_missing_engine_is_a_clear_refusal_not_a_skip(monkeypatch, tmp_path: Path):
    """With neither the script nor the module, the tool must say exactly that."""
    monkeypatch.setattr(tool.shutil, "which", lambda _name: None)
    monkeypatch.setitem(__import__("sys").modules, "git_filter_repo", None)
    monkeypatch.delitem(__import__("sys").modules, "git_filter_repo")

    import builtins

    real_import = builtins.__import__

    def refuse(name, *args, **kwargs):
        if name == "git_filter_repo":
            raise ImportError("not installed")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", refuse)
    with pytest.raises(tool.RefusedError, match="locked dev dependency"):
        tool.filter_repo_executable()


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


def commit_message(repo: Path, ref: str) -> str:
    """The message EXACTLY as the commit object stores it.

    ``git log --format=%B`` appends a newline of its own, so comparing it with
    an in-memory string shows a difference that is not in the repository. The
    object is the only place that answers the byte question.
    """
    raw = git(repo, "cat-file", "commit", ref)
    return raw.split("\n\n", 1)[1]


def commit(
    repo: Path,
    message: str,
    *,
    author: tuple[str, str] = HUMAN,
    committer: tuple[str, str] = HUMAN,
    content: str | None = None,
    when: str = "2026-01-01T12:00:00+02:00",
    cleanup: str = "whitespace",
) -> str:
    """One commit with exactly the identities, date and message the test states.

    ``cleanup`` matters for the normalisation regressions: git's own default
    ``whitespace`` mode collapses blank-line runs AT COMMIT TIME, so a fixture
    that wants a message with ``\\n\\n\\n`` in it has to ask for ``verbatim``.
    Without that, a test would "prove" preservation of something git had
    already flattened before this tool ever saw it.
    """
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
        f"--cleanup={cleanup}",
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
    out = tool.transform_message(AGENT_TRAILER_MESSAGE).text
    assert "Agent-Logs-Url" not in out
    assert f"Co-authored-by: {OTHER_HUMAN[0]}" in out
    # The prose, including the word a careless rule would chase, is untouched.
    assert "The cursor is the thing that must not move" in out
    # And the hole the removed trailer left is closed.
    assert "\n\n\n" not in out


def test_a_claude_trailer_goes_and_the_body_ends_on_its_last_substantive_line():
    out = tool.transform_message(CLAUDE_TRAILER_MESSAGE).text
    assert "Claude" not in out and "anthropic" not in out
    assert out.rstrip().endswith("character for character.")


def test_a_message_with_only_human_attribution_is_returned_unchanged():
    """The vendor names in prose are the trap, and they must survive."""
    change = tool.transform_message(HUMAN_ONLY_MESSAGE)
    out = change.text
    assert out == HUMAN_ONLY_MESSAGE
    assert change.removals == ()
    assert "Anthropic and OpenAI are named here as vendors" in out


def test_the_merge_slug_rule_is_opt_in_and_keeps_the_pr_number():
    unchanged = tool.transform_message(MERGE_MESSAGE).text
    assert unchanged == MERGE_MESSAGE, "the slug rule must not fire without being asked for"
    changed = tool.transform_message(MERGE_MESSAGE, merge_slug_prefixes=["copilot"]).text
    assert changed.startswith("Merge pull request #42 from jurassicon/fix-contacts-without-names")
    assert "copilot" not in changed


@pytest.mark.parametrize(
    ("name", "email", "expected"),
    [
        # Confirmed: GitHub's `[bot]` account suffix and agent vendors' own
        # no-reply addresses. These are shapes a person cannot hold.
        (*BOT, tool.MACHINE),
        ("Claude Opus 5", "noreply@anthropic.invalid", tool.MACHINE),
        ("Cursor Agent", "agent@cursor.invalid", tool.MACHINE),
        ("anything at all[bot]", "whatever@example.invalid", tool.MACHINE),
        # The platform, committing on a person's behalf.
        (*FORGE, tool.FORGE),
        # People.
        (*HUMAN, tool.HUMAN),
        (*OTHER_HUMAN, tool.HUMAN),
        # Resemblances. Reported, never acted on — the regression for a
        # substring match that classified these two people as bots.
        ("Jean-Claude Dupont", "jcd@example.org", tool.AMBIGUOUS),
        ("Devin Smith", "devin@example.org", tool.AMBIGUOUS),
        # Not even flagged: "claudia" does not contain "claude", so a person
        # with this name never reaches the audit's ambiguous bucket.
        ("Claudia Klein", "claudia@example.org", tool.HUMAN),
        ("cursor", "cursor@example.invalid", tool.AMBIGUOUS),
    ],
)
def test_identities_are_classified_before_anything_may_be_replaced(name, email, expected):
    assert tool.classify_identity(name, email) == expected


def test_only_a_confirmed_machine_identity_carries_an_agent_label():
    assert tool.machine_identity_label(*BOT) == "github-app-bot"
    assert tool.machine_identity_label("Claude", "noreply@anthropic.invalid") == "claude"
    assert tool.machine_identity_label("Jean-Claude Dupont", "jcd@example.org") is None
    assert tool.machine_identity_label(*HUMAN) is None


def test_the_preview_and_the_rewrite_share_one_implementation():
    """There is exactly one transformation, and the callback delegates to it.

    The replaced version described the rules in a filter-repo file and
    re-implemented them in Python for the preview; the bytes/str difference
    then made the two disagree. A callback two statements long cannot.
    """
    command = tool.filter_repo_command(mailmap_path=None, refs=[])
    callback = command[command.index("--message-callback") + 1]
    assert "transform_message_bytes" in callback
    assert "regex" not in callback and "re.sub" not in callback

    sample = "x: y\n\nGenerated with Claude Code\n"
    assert tool.transform_message_bytes(sample.encode()) == tool.transform_message(sample).text.encode()


def test_an_undecodable_message_is_left_alone_rather_than_guessed_at():
    raw = b"subject\n\n\xff\xfe not utf-8\n"
    assert tool.transform_message_bytes(raw) == raw


# ===========================================================================
# The identity map: explicit, or nothing happens
# ===========================================================================


def test_a_bot_identity_is_reported_but_never_replaced_without_a_map(history: Path):
    report = tool.audit(history, ["refs/heads/main"], count_signatures=False)
    bot = report.identities[f"{BOT[0]} <{BOT[1]}>"]
    assert bot["class"] == tool.MACHINE
    # Confirmed by the `[bot]` account suffix rather than by the product name.
    assert bot["agent"] == "github-app-bot"
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


def rewrite_clone(clone: Path, tmp_path: Path, *, identity_map=None, slugs=(), refs=(), workdir="inputs") -> dict:
    return tool.rewrite(
        clone,
        refs=list(refs),
        merge_slug_prefixes=slugs,
        identity_map=identity_map or {},
        workdir=tmp_path / workdir,
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
    rewrite_clone(second, tmp_path, slugs=["copilot"], workdir="inputs2")
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
    assert payload["refs"][0]["message_hits"][tool.IDENTITY_TRAILER_REASON] == 1
    assert payload["previews"], "an operator must be able to see a before/after"


def test_the_published_command_is_the_one_the_runbook_documents(tmp_path: Path):
    mailmap = tmp_path / "mailmap.txt"
    command = tool.filter_repo_command(mailmap_path=mailmap, refs=["refs/heads/main"])
    assert command[1] == "--force"
    assert "git-filter-repo" in command[0] or command[:3] == [__import__("sys").executable, "-m", "git_filter_repo"]
    assert "--message-callback" in command
    assert "--mailmap" in command and str(mailmap) in command
    assert command[-2:] == ["--refs", "refs/heads/main"]
    # Nothing in this tool ever offers to push.
    assert not any(part.startswith("push") or part == "--mirror" for part in command)


def test_the_written_plan_states_the_conditional_tidying(tmp_path: Path):
    """An operator has to be able to read the rule before authorising it."""
    plan_path, mailmap_path = tool.write_inputs(
        tmp_path / "inputs",
        merge_slug_prefixes=["copilot"],
        identity_map={},
    )
    plan = plan_path.read_text(encoding="utf-8")
    assert mailmap_path is None
    assert "ONLY in a message a removal above actually changed" in plan
    assert "byte for byte" in plan
    assert "does NOT trigger blank-line tidying" in plan
    assert tool.IDENTITY_TRAILER_REASON in plan


# ===========================================================================
# The runbook and the tool have to agree
# ===========================================================================

RUNBOOK = Path(__file__).resolve().parents[3] / "docs" / "ops" / "git_authorship_cleanup.md"


def test_every_push_in_the_runbook_states_what_it_expects_to_find():
    """The publication step is the one place a mistake is unrecoverable.

    ``--mirror`` would delete every remote ref the clone happens not to have,
    and a bare ``--force`` would overwrite a branch somebody moved after the
    rehearsal. The reviewed version of this test exempted creates and deletes
    from needing a lease, which was wrong about the delete: ``git push
    --delete`` removes the branch whatever state it is in, and the behavioural
    tests further down show it discarding a commit that landed after the
    snapshot. So EVERY push here carries a lease.
    """
    text = RUNBOOK.read_text(encoding="utf-8")
    pushes = [line for line in text.splitlines() if "git push" in line]
    assert pushes, "the runbook must show how to publish"
    for line in pushes:
        assert "--mirror" not in line, line
        assert " --force " not in f" {line} ", line
        assert "--force " not in line.replace("--force-with-lease", ""), line
        assert "--force-with-lease=" in line, line
    assert len(pushes) >= 4, "publish, rename-create, rename-delete and restore all have to be shown"


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
    # One lease per published example push, plus the leased delete. Anything
    # beyond a handful would be the audit pasted in.
    assert len(full_shas) <= 4, f"the runbook should carry leases, not a commit dump: {sorted(full_shas)}"
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


# ===========================================================================
# P1: the verifier has to prove each agreed ref, not just count commits
# ===========================================================================


@pytest.fixture
def two_branch_history(tmp_path: Path) -> Path:
    """Two branches with the SAME commit count and different content.

    That is the shape the review used: equal counts are what let a hijacked
    ref pass a count-only check.
    """
    repo = tmp_path / "twobranch"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main", ".")
    commit(repo, "base\n", content="base\n")
    commit(repo, "main one\n", content="main one\n")
    commit(repo, "main two\n", content="main two\n")
    git(repo, "checkout", "-q", "-b", "side", "HEAD~2")
    commit(repo, "side one\n", content="side one\n")
    commit(repo, "side two\n", content="side two\n")
    git(repo, "checkout", "-q", "main")
    assert git(repo, "rev-list", "--count", "main").strip() == git(repo, "rev-list", "--count", "side").strip()
    return repo


def mirror_of(source: Path, target: Path) -> Path:
    subprocess.run(["git", "clone", "-q", "--mirror", str(source), str(target)], check=True)
    return target


AGREED_TWO = ["refs/heads/main", "refs/heads/side"]


def test_a_correct_rewrite_of_several_branches_and_an_annotated_tag_verifies(tmp_path: Path):
    """The happy path, including a tag whose own object SHA legitimately moves."""
    repo = tmp_path / "tagged"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main", ".")
    commit(repo, "feat: one\n", content="one\n")
    commit(repo, CLAUDE_TRAILER_MESSAGE, content="two\n")
    git(repo, "tag", "-a", "release/one", "-m", "annotated")
    git(repo, "checkout", "-q", "-b", "side")
    commit(repo, "chore: side\n", content="three\n")
    git(repo, "checkout", "-q", "main")

    original = mirror_of(repo, tmp_path / "orig.git")
    rewritten = mirror_of(repo, tmp_path / "new.git")
    agreed = [*AGREED_TWO, "refs/tags/release/one"]
    rewrite_clone(rewritten, tmp_path, refs=agreed)

    # The tag object itself is a different object now, which is correct.
    assert git(rewritten, "rev-parse", "refs/tags/release/one") != git(original, "rev-parse", "refs/tags/release/one")
    assert git(rewritten, "cat-file", "-t", "refs/tags/release/one").strip() == "tag"

    report = tool.verify(original, rewritten, refs=agreed)
    assert report.ok, report.as_text()
    assert report.refs_checked == 3
    assert report.missing_refs == [] and report.tip_mismatch == [] and report.unverifiable_refs == []


def test_pointing_main_at_another_tip_with_the_same_count_is_rejected(two_branch_history: Path, tmp_path: Path):
    """The reviewed verifier reported PASS for exactly this."""
    original = mirror_of(two_branch_history, tmp_path / "orig.git")
    rewritten = mirror_of(two_branch_history, tmp_path / "new.git")
    rewrite_clone(rewritten, tmp_path, refs=AGREED_TWO)

    git(rewritten, "update-ref", "refs/heads/main", "refs/heads/side")
    assert (
        git(rewritten, "rev-list", "--count", "refs/heads/main").strip()
        == git(original, "rev-list", "--count", "refs/heads/main").strip()
    ), "the counts must still match, or this test is not the reviewed scenario"

    report = tool.verify(original, rewritten, refs=AGREED_TWO)
    assert not report.ok
    assert report.ref_count_mismatch == [], "the count check cannot be what catches this"
    assert len(report.tip_mismatch) == 1
    assert "refs/heads/main" in report.tip_mismatch[0]
    # And the content really did change, which is the harm being caught.
    assert (
        git(rewritten, "log", "-1", "--format=%s", "refs/heads/main").strip()
        != git(original, "log", "-1", "--format=%s", "refs/heads/main").strip()
    )


def test_deleting_an_agreed_branch_is_rejected(two_branch_history: Path, tmp_path: Path):
    original = mirror_of(two_branch_history, tmp_path / "orig.git")
    rewritten = mirror_of(two_branch_history, tmp_path / "new.git")
    rewrite_clone(rewritten, tmp_path, refs=AGREED_TWO)

    git(rewritten, "update-ref", "-d", "refs/heads/side")
    report = tool.verify(original, rewritten, refs=AGREED_TWO)
    assert not report.ok
    assert len(report.missing_refs) == 1 and "refs/heads/side" in report.missing_refs[0]


@pytest.mark.parametrize("sabotage", ["hijack", "delete"])
def test_the_cli_fails_with_a_nonzero_exit_for_both_defects(two_branch_history: Path, tmp_path: Path, sabotage):
    original = mirror_of(two_branch_history, tmp_path / "orig.git")
    rewritten = mirror_of(two_branch_history, tmp_path / "new.git")
    rewrite_clone(rewritten, tmp_path, refs=AGREED_TWO)

    if sabotage == "hijack":
        git(rewritten, "update-ref", "refs/heads/main", "refs/heads/side")
    else:
        git(rewritten, "update-ref", "-d", "refs/heads/side")

    code = tool.main(["--repo", str(rewritten), "--verify-against", str(original), "--refs", *AGREED_TWO])
    assert code == 1, "a sabotaged rewrite must not exit 0"


def test_without_explicit_refs_the_agreed_set_comes_from_the_original(two_branch_history: Path, tmp_path: Path):
    """A vanished branch must not vanish from its own verification.

    The reviewed CLI read the ref list out of the REWRITTEN repository, so a
    deleted branch was simply never looked at.
    """
    original = mirror_of(two_branch_history, tmp_path / "orig.git")
    rewritten = mirror_of(two_branch_history, tmp_path / "new.git")
    rewrite_clone(rewritten, tmp_path, refs=AGREED_TWO)
    git(rewritten, "update-ref", "-d", "refs/heads/side")

    assert tool.main(["--repo", str(rewritten), "--verify-against", str(original)]) == 1
    report = tool.verify(original, rewritten, refs=tool.default_refs(original))
    assert any("refs/heads/side" in entry for entry in report.missing_refs)


def test_an_explicitly_chosen_subset_is_verified_within_its_own_bounds(two_branch_history: Path, tmp_path: Path):
    """A subset stays a subset: untouched refs outside it are not demanded."""
    original = mirror_of(two_branch_history, tmp_path / "orig.git")
    rewritten = mirror_of(two_branch_history, tmp_path / "new.git")
    rewrite_clone(rewritten, tmp_path, refs=["refs/heads/main"])

    report = tool.verify(original, rewritten, refs=["refs/heads/main"])
    assert report.ok, report.as_text()
    assert report.refs_checked == 1
    # `side` was outside the agreed set and was left at its original tip; the
    # subset verification neither demands nor complains about it.
    assert (
        git(rewritten, "rev-parse", "refs/heads/side").strip() == git(original, "rev-parse", "refs/heads/side").strip()
    )


def test_an_empty_commit_map_is_not_a_pass(two_branch_history: Path, tmp_path: Path):
    original = mirror_of(two_branch_history, tmp_path / "orig.git")
    rewritten = mirror_of(two_branch_history, tmp_path / "new.git")
    rewrite_clone(rewritten, tmp_path, refs=AGREED_TWO)

    (rewritten / "filter-repo" / "commit-map").write_text("old                                      new\n")
    report = tool.verify(original, rewritten, refs=AGREED_TWO)
    assert report.pairs == 0
    assert not report.ok, "an empty commit-map means nothing was verified"
    assert "nothing was actually verified" in report.as_text()


def test_a_missing_commit_map_is_an_error_rather_than_a_verdict(two_branch_history: Path, tmp_path: Path):
    original = mirror_of(two_branch_history, tmp_path / "orig.git")
    never_rewritten = mirror_of(two_branch_history, tmp_path / "new.git")
    with pytest.raises(tool.GitError, match="commit-map"):
        tool.verify(original, never_rewritten, refs=AGREED_TWO)
    assert tool.main(["--repo", str(never_rewritten), "--verify-against", str(original), "--refs", *AGREED_TWO]) == 2


# ===========================================================================
# P2: a resemblance is not a confirmation
# ===========================================================================

JEAN_CLAUDE = "Co-Authored-By: Jean-Claude Dupont <jcd@example.org>"
DEVIN = "Co-Authored-By: Devin Smith <devin@example.org>"
CONFIRMED_CLAUDE = "Co-Authored-By: Claude Opus 5 <noreply@anthropic.invalid>"

MIXED_TRAILERS = (
    "fix: a change with four co-authors\n"
    "\n"
    "Prose that mentions Anthropic and OpenAI as vendors must survive.\n"
    "\n"
    f"{JEAN_CLAUDE}\n"
    f"{DEVIN}\n"
    f"Co-authored-by: {OTHER_HUMAN[0]} <{OTHER_HUMAN[1]}>\n"
    f"{CONFIRMED_CLAUDE}\n"
)


def test_people_whose_names_resemble_an_agent_keep_their_co_author_lines():
    """The reviewed rules deleted both of these lines by substring match."""
    out = tool.transform_message(MIXED_TRAILERS).text
    assert JEAN_CLAUDE in out
    assert DEVIN in out
    assert f"Co-authored-by: {OTHER_HUMAN[0]} <{OTHER_HUMAN[1]}>" in out
    # And the one confirmed machine line is gone.
    assert CONFIRMED_CLAUDE not in out
    assert "anthropic" not in out.lower().replace("anthropic and openai", "")
    assert "Anthropic and OpenAI as vendors must survive" in out


def test_a_rewrite_keeps_those_lines_in_the_real_commit_object(tmp_path: Path):
    """Not just the classifier: the bytes in the rewritten commit."""
    repo = tmp_path / "people"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main", ".")
    commit(repo, "feat: base\n", content="base\n")
    commit(repo, MIXED_TRAILERS, content="more\n")
    rewritten = mirror_of(repo, tmp_path / "new.git")
    rewrite_clone(rewritten, tmp_path, refs=["refs/heads/main"])

    message = git(rewritten, "log", "-1", "--format=%B", "refs/heads/main")
    assert JEAN_CLAUDE in message
    assert DEVIN in message
    assert CONFIRMED_CLAUDE not in message


def test_human_author_and_committer_with_such_names_are_untouched(tmp_path: Path):
    repo = tmp_path / "authors"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main", ".")
    commit(repo, "feat: base\n", content="base\n")
    commit(
        repo,
        "fix: by a person whose name resembles an agent\n",
        author=("Jean-Claude Dupont", "jcd@example.org"),
        committer=("Devin Smith", "devin@example.org"),
        content="more\n",
    )
    rewritten = mirror_of(repo, tmp_path / "new.git")
    rewrite_clone(rewritten, tmp_path, refs=["refs/heads/main"])

    identity = git(rewritten, "log", "-1", "--format=%an <%ae>|%cn <%ce>", "refs/heads/main").strip()
    assert identity == "Jean-Claude Dupont <jcd@example.org>|Devin Smith <devin@example.org>"


def test_an_identity_map_cannot_name_a_person_who_merely_resembles_an_agent(tmp_path: Path):
    path = tmp_path / "map.json"
    path.write_text(
        json.dumps(
            {"Jean-Claude Dupont <jcd@example.org>": {"name": HUMAN[0], "email": HUMAN[1]}},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="not a machine identity"):
        tool.load_identity_map(path)


def test_the_audit_shows_a_resemblance_without_acting_on_it(tmp_path: Path):
    repo = tmp_path / "ambiguous"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main", ".")
    commit(repo, "feat: base\n", author=("Devin Smith", "devin@example.org"), content="base\n")
    report = tool.audit(repo, ["refs/heads/main"], count_signatures=False)
    entry = report.identities["Devin Smith <devin@example.org>"]
    assert entry["class"] == tool.AMBIGUOUS and entry["agent"] is None
    assert report.refs[0].ambiguous_authors == 1
    assert report.refs[0].machine_authors == 0


def test_the_forge_committer_and_vendor_prose_survive_a_rewrite(clone: Path, tmp_path: Path):
    rewrite_clone(clone, tmp_path, slugs=["copilot"])
    identities = git(clone, "log", "--format=%cn <%ce>", "--all")
    assert FORGE[0] in identities
    assert "Anthropic and OpenAI are named here as vendors" in git(clone, "log", "--format=%B", "--all")
    assert "CURSOR_PAGE_SIZE" in git(clone, "show", "refs/heads/main:file.txt")


# ===========================================================================
# P2: the generated-with footer, in preview and in the commit object
# ===========================================================================

FOOTER_CASES = (
    ("without-emoji", "docs: a\n\nBody.\n\nGenerated with Claude Code\n", True),
    ("with-emoji", "docs: b\n\nBody.\n\n\U0001f916 Generated with Claude Code\n", True),
    ("plain-prose", "docs: c\n\nBody that was generated with care by a person.\n", False),
)


@pytest.mark.parametrize(("label", "message", "should_change"), FOOTER_CASES)
def test_the_footer_preview_matches_the_rewritten_commit(tmp_path: Path, label, message, should_change):
    """The emoji was optional in str and mandatory in bytes, so the two diverged."""
    repo = tmp_path / f"footer-{label}"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main", ".")
    commit(repo, "feat: base\n", content="base\n")
    commit(repo, message, content="more\n", cleanup="verbatim")

    preview = tool.transform_message(message)
    assert preview.changed is should_change

    rewritten = mirror_of(repo, tmp_path / "new.git")
    rewrite_clone(rewritten, tmp_path, refs=["refs/heads/main"])
    actual = commit_message(rewritten, "refs/heads/main")

    # Read from the rewritten commit object, and compared against the preview.
    assert actual == preview.text, f"{label}: preview and rewrite disagree"
    if should_change:
        assert "Generated with" not in actual
    else:
        assert "generated with care by a person" in actual


def test_a_second_audit_does_not_find_a_footer_that_was_already_removed(tmp_path: Path):
    repo = tmp_path / "footer-twice"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main", ".")
    commit(repo, "feat: base\n", content="base\n")
    commit(repo, "docs: a\n\nGenerated with Claude Code\n", content="more\n")
    rewritten = mirror_of(repo, tmp_path / "new.git")
    rewrite_clone(rewritten, tmp_path, refs=["refs/heads/main"])

    after = tool.audit(rewritten, ["refs/heads/main"], count_signatures=False)
    assert after.findings == 0
    assert "generated-with-footer" not in after.refs[0].message_hits


# ===========================================================================
# P2: normalisation only where something was actually removed
# ===========================================================================

UNRELATED_MESSAGE = "fix: ordinary change\n\n\nKeep this prose\n\n\n"


def test_a_message_without_attribution_is_preserved_byte_for_byte(tmp_path: Path):
    """Including the blank-line run and the trailing newlines.

    The reviewed tidy rules ran on every message, so this one lost a newline
    even though the tool had no business touching it.
    """
    assert tool.transform_message(UNRELATED_MESSAGE).text == UNRELATED_MESSAGE

    repo = tmp_path / "untouched"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main", ".")
    commit(repo, "feat: base\n", content="base\n")
    commit(repo, UNRELATED_MESSAGE, content="more\n", cleanup="verbatim")
    before = commit_message(repo, "refs/heads/main")
    assert before == UNRELATED_MESSAGE, "git's own cleanup must not have collapsed the fixture"

    rewritten = mirror_of(repo, tmp_path / "new.git")
    rewrite_clone(rewritten, tmp_path, refs=["refs/heads/main"])
    assert commit_message(rewritten, "refs/heads/main") == before


def test_replacing_only_a_bot_identity_leaves_the_message_alone(tmp_path: Path):
    """An identity map is not permission to reformat a message."""
    repo = tmp_path / "identity-only"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main", ".")
    commit(repo, "feat: base\n", content="base\n")
    commit(repo, UNRELATED_MESSAGE, author=BOT, committer=FORGE, content="more\n", cleanup="verbatim")
    before = commit_message(repo, "refs/heads/main")

    rewritten = mirror_of(repo, tmp_path / "new.git")
    rewrite_clone(
        rewritten,
        tmp_path,
        refs=["refs/heads/main"],
        identity_map={f"{BOT[0]} <{BOT[1]}>": {"name": HUMAN[0], "email": HUMAN[1]}},
    )

    assert commit_message(rewritten, "refs/heads/main") == before
    assert git(rewritten, "log", "-1", "--format=%an <%ae>", "refs/heads/main").strip() == f"{HUMAN[0]} <{HUMAN[1]}>"


def test_a_merge_slug_rewrite_does_not_normalise_unrelated_text():
    """Editing a word in the subject must not drag a blank line along."""
    message = "Merge pull request #42 from jurassicon/copilot/fix-thing\n\n\nBody kept as is.\n\n\n"
    out = tool.transform_message(message, merge_slug_prefixes=["copilot"]).text
    assert out == "Merge pull request #42 from jurassicon/fix-thing\n\n\nBody kept as is.\n\n\n"


def test_removing_a_machine_trailer_keeps_the_prose_and_the_human_attribution(tmp_path: Path):
    repo = tmp_path / "removal"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main", ".")
    commit(repo, "feat: base\n", content="base\n")
    commit(repo, AGENT_TRAILER_MESSAGE, author=BOT, committer=FORGE, content="more\n")
    rewritten = mirror_of(repo, tmp_path / "new.git")
    rewrite_clone(rewritten, tmp_path, refs=["refs/heads/main"])

    message = git(rewritten, "log", "-1", "--format=%B", "refs/heads/main")
    assert "Agent-Logs-Url" not in message
    assert "The cursor is the thing that must not move" in message
    assert f"Co-authored-by: {OTHER_HUMAN[0]}" in message
    assert "\n\n\n" not in message, "the hole the removed trailer left must be closed"


def test_running_the_whole_rewrite_twice_changes_nothing_the_second_time(tmp_path: Path):
    repo = tmp_path / "idempotent"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main", ".")
    commit(repo, "feat: base\n", content="base\n")
    commit(repo, MIXED_TRAILERS, content="a\n")
    commit(repo, UNRELATED_MESSAGE, content="b\n", cleanup="verbatim")
    commit(repo, "docs: f\n\nGenerated with Claude Code\n", content="c\n")

    first = mirror_of(repo, tmp_path / "first.git")
    rewrite_clone(first, tmp_path, refs=["refs/heads/main"], workdir="in1")
    once = git(first, "rev-parse", "refs/heads/main").strip()

    second = mirror_of(first, tmp_path / "second.git")
    rewrite_clone(second, tmp_path, refs=["refs/heads/main"], workdir="in2")
    assert git(second, "rev-parse", "refs/heads/main").strip() == once
    assert git(second, "log", "--format=%B%an%cn", "refs/heads/main") == git(
        first, "log", "--format=%B%an%cn", "refs/heads/main"
    )


# ===========================================================================
# P2: the rename pair, against a real local bare remote
# ===========================================================================
# A local bare repository, never the project's own origin. These tests push
# and delete for real, because the question is what git does — asserting that
# the word `--force-with-lease` appears in the Markdown proves nothing about
# whether a delete can still throw somebody's commit away.

OLD_BRANCH = "refs/heads/copilot/old-name"
NEW_BRANCH = "refs/heads/new-name"


def push(repo: Path, *args: str) -> subprocess.CompletedProcess:
    """A push that is allowed to fail, so a test can assert the refusal."""
    return subprocess.run(
        ["git", "-C", str(repo), "push", *args],
        capture_output=True,
        text=True,
        env={
            "GIT_CONFIG_GLOBAL": "/dev/null",
            "GIT_CONFIG_SYSTEM": "/dev/null",
            "GIT_TERMINAL_PROMPT": "0",
            "PATH": os.environ.get("PATH", ""),
            "HOME": str(repo),
        },
    )


@pytest.fixture
def remote_pair(tmp_path: Path) -> tuple[Path, Path, str]:
    """A bare remote holding the old branch, plus a clone and the agreed SHA."""
    remote = tmp_path / "remote.git"
    subprocess.run(["git", "init", "-q", "--bare", str(remote)], check=True)

    work = tmp_path / "work"
    work.mkdir()
    git(work, "init", "-q", "-b", "main", ".")
    commit(work, "feat: base\n", content="base\n")
    git(work, "checkout", "-q", "-b", "copilot/old-name")
    commit(work, "chore: agent work\n", content="agent\n")
    assert push(work, str(remote), "main", "copilot/old-name").returncode == 0
    agreed = git(work, "rev-parse", "refs/heads/copilot/old-name").strip()
    return remote, work, agreed


def test_the_documented_rename_pair_succeeds_on_a_quiet_remote(remote_pair):
    remote, work, agreed = remote_pair

    created = push(work, f"--force-with-lease={NEW_BRANCH}:", str(remote), f"{OLD_BRANCH}:{NEW_BRANCH}")
    assert created.returncode == 0, created.stderr
    deleted = push(work, f"--force-with-lease={OLD_BRANCH}:{agreed}", str(remote), "--delete", OLD_BRANCH)
    assert deleted.returncode == 0, deleted.stderr

    refs = git(remote, "for-each-ref", "--format=%(refname)")
    assert NEW_BRANCH in refs and OLD_BRANCH not in refs


def test_a_commit_pushed_after_the_snapshot_makes_the_delete_refuse(remote_pair, tmp_path: Path):
    """The defect: `git push --delete` removes the branch whatever state it is in."""
    remote, work, agreed = remote_pair

    # Somebody else pushes to the branch after the snapshot was taken.
    other = tmp_path / "other"
    subprocess.run(["git", "clone", "-q", str(remote), str(other)], check=True)
    git(other, "checkout", "-q", "-B", "copilot/old-name", "origin/copilot/old-name")
    commit(other, "fix: landed after the snapshot\n", content="late\n")
    assert push(other, "origin", "copilot/old-name").returncode == 0
    late = git(other, "rev-parse", "HEAD").strip()
    assert late != agreed

    created = push(work, f"--force-with-lease={NEW_BRANCH}:", str(remote), f"{OLD_BRANCH}:{NEW_BRANCH}")
    assert created.returncode == 0, created.stderr

    refused = push(work, f"--force-with-lease={OLD_BRANCH}:{agreed}", str(remote), "--delete", OLD_BRANCH)
    assert refused.returncode != 0, "a leased delete must refuse a branch that moved"
    assert "stale info" in (refused.stderr + refused.stdout)

    # The late commit and the ref it is on are both still there.
    assert git(remote, "rev-parse", OLD_BRANCH).strip() == late
    assert git(remote, "cat-file", "-t", late).strip() == "commit"
    assert git(remote, "log", "-1", "--format=%s", OLD_BRANCH).strip() == "fix: landed after the snapshot"
    # And the replacement genuinely does not contain it, which is why the
    # refusal matters.
    assert late not in git(remote, "rev-list", NEW_BRANCH)


def test_an_unleashed_delete_would_have_thrown_that_commit_away(remote_pair, tmp_path: Path):
    """Why the lease is required, stated as behaviour rather than as advice."""
    remote, work, _agreed = remote_pair
    other = tmp_path / "other"
    subprocess.run(["git", "clone", "-q", str(remote), str(other)], check=True)
    git(other, "checkout", "-q", "-B", "copilot/old-name", "origin/copilot/old-name")
    commit(other, "fix: landed after the snapshot\n", content="late\n")
    assert push(other, "origin", "copilot/old-name").returncode == 0

    # The form the runbook must NOT offer.
    unleashed = push(work, str(remote), "--delete", OLD_BRANCH)
    assert unleashed.returncode == 0, "this is the behaviour the lease protects against"
    assert OLD_BRANCH not in git(remote, "for-each-ref", "--format=%(refname)")


def test_an_occupied_new_name_makes_the_create_refuse_and_keeps_the_old_branch(remote_pair):
    remote, work, agreed = remote_pair

    # The new name is already taken by something else.
    assert push(work, str(remote), f"refs/heads/main:{NEW_BRANCH}").returncode == 0
    taken = git(remote, "rev-parse", NEW_BRANCH).strip()

    refused = push(work, f"--force-with-lease={NEW_BRANCH}:", str(remote), f"{OLD_BRANCH}:{NEW_BRANCH}")
    assert refused.returncode != 0, "an expected-absence lease must refuse an occupied name"
    assert "stale info" in (refused.stderr + refused.stdout)

    # Nothing was overwritten, and the branch to be renamed is untouched — so
    # the procedure can stop here without having lost anything.
    assert git(remote, "rev-parse", NEW_BRANCH).strip() == taken
    assert git(remote, "rev-parse", OLD_BRANCH).strip() == agreed


def test_the_runbook_leases_the_delete_and_the_create(tmp_path: Path):
    """The text has to describe the behaviour the four tests above pin."""
    text = RUNBOOK.read_text(encoding="utf-8")
    pushes = [line for line in text.splitlines() if "git push" in line]
    deletes = [line for line in pushes if "--delete" in line]
    assert deletes, "the runbook must show how to delete the old name"
    for line in deletes:
        assert "--force-with-lease=refs/heads/" in line, line
        # A lease with an expected SHA, not an empty one: a delete asserts what
        # it expects to find.
        assert re.search(r"--force-with-lease=\S+:[0-9a-f]{40}", line), line

    creates = [line for line in pushes if ":refs/heads/" in line and "--delete" not in line]
    assert creates, "the runbook must show how to create the new name"
    for line in creates:
        # An empty expected value is "expect this ref not to exist".
        assert re.search(r"--force-with-lease=refs/heads/\S*:\s", line + " "), line

    assert "If the create is refused, **stop**" in text
    assert "Do **not**" in text and "read the remote's current value just before deleting" in text
