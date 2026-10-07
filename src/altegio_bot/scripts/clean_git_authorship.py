"""Audit AI authorship metadata in git history, and rewrite it only on request.

What this is for
----------------
Commits in this repository carry three different kinds of machine attribution:
AI co-author and agent-log trailers in messages, bot identities in the author
and committer fields, and agent branch names quoted inside merge subjects. This
module finds all three and reports them. On explicit request it removes the
confirmed ones — in a separate disposable clone, never in a working checkout.

Audit is the default, and the only mode that runs without a flag
-----------------------------------------------------------------
Rewriting git history renumbers every descendant commit, so it is a decision
somebody has to make deliberately and once. :func:`main` therefore reports and
exits unless ``--apply`` is given, and ``--apply`` refuses to touch anything
that is not a bare clone (see :func:`assert_disposable_clone`). Nothing here
pushes, and nothing here talks to a forge.

What it will and will not change
--------------------------------
It removes trailer lines whose identity is a CONFIRMED machine one. It keeps
everything else: the subject, the body, PR numbers, ``Co-authored-by`` lines
naming people, and every functional mention of a tool — a paging cursor, a
dependency, a licence, a ``.gitignore`` rule. There is deliberately no
"replace this string everywhere in the sources" mode: that is how a rewrite
silently edits code.

A bot author or committer is REPORTED but never replaced on a guess. Replacing
one means stating who the person behind it was, which is a fact this module
does not have; it comes from ``--identity-map``, and only identities that
classify as machine ones may appear in that map.

One rule list, two consumers
----------------------------
The same :data:`AI_MESSAGE_RULES` drives the audit preview (applied here, in
Python) and the rewrite (written out as a ``git filter-repo
--replace-message`` file). A rule cannot therefore be reported as one thing and
applied as another, which is what :func:`rules_file_lines` and
:func:`apply_message_rules` are tested against together.
"""

from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Sequence

# ---------------------------------------------------------------------------
# Identities
# ---------------------------------------------------------------------------
# Three classes, and the difference decides what may be rewritten.
#
# ``machine`` is an agent identity: a candidate for replacement, and only ever
# through an explicit map. ``forge`` is the hosting platform committing on a
# person's behalf — a real committer of real merges, never an AI attribution
# and never rewritten. Everything else is ``human`` and is left exactly alone.

MACHINE_IDENTITY_PATTERNS: tuple[tuple[str, str], ...] = (
    ("copilot", r"copilot"),
    ("claude", r"claude"),
    ("anthropic", r"anthropic"),
    ("openai", r"openai"),
    ("chatgpt", r"chatgpt"),
    ("gemini", r"\bgemini\b"),
    ("devin", r"\bdevin\b"),
    ("aider", r"\baider\b"),
    # Deliberately NOT a bare "cursor": in this repository that word is a
    # Chatwoot paging cursor in dozens of commit messages, and a rule matching
    # it would delete real engineering prose.
    ("cursor-agent", r"\bcursor[\s-]*(?:ai|agent|bot)\b"),
    ("codex-agent", r"\bcodex[\s-]*(?:ai|agent|bot)\b"),
)

FORGE_IDENTITY_PATTERNS: tuple[tuple[str, str], ...] = (
    # The host part is matched rather than the exact TLD so that a test double
    # on ``github.invalid`` classifies the same way the real
    # ``noreply@github.com`` does. A personal ``users.noreply.github.com``
    # address does NOT match: the pattern requires the local part to be exactly
    # ``noreply``, which is the platform's own committer, not a person's.
    ("github", r"^github <noreply@github\.[a-z]+>$"),
    ("github-web-flow", r"<noreply@github\.[a-z]+>$"),
)

MACHINE = "machine"
FORGE = "forge"
HUMAN = "human"


def classify_identity(name: str, email: str) -> str:
    """Which of the three classes this ``name <email>`` belongs to.

    Checked against the pair rather than the email alone: an agent may commit
    under a forge no-reply address, and the name is then the only thing that
    says so. The machine check runs FIRST for exactly that reason — a Copilot
    identity at ``users.noreply.github.com`` is a machine identity, not the
    platform.
    """
    subject = f"{name} <{email}>".strip().lower()
    for _label, pattern in MACHINE_IDENTITY_PATTERNS:
        if re.search(pattern, subject):
            return MACHINE
    for _label, pattern in FORGE_IDENTITY_PATTERNS:
        if re.search(pattern, subject):
            return FORGE
    return HUMAN


def machine_identity_label(name: str, email: str) -> str | None:
    """Which agent this identity is, for the audit table, or ``None``."""
    subject = f"{name} <{email}>".strip().lower()
    for label, pattern in MACHINE_IDENTITY_PATTERNS:
        if re.search(pattern, subject):
            return label
    return None


# ---------------------------------------------------------------------------
# Message rules
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MessageRule:
    """One message transformation, in the one form both consumers read.

    ``pattern`` is a Python regular expression and ``replacement`` is a Python
    replacement template, because that is what ``git filter-repo``'s
    ``--replace-message`` compiles. Everything is written with an inline
    ``(?m)``: filter-repo compiles these without flags, so an anchored pattern
    that relied on ``re.MULTILINE`` would silently match nothing — which is a
    rewrite that reports success and changes not one line.
    """

    reason: str
    pattern: str
    replacement: str = ""


# Confirmed machine attribution. Each pattern is anchored to a trailer KEY, so
# the identity substring is only ever looked for inside a line that is already
# an attribution line. "anthropic" appearing in a sentence about a vendor stays.
AI_MESSAGE_RULES: tuple[MessageRule, ...] = (
    MessageRule(
        reason="ai-co-author-trailer",
        pattern=(
            r"(?mi)^co-authored-by:[^\n]*"
            r"(?:claude|anthropic|copilot|openai|chatgpt|gemini|devin|aider"
            r"|cursor[\s-]*(?:ai|agent|bot)|codex[\s-]*(?:ai|agent|bot))"
            r"[^\n]*\n?"
        ),
    ),
    MessageRule(reason="agent-logs-url-trailer", pattern=r"(?mi)^agent-logs-url:[^\n]*\n?"),
    MessageRule(
        reason="generated-with-footer",
        # The robot emoji is written as the CHARACTER, never as a ``\U`` escape.
        # The pattern is serialised into a file that filter-repo compiles as a
        # BYTES regex, where ``\U`` is not a valid escape: the rewrite would die
        # on the rule file rather than quietly skip one rule.
        pattern=(
            "(?mi)^[^\\S\\n]*\U0001f916?[^\\S\\n]*generated with [^\\n]*(?:claude|copilot|chatgpt|openai)[^\\n]*\\n?"
        ),
    ),
    MessageRule(
        reason="assistance-trailer",
        pattern=(
            r"(?mi)^(?:assisted-by|ai-assisted-by|generated-by|authored-by-ai):[^\n]*"
            r"(?:claude|anthropic|copilot|openai|chatgpt|gemini|devin|aider)[^\n]*\n?"
        ),
    ),
)

# Applied after any removal, and only then. Removing a trailer out of the
# middle of a trailer block leaves the blank lines that framed it, so a message
# that was tidy before the rewrite would come out with a hole in it.
TIDY_MESSAGE_RULES: tuple[MessageRule, ...] = (
    MessageRule(reason="collapse-blank-runs", pattern=r"\n{3,}", replacement=r"\n\n"),
    MessageRule(reason="trim-trailing-blanks", pattern=r"\n+\Z", replacement=r"\n"),
)


def merge_slug_rule(prefix: str) -> MessageRule:
    """Drop an agent branch prefix from a ``Merge pull request`` subject.

    Opt-in, and never inferred. A merge subject records the branch a change
    actually came from, so dropping part of that name is a decision about the
    record rather than a cleanup of a trailer — the operator names the prefix,
    and the PR number and the owner are kept by the backreference.
    """
    quoted = re.escape(prefix.strip("/"))
    return MessageRule(
        reason=f"merge-slug:{prefix}",
        pattern=rf"(?m)^(Merge pull request #\d+ from [^/\n]+/){quoted}/",
        replacement=r"\1",
    )


def apply_message_rules(message: str, rules: Sequence[MessageRule]) -> str:
    """Apply *rules* in order, exactly as filter-repo applies the written file."""
    out = message
    for rule in rules:
        out = re.sub(rule.pattern, rule.replacement, out)
    return out


def rules_file_lines(rules: Sequence[MessageRule]) -> list[str]:
    """The ``--replace-message`` file, one ``regex:<pattern>==><replacement>`` per rule.

    No rule may contain a newline in its serialised form: the file is read line
    by line, so a literal newline would split one rule into two unusable ones.
    Patterns therefore spell newlines as the two-character escape ``\\n``,
    which is what :func:`re` reads them as on both sides.
    """
    lines: list[str] = []
    for rule in rules:
        serialised = f"regex:{rule.pattern}==>{rule.replacement}"
        if "\n" in serialised:
            raise ValueError(f"rule {rule.reason} would not survive the line-based rules file")
        lines.append(serialised)
    return lines


def message_rules(*, merge_slug_prefixes: Sequence[str] = ()) -> tuple[MessageRule, ...]:
    """Every rule a rewrite would apply, in application order."""
    rules: list[MessageRule] = list(AI_MESSAGE_RULES)
    rules.extend(merge_slug_rule(prefix) for prefix in merge_slug_prefixes)
    rules.extend(TIDY_MESSAGE_RULES)
    return tuple(rules)


def removal_rules(*, merge_slug_prefixes: Sequence[str] = ()) -> tuple[MessageRule, ...]:
    """The rules that actually remove something, for reporting what matched."""
    return tuple(AI_MESSAGE_RULES) + tuple(merge_slug_rule(prefix) for prefix in merge_slug_prefixes)


# ---------------------------------------------------------------------------
# Git plumbing
# ---------------------------------------------------------------------------

_RECORD = "\x1e"
_FIELD = "\x1f"


class GitError(RuntimeError):
    """A git invocation this module cannot proceed without."""


def run_git(repo: Path, *args: str, check: bool = True) -> str:
    """One git command in *repo*, as text. Never a shell."""
    result = subprocess.run(  # noqa: S603 - fixed executable, argument list
        ["git", "-C", str(repo), *args],
        capture_output=True,
        text=True,
        errors="replace",
    )
    if check and result.returncode != 0:
        raise GitError(f"git {' '.join(args[:2])} failed in {repo}: {result.stderr.strip()[:200]}")
    return result.stdout


def is_bare(repo: Path) -> bool:
    return run_git(repo, "rev-parse", "--is-bare-repository").strip() == "true"


def git_common_dir(repo: Path) -> Path:
    return Path(run_git(repo, "rev-parse", "--path-format=absolute", "--git-common-dir").strip()).resolve()


# Refs a client is not allowed to write. ``git clone --mirror`` from GitHub
# brings down every ``refs/pull/N/head``, and a rewrite with no ref list would
# faithfully renumber all of them — producing hundreds of objects that can
# never be pushed, because the forge owns those refs and rejects writes to
# them. They are audited (they are evidence of what stays reachable) and never
# rewritten.
UNPUBLISHABLE_REF_PREFIXES: tuple[str, ...] = (
    "refs/pull/",
    "refs/merge-requests/",
    "refs/changes/",
)


def is_publishable(ref: str) -> bool:
    return not ref.startswith(UNPUBLISHABLE_REF_PREFIXES)


def default_refs(repo: Path, *, include_unpublishable: bool = False) -> list[str]:
    """The refs a cleanup is actually about: branches and tags, published ones.

    Covers both shapes the tool is pointed at. A working checkout keeps the
    published state under ``refs/remotes``; a ``--mirror`` clone — the only
    place a rewrite may happen — keeps it under ``refs/heads``. Asking for both
    means the same invocation describes the same history in either.

    Forge-owned refs are left out unless asked for: see
    :data:`UNPUBLISHABLE_REF_PREFIXES` for why rewriting them is work nobody
    can publish.
    """
    raw = run_git(
        repo,
        "for-each-ref",
        "--format=%(refname)",
        "refs/heads",
        "refs/remotes",
        "refs/tags",
        "refs/pull",
    ).splitlines()
    refs = []
    for line in raw:
        ref = line.strip()
        if not ref or ref.endswith("/HEAD"):
            continue
        if not include_unpublishable and not is_publishable(ref):
            continue
        refs.append(ref)
    return refs


# ---------------------------------------------------------------------------
# Audit
# ---------------------------------------------------------------------------


@dataclass
class CommitFacts:
    """Everything the audit needs about one commit. PII-free by construction."""

    sha: str
    author_name: str
    author_email: str
    committer_name: str
    committer_email: str
    message: str

    @property
    def author_class(self) -> str:
        return classify_identity(self.author_name, self.author_email)

    @property
    def committer_class(self) -> str:
        return classify_identity(self.committer_name, self.committer_email)


@dataclass
class RefFindings:
    ref: str
    commits: int = 0
    message_hits: dict[str, int] = field(default_factory=dict)
    machine_authors: int = 0
    machine_committers: int = 0

    @property
    def message_total(self) -> int:
        return sum(self.message_hits.values())


@dataclass
class AuditReport:
    """What the audit found. Kept local: it quotes the old attribution."""

    repo: str
    refs: list[RefFindings] = field(default_factory=list)
    identities: dict[str, dict[str, Any]] = field(default_factory=dict)
    trailer_lines: dict[str, int] = field(default_factory=dict)
    merge_slugs: dict[str, int] = field(default_factory=dict)
    agent_named_refs: list[str] = field(default_factory=list)
    signed_commits: int = 0
    total_commits: int = 0
    previews: list[dict[str, str]] = field(default_factory=list)
    unverified: list[str] = field(default_factory=list)

    @property
    def findings(self) -> int:
        return (
            sum(entry.message_total for entry in self.refs)
            + sum(entry.machine_authors + entry.machine_committers for entry in self.refs)
            + sum(self.merge_slugs.values())
        )

    def as_dict(self) -> dict[str, Any]:
        return {
            "repo": self.repo,
            "total_commits": self.total_commits,
            "signed_commits": self.signed_commits,
            "refs": [
                {
                    "ref": entry.ref,
                    "commits": entry.commits,
                    "message_hits": dict(entry.message_hits),
                    "machine_authors": entry.machine_authors,
                    "machine_committers": entry.machine_committers,
                }
                for entry in self.refs
            ],
            "identities": self.identities,
            "trailer_lines": self.trailer_lines,
            "merge_slugs": self.merge_slugs,
            "agent_named_refs": self.agent_named_refs,
            "previews": self.previews,
            "unverified": self.unverified,
        }

    def as_text(self) -> str:
        out: list[str] = []
        out.append(f"repository: {self.repo}")
        out.append(f"commits reachable from the audited refs: {self.total_commits}")
        out.append(f"commits carrying a gpgsig header: {self.signed_commits}")
        out.append("")
        out.append("per-ref findings")
        width = max([len(entry.ref) for entry in self.refs] + [len("ref")]) + 2
        out.append(f"{'ref':<{width}}{'commits':>8}{'messages':>10}{'authors':>9}{'committers':>12}")
        for entry in self.refs:
            out.append(
                f"{entry.ref:<{width}}{entry.commits:>8}{entry.message_total:>10}"
                f"{entry.machine_authors:>9}{entry.machine_committers:>12}"
            )
        out.append("")
        out.append("identities (appearances as author and/or committer)")
        for subject, info in sorted(self.identities.items(), key=lambda item: -item[1]["appearances"]):
            label = f" [{info['agent']}]" if info.get("agent") else ""
            roles = "/".join(info["roles"])
            out.append(f"  {info['class']:<8}{info['appearances']:>6}  {roles:<18}{subject}{label}")
        out.append("")
        out.append("attribution lines found in messages")
        if self.trailer_lines:
            for line, count in sorted(self.trailer_lines.items(), key=lambda item: -item[1]):
                out.append(f"  {count:>4}  {line}")
        else:
            out.append("  (none)")
        out.append("")
        out.append("refs whose own NAME carries an agent prefix")
        if self.agent_named_refs:
            for ref in self.agent_named_refs:
                out.append(f"  {ref}")
            out.append("  (a rename is a separate decision: a message rewrite does not move a ref)")
        else:
            out.append("  (none)")
        out.append("")
        out.append("agent branch prefixes quoted in merge subjects")
        if self.merge_slugs:
            for slug, count in sorted(self.merge_slugs.items(), key=lambda item: -item[1]):
                out.append(f"  {count:>4}  {slug}")
        else:
            out.append("  (none)")
        if self.previews:
            out.append("")
            out.append("sample rewrites")
            for preview in self.previews:
                out.append(f"  {preview['sha']}")
                for line in preview["before"].splitlines():
                    out.append(f"    - {line}")
                for line in preview["after"].splitlines():
                    out.append(f"    + {line}")
        out.append("")
        out.append("NOT verified by this tool")
        for item in self.unverified:
            out.append(f"  - {item}")
        return "\n".join(out) + "\n"


UNVERIFIED_SOURCES: tuple[str, ...] = (
    "pull request titles, bodies, review comments and review threads (no forge API client here)",
    "forge-side refs/pull/*/head and refs/pull/*/merge, which a client cannot rewrite or delete",
    "CI run logs, job summaries and build artifacts",
    "forks, and any clone somebody else already has",
    "release notes, issues, project boards and wiki pages",
    "caches and search indexes of pages that quoted a commit",
)


def read_commits(repo: Path, ref: str) -> list[CommitFacts]:
    """Every commit reachable from *ref*, with the fields the audit reads."""
    fmt = _FIELD.join(["%H", "%an", "%ae", "%cn", "%ce", "%B"]) + _RECORD
    raw = run_git(repo, "log", "--format=" + fmt, ref)
    commits: list[CommitFacts] = []
    for record in raw.split(_RECORD):
        record = record.lstrip("\n")
        if not record.strip():
            continue
        parts = record.split(_FIELD)
        if len(parts) != 6:
            continue
        commits.append(
            CommitFacts(
                sha=parts[0],
                author_name=parts[1],
                author_email=parts[2],
                committer_name=parts[3],
                committer_email=parts[4],
                message=parts[5],
            )
        )
    return commits


def count_signed(repo: Path, shas: Iterable[str]) -> int:
    """How many commits carry a ``gpgsig`` header.

    Worth counting before a rewrite rather than after: filter-repo cannot
    re-sign what it rewrites, so every one of these loses its signature.
    """
    signed = 0
    for sha in shas:
        raw = run_git(repo, "cat-file", "commit", sha, check=False)
        head = raw.split("\n\n", 1)[0]
        if any(line.startswith("gpgsig") for line in head.splitlines()):
            signed += 1
    return signed


MERGE_SLUG_PATTERN = re.compile(r"(?m)^Merge pull request #\d+ from [^/\n]+/([^/\n]+)/")


def audit(
    repo: Path,
    refs: Sequence[str],
    *,
    merge_slug_prefixes: Sequence[str] = (),
    sample: int = 2,
    count_signatures: bool = True,
) -> AuditReport:
    """Read-only. What is there, per ref, per identity, per attribution line."""
    report = AuditReport(repo=str(repo))
    rules = removal_rules(merge_slug_prefixes=merge_slug_prefixes)
    full_rules = message_rules(merge_slug_prefixes=merge_slug_prefixes)
    seen: dict[str, CommitFacts] = {}

    for ref in refs:
        findings = RefFindings(ref=ref)
        for commit in read_commits(repo, ref):
            findings.commits += 1
            seen.setdefault(commit.sha, commit)
            for rule in rules:
                if re.search(rule.pattern, commit.message):
                    findings.message_hits[rule.reason] = findings.message_hits.get(rule.reason, 0) + 1
            if commit.author_class == MACHINE:
                findings.machine_authors += 1
            if commit.committer_class == MACHINE:
                findings.machine_committers += 1
        report.refs.append(findings)

    report.total_commits = len(seen)

    for commit in seen.values():
        for name, email, role in (
            (commit.author_name, commit.author_email, "author"),
            (commit.committer_name, commit.committer_email, "committer"),
        ):
            subject = f"{name} <{email}>"
            entry = report.identities.setdefault(
                subject,
                {
                    "class": classify_identity(name, email),
                    "agent": machine_identity_label(name, email),
                    # How many author/committer SLOTS this identity fills, not
                    # how many commits it appears in: one commit it both wrote
                    # and committed counts twice, which is what makes the sum
                    # add up to two per commit.
                    "appearances": 0,
                    "roles": [],
                },
            )
            entry["appearances"] += 1
            if role not in entry["roles"]:
                entry["roles"].append(role)

        for rule in rules:
            for match in re.finditer(rule.pattern, commit.message):
                line = match.group(0).strip()
                if line:
                    report.trailer_lines[line] = report.trailer_lines.get(line, 0) + 1

        for match in MERGE_SLUG_PATTERN.finditer(commit.message):
            slug = match.group(1)
            if classify_identity(slug, "") == MACHINE:
                report.merge_slugs[slug] = report.merge_slugs.get(slug, 0) + 1

        if len(report.previews) < sample:
            after = apply_message_rules(commit.message, full_rules)
            if after != commit.message:
                report.previews.append(
                    {
                        "sha": commit.sha[:12],
                        "before": commit.message.strip(),
                        "after": after.strip(),
                    }
                )

    # A ref can carry an agent name itself, and no message rule reaches that:
    # renaming a branch is a push and a delete, not a rewrite.
    for ref in refs:
        for segment in ref.split("/"):
            if classify_identity(segment, "") == MACHINE:
                report.agent_named_refs.append(ref)
                break

    if count_signatures:
        report.signed_commits = count_signed(repo, seen)
    report.unverified = list(UNVERIFIED_SOURCES)
    return report


# ---------------------------------------------------------------------------
# Identity map
# ---------------------------------------------------------------------------


def load_identity_map(path: Path) -> dict[str, dict[str, str]]:
    """``{"Bot Name <bot@example>": {"name": ..., "email": ...}}``, validated.

    Every key must classify as a machine identity. The point of the map is to
    say who a person was behind an agent account, and a map that could name a
    human on the left would be a tool for rewriting people's attribution.
    """
    raw = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(raw, dict) or not raw:
        raise ValueError("identity map must be a non-empty JSON object")
    parsed: dict[str, dict[str, str]] = {}
    for subject, replacement in raw.items():
        name, _, email = str(subject).partition("<")
        email = email.rstrip(">")
        if classify_identity(name.strip(), email.strip()) != MACHINE:
            raise ValueError(f"identity map key is not a machine identity: {subject}")
        if not isinstance(replacement, dict) or not replacement.get("name") or not replacement.get("email"):
            raise ValueError(f"identity map entry for {subject} needs both a name and an email")
        if classify_identity(str(replacement["name"]), str(replacement["email"])) == MACHINE:
            raise ValueError(f"identity map for {subject} maps one machine identity onto another")
        parsed[str(subject)] = {"name": str(replacement["name"]), "email": str(replacement["email"])}
    return parsed


def mailmap_lines(identity_map: dict[str, dict[str, str]]) -> list[str]:
    """git mailmap entries, in the four-field form that matches name AND email."""
    lines: list[str] = []
    for subject, replacement in sorted(identity_map.items()):
        name, _, email = subject.partition("<")
        email = email.rstrip(">")
        lines.append(f"{replacement['name']} <{replacement['email']}> {name.strip()} <{email.strip()}>")
    return lines


# ---------------------------------------------------------------------------
# Rewrite
# ---------------------------------------------------------------------------


class RefusedError(RuntimeError):
    """A rewrite this module will not perform."""


def assert_disposable_clone(repo: Path, *, own_repo: Path | None = None) -> None:
    """Refuse anything that is not a throwaway bare clone.

    Two checks, and they close different holes. A bare repository has no
    working tree and no branch anybody is sitting on, which is what makes
    ``git clone --mirror`` the right place for a rewrite and a normal checkout
    the wrong one. The identity check then stops the obvious accident: pointing
    the tool at the repository it was itself read out of.
    """
    if not (repo / "HEAD").exists() and not (repo / ".git").exists():
        raise RefusedError(f"{repo} is not a git repository")
    if not is_bare(repo):
        raise RefusedError(
            f"{repo} is a working checkout; rewrite only in a disposable bare clone (git clone --mirror <url> <path>)"
        )
    reference = own_repo if own_repo is not None else Path(__file__).resolve().parents[3]
    try:
        own_common = git_common_dir(reference)
    except GitError:
        own_common = None
    if own_common is not None and git_common_dir(repo) == own_common:
        raise RefusedError("refusing to rewrite the repository this tool was loaded from")
    if shutil.which("git-filter-repo") is None:
        raise RefusedError("git-filter-repo is not on PATH")


def write_inputs(
    workdir: Path,
    *,
    rules: Sequence[MessageRule],
    identity_map: dict[str, dict[str, str]],
) -> tuple[Path, Path | None]:
    """The two declarative files filter-repo is driven with. Auditable on disk."""
    workdir.mkdir(parents=True, exist_ok=True)
    rules_path = workdir / "replace-message.txt"
    rules_path.write_text("\n".join(rules_file_lines(rules)) + "\n", encoding="utf-8")
    mailmap_path: Path | None = None
    if identity_map:
        mailmap_path = workdir / "mailmap.txt"
        mailmap_path.write_text("\n".join(mailmap_lines(identity_map)) + "\n", encoding="utf-8")
    return rules_path, mailmap_path


def filter_repo_command(
    *,
    rules_path: Path,
    mailmap_path: Path | None,
    refs: Sequence[str],
) -> list[str]:
    """The exact command, so a runbook and a test read the same one.

    ``--replace-message`` and ``--mailmap`` are both declarative files rather
    than callback code: the operator can read what is about to happen, and
    there is no generated Python in the middle to get the indentation of a
    callback body wrong.
    """
    command = ["git-filter-repo", "--force", "--replace-message", str(rules_path)]
    if mailmap_path is not None:
        command += ["--mailmap", str(mailmap_path)]
    if refs:
        command += ["--refs", *refs]
    return command


def rewrite(
    repo: Path,
    *,
    refs: Sequence[str],
    rules: Sequence[MessageRule],
    identity_map: dict[str, dict[str, str]],
    workdir: Path,
    own_repo: Path | None = None,
) -> dict[str, Any]:
    """Run the rewrite in *repo*. Guarded, local, and it never pushes."""
    assert_disposable_clone(repo, own_repo=own_repo)
    forge_owned = [ref for ref in refs if not is_publishable(ref)]
    if forge_owned:
        raise RefusedError(
            "these refs belong to the forge and cannot be pushed back, so rewriting them "
            f"would produce unpublishable objects: {', '.join(forge_owned[:5])}"
        )
    rules_path, mailmap_path = write_inputs(workdir, rules=rules, identity_map=identity_map)
    command = filter_repo_command(rules_path=rules_path, mailmap_path=mailmap_path, refs=refs)
    result = subprocess.run(  # noqa: S603 - fixed executable, argument list
        command,
        cwd=str(repo),
        capture_output=True,
        text=True,
        errors="replace",
    )
    if result.returncode != 0:
        raise GitError(f"filter-repo failed: {result.stderr.strip()[:400]}")
    return {
        "command": command,
        "rules_file": str(rules_path),
        "mailmap_file": str(mailmap_path) if mailmap_path else None,
        "commit_map": str(repo / "filter-repo" / "commit-map"),
        "stdout_tail": result.stdout.strip().splitlines()[-3:],
    }


# ---------------------------------------------------------------------------
# Verification
# ---------------------------------------------------------------------------

_ZERO = "0" * 40


def read_commit_map(repo: Path) -> list[tuple[str, str]]:
    """``old new`` pairs filter-repo recorded, excluding its header line."""
    for candidate in (repo / "filter-repo" / "commit-map", repo / ".git" / "filter-repo" / "commit-map"):
        if candidate.exists():
            pairs: list[tuple[str, str]] = []
            for line in candidate.read_text(encoding="utf-8").splitlines():
                parts = line.split()
                if len(parts) == 2 and len(parts[0]) == 40 and parts[0] != "old":
                    pairs.append((parts[0], parts[1]))
            return pairs
    raise GitError(f"no commit-map under {repo}; was a rewrite run here?")


def _commit_header(repo: Path, sha: str) -> dict[str, Any]:
    raw = run_git(repo, "cat-file", "commit", sha)
    head = raw.split("\n\n", 1)[0]
    parents: list[str] = []
    fields: dict[str, Any] = {"parents": parents, "signed": False}
    for line in head.splitlines():
        key, _, value = line.partition(" ")
        if key == "tree":
            fields["tree"] = value
        elif key == "parent":
            parents.append(value)
        elif key in ("author", "committer"):
            # "Name <email> 1700000000 +0200" — the timestamp and the offset are
            # the part a rewrite must not move.
            pieces = value.rsplit(" ", 2)
            fields[f"{key}_date"] = " ".join(pieces[-2:]) if len(pieces) == 3 else ""
            fields[key] = pieces[0]
        elif key.startswith("gpgsig"):
            fields["signed"] = True
    return fields


@dataclass
class VerifyReport:
    pairs: int = 0
    dropped: list[str] = field(default_factory=list)
    tree_mismatch: list[str] = field(default_factory=list)
    author_date_mismatch: list[str] = field(default_factory=list)
    committer_date_mismatch: list[str] = field(default_factory=list)
    parent_mismatch: list[str] = field(default_factory=list)
    signatures_lost: list[str] = field(default_factory=list)
    ref_count_mismatch: list[str] = field(default_factory=list)
    residual_findings: int = 0

    @property
    def ok(self) -> bool:
        return not (
            self.dropped
            or self.tree_mismatch
            or self.author_date_mismatch
            or self.committer_date_mismatch
            or self.parent_mismatch
            or self.ref_count_mismatch
        )

    def as_text(self) -> str:
        out = [
            f"mapped commits:            {self.pairs}",
            f"dropped commits:           {len(self.dropped)}",
            f"tree mismatches:           {len(self.tree_mismatch)}",
            f"author date mismatches:    {len(self.author_date_mismatch)}",
            f"committer date mismatches: {len(self.committer_date_mismatch)}",
            f"parent structure changes:  {len(self.parent_mismatch)}",
            f"ref commit count changes:  {len(self.ref_count_mismatch)}",
            f"signatures no longer present: {len(self.signatures_lost)}",
            f"residual attribution findings: {self.residual_findings}",
            f"verdict: {'PASS' if self.ok else 'FAIL'}",
        ]
        for label, items in (
            ("dropped", self.dropped),
            ("tree", self.tree_mismatch),
            ("author-date", self.author_date_mismatch),
            ("committer-date", self.committer_date_mismatch),
            ("parents", self.parent_mismatch),
            ("ref-counts", self.ref_count_mismatch),
        ):
            for item in items[:10]:
                out.append(f"  {label}: {item}")
        return "\n".join(out) + "\n"


def verify(original: Path, rewritten: Path, *, refs: Sequence[str] = ()) -> VerifyReport:
    """Compare every mapped commit, and the shape of the history around it.

    What a message-only rewrite is allowed to change is the message. So this
    asserts the tree is byte-identical, both dates and both offsets are
    unchanged, the parent list maps one-to-one through the commit map, no
    commit was dropped, and each audited ref still holds the same number of
    commits. Signatures are reported rather than asserted: they cannot survive,
    and pretending otherwise would be the one dishonest line in the report.
    """
    report = VerifyReport()
    mapping = dict(read_commit_map(rewritten))
    for old, new in mapping.items():
        report.pairs += 1
        if new == _ZERO:
            report.dropped.append(old)
            continue
        before = _commit_header(original, old)
        after = _commit_header(rewritten, new)
        if before.get("tree") != after.get("tree"):
            report.tree_mismatch.append(f"{old} -> {new}")
        if before.get("author_date") != after.get("author_date"):
            report.author_date_mismatch.append(f"{old} -> {new}")
        if before.get("committer_date") != after.get("committer_date"):
            report.committer_date_mismatch.append(f"{old} -> {new}")
        expected_parents = [mapping.get(parent, parent) for parent in before["parents"]]
        if expected_parents != after["parents"]:
            report.parent_mismatch.append(f"{old} -> {new}")
        if before.get("signed") and not after.get("signed"):
            report.signatures_lost.append(f"{old} -> {new}")

    for ref in refs:
        local = ref.split("refs/remotes/", 1)[-1].split("/", 1)[-1] if ref.startswith("refs/remotes/") else ref
        for candidate in (ref, local, f"refs/heads/{local}"):
            try:
                after_count = int(run_git(rewritten, "rev-list", "--count", candidate).strip())
            except GitError:
                continue
            before_count = int(run_git(original, "rev-list", "--count", ref).strip())
            if before_count != after_count:
                report.ref_count_mismatch.append(f"{ref}: {before_count} -> {after_count}")
            break
    return report


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="clean_git_authorship",
        description="Audit AI authorship metadata in git history; rewrite only with --apply.",
    )
    parser.add_argument("--repo", default=".", help="repository to read (default: the current one)")
    parser.add_argument("--refs", nargs="*", default=None, help="refs to audit or rewrite (default: remotes and tags)")
    parser.add_argument("--json", action="store_true", help="machine-readable audit output")
    parser.add_argument("--sample", type=int, default=2, help="how many before/after previews to print")
    parser.add_argument(
        "--no-signature-scan", action="store_true", help="skip the gpgsig census (it reads every commit)"
    )
    parser.add_argument(
        "--merge-slug-prefix",
        action="append",
        default=[],
        metavar="PREFIX",
        help="also drop this agent branch prefix from merge subjects (opt-in, repeatable)",
    )
    parser.add_argument(
        "--include-unpublishable-refs",
        action="store_true",
        help="also audit forge-owned refs such as refs/pull/* (they can be read, never rewritten)",
    )
    parser.add_argument("--identity-map", type=Path, help="JSON map replacing bot author/committer identities")
    parser.add_argument("--apply", action="store_true", help="rewrite; refuses outside a disposable bare clone")
    parser.add_argument("--workdir", type=Path, help="where to write the generated filter-repo inputs")
    parser.add_argument("--verify-against", type=Path, help="verify --repo as a rewrite of this original repository")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    repo = Path(args.repo).resolve()

    try:
        refs = (
            list(args.refs) if args.refs else default_refs(repo, include_unpublishable=args.include_unpublishable_refs)
        )
    except GitError as error:
        print(f"refused: {error}", file=sys.stderr)
        return 2

    if args.verify_against is not None:
        try:
            report = verify(Path(args.verify_against).resolve(), repo, refs=refs)
        except GitError as error:
            print(f"refused: {error}", file=sys.stderr)
            return 2
        residual = audit(repo, refs, merge_slug_prefixes=args.merge_slug_prefix, sample=0, count_signatures=False)
        report.residual_findings = residual.findings
        sys.stdout.write(report.as_text())
        return 0 if report.ok else 1

    identity_map: dict[str, dict[str, str]] = {}
    if args.identity_map is not None:
        try:
            identity_map = load_identity_map(args.identity_map)
        except (OSError, ValueError, json.JSONDecodeError) as error:
            print(f"refused: {error}", file=sys.stderr)
            return 2

    if not args.apply:
        report = audit(
            repo,
            refs,
            merge_slug_prefixes=args.merge_slug_prefix,
            sample=args.sample,
            count_signatures=not args.no_signature_scan,
        )
        if args.json:
            print(json.dumps(report.as_dict(), indent=2, ensure_ascii=False))
        else:
            sys.stdout.write(report.as_text())
            if identity_map:
                print("")
                print("identity map accepted; these mailmap lines would be used:")
                for line in mailmap_lines(identity_map):
                    print(f"  {line}")
            print("")
            print("audit only. Nothing was changed, and nothing was pushed.")
            print("To rewrite, clone --mirror to a throwaway path and pass --apply there.")
        return 0

    rules = message_rules(merge_slug_prefixes=args.merge_slug_prefix)
    refs = [ref for ref in refs if is_publishable(ref)] if not args.refs else refs
    workdir = (args.workdir or (repo.parent / f"{repo.name}.filter-inputs")).resolve()
    try:
        outcome = rewrite(
            repo,
            refs=refs,
            rules=rules,
            identity_map=identity_map,
            workdir=workdir,
        )
    except (RefusedError, GitError) as error:
        print(f"refused: {error}", file=sys.stderr)
        return 2

    print("rewrite finished in the clone. Nothing was pushed.")
    print(f"  command:    {' '.join(outcome['command'])}")
    print(f"  rules:      {outcome['rules_file']}")
    if outcome["mailmap_file"]:
        print(f"  mailmap:    {outcome['mailmap_file']}")
    print(f"  commit map: {outcome['commit_map']}")
    print("Next: verify with --verify-against <original repo>, then publish per the runbook.")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
