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

One transformation, two consumers
---------------------------------
:func:`transform_message` is the only implementation. The audit preview calls
it directly; the rewrite calls it through a ``git filter-repo
--message-callback`` that is two statements long. An earlier version described
the rules in a ``--replace-message`` file and re-implemented them in Python for
the preview, and the two drifted — a pattern that was optional in a str regex
turned out to be mandatory in the bytes regex filter-repo compiles, so a footer
disappeared from the preview and survived in the commit. One function removes
that whole class of bug.

Identity replacement is still a declarative ``--mailmap`` file, because that
genuinely is a mapping and filter-repo's own handling of it is the behaviour to
keep.
"""

from __future__ import annotations

import argparse
import json
import os
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


@dataclass(frozen=True)
class MachineIdentity:
    """One CONFIRMED machine identity signature.

    Both halves are full matches, case-insensitively, and both must match when
    both are given. Full matches, not substrings: the reviewed version searched
    for ``claude`` and ``devin`` anywhere in ``name <email>``, which classified
    ``Jean-Claude Dupont <jcd@example.org>`` and ``Devin Smith
    <devin@example.org>`` as bots and deleted their co-author lines.

    A signature is added here only when it is a convention a person cannot
    hold: the GitHub ``[bot]`` account suffix, or an agent vendor's own
    no-reply address. Anything that merely LOOKS like an agent is
    :data:`AMBIGUOUS` — the audit shows it, and nothing removes or replaces it.
    """

    label: str
    name_pattern: str | None = None
    email_pattern: str | None = None

    def matches(self, name: str, email: str) -> bool:
        if self.name_pattern is not None and not re.fullmatch(self.name_pattern, name.strip(), re.IGNORECASE):
            return False
        if self.email_pattern is not None and not re.fullmatch(self.email_pattern, email.strip(), re.IGNORECASE):
            return False
        return self.name_pattern is not None or self.email_pattern is not None


# The confirmed set. Short on purpose — this is the list that authorises
# deletion, so every entry has to be a fact rather than a resemblance.
MACHINE_IDENTITIES: tuple[MachineIdentity, ...] = (
    # GitHub appends "[bot]" to the display name of every GitHub App account,
    # and a user account cannot commit under that shape. This is what covers
    # `copilot-swe-agent[bot]` without naming any product.
    MachineIdentity(label="github-app-bot", name_pattern=r".*\[bot\]"),
    # Claude Code's documented co-author address. The DOMAIN is the evidence;
    # the display name beside it varies by model and is not relied on.
    MachineIdentity(label="claude", email_pattern=r"noreply@anthropic\.(?:com|invalid)"),
    # Copilot's commit identity, whatever display name it carries.
    MachineIdentity(label="copilot", email_pattern=r"\d+\+copilot@users\.noreply\.github\.(?:com|invalid)"),
    # Other agents' own no-reply addresses, kept as addresses for the same
    # reason. A product NAME alone never qualifies.
    MachineIdentity(label="openai", email_pattern=r"noreply@openai\.(?:com|invalid)"),
    MachineIdentity(label="cursor-agent", email_pattern=r"(?:noreply|agent|bot)@cursor\.(?:com|sh|invalid)"),
    MachineIdentity(label="devin", email_pattern=r"(?:noreply|devin)@cognition(?:-labs)?\.(?:ai|com|invalid)"),
)

# Resemblances. These authorise nothing: they exist so an audit can say "look
# at this one yourself" instead of either hiding it or deleting it.
AMBIGUOUS_IDENTITY_TOKENS: tuple[str, ...] = (
    "claude",
    "anthropic",
    "copilot",
    "openai",
    "chatgpt",
    "gemini",
    "devin",
    "aider",
    "codex",
    "cursor",
)

FORGE_IDENTITIES: tuple[MachineIdentity, ...] = (
    # The platform committing on a person's behalf: the real committer of every
    # merge made through the web UI. Never an AI attribution, never rewritten.
    # A personal `users.noreply.github.com` address is NOT this: the local part
    # must be exactly `noreply`.
    MachineIdentity(label="github", email_pattern=r"noreply@github\.(?:com|invalid)"),
    MachineIdentity(label="github-web-flow", email_pattern=r"noreply@users\.noreply\.github\.(?:com|invalid)"),
)

MACHINE = "machine"
AMBIGUOUS = "ambiguous"
FORGE = "forge"
HUMAN = "human"

# Tokens that mark a BRANCH name as an agent's. Reporting only, and separate
# from identity classification on purpose: a ref name has no email to confirm
# anything with, so this can never be evidence that something may be deleted.
# The merge-slug rewrite stays opt-in with an operator-supplied prefix.
AGENT_BRANCH_TOKENS: tuple[str, ...] = AMBIGUOUS_IDENTITY_TOKENS


def classify_identity(name: str, email: str) -> str:
    """Which class this ``name <email>`` belongs to.

    Four answers, and only one of them authorises anything. ``machine`` is a
    confirmed agent signature and is the only class an identity map may name or
    a trailer rule may delete. ``ambiguous`` looks like an agent and is not
    proven to be one — it is reported and otherwise left completely alone.
    ``forge`` is the platform, and ``human`` is everybody else.
    """
    for identity in MACHINE_IDENTITIES:
        if identity.matches(name, email):
            return MACHINE
    for identity in FORGE_IDENTITIES:
        if identity.matches(name, email):
            return FORGE
    subject = f"{name} <{email}>".lower()
    if any(token in subject for token in AMBIGUOUS_IDENTITY_TOKENS):
        return AMBIGUOUS
    return HUMAN


def machine_identity_label(name: str, email: str) -> str | None:
    """Which confirmed agent this identity is, for the audit table, or ``None``."""
    for identity in MACHINE_IDENTITIES:
        if identity.matches(name, email):
            return identity.label
    return None


def is_agent_branch_name(segment: str) -> bool:
    """Whether a ref or branch SEGMENT reads as an agent's. Reporting only."""
    lowered = segment.strip().lower()
    return any(lowered == token or lowered.startswith(f"{token}-") for token in AGENT_BRANCH_TOKENS)


# ---------------------------------------------------------------------------
# Message transformation
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MessageRule:
    """One regex transformation of a message.

    ``triggers_tidy`` says whether this rule can leave a hole that the tidy
    pass has to close. Removing a trailer out of the middle of a trailer block
    does; editing a word inside a subject does not, and must not drag an
    unrelated blank line along with it.
    """

    reason: str
    pattern: str
    replacement: str = ""
    triggers_tidy: bool = True


# Trailer keys whose VALUE is an identity. These are not matched by searching
# the line for a vendor word — the identity is parsed out and classified, so a
# human co-author named Jean-Claude keeps their line and a confirmed agent
# loses it.
IDENTITY_TRAILER_KEYS: tuple[str, ...] = (
    "co-authored-by",
    "assisted-by",
    "ai-assisted-by",
    "generated-by",
    "authored-by-ai",
)

_IDENTITY_TRAILER = re.compile(
    r"^(?P<key>" + "|".join(IDENTITY_TRAILER_KEYS) + r"):\s*(?P<name>[^<]*)<(?P<email>[^>]*)>\s*$",
    re.IGNORECASE,
)

IDENTITY_TRAILER_REASON = "machine-identity-trailer"

# Rules with no identity in them: the key alone is the evidence, or the value
# names an agent product rather than a person.
AI_MESSAGE_RULES: tuple[MessageRule, ...] = (
    MessageRule(reason="agent-logs-url-trailer", pattern=r"(?mi)^agent-logs-url:[^\n]*\n?"),
    MessageRule(
        reason="generated-with-footer",
        # The emoji is wrapped in a group so that the WHOLE character is
        # optional. Written as ``\U0001f916?`` it was optional only in a str
        # regex; filter-repo compiles bytes, where the emoji is four bytes and
        # ``?`` applied to the last one alone — so a footer without the emoji
        # vanished from the preview and survived the rewrite.
        pattern=(
            "(?mi)^[^\\S\\n]*(?:\U0001f916)?[^\\S\\n]*"
            "generated with [^\\n]*(?:claude|copilot|chatgpt|openai)[^\\n]*\\n?"
        ),
    ),
)

# Applied only after a removal actually fired, and only one that can leave a
# hole. A message with no attribution in it comes back byte for byte.
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

    ``triggers_tidy`` is false: this edits a word inside the subject and leaves
    no blank-line damage, so it must not normalise anything else in the body.
    """
    quoted = re.escape(prefix.strip("/"))
    return MessageRule(
        reason=f"merge-slug:{prefix}",
        pattern=rf"(?m)^(Merge pull request #\d+ from [^/\n]+/){quoted}/",
        replacement=r"\1",
        triggers_tidy=False,
    )


@dataclass(frozen=True)
class MessageChange:
    """The result of transforming one message, and what fired."""

    text: str
    removals: tuple[str, ...] = ()

    @property
    def changed(self) -> bool:
        return bool(self.removals)


def _strip_machine_identity_trailers(message: str) -> tuple[str, int]:
    """Drop attribution trailers whose identity is a CONFIRMED machine one."""
    kept: list[str] = []
    removed = 0
    for line in message.split("\n"):
        match = _IDENTITY_TRAILER.match(line.strip())
        if match is not None:
            name = match.group("name").strip()
            email = match.group("email").strip()
            if classify_identity(name, email) == MACHINE:
                removed += 1
                continue
        kept.append(line)
    return "\n".join(kept), removed


def transform_message(message: str, *, merge_slug_prefixes: Sequence[str] = ()) -> MessageChange:
    """The one transformation, used by the preview AND by the rewrite.

    There is deliberately no second implementation. The reviewed version
    described the rules in a ``--replace-message`` file for filter-repo and
    re-applied them in Python for the audit, and the two drifted: one rule was
    optional-in-str and mandatory-in-bytes, so the preview and the rewritten
    commit disagreed. The rewrite now calls this function through a
    filter-repo message callback, so a divergence of that kind cannot exist.
    """
    fired: list[str] = []
    tidy_needed = False

    text, removed = _strip_machine_identity_trailers(message)
    if removed:
        fired.append(IDENTITY_TRAILER_REASON)
        tidy_needed = True

    for rule in AI_MESSAGE_RULES:
        text, count = re.subn(rule.pattern, rule.replacement, text)
        if count:
            fired.append(rule.reason)
            tidy_needed = tidy_needed or rule.triggers_tidy

    for prefix in merge_slug_prefixes:
        rule = merge_slug_rule(prefix)
        text, count = re.subn(rule.pattern, rule.replacement, text)
        if count:
            fired.append(rule.reason)
            tidy_needed = tidy_needed or rule.triggers_tidy

    if not fired:
        # Byte for byte. Nothing was attributed to a machine, so nothing about
        # this message is this tool's business — not its blank lines either.
        return MessageChange(text=message)

    if tidy_needed:
        for rule in TIDY_MESSAGE_RULES:
            text = re.sub(rule.pattern, rule.replacement, text)

    return MessageChange(text=text, removals=tuple(fired))


def transform_message_bytes(message: bytes) -> bytes:
    """The callback filter-repo calls, in bytes, delegating to the one function.

    Configuration arrives through the environment rather than as an argument
    because filter-repo owns the call site. A message that is not valid UTF-8
    is returned untouched: guessing an encoding in order to edit somebody's
    commit message is not something this tool should do.
    """
    prefixes = tuple(part for part in os.environ.get(MERGE_SLUG_ENV, "").split(",") if part)
    try:
        decoded = message.decode("utf-8")
    except UnicodeDecodeError:
        return message
    change = transform_message(decoded, merge_slug_prefixes=prefixes)
    return change.text.encode("utf-8")


def removal_reasons(*, merge_slug_prefixes: Sequence[str] = ()) -> tuple[str, ...]:
    """Every reason label a removal can report, for the audit's counters."""
    return (
        (IDENTITY_TRAILER_REASON,)
        + tuple(rule.reason for rule in AI_MESSAGE_RULES)
        + tuple(merge_slug_rule(prefix).reason for prefix in merge_slug_prefixes)
    )


# ---------------------------------------------------------------------------
# Git plumbing
# ---------------------------------------------------------------------------

# How the merge-slug configuration reaches the filter-repo callback. An
# environment variable rather than an argument: filter-repo owns the call site,
# so the callback cannot be handed parameters.
MERGE_SLUG_ENV = "ALTEGIO_AUTHORSHIP_MERGE_SLUG_PREFIXES"

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
    # Resemblances, counted apart from confirmations. Nothing acts on these;
    # they are here so an operator can look at them rather than discover later
    # that something looked at them for him.
    ambiguous_authors: int = 0
    ambiguous_committers: int = 0

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
                    "ambiguous_authors": entry.ambiguous_authors,
                    "ambiguous_committers": entry.ambiguous_committers,
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
    seen: dict[str, CommitFacts] = {}

    for ref in refs:
        findings = RefFindings(ref=ref)
        for commit in read_commits(repo, ref):
            findings.commits += 1
            seen.setdefault(commit.sha, commit)
            # The same function the rewrite calls, so what is counted here is
            # what would actually be removed.
            for reason in transform_message(commit.message, merge_slug_prefixes=merge_slug_prefixes).removals:
                findings.message_hits[reason] = findings.message_hits.get(reason, 0) + 1
            if commit.author_class == MACHINE:
                findings.machine_authors += 1
            if commit.committer_class == MACHINE:
                findings.machine_committers += 1
            if commit.author_class == AMBIGUOUS:
                findings.ambiguous_authors += 1
            if commit.committer_class == AMBIGUOUS:
                findings.ambiguous_committers += 1
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

        change = transform_message(commit.message, merge_slug_prefixes=merge_slug_prefixes)
        if change.changed:
            # Exactly the lines this tool would take out, quoted so a reader
            # can check each one before authorising anything.
            removed = set(commit.message.split("\n")) - set(change.text.split("\n"))
            for line in removed:
                stripped = line.strip()
                if stripped:
                    report.trailer_lines[stripped] = report.trailer_lines.get(stripped, 0) + 1

        for match in MERGE_SLUG_PATTERN.finditer(commit.message):
            slug = match.group(1)
            if is_agent_branch_name(slug):
                report.merge_slugs[slug] = report.merge_slugs.get(slug, 0) + 1

        if len(report.previews) < sample:
            after = change.text
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
            if is_agent_branch_name(segment):
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
    # Raises with the dependency named if it is missing.
    filter_repo_executable()


def filter_repo_executable() -> list[str]:
    """How to invoke filter-repo, or a refusal naming the missing dependency.

    Prefers the console script the locked ``git-filter-repo`` dev dependency
    installs, and falls back to running the module with this interpreter so
    that a plain ``pytest`` outside ``uv run`` still works. It never silently
    gives up: a missing dependency is a clear error, not a skipped test.
    """
    found = shutil.which("git-filter-repo")
    if found:
        return [found]
    try:
        import git_filter_repo  # noqa: F401
    except ImportError as error:
        raise RefusedError(
            "git-filter-repo is not available; it is a locked dev dependency of this project — run `uv sync --frozen`"
        ) from error
    return [sys.executable, "-m", "git_filter_repo"]


DRIVER_FLAG = "--internal-filter-driver"


def write_inputs(
    workdir: Path,
    *,
    merge_slug_prefixes: Sequence[str],
    identity_map: dict[str, dict[str, str]],
) -> tuple[Path, Path | None]:
    """What the rewrite will do, written down so an operator can read it first.

    The message transformation is no longer a ``--replace-message`` file. It
    could not be: "tidy the blank lines, but only where something was actually
    removed" is not expressible as a list of unconditional substitutions, and
    describing the rules twice — once for filter-repo and once for the preview
    — is what let the two drift apart. So messages go through
    :func:`transform_message` in a filter-repo callback, and this file is the
    human-readable record of the configuration rather than its implementation.

    The mailmap stays a real ``--mailmap`` file: identity replacement IS a
    declarative mapping, and filter-repo's own handling of it is the behaviour
    to keep.
    """
    workdir.mkdir(parents=True, exist_ok=True)
    plan_path = workdir / "message-plan.txt"
    lines = [
        "# What the rewrite will do to commit messages.",
        "# Applied by altegio_bot.scripts.clean_git_authorship.transform_message,",
        "# through a git-filter-repo --message-callback. The audit preview calls",
        "# the same function, so this list cannot describe one thing and the",
        "# rewrite do another.",
        "",
        f"remove attribution trailers whose identity is confirmed machine: {IDENTITY_TRAILER_REASON}",
        "  keys: " + ", ".join(IDENTITY_TRAILER_KEYS),
        "  confirmed machine signatures: " + ", ".join(identity.label for identity in MACHINE_IDENTITIES),
    ]
    for rule in AI_MESSAGE_RULES:
        lines.append(f"remove by pattern: {rule.reason}")
        lines.append(f"  regex: {rule.pattern}")
    for prefix in merge_slug_prefixes:
        rule = merge_slug_rule(prefix)
        lines.append(f"rewrite merge subject (opt-in): {rule.reason}")
        lines.append(f"  regex: {rule.pattern} -> {rule.replacement}")
        lines.append("  does NOT trigger blank-line tidying")
    lines.append("")
    lines.append("tidy, ONLY in a message a removal above actually changed:")
    for rule in TIDY_MESSAGE_RULES:
        lines.append(f"  {rule.reason}: {rule.pattern} -> {rule.replacement}")
    lines.append("")
    lines.append("a message nothing was removed from is left byte for byte")
    plan_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

    mailmap_path: Path | None = None
    if identity_map:
        mailmap_path = workdir / "mailmap.txt"
        mailmap_path.write_text("\n".join(mailmap_lines(identity_map)) + "\n", encoding="utf-8")
    return plan_path, mailmap_path


def filter_repo_command(
    *,
    mailmap_path: Path | None,
    refs: Sequence[str],
) -> list[str]:
    """The exact command, so a runbook and a test read the same one.

    The message callback delegates straight to :func:`transform_message_bytes`,
    which is the function the preview uses. Nothing about the rules is encoded
    in this string: the callback is two statements long on purpose, because a
    generated callback body is a second implementation waiting to drift.
    """
    callback = "import altegio_bot.scripts.clean_git_authorship as a; return a.transform_message_bytes(message)"
    command = [*filter_repo_executable(), "--force", "--message-callback", callback]
    if mailmap_path is not None:
        command += ["--mailmap", str(mailmap_path)]
    if refs:
        command += ["--refs", *refs]
    return command


def rewrite(
    repo: Path,
    *,
    refs: Sequence[str],
    merge_slug_prefixes: Sequence[str] = (),
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
    plan_path, mailmap_path = write_inputs(
        workdir,
        merge_slug_prefixes=merge_slug_prefixes,
        identity_map=identity_map,
    )
    command = filter_repo_command(mailmap_path=mailmap_path, refs=refs)
    environment = dict(os.environ)
    # The callback runs in filter-repo's process, so the configuration travels
    # in the environment and the project has to be importable there.
    environment[MERGE_SLUG_ENV] = ",".join(merge_slug_prefixes)
    source_root = str(Path(__file__).resolve().parents[2])
    existing = environment.get("PYTHONPATH", "")
    environment["PYTHONPATH"] = f"{source_root}{os.pathsep}{existing}" if existing else source_root
    result = subprocess.run(  # noqa: S603 - fixed executable, argument list
        command,
        cwd=str(repo),
        capture_output=True,
        text=True,
        errors="replace",
        env=environment,
    )
    if result.returncode != 0:
        raise GitError(f"filter-repo failed: {result.stderr.strip()[:400]}")
    return {
        "command": command,
        "plan_file": str(plan_path),
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
    # The three that close the hole the review found. An agreed ref that is
    # gone, or that no longer points where the commit map says it must, is a
    # FAIL — not a skipped iteration. `unverifiable_refs` is the honest answer
    # for a ref this code cannot reason about: never a silent pass.
    missing_refs: list[str] = field(default_factory=list)
    tip_mismatch: list[str] = field(default_factory=list)
    unverifiable_refs: list[str] = field(default_factory=list)
    refs_checked: int = 0
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
            or self.missing_refs
            or self.tip_mismatch
            or self.unverifiable_refs
            or self.pairs == 0
        )

    def as_text(self) -> str:
        out = [
            f"mapped commits:            {self.pairs}",
            f"agreed refs checked:       {self.refs_checked}",
            f"dropped commits:           {len(self.dropped)}",
            f"tree mismatches:           {len(self.tree_mismatch)}",
            f"author date mismatches:    {len(self.author_date_mismatch)}",
            f"committer date mismatches: {len(self.committer_date_mismatch)}",
            f"parent structure changes:  {len(self.parent_mismatch)}",
            f"ref commit count changes:  {len(self.ref_count_mismatch)}",
            f"missing refs:              {len(self.missing_refs)}",
            f"ref tip mismatches:        {len(self.tip_mismatch)}",
            f"unverifiable refs:         {len(self.unverifiable_refs)}",
            f"signatures no longer present: {len(self.signatures_lost)}",
            f"residual attribution findings: {self.residual_findings}",
            f"verdict: {'PASS' if self.ok else 'FAIL'}",
        ]
        if self.pairs == 0:
            out.append("  commit-map: empty or unreadable, so nothing was actually verified")
        for label, items in (
            ("dropped", self.dropped),
            ("tree", self.tree_mismatch),
            ("author-date", self.author_date_mismatch),
            ("committer-date", self.committer_date_mismatch),
            ("parents", self.parent_mismatch),
            ("ref-counts", self.ref_count_mismatch),
            ("missing-ref", self.missing_refs),
            ("ref-tip", self.tip_mismatch),
            ("unverifiable-ref", self.unverifiable_refs),
        ):
            for item in items[:10]:
                out.append(f"  {label}: {item}")
        return "\n".join(out) + "\n"


def _ref_target_commit(repo: Path, ref: str) -> str | None:
    """The COMMIT a ref resolves to, peeling an annotated tag, or ``None``.

    An annotated tag is its own object, and a rewrite gives it a new SHA
    because the commit under it moved. So the thing to compare is the commit
    the tag points at — requiring the tag object's own SHA to be unchanged
    would fail every correct rewrite.
    """
    out = run_git(repo, "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}", check=False).strip()
    return out or None


def verify(original: Path, rewritten: Path, *, refs: Sequence[str] = ()) -> VerifyReport:
    """Compare every mapped commit, and prove each agreed ref still points right.

    What a message-only rewrite is allowed to change is the message. So this
    asserts the tree is byte-identical, both dates and both offsets are
    unchanged, the parent list maps one-to-one through the commit map, and no
    commit was dropped.

    ``refs`` is the AGREED set, and it is resolved against the ORIGINAL
    repository. That is the correction the review asked for. The reviewed
    version compared commit counts and otherwise trusted the rewritten
    repository to say which refs existed, so pointing `main` at `side` — same
    number of commits, different content — reported PASS, and deleting a branch
    was skipped by an `except: continue` and then not even listed, because the
    default ref list was read out of the rewritten clone.

    Signatures are reported rather than asserted: they cannot survive, and
    pretending otherwise would be the one dishonest line in the report.
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
        report.refs_checked += 1

        before_tip = _ref_target_commit(original, ref)
        if before_tip is None:
            # Either the agreed ref never existed in the original, or it does
            # not resolve to a commit. Both mean this code cannot prove
            # anything about it, and saying nothing would be the bug.
            report.unverifiable_refs.append(f"{ref}: does not resolve to a commit in the original repository")
            continue

        after_tip = _ref_target_commit(rewritten, ref)
        if after_tip is None:
            report.missing_refs.append(f"{ref}: absent after the rewrite")
            continue

        expected_tip = mapping.get(before_tip)
        if expected_tip is None:
            report.unverifiable_refs.append(f"{ref}: original tip {before_tip[:12]} is not in the commit map")
            continue
        if expected_tip == _ZERO:
            report.unverifiable_refs.append(f"{ref}: original tip {before_tip[:12]} was dropped by the rewrite")
            continue
        if after_tip != expected_tip:
            # The check the review found missing: an equal commit count proves
            # nothing about WHICH history a ref now names.
            report.tip_mismatch.append(f"{ref}: expected {expected_tip[:12]}, found {after_tip[:12]}")
            continue

        before_count = int(run_git(original, "rev-list", "--count", ref).strip())
        after_count = int(run_git(rewritten, "rev-list", "--count", ref).strip())
        if before_count != after_count:
            report.ref_count_mismatch.append(f"{ref}: {before_count} -> {after_count}")
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

    # In verify mode the agreed set comes from the ORIGINAL repository, never
    # from the rewritten one. Reading it from the rewrite is how a deleted
    # branch disappeared from its own verification.
    ref_source = Path(args.verify_against).resolve() if args.verify_against is not None else repo
    try:
        refs = (
            list(args.refs)
            if args.refs
            else default_refs(ref_source, include_unpublishable=args.include_unpublishable_refs)
        )
    except GitError as error:
        print(f"refused: {error}", file=sys.stderr)
        return 2

    if args.verify_against is not None:
        try:
            report = verify(ref_source, repo, refs=refs)
        except GitError as error:
            print(f"refused: {error}", file=sys.stderr)
            return 2
        # The residual audit reads the REWRITTEN repository, so it can only
        # look at refs that are still there. A ref the rewrite lost is already
        # a verification failure by name; crashing here would replace that
        # report with a traceback.
        readable = [ref for ref in refs if _ref_target_commit(repo, ref) is not None]
        residual = audit(repo, readable, merge_slug_prefixes=args.merge_slug_prefix, sample=0, count_signatures=False)
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

    refs = [ref for ref in refs if is_publishable(ref)] if not args.refs else refs
    workdir = (args.workdir or (repo.parent / f"{repo.name}.filter-inputs")).resolve()
    try:
        outcome = rewrite(
            repo,
            refs=refs,
            merge_slug_prefixes=args.merge_slug_prefix,
            identity_map=identity_map,
            workdir=workdir,
        )
    except (RefusedError, GitError) as error:
        print(f"refused: {error}", file=sys.stderr)
        return 2

    print("rewrite finished in the clone. Nothing was pushed.")
    print(f"  command:    {' '.join(outcome['command'])}")
    print(f"  plan:       {outcome['plan_file']}")
    if outcome["mailmap_file"]:
        print(f"  mailmap:    {outcome['mailmap_file']}")
    print(f"  commit map: {outcome['commit_map']}")
    print("Next: verify with --verify-against <original repo>, then publish per the runbook.")
    return 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    raise SystemExit(main())
