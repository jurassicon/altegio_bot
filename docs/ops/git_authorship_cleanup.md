# Removing AI authorship metadata from git history — runbook

This describes how to find machine attribution in this repository's history and,
as a separate decision, how to remove the confirmed parts of it.

> **Merging this document and its tool changes nothing about existing commits.**
> The cleanup is an operator procedure with a force-push at the end. Until
> somebody runs it, every commit keeps the metadata it has today.

The tool is read-only by default:

```bash
cd /Users/cherkasov/Documents/Dev/altegio_bot && uv run python -m altegio_bot.scripts.clean_git_authorship
```

It rewrites only with `--apply`, and `--apply` refuses to run anywhere except a
bare clone — a working checkout is rejected by name, and so is the repository
the tool itself was loaded from. Nothing in it pushes.

---

## 1. What counts as attribution, and what does not

Three things are treated as machine attribution:

| Finding | Example | Default |
| --- | --- | --- |
| AI co-author trailer | `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>` | removed by `--apply` |
| Agent log trailer | `Agent-Logs-Url: https://github.com/<owner>/<repo>/sessions/<id>` | removed by `--apply` |
| Generated-by footer | `🤖 Generated with [Claude Code](...)` | removed by `--apply` |
| Bot author / committer | `copilot-swe-agent[bot] <…+Copilot@users.noreply.github.com>` | **reported only**, needs `--identity-map` |
| Agent branch name in a merge subject | `Merge pull request #42 from jurassicon/copilot/fix-…` | **reported only**, needs `--merge-slug-prefix` |

Five things are deliberately left alone, and the tests pin each one:

- **Human co-authors.** `Co-authored-by: jurassicon <…>` is a person, and this
  repository has 52 of those lines. A rule that removed every
  `Co-authored-by:` would delete the owner's own attribution.
- **Anyone who merely looks like an agent.** A machine identity is confirmed by
  something a person cannot hold — GitHub's `[bot]` account suffix, or an agent
  vendor's own no-reply address — never by a word appearing somewhere in a name
  or an email. `Co-Authored-By: Jean-Claude Dupont <jcd@example.org>` and
  `Co-Authored-By: Devin Smith <devin@example.org>` are people, and both keep
  their lines. The audit reports such resemblances as **ambiguous** so that a
  human can look at them; nothing removes or replaces an ambiguous identity,
  and an identity map may not name one.
- **The forge committer.** `GitHub <noreply@github.com>` is the real committer
  of 210 merges made through the web UI. It is a platform, not an agent.
- **Functional mentions.** Dozens of commit messages in this repository discuss
  a Chatwoot paging **cursor**; several discuss vendors by name. Nothing here
  matches a word in prose — every pattern is anchored to a trailer key.
- **Tracked files.** There is no "replace this string in the sources" mode, on
  purpose. `.gitignore` legitimately lists `CLAUDE.md` and `.claude/`,
  dependencies and licence headers name their vendors, and a rewrite that
  edited files would be a code change disguised as a cleanup.

### Why a bot identity needs a map

Replacing `copilot-swe-agent[bot]` means stating who the person behind that
commit was. The tool does not know, and will not guess. Supply it:

```bash
cd /Users/cherkasov/Documents/Dev/altegio_bot && cat > /tmp/identity-map.json <<'JSON'
{
  "copilot-swe-agent[bot] <198982749+Copilot@users.noreply.github.com>": {
    "name": "Iurii Cherkasov",
    "email": "cherkasoooov@gmail.com"
  }
}
JSON
```

The left-hand side must classify as a machine identity and the right-hand side
must not, so the map cannot be used to rewrite a person's attribution into
another person's. Without the map the identity is reported and left in place.

---

## 2. Audit

Run it in the working checkout. It reads, and it writes nothing. There it
covers the local branches and tags as well as the published ones, which is what
you want from an audit — the old objects stay alive in your own clone for as
long as a local ref points at them. In a `--mirror` clone, where a rewrite
actually happens, `refs/heads` *is* the published set and the same invocation
describes the same history:

```bash
cd /Users/cherkasov/Documents/Dev/altegio_bot && uv run python -m altegio_bot.scripts.clean_git_authorship --merge-slug-prefix copilot
```

Keep the output **outside the repository**. It quotes the old attribution
verbatim, including agent session URLs, so committing it would re-add in a
tracked file exactly what the rewrite removes from the history:

```bash
cd /Users/cherkasov/Documents/Dev/altegio_bot && uv run python -m altegio_bot.scripts.clean_git_authorship --merge-slug-prefix copilot --json > /tmp/authorship-audit.json
```

To see how much stays reachable through refs nobody can rewrite:

```bash
cd /Users/cherkasov/Documents/Dev/altegio_bot && uv run python -m altegio_bot.scripts.clean_git_authorship --include-unpublishable-refs --no-signature-scan
```

### What the audit found on 2026-10-08

Numbers, not content — the content stays local.

| | |
| --- | --- |
| Commits reachable from the audited refs | 1080 |
| Published branches on `origin` | 8 |
| Published tags | 0 (the two local `backup/*` tags were never pushed) |
| `refs/pull/*/head` refs on the forge | 211, which a client cannot rewrite |
| Commits with an AI co-author trailer | 1 |
| Commits with an agent log trailer | 20 |
| Merge subjects quoting an agent branch prefix | 37 |
| Commits authored by a bot identity | 88 on `main` |
| Commits carrying a `gpgsig` header | 210 |
| Oldest affected commit | 2026-02-26, with 902 commits after it on `main` |
| Tracked files containing attribution | none; the only match is the `.gitignore` rule, which stays |
| Refs whose own NAME carries an agent prefix | 2: published `copilot/debug-chatwoot-signature-failure`, local `backup/before-remove-claude-coauthor` |

Sources the tool cannot check, and which this procedure therefore does not
clean: pull request titles, bodies, review comments and review threads; CI run
logs and artifacts; forks and clones other people already hold; releases,
issues, boards and wiki pages; and any cache or search index of a page that
quoted a commit.

---

## 3. Rehearse

A rehearsal is not optional — it is where the numbers in step 5 come from. Do
it on a throwaway mirror, and keep the untouched copy to verify against:

```bash
cd /tmp && rm -rf authorship-rehearsal.git authorship-original.git && git clone --mirror https://github.com/jurassicon/altegio_bot authorship-rehearsal.git
```

```bash
cd /tmp && cp -R authorship-rehearsal.git authorship-original.git
```

Record the published SHAs before anything moves. This file is what the
`--force-with-lease` in step 6 is built from:

```bash
cd /tmp/authorship-original.git && git for-each-ref --format='%(objectname) %(refname)' refs/heads | tee /tmp/authorship-original-shas.txt
```

Rewrite the mirror:

```bash
cd /Users/cherkasov/Documents/Dev/altegio_bot && uv run python -m altegio_bot.scripts.clean_git_authorship --repo /tmp/authorship-rehearsal.git --merge-slug-prefix copilot --identity-map /tmp/identity-map.json --workdir /tmp/authorship-filter-inputs --apply
```

Read `message-plan.txt` in the workdir before trusting the result: it is the
written record of exactly which removals are configured, which merge-slug
prefix was opted into, and the rule that tidying happens **only** in a message
something was actually removed from.

The message transformation itself is not a file of substitutions. It cannot be:
"close the blank-line hole, but only where a trailer was removed" is not
expressible as an unconditional substitution list, and describing the rules
twice — once for filter-repo, once for the preview — is what let the two drift
apart in an earlier version. The rewrite therefore calls
`transform_message` through a `git filter-repo --message-callback`, and the
audit preview calls the same function. `mailmap.txt` stays a real
`--mailmap` file, because identity replacement genuinely is a mapping.

Note what the tool scoped for you: a `--mirror` clone of a GitHub repository
brings down all 211 `refs/pull/*` refs, and those belong to the forge. The
rewrite covers the publishable branches only; asking it to rewrite a
forge-owned ref is refused, because the result could never be pushed.

---

## 4. Verify

```bash
cd /Users/cherkasov/Documents/Dev/altegio_bot && uv run python -m altegio_bot.scripts.clean_git_authorship --repo /tmp/authorship-rehearsal.git --verify-against /tmp/authorship-original.git --merge-slug-prefix copilot
```

It walks filter-repo's own `commit-map` and, for every old→new pair, asserts
that the tree is identical, that both the author and committer timestamps and
UTC offsets are unchanged, that the parent list maps one-to-one, and that no
commit was dropped.

It then proves each **agreed** ref individually, and the agreed set is read
from the ORIGINAL repository — never from the rewrite, which would let a
deleted branch vanish from its own verification. Every agreed ref must still
exist, and its tip must be the original tip mapped through the commit map. An
equal commit count is an additional check, not a substitute: pointing `main` at
`side` keeps the count and changes the history. For an annotated tag the
comparison is the commit the tag points at, because the tag object itself
legitimately gets a new SHA. A ref that cannot be reasoned about is reported as
`unverifiable`, and an empty or unreadable `commit-map` is a FAIL rather than a
vacuous pass.

Then it re-audits the result and prints what is left. It exits non-zero if any
of that fails.

Signatures are **reported, not asserted**. A rewrite cannot re-sign, so the
line `signatures no longer present: N` is the honest outcome, and the rehearsal
below shows what N is here.

### The rehearsal performed on 2026-10-08

Eight published branches, no published tags, the `copilot/` merge-slug prefix
opted into, and the Copilot bot identity mapped to a named person through an
explicit `--identity-map`:

```
mapped commits:            1080
agreed refs checked:       8
dropped commits:           0
tree mismatches:           0
author date mismatches:    0
committer date mismatches: 0
parent structure changes:  0
ref commit count changes:  0
missing refs:              0
ref tip mismatches:        0
unverifiable refs:         0
signatures no longer present: 210
residual attribution findings: 0
verdict: PASS
```

Spot checks on that rehearsal: no machine identity remained on any of the eight
branches, while `GitHub <noreply@github.com>` stayed the committer of the merges
it really made; all 52 `Co-authored-by: jurassicon <…>` lines survived; a grep
for `anthropic`, `agent-logs-url` and `generated with` across all eight branches
returned nothing; `.gitignore` still listed `CLAUDE.md` and `.claude/`; and
`Merge pull request #42 from jurassicon/copilot/fix-contacts-without-names`
became `… from jurassicon/fix-contacts-without-names`, keeping the PR number.

The bot identity was still reachable from `refs/pull/*` in the same clone. That
is not a defect of the rewrite — see step 7.

---

## 5. Decide, with the cost in view

Before publishing anything, the owner is agreeing to all of this:

- **SHAs change for 902 commits on `main`**, and for every branch that descends
  from the oldest affected commit. Every open branch, every local clone and
  every bookmark to a commit page goes stale.
- **210 signatures stop existing.** They were GitHub's own merge signatures;
  after the rewrite those merges are unsigned.
- **`main` is protected.** The force-push is rejected until the protection or
  ruleset is relaxed, and it must be restored immediately afterwards.
- **Other people's clones keep the old history** and will re-push it if they
  merge from it without resetting.
- **The old commits stay on the forge** under `refs/pull/*`, reachable by SHA.

---

## 6. Publish, one agreed ref at a time

Only after step 4 passed, and only for the refs that were agreed. Every push
carries `--force-with-lease=<ref>:<original SHA>` from
`/tmp/authorship-original-shas.txt`, so a push is refused if the remote moved
since the rehearsal. There is deliberately no `--mirror` and no bare `--force`
here: `--mirror` would also delete every ref missing from the clone.

Substitute the SHAs recorded in step 3. The ones from the 2026-10-08 rehearsal
were:

| ref | original | rehearsed result |
| --- | --- | --- |
| `refs/heads/main` | `10755c5ec630` | `a7fae533c255` |
| `refs/heads/chore/clean-authorship` | `6bf8b07e50c9` | `04c3ef60869f` |
| `refs/heads/fix/easyweek-approved-template-contract` | `38b708ff0e56` | `a42604be7186` |
| `refs/heads/feature/easyweek-manual-voucher-delivery-canary` | `54fa6b769209` | `99780ded2890` |
| `refs/heads/feature/easyweek-pr9-review-3d` | `f8693bb5943e` | `09da29aeb91f` |
| `refs/heads/chore/pytest-timing-profile` | `59b5a387e85d` | `aa90516df223` |
| `refs/heads/ci/example-branch` | `01162619f3d6` | `91c1675d8ea0` |
| `refs/heads/copilot/debug-chatwoot-signature-failure` | `46b9d7a984f7` | `a0865498a4b8` |

A rehearsal's result SHAs are **not** the ones to publish: redo the rewrite on
a fresh mirror at publication time and use that mirror's SHAs and that run's
leases. The table is here to show the shape of what is being agreed to.

One branch per command, so one rejection does not hide behind another:

```bash
cd /tmp/authorship-rehearsal.git && git push --force-with-lease=refs/heads/main:10755c5ec630fbfc126d2e268ec9aed832143bb2 https://github.com/jurassicon/altegio_bot refs/heads/main
```

```bash
cd /tmp/authorship-rehearsal.git && git push --force-with-lease=refs/heads/fix/easyweek-approved-template-contract:38b708ff0e56bb784c12b9e966549dd65a86f040 https://github.com/jurassicon/altegio_bot refs/heads/fix/easyweek-approved-template-contract
```

Repeat for each remaining agreed ref. `filter-repo` removes the `origin` remote
from the clone it rewrote, which is why the URL is spelled out.

### Renaming a branch whose name carries an agent prefix

The audit lists every ref whose own name reads as an agent's, and
`copilot/debug-chatwoot-signature-failure` is the published one. A message
rewrite cannot move a ref, so this is a separate decision and a separate pair
of pushes — **both of them leased.**

A delete is as destructive as a force-push and needs the same protection. `git
push --delete` on its own removes the old name whatever state it is in: if
somebody pushed to that branch after your snapshot, their commit is not in the
replacement you just created, and the delete throws it away. So the delete
states the SHA it expects to find, and git refuses it as `stale info` when the
remote has moved.

Use the SHA for the stage you are at: the snapshot from step 3 if the branch
was not rewritten, or the result you published in step 6 if it was. Do **not**
read the remote's current value just before deleting and feed that back in —
that defeats the whole point, because it would accept whatever happens to be
there, which is exactly the commit you have not seen.

**First** create the new name, with a lease asserting the name is still free.
An empty expected value means "expect this ref not to exist", so an occupied
name is refused rather than overwritten:

```bash
cd /tmp/authorship-rehearsal.git && git push --force-with-lease=refs/heads/debug-chatwoot-signature-failure: https://github.com/jurassicon/altegio_bot refs/heads/copilot/debug-chatwoot-signature-failure:refs/heads/debug-chatwoot-signature-failure
```

**Only if that succeeded**, delete the old name, leased at the SHA you agreed:

```bash
cd /tmp/authorship-rehearsal.git && git push --force-with-lease=refs/heads/copilot/debug-chatwoot-signature-failure:46b9d7a984f70a8a4d9cd1f1df0d94346641070d https://github.com/jurassicon/altegio_bot --delete refs/heads/copilot/debug-chatwoot-signature-failure
```

If the create is refused, **stop**: do not run the delete. There would be no
replacement for the branch you are about to remove. Investigate the name that
is already taken, then start this pair again.

If the delete is refused, the branch has moved since your snapshot. The commit
that caused the refusal is still on the remote and still reachable — nothing is
lost by the refusal. Fetch it, decide what to do with it, re-run the rewrite
including it, publish, and only then delete with the new expected SHA.

Deleting a branch that still has an open PR closes that PR, so check before the
delete. The local `backup/before-remove-claude-coauthor` branch was never
published; it is renamed or dropped in your own clone only.

---

## 7. What this does not achieve

- **`refs/pull/*` stays.** GitHub owns those refs; a client cannot rewrite or
  delete them. Of the 211 PR head refs here, 203 were present locally and 38 of
  those point directly at a commit carrying machine attribution. After a
  perfect rewrite, every one of those commits is still fetchable by SHA.
- **Pages and caches stay.** Commit pages, PR timelines, review comments
  quoting a diff, CI logs and anything that indexed them are outside git.
- **Forks and other clones stay**, until each holder resets.
- **Nothing here claims otherwise.** If the requirement is that no page
  anywhere shows the old attribution, this procedure cannot deliver it, and
  support for deleting forge-side refs has to be requested from GitHub.

---

## 8. Restoring

The point of `/tmp/authorship-original.git` is that recovery needs no forge
feature. With the original SHAs from step 3:

```bash
cd /tmp/authorship-original.git && git push --force-with-lease=refs/heads/main:<SHA pushed in step 6> https://github.com/jurassicon/altegio_bot refs/heads/main
```

Keep that copy until the rewrite has been accepted. Afterwards, bring each
working checkout back in line:

```bash
cd /Users/cherkasov/Documents/Dev/altegio_bot && git fetch origin --prune --force
```

A local branch that descends from the old history has to be reset onto the new
one branch by branch; local-only work — including the two `backup/*` tags,
which were never published — keeps the old commits alive in that clone until it
is reset or dropped.

---

## 9. Where the parts live

| | |
| --- | --- |
| Tool | `src/altegio_bot/scripts/clean_git_authorship.py` |
| Tests | `src/altegio_bot/tests/test_git_authorship_cleanup.py`, in the required gate |
| Engine | `git-filter-repo`, a **locked dev dependency** in `[dependency-groups] dev`; `uv sync --frozen` installs it on a laptop and on every required CI runner |
| What an operator can read before applying | generated into `--workdir`: `message-plan.txt`, `mailmap.txt` |
| filter-repo's own old→new record | `<clone>/filter-repo/commit-map` |

The tests build their own repositories under `tmp_path` and rewrite only bare
clones they just made. They cover each removal rule, the human co-author and
forge committer that must survive, the paging-cursor prose and the licence and
dependency lines in a tracked file, the refusal to rewrite a working checkout,
the refusal to map a human identity, opt-in behaviour for merge slugs and
identities, tree/date/parent/count verification, and a second rewrite finding
nothing left to do.
