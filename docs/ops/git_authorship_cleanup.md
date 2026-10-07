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

Four things are deliberately left alone, and the tests pin each one:

- **Human co-authors.** `Co-authored-by: jurassicon <…>` is a person, and this
  repository has 52 of those lines. A rule that removed every
  `Co-authored-by:` would delete the owner's own attribution.
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
| Published branches on `origin` | 7 |
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

The two files it generates — `replace-message.txt` and `mailmap.txt` — are the
whole instruction set, and they are plain text. Read them before trusting the
result.

Note what the tool scoped for you: a `--mirror` clone of a GitHub repository
brings down all 211 `refs/pull/*` refs, and those belong to the forge. The
rewrite covers the 7 publishable branches only; asking it to rewrite a
forge-owned ref is refused, because the result could never be pushed.

---

## 4. Verify

```bash
cd /Users/cherkasov/Documents/Dev/altegio_bot && uv run python -m altegio_bot.scripts.clean_git_authorship --repo /tmp/authorship-rehearsal.git --verify-against /tmp/authorship-original.git --merge-slug-prefix copilot
```

It walks filter-repo's own `commit-map` and, for every old→new pair, asserts
that the tree is identical, that both the author and committer timestamps and
UTC offsets are unchanged, that the parent list maps one-to-one, that no commit
was dropped, and that each ref still holds the same number of commits. Then it
re-audits the result and prints what is left. It exits non-zero if any of that
fails.

Signatures are **reported, not asserted**. A rewrite cannot re-sign, so the
line `signatures no longer present: N` is the honest outcome, and the rehearsal
below shows what N is here.

### The rehearsal performed on 2026-10-08

```
mapped commits:            1079
dropped commits:           0
tree mismatches:           0
author date mismatches:    0
committer date mismatches: 0
parent structure changes:  0
ref commit count changes:  0
signatures no longer present: 210
residual attribution findings: 0
verdict: PASS
```

Spot checks on that rehearsal: the Claude trailer was gone and its body ended
on its last substantive line; an agent log trailer was gone while the human
`Co-authored-by` line beside it survived; `Merge pull request #42 from
jurassicon/copilot/fix-contacts-without-names` became `… from
jurassicon/fix-contacts-without-names`, keeping the PR number; `.gitignore`
still listed `CLAUDE.md` and `.claude/`; and the bot identity was gone from all
seven branches while `GitHub <noreply@github.com>` remained the committer of
the merges it really made.

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

If a branch is also to be renamed — the audit lists every ref whose own name
carries an agent prefix, and `copilot/debug-chatwoot-signature-failure` is the
published one — that is a separate decision. A message rewrite cannot move a
ref: push the new name and delete the old one explicitly.

```bash
cd /tmp/authorship-rehearsal.git && git push https://github.com/jurassicon/altegio_bot refs/heads/copilot/debug-chatwoot-signature-failure:refs/heads/debug-chatwoot-signature-failure
```

```bash
cd /tmp/authorship-rehearsal.git && git push https://github.com/jurassicon/altegio_bot --delete refs/heads/copilot/debug-chatwoot-signature-failure
```

Deleting a branch that still has an open PR closes that PR, so check before the
second command. The local `backup/before-remove-claude-coauthor` branch was
never published; it is renamed or dropped in your own clone only.

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
| Rules an operator can read | generated into `--workdir`: `replace-message.txt`, `mailmap.txt` |
| filter-repo's own old→new record | `<clone>/filter-repo/commit-map` |

The tests build their own repositories under `tmp_path` and rewrite only bare
clones they just made. They cover each removal rule, the human co-author and
forge committer that must survive, the paging-cursor prose and the licence and
dependency lines in a tracked file, the refusal to rewrite a working checkout,
the refusal to map a human identity, opt-in behaviour for merge slugs and
identities, tree/date/parent/count verification, and a second rewrite finding
nothing left to do.
