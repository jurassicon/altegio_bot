# EasyWeek: align the three marketing templates with the approved Meta content

**Scope of this operational task.** Three EasyWeek codes — `review_3d`,
`repeat_10d`, `comeback_3d` — and the `message_templates` rows and default
senders that belong to them. Nothing else.

This is **not** a re-run of PR-12, **not** a change to the Meta template cloner,
**not** an enablement of any messaging, and **not** newsletters, campaigns or
promo. The canonical `docs/easyweek/INTEGRATION_PLAN.md` is not amended by this
task; its active-phase status is stale in places, and correcting it needs the
owner's separate decision.

Three separate things are deliberately kept apart in this document, because
conflating them is how a template fix turns into an unplanned send:

1. **finishing the code change** (a merged PR);
2. **external production prerequisites** (Meta templates that do not exist yet,
   a sender nobody has restored);
3. **authorisation to send** (a different decision, with its own evidence).

Completing 1 and 2 does not produce 3.

---

## 0. What the audit found

Read-only audit, 2026-09-02:

| Branch | company_id | Meta APPROVED | Active `de` rows | Active default sender |
|---|---|---|---|---|
| Durlach | 308697 | all three | `review_3d` only | yes |
| Rastatt | 315607 | `review_3d` only | `review_3d` only | yes |
| Karlsruhe | 322579 | all three | none | **no** |

And for all seven templates that do exist: `de`, MARKETING, POSITIONAL,
BODY-only, parameter count and order confirmed — but the source-owned body did
**not** match the approved Meta content.

Be precise about what that does and does not mean, because the two comparisons
are different:

- the **audit** compared APPROVED Meta content against the source contract, and
  found them different;
- the **runtime** guard compares the selected DB row's body and name against the
  source contract — it does not read Meta.

So a pre-existing row that matched the OLD code could pass the runtime guard
perfectly well while still differing from what Meta had approved. That is the
state the two existing review rows were in. This PR brings all three into
agreement: Meta, the code and the database rows.

Two facts worth keeping in mind while reading the rest:

- **Zero ACTIVE rows is not "no row".** There may be an inactive row for the same
  key. The reconcile command reads active and inactive rows together for exactly
  this reason, and refuses when it finds duplicates instead of picking one.
- **The flag snapshot in that report is historical.** It described one outbox
  container at one moment. It is not proof of today's state in every process —
  step 1 below re-reads it.

---

## 1. Fresh read-only check

Nothing here writes. Run these first, every time; do not reason from the audit
file.

```bash
cd /opt/altegio_bot
$COMPOSE exec -T altegio-outbox-worker /app/.venv/bin/python -c 'from altegio_bot.settings import settings as s; print({"notifications": s.easyweek_notifications_enabled, "reviews": s.easyweek_reviews_enabled, "review_send": s.easyweek_review_send_enabled, "retention": s.easyweek_retention_enabled, "retention_send": s.easyweek_retention_send_enabled})'
```

```bash
cd /opt/altegio_bot
$COMPOSE exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT company_id, code, language, is_active, meta_template_name FROM message_templates WHERE provider = '"'"'easyweek'"'"' AND code IN ('"'"'review_3d'"'"','"'"'repeat_10d'"'"','"'"'comeback_3d'"'"') ORDER BY company_id, code, id"'
```

The second query deliberately does **not** filter on `is_active`: an inactive row
is the case that would otherwise surprise the apply.

---

## 2. Close the two affected send fences — and only those

The body of a live template is about to change. Any message that goes out
between the deploy and the reconciliation would render the new code against the
old row, or the reverse.

```text
EASYWEEK_REVIEW_SEND_ENABLED=false
EASYWEEK_RETENTION_SEND_ENABLED=false
```

```bash
cd /opt/altegio_bot
$COMPOSE up -d --force-recreate altegio-outbox-worker
```

```bash
cd /opt/altegio_bot
$COMPOSE exec -T altegio-outbox-worker /app/.venv/bin/python -c 'import sys; from altegio_bot.settings import settings as s; ok = s.easyweek_review_send_enabled is False and s.easyweek_retention_send_enabled is False; print({"review_send": s.easyweek_review_send_enabled, "retention_send": s.easyweek_retention_send_enabled, "gate": "PASS" if ok else "FAIL"}); sys.exit(0 if ok else 1)'
```

`PASS` and exit code `0` are required before the deploy.

**Do not use `EASYWEEK_NOTIFICATIONS_ENABLED=false` as a general kill-switch
here.** It is not one: on the send side it fences only PR-12 retention (§16.1 of
the activation runbook), and using it would additionally stop planning for every
EasyWeek code — capture, `visits_total` and the ordinary lifecycle and reminder
flows should all keep running. If a genuine full stop is ever needed, that is
§8.2, not this procedure.

---

## 3. Deploy the code with those fences closed

Deploy the merged branch. The three source-owned bodies now equal the approved
Meta content; the six branch-specific codes are unchanged.

At this point code and Meta agree, and the database does not yet. That is the
expected intermediate state, and it is safe precisely because the two fences are
shut.

---

## 4. External prerequisite: the two missing Rastatt templates

Rastatt has no approved `repeat_10d` or `comeback_3d`. That is an external fact,
not something any command here may work around: the reconcile CLI blocks on it,
and it must never be satisfied by pointing Rastatt at the Karlsruhe template.

The existing cloner creates them from the Karlsruhe originals. It selects
templates by **exact name**, repeated once per template — there is no `--only`
flag — and `--include-neutral` is required because these two bodies contain no
Karlsruhe address to rewrite.

**(a) Dry-run.** Writes nothing and submits nothing.

```bash
cd /opt/altegio_bot
$COMPOSE run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.clone_meta_templates_for_location --source-location ka --target-location ra --language de --include-neutral --template-name kitilash_ka_repeat_10d_v1 --template-name kitilash_ka_comeback_3d_v1
```

Read the printed plan. It must name exactly two prepared templates,
`kitilash_ra_repeat_10d_v1` and `kitilash_ra_comeback_3d_v1`, and no others.

**(b) Apply, after the operator has reviewed that plan.** `--apply` refuses to
inherit the dry-run defaults, so `--target-location`, `--address` and
`--maps-url` must all be given explicitly — even though these two neutral bodies
never print an address. The values are Rastatt's own, taken from the
source-controlled branch profile:

```bash
cd /opt/altegio_bot
$COMPOSE run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.clone_meta_templates_for_location --source-location ka --target-location ra --language de --include-neutral --template-name kitilash_ka_repeat_10d_v1 --template-name kitilash_ka_comeback_3d_v1 --address "76437 Rastatt, Rathausstraße 5" --maps-url https://maps.app.goo.gl/xvYYbJbPaWcnp9Xv5 --apply
```

The cloner then asks for its own confirmation phrase, which changes whenever the
plan changes. For these two templates it is:

```text
CREATE:RA:2
```

Type it exactly. **Do not add `--yes`** — that flag skips the confirmation, and
the confirmation is the point of this step.

**(c) Read-only confirmation, after Meta has reviewed them.** Submission is not
approval; Meta moves a new template through PENDING first.

```bash
cd /opt/altegio_bot
$COMPOSE run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.clone_meta_templates_for_location --source-location ka --target-location ra --language de --include-neutral --template-name kitilash_ka_repeat_10d_v1 --template-name kitilash_ka_comeback_3d_v1
```

Re-running the dry-run reports what now exists on the target side. Both templates
must be present under their `ra` names and APPROVED. **PENDING, absent, or an
existing target that does not match is not readiness** — step 5 for Rastatt
blocks until they are genuinely APPROVED, and that block is correct.

Durlach and Karlsruhe need nothing here.

---

## 5. Reconcile the database rows

Dry-run first. Selectors are mandatory; there is no "all branches" and no "all
codes".

```bash
cd /opt/altegio_bot
$COMPOSE run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.reconcile_easyweek_templates --branch durlach --branch karlsruhe --code review_3d --code repeat_10d --code comeback_3d
```

Read the plan. Expected on the first run: `create` for the rows that do not
exist, `update` for the ones whose body is stale, `unchanged` for anything
already correct. Any `blockers` entry means **stop** — the whole selected apply
is refused rather than partially written, which is the point.

When the plan is what you expect:

`--apply` requires `--snapshot`: without a record of the previous rows there is
no rollback. The path is on the host, under the directory the service already
mounts, and the command refuses to overwrite an existing file.

```bash
cd /opt/altegio_bot
mkdir -p outputs/easyweek_templates
```

```bash
cd /opt/altegio_bot
$COMPOSE run --rm --no-deps -v /opt/altegio_bot/outputs:/app/outputs --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.reconcile_easyweek_templates --branch durlach --branch karlsruhe --code review_3d --code repeat_10d --code comeback_3d --apply --snapshot /app/outputs/easyweek_templates/reconcile-$(date -u +%Y%m%dT%H%M%SZ).json
```

The report prints `snapshot_written`. Keep that file: §9 needs it, and it cannot
be reconstructed afterwards.

Rastatt is added to the same commands only after step 4 has completed and both
templates are APPROVED.

---

## 6. Re-audit

Run the dry-run from step 5 again. A second run over an aligned state must
report `unchanged` for every selected pair and `mutations_attempted: 0`. Re-run
the read-only DB query from step 1 and confirm the rows say what the plan said.

---

## 7. Karlsruhe's sender — a separate decision, with a consequence

Karlsruhe has no active default sender. Restoring it is **not** part of a body
reconciliation, which is why it needs its own flag.

Before deciding, look at what is actually there:

```bash
cd /opt/altegio_bot
$COMPOSE exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT id, provider, company_id, sender_code, is_active FROM whatsapp_senders WHERE company_id = 322579 ORDER BY id"'
```

```bash
cd /opt/altegio_bot
$COMPOSE exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT job_type, status, count(*) FROM message_jobs WHERE provider = '"'"'easyweek'"'"' AND company_id = 322579 GROUP BY job_type, status ORDER BY job_type, status"'
```

**Read the queue result before activating anything.** A sender is what makes an
EasyWeek job sendable at all, so activating it can release ORDINARY messages —
lifecycle confirmations, reminders — that are already `queued` for Karlsruhe.
That may be exactly what is wanted, or it may be a batch of stale notifications
about appointments customers made weeks ago. Decide deliberately, in writing,
before running the next command.

```bash
cd /opt/altegio_bot
$COMPOSE run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.reconcile_easyweek_templates --branch karlsruhe --code review_3d --code repeat_10d --code comeback_3d --include-sender
```

After reviewing that dry-run and approving the queue consequence, apply with a
**new, separate snapshot** and the same persistent host mount as step 5:

```bash
cd /opt/altegio_bot
$COMPOSE run --rm --no-deps -v /opt/altegio_bot/outputs:/app/outputs --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.reconcile_easyweek_templates --branch karlsruhe --code review_3d --code repeat_10d --code comeback_3d --include-sender --apply --snapshot /app/outputs/easyweek_templates/karlsruhe-sender-$(date -u +%Y%m%dT%H%M%SZ).json
```

Keep the printed `snapshot_written` file for this run. It captures the template
state immediately before **this** apply, not the earlier BODY changes; it does
not replace step 5's snapshot. Never reuse an earlier snapshot path; an existing
path is refused, not overwritten. Template restore does **not** roll back a
sender. A sender pointing at a different WhatsApp line is refused, never rewritten.

Do **not** re-drive `failed` jobs. And do not conclude that "Karlsruhe
notifications are restored" from this: only three codes and one sender were
checked here; the six lifecycle and reminder codes for that branch have their own
rows and their own state.

---

## 8. Re-opening sends — separately, and in this order

Aligned templates are not permission to send.

**Review — the PR-9 preflight is mandatory, and it comes first.**

`EASYWEEK_REVIEW_SEND_ENABLED` is **global**, not per branch. Opening it releases
queued `review_3d` for *every* EasyWeek branch, so a green result for the branch
you happen to care about is not permission to open it.

The gate is the existing `easyweek_review_preflight` and the rules of §13.2 of
`durlach_activation_runbook.md`. Do **not** replay the original rollout from the
beginning: no second broad seed, no webhook re-creation, and do not switch off
planning that is already running.

1. **The review send fence stays closed** through this whole step.
2. Complete the template reconciliation (step 5) and make the sender decision
   (step 7) *first*. A branch whose sender is missing cannot pass.
3. Run the preflight in a **fresh one-off container**, same Compose project and
   both production compose files:

```bash
cd /opt/altegio_bot
$COMPOSE run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.easyweek_review_preflight
```

`docker compose exec` is wrong here, and `exec -e` to paper over a stale
environment is not an alternative: the running worker holds the environment it
was created with.

4. Required to proceed, all of them: `ready=true`, exit code `0`,
   `candidate_count > 0`, `truncated=false`, no `config_error`, no blocked
   candidates.
5. **If any branch fails, the correct action is STOP with the fence still
   closed.** In particular, if Karlsruhe's sender was deliberately not activated
   in step 7, its review candidates will report `sender_missing_or_inactive` and
   the preflight will not go green. Do not work around that by removing the
   branch from `EASYWEEK_LOCATION_MAP`, and do not invent a per-branch send gate
   — neither exists, and both would be a bigger change than the one being made.
6. Only on a fully green preflight does the operator separately set
   `EASYWEEK_REVIEW_SEND_ENABLED=true`, recreate **only** the outbox worker, and
   confirm the effective flag from inside the new container.
7. Then watch one natural delivery through `message_jobs` → `outbox_messages` →
   Meta `delivered`/`read`. Do not edit `run_at` or a payload to make it fire
   sooner, and do not re-drive `failed` jobs.

Review and retention are prepared independently. If Rastatt's `review_3d` is
already APPROVED, it may be reconciled on its own with `--code review_3d` without
waiting for its missing retention pair. What must not happen is opening the
global review fence while another live branch still holds the old review body —
that is precisely what step 5 exists to prevent.

**Retention.** This one has its own sequence and it is unchanged — §16.2 of
`durlach_activation_runbook.md`, in full:

1. planning on with the send fence still shut;
2. read-only `easyweek_retention_preflight` over the real queue;
3. one `EASYWEEK_RETENTION_CANARY_JOB_ID`, preflight again under it;
4. open the fence, wait for the **natural** `run_at`, confirm delivery through
   `message_jobs` → `outbox_messages` → Meta `delivered`/`read` → Chatwoot;
5. close the fence, unset the canary;
6. full preflight over the whole queue;
7. bulk.

Never edit `run_at` or a payload to make a test fire sooner. A job that was
hurried is not evidence about the job that would have gone out.

---

## 9. Rollback

**Order matters, and not for the obvious reason.** `reconcile_easyweek_templates`
and `restore_easyweek_templates` exist only in *this* version. Deploying the
previous code first would take both away, and the ordinary apply would be no help
anyway: it writes the current contract rather than restoring what was there. So
the rows go back **while this version is still deployed**, and the code is
reverted afterwards.

This is why step 5's apply requires `--snapshot`. That file is the only record of
what the rows were, it lands on a host path, and it survives the `--rm`
container.

Snapshot **version 2** freezes the previous values and `expected_after` together
for each `(provider, company_id, code, language)` key: exact BODY, Meta template
name and activity, plus the original row id for an existing row. Restore accepts
only the exact expected-after state or an already restored state. A created row
is an idempotent no-op only when its BODY and name still match and it is inactive.
An inactive row with edited text or name still blocks the **whole batch**, as
does a later edit to an existing row's name or activity.

The proof comes from the saved snapshot, never from the current branch registry.
A missing/invalid registry cannot bypass it. Version 1 and incomplete snapshots
without `expected_after` are refused before any write; do not hand-upgrade them
or fill in guessed values. They require a separate manual rollback decision.

**1. Close both send fences and prove it**, exactly as in step 2:

```bash
cd /opt/altegio_bot
$COMPOSE exec -T altegio-outbox-worker /app/.venv/bin/python -c 'import sys; from altegio_bot.settings import settings as s; ok = s.easyweek_review_send_enabled is False and s.easyweek_retention_send_enabled is False; print({"review_send": s.easyweek_review_send_enabled, "retention_send": s.easyweek_retention_send_enabled, "gate": "PASS" if ok else "FAIL"}); sys.exit(0 if ok else 1)'
```

**2. Restore the rows, dry-run first**, still on this version. The snapshot lives
under `outputs/`, which the compose service already mounts, so the same path is
visible inside the one-off container:

```bash
cd /opt/altegio_bot
$COMPOSE run --rm --no-deps -v /opt/altegio_bot/outputs:/app/outputs --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.restore_easyweek_templates --snapshot /app/outputs/easyweek_templates/<the file step 5 wrote>
```

Read the plan. `restore` puts a previous row back; `deactivate` retires a row
this apply created — created rows are never deleted, because their ids are
referenced elsewhere. Any `blockers` entry means **stop**: a row changed again
after the apply, and overwriting somebody's later edit is not a rollback.

**3. Apply the restore**, same command plus `--apply`:

```bash
cd /opt/altegio_bot
$COMPOSE run --rm --no-deps -v /opt/altegio_bot/outputs:/app/outputs --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.restore_easyweek_templates --snapshot /app/outputs/easyweek_templates/<the file step 5 wrote> --apply
```

**4. Verify the rows** with the read-only query from step 1. Re-running the
restore is safe and must report `unchanged` for everything.

**5. Only now deploy the previous code version.**

**6. Leave the send fences closed.** The rollback does not re-open them, and
nothing here authorises it to: a restored body is the body that was there before,
**not** evidence that it matches what Meta has approved today. Re-opening review
goes through the preflight in step 8 again.

The sender is untouched by all of this. A template rollback never activates,
deactivates or re-points a sender — that is its own operation with its own
decision.

Do **not** restore a database backup, delete queues, or relax the body-equality
check to "make it match". That check is the only thing that noticed this
mismatch in the first place.

---

## Out of scope, found along the way

- The canonical integration plan's active-phase status is stale. Not corrected
  here; it needs the owner's decision.
- Karlsruhe's six lifecycle and reminder rows were not audited by this task.
