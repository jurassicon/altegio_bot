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
**not** match Meta. So the runtime body-equality guard refused every one of them,
including the two review rows that existed and looked fine.

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

The existing cloner creates them from the Karlsruhe originals. **Dry-run first**,
and creation only after a separate operator decision:

```bash
cd /opt/altegio_bot
$COMPOSE run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.clone_meta_templates_for_location --source-location ka --target-location ra --only repeat_10d --only comeback_3d
```

Review the printed plan. Only then re-run it with the cloner's own confirmation
flag, and afterwards wait for Meta to move both templates to APPROVED. Until
they are APPROVED, step 5 for Rastatt will block — correctly.

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

```bash
cd /opt/altegio_bot
$COMPOSE run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.reconcile_easyweek_templates --branch durlach --branch karlsruhe --code review_3d --code repeat_10d --code comeback_3d --apply
```

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

Then the same command with `--apply`. A sender pointing at a different WhatsApp
line is refused, never rewritten.

Do **not** re-drive `failed` jobs. And do not conclude that "Karlsruhe
notifications are restored" from this: only three codes and one sender were
checked here; the six lifecycle and reminder codes for that branch have their own
rows and their own state.

---

## 8. Re-opening sends — separately, and in this order

Aligned templates are not permission to send.

**Review.** Re-open `EASYWEEK_REVIEW_SEND_ENABLED` on its own, recreate the
outbox worker, and confirm the effective value from inside the new container.
Watch one real delivery before treating it as done.

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

Roll the code version and the selected rows back **together**, with the two send
fences shut:

1. close `EASYWEEK_REVIEW_SEND_ENABLED` and `EASYWEEK_RETENTION_SEND_ENABLED`,
   recreate the outbox worker, confirm from inside it;
2. deploy the previous code version;
3. re-run the reconcile dry-run for the same selectors and read the plan;
4. apply it, so the rows match the code that is actually running.

Do **not** restore a database backup, delete queues, or relax the body-equality
check to "make it match". That check is the only thing that noticed this
mismatch in the first place.

---

## Out of scope, found along the way

- The canonical integration plan's active-phase status is stale. Not corrected
  here; it needs the owner's decision.
- Karlsruhe's six lifecycle and reminder rows were not audited by this task.
