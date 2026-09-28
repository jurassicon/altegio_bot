# Production EasyWeek voucher mailing (§42) — runbook

The working mode: a real operator-curated list, **one €15 voucher per
recipient**, one WhatsApp message each. Every stage is a separate command behind
a separately approved plan. There is no command that runs two stages, and there
will not be one.

Read `docs/easyweek/INTEGRATION_PLAN.md` §42 before the first command.

> **This runbook does not authorise anything.** Each real payment and each real
> Meta POST happens only after the owner approves that specific stage, that
> specific plan digest, and the voucher text and terms. A previous approval
> never carries over to the next stage, to the next batch, or to the next month.

---

## 0. What is different from §41, and why it matters to you

§41 was a controlled batch: at most five recipients, at most €75, exactly one
batch ever, all of it as literals in the code and CHECK constraints in the
database. That phase is finished and untouched — its ledger, its rows and its
one completed batch stay exactly as they are.

This phase is the one you actually run every month, and it differs in two ways
that change what you have to type.

### There is no recipient ceiling — you state the size yourself

There is deliberately no maximum number of recipients and no `--limit`. A
ceiling invented here would be this tool deciding how large a real campaign may
be, and a ceiling an environment variable could raise would not be a ceiling at
all.

What replaces it is an **arithmetic identity you have to state before the
freeze**:

```
approved_exposure_minor = expected_recipient_count * 1500
```

A freeze requires `--expected-recipient-count` and `--approved-exposure-minor`,
and **both must describe the full active snapshot exactly**:

| | |
| --- | --- |
| `--expected-recipient-count` | must be > 0 **and** equal the number of active recipients actually in the preview |
| `--approved-exposure-minor` | must equal `--expected-recipient-count × 1500` (€15 each, in minor units) |

A missing number, a wrong count or a wrong total **refuses the whole freeze**.
Nothing is truncated to the number you typed, nobody is dropped, and the tool
never picks a number for you. If you approve four people for a snapshot of six,
you get a refusal — not a mailing to the first four.

Both numbers are stored on the batch forever, and CHECK constraints require them
to equal what was actually frozen. A batch whose approved numbers do not
describe its own composition cannot exist.

### Batches are plural — every command names one

§41 had one batch, so a slot number was enough to address anything. This phase
runs again next month and may have two mailings in flight in one week, so:

- **`--batch-id` is required on every command after the freeze**, together with
  `--preview-run-id`, and the two are compared: a batch that is not bound to
  that preview is a refusal.
- **slot numbers repeat across batches.** Slot 1 exists in every mailing. A slot
  is a position inside one frozen composition, never a global identifier.
- there is **no "latest batch"** and nothing resolves one. A digit slip is a
  refusal, not a stage against last month's audience.

`status` prints the ids. `freeze` prints the id it just created.

---

## 1. Before anything

The mailing acts on **every active recipient of one completed EasyWeek
preview**:

- `provider=easyweek`, `company_id=322579` (Karlsruhe), campaign
  `new_clients_monthly`;
- every active recipient's `recipient_basis` is `operator_manual_selection`;
- every active recipient's status is `candidate`;
- there is at least one of them;
- each has a stored EasyWeek customer UUID and exactly one local EasyWeek
  `Client`.

**Nothing is filtered.** An earned or owner-test recipient sitting in the same
preview refuses the whole mailing. Curate the snapshot in the preview editor
first, then freeze.

A manual basis is an operator's decision, not a proven first visit. Every report
says so: `first_visit_proof=not_applicable`.

Build a **fresh** preview and pick the recipients again. Previews and recipients
already spent by §36, §37.2 or §41 are refused, and readiness from an older run
is not carried forward.

### One voucher per person per campaign period

The entitlement key is `provider + company + campaign + customer UUID + both
campaign period bounds`, unique across **all** production batches in
PostgreSQL. So:

- the same person in **two previews for the same wave** is refused;
- the same person in a **different wave** is allowed — that is what a monthly
  campaign means;
- two operators freezing two previews for the same person race, and exactly one
  of them commits.

**The period is the entitlement, not the send date.** A transitional **August**
audience mailed in **October** is an **August** entitlement.

### What this can cost, and what can go wrong

| | |
| --- | --- |
| Value per recipient | €15 (1500 minor units), exactly — a CHECK constraint, not a default |
| Total exposure | `recipient_count × €15`, and equal to what you approved |
| Maximum Meta messages | one per slot, one attempt each, for the lifetime of the row |

**A mailing can end up partial, and that is by design.** Slots are walked in
slot order, and the first outcome that cannot be proven stops every slot after
it **in that batch**. So a realistic bad day on a list of twenty looks like:
nine vouchers created and paid, one unknown, ten never attempted at all. The
report says exactly that, per slot, and the batch is left halted until a human
resolves the one that went wrong. Other batches are unaffected.

---

## 2. Environment

Both compose files are in use; every command below runs in the `altegio-api`
container.

```bash
cd /opt/altegio_bot
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml ps
```

Required variables (all empty or false by default):

| Variable | Meaning |
| --- | --- |
| `EASYWEEK_VOUCHER_PRODUCTION_MAILING_ENABLED` | the §42 fence. False ⇒ every acting stage refuses before any HTTP request, the read-only plan included |
| `EASYWEEK_VOUCHER_PRODUCTION_MAILING_STAFFER_UUID` | who performs the sales |
| `EASYWEEK_VOUCHER_PRODUCTION_MAILING_ACCOUNT_UUID` | which POS account is charged |
| `EASYWEEK_VOUCHER_DELIVERY_HMAC_KEY` | ≥32 bytes; binds each issued code to its batch and slot |
| `EASYWEEK_VOUCHER_DELIVERY_HMAC_KEY_ID` | names which key produced a stored MAC |

Turning on the §36, §37.2 or §41 fence does **not** turn on this one, and this
one does not reopen any of them.

Two things keep working with this fence **closed**, deliberately:

- **`status`**, because the moment you most need to read what a halted mailing
  left behind is just after an emergency `false`;
- **the delivery webhook**, because `delivered` and `read` are facts about
  messages that have *already* been sent, and dropping them would silently
  corrupt the record of a mailing that really happened.

There is deliberately no variable for the batch size or the exposure. See §0.

---

## 3. Migration

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run alembic current
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run alembic upgrade head
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run alembic heads
```

**What to check.** `alembic heads` must print exactly ONE revision, and
`alembic current` must equal it. Do not compare either against a literal written
into this runbook: later phases add revisions, and a hard-coded head here would
send an operator looking for a drift that is simply the next PR.

For the repository state this phase ships with, that single head is
`e2c7b4f16a83`, which adds the three §42 tables on top of `d7b2f6a4c318`.
`b3f7c2a90d14` remains the historical §41 migration — it must still be present
in the chain (`alembic history` shows it) and is no longer the global head.

**Run the migration with the fence still closed.** The tables are created empty;
nothing about deploying them starts a mailing.

---

## 4. Status — read-only, database only, no network

Every mailing, newest first:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_production_mailing status
```

One mailing in full:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_production_mailing status --batch-id BATCH
```

The same state is readable in the browser at
`/ops/docs/voucher-production-mailing`. That page has nothing to click on
purpose, and it links each mailing to its id.

### Read the four delivery facts separately

`status` reports four different things and never merges them:

| Field | What it means |
| --- | --- |
| `execution_completed` | every slot's stage sequence has ended |
| `provider_accepted_count` | Meta accepted the request |
| `webhook_delivered_count` | a webhook confirmed `delivered` |
| `webhook_read_count` | a webhook confirmed `read` |

**`completed` does not mean the messages were delivered, and it certainly does
not mean they were read.** The §41 production run ended with one recipient at
`read` and one at `provider_accepted` — that is a normal and honest outcome, not
a fault. Only `webhook_read_count` means somebody has seen their voucher.

---

## 5. Prepare the audience

Build a fresh, **manual-only** preview in the existing editor and pick the real
recipients by hand. Then, before you plan anything:

1. count the active recipients in the editor;
2. multiply by 15 € — that is your `--approved-exposure-minor` in minor units
   (`count × 1500`);
3. check the period shown on the preview is the wave the vouchers are *earned
   for*.

Do not reuse a preview that has already been frozen, and do not reuse the §41
test preview or its recipients.

---

## 6. Plan a stage — read-only, GET-only

A plan performs GETs only. It creates no batch, sends nothing and writes
nothing.

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_production_mailing plan --stage freeze --preview-run-id RUN --expected-recipient-count COUNT --approved-exposure-minor EXPOSURE
```

Read `ready`, `reasons`, `snapshot.composition` and `snapshot.baseline` before
deciding anything.

**Check the count and the money.** `snapshot.composition` prints both what was
observed and what you approved:

- `observed_active_recipients` — what is really in the preview;
- `recipient_count` — the size the batch would be frozen at;
- `expected_recipient_count` / `approved_exposure_minor` — what you typed;
- `total_exposure_minor` — what it will actually cost.

If your numbers do not match, the plan is **not ready** and names which one is
wrong:

| Reason | Meaning |
| --- | --- |
| `voucher_production_approved_count_missing` | you did not supply a count, or it was ≤ 0 |
| `voucher_production_approved_exposure_missing` | you did not supply an exposure, or it was ≤ 0 |
| `voucher_production_approved_count_mismatch` | your count is not the number of active recipients |
| `voucher_production_approved_exposure_mismatch` | your exposure is not `count × 1500` |

Fix the preview or fix the numbers. Do **not** look for a flag that lets the
stage proceed anyway; there is none.

**Check the campaign period.** `snapshot.composition.campaign_period` reads as
`YYYY-MM-DD..YYYY-MM-DD`. It must be the period the vouchers are *earned for*,
which is not the period you are sending in:

> A transitional **August** audience mailed in **October** is an **August**
> entitlement. The plan must read `2026-08-01..2026-08-31`. If it reads
> `2026-10-01..2026-10-31`, the preview was built for the wrong wave — stop and
> rebuild it. The send date never replaces the entitlement period, and a batch
> frozen against the wrong one can hand a second €15 to people an earlier batch
> already served.

The period is part of the frozen composition and part of the digest, so
changing it invalidates any approval taken before the change.

Keep three values from the output for the next command:

- `plan_digest`
- `plan_issued_at`
- `confirmation_phrase`

They expire in **30 minutes**, and the TTL is deliberately not longer for a
bigger list. A plan for one stage never authorises another, a plan for one batch
never authorises another, and a plan built for one composition never authorises a
different one.

**Baseline.** `snapshot.baseline.baseline_version` must read `2026-09-27-43` and
`mismatched_fields` must be empty. That baseline expects `services_count=43` and
`all_services_count=43`, the owner-approved live catalogue of 27.09.2026,
together with every other frozen template field unchanged.

A drift stops the stage; it is never adapted to. Take a drift to the owner before
doing anything else — in particular, a report naming `services_count` and
`all_services_count` means the catalogue moved again, and the answer is a
reviewed code change to the baseline, never an edit here.

§37.2 keeps its own historical baseline `2026-09-15-42` at 42/42 and §41 its own
constant. A §42 plan reading 42/42 is a refusal, not a fallback.

---

## 7. FREEZE — a local mutation, no money and no message

Freezing writes the batch header and its slots. **Nothing leaves the process.**
After it, the preview can no longer be edited, discarded or deleted, and the
composition, the count and the money are immutable.

Only after the owner approved this exact digest:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_production_mailing freeze --preview-run-id RUN --expected-recipient-count COUNT --approved-exposure-minor EXPOSURE --apply --plan-digest DIGEST --plan-issued-at ISSUED_AT --confirm PHRASE
```

The count and the exposure are supplied **again** here and re-checked inside the
freeze transaction against the rows that transaction can see — so a recipient
added or removed between the plan and the apply refuses the freeze rather than
being silently included or excluded.

**Write down the `batch_id` the report prints.** Every command from here on
needs it.

Check the printed `batch.items`: the slots, the recipient row ids and the markers
are what every later stage will act on, and `batch.campaign_period` is the wave
they are entitled to.

---

## 8. CREATE — one external effect per slot, no money yet

Plan first (§6, `--stage create`, with `--batch-id`), then, only with the owner's
approval of that exact digest:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_production_mailing create --preview-run-id RUN --batch-id BATCH --apply --plan-digest DIGEST --plan-issued-at ISSUED_AT --confirm PHRASE
```

Exit codes: `0` proven, `3` **unknown or halted — stop and reconcile**, `4`
refused (nothing left the process), `6` proven with open drafts to clean up.

Read `external_calls.create` and `slots[]`. Compare `external_calls.create`
against the number of slots the plan targeted: that is how "one call per slot"
stops being a promise and becomes something you can read off a report.

An `unknown` never means "try again". Go to §13.

---

## 9. Reconcile — GET-only

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_production_mailing reconcile --preview-run-id RUN --batch-id BATCH
```

Reads only. It moves a slot forward only where an exact order readback proves
the move, and it lifts the halt only when no slot is unresolved any more.

---

## 10. PAY — real money, irreversible after any send

Plan first (§6, `--stage pay`). Then, only with the owner's **separate**
approval for real payments of this exact total:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_production_mailing pay --preview-run-id RUN --batch-id BATCH --apply --plan-digest DIGEST --plan-issued-at ISSUED_AT --confirm PHRASE
```

Then reconcile again (§9).

---

## 11. DELIVER — the messages reach real people

Requires, in addition to the plan: the approved Meta template
`kitilash_ka_new_client_voucher_v1`, an active sender, and the owner's approval
of the voucher text and terms.

Plan first (§6, `--stage deliver`). Then:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_production_mailing deliver --preview-run-id RUN --batch-id BATCH --apply --plan-digest DIGEST --plan-issued-at ISSUED_AT --confirm PHRASE
```

`provider_accepted` means Meta accepted the request. It does **not** mean
delivered: `delivered` and `read` are written only by the existing webhook, on
the exact provider message id. See §4 for the four separate counters.

One attempt exists per slot for the lifetime of that row — the database caps it
at one. A `send_unknown` is a case for a human, never for a second command, and
it stops every slot after it.

---

## 12. REFUND — one named slot of one named batch, pre-send only

Available only for a slot nothing has ever been sent for. Forbidden — by plan,
by claim and by a CHECK constraint — from `send_claimed`, `send_unknown`,
`send_rejected`, `provider_accepted`, `delivered` and `read`.

There is deliberately no refund-everything, at any batch size.

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_production_mailing plan --stage refund --preview-run-id RUN --batch-id BATCH --slot K
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_production_mailing refund --preview-run-id RUN --batch-id BATCH --slot K --apply --plan-digest DIGEST --plan-issued-at ISSUED_AT --confirm PHRASE
```

A refund stays available while the batch is **halted**, and deliberately: a halt
is exactly when an untouched paid slot most needs its money back.

There is no documented cancel endpoint for an open draft. If a CREATE succeeded
and PAY never ran for that slot, close the draft by hand in the EasyWeek
dashboard using that slot's `reconciliation_marker`; a later `reconcile` records
that as an observation.

---

## 13. When something is unknown

1. **Do not repeat the command.**
2. Run `status --batch-id BATCH` and `reconcile`.
3. For an unknown CREATE or PAY: find the order in the EasyWeek dashboard by the
   slot's `reconciliation_marker` that the report prints.
4. For an unknown DELIVER: check the WhatsApp line. The voucher may already be
   in the customer's hands — a refund is forbidden for that slot from here on,
   and no second send is possible.
5. Report to the owner before any further command. The batch stays halted, so no
   later slot of it can be claimed until it is resolved. Other batches are
   unaffected.

### Continuing a batch that was interrupted

Allowed, and it is the same batch — never a new one:

1. `reconcile` until every unresolved slot is either proven or explicitly parked
   for a human;
2. build a **fresh** plan for the stage;
3. check `snapshot.target_slots` — it will list only the slots that were
   provably never started;
4. get a **new** confirmation and apply it with the **same `--batch-id`**.

Successful effects are never repeated: a slot already `created` is not in
`target_slots`, and a slot whose delivery was claimed or attempted can never be
sent again. Creating a second batch to get around an unknown is not a recovery —
it is a second mailing, and the entitlement rule will refuse it anyway.

---

## 14. Afterwards — turning the fence off

Set `EASYWEEK_VOUCHER_PRODUCTION_MAILING_ENABLED=false` in the environment file
first.

Then recreate the API service. **A plain `docker compose restart` is not
enough and must not be used here:** `restart` stops and starts the existing
container, which keeps the environment it was created with, so the fence would
still read `true` inside it while the file on disk says `false`. The
environment is only re-read when the container is created again.

```bash
cd /opt/altegio_bot
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml up -d --no-deps --force-recreate altegio-api
```

`--no-deps` keeps this to the one service; `--force-recreate` is what makes the
new environment take effect.

Then verify the value **inside the container**, not in the file:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api printenv EASYWEEK_VOUCHER_PRODUCTION_MAILING_ENABLED
```

Expected output, exactly:

```
false
```

Anything else — `true`, or no output at all — means the fence is still open or
the variable is not set as intended. Do not stop here; fix it and verify again.

`status` keeps working with the fence closed, so the mailing state stays
readable afterwards:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_production_mailing status --batch-id BATCH
```

Check the four delivery counters here rather than assuming the mailing landed:
`provider_accepted_count` is what Meta took, and `webhook_delivered_count` and
`webhook_read_count` are what actually arrived. Webhooks keep being recorded
after the fence is closed, so these numbers can still rise — read them again the
next day before drawing conclusions.

---

## 15. What this does not do

- **Redemption is not tracked.** Nothing here records whether a voucher was ever
  applied to a booking. The ledger models issue, payment, delivery and the
  webhook statuses, and stops there. Owner-reported manual evidence that a
  voucher was used is exactly that — it is not a fact this system can prove.
- **No scheduler, no worker, no retry.** No generic campaign runner,
  `MessageJob`, Outbox row, worker, scheduler, retry, resume or follow-up is
  involved at any point. Every stage of every mailing is a human typing a
  command after reading a plan.
- **No automatic audience import.** The list comes from the preview editor, by
  hand.
- **One branch, one campaign, one amount, one template.** Karlsruhe,
  `new_clients_monthly`, €15, `kitilash_ka_new_client_voucher_v1` in German.
- **`earned_first_visit` is not served here**, and a mixed-basis snapshot is
  refused rather than filtered.
- **§41 is untouched.** Its singleton batch, its rows, its constraints and its
  HMAC bindings are exactly as they were. Its recipients are excluded from this
  phase.

Nothing about a successful mailing authorises a campaign. Every report keeps
saying `campaign_send_authorized=false`, `bulk_delivery_authorized=false` and
`ready_for_send=false`, and they stay false until a separate PR says otherwise.
