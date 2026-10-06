# Production EasyWeek voucher mailing — runbook (§44 mixed audience, §43 operation)

The working mode: a real operator-curated list, **one €15 voucher per
recipient**, one WhatsApp message each. Every stage is confirmed separately, in
the browser. There is no control that runs two stages, and there will not be one.

Read `docs/easyweek/INTEGRATION_PLAN.md` §§42–44 before the first action.
§44 adds UUID-first customers, a checked phone list and mixed earned/manual batches.

> **This runbook does not authorise anything.** Each real payment and each real
> Meta POST happens only after the owner approves that specific stage and the
> voucher text and terms. A previous approval never carries over to the next
> stage, to the next batch, or to the next month.

**Who does what**

| | |
| --- | --- |
| **Administrator**, once per deployment | §§1–4: variables, migration, the executor, the post-deploy checks |
| **Operator**, every mailing | §§5–9: the whole process, in a browser, with no commands at all |

§43 replaced the CLI as the way a mailing is run. `freeze`, `create`, `pay`,
`deliver` and `refund` on the command line now **refuse** with
`voucher_production_cli_mutation_closed` and do nothing. `status`, `plan` and
`reconcile` still read and still diagnose — see §10.

---

## 0. What this costs and what can go wrong

| | |
| --- | --- |
| Value per recipient | €15 (1500 minor units), exactly — a CHECK constraint, not a default |
| Total exposure | `recipient_count × €15`, equal to what the operator confirmed |
| Maximum Meta messages | one per slot, one attempt each, for the lifetime of the row |

**There is no recipient ceiling, and the operator states the size.** A ceiling
invented here would be this tool deciding how large a real campaign may be, and
one an environment variable could raise would not be a ceiling at all. What
replaces it is an arithmetic identity confirmed before the freeze:

```
approved_exposure_minor = expected_recipient_count * 1500
```

Both numbers must describe the full active snapshot exactly. A wrong count or a
wrong total refuses the **whole** freeze: nothing is truncated to the number that
was typed, nobody is dropped, and the tool never picks a number for anybody.

**A mailing can end up partial, and that is by design.** Slots are walked in slot
order and the first outcome that cannot be proven stops every slot after it **in
that batch**. A realistic bad day on a list of twenty is: nine created and paid,
one unknown, ten never attempted at all. The mailing page says exactly that, per
recipient, and the batch stays halted until a human resolves the one that went
wrong. Other mailings are unaffected.

**Batches are plural.** One preview is frozen into at most one batch, ever;
`campaign_run_id` is UNIQUE. Slot numbers repeat across batches by design — a
slot is a position inside one frozen composition, never a global identifier.

**One voucher per person per campaign period.** The entitlement key is
`provider + company + campaign + customer UUID + both period bounds`, unique
across all production batches in PostgreSQL. The same person in two previews for
the same wave is refused; in a different wave they are allowed, because that is
what a monthly campaign means. **The preview period is the entitlement, not the send date.** A manual
Altegio addition may sit in a later campaign preview; its operator attestation
is shown separately and does not assert a first visit in that preview period.
Do not change the period, basis or provider to evade an existing conflict.
This is not universal protection against historical gifts across providers.

---

## 1. Administrator: environment

Both compose files are in use; every command below runs in the `altegio-api`
container.

```bash
cd /opt/altegio_bot
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml ps
```

### 1.1 The variables this phase reads

All empty or false by default. **The operator never edits any of them**, and they
are not changed between mailings.

| Variable | Meaning |
| --- | --- |
| `EASYWEEK_VOUCHER_PRODUCTION_MAILING_ENABLED` | the fence. False ⇒ every acting stage refuses before any HTTP request, the read-only plan included |
| `EASYWEEK_VOUCHER_PRODUCTION_MAILING_STAFFER_UUID` | **the one approved issuer** — see §2 |
| `EASYWEEK_VOUCHER_PRODUCTION_MAILING_ACCOUNT_UUID` | which POS account is charged, and which one a refund returns to |
| `EASYWEEK_VOUCHER_DELIVERY_HMAC_KEY` | ≥32 bytes; binds each issued code to its batch and slot |
| `EASYWEEK_VOUCHER_DELIVERY_HMAC_KEY_ID` | names which key produced a stored MAC |
| `EASYWEEK_VOUCHER_PRODUCTION_EXECUTOR_ENABLED` | this deployment runs the dedicated executor — see §3 |
| `EASYWEEK_VOUCHER_PRODUCTION_EXECUTOR_POLL_SEC` | how long the executor sleeps on an empty queue (default `2.0`) |

**Do not rotate `EASYWEEK_VOUCHER_DELIVERY_HMAC_KEY` or its key id for this
phase.** Historical bindings must stay verifiable; the key id is what lets an old
MAC be checked against the key that made it.

Ops authentication must be configured, because the new controlling actions
require a real browser session:

| Variable | Meaning |
| --- | --- |
| `OPS_USER` | the Ops account the operator logs in as |
| `OPS_PASS` | its password |
| `OPS_SECRET` | the key the session cookie and the CSRF token are derived from |

With `OPS_USER` or the signing key unset, every voucher-mailing action refuses
with `voucher_production_ops_session_required`. That is deliberate: the Ops
cabinet's historical door allows an unconfigured deployment through for read-only
pages, and a machine nobody finished setting up is the last place a €15 purchase
should be possible.

`EASYWEEK_VOUCHER_PRODUCTION_EXECUTOR_ENABLED=true` is **not** a second fence and
authorises nothing. With it true and the mailing fence false, every stage still
refuses. What it says is that this deployment runs an executor, so a confirmation
will be picked up rather than parked in a queue nobody drains.

Turning on the §36, §37.2 or §41 fence does **not** turn on this one, and this
one does not reopen any of them.

Two things keep working with the fence **closed**, deliberately:

- **status and every page**, because the moment you most need to read what a
  halted mailing left behind is just after an emergency `false`;
- **the delivery webhook**, because `delivered` and `read` are facts about
  messages that have *already* been sent, and dropping them would silently
  corrupt the record of a mailing that really happened.

---

## 2. Administrator: the one approved issuer

Every production voucher is sold by **one** EasyWeek staffer — the owner of all
the salons — whoever the client is, whoever served them, and whoever is logged
into Ops. The UI says so (*«Ваучеры оформляются от Юлии Мюллер»*) and offers no
way to choose or type a staffer.

Set `EASYWEEK_VOUCHER_PRODUCTION_MAILING_STAFFER_UUID` to the approved staffer's
EasyWeek UUID. **Placeholder in this document on purpose** — the real value lives
in the server's environment and in the owner's own records, and is deliberately
not in this repository, in any example, in any fixture or in any log line:

```
EASYWEEK_VOUCHER_PRODUCTION_MAILING_STAFFER_UUID=<approved-issuer-uuid>
```

### 2.1 What "approved" means here, and why a valid UUID is not enough

Eight people work at the Karlsruhe branch. Seven of them have a UUID that passes
every other check in this phase — the branch proof, the account proof, the
template proof and the live staffer listing all stay green while the vouchers are
issued by somebody the owner did not approve.

So the configured value is canonicalised and fingerprinted:

```
SHA-256( "easyweek-voucher-production-issuer-v1:" + str(UUID(configured_value)) )
```

and compared against a constant in the source
(`campaigns/easyweek_voucher_production/issuer.py`). Exactly one UUID on earth
satisfies it. This is an **identity pin**, not a secret, not authorisation and
not the voucher HMAC, and it is deliberately not a second editable environment
value: an expected fingerprint an administrator could set is a pin that pins
nothing. Changing the approved issuer is an owner decision and a line somebody
changes in that file, in a diff a reviewer reads.

Writing the UUID in upper case, in braces or without hyphens is the same
identity — it is canonicalised before hashing, so a cosmetic rewrite is not a
drift.

### 2.2 What is checked, every stage

| Check | Failure code |
| --- | --- |
| something is configured | `voucher_production_staffer_unconfigured` |
| it is the approved issuer | `voucher_production_issuer_not_approved` |
| they still belong to this location, from a **complete** catalogue walk | `voucher_production_issuer_membership_missing` |
| the UUID appears once, not twice | `voucher_production_issuer_membership_ambiguous` |
| the walk could be proven complete at all | `voucher_production_issuer_membership_incomplete` |

The last one is **unknown, not absent**. An unreadable or unprovable catalogue
blocks new stages rather than being read as "they have left": treating unknown as
absence would block a correct mailing, and treating it as presence would let an
unprovable identity sell vouchers. The catalogue is walked **once per stage**, not
once per recipient.

Any of these blocks new FREEZE, CREATE, PAY and DELIVER, for zero external calls.

### 2.3 What the issuer rule deliberately does not touch

- **An allowed pre-send refund.** A refund sells nothing and sends nothing, so it
  must not be stranded because the staffer setting was emptied, corrected or
  pointed at somebody new since the freeze. It keeps its own guards: the frozen
  order, the payment account, the fence, the MAC binding and the absence of any
  send claim.
- **Historical batches, orders and bindings.** A frozen batch keeps the staffer it
  was frozen with, forever. There is no automatic re-attribution.
- **Status, readback and reconciliation.** These stay available when the pin
  fails.
- **The payment account and the WhatsApp sender.** Separate concepts; choosing an
  issuer does not change either.
- **Who authorised the action.** The audit records the Ops account and a digest of
  its session separately from the EasyWeek issuer. With one shared Ops
  credential that identifies the **account and the session, not a person** —
  every audit row says so in `identification_limit`.

---

## 3. Administrator: migration and the executor

### 3.1 Migration

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run alembic current
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run alembic upgrade head
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run alembic heads
```

**What to check.** `alembic heads` must print exactly ONE revision and
`alembic current` must equal it. Do not compare either against a literal written
into this runbook: later phases add revisions, and a hard-coded head here would
send somebody looking for a drift that is simply the next PR.

For the repository state this phase ships with, that single head adds four tables
on top of §42's three:

| Table | What it holds |
| --- | --- |
| `easyweek_voucher_production_approvals` | one immutable plan per offer: the principal, the batch, the stage, the exact target slots, the count, the money, and its own expiry |
| `easyweek_voucher_production_operations` | the durable record that an approval was spent. UNIQUE on `approval_id` |
| `easyweek_voucher_production_stop_requests` | the operator's stop, one row per batch |
| `easyweek_voucher_production_audit` | who did what, when, to which plan and operation |

**Run the migration with the fence still closed.** The tables are created empty;
nothing about deploying them starts a mailing.

### 3.2 The dedicated executor

A confirmed stage is **not** executed inside the HTTP request that confirmed it.
It is stored in PostgreSQL first, and a separate **supervised service** runs it:
`altegio-easyweek-voucher-executor`, defined in `docker-compose.yml` next to the
other workers.

It is not something anybody runs by hand. A stage over a real list takes minutes
and must survive the request that confirmed it, the tab that was closed and the
process that was restarted — and an interactive `docker compose exec` gives it none
of that: the work dies with the terminal, and nobody can tell "still running" from
"died silently".

Start it with the rest of the stack:

```bash
cd /opt/altegio_bot
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml up -d altegio-easyweek-voucher-executor
```

Check it is up, and read its log:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml ps altegio-easyweek-voucher-executor
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml logs --tail 50 altegio-easyweek-voucher-executor
```

Set `EASYWEEK_VOUCHER_PRODUCTION_EXECUTOR_ENABLED=true` for the API as well, so the
UI knows a confirmation can be picked up. **That flag is a statement about the
deployment, not a health check**: it means "this deployment runs an executor", never
"one is alive this second". An executor that is down shows up as an operation
sitting in `queued` on the mailing page — that, and the service's own status, are
the real evidence.

**Supported topology: one API container and ONE executor, with no rolling
deploy.** The executor relies on that: a `running` operation it finds at start-up
cannot belong to a live process, so it marks every one of them `interrupted`
before taking new work. If a second executor were deployed by mistake, the
database still stops them both running one stage — the claim is
`FOR UPDATE SKIP LOCKED` plus a compare-and-set — but the supported shape is one.

It is deliberately **not** the generic campaign worker. That worker's purpose is
to retry what it finds, which is right for an idempotent job and catastrophically
wrong here: EasyWeek publishes no write idempotency key and Meta will deliver
twice. This executor has **no retry path at all** — no backoff, no attempt
ceiling, no requeue. There is no transition from `running` or `interrupted` back
to `queued` anywhere in the schema or the code.

#### Stopping and draining it

A SIGTERM is checked **between** operations, never during one, so a stage that is
mid-flight finishes and records its outcome. The service declares
`stop_grace_period: 120s` for that reason.

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml stop altegio-easyweek-voucher-executor
```

**If a stage outlasts the grace period** Docker kills it. That is not a lost
mailing and it is not a silent one: whatever each slot reached is already in the
per-item ledger, and the operation reads `interrupted` on the mailing page. It is
never retried — resolve it with «Сверить с EasyWeek» and then a fresh confirmation
for what is provably untouched (§9.4).

To stop a mailing **without** stopping the service, use the operator's own control:
«Остановить после текущего запроса» (§8). That is the ordinary way, and the only one
that leaves no operation in doubt.

---

## 4. Administrator: post-deploy checks with the fence CLOSED

Nothing here creates a batch, a voucher, a payment or a message. Do all of it
before the fence is opened.

1. `alembic heads` prints one revision, and `alembic current` equals it (§3.1).
2. Open `/ops/voucher-mailings` in a browser. Expect the page to render, the
   readiness panel to list the blockers by code, and the issuer line to read
   *«Ваучеры оформляются от Юлии Мюллер»*.
3. Request the same page **without logging in**. Expect a redirect to the login
   form, not the page.
4. With the fence closed, press nothing — but confirm the readiness panel names
   `voucher_production_disabled`. No value of any secret appears anywhere on the
   page; readiness is reported as reason codes only.
5. Confirm a mutating CLI command refuses and does nothing:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_production_mailing create --preview-run-id 1 --batch-id 1 --apply --plan-digest x --plan-issued-at 2026-01-01T00:00:00+00:00 --confirm x
```

Expected: `"reasons": ["voucher_production_cli_mutation_closed"]`,
`"external_effect_attempted": false`, exit code 4.

This is the answer **whether the fence is open or shut**. Whether the CLI may
mutate is a property of the command, not of the deployment, so the closure is
checked before the fence — otherwise this smoke would come back
`voucher_production_disabled`, which proves the fence works and says nothing about
the closure you were checking.

6. Confirm the executor service is up and idle:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml ps altegio-easyweek-voucher-executor
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml logs --tail 30 altegio-easyweek-voucher-executor
```

Expect exactly one container, `running`, with an idle log and no queued operations.
Its own status is the evidence that it is alive — the `..._EXECUTOR_ENABLED` flag only
says this deployment runs one.

7. Confirm exactly one executor is supervised, not several:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml ps --format '{{.Service}}' | sort | uniq -c | grep voucher-executor
```

Expect the count to be `1`. Two overlapping executors are not a supported topology:
each interrupts the other's running operations at start-up.

**Then, and only then:** open the fence as a separate, deliberate administrative
step. Opening it creates nothing by itself — every stage still requires its own
confirmation in the UI.

**The first real mailing requires the owner's separate agreement**, and each
irreversible stage of it requires the owner's agreement again. Rendering a page
or seeing a green readiness panel is not that agreement.

---

## 5. Operator: prepare the audience

Everything from here on happens in the browser. No commands, no SSH, no digests,
no timestamps, no ids to type, no `.env`, no container restarts.

1. Build or open an editable EasyWeek Karlsruhe `new_clients_monthly` preview.
   Its automatic `earned_first_visit` candidates keep their source proofs.
   Historical canaries and frozen previews remain locked.
2. To add the transitional Altegio audience, paste one phone per line in
   **«Добавить список: предыдущий визит в Altegio»**. Explicitly confirm the previous Altegio visit and
   assignment to Karlsruhe, then press **«Проверить список»**.
3. Read every result: addable, already present, duplicate, absent customer,
   ambiguous identity, nonempty/unproven history, branch conflict, opt-out or
   missing data. The server accepts only existing external EasyWeek customers
   with exactly zero bookings in this mode — cancelled and future bookings also
   exclude. A timeout is not an empty history.
4. The check changes no Client or recipient. Confirm the exact shown eligible
   subset separately. The server repeats live checks and adds the entire subset
   atomically with counters. A changed fact means check again; it never leaves
   half an unnoticed list. Refresh/double-click returns the same applied result.
5. Existing earned/manual candidates stay unchanged. Ordinary single Add uses
   the same identity resolver; the additional checkbox assigns a missing local
   card to Karlsruhe. It retains historical manual semantics and does not silently
   enable the new zero-booking policy. Remove/Restore retain basis and audit.
6. Both `earned_first_visit` and `operator_manual_selection` may be in one batch.
   Unknown and owner-test bases refuse the whole mailing. Earned is re-proven
   against source/customer/history; manual is the operator's decision, with the
   typed zero-booking policy shown only where it was explicitly applied.

The campaign period and manual rationale are different facts. Declaring an
Altegio visit does not prove its first-visit status, its branch or past gifts.
The UI shows automatic/manual counts, readiness of composition, administrative
mailing fence and executor configuration separately. A preview is not permission
to send. Generic campaign Send remains closed.

Then open **Ops → 🎁 Ваучеры** (`/ops/voucher-mailings`). The preview appears
under «Можно подготовить» with its period and its active count, and
**«Подготовить рассылку»** leads to it. The link carries the id; nothing has to be
copied anywhere.

---

## 6. Operator: freeze the list

On the preparation page:

1. Press **«Проверить состав»**. The page shows the campaign period, the exact
   number of recipients, €15 each, the total, and **who each recipient is** — name
   and the preview row they came from. Checking the list is a read: it never arms
   the confirmation and never creates anything.
2. Read the period. It is the campaign wave, not the month of sending. Manual additions
   retain their own operator rationale rather than claiming an earned first visit.
3. Read the message and the voucher's terms, shown lower on the page. The voucher
   code is an explicit placeholder (`XXXX-XXXX-XXXX`); a real code is never
   displayed anywhere, at any stage.
4. If the list is wrong, go back to the preview editor and change it there. After
   the freeze the composition cannot change.
5. Type the number of recipients and the total in euro, exactly as shown, and
   press **«Проверить и зафиксировать»**. The server compares both against the
   live snapshot and refuses the whole freeze on any mismatch.
6. A confirmation panel states what will happen. Press **«Подтвердить»**.

The freeze writes the batch locally. **No money moves and no message is sent.**

The freeze runs on the executor like every other step, so for a moment there is a
confirmed operation and no batch yet. The page says so and waits; when the batch
exists it takes you to it. **Refreshing or logging in again resumes watching the
same operation** rather than offering a second freeze — if the page shows a step in
progress, that step is yours and it is already running. A freeze the executor
refuses is reported as refused, with its reason, and creates nothing.

---

## 7. Operator: create, pay, deliver — three separate confirmations

The mailing page offers one next step at a time. Each has its own confirmation
panel naming **what this press does**:

| Step | What the confirmation states |
| --- | --- |
| **«Создать ваучеры»** | how many vouchers will be created, and for how much. No payment happens at this step |
| **«Оплатить ваучеры»** | how many will be paid and the exact sum — irreversible after any send |
| **«Отправить сообщения»** | how many messages will be sent, one attempt each, no money |

Each panel also shows the **whole mailing's** size and cost beside the step's own
numbers, so a part is never mistaken for the total. After a partial run the step's
numbers are the remainder — nine of twenty paid means the next payment is for
eleven, and the panel says eleven.

**Finishing a step does not authorise the next one.** There is no control that
runs create → pay → deliver, and the stop between them is the point.

While a step runs the page shows progress and says that the page may be closed:
the work is in PostgreSQL and the executor is doing it. A refresh, a second tab, a
new login or a different device all show the same operation. Pressing a
confirmation twice does not run anything twice — the second press is told that the
step was already accepted.

### What the progress means

Four different facts, never merged:

| Field | What it means |
| --- | --- |
| «Исполнение шагов завершено» | every slot's stage sequence has ended |
| «Meta приняла» | Meta accepted the request |
| «Подтверждена доставка» | a webhook confirmed `delivered` |
| «Прочитано» | a webhook confirmed `read` |

**"Completed" does not mean the messages were delivered, and it certainly does not
mean they were read.** The §41 production run ended with one recipient at `read`
and one at `provider_accepted` — a normal and honest outcome, not a fault. Only
the read counter means somebody has seen their voucher. Webhooks keep arriving
after a step ends and after the fence is closed, so read these again the next day
before drawing conclusions.

---

## 8. Operator: stopping

**«Остановить после текущего запроса»** saves the request for that mailing and
takes effect at the **next** per-recipient claim, atomically.

What it does **not** do:

- it does not cancel a request that is already on the wire. That request's answer
  — including "unknown" — is recorded as it arrives;
- it does not refund anything;
- it does not halt the batch;
- it does not block status, webhooks, reconciliation or an allowed refund.

Pressing it twice is the same stop. The page shows «Остановлено оператором»,
which is deliberately a different banner from an unknown outcome, because the next
step differs: a stop resumes with a fresh confirmation, an unknown needs a
readback first.

**Continuing** is a fresh plan and a fresh confirmation for the slots that plan
authorises. That confirmation is what lifts the stop — there is no separate
"resume" button, because continuing must be a decision rather than a toggle.
Successful and attempted actions are never repeated.

Two refusals you may meet, and both are the system protecting the stop:

| Reason | What it means |
| --- | --- |
| `voucher_production_stop_active` | the step you are confirming was prepared **before** the stop. A plan from before a stop cannot be the decision to carry on past it — prepare the step again and confirm that |
| `voucher_production_operation_in_flight` | another step of this mailing is still running. Wait for it to finish, then prepare the next one |

So a second browser tab holding a step prepared earlier cannot lift your stop, and
confirming something in one tab cannot resume a step that is already running in
another.

---

## 9. Operator: unknown outcomes, reconciliation and refunds

### 9.1 An unknown outcome

The first outcome that cannot be proven stops every slot after it in that batch.
The page says so, marks the mailing as needing reconciliation, and offers
**«Сверить с EasyWeek»**. What it does **not** offer is a retry: for a send, an
unknown may mean the customer is already holding the code.

1. Press **«Сверить с EasyWeek»**. It performs reads only and records what it
   read.

   If it answers `voucher_production_reconcile_busy`, a step of this mailing is
   executing right now. That is not a failure: a readback exists to reinterpret
   claims left behind by a process that died, and doing it to a live one would move
   the row out from under a request whose answer is still on its way. Wait for the
   step to finish — the page shows when it has — and press it again.
2. If reconciliation resolves the slot, the mailing continues from a fresh,
   confirmed plan for the slots that are provably untouched.
3. If it does not resolve, stop and escalate. Creating a second batch to get
   around an unknown is not a recovery — it is a second mailing, and the
   entitlement rule refuses it anyway.

### 9.2 A draft left open in EasyWeek

A CREATE whose answer was lost may have left an open draft in the POS. The mailing
page shows **«Нужно закрыть draft в EasyWeek: да»** when that is possible, and the
draft must be closed **by hand in EasyWeek** — this project has proven no
cancellation API and does not invent one. Afterwards press
**«Сверить с EasyWeek»** so the ledger records what is actually there.

### 9.3 A refund

A **«Вернуть оплату»** button appears next to a recipient only while a refund is
allowed: the slot is paid and **nothing was ever sent for it**. The server enforces
the same rule independently — a refund after any send claim or attempt is refused
by the plan, by the claim and by a CHECK constraint, because returning the money
for a code somebody is already holding is worse than losing the €15.

A refund is one named recipient of one named mailing, with its own confirmation —
and the confirmation **names the client**, not only the slot number, so there is no
way to return the wrong person's money by misreading a row.

Which rows offer the button comes from the server's own rule rather than from the
page's idea of it, so what you can press and what the server will accept are the
same list.
It stays available when the batch is halted — a halt is exactly when an untouched
paid slot most needs its money back — and when the issuer configuration has
changed or disappeared.

### 9.4 Interrupted

If the executor died mid-stage, the operation reads **«прервано — нужна сверка»**.
This is terminal and is never retried: a slot may hold a committed claim whose
request went out. Reconcile, then confirm a fresh plan for what is provably
untouched.

---

## 10. What the CLI is still for

Read-only, and most useful exactly when the acting path is blocked.

Every mailing, newest first:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_production_mailing status
```

One mailing in full:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_production_mailing status --batch-id BATCH
```

`status` reads the durable ledger with no HTTP at all, and works with the fence
closed. `plan --stage ...` performs reads only and writes nothing. `reconcile`
reads the outside world back and records what it read.

`freeze`, `create`, `pay`, `deliver` and `refund` refuse. There is no flag that
reopens them, `--apply` included. The internal executor is not an exception to
this: it runs a stored, authorised UI action, never a command somebody typed.

---

## 11. Closing the fence afterwards

Set `EASYWEEK_VOUCHER_PRODUCTION_MAILING_ENABLED=false` in the environment file
first.

Then recreate **both** the API and the executor. Two things matter here.

**A plain `docker compose restart` is not enough and must not be used:** `restart`
stops and starts the existing container, which keeps the environment it was created
with, so the fence would still read `true` inside it while the file on disk says
`false`. The environment is only re-read when the container is created again.

**Recreating only the API is not enough either.** The executor is a separate
container with its own copy of the environment, and it is the process that actually
performs a stage. An API recreated with the fence shut would refuse new
confirmations while an executor still holding `true` went on executing anything
already queued.

```bash
cd /opt/altegio_bot
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml up -d --no-deps --force-recreate altegio-api altegio-easyweek-voucher-executor
```

`--no-deps` keeps this to those two services; `--force-recreate` is what makes the
new environment take effect.

Then verify the value **inside each container**, not in the file:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api printenv EASYWEEK_VOUCHER_PRODUCTION_MAILING_ENABLED
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-easyweek-voucher-executor printenv EASYWEEK_VOUCHER_PRODUCTION_MAILING_ENABLED
```

Expected output from both, exactly:

```
false
```

Anything else — `true`, or no output at all — means the fence is still open in that
container or the variable is not set as intended. Do not stop here; fix it and
verify again.

Pages, `status` and the delivery webhooks keep working with the fence closed, so
the mailing state stays readable afterwards.

---

## 12. Limits and rollback of the new infrastructure

### 12.1 Limits worth knowing

- **The executor is a single point of execution.** If the service is not running,
  confirmations stay `queued` and nothing happens. The confirmation refuses up
  front when `EASYWEEK_VOUCHER_PRODUCTION_EXECUTOR_ENABLED` is false, but that
  flag means "this deployment runs one", never "it is alive this second". An
  executor that died shows up as an operation sitting in `queued` on the mailing
  page, and as a stopped container in `docker compose ps`.
- **A fence change must reach the executor too.** It is a separate container with
  its own copy of the environment; recreating only the API leaves it running on the
  old settings. See §11.
- **A rolling deploy is not supported.** The executor interrupts every `running`
  operation it finds at start-up, which is correct for one executor and wrong for
  two overlapping ones.
- **Approvals expire after 30 minutes**, measured from when the plan was issued.
  Queue time is not reading time: an operation picked up after that is refused
  with zero external effects and needs a new plan and a new confirmation.
- **A shared Ops account cannot distinguish people.** The audit records the
  account and a digest of the session, and says so.
- **Any ledger change between a plan and its confirmation refuses the stage.**
  That is safe and occasionally inconvenient: the answer is to plan again.

### 12.2 Rollback

**Historical §43 scopes below.** On a §44 deployment, first read §14. The new
migration refuses these targets while new identity/proof/plan/v2 ledger data
exists. A database backup is not permission to erase that data.


Two different operations get called "the rollback", and they remove different
things. Decide which one is wanted before running anything.

The §43 chain is three revisions, in this order:

```
e2c7b4f16a83  →  a4f1c9d26b70  →  c7e3b8a14f29
```

* `a4f1c9d26b70` created the four §43 tables.
* `c7e3b8a14f29` added `stop_generation_at_plan` to the approvals table, with its
  `>= 0` CHECK constraint.

| | **Scope A — the stop-generation fix** | **Scope B — all of §43** |
|---|---|---|
| Target revision | `a4f1c9d26b70` | `e2c7b4f16a83` |
| Schema change | drops `stop_generation_at_plan` and its CHECK | drops all four §43 tables |
| What is lost | which stop each stored plan was built under | every approval, operation, stop request and audit row |
| Mailing still possible afterwards | yes, on the matching older application | **no** |

**`alembic downgrade -1` is scope A, and this runbook used to describe it as scope
B.** One step back from the §43 head `c7e3b8a14f29` lands on `a4f1c9d26b70`: the
four tables stay exactly where they are, and only the stop-generation column and its
constraint go. Counting steps is how that sentence became wrong — the count was
right when there was one §43 revision and silently meant something else as soon as
there were two — so every command below names its target revision instead. The chain
is deliberately **not** re-pointed to make `-1` mean something tidier: rewriting
published revisions is a far worse problem than a longer command.

#### The order matters

The schema and the application version have to move together, and the application
must not be serving across the gap: this version's code reads
`stop_generation_at_plan` on every plan and every confirmation, so it cannot run on
a database where that column has already been removed.

1. Close the fence (§11), so nothing new can be confirmed.
2. Let any operation in flight finish, or stop the executor and treat what it was
   doing as `interrupted` and in need of a readback (§10).
3. Stop **both** application containers. Nothing serves while the schema moves.
4. Back the database up.
5. Run the downgrade **from the image that is deployed right now**. It is the only
   one that contains the revision scripts the database is currently stamped with; an
   older image cannot walk down from a revision it has never heard of.
6. Only then deploy the older application version.
7. Verify, before anything is opened again.

#### Steps

```bash
cd /opt/altegio_bot
```

Stop the two application containers (PostgreSQL stays up — the migration needs it):

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml stop altegio-api altegio-easyweek-voucher-executor
```

Back up. This is not optional for either scope, and for scope B it is the only copy
of the authorisation history that will exist afterwards:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec postgres sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc -f /tmp/voucher-rollback.dump'
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml cp postgres:/tmp/voucher-rollback.dump ./voucher-rollback.dump
```

Then **one** of the two downgrades.

Scope A — undo the stop-generation fix only:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml run --rm --no-deps altegio-api uv run alembic downgrade a4f1c9d26b70
```

Scope B — undo all of §43:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml run --rm --no-deps altegio-api uv run alembic downgrade e2c7b4f16a83
```

Now deploy the application version that matches the schema just restored — for
scope A the commit before this fix, for scope B the commit before PR-20 — and bring
the stack back up with the fence still closed.

#### Verify before opening anything

The stamped revision must be the target, not the head:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml run --rm --no-deps altegio-api uv run alembic current
```

The four §43 tables: four of them after scope A, none after scope B.

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec postgres sh -c "psql -U \$POSTGRES_USER -d \$POSTGRES_DB -t -c \"SELECT count(*) FROM information_schema.tables WHERE table_name IN ('easyweek_voucher_production_approvals', 'easyweek_voucher_production_operations', 'easyweek_voucher_production_stop_requests', 'easyweek_voucher_production_audit')\""
```

The stop-generation column: `0` after either scope.

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec postgres sh -c "psql -U \$POSTGRES_USER -d \$POSTGRES_DB -t -c \"SELECT count(*) FROM information_schema.columns WHERE table_name = 'easyweek_voucher_production_approvals' AND column_name = 'stop_generation_at_plan'\""
```

The historical ledgers, which neither scope touches. Expected: `5`, in both.

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec postgres sh -c "psql -U \$POSTGRES_USER -d \$POSTGRES_DB -t -c \"SELECT count(*) FROM information_schema.tables WHERE table_name IN ('easyweek_voucher_production_batches', 'easyweek_voucher_production_batch_items', 'easyweek_voucher_production_batch_attempts', 'easyweek_voucher_snapshot_batches', 'easyweek_voucher_canary_ledger')\""
```

Going forward again is `alembic upgrade head`, after the application version that
matches it is deployed, in that order for the same reason.

#### What a rollback costs

No §35–§42 table, row, constraint or HMAC binding is referenced by the §43 tables
and every foreign key points the other way, so the historical ledgers and the frozen
production batches survive both scopes intact. That is the part that is safe.

What scope B destroys is the record of **who authorised what**: the approvals, the
operations, the stop requests and the audit rows all go, and a `pg_dump` file is
then the only evidence that a mailing was ever authorised by anybody. It is a
decision to abandon that history, not a maintenance step, and it belongs to backing
out this PR rather than to operating it. Scope A costs much less — the stop
generation each stored plan was built under — and that is still a fact an
investigation of a stop might want, so the backup is taken either way.

With §43 rolled back there is **no supported way to run a mailing**: the CLI
mutations stay closed and no flag reopens them. A rollback is a decision to stop
mailing, not a way back to the terminal process.

---

## 13. What this does not do

- **Redemption is not tracked.** Nothing here records whether a voucher was ever
  applied to a booking. The ledger models issue, payment, delivery and the webhook
  statuses, and stops there. Owner-reported manual evidence that a voucher was
  used is exactly that — not a fact this system can prove.
- **No scheduler and no retry.** No generic campaign runner, `MessageJob`, Outbox
  row, scheduler, retry, resume or follow-up is involved at any point. The
  executor runs one confirmed stage and stops.
- **No automatic audience import.** The list comes from the preview editor, by
  hand.
- **One branch, one campaign, one amount, one template.** Karlsruhe,
  `new_clients_monthly`, €15, `kitilash_ka_new_client_voucher_v1` in German, three
  BODY parameters.
- **Owner-test and unknown bases are refused.** Earned and manual are served
  together with distinct per-recipient proofs under §44.
- **No staffer choice.** One approved issuer, configured on the server (§2).
- **§41 and the historical canaries are untouched.** Their rows, constraints and
  HMAC bindings are exactly as they were, and their recipients are excluded from
  this phase.

Nothing about a successful mailing authorises a campaign. Every report keeps
saying `campaign_send_authorized=false`, `bulk_delivery_authorized=false` and
`ready_for_send=false`, and they stay false until a separate PR says otherwise.


## 14. §44 identity, migration and closed-fence smoke

No new environment setting or HMAC rotation is needed. `f6a8d2c91b47` follows
`c7e3b8a14f29` and is the one head for this change. It adds:

- typed unique workspace EasyWeek UUID on Client, nullable numeric ID only for
  such a UUID identity, and time of operator branch assignment;
- recipient manual policy plus API-check and operator-attestation timestamps;
- private, operator/session/preview-bound list plans (15-minute TTL);
- version 2 mixed composition and per-item policy/source proof fields.

The customer API does not prove numeric ID. A local card created here leaves it
NULL; no ID is invented, no Altegio card is converted, and the visit count stays
NULL. The first real booking webhook proves its booking/customer identity via
GET and attaches the real webhook numeric ID to this same Client. Identity
conflicts leave the event recoverable without creating a second card. Opt-outs
survive. No reminders, planner, messages or external customers are created by Add.

Old manual recipients keep policy NULL. Old batches and HMAC bindings retain
version 1 semantics; deploying version 2 gives an old approval no extra authority.
Basis, policy, source and customer drift refuse continuation. A new EasyWeek
booking after PAY blocks DELIVER for the stage; the frozen list and total stay
unchanged. Use reconciliation and the existing per-item pre-send refund when
allowed. Do not remove a frozen recipient or create a replacement batch.

**List limits protect the API, not the size of a mailing:** 16 KiB / 200 input
lines / 100 distinct phones per check, at most two reads in parallel, 15 seconds
per contact and 90 seconds per list, three checks per minute per operator.
Multiple separately confirmed lists can extend the same editable preview. The
server retains no raw API payload; applied plans discard contact material, and
expired plan contact material is cleared during subsequent list preparation.
An expired check must be repeated. With a shared Ops account audit identifies
that account/session, not a distinct human.

**Rollout:** close the mailing fence; stop/drain application and executor together;
back up; apply the new migration and matching application; start one API and one
executor. No rolling deploy and no automatic data import. Before opening the fence:

1. Verify one Alembic head and current revision matches it.
2. Log into Ops and open an editable preview with automatic candidates. Verify
   the automatic/manual counts, period, identity/composition status, administrative
   fence, executor state and working transition to «Ваучеры».
3. Confirm the bulk form offers the two explicit operator attestations and separate
   check/add actions. A check, if separately authorised against real contacts,
   performs only GET and does not alter recipients. Rendering the form needs no
   provider reads. Do not confirm real additions merely to test deployment.
4. The mailing page must name the closed administrative fence; attempting a new
   stage cannot execute it. Status, existing delivery webhooks and readable ledgers
   remain available. Generic send-real stays closed.
5. Check no unexpected queued operation or migration-created Client/recipient exists.
   No real FREEZE/CREATE/PAY/DELIVER is needed for this smoke.

**Rollback:** a downgrade to `c7e3b8a14f29` is allowed only while no new UUID
identities, policy proofs, list plans or version 2 batches exist. Otherwise it
fails before removing any column/table with `PR-21 downgrade refused`. Even a
Client whose numeric ID has since been learned still has UUID identity evidence;
that evidence cannot be erased by downgrade. Preserve the database, close the
fence and use a reviewed forward fix. Do not delete rows to make downgrade pass.
On an unused installation only, with both application processes stopped and a
backup secured, the administrator can explicitly target the parent:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml run --rm --no-deps altegio-api uv run alembic downgrade c7e3b8a14f29
```

Deploy the matching older application only after successful schema rollback.
Historical §43 rollback scopes in §12.2 remain documentation of that phase;
they do not override the §44 data-preservation guard.
