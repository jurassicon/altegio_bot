# Controlled EasyWeek voucher snapshot batch (§41) — runbook

Up to five manually selected recipients, one €15 voucher each, one WhatsApp
message each. **Maximum financial exposure: €75.** Every stage is a separate
command behind a separately approved plan. There is no command that runs two
stages, and there will not be one.

Read `docs/easyweek/INTEGRATION_PLAN.md` §41 before the first command.

> **This runbook does not authorise anything.** Each real payment and each real
> Meta POST happens only after the owner approves that specific stage, that
> specific plan digest, and the voucher text and terms. A previous approval
> never carries over to the next stage.

---

## 0. Before anything

The batch acts on EVERY active recipient of ONE completed EasyWeek preview:

- `provider=easyweek`, `company_id=322579` (Karlsruhe), campaign
  `new_clients_monthly`;
- every active recipient's `recipient_basis` is `operator_manual_selection`;
- every active recipient's status is `candidate`;
- there are between 1 and 5 of them;
- each has a stored EasyWeek customer UUID and exactly one local EasyWeek
  `Client`.

**Nothing is filtered.** An earned or owner-test recipient sitting in the same
preview refuses the whole batch, and a sixth recipient refuses it too. Curate
the snapshot in the preview editor first, then freeze.

A manual basis is an operator's decision, not a proven first visit. Every report
says so: `first_visit_proof=not_applicable`.

Build a **fresh** preview and pick the recipients again. The §37.2 canary's
preview and its recipient may not be reused, and readiness from an older run is
not carried forward.

### What this can cost, and what can go wrong

| | |
| --- | --- |
| Maximum vouchers | 5 |
| Value each | €15 |
| **Maximum exposure** | **€75** |
| Maximum Meta messages | 5, one per slot, one attempt each |

**A batch can end up partial, and that is by design.** Slots are walked in slot
order, and the first outcome that cannot be proven stops every slot after it.
So a realistic bad day looks like: two vouchers created and paid, one unknown,
two never attempted at all. The report says exactly that, per slot, and the
batch is left halted until a human resolves the one that went wrong.

---

## 1. Environment

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
| `EASYWEEK_VOUCHER_SNAPSHOT_BATCH_ENABLED` | the §41 fence. False ⇒ every stage refuses before any HTTP request, the read-only plan included |
| `EASYWEEK_VOUCHER_SNAPSHOT_BATCH_STAFFER_UUID` | who performs the sales |
| `EASYWEEK_VOUCHER_SNAPSHOT_BATCH_ACCOUNT_UUID` | which POS account is charged |
| `EASYWEEK_VOUCHER_DELIVERY_HMAC_KEY` | ≥32 bytes; binds each issued code to its slot |
| `EASYWEEK_VOUCHER_DELIVERY_HMAC_KEY_ID` | names which key produced a stored MAC |

Turning on the §36 or §37.2 canary does **not** turn on this one, and vice
versa. There is deliberately no variable for the batch size or the exposure:
five and €75 are literals in the code and CHECK constraints in the database.

---

## 2. Migration

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run alembic current
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run alembic upgrade head
```

Expected head: `b3f7c2a90d14`.

---

## 3. Status — read-only, database only, no network

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_snapshot_batch status
```

The same state is readable in the browser at `/ops/docs/voucher-snapshot-batch`.
That page has nothing to click on purpose.

---

## 4. Plan a stage — read-only, GET-only

A plan performs GETs only. It creates no batch, sends nothing and writes
nothing.

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_snapshot_batch plan --stage freeze --preview-run-id RUN
```

Read `ready`, `reasons`, `snapshot.composition` and `snapshot.baseline` before
deciding anything. Check `snapshot.composition.recipient_count` and
`snapshot.composition.total_exposure_minor` against what you intend to spend.

Keep three values from the output for the next command:

- `plan_digest`
- `plan_issued_at`
- `confirmation_phrase`

They expire in 30 minutes. A plan for one stage never authorises another, and a
plan built for one composition never authorises a different one.

**Baseline.** `snapshot.baseline.baseline_version` must read `2026-09-15-42` and
`mismatched_fields` must be empty. A drift stops the stage; it is never adapted
to. Take a drift to the owner before doing anything else.

---

## 5. FREEZE — a local mutation, no money and no message

Freezing writes the batch header and its slots. **Nothing leaves the process.**
After it, the preview can no longer be edited, discarded or deleted, and the
composition is immutable.

Only after the owner approved this exact digest:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_snapshot_batch freeze --preview-run-id RUN --apply --plan-digest DIGEST --plan-issued-at ISSUED_AT --confirm PHRASE
```

Check the printed `batch.items`: the slots, the recipient row ids and the
markers are what every later stage will act on.

---

## 6. CREATE — up to five external effects, no money yet

Plan first (step 4, `--stage create`), then, only with the owner's approval of
that exact digest:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_snapshot_batch create --preview-run-id RUN --apply --plan-digest DIGEST --plan-issued-at ISSUED_AT --confirm PHRASE
```

Exit codes: `0` proven, `3` **unknown or halted — stop and reconcile**, `4`
refused (nothing left the process), `6` proven with open drafts to clean up.

Read `external_calls.create` and `slots[]`. An `unknown` never means "try
again". Go to step 11.

---

## 7. Reconcile — GET-only

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_snapshot_batch reconcile --preview-run-id RUN
```

Reads only. It moves a slot forward only where an exact order readback proves
the move, and it lifts the halt only when no slot is unresolved any more.

---

## 8. PAY — real money, up to €75, irreversible after any send

Plan first (step 4, `--stage pay`). Then, only with the owner's **separate**
approval for real payments of this exact amount:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_snapshot_batch pay --preview-run-id RUN --apply --plan-digest DIGEST --plan-issued-at ISSUED_AT --confirm PHRASE
```

Then reconcile again (step 7).

---

## 9. DELIVER — the messages reach real people

Requires, in addition to the plan: the approved Meta template
`kitilash_ka_new_client_voucher_v1`, an active sender, and the owner's approval
of the voucher text and terms.

Plan first (step 4, `--stage deliver`). Then:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_snapshot_batch deliver --preview-run-id RUN --apply --plan-digest DIGEST --plan-issued-at ISSUED_AT --confirm PHRASE
```

`provider_accepted` means Meta accepted the request. It does **not** mean
delivered: `delivered` and `read` are written only by the existing webhook, on
the exact provider message id.

One attempt exists per slot for the lifetime of that row — the database caps it
at one. A `send_unknown` is a case for a human, never for a second command, and
it stops every slot after it.

---

## 10. REFUND — one named slot, pre-send only

Available only for a slot nothing has ever been sent for. Forbidden — by plan,
by claim and by a CHECK constraint — from `send_claimed`, `send_unknown`,
`send_rejected`, `provider_accepted`, `delivered` and `read`.

There is deliberately no refund-everything.

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_snapshot_batch plan --stage refund --preview-run-id RUN --slot K
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_voucher_snapshot_batch refund --preview-run-id RUN --slot K --apply --plan-digest DIGEST --plan-issued-at ISSUED_AT --confirm PHRASE
```

There is no documented cancel endpoint for an open draft. If a CREATE succeeded
and PAY never ran for that slot, close the draft by hand in the EasyWeek
dashboard using that slot's `reconciliation_marker`; a later `reconcile` records
that as an observation.

---

## 11. When something is unknown

1. Do not repeat the command.
2. Run `status` and `reconcile`.
3. For an unknown CREATE or PAY: find the order in the EasyWeek dashboard by the
   slot's `reconciliation_marker` that the report prints.
4. For an unknown DELIVER: check the WhatsApp line. The voucher may already be
   in the customer's hands — a refund is forbidden for that slot from here on.
5. Report to the owner before any further command. The batch stays halted, so no
   later slot can be claimed until it is resolved.

---

## 12. Afterwards

Turn the fence off:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api env | grep EASYWEEK_VOUCHER_SNAPSHOT_BATCH_ENABLED
```

Set `EASYWEEK_VOUCHER_SNAPSHOT_BATCH_ENABLED=false` in the environment file and
restart the API service.

---

## 13. What this does not do

- **Redemption is not tracked.** Nothing here records whether a voucher was ever
  applied to a booking. The ledger models issue, payment, delivery and the
  webhook statuses, and stops there. Owner-reported manual evidence that a
  voucher was used is exactly that — it is not a fact this system can prove.
- **A second batch is impossible.** The scope is unique and pinned to one
  literal in the database. Another batch needs a new owner-approved plan, a new
  migration and a new PR.
- **Repeatable or scheduled sending is not enabled.** No generic campaign
  runner, `MessageJob`, Outbox row, worker, scheduler, retry, resume or
  follow-up is involved at any point.

Nothing about a successful batch authorises a campaign. Every report keeps
saying `campaign_send_authorized=false`, `bulk_delivery_authorized=false` and
`ready_for_send=false`, and they stay false until a separate PR says otherwise.
