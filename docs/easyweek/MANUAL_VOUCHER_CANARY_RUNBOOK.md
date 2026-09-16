# Controlled manual-basis voucher delivery canary (§37.2) — runbook

One manually selected recipient, one €15 voucher, one WhatsApp message. Every
stage is a separate command behind a separately approved plan. There is no
command that runs two stages, and there will not be one.

Read `docs/easyweek/INTEGRATION_PLAN.md` §37.2 before the first command.

> **This runbook does not authorise anything.** The real €15 payment and the
> real Meta POST happen only after the owner approves that specific stage, that
> specific plan digest, and the voucher text and terms.

---

## 0. Before anything

The canary acts on ONE recipient of ONE completed EasyWeek preview:

- `provider=easyweek`, `company_id=322579` (Karlsruhe), campaign
  `new_clients_monthly`;
- the recipient's `recipient_basis` is `operator_manual_selection`;
- the recipient's status is `candidate`;
- the recipient has a stored EasyWeek customer UUID and exactly one local
  EasyWeek `Client`.

A manual basis is an operator's decision, not a proven first visit. Every report
says so: `first_visit_proof=not_applicable`.

Build a fresh preview and pick the recipient again. Readiness from an older run
is not carried forward.

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
| `EASYWEEK_MANUAL_VOUCHER_CANARY_ENABLED` | the §37.2 fence. False ⇒ every stage refuses before any HTTP request, the read-only plan included |
| `EASYWEEK_MANUAL_VOUCHER_STAFFER_UUID` | who performs the sale |
| `EASYWEEK_MANUAL_VOUCHER_ACCOUNT_UUID` | which POS account is charged |
| `EASYWEEK_VOUCHER_DELIVERY_HMAC_KEY` | ≥32 bytes; binds the issued code to the ledger row |
| `EASYWEEK_VOUCHER_DELIVERY_HMAC_KEY_ID` | names which key produced a stored MAC |

Turning on the §36 canary does **not** turn on this one, and vice versa.

---

## 2. Migration

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run alembic current
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run alembic upgrade head
```

Expected head: `a7d4f2e81c95`.

---

## 3. Status (read-only, no network)

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_manual_voucher_canary status
```

The same state is readable in the browser at `/ops/docs/manual-voucher-canary`.
That page has nothing to click on purpose.

---

## 4. Plan a stage (read-only)

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_manual_voucher_canary plan --stage create --preview-run-id RUN --campaign-recipient-id REC
```

The plan performs GETs only. It creates no ledger row, sends nothing and writes
nothing. Read `ready`, `reasons`, `snapshot.baseline` and
`snapshot.recipient.checks` before deciding anything.

Keep three values from the output for the next command:

- `plan_digest`
- `plan_issued_at`
- `confirmation_phrase`

They expire in 30 minutes. A plan for one stage never authorises another.

**Baseline.** `snapshot.baseline.baseline_version` must read `2026-09-15-42`
and `mismatched_fields` must be empty. A drift stops the stage; it is never
adapted to. Take a drift to the owner before doing anything else.

---

## 5. CREATE (first external effect — no money yet)

Only after the owner approved this exact digest:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_manual_voucher_canary create --preview-run-id RUN --campaign-recipient-id REC --apply --plan-digest DIGEST --plan-issued-at ISSUED_AT --confirm PHRASE
```

Exit codes: `0` proven, `3` **unknown — stop and reconcile**, `4` refused
(nothing left the process), `6` proven with an open draft to clean up.

An `unknown` never means "try again". Go to step 9.

---

## 6. Reconcile

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_manual_voucher_canary reconcile --preview-run-id RUN --campaign-recipient-id REC
```

Reads only. It moves the ledger forward only where an exact order readback
proves the move.

---

## 7. PAY (real €15, irreversible after the send)

Plan first:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_manual_voucher_canary plan --stage pay --preview-run-id RUN --campaign-recipient-id REC
```

Then, only with the owner's separate approval for a real payment:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_manual_voucher_canary pay --preview-run-id RUN --campaign-recipient-id REC --apply --plan-digest DIGEST --plan-issued-at ISSUED_AT --confirm PHRASE
```

Then reconcile again (step 6).

---

## 8. DELIVER (the message reaches a real person)

Requires, in addition to the plan: the approved Meta template
`kitilash_ka_new_client_voucher_v1`, an active sender, and the owner's approval
of the voucher text and terms.

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_manual_voucher_canary plan --stage deliver --preview-run-id RUN --campaign-recipient-id REC
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_manual_voucher_canary deliver --preview-run-id RUN --campaign-recipient-id REC --apply --plan-digest DIGEST --plan-issued-at ISSUED_AT --confirm PHRASE
```

`provider_accepted` means Meta accepted the request. It does **not** mean
delivered: `delivered` and `read` are written only by the existing webhook, on
the exact provider message id.

One attempt exists for the lifetime of this row — the database caps it at one.
A `send_unknown` is a case for a human, never for a second command.

---

## 9. When something is unknown

1. Do not repeat the command.
2. Run `status` and `reconcile`.
3. For an unknown CREATE or PAY: find the order in the EasyWeek dashboard by the
   `reconciliation_marker` the report prints.
4. For an unknown DELIVER: check the WhatsApp line. The voucher may already be
   in the customer's hands — a refund is forbidden from here on.
5. Report to the owner before any further command.

---

## 10. REFUND (pre-send only)

Available only while nothing has been sent. Forbidden — by plan, by claim and by
a CHECK constraint — from `send_claimed`, `send_unknown`, `send_rejected`,
`provider_accepted`, `delivered` and `read`.

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_manual_voucher_canary plan --stage refund --preview-run-id RUN --campaign-recipient-id REC
```

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api uv run python -m altegio_bot.scripts.easyweek_manual_voucher_canary refund --preview-run-id RUN --campaign-recipient-id REC --apply --plan-digest DIGEST --plan-issued-at ISSUED_AT --confirm PHRASE
```

There is no documented cancel endpoint for an open draft. If CREATE succeeded
and PAY never ran, close the draft by hand in the EasyWeek dashboard using the
marker; a later `reconcile` records that as an observation.

---

## 11. Afterwards

Turn the fence off:

```bash
docker compose -f docker-compose.yml -f docker-compose.chatwoot-internal.yml exec altegio-api env | grep EASYWEEK_MANUAL_VOUCHER_CANARY_ENABLED
```

Set `EASYWEEK_MANUAL_VOUCHER_CANARY_ENABLED=false` in the environment file and
restart the API service.

Nothing about a successful canary authorises a campaign. Every report keeps
saying `campaign_send_authorized=false`, `bulk_delivery_authorized=false` and
`ready_for_send=false`, and they stay false until a separate PR says otherwise.
