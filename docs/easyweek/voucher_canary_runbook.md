# EasyWeek controlled voucher mutation canary — operator runbook (§35)

This runbook covers one narrow, operator-driven, one-off research flow: creating,
paying for, investigating and refunding exactly **one** real EasyWeek voucher
order.

It is not a campaign. It sends nothing to any customer, it opens no campaign
execution, it changes no readiness fence, and a green stage is a research result
rather than a permission. The test customer is a real person's card in a real
POS system, and a real card is charged and refunded — so every step below stops
and waits for a human.

**Nothing in this runbook may be executed during development or review.** The
production plan, create, pay and refund are separate manual actions by the
owner, after merge and deployment, under a freshly approved plan digest.

## 0. What exists in code, and what does not

| Capability | Where |
|---|---|
| Reviewed GETs, including the POS reads used for reconciliation | the GET-only client |
| `GET /orders?location_uuid&customer_uuid&staffer_uuid&page&per_page` | the GET-only client |
| `GET /locations/{uuid}/accounts` and `GET /locations/{uuid}/staffers` | the GET-only client |
| `POST /orders/calculate` | the calculate-only client (§34 follow-up, untouched) |
| `POST /orders`, `POST /orders/{uuid}/pay`, `POST /orders/{uuid}/refund` | the mutation client, and nowhere else |
| Durable claim state | one row in `easyweek_voucher_canary_ledger` |

There is no cancel endpoint for an open order, and none is invented: no
undocumented `DELETE` or `PATCH` is ever sent. An open draft is closed by a
human in the EasyWeek dashboard — see section 9.

## 1. Development: tests only, no network

```bash
uv run pytest src/altegio_bot/tests/test_easyweek_voucher_mutation.py src/altegio_bot/tests/test_easyweek_voucher_canary_plan.py src/altegio_bot/tests/test_easyweek_voucher_canary_ledger.py src/altegio_bot/tests/test_easyweek_voucher_canary_runner.py src/altegio_bot/tests/test_easyweek_voucher_canary_cli.py src/altegio_bot/tests/test_easyweek_voucher_canary_pii.py -q
```

Every HTTP interaction in those suites runs through `httpx.MockTransport`; no
test opens a socket. The ledger tests run against the project's PostgreSQL
instance, because the guarantees they assert are database guarantees.

## 2. Deployment

The canary needs the migration applied and the worker image rebuilt. It reads
its configuration from the existing `easyweek.env`, which the one-off container
already mounts through the outbox worker's `env_file` list.

```bash
docker compose -p altegio_bot run --rm migrate
```

Then set the canary variables in `easyweek.env` on the server. All four are
absent or fail-closed by default; see `easyweek.env.example` for the documented
block. The three identity variables carry the owner-chosen production customer,
staffer and Card account UUIDs and are **never** committed to this repository.

```
EASYWEEK_VOUCHER_CANARY_ENABLED=true
EASYWEEK_VOUCHER_CANARY_CUSTOMER_UUID=<runtime>
EASYWEEK_VOUCHER_CANARY_STAFFER_UUID=<runtime>
EASYWEEK_VOUCHER_CANARY_ACCOUNT_UUID=<runtime>
```

`docker compose restart` does not re-read `env_file`; the one-off container
below picks up the new values because it is created fresh each time.

Before starting, freeze the voucher template administratively: the canary
refuses to run if any frozen field moves, and an edit mid-canary would
invalidate every observation.

## 3. Read-only plan — one per stage

Each mutation is authorised by its OWN plan. A single plan could only ever
authorise the first one: the moment `create` succeeds, a plan that required no
marker order to exist can never be satisfied again, and the voucher counters may
legitimately have moved.

```bash
docker compose -p altegio_bot run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.easyweek_voucher_canary plan --stage create
```

Replace `create` with `pay` or `refund` before those stages. Each prints its own
`plan_digest`, `plan_issued_at`, `plan_expires_at` and a single
`confirmation_phrase` bound to that digest. A create digest is never valid for a
payment or a refund.

Every plan performs GETs only. It creates no ledger row and sends no mutation.
What each one asserts:

* **create** — no ledger row, no existing marker order, frozen configuration and
  readable counters;
* **pay** — the ledger is exactly `created`, the target order is proven, exactly
  one marker order exists, it is still open and in the canary's own
  customer/template scope;
* **refund** — the ledger is exactly `paid` and the target order reads as paid.

The output separates four different things, and the separation matters:

| Field | What it is |
|---|---|
| `immutable_template_digest` | the frozen product configuration. A change here is a template edit and stops the canary |
| `counters_observed` | this stage's counter baseline. An observation, deliberately NOT part of the authorisation |
| `ledger_state` / `order_state` | where the canary is, and what the order currently reads as |
| `plan_digest` | the stage authorisation itself |

A counter that moved because a voucher was issued is the product working. It is
never read as a template edit, and it never blocks a refund.

`exit 0` means the plan is ready. Any other code means something is not proven,
and the `reasons` list says which fact.

## 4. Owner review of the stage digest

The owner reads the printed snapshot and approves **the exact digest of that
stage**. It is the authorisation token for that stage and no other. It is
deliberately not stored anywhere: each mutation command recomputes its own stage
plan live, seconds before it claims, and refuses unless the recomputed digest is
identical. A template price that moved, a staffer who left the branch, an account
that disappeared, a ledger that is not where the stage requires it to be, or a
target order that is no longer in the expected state all change the snapshot, so
they all change the digest, so they all stop the stage before anything is
claimed.

An approval also goes stale: `plan_issued_at` must be within the plan's short
maximum age. Re-run `plan` and get a fresh approval if it has expired.

## 5. Create

```bash
docker compose -p altegio_bot run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.easyweek_voucher_canary create --apply --plan-digest "$PLAN_DIGEST" --plan-issued-at "$PLAN_ISSUED_AT" --confirm "$CREATE_PHRASE"
```

One open POS order, one voucher line, price 1500, quantity 1, the canary marker
in the comment. The claim is committed to PostgreSQL **before** the request
leaves, so an interruption anywhere afterwards reads as "the request may have
gone out" — which is the only reading that cannot create a second order.

A 2xx is not yet a success. Before the ledger says `created`, the order is read
back and has to be the order we meant: a canonical UUID, our marker, our
customer, still open, with its voucher facts recorded rather than demanded, and
the frozen template configuration unchanged. Anything short of that is recorded
as `create_unknown` — with the candidate UUID kept so a later read has something
to read — and exits `3`.

Stop here and look at the report. Do not continue on autopilot.

## 6. Reconcile after create

```bash
docker compose -p altegio_bot run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.easyweek_voucher_canary reconcile
```

Reads only, and safe to repeat. For an unresolved create it walks this
customer's orders in this branch completely and matches the exact marker inside
the bounded window the ledger recorded.

The branch, the customer and the staffer are proven by the documented request
filters, not by fields in the response: the observed order body carries neither a
top-level `location_uuid` nor a `staffer_uuid`. An unfamiliar voucher shape is
recorded as a contract observation and is never read as "this order belongs to
somebody else". Completeness comes from the published `meta.last_page`, not from
the first empty page.

* exactly one match → a *candidate*, which still has to pass the same exact
  readback as a fresh create before the ledger says `created`;
* zero matches, or a walk that could not prove it saw everything → **unresolved**,
  never "it was not created". Wait and repeat, then look in the dashboard by
  marker;
* two or more matches → **ambiguous**. Stop completely and involve the owner.

Reconciliation is monotonic. A read taken while a POST is still in flight can
move a stage forward or leave it alone, but it can never move it back to a state
the same POST could be claimed from: a `pay_claimed` order that still reads open
becomes `pay_unknown`, never `created`, and a `refund_claimed` order that still
reads paid stays exactly where it is.

## 7. Pay

```bash
docker compose -p altegio_bot run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.easyweek_voucher_canary pay --apply --plan-digest "$PLAN_DIGEST" --plan-issued-at "$PLAN_ISSUED_AT" --confirm "$PAY_PHRASE"
```

Only a proven created order may be paid for, once, on the approved Card account.
The request body is exactly `{"account_uuid": ...}` — the documented endpoint
takes no amount, and it does not need one: the sum is already fixed by the exact
open order and its one voucher line. A real card is charged here.

Stop and look at the report.

## 8. Reconcile after pay

Same `reconcile` command. It reads the exact order and distinguishes open, paid,
refunded/cancelled and malformed. Payment is proven from documented order fields
— never from `account_paid_amount`, which is an opaque bookkeeping figure.

This is also where the voucher artifact is investigated. What the report shows
is shape only: where a field was seen, its JSON type, whether it was present,
and — where a length is a count rather than content — a bounded length.

No value is stored, and **no digest of a value** either. A plain SHA-256 of a
twelve-character voucher code, a phone number or an e-mail address is
brute-forceable in seconds, so a "fingerprint" of one would be the value in
disguise. The honest consequence is stated in every observation:
`cross_stage_equality_proven: false` — the canary cannot prove the code in the
pay response is the same string as the code in the readback, and it says so
rather than implying otherwise with a weak hash.

A customer subtree is not described beyond "it was there": presence, type, and
`subtree_redacted: true`. Personal scalar fields carry not even a length. An
unknown field is recorded as shape and explicitly **not** as a contract.

If the artifact turns out to be malformed or unreadable, that is a research
disappointment, not a blocker: continue to the refund.

## 9. Refund

```bash
docker compose -p altegio_bot run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.easyweek_voucher_canary refund --apply --plan-digest "$PLAN_DIGEST" --plan-issued-at "$PLAN_ISSUED_AT" --confirm "$REFUND_PHRASE"
```

One refund, once, on the exact paid order, with the documented empty body. The
refund stays available even when the artifact investigation failed — cleanup
matters more than research.

## 10. Final reconcile

Run `reconcile` once more. A proven rollback means the order reads as
refunded/reverted, and the report says `remote_rollback_proven: true`. The
report also shows the template counters before and after, so an unexplained
drift is visible rather than assumed away.

Then run `status` (database only, no network) for the final durable record.

## 11. Manual cleanup of an open draft

If a create is proven but the payment never happened — a rejected pay, or a
decision to stop — the order stays an open draft and the report says
`manual_cleanup_required: true`.

There is no documented cancel endpoint, so a human closes it:

1. take `reconciliation_marker` from the report;
2. find the order in the EasyWeek dashboard by that marker;
3. cancel it there;
4. run `reconcile` again.

The tool then observes the cancelled state and records **manual cleanup
observed**. It does not claim it performed the rollback, because it did not.

## 12. Incident and UNKNOWN procedure

An UNKNOWN outcome means the request left this process and its effect is not
known. It is never retried automatically, and it must never be wired to one.

1. **Do not re-run the mutation command.** The ledger will refuse it anyway;
   that refusal is the design, not an obstacle to work around.
2. Run `reconcile`. Repeat it — it is a read.
3. If it stays unresolved, look in the EasyWeek dashboard by marker.
4. If two marker orders exist, stop and involve the owner: an ambiguous state is
   never resolved by guessing which one is ours.
5. If a payment is proven and the refund is unknown, keep reconciling; the
   refund is not re-sent, and the order's own state is the answer.
6. Record the outcome. `status` prints the durable record without touching the
   network.

## 13. Exit codes

| Code | Meaning |
|------|---------|
| `0` | The requested stage was proven. **Not** a permission to send anything |
| `2` | Bad arguments, `--help`, missing `--apply`, env fence off, or unusable configuration |
| `3` | **UNKNOWN — do not auto-retry.** A request went out and its effect is unproven |
| `4` | Contract mismatch or refusal — a fact did not hold, a stage plan did not authorise it, or a stage was rejected |
| `5` | Ambiguous reconciliation — more than one candidate order |
| `6` | Manual dashboard cleanup required |
| `7` | Final rollback unproven — the order does not read as refunded |

Every report, at every exit code, repeats `campaign_send_authorized: false`,
`customer_message_sent: false` and `ready_for_send: false`.

## 14. What a successful canary does and does not unlock

It does not, by itself, change anything. `campaign_execution_not_authorized`,
the empty EasyWeek `supported_job_types`, `ready_for_send=false`,
`gift_card_online_sales_disabled`, `gift_card_public_url_unproven`,
`gift_card_semantics_unproven` and `gift_card_issue_contract_unproven` all stay
exactly as they are.

Revisiting any of them requires the actual production transcript of a successful
canary, reviewed separately, in its own PR.

## 15. Development boundary

Do not run production commands, SSH, authenticated probes, a real create, a real
pay or a real refund while developing or reviewing this work, and do not edit
`easyweek.env` on the server as part of it. Validation is local tests and a
disposable PostgreSQL instance only.
