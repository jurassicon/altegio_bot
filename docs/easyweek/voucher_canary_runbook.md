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
| `GET /orders?location_uuid&customer_uuid&page&per_page` | the GET-only client |
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

* **create** — no ledger row (or a proven-rejected one, see section 4a), no
  existing marker order, frozen configuration and readable counters;
* **pay** — the ledger is `created` (or `pay_rejected`), the target order is
  proven, exactly one marker order exists, it is still open, it is in the
  canary's own customer/template scope, and it is **exactly** the order §35
  authorises: one voucher line, the confirmed template, price 1500 as an
  integer, its count proven (see below), no services or goods, and at least one
  published total, every published total being exactly 1500. A payment settles
  whatever the order contains, so "is this our order?" is not the same question
  as "is this the order we approved?" — and a sum nobody could read is not a
  small gap, it is the whole amount being unproven;
* **refund** — the ledger is `paid` (or `refund_rejected`) and the target order
  reads as paid. The refund deliberately does **not** inspect the voucher
  contents: an unreadable artifact must never leave a real payment standing.

Every stage after the first also re-proves that the customer, staffer and
account in `easyweek.env` are still the ones the ledger row was opened with. A
different account of the same branch passes every other check there is and would
still be a real payment on somebody else, so the comparison is part of the
signed plan and is checked again under the database row lock at claim time. The
report says only `identity_binding_proven: true|false`.

An order whose state is missing, empty, unfamiliar, or self-contradictory is
`unknown` — never `open`. `open` is the one state a payment may be sent from, so
anything we cannot name is fail-closed out of it.

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
stored in the stage's ledger claim only after the mutation command recomputes
the stage plan live, seconds before the claim, and proves that the supplied
digest is exactly that live snapshot at the supplied `plan_issued_at`. The fresh
internal plan has a later issue time and therefore a different digest; that
internal digest is not substituted for the operator-approved one. A template
price that moved, a staffer who left the branch, an account that disappeared, a
ledger that is not where the stage requires it to be, or a target order that is
no longer in the expected state all change the snapshot, so they stop the stage
before anything is claimed.

An approval also goes stale: `plan_issued_at` must be within the plan's short
maximum age. Re-run `plan` and get a fresh approval if it has expired.

`plan_issued_at` is part of the digest itself, not a label printed beside it. An
old digest cannot be revived by typing a fresh timestamp next to it: the two are
verified together, and two plans one microsecond apart are two different
approvals. The voucher counters stay out of the digest, so a voucher that was
issued never invalidates an approval.

## 4a. Trying again after a rejection

A stage that was rejected **by the endpoint's own validation** — its refusal
envelope, on a status decided before the handler acts — leaves the ledger in
`create_rejected`, `pay_rejected` or `refund_rejected`. That is the one state an
operator may deliberately try again from, and it exists so a fixable 422 does
not strand an open draft order.

Trying again is manual from end to end: fix the cause, run `plan --stage ...`
again, have the owner approve the **new** digest, and pass the new
`--plan-digest`, `--plan-issued-at`, `--confirm` and `--apply`. The previous
approval is spent and will be refused. Nothing is ever re-sent automatically.

Every other failure — a timeout, a transport error, a 429 or 5xx, a 2xx whose
body could not be read, a redirect, and every 4xx that does **not** prove the
endpoint declined before acting (a 402, a 409, a 423, a bodiless 405 from an
edge) — is UNKNOWN, not rejected. Those go to `reconcile` and are never
retried.

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

The branch and the customer are proven by the request filters, not by fields in
the response: the observed order body carries neither a top-level
`location_uuid` nor a `staffer_uuid`.

**The listing sends no `staffer_uuid`.** A production probe ran the same listing
twice against a real, confirmed voucher order. With the staffer filter the
completed walk did not contain it; without the staffer filter the completed walk
did. That is the proven fact — why the provider behaves that way was not
established and is not guessed at — and it is enough: a filter that hides the
order we must find is not sent. The honest consequence is that this listing
proves branch and customer scope and does **not** prove a remote staffer
attribution. The staffer stays mandatory everywhere it IS provable: runtime
identity, live Karlsruhe membership, the CREATE request, the identity
fingerprint and the durable ledger binding, re-checked under the row lock.

**The listing sends no date filter.** Passing the ledger window as
`created_at_from`/`created_at_to` was answered 422, consistently — so that
server-side date form is not used here, and the bounded create window is proven
locally instead, against each row's own timezone-aware `created_at`. A missing,
unparseable, naive or out-of-window timestamp means "not our order". An unfamiliar voucher shape is
recorded as a contract observation and is never read as "this order belongs to
somebody else".

Completeness is proven, not assumed. A page counts only when its own metadata
agrees with the request that produced it: `current_page` is the page that was
asked for, `last_page` is consistent across the walk, and `per_page` is the fixed
size. Missing, malformed or repeated metadata — a server answering page 2 with
page 1 — makes the walk **incomplete**, and an incomplete walk is unresolved,
never "there is nothing there". An empty page is not an end marker either.

`reconcile` refuses before its first GET if the environment's identity is not
the one the ledger row was opened with. Searching a different customer's orders
for our marker, and then judging what it found against this row, is worse than
not looking at all.

* exactly one match → a *candidate*, which still has to pass the same exact
  readback as a fresh create before the ledger says `created`;
* zero matches, or a walk that could not prove it saw everything → **unresolved**,
  never "it was not created". Wait and repeat, then look in the dashboard by
  marker;
* two or more matches → **ambiguous**. Stop completely and involve the owner.

Reconciliation is monotonic and complete. Every combination of ledger state and
remote order state has a defined answer, and a read taken while a POST is still
in flight can move a stage forward or leave it alone — never back to a state the
same POST could be claimed from:

| The order reads | What the ledger does |
|---|---|
| refunded | any unfinished stage after `created` advances to `refunded`. A payment that never came back does not stay unknown over an order that was refunded |
| paid | `created`, `pay_claimed`, `pay_unknown` and `pay_rejected` advance to `paid`. A refund stage does **not** move: that would make the order payable again |
| open | only `pay_claimed` moves, and only to `pay_unknown` — a payment in flight reads open, and calling that "not paid" would send it twice |
| cancelled | `created` and `pay_rejected` record **manual cleanup observed**. Nothing where a payment of ours may still be outstanding |
| unknown or malformed | nothing at all, and the outcome is never `proven` |

A verification timestamp is written only from `pay_claimed`/`pay_unknown` or
`refund_claimed`/`refund_unknown`, where this canary's operation may have had the
observed effect. A later change after a proven `*_rejected` response, or somebody
settling or cancelling the order in the dashboard, is recorded as an
observation, never as proof of our operation. And when the ledger and the order
contradict each other — a `refunded` row over an order that reads paid — the
report says so instead of `proven`, because the expensive possibility is that
the money is still out.

## 7. Pay

```bash
docker compose -p altegio_bot run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.easyweek_voucher_canary pay --apply --plan-digest "$PLAN_DIGEST" --plan-issued-at "$PLAN_ISSUED_AT" --confirm "$PAY_PHRASE"
```

Only a proven created order may be paid for, once, on the approved Card account.
Proven means the whole of the pay plan above, including the exact contents of
the order: our marker and our customer are not enough, because the payment
settles the order's own sum. An open order carrying our marker but an empty
voucher list and some other total is refused before the claim is taken and
before anything is sent.

The request body is exactly `{"account_uuid": ...}` — the documented endpoint
takes no amount, and it does not need one: the sum is already fixed by the exact
open order and its one voucher line. A real card is charged here.

Stop and look at the report.

### How "exactly one voucher" is proven

Two proofs are accepted, and the report names which one was used in
`voucher_quantity_proof`:

| Label | What the body actually said |
|---|---|
| `explicit_quantity` | one voucher line, our template, `price` exactly 1500, and a `quantity` key holding exactly the integer 1 |
| `singleton_issued_artifact` | `vouchers` is a list of exactly one object, it has no `quantity` key at all, and that object is an ISSUED voucher: a non-empty `code`, our template, `price` and `value` both exactly 1500 |
| `unproven` | anything else — and the payment is refused |

The second exists because the order production created carries no `quantity`
field. Writing `quantity` in as 1 when it is missing would have unblocked that
payment and every other one: an order for ten vouchers whose count arrives in a
field we do not know about would look identical. What the body does prove is a
count in a different place — the list holds one issued artifact, with one code —
and that is a fact about the list rather than a guess about a missing key.

A `quantity` that IS present decides by itself. `null`, `true`, `1.0`, `"1"`, 0
and 2 all refuse, and none of them falls back to the singleton proof: the field
was readable and it did not say one. `true` is called out because in Python
`True == 1`, so a truthiness check would have accepted a boolean as a count.

The CREATE request is unchanged: it still sends an exact integer `quantity: 1`.
What we ask for and what we can prove we received are different things.

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

Then run `status` for the final durable record. `status` reads the ledger and
nothing else: no HTTP client, no API key, no runtime identity and no canary
fence. It answers even when `easyweek.env` is empty or wrong, which is exactly
when an operator needs to know where the canary stands.

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
This is not the same as a proven rejection — see section 4a, which is the only
case an operator may deliberately attempt again.

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
| `4` | Contract mismatch or refusal — a fact did not hold, a stage plan did not authorise it, a stage was rejected, or the ledger and the order disagree |
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

## 15. Where this canary stands, and the only next step

A CREATE has already been performed once in production and proven. The durable
ledger says `created`, it holds the target order UUID, and no payment has been
attempted. This deployment fixes the two things that blocked the payment — the
listing that could not find the order, and the voucher proof that demanded a
`quantity` the order does not carry.

Nothing about that row changes: no new migration, no new canary scope, no schema
version bump, no repeated CREATE. The old failed plan digest is spent and is not
reused.

After deployment, in this order and no other:

1. `status` — database only, to confirm the ledger still reads `created` and
   still names the target;
2. `plan --stage pay` — read-only, producing a **fresh** digest, `plan_issued_at`
   and confirmation phrase;
3. the owner approves that exact digest, separately and explicitly;
4. one `pay --apply` with those fresh values, and then a stop;
5. if the result is UNKNOWN: `reconcile` only, repeatedly. The payment is never
   re-sent;
6. once the payment is proven: a separate `plan --stage refund`, a separate
   approval, and then the refund.

## 16. Development boundary

Do not run production commands, SSH, authenticated probes, a real create, a real
pay or a real refund while developing or reviewing this work, and do not edit
`easyweek.env` on the server as part of it. Validation is local tests and a
disposable PostgreSQL instance only.
