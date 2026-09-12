# EasyWeek controlled voucher delivery canary — operator runbook (§36)

This runbook covers one narrow, operator-driven, one-off flow: giving **one**
€15 EasyWeek voucher to **one** proven recipient, over WhatsApp, once.

It is not a campaign and it does not open one. Ordinary EasyWeek campaign
delivery stays technically closed: `require_campaign_execution_provider`
still refuses EasyWeek, `supported_job_types` stays empty, the campaigns runner
and the outbox worker gain no voucher path, and every report this tool prints
repeats `campaign_send_authorized=false`, `bulk_delivery_authorized=false` and
`global_ready_for_send=false`.

**Nothing in this runbook may be executed during development or review.** Each
production stage is a separate manual action by the owner, after merge, after
deployment, and after a fresh approval of that exact stage's digest.

## 0. What this canary is, and what it is not

§35 proved this application can create, pay for and refund one real voucher.
It deliberately stopped there and sent nothing to anybody. §36 adds the step it
refused: handing the resulting code to a real person.

That changes the risk. A voucher code is a bearer secret — whoever reads it can
spend €15 — and a WhatsApp message cannot be unsent. So two rules shape
everything below:

* **one delivery attempt, ever.** Not a retry budget that happens to be one: the
  database cannot represent a second attempt;
* **an unknown result is never retried and never auto-refunded.** If we cannot
  prove Meta did not take the message, we assume the customer may be holding the
  code.

EasyWeek publishes no write idempotency key. The owner approved this canary on
that basis: a single attempt, the claim recorded before the request, and a full
stop on an unknown result take the place of a safe repeat.

## 1. Prerequisites

Before anything is run in production, all of these must be true:

1. the owner has approved the **exact message text** and the voucher's terms of
   use — the body in `template_contract.py` is a development contract, not an
   approval;
2. the Meta template `kitilash_ka_new_client_voucher_v1` exists and is
   **APPROVED**, MARKETING, `de`, positional, one BODY, three parameters. This
   repository never creates it: that is a human action in the Business Manager;
3. the stored template row matches it, proven by a live Meta read:
   ```bash
   docker compose -p altegio_bot run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.reconcile_easyweek_voucher_template --company-id <company>
   ```
   then the same command with `--apply` once the audit is green;
4. `EASYWEEK_VOUCHER_DELIVERY_CANARY_ENABLED=true` and a fresh
   `EASYWEEK_VOUCHER_DELIVERY_HMAC_KEY` (≥32 bytes) with an
   `EASYWEEK_VOUCHER_DELIVERY_HMAC_KEY_ID` are set in `easyweek.env`;
5. a **fresh** preview and preflight have been run, and the owner has chosen one
   recipient from them. Nothing about an earlier run carries over;
6. the owner has separately authorised a **non-refundable** €15 test payment for
   this recipient — once the message is sent, the refund path is closed;
7. the owner authorises each of CREATE, PAY and DELIVER as its own decision.

## 2. The command surface

```bash
docker compose -p altegio_bot run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-outbox-worker -m altegio_bot.scripts.easyweek_voucher_delivery_canary <command> ...
```

| Command | What it does |
|---|---|
| `status` | Database only. No client, no key, no identity, no fence |
| `plan --stage create\|pay\|deliver\|refund` | Read-only. Prints the plan, its digest and its phrase |
| `create --apply` | One EasyWeek order |
| `pay --apply` | One €15 payment |
| `deliver --apply` | **One WhatsApp message to a real person** |
| `refund --apply` | One refund — only while nothing has been sent |
| `reconcile` | Reads only. Resolves an unknown EasyWeek stage by looking |

Every command except `status` takes the exact recipient:

```
--preview-run-id <N> --campaign-recipient-id <M>
```

Never a phone number, a name or a bare customer UUID. The entitlement is earned
by one visit recorded in one preview run, and an addressing mode without that
link would be a different feature.

**There is no command that runs create, pay and deliver in sequence, and there
never may be.** No example in this document may combine stages.

## 3. The plan / apply cycle, once per stage

```bash
... easyweek_voucher_delivery_canary plan --stage create --preview-run-id N --campaign-recipient-id M
```

The plan performs reads only. It creates no ledger row and sends nothing. It
prints `stage`, `ready`, a closed list of `reasons`, `plan_digest`,
`plan_issued_at`, `plan_expires_at` and a `confirmation_phrase` bound to that
digest.

The owner reads it and approves **that exact digest**. Then:

```bash
... easyweek_voucher_delivery_canary create --apply --preview-run-id N --campaign-recipient-id M --plan-digest "$D" --plan-issued-at "$T" --confirm "$P"
```

The apply command rebuilds the plan live, seconds before it claims, and refuses
unless the rebuilt plan still hashes to the approved digest. `plan_issued_at` is
part of what is signed, so an old digest cannot be revived by typing a fresh
timestamp beside it. A new plan invalidates the previous approval, and one
stage's digest is never valid for another.

## 4. What is proven before every stage

**The message.** The fence, the HMAC key, the stored template row and the active
sender — all four, before CREATE. Issuing and paying for a voucher that cannot
legally be delivered would leave a real €15 with only two exits.

**The person.** A full live guard, every stage, every time: provider, company,
the exact run and recipient, the campaign code, the source booking still
completed and current, the EasyWeek customer identity, a complete untruncated
history, exactly one completed visit, no active future booking, no opt-out, and
a current phone number equal to the one the preview recorded. Any single
mismatch is a closed reason code and **zero** external calls.

**The voucher.** After CREATE the code is read once, a keyed MAC of it is stored
— bound to this ledger row, this order and this key — and the plaintext is
dropped. Before PAY and before DELIVER the order is read again, the code
extracted again, the MAC recomputed and compared. A mismatch stops everything
before any external effect.

## 5. Reading the states

| State | What it means | What to do |
|---|---|---|
| `planned` | The recipient is recorded; nothing has happened | Plan `create` |
| `create_claimed` | The create request may have gone out | `reconcile` |
| `create_unknown` | It went out and we cannot prove the result | `reconcile`, then the dashboard |
| `created` | One order exists, proven, with the code bound | Plan `pay` |
| `pay_claimed` / `pay_unknown` | The payment may have happened | `reconcile` only |
| `paid` | €15 is paid, proven | Plan `deliver` **or** plan `refund` |
| `ambiguous` | Two marker orders, or a paid order this canary cannot account for | Stop. A human decides |
| `send_claimed` | The message may have gone out | Stop. Investigate by hand |
| `send_unknown` | **The customer may be holding the code** | Stop. No resend, no refund |
| `send_rejected` | Meta refused before acting | Stop. The one attempt is spent |
| `provider_accepted` | Meta took the message — NOT delivered | Wait for webhooks |
| `delivered` / `read` | A webhook for this exact message id said so | Record the outcome |
| `refunded` | The money came back; nothing was ever sent | Done |
| `manually_cleaned` | Somebody else closed or reversed the order; we sent nothing | Done |

`provider_accepted` is not delivery. `delivered` and `read` are written only by
a webhook naming this exact `provider_message_id`, and they are monotonic: a
duplicate or out-of-order callback cannot move the row backwards.

Once the voucher has reached its person there is no draft left to close, so
`provider_accepted`, `delivered` and `read` all clear `manual_cleanup_required`
— and so do `refunded` and `manually_cleaned`. A `status` after a successful
delivery asks for nothing. `send_unknown` is the opposite and stays that way:
cleanup required, reconciliation required, resend and refund both closed.

## 6. When the refund is forbidden

A refund is the **pre-send** escape hatch and nothing else. It is refused — by
the plan, by the claim, and by a database CHECK constraint — once any of these
is true: `send_claimed`, `send_unknown`, `send_rejected`, `provider_accepted`,
`delivered`, `read`.

This is deliberate. Refunding a voucher whose code a customer already received
would leave them holding a code that no longer works, which is worse than the
€15.

**What does NOT block a refund.** The refund reaches no customer, so it does not
prove the machinery of a message it will never send. A paused or deleted Meta
template, a switched-off sender, a missing or rotated HMAC key, a voucher body
that stopped making sense — every one of those is a reason to get the money
back, not a reason to leave it out there. The refund plan reports those checks
as `not_required_for_refund` rather than as a comfortable `true`.

What it still proves: the fence, its own `--apply`, its own digest, issued_at
and phrase, the ledger identity, the exact order, our marker, our customer, the
live guard, that nothing has been sent — and that the order reads **strictly
paid** immediately before the claim.

**An order that is already refunded.** If the read before the claim shows the
money is already back — somebody refunded it in the dashboard between the plan
and the apply — no POST is sent at all. The state is settled by reading, and the
report says `voucher_order_already_refunded`.

This also covers the case where the refund plan itself refuses for that reason,
which it does whenever the dashboard refund happened before you rebuilt the plan.
A refusal there used to be a dead end: no POST, but the ledger stayed `paid`
against a refunded order and every later `reconcile` answered
`contract_mismatch`. Now `refund --apply` re-reads the exact order, proves it is
ours, and settles it to **`manually_cleaned`** — never to `refunded`, because
this application refunded nothing. No claim, no attempt, no verification
timestamp and no plan digest are back-filled: each would record a request that
never went out.

The same ending is reachable with `reconcile` alone, and settlement is refused
if the order cannot be proven ours, if the money is not actually back, or if
anything was ever sent from this row. A refund that a customer's message has
already made irreversible is not something to tidy away.

## 7. An unknown result

An unknown outcome means the request left this process and its effect is not
known. It is never retried automatically, and it must never be wired to one.

`reconcile` is how an unknown EasyWeek stage ends. It reads — and then it
**writes down what it proved**, so a resolved stage really is resolved and the
next one becomes reachable:

* an unknown pay over an order that reads paid becomes `paid`, and only then
  does a fresh `deliver` or `refund` plan become possible;
* an unknown pay over an order that still reads open stays unknown: an open
  order is not proof the payment failed, and the answer may be in flight;
* an unknown refund over an order that reads refunded becomes `refunded`;
* an unknown create finds its order by marker — branch and customer scope, the
  window proven locally — and an incomplete walk stays unresolved rather than
  becoming "no order exists";
* two marker orders become `ambiguous`, which stops the canary for a human;
* a paid order the canary cannot account for also becomes `ambiguous`: no
  payment is invented on our behalf, and `deliver` does not open;
* an order somebody closed or reversed by hand becomes `manually_cleaned`, as an
  observation — the tool never claims it performed the rollback. This includes a
  ledger that still reads `created` or `paid` while the order reads cancelled or
  refunded, provided nothing was ever sent from that row;
* an order the exact read cannot prove is ours — a different UUID, a different
  comment, a different customer — resolves to nothing at all. It stays
  unresolved with `voucher_order_unproven`, and no state is written. A listing
  hit is a lead, not an identity: the listing was scoped by branch and customer,
  and the exact read is where the order, the marker and the customer are
  compared.

`reconcile` never creates, pays, refunds or sends. Its only external calls are
GETs, and it refuses before the first one if the ledger names a different
recipient.

1. **Do not re-run the stage.** The ledger refuses it anyway; that refusal is the
   design;
2. for an unknown EasyWeek create or pay, run `reconcile` — it is a read, and it
   is safe to repeat;
3. for an unknown **send** there is nothing to reconcile against: Meta's message
   endpoint does not answer "did you accept this?". Check the Business Manager
   and, if necessary, ask the customer. Record what you find;
4. never refund an unknown send;
5. `status` prints the durable record without touching the network.

## 8. Closing an open draft by hand

There is no documented cancel endpoint for an open EasyWeek order, and none is
invented. If a create is proven but the payment never happened, the report says
`manual_cleanup_required: true`:

1. take `reconciliation_marker` from the report;
2. find the order in the EasyWeek dashboard by that marker;
3. close it there;
4. run `reconcile` again.

## 9. The voucher code

The code is a bearer secret. It exists in memory between one read of the paid
order and one Meta request, and nowhere else — not in the ledger, the evidence,
a campaign row, a job, an outbox row, a log, an exception, a report, a Chatwoot
note or a ticket.

**Do not paste a voucher code into a ticket, a chat, a log or a screenshot.** If
one is ever needed for support, read it from the EasyWeek dashboard rather than
from anything this tool produced.

A Chatwoot note after a proven acceptance is optional and must be fully
redacted, for example: *"EasyWeek voucher canary accepted by Meta; voucher code
intentionally omitted."*

## 10. Exit codes

| Code | Meaning |
|------|---------|
| `0` | The requested stage was proven. **Not** a permission to send anything else |
| `2` | Bad arguments, `--help`, missing `--apply`, fence off, or unusable configuration |
| `3` | **UNKNOWN — do not auto-retry.** Something left this process unproven |
| `4` | Contract mismatch or refusal — a fact did not hold |
| `5` | Ambiguous reconciliation |
| `6` | Manual dashboard cleanup required |

## 11. What a successful canary does and does not unlock

It does not, by itself, change anything. `campaign_execution_not_authorized`,
the empty EasyWeek `supported_job_types`, `ready_for_send=false`,
`gift_card_online_sales_disabled`, `gift_card_public_url_unproven`,
`gift_card_semantics_unproven` and `gift_card_issue_contract_unproven` all stay
exactly as they are.

A public customer-facing URL is not needed here, because a proven code is sent
to one proven person — but that proves nothing about public gift-card semantics,
expiry, redemption rules or bulk readiness. Revisiting any of those requires the
actual production transcript of a successful canary, reviewed separately, in its
own PR.

## 12. Development boundary

Do not run production commands, SSH, authenticated probes, a real create, a real
pay, a real delivery or a real refund while developing or reviewing this work,
and do not edit `easyweek.env` on the server as part of it. Validation is local
tests and a disposable PostgreSQL instance only.
