# WhatsApp consent and proven delivery failure — contract (PR-22.1)

This is the tracked contract for the foundation the EasyWeek voucher follow-up
will stand on: durable unsubscribes, a checkable order between a refusal and a
send, and recorded facts about failures of the **original** mailing.

It describes what is now true in the code. It is **not** a statement that any
follow-up exists, is approved, or may be sent. No follow-up audience, no
follow-up text, no follow-up ledger and no follow-up executor is part of this
stage, and the send path for one is not open.

Read with `VOUCHER_PRODUCTION_MAILING_RUNBOOK.md` (how the original mailing is
operated) and `VOUCHER_MAILING_BOOKING_REPORT.md` (what the local report does
and does not prove).

---

## 1. What is recognised as an unsubscribe

One parser, shared by the webhook ingress and the inbox worker, so the two
cannot reach different verdicts about the same message.

- `stop` and `стоп` are equal. So are the existing aliases: `unsubscribe`,
  `unsub`, `abmelden`, `отписка`, `отпиши`, `odjava`.
- Case is folded, runs of whitespace collapse, and ordinary punctuation is
  removed from the edges of the first token. `STOP`, `stop.`, `(stop)`,
  `*STOP*`, `«СТОП»` and `СТОП, bitte` are all the same command.
- The command is the **first** whitespace token, which is the rule this project
  already had: `STOP please` is an unsubscribe.
- A word that merely contains the command is not one. `nonstop`,
  `stop-messages`, `Stopp` and `bitte stop` are not commands.
- A reaction is never a command; an emoji cannot become a STOP.
- **Every** message of a webhook delivery counts, each against its own sender.
  The worker previously acted on the first message only.
- Subscription keywords are deliberately **not** extended. A wider net for a
  refusal costs one unsent marketing message; a wider net for permission is a
  new way for permission to be claimed.

Chatwoot mirrors and operator relay events never execute commands: a mirror of
a message Meta already delivered is the same message, not a second one.

---

## 2. Where a refusal lives

`whatsapp_consent_suppressions`, one row per normalised WhatsApp address.

A refusal no longer needs a client card. It survives:

- a person who said STOP before any card existed;
- a card being created, imported, merged or replaced afterwards;
- a phone number change, when a proven EasyWeek customer UUID links the two;
- a refusal recorded in another branch, or on an Altegio card.

`banned_easyweek_customer_uuid` is a **ban extension, not an identity claim**.
When a STOP arrives, an unambiguous EasyWeek card on that number contributes its
customer UUID, so the refusal follows the person through a number change. It is
never read the other way round: a phone match does not prove who somebody is,
which branch they belong to, or that they are an EasyWeek customer. An ambiguous
match binds nothing and is reported as a gap; the phone refusal still stands.

The legacy sources are **read, not replaced**. One aggregate answer consults:

1. the suppression ledger for this address and for the addresses this UUID's
   cards hold now;
2. a suppression bound to this UUID by an earlier refusal;
3. `Client.wa_opted_out` on any matching card, any branch, any provider;
4. the historical `CampaignRecipient.is_opted_out` snapshot, unless a proven,
   verified opt-in landed after that snapshot was taken.

**The absence of a refusal is not evidence of marketing consent.** It means only
that this database holds no proven refusal for that address.

### Where it is enforced

`campaigns/easyweek_manual_identity.local_identity` — the one shared veto point
every EasyWeek caller already passes through: the composition read, the freeze,
the per-item claim, the manual Add and the batch Add. A ledger refusal there
returns `manual_recipient_consent_suppressed`, kept apart from the card-flag
refusal `manual_recipient_opted_out` so an operator can tell the two apart.

The §36 and §37.2 canaries and generic EasyWeek campaigns are **not** rewired by
this stage; they keep their existing card-flag checks.

---

## 3. STOP / START ordering

Deliberately asymmetric. A refusal is cheap to make and expensive to undo.

**A STOP always applies.** Whatever its authenticity, whatever its timestamp.
Idempotent: a duplicate is a no-op that records itself. A late or replayed STOP
cannot move the refusal's own recorded time backwards.

**A START lifts a refusal only if all of these hold:**

- Meta's HMAC was configured **and** verified for that very request;
- the START's own event time is proven;
- the refusal's event time is proven;
- the START's is **strictly** later — an equal timestamp is not newer;
- its message identifier has not been seen naming another address.

Anything else refuses with a reason code, and the refusal stands:

| reason | meaning |
| --- | --- |
| `start_authenticity_unproven` | no signature was checked for that delivery |
| `start_event_time_unproven` | the claimed event time does not parse, or is implausibly in the future |
| `stop_event_time_unproven` | the refusal's own time is unknown, so "newer" has no answer |
| `start_not_newer_than_stop` | equal or earlier than the refusal |
| `start_event_identifier_conflict` | that message id already names another address |

A START for an address with no refusal creates **no row**: an absent ban is
never written down as a granted permission.

The last rule is a CHECK constraint, not only a code path: an `opted_in` row
whose decision was a START must carry a verified signature and a proven event
time, so an unsigned, replayed or time-unknown opt-in is unrepresentable.

### Consequence to know before rollout

**If `META_APP_SECRET` is not configured, no START can lift a ledger refusal.**
Without the secret the webhook performs no signature check, every delivery is
recorded `unverified`, and `start_authenticity_unproven` refuses. This is
§46.4.7 working as specified — "a check that is not configured is not a check" —
and it is deliberately fail-closed.

What it does **not** change: the existing START behaviour is untouched. A START
still clears `Client.wa_opted_out` exactly as before, so Altegio and generic
EasyWeek marketing behave as they do today. Only the new ledger — read by the
EasyWeek consent guard and by the future follow-up — is strict.

A refusal whose own event time is unknown (a historical projection, a legacy
flag) can never be lifted by a START, because "strictly newer" cannot be
established. That is a deliberate, conservative boundary, not an oversight.

Neither an ordinary reply, a `delivered`/`read` status, a new booking, nor a
failed acknowledgement is an opt-in. A lifted refusal does **not** revive
cancelled jobs.

---

## 4. The pending-inbound boundary and the final send guard

`whatsapp_inbound_receipts`, one row per inbound message, written **in the same
transaction as the `WhatsAppEvent`** and therefore committed before the 200 the
sender sees.

That ordering is the durable boundary. Once Meta has been told the delivery was
accepted, the fact that an address said something is on disk, so a stopped, slow
or crashed inbox worker leaves a **hold** for those people rather than a hole.

A receipt also records the one thing nothing downstream can: whether the
signature was actually checked. The ingress strips Meta's signature header from
the stored copy by design, so after that point the proof no longer exists and no
later process may reconstruct one from a sanitised header. `whatsapp_events.
inbound_authenticity` carries the same fact for a delivery that has no messages
at all — a `statuses` payload has no receipt to hang it on.

`resolved_at` is the pending marker. Rows are never deleted; the pending set
drains by being resolved, which is a fact rather than a cleanup. A delivery from
a sender route this deployment does not allow is resolved at the ingress, because
no worker will ever look at it and a permanent hold would be a bug.

### The guard

`whatsapp_consent.final_send_guard` takes a transaction-scoped advisory lock on
the address, then answers. It refuses when:

- a proven refusal matches the person (`consent_refusal_active`);
- anything from them is unresolved (`pending_inbound_unresolved`) — a queue that
  is behind must not read as permission;
- the address cannot be normalised (`address_not_normalisable`).

It returns a `fingerprint` over every matched `(address, consent_version)` pair
and the verdict. A sender re-checks it with `consent_unchanged` **inside the same
transaction as its send claim**. A refusal that landed in between, a version that
moved, or a suppression row that did not exist before all change the fingerprint
and refuse the claim.

The lock takes part in no cycle with the voucher stages: it is taken by the
consent writer and by the guard, and by nothing that holds a batch header or
item lock. No HTTP happens under it, and none may.

### The exact guarantee, and its limit

**Guaranteed:** zero Meta requests for a person whose refusal became durable
before this boundary. The refusal is committed in its own short transaction,
before the acknowledgement is even attempted, so an ack that fails, a Chatwoot
outage, an unhandled exception in the handler, or the process dying cannot roll
it back.

**Not guaranteed:** cancelling a request that has already left the process.
Nothing can. A STOP arriving while a POST is in flight is recorded with its own
time, the POST's answer with its own, and the record keeps both.

### Durability of the consent write

Applied by `process_one_event` **before** the event's own transaction opens, in
a transaction of its own, idempotently. A recovery pass re-examining the event, a
duplicate delivery and a restart all converge instead of double-applying. A
failure of the consent write itself is allowed to propagate: the event stays
unprocessed and its receipts stay unresolved, which holds sends rather than
letting an unread STOP look like permission.

---

## 5. Proven delivery failure of the original send

`easyweek_voucher_production_delivery_failures`.

### What counts as proof — exactly two things

- Meta refused the request itself, with its own error envelope, on a status it
  decides before acting on the message (`meta_api_rejected`);
- a `failed` delivery status naming the exact message id of the slot
  (`meta_callback_failed`).

### What does not, and never becomes proof later

A timeout, a transport failure, a 429, a 409, a redirect, an unreadable 2xx, a
2xx without a message id, a missing callback, `accepted`-only, a slot with no
attempt, a CREATE or PAY error, and this process's **own** parameter refusal.

The last one is the ambiguity this stage removes. `send_rejected` is written both
for Meta's refusal and for a local parameter refusal that never left the process,
so the status alone is not evidence; only the proven-rejection branch records a
fact. A general historical `rejected` with insufficient data stays unknown.

### The route is frozen with the attempt

Two one-way digests on `easyweek_voucher_production_batch_attempts` —
`sender_phone_number_id_digest` and `recipient_digest` — written in the same
committed transaction that spends the slot's single attempt, and never updated.
Not the values: this table stores no phone number and no sender id.

They exist because both of their sources are mutable. The client card's number
can change and the sender configuration can be repointed, so a confirmed failure
must be checked against the route the message **actually took**. Both are NULL on
every attempt written before this stage, deliberately: a historical attempt proved
no route, and inventing one from today's configuration would manufacture evidence.
Its facts say `route_proven = false` instead.

### Match states

| state | meaning |
| --- | --- |
| `confirmed` | bound to one proven attempt of one slot, with no `delivered`/`read` present |
| `unmatched` | nobody owns this identifier yet; kept so an early callback is not lost, bindable later on the exact id |
| `conflicting` | names a slot but disagrees with it — a foreign sender or recipient, or a failure after `delivered`/`read`. Kept for audit; changes nothing |

### Callback rules

- An early callback — one that overtakes the commit recording the message id —
  is stored `unmatched` and bound by the acceptance that records that id.
  Matching is only ever by exact proven identity. Never by phone similarity and
  never by timestamp proximity.
- Duplicates are idempotent: the fact key is computed from the proven contents
  of the fact, not from when it arrived.
- A callback naming a sender or recipient that is not the frozen route does not
  update the original send.
- `read` outranks `delivered`; both outrank a late `failed`. Success is never
  rolled back, and the contradiction is kept.
- A `failed` callback for a production slot is **claimed** by this handler and
  does not fall through to the generic `OutboxMessage` path, whose matching is
  looser and which knows nothing about the frozen route.

### The untrusted side is reduced to numbers

Only `meta_error_code` (an integer, or NULL when the value did not parse as one)
and `http_status` are stored. The class — `permanent`, `transient`, `unknown` —
is derived from that integer alone; an unknown code yields `unknown`, never a
guess. A Meta error title, message or detail can quote the request it refused,
which for this phase means a voucher code, a rendered body or a customer's name,
so none of it is stored, logged or classified from. `observed_at` (when we saw
it) and `event_at` (proven, NULL when unprovable) are kept apart.

### A fact is not a permission

Nothing here re-opens a slot. `send_attempt_count <= 1` is untouched; no retry,
no new POST, no job and no follow-up send exists in this stage. A confirmed
failure says what happened to the one attempt that was made. Whether a reason
permits a follow-up at all is a separate policy, in a later stage.

---

## 6. Historical reconciliation

`altegio_bot.whatsapp_consent_projection`, driven by:

```bash
uv run python -m altegio_bot.scripts.reconcile_whatsapp_consent
uv run python -m altegio_bot.scripts.reconcile_whatsapp_consent --failures
uv run python -m altegio_bot.scripts.reconcile_whatsapp_consent --apply
```

**Dry run unless `--apply`.** The command makes no external request of any kind:
no acknowledgement, no Chatwoot note, no Meta or EasyWeek call, no job, no
message. It does not call the inbox worker's event handler and does not touch
`whatsapp_events.status`, so it cannot replay anything. Its output is safe to
paste into a ticket: counts, origins and reason labels, never a phone number, a
name, a voucher code or a message body.

Sources read:

- `Client.wa_opted_out`, any branch and any provider;
- `CampaignRecipient.is_opted_out`;
- STOP commands in stored payloads, found by the **same** extractor the live
  ingress uses — including the Cyrillic `стоп` the old keyword set could not see,
  and including messages after the first;
- production slots whose single attempt ended in a proven Meta refusal.

Rules:

- Every write goes through the ordinary consent ledger, under its address lock
  and its monotonicity rules. The projection gets no private write path, so a
  run against a stale snapshot converges on the newer state instead of
  overwriting it, and a second run is a no-op.
- A historical START is never projected. Its authenticity is unrecoverable from
  a sanitised stored event, so it could not lift a refusal even if it were
  applied, and writing it would create an opt-in record out of an unverifiable
  payload.
- A flagged card with no usable number is counted, never guessed at.
- A Chatwoot mirror is not counted a second time.
- An unprovable delivery outcome stays unknown and never becomes a confirmed
  failure. A provable historical rejection is recorded with `route_proven=false`
  and no numeric code — the honest shape of an old fact.
- The report states `historical_signature_unrecoverable` on every run, because
  it never stops being true, and declares `truncated` when the stored-event scan
  hit its limit. **Completeness of the historical check is never claimed when
  the available data does not prove it.**

### What to expect before applying

Applying can make people newly unsendable who are sendable today: somebody whose
historical STOP sits only in a stored payload, and whose later START cannot be
proven. That is the specified behaviour, and the dry run exists so the owner sees
the count — `newly_suppressed_addresses` — before deciding.

---

## 7. Migration and rollout order

One Alembic revision, `a1c4e7b25d80`, on top of `c9e7a34d6210`. Single head.

Adds three tables, and three nullable columns to existing ones
(`whatsapp_events.inbound_authenticity`, and the two attempt route digests).
Nothing historical is rewritten: no amount, no price, no attempt counter and no
binding is touched, and an existing row keeps saying exactly what it said before
— which for authenticity and for the route means **unknown**, honestly, rather
than a value derived from today's settings.

**The downgrade refuses while any consent decision, inbound receipt or delivery
failure fact exists, and it refuses before any DDL**, under `ACCESS EXCLUSIVE`
locks taken first. Both halves matter: checking emptiness without the locks would
race a concurrent STOP, and dropping first would mean the data was already gone.
An operator told the rollback is unsafe finds every refusal still exactly where
it was.

Rollout order:

1. Deploy the schema and the application with the follow-up send path closed —
   which it is: no such path exists in this stage.
2. Run the reconciliation **dry** and read the report, in particular
   `newly_suppressed_addresses`, `cyrillic_stop_events`, `source_gaps` and
   `truncated`.
3. Decide, then run it with `--apply`.
4. Only then consider the next stage. Opening a follow-up audience or a
   follow-up send earlier, by any switch, is not permitted.

Production access, including read-only, requires permission for the specific
operation. These commands are handed to the owner to run.

---

## 8. Explicitly closed in this stage

Not implemented, and not reachable by any flag:

- follow-up audience selection, interval or text;
- a follow-up confirmation interface;
- a follow-up ledger or executor, and any follow-up send;
- automatic repeat messages, retries of the original send, or a third message;
- any change to voucher face value, issue price, the FREEZE/CREATE/PAY/DELIVER/
  REFUND authorities, the single-attempt, entitlement and dedupe limits, the
  mailing's geography, the UUID-first identity model, provider-managed validity,
  booking attribution, the compact report, Altegio campaign logic, generic
  EasyWeek send-real, Outbox retry or the scheduler.

---

## 9. Tests

| Area | Module |
| --- | --- |
| Recognition, durability, STOP/START order, pending boundary, guard | `tests/test_whatsapp_consent_ledger.py` |
| Proven failures, early/duplicate/foreign callbacks, local vs external refusal | `tests/test_easyweek_voucher_delivery_failures.py` |
| Historical reconciliation, its limits, racing live ingress | `tests/test_whatsapp_consent_projection.py` |
| Migration cycle, ORM parity, destructive-downgrade refusal, PG16 concurrency | `tests/test_easyweek_voucher_mailing_durability.py` |

All are required. No marker, skip or xfail was added. A change to shared inbound
also requires `uv run pytest -q -m legacy_altegio`, per
`docs/ops/test_suite_tiers.md`.
