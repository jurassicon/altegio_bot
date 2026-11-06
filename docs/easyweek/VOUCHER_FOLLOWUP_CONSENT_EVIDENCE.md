# WhatsApp consent and proven delivery failure — contract (PR-22.1)

This is the tracked contract for the foundation the EasyWeek voucher follow-up
will stand on: durable unsubscribes, a checkable order between a refusal and a
send, and recorded facts about failures of the **original** mailing.

It describes what is now true in the code. It is **not** a statement that any
follow-up exists, is approved, or may be sent. No follow-up audience, no
follow-up text, no follow-up ledger and no follow-up executor is part of this
stage, and the send path for one is not open.

Read with `VOUCHER_PRODUCTION_MAILING_RUNBOOK.md` (how the original mailing is
operated), `VOUCHER_MAILING_BOOKING_REPORT.md` (what the local report does and
does not prove) and `VOUCHER_FOLLOWUP_PREVIEW_CONTRACT.md` (PR-22.2, which
consumes this evidence to compose a preview — and still sends nothing).

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

### The sender route is decided per message

A delivery may carry several `changes`, each with its own `phone_number_id`, and
each is judged on its own against the allow list. The verdict is stored on the
message's receipt (`route_allowed`, plus a one-way `sender_route_digest`), so
the inbox worker and the historical reconciliation read the decision that was
actually made instead of re-deriving one.

- A delivery is marked `ignored` only when **none** of its routes is allowed.
- A refused message is resolved where that decision was made, so it never holds
  a send and is never executed later.
- The worker filters **per message**, by position against the stored verdict,
  with the sender address checked on every row as the proof the two readings
  still line up. A refused message triggers none of what follows — not its
  command, not its Chatwoot mirror, not its promo branch, and not an
  acknowledgement through the sender it named. Filtering by ADDRESS was a defect
  with an outbound consequence: a delivery carrying a START on a refused route
  and a STOP on an allowed one named an address with at least one accepted
  message, the whole address passed, the first action survived, and the positive
  acknowledgement went out through the very sender the deployment had declined.
- A delivery whose receipts cannot be lined up with its payload yields
  **nothing**: that is a disagreement about one payload, and acting on none of it
  is the honest answer.
- A delivery with no receipts at all — every event stored before this stage — is
  handled exactly as it was then.
- Re-deriving an old message's verdict from today's settings is forbidden. The
  allow list is configuration; re-reading it would turn a route that was refused
  into an executable command, or refuse one that was accepted.

Delivery **statuses** are deliberately not filtered by the allow list. A status
proves which message it is about by exact provider message id, and §46.3 checks
it against the route the attempt froze — a stronger check that does not change
meaning when the configuration does.

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

The binding is also written **when a UUID is proven later**. A refusal recorded
before any card existed has no UUID to record, so
`easyweek_uuid_identity.resolve_webhook_identity` — the one path that proves the
numeric↔UUID bridge against EasyWeek's own records — extends an existing refusal
for the numbers that proof was about. Without it the chain "STOP with no card →
card adopted → number changed" lost the refusal, because the aggregate read looks
for refusals on the numbers cards carry now. The binding never creates, lifts or
weakens a refusal, and a row already bound to a different UUID is left alone and
the disagreement recorded: one number can belong to two people over time.

When the caller names no UUID, the aggregate derives one from an unambiguous card
on that number — for the same purpose and in the same direction, to widen the
search for refusals and nothing else.

The legacy sources are **read, not replaced**. One aggregate answer consults:

1. the suppression ledger for this address and for the addresses this UUID's
   cards hold now;
2. a suppression bound to this UUID by an earlier refusal;
3. `Client.wa_opted_out` on any matching card, any branch, any provider;
4. the historical `CampaignRecipient.is_opted_out` snapshot, unless a proven,
   verified opt-in landed after that snapshot was taken **on that snapshot's own
   number**.

That last qualifier is load-bearing. §46.4.7 lets a START lift the refusal on
the number it arrived from and no other, and the reader used to compare every
snapshot against one newest-opt-in figure for the whole person: the same uuid,
two numbers, a resubscribe on one of them, and the historical refusal recorded
against the other was silently answered. The opt-in times are now kept per
address and each snapshot is ordered against the opt-in for its own number. An
unknown time on either side and a tie still refuse, as everywhere else.

The snapshot read is also per address rather than "the newest fifty rows", for a
reason a limit cannot fix: one busy number's answered history filled the limit
on its own and a live refusal on another number of the same person was never
read at all. One grouped row per matched address has no ordering and no limit,
and the number of matched addresses is already bounded by the aggregate.

**The absence of a refusal is not evidence of marketing consent.** It means only
that this database holds no proven refusal for that address.

### The address set, and who may ask for it

`matched_addresses` exposes the set this aggregate matches a person by, for a
caller that has to ask a QUESTION about those numbers rather than read the
decision. PR-22.2's §46.4.4 backlog scan is that caller: it is given the numbers
a preview's recipients are known by and reports the refusals sitting unaccounted
among them. Assembling that question from the cards left out exactly the numbers
§46.4.2 cares about most — a number the card has since dropped, reachable only
from the links written down so the ban could travel — so the scan never saw the
event and the veto afterwards had nothing to find.

Ban extension only, as everywhere in this section: a number in that set is a
number a refusal for this person could be recorded against, and never a proof
that it belongs to this customer or to their branch.

That caller reads this set, the backlog it describes and the decision below in
ONE snapshot, at REPEATABLE READ. They are three views of one state, and under
READ COMMITTED each statement takes a fresh one: a card appearing mid-read made
the question narrower than the fingerprint that was stored from it, and a START
applied mid-read let a refusal the scan had just accounted for be answered by a
ledger state that no longer existed. Nothing in this section's own writers
changed for it — the snapshot is read-only, takes no lock, and delays no
arriving STOP.

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

**A STOP always applies**, with one narrow exception. Whatever its
authenticity, whatever its timestamp. Idempotent: a duplicate is a no-op that
records itself. A late or replayed STOP cannot move the refusal's own recorded
time backwards.

The exception: a STOP that can **prove** it happened before the opt-in currently
in force is a replay of something already answered — a reconciliation re-reading
an old stored event, or Meta delivering the same payload again — and it does not
take the state back (`stop_older_than_proven_opt_in`). It is recorded and
reported. Only a provable order refuses it: an equal time, an unknown time on
either side, or a newer STOP all apply, so a tie and an unproven order both fall
to the refusal.

That exception now has exactly one definition in the code,
`stop_superseded_by_opt_in`, and `apply_consent_decision` is its first caller.
It was lifted out because a second reader appeared: PR-22.2's §46.4.4 backlog
scan asks the very same question of a STOP it finds in a stored event — "has a
proven newer opt-in on this same address already answered this?" — and a scan
with its own private answer would have drifted from the writer's. It had: the
first round of that scan treated any STOP as unaccounted whenever the ledger did
not currently read `opted_out`, which re-imposed a withdrawn refusal on every
preview. One predicate, one rule, read by both.

The bar that exception is measured against is **`decided_event_at` of the
decision in force**, and nothing else. `last_start_observed_at` and
`last_start_event_at` are audit: they move only for a START this ledger actually
honoured, and no rule reads them. Deriving the bar from the high-water mark was a
defect — the mark moved for every START, including the ones the proofs rejected,
so an unsigned attempt timestamped an hour later raised the bar to that hour and
the next genuine refusal was filed away as a replay of something that never
happened. An audit trail of rejected attempts may not license ignoring a
refusal.

**A START lifts a refusal only if all of these hold:**

- Meta's HMAC was configured **and** verified for that very request;
- the START's own event time is proven;
- the refusal's event time is proven;
- **no** accepted STOP for that address has an unprovable event time;
- the START's is **strictly** later — an equal timestamp is not newer;
- its message identifier has not been seen naming another address.

Anything else refuses with a reason code, and the refusal stands:

| reason | meaning |
| --- | --- |
| `start_authenticity_unproven` | no signature was checked for that delivery |
| `start_event_time_unproven` | the claimed event time does not parse, or is implausibly in the future |
| `stop_event_time_unproven` | the refusal's own time is unknown, or an accepted STOP for this address has an unprovable one, so "newer" has no answer |
| `start_not_newer_than_stop` | equal or earlier than the refusal |
| `start_event_identifier_conflict` | that message id already names another address |

A START for an address with no refusal creates **no row**: an absent ban is
never written down as a granted permission.

The last rule is a CHECK constraint, not only a code path: an `opted_in` row
whose decision was a START must carry a verified signature and a proven event
time, so an unsigned, replayed or time-unknown opt-in is unrepresentable.

`stop_event_time_unknown_at` records a STOP whose own event time could not be
proven. It is load-bearing: the high-water mark only ever moves to a known time,
so without it a later unorderable refusal left an older, provable one as the bar
a START had to clear — and clearing that bar lifted a refusal nobody could order
it against. **Once such a refusal exists for an address, no START lifts it.** That
is a real limitation, accepted knowingly: the alternative is to order events by
our own reception clock, which Meta promises nothing about.

### What the legacy card flag says

`Client.wa_opted_out` is a **projection of the ledger**, not a second decision.
`message_planner`, the outbox worker and every campaign path read it, so the
inbox worker writes it from the durable consent answer rather than from the
command it just read:

- the ledger refuses this address → the card refuses. A card that disagreed is
  converged without restamping one that already agrees, so `wa_opted_out_at` and
  `wa_opt_out_reason` keep saying when and why somebody refused;
- the ledger has proven an opt-in → the card is cleared;
- the ledger has **no row** for this address → the historical behaviour of the
  command stands. Nothing has been recorded about this number, so there is no
  authoritative answer to follow, and refusing to clear here would strand
  everybody whose only refusal predates this stage.

The projection runs in a **transaction of its own**, after the consent write and
before the event transaction. Both of those matter: it is then durable against
an acknowledgement that fails, a Chatwoot outage, an exception in the handler or
the process dying, and it holds nothing while the handler makes its network
calls.

It is **serialised**. Before reading the consent decision, the live projection
locks the whole delivery's matching rows in this order:

1. every **card row** of every address in the delivery, in one statement, in
   ascending id order, using `FOR NO KEY UPDATE`;
2. the matching **queued marketing `MessageJob` rows**, in ascending id order;
3. every **consent address**, sorted.

Both STOP and START addresses participate: a rejected START can still leave an
active refusal. The decision to cancel jobs is made only after reading the
current ledger under the consent lock. A proven newer START that commits while
the projection waits for jobs is therefore respected.

Card writes and job cancellations are restricted to their respective locked id
sets. Cancellation also re-checks the current phone/client scope, marketing type
and `queued` status. The projection does not acquire another card or job row
after taking consent locks. This matters when a booking update has already
locked a marketing job and is about to settle its card's consent: the projection
must wait for that job **before** holding the consent lock the update needs.

The Altegio `message_planner` takes its job locks in the **same id order**, once
before its first cancellation or `add_job`, including on a create event. Its set
is all existing `SYSTEM_JOB_TYPES` rows for the record and company, **in every
status**.
A later dedupe upsert can revive a cancelled/failed row, and `ON CONFLICT` locks
even a row that its `WHERE` leaves unchanged. Locking only the currently queued
rows would therefore leave a lower-id lock to be acquired after a higher one.
The cancellation itself remains narrower: it touches only those locked ids that
still match the original company, record, system type and `queued` predicates.
The standalone cancellation helper takes the same ordered set when the caller
has not already supplied it.

When Outbox returns the unprocessed remainder of a claimed batch after token
expiry, `_requeue_processing_jobs` also locks rows in ascending id order. It
selects only the supplied ids still in `processing`, waits for their locks
without `SKIP LOCKED`, then updates only those locked ids with `processing`
checked again. Selection and return to `queued` remain in the caller's one
transaction. This orders the return against the planner's wider lock set;
sorting the ids inside an `IN` predicate alone would not order an `UPDATE`.

The participating inbox writer already holds its card and record; the record
write orders competing booking planners. This is a protocol for their existing
job rows, not a claim to lock jobs that unrelated callers might insert later.
Neither a bulk `UPDATE` nor a particular index scan guarantees row order: an
ordinary update of `run_at` can make the visible tuple order differ from id order.

The card lock deliberately permits **foreign-key references** while excluding
other card writers. A running Outbox job may need a `KEY SHARE` lock on that card
to insert its `OutboxMessage`. A stronger `FOR UPDATE` card lock could block that
insert while the planner waits for the running job and the projection waits for
the planner's job. `FOR NO KEY UPDATE` allows the reference to finish without
allowing a concurrent phone/flag write. It does not weaken the final card veto.

Jobs created or made eligible after the job selection are outside that
cancellation set. Outbox's final card check rejects a send when the card refusal
is already committed at that check. This protocol does not promise to cancel
those later jobs or retract an external send that has already begun.

**A card that starts matching after step 1 is outside the projection's locked
set.** Its writer must converge it against the ledger before committing that
same transaction. Re-reading the set would not protect against a later arrival;
the writer's final consent section is what covers that window.

So the obligation is turned around, and it lives in `whatsapp_consent_cards`:
**whoever writes a card's phone converges that card itself**. See
*The card-writer protocol* below.
Both halves were defects. A compare-and-set on the write narrowed the window but
could not remove it: the gap that was left is between the last read and the
**commit**, so a newer STOP committing there decided the outcome by commit order
alone — the ledger refusing while the flag every sender reads said sendable,
`compute_excluded_reason` allowing the send, and the acknowledgement confirming a
subscription. No extra `SELECT` and no number of retries can close that; holding
the locks to the commit can, and this transaction may hold them because it makes
no external call. And without one order over the card rows, two deliveries naming
the same people in different orders — `[STOP A, STOP B, STOP C]` against
`[STOP B, STOP A]` — each held what the other was waiting for: PostgreSQL aborted
one, ordinary inbound has no recovery pass, so the event stayed `processing` and
the address only that delivery mentioned kept a sendable card against a refusing
ledger.

The order the commands are **applied** in is untouched. The live projection uses
**cards → queued marketing jobs → consent addresses**. Card writers finish their
business and job writes before settling consent; the historical projection uses
**cards → consent addresses** and never cancels jobs. Receipt resolution happens
in the earlier consent transaction, not in this card/job projection.

`resolve_webhook_identity` takes its consent locks **last of all**, after the
ordinary EasyWeek write for that event. That position is part of the order:
`apply_stale_contact` runs the event's business changes and touches card rows of
its own, so binding the refusal before it would have left that transaction
holding a consent lock while asking for a card row — the same inversion from the
other side. The addresses it binds are read before the number changes, so the
position does not change what is bound.

### The card-writer protocol

`Client.wa_opted_out` is what `message_planner`, the outbox worker and every
campaign path actually read, and a card can start matching a refused address at
any moment — long after any consent decision was made. So every path that
creates a card or moves a phone onto an address converges that card against the
ledger **in the same transaction**, and it does it in two steps:

1. the write site calls `note_card_write`, which takes no lock and runs no
   query — it only remembers the address and the row id;
2. the transaction's **one** consent section, `settle_card_writes`, runs after
   everything external is done and just before the commit. It takes every
   consent address the transaction needs in a **single sorted acquisition** and
   only then converges the cards.

| path | notes | settles |
| --- | --- | --- |
| `workers/inbox_worker` | `upsert_client` | end of `handle_event`, on every path including a failing one |
| `workers/easyweek_inbox_worker` | `upsert_client` | end of `process_claimed_event` |
| `easyweek_uuid_identity.resolve_webhook_identity` | the rows it adopted or created | after the business write, together with the UUID binding |
| live consent projection | — | locks cards and queued marketing jobs before consent; writes only those ids |
| historical consent projection | — | locks cards before consent; does not cancel jobs |

Both halves of that split are load-bearing.

**No consent lock across an external request.** Both providers write a card
early in their event transaction and then make an HTTP call before they commit:
Altegio looks up the service category, EasyWeek proves the two-service
snapshot. A lock taken at the write site was still held at that point, so a STOP
for the same number could not have its receipt stored until the CRM answered —
the durable inbound boundary of §46.4.6 waiting on a service lookup. Checking
that the helper itself makes no request was not enough; the callers had to be
read.

**One acquisition per transaction, not per call.** Sorting inside each helper
left the UUID resolver taking the proven number's lock in its nested client
upsert and the old number's in its own binding, which deadlocked against an
ingress holding the old one and waiting for the proven one. The resolver now
declares both sets at once, and `process_claimed_event` is told not to settle
when it runs as that resolver's business half (`settle_consent=False`) —
settling there as well would be the piecemeal acquisition again.

It only ever moves a card **to** refusing. Clearing a refusal needs the proven,
strictly newer opt-in that only the ledger can establish, and that projection
belongs to the inbox worker; a card that already refuses is never restamped,
because `wa_opted_out_at` and `wa_opt_out_reason` say when and why somebody
refused rather than when a writer noticed.

For the listed writers, card and job writes and all external requests finish
before the final consent section, including work in nested callbacks. The live
projection takes its job locks before that same boundary. Neither side waits for
a new card or job row while holding consent locks. This is the protocol for these
participants, not a universal lock-order claim about every service subsystem.

A writer outside that table — a manual card edit, a future import — can still
put an unconverged row in front of a refusal, and so can a transaction that
fails after noting a card but before its consent section. Both are caught by the
reconciliation below, which converges any card flag that disagrees with the
ledger; and any new card writer is expected to use the same pair of calls.

The compare-and-set stays on the write as a second line of defence, and it is
the only protection on the one path that cannot hold the locks: a direct
in-transaction call to `handle_event`, whose caller goes on to make external
requests in the same transaction. `process_one_event` never uses that path.

### What the acknowledgement says

The acknowledgement describes the state that **settled**, for the address the
message came from, and it is sent only when some accepted message of that
delivery asked for exactly that state.

- the ledger refuses and a STOP was among the delivery's accepted commands →
  the unsubscribe confirmation;
- the ledger has proven an opt-in and a START was among them → the subscription
  confirmation;
- the ledger has no row → the command's own historical answer;
- otherwise — a START the ledger refused, a STOP it proved to be a replay —
  **nothing is sent**, and the reason is recorded on the event.

Taking it from the first message of the payload was a defect. A delivery of
[START, STOP] from one person leaves them opted out, and the customer was told
"you are subscribed again" anyway; §46.4.7 rules that out by name. Nothing may
confirm a subscription when the settled state is a refusal. On a deployment with
no app secret the silent case is every START from a refused address, which makes
the consequence below the thing to fix first. A lifted refusal never revives
cancelled marketing jobs.

### Consequence to know before rollout

**If `META_APP_SECRET` is not configured, no START can lift a ledger refusal.**
Without the secret the webhook performs no signature check, every delivery is
recorded `unverified`, and `start_authenticity_unproven` refuses. This is
§46.4.7 working as specified — "a check that is not configured is not a check" —
and it is deliberately fail-closed.

What it does **not** change: no existing sender was rewritten, and an address
this database holds no ledger row for behaves exactly as it did before — the
command's own historical answer stands, so nobody whose only refusal predates
this stage is stranded. What did change, deliberately, is that where a ledger row
exists it is the authority: a START it refuses no longer clears
`Client.wa_opted_out`, because that flag is what Altegio and generic EasyWeek
marketing read, and leaving it cleared was the refusal being lost.

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
drains by being resolved, which is a fact rather than a cleanup. A message from
a sender route this deployment does not allow is resolved at the ingress, because
no worker will ever act on it and a permanent hold would be a bug.

The ingress takes the **same address locks** as the guard before it writes. That
is what makes the pending set a boundary rather than a best effort: a receipt
either lands before the guard reads — and holds the send — or waits for the
guard's transaction to end and is caught by the claim-time re-check. There is no
interleaving in which an unresolved refusal is invisible to both.

### The guard

`whatsapp_consent.final_send_guard` takes transaction-scoped advisory locks on
**every** address it reads — the person's own number and every number an EasyWeek
card of their proven UUID carries, always in sorted order — then answers. It
refuses when:

- a proven refusal matches the person (`consent_refusal_active`);
- anything from them is unresolved (`pending_inbound_unresolved`) — a queue that
  is behind must not read as permission;
- the address cannot be normalised (`address_not_normalisable`);
- the set of addresses changed under the locks
  (`address_set_changed_under_the_lock`), so the read is not one the locks make
  safe. Held, and the next attempt sees a settled set.

It returns a `fingerprint` over every matched `(address, consent_version)` pair,
the matched address set, the identities of the unresolved receipts, and the
verdict. A sender re-checks it with `recheck_consent_boundary` **inside the same
transaction as its send claim**, and that function re-asks the whole question:
it takes the same locks, reaches the verdict through the same code, and requires
the fingerprint to be the one the guard handed out (`consent_state_changed`
otherwise).

All three parts are needed, and the missing ones were a defect. A bare
fingerprint comparison answered only "is the state the one I was issued
against?", so a STOP sitting unresolved in the pending set — which a fresh guard
refuses on sight — compared equal and the claim went ahead; and it held no lock,
so a receipt could commit between the comparison and the claim with neither side
noticing.

One boundary the locks cannot cover, stated rather than hidden: a refusal
arriving on a number that **nothing** links to this person — no card, no
suppression row, no stamped receipt — cannot be locked in advance, because the
link does not exist until the refusal does. It changes the fingerprint, so the
claim-time re-check catches it. A number only a stored negative link names is
covered; see below.

The locks take part in no cycle with the voucher stages: they are taken by the
consent writer, the ingress and the guard, and by nothing that holds a batch
header or item lock; among themselves they are always taken in sorted order. No
HTTP happens under them, and none may.

**Sorted means the whole set, at once, on every side.** The consent worker used
to take a delivery's addresses one at a time in MESSAGE order, so a payload of
[STOP B, STOP A] left it holding B and waiting for A while an ingress for the
same two addresses held A and waited for B. PostgreSQL resolves that by aborting
one of them, and the one it aborts here loses for good: ordinary inbound has no
recovery pass of its own, so the event stayed `processing`, the suppression was
never written, and the receipts stayed unresolved. The worker now locks every
address of the delivery before it applies the first decision. The ORDER OF
APPLICATION is unchanged — commands are still applied in message order, with the
ledger's own precedence rules.

### Addresses a card no longer carries

The cards hold today's numbers, and they are not the history of the negative
links. Two stored facts are read as that history, and both only ever widen who
must not be messaged:

- a suppression row bound to this UUID — the address it names is searched and
  locked even when no card carries it any more;
- an **unresolved receipt** stamped with this UUID when the booking bridge
  proved it (`linked_easyweek_customer_uuid`). The ingress may have accepted a
  STOP seconds before the number changed, and a protection that began at the
  suppression row skipped exactly that case: the adoption found nothing to bind,
  the number moved, and the worker then wrote the refusal against a number no
  card names. The decision carries the stamped proof onto the suppression row
  (`proven_uuid_link_on_receipt`).

Neither is ever read back as proof that an address belongs to that customer, to
that branch, or to EasyWeek at all. A row already linked to a different UUID is
left exactly as it is — one number can belong to two people over time — and the
disagreement is recorded.

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
  decides before acting on the message (`meta_api_rejected`). This process
  watched that happen on its own request, so there is no webhook signature to
  require and the link to the slot is the intent committed before the request
  left — a historical attempt that froze no route is still confirmed;
- a `failed` delivery status naming the exact message id of the slot
  (`meta_callback_failed`), **with** the signature the ingress verified over the
  raw body **and** a route matching the one that slot's single attempt froze.

The second one needs all three parts. An unsigned callback is an unattributed
claim — anybody who can reach the webhook can make it — and a slot whose attempt
froze no route gives nothing to compare a claimed route against. Either way the
fact is kept as `unproven` and marks nothing: "the right message id and nothing
contradicting it" is a weaker statement than proof. For a callback this is a
CHECK constraint as well as a code path.

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
| `conflicting` | names a slot but disagrees with it — a foreign sender or recipient, a route the fact does not name at all, or a failure after `delivered`/`read`. Kept for audit; changes nothing |
| `unproven` | names the right slot and contradicts nothing, but its source's proof is incomplete: an unsigned callback, or one about an attempt that froze no route. Kept for audit; marks nothing |

### Callback rules

- An early callback — one that overtakes the commit recording the message id —
  is stored `unmatched` and bound by the acceptance that records that id.
  Matching is only ever by exact proven identity. Never by phone similarity and
  never by timestamp proximity.
- **Binding is guaranteed in either commit order**, by a transaction-scoped
  advisory lock on the message identifier that both sides take first (order:
  message → header → item, the same relative order the stage writers use). A
  callback transaction declares its WHOLE scope up front: every message
  identifier its payload names, in sorted order, and only then the production
  headers those identifiers belong to, in ascending id order. Per-status locking
  was not enough, and the counterexample is an ordinary Meta delivery — one
  payload carrying `delivered` for one message of a batch and `failed` for
  another. Handling them one at a time took the header to record the delivery and
  then asked for the second identifier, while the transaction accepting that
  second message held its identifier and was waiting for the same header. The
  cycle closed, PostgreSQL aborted one of them, and the one it aborted was the
  acceptance: Meta already had the message, the slot stayed `send_claimed` with
  no identifier, its failure stayed unmatched, and the executor gave up on an
  operation it could no longer describe. Sorting the failures after the header
  was already held could not help, because the header was the half taken first.
  After the scope is declared, no identifier lock is ever requested behind a
  header lock. That scope is also why the delivery evidence is now recorded in a
  **transaction of its own**, committed before the inbound half of the same
  delivery runs: the inbound half acknowledges the customer and mirrors messages
  into Chatwoot, and those locks must not be held across a network call. Before
  this phase a `sent` callback took no header lock at all, so holding one through
  an external request was a new way to block a batch — another session asking for
  the header with `FOR UPDATE NOWAIT` got `55P03`, which is a refused claim, an
  acceptance that cannot be saved and an operator STOP that cannot be applied.
  The scope itself is not narrowed by status: narrowing it is what the locks
  exist to prevent. The
  window it closes cannot be closed by re-reading: the callback looks for the
  slot before the identifier is committed and the acceptance looks for unmatched
  facts before the callback is committed, so both are correct about what they saw
  and the fact ends up owned by nobody. `SELECT ... FOR UPDATE` does not help
  either — there is no row to lock in either direction — while an advisory lock
  on the identifier exists before the rows do.
- A bound fact is re-judged from scratch and is never promoted for having
  waited: an unsigned early callback binds as `unproven`, a foreign-route one as
  `conflicting`.
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

### The positive half, added by PR-22.2

§46.3 asks the same of a `delivered`/`read` that it asks of a `failed`, and
PR-22.1 recorded no proof behind a positive status: the slot's timestamps are
the mailing's own record and keep neither the signature nor the route. PR-22.2
adds `easyweek_voucher_production_delivery_receipts` beside this table, with the
same vocabulary and the same early-callback protocol — `unmatched` kept on the
identifier alone, bound by the acceptance that records it, re-judged against the
frozen route — and the full contract lives in
`VOUCHER_FOLLOWUP_PREVIEW_CONTRACT.md`. Nothing in this section's table, its
match states or its slot marking changed, and the positive transition still
applies whether or not a proof could be stored.

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
- Each candidate is written in **two transactions**, for the same reason the
  live path is split: the ledger first, under the address lock alone, and then
  the card flags — card rows by id, then the consent address, writing only the
  rows it locked. The refusal is therefore durable before any card is touched,
  which is also what lets every writer that commits afterwards read it and
  carry it; while the two shared one transaction there was an interleaving
  nobody covered, and a run that gave up left a known STOP with no durable
  refusal at all.
- **A refusal in the ledger is not proof that the cards are done.** Because the
  two halves commit separately, a lost card transaction leaves a refusal on disk
  and a card still reading as sendable — and treating the ledger row as "already
  handled" left it that way, run after run, while the real campaign classifier
  kept allowing marketing. A snapshot candidate whose refusal is already active
  is therefore checked against the cards: if any matching card still says
  sendable, the candidate is projectable for its **card half alone**. No
  evidence is written a second time — re-applying a snapshot would stamp a fresh
  "order unknown" onto a refusal that may have a provable time, which is a
  downgrade, not a repair — and the card half re-reads the ledger under the
  address lock, so a proven, strictly newer START that landed in between is
  honoured and nothing is written. `card_recovery` counts those candidates and
  `cards_converged` the flags actually fixed, which is how a repeat of the
  evidence is told apart from a repair of the cards.
- A refusal it writes is also projected onto the card flags it locked, in the
  refusing direction only, through the same `converge_cards_with_ledger` every
  card writer uses. `cards_converged` counts them.
- Without that convergence a run left the ledger refusing and
  `Client.wa_opted_out` saying sendable, which is the refusal being ignored by
  everything except the new consent guard. Queued marketing jobs are **not**
  cancelled — that is one of the inbox worker's business actions and §46.4.4
  keeps this module out of them; the outbox re-reads the flag before it sends.
- A historical START is never projected. Its authenticity is unrecoverable from
  a sanitised stored event, so it could not lift a refusal even if it were
  applied, and writing it would create an opt-in record out of an unverifiable
  payload.
- A flagged card with no usable number is counted, never guessed at.
- A Chatwoot mirror is not counted a second time.
- The **sender route decision of the day is honoured, never re-made**. An event
  the ingress marked `ignored` named no allowed route, and a message whose
  receipt says `route_allowed = false` was refused individually; neither is
  projected, and both are counted (`events_route_not_allowed`,
  `messages_route_not_allowed`). Otherwise `--apply` would be the one way to
  apply exactly the commands the configuration exists to refuse. Where the
  per-message verdict was never recorded — every event older than that column —
  the event-level status is what is known, and the report says so every run
  (`historical_per_message_route_unrecorded`).
- **The decision is made again inside the write transaction**, under the address
  lock, not only when the candidate was collected. A **snapshot** — a card flag
  or a preview row — with no event time of its own used to be written as a fresh
  refusal even after a proven opt-in for that address had been observed later;
  and because such a refusal has no provable order, §46.4.4 then makes it
  permanent. It is now reported as `superseded_by_proven_opt_in` /
  `projection_older_than_proven_opt_in` and written nowhere. Both sides of that
  comparison are this process's own clock, so no external timestamp is invented,
  and it is strictly per address: an opt-in proven for another number of the same
  person is not an opt-in for this one.
- That rule applies to **snapshots only**. A stored STOP MESSAGE whose timestamp
  cannot be read is not stale because of when this process happened to receive
  it: a delivery can sit in a queue, and a signed START sent at 10:10 can arrive
  at 10:30 behind a STOP received at 10:20. Dropping it on that basis made the
  reconciliation and `apply_inbound_consent` reach different answers about one
  and the same message — live processing applies it and refuses, while the
  projection skipped it and the send went ahead. A real message goes through the
  ordinary ledger rules, where an unknown order refuses, and the preview uses
  the same predicate as the write.
- A **stored STOP is applied even when a refusal is already active**, which is
  how the boundary a future START has to beat moves to the newest known
  refusal. The collection-time shortcut used to skip every candidate once any
  refusal was active, so a stored STOP from 11:00 never advanced a ledger
  refusing from 10:00 — and a START from 10:30 then lifted a refusal this
  database could prove was older than a STOP it already held. A card flag or a
  snapshot is still skipped in that case: it carries no time of its own, so it
  has nothing to add.
- A STOP the ledger has already moved past is not re-applied: a proven, verified
  opt-in that came after it stands, and the run reports the STOP as a no-op.
  This is what makes the reconciliation safe to run nightly rather than once.
- The counters describe what the writes actually did. `written` is the number of
  refusals that really landed, `noop` the ones that changed nothing,
  `superseded` the ones the re-decision declined, and
  `newly_suppressed_addresses` counts people who were sendable before the run
  and are not after it.
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

**The downgrade refuses while any of the facts it would destroy exists, and it
refuses before any DDL**, under `ACCESS EXCLUSIVE` locks taken first. Both halves
matter: checking emptiness without the locks would race a concurrent write, and
dropping first would mean the data was already gone. An operator told the
rollback is unsafe finds every fact still exactly where it was.

"Any fact" includes the ones that live in **columns on existing tables**, not
only the ones in the new tables: a frozen send route and a recorded inbound
authenticity are both unrecoverable — the route's two sources are mutable and the
signature header is stripped before the event is stored — and a re-upgrade brings
them back as NULL. So the emptiness check and the locks cover
`easyweek_voucher_production_batch_attempts` and `whatsapp_events` as well. Both
are hot tables, which is the point: a destructive downgrade is an offline
operation and behaves like one.

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
| Historical reconciliation, its limits, racing live ingress, route provenance | `tests/test_whatsapp_consent_projection.py` |
| A refusal surviving a UUID proven later and a number change | `tests/test_whatsapp_consent_uuid_adoption.py` |
| Consent locks outside HTTP and one address order across nested writers | `tests/test_whatsapp_consent_card_locks.py` |
| Booking reassignment racing STOP, multi-job order, dedupe and Outbox foreign-key locks | `tests/test_whatsapp_consent_job_locks.py` |
| Migration cycle, ORM parity, destructive-downgrade refusal, PG16 concurrency | `tests/test_easyweek_voucher_mailing_durability.py` |

All are required. No marker, skip or xfail was added. A change to shared inbound
also requires `uv run pytest -q -m legacy_altegio`, per
`docs/ops/test_suite_tiers.md`.

The mutual-block cases are covered by deterministic PostgreSQL 16 schedules
rather than by repetition: a third transaction holds the first lock of the
agreed order and the code under test must be queued on it **holding nothing**,
which is the property that rules the cycle out. Each of them reproduces a real
`deadlock detected` when the fix is reverted — the consent worker against the
ingress, two deliveries naming the same people in different orders, a mixed
`delivered`/`failed` callback against the acceptance of another slot of the same
batch, and two callbacks naming the same identifiers in opposite payload order.

The serialisation of the card projection is covered by starting the competing
decision inside the projection's own window and then asserting, through
`pg_locks`, that it is **waiting** — if it could run, the window would still be
open. The card-writer protocol is covered with the **real writers**, not with a
stand-in: the production Altegio importer moves an existing card onto a refused
address and creates a fresh one there, the identity path moves a card inside a
projection's own window, and a second proof of the same booking takes that row
and queues on the consent lock. Each test asserts the ledger, the card flag, the
reason label, `compute_excluded_reason`'s actual verdict, the event's status and
the absence of any external send. Reverting the convergence leaves the flag
false with the ledger refusing; reverting the id-scoped write reproduces the
reported `deadlock detected` exactly — one process waiting for the advisory
lock, the other for the row. The release of the delivery locks is asserted from inside the external
call itself: the acknowledgement probes the batch header with
`FOR UPDATE NOWAIT` and the message identifier with `pg_try_advisory_xact_lock`,
for every status kind including `sent`, and both must be free.

The job-lock regressions use a real Altegio booking reassignment, its real job
planner and a claimed WhatsApp STOP. They cover the job/consent boundary and
multiple jobs whose visible tuple order differs from their id order after an
ordinary `run_at` update. A SQL barrier fixes the scheduling window; a synthetic
deadlock exception does not stand in for the database. Both events must finish,
both cards must refuse, and the queued marketing jobs must be cancelled without
replaying external actions. Further cases cover existing dedupe rows outside
`queued` and a running Outbox job that needs a foreign-key reference to the card.
Unexpected HTTP requests are blocked in these reproductions.
