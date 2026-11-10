# EasyWeek voucher follow-up SEND — contract (PR-22.3)

This is the tracked contract for the follow-up **send**: how one extra WhatsApp
message per recipient of an original production voucher mailing is authorised,
executed exactly once, recorded and stopped.

It describes what is now true in the code. It is **not** a statement that a
follow-up may be sent on a production audience: the send has its own fence, it
is closed by default, and §11 below lists the prerequisites that are not
satisfied.

Read with `VOUCHER_FOLLOWUP_PREVIEW_CONTRACT.md` (PR-22.2, the proposal this
acts on), `VOUCHER_FOLLOWUP_CONSENT_EVIDENCE.md` (PR-22.1, the consent and
failure evidence both stand on), `VOUCHER_PRODUCTION_MAILING_RUNBOOK.md` (how
the original mailing is operated, and §18 there for this one's operation) and
`VOUCHER_MAILING_BOOKING_REPORT.md` (what the local report does and does not
prove).

---

## 1. The four steps, and why they are four

1. **Opening the page** computes nothing and writes nothing.
2. **Checking the composition** stores an immutable preview version. It spends
   no attempt, reserves nobody and is not permission (PR-22.2).
3. **Preparing a send** re-proves everything and stores one immutable, single-use
   offer with its own expiry. It still sends nothing and spends nothing. What it
   adds is a number: how many messages the next click can produce.
4. **Confirming** consumes that offer and creates one durable run. Only the
   dedicated executor then sends, and it sends at most one message per recipient.

Each step refuses on its own terms. Nothing about step 4 can be reached by
repeating step 3, and nothing about step 3 can be reached by repeating step 2.

## 2. The fence

`EASYWEEK_VOUCHER_FOLLOWUP_SEND_ENABLED`, default **false**.

It is read by the follow-up code and by nothing else.
`EASYWEEK_VOUCHER_PRODUCTION_MAILING_ENABLED` — the primary mailing's
permission — is deliberately **not** consulted anywhere in this path, so a
deployment running the original mailing still refuses every follow-up plan,
every confirmation and every claim.

`EASYWEEK_VOUCHER_FOLLOWUP_EXECUTOR_ENABLED`, default **false**, says the
deployment runs the dedicated executor. It is not a second fence and authorises
nothing: with it true and the send fence false, every action still refuses. Its
only job is to let a confirmation refuse up front instead of parking in a queue
nobody drains.

What is **not** fenced is the truth arriving about a follow-up that was already
sent. A `delivered` / `read` / `failed` callback about one of its messages is
recorded whatever the fence says, because dropping it would corrupt the record
of a send that really happened. The fence exists to stop new effects.

## 3. Where the one extra attempt is consumed

**At the claim, and the claim is a committed transaction that happens strictly
before the external POST.**

Precisely: the committed transaction that moves this recipient's follow-up row
to `claimed` with `attempt_count = 1`, a durable intent and the frozen route.
`send_identity.ATTEMPT_BOUNDARY` states it in one sentence so the code, this
document, the runbook and the tests all point at the same line.

Once that commit returns, this recipient's extra message is spent whatever
happens next. A crash, a timeout, an unreadable answer, a restart, a second
confirmation, a new preview and a new run all find the row occupied, and none of
them gives the attempt back. What is available afterwards is reconciliation of
facts, never a resend.

In the one transaction the claim also re-proves the remaining LOCAL conditions —
the whole per-attempt check, once more, with every lock already granted. That is
not a duplicate of the pass before it: this transaction can WAIT for a lock, and
everything the caller proved describes the world before that wait. A reply
processed, a template switched off, a read receipt moving the row out of the
selected groups or an identity reassigned while the claim was queued are all
caught here, and the consent fingerprint is a version of CONSENT and of nothing
else. Local reads only, so nothing external runs under the locks.

The claim also re-reads captured booking evidence after the locks are granted,
using the current time and the complete booking history proved before the claim.
A new booking captured during the wait excludes the recipient without spending
the attempt. A new fact that cannot be attributed safely leaves eligibility
unproven; it is not treated as an empty booking history. A proven booking of
another customer does not exclude this one.

Freshness is compared using bounded sets of canonical delivery fingerprints
kept only in memory. A reschedule of the same booking UUID invalidates its
cached live state; an identical delivery does not. Newly visible events are
checked even when their receipt time predates the external read. Unattributable
changes remain unknown. These extra capture fields are not added to the booking
report, approval, audit or stored preview.

Each new delivery's customer is checked in its own branch. A booking created by
somebody else can since have been assigned to this recipient, so its creator
does not prove the change unrelated. A new delivery naming this recipient or
leaving its owner unproven invalidates the cached history, including when the
creation itself is disputed. Changes proven to concern other people remain
allowed. The check uses the new deliveries, not every historical holder, and
does not change the report's attribution of the original creation.

And:

* takes the **original batch header's row lock** and reads that batch's terminal
  stop under it. That header is the lock the primary mailing's own
  `request_stop` takes first, so an operator stopping the original mailing and
  this claim are serialised by PostgreSQL: once the stop has committed the claim
  sees it and refuses, and once the claim has committed the stop waits for it
  and finds the attempt already spent. Reading that stop without the lock was a
  confirmed defect — a claim could prove "no stop", the stop could commit and be
  reported to the operator, and the attempt was spent anyway;
* re-proves the consent boundary under the consent **address locks**, through
  `whatsapp_consent.recheck_consent_boundary` — so a durable refusal, a durable
  unresolved inbound message, an address set that moved, or a consent decision
  that landed and was answered since all refuse here;
* re-reads the run under its row lock and refuses if the operator stopped it or
  if this executor no longer holds the lease.

No network call happens inside it.

## 4. One attempt, and the scope of the key

The uniqueness lives in `easyweek_voucher_followup_sends`, as three **partial**
unique indexes over the states that account for the attempt:

| Index | Columns | Scope |
| --- | --- | --- |
| `uq_ew_followup_send_source_slot` | `(source_batch_id, source_slot)` | the slot of the original send |
| `uq_ew_followup_send_source_customer` | `(source_batch_id, easyweek_customer_uuid)` | the canonical person **within that mailing** |
| `uq_ew_followup_send_source_entitlement` | `(source_entitlement_digest)` | the original voucher artifact (its digest already carries the batch and the slot) |

The predicate is `status IN ('claimed','accepted','delivered','read','rejected',
'failed','unknown','stopped')` — every spent attempt **and** every terminal stop
tombstone.

**Why this scope, and not the customer for life.** The entitlement a follow-up
talks about is the one the original mailing issued, so the thing that may happen
at most once is "one extra message about THIS original send". Keying on the
original batch and the original entitlement says exactly that. Keying on the
customer uuid alone would say something else — that this person may never
receive a follow-up again, including the follow-up of a future, independent
primary mailing — and §46.7 rules that out by name. The customer index is
therefore batch-scoped: it catches a duplicated card or a second representation
of the same human **inside one original mailing**, which is the real bypass, and
says nothing about other mailings.

**What the key is not bypassed by.** A different group selection, a new preview
version, a new follow-up run, a different template, a second tab, a second
executor, a duplicated card, a restart. Each of them ends at the same three
indexes, and the loser's transaction is rolled back by PostgreSQL.

`planned` and `skipped` are deliberately outside the predicate: the first is a
reservation §46.7 allows to be recomputed and the second spent nothing, so
neither blocks a later preview.

## 5. The offer, and what a browser cannot name

An approval row (`easyweek_voucher_followup_approvals`) is written by the server
and pins:

* the operator (account plus session fingerprint) and the one `action`;
* the preview version and its composition digest; and, for the exact **sendable
  subset** the operator was shown a number for, that subset's own digest, its
  count and its `offered_source_item_ids`. All three are derived from one list,
  which is the fix for a confirmed defect: the digest used to cover every
  included row of the preview while the count covered only the sendable subset,
  so a recipient who regained eligibility between the offer and the confirmation
  joined a run the operator never saw them in. A count comparison could not have
  caught it either — one person swapped for another leaves the count identical —
  which is why the identities are pinned and not only their number;
* the selected groups and the explicit minimum interval;
* the whole message contract: code, Meta name, language, category, parameter
  order, body digest, plus the preview contract and failure policy versions;
* the branch sender's digest;
* the source batch's frozen digest, its request schema and its **stop
  generation** at plan time;
* `issued_at`, `expires_at` (ten minutes from issue, never from the write; the
  offer is already expired at `expires_at`) and a nonce;
* a keyed signature over all of the above.

A request may name a batch (in the path) and a preview version. Nothing else. A
request that tries to name a recipient list, a count, a digest, a template or an
operator is refused by name (`extra="forbid"`), and the three values a
confirmation does carry — approval id, signature, count — are **compared** and
discarded.

A confirmation **may only narrow** the offer. A row the offer did not cover is
written down as `skipped` with `followup_not_offered` and waits for the next
offer; it is never added to this run.

A confirmation is refused when: the signature does not verify, the presented
signature is not the stored one, the action is not this ledger's, the principal
differs, the offer has expired, the count differs, the message contract or the
failure policy moved, the live Meta approval is gone, the sender or the frozen
composition moved, the source batch was stopped (or stopped again) since, the
offered rows are no longer the preview's own or no longer digest to what was
offered, the approval carries no pinned offer at all (fail-closed for a row
written before that column existed), or another run of this mailing is in flight.
An older signature may fail validation before the missing-offer check.

The admission — both stop reads and the "one active run" check — happens under
the original batch header's row lock, so two different approvals of one source
batch resolve against each other. On the reviewed commit both passed an
unserialised `SELECT` and queued two runs with the same recipients, which is
also what later made a terminal stop abort.

A second confirmation of the same offer returns **the existing run** with
`created: false`. One offer becomes at most one run: the approval's single-use
status under its row lock, plus `UNIQUE (approval_id)` on the run.

For a pending offer, expiry is checked against fresh time after the required
locks, immediately before consumption. Time spent checking the template or
waiting for a lock does not extend the ten-minute permission. Returning an
already consumed offer's existing run is a read of history, not a new permission.

A database exception at COMMIT does not prove that the transaction rolled back.
Confirmation distinguishes a proven refusal, a saved run and an unknown result.
An uncertain write is reconciled by reading the corresponding approval and run,
with the same operator and signature checks. It is never reconciled by repeating
the confirmation. If the read is unavailable, the page reports uncertainty until
the saved state can be read and its STOP control shown.

Cancelling preparation is available only before the confirmation request starts.
During confirmation, cancellation and preparation controls cannot dismiss or
replace the pending confirmation. Once a run exists, its terminal STOP is the
operation that cancels remaining work; hiding a panel does not cancel a run.

## 6. Execution

A dedicated executor (`workers/easyweek_voucher_followup_worker.py`,
`altegio-easyweek-voucher-followup-executor`), one instance, no rolling deploy.

It is **not** the old `followup_worker`, not the generic Outbox, not the
campaign worker and not the production voucher executor. None of them is reached
from this path and none of them gains authority over these rows. The generic
paths exist to retry what they find, which is right for an idempotent job and
wrong here.

Per planned row, with nothing held across a request, and four phases rather
than two:

1. **local** — the source attempt and its group, the sender geography, the
   interval, the failure policy, the entitlement, the identity, the consent
   aggregate, the stored-event backlog, the replies, **the approved contract,
   the local template row and the active sender**. The last three are
   re-proven for every attempt, not once per plan: a template switched off after
   the confirmation, a template whose body was edited, and a sender deactivated
   or repointed between two recipients each stopped nothing on the reviewed
   commit;
2. **the live Meta proof** of the exact APPROVED template, in this evaluation.
   An approved template can be paused between two rows, so a previous proof is
   not evidence — and it must be the contract this run was confirmed under, not
   merely some approved template;
3. **the external proofs** — the original artifact and the live fully paginated
   booking history, plus the stored creation deliveries;
4. **local again**, in a fresh snapshot. It must still be eligible, and it must
   still agree with the first pass about the addressee, the route and the
   consent version; the values the claim then acts on are the second pass's.
   Without this, anything local that moved while the network was busy was never
   looked at again: a proven `read` arriving for a row chosen as "delivered,
   read not confirmed", and the customer's own reply being processed, both left
   the row eligible and the executor sent. A move between two groups the
   operator **already selected** is still allowed — that is §46.5 — and a move
   out of the selection is refused with its own reason.

A row that lost eligibility in any phase is `skipped` with that reason, and
**nobody takes its place**. Then:

5. claim (§3);
6. one POST, through `VoucherDeliveryClient`: one request, no redirect followed,
   no backoff, no attempt count, no requeue;
7. record the answer — `accepted` with its message id, `rejected`, or `unknown`.

Outcomes, and what each means:

| Row state | Meaning |
| --- | --- |
| `accepted` | Meta took responsibility for the message. **Not** delivery. |
| `delivered` / `read` | a proven callback said so (§8) |
| `rejected` | Meta refused the request itself. The attempt is still spent. |
| `unknown` | timeout, transport failure, 429, 409, 5xx, redirect, unreadable answer, 2xx with no message id, or a crash past the boundary. The attempt is spent; no resend. |
| `skipped` | lost eligibility before the boundary. Nothing spent. |
| `stopped` | terminal stop tombstone. Nothing spent, and it blocks for good. |

A crash between the claim and the answer leaves `claimed`. The next sweep — the
executor's own loop, or its start-up pass, which interrupts every `running` run
because one executor cannot own a row from a previous process — turns every such
row into `unknown` and the run into `interrupted`. There is no transition back
to `planned` and no transition back to `queued` anywhere in the module.

The run itself is never called "delivered": its six states are `queued`,
`running`, `completed`, `stopped`, `interrupted`, `refused`, and what reached a
person is the per-row status.

Nothing in this path creates a voucher, pays for one, refunds one, replaces a
code or extends a term. The original slot's `send_attempt_count <= 1`, its
attempts, its receipts, its failures, its HMAC bytes and its schema 3/4 rules are
read and never written.

## 7. STOP

Terminal, and it is the operator's own control.

`POST .../followup/stop` sets the stop on the run and writes a `stopped`
tombstone for every row that has not been claimed. It is available while the run
is queued, running or already finished, and **with the fence closed** — those are
exactly the moments an operator wants it. Pressing it twice is reported as such.

* A stop that becomes durable **before** a row's boundary produces **zero** Meta
  requests for that row and every row after it, and the executor ends the run
  rather than skipping one person — tombstoning every row it had not started on
  the way out. That also covers the ORIGINAL mailing's terminal stop, which no
  follow-up row knows about until the claim reads it: those rows are tombstoned
  too, because that stop is irreversible. Every other refusal of the batch gate
  — a sender momentarily gone, a template not proven — is **not** terminal, so
  those rows keep their reservation for a later run;
* A stop is **idempotent against the key**: a recipient another run already
  accounts for, by a spent attempt or by its own tombstone, is recorded as
  already accounted for instead of being tombstoned twice. Writing the second
  tombstone used to violate a partial unique index and abort the whole
  transaction, which left `stop_requested_at` NULL after the operator had been
  told the run was stopped. The stop itself always commits;
* A request already in flight cannot be promised cancelled. Its row keeps its
  own state and its own honest outcome, and the report shows both moments.
* The tombstones occupy the single-attempt key, so a new run, a new preview, a
  new group selection and a new template cannot reopen those recipients.
* A terminal stop of the **original** batch forbids creating or executing its
  follow-up as well, checked at the plan, at the confirmation and inside the
  claim.
* Nothing clears a follow-up stop. A later START does not resume a stopped run.

## 8. Callbacks about the follow-up's own messages

Routed by three things at once: the exact `provider_message_id` the row recorded
(`UNIQUE` across every run), the sender and recipient digests that row **froze**
with its claim, and the signature the ingress verified. Anything short of all
three is stored and marked rather than acted on.

* Meta ACCEPTING the request is not a proven outcome, while a signed `failed` on
  the frozen route is — so a proven failure outranks both `accepted` and
  `unknown`, and such a row moves to `failed` with its reason and in the live
  report. On the reviewed commit `failed` ranked below `accepted`: the fact was
  stored and the row went on reading "accepted by the provider" about a message
  Meta had said it would not deliver;
* a proven DELIVERY still outranks a failure. `read` outranks `delivered`, and
  both outrank a late `failed`: that failure is kept as `conflicting`, counts a
  contradiction and rolls nothing back. A failure after an `accepted` is **not**
  a contradiction — the provider did not contradict itself, it finished its
  account of one message. Both timestamps are recorded even when the status does
  not move.
* Duplicates and reordering are one fact: the key is computed from the
  callback's proven contents, never from when it was processed.
* An early callback — Meta can deliver a status while the commit recording the
  message id is still in flight — is kept as `unmatched` if it is signed, and
  bound by that very commit on the exact identifier, under the identifier's
  advisory lock. Never by phone similarity or timestamp proximity. An unsigned
  early callback is not kept at all.
* A foreign sender or recipient, a message id this phase does not own, and an
  unverified signature each prove nothing and move nothing.
* **Follow-up callbacks never touch the original message**: not its status, its
  counters, its receipts, its failures, the booking report or that report's
  anchor. Follow-up attribution is explicitly not part of this stage.

## 9. The lock order

The order is derived from the paths that already exist rather than chosen for
this phase. Two of them matter: the primary mailing takes the production batch
**header** first and then the item, in every claim and in `request_stop`; and
the WhatsApp status/inbound unit of work takes the message-identifier advisory
locks, then the production headers behind them, and only then — when the same
transaction goes on to process that delivery's inbound messages — the consent
**address** advisory locks.

So the existing global order is
`message id → production header → production item → consent address`, and this
phase extends it to the right rather than inserting anything to the left:

**production batch header → consent address → follow-up approval → follow-up run
→ follow-up send.**

* a **confirmation**: the original batch header → the approval row → the run and
  its rows;
* a **claim**: the original batch header (reading that batch's stop under it) →
  the consent address locks → the run row → the send row, and then the final
  local re-check inside that same transaction;
* a **stop**: the original batch header → the run row → its send rows. The
  header is what makes "which recipients does another run already account for"
  unable to change under the transaction;
* a **callback**: the message-identifier advisory lock (the one
  `lock_callback_scope` already holds for every identifier in a delivery) → the
  send row.

Header-before-consent is the direction the status/inbound path already uses, so
nothing here can be the reverse half of a cycle. No HTTP call happens under any
of these locks: every external proof runs before the claim opens its
transaction.

## 10. The message

Unchanged from PR-22.2, and it is the contract:

* local code `new_client_voucher_followup_v1`; Meta name
  `kitilash_ka_new_client_voucher_followup_v1`; language `de`; category
  `MARKETING`; `POSITIONAL`; exactly one `BODY`; parameters `client_name`,
  `booking_link` in that order;
* local body carries `{client_name}` and `{booking_link}`; the Meta body carries
  `{{1}}` and `{{2}}`;
* the text:

  > Hallo {client_name}, für Sie ist bei uns ein Gutschein über 10 € hinterlegt.
  > Termin buchen: {booking_link}. Wenn Sie keine Nachrichten mehr von uns
  > möchten, antworten Sie mit STOP (СТОП).

The local row must be the **only** one for this provider, company, code and
language, and it must be active; an inactive duplicate is not ignored, it makes
the contract unproven. A live Meta read must prove the exact APPROVED contract
in the evaluation that uses it — a previous proof is not evidence.

The two parameters are built per message from the proven card's display name and
the branch's booking page from the server-side registry. A missing link or a
missing name refuses that row rather than sending a message that is not the one
Meta approved. No voucher code is part of the message: this application stores a
keyed MAC and never the code.

Nothing in the application creates or changes a Meta template, and no migration,
startup hook or shared seed adds the local row. Its controlled preparation is a
separate operational step, described in the runbook's §18.2.

**The failure-group policy still admits nobody.** `followup-failure-policy-v1`
permits no cause, so every confirmed-failure row is excluded with a readable
reason. That is deliberate and is not worked around here: an allowlist or a
waiting time would be a separate owner decision.

## 11. Privacy

Never stored in this ledger, never logged, never in an approval, an audit row, a
report or an error: a voucher code, a phone number whole or partial, a customer
name, a customer or order uuid, a raw Meta identifier beyond the message id the
routing needs, a rendered BODY, a secret, a raw provider response. The ledger
keeps one-way digests, reason labels, counts and timestamps.

The page shows slot numbers, group labels, state labels and reasons in words,
plus the message as a shape with square-bracket placeholders.

## 12. Verification

Behavioural, on PostgreSQL 16:

* the fence closed by default; the primary mailing's permission not opening it;
  a confirmed run refusing to execute when the fence closes after confirmation;
* the offer's exact count, its signature, its TTL measured from issue, the
  operator binding, the action binding, the count comparison, a changed template,
  a stop of the original batch, a stale preview, a missing preview, and one run
  at a time per mailing;
* one message per recipient; a second run sending nobody; another group
  selection, another preview and another template not buying a second attempt;
  the three partial indexes refusing a hand-promoted row; and the key's scope
  proven to be the original mailing rather than the customer for life;
* zero Meta requests when a refusal or an unresolved inbound message is durable
  before the boundary; a stop mid-run leaving the started attempt alone and
  sending nothing further; a terminal stop with zero attempts leaving a durable
  tombstone that a new run cannot bypass;
* a crash past the boundary becoming an honest `unknown`; a start-up sweep
  turning `claimed` into `unknown` and never into `planned`; an interrupted run
  never claimed again; an `unknown` and a `rejected` both consuming the attempt;
  exactly one POST per recipient whatever the answer;
* the original mailing's rows, counters, bindings and statuses unchanged;
  no `MessageJob`, no `OutboxMessage`, no voucher mutation, no production
  operation row;
* callbacks: proven `delivered` then `read`; duplicates; a `read` before a
  `delivered`; a late `failed` after a `read`; a foreign sender; an unsigned
  callback; a foreign message id; an early callback kept and bound; an unsigned
  early callback not kept;
* the executor: one run claimed at a time, two executors not both taking it, a
  claim refused without the lease, a claim refused when the consent fingerprint
  moved.

Concurrency, with the races run for real rather than slept through: two
concurrent confirmations of one offer producing one run; two executors racing one
row granting exactly one attempt; a stop racing the boundary never producing both
a tombstone and a spent attempt for the same recipient.

Over HTTP: anonymous, missing CSRF, cross-origin and token-authenticated callers
refused on every send action with the ledger unchanged; the whole authorised
scenario reaching exactly one message per recipient; a double click and two tabs
producing one run; an offer of another mailing refused; an expired offer refused;
a wrong count refused; a request naming a recipient list refused; the fence
refusing with a readable reason; the page and the status endpoint starting
nothing; a reload after a confirmation not duplicating the run; the stop
available with the fence closed; a run of another mailing not stoppable from this
page; and no personal value or secret anywhere on the page or in any answer.

Browser acceptance, in Chromium against the real pages: group selection, the
explicit interval, the composition check, the separate confirmation stating the
exact number, cancellation, the confirmation, a **partial** execution ending in
one `accepted`, one `unknown` and one terminal-stop tombstone shown as three
different words, the original message's status shown beside the follow-up's, and
a new preview plus a new run proven unable to reopen the stopped recipients.

Additional boundary regressions cover bookings captured while the claim waits,
including newly received and ambiguous facts; a real COMMIT whose acknowledgement
is lost, with both available and unavailable reconciliation; cancellation during
an outstanding confirmation; and expiry during external validation or a lock
wait. Controls prove that unrelated bookings and still-live approvals continue
to work, and that no recovery path repeats a confirmation or sends a message.

Round 2 of review added these, each verified to fail on the previous commit:

* a stop that waited for the batch header while an executor claimed the run —
  the stale ORM read used to write ``stopped`` over an active lease, violate
  ``ck_ew_followup_run_lease_only_while_running`` and roll the whole stop back;
  two concurrent stops; a run that finishes while a stop waits keeping its own
  outcome;
* **every** result of a concurrent confirmation of one approval carrying that
  approval's run, with exactly one ``created``. Nothing is filtered out of the
  assertion, which is how the losing refusal was being hidden;
* a processed client reply, and a deactivated local template, each committed
  while the real executor's claim is provably queued behind the batch header
  (``pg_stat_activity`` is the barrier) — zero requests, a reason, and
  ``attempt_count = 0``; plus the positive control, where nothing moves and
  exactly one attempt happens;
* an unreadable 502 answer to a confirmation that really committed: the page
  reports an unknown result, finds the stored run, and stops it in the browser;
  and the separate case of a request that never reached the server;
* a live pending offer blocking the column's rollback, an expired one not
  blocking it, the TTL boundary from both sides, a consumed approval and the
  ledger's own guard keeping their guarantees, and a concurrent writer unable to
  slip past the table lock.

Round 1 added these, each reproducing a path that really sent or really lost a
stop:

* the claim **waiting** for the original batch header while a stop is written
  inside that header's transaction, and refusing once it is released; the other
  order leaving exactly one spent attempt with the rest tombstoned; a repeated
  source stop changing nothing;
* a recipient who regained eligibility after the offer not being added to the
  run; one person not substitutable for another at the same count; an approval
  with no pinned offer refused; editing the pinned offer breaking the signature;
  and the unchanged composition confirmed in full;
* a template deactivated after the confirmation, edited, switched off between two
  rows, or not APPROVED right now (paused, pending, disabled, unreadable listing)
  sending nobody; a sender deactivated between two rows or repointed at another
  number sending nobody;
* a proven `read`, a processed reply, an identity change and a refusal each
  landing **during** the external reads and excluding that row with zero
  requests; and the in-selection group move still sending;
* two different approvals of one batch behind a header barrier producing at most
  one active run; one approval repeated returning the same run; a terminal stop
  surviving a recipient another run already accounts for; a stopped run not
  bypassed by a later confirmation;
* a proven `failed` after `accepted` and after `unknown` moving the row, said in
  the live counters; an early `failed` bound by the commit that records the
  message id; duplicates and reordering; the original message untouched.

Migration: two additive revisions over the previous head; upgrade, downgrade and
re-upgrade on an empty ledger; the destructive downgrade refusing **before any
DDL** while a spent attempt or a terminal tombstone exists; the offer-identity
column's own downgrade refusing while an unexpired PENDING offer carries one; the
historical tables untouched by the rollback.

## 13. Rollout prerequisites — NOT satisfied

Before this is used on a production audience, in this order:

1. the schema and the application deployed with the fence **closed**;
2. the local template row prepared by the controlled step in the runbook's
   §18.2, and a live Meta read proving the exact APPROVED contract;
3. the historical unsubscribe reconciliation and the delivery evidence reviewed,
   with no external effects — a stored `source_gaps` does not become proof;
4. the interval chosen by the owner, and the groups chosen deliberately: the
   confirmed-failure group currently yields an honest zero;
5. the dedicated executor deployed, one instance, no rolling deploy;
6. every required gate green, including the browser acceptance;
7. a preview of the concrete audience read in full, with its exclusions and
   unknowns;
8. only then the separate operator confirmation in Ops.

Production access, including read-only, needs permission for the specific
operation. Development does not grant it, and neither does this document.
