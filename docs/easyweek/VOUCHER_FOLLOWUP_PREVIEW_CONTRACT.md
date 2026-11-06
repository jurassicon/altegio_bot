# EasyWeek voucher follow-up PREVIEW — contract (PR-22.2)

This is the tracked contract for the follow-up **preview**: how a repeat message
for the recipients of an original production voucher mailing is composed,
checked and recorded.

It describes what is now true in the code. It is **not** a statement that a
follow-up may be sent. There is no send in this stage: no send button, no send
confirmation, no executor, no queue, no job, no spent attempt and no server path
that could become one. The preview is a proposal and an audit record, and
nothing else.

Read with `VOUCHER_FOLLOWUP_CONSENT_EVIDENCE.md` (PR-22.1, the evidence this
stands on), `VOUCHER_PRODUCTION_MAILING_RUNBOOK.md` (how the original mailing is
operated) and `VOUCHER_MAILING_BOOKING_REPORT.md` (what the local report does
and does not prove).

---

## 1. Where the audience comes from

One **original** production batch, chosen by the operator in the existing Ops
section. Nothing else, and nothing wider.

Checked on the server for every request, not by hiding a control:

- the batch exists and is an EasyWeek production batch;
- its request schema is one of the fixed owner-approved 10 € products
  (schemas 3 and 4). Schemas 1 and 2 are the historical 15 € contract, keep
  their own older rules, and are refused by name
  (`followup_batch_schema_unsupported`);
- its composition basis is one a production batch can carry:
  `earned_first_visit`, `operator_manual_selection` or `mixed`. §46.2 keeps the
  first version to the fixed €10 batches of schema 3/4 with proven own bindings
  and does **not** narrow the audience to one basis: a manually selected
  recipient of a production mailing received the same approved message from the
  same sender as an earned one, and what differs is only which link proves their
  identity. That is proven per slot — an earned row by the booking its voucher
  was earned on, a manual row by the operator-proven customer uuid on the
  campaign row — and a slot whose basis has moved since is
  `followup_identity_conflict`. A canary, a snapshot ledger, an owner test and
  an Altegio campaign are other kinds of mailing with their own tables and are
  refused by provider and schema (`followup_batch_scope_unsupported`,
  `followup_batch_schema_unsupported`);
- it carries a frozen composition digest;
- the branch's own WhatsApp sender is configured and active, and its Meta
  identifier digests to the route the batch's attempts froze
  (`followup_sender_unproven`). The repeat message stays on the original send
  geography — the Karlsruhe line and its confirmed sender;
- a §45.4 **terminal operator stop** on the original batch forbids the preview
  outright (`followup_batch_terminal_stop`), and the check is repeated inside
  the transaction that would store the snapshot.

No extra recipients are looked for outside the batch. The addressee is never
substituted by the current owner of a matching number: the person is resolved
through the EasyWeek card of the uuid the slot recorded, that card's number must
be the number on the campaign row, and that number must digest to the route the
original attempt froze. Any contradiction of batch, workspace, addressee, sender
or binding is an exclusion with `followup_identity_conflict`.

Booking checks span **all** configured branches of the same EasyWeek workspace,
because EasyWeek's customer booking list is workspace-wide. That does not widen
the send audience, which stays with the original batch and its branch.

## 2. The three groups, and the fourth answer

| Group | Code | What it means |
| --- | --- | --- |
| Прочитано | `read` | a read receipt arrived for this slot's own message |
| Доставлено, прочтение не подтверждено | `delivered_unread` | a delivery receipt arrived and no read receipt did |
| Подтверждённая ошибка | `confirmed_failure` | PR-22.1 holds a **confirmed** failure fact about this slot's one attempt |

Everything else is `followup_evidence_unproven`: accepted by Meta and then
silence, a timeout, `send_unknown`, a slot whose attempt was never completed, a
local parameter refusal that never left the process, a CREATE or PAY error, a
slot never attempted. **None of them is "did not read"**, and none is a
confirmed failure. A slot with no provable group is stored under the row group
code `unproven` with its reason, so a snapshot accounts for the whole original
mailing.

Precedence: **read > delivered > failure**, whatever the arrival order. A
`failed` callback after a delivery is a contradiction in the provider's own
account of one message; PR-22.1 refuses to bind such a fact, and the group
resolution refuses it a second time from the other side.

Nothing is selected by default. An empty selection is **not** "all three", and a
selection naming only groups that do not exist is empty and refused
(`followup_groups_required`). The stored selection is canonical and
de-duplicated, so one person is in exactly one group and the composition does
not depend on the order a form submitted.

Group membership is read only off facts tied to the slot by exact identity — its
own `provider_message_id`, its own single attempt row, and the sender and
recipient digests that attempt froze. Never a phone number, never a batch-wide
timestamp, never time proximity.

## 3. The failure-group policy — and why it currently admits nobody

`followup-failure-policy-v1` carries **no** admissible class, **no** admissible
Meta error code and **no** waiting time. Every confirmed failure is therefore
excluded with `followup_failure_policy_unproven`, and the Ops page reports that
as a missing prerequisite rather than as a coincidence. The operator may still
select the group: the result is an honest zero.

This is a decision, not an omission. §46.3 forbids inventing permitted error
codes or waiting times, and no proven policy exists in this repository. A later
version is a data change under review, and it cannot smuggle the refused cases
back in — the policy object validates itself at import against a floor no
version may lower:

- only a `confirmed` fact counts; `unmatched`, `conflicting` and `unproven`
  never do;
- only the `transient` class may ever be admissible, so `permanent` (a block, a
  policy restriction, a template defect) and `unknown` stay out by
  construction;
- an admissible code must be a specific integer from the transient set, named
  one by one. Meta's catch-all `0` is refused: it identifies nothing;
- a version that admits anything must state a positive waiting time, and the
  binding interval is the **longer** of that and the operator's own.

A row in the failure group with several confirmed facts is admissible only if
**every** one of them is: one inadmissible reason among several is a veto, never
an average.

## 4. The minimum interval

Given by the operator explicitly, in days on the wire and seconds in the stored
contract. There is no default — not 3 days, not 7, not "whatever last time
used". A missing, zero, negative or unparseable value forbids the preview
(`followup_interval_required`).

The anchor is the slot's own proven `send_attempted_at` and nothing else: not a
callback's arrival, not the row's `updated_at`, not the moment the page was
opened. For the failure group the same anchor applies, and it is explicitly
**not** evidence that anything was received — it is when this system last tried
that number. The boundary is inclusive: exactly the interval is enough.

## 5. Per-recipient eligibility

In order, cheap and local first, the one external step last:

1. **identity** — as in section 1 (`followup_identity_conflict`);
2. **group** — section 2 (`followup_source_attempt_unproven`,
   `followup_evidence_unproven`, `followup_sender_unproven`);
3. **interval** — section 4 (`followup_interval_not_elapsed`);
4. **failure policy**, for the third group only — section 3;
5. **entitlement** — the ORIGINAL artifact, read and proven rather than
   inferred from the presence of its binding columns. Locally: an explicitly
   refunded, cleaned or unsettled slot is excluded
   (`followup_entitlement_invalid`), and a slot whose MAC, key id or order uuid
   is missing, or whose MAC was written under a key this deployment no longer
   holds, is `followup_entitlement_unproven` — nothing is wrong with the
   voucher, the **proof** is gone. Then one exact read-only GET of that order
   (see section 5a). No new voucher, entitlement, CREATE, PAY, code replacement,
   validity extension, refund or promise of an unverified balance exists here;
6. **consent** — the PR-22.1 aggregate unchanged: the suppression ledger for
   this address and for every address a card of this uuid holds, a refusal bound
   to the uuid, `Client.wa_opted_out` on **any** matching card including another
   branch's or an Altegio one, and the historical recipient snapshot. A proven
   refusal excludes (`followup_consent_refused`); accepted but unprocessed
   inbound **holds** (`followup_consent_pending_inbound`) — a busy worker queue
   is not a reason to write to somebody who may have just said stop; a gap is
   `followup_consent_unknown`. An ordinary reply, a new booking and a delivery
   status are never opt-in, and the proven-START rules are untouched.

   And then §46.4.4's backlog, which none of those sources can see. A WhatsApp
   event stored before PR-22.1 existed has no receipt, no suppression row and no
   card flag — a «стоп» the old keyword set could not even recognise is
   invisible to the whole aggregate. One bounded, read-only pass per preview
   therefore scans the stored events that have **no receipts at all**, with the
   same extractor the live ingress uses, asked only about the numbers this
   batch's recipients are known by (the campaign row's and the uuid's card).
   A refusal it finds that nothing has accounted for is
   `followup_historical_stop_unaccounted`; a truncated pass is
   `followup_historical_consent_unproven` for **everybody**, because a partial
   answer about a backlog is not an answer. The scan writes nothing, runs no
   reconciliation and acknowledges nothing: §46.4.4 keeps the projection a
   separate explicit operation, and a Chatwoot mirror, an `ignored` route and a
   historical START are all excluded for the reasons that operation excludes
   them. An empty suppression table is therefore not read as consent.

   Two things decide whether a stored refusal is **accounted for**, and both
   were wrong in the first round:

   * **whose refusal it is.** §46.4.2–3 keep the veto a fact about the PERSON,
     so it is taken against the whole aggregate the live veto itself matched —
     the outbound number, and every number a card of this proven uuid holds,
     another branch's or an Altegio one. The first round asked only about the
     number this slot would write to, so a refusal sent from the person's other
     card was found by the scan and then ignored by the preview. The widening
     bans and nothing more: a matching number never proves a positive identity
     or a branch membership;
   * **whether it was already answered.** A refusal can have been accounted for
     and then LIFTED, by the one thing §46.4.7 allows: a proven, strictly newer
     explicit opt-in under the existing START contract. "The ledger does not
     refuse this number" is therefore not the same as "nothing accounted for
     this", and the first round re-imposed a withdrawn refusal on every preview.
     The question asked of each found STOP is now the writer's own —
     `whatsapp_consent.stop_superseded_by_opt_in`, the predicate
     `apply_consent_decision` applies when a STOP arrives against a row that is
     already opted in. One rule, one place, and no comparison of arrival times:
     a delivery can arrive days after the message it carries. An unproven order,
     an equal time, an unknown time on either side, a START that never became
     the decision in force, and a START on a DIFFERENT number all leave the
     refusal standing;
7. **replies** — a proven reply from the addressee after the original attempt
   excludes (`followup_customer_replied`). The reply need not mention the first
   message. The receipt's signature must have been verified by the ingress and
   its sender route must not be one this deployment refused; a Chatwoot mirror or
   operator relay is not an independent client reply (only the Meta ingress
   writes receipts);
8. **bookings** — one live, strictly paginated, read-only EasyWeek read. An
   active booking in the future excludes (`followup_future_booking_active`); any
   booking at all after the original attempt excludes, cancelled included
   (`followup_new_booking_after_attempt`); the past visit that earned the
   voucher excludes nothing by itself. An incomplete or untrusted read is
   `followup_history_unproven` — a timeout, a page that did not come back,
   pagination that does not add up, a card whose uuid resolves to somebody else.
   **An API failure is never an empty booking list.**

Inclusion needs proof of every condition. Exclusion needs proof of one.
Everything in between is `unknown`, which is a third answer and never a quiet
yes.

### 5a. The original artifact, read and proven

§46.6 requires the original artifact to be read and proven, with its HMAC, its
key id and its own schema's contract. The preview therefore performs one exact
read-only `GET` of the slot's `target_order_uuid` and reuses the §42 stage
preconditions rather than reimplementing them:

* the body must describe the order that was asked about — a 200 is not proof;
* the order's `comment` must equal the slot's reconciliation marker;
* `observe_artifact` against the **slot's own** schema contract: schema 3 is the
  paid €10 product and schema 4 the free gift certificate, and §46.6 forbids
  applying one's conditions to the other;
* an observed refund, or a refunded/cancelled order state, excludes; an unknown
  or malformed state is unproven;
* the documented validity signals of §45.4 — an explicit `is_expired`, an
  `expired` status, an `expires_at` or `valid_until` already past — exclude. The
  **absence** of individual dates blocks nothing: it is the ordinary shape of a
  correct voucher under this contract;
* **settlement and the amounts**, through the same `paid_order_amounts_proven`
  the primary delivery gate applies to this schema: a documented paid status AND
  every published total, at both the order and the invoice level, equal to what
  the ORIGINAL schema's contract names, with `amount_due` zero. An order that is
  merely `open` was never settled, and the first round let one through — a code
  in the response and a MAC that verifies prove a binding was made, not that the
  order behind it was paid. For a free schema-4 issue the zero totals are
  necessary and never sufficient, because an untouched draft publishes the same
  ones, so its documented zero-amount adjustments are required as well. A
  contradictory, missing or account-funded amount is refused rather than
  trusted. The row is `followup_entitlement_settlement_unproven`, which is
  `unknown`;
* the real MAC check. The code is read out of the order through the same proof
  the payment gate uses and compared with the slot's stored binding by
  `ledger.binding_matches`, which also pins the batch, the slot, the template
  uuid and the order. A rotated key, another order or another code all fail it,
  the MAC never leaves the ledger, and the code is never stored, returned or
  logged.

An unreadable answer, a timeout or a body about another order is
`followup_entitlement_unreadable` — never "probably still valid". The read takes
no database handle; the MAC check opens its own short session afterwards.

### Locks, the network, and the re-check

The external steps hold no database session, no row lock and no advisory lock —
by construction, because that phase has no database handle. After them, and
before the snapshot is stored, the mutable local facts are asked again:

* the original batch's **terminal stop**, the branch **sender**, and the local
  **template row**. Those are contract facts of the whole preview, so a change
  refuses the save outright — a composition must not be stored under a sender or
  a message contract that no longer holds;
* each included row's **source facts**: the attempt, the proven receipts and the
  confirmed failures are re-read and the group is resolved again. A `read`
  callback can land while the booking read is in flight. §46.5 allows a move
  between two groups the operator ALREADY selected when the approved message
  contract is the same, and the **actual** group is what gets stored; a move out
  of the selection is `followup_group_moved_out_of_selection`, and a selection
  whose groups do not share one contract refuses the save;
* each included row's **identity and address**, re-proven against the row's own
  basis;
* **consent**, compared against the fingerprint the first pass recorded, so a
  state that moved at all demotes the row even when the new answer still reads
  "allowed";
* **replies**.

A row that loses eligibility is excluded with a reason and its place is filled by
nobody: §46.5 forbids substituting another recipient, so the stored composition
may be smaller than the first pass expected.

The preview takes **no** consent address lock, which is the one place it differs
from a send: a claim must be serialised against concurrent writers, while a
preview must not delay an arriving STOP by so much as one transaction. The
PR-22.1 lock order is therefore untouched, and PR-22.3's locked guard remains the
thing that serialises an actual send.

### The two booking dates, kept apart

§46.5 names two exclusions that rest on two different facts, and the
implementation keeps them separate:

* **a booking created after the original attempt** — read off the provider's own
  `booking_created_at`, taken from the captured `booking-created` deliveries the
  §PR-17 booking report already validates. Cancelled ones count, and the
  appointment time is irrelevant: a booking made the day after the mailing to
  record a visit that already happened is exactly the engagement this rule is
  about;
* **an existing active booking ahead** — read off the appointment time in the
  live, fully paginated history, whenever it was made.

Reading the appointment time as the creation time is wrong in **both**
directions, and an earlier revision of this document wrongly claimed it could
only over-exclude. It cannot: a booking made in advance for a later date and
then cancelled was called new although nothing had happened since the mailing,
and a booking made after the mailing for a visit already past was left included.
Both are covered by regression tests.

The webhook's ARRIVAL time is never read as the creation time. A capture whose
`booking_created_at` cannot be parsed is rejected by the report's own validator
rather than falling back to `received_at`.

Absence of a captured creation is never absence of a booking. **Every** booking
the live read can see needs a proven creation before "nothing new since the
attempt" may be asserted, whenever its appointment is: an appointment in the
past does not establish an old booking, and a review-round-2 reproduction showed
a backdated booking with a missing creation webhook coming out `included`. Such a
row is `followup_booking_creation_unproven`; when the branch registry is not
configured, when the bounded capture scan truncated, or when two numeric ids
claim one person, the row is `followup_booking_capture_unproven`. Both are
`unknown`, never `included`.

How far back the capture reads
------------------------------
Further than the §PR-17 report itself, and that report's own contract is
unchanged. The report asks "was a booking created after the send?", so one day of
slack below the earliest send is all it can need, and it still scans exactly
that. The follow-up asks a second question about the same bookings — when was
this one made — whose answer may be months earlier, and with the report's floor
that evidence sat in the database unread: an ordinary cancelled booking made ten
days before the mailing turned the recipient `unknown`.

So the follow-up passes `lookback=None` to the same scan, which then reads the
most recent `MAX_SCANNED_EVENTS` lifecycle deliveries newest-first through the
primary key and applies them in their own order. Bounded in rows and in work,
with `truncated` reported rather than a short answer — never an unbounded full
scan, and never a silently widened report.

Which bookings belong to whom
-----------------------------
A person's creations are found two ways, and both are UUID-first:

* **by booking identity.** The live read is keyed on the proven customer uuid
  and returns that person's booking ids, so their creations are looked up
  without any numeric id. This is what makes a genuine manual recipient
  provable: `ensure_local_identity` creates their card with the proven uuid and
  `altegio_client_id = None`, which is the ordinary shape of that data rather
  than damage, and demanding a numeric bridge from it refused somebody for the
  way their card was made;
* **by numeric customer id**, when the frozen card carries one. This finds the
  creations the live set cannot show — a booking of theirs that has since been
  moved to somebody else and so left their list.

A reassignment never moves a creation. The creation stays with the customer its
own creation delivery named, because that is the historical fact and a later move
does not undo somebody's visit to the booking page; for the booking's new holder
that creation is not theirs and the row is
`followup_booking_creation_unproven` rather than an exclusion credited to them.

What is disputed, and whose problem it is
-----------------------------------------
A booking the capture cannot attribute has two different things wrong with it,
and they are kept apart: its OWNER may be in doubt, or only its TIME. A booking
whose creation deliveries agree about the creator and disagree about the instant
has a proven owner and an unproven date — so it is that person's gap and nobody
else's, and it has no business holding a stranger's empty history open. One such
booking used to hold every bridge-less recipient in the batch at once, which
confused "the capture has a gap" with "this recipient may have booked".

So a dispute outside somebody's list is out of the way as soon as its creator is
proven to be somebody else, in the branch that delivered the creation — with the
thinnest evidence there is, a single undated creation, and with or without a
later delivery handing the booking on. When the creator really cannot be
established and the booking could be theirs, the row stays `unknown`, and an
active booking ahead still excludes on its own.

Who HOLDS a booking is not who made it, and only a creation delivery answers
the second question. An update or a cancellation naming somebody else is an
event in the life of a booking this person may well have made and handed on, so
it cannot close a question about the creator: a disputed booking with no
creation observed at all stays open whatever its later deliveries named, and
reading one of them as "not theirs" shut the only question that was open. What
closes it is what always did — a creator proven to be somebody else, or a
booking this deployment had provably already heard of before the attempt.

Two creation deliveries naming two branches are a contradiction about the
booking and not automatically one about its owner. The owner is resolved from
the pairs the deliveries really named — never from a product of the branches
and the numbers, which would ask cards about customer records nothing observed
— and when every one of those pairs reads back to the same person, or to
different strangers, the booking is somebody else's and an unrelated
recipient's complete empty history stays provable. Anything less is unknown,
deliberately: a pair that bridges to nobody leaves it open, and a pair that
reads back to the recipient themselves is precisely the case where the dispute
may be about their own booking. And the same booking INSIDE their list is their
problem whoever made it.

Without the numeric bridge the live set is the whole account of this person's
bookings, and the one thing it cannot show is a booking that LEFT it. So a
reassignment observed after this slot's own send anchor, of a booking that is not
in their list, leaves the slot `unknown` — **unless that booking's own creator is
proven**. The creation delivery names a numeric customer id; when that id
resolves, through a card of this workspace, to a single proven uuid that is
somebody else's, the reassignment is an event in that person's history and the
recipient being judged stays provable. An absent or ambiguous link proves
nothing and the slot stays `unknown`. A window with no reassignments hides
nothing, and the recipient is provable.

What a delivery really named: the pair
--------------------------------------
Every statement the capture makes about a customer is a `(branch, number)`
pair, because that pair is the smallest thing that names a customer RECORD. The
capture used to keep the bare numbers, so a booking created in one configured
branch under a number and then named by a lifecycle delivery of another branch
under the same digits looked like one customer twice — no reassignment at all,
for a booking that had in fact moved between two different records. For a
recipient whose only account of themselves is their current list, that was the
one thing the list cannot show, reported as nothing to see.

A delivery that named NO customer forms no pair, and that is not the same as a
delivery that changed nothing. Its branch is still proven, so what it records
is that something happened to this booking in that branch — the booking's
custody is unproven, which is weaker than a reassignment and is kept apart from
it: nobody is proven to have taken the booking over and nothing proves it
stayed either. The missing field used to make the whole delivery disappear, and
a booking that may have left a manual recipient's list then read as a booking
that never moved; the very same delivery WITH a number had always read as a
move, so a missing field decided the answer.

So the pairs are what the follow-up reads, in both directions: a booking has
moved when any delivery of it names a pair other than the one its creation
named, and a dispute's owner is resolved from the pairs its creation deliveries
really named. A branch of its own proves no change of person — two pairs bound
by cards of this workspace to ONE proven uuid are one person with a record in
each branch, and a booking moving between them is that person's business — and
matching digits prove no sameness either.

Whose numeric id it is — the branch is half of the answer
---------------------------------------------------------
A numeric customer id is unique **per branch**, not per workspace. The same
digits name one person in Karlsruhe and somebody entirely different in
Ettlingen, so a link is only a link when the branch of the CARD is the branch of
the creation. Both directions of the UUID ↔ numeric-id bridge are therefore
asked inside the creation's own branch:

* the reverse one — a card of that branch carrying both the number and a proven
  uuid names the person the delivery was about;
* the forward one — this recipient's own number, when their card proves it in
  that same branch, is compared with the number the delivery named. Which
  branches those are is recorded beside the number the identity resolver
  returns.

Matching digits in another branch establish nothing. A card there used to settle
the creator, which turned an unanswerable reassignment into "nothing has
happened since the mailing" for a recipient the capture knows nothing about; and
a booking created in a branch the recipient has no card in used to be credited
to them because the numbers happened to agree.

Within that scope the result is one of three things, and they are not
interchangeable:

* **one proven uuid**, on a card of the creation's own branch. Positive, and it
  decides in either direction: the creation is this person's, or demonstrably
  somebody else's;
* **conflicting** — two cards of that one branch bind the same number to two
  different uuids. Defence in depth, since the schema makes a number unique
  inside a branch: if a historical row produces it, the link is not positive;
* **absent** — nothing is established either way, and the older rule stands for
  a card with no numeric bridge of its own. For a recipient whose number IS
  proven, a creation from a branch their card does not cover is simply
  unresolved; being permissive there is what credited them with a stranger's
  booking.

A numeric bridge is therefore not a complete view of a person, and the
question "could a booking have left this list unseen?" is asked of every slot
rather than only of the bridge-less ones. A bridge proven in one branch reads
that person's bookings there and nothing at all in the others: a booking
created in another configured branch and handed on inside the window used to
pass unnoticed — with the same digits, which are a different customer record
there, and with different ones, which say nothing about who that person is
there. It is out of the way on the same single condition as before: a creator
proven to be somebody else, in the branch that delivered the creation. Their
own booking, proven under the number their own card covers in the branch that
card is in, still excludes them.

The identity check reads the other direction per branch for the same reason. A
different customer's card in another configured branch carrying this
recipient's digits against a different proven uuid is not a competing claim
about anybody: it used to read as «two people claim this number» and refuse the
whole booking question as an identity conflict, with a completely empty
history. The refusals that are real are untouched — the frozen card now naming
a different person, and two different numbers claiming one proven uuid, which
is what the schema permits and the genuine ambiguity about which number the
capture's deliveries are about. The §PR-17 report passes no branch scope and
keeps the workspace-wide reading its own question needs.

A card in a branch that is **not** configured takes no part in any of this. It
is another workspace's row, or one from the Altegio era, and it may neither
supply a bridge nor contradict one: it used to do both — handing a manual
recipient a numeric bridge proven where no creation of this mailing can come
from, and turning a stranger's matching number into an identity conflict that
refused the whole booking question. The §PR-17 report keeps its own
workspace-wide identity scope, because its question — which cards speak for one
recipient — is not branch-limited.

An exact creation date, and a proof of existence
------------------------------------------------
Two different facts, and only the first can exclude anybody:

* **the creation instant**, as the provider stated it in `booking_created_at`.
  This is what "созданная после исходной попытки" is read from, and nothing
  else ever stands in for it;
* **a proof that the booking existed already** when the attempt was made. A
  delivery ABOUT a booking proves the booking existed when that delivery was
  made. That is a lower bound on its existence and never a creation date, and it
  answers exactly one question: no, this cannot be a booking made since the
  mailing.

The second is why a creation delivery that proves the branch, the booking
identity and the owner but states no usable instant no longer holds its own
recipient for ever. Their cancelled booking sits in their complete current
history, the capture cannot date it, and the delivery that named it was stored
weeks before the send — so nothing new is hiding there, and no date is invented
to say so. `received_at` is still never written as a creation date, and such a
booking still excludes nobody: only `booking_created_at` can do that.

The proof settles an INCOMPLETE record and never a self-contradictory one. A
booking whose own creation deliveries name two owners, two branches, or two
instants asserts two things that cannot both hold, and an earlier sighting of it
does not decide which to believe. A cancellation stored before the mailing
therefore does not clear a creator conflict: the row stays
`followup_booking_capture_unproven`. Controls: a first sighting after the
attempt, an old appointment with no early delivery at all, and an active booking
ahead — which excludes for its own reason, as always.

Nor does the sighting say anything about WHOSE booking it is, so it is not
applied before that is checked. For a booking in the person's own current
history the bar is the same one the dated case has to clear: the creation names
them, in the branch that delivered it, and the booking was provably already
there. Losing the creation instant must not make a row admissible that reads
unknown with it — an undated creation naming somebody else, visible in this
person's history and heard of weeks earlier, used to come out `included` while
the identical row with a usable date came out unknown. It must not make one
unknown that reads fine with it either: where the capture establishes nothing
about the owner, a bridge-less card follows the same older rule in both cases.

A chronology of its own
-----------------------
The sighting is also read back against the creation. A booking whose creation
claims an instant LATER than the first delivery this deployment stored about it
is making two claims that cannot both be true — and the half this phase acted
on was the harsher one: the person was excluded for a new booking the capture's
own earlier row says was already there. Such a booking is not a proven creation
and the row is `followup_booking_capture_unproven`.

The attempt's own date does not decide this. An earlier revision compared the
two instants against the send anchor, so a cancellation stored the day AFTER
the mailing bounded nothing and the same contradiction excluded somebody; the
comparison is now between the two instants the capture holds about that
booking, and the attempt is where the exclusion question is asked, not where
this one is. The same rule at the other end catches a creation dated after the
moment the preview is computed at, which nothing can have been created after.

Nothing is invented by it: an ordinary late delivery is still allowed — this
provider may deliver a creation after the cancellation it precedes, carrying a
truthful earlier instant, which contradicts nothing and has its own regression
— a creation delivered at the instant it states is the ordinary prompt webhook
and still excludes, `received_at` is never substituted for a creation date, and
a second booking whose own facts agree, provably created after the attempt,
still excludes on its own.

The first sighting is taken from every delivery whose branch and booking
identity are proven, including a `booking-created` that named no customer. Such
a delivery states that this booking exists and nothing about whose it is: it
credits nobody, is no conversion, and is not a dispute of its own — a partial
row may not hold a whole batch open — and it used to be dropped whole, so a
later complete creation claiming an instant days afterwards looked like a new
booking. A delivery from an unconfigured branch, one whose booking identity is
unreadable, and one whose payload proves nothing at all date nothing.

An instant that cannot be expressed at all
------------------------------------------
`booking_created_at` may be syntactically valid ISO-8601 and still have no UTC
representation: midnight on the first day of year one with a positive offset,
the last second of year 9999 with a negative one. Converting either one
overflows. That overflow used to leave the capture, cross the whole preview and
reach the operator as HTTP 500 for the entire batch — no rows, no reasons,
nothing to act on, and nothing saying which delivery caused it.

It is an unusable creation time, exactly like an unreadable string: the
delivery's branch, booking identity and customer stay proven, the rejection is
counted as `booking_created_at_unusable`, no arrival time is substituted for the
missing instant, and only the row whose own booking it is becomes `unknown`.
Every other row of the batch gets its ordinary answer. The exception is handled
where the instant is read, and not suppressed at the level of the Ops request.

A repeat of a creation is not a disagreement
--------------------------------------------
Three shapes are distinguished for one booking uuid:

* **a repeat** — the same owner again, including a second delivery that merely
  adds the instant an earlier one omitted. Ordinary, and the booking stays
  usable evidence;
* **a reassignment** — a LIFECYCLE delivery naming a customer other than the one
  the creation named. Reported, and it never moves the creation;
* **a creators' conflict** — two CREATION deliveries naming different customers,
  different branches or different instants. Each disagreement is read from the
  SET of what the creation deliveries named and is counted once per booking, so
  a delivery that could not state one field still contradicts a second delivery
  over the fields it did state: a creation naming no customer states where and,
  when its payload carried a usable one, when. The instant used to be compared
  only against whichever delivery was COMPLETE, so an instant stated beside a
  missing customer disagreed with nothing and the complete delivery decided
  alone — `included` for an instant before the mailing, `excluded` for one after
  it, on a record that contradicts itself. Such a delivery still names nobody
  and is still no conversion; alone, with nothing else ever delivered about that
  booking, it holds nobody open.

  WHO created a booking is a fact a creation delivery states even when it could
  not state WHEN, so a missing instant on one of the two is a second gap and not
  a resolution of the first. The undated delivery used to leave the aggregation
  before any comparison was made, and the booking came out cleanly attributed to
  whichever creation happened to carry a date — in either delivery order, and
  with an empty current history reading as "nothing has happened since the
  mailing". Both orders are covered by regression tests.

One pass, in statements that fit
--------------------------------
The reverse owner lookup asks about every `(branch, number)` pair the capture
named, and one branch with a long history names tens of thousands of them. A
single statement carrying one bound parameter per number crosses the PostgreSQL
wire protocol's own 16-bit limit at 32 766 ids, and the driver's refusal
reached the operator as «the database is unavailable» — an infrastructure
answer to an ordinary amount of history. The numbers are now asked for in
order, in slices that fit, and the answer is the one the single statement gave
while it fitted: every number is asked about, including the last slice, and no
error is turned into an empty selection. No protective bound is lowered —
`MAX_SCANNED_EVENTS` and the truncation report are unchanged — and nothing is
read that was not read before.

A scan that already truncated asks this lookup nothing at all. It has no answer
about anybody by definition, so the owner question has nothing left to inform;
with a capture large enough to truncate it was also the query that failed
first, which turned the planned incomplete result — every slot
`followup_booking_capture_unproven` — into that same 503.

### Proven `delivered` / `read`

§46.3 asks a status for a verified inbound context with an exact sender and
message id before it may become an audience. The slot's `delivered_at` and
`read_at` are the **mailing's own record** — applied on an exact message-id
match, deliberately outside the §42 fence, because dropping a status about a
message already sent would corrupt the history of a mailing that really
happened — and they are not that proof: the transition records neither the
signature nor the route, and the ingress strips Meta's signature header out of
the stored copy, so no later reader can recover it.

So the proof is written beside the transition, in
`easyweek_voucher_production_delivery_receipts`. A row is written only for a
callback whose delivery signature the ingress verified, naming the slot's exact
message id, on a route equal to the one that slot's single attempt froze; the
table's own CHECK constraints require all three, so a stored row **is** the
proof. The slot's columns, counters and statistics are untouched, and the
transition still applies when no proof can be recorded.

A slot with a status and no proof is `followup_status_evidence_unproven` —
including every historical slot, which is the honest answer. It is never demoted
to the weaker group instead: that would state something weaker than the
mailing's record and stronger than what is proven.

#### An early callback, kept and bound

Meta can deliver a `read` for a message id while the transaction that records
that id is still in flight: the slot is `send_claimed`, the POST has returned,
and the acceptance has not committed. "No slot owns this id" is then an ordinary
momentary state rather than evidence that the callback is foreign, and a repeat
callback is not guaranteed — a review-round-2 reproduction through the real
worker left the slot at `provider_accepted` with an empty `read_at`, no receipt
and therefore an `unknown` audience for somebody who had demonstrably read the
message.

So a signed positive callback that nothing owns yet is stored `unmatched`: it
names no slot, carries the route it observed with nothing to compare it against,
and is kept only while some slot of this phase could still claim a NEW message
id — a decision about whether to KEEP the callback, never about which slot it
belongs to. `record_item_outcome` then binds it on the exact identifier, inside
the transaction that records that identifier and under the same locks, re-judges
it from scratch against the frozen route, and advances the slot through the same
transition rule the live callback path uses. A foreign or unverifiable route
makes the row `conflicting`, kept for audit and evidence for nobody.

An UNSIGNED callback is not kept at all. Authenticity cannot be established after
the fact, so there is nothing a later binding could promote.

The message-identity lock is taken FIRST on both sides, in the one global order
(message → header → item) PR-22.1 proved, so the gap between "nothing owns this
id" and keeping the callback is unreachable rather than merely narrow. Read still
outranks delivered, a late `failed` still cannot undo a success, and the
contradiction is still kept for audit.

#### A failure read first, withdrawn by the success

The ordering above decides a pair bound in one transaction. A pair the process
read in two could still disagree with it: a `failed` with nothing yet
contradicting it becomes `confirmed` and stamps `confirmed_delivery_failure` on
the slot, and the success that followed advanced the slot and left that stamp
standing — a delivery failure asserted about a message the customer had
demonstrably read.

A PROVEN success now withdraws it, by one rule in one place invoked wherever a
success is proven: the live callback, and the binding of an early one. The
failure row is re-stated as a conflict and keeps its reason code, its class, its
route and its times; the slot records that a confirmation was made and then
contradicted rather than losing both halves; and the withdrawal runs in the same
transaction and under the same locks as the success that caused it. So both
arrival orders end in the same ledger state.

The decision is precedence, never chronology: no event time is required, and the
row ids of two different tables are not compared as clocks. Only a proven
success does it — an unsigned callback, a foreign sender and a foreign recipient
each leave the status without the proof the receipt table requires, and none of
them may clear a confirmation that was proven. A lone failure keeps its
confirmation. The failed group's admissibility policy and the mailing's attempt
count are untouched.

#### The slot's rank and one callback's proof

Two questions, asked separately. The slot's status is the record of a mailing
that really happened and only ever moves forward; the receipt is a fact about
ONE callback — that its signature was verified and that it named the route this
slot's single attempt froze.

Asking for the proof only when the status moved tied them together. A signed
`delivered`, on the frozen route, arriving after an unproven `read` had already
walked the slot to the higher rank was refused by the monotonicity rule and
discarded WITH its proof: §46.3's precedence never ran, and the slot went on
asserting a delivery failure about a message Meta had signed for. The proof is
now recorded for every callback whose exact message id belongs to the slot,
whether or not the rank moved.

Nothing else changes. The rank still never walks backwards, so the slot stays at
`read`. An unproven callback still proves nothing — a `read` that cannot be
verified does not become a proven one by being recorded, because the receipt is
refused on its own terms. A message id no slot of this phase owns is still
nobody's business and falls through untouched. Two proven statuses of one
message leave one receipt each, and a repeat of either adds none.

## 6. The message

The follow-up has its **own** contract. The original §45 message is not edited,
not extended and not reused.

- code `new_client_voucher_followup_v1`, Meta name
  `kitilash_ka_new_client_voucher_followup_v1` (per-branch by construction),
  language `de`, category `MARKETING`, positional parameters
  `(client_name, booking_link)`, one BODY component and nothing else;
- the local NAMED body is the contract and is rendered into Meta's `{{n}}` form,
  never parsed back out of it;
- the stored snapshot keeps the template identity, language, category, parameter
  order and a digest of the exact body;
- it is **proven** only when the local row matches the contract exactly **and** a
  live Meta read shows `APPROVED` in that same evaluation. A previous proof is
  not evidence: a template can be paused between two page loads;
- nothing in this stage creates or modifies a Meta template, and free WhatsApp
  text is never substituted for an approved one. An unproven contract blocks the
  whole preview with `followup_template_unproven`.

**One neutral text for all three groups**, declared explicitly for each of them
with no implicit fallback. The constraint that shapes it is the failure group:
those people may never have received anything, so the text may not say "as we
wrote to you", "you received" or "you have not opened it". It states a fact about
our own records — a voucher is on file for this person — and invites a booking.

The repeat message carries **no voucher code**, because this application does
not have one: §45 stores a keyed MAC and never the code. A follow-up that printed
a code would have to mint or look up a new one, which §46.4 forbids.

The body states how to unsubscribe (`STOP` / `СТОП`). The preview shows the text
with square-bracket placeholders only: no real name, link, number, code, body,
provider identifier, uuid or secret appears in the interface, the logs or the
error paths.

### Current prerequisite

Neither the Meta template nor its local row exists yet. On a correctly
configured deployment the preview therefore answers
`followup_template_unproven`, and the Ops page says so. The owner must create
the template and get it approved in Meta's own interface, and the local row must
be added; this application will not do either.

## 7. The stored snapshot

One immutable version per recalculation, in two new tables
(`easyweek_voucher_followup_previews` and
`easyweek_voucher_followup_preview_rows`). Nothing in the original batch, its
slots, its attempt counters or its HMAC bytes is read for writing or rewritten.

Each version records: the original batch and its frozen digest, the campaign run,
company and location, the stop generation, the selected groups, the minimum
interval, the exact template contract and whether Meta verified it in that
evaluation, the contract version, the failure-policy version, the check time and
the validity window, the included / excluded / unknown counts, a deterministic
composition digest, and the operator principal. Each row records its source
batch, item, slot, campaign recipient, canonical customer uuid, an entitlement
handle, the frozen recipient digest, the original attempt time, its group, its
decision and a safe reason code.

- **Server data is the source of truth.** A caller may choose a batch, a set of
  groups and an interval. It cannot pass an allowed list, substitute bindings,
  name a digest or ask for a count; the request model refuses such a field by
  name rather than ignoring it. A replayed digest therefore cannot describe a
  different composition.
- **Re-viewing a version changes nothing.** An explicit recalculation inserts
  the next version beside it; the earlier one keeps its groups, interval,
  template contract, policy version, counts and digest. Nothing marks an old
  version as superseded, because the current one is simply the highest.
- Changing the groups, the interval, the template or the policy version changes
  the digest, and therefore requires a new snapshot.
- Validity is 24 hours, and age is only one of three things that decide whether
  a saved result may be presented as current. The server also compares the
  stored contract with today's — the template code, Meta name, language,
  category, parameter order and **body digest**, the failure-policy version, the
  preview contract version, the source batch's frozen digest and its stop
  generation — and asks whether anything blocks a follow-up right now (a
  terminal stop, a missing sender, a template that is no longer proven). Only
  the conjunction reads as "nothing forbids a follow-up"; everything else is
  shown as a historical result that needs recomputing, with the reason named. So
  a paused template, a stop since, an expired window or a refused refresh cannot
  leave a stale block looking fresh.
- **And that conjunction is all the page claims.** A GET re-proves those three
  things and nothing else: it reads nobody's consent, replies, artifact or
  bookings, because that is an external recalculation and §46.6 makes it an
  explicit operator action rather than a side effect of opening a page. So the
  green state names the three things it re-checked and says, in the same breath,
  that the composition was not recomputed; every stored figure is printed with
  the moment it was computed; and nothing says the recipients or their number
  are confirmed now. Claiming otherwise was wrong in the most expensive
  direction available here — an operator reading «эти проверки актуальны»
  beside a count of one, for somebody who had since sent a STOP.
- **And the page it is already on stays honest too.** A standing the server
  rendered describes the moment the page loaded. When the operator then presses
  «Проверить состав» and no new result comes back — a 409, a transport failure,
  or an answer that cannot be read — that click has just proved the opposite, so
  the displayed block drops to the historical state immediately and without a
  reload: the success colour goes, the standing sentence says the checks are not
  confirmed now, and the claim that the counts are confirmed is replaced. The
  second round changed only one line of status text underneath, leaving the
  green block asserting that its counts were confirmed now. The
  snapshot stays fully readable as the history it is, and nothing about it is
  rewritten: this is display state, carried on the block as
  `data-followup-standing`.
- The stored snapshot keeps the body's **digest** and not the body. When the
  digest no longer matches, the page says the old text cannot be shown and that
  the current text belongs to a different composition; it never substitutes
  today's text for the one a stored composition was computed with.
- **A snapshot is not a send permission**: it is evidence of what was true at its
  check time.
- A preview neither spends nor reserves a send attempt, and creating one twice
  must not block anybody permanently — the dedupe contract is scoped to **one**
  version. Within a version the database itself enforces at most one included
  row per source slot, per canonical customer uuid and per original entitlement
  (two partial unique indexes). That is the source/dedupe contract PR-22.3 will
  claim against; the global one-shot execution, the claims and the terminal
  tombstones belong to that stage and are deliberately absent here.
- Nothing fabricates a successful check status of a previous follow-up attempt.
  The preview model and the future execution ledger are separate.

### The migration's receipt table

`b7d41f9c0a52`, one Alembic head over `a1c4e7b25d80`. Additive and narrow:
three new tables — the two preview tables and
`easyweek_voucher_production_delivery_receipts` — with no column of an existing
table altered and no consent row, delivery fact, attempt counter, HMAC byte or
financial value touched. Its `downgrade()` locks all three, counts the stored
previews and the stored receipts, and **refuses before any DDL** if either
exists: a receipt is evidence about a message that really was delivered and its
proof cannot be recovered once dropped, which is §46.9's rule. Upgrade from
scratch, `downgrade -1` and re-upgrade were verified on a disposable
PostgreSQL 16.

The receipt table carries a `match_state` of `confirmed`, `unmatched` or
`conflicting`, and its CHECK constraints are what make a stored row either proof
or honestly not: `confirmed` requires the slot, the batch, the slot number and a
proven route; `unmatched` requires all four to be absent; `conflicting` names the
slot it was tested against. The verified signature is required of **every** row,
whatever its state, because authenticity is the one part that cannot be
established after the fact. Only `confirmed` rows are read as a slot's evidence.

The preview header deliberately carries **no** `provider` column, for the same
reason `easyweek_voucher_production_stop_requests` has none: the row names a
batch, and that batch already says which provider it belongs to. The §PR-3
boundary test enforces that.

## 8. The operator's path

Original mailing → «Повторное сообщение: предпросмотр» → groups → minimum
interval → the proven text → «Проверить состав».

The page shows: which original mailing this is, its state, size, product schema
and composition basis; the exact text with placeholders; the selected groups and
interval; the number of eligible recipients; the exclusions and the unknowns with
reasons in words, with opt-outs, replies and bookings as separate reasons; the
snapshot's freshness; and the prerequisites that are still missing.

It says, in its own words: that only a preview is available in this version;
that the absence of a read receipt does not prove nobody read the first message;
that an empty audience is a correct result and not grounds to weaken anything;
that "unknown" is not permission; and that the absence of suppression rows is not
proof of consent.

The operator never types SQL, a uuid, a batch id, a digest or an environment
setting. The page works at desktop and phone width.

Authorisation is the section's existing one, reused and not rebuilt: the router
carries the Ops session dependency, and the single POST requires the session
cookie, this session's CSRF token in a header, and a same-origin request. The
GET creates no operation and changes nothing. There are exactly two routes:

```
GET  /ops/voucher-mailings/{batch_id}/followup
POST /ops/voucher-mailings/{batch_id}/followup/preview
```

Neither is a send, and no other follow-up route exists. A direct API request, an
old approval or an action substitution cannot create a send through the generic
dispatch, the Outbox or the old follow-up worker: those paths are untouched, and
there is no follow-up code that calls them.

## 9. What is deliberately NOT implemented (PR-22.3 and later)

- sending a follow-up, confirming a send, claiming one, or any executor,
  scheduler or transport retry for it;
- follow-up `MessageJob` / Outbox rows, follow-up callbacks, follow-up send
  statistics;
- CREATE / PAY / DELIVER / REFUND for a follow-up, or any new financial schema;
- multiple waves, retry after an unknown result, a new redemption engine;
- changes to the Altegio campaign paths, or production reconciliation / replay /
  recovery of stuck events;
- follow-up attribution in the booking report.

The existing send fence stays closed. The primary DELIVER authorisation grants no
follow-up authority, and the previous `send_attempt_count <= 1` and primary STOP
rules are unchanged.

## 10. Verification

New suites, all required (no marker, skip or xfail was added):

```
src/altegio_bot/tests/test_easyweek_voucher_followup_preview.py
src/altegio_bot/tests/test_easyweek_voucher_followup_ops.py
```

The §46.3 proof behind a positive status is exercised where the rest of the
production mailing's delivery evidence is, through the real worker and the real
acceptance write:

```
src/altegio_bot/tests/test_easyweek_voucher_delivery_failures.py
```

plus browser acceptance tests in
`src/altegio_bot/tests/test_easyweek_voucher_mailing_browser.py`, which drive
the operator path in Chromium: the link from the mailing page, the empty
selection being refused, the groups ticked, the interval typed, the button
clicked, the composition read off the repainted page — and then the server
asserted to hold one stored version, no job, no Outbox row and no second
attempt.

Every reproduction blocks unexpected HTTP with `respx(assert_all_mocked=True)`,
so a request that escaped to Meta or EasyWeek fails the test rather than
depending on credentials being absent. Races run on PostgreSQL 16.

Two findings worth recording, because they shape what the tests assert: §45's own
CHECK constraints make several invalid-entitlement states unreachable for a slot
that was sent at all (`refunded` requires no acceptance, no delivery and no
read; a paid slot always has an order uuid, a MAC and a key id), and §45 already
forbids one batch holding the same customer twice. Those branches are therefore
asserted against the rules themselves and against the database's own refusal,
and they remain in the code as defence in depth for historical rows and for the
later stage that will compose from more than one source.

The first review round's seven findings each have their own reproductions, all
of which failed before the fix: a stored «СТОП» with no receipt, flag or
suppression row; a booking made before the attempt for a later date and then
cancelled, and one made after the attempt for a visit already past; a callback
on this slot's message id with an unverified signature or a foreign
sender/recipient; an order that was refunded, expired, rewritten or whose code
does not match the stored MAC; a `read` callback arriving while the booking read
is in flight with only `delivered_unread` selected; a manual and a mixed
schema-3/4 production batch; and a paused template, a changed body digest, a
stop since and an expired window each leaving a saved result shown as
historical rather than current.

The second round's six findings have theirs too, and all six failed before the
fix:

1. a stored refusal sent from the person's OTHER card, at another branch, with
   the mailing having gone to the first number; and a withdrawn refusal — a
   proven newer START on the same number, both events before the mailing — being
   re-imposed on every preview. Plus the cases that must NOT lift it: a STOP
   newer than the opt-in, one at the same instant, one whose own time the
   payload never stated, an unsigned START, and a START on another number of the
   same person;
2. a booking appointed before the attempt with no creation webhook coming out
   `included`; a booking created after the attempt and then reassigned away,
   leaving its creator's live history empty; and an ordinary cancelled booking
   made ten days before the mailing whose creation sat outside the report's own
   window. Plus the other direction: a booking moved IN is not the new holder's
   creation;
3. through the real worker and `record_item_outcome`: a signed `read` arriving
   while the slot was `send_claimed` with no message id, the acceptance then
   storing the POST result, and the slot left at `provider_accepted` with an
   empty `read_at` and no receipt. Plus `delivered`, read-before-delivered,
   duplicates, a restart between keeping and binding, an unsigned callback, a
   foreign sender, a foreign recipient, a foreign message id, a late `failed`
   after a bound `read`, and the two halves run concurrently;
4. an `open`, `is_paid: false` order of schema 3 and of schema 4 being included;
   a paid order whose totals contradict its schema's contract; one publishing no
   total at all; a free schema-4 issue funded from an account balance; and a
   zero order that is merely open;
5. a manual card in its production shape — proven uuid, `altegio_client_id`
   `None` — being refused for the way it was created, with a complete empty live
   history. Plus the cases that must stay strict: a real new booking still
   excludes, and a reassignment in the window still leaves it `unknown`;
6. in Chromium, on an already-open page and with no reload: the saved result
   staying green and claiming to be confirmed now after a 409 and after a
   network failure, checked at desktop and phone width, with a successful
   recalculation still producing a new version and no PII anywhere on the block.

The third round's five findings, same rule, all failing before the fix:

1. the mailing went to the person's current number while a raw «стоп» with no
   stated time sat on a number no card carries any more, reachable only through
   the uuid the ban was bound to. The veto would have asked about it; the scan
   was handed a narrower set and never saw it. Positive control: the same shape
   with the order provable and a newer proven START on that number, which lifts
   it. Plus the bound: a truncated pass is still unknown for everybody;
2. a historical campaign refusal on A with a proven opt-in on B of the same
   person, through `consent_decision` and through a real preview. Controls: the
   opt-in on A's own number does answer A's snapshot, an older one does not, and
   a tie does not. Plus sixty answered snapshots on B and one refusing snapshot
   on A, which the earlier time-ordered row limit hid completely;
3. a booking in this person's current history whose single creation delivery
   named another customer, before and after the attempt; two creations of one
   booking naming different owners with an empty live history; and lifecycle
   deliveries from two customers with no creation at all. Controls: a stranger's
   ordinary booking changes nothing, the proven creator is still excluded, an
   active booking ahead still excludes whatever the creation says, and an
   ordinary manual card with a uuid and no numeric id is unaffected;
4. a signed `delivered`/`failed` and a signed `read`/`failed` pair for one exact
   message id and route, in both arrival orders, both entirely before the
   acceptance write and entirely after it. All four leave the success standing,
   the failure a conflict and no `confirmed_delivery_failure` on the slot. Plus
   a lone failure, a repeated pair, a foreign route and an unsigned pair;
5. through the real Ops route and in Chromium: a composition saved with one
   eligible recipient, that person's STOP recorded, the page reopened — the block
   is still green, because the batch, the window and the template are all
   unchanged, and it no longer claims the count is confirmed now. The stored
   figures carry their check time, and the recalculation afterwards stores zero.

The fourth round's six findings, all failing before the fix, and the first two
of them as deterministic races driven on independent PostgreSQL connections:

1. a card of this uuid appearing with an unprojected «стоп» after the addresses
   were chosen; and a proven START applied between the scan and the local
   decision while an undated «стоп» remained on that number. Positive control: a
   strictly newer proven START on the same number, resolved before the preview
   runs, which does open the row. Plus the coverage invariant, exercised by
   handing the scan a narrower question than the veto;
2. a creation delivery with a proven branch, booking and customer and no usable
   instant, a later delivery of the same booking naming somebody else, and an
   empty current history. Plus the three deliveries that prove none of it — a
   foreign branch, an unreadable booking id, an arbitrary payload — which extend
   nothing;
3. a manual card with a proven uuid and no numeric id holding a booking whose
   creation is positively linked to another customer, dated before and after the
   attempt. Control: the ordinary manual card with a complete empty history;
4. a cancellation of somebody else's booking stored weeks before the mailing,
   which blocked a manual recipient as a possible new booking and provably
   cannot be one. Control: the same shape first seen after the attempt, which
   still reads unknown;
5. a booking provably created by this person after the attempt beside a separate
   unattributable one, which used to read unknown instead of excluded. Control:
   the dispute alone is still no reason to exclude;
6. a `failed` processed first and a proven `read`/`delivered` after it, for one
   exact message id and route, both after the acceptance and with the failure
   overtaking it. Plus a repeated success, a success with no stated time, a lone
   failure, and the three unproven successes that may not clear a confirmation.

The fifth round's five findings, all failing before the fix:

1. two creation deliveries of one booking naming different owners with one of
   them undated, in BOTH delivery orders, and an empty current history: the
   booking came out cleanly attributed to whoever's creation carried the date,
   and the recipient `included`. Plus a separate one: a cancellation of that
   booking stored before the mailing, with the creations claiming a later
   instant and two owners, where the early sighting must not hide the
   contradiction. Control: a repeat with the same owner that merely adds the
   instant stays usable evidence;
2. one numeric customer id claimed by two cards of this workspace against two
   different proven uuids, for a manual recipient holding that booking in their
   current history — `included` for a creation before the attempt and `excluded`
   for one after it, both invented from an ambiguous link read as an absent one.
   Controls: an unambiguously foreign creator, an ordinary manual card with a
   complete empty history, and a card in a branch nobody configured, which
   competes with nothing;
3. a booking somebody else provably created and then handed on, outside this
   person's list and inside the window, which left every bridge-less recipient
   `unknown`. Controls: the same handover with no card bridging its creator
   stays `unknown`, and this person's own active booking ahead still excludes
   them;
4. their OWN cancelled booking, still in their complete current history, whose
   creation delivery proves branch, booking and owner and states no usable
   instant, stored weeks before the mailing. Controls: a first sighting after
   the attempt, an old appointment with no early delivery at all, a creators'
   conflict, and an active booking ahead;
5. through the real worker: a confirmed `failed`, then an unproven `read` that
   walked the slot to the higher rank, then a signed `delivered` on the frozen
   route — refused by monotonicity and discarded with its proof, so the slot
   kept asserting the failure. Checked with both unproven middles, an unsigned
   callback and a foreign recipient. Controls: two proven statuses leave one
   receipt each and repeats add none, and a foreign message id is still
   nobody's business.

The sixth round's four findings, all failing before the fix:

1. a booking created in one configured branch and handed on inside the window,
   with the only card holding those digits in ANOTHER configured branch: it
   named the creator and moved a manual recipient from unknown to `included`.
   Control: the same card in the creation's own branch still names the creator.
   Plus the forward direction, asserted against the control that has no such
   card: an unconfigured card supplying a recipient's numeric bridge, and a
   stranger's unconfigured card sharing a proven number and refusing the whole
   booking question as an identity conflict;
2. a visible booking of this person's, heard of weeks before the mailing, whose
   undated creation names somebody else — `included`, while the identical row
   with a usable creation date read unknown. Checked for a bridged recipient
   whose number the creation does not match, for a manual one whose creation is
   positively another's, and for one where nothing is established either way,
   each with and without the instant. Plus two creations of one booking from two
   configured branches naming the same number, with both instants, with the
   first missing and with the second missing. Control: their own cancelled
   booking with owner and branch agreeing;
3. a stranger's old booking whose two creations agree about its creator and
   disagree about its date, holding two unrelated manual recipients open at
   once; and the same with a single undated creation, with and without a later
   handover. Controls: a creator that really cannot be established still reads
   unknown, and a booking ahead still excludes;
4. a cancellation of this person's booking stored weeks before the mailing
   beside a single creation claiming an instant after it — excluded as a new
   booking the capture's own earlier row says was already there. Controls: a
   late creation delivery carrying a truthful earlier date is ordinary, and a
   second booking whose own facts agree still excludes.

The seventh round's seven findings, each failing before its fix:

1. a recipient whose numeric id is proven only in branch A, and a booking that
   appeared in configured branch B after the attempt, was handed on, and is not
   in their current history: `included`, because having a bridge switched the
   slot to rules that assume the bridge sees everything. Checked dated and
   undated, with the same digits in A and B and with different ones. Controls:
   a proven foreign creator in B settles it, and their own proven new booking
   still excludes them;
2. one booking created in A under a number and cancelled in B under the SAME
   number, gone from a manual recipient's list with no creator established
   anywhere: `included`, because the capture compared bare numbers and saw no
   reassignment. Both delivery orders, dated and undated. Controls: the same
   pair twice in one branch is no handover, two pairs proven to be one person
   move nothing, and in the §PR-17 report that booking is still one booking and
   one conversion with the disagreement shown;
3. the attempt, a cancellation stored the day after it, and the booking's one
   creation claiming the day after that: `excluded` as a new booking. Also with
   an instant later than the preview itself. Controls: a late delivery with a
   truthful old date is ordinary, a creation dated at its own first sighting
   still excludes, and another proven new booking or a booking ahead excludes
   for its own reason;
4. an early `booking-created` with a proven branch and booking identity and no
   customer, dropped whole, so a later complete creation of the same booking
   claiming an instant after the attempt read as a new booking and `excluded`
   somebody. Controls: an unconfigured branch, an unreadable booking identity
   and an arbitrary payload date nothing, and such a delivery alone blocks
   nobody and is no conversion;
5. a different customer's card in another configured branch carrying this
   recipient's digits against a different proven uuid: an identity conflict
   (`followup_identity_conflict`) with a completely empty history. Controls: a frozen card now
   naming somebody else still refuses, two numbers claiming one proven uuid
   still refuse, and the §PR-17 report's own resolver call is unchanged;
6. two creations of one booking naming two branches, both pairs proven by cards
   to one stranger, outside a manual recipient's list: `unknown`, because two
   branches meant no owner could be named at all. Checked with two different
   strangers too. Controls: a pair bridging to nobody, a pair that reads back
   to the recipient, and the same booking inside their own list all stay
   `unknown`;
7. through the real Ops POST: 32 766 ids in the owner lookup returned 503
   `voucher_production_database_unavailable`, and a capture past
   `MAX_SCANNED_EVENTS` failed there before the truncated pass could answer.
   Controls: the proven card sits at the top of the range asked about, and the
   same request over the real endpoint gives the same counts with a statement
   budget small enough to force many slices.

The eighth round's four findings, each failing before its fix:

1. a booking created in one configured branch under a number nothing resolves,
   with a cancellation of it from the OTHER configured branch naming no
   customer: `included` for a manual recipient with a completely empty history,
   because the missing field made the delivery disappear while the same
   delivery with a number read as a move. Both delivery orders, and with their
   list holding a proven old booking of their own. Controls: the same shape
   with an old creation is no gap at all, a proven foreign creator settles it,
   a cancellation from the creation's own branch moves nothing (named or not),
   and two pairs proven to be one person still move nothing;
2. one booking uuid with a complete creation and a second creation naming no
   customer — two different configured branches with one instant, and one
   branch with two instants straddling the mailing, both deliveries stored
   later than both instants. The complete delivery decided alone, so the row
   read `included` or `excluded` according to the date that delivery happened
   to carry. Both delivery orders and both sides of the send date. Controls: a
   partial delivery that agrees is an ordinary repeat and the booking still
   excludes, an unconfigured branch, an unreadable booking identity and an
   arbitrary payload contradict nothing, and a lone partial creation blocks
   neither recipient of a two-slot batch;
3. no creation observed, a cancellation naming nobody from a branch the
   recipient has no card in, and then a `booking-updated` naming a DIFFERENT
   customer in their own branch: `included`, because a statement about the
   later holder was read as an answer about the creator. Also with a partial
   creation elsewhere and a late cancellation here, and with two updates for
   two holders. Controls: a proven foreign creator and a booking already known
   before the attempt both release it, and their own new booking or a booking
   ahead still excludes for its own reason;
4. through the authorised local Ops POST: `booking_created_at` of
   `0001-01-01T00:00:00+01:00`, and separately `9999-12-31T23:59:59-01:00` —
   the UTC conversion overflowed and the whole batch came back as HTTP 500.
   Controls: an ordinary unreadable string, a valid instant with an offset
   which still excludes, and a second independent row of the batch which keeps
   its ordinary answer.

## 11. Rollout prerequisites — NOT satisfied

Before any production follow-up audience is used, independently of this stage:

1. **The historical consent reconciliation is NOT finished.** At the last
   diagnosis supplied by the owner the stored-event scan was incomplete and
   unapplied, and the suppression table was empty. **The absence of suppression
   rows is not proof of consent.** This is a separate mandatory owner step; it
   is not marked complete by this stage, no diagnostic number from it is
   hard-coded anywhere, and its `--apply` is not run during development.

   The preview no longer depends on that step having been taken before it is
   safe to open: it performs its own bounded, read-only completeness check for
   the recipients it is about (section 5), blocks an addressee whose stored
   refusal is unaccounted, and reports `unknown` for everybody when that check
   cannot be completed. Finishing the reconciliation is still required before a
   production audience is used — the per-preview check is a fail-closed
   precondition, not a substitute for the projection.
2. **The follow-up Meta template does not exist.** It must be created and
   approved by its owner, and its local row added (section 6).
3. **There is no proven failure-admissibility policy** (section 3), so the third
   group yields zero until the owner decides one.
4. **Positive statuses need their proof recorded going forward.** The
   `delivered`/`read` path now writes the §46.3 proof beside the transition, but
   only for callbacks that arrive from now on. Every slot of a mailing already
   sent has a status and no proof, and is therefore `unknown` in a preview. That
   is the honest answer and not a defect to work around; a production audience
   built from an older mailing will be correspondingly small until its slots
   receive proven callbacks, and the owner decides whether to wait for that or
   to confine the first follow-up to a mailing sent after this deploy.
5. **Sending is not implemented** (section 9). A stored preview authorises
   nothing.
