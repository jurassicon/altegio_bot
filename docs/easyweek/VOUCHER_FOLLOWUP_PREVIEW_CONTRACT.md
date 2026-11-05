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

Without the numeric bridge the live set is the whole account of this person's
bookings, and the one thing it cannot show is a booking that LEFT it. So a
reassignment observed after this slot's own send anchor, of a booking that is not
in their list, leaves the slot `unknown`. A window with no reassignments hides
nothing, and the recipient is provable.

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
  the conjunction reads as "these checks hold"; everything else is shown as a
  historical result that needs recomputing, with the reason named. So a paused
  template, a stop since, an expired window or a refused refresh cannot leave a
  stale block looking fresh.
- **And the page it is already on stays honest too.** A standing the server
  rendered describes the moment the page loaded. When the operator then presses
  «Проверить состав» and no new result comes back — a 409, a transport failure,
  or an answer that cannot be read — that click has just proved the opposite, so
  the displayed block drops to the historical state immediately and without a
  reload: the success colour goes, the standing sentence says the checks are not
  confirmed now, and the claim that the counts are confirmed is replaced. The
  first round changed only one line of status text underneath, leaving the green
  block asserting «Эти проверки актуальны» and «Они подтверждены и сейчас». The
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
