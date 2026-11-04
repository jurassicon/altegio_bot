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
- its recipient basis is the earned one. A canary, a snapshot ledger, a
  manually composed batch or an Altegio campaign is refused by name
  (`followup_batch_scope_unsupported`);
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
5. **entitlement** — the ORIGINAL artifact. An explicitly refunded, cleaned or
   unsettled slot is excluded (`followup_entitlement_invalid`). A slot whose MAC,
   key id or order uuid is missing, or whose MAC was written under a key this
   deployment no longer holds, is `followup_entitlement_unproven`: nothing is
   wrong with the voucher, the **proof** is gone. §45.4 is respected in full —
   EasyWeek owns validity, and an artifact that simply carries no activation or
   expiry date is the ordinary shape of a correct voucher, never a new
   prohibition. No new voucher, entitlement, CREATE, PAY, code replacement,
   validity extension or promise of an unverified balance exists here;
6. **consent** — the PR-22.1 aggregate unchanged: the suppression ledger for
   this address and for every address a card of this uuid holds, a refusal bound
   to the uuid, `Client.wa_opted_out` on **any** matching card including another
   branch's or an Altegio one, and the historical recipient snapshot. A proven
   refusal excludes (`followup_consent_refused`); accepted but unprocessed
   inbound **holds** (`followup_consent_pending_inbound`) — a busy worker queue
   is not a reason to write to somebody who may have just said stop; a gap is
   `followup_consent_unknown`. An ordinary reply, a new booking and a delivery
   status are never opt-in, and the proven-START rules are untouched;
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

### Locks, the network, and the re-check

The external step holds no database session, no row lock and no advisory lock —
by construction, because that phase has no database handle. After it, and before
the snapshot is stored, every included row's mutable local facts are asked again:
consent (compared against the fingerprint the first pass recorded, so a state
that moved at all demotes the row), replies, and the original batch's terminal
stop. A terminal stop found there refuses the whole save; nothing is stored.

The preview takes **no** consent address lock, which is the one place it differs
from a send: a claim must be serialised against concurrent writers, while a
preview must not delay an arriving STOP by so much as one transaction. The
PR-22.1 lock order is therefore untouched, and PR-22.3's locked guard remains the
thing that serialises an actual send.

### One approximation, stated plainly

EasyWeek's live booking rows carry no creation time, so "booked after the
attempt" is read off the **appointment** time. A booking created before the
attempt for a date after it is therefore treated as new. That can only
over-exclude, never under-exclude.

### One asymmetry inherited from PR-22.1

The `delivered`/`read` transition path does not record callback authenticity
today, while the failure path requires a verified signature. The delivered and
read groups therefore rest on exact message-identity linkage — the slot's own
`provider_message_id`, which exists only because Meta answered this process with
it — and not on a stored signature proof. Before any production follow-up is
sent, the owner decides whether that is sufficient or whether the delivered/read
path must record authenticity as the failure path does. Nothing in this stage
depends on the answer, because this stage sends nothing.

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
- Validity is 24 hours. A stale snapshot is shown as stale and never quietly as
  current; a failed check never leaves an old preview looking like today's
  answer. **A snapshot is not a send permission**: it is evidence of what was
  true at its check time.
- A preview neither spends nor reserves a send attempt, and creating one twice
  must not block anybody permanently — the dedupe contract is scoped to **one**
  version. Within a version the database itself enforces at most one included
  row per source slot, per canonical customer uuid and per original entitlement
  (two partial unique indexes). That is the source/dedupe contract PR-22.3 will
  claim against; the global one-shot execution, the claims and the terminal
  tombstones belong to that stage and are deliberately absent here.
- Nothing fabricates a successful check status of a previous follow-up attempt.
  The preview model and the future execution ledger are separate.

### Migration

`b7d41f9c0a52`, one Alembic head over `a1c4e7b25d80`. Additive and narrow: two
new tables, no column of an existing table altered, no consent row, delivery
fact, attempt counter, HMAC byte or financial value touched. Its `downgrade()`
locks both tables, counts stored previews and **refuses before any DDL** if any
exist. Upgrade from scratch, `downgrade -1` and re-upgrade were verified on a
disposable PostgreSQL 16.

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

plus one browser acceptance test in
`src/altegio_bot/tests/test_easyweek_voucher_mailing_browser.py`, which drives
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

## 11. Rollout prerequisites — NOT satisfied

Before any production follow-up audience is used, independently of this stage:

1. **The historical consent reconciliation is NOT finished.** At the last
   diagnosis 29 244 WhatsApp events carried a payload, the dry run covered
   20 000 of them (`truncated=true`, `applied=false`), 2 potentially new refusal
   addresses were found in the scanned part, and the suppression table was
   empty. **The absence of suppression rows is not proof of consent.** This is a
   separate mandatory owner step and is not marked complete by this stage;
   its `--apply` is not run during development.
2. **The follow-up Meta template does not exist.** It must be created and
   approved by its owner, and its local row added (section 6).
3. **There is no proven failure-admissibility policy** (section 3), so the third
   group yields zero until the owner decides one.
4. **The delivered/read authenticity asymmetry** (section 5) is an owner
   decision.
5. **Sending is not implemented** (section 9). A stored preview authorises
   nothing.
