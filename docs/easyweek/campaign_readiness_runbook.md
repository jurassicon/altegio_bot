# EasyWeek campaign readiness after PR-15

PR-15 adds a read-only customer booking-history guard to the proven subset. Existing
Altegio campaigns keep their current preview, send-real, loyalty, follow-up,
retry and reporting paths. EasyWeek campaign sending is **not enabled**.

## Expected state

An EasyWeek preview creates a completed `CampaignRun` and durable recipient
snapshots from processed `booking-succeeded` evidence. It reads only local
EasyWeek rows. The separate preflight uses reviewed EasyWeek GETs only. EasyWeek send-real, resume, retry and
follow-up requests remain refused. A late EasyWeek campaign/newsletter job is
made terminal before template/sender lookup, CRM or loyalty access, Outbox
creation, Meta, Chatwoot, or an attempt increment.

Transport configuration is necessary but not sufficient. Readiness resolves
the provider, location and booking page, sender, DB-first Meta template,
language, segment source, live guard and supported job types as one scope. A
configured URL, sender and template cannot substitute for delivery authorization.
The segment source remains `easyweek_booking_succeeded_local_proven_subset`;
the live guard is `easyweek_customer_booking_history_reproof`, while supported
EasyWeek job types remain empty and `campaign_execution_not_authorized` keeps
global readiness false.

## Preview and coverage

Use the existing campaign preview entry point with `provider=easyweek`, one
registry-backed `company_id` and a half-open UTC period. The resulting run meta
contains `segment_completeness=proven_subset`, coverage counters and PII-free
reason counts. It contains no raw EasyWeek payload.

The counters distinguish seen/normalized succeeded events, missing or
non-first current/source visits totals, missing or mismatched domain identity,
service/category/phone/opt-out/future-booking exclusions, eligible evidence
rows, unique clients, duplicates and conflicting first-visit evidence.

The observed baseline had 265 of 300 EasyWeek clients without a proven current
`visits_total`. They are not treated as new: missing history is unknown, never
zero. A past `starts_at` only places an already proven succeeded booking in the
period; it does not prove attendance by itself.

## Durable source-event retention

An eligible EasyWeek preview recipient keeps its referenced
`easyweek_events` row as durable source proof. The manual event-retention
procedure in `capture_runbook.md` therefore skips every old event referenced by
an EasyWeek `CampaignRecipient` and reports the retained count separately.

Discarding, hiding or soft-deleting a preview does not physically delete its
`CampaignRecipient` audit rows, so it does not release the referenced source
events. Physically deleting campaign audit history, defining its retention
period or releasing those events is outside PR-14 and requires a separate,
explicitly authorised task.

## Read-only preflight

Run one bounded re-proof from the application environment:

```bash
uv run python -m altegio_bot.scripts.easyweek_campaign_preflight PREVIEW_RUN_ID --limit 50
```

The command reads eligible recipient snapshots, then sequentially reads the
source booking, its exact customer card, and every fixed-size page of
`GET /bookings?customer_uuid=...`. Requests and pages are paced and bounded.
The safe report separates `live_guard_ready` from
`delivery_authorized=false` and `ready_for_send=false`; it contains counters,
page count, reason distribution and `truncated`, but no UUID or customer PII.
It never changes recipient status or attempts and never creates jobs/outbox. A
truncated result proves only the inspected slice. The command deliberately
exits non-zero because this eligibility proof cannot grant send permission.

`GET /customers/{uuid}` does not provide `visits_total`, and the Public API has
no usable `/customers/{uuid}/bookings` endpoint. The proof therefore reconciles
the complete, strictly paginated booking filter above and verifies every row
belongs to the same EasyWeek customer card. It requires exactly one completed,
non-canceled visit and no active future booking. This is not an import or
reconstruction of historical Altegio visits.

## Zero counter reported by a delivery

A processed `booking-succeeded` may carry the JSON integer `0` in
`visits_total`. The counter is still refused — `Client.easyweek_visits_total`
stays NULL and the normalizer keeps rejecting the value — but the preview no
longer loses the person.

**Where the row is.** Open the preview, then **Recipients**
(`/ops/campaigns/<run_id>/recipients`). The row is `skipped`, its basis is
`auto`, and its reason reads: «EasyWeek подтвердил завершение визита, но передал
счётчик 0. Первый визит требует проверки». Filter by the stable code
`easyweek_campaign_source_visits_total_zero` to list only these. The run meta
counts them twice: `zero_visits_total_reported_events` is deliveries and
`zero_visits_total_unique_clients` is people, so a replayed webhook does not
inflate the number of cards to review.

The row is reviewed and excluded. It is not in the active audience, it does not
change the voucher nominal, and it carries no source proof. Its basis reads
`auto` because that is the default for every non-manual row, so the screens show
«первый визит не доказан» beside it and the recipients JSON answers
`first_visit_proof_recorded: false`. It claims no confirmed visit in an allowed
service category either, because none was ever checked for it. A person the
segmenter already proved, or already excluded for a reason of its own, keeps
that verdict: this row only exists for somebody who would otherwise have no row.

`total_records_in_period` on that row is the count the field's name promises —
this client's distinct, non-deleted records starting inside the campaign's
half-open period, counted in SQL. Two different bookings in the period read as
2; one booking delivered twice reads as 1. It is never a count of zero-counter
deliveries.

**Which delivery the row is about.** The preview records it when it creates the
row: the event id, the record id and the booking's identity, stored in the
recipient's own snapshot and deliberately **not** in the five source-proof
columns. Opening the row re-proves that chain and nothing else, so a
zero-counter delivery that arrives later cannot become the source of a row that
was written about a different visit. If the preview saw two different
zero-counter bookings for one card, it records that too and the check refuses as
ambiguous rather than picking one. A row from before this path existed has no
binding, and the check says «источник не установлен» instead of guessing. The
binding is never written onto an existing, manual or frozen row afterwards.

**Which delivery the contact comes from.** Recorded in the same snapshot, as its
own two fields, because "which delivery carried the counter" and "which delivery
carried the contact" are different questions. A zero-counter delivery often
carries no phone at all, so the preview also looks at the other processed
deliveries of that same booking — a repeat of it, or its `booking-created` /
`booking-updated` — and takes a contact only from one that proves the same
branch, the same booking UUID, the same numeric booking id and the same numeric
customer id. It stores how many different contacts those deliveries carried and,
when the one contact came from a sibling delivery, which delivery that was. The
number itself is **not** stored. One contact is usable; none means the identity
cannot come from this route; more than one means the deliveries contradict each
other about who this is, and the check refuses rather than choosing. A delivery
that arrives after the preview supplies nothing, because the binding was already
decided.

**How to check the history.** Press **🔎 Проверить историю** on that row. The
server takes the run id and the recipient id only, and re-derives the branch,
the period, the client and the originating booking from its own rows; a customer
UUID or phone number sent by the browser is refused. One operator gets one check
at a time. The identity comes from one of the two routes §44.2 allows: the
stored numeric-to-UUID correspondence, or — when there is none — the contact
**one of that booking's deliveries captured**, resolved across the whole
workspace to exactly one customer and then read back directly. The current
owner of the booking is never used to supply a missing identity.

The correspondence is read **workspace-wide**, through the same rule the webhook
bridge uses: one customer UUID may legitimately hold cards in several proven
branches, so a correspondence stored on another branch's card identifies the same
person here. That case is checked and works. Two different UUIDs stored against
one numeric customer id is a conflict and stops the check. So is a live answer
naming a different customer than the stored correspondence does — which is the
case where the Karlsruhe card has no UUID, another branch says `N → A`, and the
phone and the current booking both point at `B`.

Because the lookup is a network call and the webhook bridge writes during it, the
local identity facts — the Karlsruhe card included — are read a second time
afterwards. A correspondence that appeared mid-lookup and names somebody else, a
second card that has taken the proven UUID, or a card whose contact has changed
all stop the check there. In every one of these cases nobody's booking history is
read at all.

The diagnostic's phone lookup is also stricter than the migration one about
coverage: page parameters and metadata must be consistent, `per_page` stable,
every page before the last one full, the rows handed over must add up to the
declared total, and every row must be a distinct customer. A listing declaring
`total: 2` that repeats one UUID is indeterminate here — the row never shown may
be a second person on that number — and the check stops without reading a
history. The migration lookup and the manual Add keep their own contract, in
which one UUID listed twice is one customer, unchanged.

It then reads the source booking and that customer's complete booking history,
read-only, within one deadline that covers the local checks and the network
together.

A successful answer states the counter the delivery carried (0), the number of
completed non-canceled visits in the full API history, what became of the exact
source booking, whether anything is booked ahead, the moment it was read, and
the state of the row as the server saw it while checking. The source booking's
state is reported in its own words, because "in the history" and "counted as a
completed visit" are two different facts: it is either completed and not
canceled (counted), found but canceled, found but not finished, or not in the
history at all.

Kinds of refusal, and they mean different things:

- «не удалось подтвердить» — we will not say whose history this is. The source
  event no longer attributes to this person, the booking has been reassigned,
  the identity is unproven, or one contact resolves to more than one card.
- «конфликт идентификаторов клиента» — the workspace or the API contradicts
  itself about who this numeric customer is. Repeating the check cannot settle
  it; the cards have to be looked at.
- «не удалось проверить историю» — the history itself could not be trusted:
  incomplete pagination, or a listing whose coverage could not be proven.
- «EasyWeek отказал в запросе» — a permanent 4xx (401, 403, 404, 422) or an
  unusable API configuration. **Not** offered as a retry: retrying cannot work.
- «EasyWeek вернул ответ неожидаемого формата» — a 200 whose body is not the
  promised shape. Also not a retry.
- «временно не удалось проверить историю» — a 429, a 5xx or a timeout. Nothing
  is wrong with the data and only this one is worth repeating.
- «проверка не успела завершиться на нашей стороне; EasyWeek не запрашивался» —
  the one deadline expired while reading our own rows. EasyWeek was never asked,
  so the message does not say EasyWeek failed to answer.

An unavailable history is never shown as zero visits, and no count appears for
an answer that was not read.

The check writes nothing: no status, no reason, no counter, no consent, no UUID,
no phone, no entitlement. It holds no database lock while the API is read.
Having looked at the row grants nothing — automatic eligibility still requires a
source and current `visits_total` of exactly 1 with a paired timestamp, and
reading the API produces neither. The screen says so on every answer, and a
refusal says only that this check changed nothing: if another tab has already
included the person, the refusal reports that decision instead of claiming the
row is still excluded.

**What a manual decision means.** If an operator decides to include the person,
use the existing single **Add recipient** with their phone number. The same row
becomes `operator_manual_selection`, its original verdict is kept in
`auto_excluded_reason`, and no second row is created. It is recorded as a
person's decision, not as a proven first visit: the source-proof columns stay
NULL and a database CHECK keeps them that way. A matching opt-out still blocks
the addition, and a frozen or already-used preview still refuses it. The §44
list mode «предыдущий визит в Altegio / ноль записей EasyWeek» does not apply to
this case.

## Read-only voucher evidence

The only confirmed Public API voucher-template identity is UUID
`49bc000c-c3a6-47c7-bdfd-b8ccd3ae2677`. Numeric dashboard identity is not an
API identity. An administrative edit page is not a customer purchase URL and
must never be placed in a job, outbox row or Meta parameter.

The readiness probe uses only reviewed GET operations:

- `/workspace`;
- `/locations`;
- `/voucher-templates`;
- `/voucher-templates/{voucher_template_uuid}` with the exact confirmed UUID.

The observed template is connected to all three branches, not Karlsruhe only.
It is disabled for online sales. Website or marketplace visibility does not
override that fact. The API supplied no proven customer-facing purchase URL,
and the application does not synthesize one.

`cost=1500` and `value=1500` in EUR are shown as an observed €15 monetary
nominal. Text containing `10%` is not machine-readable discount proof.
`validity=null` is reported as `unproven_or_unlimited_per_api`. Template and
voucher counters do not prove an issuance contract.

## Non-persistent calculation evidence

A separate operator command proves that the officially non-persistent
`POST /orders/calculate` returns a supported invoice for one €15 voucher line
and persists nothing. It is documented in
`voucher_contract_evidence_runbook.md`.

It is a manual command and nothing else: `GET /ops/campaigns/new-clients/readiness`
stays GET-only and never issues that POST, not even with
`include_gift_card=true`. It never follows a redirect, so it can never reach the
persistent order endpoint. Its `exit 3` means UNKNOWN and must not be wired to an
automatic re-run; `--help` exits `2`, so only a confirmed run with a proven
report can return `0`. A green run is momentary operator evidence, is stored
nowhere, and is not send authorization — `issue_contract_ready`,
`delivery_authorized` and `ready_for_send` stay `false` in every report, and
`gift_card_online_sales_disabled`, `gift_card_public_url_unproven`,
`gift_card_semantics_unproven`, `gift_card_issue_contract_unproven` and
`campaign_execution_not_authorized` are unchanged by it.

## Required evidence before a later send PR

The next separately authorized PR must prove gift-card issue/sale semantics,
then implement the write path and controlled delivery. It requires:

- a supported way to issue or sell one customer voucher;
- idempotent write identity;
- a customer-facing purchase URL or an individual voucher code;
- reconciliation after an unknown write result;
- exact product semantics and any required branch restriction;
- a controlled canary with an explicit rollback/reconciliation procedure.

Until every relevant proof exists, EasyWeek campaign and gift-card send-real
remain fail closed.

## Development boundary

Do not run production commands, authenticated production probes, CRM mutations,
voucher/POS writes, or customer sends while developing or reviewing PR-15.
Validation uses local tests and a disposable PostgreSQL 16 instance only.
