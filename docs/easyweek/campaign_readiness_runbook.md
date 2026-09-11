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
