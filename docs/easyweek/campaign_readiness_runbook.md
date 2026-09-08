# EasyWeek campaign readiness after PR-14

PR-14 adds a local read-only EasyWeek preview of a proven subset. Existing
Altegio campaigns keep their current preview, send-real, loyalty, follow-up,
retry and reporting paths. EasyWeek campaign sending is **not enabled**.

## Expected state

An EasyWeek preview creates a completed `CampaignRun` and durable recipient
snapshots from processed `booking-succeeded` evidence. It reads only local
EasyWeek rows and calls no CRM API. EasyWeek send-real, resume, retry and
follow-up requests remain refused. A late EasyWeek campaign/newsletter job is
made terminal before template/sender lookup, CRM or loyalty access, Outbox
creation, Meta, Chatwoot, or an attempt increment.

Transport configuration is necessary but not sufficient. Readiness resolves
the provider, location and booking page, sender, DB-first Meta template,
language, segment source, live guard and supported job types as one scope. A
configured URL, sender and template cannot substitute for an implemented
EasyWeek live eligibility guard. The segment source is now
`easyweek_booking_succeeded_local_proven_subset`; the live guard remains
`campaign_live_guard_unproven` and supported EasyWeek job types remain empty.

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

The command reads eligible recipient snapshots and issues at most one reviewed
`GET /bookings/{uuid}` per selected recipient, sequentially. It reports counts,
reason distribution and `truncated`; it never changes recipient status or
attempts and never creates jobs/outbox. A truncated result proves only the
inspected slice. The command deliberately exits non-zero because PR-14 cannot
grant send permission.

A successful GET proves only that the named booking UUID is current in the
expected location, not canceled, and has one ordered service. Customer PII and
UUID, status display text, links, service names and prices are ignored. The GET
does not prove numeric customer identity, current visits total, full history or
absence of other future bookings, so `campaign_live_guard_unproven` remains.

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

## Required evidence before a later send PR

The next separately authorized PR must prove customer-level live eligibility
using a documented current-visits/customer identity endpoint, a documented
customer booking/history listing, another confirmed customer-level source, or
a separate reconciliation contract. Gift-card sending additionally requires:

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
voucher/POS writes, or customer sends while developing or reviewing PR-14.
Validation uses local tests and a disposable PostgreSQL 16 instance only.
