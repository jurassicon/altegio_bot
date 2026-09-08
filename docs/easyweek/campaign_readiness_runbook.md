# EasyWeek campaign readiness after PR-13

PR-13 makes campaign data and execution provider-scoped. Existing Altegio
campaigns keep their current preview, send-real, loyalty, follow-up, retry and
reporting paths. EasyWeek campaigns are **not enabled** by this work.

## Expected state

An EasyWeek preview records a failed diagnostic run with the PII-free reason
`easyweek_campaign_segment_not_implemented`. EasyWeek send-real, resume, retry
and follow-up requests are refused. A late EasyWeek campaign/newsletter job is
made terminal before template/sender lookup, CRM or loyalty access, Outbox
creation, Meta, Chatwoot, or an attempt increment.

Transport configuration is necessary but not sufficient. Readiness resolves
the provider, location and booking page, sender, DB-first Meta template,
language, segment source, live guard and supported job types as one scope. A
configured URL, sender and template cannot substitute for an implemented
EasyWeek segment source and live eligibility guard.

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

The next separately authorized PR must first prove EasyWeek segmentation and a
send-time live eligibility guard. Gift-card sending additionally requires all
of the following:

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
voucher/POS writes, or customer sends while developing or reviewing PR-13.
Validation uses local tests and a disposable PostgreSQL 16 instance only.
