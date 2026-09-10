# EasyWeek voucher calculation contract — evidence runbook

This runbook covers one narrow, operator-driven, fail-closed capability: proving
that the officially non-persistent

    POST https://my.easyweek.io/api/public/v2/orders/calculate

returns a supported invoice for exactly one €15 voucher line in the confirmed
Karlsruhe/template scope, and that it persists nothing.

It does **not** authorize issuing a certificate, taking payment, refunding,
binding a voucher to a customer, running a campaign send-real, or sending
anything to anybody. A green run of the command below is a moment of evidence,
not a permission, and it is not stored anywhere.

## 1. The officially documented contract

Per the current documentation at `developers.easyweek.io`:

- `/orders/calculate` computes an order preview and creates nothing;
- the request carries a location and one or more order lines;
- a voucher line is identified by a voucher template UUID and carries a price
  and a quantity;
- the response carries an invoice with the computed amounts.

Legacy `api-docs.easyweek.io` is not a trusted source and is not used here.

The application supports a deliberately smaller subset than the documentation
allows: exactly one voucher line, quantity exactly `1`, the confirmed Karlsruhe
location, the confirmed voucher template, and the price read from that
template's own fresh `cost`. There is no interface through which a caller can
supply a location, a template, a price, a quantity, a discount, a promocode, a
customer, a staffer, an account, goods or services.

## 2. Live evidence, 10.09.2026

Read-only context, re-proved on every run before the POST:

- API version `v12.108.2`;
- workspace `e66be240-362c-4fe4-9388-6ed187b27b93`, slug `kitilash`, currency
  `EUR`;
- Karlsruhe location `8395fab6-7ee8-4702-88d9-fd78f92539c1`;
- voucher template `49bc000c-c3a6-47c7-bdfd-b8ccd3ae2677`.

The numeric dashboard identity is not an API identity. It is not in the code, in
a fixture, in an endpoint or in this document, and it must not be added.

Template state at the time of the evidence: `enabled=true`, `is_online=false`,
`is_single_charge=true`, `cost=1500`, `value=1500`, `EUR`, `validity=null`,
`forces_activation=true`, connected to all three branches and to all 43
services, `vouchers_count=0`, `activated_vouchers_count=0`, no `value_type`
field, no customer-facing URL.

An exact `GET` of an existing POS order was confirmed only for a **cancelled**
order carrying a service and `vouchers=[]`. That proves nothing about a voucher
item's schema, its code, its URL or any customer binding.

### The calculate matrix that was actually run

| # | Request | Result |
|---|---------|--------|
| 1 | `price=1500` | HTTP 200; `base_amount`, `base_price`, `subtotal`, `total`, `amount_due` all `1500`; `discount_amount=0`; `amount_paid=0`; `voucher_paid_amount=0`; `order_uuid` present and `null`; `status` present and `null`; `account_paid_amount=-1500`; counters unchanged |
| 2 | `price` omitted | HTTP 422 naming validation field `vouchers.0.price`; counters unchanged |
| 3 | `price=0` | HTTP 200; every principal amount `0`; `order_uuid`/`status` `null`; counters unchanged |
| 4 | `price=1499` | HTTP 200; the API invoiced exactly `1499`; `order_uuid`/`status` `null`; counters unchanged |
| 5 | `price=1500` + `discount_amount=1500` | HTTP 200; `base_amount=1500`; response `discount_amount=-1500`; `subtotal`/`total`/`amount_due` `0`; `order_uuid`/`status` `null`; counters unchanged |

Only case 1 is the supported contract. Cases 2–5 are recorded as boundaries and
are covered by regression tests so that they can never become supported by
accident.

## 3. Observed but not interpreted

These fields were seen and are deliberately given no meaning:

- `account_paid_amount = -1500`. An opaque bookkeeping figure. It is projected
  into the report only when it is an exact integer, it takes no part in
  readiness, and it is **not** a payment.
- A negative `discount_amount` in the response. That is the shape the API uses
  to report a discount; it is not permission to send one.
- `cost = value = 1500` proves an observed €15 monetary nominal. It does not
  confirm the percentage semantics suggested by the template's title.
- `validity = null`, `forces_activation`, the template title and the
  all-branches connection are not read as issue, discount or scope semantics.
- `vouchers_count = 0` is reported as `template_pristine`. Zero is today's
  observation, not a permanent product rule.

## 4. Normative conclusions

- EasyWeek requires `price` to be present but does not check it against the
  template. `/orders/calculate` is therefore **not** a server-side price
  validator, and this application validates the price on its own side instead.
- The API accepts a zero price, an arbitrary price and a fully discounted
  preview. `price=0`, `price=1499` and any `discount_amount` are refused by the
  supported contract — in the transport, before the wire, and again in the
  invoice projection.
- A successful zero-total calculation does not prove that a free voucher can be
  created, paid for or handed to anybody.
- Calculate proves nothing about issuance, a unique code, a customer URL,
  activation, redemption, customer binding, payment or refund.

## 5. What is still unknown

- How a voucher is actually created, and what it looks like when it is.
- Whether an individual voucher carries a code or a customer-facing URL.
- Whether a write can be made idempotent, and how an unknown write result would
  be reconciled.
- Whether a payment account and a staffer with POS access exist for this
  workspace, and which ones.
- Whether the template should be frozen for the duration of a canary.
- The exact product semantics behind the template's title.

## 6. Running the preflight safely

```bash
uv run python -m altegio_bot.scripts.easyweek_voucher_contract_preflight --confirm-nonpersistent-calculate
```

Without `--confirm-nonpersistent-calculate` the command performs **no HTTP
request at all** — not even the reads — and exits with code `2`. Flag
abbreviations are disabled, so `--confirm` is not accepted.

What one confirmed run does, in this order:

1. `GET /workspace`, `GET /locations`, `GET /voucher-templates` and the exact
   `GET /voucher-templates/{uuid}`, re-proving workspace UUID, slug and
   currency, the Karlsruhe location, the exact template UUID, `is_enabled`,
   `is_single_charge` and an exact-integer `cost = value = 1500`;
2. exactly one `POST /orders/calculate` with one voucher line, quantity `1` and
   the price taken from that fresh template read;
3. the exact `GET /voucher-templates/{uuid}` again, to compare
   `vouchers_count` and `activated_vouchers_count` against the pre-call
   snapshot.

The POST happens at most once, whatever the outcome. A timeout, a transport
failure, a 429 and any 5xx are reported as uncertainty after a single attempt
and are never retried automatically: this command exists to prove that nothing
was persisted, and a second POST would weaken that proof.

The command opens no database session, writes no row, creates no
`CampaignRun`, `CampaignRecipient`, `MessageJob` or outbox entry, and saves no
raw response to disk. Its stdout is a PII-free report containing no customer,
no order UUID, no response body, no notes, no description, no voucher code, no
URL, no header and no credential.

## 7. Exit codes

| Code | Meaning |
|------|---------|
| `0` | The calculation contract was proven at that moment |
| `2` | Missing confirmation flag, bad arguments, or unusable configuration |
| `3` | Retryable API uncertainty — the outcome is unknown, nothing is proven |
| `4` | Contract mismatch — the reads, the invoice or the counters failed closed |

`exit 0` is **not** permission to create a voucher and **not** permission to
send a message. Every report, green included, repeats
`send_authorization: calculation_evidence_is_not_send_authorization`, and
`issue_contract_ready`, `individual_voucher_artifact_proven`,
`customer_binding_proven`, `write_idempotency_proven`,
`unknown_result_reconciliation_proven`, `delivery_authorized` and
`ready_for_send` are all constant `false`.

## 8. Stable reasons

All PII-free, all testable, none carrying a UUID, a value or provider prose:

- `gift_card_calculation_configuration_unavailable`;
- `gift_card_calculation_template_unproven`;
- `gift_card_calculation_price_unproven`;
- `gift_card_calculation_rejected`;
- `gift_card_calculation_retryable_uncertainty`;
- `gift_card_calculation_response_malformed`;
- `gift_card_calculation_amount_mismatch`;
- `gift_card_calculation_persistence_signal`;
- `gift_card_template_counter_drift`;
- `gift_card_calculation_contract_unproven`;
- `gift_card_calculation_not_confirmed` (missing confirmation flag).

A 422 may surface only the **names** of fields this client itself sends —
`location_uuid`, `vouchers`, `voucher_template_uuid`, `price`, `quantity` —
never a value, never the server's message, never the body.

## 9. Do not repeat the research matrix

The matrix in section 2 was a one-off, owner-approved production research pass.
It must not be re-run as routine practice. In particular, do not probe
`price=0`, `price=1499` or `discount_amount` against production again: those
boundaries are now regression tests driven by `httpx.MockTransport`, and
re-running them adds unexplained server-side events without adding evidence.

Only the supported canonical case may be run against production, only through
the command in section 6, and only when an operator needs current evidence.

## 10. Next stage

The next stage is a separate, explicitly authorized **controlled mutation
canary**. It cannot start until all of the following are decided and designed:

- a staffer with POS access is chosen;
- a safe payment account is chosen;
- a decision is taken on whether to freeze the template during the canary;
- unknown-result reconciliation is designed, including what a durable marker is
  and how a possibly-created voucher is found again;
- write idempotency is established, or its absence is explicitly accepted with
  a reconciliation procedure;
- a rollback procedure exists.

Until that separate PR exists and is authorized, gift-card issuance, payment,
refund and every EasyWeek campaign send remain fail closed.

## 11. Development boundary

Do not run production commands, SSH, authenticated EasyWeek probes or real
POST requests while developing or reviewing this work. Every external scenario
is modelled with `httpx.MockTransport`; no test opens a socket.
