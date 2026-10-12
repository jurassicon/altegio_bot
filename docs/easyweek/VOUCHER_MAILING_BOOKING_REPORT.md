# Voucher mailing report and post-send bookings — contract

The tracked contract of the compact Ops report for EasyWeek voucher mailings and
of the statement **«записался после рассылки»**. It covers what the report
claims, what it refuses to claim, how the time window is defined, which
branches are in scope, and what the report may never do.

This is a reporting stage. It does not change the voucher product, the gift
conditions, the message, the issuing procedure, the mailing's own geography, or
any financial, FREEZE/CREATE/PAY/DELIVER, STOP, entitlement, dedupe,
reconciliation or refund rule. See
`docs/easyweek/VOUCHER_PRODUCTION_MAILING_RUNBOOK.md` for the mailing itself.

* Policy identifier: `easyweek-booking-report-v1`
* Attribution window: **the campaign's own `attribution_window_days`** (30 is
  the campaign API's default, not a universal contract)
* Code: `src/altegio_bot/campaigns/easyweek_voucher_production/booking_attribution.py`
* Interface: `src/altegio_bot/ops/voucher_mailing_report.py`,
  page `/ops/voucher-mailings/report?batch_id=…` (or `?preview_run_id=…`)

---

## 1. What the report claims

Exactly one observable fact per recipient:

> a NEW booking was created somewhere in this EasyWeek workspace after a send
> this application can prove left it.

It is **not** evidence of advertising influence, of a visit, of a payment, or of
a voucher being used. The page says so in those words, next to the number.

Three further statements the report makes and never blurs:

* **Acceptance, delivery and reading overlap.** A read message was also
  delivered and also accepted. The three counters are independent facts about
  the same recipients, never mutually exclusive parts of a total.
* **No read callback is not "unread".** The absence of a confirmation is the
  absence of a confirmation.
* **A delivery or read time is the moment the confirmation was recorded**, not
  the moment the customer acted. It appears only in the per-row details, under
  that description.

## 2. The send anchor, and the window

| | |
| --- | --- |
| Anchor | the slot's durable `send_attempted_at` |
| Used only when | the **same** send has positive acceptance evidence: `provider_accepted_at`, or a `delivered`/`read` webhook (the schema makes both imply acceptance) |
| Window | `send_anchor < booking_created_at <= send_anchor + window` |
| Window length | `CampaignRun.attribution_window_days` of the campaign the batch was frozen from |

* `provider_accepted_at` is **never** the anchor. A callback may record
  acceptance long after the attempt, and anchoring on it would move the start
  of the window forward in time and retroactively disqualify bookings that were
  already inside it.
* One attempt with no proven acceptance yields **no positive attribution**:
  the row reads «недостаточно данных», reason `send_attempt_unconfirmed`.
* With no proven anchor the report states that, and never invents a send time.
* The window is a duration from an instant, not a calendar step, so a DST
  change moves no boundary. Storage and comparison are timezone-aware UTC;
  only the rendering is local.
* `booking_created_at` comes from the captured `booking-created` payload. It is
  never substituted by the webhook receipt time, the visit date,
  `last_change_at`, the preview time or the batch creation time.
* The **policy** is a code constant and deliberately not a setting: a late
  configuration change must not silently redefine what an older report meant.
  The report prints the policy version that produced it.
* The **window** is not a constant: it belongs to the campaign.
  `CampaignRun.attribution_window_days` is written once when the run is created
  and nothing in this project updates it afterwards, so a frozen batch keeps
  the window its own campaign was created with however the default moves. The
  report prints the number, and names `campaign_run` as its source. No
  reporting metadata is added and no migration is needed for this: the stored
  column is already durable and already immutable in practice.
* A stored value outside the bounds the campaign API itself enforces (1–365) is
  reported as `attribution_window_unusable` and the rows read «недостаточно
  данных». It is **never** replaced by 30 or by any other convenient number.
* Reading a report never rewrites a signed batch fact or an HMAC binding.

## 3. Identity

A frozen slot carries the customer UUID proven at freeze time. EasyWeek's
booking webhooks name a customer by NUMERIC id, so the two are bridged using
proofs that already exist:

1. the local card the freeze itself re-proved for that slot
   (`CampaignRecipient.client_id`, checked under lock by `local_identity`) and
   the EasyWeek numeric id on it — this is the earned, numeric-only shape;
2. any EasyWeek `Client` row whose `easyweek_customer_uuid` equals the slot's
   proven UUID — the independent binding the inbox worker establishes.

Rules:

* a matching name or phone is **never** a bridge;
* the current booking customer is never used to decide who owns a historical
  event, and history is never linked through `Record.client_id` — the booking
  may have been reassigned;
* two different numeric ids for one proven UUID, or a numeric id that another
  card binds to a different UUID, is a **conflict**: the row is reported as
  unknown and nobody is credited;
* one customer UUID may hold cards in several branches. That is one recipient,
  not several conversions. The report lists the branches and merges nothing;
* opening or refreshing the report fills no `Client` UUID, merges no cards and
  writes nothing at all.

## 4. Branch scope

* The scope is the **configured EasyWeek location registry** — every branch of
  the workspace — and deliberately not the mailing's own `company_id`. The
  mailing keeps its existing single-branch limits; only the analytics are
  workspace-wide.
* A delivery counts only when its `location_id` is in the registry **and** its
  `location_uuid` matches that entry exactly. Altegio data, unknown locations
  and other workspaces are excluded and counted as gaps.
* If the registry is unreadable, neither a positive nor a negative answer is
  available: every row reads «недостаточно данных».

## 5. Booking identity, duplicates and state

* Bookings are deduplicated by provider/workspace booking identity
  (`uid`, canonicalised). A retried webhook, a Resend and a rescheduled
  appointment are one booking, never a second conversion.
* Only a `booking-created` delivery can establish a creation. Two creation
  deliveries for one booking that disagree about the customer, the instant or
  the branch are a conflict and are not a positive fact for anybody.
* A late event about a booking created **before** the send is not counted.
* Recipients are counted uniquely. Several bookings by one recipient are one
  recipient in the counter and several entries in the details.
* **Webhook arrival order is not business order.** EasyWeek may deliver a
  cancellation before the creation it cancels. The creation evidence (who,
  where, when) and the state evidence are therefore computed separately, and a
  late creation never resets a booking to `active`.
* Current booking state is reported separately from the historical fact:
  `active`, `cancelled`, `completed`, `unknown`. It is derived from the SET of
  distinct lifecycle facts observed, not from whichever delivery was read last.
  One terminal fact decides the state whichever order it arrived in, because a
  booking is always created before it can be cancelled or completed.
* A cancellation does not erase "created a booking"; one cancelled and one
  active booking are both visible. Only a `booking-succeeded` delivery makes a
  booking `completed` — a single `booking-created` never means the customer
  visited the salon.
* An edit or a reschedule contributes no state fact at all: it does not revive
  a cancellation and does not undo a completed visit, because neither
  transition is proven for this provider.
* A cancellation **and** a completion for one booking cannot be ordered from
  anything the captured payload carries, so the state is `unknown` with
  `state_reason: lifecycle_order_unproven` rather than a guess.
* An identical Resend is recognised by the capture's own content identity
  (`payload_hash`) and adds no second fact, so replaying an old payload cannot
  move the answer. Nothing is replayed by this report.
* A booking some delivery assigns to another customer keeps the creation
  attributed to the customer its own creation delivery named, flags
  `customer_reassigned` in the details, and credits the other customer with
  nothing.

## 6. Data completeness

Four answers are distinguished and never collapsed:

| Code | Meaning |
| --- | --- |
| `booking_found` | a booking was observed inside the window |
| `booking_not_observed` | nothing was observed in the available data |
| `insufficient_data` | the check could not be made (unconfirmed send, unknown or conflicting identity, no branch registry) |
| `send_not_attempted` | there has been no send yet |

* The absence of a webhook is not proof that no booking exists.
* `processed` does not by itself make an event usable for analytics, and
  `failed` does not mean there was no booking — a failure may concern
  notifications or an unsupported service. The report therefore validates
  booking identity, branch, customer and creation instant **itself**, from the
  captured payload, and ignores the inbox worker's status.
* Incomplete, truncated and conflicting payloads never become positive facts.
  They are counted by reason in the coverage block, with no personal data.
* The coverage block reports, per configured branch, how many new bookings were
  observed in the scanned range. A zero means "nothing observed here", not a
  proven absence of bookings.
* Failed events are not replayed and the EasyWeek inbox is not repaired here.

## 7. Reads, bounds and isolation

* Every route is a GET that reads the local database. No EasyWeek request, no
  Meta request, no write.
* The capture scan is bounded: below by the earliest proven send minus one day
  of late-capture margin, above by the moment of reading, with a hard row cap.
  If the cap bites, the report says the coverage was truncated.
* With no proven send there is no window, so nothing is scanned at all.
* Late events and a later proven identity binding are picked up by the next
  refresh, because each refresh re-reads.
* No live API lookup per row and per poll, and no unbounded scan of the whole
  webhook history on page load.
* Booking analytics never change recipients, eligibility, issuing stages,
  dispatch, execution counters or send permissions, and a failure of the
  analytics cannot break mailing execution or webhook handling.
* Booking states are stored and computed separately from execution statuses.
  `sent`, `paid` and `delivered` keep their meaning; `booked` never replaces
  them.

## 8. Interface

* Added to the existing Ops voucher-mailing section, as its own page, reachable
  from the mailing list and from the mailing page.
* The mailing page is unchanged: the stage buttons, STOP, «Сверить с EasyWeek»
  and the per-recipient refund stay there with exactly their previous
  restrictions, and the report links to them.
* The compact summary names: recipients, accepted by Meta, delivered, read,
  booked after the send, and rows needing attention (an execution error or
  insufficient data).
* The main table has five columns: client (name, with the phone as a second
  line only when two rows share a display name), message state, booking after
  the send, the created bookings, and the details toggle.
* Dates on the page are **creation** dates and are labelled «Создана: …». The
  column is «Созданные записи». The report has no proven appointment start, so
  it never presents a creation as the customer's next visit, and the details
  say so in words. There are no
  wide columns for UUIDs, attempts, reason codes, basis, reconciliation or
  technical timestamps — all of those are in the per-row details. The Altegio
  campaign/follow-up table is not brought here.
* Filters by message state and by booking state.
* Refreshing shows a loading indicator, the time of the last successful update
  and the error when one occurs. Rows already on screen are not blanked by a
  failed refresh.
* Times are shown only in the salon's configured zone (`OPS_LOCAL_TZ`,
  `Europe/Berlin` for the German branches), named once beside the report. UTC is
  not duplicated.
* If no batch has been frozen, or nothing has been sent, the page says so
  instead of drawing a preview as a completed mailing. For a preview the
  recipient count is the preview's own number of active recipients; for a
  frozen batch it is the batch's own composition.
* Checked at phone and desktop width. The one responsive rule the page depends
  on is defined locally, because the Bootstrap bundle comes from a CDN that may
  be blocked.

## 9. Security

* Ops authentication, as the rest of this section has it.
* Tenant/provider isolation: EasyWeek only, the configured workspace only.
* No voucher code, HMAC, token, raw payload, order UUID, intent UUID, customer
  UUID or booking UUID is served or rendered.
* Personal data appears only in one narrow endpoint for the authenticated
  operator (`/api/booking-report/clients`), under `Cache-Control: no-store`, and
  never in the report body, which is PII-free so it can be quoted in a review.
  Diagnostic logs get none of it.
* Everything the page renders from stored data is written as text, not markup.
* No route here changes local state, so there is nothing for a CSRF token to
  protect; the write endpoints of the mailing keep their CSRF and same-origin
  guards unchanged.

## 10. Tests

Required by default, no markers, no skips:

* `src/altegio_bot/tests/test_easyweek_voucher_mailing_booking_report.py`
* the booking-report group in
  `src/altegio_bot/tests/test_easyweek_voucher_mailing_browser.py`, run by the
  browser acceptance gate
* shared fixtures: `src/altegio_bot/tests/easyweek_booking_report_fixtures.py`
