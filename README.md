# Altegio Bot

WhatsApp bot for integration with Altegio CRM. Automates sending notifications to clients about appointments, reminders, and newsletters.

## Main Features

- 📅 Automatic notifications for appointment creation/modification/cancellation
- ⏰ Reminders 24 hours and 2 hours before the visit
- 💳 Loyalty card issuance via Altegio API
- 📧 Monthly newsletters for new clients
- 📊 Ops cabinet for message monitoring

## Architecture

- **API**: FastAPI (webhooks from Altegio and WhatsApp)
- **DB**: PostgreSQL + SQLAlchemy
- **Queue**: Asynchronous worker for sending messages
- **WhatsApp**: Meta Business API (official templates)

## Installation and Launch

### Environment Variables

Create a `.env` file:

```bash
# Database
DATABASE_URL=postgresql+asyncpg://user:password@localhost/altegio_bot

# Altegio API
ALTEGIO_PARTNER_TOKEN=your_partner_token
ALTEGIO_USER_TOKEN=your_user_token
ALTEGIO_WEBHOOK_SECRET=your_webhook_secret
ALTEGIO_API_BASE_URL=https://api.alteg.io/api/v1

# WhatsApp Meta Business
WHATSAPP_ACCESS_TOKEN=your_meta_access_token
META_WA_PHONE_NUMBER_ID=your_phone_number_id
META_WABA_ID=your_waba_id
META_APP_SECRET=your_app_secret
WHATSAPP_WEBHOOK_VERIFY_TOKEN=your_verify_token

# Loyalty cards
LOYALTY_CARD_TYPE_ID=46454

# Ops-cabinet
OPS_USER=admin
OPS_PASS=your_secure_password
OPS_SECRET=your_jwt_secret

# Settings
ALLOW_REAL_SEND=true
WHATSAPP_SEND_MODE=template
WA_OPTOUT_POLICY=marketing_only
```

### Docker Compose

```bash
docker compose up -d
```

### Migrations

Local check (no `.env` required):
```bash
uv run alembic heads      # must show exactly one head
```

Via Docker (production-style, reads `.env`):
```bash
docker compose up -d postgres
docker compose --profile ops run --rm migrate
```

## Testing

### 🧪 Loyalty Card Creation Testing

The project includes a special script for comprehensive testing of loyalty card issuance and message sending functionality.

#### Basic Test (with automatic card deletion)

```bash
docker exec -i altegio-api sh -lc '
set -a
. /app/.env
set +a

/app/.venv/bin/python -m altegio_bot.scripts.run_test_newsletter_smart \
  --phone 381638400431 \
  --company-id 758285 \
  --booking-link https://n813709.alteg.io/ \
  --template kitilash_ka_newsletter_new_clients_monthly_v1 \
  --expect-status delivered \
  --timeout 180 \
  --cleanup \
  --card-type-id 46454
'```

**`--cleanup` flag**: Automatically deletes the test card after the test is completed. Use it for "clean" tests that leave no traces in the CRM.

#### Test with card preservation in CRM

If you need to leave the card in the Altegio system (e.g., for manual verification in the CRM):

```bash
docker exec -i altegio-api sh -lc '
set -a
. /app/.env
set +a

/app/.venv/bin/python -m altegio_bot.scripts.run_test_newsletter_smart \
  --phone 381638400431 \
  --company-id 758285 \
  --booking-link https://n813709.alteg.io/ \
  --template kitilash_ka_newsletter_new_clients_monthly_v1 \
  --expect-status delivered \
  --timeout 180 \
  --card-type-id 46454
'```

⚠️ **Without the `--cleanup` flag, the card will remain in Altegio CRM!**

#### What does the test do?

1. ✅ **Checks Meta template**: Ensures the template exists and has `APPROVED` status in WhatsApp Business API.
2. ✅ **Checks idempotency**: Skips the test if it has already been successfully performed in the last 24 hours (can be bypassed with the `--force` flag).
3. ✅ **Creates loyalty card**: Issues a test card via Altegio API with prefix `99` (test cards).
4. ✅ **Sends WhatsApp message**: Uses the official Meta template with the card number.
5. ✅ **Tracks delivery**: Waits for webhook events from WhatsApp (`sent` → `delivered` → `read`).
6. ✅ **Optionally deletes card**: If `--cleanup` is specified, deletes the test card from Altegio.

#### Command Parameters

| Parameter | Description | Required | Example |
|-----------|-------------|----------|---------|
| `--phone` | Recipient's phone number (with country code) | ✅ | `381638400431` or `+381638400431` |
| `--company-id` | Company ID in Altegio | ✅ | `758285` |
| `--card-type-id` | Loyalty card type ID | ✅ | `46454` |
| `--booking-link` | Online booking link | | `https://n813709.alteg.io/` |
| `--template` | Meta template name | | `kitilash_ka_newsletter_new_clients_monthly_v1` |
| `--expect-status` | Expected delivery status | | `delivered` (default) or `sent` |
| `--timeout` | Webhook wait timeout (seconds) | | `180` (default) |
| `--cleanup` | Delete card after test | | flag without value |
| `--force` | Ignore cache (run again) | | flag without value |
| `--location-id` | Location ID (if different from company-id) | | `758285` |

#### Card Number Formats

- **Test cards**: Prefix `99` + `YYMMDD` + 8 random digits = 16 characters
  - Example: `9926022832320706` (test from 2026-02-28)
  
- **Production cards**: Prefix `00` + `YYMMDD` + 8 random digits = 16 characters
  - Example: `0026022845678901` (production from 2026-02-28)
  - Used in real newsletters (`run_monthly_newsletter_smart.py --mode send-real`)

#### Successful Test Logs

```
INFO smart_test: Smart test START phone=+381638400431 company=758285
INFO smart_test: Template 'kitilash_ka_newsletter_new_clients_monthly_v1' APPROVED
INFO smart_test: Issuing loyalty card number=9926022832320706 type_id=46454 phone=381638400431
INFO smart_test: Card issued: id=47719627 number=9926022832320706
INFO smart_test: Message sent: provider_message_id=wamid.HBg...
INFO smart_test: Poll complete: outcome=pass statuses_seen=['sent', 'delivered']
INFO smart_test: Card deleted: card_id=47719627
INFO smart_test: === RESULT: PASS ===
```

#### Exit Codes

- `0` = PASS (test successful)
- `2` = FAIL (test failed or error)

### 🧪 Unit Tests

Run all tests:

```bash
docker exec -i altegio-api pytest
```

Tests with coverage:

```bash
docker exec -i altegio-api pytest --cov=altegio_bot --cov-report=html
```

Loyalty card client tests:

```bash
docker exec -i altegio-api pytest src/altegio_bot/tests/test_altegio_loyalty.py -v
```

### 📋 Manual Card Creation via Python

If you need to create a card programmatically without sending a message:

```python
import asyncio
from altegio_bot.altegio_loyalty import AltegioLoyaltyClient

async def create_card():
    loyalty = AltegioLoyaltyClient()
    try:
        card = await loyalty.issue_card(
            location_id=758285,
            loyalty_card_number='9926022899999999',  # Unique 16-digit number
            loyalty_card_type_id='46454',
            phone=381638400431,  # Number WITHOUT + sign
        )
        print(f"Card created: {card}")
        print(f"Card ID: {card.get('id')}")
        print(f"Number: {card.get('loyalty_card_number')}")
    finally:
        await loyalty.aclose()

asyncio.run(create_card())
```

### 📧 Monthly Newsletter Testing

#### `list` mode (view candidates)

```bash
docker exec -i altegio-api /app/.venv/bin/python \
  -m altegio_bot.scripts.run_monthly_newsletter_smart \
  --mode list \
  --company-id 758285
```

#### `dry-run` mode (simulation without sending)

```bash
docker exec -i altegio-api /app/.venv/bin/python \
  -m altegio_bot.scripts.run_monthly_newsletter_smart \
  --mode dry-run \
  --company-id 758285 \
  --booking-link https://n813709.alteg.io/
```

#### `send-test` mode (sending to one test recipient)

```bash
docker exec -i altegio-api /app/.venv/bin/python \
  -m altegio_bot.scripts.run_monthly_newsletter_smart \
  --mode send-test \
  --company-id 758285 \
  --test-phone 381638400431 \
  --booking-link https://n813709.alteg.io/ \
  --cleanup
```

#### `send-real` mode (production newsletter)

⚠️ **WARNING**: This mode creates real cards and sends real messages!

```bash
docker exec -i altegio-api /app/.venv/bin/python \
  -m altegio_bot.scripts.run_monthly_newsletter_smart \
  --mode send-real \
  --company-id 758285 \
  --booking-link https://n813709.alteg.io/ \
  --limit 10  # Limit to the first 10 recipients
```

### 🗑️ Manual cleanup of expired promo loyalty cards

Deletes Altegio loyalty cards for expired promo leads (`status='issued'`, `expires_at <= now`).
Only touches cards created by the promo funnel (`meta.loyalty_card_issued=True`).
Leads with `status='booked'` or `'applied'` are intentionally excluded — their card deletion
policy is out of scope for this script.
The script is **idempotent** — rows with `meta.promo_card_deleted_at` already set are excluded by the SQL query.

**Local:**
```bash
uv run python -m altegio_bot.scripts.cleanup_expired_promo_cards
```

**Docker:**
```bash
docker compose exec -T altegio-api python -m altegio_bot.scripts.cleanup_expired_promo_cards
```

Exit codes: `0` — all eligible cards deleted (or nothing to do); `1` — one or more deletions failed.

> **Note:** Scheduler/cron integration is intentionally out of scope — run manually or wire to your own job scheduler.

## Project Structure

```
altegio_bot/
├── src/altegio_bot/
│   ├── altegio_loyalty.py       # Altegio Loyalty API client
│   ├── meta_templates.py        # WhatsApp template mapping
│   ├── message_planner.py       # Message sending planner
│   ├── models/
│   │   └── models.py            # SQLAlchemy models
│   ├── ops/
│   │   ├── router.py            # Ops cabinet (monitoring)
│   │   └── auth.py              # Authentication
│   ├── scripts/
│   │   ├── run_test_newsletter_smart.py      # 🧪 Test newsletter
│   │   ├── run_monthly_newsletter_smart.py   # 📧 Production newsletter
│   │   └── seed_templates.py                 # Message templates seed
│   ├── workers/
│   │   └── outbox_worker.py     # Message sending worker
│   ├── webhooks/
│   │   └── whatsapp.py          # WhatsApp webhook handler
│   └── tests/                   # Unit tests
├── alembic/                     # DB migrations
├── docker-compose.yml
├── Dockerfile
└── README.md
```

## Monitoring and Debugging

### Ops Cabinet

Open in browser: `http://localhost:8000/ops/login`

- **Monitoring**: Message statistics for 24h, failed tasks
- **Outbox**: Outgoing message queue
- **Events**: WhatsApp webhook events
- **Opt-outs**: Clients who opted out of newsletters
- **Campaign Runs**: Newsletter history

### Logs

```bash
# API logs
docker logs -f altegio-api

# Worker logs
docker logs -f altegio-worker
```

### Health Check

```bash
curl http://localhost:8000/health
```

## API Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/webhooks/altegio` | Altegio webhook (appointments) |
| `POST` | `/webhooks/whatsapp` | WhatsApp webhook (delivery statuses) |
| `GET` | `/health` | Health check |
| `GET` | `/ops/monitoring` | Monitoring dashboard (requires auth) |
| `GET` | `/ops/outbox` | Message queue (requires auth) |

## Troubleshooting

### Card created but not visible in CRM

✅ **This is normal** if you used the `--cleanup` flag. The card is created and immediately deleted after the test.

🔧 **Solution**: Run the test **without** `--cleanup` so the card remains in the system.

### Message not delivered

1. Check if the phone is registered in WhatsApp.
2. Ensure the Meta template has `APPROVED` status.
3. Check sending limits in WhatsApp Business API.

### Error "No loyalty card types found"

Ensure that:
1. Loyalty card types are configured in Altegio.
2. The `location_id` is correct (usually equals `company_id`).
3. API tokens have permissions to work with cards.

## License

Proprietary project.

## Chatwoot Integration

### Overview

The bot supports an optional **dual-write hybrid strategy** that mirrors messages
to a self-hosted [Chatwoot](https://www.chatwoot.com/) instance. This lets salon
administrators see full conversation history and respond manually in the
Chatwoot UI, while the automated bot continues to work normally.

```
Outgoing (Bot → Customer):
  MessageJob → outbox_worker → ChatwootHybridProvider
    ├→ [PRIMARY]   MetaCloudProvider.send()  → Meta API → Customer ✅
    └→ [SECONDARY] ChatwootClient.send_message() → Chatwoot API (async)
         ↳ If fails → log warning, continue

Incoming (Customer → Bot via Chatwoot):
  Customer → Meta → Chatwoot (Meta webhooks point here)
    ↓
  Chatwoot UI (admins see message)
    ↓
  Chatwoot webhook → /webhook/chatwoot
    ↓
  WhatsAppEvent (DB)   [chatwoot_conversation_id is set]
    ↓
  whatsapp_inbox_worker
    ↓
  If START/STOP → auto-reply via ChatwootHybridProvider
  Else → do nothing (admin replies manually)
```

### Configuration

Add to your `.env`:

```bash
# Enable Chatwoot integration
CHATWOOT_ENABLED=true
CHATWOOT_BASE_URL=https://chatwoot.kitilash.com
CHATWOOT_API_TOKEN=your_chatwoot_api_token
CHATWOOT_ACCOUNT_ID=1
CHATWOOT_INBOX_ID=1

# Optional: verify webhook signatures from Chatwoot
CHATWOOT_WEBHOOK_SECRET=your_shared_secret

# Switch the provider to dual-write mode
WHATSAPP_PROVIDER=chatwoot_hybrid
```

When `CHATWOOT_ENABLED=false` (default) the bot works exactly as before —
webhooks come directly from Meta to `/webhook/whatsapp`.

### Migrate Existing Contacts

```bash
docker exec -i altegio-api sh -lc '
set -a
. /app/.env
set +a
/app/.venv/bin/python -m altegio_bot.scripts.migrate_contacts_to_chatwoot
'
```

### DB Migration

```bash
docker compose --profile ops run --rm migrate
```

This adds the `chatwoot_conversation_id` column to `whatsapp_events`.

### Backward Compatibility

| Setting | Behaviour |
|---------|-----------|
| `CHATWOOT_ENABLED=false` (default) | Exactly as before — Meta direct |
| `CHATWOOT_ENABLED=true` + `WHATSAPP_PROVIDER=chatwoot_hybrid` | Dual-write enabled |

## MVP-3: controlled promo discount apply rollout

MVP-2 is already enough to issue the discount right: a secret word can create a
new-client `PromoLead.status='issued'` and issue/store the Altegio loyalty card.
It does **not** safely apply the discount to a visit by itself.

MVP-3 adds a controlled automatic apply path after an Altegio **record create**
webhook. It is disabled by default and must stay disabled on normal production
traffic until the apply endpoint, payload, booking timestamp source, and customer
notification flow have been smoke-tested on an owner-controlled test number.

**Explicit warning:** do not test this on real clients. Use only test numbers
owned by the business/operator. Before a smoke test, clean up only test rows
scoped by `phone_e164` and `campaign_name`; do not run broad promo cleanup SQL.

### How it works

1. Client sends secret word via WhatsApp -> `PromoLead` created, loyalty card issued.
2. Client books via online booking URL.
3. Altegio sends a **record create** webhook.
4. `inbox_worker` processes the webhook and calls `try_apply_promo_discount`.
5. The function matches the booking to the client's active `PromoLead` by company_id
   and phone number.
6. `rejected_not_new`, `pending_check`, `applied`, expired, missing-card, and
   wrong-company leads are ignored.
7. The booking creation timestamp must be after `PromoLead.issued_at`.
8. The record must contain at least one service from `PROMO_ALLOWED_SERVICE_IDS`.
9. If both apply gates are enabled, the discount program is applied via Altegio API.
10. `PromoLead.status` advances: `issued -> booked -> applied`.
11. A `MessageJob` is queued; `outbox_worker` sends the client a German WhatsApp confirmation.

The apply HTTP call is:

```text
POST /visit/loyalty/apply_discount_program/{location_id}/{card_id}/{program_id}
body: {"record_id": <altegio_record_id>}
```

`location_id`, `card_id`, and `program_id` come from the issued `PromoLead`;
`record_id` is the external `Record.altegio_record_id` from the webhook-synced
booking. The response is treated as success only when the JSON body is an object
with `success: true`. `success: false`, missing `success`, invalid JSON, and HTTP
or network errors all fail closed: `PromoLead.status='apply_failed'`, error
metadata is stored, and no customer "discount applied" notification is queued.

**Update webhooks are intentionally ignored.** Only create webhooks trigger promo
discount apply, to avoid accidentally applying a promo to a booking that was made
before the promo was issued.

**Booking created timestamp guard:** automatic apply requires a confirmed
`booking_created_at` - the actual time the booking was created in Altegio, not
the time the webhook was received by the bot. The lookup is lazy: `inbox_worker`
passes a resolver into `try_apply_promo_discount`, and that resolver is called
only after local candidate checks pass. Those checks are: apply feature enabled,
record has a local client with phone, matching active `issued`/`booked`
`PromoLead` exists, the service allowlist is configured and intersects the
record services, and the local prior-attended-visit guard passes.

If those cheap checks pass, the resolver first reads trusted creation fields
from the record create webhook payload (`create_date`, `created_at`,
`datetime_created`), then falls back to read-only
`GET /record/{location_id}/{record_id}` and the same trusted fields from the
Altegio record details response. `location_id` is taken from the payload when
present, otherwise from `PROMO_LOCATION_ID_BY_COMPANY`. If the cheap checks do
not pass, `GET /record` is not called.

After the resolver returns, `try_apply_promo_discount` re-reads the same
candidate `PromoLead` (`lead.id` must match) with `SELECT FOR UPDATE` and
refreshes the ORM row before checking the timestamp or calling the external
apply endpoint. It also re-runs the mutable local guards after the resolver:
service allowlist intersection and the local prior-attended-visit check. If
another worker changed the lead while `GET /record` was in flight (for example
bound it to another booking, marked it `applied`, deleted the promo card, synced
a prior attended visit, or changed the record services), the apply attempt stops
without calling Altegio apply and without a customer notification.

If the timestamp cannot be confirmed, the resolver returns `None` and
`try_apply_promo_discount` skips the apply with
`PromoLead.meta.apply_skip_reason = 'missing booking created timestamp'`.

`event.received_at` is an audit timestamp that records when our bot received the
webhook. It is never used as the booking creation time: a delayed or
backfilled create webhook for a booking that predates the promo could arrive
after `PromoLead.issued_at`, making `received_at >= issued_at` true while the
booking itself predates the promo.

**Booked-lead rebinding guard:** a `PromoLead` with `status='booked'` is only
eligible for retry against the same stored record (`lead.record_id == record.id`
or `lead.altegio_record_id == record.altegio_record_id`). A booked lead bound to
a different booking is silently skipped so the original attribution is never
overwritten.

**Customer notification:** after a successful apply, a `MessageJob` with
`job_type='promo_discount_applied'` is queued for immediate delivery. The
`outbox_worker` sends a free-form German WhatsApp message confirming the discount
to the client. `MessageJob.dedupe_key` prevents duplicate jobs on webhook retries;
concurrent inserts are protected via savepoint (`begin_nested`) with
`IntegrityError` recovery. Delivery status is reconciled in `PromoLead.meta`:

| Event | `customer_notification` |
|---|---|
| MessageJob created | `queued` |
| outbox_worker: sent | `sent` |
| outbox_worker: final failure | `failed` |
| outbox_worker: no active sender | `failed` |
| outbox_worker: missing body | `failed` |
| outbox_worker: retryable failure | `queued` (+ `customer_notification_last_error`) |

Note: the online booking form and the first confirmation email may still show
regular prices. The discount is visible to staff in the Altegio CRM.

**Old-client referral reply:** clients who are not eligible as Neukunden receive
a soft rejection reply with a plain `https://wa.me/?text=...` share link. The
shared text tells a friend to send the promo keyword directly from her own
WhatsApp number, so the discount can be linked to her booking correctly.
The share text uses the currently matched promo keyword and current promo
discount settings, not stored audit values from an older rejected lead.

**Async promo eligibility check:** by default
`PROMO_ASYNC_ELIGIBILITY_CHECK_ENABLED=false`, so the current immediate MVP-1
flow stays unchanged. When enabled, a first promo keyword with no existing lead
creates `PromoLead.status='pending_check'`, sends an immediate checking reply,
and queues `MessageJob.job_type='promo_eligibility_check'`. The background
worker makes the final eligibility decision and then sends the issued/card,
soft-rejection, or manual-check reply. The interim reply does not promise a
discount or issue a loyalty card. The initial checking reply is still sent
synchronously from the webhook handler; only the final eligibility decision is
handled by `MessageJob`.

`pending_check` remains pending until the final reply is successfully sent. If
the final send fails, the job is retried according to the normal send-attempt
budget and the customer still has a pending promo check. If a loyalty card is
issued but that final card reply fails, the card fields stay on the pending lead,
`PromoLead.meta.promo_check_card_reply_pending=true`, and retries reuse the
stored card number instead of issuing a second card.

When local prior-visit history already proves the client is not new, the async
job skips the external Altegio lookup and records
`PromoLead.meta.altegio_new_client_check='skipped_local_rejection'` after the
soft-rejection reply is sent.

External Altegio history lookup is still controlled separately by
`PROMO_CHECK_NEW_CLIENT_IN_ALTEGIO`. Enable it only after the
`clients/visits/search` endpoint has been smoke-tested for the target
`PROMO_LOCATION_ID_BY_COMPANY` mapping.

**New-client eligibility check:** by default the promo funnel keeps the existing
local-only check for prior attended visits and makes no extra Altegio API call.
Set `PROMO_CHECK_NEW_CLIENT_IN_ALTEGIO=true` to run an external Altegio history
check before issuing a new promo lead or loyalty card.

When enabled, the funnel calls the documented phone-based history endpoint
`POST /company/{location_id}/clients/visits/search` with `client_phone`.
`location_id` is resolved from `PROMO_LOCATION_ID_BY_COMPANY`; `company_id` is
not used as the path parameter. Missing or invalid mapping fails closed before
any Altegio API call. Any returned visit/record makes the client not eligible for this promo:
`PromoLead.status='rejected_not_new'`, no loyalty card is issued, and the client
receives the soft Neukunden rejection reply. The request sends only
`client_phone` and `payment_statuses=[]`, with no explicit `null` filters, so
cancelled, no-show, waiting, confirmed, attended, paid, and unpaid records count
when Altegio returns them.

If the external check fails, the funnel fails closed: no discount promise is
sent, no loyalty card is issued, and the `PromoLead.meta` stores
`altegio_new_client_check_error` for manual follow-up. The customer receives a
neutral manual-check reply. There is no automatic retry for
`altegio_new_client_check_failed`; ops/manual follow-up is required until a
future retry or manual reset flow exists.

For newly issued leads where the external check is disabled,
`PromoLead.meta.altegio_new_client_check = 'disabled'`. When enabled and no
records are found, the value is `no_records`.

Deleted-record semantics depend on Altegio API behaviour. If this endpoint
returns explicitly deleted records, the promo check treats them as evidence that
the client is not new. If Altegio omits deleted records from this endpoint, that
case remains invisible to the bot and should be verified with a real API smoke
test before changing business policy.

Existing active `issued` / `booked` / `applied` leads are not revoked by this
check. A repeat secret word for an already issued lead only resends the existing
active/card reply; retroactive cleanup is out of scope.

**Out of scope for the current implementation:**
- Retry worker for `apply_failed` leads
- Customer notification on apply failure
- Meta paid templates for promo notification
- Automatic retry/manual reset for `altegio_new_client_check_failed`
- Moving the initial checking reply into Outbox
- Cleanup or Ops reset for stuck old `pending_check` leads
- Retry flow for cancelled technical eligibility checks
- Changing existing issued leads retroactively
- Enabling production flags without a completed smoke test

**The discount-apply Altegio endpoint is UNCONFIRMED** (source: developer
discussion, not OpenAPI spec). Both discount-apply feature gates must be
explicitly enabled after verification.

### Required environment variables

```bash
# Promo funnel must already be enabled for MVP-2 issuance.
PROMO_LEAD_FUNNEL_ENABLED=true

# Optional async promo eligibility flow.
# Default false keeps the immediate MVP-1 promo reply flow.
# If true, a pending_check PromoLead is created and MessageJob handles the final reply.
PROMO_ASYNC_ELIGIBILITY_CHECK_ENABLED=false

# Optional new-client CRM history check for WhatsApp promo leads.
# Default false keeps local-only behaviour and makes no Altegio API call.
# If true, PROMO_LOCATION_ID_BY_COMPANY must map company_id to Altegio location_id.
PROMO_CHECK_NEW_CLIENT_IN_ALTEGIO=false

# Required when PROMO_CHECK_NEW_CLIENT_IN_ALTEGIO=true.
# JSON mapping company_id (str) → Altegio location_id (int).
PROMO_LOCATION_ID_BY_COMPANY={"1":9001}

# Master gate. Default false: automatic apply is impossible.
PROMO_APPLY_DISCOUNT_ENABLED=false

# Endpoint verification gate — set True only after confirming the
# POST /visit/loyalty/apply_discount_program/{location_id}/{card_id}/{program_id}
# endpoint against Altegio API docs and completing a smoke test
PROMO_APPLY_DISCOUNT_API_VERIFIED=false

# Comma-separated Altegio service IDs eligible for the promo discount.
# If empty, discount is never applied automatically (fail-closed).
PROMO_ALLOWED_SERVICE_IDS=12345,67890
```

Production automatic apply is impossible unless both of these are explicitly
enabled together:

```bash
PROMO_APPLY_DISCOUNT_ENABLED=true
PROMO_APPLY_DISCOUNT_API_VERIFIED=true
```

Do not enable them immediately for production traffic. The endpoint is still
marked unconfirmed in code, applying a discount changes Altegio CRM state, a
successful apply triggers a customer WhatsApp notification, and the automatic
webhook path stays fail-closed whenever a confirmed booking-created timestamp
cannot be resolved from the webhook payload or read-only `GET /record`.

### Controlled smoke plan

1. Use an owner-controlled test WhatsApp number and the target campaign, for
   example `PROMO_CAMPAIGN_NAME=sommer_2026`. Do not use a real client.
2. If cleanup is needed, first inspect and then delete only test rows scoped by
   `phone_e164` and `campaign_name`. Clean related `message_jobs` and
   `outbox_messages` only through the test `PromoLead` ids / test phone.
3. Send the secret word from the test number and confirm a new-client
   `PromoLead.status='issued'` exists with `loyalty_card_id`, `location_id`,
   and `discount_program_id`.
4. Create the test booking in Altegio using an allowed service id.
5. Find a candidate:

```bash
docker compose exec -T altegio-api python -m altegio_bot.scripts.find_promo_discount_smoke_candidate \
  --campaign-name sommer_2026 --phone +49...
```

6. Run the printed dry-run command. It must not call Altegio.
7. Manually verify in Altegio that the booking belongs to the test number, was
   created after `PromoLead.issued_at`, and uses an allowed service. The
   production resolver checks `create_date`/`created_at`/`datetime_created`
   lazily from the webhook payload or read-only `GET /record` only after the
   booking has passed local promo candidate checks.
8. Run the real single-record smoke only with explicit API verification:

```bash
PROMO_APPLY_DISCOUNT_API_VERIFIED=true \
uv run python -m altegio_bot.scripts.smoke_apply_promo_discount \
  --location-id 123 \
  --card-id 456 \
  --program-id 789 \
  --record-id 111 \
  --yes-apply
```

9. Verify `PromoLead`:

```sql
SELECT id, status, applied_at, record_id, altegio_record_id, meta
FROM promo_leads
WHERE phone_e164 = '+49...' AND campaign_name = 'sommer_2026'
ORDER BY id DESC;
```

10. Verify the notification job:

```sql
SELECT id, status, dedupe_key, payload, last_error
FROM message_jobs
WHERE job_type = 'promo_discount_applied'
  AND payload->>'phone_e164' = '+49...'
ORDER BY id DESC;
```

11. Verify the outbound message:

```sql
SELECT id, status, provider_message_id, error, sent_at
FROM outbox_messages
WHERE phone_e164 = '+49...'
ORDER BY id DESC
LIMIT 20;
```

12. To disable automatic apply again, set:

```bash
PROMO_APPLY_DISCOUNT_ENABLED=false
PROMO_APPLY_DISCOUNT_API_VERIFIED=false
```

### Lifecycle after cleanup

The cleanup script (`scripts/cleanup_expired_promo_cards.py`) only processes
`status='issued'` leads that expired without booking. Leads with status
`booked`, `applied`, or `used` are intentionally excluded — their card
lifecycle is managed separately.

### Manual smoke test for promo new-client check

`scripts/smoke_promo_new_client_check.py` is a **read-only** script for manually
verifying the `POST /company/{location_id}/clients/visits/search` endpoint before
enabling `PROMO_CHECK_NEW_CLIENT_IN_ALTEGIO=true` in production.

**The script is read-only:** it makes no DB writes, creates no `PromoLead`,
sends no WhatsApp message, issues no loyalty card, and does not modify any
Altegio records. It only calls the Altegio visit-search endpoint and reports
the boolean result.

**This script does not enable `PROMO_CHECK_NEW_CLIENT_IN_ALTEGIO`** and does
not affect the automatic promo funnel in any way.

#### Run locally

```bash
uv run python -m altegio_bot.scripts.smoke_promo_new_client_check \
  --location-id 9001 \
  --phone +491234567890
```

#### Run on server

```bash
docker compose exec -T altegio-api \
  python -m altegio_bot.scripts.smoke_promo_new_client_check \
  --location-id 9001 \
  --phone +491234567890
```

#### Expected output

```
Promo new-client check smoke test
location_id=9001
phone=+491234567890
has_any_altegio_record=false   # or true
```

Exit code `0` for both found and not-found results; exit code `1` on API error.

#### Before enabling `PROMO_CHECK_NEW_CLIENT_IN_ALTEGIO=true`

Verify all of the following with a real API call:

1. A phone with **no prior visits** returns `has_any_altegio_record=false`.
2. A phone with **at least one prior visit** (any status: cancelled, no-show,
   attended, waiting) returns `has_any_altegio_record=true`.
3. The `--location-id` matches the value in `PROMO_LOCATION_ID_BY_COMPANY` for
   the target company.
4. No `AltegioNewClientCheckError` is reported (exit code 0).

The script reports a boolean result only. To inspect the raw response shape,
use the Altegio developer console or a tool such as `curl` / Postman against
the same endpoint with the same authorization header.

### Finding a promo discount smoke-test candidate

`scripts/find_promo_discount_smoke_candidate.py` is a **read-only** helper that
queries the local DB for a PromoLead with all required IDs and a linked booking,
then prints the dry-run command for `smoke_apply_promo_discount.py`.

The script makes **no Altegio API calls**, applies **no discount**, and writes
**nothing** to the database. It does not require `PROMO_APPLY_DISCOUNT_ENABLED`
or `PROMO_APPLY_DISCOUNT_API_VERIFIED` to be set.

```bash
# Find candidates (all companies)
docker compose exec -T altegio-api python -m altegio_bot.scripts.find_promo_discount_smoke_candidate

# Filter by company or phone
docker compose exec -T altegio-api python -m altegio_bot.scripts.find_promo_discount_smoke_candidate \
  --company-id 1 --phone +49...
```

**The `--yes-apply` command is intentionally not printed.** The local `Record`
table has no trusted booking-created column, so this helper cannot prove that a
booking was created *after* the promo lead was issued. Use Altegio UI or the
read-only `GET /record` research helper to confirm the booking creation time
before any real smoke apply. A booking that predates the promo would receive an
unintended discount.

Before constructing a `--yes-apply` command, manually verify in Altegio:
1. The booking belongs to the promo client.
2. The booking was created *after* the promo lead `issued_at`.
3. The booked service is eligible for the promo (the helper prints allowlist diagnostics).

The output includes:
- the dry-run command (safe — no API call);
- a service allowlist diagnostic (`allowed_service_match=yes/no/not_configured`);
- an explanation of why the real apply command is omitted.

### Manual research for booking_created_at

`scripts/research_booking_created_at.py` is a **read-only** helper for checking
which timestamp fields Altegio returns for one appointment via
`GET /record/{location_id}/{record_id}`. It exists because automatic promo
apply must compare `PromoLead.issued_at` with the actual time the client created
the booking in Altegio.

Do **not** use `event.received_at` as `booking_created_at`: it is only the time
our bot received the webhook. A delayed or backfilled webhook can arrive after a
promo lead was issued even when the booking itself was created earlier.

The script makes no DB writes, creates no `PromoLead`, sends no WhatsApp
message, issues no loyalty card, applies no discount, and does not modify any
Altegio record.

#### Run locally

```bash
uv run --env-file .env python -m altegio_bot.scripts.research_booking_created_at \
  --location-id 9001 \
  --record-id 123456789
```

#### Run in Docker

```bash
docker compose exec -T altegio-api \
  python -m altegio_bot.scripts.research_booking_created_at \
  --location-id 9001 \
  --record-id 123456789
```

#### Interpreting output

The script prints a sanitized timestamp summary:
- `date` / `datetime` are appointment start fields, not booking creation time.
- `last_change_date` / `last_change_at` are last-change fields and are not
  reliable as creation time.
- `created_at`, `create_date`, and `datetime_created` are trusted booking
  creation fields when present and parseable.

`confirmed_booking_created_at=<none>` and `safe_for_auto_apply=false` mean the
automatic promo apply guard stays fail-closed for that record. The production
webhook path applies the same rule: no confirmed creation timestamp means no
automatic discount apply.

### Manual smoke test for promo discount application

The endpoint (`POST /visit/loyalty/apply_discount_program/…`) is marked as
**UNCONFIRMED** — it must be verified against real Altegio data before enabling
the automatic webhook flow.

Use `scripts/smoke_apply_promo_discount.py` to test the endpoint on a single
known visit without touching any production booking logic.

**The script is dry-run by default — no API call is made without `--yes-apply`.**

#### Before running with `--yes-apply`

Set in environment:

```bash
PROMO_APPLY_DISCOUNT_API_VERIFIED=true
```

`PROMO_APPLY_DISCOUNT_ENABLED` is **not** required for the manual smoke script
and does not affect the automatic webhook flow.

#### Run locally (dry-run)

```bash
uv run python -m altegio_bot.scripts.smoke_apply_promo_discount \
  --location-id 123 \
  --card-id 456 \
  --program-id 789 \
  --record-id 111
```

#### Run on server (dry-run)

```bash
docker compose exec -T altegio-api python -m altegio_bot.scripts.smoke_apply_promo_discount \
  --location-id 123 \
  --card-id 456 \
  --program-id 789 \
  --record-id 111
```

#### Real API call (requires `--yes-apply`)

```bash
PROMO_APPLY_DISCOUNT_API_VERIFIED=true \
uv run python -m altegio_bot.scripts.smoke_apply_promo_discount \
  --location-id 123 \
  --card-id 456 \
  --program-id 789 \
  --record-id 111 \
  --yes-apply
```

#### After a successful smoke test

Once the endpoint shape is confirmed, enable the automatic webhook flow as a
separate decision:

```bash
PROMO_APPLY_DISCOUNT_ENABLED=true
PROMO_APPLY_DISCOUNT_API_VERIFIED=true
PROMO_ALLOWED_SERVICE_IDS=12345,67890
```
