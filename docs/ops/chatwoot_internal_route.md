# Chatwoot internal Docker route — ops runbook

How to switch `altegio_bot` backend-to-backend Chatwoot API calls from the
public URL to an internal Docker route, safely and with a tested rollback.

## 1. Problem statement

All app containers currently call Chatwoot through the public URL
`https://chatwoot.kitilash.com`. Every API call goes through public
DNS, TLS handshake, the reverse proxy and a hairpin route back into the
same host, even though Chatwoot runs in Docker on the same server.

In the Irida project, moving the same kind of backend-to-backend calls to an
internal Docker route reduced average latency roughly 10×.

Measured `altegio_bot` baseline through the public route
(15× `contacts/search` requests):

```text
count=15
avg=0.146s
median=0.113s
min=0.059s
max=0.456s
```

## 2. Current production topology

- `altegio_bot` compose project network: `altegio_bot_default`
- Chatwoot compose project network: `chatwoot_default`
- Chatwoot containers: `chatwoot_rails_1`, `chatwoot_sidekiq_1`,
  `chatwoot_postgres_1`, `chatwoot_redis_1`
- The two projects do **not** share a network yet: from app containers
  `chatwoot.kitilash.com` resolves (public IP), while `cw-web`, `chatwoot`,
  `chatwoot-web`, `cw-rails` all fail with `gaierror`.

So an internal `CHATWOOT_BASE_URL` cannot work until the app services are
attached to the Chatwoot network. The actual resolvable internal hostname
(`chatwoot_rails_1`, `rails`, or a compose alias) must be verified from
inside a worker container after attachment — do not assume it.

## 3. Safety rule

**Never change `.env` to an internal route before the DNS probe (step 6) and
the HTTP/API probe (step 7) pass from inside the app containers.**

Also:

- Do not change Chatwoot's `FRONTEND_URL` — it is for browser/UI traffic and
  webhook link generation, not for our API calls.
- Do not touch Chatwoot containers, database or volumes at any step.
- The public URL `https://chatwoot.kitilash.com` always remains a valid
  fallback.

## 4. Enable the optional override

The override file `docker-compose.chatwoot-internal.yml` attaches only the
app services (`altegio-api` and the four workers) to the external Chatwoot
network. It is opt-in: default `docker compose up` keeps working without it.

The external network name defaults to `chatwoot_default`. If the Chatwoot
project ever uses a different network name, set
`CHATWOOT_INTERNAL_NETWORK=<name>` in the shell or `.env` before running
compose.

Use an explicit project name and file list:

```bash
cd /opt/altegio_bot

COMPOSE="docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml"

$COMPOSE config >/tmp/altegio_compose_config.txt
$COMPOSE up -d --build \
  altegio-api \
  altegio-inbox-worker \
  altegio-outbox-worker \
  altegio-whatsapp-inbox-worker \
  altegio-campaign-worker
```

Note: from now on, every `up`/`restart` of these services must use the same
`-f docker-compose.yml -f docker-compose.chatwoot-internal.yml` file set,
otherwise compose will detach the services from the Chatwoot network again.

## 5. Verify shared network attachment

```bash
docker inspect altegio_bot-altegio-outbox-worker-1 \
  --format '{{json .NetworkSettings.Networks}}' | jq .
```

Expected: both networks present —

- `altegio_bot_default`
- `chatwoot_default`

## 6. DNS probe from an actual worker

```bash
$COMPOSE exec -T altegio-outbox-worker /app/.venv/bin/python -c '
import socket
for host in ["chatwoot_rails_1", "rails", "cw-web"]:
    try:
        print(host, socket.gethostbyname(host))
    except Exception as exc:
        print(host, type(exc).__name__, exc)
'
```

At least one candidate must resolve to a private Docker IP. Prefer a stable
compose service alias (e.g. `rails`) over the numbered container name
(`chatwoot_rails_1`) if both resolve — container names can change on
recreate, service aliases do not.

## 7. HTTP/API probe for each candidate host

Probe the current public URL first (sanity check), then each internal
candidate that resolved in step 6. Uses the real token from the container
env; the token is never printed. Replace `<known-test-phone-e164>` with a
known test phone in E.164 format — do not put real client numbers in docs
or shell history.

```bash
# Public baseline + each resolved candidate:
$COMPOSE exec -T altegio-outbox-worker /app/.venv/bin/python \
  -m altegio_bot.scripts.probe_chatwoot_latency \
  --query "+490000000000"

$COMPOSE exec -T altegio-outbox-worker /app/.venv/bin/python \
  -m altegio_bot.scripts.probe_chatwoot_latency \
  --base-url "http://rails:3000" \
  --forwarded-proto https \
  --requests 15 \
  --query "<known-test-phone-e164>"
```

A candidate passes when all requests return HTTP 200 and latency is below
the public baseline. If every internal candidate fails, stop here — the
public URL stays in place and nothing is broken.

## 8. Internal route requires X-Forwarded-Proto

Production diagnostics (June 2026) after attaching the workers to
`chatwoot_default`: internal DNS works (`rails` → 172.19.0.4,
`chatwoot_rails_1` → 172.19.0.4), but plain internal HTTP is redirected by
Rails because it enforces HTTPS:

```text
http://rails:3000 plain                                  -> 301
http://chatwoot_rails_1:3000 plain                       -> 301
http://rails:3000 + X-Forwarded-Proto: https             -> 200
http://chatwoot_rails_1:3000 + X-Forwarded-Proto: https  -> 200
```

A `Host` header is not needed — `X-Forwarded-Proto: https` alone is enough
for a 200. This is exactly what the reverse proxy adds on the public route.

`altegio_bot` supports this via the opt-in env var
`CHATWOOT_API_FORWARDED_PROTO`. When set to `https`, every outgoing Chatwoot
API request (and only Chatwoot API requests) carries
`X-Forwarded-Proto: https`. Empty (default) — no header, behaviour
unchanged. Invalid values are ignored with a warning. Chatwoot's own
`FRONTEND_URL` is not involved and must not be changed.

Recommended production env after verification:

```env
CHATWOOT_BASE_URL=http://rails:3000
CHATWOOT_API_FORWARDED_PROTO=https
```

But only after the probe in step 7 returns `200x15` for that exact
combination.

## 9. Update production `.env` (only after probes pass)

Manually edit `/opt/altegio_bot/.env`:

```env
CHATWOOT_BASE_URL=http://<verified-host>:3000
CHATWOOT_API_FORWARDED_PROTO=https
```

Then recreate only the app containers with the same compose file set:

```bash
$COMPOSE up -d \
  altegio-api \
  altegio-inbox-worker \
  altegio-outbox-worker \
  altegio-whatsapp-inbox-worker \
  altegio-campaign-worker
```

## 10. Runtime env verification

```bash
$COMPOSE exec -T altegio-outbox-worker sh -lc \
  'echo "CHATWOOT_BASE_URL=$CHATWOOT_BASE_URL"; echo "CHATWOOT_API_FORWARDED_PROTO=$CHATWOOT_API_FORWARDED_PROTO"'
```

Must print the new internal URL and `https`.

## 11. Latency before/after

Re-run the same 15-request probe from step 7 against the now-active runtime
env (no `--base-url` and no `--forwarded-proto` flags = both read from env)
and compare with the recorded public baseline (avg=0.146s, median=0.113s,
max=0.456s; re-measured June 2026: avg=0.053s, median=0.042s):

```bash
$COMPOSE exec -T altegio-outbox-worker /app/.venv/bin/python \
  -m altegio_bot.scripts.probe_chatwoot_latency \
  --requests 15 \
  --query "<known-test-phone-e164>"
```

The probe prints `forwarded_proto=https` when the env var is active.

## 12. Functional smoke tests

After the switch, verify end to end:

- inbound WhatsApp message appears in Chatwoot;
- operator message from Chatwoot reaches WhatsApp;
- native reply PR1 (Chatwoot reply context → WhatsApp) still works;
- native reply PR2 (WhatsApp reply context → Chatwoot) still works;
- Chatwoot private note / mirror flow still works;
- no tracebacks or errors in `altegio-api` and worker logs:

```bash
$COMPOSE logs --since 15m altegio-api altegio-outbox-worker \
  altegio-inbox-worker altegio-whatsapp-inbox-worker | grep -iE "traceback|error" | head
```

## 13. Rollback

Set the public URL back in `/opt/altegio_bot/.env` and clear the forwarded
proto (the header is harmless on the public route, but keep rollback exact):

```env
CHATWOOT_BASE_URL=https://chatwoot.kitilash.com
CHATWOOT_API_FORWARDED_PROTO=
```

Recreate only the app containers (same compose file set as in step 9).
Do not touch Chatwoot containers, database or volumes. If needed, the
network attachment itself can also be dropped by running `up -d` with only
`-f docker-compose.yml` — but that is not required for rollback; the public
URL works in both topologies.
