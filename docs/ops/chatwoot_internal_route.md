# Chatwoot internal Docker route — production runbook

Production uses the internal route. For an ordinary production recreation,
start with the shared setup in §4, use the env-only command in §9 if applicable,
and verify the result in §§5–6 and §§10–12. The initial build in §4 is a
separate operation. For a public-route rollback, follow §13.

## 1. Why use the internal route?

The public URL `https://chatwoot.kitilash.com` takes backend API traffic
through public DNS, TLS and the host reverse proxy back to Chatwoot on the
same server. On the internal route the app calls Rails over Docker networking.
Browser links and webhooks continue to use the public URL.

## 2. Production topology

The five services listed in §4 use both the app project network (normally
`altegio_bot_default`) and Chatwoot's external network (normally
`chatwoot_default`). The base file alone is for environments that do not
use the internal integration. Postgres, Redis and migration services do
not need to join the Chatwoot network.

The production Rails service alias verified on 2026-11-27 was `rails`,
port `3000`. Check the current Rails container and its network aliases when
selecting the endpoint. Container IPs can change on recreation.

## 3. Production access and scope

The operator runs these commands with authorization for the specific operation.
Use the deployment's existing Compose file set and already running dependencies.
Leave Chatwoot's own containers, volumes, database and `FRONTEND_URL` alone.
Before switching to an internal URL, pass §§5–7 from the affected app services.
Keep the public URL available for §13.

## 4. Shared setup and initial attachment

**Shared setup for every path.** Use Bash in the production checkout. Keep
the same shell for later sections. Start by reading the effective file lists
on the existing containers. This prints only names and file paths.

```bash
cd /opt/altegio_bot || exit 1
docker ps -a --filter label=com.docker.compose.project=altegio_bot \
  --format '{{.Names}} {{.Label "com.docker.compose.project.config_files"}}'
```

Compare the labels with the deployment's file list. An incident may have
already removed the overlay from one container. Always include the base
file first and the Chatwoot overlay second, including on rollback. If the
deployment uses additional overrides, add their **absolute paths** to
`extra_files` in verified order before proceeding. Resolve unexpected paths
or differing overrides before grouping services in one command.

```bash
extra_files=()
compose_project=altegio_bot
dc() {
  local file
  local -a extra_args=()
  for file in "${extra_files[@]}"; do extra_args+=(-f "$file"); done
  docker compose -p altegio_bot \
    -f docker-compose.yml -f docker-compose.chatwoot-internal.yml \
    "${extra_args[@]}" "$@"
}

chatwoot_services=(altegio-api altegio-inbox-worker altegio-outbox-worker
                   altegio-whatsapp-inbox-worker altegio-campaign-worker)
targets=()
app_network=altegio_bot_default
chatwoot_network=chatwoot_default
rails_host=rails
public_url=https://chatwoot.kitilash.com

target_container() {
  local svc=$1 out
  out=$(docker ps --all --quiet \
    --filter "label=com.docker.compose.project=$compose_project" \
    --filter "label=com.docker.compose.service=$svc" \
    --filter "label=com.docker.compose.oneoff=False") || return 1
  if [[ -z "$out" || "$out" == *$'\n'* ]]; then
    printf 'STOP: %s must have exactly one ordinary container in project %s, found: %s\n' \
      "$svc" "$compose_project" "${out:-none}" >&2
    return 1
  fi
  printf '%s\n' "$out"
}

require_targets() {
  local svc allowed
  if (( ${#targets[@]} == 0 )); then
    printf 'STOP: select the target services in section 4 first.\n' >&2
    return 1
  fi
  for svc in "${targets[@]}"; do
    for allowed in "${chatwoot_services[@]}"; do
      [[ "$svc" == "$allowed" ]] && continue 2
    done
    printf 'STOP: unknown Chatwoot service: %s\n' "$svc" >&2
    return 1
  done
}

before_change() {
  local svc cid old_files base overlay file extras=''
  require_targets || return 1
  dc config --quiet || return 1
  docker network inspect --format '{{.Name}}' "$app_network" "$chatwoot_network" || return 1
  base="$PWD/docker-compose.yml"
  overlay="$PWD/docker-compose.chatwoot-internal.yml"
  for file in "${extra_files[@]}"; do extras+=",$file"; done
  for svc in "${targets[@]}"; do
    cid=$(target_container "$svc") || return 1
    old_files=$(docker inspect --format '{{index .Config.Labels "com.docker.compose.project.config_files"}}' "$cid") || return 1
    docker inspect --format '{{.Name}} OLD_CONTAINER_CREATED={{.Created}}' "$cid" || return 1
    if [[ "$old_files" != "$base,$overlay$extras" && "$old_files" != "$base$extras" ]]; then
      printf 'STOP: review Compose overrides for %s: %s\n' "$svc" "$old_files" >&2
      return 1
    fi
  done
}
```

**Choose the services for this operation.** The setup above clears any prior
selection. Copy the assignment from the applicable row into the same Bash
shell. Keep this selection for the prechecks, recreation and verification.

| Intended change | Bash assignment |
| --- | --- |
| Change only the outbox worker's environment | `targets=(altegio-outbox-worker)` |
| Attach all five services, switch their shared route, or roll that switch back | `targets=("${chatwoot_services[@]}")` |

For another subset, list the affected service names explicitly in `targets`.
With an empty selection, the checks return an error before invoking Docker.
Start each new operation with the shared setup to clear the previous selection.

Set `app_network`, `chatwoot_network` and `rails_host` to the verified
deployment values if different. For a custom external network, Compose's
`CHATWOOT_INTERNAL_NETWORK` must select that same network. Confirm the host
is the trusted Rails endpoint on that network before using an API token.
For the production Rails container listed below, this read-only check shows
its networks, addresses and DNS aliases. If the container name has changed,
use the Rails container identified in the Chatwoot deployment.

```bash
docker inspect chatwoot_rails_1 \
  --format '{{range $name, $net := .NetworkSettings.Networks}}{{println $name $net.IPAddress (json $net.Aliases)}}{{end}}'
```

`target_container` resolves the one ordinary container of a service from its
Compose labels: the project in `compose_project`, which must stay equal to the
`-p` value in `dc`, the service name, and `oneoff=False`. A container left by
`compose run` carries `oneoff=True` and is therefore never taken for a missing
worker, and anything other than exactly one match stops the group. Every later
check resolves the container the same way, so the creation time below, the
status in §9 and the image in §9 all describe the same ordinary container.

Keep the pre-change output with the incident or deployment record. The label
check accepts the verified file set or that set missing just the Chatwoot
overlay (initial attachment or repair). All production `up` and `restart`
commands for these services use the full file set. Use only `config --quiet`
for validation because rendered configuration may include secrets.

**Initial attachment with an approved build only.** Run the following block
when first enabling the overlay on the existing deployment and building the
app images. Keep the working URL in the environment during this step. For an
env-only change or repair using deployed images go directly to §9.

```bash
before_change && dc up -d --no-deps --build "${targets[@]}"
```

Check §§5–7 and the *current* runtime route (§10) after attachment. Use §12
to verify mirroring on ordinary traffic. Switch the URL separately in §9.

## 5. Verify both networks on every target

```bash
check_networks() {
  local svc cid networks required
  require_targets || return 1
  for svc in "${targets[@]}"; do
    cid=$(dc ps --quiet "$svc") || return 1
    networks=$(docker inspect --format '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' "$cid") || return 1
    for required in "$app_network" "$chatwoot_network"; do
      if ! grep -Fxq -- "$required" <<< "$networks"; then
        printf 'FAIL %s: missing network %s\n' "$svc" "$required" >&2
        return 1
      fi
    done
    printf '%s: attached to %s and %s\n' "$svc" "$app_network" "$chatwoot_network"
  done
}
check_networks
```

## 6. Verify internal DNS on every target

```bash
check_internal_dns() {
  local svc
  require_targets || return 1
  for svc in "${targets[@]}"; do
    printf '%s: ' "$svc"
    dc exec -T "$svc" /app/.venv/bin/python -c \
      'import socket, sys; print(sys.argv[1], socket.gethostbyname(sys.argv[1]))' \
      "$rails_host" || return 1
  done
}
check_internal_dns
```

Compare the resolved address with the Rails endpoint on the verified Chatwoot
network. Stop before switching to an internal URL if DNS fails or points to
another endpoint.

## 7. Compare the public and internal APIs before switching

The repository probe makes authenticated GET requests to `contacts/search`
using the container token and its built-in synthetic query. It prints status
counts and latency, and exits unsuccessfully if any response is not HTTP 200.
Give both routes explicitly so the public baseline remains public regardless
of the current environment.

```bash
probe() {
  dc exec -T "$1" /app/.venv/bin/python \
    -m altegio_bot.scripts.probe_chatwoot_latency "${@:2}"
}
compare_routes() {
  local svc
  require_targets || return 1
  for svc in "${targets[@]}"; do
    printf '%s\n' "$svc"
    probe "$svc" --base-url "$public_url" --forwarded-proto '' --requests 15 || return 1
    probe "$svc" --base-url "http://$rails_host:3000" --forwarded-proto https --requests 15 || return 1
  done
}
compare_routes
```

Require `statuses: 200x15` for each route on each target before switching.
For a migration intended to reduce latency, also confirm the internal route
improves on the public baseline from the same run (§11).

## 8. Why internal HTTP needs a forwarded protocol

Production Rails redirects plain internal HTTP to HTTPS. The verified
combination is `http://rails:3000` with `X-Forwarded-Proto: https`. Configure
the header with `CHATWOOT_API_FORWARDED_PROTO=https`. Clearing the variable
omits the header, as used on the public route. The §7 probe does not follow
redirects, so a 301 is visible as a failed check.

## 9. Apply an environment change using deployed images

For a switch from public to internal, run the §4 setup, choose the shared
route row, and run §§5–7 first.
After those checks pass, update only the route settings in
`/opt/altegio_bot/.env`. Build the URL from the same `rails_host` value that
§§6–7 verified, so that no alias is retyped by hand:

```bash
printf 'CHATWOOT_BASE_URL=http://%s:3000\n' "$rails_host"
```

Copy the printed line into `/opt/altegio_bot/.env` literally, together with the
forwarded protocol below. Compose does interpolate `.env`, so a `$` in a value is
substituted there as well, and `rails_host` exists only in this shell: written as
a variable reference it would collapse to `http://:3000`. Write the printed
literal URL. With the standard alias the printed line is the first of these two:

```env
CHATWOOT_BASE_URL=http://rails:3000
CHATWOOT_API_FORWARDED_PROTO=https
```

With a verified alternative alias the printed URL differs, and then the host
verified in §§6–7, the value written to `.env` and the address expected in §10
must be one and the same. Keep `rails_host` unchanged in this shell for the rest
of the operation: §10 checks the container against `http://$rails_host:3000`,
so a mismatch there means the applied value was not the verified one.

For a routine env-only change on an already internal deployment, leave the
route as configured. Within the five services covered here, select only those
that consume the changed setting. Run the §4 shared setup even when the
change is in `easyweek.env`. If the setting has consumers outside these five,
apply and verify it there using their deployment procedure as well. For
example, the EasyWeek category allowlist is used by both the shared outbox
worker and `altegio-easyweek-inbox-worker`.

This operation must keep the deployed application version. Even with
`--no-build --pull never`, `--force-recreate` creates the new container from
whatever the local image tag points to *now*, so a tag moved by a deploy or a
build would change the version silently along with the environment. Run §9 and
§13 on their own, never while a deploy or a build is in progress: the check
below reads the local images once and cannot see a tag that moves afterwards.
The approved build in §4 is the opposite case and replaces the images on
purpose, so it does not use this check.

```bash
check_targets_running() {
  local svc cid status
  require_targets || return 1
  for svc in "${targets[@]}"; do
    cid=$(target_container "$svc") || return 1
    if ! status=$(docker inspect --format '{{.State.Status}}' "$cid"); then
      printf 'STOP: %s: cannot read the container state\n' "$svc" >&2
      return 1
    fi
    if [[ "$status" != running ]]; then
      printf 'STOP: %s is %s, not running; this change neither starts it nor skips it.\n' \
        "$svc" "${status:-unknown}" >&2
      return 1
    fi
    printf '%s: running\n' "$svc"
  done
}

selected_image() {
  local svc=$1 described kind line other other_images own remaining
  local -a others=() excluded=()
  described=$(dc config --format json "$svc" | python3 -c '
import json, sys

svc = sys.argv[1]
model = json.load(sys.stdin).get("services", {})
service = model.get(svc)
if service is None:
    raise SystemExit(f"{svc}: not in the effective configuration")
image = service.get("image") or ""
if image:
    if "$" in image:
        raise SystemExit(f"{svc}: unresolved image reference {image}")
    print("image " + image)
elif service.get("build"):
    print("build")
    for name in sorted(model):
        if name != svc:
            print(name)
else:
    raise SystemExit(f"{svc}: the configuration selects no image and no build")
' "$svc") || return 1
  kind=''
  while IFS= read -r line; do
    if [[ -z "$kind" ]]; then kind=$line; else others+=("$line"); fi
  done <<< "$described"
  case "$kind" in
    'image '*) printf '%s\n' "${kind#image }"; return 0 ;;
    build) ;;
    *) printf 'STOP: %s: cannot read the configured image\n' "$svc" >&2; return 1 ;;
  esac
  own=$(dc config --images "$svc") || return 1
  for other in "${others[@]}"; do
    other_images=$(dc config --images "$other") || return 1
    excluded+=("$other_images")
  done
  remaining=$(printf '%s\n' "$own" \
    | grep -Fxv -f <(printf '%s\n' "${excluded[@]}"))
  if [[ -z "$remaining" || "$remaining" == *$'\n'* ]]; then
    printf 'STOP: %s: Compose does not name exactly one image for this service\n' "$svc" >&2
    return 1
  fi
  printf '%s\n' "$remaining"
}

check_deployed_image_unchanged() {
  local svc cid ref selected deployed
  require_targets || return 1
  for svc in "${targets[@]}"; do
    cid=$(target_container "$svc") || return 1
    if ! ref=$(selected_image "$svc"); then
      printf 'STOP: %s: cannot tell which image the configuration selects\n' "$svc" >&2
      return 1
    fi
    if ! selected=$(docker image inspect --format '{{.Id}}' "$ref"); then
      printf 'STOP: %s: no local image %s\n' "$svc" "$ref" >&2
      return 1
    fi
    deployed=$(docker inspect --format '{{.Image}}' "$cid") || return 1
    if [[ "$selected" != "$deployed" ]]; then
      printf 'STOP: %s would move from %s to %s (%s)\n' \
        "$svc" "$deployed" "$selected" "$ref" >&2
      return 1
    fi
    printf '%s: %s is still the deployed image %s\n' "$svc" "$ref" "$deployed"
  done
}
```

Both checks cover every selected service before anything is recreated, and
either one failing stops the operation for the whole group, including the
services that passed. Both read the ordinary container resolved in §4, so a
container left behind by `compose run` neither passes a check nor stands in for
a worker that is gone.

Only the exact status `running` is accepted. `exited`, `paused`, `restarting`,
`created`, `dead`, an unexpected value and a failed `inspect` all stop the group
and print the service name with the status that caused it; the state is read as
`.State.Status`, because a paused or restarting container still reports
`.State.Running` as `true`. Starting a stopped worker stays a separate operator
decision under its own authorization — it may have been stopped deliberately —
and this procedure neither starts it nor skips the check.

The image check answers one question per service: would the recreation run a
different image than the container runs now? It takes the image from Compose's
own result for this file set, project and environment, and compares that local
image's ID with the container's `.Image`. A service with its own `image:` key,
from the base file or from an override, is checked against that key as the
rendered configuration reports it. For a service built from `build:` the name is
whatever Compose prints for it, never a name assembled here, so the separator
Compose uses between the project and the service — a hyphen, or an underscore in
compatibility mode, however that mode happens to be set — is Compose's answer
and not a rule this runbook has to repeat. Because `config --images` for one
service also prints the images of its dependencies, the dependencies' own image
lists are subtracted from it, and the check continues only when exactly one name
is left: nothing left or more than one name is an ambiguity that stops the
service, as does any failing Compose call. The decision then follows from image
IDs, so two tags of one ID pass and one tag pointing at another ID stops.

The configuration is filtered on the host by the reader in the function, which
prints one field and nothing else: never print the configuration, write it to a
file, or pass it into a container. The reader needs `python3` on the host; where
it is missing the check ends and nothing is recreated.

The group also stops when the service has no ordinary container in this project
or more than one, when the service is absent from the effective configuration,
when its image reference is unresolved or it selects neither an image nor a
build, when Compose leaves more than one or no candidate name for a built
service, and when the selected image is not present locally. Report the printed
reason to the operator and resolve it before retrying; do not work around the
check.

```bash
before_change && check_targets_running && check_deployed_image_unchanged \
  && dc up -d --no-deps --no-build --pull never --force-recreate "${targets[@]}"
```

Continue only after successful recreation. Repeat §§5–6, then §§10 and 12.
A restart does not reload an env file. Do not use `down`.

## 10. Verify the active route from every recreated container

Use the URL and header expected for this operation. For §9 on the internal
route, use the call below. For §4 on an existing public route or for §13,
use the public call in §13. The GET reads the container's actual settings
without CLI route overrides.

```bash
check_runtime() {
  local svc
  require_targets || return 1
  for svc in "${targets[@]}"; do
    printf '%s\n' "$svc"
    dc exec -T "$svc" sh -c '
      printf "CHATWOOT_BASE_URL=%s\nCHATWOOT_API_FORWARDED_PROTO=%s\n" \
        "$CHATWOOT_BASE_URL" "$CHATWOOT_API_FORWARDED_PROTO"
      [ "$CHATWOOT_BASE_URL" = "$1" ] && [ "$CHATWOOT_API_FORWARDED_PROTO" = "$2" ]
    ' sh "$1" "$2" || return 1
    dc exec -T "$svc" /app/.venv/bin/python \
      -m altegio_bot.scripts.probe_chatwoot_latency --requests 1 || return 1
  done
}
```

```bash
check_runtime "http://$rails_host:3000" https
```

Require the expected settings and `statuses: 200x1` for every target. For an
unrelated env change also check the specific non-secret setting in each affected
container, e.g. `dc exec -T altegio-outbox-worker printenv EASYWEEK_ALLOWED_SERVICE_CATEGORIES`
for an outbox category change. Compare against the intended value. Avoid a
full environment dump.

## 11. Compare latency after a route switch

For each selected service, compare the active route with its §7 baseline
using the same synthetic query and sample size.

```bash
measure_latency() {
  local svc
  require_targets || return 1
  for svc in "${targets[@]}"; do
    dc exec -T "$svc" /app/.venv/bin/python \
      -m altegio_bot.scripts.probe_chatwoot_latency --requests 15 || return 1
  done
}
measure_latency
```

Use the public baseline from this change. Historical public measurements for
15 requests were 0.146 s average / 0.113 s median and, in June 2026, 0.053 s
average / 0.042 s median. Latency measures route performance only.

## 12. Confirm mirroring on ordinary traffic

Wait for the next ordinary outbound WhatsApp notification after recreation.
Open the recipient's conversation in the expected Chatwoot account and inbox.
Confirm the corresponding **private outgoing note**. For an exact API match,
check that its `content_attributes.altegio_bot_message_kind` is
`whatsapp_outbound_mirror_v1` and its `whatsapp_provider_message_id` attribute
matches the outbox row's `provider_message_id`. Record the outbox, conversation
and Chatwoot message IDs. Once the connectivity checks pass, report
“Connectivity verified, mirroring awaiting confirmation” until that note is
found. DNS, HTTP 200 and WhatsApp `delivered` or `read` cannot prove the copy.
Observe inbound, operator replies and native reply context on normal traffic
as applicable. Never send a customer message just to test.

On 2026-11-25 a base-only outbox recreation removed its Chatwoot network.
WhatsApp delivery continued while mirroring failed on DNS for `rails` until
the overlay was restored on 2026-11-27. Use the old container creation time
from §4 to bound a similar investigation. Before recovering history, check
Chatwoot for duplicates. A missing mirror-registry row cannot prove absence
when `CHATWOOT_INSTALLATION_GENERATION` is unset. Restore only confirmed
missing private notes under separate authorization. Never replay customer
sends or reset completed jobs to repair history.

## 13. Roll back the URL while retaining both networks

Run the shared setup in §4 and select the services whose route you are
reverting. For a shared route rollback, choose all five. Use the same
verified Compose files, overrides, networks and
deployed images. First verify the public endpoint from the selected services.
This check runs `exec` inside each selected container, so every one of them
must already be running; a stopped target ends the rollback here, and starting
it stays a separate operator decision:

```bash
check_public_fallback() {
  local svc
  require_targets || return 1
  for svc in "${targets[@]}"; do
    dc exec -T "$svc" /app/.venv/bin/python \
      -m altegio_bot.scripts.probe_chatwoot_latency \
      --base-url "$public_url" --forwarded-proto '' --requests 1 || return 1
  done
}
check_public_fallback
```

After all targets return HTTP 200, set these values in `/opt/altegio_bot/.env`:

```env
CHATWOOT_BASE_URL=https://chatwoot.kitilash.com
CHATWOOT_API_FORWARDED_PROTO=
```

The rollback changes only the URL, so it keeps the deployed version too.
Define the §9 preflight functions in this shell, run the rollback separately
from any deploy or build, and stop the whole group on a printed STOP:

```bash
before_change && check_targets_running && check_deployed_image_unchanged \
  && dc up -d --no-deps --no-build --pull never --force-recreate "${targets[@]}"
```

After successful recreation, run the §5 network and §6 internal DNS checks.
Use the §10 function definition with the **public** expectations:

```bash
check_runtime "$public_url" ''
```

The public route must work with an empty forwarded protocol from every
affected container. Report any lingering internal DNS failure separately
even if the public API works. Keep both networks attached and verify the
private note on the next ordinary notification (§12).
