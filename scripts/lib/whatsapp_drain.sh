#!/usr/bin/env bash
#
# Safe retirement of altegio-whatsapp-inbox-worker.
#
# This worker is the second producer of EasyWeek lifecycle jobs
# (`_handle_failed_delivery_status` turns a late Meta `failed` callback into a
# `provider='easyweek'` delivery retry), so the PR-7.1 rollout has to take it
# out of the picture before swapping the outbox. Taking it down is only safe if
# nothing is left behind:
#
#   `lock_next_batch` commits `received -> processing` for a whole batch and
#   only then processes the ids. The normal claim reads `received` only, and
#   `recover_stale_processing_events` covers Chatwoot operator-relay rows alone
#   — so an ordinary inbound message or status callback abandoned in
#   `processing` is stranded permanently.
#
# Two different situations, deliberately kept apart:
#
#   * a RUNNING container that already has the graceful contract can be asked to
#     drain with SIGTERM, and the drain is then PROVEN from its exit state;
#   * a running container from an image that predates the contract (a82d449 and
#     earlier) cannot. SIGTERM kills it wherever it is. That one needs the
#     transitional DB-barrier retirement
#     (`altegio_bot.scripts.retire_legacy_whatsapp_worker`), and this library
#     refuses to guess on its behalf.
#
# Capability is read from the RUNNING container, never from the checked-out
# source: the working tree always contains the new runner, including on the very
# deploy that is still about to install it.
#
# EVERY exit path ends in a proven `whatsapp_events` processing count. "No
# container" is not evidence that nothing was stranded — a crash, an OOM kill or
# a hand-run `docker rm` leaves committed `processing` rows behind and removes
# the container that would have shown it.
#
# Callers must provide `$COMPOSE` and a `psql_scalar` function. Nothing here
# prints payloads, phone numbers, e-mail, message bodies, tokens or raw
# container error text — only container ids, technical states and counts.

WA_SERVICE="altegio-whatsapp-inbox-worker"

# Discovery outcomes, kept distinct on purpose: a Docker failure must never be
# indistinguishable from "there is no container".
WA_FOUND_ONE=0
WA_FOUND_NONE=1
WA_FOUND_UNTRUSTED=2
WA_DISCOVERY_FAILED=3

# `docker inspect` for one field, separating "the field is empty" from "the
# call failed". Returns non-zero only on the latter; prints the raw value.
wa_docker_field() {
  local id="$1" fmt="$2" out
  if ! out="$(docker inspect -f "$fmt" "$id" 2>/dev/null)"; then
    return 1
  fi
  printf '%s' "$out"
}

# The service container, and only if there is exactly one trusted one.
#
# Prints the id on success. See the WA_* codes above — the caller must branch on
# all of them.
wa_service_container_id() {
  local ids count id oneoff

  if ! ids="$(docker ps -a \
    --filter "label=com.docker.compose.project=altegio_bot" \
    --filter "label=com.docker.compose.service=${WA_SERVICE}" \
    --format '{{.ID}}' 2>/dev/null)"; then
    echo "❌ Cannot list ${WA_SERVICE} containers (Docker discovery failed)." >&2
    return "$WA_DISCOVERY_FAILED"
  fi

  count="$(printf '%s' "$ids" | grep -c '[0-9a-f]' || true)"
  if [ "$count" = "0" ]; then
    return "$WA_FOUND_NONE"
  fi
  if [ "$count" != "1" ]; then
    echo "❌ Expected exactly one ${WA_SERVICE} container, found ${count}." >&2
    return "$WA_FOUND_UNTRUSTED"
  fi

  id="$(printf '%s\n' "$ids" | head -n 1)"

  # An unreadable or absent label is NOT proof that this is a service
  # container; only an explicit negative is.
  #
  # Compose writes this label with a capitalised Python bool on some versions
  # (`False` on v5.3.1, `false` elsewhere) and omits it on others, so the value
  # is case-folded before it is judged. Anything unrecognised is refused rather
  # than assumed benign.
  if ! oneoff="$(wa_docker_field "$id" '{{index .Config.Labels "com.docker.compose.oneoff"}}')"; then
    echo "❌ Cannot inspect the ${WA_SERVICE} container (Docker inspect failed)." >&2
    return "$WA_DISCOVERY_FAILED"
  fi
  case "$(printf '%s' "$oneoff" | tr '[:upper:]' '[:lower:]')" in
    false | "<no value>" | "") ;;
    true)
      echo "❌ The resolved ${WA_SERVICE} container is a one-off, not the service container." >&2
      return "$WA_FOUND_UNTRUSTED"
      ;;
    *)
      echo "❌ Unrecognisable one-off label on the ${WA_SERVICE} container; refusing to continue." >&2
      return "$WA_FOUND_UNTRUSTED"
      ;;
  esac

  printf '%s\n' "$id"
  return "$WA_FOUND_ONE"
}

# Does the image this container is RUNNING honour the graceful contract?
#
# Asked of the live process, by inspecting the signature the loop was built
# with. A source check would answer for the code being deployed, not for the
# code currently holding claimed rows.
wa_worker_supports_graceful_shutdown() {
  local id="$1" answer
  answer="$(docker exec "$id" /app/.venv/bin/python -c '
import inspect
from altegio_bot.workers import whatsapp_inbox_worker as w
graceful = "stop_event" in inspect.signature(w.run_loop).parameters and hasattr(w, "run_with_graceful_shutdown")
print("graceful" if graceful else "legacy")
' 2>/dev/null | tr -d '[:space:]')"

  [ "$answer" = "graceful" ]
}

# Ordinary events sitting in `processing`. Never rewritten automatically:
# replaying an inbound message or a status callback is not proven side-effect
# free, so a non-zero count is an operator decision, not a repair the deploy
# performs.
wa_stranded_processing_count() {
  psql_scalar "SELECT count(*) FROM whatsapp_events WHERE status = 'processing';"
}

# The gate every path has to pass: the count must be readable, numeric and zero.
wa_require_no_stranded_processing() {
  local context="${1:-}" stranded
  stranded="$(wa_stranded_processing_count)"

  case "$stranded" in
    "" | *[!0-9]*)
      echo "❌ Cannot read the whatsapp_events processing count${context:+ (${context})}; refusing to continue." >&2
      return 1
      ;;
  esac

  if [ "$stranded" != "0" ]; then
    echo "❌ ${stranded} whatsapp_events row(s) left in 'processing'${context:+ (${context})}." >&2
    echo "   STOP: analyse them individually. Do NOT bulk-update them back to 'received'" >&2
    echo "   — replay safety for ordinary webhook events is not proven." >&2
    return 1
  fi

  return 0
}

# Everything that must hold before the container may be discarded.
#
# Deliberately reads the STOPPED container: once it is removed, ExitCode,
# OOMKilled and Error are gone, and with them the only evidence of whether the
# drain finished or the kernel killed it mid-batch.
wa_verify_drained() {
  local id="$1" state exit_code oom err

  if ! state="$(wa_docker_field "$id" '{{.State.Status}}')"; then
    echo "❌ Cannot inspect ${WA_SERVICE} after the stop; its drain is unproven." >&2
    return 1
  fi
  if [ -z "$state" ]; then
    echo "❌ ${WA_SERVICE} container disappeared before its drain could be verified." >&2
    return 1
  fi
  if [ "$state" != "exited" ]; then
    echo "❌ ${WA_SERVICE} is '${state}', expected 'exited' — the drain did not finish." >&2
    return 1
  fi

  if ! exit_code="$(wa_docker_field "$id" '{{.State.ExitCode}}')" || [ -z "$exit_code" ]; then
    echo "❌ Cannot read the ${WA_SERVICE} exit code; its drain is unproven." >&2
    return 1
  fi
  if [ "$exit_code" != "0" ]; then
    # 137 is the SIGKILL that follows an expired stop timeout: the batch was
    # cut off, not drained.
    echo "❌ ${WA_SERVICE} exited with code ${exit_code}; the claimed batch was not drained." >&2
    return 1
  fi

  # Must be an explicit `false`. An unreadable value is not evidence.
  if ! oom="$(wa_docker_field "$id" '{{.State.OOMKilled}}')" || [ "$oom" != "false" ]; then
    echo "❌ ${WA_SERVICE} OOM status is '${oom:-unreadable}', expected 'false'." >&2
    return 1
  fi

  if ! err="$(wa_docker_field "$id" '{{.State.Error}}')"; then
    echo "❌ Cannot read the ${WA_SERVICE} container error field; its drain is unproven." >&2
    return 1
  fi
  if [ -n "$err" ]; then
    # The field's presence, never its content: it can carry arbitrary text.
    echo "❌ ${WA_SERVICE} recorded a container-level error; refusing to treat this as a clean drain." >&2
    return 1
  fi

  wa_require_no_stranded_processing "after drain" || return 1

  echo "✅ ${WA_SERVICE} drained cleanly (exit 0, no OOM, no stranded processing rows)."
}

# Ask a graceful-capable worker to stop, then PROVE it drained.
#
# The container is not removed here. `docker stop` returning 0 only means the
# daemon delivered the signal and the process eventually went away — including
# via SIGKILL after the timeout. The verdict comes from the exit state.
wa_graceful_quiesce() {
  local id="$1" timeout="${2:-300}"

  echo "🛑 Asking ${WA_SERVICE} to finish its claimed batch (timeout ${timeout}s)..."
  docker stop -t "$timeout" "$id" >/dev/null 2>&1 || true

  wa_verify_drained "$id"
}

wa_legacy_instructions() {
  cat >&2 <<'LEGACY'
❌ The running altegio-whatsapp-inbox-worker predates the graceful shutdown
   contract: it has no SIGTERM handler, so `docker stop` (any timeout) kills it
   wherever it is and strands the batch it already committed as `processing`.

   This deploy refuses to touch it. Run the ONE-TIME transitional retirement,
   which holds a DB-side barrier across the retirement:

     docker compose -p altegio_bot -f docker-compose.yml \
       -f docker-compose.chatwoot-internal.yml \
       run --rm --no-deps --entrypoint /app/.venv/bin/python altegio-api \
       -m altegio_bot.scripts.retire_legacy_whatsapp_worker --container <id>

   See docs/easyweek/durlach_activation_runbook.md, then run the deploy again.
LEGACY
}

# Shared decision for both the preflight and the final gate.
#
# `stage` only tunes the wording; the rules are identical, because the state can
# change between the two (build, backup and migrations run in between).
wa_assert_retirable() {
  local stage="${1:-gate}" id status state

  # The resolved id is returned through this global, not through stdout, so the
  # human-readable progress below cannot be mistaken for a container id.
  WA_RESOLVED_ID=""

  id="$(wa_service_container_id)"
  status=$?

  case "$status" in
    "$WA_FOUND_NONE")
      # No container is NOT evidence that nothing was stranded: a crash or a
      # manual `docker rm` removes exactly the thing that would have shown it.
      echo "ℹ️  No ${WA_SERVICE} container present (${stage}); proving nothing was stranded..."
      wa_require_no_stranded_processing "no container" || return 1
      echo "✅ No ${WA_SERVICE} container and no stranded processing rows."
      return 0
      ;;
    "$WA_FOUND_UNTRUSTED" | "$WA_DISCOVERY_FAILED")
      return 1
      ;;
    "$WA_FOUND_ONE") ;;
    *)
      echo "❌ Unknown ${WA_SERVICE} discovery result; refusing to continue." >&2
      return 1
      ;;
  esac

  if ! state="$(wa_docker_field "$id" '{{.State.Status}}')" || [ -z "$state" ]; then
    echo "❌ Cannot read the ${WA_SERVICE} container state (${stage}); refusing to continue." >&2
    return 1
  fi

  # Transitional states are NOT "not running". A paused container still owns
  # whatever it claimed, and its process is frozen mid-batch: scaling it to zero
  # would discard exactly the batch the barrier procedure exists to protect.
  # Restarting/removing/created/dead are equally unproven. Only `exited` — a
  # process that actually finished — may be cleared by the database alone.
  local paused restarting
  paused="$(wa_docker_field "$id" '{{.State.Paused}}')" || paused="unreadable"
  restarting="$(wa_docker_field "$id" '{{.State.Restarting}}')" || restarting="unreadable"
  if [ "$paused" = "true" ] || [ "$restarting" = "true" ]; then
    echo "❌ ${WA_SERVICE} is paused/restarting (${stage}); STOP." >&2
    echo "   A frozen or restarting worker must be handled by the one-time legacy" >&2
    echo "   retirement or manually — never scaled to zero by this deploy." >&2
    return 1
  fi

  case "$state" in
    exited)
      # Stopped by something other than this deploy. Its exit evidence proves
      # nothing about a drain, so only the database can clear it.
      echo "ℹ️  ${WA_SERVICE} is 'exited' (${stage}); this is not a proven graceful drain."
      wa_require_no_stranded_processing "container exited" || return 1
      echo "✅ ${WA_SERVICE} is not running and no processing rows are stranded."
      return 0
      ;;
    running) ;;
    *)
      echo "❌ ${WA_SERVICE} is '${state}' (${stage}); STOP." >&2
      echo "   Only 'running' (drained here) or 'exited' (cleared by the database)" >&2
      echo "   are safe to continue on." >&2
      return 1
      ;;
  esac

  if ! wa_worker_supports_graceful_shutdown "$id"; then
    wa_legacy_instructions
    return 1
  fi

  echo "✅ Running ${WA_SERVICE} honours the graceful shutdown contract (${stage})."
  WA_RESOLVED_ID="$id"
  return 0
}

# Runs BEFORE anything is built, migrated or reconciled, so a legacy container
# or a stranded backlog aborts while production is still untouched.
wa_require_drainable_worker() {
  wa_assert_retirable preflight || return 1
  echo "✅ Preflight: ${WA_SERVICE} can be retired safely."
}

# The gate immediately before the Compose reconciliation.
#
# Re-runs the full discovery: between the preflight and here the deploy has
# built images, started infrastructure, taken a backup and migrated, and the
# container set may have changed under it. On success the worker is stopped and
# its drain is proven; only then may the caller scale it to zero.
wa_retire_before_reconciliation() {
  wa_assert_retirable "final gate" || return 1

  # An empty id means the branch already proved safety without a running
  # worker — there is nothing left to stop.
  if [ -z "$WA_RESOLVED_ID" ]; then
    return 0
  fi

  wa_graceful_quiesce "$WA_RESOLVED_ID" "${1:-300}"
}
