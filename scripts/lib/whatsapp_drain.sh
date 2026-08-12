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
#     transitional freeze-check-retire procedure in the runbook, performed by an
#     operator, and this library refuses to guess on its behalf.
#
# Capability is read from the RUNNING container, never from the checked-out
# source: the working tree always contains the new runner, including on the very
# deploy that is still about to install it.
#
# Callers must provide `$COMPOSE` and a `psql_scalar` function. Nothing here
# prints payloads, phone numbers, e-mail, message bodies or raw exception text —
# only container ids, technical states and counts.

WA_SERVICE="altegio-whatsapp-inbox-worker"

# The service container, and only if there is exactly one.
#
# Prints the id. Fails when none exists (nothing to drain — the caller decides
# whether that is fine) or when several do: an extra one-off replica started by
# hand may hold its own claimed batch, and draining one container would say
# nothing about the other.
wa_service_container_id() {
  local ids count id oneoff
  ids="$(docker ps -a \
    --filter "label=com.docker.compose.project=altegio_bot" \
    --filter "label=com.docker.compose.service=${WA_SERVICE}" \
    --format '{{.ID}}')"

  count="$(printf '%s' "$ids" | grep -c '[0-9a-f]' || true)"
  if [ "$count" = "0" ]; then
    return 1
  fi
  if [ "$count" != "1" ]; then
    echo "❌ Expected exactly one ${WA_SERVICE} container, found ${count}." >&2
    return 2
  fi

  id="$(printf '%s\n' "$ids" | head -n 1)"
  oneoff="$(docker inspect -f '{{index .Config.Labels "com.docker.compose.oneoff"}}' "$id" 2>/dev/null || true)"
  if [ "$oneoff" = "True" ]; then
    echo "❌ The resolved ${WA_SERVICE} container is a one-off, not the service container." >&2
    return 2
  fi

  printf '%s\n' "$id"
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

# Everything that must hold before the container may be discarded.
#
# Deliberately reads the STOPPED container: once it is removed, ExitCode,
# OOMKilled and Error are gone, and with them the only evidence of whether the
# drain finished or the kernel killed it mid-batch.
wa_verify_drained() {
  local id="$1" state exit_code oom err stranded

  state="$(docker inspect -f '{{.State.Status}}' "$id" 2>/dev/null || true)"
  if [ -z "$state" ]; then
    echo "❌ ${WA_SERVICE} container disappeared before its drain could be verified." >&2
    return 1
  fi
  if [ "$state" != "exited" ]; then
    echo "❌ ${WA_SERVICE} is '${state}', expected 'exited' — the drain did not finish." >&2
    return 1
  fi

  exit_code="$(docker inspect -f '{{.State.ExitCode}}' "$id" 2>/dev/null || true)"
  if [ "$exit_code" != "0" ]; then
    # 137 is the SIGKILL that follows an expired stop timeout: the batch was
    # cut off, not drained.
    echo "❌ ${WA_SERVICE} exited with code ${exit_code:-unknown}; the claimed batch was not drained." >&2
    return 1
  fi

  oom="$(docker inspect -f '{{.State.OOMKilled}}' "$id" 2>/dev/null || true)"
  if [ "$oom" = "true" ]; then
    echo "❌ ${WA_SERVICE} was OOM-killed; the claimed batch was not drained." >&2
    return 1
  fi

  err="$(docker inspect -f '{{.State.Error}}' "$id" 2>/dev/null || true)"
  if [ -n "$err" ]; then
    # The field itself, not its content: it can carry arbitrary runtime text.
    echo "❌ ${WA_SERVICE} recorded a container-level error; refusing to treat this as a clean drain." >&2
    return 1
  fi

  stranded="$(wa_stranded_processing_count)"
  if [ -z "$stranded" ]; then
    echo "❌ Cannot read the whatsapp_events processing count; refusing to continue." >&2
    return 1
  fi
  if [ "$stranded" != "0" ]; then
    echo "❌ ${stranded} whatsapp_events row(s) left in 'processing'. STOP: analyse them individually." >&2
    echo "   Do NOT bulk-update them back to 'received' — replay safety is not proven." >&2
    return 1
  fi

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

# The gate that runs BEFORE anything is built, migrated or reconciled.
#
# A legacy container must never be stopped by this script: SIGTERM would strand
# its claimed batch. The deploy fails instead, and the operator performs the
# transitional freeze-check-retire from the runbook, which is safe for an image
# with no signal handler.
wa_require_drainable_worker() {
  local id status

  id="$(wa_service_container_id)"
  status=$?
  if [ "$status" = "1" ]; then
    echo "ℹ️  No ${WA_SERVICE} container present; nothing to drain."
    return 0
  fi
  if [ "$status" != "0" ]; then
    return 1
  fi

  if ! docker inspect -f '{{.State.Status}}' "$id" 2>/dev/null | grep -q '^running$'; then
    echo "ℹ️  ${WA_SERVICE} is not running; nothing to drain."
    return 0
  fi

  if wa_worker_supports_graceful_shutdown "$id"; then
    echo "✅ Running ${WA_SERVICE} honours the graceful shutdown contract."
    return 0
  fi

  cat >&2 <<'LEGACY'
❌ The running altegio-whatsapp-inbox-worker predates the graceful shutdown
   contract: it has no SIGTERM handler, so `docker stop` (any timeout) kills it
   wherever it is and strands the batch it already committed as `processing`.

   This deploy refuses to touch it. Perform the ONE-TIME transitional quiesce
   from docs/easyweek/durlach_activation_runbook.md ("Разовый переход со старого
   image") — freeze, prove processing = 0, retire — and run the deploy again.
LEGACY
  return 1
}
