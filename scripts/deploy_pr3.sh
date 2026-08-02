#!/usr/bin/env bash
#
# PR-3 production deployment.
#
# This lives in the repository rather than inside the workflow because the
# GitHub Actions `script:` input is evaluated as a single template expression,
# which is capped at 21000 characters. The full program is far larger than that,
# so an inline version made the workflow file itself invalid and no job — not
# even lint or tests — could start.
#
# The workflow now only fetches the exact commit and hands over to this file, so
# the deploy logic is versioned, reviewable, shell-checkable and directly
# testable. Nothing about the rollout behaviour changed.
#
# Required environment:
#   DEPLOY_SHA - the exact commit the workflow is deploying.
set -Eeuo pipefail

# Always operate on the repository this script belongs to, whatever the caller's
# working directory was.
cd "$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

COMPOSE="docker compose -p altegio_bot -f docker-compose.yml -f docker-compose.chatwoot-internal.yml"

# The bootstrap already fetched and reset to DEPLOY_SHA. Re-verify it HERE,
# before any migration or queue mutation: if the checkout drifted (a concurrent
# deploy, a failed reset, a hand-run of this script) every later guard would be
# reasoning about the wrong code.
if [ -z "${DEPLOY_SHA:-}" ]; then
  echo "❌ DEPLOY_SHA is not set; refusing to deploy."
  exit 1
fi

DEPLOYED_SHA="$(git rev-parse HEAD)"
if [ "$DEPLOYED_SHA" != "$DEPLOY_SHA" ]; then
  echo "❌ Checked-out SHA ($DEPLOYED_SHA) does not match DEPLOY_SHA ($DEPLOY_SHA)"
  exit 1
fi
echo "✅ Deployed exact commit: $DEPLOYED_SHA"

# ── PR-3 deploy phases ────────────────────────────────────────────
# The migration swaps the unique constraints that the RUNNING inbox
# worker of the PREVIOUS release pins by name in
# `ON CONFLICT ON CONSTRAINT`. If it keeps working across the swap,
# any event it processes in that window hits a missing constraint and
# is stored as `failed` — and nothing retries a failed event. So the
# old worker is drained BEFORE the migration and a new-image worker is
# proven AFTER it, while altegio-api keeps accepting webhooks the
# whole time (they simply pile up as `received`).
#
# These flags let the trap below know exactly how far the deploy got.
# The trap is installed before anything is stopped, so every failure
# path from here on has a defined recovery.
# The special flow is ONE-TIME. It is armed only for the exact
# revision step below; a repeat deploy, or any later revision, takes
# the ordinary migrate-and-roll path and can never reach the
# PR-3 downgrade branch.
PR3_TRANSITION=0
PR3_TRANSITION_STARTED=0
PR3_TRANSITION_APPLIED=0
CANARY_VERIFIED=0
REGULAR_WORKER_VERIFIED=0
OLD_WORKER_STOPPED=0
REVISION_BEFORE=""
TARGET_HEAD=""
CANARY_ID=""
CANARY_IMAGE=""
PR3_REVISION="c1a7d3f905b2"
PRE_PR3_REVISION="9a1f4c7b2e3d"
CANARY_NAME="altegio-inbox-worker-pr3-canary"
CONSTRAINT_FAILURES_BEFORE=""
CANARY_DRAIN_UNCERTAIN=0
DEPLOY_BOUNDARY_EPOCH_US=""

# ── Phase A: audited pre-PR-3 catch-up ────────────────────────────
# Production can legitimately sit a few ORDINARY revisions behind the
# pre-PR-3 revision. Those must be applied on their own, while the
# legacy runtime keeps working, BEFORE the worker-drained constraint
# swap — mixing them into the PR-3 step would make the bounded
# rollback ambiguous about how far back to go.
#
# This is the EXACT audited path, oldest → newest, ending at
# PRE_PR3_REVISION. Every revision in it was reviewed to be:
#   * additive (ADD COLUMN / CREATE TABLE / CREATE INDEX only),
#   * idempotent (IF NOT EXISTS, or a guarded DO $$ block),
#   * readable and writable by the OLD running image, and
#   * free of any rename/drop of a constraint the old worker pins by
#     name in `ON CONFLICT ON CONSTRAINT`.
# The single constraint touched anywhere in this path is the
# `ck_promo_leads_status` CHECK, which is WIDENED (a value is added to
# the allowed set), so the old runtime's writes stay valid.
#
# A DB revision outside this list is NOT auto-upgraded: the deploy
# fails closed and asks for a human audit. See docs/ops/pr3_deploy.md.
PHASE_A_REQUIRED=0
PHASE_A_APPLIED=0
AUDITED_CATCHUP_PATH="b7c8d9e0f1a2 c9d0e1f2a3b4 d0e1f2a3b4c5 d8f6e4c2b1a0 e9a7c6b5d4f3 8923be993170 8705ec49cc73 9a1f4c7b2e3d"

# Reads one scalar over stdin so no SQL quoting has to survive the
# ssh -> sh -> psql nesting. Credentials stay inside the container.
psql_scalar() {
  printf '%s\n' "$1" \
    | $COMPOSE exec -T postgres sh -lc 'psql -tAX -U "$POSTGRES_USER" -d "$POSTGRES_DB" -v ON_ERROR_STOP=1' \
    | tr -d '[:space:]'
}

# Reads the applied revision. `alembic current` prints its FAILURE text
# to STDOUT — "FAILED: Can't locate revision identified by 'deadbeef1234'"
# — and that text contains a 12-hex token. Scraping stdout without first
# checking the exit status therefore echoes an id back out of an ERROR
# MESSAGE and reports it as if the database were on that revision, which
# turns "this code cannot resolve the DB revision at all" into a
# plausible-looking ordinary revision. The status is checked FIRST, and
# only a bare id on its own line is accepted.
#
# Exit status: 0 with the id on stdout, 0 with empty stdout for a database
# that has no revision yet, 1 when Alembic could not resolve it.
alembic_revision() {
  ALEMBIC_CURRENT_OUT="$($COMPOSE --profile ops run --rm --no-deps migrate alembic current 2>/dev/null)" || return 1
  printf '%s' "$ALEMBIC_CURRENT_OUT" | tr -d '\r' \
    | grep -oE '^[0-9a-f]{12}' | head -n 1
  return 0
}

# Facts about the NEW code's revision graph, straight from Alembic's
# ScriptDirectory. String comparison alone cannot tell whether PR-3
# is an ancestor of the target head, and a hardcoded head would make
# every future revision fail this deploy.
alembic_script_facts() {
  $COMPOSE --profile ops run --rm --no-deps -T -e DB_REVISION="$1" migrate \
    /app/.venv/bin/python -c '
import os
from alembic.config import Config
from alembic.script import ScriptDirectory
pr3 = "c1a7d3f905b2"
pre_pr3 = "9a1f4c7b2e3d"
sd = ScriptDirectory.from_config(Config("/app/alembic.ini"))
heads = sd.get_heads()
db = (os.environ.get("DB_REVISION") or "").strip()
def lineage(rev):
    if not rev:
        return set()
    try:
        return {r.revision for r in sd.iterate_revisions(rev, "base")}
    except Exception:
        return set()
head = heads[0] if len(heads) == 1 else ""
# Is the DB revision a node this code actually knows? An unresolvable id
# must never be treated as "simply an older revision".
known = "0"
if db:
    try:
        sd.get_revision(db)
        known = "1"
    except Exception:
        known = "0"
# The revisions Alembic would really apply to go from the DB revision up
# to the pre-PR-3 revision, in order. Empty when already there, and empty
# when no upgrade path exists at all.
catchup = []
descendant = "0"
if known == "1":
    try:
        revs = list(sd.iterate_revisions(pre_pr3, db))
        revs.reverse()
        catchup = [r.revision for r in revs if r.revision != db]
        descendant = "1"
    except Exception:
        catchup = []
        descendant = "0"
pr3_rev = None
try:
    pr3_rev = sd.get_revision(pr3)
except Exception:
    pr3_rev = None
pr3_parent = ""
if pr3_rev is not None:
    down = pr3_rev.down_revision
    if isinstance(down, str):
        pr3_parent = down
    elif down:
        pr3_parent = ",".join(down)
print("HEAD_COUNT=" + str(len(heads)))
print("TARGET_HEAD=" + head)
print("PR3_IN_HEAD_LINEAGE=" + ("1" if pr3 in lineage(head) else "0"))
print("PR3_IN_DB_LINEAGE=" + ("1" if pr3 in lineage(db) else "0"))
print("DB_REVISION_KNOWN=" + known)
print("PRE_PR3_IS_DESCENDANT=" + descendant)
print("PR3_PARENT=" + pr3_parent)
print("CATCHUP_PATH=" + " ".join(catchup))
' 2>/dev/null | tr -d '\r'
}

fact() {
  printf '%s\n' "$1" | grep -E "^$2=" | head -n 1 | cut -d= -f2-
}

# The audited revisions that must still be applied to get from $1 up to
# the end of AUDITED_CATCHUP_PATH, as a space-separated list in order.
#
# Prints nothing and returns 1 when $1 is not ON the audited path, or is
# its final element — both mean "there is no audited catch-up to run from
# here", and the caller must fail closed rather than invent a path.
expected_catchup_from() {
  CATCHUP_FROM="$1"
  CATCHUP_ACC=""
  CATCHUP_FOUND=0
  for AUDITED_REVISION in $AUDITED_CATCHUP_PATH; do
    if [ "$CATCHUP_FOUND" -eq 1 ]; then
      if [ -z "$CATCHUP_ACC" ]; then
        CATCHUP_ACC="$AUDITED_REVISION"
      else
        CATCHUP_ACC="$CATCHUP_ACC $AUDITED_REVISION"
      fi
    elif [ "$AUDITED_REVISION" = "$CATCHUP_FROM" ]; then
      CATCHUP_FOUND=1
    fi
  done
  if [ "$CATCHUP_FOUND" -ne 1 ] || [ -z "$CATCHUP_ACC" ]; then
    return 1
  fi
  printf '%s' "$CATCHUP_ACC"
  return 0
}

# EXACT id of the regular Compose service container. `docker ps` by
# service label would also match the one-off canary, and mistaking
# one for the other could leave the system with no inbox worker at
# all. `compose ps` excludes one-off containers by design.
regular_worker_id() {
  $COMPOSE ps -q altegio-inbox-worker 2>/dev/null | head -n 1
}

container_field() {
  docker inspect -f "$2" "$1" 2>/dev/null
}

container_is_running() {
  [ "$(container_field "$1" '{{.State.Status}}')" = "running" ]
}

# ANY inbox worker, by Compose label — the regular service container,
# the PR-3 canary, and any other one-off an operator may have started
# with `docker compose run -d altegio-inbox-worker` under a different
# name. Deliberately NOT built from known ids/names: an unknown worker
# still holding rows in `processing` must block every status reset.
# This is the "is anything running?" question only; regular identity is
# answered separately by `regular_worker_id`.
running_inbox_worker_ids() {
  docker ps -q \
    --filter "label=com.docker.compose.project=altegio_bot" \
    --filter "label=com.docker.compose.service=altegio-inbox-worker"
}

any_inbox_worker_running() {
  [ -n "$(running_inbox_worker_ids)" ]
}

require_no_inbox_worker_running() {
  if any_inbox_worker_running; then
    echo "❌ An inbox worker container is still running (Compose label scan)."
    return 1
  fi
  return 0
}

# `docker stop` exits 0 even when the grace period expired and the
# container was SIGKILLed, so its exit code proves nothing. Drain is
# only established by the container's own final state.
verify_container_drained() {
  VERIFY_ID="$1"
  VERIFY_LABEL="$2"
  VERIFY_STATUS="$(container_field "$VERIFY_ID" '{{.State.Status}}')"
  VERIFY_EXIT="$(container_field "$VERIFY_ID" '{{.State.ExitCode}}')"
  VERIFY_OOM="$(container_field "$VERIFY_ID" '{{.State.OOMKilled}}')"
  VERIFY_ERROR="$(container_field "$VERIFY_ID" '{{.State.Error}}')"
  VERIFY_FINISHED="$(container_field "$VERIFY_ID" '{{.State.FinishedAt}}')"
  if [ -z "$VERIFY_STATUS" ]; then
    echo "❌ Cannot read the $VERIFY_LABEL container state; drain is UNCERTAIN."
    return 1
  fi
  if [ "$VERIFY_STATUS" != "exited" ]; then
    echo "❌ $VERIFY_LABEL is '$VERIFY_STATUS', expected exited; drain is UNCERTAIN."
    return 1
  fi
  if [ "$VERIFY_EXIT" != "0" ]; then
    # 137 = SIGKILL after the grace period: the worker may have been
    # cut off mid-batch, leaving claimed rows in `processing`.
    echo "❌ $VERIFY_LABEL exited with code ${VERIFY_EXIT:-unknown}; drain is UNCERTAIN."
    return 1
  fi
  if [ "$VERIFY_OOM" != "false" ]; then
    echo "❌ $VERIFY_LABEL was OOM-killed; drain is UNCERTAIN."
    return 1
  fi
  if [ -n "$VERIFY_ERROR" ]; then
    echo "❌ $VERIFY_LABEL reported a runtime error; drain is UNCERTAIN."
    return 1
  fi
  echo "✅ $VERIFY_LABEL drained cleanly (exit 0, finished ${VERIFY_FINISHED:-unknown})."
  return 0
}

# Stop ONLY. Removal is a separate decision, because a container that
# was killed rather than drained must survive until its rows have been
# recovered.
stop_canary_and_verify_exit() {
  if [ -z "$CANARY_ID" ]; then
    return 0
  fi
  if container_is_running "$CANARY_ID"; then
    echo "🧹 Draining the canary (up to 300s)..."
    if ! docker stop -t 300 "$CANARY_ID" >/dev/null; then
      echo "❌ docker stop failed for the canary; drain is UNCERTAIN."
      CANARY_DRAIN_UNCERTAIN=1
      return 1
    fi
  fi
  if ! verify_container_drained "$CANARY_ID" "canary"; then
    CANARY_DRAIN_UNCERTAIN=1
    return 1
  fi
  CANARY_DRAIN_UNCERTAIN=0
  return 0
}

# Only ever called once the canary is proven to have exited cleanly,
# or once its rows have been recovered under a full worker stop.
remove_stopped_canary() {
  if [ -z "$CANARY_ID" ]; then
    return 0
  fi
  if container_is_running "$CANARY_ID"; then
    echo "❌ Refusing to remove a running canary."
    return 1
  fi
  docker rm "$CANARY_ID" >/dev/null
  CANARY_ID=""
  echo "✅ Canary removed."
  return 0
}

processing_count() {
  psql_scalar "SELECT count(*) FROM altegio_events WHERE status = 'processing';"
}

# Bounded orphan recovery. ONLY rows that were claimed and never
# reached a terminal state (`processed_at IS NULL`) go back to
# `received`. Claim and processing run in separate transactions, so a
# process killed between them commits the claim and rolls back the
# work — those rows are unreachable otherwise, because the worker only
# ever selects `received`. Payload, timestamps, resource ids and
# customer data are untouched, and only a count is logged.
#
# Allowed exclusively inside the one-time PR-3 transition (or its
# recovery) and only once NO inbox worker of any kind runs.
recover_orphaned_processing_rows() {
  if ! require_no_inbox_worker_running; then
    echo "❗ Refusing to touch event statuses."
    return 1
  fi
  RESET_COUNT="$(psql_scalar "WITH reset AS (UPDATE altegio_events SET status = 'received' WHERE status = 'processing' AND processed_at IS NULL RETURNING 1) SELECT count(*) FROM reset;")"
  echo "♻️ Returned ${RESET_COUNT:-unknown} orphaned event(s) from processing to received."
  REMAINING="$(processing_count)"
  if [ "$REMAINING" != "0" ]; then
    echo "❌ ${REMAINING:-unknown} event(s) still in processing after bounded recovery."
    echo "❗ Not proceeding; these rows are in an unexpected state."
    return 1
  fi
  echo "✅ No Altegio event left in processing."
  return 0
}

# THE single definition of a constraint-swap failure, shared by the
# counts and by the requeue so they can never drift apart. It needs
# BOTH the missing-object signature AND one of the four PR-3
# constraint names: a plain unique violation also mentions the name,
# and a bare missing-object phrase also matches unrelated errors.
# Bounded to this deploy by a PostgreSQL-side timestamp.
constraint_failure_predicate() {
  printf '%s' "status = 'failed' AND processed_at >= to_timestamp(${DEPLOY_BOUNDARY_EPOCH_US} / 1000000.0) AND error LIKE '%does not exist%' AND (error LIKE '%uq_clients_company_altegio_id%' OR error LIKE '%uq_records_company_altegio_id%' OR error LIKE '%uq_clients_provider_company_altegio_id%' OR error LIKE '%uq_records_provider_company_altegio_id%')"
}

constraint_failure_count() {
  psql_scalar "SELECT count(*) FROM altegio_events WHERE $(constraint_failure_predicate);"
}

# Returns the events this deploy broke to `received` so the worker
# picks them up again. Clears ONLY status/processed_at/error; payload,
# received_at, company_id, resource and every other column are left
# exactly as they are. Rows failed before the boundary, and failures
# that are not constraint-swap failures, are never touched.
recover_current_deploy_constraint_failures() {
  if [ "$PR3_TRANSITION" -ne 1 ]; then
    echo "❌ Constraint-failure recovery is only valid inside the PR-3 transition."
    return 1
  fi
  if [ -z "$DEPLOY_BOUNDARY_EPOCH_US" ]; then
    echo "❌ No deploy boundary recorded; refusing to requeue failed events."
    return 1
  fi
  if ! require_no_inbox_worker_running; then
    echo "❗ Refusing to requeue failed events."
    return 1
  fi
  REQUEUED="$(psql_scalar "WITH reset AS (UPDATE altegio_events SET status = 'received', processed_at = NULL, error = NULL WHERE $(constraint_failure_predicate) RETURNING 1) SELECT count(*) FROM reset;")"
  echo "♻️ Requeued ${REQUEUED:-unknown} constraint-failed event(s) from this deploy."
  STILL_FAILED="$(constraint_failure_count)"
  if [ "$STILL_FAILED" != "0" ]; then
    echo "❌ ${STILL_FAILED:-unknown} constraint-failed event(s) remain after recovery."
    return 1
  fi
  echo "✅ No constraint-failed event from this deploy remains."
  return 0
}

start_preserved_old_worker() {
  echo "▶️ Restarting the preserved old inbox worker..."
  if ! $COMPOSE start altegio-inbox-worker; then
    echo "❌ Could not start the old inbox worker. Manual intervention required."
    return 1
  fi
  sleep 5
  RECOVERED_ID="$(regular_worker_id)"
  if [ -n "$RECOVERED_ID" ] && container_is_running "$RECOVERED_ID"; then
    echo "✅ Old inbox worker is running again ($RECOVERED_ID)."
    return 0
  fi
  echo "❌ Old inbox worker did not come back up. Manual intervention required."
  return 1
}

recover() {
  RECOVER_STATUS=$1
  set +e
  if [ "$RECOVER_STATUS" -eq 0 ]; then
    return 0
  fi

  echo "❌ Deploy failed (exit $RECOVER_STATUS). Entering recovery..."

  if [ "$REGULAR_WORKER_VERIFIED" -eq 1 ]; then
    # The schema and the regular worker already agree, so a downgrade
    # would break the thing that works. But if the canary was killed
    # rather than drained it may have stranded rows in `processing`,
    # and those must still be recovered — on the PR-3 schema.
    echo "ℹ️ The regular worker is verified on the new schema; NO schema rollback."
    if [ "$CANARY_DRAIN_UNCERTAIN" -eq 0 ] && [ -z "$CANARY_ID" ]; then
      echo "ℹ️ Canary already retired cleanly."
      echo "❗ Manual investigation required for the failing step."
      exit "$RECOVER_STATUS"
    fi

    echo "🛑 Canary drain is uncertain; stopping the regular worker to recover the queue..."
    if ! $COMPOSE stop -t 300 altegio-inbox-worker; then
      echo "❌ Could not stop the regular worker."
      echo "❗ No status reset, no canary removal, no rollback. Manual intervention required."
      exit "$RECOVER_STATUS"
    fi
    STOPPED_REGULAR_ID="$(regular_worker_id)"
    if [ -n "$STOPPED_REGULAR_ID" ] && ! verify_container_drained "$STOPPED_REGULAR_ID" "regular worker"; then
      echo "❗ No status reset, no canary removal, no rollback. Manual intervention required."
      exit "$RECOVER_STATUS"
    fi
    if ! require_no_inbox_worker_running; then
      echo "❗ No status reset, no canary removal, no rollback. Manual intervention required."
      exit "$RECOVER_STATUS"
    fi
    if ! recover_orphaned_processing_rows || ! recover_current_deploy_constraint_failures; then
      echo "❗ Queue recovery failed. Manual intervention required."
      exit "$RECOVER_STATUS"
    fi
    remove_stopped_canary
    echo "▶️ Restarting the verified PR-3 regular worker..."
    if $COMPOSE start altegio-inbox-worker; then
      sleep 10
      RESTARTED_ID="$(regular_worker_id)"
      RESTARTED_ONEOFF="$(container_field "$RESTARTED_ID" '{{index .Config.Labels "com.docker.compose.oneoff"}}')"
      RESTARTED_RESTARTS="$(container_field "$RESTARTED_ID" '{{.RestartCount}}')"
      RESTARTED_IMAGE="$(container_field "$RESTARTED_ID" '{{.Image}}')"
      if [ -n "$RESTARTED_ID" ] && container_is_running "$RESTARTED_ID" \
        && [ "$RESTARTED_ONEOFF" != "True" ] && [ "$RESTARTED_RESTARTS" = "0" ] \
        && [ "$RESTARTED_IMAGE" = "$CANARY_IMAGE" ]; then
        echo "✅ Regular PR-3 worker is running again ($RESTARTED_ID) on the verified image."
      else
        echo "❌ The regular worker did not come back up correctly. Manual intervention required."
      fi
    else
      echo "❌ Could not restart the regular worker. Manual intervention required."
    fi
    echo "❗ The rollout required recovery. This is a FAILED deploy."
    exit "$RECOVER_STATUS"
  fi

  if [ "$CANARY_VERIFIED" -eq 1 ]; then
    echo "ℹ️ The canary is verified and left RUNNING as a temporary compatible inbox worker."
    echo "ℹ️ NOT rolling back the schema and NOT removing the canary."
    echo "❗ This is a FAILED deploy. Manual investigation required."
    exit "$RECOVER_STATUS"
  fi

  if [ "$OLD_WORKER_STOPPED" -eq 0 ]; then
    echo "ℹ️ No inbox worker was ever stopped; nothing to recover."
    exit "$RECOVER_STATUS"
  fi

  if [ "$PR3_TRANSITION" -ne 1 ]; then
    echo "❗ A worker was stopped outside the PR-3 transition. Manual intervention required."
    exit "$RECOVER_STATUS"
  fi

  # The old container was only STOPPED, never recreated, so it still
  # exists and still carries the previous image.
  CURRENT_REVISION="$(alembic_revision)"
  echo "ℹ️ Alembic revision is now: ${CURRENT_REVISION:-unknown}"
  if [ "$CURRENT_REVISION" = "$PR3_REVISION" ]; then
    PR3_TRANSITION_APPLIED=1
  fi

  # An uncertain drain is NOT fatal here: the canary is stopped either
  # way, and the bounded recovery below is exactly what repairs a
  # SIGKILLed worker's rows. What matters is that nothing is still
  # running when statuses are touched.
  stop_canary_and_verify_exit
  if container_is_running "$CANARY_ID"; then
    echo "❗ Canary is still running. NOT rolling back and NOT starting the old worker."
    exit "$RECOVER_STATUS"
  fi

  if ! require_no_inbox_worker_running; then
    echo "❗ NOT rolling back and NOT starting the old worker."
    exit "$RECOVER_STATUS"
  fi

  if ! recover_orphaned_processing_rows; then
    echo "❗ NOT rolling back and NOT starting the old worker."
    exit "$RECOVER_STATUS"
  fi

  # Events the swap already broke go back to `received` so the
  # restarted old worker picks them up. Bounded to this deploy.
  if [ -n "$DEPLOY_BOUNDARY_EPOCH_US" ] && ! recover_current_deploy_constraint_failures; then
    echo "❗ NOT rolling back and NOT starting the old worker."
    exit "$RECOVER_STATUS"
  fi

  if [ "$PR3_TRANSITION_APPLIED" -eq 1 ]; then
    if [ "$CURRENT_REVISION" != "$PR3_REVISION" ]; then
      echo "❌ Revision is '${CURRENT_REVISION:-unknown}', refusing to downgrade."
      exit "$RECOVER_STATUS"
    fi
    # Bounded to the PR-3 constraint swap ONLY. The target is the literal
    # pre-PR-3 revision, never a relative step and never the revision the
    # database happened to be on when this deploy started: an audited
    # Phase A catch-up that already succeeded is additive, compatible with
    # the old runtime, and stays applied. Production correctly comes to
    # rest on $PRE_PR3_REVISION after this recovery.
    echo "⏪ Downgrading to exactly $PRE_PR3_REVISION (PR-3 only; catch-up is preserved)..."
    if ! $COMPOSE --profile ops run --rm --no-deps migrate alembic downgrade "$PRE_PR3_REVISION"; then
      echo "❌ Downgrade failed."
      echo "❗ NOT starting the old worker against an unknown schema. Manual intervention required."
      exit "$RECOVER_STATUS"
    fi
    CURRENT_REVISION="$(alembic_revision)"
    if [ "$CURRENT_REVISION" != "$PRE_PR3_REVISION" ]; then
      echo "❌ Revision after downgrade is '${CURRENT_REVISION:-unknown}', expected $PRE_PR3_REVISION."
      echo "❗ NOT starting the old worker. Manual intervention required."
      exit "$RECOVER_STATUS"
    fi
    echo "✅ Schema is back on $PRE_PR3_REVISION."
  elif [ "$CURRENT_REVISION" != "$PRE_PR3_REVISION" ]; then
    echo "❌ Revision is '${CURRENT_REVISION:-unknown}', expected the pre-transition $PRE_PR3_REVISION."
    echo "❗ Schema state is unexpected. NOT starting the old worker. Manual intervention required."
    exit "$RECOVER_STATUS"
  else
    echo "✅ Migration never applied; schema is still on $PRE_PR3_REVISION."
  fi

  # Safe to remove now: it is stopped and its rows were recovered.
  remove_stopped_canary
  start_preserved_old_worker
  exit "$RECOVER_STATUS"
}
trap 'recover $?' EXIT

echo "🔨 Building new images (without starting containers)..."
$COMPOSE build

echo "🟢 Starting infrastructure services..."
$COMPOSE up -d postgres redis

echo "⏳ Waiting for Postgres to be healthy..."
MAX_ATTEMPTS=60
ATTEMPTS=0

# Ищем контейнер, который принадлежит сервису postgres.
until [ $ATTEMPTS -ge $MAX_ATTEMPTS ]; do
    # Находим имя контейнера базы данных, как бы он ни назывался
    DB_CONTAINER=$(docker ps --filter "label=com.docker.compose.service=postgres" --filter "label=com.docker.compose.project=altegio_bot" --format "{{.Names}}" | head -n 1)

    if [ -n "$DB_CONTAINER" ] && [ "$(docker inspect -f '{{.State.Health.Status}}' $DB_CONTAINER)" == "healthy" ]; then
        echo "✅ Postgres ($DB_CONTAINER) is ready!"
        break
    fi

    ATTEMPTS=$((ATTEMPTS + 1))
    echo "... waiting for database ($ATTEMPTS/$MAX_ATTEMPTS) ..."
    sleep 1
done

if [ $ATTEMPTS -ge $MAX_ATTEMPTS ]; then
    echo "❌ Timed out waiting for Postgres"
    exit 1
fi

# Postgres is confirmed healthy from this point on.
# Backup is a mandatory gate before migrations — any failure here stops the deploy.
echo "💾 Creating pre-deploy database backup..."
mkdir -p /opt/altegio_bot/backups
DUMP_FILE="/opt/altegio_bot/backups/altegio_before_deploy_$(date -u +%Y%m%dT%H%M%SZ).dump"

# Credentials come from inside the container (docker compose injected them from .env).
# This is more reliable than sourcing .env in the remote shell.
$COMPOSE exec -T postgres sh -lc '
  psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "SELECT 1"
' >/dev/null

$COMPOSE exec -T postgres sh -lc '
  pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc
' > "$DUMP_FILE"

test -s "$DUMP_FILE" || {
  echo "❌ Pre-deploy dump is empty — aborting."
  exit 1
}
echo "✅ Backup created: $DUMP_FILE"

BACKUP_DIR="/opt/altegio_bot/backups"
KEEP_BACKUPS=10

echo "🧹 Rotating pre-deploy database backups..."
echo "Keeping latest ${KEEP_BACKUPS} backups"

ls -1t "${BACKUP_DIR}"/altegio_before_deploy_*.dump 2>/dev/null \
  | tail -n +"$((KEEP_BACKUPS + 1))" \
  | xargs -r rm -v --

echo "Current backup count:"
find "${BACKUP_DIR}" -maxdepth 1 -type f \
  -name 'altegio_before_deploy_*.dump' \
  | wc -l

# ── Classify this deploy ──────────────────────────────────────────
# A revision the new code cannot resolve is NOT an older revision: no
# upgrade path can be computed from it, so every later guard would be
# reasoning about a database it does not understand. Fail closed here,
# before the schema, the queue or any worker is touched.
if ! REVISION_BEFORE="$(alembic_revision)"; then
  echo "❌ Alembic could not resolve the revision recorded in this database."
  echo "❗ The alembic_version value is not a revision this code knows, so no"
  echo "   upgrade path exists. Nothing was migrated, stopped or modified."
  echo "❗ Investigate the database identity by hand before redeploying."
  exit 1
fi
echo "📌 Alembic revision before migration: ${REVISION_BEFORE:-none}"

SCRIPT_FACTS="$(alembic_script_facts "$REVISION_BEFORE")"
HEAD_COUNT="$(fact "$SCRIPT_FACTS" HEAD_COUNT)"
TARGET_HEAD="$(fact "$SCRIPT_FACTS" TARGET_HEAD)"
PR3_IN_HEAD_LINEAGE="$(fact "$SCRIPT_FACTS" PR3_IN_HEAD_LINEAGE)"
PR3_IN_DB_LINEAGE="$(fact "$SCRIPT_FACTS" PR3_IN_DB_LINEAGE)"
DB_REVISION_KNOWN="$(fact "$SCRIPT_FACTS" DB_REVISION_KNOWN)"
PRE_PR3_IS_DESCENDANT="$(fact "$SCRIPT_FACTS" PRE_PR3_IS_DESCENDANT)"
PR3_PARENT="$(fact "$SCRIPT_FACTS" PR3_PARENT)"
CATCHUP_PATH="$(fact "$SCRIPT_FACTS" CATCHUP_PATH)"

if [ "$HEAD_COUNT" != "1" ] || [ -z "$TARGET_HEAD" ]; then
  echo "❌ Expected exactly one Alembic head, got '${HEAD_COUNT:-unknown}'."
  exit 1
fi
echo "📌 Target Alembic head from the new code: $TARGET_HEAD"

if [ "$PR3_IN_DB_LINEAGE" = "1" ]; then
  # PR-3 is already applied — a repeat deploy or any later revision.
  # The constraint swap has happened once and must never be redone or
  # rolled back from here.
  PR3_TRANSITION=0
  echo "ℹ️ PR-3 schema is already in place; ordinary migration flow."
elif [ "$PR3_IN_HEAD_LINEAGE" = "1" ]; then
  # PR-3 must still be exactly one step past the pre-PR-3 revision,
  # otherwise the bounded downgrade below no longer means what it says.
  if [ "$PR3_PARENT" != "$PRE_PR3_REVISION" ]; then
    echo "❌ $PR3_REVISION is no longer a direct child of $PRE_PR3_REVISION"
    echo "   (its parent is '${PR3_PARENT:-unknown}')."
    echo "❗ The bounded PR-3 rollback is no longer well defined. No schema change was made."
    exit 1
  fi

  if [ "$REVISION_BEFORE" = "$PRE_PR3_REVISION" ]; then
    PR3_TRANSITION=1
    echo "🔁 One-time PR-3 transition: $PRE_PR3_REVISION → $PR3_REVISION."
  elif [ "$DB_REVISION_KNOWN" != "1" ] || [ "$PRE_PR3_IS_DESCENDANT" != "1" ]; then
    echo "❌ '${REVISION_BEFORE:-none}' is not a known ancestor of $PRE_PR3_REVISION."
    echo "❗ No audited catch-up path exists from here. No schema change was made."
    exit 1
  else
    # The database is behind. The catch-up is allowed ONLY when the path
    # Alembic actually computed is exactly the audited tail — a new,
    # unreviewed revision inserted into the chain changes this string and
    # stops the deploy instead of silently applying unaudited DDL.
    if ! EXPECTED_CATCHUP="$(expected_catchup_from "$REVISION_BEFORE")"; then
      echo "❌ '${REVISION_BEFORE:-none}' is not on the audited pre-PR-3 catch-up path."
      echo "❗ Audited path: $AUDITED_CATCHUP_PATH"
      echo "❗ Audit the missing revisions by hand first. No schema change was made."
      exit 1
    fi
    if [ "$CATCHUP_PATH" != "$EXPECTED_CATCHUP" ]; then
      echo "❌ The real migration path does not match the audited one."
      echo "   expected: $EXPECTED_CATCHUP"
      echo "   actual:   ${CATCHUP_PATH:-none}"
      echo "❗ A revision was added or reordered since the audit. No schema change was made."
      exit 1
    fi
    PHASE_A_REQUIRED=1
    echo "🧩 Phase A required: $REVISION_BEFORE → $PRE_PR3_REVISION."
    echo "   Audited catch-up revisions: $CATCHUP_PATH"
  fi
else
  PR3_TRANSITION=0
  echo "ℹ️ PR-3 is not part of this revision graph; ordinary migration flow."
fi

# ── Phase A: audited pre-PR-3 catch-up ────────────────────────────
# Runs AFTER the backup and BEFORE the deploy boundary, the worker stop
# and every queue mutation. The legacy inbox worker keeps running and
# keeps processing events throughout: every revision here is additive
# and leaves the constraints the old worker pins by name untouched.
if [ "$PHASE_A_REQUIRED" -eq 1 ]; then
  echo "🧩 Phase A: applying the audited catch-up up to $PRE_PR3_REVISION..."
  echo "   The legacy inbox worker stays RUNNING; no event status is touched."

  # Strictly `upgrade <pre-PR-3>`, never `upgrade head`: head is PR-3, and
  # reaching it here would perform the constraint swap with the old worker
  # still live — exactly the failure this deploy exists to prevent.
  if ! $COMPOSE --profile ops run --rm --no-deps migrate alembic upgrade "$PRE_PR3_REVISION"; then
    echo "❌ Phase A failed."
    echo "❗ No worker was stopped and no event status was changed."
    echo "❗ Catch-up migrations are NOT rolled back automatically; they are additive."
    exit 1
  fi

  # Re-read from the database rather than assuming the upgrade landed.
  if ! REVISION_AFTER_CATCHUP="$(alembic_revision)"; then
    echo "❌ Alembic could not resolve the revision after Phase A."
    echo "❗ No worker was stopped and no event status was changed."
    exit 1
  fi
  if [ "$REVISION_AFTER_CATCHUP" != "$PRE_PR3_REVISION" ]; then
    echo "❌ Revision after Phase A is '${REVISION_AFTER_CATCHUP:-unknown}', expected $PRE_PR3_REVISION."
    echo "❗ No worker was stopped and no event status was changed."
    exit 1
  fi

  PHASE_A_APPLIED=1
  REVISION_BEFORE="$PRE_PR3_REVISION"
  echo "✅ Phase A complete; database is on $PRE_PR3_REVISION."

  # Only an exact match arms Phase B. The classification is redone from
  # the freshly read revision, not inherited from before the catch-up.
  PR3_TRANSITION=1
  echo "🔁 One-time PR-3 transition: $PRE_PR3_REVISION → $PR3_REVISION."
fi

if [ "$PR3_TRANSITION" -eq 1 ]; then
  # ── One-time constraint-swap window ─────────────────────────────
  # A stale canary from an aborted earlier attempt would make every
  # check below ambiguous, and force-removing it could strand rows.
  if docker ps -a --format '{{.Names}}' | grep -Fxq "$CANARY_NAME"; then
    echo "❌ A stale container named $CANARY_NAME already exists."
    echo "❗ Inspect and retire it by hand before deploying. Nothing was stopped."
    exit 1
  fi

  # `stop`, deliberately NOT `down` or `up --force-recreate`: the old
  # container has to survive so recovery can start it again on the old
  # schema. altegio-api is NOT stopped — webhooks keep arriving and
  # wait as `received` until a new worker picks them up.
  #
  # This is NOT a graceful drain: the PARENT image has no SIGTERM
  # handler, so it can be killed between claiming a batch and
  # finishing it. That is precisely why the bounded orphan recovery
  # below exists.
  # Deploy boundary, read from PostgreSQL rather than the runner host:
  # it scopes the constraint-failure count and requeue to THIS deploy,
  # so a pre-existing failed event can neither fail the deploy nor be
  # rewritten. Microsecond precision, taken before anything is stopped.
  DEPLOY_BOUNDARY_EPOCH_US="$(psql_scalar "SELECT (extract(epoch FROM clock_timestamp()) * 1000000)::bigint;")"
  case "$DEPLOY_BOUNDARY_EPOCH_US" in
    ''|*[!0-9]*)
      echo "❌ Could not read a numeric deploy boundary from PostgreSQL."
      exit 1
      ;;
  esac
  if [ "$DEPLOY_BOUNDARY_EPOCH_US" -le 0 ]; then
    echo "❌ Deploy boundary must be positive."
    exit 1
  fi
  echo "📌 Deploy boundary recorded from PostgreSQL."

  echo "🛑 Stopping the legacy altegio-inbox-worker (up to 300s)..."
  $COMPOSE stop -t 300 altegio-inbox-worker
  OLD_WORKER_STOPPED=1
  PR3_TRANSITION_STARTED=1

  STOPPED_ID="$(regular_worker_id)"
  if [ -n "$STOPPED_ID" ] && container_is_running "$STOPPED_ID"; then
    echo "❌ altegio-inbox-worker is still running after the stop."
    exit 1
  fi
  # Label scan, so an operator's ad-hoc one-off worker under any name
  # is caught too — it would otherwise keep claiming rows while the
  # bounded reset below runs.
  if ! require_no_inbox_worker_running; then
    echo "❗ Migration NOT applied."
    exit 1
  fi
  echo "✅ No inbox worker running; altegio-api still accepting webhooks."

  echo "🔎 Altegio events in processing: $(processing_count)"
  if ! recover_orphaned_processing_rows; then
    echo "❗ Migration NOT applied."
    exit 1
  fi

  CONSTRAINT_FAILURES_BEFORE="$(constraint_failure_count)"
  echo "📌 Constraint-swap failures since the deploy boundary: ${CONSTRAINT_FAILURES_BEFORE:-unknown}"
fi

# ── Migration ─────────────────────────────────────────────────────
echo "⚙️ Applying DB Migrations..."
$COMPOSE --profile ops run --rm --no-deps migrate

if ! REVISION_AFTER="$(alembic_revision)"; then
  echo "❌ Alembic could not resolve the revision after the migration."
  exit 1
fi
echo "📌 Alembic revision after migration: ${REVISION_AFTER:-unknown}"
if [ "$REVISION_AFTER" != "$TARGET_HEAD" ]; then
  echo "❌ Expected revision $TARGET_HEAD after migration, got '${REVISION_AFTER:-unknown}'."
  exit 1
fi

# Only a real, completed PR-3 step arms the rollback branch. A no-op
# migration on an already-migrated database never does.
if [ "$PR3_TRANSITION" -eq 1 ] \
  && [ "$REVISION_BEFORE" = "$PRE_PR3_REVISION" ] \
  && [ "$REVISION_AFTER" = "$PR3_REVISION" ] \
  && [ "$REVISION_AFTER" != "$REVISION_BEFORE" ]; then
  PR3_TRANSITION_APPLIED=1
fi

if [ "$PR3_TRANSITION" -eq 1 ]; then
  # ── Canary: prove a NEW-image worker before touching the old one ─
  # A one-off container from the new image, using the same service
  # definition (env, networks). The old container stays intact behind
  # it, which is what makes the recovery path above actually work.
  echo "🐤 Starting the new-image inbox worker canary..."
  CANARY_ID="$($COMPOSE run -d --no-deps --name "$CANARY_NAME" altegio-inbox-worker)"
  if [ -z "$CANARY_ID" ]; then
    echo "❌ Could not start the canary."
    exit 1
  fi

  echo "⏳ Observing the canary..."
  sleep 30

  # Everything is checked against the EXACT container id, never a
  # service-label search that would also match the regular worker.
  CANARY_STATE="$(container_field "$CANARY_ID" '{{.State.Status}}')"
  CANARY_RESTARTS="$(container_field "$CANARY_ID" '{{.RestartCount}}')"
  CANARY_IMAGE="$(container_field "$CANARY_ID" '{{.Image}}')"
  CANARY_ONEOFF="$(container_field "$CANARY_ID" '{{index .Config.Labels "com.docker.compose.oneoff"}}')"
  if [ "$CANARY_STATE" != "running" ]; then
    echo "❌ Canary state is '${CANARY_STATE:-unknown}', expected running."
    exit 1
  fi
  if [ "$CANARY_RESTARTS" != "0" ]; then
    echo "❌ Canary restarted $CANARY_RESTARTS time(s) — crash loop."
    exit 1
  fi
  if [ -z "$CANARY_IMAGE" ]; then
    echo "❌ Could not resolve the canary image."
    exit 1
  fi
  if [ "$CANARY_ONEOFF" != "True" ]; then
    echo "❌ Canary is not a one-off container; refusing to continue."
    exit 1
  fi

  # Safe metadata only: counts of constraint-shaped failures, compared
  # against the baseline taken before the swap. A pre-existing failed
  # event must never trigger a rollback. No payload, error text,
  # phone or customer data is ever printed.
  CONSTRAINT_FAILURES_AFTER="$(constraint_failure_count)"
  echo "📌 Constraint-related failed events now: ${CONSTRAINT_FAILURES_AFTER:-unknown} (baseline ${CONSTRAINT_FAILURES_BEFORE:-unknown})"
  if [ "$CONSTRAINT_FAILURES_AFTER" -gt "$CONSTRAINT_FAILURES_BEFORE" ]; then
    echo "❌ New constraint-related event failures appeared during this deploy."
    exit 1
  fi

  CANARY_VERIFIED=1
  echo "✅ Canary healthy on the new schema; no new constraint-related failures."

  # ── Regular worker, verified by its EXACT Compose container id ───
  echo "🚀 Recreating the regular altegio-inbox-worker from the new image..."
  $COMPOSE up -d --force-recreate altegio-inbox-worker
  sleep 10

  REGULAR_WORKER_IDS="$($COMPOSE ps -q altegio-inbox-worker)"
  REGULAR_WORKER_COUNT="$(printf '%s\n' "$REGULAR_WORKER_IDS" | grep -c '[0-9a-f]')"
  REGULAR_WORKER_ID="$(printf '%s\n' "$REGULAR_WORKER_IDS" | head -n 1)"
  REGULAR_STATE="$(container_field "$REGULAR_WORKER_ID" '{{.State.Status}}')"
  REGULAR_RESTARTS="$(container_field "$REGULAR_WORKER_ID" '{{.RestartCount}}')"
  REGULAR_ONEOFF="$(container_field "$REGULAR_WORKER_ID" '{{index .Config.Labels "com.docker.compose.oneoff"}}')"
  REGULAR_IMAGE="$(container_field "$REGULAR_WORKER_ID" '{{.Image}}')"

  # The canary is left RUNNING on every failure below: it is a
  # compatible worker and removing it would leave nothing processing.
  if [ -z "$REGULAR_WORKER_ID" ] || [ "$REGULAR_WORKER_COUNT" != "1" ]; then
    echo "❌ Expected exactly one regular inbox-worker container, got '${REGULAR_WORKER_COUNT:-0}'."
    exit 1
  fi
  if [ "$REGULAR_WORKER_ID" = "$CANARY_ID" ]; then
    echo "❌ The regular worker id equals the canary id; refusing to continue."
    exit 1
  fi
  if [ "$REGULAR_ONEOFF" = "True" ]; then
    echo "❌ The resolved container is a one-off, not the Compose service container."
    exit 1
  fi
  if [ "$REGULAR_STATE" != "running" ]; then
    echo "❌ The regular inbox worker is '${REGULAR_STATE:-missing}', expected running."
    exit 1
  fi
  if [ "$REGULAR_RESTARTS" != "0" ]; then
    echo "❌ The regular inbox worker restarted $REGULAR_RESTARTS time(s) — crash loop."
    exit 1
  fi
  if [ "$REGULAR_IMAGE" != "$CANARY_IMAGE" ]; then
    echo "❌ The regular worker does not run the image this deploy just verified."
    exit 1
  fi

  REGULAR_WORKER_VERIFIED=1
  echo "✅ Regular inbox worker running ($REGULAR_WORKER_ID) on the verified image."

  # ── Retire the canary, gracefully, only now ──────────────────────
  # Drain is judged from the canary's OWN exit state. A global
  # `processing` count would be meaningless here: the regular worker
  # is live and legitimately claiming rows, so it would race with
  # normal operation instead of proving anything about the canary.
  if ! stop_canary_and_verify_exit; then
    echo "❗ Canary drain is uncertain; leaving the container in place for recovery."
    exit 1
  fi
  remove_stopped_canary
fi

echo "🚀 Starting updated containers..."
$COMPOSE up -d --remove-orphans

# The follow-up worker is NOT attached to the Chatwoot network (it only
# claims due campaign runs and creates MessageJob rows — no Chatwoot API
# calls), so it is verified here for liveness instead of in the Chatwoot
# attachment check below.
echo "🔎 Verifying follow-up worker is running..."
FOLLOWUP_CONTAINER=$(docker ps \
  --filter "label=com.docker.compose.project=altegio_bot" \
  --filter "label=com.docker.compose.service=altegio-followup-worker" \
  --filter "status=running" \
  --format "{{.Names}}" \
  | head -n 1)

if [ -z "$FOLLOWUP_CONTAINER" ]; then
  echo "❌ altegio-followup-worker container is not running"
  exit 1
fi
echo "✅ altegio-followup-worker running ($FOLLOWUP_CONTAINER)"

echo "🔎 Verifying Chatwoot internal route..."

WA_CONTAINER=$(docker ps \
  --filter "label=com.docker.compose.project=altegio_bot" \
  --filter "label=com.docker.compose.service=altegio-whatsapp-inbox-worker" \
  --format "{{.Names}}" \
  | head -n 1)

if [ -z "$WA_CONTAINER" ]; then
  echo "❌ altegio-whatsapp-inbox-worker container not found"
  exit 1
fi

echo "🔎 Resolving Chatwoot internal network from Docker Compose config..."
# Docker Compose applies .env while rendering config. Parse the JSON
# inside an app container so the deploy host needs no extra tools.
CHATWOOT_NETWORK="$($COMPOSE config --format json | $COMPOSE exec -T altegio-whatsapp-inbox-worker /app/.venv/bin/python -c '
import json
import sys

config = json.load(sys.stdin)
try:
    name = config["networks"]["chatwoot_internal"]["name"]
except KeyError as exc:
    raise SystemExit(f"chatwoot_internal network name is missing from compose config: {exc}")

if not name:
    raise SystemExit("chatwoot_internal network name is empty")

print(name)
')"

if [ -z "$CHATWOOT_NETWORK" ]; then
  echo "❌ Failed to resolve chatwoot_internal network name from docker compose config"
  exit 1
fi

echo "Resolved Chatwoot network: $CHATWOOT_NETWORK"
docker network inspect "$CHATWOOT_NETWORK" >/dev/null

echo "🔎 Verifying app containers are attached to Chatwoot network..."

CHATWOOT_SERVICES="
altegio-api
altegio-inbox-worker
altegio-outbox-worker
altegio-whatsapp-inbox-worker
altegio-campaign-worker
"

for SERVICE in $CHATWOOT_SERVICES; do
  CONTAINER=$(docker ps \
    --filter "label=com.docker.compose.project=altegio_bot" \
    --filter "label=com.docker.compose.service=${SERVICE}" \
    --format "{{.Names}}" \
    | head -n 1)

  if [ -z "$CONTAINER" ]; then
    echo "❌ ${SERVICE} container not found"
    exit 1
  fi

  docker inspect -f '{{json .NetworkSettings.Networks}}' "$CONTAINER" \
    | grep -Fq "\"${CHATWOOT_NETWORK}\"" || {
      echo "❌ ${CONTAINER} (${SERVICE}) is not attached to ${CHATWOOT_NETWORK}"
      echo "Container networks:"
      docker inspect -f '{{range $name, $_ := .NetworkSettings.Networks}}{{println $name}}{{end}}' "$CONTAINER" || true
      exit 1
    }

  echo "✅ ${SERVICE} attached to ${CHATWOOT_NETWORK}"
done

echo "🔎 Verifying Chatwoot internal DNS..."
# Resolve the Chatwoot host from the app's own settings so this check
# follows CHATWOOT_BASE_URL instead of a hardcoded hostname.
$COMPOSE exec -T altegio-whatsapp-inbox-worker sh -lc '
CHATWOOT_HOST=$(/app/.venv/bin/python - <<'"'"'PY'"'"'
from urllib.parse import urlparse
from altegio_bot.settings import settings

host = urlparse(settings.chatwoot_base_url).hostname
if not host:
    raise SystemExit("CHATWOOT_BASE_URL host is empty")
print(host)
PY
)

echo "Resolved Chatwoot host: $CHATWOOT_HOST"
getent hosts "$CHATWOOT_HOST" >/dev/null || {
  echo "❌ Cannot resolve Chatwoot host inside altegio-whatsapp-inbox-worker: $CHATWOOT_HOST"
  exit 1
}
'

echo "✅ Chatwoot internal route verified"

echo "🧹 Aggressive cleanup of unused images..."
docker image prune -af

echo "✅ Done! Containers status:"
$COMPOSE ps
