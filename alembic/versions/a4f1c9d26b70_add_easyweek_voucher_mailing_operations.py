"""add browser-authorised operation storage for the voucher mailing (§43 / PR-20)

§42 proved the stages and drove them from a terminal, where the operator carried
the plan digest and the timestamp by hand. The owner's decision of 28.09.2026 is
that the real mailing happens from the interface, and that moves exactly one
thing: where the authorisation lives. This migration is the storage it needs.

What a reader should see immediately:

* these are NEW tables. §§35–37.2, §41 and §42's own three tables — their rows,
  their constraints and their historical HMAC bindings — are not touched, not
  widened and not backfilled. Every production batch, item, attempt and ledger
  row that exists keeps its exact meaning, and nothing here reinterprets one.
* the approval is immutable and expires. It holds the authenticated principal,
  the branch, the preview, the batch, the stage, the EXACT target slots, the
  count and the money, plus the verdicts about the pinned issuer and the frozen
  identity. The browser gets an id and confirms that; it does not assemble
  permission out of request fields.
* ``operations.approval_id`` is UNIQUE. That one constraint is what a
  double-click, a POST retried after a lost response, a refresh, two tabs and two
  operators all collide on — in PostgreSQL, not in a disabled button. One
  approval becomes at most one operation, ever.
* the operation status vocabulary has no retryable state. ``interrupted`` is
  terminal: a stage whose executor died may have left a committed claim with its
  request in flight, so continuing means a readback and a NEW approval for
  whatever is provably untouched. There is deliberately no transition back to
  ``queued`` anywhere in the schema or the code.
* the stop request is one row per batch, UNIQUE on ``batch_id``, so pressing stop
  twice is the same request. It is read inside the same transaction as the next
  per-item claim, after the batch header's row lock, which is what makes "no
  further claim is granted" atomic rather than best effort.

Four tables because they answer four different questions: what was offered, what
was spent, what the operator asked to stop, and who did it.

Downgrade drops exactly these four and nothing else. It is safe with historical
ledgers present because nothing in §§35–42 references them: the foreign keys all
point the other way, from the new tables into ``campaign_runs`` and the §42 batch
header, with RESTRICT so a batch cannot be deleted out from under an audit trail.

PII-free throughout, like every other table of this phase: slots, ids, counts,
booleans, digests and reason codes. No phone, no name, no customer UUID, no
voucher code, no staffer UUID, no cookie and no session token.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision = "a4f1c9d26b70"
down_revision = "e2c7b4f16a83"
branch_labels = None
depends_on = None

APPROVALS = "easyweek_voucher_production_approvals"
OPERATIONS = "easyweek_voucher_production_operations"
STOPS = "easyweek_voucher_production_stop_requests"
AUDIT = "easyweek_voucher_production_audit"

BATCHES = "easyweek_voucher_production_batches"

# Spelled out rather than imported: a migration must keep meaning the same thing
# after the constants move.
_SCOPE = "easyweek_voucher_production_mailing_v1"
_COMPANY_ID = 322579

_STAGES = ("freeze", "create", "pay", "deliver", "refund")
_STAGE_SQL = ", ".join(f"'{value}'" for value in _STAGES)

_APPROVAL_STATUSES = ("pending", "consumed")
_APPROVAL_STATUS_SQL = ", ".join(f"'{value}'" for value in _APPROVAL_STATUSES)

_OPERATION_STATUSES = ("queued", "running", "completed", "refused", "expired", "interrupted")
_OPERATION_STATUS_SQL = ", ".join(f"'{value}'" for value in _OPERATION_STATUSES)

_OPERATION_TERMINAL = ("completed", "refused", "expired", "interrupted")
_OPERATION_TERMINAL_SQL = ", ".join(f"'{value}'" for value in _OPERATION_TERMINAL)


def upgrade() -> None:
    op.create_table(
        APPROVALS,
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("batch_scope", sa.String(length=128), nullable=False),
        sa.Column("request_schema_version", sa.String(length=16), nullable=False),
        sa.Column("provider", sa.String(length=32), server_default=sa.text("'altegio'"), nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("stage", sa.String(length=32), nullable=False),
        # Who, as the SERVER resolved them. Never an actor from a payload.
        sa.Column("principal", sa.String(length=128), nullable=False),
        sa.Column("session_fingerprint", sa.String(length=64), nullable=False),
        # What one shared Ops credential can and cannot prove, recorded rather
        # than implied by its absence.
        sa.Column("identification_limit", sa.String(length=32), nullable=False),
        sa.Column("campaign_run_id", sa.BigInteger(), nullable=False),
        sa.Column("batch_id", sa.BigInteger(), nullable=True),
        # The §42.7 fix, stored: execution acts on exactly these slots, and a slot
        # that becomes actionable afterwards belongs to the NEXT plan.
        sa.Column("target_slots", JSONB(), nullable=False),
        sa.Column("target_slot_count", sa.Integer(), nullable=False),
        sa.Column("stage_target_count", sa.Integer(), nullable=False),
        sa.Column("stage_amount_minor", sa.Integer(), nullable=False),
        sa.Column("batch_recipient_count", sa.Integer(), nullable=False),
        sa.Column("batch_exposure_minor", sa.Integer(), nullable=False),
        sa.Column("campaign_period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("campaign_period_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("plan_digest", sa.String(length=64), nullable=False),
        sa.Column("plan_issued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("issuer_pinned", sa.Boolean(), nullable=False),
        sa.Column("issuer_membership_proven", sa.Boolean(), nullable=False),
        sa.Column("runtime_identity_bound", sa.Boolean(), nullable=False),
        sa.Column("baseline_version", sa.String(length=32), nullable=False),
        sa.Column("frozen_digest", sa.String(length=64), nullable=True),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_production_approval_provider"),
        sa.CheckConstraint(f"batch_scope = '{_SCOPE}'", name="ck_ew_voucher_production_approval_scope"),
        sa.CheckConstraint(f"company_id = {_COMPANY_ID}", name="ck_ew_voucher_production_approval_company"),
        sa.CheckConstraint(f"stage IN ({_STAGE_SQL})", name="ck_ew_voucher_production_approval_stage"),
        sa.CheckConstraint(f"status IN ({_APPROVAL_STATUS_SQL})", name="ck_ew_voucher_production_approval_status"),
        # Consumed exactly when there is a moment it was consumed at, so "spent"
        # is one fact rather than two that might disagree.
        sa.CheckConstraint(
            "(status = 'consumed') = (consumed_at IS NOT NULL)",
            name="ck_ew_voucher_production_approval_consumed",
        ),
        # An approval covering no slot authorises nothing, and an empty list is
        # exactly what an executor could misread as "no restriction".
        sa.CheckConstraint("target_slot_count >= 1", name="ck_ew_voucher_production_approval_slots"),
        sa.CheckConstraint("stage_target_count >= 1", name="ck_ew_voucher_production_approval_targets"),
        sa.CheckConstraint("stage_amount_minor >= 0", name="ck_ew_voucher_production_approval_amount"),
        # Queue time is not reading time: the window is measured from when the
        # plan was ISSUED, and it has to be a window at all.
        sa.CheckConstraint("expires_at > plan_issued_at", name="ck_ew_voucher_production_approval_ttl"),
        # Every stage after the freeze is about one existing batch, by id.
        sa.CheckConstraint(
            "(stage = 'freeze') OR (batch_id IS NOT NULL)",
            name="ck_ew_voucher_production_approval_batch",
        ),
        sa.ForeignKeyConstraint(
            ["campaign_run_id", "provider"],
            ["campaign_runs.id", "campaign_runs.provider"],
            name="fk_ew_voucher_production_approval_run_provider",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["batch_id"],
            [f"{BATCHES}.id"],
            name="fk_ew_voucher_production_approval_batch",
            ondelete="RESTRICT",
        ),
    )
    op.create_index(
        "ix_ew_voucher_production_approval_batch_stage",
        APPROVALS,
        ["batch_id", "stage"],
    )
    op.create_index("ix_ew_voucher_production_approval_status", APPROVALS, ["status"])

    op.create_table(
        OPERATIONS,
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("approval_id", sa.BigInteger(), nullable=False),
        sa.Column("batch_scope", sa.String(length=128), nullable=False),
        sa.Column("provider", sa.String(length=32), server_default=sa.text("'altegio'"), nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("stage", sa.String(length=32), nullable=False),
        sa.Column("campaign_run_id", sa.BigInteger(), nullable=False),
        sa.Column("batch_id", sa.BigInteger(), nullable=True),
        sa.Column("slot", sa.Integer(), nullable=True),
        sa.Column("principal", sa.String(length=128), nullable=False),
        sa.Column("session_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("identification_limit", sa.String(length=32), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("lease_owner", sa.String(length=128), nullable=True),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("queued_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("outcome_code", sa.String(length=64), nullable=True),
        sa.Column("reason_codes", JSONB(), server_default=sa.text("'[]'::jsonb"), nullable=False),
        sa.Column("result", JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        # The idempotency key of the whole phase.
        sa.UniqueConstraint("approval_id", name="uq_ew_voucher_production_operation_approval"),
        sa.CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_production_operation_provider"),
        sa.CheckConstraint(f"batch_scope = '{_SCOPE}'", name="ck_ew_voucher_production_operation_scope"),
        sa.CheckConstraint(f"stage IN ({_STAGE_SQL})", name="ck_ew_voucher_production_operation_stage"),
        sa.CheckConstraint(f"status IN ({_OPERATION_STATUS_SQL})", name="ck_ew_voucher_production_operation_status"),
        sa.CheckConstraint(
            f"(status IN ({_OPERATION_TERMINAL_SQL})) = (finished_at IS NOT NULL)",
            name="ck_ew_voucher_production_operation_finished",
        ),
        # A queued operation has not started and holds no lease, so a sweep can
        # never mistake a waiting row for an abandoned one.
        sa.CheckConstraint(
            "status <> 'queued' OR (started_at IS NULL AND lease_owner IS NULL AND lease_expires_at IS NULL)",
            name="ck_ew_voucher_production_operation_queued_idle",
        ),
        sa.CheckConstraint("attempts >= 0", name="ck_ew_voucher_production_operation_attempts"),
        sa.ForeignKeyConstraint(
            ["approval_id"],
            [f"{APPROVALS}.id"],
            name="fk_ew_voucher_production_operation_approval",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["batch_id"],
            [f"{BATCHES}.id"],
            name="fk_ew_voucher_production_operation_batch",
            ondelete="RESTRICT",
        ),
    )
    op.create_index("ix_ew_voucher_production_operation_status", OPERATIONS, ["status"])
    op.create_index("ix_ew_voucher_production_operation_batch", OPERATIONS, ["batch_id"])

    op.create_table(
        STOPS,
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("batch_id", sa.BigInteger(), nullable=False),
        sa.Column("requested_by", sa.String(length=128), nullable=False),
        sa.Column("requested_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("cleared_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("cleared_by", sa.String(length=128), nullable=True),
        sa.Column("stop_count", sa.Integer(), server_default=sa.text("1"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        # One row per batch, ever: pressing stop twice is the same request.
        sa.UniqueConstraint("batch_id", name="uq_ew_voucher_production_stop_batch"),
        sa.ForeignKeyConstraint(
            ["batch_id"],
            [f"{BATCHES}.id"],
            name="fk_ew_voucher_production_stop_batch",
            ondelete="RESTRICT",
        ),
    )
    op.create_index("ix_ew_voucher_production_stop_active", STOPS, ["batch_id", "cleared_at"])

    op.create_table(
        AUDIT,
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("provider", sa.String(length=32), server_default=sa.text("'altegio'"), nullable=False),
        sa.Column("principal", sa.String(length=128), nullable=False),
        sa.Column("session_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("identification_limit", sa.String(length=32), nullable=False),
        sa.Column("action", sa.String(length=32), nullable=False),
        sa.Column("stage", sa.String(length=32), nullable=True),
        sa.Column("campaign_run_id", sa.BigInteger(), nullable=True),
        sa.Column("batch_id", sa.BigInteger(), nullable=True),
        sa.Column("approval_id", sa.BigInteger(), nullable=True),
        sa.Column("operation_id", sa.BigInteger(), nullable=True),
        sa.Column("outcome", sa.String(length=64), nullable=False),
        sa.Column("detail", JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_production_audit_provider"),
    )
    # Deliberately no foreign keys on the audit's id columns. An audit row has to
    # outlive what it describes and has to be writable even when the thing it is
    # about was refused before it existed; a FK would make the record of an action
    # depend on the action having succeeded.
    op.create_index("ix_ew_voucher_production_audit_at", AUDIT, ["at"])
    op.create_index("ix_ew_voucher_production_audit_batch", AUDIT, ["batch_id"])


def downgrade() -> None:
    # Dropped newest-dependency-first. Operations reference approvals, so they go
    # before them; nothing in §§35–42 references any of these four, which is why
    # a downgrade leaves every historical ledger, binding and constraint exactly
    # as it was. What a downgrade DOES lose is the record of who authorised what,
    # so it belongs to a rollback of this PR and not to routine operation.
    op.drop_index("ix_ew_voucher_production_audit_batch", table_name=AUDIT)
    op.drop_index("ix_ew_voucher_production_audit_at", table_name=AUDIT)
    op.drop_table(AUDIT)

    op.drop_index("ix_ew_voucher_production_stop_active", table_name=STOPS)
    op.drop_table(STOPS)

    op.drop_index("ix_ew_voucher_production_operation_batch", table_name=OPERATIONS)
    op.drop_index("ix_ew_voucher_production_operation_status", table_name=OPERATIONS)
    op.drop_table(OPERATIONS)

    op.drop_index("ix_ew_voucher_production_approval_status", table_name=APPROVALS)
    op.drop_index("ix_ew_voucher_production_approval_batch_stage", table_name=APPROVALS)
    op.drop_table(APPROVALS)
