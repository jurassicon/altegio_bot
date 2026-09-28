"""add the EasyWeek production voucher MAILING tables (§42 / PR-19)

§41 proved the whole irreversible sequence — issue, pay, deliver, observe — for
a bounded handful of manually selected people, in production, on 28.09.2026.
§42 is the working mode, and this migration is the storage it needs.

Three things a reader should see immediately:

* these are NEW tables. The §35, §36, §37.2 and §41 tables — their rows, their
  singleton constraints and their historical HMAC bindings — are not touched,
  not widened and not backfilled. §41's batch stays exactly the one batch it
  is; nothing here reinterprets it as a production run.
* there is no recipient ceiling, and that is deliberate. §41 capped itself at
  five recipients and €75 as literals, which was right for an experiment;
  inventing a replacement number here would be a migration deciding how many
  real customers a real campaign may have. What replaces the cap is an
  ARITHMETIC IDENTITY: ``approved_recipient_count = recipient_count``,
  ``approved_exposure_minor = total_exposure_minor`` and
  ``total_exposure_minor = voucher_unit_price_minor * recipient_count`` with the
  unit price pinned to 1500. An operator cannot freeze a batch without stating
  its size and its cost, and the numbers they state cannot describe anything
  other than the composition that was frozen.
* batches are plural, and one preview is frozen once. ``campaign_run_id`` is
  UNIQUE on the header, which is what replaces §41's single-literal scope:
  there is no "the batch" in this phase, so every later stage names an id, and
  a slot cannot escape its batch — items carry the batch's declared size as a
  column and reference ``(id, recipient_count)`` through a composite foreign
  key, with a CHECK requiring ``1 <= slot <= batch_recipient_count``.

The entitlement rule is the one constraint worth reading twice. It is
TABLE-WIDE, over provider, company, campaign, customer and both period bounds,
so a second batch built from a different preview in a different month cannot
hand the same person a second voucher for the same wave — and a race between two
operators freezing two previews for the same person is decided by PostgreSQL
rather than by whichever process checked first.

Three tables, because they answer different questions: the header is the size,
the approved cost and the frozen composition; the items are the state machines
and the uniqueness rules; the attempts table is the redacted outbound intent,
committed before a send. The last is deliberately NOT an ``outbox_messages``
row — that table is swept by a generic worker whose purpose is to retry what it
finds, and the one property these rows must have is that nothing may ever pick
them up and send them again.

Nothing is backfilled and nothing existing is altered. This phase starts empty
by definition.

Revision ID: e2c7b4f16a83
Revises: d7b2f6a4c318
Create Date: 2026-09-28
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "e2c7b4f16a83"
down_revision = "d7b2f6a4c318"
branch_labels = None
depends_on = None

BATCHES = "easyweek_voucher_production_batches"
ITEMS = "easyweek_voucher_production_batch_items"
ATTEMPTS = "easyweek_voucher_production_batch_attempts"

_BATCH_STATUSES = ("frozen", "in_progress", "halted", "completed")
_BATCH_STATUS_SQL = ", ".join(f"'{value}'" for value in _BATCH_STATUSES)

_ITEM_STATUSES = (
    "planned",
    "create_claimed",
    "create_unknown",
    "create_rejected",
    "created",
    "pay_claimed",
    "pay_unknown",
    "pay_rejected",
    "paid",
    "send_claimed",
    "send_unknown",
    "send_rejected",
    "provider_accepted",
    "delivered",
    "read",
    "refund_claimed",
    "refund_unknown",
    "refund_rejected",
    "refunded",
    "manually_cleaned",
    "ambiguous",
)
_ITEM_STATUS_SQL = ", ".join(f"'{value}'" for value in _ITEM_STATUSES)

# Spelled out rather than imported: a migration must keep meaning the same thing
# after the constants move.
_SCOPE = "easyweek_voucher_production_mailing_v1"
_BASIS = "operator_manual_selection"
_KARLSRUHE_COMPANY_ID = 322579
_CAMPAIGN_CODE = "new_clients_monthly"
_UNIT_PRICE_MINOR = 1500


def upgrade() -> None:
    op.create_table(
        BATCHES,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        # -- identity ------------------------------------------------------
        sa.Column("batch_scope", sa.String(length=128), nullable=False),
        sa.Column("request_schema_version", sa.String(length=16), nullable=False),
        sa.Column("baseline_version", sa.String(length=32), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("campaign_code", sa.String(length=128), nullable=False),
        sa.Column("recipient_basis", sa.String(length=32), nullable=False),
        sa.Column("campaign_run_id", sa.BigInteger(), nullable=False),
        sa.Column("campaign_period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("campaign_period_end", sa.DateTime(timezone=True), nullable=False),
        # -- the frozen EasyWeek identity this batch may act on -------------
        sa.Column("location_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("staffer_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("payment_account_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("voucher_template_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        # -- the frozen composition -----------------------------------------
        sa.Column("frozen_digest", sa.String(length=64), nullable=False),
        sa.Column("recipient_count", sa.Integer(), nullable=False),
        sa.Column("voucher_unit_price_minor", sa.Integer(), nullable=False),
        sa.Column("total_exposure_minor", sa.Integer(), nullable=False),
        # -- what the operator explicitly approved --------------------------
        sa.Column("approved_recipient_count", sa.Integer(), nullable=False),
        sa.Column("approved_exposure_minor", sa.Integer(), nullable=False),
        # -- state ----------------------------------------------------------
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("halted_reason_code", sa.String(length=64), nullable=True),
        sa.Column("reconciliation_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        # -- operator authorisation provenance ------------------------------
        sa.Column("freeze_plan_digest", sa.String(length=64), nullable=True),
        sa.Column("frozen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("halted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("evidence", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        # One production batch per preview, ever. This is what replaces §41's
        # single-literal scope: batches are plural, previews are not reusable.
        sa.UniqueConstraint("campaign_run_id", name="uq_ew_voucher_production_batch_run"),
        # The FK target items use to prove their slot is inside this batch's own
        # declared size. Redundant next to the primary key, and load bearing:
        # without it the composite foreign key on items cannot exist.
        sa.UniqueConstraint("id", "recipient_count", name="uq_ew_voucher_production_batch_id_count"),
        sa.ForeignKeyConstraint(
            ["campaign_run_id", "provider"],
            ["campaign_runs.id", "campaign_runs.provider"],
            name="fk_ew_voucher_production_batch_run_provider",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_production_batch_provider"),
        sa.CheckConstraint(f"batch_scope = '{_SCOPE}'", name="ck_ew_voucher_production_batch_scope"),
        sa.CheckConstraint(
            f"company_id = {_KARLSRUHE_COMPANY_ID}",
            name="ck_ew_voucher_production_batch_company",
        ),
        sa.CheckConstraint(
            f"campaign_code = '{_CAMPAIGN_CODE}'",
            name="ck_ew_voucher_production_batch_campaign",
        ),
        sa.CheckConstraint(f"recipient_basis = '{_BASIS}'", name="ck_ew_voucher_production_batch_basis"),
        # At least one recipient. An empty batch is not a small batch: there is
        # nothing to approve. Deliberately NO upper bound — see the docstring.
        sa.CheckConstraint("recipient_count >= 1", name="ck_ew_voucher_production_batch_recipient_count"),
        # The money, as arithmetic the database will not let drift.
        sa.CheckConstraint(
            f"voucher_unit_price_minor = {_UNIT_PRICE_MINOR}",
            name="ck_ew_voucher_production_batch_unit_price",
        ),
        sa.CheckConstraint(
            "total_exposure_minor = voucher_unit_price_minor * recipient_count",
            name="ck_ew_voucher_production_batch_exposure_matches",
        ),
        # And the two numbers the operator approved, equal to the two the freeze
        # computed. This pair is what makes "state the size and the cost" a
        # property of the schema rather than a habit of the CLI.
        sa.CheckConstraint(
            "approved_recipient_count = recipient_count",
            name="ck_ew_voucher_production_batch_count_approved",
        ),
        sa.CheckConstraint(
            "approved_exposure_minor = total_exposure_minor",
            name="ck_ew_voucher_production_batch_exposure_approved",
        ),
        sa.CheckConstraint(
            "campaign_period_start < campaign_period_end",
            name="ck_ew_voucher_production_batch_period_order",
        ),
        sa.CheckConstraint(
            f"status IN ({_BATCH_STATUS_SQL})",
            name="ck_ew_voucher_production_batch_status",
        ),
        sa.CheckConstraint(
            "(status = 'halted') = (halted_reason_code IS NOT NULL)",
            name="ck_ew_voucher_production_batch_halt_has_reason",
        ),
    )
    op.create_index("ix_ew_voucher_production_batch_status", BATCHES, ["status"])

    op.create_table(
        ITEMS,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        # -- membership -----------------------------------------------------
        sa.Column("batch_id", sa.BigInteger(), nullable=False),
        sa.Column("batch_recipient_count", sa.Integer(), nullable=False),
        sa.Column("slot", sa.Integer(), nullable=False),
        # -- identity -------------------------------------------------------
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("campaign_code", sa.String(length=128), nullable=False),
        sa.Column("recipient_basis", sa.String(length=32), nullable=False),
        sa.Column("campaign_run_id", sa.BigInteger(), nullable=False),
        sa.Column("campaign_recipient_id", sa.BigInteger(), nullable=False),
        sa.Column("easyweek_customer_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("campaign_period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("campaign_period_end", sa.DateTime(timezone=True), nullable=False),
        # -- the exact sale this slot authorises ----------------------------
        sa.Column("voucher_value_minor", sa.Integer(), nullable=False),
        sa.Column("voucher_quantity", sa.Integer(), nullable=False),
        sa.Column("reconciliation_marker", sa.String(length=64), nullable=False),
        # -- results --------------------------------------------------------
        sa.Column("target_order_uuid", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("outbound_intent_uuid", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("provider_message_id", sa.String(length=128), nullable=True),
        # -- the voucher, as a keyed proof and nothing else ------------------
        sa.Column("voucher_code_hmac", sa.String(length=64), nullable=True),
        sa.Column("hmac_key_id", sa.String(length=64), nullable=True),
        # -- state ----------------------------------------------------------
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=True),
        sa.Column("manual_cleanup_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("reconciliation_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("manual_cleanup_observed_at", sa.DateTime(timezone=True), nullable=True),
        # -- operator authorisation provenance ------------------------------
        sa.Column("create_plan_digest", sa.String(length=64), nullable=True),
        sa.Column("pay_plan_digest", sa.String(length=64), nullable=True),
        sa.Column("deliver_plan_digest", sa.String(length=64), nullable=True),
        sa.Column("refund_plan_digest", sa.String(length=64), nullable=True),
        # -- claim / attempt / verification, per stage ----------------------
        sa.Column("create_claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("create_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("create_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("create_window_start", sa.DateTime(timezone=True), nullable=True),
        sa.Column("create_window_end", sa.DateTime(timezone=True), nullable=True),
        sa.Column("pay_claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("pay_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("pay_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("live_guard_reproven_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("send_claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("send_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("send_attempt_count", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("provider_accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refund_claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refund_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refund_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("evidence", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        # The slot belongs to this batch AND to this batch's declared size.
        sa.ForeignKeyConstraint(
            ["batch_id", "batch_recipient_count"],
            [f"{BATCHES}.id", f"{BATCHES}.recipient_count"],
            name="fk_ew_voucher_production_item_batch_size",
            ondelete="RESTRICT",
        ),
        # 1 <= slot <= the batch's own declared size. No global ceiling.
        sa.CheckConstraint(
            "slot >= 1 AND slot <= batch_recipient_count AND batch_recipient_count >= 1",
            name="ck_ew_voucher_production_item_slot_range",
        ),
        # Three uniqueness rules for three different mistakes, all scoped to ONE
        # batch: slot numbers repeat across batches by design.
        sa.UniqueConstraint("batch_id", "slot", name="uq_ew_voucher_production_item_slot"),
        sa.UniqueConstraint("batch_id", "campaign_recipient_id", name="uq_ew_voucher_production_item_recipient"),
        sa.UniqueConstraint("batch_id", "easyweek_customer_uuid", name="uq_ew_voucher_production_item_customer"),
        # One voucher per person per campaign period, TABLE-WIDE. The rule that
        # survives a second preview and a second batch, and the one that decides
        # a race between two concurrent freezes.
        sa.UniqueConstraint(
            "provider",
            "company_id",
            "campaign_code",
            "easyweek_customer_uuid",
            "campaign_period_start",
            "campaign_period_end",
            name="uq_ew_voucher_production_item_entitlement",
        ),
        # A result may belong to exactly one slot, table-wide.
        sa.UniqueConstraint("reconciliation_marker", name="uq_ew_voucher_production_item_marker"),
        sa.UniqueConstraint("target_order_uuid", name="uq_ew_voucher_production_item_target_order"),
        sa.UniqueConstraint("outbound_intent_uuid", name="uq_ew_voucher_production_item_intent"),
        sa.UniqueConstraint("provider_message_id", name="uq_ew_voucher_production_item_provider_message"),
        sa.ForeignKeyConstraint(
            ["campaign_recipient_id", "provider"],
            ["campaign_recipients.id", "campaign_recipients.provider"],
            name="fk_ew_voucher_production_item_recipient_provider",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["campaign_run_id", "provider"],
            ["campaign_runs.id", "campaign_runs.provider"],
            name="fk_ew_voucher_production_item_run_provider",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            f"status IN ({_ITEM_STATUS_SQL})",
            name="ck_ew_voucher_production_item_status",
        ),
        sa.CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_production_item_provider"),
        sa.CheckConstraint(
            f"company_id = {_KARLSRUHE_COMPANY_ID}",
            name="ck_ew_voucher_production_item_company",
        ),
        sa.CheckConstraint(
            f"campaign_code = '{_CAMPAIGN_CODE}'",
            name="ck_ew_voucher_production_item_campaign",
        ),
        sa.CheckConstraint(f"recipient_basis = '{_BASIS}'", name="ck_ew_voucher_production_item_basis"),
        # Exactly one voucher of exactly €15. Not a default and not a maximum.
        sa.CheckConstraint(
            f"voucher_value_minor = {_UNIT_PRICE_MINOR} AND voucher_quantity = 1",
            name="ck_ew_voucher_production_item_exact_voucher",
        ),
        sa.CheckConstraint(
            "campaign_period_start < campaign_period_end",
            name="ck_ew_voucher_production_item_period_order",
        ),
        # Stage order.
        sa.CheckConstraint(
            "(create_attempted_at IS NULL OR create_claimed_at IS NOT NULL) "
            "AND (create_verified_at IS NULL OR create_attempted_at IS NOT NULL) "
            "AND (pay_attempted_at IS NULL OR pay_claimed_at IS NOT NULL) "
            "AND (pay_verified_at IS NULL OR pay_attempted_at IS NOT NULL) "
            "AND (send_attempted_at IS NULL OR send_claimed_at IS NOT NULL) "
            "AND (refund_attempted_at IS NULL OR refund_claimed_at IS NOT NULL) "
            "AND (refund_verified_at IS NULL OR refund_attempted_at IS NOT NULL)",
            name="ck_ew_voucher_production_item_stage_order",
        ),
        # Money cannot move before the order it pays for was proven to exist.
        sa.CheckConstraint(
            "pay_claimed_at IS NULL OR (create_verified_at IS NOT NULL AND target_order_uuid IS NOT NULL)",
            name="ck_ew_voucher_production_item_pay_needs_created",
        ),
        # Nothing may be sent before the voucher is proven paid for, bound by
        # MAC, and proven still deliverable by a guard taken AFTER the payment.
        sa.CheckConstraint(
            "send_claimed_at IS NULL OR ("
            "pay_verified_at IS NOT NULL "
            "AND voucher_code_hmac IS NOT NULL "
            "AND hmac_key_id IS NOT NULL "
            "AND live_guard_reproven_at IS NOT NULL "
            "AND live_guard_reproven_at >= pay_verified_at)",
            name="ck_ew_voucher_production_item_send_needs_paid",
        ),
        # Acceptance needs both the attempt and Meta's identifier; the webhook
        # ladder may not be climbed out of order. This is what makes
        # "completed is not read" a property of the schema.
        sa.CheckConstraint(
            "provider_accepted_at IS NULL OR (send_attempted_at IS NOT NULL AND provider_message_id IS NOT NULL)",
            name="ck_ew_voucher_production_item_accepted_needs_attempt",
        ),
        sa.CheckConstraint(
            "delivered_at IS NULL OR provider_accepted_at IS NOT NULL",
            name="ck_ew_voucher_production_item_delivered_needs_accepted",
        ),
        sa.CheckConstraint(
            "read_at IS NULL OR delivered_at IS NOT NULL",
            name="ck_ew_voucher_production_item_read_needs_delivered",
        ),
        # A refund is only ever the pre-send escape hatch.
        sa.CheckConstraint(
            "refund_claimed_at IS NULL OR ("
            "provider_accepted_at IS NULL "
            "AND delivered_at IS NULL "
            "AND read_at IS NULL "
            "AND send_claimed_at IS NULL "
            "AND send_attempted_at IS NULL)",
            name="ck_ew_voucher_production_item_refund_is_pre_send",
        ),
        sa.CheckConstraint(
            "status <> 'refunded' OR (provider_accepted_at IS NULL AND delivered_at IS NULL AND read_at IS NULL)",
            name="ck_ew_voucher_production_item_refunded_never_sent",
        ),
        # At most one delivery attempt in the lifetime of this slot.
        sa.CheckConstraint(
            "send_attempt_count >= 0 AND send_attempt_count <= 1",
            name="ck_ew_voucher_production_item_single_attempt",
        ),
        sa.CheckConstraint(
            "(send_attempt_count = 0) = (send_attempted_at IS NULL)",
            name="ck_ew_voucher_production_item_attempt_count_matches",
        ),
        sa.CheckConstraint(
            "(voucher_code_hmac IS NULL) = (hmac_key_id IS NULL)",
            name="ck_ew_voucher_production_item_hmac_pair",
        ),
    )
    op.create_index("ix_ew_voucher_production_item_batch", ITEMS, ["batch_id"])
    op.create_index("ix_ew_voucher_production_item_status", ITEMS, ["status"])
    op.create_index("ix_ew_voucher_production_item_run", ITEMS, ["campaign_run_id"])
    op.create_index("ix_ew_voucher_production_item_recipient", ITEMS, ["campaign_recipient_id"])
    # The header settle reads its items by (batch, status). Per batch, which is
    # what keeps a mailing of fifty from costing fifty full-table reads.
    op.create_index("ix_ew_voucher_production_item_batch_status", ITEMS, ["batch_id", "status"])

    op.create_table(
        ATTEMPTS,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("item_id", sa.BigInteger(), nullable=False),
        sa.Column("intent_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("template_code", sa.String(length=64), nullable=False),
        sa.Column("meta_template_name", sa.String(length=128), nullable=False),
        sa.Column("template_language", sa.String(length=8), nullable=False),
        sa.Column("sender_id", sa.BigInteger(), nullable=True),
        sa.Column("campaign_recipient_id", sa.BigInteger(), nullable=False),
        sa.Column("batch_id", sa.BigInteger(), nullable=False),
        sa.Column("slot", sa.Integer(), nullable=False),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=True),
        sa.Column("provider_message_id", sa.String(length=128), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("intent_uuid", name="uq_ew_voucher_production_attempt_intent"),
        # RESTRICT in both directions of intent: the audit of a real send
        # attempt must not disappear with the slot, nor allow it to be deleted.
        sa.ForeignKeyConstraint(
            ["item_id"],
            [f"{ITEMS}.id"],
            name="fk_ew_voucher_production_attempt_item",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "outcome IN ('claimed', 'provider_accepted', 'unknown', 'rejected')",
            name="ck_ew_voucher_production_attempt_outcome",
        ),
        sa.CheckConstraint(
            "outcome <> 'provider_accepted' OR provider_message_id IS NOT NULL",
            name="ck_ew_voucher_production_attempt_accepted_has_id",
        ),
    )
    op.create_index("ix_ew_voucher_production_attempt_item", ATTEMPTS, ["item_id"])


def downgrade() -> None:
    # Only the three objects this revision created, and in reverse dependency
    # order: the FKs are RESTRICT, and the audit rows reference the items, which
    # reference the header. Nothing belonging to §35, §36, §37.2 or §41 is
    # touched — this revision never altered any of their tables, so there is
    # nothing of theirs to put back.
    op.drop_index("ix_ew_voucher_production_attempt_item", table_name=ATTEMPTS)
    op.drop_table(ATTEMPTS)
    op.drop_index("ix_ew_voucher_production_item_batch_status", table_name=ITEMS)
    op.drop_index("ix_ew_voucher_production_item_recipient", table_name=ITEMS)
    op.drop_index("ix_ew_voucher_production_item_run", table_name=ITEMS)
    op.drop_index("ix_ew_voucher_production_item_status", table_name=ITEMS)
    op.drop_index("ix_ew_voucher_production_item_batch", table_name=ITEMS)
    op.drop_table(ITEMS)
    op.drop_index("ix_ew_voucher_production_batch_status", table_name=BATCHES)
    op.drop_table(BATCHES)
