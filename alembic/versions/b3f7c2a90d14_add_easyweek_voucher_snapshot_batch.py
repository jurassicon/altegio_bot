"""add the EasyWeek controlled voucher snapshot BATCH tables (§41 / PR-18)

§37.2 proved the whole irreversible sequence — issue, pay, deliver, observe —
for exactly one manually selected person, in production. §41 repeats it for a
bounded handful of them, and the only new question is *how many*.

This migration is the answer. Three things a reader should see immediately:

* the header table can hold at most ONE row. ``batch_scope`` is unique AND
  pinned to a single literal by a CHECK, so a second batch is not a flag
  somebody flips twice — it is a code change plus a migration plus a review.
* a slot cannot escape its batch. Items carry the batch's declared size as a
  column and reference ``(id, recipient_count)`` of the header through a
  composite foreign key, and a CHECK then requires
  ``1 <= slot <= batch_recipient_count <= 5``. The sixth recipient is not a
  case the application rejects; it is a row PostgreSQL will not store.
* the money is derived, never asserted. ``total_exposure_minor`` must equal
  ``voucher_unit_price_minor * recipient_count``, the unit price is pinned to
  1500 and the count is capped at five, so €75 is the arithmetic ceiling of
  this schema rather than a promise in a document.

Three tables, because they answer different questions: the header is the size
and the frozen composition; the items are the state machines and the
uniqueness rules; the attempts table is the redacted outbound intent, committed
before a send. The last is deliberately NOT an ``outbox_messages`` row — that
table is swept by a generic worker whose purpose is to retry what it finds, and
the one property these rows must have is that nothing may ever pick them up and
send them again.

Nothing is backfilled and nothing existing is touched. This batch starts empty
by definition, and the §35, §36 and §37.2 tables — including their historical
canary rows — are left exactly as they are.

Revision ID: b3f7c2a90d14
Revises: a7d4f2e81c95
Create Date: 2026-09-22
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "b3f7c2a90d14"
down_revision = "a7d4f2e81c95"
branch_labels = None
depends_on = None

BATCHES = "easyweek_voucher_snapshot_batches"
ITEMS = "easyweek_voucher_snapshot_batch_items"
ATTEMPTS = "easyweek_voucher_snapshot_batch_attempts"

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
_SCOPE = "easyweek_voucher_snapshot_batch_v1"
_BASIS = "operator_manual_selection"
_KARLSRUHE_COMPANY_ID = 322579
_CAMPAIGN_CODE = "new_clients_monthly"
_MAX_RECIPIENTS = 5
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
        # 1-2. One batch, ever — unique AND pinned to the single approved scope.
        # Together these mean this table can physically hold at most one row.
        sa.UniqueConstraint("batch_scope", name="uq_ew_voucher_batch_scope"),
        sa.CheckConstraint(f"batch_scope = '{_SCOPE}'", name="ck_ew_voucher_batch_single_scope"),
        # 3. The FK target items use to prove their slot is inside this batch's
        # own declared size. Redundant next to the primary key, and load
        # bearing: without it the composite foreign key cannot exist.
        sa.UniqueConstraint("id", "recipient_count", name="uq_ew_voucher_batch_id_count"),
        # 4. The preview this batch was frozen from, under the same provider.
        sa.ForeignKeyConstraint(
            ["campaign_run_id", "provider"],
            ["campaign_runs.id", "campaign_runs.provider"],
            name="fk_ew_voucher_batch_run_provider",
            ondelete="RESTRICT",
        ),
        # 5-8. The topology the owner approved, as literals rather than as
        # configuration a misconfigured environment could point elsewhere.
        sa.CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_batch_provider"),
        sa.CheckConstraint(
            f"company_id = {_KARLSRUHE_COMPANY_ID}",
            name="ck_ew_voucher_batch_company",
        ),
        sa.CheckConstraint(
            f"campaign_code = '{_CAMPAIGN_CODE}'",
            name="ck_ew_voucher_batch_campaign",
        ),
        sa.CheckConstraint(f"recipient_basis = '{_BASIS}'", name="ck_ew_voucher_batch_basis"),
        # 9. One to five. The sixth slot has nowhere to go.
        sa.CheckConstraint(
            f"recipient_count >= 1 AND recipient_count <= {_MAX_RECIPIENTS}",
            name="ck_ew_voucher_batch_recipient_count",
        ),
        # 10-11. The money, derived rather than asserted. €15 each, and a total
        # that must be exactly the product — so €75 is the ceiling of the table.
        sa.CheckConstraint(
            f"voucher_unit_price_minor = {_UNIT_PRICE_MINOR}",
            name="ck_ew_voucher_batch_unit_price",
        ),
        sa.CheckConstraint(
            "total_exposure_minor = voucher_unit_price_minor * recipient_count",
            name="ck_ew_voucher_batch_exposure_matches",
        ),
        # 12. A campaign period is an interval.
        sa.CheckConstraint(
            "campaign_period_start < campaign_period_end",
            name="ck_ew_voucher_batch_period_order",
        ),
        # 13-14. A closed status vocabulary, and a halt that must say why.
        sa.CheckConstraint(f"status IN ({_BATCH_STATUS_SQL})", name="ck_ew_voucher_batch_status"),
        sa.CheckConstraint(
            "(status = 'halted') = (halted_reason_code IS NOT NULL)",
            name="ck_ew_voucher_batch_halt_has_reason",
        ),
    )

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
        # -- results ---------------------------------------------------------
        sa.Column("target_order_uuid", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("outbound_intent_uuid", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("provider_message_id", sa.String(length=128), nullable=True),
        # -- the voucher, as a keyed proof and nothing else -------------------
        sa.Column("voucher_code_hmac", sa.String(length=64), nullable=True),
        sa.Column("hmac_key_id", sa.String(length=64), nullable=True),
        # -- state ------------------------------------------------------------
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=True),
        sa.Column("manual_cleanup_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("reconciliation_required", sa.Boolean(), nullable=False, server_default=sa.text("false")),
        sa.Column("manual_cleanup_observed_at", sa.DateTime(timezone=True), nullable=True),
        # -- operator authorisation provenance --------------------------------
        sa.Column("create_plan_digest", sa.String(length=64), nullable=True),
        sa.Column("pay_plan_digest", sa.String(length=64), nullable=True),
        sa.Column("deliver_plan_digest", sa.String(length=64), nullable=True),
        sa.Column("refund_plan_digest", sa.String(length=64), nullable=True),
        # -- claim / attempt / verification, per stage ------------------------
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
        # 1. The slot belongs to this batch AND to this batch's declared size.
        sa.ForeignKeyConstraint(
            ["batch_id", "batch_recipient_count"],
            [f"{BATCHES}.id", f"{BATCHES}.recipient_count"],
            name="fk_ew_voucher_batch_item_batch_size",
            ondelete="RESTRICT",
        ),
        # 2. 1 <= slot <= the batch's own size <= 5. The sixth slot has no home.
        sa.CheckConstraint(
            "slot >= 1 AND slot <= batch_recipient_count "
            f"AND batch_recipient_count >= 1 AND batch_recipient_count <= {_MAX_RECIPIENTS}",
            name="ck_ew_voucher_batch_item_slot_range",
        ),
        # 3-5. Three uniqueness rules for three different mistakes: the same
        # preview row twice, two preview rows for one human, and a slot reused.
        sa.UniqueConstraint("batch_id", "slot", name="uq_ew_voucher_batch_item_slot"),
        sa.UniqueConstraint("batch_id", "campaign_recipient_id", name="uq_ew_voucher_batch_item_recipient"),
        sa.UniqueConstraint("batch_id", "easyweek_customer_uuid", name="uq_ew_voucher_batch_item_customer"),
        # 6. One voucher per person per campaign period, table-wide.
        sa.UniqueConstraint(
            "provider",
            "company_id",
            "campaign_code",
            "easyweek_customer_uuid",
            "campaign_period_start",
            "campaign_period_end",
            name="uq_ew_voucher_batch_item_entitlement",
        ),
        # 7-10. A result may belong to exactly one slot.
        sa.UniqueConstraint("reconciliation_marker", name="uq_ew_voucher_batch_item_marker"),
        sa.UniqueConstraint("target_order_uuid", name="uq_ew_voucher_batch_item_target_order"),
        sa.UniqueConstraint("outbound_intent_uuid", name="uq_ew_voucher_batch_item_intent"),
        sa.UniqueConstraint("provider_message_id", name="uq_ew_voucher_batch_item_provider_message"),
        # 11. The recipient and the run, under the same provider. Composite FKs,
        # because two separate ones would each be satisfied by rows that have
        # nothing to do with each other.
        sa.ForeignKeyConstraint(
            ["campaign_recipient_id", "provider"],
            ["campaign_recipients.id", "campaign_recipients.provider"],
            name="fk_ew_voucher_batch_item_recipient_provider",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["campaign_run_id", "provider"],
            ["campaign_runs.id", "campaign_runs.provider"],
            name="fk_ew_voucher_batch_item_run_provider",
            ondelete="RESTRICT",
        ),
        # 12-15. The approved topology and a closed status vocabulary.
        sa.CheckConstraint(f"status IN ({_ITEM_STATUS_SQL})", name="ck_ew_voucher_batch_item_status"),
        sa.CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_batch_item_provider"),
        sa.CheckConstraint(
            f"company_id = {_KARLSRUHE_COMPANY_ID}",
            name="ck_ew_voucher_batch_item_company",
        ),
        sa.CheckConstraint(
            f"campaign_code = '{_CAMPAIGN_CODE}'",
            name="ck_ew_voucher_batch_item_campaign",
        ),
        sa.CheckConstraint(f"recipient_basis = '{_BASIS}'", name="ck_ew_voucher_batch_item_basis"),
        # 16. Exactly one voucher of exactly €15. Not a default and not a
        # maximum: the two numbers this slot is allowed to be worth.
        sa.CheckConstraint(
            f"voucher_value_minor = {_UNIT_PRICE_MINOR} AND voucher_quantity = 1",
            name="ck_ew_voucher_batch_item_exact_voucher",
        ),
        sa.CheckConstraint(
            "campaign_period_start < campaign_period_end",
            name="ck_ew_voucher_batch_item_period_order",
        ),
        # 17. Stage order.
        sa.CheckConstraint(
            "(create_attempted_at IS NULL OR create_claimed_at IS NOT NULL) "
            "AND (create_verified_at IS NULL OR create_attempted_at IS NOT NULL) "
            "AND (pay_attempted_at IS NULL OR pay_claimed_at IS NOT NULL) "
            "AND (pay_verified_at IS NULL OR pay_attempted_at IS NOT NULL) "
            "AND (send_attempted_at IS NULL OR send_claimed_at IS NOT NULL) "
            "AND (refund_attempted_at IS NULL OR refund_claimed_at IS NOT NULL) "
            "AND (refund_verified_at IS NULL OR refund_attempted_at IS NOT NULL)",
            name="ck_ew_voucher_batch_item_stage_order",
        ),
        # 18. Money cannot move before the order it pays for was proven to exist.
        sa.CheckConstraint(
            "pay_claimed_at IS NULL OR (create_verified_at IS NOT NULL AND target_order_uuid IS NOT NULL)",
            name="ck_ew_voucher_batch_item_pay_needs_created",
        ),
        # 19. Nothing may be sent before the voucher is proven paid for, bound
        # to this row by MAC, and proven still deliverable by a guard taken
        # AFTER the payment.
        sa.CheckConstraint(
            "send_claimed_at IS NULL OR ("
            "pay_verified_at IS NOT NULL "
            "AND voucher_code_hmac IS NOT NULL "
            "AND hmac_key_id IS NOT NULL "
            "AND live_guard_reproven_at IS NOT NULL "
            "AND live_guard_reproven_at >= pay_verified_at)",
            name="ck_ew_voucher_batch_item_send_needs_paid",
        ),
        # 20-22. Acceptance needs both the attempt and Meta's identifier; the
        # webhook ladder may not be climbed out of order.
        sa.CheckConstraint(
            "provider_accepted_at IS NULL OR (send_attempted_at IS NOT NULL AND provider_message_id IS NOT NULL)",
            name="ck_ew_voucher_batch_item_accepted_needs_attempt",
        ),
        sa.CheckConstraint(
            "delivered_at IS NULL OR provider_accepted_at IS NOT NULL",
            name="ck_ew_voucher_batch_item_delivered_needs_accepted",
        ),
        sa.CheckConstraint(
            "read_at IS NULL OR delivered_at IS NOT NULL",
            name="ck_ew_voucher_batch_item_read_needs_delivered",
        ),
        # 23-24. A refund is only ever the pre-send escape hatch.
        sa.CheckConstraint(
            "refund_claimed_at IS NULL OR ("
            "provider_accepted_at IS NULL "
            "AND delivered_at IS NULL "
            "AND read_at IS NULL "
            "AND send_claimed_at IS NULL "
            "AND send_attempted_at IS NULL)",
            name="ck_ew_voucher_batch_item_refund_is_pre_send",
        ),
        sa.CheckConstraint(
            "status <> 'refunded' OR (provider_accepted_at IS NULL AND delivered_at IS NULL AND read_at IS NULL)",
            name="ck_ew_voucher_batch_item_refunded_never_sent",
        ),
        # 25-26. At most one delivery attempt in the lifetime of this slot. Not
        # a retry budget: a counter that can only be zero or one.
        sa.CheckConstraint(
            "send_attempt_count >= 0 AND send_attempt_count <= 1",
            name="ck_ew_voucher_batch_item_single_attempt",
        ),
        sa.CheckConstraint(
            "(send_attempt_count = 0) = (send_attempted_at IS NULL)",
            name="ck_ew_voucher_batch_item_attempt_count_matches",
        ),
        # 27. A MAC without the key that made it cannot be verified later.
        sa.CheckConstraint(
            "(voucher_code_hmac IS NULL) = (hmac_key_id IS NULL)",
            name="ck_ew_voucher_batch_item_hmac_pair",
        ),
    )
    op.create_index("ix_ew_voucher_batch_item_batch", ITEMS, ["batch_id"])
    op.create_index("ix_ew_voucher_batch_item_status", ITEMS, ["status"])
    op.create_index("ix_ew_voucher_batch_item_run", ITEMS, ["campaign_run_id"])
    op.create_index("ix_ew_voucher_batch_item_recipient", ITEMS, ["campaign_recipient_id"])

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
        sa.Column("slot", sa.Integer(), nullable=False),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=True),
        sa.Column("provider_message_id", sa.String(length=128), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("intent_uuid", name="uq_ew_voucher_batch_attempt_intent"),
        # RESTRICT in both directions of intent: the audit of a real send
        # attempt must not disappear with the slot, nor allow it to be deleted.
        sa.ForeignKeyConstraint(
            ["item_id"],
            [f"{ITEMS}.id"],
            name="fk_ew_voucher_batch_attempt_item",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "outcome IN ('claimed', 'provider_accepted', 'unknown', 'rejected')",
            name="ck_ew_voucher_batch_attempt_outcome",
        ),
        sa.CheckConstraint(
            "outcome <> 'provider_accepted' OR provider_message_id IS NOT NULL",
            name="ck_ew_voucher_batch_attempt_accepted_has_id",
        ),
    )
    op.create_index("ix_ew_voucher_batch_attempt_item", ATTEMPTS, ["item_id"])


def downgrade() -> None:
    # Only the three objects this revision created, and in reverse dependency
    # order: the FKs are RESTRICT, and the audit rows reference the items, which
    # reference the header. Nothing belonging to §35, §36 or §37.2 is touched.
    op.drop_index("ix_ew_voucher_batch_attempt_item", table_name=ATTEMPTS)
    op.drop_table(ATTEMPTS)
    op.drop_index("ix_ew_voucher_batch_item_recipient", table_name=ITEMS)
    op.drop_index("ix_ew_voucher_batch_item_run", table_name=ITEMS)
    op.drop_index("ix_ew_voucher_batch_item_status", table_name=ITEMS)
    op.drop_index("ix_ew_voucher_batch_item_batch", table_name=ITEMS)
    op.drop_table(ITEMS)
    op.drop_table(BATCHES)
