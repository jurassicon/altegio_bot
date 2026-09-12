"""add the EasyWeek controlled voucher DELIVERY canary ledger (§36)

§35 proved this application can create, pay for and refund one real voucher.
§36 adds the step §35 refused: handing the resulting code to a real person. The
durable state therefore has to answer a second question that has no undo —
"could a customer already be holding a €15 code?" — so the constraints that
enforce it are created with the tables rather than added afterwards.

Two tables, because they answer different questions:

* the ledger is the state machine and the uniqueness rules;
* the attempts table is the redacted outbound intent, committed before a send.
  It is deliberately NOT an ``outbox_messages`` row: that table is swept by a
  generic worker whose purpose is to retry what it finds, and the one property
  this row must have is that nothing may ever pick it up and send it again.

No historical campaign row is backfilled. There is nothing to migrate: this
canary starts empty by definition.

Revision ID: c9d2e6f70b41
Revises: b3f1c07d5a48
Create Date: 2026-09-12
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "c9d2e6f70b41"
down_revision = "b3f1c07d5a48"
branch_labels = None
depends_on = None

LEDGER = "easyweek_campaign_voucher_delivery_ledger"
ATTEMPTS = "easyweek_campaign_voucher_delivery_attempts"

_STATUSES = (
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
_STATUS_SQL = ", ".join(f"'{value}'" for value in _STATUSES)


def upgrade() -> None:
    # A composite foreign key needs a matching unique key on the far side.
    # `campaign_runs` already has one from §32; recipients did not, because
    # nothing referenced them together with their provider until now. Adding it
    # is additive — `id` is already the primary key — and it is what stops a
    # referencing row from naming an EasyWeek recipient while claiming to be
    # Altegio.
    op.create_unique_constraint(
        "uq_campaign_recipients_id_provider",
        "campaign_recipients",
        ["id", "provider"],
    )

    op.create_table(
        LEDGER,
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        # -- identity ------------------------------------------------------
        sa.Column("canary_scope", sa.String(length=128), nullable=False),
        sa.Column("request_schema_version", sa.String(length=16), nullable=False),
        sa.Column("provider", sa.String(length=32), server_default=sa.text("'altegio'"), nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("campaign_code", sa.String(length=128), nullable=False),
        sa.Column("campaign_run_id", sa.BigInteger(), nullable=False),
        sa.Column("campaign_recipient_id", sa.BigInteger(), nullable=False),
        sa.Column("source_booking_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("easyweek_customer_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        # -- frozen EasyWeek identity --------------------------------------
        sa.Column("location_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("staffer_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("payment_account_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("voucher_template_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("reconciliation_marker", sa.String(length=64), nullable=False),
        # -- results -------------------------------------------------------
        sa.Column("target_order_uuid", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("outbound_intent_uuid", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("provider_message_id", sa.String(length=128), nullable=True),
        # -- the voucher as a keyed proof, never as a value ----------------
        sa.Column("voucher_code_hmac", sa.String(length=64), nullable=True),
        sa.Column("hmac_key_id", sa.String(length=64), nullable=True),
        # -- state ---------------------------------------------------------
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=True),
        sa.Column("manual_cleanup_required", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("reconciliation_required", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("manual_cleanup_observed_at", sa.DateTime(timezone=True), nullable=True),
        # -- operator authorisation provenance ------------------------------
        sa.Column("create_plan_digest", sa.String(length=64), nullable=True),
        sa.Column("pay_plan_digest", sa.String(length=64), nullable=True),
        sa.Column("deliver_plan_digest", sa.String(length=64), nullable=True),
        sa.Column("refund_plan_digest", sa.String(length=64), nullable=True),
        # -- claim / attempt / verification ---------------------------------
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
        sa.Column("send_attempt_count", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("provider_accepted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("delivered_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("read_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refund_claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refund_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refund_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("evidence", postgresql.JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        # One canary, ever.
        sa.UniqueConstraint("canary_scope", name="uq_ew_voucher_delivery_scope"),
        # One voucher per earned entitlement, whatever preview run proposes it.
        sa.UniqueConstraint(
            "provider",
            "company_id",
            "campaign_code",
            "source_booking_uuid",
            name="uq_ew_voucher_delivery_entitlement",
        ),
        sa.UniqueConstraint("target_order_uuid", name="uq_ew_voucher_delivery_target_order"),
        sa.UniqueConstraint("outbound_intent_uuid", name="uq_ew_voucher_delivery_intent"),
        sa.UniqueConstraint("provider_message_id", name="uq_ew_voucher_delivery_provider_message"),
        # The run and the recipient must agree with each other and with us.
        sa.ForeignKeyConstraint(
            ["campaign_run_id", "provider"],
            ["campaign_runs.id", "campaign_runs.provider"],
            name="fk_ew_voucher_delivery_run_provider",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["campaign_recipient_id", "provider"],
            ["campaign_recipients.id", "campaign_recipients.provider"],
            name="fk_ew_voucher_delivery_recipient_provider",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(f"status IN ({_STATUS_SQL})", name="ck_ew_voucher_delivery_status"),
        sa.CheckConstraint("provider = 'easyweek'", name="ck_ew_voucher_delivery_provider"),
        sa.CheckConstraint(
            "(create_attempted_at IS NULL OR create_claimed_at IS NOT NULL) "
            "AND (create_verified_at IS NULL OR create_attempted_at IS NOT NULL) "
            "AND (pay_attempted_at IS NULL OR pay_claimed_at IS NOT NULL) "
            "AND (pay_verified_at IS NULL OR pay_attempted_at IS NOT NULL) "
            "AND (send_attempted_at IS NULL OR send_claimed_at IS NOT NULL) "
            "AND (refund_attempted_at IS NULL OR refund_claimed_at IS NOT NULL) "
            "AND (refund_verified_at IS NULL OR refund_attempted_at IS NOT NULL)",
            name="ck_ew_voucher_delivery_stage_order",
        ),
        sa.CheckConstraint(
            "pay_claimed_at IS NULL OR (create_verified_at IS NOT NULL AND target_order_uuid IS NOT NULL)",
            name="ck_ew_voucher_delivery_pay_needs_created",
        ),
        sa.CheckConstraint(
            "send_claimed_at IS NULL OR ("
            "pay_verified_at IS NOT NULL "
            "AND voucher_code_hmac IS NOT NULL "
            "AND hmac_key_id IS NOT NULL "
            "AND live_guard_reproven_at IS NOT NULL "
            "AND live_guard_reproven_at >= pay_verified_at)",
            name="ck_ew_voucher_delivery_send_needs_paid",
        ),
        sa.CheckConstraint(
            "provider_accepted_at IS NULL OR (send_attempted_at IS NOT NULL AND provider_message_id IS NOT NULL)",
            name="ck_ew_voucher_delivery_accepted_needs_attempt",
        ),
        sa.CheckConstraint(
            "delivered_at IS NULL OR provider_accepted_at IS NOT NULL",
            name="ck_ew_voucher_delivery_delivered_needs_accepted",
        ),
        sa.CheckConstraint(
            "read_at IS NULL OR delivered_at IS NOT NULL",
            name="ck_ew_voucher_delivery_read_needs_delivered",
        ),
        sa.CheckConstraint(
            "refund_claimed_at IS NULL OR ("
            "provider_accepted_at IS NULL "
            "AND delivered_at IS NULL "
            "AND read_at IS NULL "
            "AND send_attempted_at IS NULL)",
            name="ck_ew_voucher_delivery_refund_is_pre_send",
        ),
        sa.CheckConstraint(
            "status <> 'refunded' OR "
            "(provider_accepted_at IS NULL AND delivered_at IS NULL AND read_at IS NULL)",
            name="ck_ew_voucher_delivery_refunded_never_sent",
        ),
        sa.CheckConstraint(
            "send_attempt_count >= 0 AND send_attempt_count <= 1",
            name="ck_ew_voucher_delivery_single_attempt",
        ),
        sa.CheckConstraint(
            "(send_attempt_count = 0) = (send_attempted_at IS NULL)",
            name="ck_ew_voucher_delivery_attempt_count_matches",
        ),
        sa.CheckConstraint(
            "(voucher_code_hmac IS NULL) = (hmac_key_id IS NULL)",
            name="ck_ew_voucher_delivery_hmac_pair",
        ),
    )
    op.create_index("ix_ew_voucher_delivery_status", LEDGER, ["status"])
    op.create_index("ix_ew_voucher_delivery_recipient", LEDGER, ["campaign_recipient_id"])

    op.create_table(
        ATTEMPTS,
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("ledger_id", sa.BigInteger(), nullable=False),
        sa.Column("intent_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("template_code", sa.String(length=64), nullable=False),
        sa.Column("meta_template_name", sa.String(length=128), nullable=False),
        sa.Column("template_language", sa.String(length=8), nullable=False),
        sa.Column("sender_id", sa.BigInteger(), nullable=True),
        sa.Column("campaign_recipient_id", sa.BigInteger(), nullable=False),
        sa.Column("outcome", sa.String(length=32), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=True),
        sa.Column("provider_message_id", sa.String(length=128), nullable=True),
        sa.Column("claimed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("intent_uuid", name="uq_ew_voucher_delivery_attempt_intent"),
        # RESTRICT, not CASCADE: an audit row of a real send attempt must not
        # vanish because somebody deleted the ledger, and must not let them.
        sa.ForeignKeyConstraint(
            ["ledger_id"],
            [f"{LEDGER}.id"],
            name="fk_ew_voucher_delivery_attempt_ledger",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "outcome IN ('claimed', 'provider_accepted', 'unknown', 'rejected')",
            name="ck_ew_voucher_delivery_attempt_outcome",
        ),
        sa.CheckConstraint(
            "outcome <> 'provider_accepted' OR provider_message_id IS NOT NULL",
            name="ck_ew_voucher_delivery_attempt_accepted_has_id",
        ),
    )
    op.create_index("ix_ew_voucher_delivery_attempt_ledger", ATTEMPTS, ["ledger_id"])


def downgrade() -> None:
    op.drop_index("ix_ew_voucher_delivery_attempt_ledger", table_name=ATTEMPTS)
    op.drop_table(ATTEMPTS)
    op.drop_index("ix_ew_voucher_delivery_recipient", table_name=LEDGER)
    op.drop_index("ix_ew_voucher_delivery_status", table_name=LEDGER)
    op.drop_table(LEDGER)
    op.drop_constraint("uq_campaign_recipients_id_provider", "campaign_recipients", type_="unique")
