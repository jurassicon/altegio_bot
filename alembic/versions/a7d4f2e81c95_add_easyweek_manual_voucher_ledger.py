"""add the EasyWeek controlled MANUAL-BASIS voucher delivery canary ledger (§37.2)

§36 delivers a voucher to somebody whose first visit earned it, or to the one
pre-configured test account. §37.2 delivers to a real customer an OPERATOR chose
by hand in a preview. The machinery is the same shape; what stands behind the
row is not, and the schema is what keeps the two from being confused.

Two differences a reader should see immediately:

* there is no ``source_booking_uuid`` column. A manual selection has no visit to
  point at, and a nullable column would be an invitation to fill it with a
  borrowed, random or customer-shaped value. The absence is structural.
* the entitlement key is the customer plus the campaign PERIOD. That is the only
  identity a manual selection has, and it is what stops a fresh preview of the
  same August wave from issuing a second €15 to the same person.

Two tables, because they answer different questions: the ledger is the state
machine and the uniqueness rules; the attempts table is the redacted outbound
intent, committed before a send. The latter is deliberately NOT an
``outbox_messages`` row — that table is swept by a generic worker whose purpose
is to retry what it finds, and the one property this row must have is that
nothing may ever pick it up and send it again.

Nothing is backfilled. This canary starts empty by definition, and the §36
tables are not touched.

Revision ID: a7d4f2e81c95
Revises: e5b3c81f74a2
Create Date: 2026-09-15
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "a7d4f2e81c95"
down_revision = "e5b3c81f74a2"
branch_labels = None
depends_on = None

LEDGER = "easyweek_manual_voucher_delivery_ledger"
ATTEMPTS = "easyweek_manual_voucher_delivery_attempts"

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

# The one basis this canary serves, spelled out rather than imported: a
# migration must keep meaning the same thing after the constant moves.
_BASIS = "operator_manual_selection"

# The one branch the owner approved for this canary. A literal in the schema,
# so a misconfigured environment cannot point a real payment at another branch.
_KARLSRUHE_COMPANY_ID = 322579


def upgrade() -> None:
    op.create_table(
        LEDGER,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        # -- identity ------------------------------------------------------
        sa.Column("canary_scope", sa.String(length=128), nullable=False),
        sa.Column("request_schema_version", sa.String(length=16), nullable=False),
        sa.Column("baseline_version", sa.String(length=32), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("company_id", sa.Integer(), nullable=False),
        sa.Column("campaign_code", sa.String(length=128), nullable=False),
        sa.Column("recipient_basis", sa.String(length=32), nullable=False),
        sa.Column("campaign_run_id", sa.BigInteger(), nullable=False),
        sa.Column("campaign_recipient_id", sa.BigInteger(), nullable=False),
        sa.Column("easyweek_customer_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("campaign_period_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("campaign_period_end", sa.DateTime(timezone=True), nullable=False),
        # -- the frozen EasyWeek identity this canary may act on ------------
        sa.Column("location_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("staffer_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("payment_account_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("voucher_template_uuid", postgresql.UUID(as_uuid=True), nullable=False),
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
        # -- safe evidence ---------------------------------------------------
        sa.Column("evidence", postgresql.JSONB(), nullable=False, server_default=sa.text("'{}'::jsonb")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        # 1. One canary, ever.
        sa.UniqueConstraint("canary_scope", name="uq_ew_manual_voucher_scope"),
        # 2. One voucher per person per campaign period, whatever run proposes
        # it. A manual selection has no booking to be unique about, so the thing
        # that must not repeat is "this person, this campaign, this month".
        sa.UniqueConstraint(
            "provider",
            "company_id",
            "campaign_code",
            "easyweek_customer_uuid",
            "campaign_period_start",
            "campaign_period_end",
            name="uq_ew_manual_voucher_entitlement",
        ),
        # 3-5. A result may belong to exactly one canary row.
        sa.UniqueConstraint("target_order_uuid", name="uq_ew_manual_voucher_target_order"),
        sa.UniqueConstraint("outbound_intent_uuid", name="uq_ew_manual_voucher_intent"),
        sa.UniqueConstraint("provider_message_id", name="uq_ew_manual_voucher_provider_message"),
        # 6. The recipient must belong to the run this row names, under the same
        # provider. Composite FKs, because two separate ones would each be
        # satisfied by rows that have nothing to do with each other.
        sa.ForeignKeyConstraint(
            ["campaign_run_id", "provider"],
            ["campaign_runs.id", "campaign_runs.provider"],
            name="fk_ew_manual_voucher_run_provider",
            ondelete="RESTRICT",
        ),
        sa.ForeignKeyConstraint(
            ["campaign_recipient_id", "provider"],
            ["campaign_recipients.id", "campaign_recipients.provider"],
            name="fk_ew_manual_voucher_recipient_provider",
            ondelete="RESTRICT",
        ),
        # 7-9. Closed vocabularies and the topology this canary was approved for.
        sa.CheckConstraint(f"status IN ({_STATUS_SQL})", name="ck_ew_manual_voucher_status"),
        sa.CheckConstraint("provider = 'easyweek'", name="ck_ew_manual_voucher_provider"),
        sa.CheckConstraint(
            f"company_id = {_KARLSRUHE_COMPANY_ID}",
            name="ck_ew_manual_voucher_company",
        ),
        sa.CheckConstraint(
            f"recipient_basis = '{_BASIS}'",
            name="ck_ew_manual_voucher_basis",
        ),
        sa.CheckConstraint(
            "campaign_period_start < campaign_period_end",
            name="ck_ew_manual_voucher_period_order",
        ),
        # 10. Stage order.
        sa.CheckConstraint(
            "(create_attempted_at IS NULL OR create_claimed_at IS NOT NULL) "
            "AND (create_verified_at IS NULL OR create_attempted_at IS NOT NULL) "
            "AND (pay_attempted_at IS NULL OR pay_claimed_at IS NOT NULL) "
            "AND (pay_verified_at IS NULL OR pay_attempted_at IS NOT NULL) "
            "AND (send_attempted_at IS NULL OR send_claimed_at IS NOT NULL) "
            "AND (refund_attempted_at IS NULL OR refund_claimed_at IS NOT NULL) "
            "AND (refund_verified_at IS NULL OR refund_attempted_at IS NOT NULL)",
            name="ck_ew_manual_voucher_stage_order",
        ),
        # 11. Money cannot move before the order it pays for was proven to exist.
        sa.CheckConstraint(
            "pay_claimed_at IS NULL OR (create_verified_at IS NOT NULL AND target_order_uuid IS NOT NULL)",
            name="ck_ew_manual_voucher_pay_needs_created",
        ),
        # 12. Nothing may be sent before the voucher is proven paid for, bound to
        # this row by MAC, and proven still deliverable by a guard taken AFTER
        # the payment. A guard from before the payment says nothing about now.
        sa.CheckConstraint(
            "send_claimed_at IS NULL OR ("
            "pay_verified_at IS NOT NULL "
            "AND voucher_code_hmac IS NOT NULL "
            "AND hmac_key_id IS NOT NULL "
            "AND live_guard_reproven_at IS NOT NULL "
            "AND live_guard_reproven_at >= pay_verified_at)",
            name="ck_ew_manual_voucher_send_needs_paid",
        ),
        # 13-15. Acceptance needs both the attempt and Meta's identifier; the
        # webhook ladder may not be climbed out of order.
        sa.CheckConstraint(
            "provider_accepted_at IS NULL OR (send_attempted_at IS NOT NULL AND provider_message_id IS NOT NULL)",
            name="ck_ew_manual_voucher_accepted_needs_attempt",
        ),
        sa.CheckConstraint(
            "delivered_at IS NULL OR provider_accepted_at IS NOT NULL",
            name="ck_ew_manual_voucher_delivered_needs_accepted",
        ),
        sa.CheckConstraint(
            "read_at IS NULL OR delivered_at IS NOT NULL",
            name="ck_ew_manual_voucher_read_needs_delivered",
        ),
        # 16-17. A refund is only ever the pre-send escape hatch.
        sa.CheckConstraint(
            "refund_claimed_at IS NULL OR ("
            "provider_accepted_at IS NULL "
            "AND delivered_at IS NULL "
            "AND read_at IS NULL "
            "AND send_attempted_at IS NULL)",
            name="ck_ew_manual_voucher_refund_is_pre_send",
        ),
        sa.CheckConstraint(
            "status <> 'refunded' OR (provider_accepted_at IS NULL AND delivered_at IS NULL AND read_at IS NULL)",
            name="ck_ew_manual_voucher_refunded_never_sent",
        ),
        # 18. At most one delivery attempt in the lifetime of this row. Not a
        # retry budget: a counter that can only be zero or one.
        sa.CheckConstraint(
            "send_attempt_count >= 0 AND send_attempt_count <= 1",
            name="ck_ew_manual_voucher_single_attempt",
        ),
        sa.CheckConstraint(
            "(send_attempt_count = 0) = (send_attempted_at IS NULL)",
            name="ck_ew_manual_voucher_attempt_count_matches",
        ),
        # 19. A MAC without the key that made it cannot be verified later.
        sa.CheckConstraint(
            "(voucher_code_hmac IS NULL) = (hmac_key_id IS NULL)",
            name="ck_ew_manual_voucher_hmac_pair",
        ),
    )
    op.create_index("ix_ew_manual_voucher_status", LEDGER, ["status"])
    op.create_index("ix_ew_manual_voucher_recipient", LEDGER, ["campaign_recipient_id"])
    op.create_index("ix_ew_manual_voucher_run", LEDGER, ["campaign_run_id"])

    op.create_table(
        ATTEMPTS,
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
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
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.UniqueConstraint("intent_uuid", name="uq_ew_manual_voucher_attempt_intent"),
        # RESTRICT in both directions of intent: the audit of a real send
        # attempt must not disappear with the ledger row, nor allow it to be
        # deleted.
        sa.ForeignKeyConstraint(
            ["ledger_id"],
            [f"{LEDGER}.id"],
            name="fk_ew_manual_voucher_attempt_ledger",
            ondelete="RESTRICT",
        ),
        sa.CheckConstraint(
            "outcome IN ('claimed', 'provider_accepted', 'unknown', 'rejected')",
            name="ck_ew_manual_voucher_attempt_outcome",
        ),
        sa.CheckConstraint(
            "outcome <> 'provider_accepted' OR provider_message_id IS NOT NULL",
            name="ck_ew_manual_voucher_attempt_accepted_has_id",
        ),
    )
    op.create_index("ix_ew_manual_voucher_attempt_ledger", ATTEMPTS, ["ledger_id"])


def downgrade() -> None:
    # Attempts first: the FK is RESTRICT, and the audit rows are what reference
    # the ledger.
    op.drop_index("ix_ew_manual_voucher_attempt_ledger", table_name=ATTEMPTS)
    op.drop_table(ATTEMPTS)
    op.drop_index("ix_ew_manual_voucher_run", table_name=LEDGER)
    op.drop_index("ix_ew_manual_voucher_recipient", table_name=LEDGER)
    op.drop_index("ix_ew_manual_voucher_status", table_name=LEDGER)
    op.drop_table(LEDGER)
