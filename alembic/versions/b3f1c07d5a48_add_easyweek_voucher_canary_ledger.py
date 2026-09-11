"""add the EasyWeek controlled voucher mutation canary ledger (§35)

One durable row per canary scope. It is what makes "did we already claim this
mutation, and could it have reached EasyWeek?" answerable after a crash, a
timeout or a redeploy — so the table is created with its constraints, not with
them added later.

Each stage carries its own approved plan digest, because one plan can only ever
authorise the first mutation: after `create` succeeds, a plan that required no
marker order to exist can never be satisfied again. The frozen template
configuration is digested separately from the voucher counters, which
legitimately move when a voucher is issued.

This migration has not been released, so it is edited in place rather than
followed by a compensating migration against a schema that never existed.

Revision ID: b3f1c07d5a48
Revises: a7c14e9b2d63
Create Date: 2026-09-11
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "b3f1c07d5a48"
down_revision = "a7c14e9b2d63"
branch_labels = None
depends_on = None

_TABLE = "easyweek_voucher_canary_ledger"

_STATUSES = (
    "create_claimed",
    "create_unknown",
    "create_rejected",
    "created",
    "pay_claimed",
    "pay_unknown",
    "pay_rejected",
    "paid",
    "refund_claimed",
    "refund_unknown",
    "refund_rejected",
    "refunded",
    "ambiguous",
    "manually_cleaned",
)
_STATUS_LIST = ",".join(f"'{status}'" for status in _STATUSES)
_PRE_TARGET_STATUSES = "'create_claimed','create_unknown','create_rejected','ambiguous'"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("canary_scope", sa.String(length=64), nullable=False),
        sa.Column("request_schema_version", sa.String(length=16), nullable=False),
        sa.Column("template_config_digest", sa.String(length=64), nullable=False),
        sa.Column("create_plan_digest", sa.String(length=64), nullable=False),
        sa.Column("pay_plan_digest", sa.String(length=64), nullable=True),
        sa.Column("refund_plan_digest", sa.String(length=64), nullable=True),
        sa.Column("customer_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("staffer_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("account_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("reconciliation_marker", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=True),
        sa.Column("target_order_uuid", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("create_window_start", sa.DateTime(timezone=True), nullable=False),
        sa.Column("create_window_end", sa.DateTime(timezone=True), nullable=False),
        sa.Column("create_claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("create_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("create_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("pay_claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("pay_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("pay_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refund_claimed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refund_attempted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("refund_verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("manual_cleanup_observed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "evidence",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column(
            "stage_counters",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'{}'::jsonb"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("canary_scope", name="uq_easyweek_voucher_canary_scope"),
        sa.CheckConstraint(f"status IN ({_STATUS_LIST})", name="ck_easyweek_voucher_canary_status"),
        sa.CheckConstraint(
            f"status IN ({_PRE_TARGET_STATUSES}) OR target_order_uuid IS NOT NULL",
            name="ck_easyweek_voucher_canary_target_required",
        ),
        sa.CheckConstraint(
            "(create_attempted_at IS NULL OR create_claimed_at IS NOT NULL) AND "
            "(pay_attempted_at IS NULL OR pay_claimed_at IS NOT NULL) AND "
            "(refund_attempted_at IS NULL OR refund_claimed_at IS NOT NULL)",
            name="ck_easyweek_voucher_canary_attempt_needs_claim",
        ),
        sa.CheckConstraint(
            "(create_verified_at IS NULL OR create_attempted_at IS NOT NULL) AND "
            "(pay_verified_at IS NULL OR pay_attempted_at IS NOT NULL) AND "
            "(refund_verified_at IS NULL OR refund_attempted_at IS NOT NULL)",
            name="ck_easyweek_voucher_canary_verify_needs_attempt",
        ),
        sa.CheckConstraint(
            "(pay_claimed_at IS NULL OR create_claimed_at IS NOT NULL) AND "
            "(refund_claimed_at IS NULL OR pay_claimed_at IS NOT NULL)",
            name="ck_easyweek_voucher_canary_stage_order",
        ),
        sa.CheckConstraint(
            "char_length(customer_fingerprint) = 64 AND "
            "char_length(staffer_fingerprint) = 64 AND "
            "char_length(account_fingerprint) = 64 AND "
            "char_length(template_config_digest) = 64 AND "
            "char_length(create_plan_digest) = 64 AND "
            "(pay_plan_digest IS NULL OR char_length(pay_plan_digest) = 64) AND "
            "(refund_plan_digest IS NULL OR char_length(refund_plan_digest) = 64)",
            name="ck_easyweek_voucher_canary_digest_lengths",
        ),
        sa.CheckConstraint(
            "(pay_claimed_at IS NULL) = (pay_plan_digest IS NULL) AND "
            "(refund_claimed_at IS NULL) = (refund_plan_digest IS NULL)",
            name="ck_easyweek_voucher_canary_stage_plan_recorded",
        ),
        sa.CheckConstraint(
            "create_window_end > create_window_start",
            name="ck_easyweek_voucher_canary_window_ordered",
        ),
    )
    op.create_index("ix_easyweek_voucher_canary_status", _TABLE, ["status"])


def downgrade() -> None:
    op.drop_index("ix_easyweek_voucher_canary_status", table_name=_TABLE)
    op.drop_table(_TABLE)
