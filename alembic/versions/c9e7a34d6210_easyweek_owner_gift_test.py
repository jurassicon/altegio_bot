"""Persist one isolated, non-repeatable owner gift issuance test."""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "c9e7a34d6210"
down_revision = "b4d7f1c90ae2"
branch_labels = None
depends_on = None

TABLE = "easyweek_voucher_owner_test"


def upgrade() -> None:
    op.create_table(
        TABLE,
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("customer_uuid", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("binding_digest", sa.String(64), nullable=False),
        sa.Column("marker", sa.String(64), nullable=False, unique=True),
        sa.Column("state", sa.String(32), nullable=False),
        sa.Column("stopped", sa.Boolean(), nullable=False),
        sa.Column("create_attempted", sa.Boolean(), nullable=False),
        sa.Column("pay_attempted", sa.Boolean(), nullable=False),
        sa.Column("order_uuid", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("code_present", sa.Boolean(), nullable=False),
        sa.Column("reason", sa.String(96), nullable=True),
        sa.Column("approval", postgresql.JSONB(none_as_null=True), nullable=True),
        sa.Column("audit", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.CheckConstraint("id = 1", name="ck_ew_owner_test_singleton"),
        sa.CheckConstraint(
            "state IN ('new', 'create_queued', 'create_running', 'open', "
            "'pay_queued', 'pay_running', 'paid', 'unknown', 'blocked')",
            name="ck_ew_owner_test_state",
        ),
        sa.CheckConstraint("NOT pay_attempted OR create_attempted", name="ck_ew_owner_test_attempts"),
    )


def downgrade() -> None:
    # An offer inserts the singleton before any provider mutation. Serialize the
    # emptiness proof with that insert; otherwise its evidence could be dropped.
    op.execute(sa.text(f"LOCK TABLE {TABLE} IN ACCESS EXCLUSIVE MODE"))
    if op.get_bind().execute(sa.text(f"SELECT EXISTS (SELECT 1 FROM {TABLE})")).scalar_one():
        raise RuntimeError("owner gift test evidence exists; downgrade refused before DDL")
    op.drop_table(TABLE)
