"""UUID-first local customers and versioned mixed voucher audience (§44)."""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "f6a8d2c91b47"
down_revision = "c7e3b8a14f29"
branch_labels = None
depends_on = None

POLICY = "altegio_visit_zero_easyweek_bookings"
BATCHES = "easyweek_voucher_production_batches"
ITEMS = "easyweek_voucher_production_batch_items"
PLANS = "easyweek_manual_recipient_plans"


def upgrade() -> None:
    op.add_column("clients", sa.Column("easyweek_customer_uuid", postgresql.UUID(as_uuid=True)))
    op.add_column("clients", sa.Column("easyweek_identity_assigned_at", sa.DateTime(timezone=True)))
    op.alter_column("clients", "altegio_client_id", existing_type=sa.BigInteger(), nullable=True)
    op.create_unique_constraint("uq_clients_provider_easyweek_uuid", "clients", ["provider", "easyweek_customer_uuid"])
    op.create_check_constraint(
        "ck_clients_easyweek_uuid_provider", "clients", "easyweek_customer_uuid IS NULL OR provider = 'easyweek'"
    )
    op.create_check_constraint(
        "ck_clients_external_identity",
        "clients",
        "altegio_client_id IS NOT NULL OR (provider = 'easyweek' AND easyweek_customer_uuid IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_clients_easyweek_assignment",
        "clients",
        "easyweek_identity_assigned_at IS NULL OR (provider = 'easyweek' AND easyweek_customer_uuid IS NOT NULL)",
    )
    op.add_column("campaign_recipients", sa.Column("manual_policy", sa.String(64)))
    op.add_column("campaign_recipients", sa.Column("manual_policy_checked_at", sa.DateTime(timezone=True)))
    op.add_column("campaign_recipients", sa.Column("manual_operator_attested_at", sa.DateTime(timezone=True)))
    op.create_check_constraint(
        "ck_campaign_recipients_manual_policy",
        "campaign_recipients",
        "manual_policy IS NULL OR (provider = 'easyweek' AND recipient_basis = 'operator_manual_selection' "
        f"AND manual_policy = '{POLICY}')",
    )
    op.create_check_constraint(
        "ck_campaign_recipients_manual_policy_proof",
        "campaign_recipients",
        "(manual_policy IS NULL) = (manual_policy_checked_at IS NULL) AND "
        "(manual_policy IS NULL) = (manual_operator_attested_at IS NULL)",
    )
    op.drop_constraint("ck_ew_voucher_production_batch_basis", BATCHES, type_="check")
    op.create_check_constraint(
        "ck_ew_voucher_production_batch_basis",
        BATCHES,
        "(request_schema_version = '1' AND recipient_basis = 'operator_manual_selection') OR "
        "(request_schema_version = '2' AND recipient_basis IN "
        "('earned_first_visit', 'operator_manual_selection', 'mixed'))",
    )
    op.add_column(ITEMS, sa.Column("manual_policy", sa.String(64)))
    op.add_column(ITEMS, sa.Column("source_booking_uuid", postgresql.UUID(as_uuid=True)))
    op.add_column(ITEMS, sa.Column("source_proof_digest", sa.String(64)))
    op.drop_constraint("ck_ew_voucher_production_item_basis", ITEMS, type_="check")
    op.create_check_constraint(
        "ck_ew_voucher_production_item_basis",
        ITEMS,
        "recipient_basis IN ('earned_first_visit', 'operator_manual_selection')",
    )
    op.create_check_constraint(
        "ck_ew_voucher_production_item_source",
        ITEMS,
        "(recipient_basis = 'earned_first_visit' AND source_booking_uuid IS NOT NULL "
        "AND source_proof_digest IS NOT NULL AND manual_policy IS NULL) OR "
        "(recipient_basis = 'operator_manual_selection' AND source_booking_uuid IS NULL "
        "AND source_proof_digest IS NULL)",
    )
    op.create_check_constraint(
        "ck_ew_voucher_production_item_policy",
        ITEMS,
        f"manual_policy IS NULL OR (recipient_basis = 'operator_manual_selection' AND manual_policy = '{POLICY}')",
    )
    op.create_table(
        PLANS,
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True),
        sa.Column("run_id", sa.BigInteger(), sa.ForeignKey("campaign_runs.id", ondelete="RESTRICT"), nullable=False),
        sa.Column("operator_digest", sa.String(64), nullable=False),
        sa.Column("session_digest", sa.String(64), nullable=False),
        sa.Column("policy", sa.String(64), nullable=False),
        sa.Column("proof_digest", sa.String(64), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("applied_at", sa.DateTime(timezone=True)),
        sa.Column("result", postgresql.JSONB(none_as_null=True)),
        sa.CheckConstraint(f"policy = '{POLICY}'", name="ck_ew_manual_plan_policy"),
        sa.CheckConstraint("expires_at > created_at", name="ck_ew_manual_plan_expiry"),
        sa.CheckConstraint("(applied_at IS NULL) = (result IS NULL)", name="ck_ew_manual_plan_applied"),
    )
    op.create_index("ix_ew_manual_plan_expiry", PLANS, ["expires_at"])


def downgrade() -> None:
    # Fail before dropping even one column. These are identities and proof/audit
    # facts: a downgrade may not silently erase them, even after numeric adoption.
    connection = op.get_bind()
    occupied = connection.execute(
        sa.text(f"""
        SELECT EXISTS (SELECT 1 FROM clients WHERE easyweek_customer_uuid IS NOT NULL
                       OR easyweek_identity_assigned_at IS NOT NULL OR altegio_client_id IS NULL)
            OR EXISTS (SELECT 1 FROM campaign_recipients WHERE manual_policy IS NOT NULL)
            OR EXISTS (SELECT 1 FROM {BATCHES} WHERE request_schema_version <> '1')
            OR EXISTS (SELECT 1 FROM {ITEMS} WHERE manual_policy IS NOT NULL
                       OR source_booking_uuid IS NOT NULL OR source_proof_digest IS NOT NULL
                       OR recipient_basis <> 'operator_manual_selection')
            OR EXISTS (SELECT 1 FROM {PLANS})
    """)
    ).scalar_one()
    if occupied:
        raise RuntimeError(
            "PR-21 downgrade refused: UUID identities, selection proofs, plans or v2 ledgers exist; "
            "preserve data and use forward recovery"
        )
    op.drop_table(PLANS)
    for name in (
        "ck_ew_voucher_production_item_source",
        "ck_ew_voucher_production_item_policy",
        "ck_ew_voucher_production_item_basis",
    ):
        op.drop_constraint(name, ITEMS, type_="check")
    op.create_check_constraint(
        "ck_ew_voucher_production_item_basis", ITEMS, "recipient_basis = 'operator_manual_selection'"
    )
    for column in ("manual_policy", "source_booking_uuid", "source_proof_digest"):
        op.drop_column(ITEMS, column)
    op.drop_constraint("ck_ew_voucher_production_batch_basis", BATCHES, type_="check")
    op.create_check_constraint(
        "ck_ew_voucher_production_batch_basis", BATCHES, "recipient_basis = 'operator_manual_selection'"
    )
    op.drop_constraint("ck_campaign_recipients_manual_policy_proof", "campaign_recipients", type_="check")
    op.drop_constraint("ck_campaign_recipients_manual_policy", "campaign_recipients", type_="check")
    for column in ("manual_policy", "manual_policy_checked_at", "manual_operator_attested_at"):
        op.drop_column("campaign_recipients", column)
    for name in ("ck_clients_external_identity", "ck_clients_easyweek_assignment", "ck_clients_easyweek_uuid_provider"):
        op.drop_constraint(name, "clients", type_="check")
    op.drop_constraint("uq_clients_provider_easyweek_uuid", "clients", type_="unique")
    op.alter_column("clients", "altegio_client_id", existing_type=sa.BigInteger(), nullable=False)
    op.drop_column("clients", "easyweek_identity_assigned_at")
    op.drop_column("clients", "easyweek_customer_uuid")
