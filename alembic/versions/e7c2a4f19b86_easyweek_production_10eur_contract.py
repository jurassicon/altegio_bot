"""Bind new production batches to the fixed 10 EUR single-use monthly product."""

import sqlalchemy as sa

from alembic import op

revision = "e7c2a4f19b86"
down_revision = "d8b4e6a29c13"
branch_labels = None
depends_on = None

BATCH = "easyweek_voucher_production_batches"
ITEM = "easyweek_voucher_production_batch_items"
APPROVAL = "easyweek_voucher_production_approvals"
LEGACY_VERSION = "easyweek-production-15eur-v1"


# Literals belong to this historical migration, independent of future code.
PRODUCT_CHECK = (
    "(request_schema_version IN ('1', '2') "
    "AND product_contract_version = 'easyweek-production-15eur-v1' "
    "AND message_contract_code = 'new_client_voucher' "
    "AND voucher_unit_price_minor = 1500) OR "
    "(request_schema_version = '3' "
    "AND product_contract_version = 'easyweek-production-10eur-v1' "
    "AND message_contract_code = 'new_client_voucher_10eur_v2' "
    "AND voucher_template_uuid = '0ffb0346-57b8-475e-9c22-152dd23e25ca'::uuid "
    "AND voucher_unit_price_minor = 1000)"
)


def upgrade() -> None:
    # Existing values, proofs and digests remain untouched. Column defaults label
    # precisely the only product those old request versions could authorise.
    op.add_column(
        BATCH, sa.Column("product_contract_version", sa.String(64), nullable=False, server_default=LEGACY_VERSION)
    )
    op.add_column(
        BATCH, sa.Column("message_contract_code", sa.String(64), nullable=False, server_default="new_client_voucher")
    )
    op.add_column(
        APPROVAL, sa.Column("product_contract_version", sa.String(64), nullable=False, server_default=LEGACY_VERSION)
    )
    op.drop_constraint("ck_ew_voucher_production_batch_basis", BATCH, type_="check")
    op.create_check_constraint(
        "ck_ew_voucher_production_batch_basis",
        BATCH,
        "(request_schema_version = '1' AND recipient_basis = 'operator_manual_selection') OR "
        "(request_schema_version IN ('2', '3') AND recipient_basis IN "
        "('earned_first_visit', 'operator_manual_selection', 'mixed'))",
    )
    op.drop_constraint("ck_ew_voucher_production_batch_unit_price", BATCH, type_="check")
    op.create_check_constraint("ck_ew_voucher_production_batch_unit_price", BATCH, PRODUCT_CHECK)
    op.create_unique_constraint("uq_ew_voucher_production_batch_id_price", BATCH, ["id", "voucher_unit_price_minor"])
    op.drop_constraint("ck_ew_voucher_production_item_exact_voucher", ITEM, type_="check")
    op.create_check_constraint(
        "ck_ew_voucher_production_item_exact_voucher",
        ITEM,
        "voucher_value_minor IN (1500, 1000) AND voucher_quantity = 1",
    )
    op.create_foreign_key(
        "fk_ew_voucher_production_item_batch_price",
        ITEM,
        BATCH,
        ["batch_id", "voucher_value_minor"],
        ["id", "voucher_unit_price_minor"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        "ck_ew_voucher_production_approval_product",
        APPROVAL,
        "(request_schema_version IN ('1', '2') AND product_contract_version = 'easyweek-production-15eur-v1') "
        "OR (request_schema_version = '3' AND product_contract_version = 'easyweek-production-10eur-v1')",
    )

    op.create_check_constraint(
        "ck_ew_voucher_production_approval_product_amount",
        APPROVAL,
        "request_schema_version <> '3' OR (batch_recipient_count >= 1 "
        "AND batch_exposure_minor = batch_recipient_count * 1000 "
        "AND stage_amount_minor = CASE WHEN stage = 'freeze' THEN batch_exposure_minor "
        "WHEN stage IN ('create', 'pay', 'refund') THEN stage_target_count * 1000 ELSE 0 END)",
    )


def downgrade() -> None:
    # Check every durable use BEFORE any DDL. Never erase a new approval or
    # reinterpret the value of a voucher already created under schema 3.
    op.get_bind().execute(
        sa.text(
            "LOCK TABLE easyweek_voucher_production_batches, easyweek_voucher_production_approvals "
            "IN ACCESS EXCLUSIVE MODE"
        )
    )
    has_new_contract = (
        op.get_bind()
        .execute(
            sa.text(
                "SELECT EXISTS (SELECT 1 FROM easyweek_voucher_production_batches "
                "WHERE request_schema_version = '3' OR product_contract_version <> 'easyweek-production-15eur-v1') "
                "OR EXISTS (SELECT 1 FROM easyweek_voucher_production_approvals "
                "WHERE request_schema_version = '3' OR product_contract_version <> 'easyweek-production-15eur-v1')"
            )
        )
        .scalar_one()
    )
    if has_new_contract:
        raise RuntimeError("10 EUR downgrade refused: new production contract data exists; preserve data")
    op.drop_constraint("ck_ew_voucher_production_approval_product_amount", APPROVAL, type_="check")
    op.drop_constraint("ck_ew_voucher_production_approval_product", APPROVAL, type_="check")
    op.drop_constraint("fk_ew_voucher_production_item_batch_price", ITEM, type_="foreignkey")
    op.drop_constraint("uq_ew_voucher_production_batch_id_price", BATCH, type_="unique")
    op.drop_constraint("ck_ew_voucher_production_item_exact_voucher", ITEM, type_="check")
    op.create_check_constraint(
        "ck_ew_voucher_production_item_exact_voucher", ITEM, "voucher_value_minor = 1500 AND voucher_quantity = 1"
    )
    op.drop_constraint("ck_ew_voucher_production_batch_unit_price", BATCH, type_="check")
    op.create_check_constraint("ck_ew_voucher_production_batch_unit_price", BATCH, "voucher_unit_price_minor = 1500")
    op.drop_constraint("ck_ew_voucher_production_batch_basis", BATCH, type_="check")
    op.create_check_constraint(
        "ck_ew_voucher_production_batch_basis",
        BATCH,
        "(request_schema_version = '1' AND recipient_basis = 'operator_manual_selection') OR "
        "(request_schema_version = '2' AND recipient_basis IN "
        "('earned_first_visit', 'operator_manual_selection', 'mixed'))",
    )
    op.drop_column(APPROVAL, "product_contract_version")
    op.drop_column(BATCH, "message_contract_code")
    op.drop_column(BATCH, "product_contract_version")
