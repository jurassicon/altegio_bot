"""Separate a voucher's face value from its issue price; add the free gift contract.

Historical rows keep every number they had. For the 15 EUR and 10 EUR contracts the
two sums are the same, so the new columns are backfilled from the existing ones:
that is not a rewrite, it is writing down what those rows already meant.
"""

import sqlalchemy as sa

from alembic import op

revision = "b4d7f1c90ae2"
down_revision = "e7c2a4f19b86"
branch_labels = None
depends_on = None

BATCH = "easyweek_voucher_production_batches"
ITEM = "easyweek_voucher_production_batch_items"
APPROVAL = "easyweek_voucher_production_approvals"
GIFT_VERSION = "easyweek-production-gift-10eur-v1"

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
    "AND voucher_unit_price_minor = 1000) OR "
    "(request_schema_version = '4' "
    "AND product_contract_version = 'easyweek-production-gift-10eur-v1' "
    "AND message_contract_code = 'new_client_voucher_10eur_v2' "
    "AND voucher_template_uuid = '0ffb0346-57b8-475e-9c22-152dd23e25ca'::uuid "
    "AND voucher_unit_price_minor = 1000 "
    "AND voucher_issue_price_minor = 0)"
)
PREVIOUS_PRODUCT_CHECK = (
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
    # The money columns, added beside the nominal ones rather than instead of them.
    # ``server_default`` is only what an INSERT that forgot them would get; the
    # backfill below is what gives existing rows their true value, and the defaults
    # are dropped afterwards so a future insert has to say which sum it means.
    op.add_column(BATCH, sa.Column("voucher_issue_price_minor", sa.Integer(), nullable=False, server_default="0"))
    op.add_column(BATCH, sa.Column("total_issue_price_minor", sa.Integer(), nullable=False, server_default="0"))
    op.add_column(APPROVAL, sa.Column("stage_issue_price_minor", sa.Integer(), nullable=False, server_default="0"))
    op.add_column(APPROVAL, sa.Column("batch_issue_price_minor", sa.Integer(), nullable=False, server_default="0"))
    op.execute(
        sa.text(
            f"UPDATE {BATCH} SET voucher_issue_price_minor = voucher_unit_price_minor, "
            "total_issue_price_minor = total_exposure_minor "
            "WHERE request_schema_version IN ('1', '2', '3')"
        )
    )
    op.execute(
        sa.text(
            f"UPDATE {APPROVAL} SET stage_issue_price_minor = stage_amount_minor, "
            "batch_issue_price_minor = batch_exposure_minor "
            "WHERE request_schema_version IN ('1', '2', '3')"
        )
    )
    op.alter_column(BATCH, "voucher_issue_price_minor", server_default=None)
    op.alter_column(BATCH, "total_issue_price_minor", server_default=None)
    op.alter_column(APPROVAL, "stage_issue_price_minor", server_default=None)
    op.alter_column(APPROVAL, "batch_issue_price_minor", server_default=None)

    op.drop_constraint("ck_ew_voucher_production_batch_basis", BATCH, type_="check")
    op.create_check_constraint(
        "ck_ew_voucher_production_batch_basis",
        BATCH,
        "(request_schema_version = '1' AND recipient_basis = 'operator_manual_selection') OR "
        "(request_schema_version IN ('2', '3', '4') AND recipient_basis IN "
        "('earned_first_visit', 'operator_manual_selection', 'mixed'))",
    )
    op.drop_constraint("ck_ew_voucher_production_batch_unit_price", BATCH, type_="check")
    op.create_check_constraint("ck_ew_voucher_production_batch_unit_price", BATCH, PRODUCT_CHECK)
    op.create_check_constraint(
        "ck_ew_voucher_production_batch_issue_price_matches",
        BATCH,
        "total_issue_price_minor = voucher_issue_price_minor * recipient_count",
    )
    op.create_check_constraint(
        "ck_ew_voucher_production_batch_issue_price_contract",
        BATCH,
        "(request_schema_version IN ('1', '2', '3') "
        "AND voucher_issue_price_minor = voucher_unit_price_minor) OR "
        "(request_schema_version = '4' AND voucher_issue_price_minor = 0)",
    )

    op.drop_constraint("ck_ew_voucher_production_approval_product", APPROVAL, type_="check")
    op.create_check_constraint(
        "ck_ew_voucher_production_approval_product",
        APPROVAL,
        "(request_schema_version IN ('1', '2') AND product_contract_version = 'easyweek-production-15eur-v1') "
        "OR (request_schema_version = '3' AND product_contract_version = 'easyweek-production-10eur-v1') "
        "OR (request_schema_version = '4' AND product_contract_version = 'easyweek-production-gift-10eur-v1')",
    )
    op.drop_constraint("ck_ew_voucher_production_approval_product_amount", APPROVAL, type_="check")
    op.create_check_constraint(
        "ck_ew_voucher_production_approval_product_amount",
        APPROVAL,
        "request_schema_version NOT IN ('3', '4') OR (batch_recipient_count >= 1 "
        "AND batch_exposure_minor = batch_recipient_count * 1000 "
        "AND stage_amount_minor = CASE WHEN stage = 'freeze' THEN batch_exposure_minor "
        "WHEN stage IN ('create', 'pay', 'refund') THEN stage_target_count * 1000 ELSE 0 END)",
    )
    op.create_check_constraint(
        "ck_ew_voucher_production_approval_issue_price",
        APPROVAL,
        "(request_schema_version IN ('1', '2', '3') "
        "AND stage_issue_price_minor = stage_amount_minor "
        "AND batch_issue_price_minor = batch_exposure_minor) OR "
        "(request_schema_version = '4' "
        "AND stage_issue_price_minor = 0 AND batch_issue_price_minor = 0)",
    )
    op.create_check_constraint(
        "ck_ew_voucher_production_approval_issue_price_sign",
        APPROVAL,
        "stage_issue_price_minor >= 0 AND batch_issue_price_minor >= 0",
    )


def downgrade() -> None:
    # Every durable use is checked BEFORE any DDL. A gift batch or approval that
    # exists cannot be expressed by the previous schema at all, so this refuses
    # rather than dropping the columns that record what it cost.
    op.get_bind().execute(sa.text(f"LOCK TABLE {BATCH}, {APPROVAL} IN ACCESS EXCLUSIVE MODE"))
    has_gift = (
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT EXISTS (SELECT 1 FROM {BATCH} "
                "WHERE request_schema_version = '4' OR product_contract_version = :gift) "
                f"OR EXISTS (SELECT 1 FROM {APPROVAL} "
                "WHERE request_schema_version = '4' OR product_contract_version = :gift)"
            ),
            {"gift": GIFT_VERSION},
        )
        .scalar_one()
    )
    if has_gift:
        raise RuntimeError("gift contract downgrade refused: free-issue data exists; preserve data")
    # A paid row whose two sums disagree would lose information here, so it is a
    # refusal too rather than a silent truncation.
    inconsistent = (
        op.get_bind()
        .execute(
            sa.text(
                f"SELECT EXISTS (SELECT 1 FROM {BATCH} "
                "WHERE voucher_issue_price_minor <> voucher_unit_price_minor "
                "OR total_issue_price_minor <> total_exposure_minor) "
                f"OR EXISTS (SELECT 1 FROM {APPROVAL} "
                "WHERE stage_issue_price_minor <> stage_amount_minor "
                "OR batch_issue_price_minor <> batch_exposure_minor)"
            )
        )
        .scalar_one()
    )
    if inconsistent:
        raise RuntimeError("gift contract downgrade refused: issue prices differ from nominals; preserve data")

    op.drop_constraint("ck_ew_voucher_production_approval_issue_price_sign", APPROVAL, type_="check")
    op.drop_constraint("ck_ew_voucher_production_approval_issue_price", APPROVAL, type_="check")
    op.drop_constraint("ck_ew_voucher_production_approval_product_amount", APPROVAL, type_="check")
    op.create_check_constraint(
        "ck_ew_voucher_production_approval_product_amount",
        APPROVAL,
        "request_schema_version <> '3' OR (batch_recipient_count >= 1 "
        "AND batch_exposure_minor = batch_recipient_count * 1000 "
        "AND stage_amount_minor = CASE WHEN stage = 'freeze' THEN batch_exposure_minor "
        "WHEN stage IN ('create', 'pay', 'refund') THEN stage_target_count * 1000 ELSE 0 END)",
    )
    op.drop_constraint("ck_ew_voucher_production_approval_product", APPROVAL, type_="check")
    op.create_check_constraint(
        "ck_ew_voucher_production_approval_product",
        APPROVAL,
        "(request_schema_version IN ('1', '2') AND product_contract_version = 'easyweek-production-15eur-v1') "
        "OR (request_schema_version = '3' AND product_contract_version = 'easyweek-production-10eur-v1')",
    )
    op.drop_constraint("ck_ew_voucher_production_batch_issue_price_contract", BATCH, type_="check")
    op.drop_constraint("ck_ew_voucher_production_batch_issue_price_matches", BATCH, type_="check")
    op.drop_constraint("ck_ew_voucher_production_batch_unit_price", BATCH, type_="check")
    op.create_check_constraint("ck_ew_voucher_production_batch_unit_price", BATCH, PREVIOUS_PRODUCT_CHECK)
    op.drop_constraint("ck_ew_voucher_production_batch_basis", BATCH, type_="check")
    op.create_check_constraint(
        "ck_ew_voucher_production_batch_basis",
        BATCH,
        "(request_schema_version = '1' AND recipient_basis = 'operator_manual_selection') OR "
        "(request_schema_version IN ('2', '3') AND recipient_basis IN "
        "('earned_first_visit', 'operator_manual_selection', 'mixed'))",
    )
    op.drop_column(APPROVAL, "batch_issue_price_minor")
    op.drop_column(APPROVAL, "stage_issue_price_minor")
    op.drop_column(BATCH, "total_issue_price_minor")
    op.drop_column(BATCH, "voucher_issue_price_minor")
