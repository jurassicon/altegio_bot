"""add the owner-approved test recipient basis (§36.11)

The canary has to send to somebody. The owner's test account cannot pass the
first-visit proof — its history cannot be cleared, and a fresh account per
attempt is not a workable way to test — so one pre-configured account is
approved as a TEST IDENTITY. That is not an entitlement, and the schema is where
the difference stops being a matter of trust.

Two things are added, and both are about making the two bases impossible to
confuse:

* ``campaign_recipients.easyweek_test_customer_uuid`` — the typed binding. Its
  presence IS the basis, rather than a key in ``meta`` that nothing enforces,
  and a CHECK forbids the earned-first-visit source proof on the same row. A
  test recipient cannot be edited into a first visit nobody made.

* ``easyweek_campaign_voucher_delivery_ledger.recipient_basis`` — the same
  distinction in the canary's own state, with ``source_booking_uuid`` becoming
  nullable so a test row can name no booking at all. The CHECK is an equality,
  not an implication: an earned row must have a booking and a test row must not,
  which is what stops a borrowed, random or customer-shaped UUID from being
  written into that column to satisfy a NOT NULL.

Existing rows are earned by definition — every one of them was created under the
§36 contract, which required a source booking — so the server default classifies
them without a backfill pass and without a guess.

The entitlement uniqueness becomes partial rather than disappearing: it still
holds for earned rows, which are the only rows that have an entitlement to be
unique about. A separate partial unique index does the equivalent job for test
rows, keyed on the only identity they have.

Revision ID: d4e7a1c95b30
Revises: c9d2e6f70b41
Create Date: 2026-09-13
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "d4e7a1c95b30"
down_revision = "c9d2e6f70b41"
branch_labels = None
depends_on = None

LEDGER = "easyweek_campaign_voucher_delivery_ledger"
RECIPIENTS = "campaign_recipients"

BASIS_EARNED = "earned_first_visit"
BASIS_TEST = "owner_test_account"


def upgrade() -> None:
    # -- campaign_recipients: the typed test binding ------------------------
    op.add_column(
        RECIPIENTS,
        sa.Column("easyweek_test_customer_uuid", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.create_check_constraint(
        "ck_campaign_recipients_test_customer_provider",
        RECIPIENTS,
        "easyweek_test_customer_uuid IS NULL OR provider = 'easyweek'",
    )
    op.create_check_constraint(
        "ck_campaign_recipients_test_customer_has_no_source_proof",
        RECIPIENTS,
        "easyweek_test_customer_uuid IS NULL OR ("
        "source_easyweek_event_id IS NULL "
        "AND source_record_id IS NULL "
        "AND source_booking_uuid IS NULL "
        "AND source_visits_total IS NULL "
        "AND source_visits_total_updated_at IS NULL)",
    )
    op.create_index(
        "uq_campaign_recipients_test_customer_per_run",
        RECIPIENTS,
        ["provider", "campaign_run_id", "easyweek_test_customer_uuid"],
        unique=True,
        postgresql_where=sa.text("easyweek_test_customer_uuid IS NOT NULL"),
    )

    # -- the ledger: two bases, one of which names no booking ---------------
    op.add_column(
        LEDGER,
        sa.Column(
            "recipient_basis",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text(f"'{BASIS_EARNED}'"),
        ),
    )
    op.alter_column(LEDGER, "source_booking_uuid", existing_type=postgresql.UUID(), nullable=True)

    # The old rule assumed every row has an entitlement. Replace it with the
    # partial form before the new basis can create a row that has none.
    op.drop_constraint("uq_ew_voucher_delivery_entitlement", LEDGER, type_="unique")
    op.create_index(
        "uq_ew_voucher_delivery_entitlement",
        LEDGER,
        ["provider", "company_id", "campaign_code", "source_booking_uuid"],
        unique=True,
        postgresql_where=sa.text(f"recipient_basis = '{BASIS_EARNED}'"),
    )
    op.create_index(
        "uq_ew_voucher_delivery_test_identity",
        LEDGER,
        ["provider", "company_id", "campaign_code", "easyweek_customer_uuid"],
        unique=True,
        postgresql_where=sa.text(f"recipient_basis = '{BASIS_TEST}'"),
    )

    op.create_check_constraint(
        "ck_ew_voucher_delivery_basis",
        LEDGER,
        f"recipient_basis IN ('{BASIS_EARNED}', '{BASIS_TEST}')",
    )
    op.create_check_constraint(
        "ck_ew_voucher_delivery_basis_source",
        LEDGER,
        f"(recipient_basis = '{BASIS_EARNED}') = (source_booking_uuid IS NOT NULL)",
    )


def downgrade() -> None:
    # A test row cannot exist under the old schema: it has no source booking,
    # and the column it would have to fill is about to become NOT NULL again.
    # Refuse rather than invent a booking UUID to satisfy the constraint.
    bound = op.get_bind()
    test_rows = bound.execute(
        sa.text(f"SELECT count(*) FROM {LEDGER} WHERE recipient_basis = :basis"),  # noqa: S608 - fixed table name
        {"basis": BASIS_TEST},
    ).scalar_one()
    if int(test_rows or 0) > 0:
        raise RuntimeError(
            "refusing to downgrade: the voucher delivery ledger holds "
            f"{test_rows} owner_test_account row(s), which the earlier schema "
            "cannot represent without inventing a source booking"
        )

    op.drop_constraint("ck_ew_voucher_delivery_basis_source", LEDGER, type_="check")
    op.drop_constraint("ck_ew_voucher_delivery_basis", LEDGER, type_="check")
    op.drop_index("uq_ew_voucher_delivery_test_identity", table_name=LEDGER)
    op.drop_index("uq_ew_voucher_delivery_entitlement", table_name=LEDGER)
    op.create_unique_constraint(
        "uq_ew_voucher_delivery_entitlement",
        LEDGER,
        ["provider", "company_id", "campaign_code", "source_booking_uuid"],
    )
    op.alter_column(LEDGER, "source_booking_uuid", existing_type=postgresql.UUID(), nullable=False)
    op.drop_column(LEDGER, "recipient_basis")

    op.drop_index("uq_campaign_recipients_test_customer_per_run", table_name=RECIPIENTS)
    op.drop_constraint("ck_campaign_recipients_test_customer_has_no_source_proof", RECIPIENTS, type_="check")
    op.drop_constraint("ck_campaign_recipients_test_customer_provider", RECIPIENTS, type_="check")
    op.drop_column(RECIPIENTS, "easyweek_test_customer_uuid")
