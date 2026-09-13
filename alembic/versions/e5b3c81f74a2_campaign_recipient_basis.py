"""add the typed campaign recipient basis (§37.1)

EasyWeek only started on 1 September, so the transitional August list cannot be
proven from EasyWeek data. An operator assembles that one list by typing phone
numbers, and every later list comes from the segmenter as before. Those are two
genuinely different grounds for a row to exist, and a third — the one approved
canary test account (§36.11) — already existed without being named.

So the basis becomes a column instead of something a reader infers:

  earned_first_visit         the segmenter proved a first visit (§33)
  owner_test_account         the one pre-configured canary account (§36.11)
  operator_manual_selection  an operator decided (§37.1)

`easyweek_customer_uuid` holds the customer a manual selection was proven
against, live, before the row was written. `auto_excluded_reason` keeps the
segmenter's original verdict when an operator includes somebody anyway, so an
override never erases what it overrode.

The CHECKs are equivalences rather than implications. A basis without its
identity column is as wrong as an identity column without its basis, and a
manual row is forbidden — not merely expected to lack — the earned source proof
columns, so no later code path can quietly promote a decision into a first visit
nobody made.

Existing rows are classified before the constraints are added, not guessed at
afterwards: anything already carrying `easyweek_test_customer_uuid` is the test
account, and everything else is what the segmenter produced.

Revision ID: e5b3c81f74a2
Revises: d4e7a1c95b30
Create Date: 2026-09-13
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "e5b3c81f74a2"
down_revision = "d4e7a1c95b30"
branch_labels = None
depends_on = None

RECIPIENTS = "campaign_recipients"

BASIS_EARNED = "earned_first_visit"
BASIS_TEST = "owner_test_account"
BASIS_MANUAL = "operator_manual_selection"


def upgrade() -> None:
    op.add_column(
        RECIPIENTS,
        sa.Column(
            "recipient_basis",
            sa.String(length=32),
            nullable=False,
            server_default=sa.text(f"'{BASIS_EARNED}'"),
        ),
    )
    op.add_column(
        RECIPIENTS,
        sa.Column("easyweek_customer_uuid", postgresql.UUID(as_uuid=True), nullable=True),
    )
    op.add_column(RECIPIENTS, sa.Column("auto_excluded_reason", sa.String(length=64), nullable=True))

    # Classify what is already there BEFORE the equivalences are enforced. The
    # §36.11 rows are the only ones that are not what the segmenter produced,
    # and they identify themselves by the binding they already carry.
    op.execute(
        sa.text(
            f"UPDATE {RECIPIENTS} SET recipient_basis = :basis "  # noqa: S608 - fixed table name
            "WHERE easyweek_test_customer_uuid IS NOT NULL"
        ).bindparams(basis=BASIS_TEST)
    )

    op.create_check_constraint(
        "ck_campaign_recipients_basis",
        RECIPIENTS,
        f"recipient_basis IN ('{BASIS_EARNED}', '{BASIS_TEST}', '{BASIS_MANUAL}')",
    )
    op.create_check_constraint(
        "ck_campaign_recipients_basis_provider",
        RECIPIENTS,
        f"recipient_basis = '{BASIS_EARNED}' OR provider = 'easyweek'",
    )
    op.create_check_constraint(
        "ck_campaign_recipients_basis_test_binding",
        RECIPIENTS,
        f"(recipient_basis = '{BASIS_TEST}') = (easyweek_test_customer_uuid IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_campaign_recipients_basis_manual_binding",
        RECIPIENTS,
        f"(recipient_basis = '{BASIS_MANUAL}') = (easyweek_customer_uuid IS NOT NULL)",
    )
    op.create_check_constraint(
        "ck_campaign_recipients_manual_has_no_source_proof",
        RECIPIENTS,
        f"recipient_basis <> '{BASIS_MANUAL}' OR ("
        "source_easyweek_event_id IS NULL "
        "AND source_record_id IS NULL "
        "AND source_booking_uuid IS NULL "
        "AND source_visits_total IS NULL "
        "AND source_visits_total_updated_at IS NULL)",
    )
    op.create_check_constraint(
        "ck_campaign_recipients_auto_excluded_provider",
        RECIPIENTS,
        "auto_excluded_reason IS NULL OR provider = 'easyweek'",
    )
    op.create_index(
        "uq_campaign_recipients_manual_customer_per_run",
        RECIPIENTS,
        ["provider", "campaign_run_id", "easyweek_customer_uuid"],
        unique=True,
        postgresql_where=sa.text("easyweek_customer_uuid IS NOT NULL"),
    )


def downgrade() -> None:
    # The §36.11 basis survives a downgrade: it was carried by
    # `easyweek_test_customer_uuid` before this revision existed and still is,
    # so dropping the column loses no test-account binding.
    #
    # A manual selection has nowhere to go, though — the earlier schema has no
    # way to say "an operator decided" — so refuse rather than silently leave
    # rows that will read as earned first visits.
    bound = op.get_bind()
    manual_rows = bound.execute(
        sa.text(f"SELECT count(*) FROM {RECIPIENTS} WHERE recipient_basis = :basis"),  # noqa: S608 - fixed table
        {"basis": BASIS_MANUAL},
    ).scalar_one()
    if int(manual_rows or 0) > 0:
        raise RuntimeError(
            f"refusing to downgrade: {manual_rows} campaign recipient(s) are on the "
            "operator_manual_selection basis, which the earlier schema would silently "
            "turn into earned first visits"
        )

    op.drop_index("uq_campaign_recipients_manual_customer_per_run", table_name=RECIPIENTS)
    for name in (
        "ck_campaign_recipients_auto_excluded_provider",
        "ck_campaign_recipients_manual_has_no_source_proof",
        "ck_campaign_recipients_basis_manual_binding",
        "ck_campaign_recipients_basis_test_binding",
        "ck_campaign_recipients_basis_provider",
        "ck_campaign_recipients_basis",
    ):
        op.drop_constraint(name, RECIPIENTS, type_="check")
    op.drop_column(RECIPIENTS, "auto_excluded_reason")
    op.drop_column(RECIPIENTS, "easyweek_customer_uuid")
    op.drop_column(RECIPIENTS, "recipient_basis")
