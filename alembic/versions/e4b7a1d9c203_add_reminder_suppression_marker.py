"""add intentional reminder suppression marker (PR-11.2, §30.13)

Revision ID: e4b7a1d9c203
Revises: d5a8c31e7f04
Create Date: 2026-09-07
"""

import sqlalchemy as sa

from alembic import op

revision = "e4b7a1d9c203"
down_revision = "d5a8c31e7f04"
branch_labels = None
depends_on = None

_LEDGER = "easyweek_migration_ledger"
_AT = "reminders_suppressed_at"
_DIGEST = "reminder_suppression_plan_digest"
_REASON = "reminder_suppression_reason_code"
_REASON_VALUE = "service_category_not_allowed"
_COMPLETE_CHECK = "ck_easyweek_migration_ledger_reminder_suppression_complete"
_REASON_CHECK = "ck_easyweek_migration_ledger_reminder_suppression_reason"
_EXCLUSIVE_CHECK = "ck_easyweek_migration_ledger_reminder_marker_exclusive"
_INDEX = "ix_easyweek_migration_ledger_reminder_suppression"


def upgrade() -> None:
    op.add_column(_LEDGER, sa.Column(_AT, sa.DateTime(timezone=True), nullable=True))
    op.add_column(_LEDGER, sa.Column(_DIGEST, sa.String(length=64), nullable=True))
    op.add_column(_LEDGER, sa.Column(_REASON, sa.String(length=64), nullable=True))
    op.create_check_constraint(
        _COMPLETE_CHECK,
        _LEDGER,
        f"(({_AT} IS NULL) = ({_DIGEST} IS NULL)) AND (({_AT} IS NULL) = ({_REASON} IS NULL))",
    )
    op.create_check_constraint(
        _REASON_CHECK,
        _LEDGER,
        f"{_REASON} IS NULL OR {_REASON} = '{_REASON_VALUE}'",
    )
    op.create_check_constraint(
        _EXCLUSIVE_CHECK,
        _LEDGER,
        f"NOT (reminders_handed_over_at IS NOT NULL AND {_AT} IS NOT NULL)",
    )
    op.create_index(
        _INDEX,
        _LEDGER,
        ["source_provider", "source_company_id", "source_record_id"],
        unique=False,
        postgresql_where=sa.text(f"{_AT} IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index(_INDEX, table_name=_LEDGER)
    op.drop_constraint(_EXCLUSIVE_CHECK, _LEDGER, type_="check")
    op.drop_constraint(_REASON_CHECK, _LEDGER, type_="check")
    op.drop_constraint(_COMPLETE_CHECK, _LEDGER, type_="check")
    op.drop_column(_LEDGER, _REASON)
    op.drop_column(_LEDGER, _DIGEST)
    op.drop_column(_LEDGER, _AT)
