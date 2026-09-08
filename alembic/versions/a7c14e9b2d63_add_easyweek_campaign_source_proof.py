"""add durable EasyWeek campaign preview source proof (PR-14)

Revision ID: a7c14e9b2d63
Revises: f6c8a2d4e190
Create Date: 2026-09-08
"""

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "a7c14e9b2d63"
down_revision = "f6c8a2d4e190"
branch_labels = None
depends_on = None

_TABLE = "campaign_recipients"


def upgrade() -> None:
    op.add_column(_TABLE, sa.Column("source_easyweek_event_id", sa.BigInteger(), nullable=True))
    op.add_column(_TABLE, sa.Column("source_record_id", sa.BigInteger(), nullable=True))
    op.add_column(_TABLE, sa.Column("source_booking_uuid", postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column(_TABLE, sa.Column("source_visits_total", sa.Integer(), nullable=True))
    op.add_column(
        _TABLE,
        sa.Column("source_visits_total_updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_campaign_recipients_source_easyweek_event",
        _TABLE,
        "easyweek_events",
        ["source_easyweek_event_id"],
        ["id"],
    )
    op.create_foreign_key(
        "fk_campaign_recipients_source_record",
        _TABLE,
        "records",
        ["source_record_id"],
        ["id"],
    )
    op.create_check_constraint(
        "ck_campaign_recipients_easyweek_source_proof_complete",
        _TABLE,
        "((source_easyweek_event_id IS NULL) = (source_record_id IS NULL)) "
        "AND ((source_easyweek_event_id IS NULL) = (source_booking_uuid IS NULL)) "
        "AND ((source_easyweek_event_id IS NULL) = (source_visits_total IS NULL)) "
        "AND ((source_easyweek_event_id IS NULL) = (source_visits_total_updated_at IS NULL))",
    )
    op.create_check_constraint(
        "ck_campaign_recipients_easyweek_source_proof_provider",
        _TABLE,
        "source_easyweek_event_id IS NULL OR provider = 'easyweek'",
    )
    op.create_check_constraint(
        "ck_campaign_recipients_easyweek_source_visits_first",
        _TABLE,
        "source_visits_total IS NULL OR source_visits_total = 1",
    )
    op.create_index(
        "ix_campaign_recipients_source_easyweek_event_id",
        _TABLE,
        ["source_easyweek_event_id"],
    )
    op.create_index(
        "ix_campaign_recipients_source_record_id",
        _TABLE,
        ["source_record_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_campaign_recipients_source_record_id", table_name=_TABLE)
    op.drop_index("ix_campaign_recipients_source_easyweek_event_id", table_name=_TABLE)
    op.drop_constraint("ck_campaign_recipients_easyweek_source_visits_first", _TABLE, type_="check")
    op.drop_constraint("ck_campaign_recipients_easyweek_source_proof_provider", _TABLE, type_="check")
    op.drop_constraint("ck_campaign_recipients_easyweek_source_proof_complete", _TABLE, type_="check")
    op.drop_constraint("fk_campaign_recipients_source_record", _TABLE, type_="foreignkey")
    op.drop_constraint("fk_campaign_recipients_source_easyweek_event", _TABLE, type_="foreignkey")
    op.drop_column(_TABLE, "source_visits_total_updated_at")
    op.drop_column(_TABLE, "source_visits_total")
    op.drop_column(_TABLE, "source_booking_uuid")
    op.drop_column(_TABLE, "source_record_id")
    op.drop_column(_TABLE, "source_easyweek_event_id")
