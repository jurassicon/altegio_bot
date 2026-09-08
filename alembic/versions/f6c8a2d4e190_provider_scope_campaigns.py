"""provider-scope campaign runs and recipients (PR-13)

Revision ID: f6c8a2d4e190
Revises: e4b7a1d9c203
Create Date: 2026-09-07
"""

import sqlalchemy as sa

from alembic import op

revision = "f6c8a2d4e190"
down_revision = "e4b7a1d9c203"
branch_labels = None
depends_on = None

_DEFAULT = sa.text("'altegio'")


def upgrade() -> None:
    # The server default both backfills historical campaign rows and keeps the
    # rolling Alembic upgrade compatible with an older application process.
    # PR-13 application code nevertheless supplies provider explicitly.
    op.add_column(
        "campaign_runs",
        sa.Column("provider", sa.String(length=32), server_default=_DEFAULT, nullable=False),
    )
    op.add_column(
        "campaign_recipients",
        sa.Column("provider", sa.String(length=32), server_default=_DEFAULT, nullable=False),
    )

    op.create_unique_constraint(
        "uq_campaign_runs_id_provider",
        "campaign_runs",
        ["id", "provider"],
    )
    op.drop_constraint(
        "campaign_recipients_campaign_run_id_fkey",
        "campaign_recipients",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "fk_campaign_recipients_run_provider",
        "campaign_recipients",
        "campaign_runs",
        ["campaign_run_id", "provider"],
        ["id", "provider"],
        ondelete="CASCADE",
    )
    op.create_index(
        "ix_campaign_runs_provider_campaign_created",
        "campaign_runs",
        ["provider", "campaign_code", "created_at"],
    )
    op.create_index(
        "ix_campaign_recipients_provider_run",
        "campaign_recipients",
        ["provider", "campaign_run_id"],
    )
    op.create_index(
        "ix_campaign_recipients_provider_company_client",
        "campaign_recipients",
        ["provider", "company_id", "client_id"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_campaign_recipients_provider_company_client",
        table_name="campaign_recipients",
    )
    op.drop_index("ix_campaign_recipients_provider_run", table_name="campaign_recipients")
    op.drop_index("ix_campaign_runs_provider_campaign_created", table_name="campaign_runs")
    op.drop_constraint(
        "fk_campaign_recipients_run_provider",
        "campaign_recipients",
        type_="foreignkey",
    )
    op.create_foreign_key(
        "campaign_recipients_campaign_run_id_fkey",
        "campaign_recipients",
        "campaign_runs",
        ["campaign_run_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.drop_constraint("uq_campaign_runs_id_provider", "campaign_runs", type_="unique")
    op.drop_column("campaign_recipients", "provider")
    op.drop_column("campaign_runs", "provider")
