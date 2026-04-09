"""campaign_runs: добавить excluded_crm_unavailable + merge heads.

Слияние двух параллельных голов:
  - 075dc7843609 (merge предыдущих campaign_mvp и chatwoot веток)
  - f1a2b3c4d5e6 (campaign_recipients: CRM diagnostics fields)

Добавляет счётчик excluded_crm_unavailable в campaign_runs.
Считает клиентов, для которых Altegio CRM API был недоступен при сегментации.

Revision ID: b4c5d6e7f8a9
Revises: 075dc7843609, f1a2b3c4d5e6
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "b4c5d6e7f8a9"
down_revision = ("075dc7843609", "f1a2b3c4d5e6")
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "campaign_runs",
        sa.Column(
            "excluded_crm_unavailable",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )


def downgrade() -> None:
    op.drop_column("campaign_runs", "excluded_crm_unavailable")
