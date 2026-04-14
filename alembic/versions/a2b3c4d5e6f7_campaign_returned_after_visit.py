"""campaign: returned_after_first_visit exclusion counters

Добавляет два поля для новой логики исключения клиентов, которые уже
вернулись в салон после окончания периода кампании:

campaign_runs:
  - excluded_returned_after_visit  — счётчик клиентов, исключённых по новой причине

campaign_recipients:
  - records_after_period           — кол-во не-удалённых CRM-записей после period_end

Revision ID: a2b3c4d5e6f7
Revises: f1a2b3c4d5e6
Create Date: 2026-04-14 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "a2b3c4d5e6f7"
down_revision: Union[str, Sequence[str], None] = "c5d6e7f8a9b0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "campaign_runs",
        sa.Column(
            "excluded_returned_after_visit",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "campaign_recipients",
        sa.Column(
            "records_after_period",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
    )


def downgrade() -> None:
    op.drop_column("campaign_runs", "excluded_returned_after_visit")
    op.drop_column("campaign_recipients", "records_after_period")
