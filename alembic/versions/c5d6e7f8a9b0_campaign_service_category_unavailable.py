"""campaign_runs: добавить excluded_service_category_unavailable.

Добавляет счётчик клиентов, для которых Altegio service category API
был недоступен при определении ресничности услуги. Такие клиенты
исключаются с причиной 'service_category_unavailable' вместо молчаливого
считывания non-lash (что дало бы ложно-eligible клиентов).

Revision ID: c5d6e7f8a9b0
Revises: b4c5d6e7f8a9
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision = "c5d6e7f8a9b0"
down_revision = "b4c5d6e7f8a9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "campaign_runs",
        sa.Column(
            "excluded_service_category_unavailable",
            sa.Integer(),
            nullable=False,
            server_default="0",
        ),
    )


def downgrade() -> None:
    op.drop_column("campaign_runs", "excluded_service_category_unavailable")
