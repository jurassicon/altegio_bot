"""campaign_recipients: CRM diagnostics fields

Добавляет диагностические поля к campaign_recipients для хранения
данных из Altegio CRM API при сегментации новых клиентов:

- lash_records_in_period        — ресничные записи в периоде
- confirmed_lash_records_in_period — подтверждённые ресничные записи
- service_titles_in_period      — названия услуг (JSONB-массив строк)
- total_records_before_period_any — все записи до периода (из CRM)
- local_client_found            — найден ли локальный Client в нашей БД

Revision ID: f1a2b3c4d5e6
Revises: e4f5a6b7c8d9
Create Date: 2026-04-08 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "f1a2b3c4d5e6"
down_revision: Union[str, Sequence[str], None] = "e4f5a6b7c8d9"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "campaign_recipients",
        sa.Column(
            "lash_records_in_period",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "campaign_recipients",
        sa.Column(
            "confirmed_lash_records_in_period",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "campaign_recipients",
        sa.Column(
            "service_titles_in_period",
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
    )
    op.add_column(
        "campaign_recipients",
        sa.Column(
            "total_records_before_period_any",
            sa.Integer(),
            server_default="0",
            nullable=False,
        ),
    )
    op.add_column(
        "campaign_recipients",
        sa.Column(
            "local_client_found",
            sa.Boolean(),
            server_default="false",
            nullable=False,
        ),
    )


def downgrade() -> None:
    for col in (
        "local_client_found",
        "total_records_before_period_any",
        "service_titles_in_period",
        "confirmed_lash_records_in_period",
        "lash_records_in_period",
    ):
        op.drop_column("campaign_recipients", col)
