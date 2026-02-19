"""unique outbox job_id

Revision ID: 8f3adb755462
Revises: 16ffb80bd379
Create Date: 2026-02-06 21:58:13.744225
"""
from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "8f3adb755462"
down_revision = "16ffb80bd379"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_index(
        "uq_outbox_messages_job_id",
        "outbox_messages",
        ["job_id"],
        unique=True,
        postgresql_where=sa.text("job_id is not null"),
    )


def downgrade() -> None:
    op.drop_index(
        "uq_outbox_messages_job_id",
        table_name="outbox_messages",
    )
