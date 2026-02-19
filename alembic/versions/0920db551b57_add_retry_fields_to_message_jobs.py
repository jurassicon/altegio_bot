"""add retry fields to message_jobs

Revision ID: 0920db551b57
Revises: 8f3adb755462
Create Date: 2026-02-10 16:32:48.179138
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0920db551b57"
down_revision: Union[str, Sequence[str], None] = "8f3adb755462"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

_OUTBOX_JOB_ID_UNIQUE_INDEX = "uq_outbox_messages_job_id"


def upgrade() -> None:
    op.add_column(
        "message_jobs",
        sa.Column(
            "max_attempts",
            sa.Integer(),
            nullable=False,
            server_default="5",
        ),
    )

    # Убираем дефолт на уровне БД (пусть дефолт живёт в модели).
    op.alter_column(
        "message_jobs",
        "max_attempts",
        server_default=None,
    )

    # Теперь на один job_id может быть несколько outbox-строк (история попыток).
    op.drop_index(
        _OUTBOX_JOB_ID_UNIQUE_INDEX,
        table_name="outbox_messages",
        postgresql_where=sa.text("(job_id IS NOT NULL)"),
    )


def downgrade() -> None:
    # ВНИМАНИЕ: если в outbox_messages уже есть несколько строк на один job_id,
    # то создание UNIQUE индекса упадёт. Для downgrade это нормально, но
    # если хочешь сделать downgrade “железобетонным”, нужно предварительно
    # дедуплицировать данные (например, оставить последнюю запись на job_id).
    op.create_index(
        _OUTBOX_JOB_ID_UNIQUE_INDEX,
        "outbox_messages",
        ["job_id"],
        unique=True,
        postgresql_where=sa.text("(job_id IS NOT NULL)"),
    )

    op.drop_column("message_jobs", "max_attempts")
