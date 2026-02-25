'''ensure message_jobs locked_at exists

Revision ID: f3a1c4e9b2d0
Revises: c6b3a9d7e210
Create Date: 2026-02-25 00:00:00.000000
'''

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = 'f3a1c4e9b2d0'
down_revision: Union[str, Sequence[str], None] = 'c6b3a9d7e210'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            'ALTER TABLE message_jobs '
            'ADD COLUMN IF NOT EXISTS locked_at TIMESTAMPTZ'
        )
    )
    op.execute(
        sa.text(
            'CREATE INDEX IF NOT EXISTS ix_message_jobs_locked_at '
            'ON message_jobs (locked_at)'
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text('DROP INDEX IF EXISTS ix_message_jobs_locked_at')
    )
    op.execute(
        sa.text(
            'ALTER TABLE message_jobs '
            'DROP COLUMN IF EXISTS locked_at'
        )
    )