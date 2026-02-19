"""job defaults for message_jobs

Revision ID: "b09b0b0548dc"
Revises: "0920db551b57"
Create Date: 2026-02-10
"""

from alembic import op
import sqlalchemy as sa


revision = "b09b0b0548dc"
down_revision = "0920db551b57"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        "UPDATE message_jobs SET status = 'queued' WHERE status IS NULL"
    )
    op.execute("UPDATE message_jobs SET attempts = 0 WHERE attempts IS NULL")
    op.execute(
        "UPDATE message_jobs SET max_attempts = 5 WHERE max_attempts IS NULL"
    )

    op.alter_column(
        "message_jobs",
        "status",
        existing_type=sa.String(length=32),
        nullable=False,
        server_default=sa.text("'queued'"),
    )
    op.alter_column(
        "message_jobs",
        "attempts",
        existing_type=sa.Integer(),
        nullable=False,
        server_default=sa.text("0"),
    )
    op.alter_column(
        "message_jobs",
        "max_attempts",
        existing_type=sa.Integer(),
        nullable=False,
        server_default=sa.text("5"),
    )


def downgrade() -> None:
    op.alter_column(
        "message_jobs",
        "max_attempts",
        existing_type=sa.Integer(),
        nullable=True,
        server_default=None,
    )
    op.alter_column(
        "message_jobs",
        "attempts",
        existing_type=sa.Integer(),
        nullable=True,
        server_default=None,
    )
    op.alter_column(
        "message_jobs",
        "status",
        existing_type=sa.String(length=32),
        nullable=True,
        server_default=None,
    )