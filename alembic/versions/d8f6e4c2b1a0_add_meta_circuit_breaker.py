"""add meta circuit breaker table

Revision ID: d8f6e4c2b1a0
Revises: d0e1f2a3b4c5
Create Date: 2026-06-15 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "d8f6e4c2b1a0"
down_revision: Union[str, Sequence[str], None] = "d0e1f2a3b4c5"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS meta_circuit_breaker ("
            "scope VARCHAR(64) PRIMARY KEY, "
            "state VARCHAR(16) NOT NULL DEFAULT 'open', "
            "reason VARCHAR(256), "
            "opened_at TIMESTAMPTZ, "
            "closed_at TIMESTAMPTZ, "
            "updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
            "next_probe_at TIMESTAMPTZ, "
            "probe_attempts INTEGER NOT NULL DEFAULT 0, "
            "last_error_kind VARCHAR(64), "
            "last_error_code VARCHAR(64)"
            ")"
        )
    )


def downgrade() -> None:
    op.execute(sa.text("DROP TABLE IF EXISTS meta_circuit_breaker"))
