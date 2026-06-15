"""add meta circuit probe lease fields

Revision ID: e9a7c6b5d4f3
Revises: d8f6e4c2b1a0
Create Date: 2026-06-15 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

revision: str = "e9a7c6b5d4f3"
down_revision: Union[str, Sequence[str], None] = "d8f6e4c2b1a0"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(sa.text("ALTER TABLE meta_circuit_breaker ADD COLUMN IF NOT EXISTS probe_token VARCHAR(64)"))
    op.execute(sa.text("ALTER TABLE meta_circuit_breaker ADD COLUMN IF NOT EXISTS probe_started_at TIMESTAMPTZ"))
    op.execute(sa.text("ALTER TABLE meta_circuit_breaker ADD COLUMN IF NOT EXISTS probe_lease_until TIMESTAMPTZ"))


def downgrade() -> None:
    op.execute(sa.text("ALTER TABLE meta_circuit_breaker DROP COLUMN IF EXISTS probe_lease_until"))
    op.execute(sa.text("ALTER TABLE meta_circuit_breaker DROP COLUMN IF EXISTS probe_started_at"))
    op.execute(sa.text("ALTER TABLE meta_circuit_breaker DROP COLUMN IF EXISTS probe_token"))
