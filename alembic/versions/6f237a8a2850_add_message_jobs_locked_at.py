"""add message_jobs locked_at

Revision ID: 6f237a8a2850
Revises: b09b0b0548dc
Create Date: 2026-02-13 21:44:49.952448

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '6f237a8a2850'
down_revision: Union[str, Sequence[str], None] = 'b09b0b0548dc'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
