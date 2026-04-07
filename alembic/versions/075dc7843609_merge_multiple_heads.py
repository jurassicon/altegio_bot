"""merge multiple heads

Revision ID: 075dc7843609
Revises: d1e2f3a4b5c6, e4f5a6b7c8d9
Create Date: 2026-04-07 16:10:05.102992

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '075dc7843609'
down_revision: Union[str, Sequence[str], None] = ('d1e2f3a4b5c6', 'e4f5a6b7c8d9')
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
