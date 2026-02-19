"""add whatsapp_events

Revision ID: 98439805d041
Revises: 6f237a8a2850
Create Date: 2026-02-18 15:20:00.549048

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '98439805d041'
down_revision: Union[str, Sequence[str], None] = '6f237a8a2850'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    pass


def downgrade() -> None:
    """Downgrade schema."""
    pass
