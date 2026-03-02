"""add chatwoot_conversation_id to whatsapp_events

Revision ID: d1e2f3a4b5c6
Revises: b2c3d4e5f6a7
Create Date: 2026-03-02 18:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'd1e2f3a4b5c6'
down_revision: Union[str, Sequence[str], None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'whatsapp_events',
        sa.Column(
            'chatwoot_conversation_id',
            sa.BigInteger(),
            nullable=True,
        ),
    )
    op.create_index(
        'ix_whatsapp_events_chatwoot_conversation_id',
        'whatsapp_events',
        ['chatwoot_conversation_id'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        'ix_whatsapp_events_chatwoot_conversation_id',
        table_name='whatsapp_events',
    )
    op.drop_column('whatsapp_events', 'chatwoot_conversation_id')
