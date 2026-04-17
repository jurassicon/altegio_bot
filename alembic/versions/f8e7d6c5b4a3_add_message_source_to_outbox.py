"""add message_source to outbox_messages

Revision ID: f8e7d6c5b4a3
Revises: 075dc7843609
Create Date: 2026-04-16 00:00:00.000000

Additive migration — safe for rolling / manual deploy.

Adds a nullable-equivalent column (NOT NULL with server_default='bot') to
outbox_messages.  All existing rows automatically receive 'bot' via the
server default.  Old code that does not reference this column continues to
work during rollout because:

  * INSERTs from old code omit the column → DB fills server_default='bot'.
  * SELECTs from old code do not include the column → no error.
  * New code that sets message_source='operator' only runs when the feature
    flag chatwoot_operator_relay_enabled=True, which should be enabled only
    after the migration has been applied.

Recommended deploy order:
  1. alembic upgrade head   (this migration)
  2. Deploy / restart api + workers
  3. (Later) set CHATWOOT_OPERATOR_RELAY_ENABLED=True to activate relay
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'f8e7d6c5b4a3'
down_revision: Union[str, Sequence[str], None] = '075dc7843609'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'outbox_messages',
        sa.Column(
            'message_source',
            sa.String(32),
            nullable=False,
            server_default='bot',
        ),
    )


def downgrade() -> None:
    op.drop_column('outbox_messages', 'message_source')
