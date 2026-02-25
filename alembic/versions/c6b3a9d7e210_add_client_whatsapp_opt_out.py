"""add client whatsapp opt-out

Revision ID: c6b3a9d7e210
Revises: 2c24d34e45e2
Create Date: 2026-02-25 12:10:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = 'c6b3a9d7e210'
down_revision: Union[str, Sequence[str], None] = '2c24d34e45e2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        'clients',
        sa.Column(
            'wa_opted_out',
            sa.Boolean(),
            nullable=False,
            server_default=sa.text('false'),
        ),
    )
    op.add_column(
        'clients',
        sa.Column(
            'wa_opted_out_at',
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        'clients',
        sa.Column(
            'wa_opt_out_reason',
            sa.Text(),
            nullable=True,
        ),
    )

    op.create_index(
        'ix_clients_wa_opted_out',
        'clients',
        ['wa_opted_out'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index('ix_clients_wa_opted_out', table_name='clients')
    op.drop_column('clients', 'wa_opt_out_reason')
    op.drop_column('clients', 'wa_opted_out_at')
    op.drop_column('clients', 'wa_opted_out')