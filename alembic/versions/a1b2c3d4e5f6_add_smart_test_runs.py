"""add smart_test_runs table

Revision ID: a1b2c3d4e5f6
Revises: e1f2a3b4c5d6
Create Date: 2026-02-27 20:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'e1f2a3b4c5d6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'smart_test_runs',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('test_code', sa.String(length=128), nullable=False),
        sa.Column('phone_e164', sa.String(length=32), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=False),
        sa.Column('location_id', sa.Integer(), nullable=True),
        sa.Column('loyalty_card_id', sa.String(length=128), nullable=True),
        sa.Column('loyalty_card_number', sa.String(length=64), nullable=True),
        sa.Column('loyalty_card_type_id', sa.String(length=64), nullable=True),
        sa.Column(
            'provider_message_id', sa.String(length=128), nullable=True
        ),
        sa.Column('template_name', sa.String(length=128), nullable=True),
        sa.Column('outcome', sa.String(length=32), nullable=True),
        sa.Column('deleted_at', sa.DateTime(timezone=True), nullable=True),
        sa.Column('delete_status', sa.String(length=32), nullable=True),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.Column(
            'meta',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        op.f('ix_smart_test_runs_company_id'),
        'smart_test_runs',
        ['company_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_smart_test_runs_created_at'),
        'smart_test_runs',
        ['created_at'],
        unique=False,
    )
    op.create_index(
        op.f('ix_smart_test_runs_phone_e164'),
        'smart_test_runs',
        ['phone_e164'],
        unique=False,
    )
    op.create_index(
        op.f('ix_smart_test_runs_provider_message_id'),
        'smart_test_runs',
        ['provider_message_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_smart_test_runs_test_code'),
        'smart_test_runs',
        ['test_code'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f('ix_smart_test_runs_test_code'), table_name='smart_test_runs'
    )
    op.drop_index(
        op.f('ix_smart_test_runs_provider_message_id'),
        table_name='smart_test_runs',
    )
    op.drop_index(
        op.f('ix_smart_test_runs_phone_e164'), table_name='smart_test_runs'
    )
    op.drop_index(
        op.f('ix_smart_test_runs_created_at'), table_name='smart_test_runs'
    )
    op.drop_index(
        op.f('ix_smart_test_runs_company_id'), table_name='smart_test_runs'
    )
    op.drop_table('smart_test_runs')
