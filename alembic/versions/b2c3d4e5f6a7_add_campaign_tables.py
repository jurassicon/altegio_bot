"""add campaign_runs and campaign_recipients tables

Revision ID: b2c3d4e5f6a7
Revises: a1b2c3d4e5f6
Create Date: 2026-02-27 21:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = 'b2c3d4e5f6a7'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        'campaign_runs',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('campaign_code', sa.String(length=128), nullable=False),
        sa.Column('mode', sa.String(length=32), nullable=False),
        sa.Column(
            'company_ids',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            'period_start', sa.DateTime(timezone=True), nullable=False
        ),
        sa.Column(
            'period_end', sa.DateTime(timezone=True), nullable=False
        ),
        sa.Column(
            'status',
            sa.String(length=32),
            server_default='running',
            nullable=False,
        ),
        sa.Column(
            'total_clients_seen', sa.Integer(), server_default='0', nullable=False
        ),
        sa.Column(
            'candidates_count', sa.Integer(), server_default='0', nullable=False
        ),
        sa.Column(
            'excluded_opted_out', sa.Integer(), server_default='0', nullable=False
        ),
        sa.Column(
            'excluded_more_than_one_record',
            sa.Integer(),
            server_default='0',
            nullable=False,
        ),
        sa.Column(
            'excluded_has_arrived',
            sa.Integer(),
            server_default='0',
            nullable=False,
        ),
        sa.Column(
            'excluded_no_phone',
            sa.Integer(),
            server_default='0',
            nullable=False,
        ),
        sa.Column(
            'sent_count', sa.Integer(), server_default='0', nullable=False
        ),
        sa.Column(
            'failed_count', sa.Integer(), server_default='0', nullable=False
        ),
        sa.Column(
            'created_at',
            sa.DateTime(timezone=True),
            server_default=sa.text('now()'),
            nullable=False,
        ),
        sa.Column(
            'completed_at', sa.DateTime(timezone=True), nullable=True
        ),
        sa.Column(
            'meta',
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        op.f('ix_campaign_runs_campaign_code'),
        'campaign_runs',
        ['campaign_code'],
        unique=False,
    )
    op.create_index(
        op.f('ix_campaign_runs_created_at'),
        'campaign_runs',
        ['created_at'],
        unique=False,
    )

    op.create_table(
        'campaign_recipients',
        sa.Column('id', sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column('campaign_run_id', sa.BigInteger(), nullable=False),
        sa.Column('company_id', sa.Integer(), nullable=False),
        sa.Column('client_id', sa.BigInteger(), nullable=True),
        sa.Column('altegio_client_id', sa.BigInteger(), nullable=True),
        sa.Column('phone_e164', sa.String(length=32), nullable=True),
        sa.Column('display_name', sa.String(length=256), nullable=True),
        sa.Column(
            'status',
            sa.String(length=32),
            server_default='candidate',
            nullable=False,
        ),
        sa.Column('excluded_reason', sa.String(length=64), nullable=True),
        sa.Column(
            'total_records_in_period',
            sa.Integer(),
            server_default='0',
            nullable=False,
        ),
        sa.Column(
            'arrived_records_in_period',
            sa.Integer(),
            server_default='0',
            nullable=False,
        ),
        sa.Column(
            'is_opted_out',
            sa.Boolean(),
            server_default='false',
            nullable=False,
        ),
        sa.Column('loyalty_card_id', sa.String(length=128), nullable=True),
        sa.Column('loyalty_card_number', sa.String(length=64), nullable=True),
        sa.Column(
            'provider_message_id', sa.String(length=128), nullable=True
        ),
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
        sa.ForeignKeyConstraint(
            ['campaign_run_id'],
            ['campaign_runs.id'],
            ondelete='CASCADE',
        ),
        sa.ForeignKeyConstraint(
            ['client_id'], ['clients.id'], ondelete='SET NULL'
        ),
        sa.PrimaryKeyConstraint('id'),
    )
    op.create_index(
        op.f('ix_campaign_recipients_campaign_run_id'),
        'campaign_recipients',
        ['campaign_run_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_campaign_recipients_company_id'),
        'campaign_recipients',
        ['company_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_campaign_recipients_client_id'),
        'campaign_recipients',
        ['client_id'],
        unique=False,
    )
    op.create_index(
        op.f('ix_campaign_recipients_phone_e164'),
        'campaign_recipients',
        ['phone_e164'],
        unique=False,
    )
    op.create_index(
        op.f('ix_campaign_recipients_provider_message_id'),
        'campaign_recipients',
        ['provider_message_id'],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        op.f('ix_campaign_recipients_provider_message_id'),
        table_name='campaign_recipients',
    )
    op.drop_index(
        op.f('ix_campaign_recipients_phone_e164'),
        table_name='campaign_recipients',
    )
    op.drop_index(
        op.f('ix_campaign_recipients_client_id'),
        table_name='campaign_recipients',
    )
    op.drop_index(
        op.f('ix_campaign_recipients_company_id'),
        table_name='campaign_recipients',
    )
    op.drop_index(
        op.f('ix_campaign_recipients_campaign_run_id'),
        table_name='campaign_recipients',
    )
    op.drop_table('campaign_recipients')
    op.drop_index(
        op.f('ix_campaign_runs_created_at'), table_name='campaign_runs'
    )
    op.drop_index(
        op.f('ix_campaign_runs_campaign_code'), table_name='campaign_runs'
    )
    op.drop_table('campaign_runs')
