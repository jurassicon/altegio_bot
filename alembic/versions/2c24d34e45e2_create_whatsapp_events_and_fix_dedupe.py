"""create whatsapp_events and fix dedupe

Revision ID: 2c24d34e45e2
Revises: 98439805d041
Create Date: 2026-02-18 19:49:24.919653

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '2c24d34e45e2'
down_revision: Union[str, Sequence[str], None] = '98439805d041'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _table_exists(table_name: str) -> bool:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    return table_name in inspector.get_table_names()


def _get_index(table_name: str, index_name: str) -> dict | None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    for idx in inspector.get_indexes(table_name):
        if idx.get('name') == index_name:
            return idx
    return None


def upgrade() -> None:
    # 1) Создаём whatsapp_events, если её ещё нет
    if not _table_exists('whatsapp_events'):
        op.create_table(
            'whatsapp_events',
            sa.Column(
                'id',
                sa.BigInteger(),
                primary_key=True,
                autoincrement=True,
                nullable=False,
            ),
            sa.Column('dedupe_key', sa.String(length=128), nullable=False),
            sa.Column(
                'received_at',
                sa.DateTime(timezone=True),
                server_default=sa.text('now()'),
                nullable=False,
            ),
            sa.Column('processed_at', sa.DateTime(timezone=True),
                      nullable=True),
            sa.Column('status', sa.String(length=32), nullable=False),
            sa.Column('error', sa.Text(), nullable=True),
            sa.Column('company_id', sa.Integer(), nullable=True),
            sa.Column('resource', sa.String(length=32), nullable=True),
            sa.Column('resource_id', sa.BigInteger(), nullable=True),
            sa.Column('event_status', sa.String(length=32), nullable=True),
            sa.Column(
                'query',
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
            ),
            sa.Column(
                'headers',
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
            ),
            sa.Column(
                'payload',
                postgresql.JSONB(astext_type=sa.Text()),
                nullable=False,
            ),
        )

        op.create_index(
            'ix_whatsapp_events_dedupe_key',
            'whatsapp_events',
            ['dedupe_key'],
            unique=True,
        )
        op.create_index(
            'ix_whatsapp_events_received_at',
            'whatsapp_events',
            ['received_at'],
            unique=False,
        )
        op.create_index(
            'ix_whatsapp_events_status',
            'whatsapp_events',
            ['status'],
            unique=False,
        )
        op.create_index(
            'ix_whatsapp_events_company_id',
            'whatsapp_events',
            ['company_id'],
            unique=False,
        )
        op.create_index(
            'ix_whatsapp_events_resource',
            'whatsapp_events',
            ['resource'],
            unique=False,
        )
        op.create_index(
            'ix_whatsapp_events_resource_id',
            'whatsapp_events',
            ['resource_id'],
            unique=False,
        )
        op.create_index(
            'ix_whatsapp_events_event_status',
            'whatsapp_events',
            ['event_status'],
            unique=False,
        )

    # 2) Делаем dedupe по altegio_events реально уникальным
    #    (код API рассчитывает на IntegrityError)
    op.execute(
        'delete from altegio_events a using altegio_events b '
        'where a.dedupe_key = b.dedupe_key and a.id > b.id'
    )

    idx_name = 'ix_altegio_events_dedupe_key'
    idx = _get_index('altegio_events', idx_name)

    if idx is not None and not idx.get('unique', False):
        op.drop_index(idx_name, table_name='altegio_events')
        op.create_index(
            idx_name,
            'altegio_events',
            ['dedupe_key'],
            unique=True,
        )
    if idx is None:
        op.create_index(
            idx_name,
            'altegio_events',
            ['dedupe_key'],
            unique=True,
        )


def downgrade() -> None:
    # Откатываем уникальность dedupe_key обратно (как было)
    idx_name = 'ix_altegio_events_dedupe_key'
    idx = _get_index('altegio_events', idx_name)

    if idx is not None and idx.get('unique', False):
        op.drop_index(idx_name, table_name='altegio_events')
        op.create_index(
            idx_name,
            'altegio_events',
            ['dedupe_key'],
            unique=False,
        )

    if _table_exists('whatsapp_events'):
        op.drop_table('whatsapp_events')
