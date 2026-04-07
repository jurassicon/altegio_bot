"""campaign mvp: extend campaign_runs and campaign_recipients

Добавляет новые колонки к существующим таблицам campaign_runs и
campaign_recipients. Старые колонки сохранены для обратной
совместимости.

Revision ID: e4f5a6b7c8d9
Revises: b2c3d4e5f6a7
Create Date: 2026-04-06 00:00:00.000000

"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = 'e4f5a6b7c8d9'
down_revision: Union[str, Sequence[str], None] = 'b2c3d4e5f6a7'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ------------------------------------------------------------------
    # campaign_runs — новые колонки
    # ------------------------------------------------------------------
    op.add_column(
        'campaign_runs',
        sa.Column(
            'source_preview_run_id',
            sa.BigInteger(),
            sa.ForeignKey('campaign_runs.id', ondelete='SET NULL'),
            nullable=True,
        ),
    )
    op.create_index(
        'ix_campaign_runs_source_preview_run_id',
        'campaign_runs',
        ['source_preview_run_id'],
        unique=False,
    )

    op.add_column(
        'campaign_runs',
        sa.Column('location_id', sa.Integer(), nullable=True),
    )
    op.add_column(
        'campaign_runs',
        sa.Column('card_type_id', sa.String(64), nullable=True),
    )

    op.add_column(
        'campaign_runs',
        sa.Column(
            'attribution_window_days',
            sa.Integer(),
            server_default='30',
            nullable=False,
        ),
    )

    # Follow-up
    op.add_column(
        'campaign_runs',
        sa.Column(
            'followup_enabled',
            sa.Boolean(),
            server_default='false',
            nullable=False,
        ),
    )
    op.add_column(
        'campaign_runs',
        sa.Column('followup_delay_days', sa.Integer(), nullable=True),
    )
    op.add_column(
        'campaign_runs',
        sa.Column('followup_policy', sa.String(32), nullable=True),
    )
    op.add_column(
        'campaign_runs',
        sa.Column('followup_template_name', sa.String(128), nullable=True),
    )

    # Новые счётчики исключений
    for col in (
        'excluded_multiple_records',
        'excluded_no_confirmed_record',
        'excluded_has_records_before',
        'excluded_invalid_phone',
        'excluded_no_whatsapp',
        'queued_count',
        'provider_accepted_count',
        'delivered_count',
        'read_count',
        'replied_count',
        'booked_after_count',
        'opted_out_after_count',
        'cleanup_failed_count',
        'cards_deleted_count',
        'cards_issued_count',
    ):
        op.add_column(
            'campaign_runs',
            sa.Column(
                col, sa.Integer(), server_default='0', nullable=False
            ),
        )

    # ------------------------------------------------------------------
    # campaign_recipients — новые колонки
    # ------------------------------------------------------------------
    op.add_column(
        'campaign_recipients',
        sa.Column(
            'confirmed_records_in_period',
            sa.Integer(),
            server_default='0',
            nullable=False,
        ),
    )
    op.add_column(
        'campaign_recipients',
        sa.Column(
            'records_before_period',
            sa.Integer(),
            server_default='0',
            nullable=False,
        ),
    )

    # Tracking отправки
    op.add_column(
        'campaign_recipients',
        sa.Column(
            'message_job_id',
            sa.BigInteger(),
            sa.ForeignKey('message_jobs.id', ondelete='SET NULL'),
            nullable=True,
        ),
    )
    op.create_index(
        'ix_campaign_recipients_message_job_id',
        'campaign_recipients',
        ['message_job_id'],
        unique=False,
    )

    op.add_column(
        'campaign_recipients',
        sa.Column(
            'outbox_message_id',
            sa.BigInteger(),
            sa.ForeignKey('outbox_messages.id', ondelete='SET NULL'),
            nullable=True,
        ),
    )
    op.create_index(
        'ix_campaign_recipients_outbox_message_id',
        'campaign_recipients',
        ['outbox_message_id'],
        unique=False,
    )

    op.add_column(
        'campaign_recipients',
        sa.Column('loyalty_card_type_id', sa.String(64), nullable=True),
    )
    op.add_column(
        'campaign_recipients',
        sa.Column(
            'cleanup_card_ids',
            postgresql.JSONB(astext_type=sa.Text()),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
    )
    op.add_column(
        'campaign_recipients',
        sa.Column(
            'cleanup_failed_reason', sa.String(256), nullable=True
        ),
    )

    # Attribution timestamps
    for col in (
        'sent_at',
        'read_at',
        'replied_at',
        'booked_after_at',
        'opted_out_after_at',
    ):
        op.add_column(
            'campaign_recipients',
            sa.Column(col, sa.DateTime(timezone=True), nullable=True),
        )

    # Follow-up
    op.add_column(
        'campaign_recipients',
        sa.Column('followup_status', sa.String(32), nullable=True),
    )
    op.add_column(
        'campaign_recipients',
        sa.Column(
            'followup_message_job_id',
            sa.BigInteger(),
            sa.ForeignKey('message_jobs.id', ondelete='SET NULL'),
            nullable=True,
        ),
    )
    op.add_column(
        'campaign_recipients',
        sa.Column(
            'followup_outbox_id',
            sa.BigInteger(),
            sa.ForeignKey('outbox_messages.id', ondelete='SET NULL'),
            nullable=True,
        ),
    )
    op.add_column(
        'campaign_recipients',
        sa.Column(
            'followup_sent_at',
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )


def downgrade() -> None:
    # campaign_recipients
    for col in (
        'followup_sent_at',
        'followup_outbox_id',
        'followup_message_job_id',
        'followup_status',
        'opted_out_after_at',
        'booked_after_at',
        'replied_at',
        'read_at',
        'sent_at',
        'cleanup_failed_reason',
        'cleanup_card_ids',
        'loyalty_card_type_id',
        'records_before_period',
        'confirmed_records_in_period',
    ):
        op.drop_column('campaign_recipients', col)

    op.drop_index(
        'ix_campaign_recipients_outbox_message_id',
        table_name='campaign_recipients',
    )
    op.drop_column('campaign_recipients', 'outbox_message_id')
    op.drop_index(
        'ix_campaign_recipients_message_job_id',
        table_name='campaign_recipients',
    )
    op.drop_column('campaign_recipients', 'message_job_id')

    # campaign_runs
    for col in (
        'cards_issued_count',
        'cards_deleted_count',
        'cleanup_failed_count',
        'opted_out_after_count',
        'booked_after_count',
        'replied_count',
        'read_count',
        'delivered_count',
        'provider_accepted_count',
        'queued_count',
        'excluded_no_whatsapp',
        'excluded_invalid_phone',
        'excluded_has_records_before',
        'excluded_no_confirmed_record',
        'excluded_multiple_records',
        'followup_template_name',
        'followup_policy',
        'followup_delay_days',
        'followup_enabled',
        'attribution_window_days',
        'card_type_id',
        'location_id',
    ):
        op.drop_column('campaign_runs', col)

    op.drop_index(
        'ix_campaign_runs_source_preview_run_id',
        table_name='campaign_runs',
    )
    op.drop_column('campaign_runs', 'source_preview_run_id')
