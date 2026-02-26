"""add ops performance indexes

Revision ID: e1f2a3b4c5d6
Revises: f3a1c4e9b2d0
Create Date: 2026-02-26 20:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = 'e1f2a3b4c5d6'
down_revision: Union[str, Sequence[str], None] = 'f3a1c4e9b2d0'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # message_jobs: composite indexes for queue page
    op.execute(sa.text(
        'CREATE INDEX IF NOT EXISTS ix_message_jobs_status_run_at '
        'ON message_jobs (status, run_at)'
    ))
    op.execute(sa.text(
        'CREATE INDEX IF NOT EXISTS ix_message_jobs_company_type_status '
        'ON message_jobs (company_id, job_type, status)'
    ))
    op.execute(sa.text(
        'CREATE INDEX IF NOT EXISTS ix_message_jobs_status_locked_at '
        'ON message_jobs (status, locked_at)'
    ))

    # outbox_messages: time-range and lookup indexes
    op.execute(sa.text(
        'CREATE INDEX IF NOT EXISTS ix_outbox_messages_sent_at '
        'ON outbox_messages (sent_at)'
    ))
    op.execute(sa.text(
        'CREATE INDEX IF NOT EXISTS ix_outbox_messages_company_sent_at '
        'ON outbox_messages (company_id, sent_at)'
    ))
    op.execute(sa.text(
        'CREATE INDEX IF NOT EXISTS ix_outbox_messages_created_at '
        'ON outbox_messages (created_at)'
    ))
    op.execute(sa.text(
        'CREATE INDEX IF NOT EXISTS ix_outbox_messages_company_created_at '
        'ON outbox_messages (company_id, created_at)'
    ))

    # whatsapp_events: expression index for delivery status lookup by msg_id
    op.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS ix_whatsapp_events_msg_id "
        "ON whatsapp_events "
        "((payload #>> '{entry,0,changes,0,value,statuses,0,id}'))"
    ))

    # whatsapp_events: expression index for STOP/START command filtering
    op.execute(sa.text(
        "CREATE INDEX IF NOT EXISTS ix_whatsapp_events_cmd_body "
        "ON whatsapp_events "
        "((upper(trim(payload #>> '{entry,0,changes,0,value,messages,0,text,body}'))))"
    ))

    # clients: compound indexes for opt-out queries
    op.execute(sa.text(
        'CREATE INDEX IF NOT EXISTS ix_clients_wa_opted_out_at '
        'ON clients (wa_opted_out, wa_opted_out_at)'
    ))
    op.execute(sa.text(
        'CREATE INDEX IF NOT EXISTS ix_clients_company_phone '
        'ON clients (company_id, phone_e164)'
    ))


def downgrade() -> None:
    op.execute(sa.text('DROP INDEX IF EXISTS ix_message_jobs_status_run_at'))
    op.execute(sa.text('DROP INDEX IF EXISTS ix_message_jobs_company_type_status'))
    op.execute(sa.text('DROP INDEX IF EXISTS ix_message_jobs_status_locked_at'))
    op.execute(sa.text('DROP INDEX IF EXISTS ix_outbox_messages_sent_at'))
    op.execute(sa.text('DROP INDEX IF EXISTS ix_outbox_messages_company_sent_at'))
    op.execute(sa.text('DROP INDEX IF EXISTS ix_outbox_messages_created_at'))
    op.execute(sa.text('DROP INDEX IF EXISTS ix_outbox_messages_company_created_at'))
    op.execute(sa.text('DROP INDEX IF EXISTS ix_whatsapp_events_msg_id'))
    op.execute(sa.text('DROP INDEX IF EXISTS ix_whatsapp_events_cmd_body'))
    op.execute(sa.text('DROP INDEX IF EXISTS ix_clients_wa_opted_out_at'))
    op.execute(sa.text('DROP INDEX IF EXISTS ix_clients_company_phone'))
