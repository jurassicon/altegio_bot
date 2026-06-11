"""add reply context lookup composite index

Follow-up to c9d0e1f2a3b4.  Adds the composite index backing the hot
reply-target lookup in whatsapp_inbox_worker._get_reply_context_target, which
filters by (provider_message_id, phone_e164, message_source='operator') and
orders by (created_at DESC, id DESC).

Kept as a SEPARATE migration rather than amending c9d0e1f2a3b4: if that
revision was already applied in any environment, an in-place CREATE INDEX would
never run there.  As its own revision this index is guaranteed to be applied
everywhere on upgrade head.

Partial on provider_message_id so it only covers rows that can ever be a reply
target.  The single-column indexes from c9d0e1f2a3b4 are left untouched.

Additive and idempotent (IF NOT EXISTS) — safe for rolling deploy.

Revision ID: d0e1f2a3b4c5
Revises: c9d0e1f2a3b4
Create Date: 2026-06-11 12:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "d0e1f2a3b4c5"
down_revision: Union[str, Sequence[str], None] = "c9d0e1f2a3b4"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_outbox_messages_reply_context_lookup "
            "ON outbox_messages ("
            "provider_message_id, phone_e164, message_source, created_at DESC, id DESC"
            ") WHERE provider_message_id IS NOT NULL"
        )
    )


def downgrade() -> None:
    op.execute(sa.text("DROP INDEX IF EXISTS ix_outbox_messages_reply_context_lookup"))
