"""add chatwoot reply context columns

Indexed mapping columns for native WhatsApp customer reply → Chatwoot native
reply rendering (PR1).

whatsapp_events:
  * chatwoot_message_id — Chatwoot Message.id linked to the event (for
    Meta-origin events: the message created in Chatwoot when the inbound
    text was forwarded there).
  * forwarded_chatwoot_conversation_id — DESTINATION conversation a
    Meta-origin inbound was forwarded to.  Deliberately separate from
    chatwoot_conversation_id, which stays a SOURCE-only marker for
    Chatwoot-origin webhook events, so forwarding never flips a Meta event
    into "chatwoot-origin".
  * whatsapp_message_id — Meta wamid from payload messages[0].id (audit /
    future native-reply mapping).  NOT the Chatwoot message id and NOT a
    replacement for dedupe_key.

outbox_messages:
  * chatwoot_conversation_id / chatwoot_message_id — promoted from
    meta->>'chatwoot_conversation_id' / meta->>'chatwoot_message_id' so an
    inbound reply's context.id (== provider_message_id) resolves its native
    Chatwoot reply target via an indexed lookup.

Additive and idempotent (IF NOT EXISTS) — safe for rolling deploy.  The
backfill is PostgreSQL-only (JSONB operators); non-PostgreSQL databases
(e.g. SQLite) build the schema from models and skip it.

Revision ID: c9d0e1f2a3b4
Revises: b7c8d9e0f1a2
Create Date: 2026-06-11 00:00:00.000000
"""

from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "c9d0e1f2a3b4"
down_revision: Union[str, Sequence[str], None] = "b7c8d9e0f1a2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── whatsapp_events ─────────────────────────────────────────────────
    op.execute(sa.text("ALTER TABLE whatsapp_events ADD COLUMN IF NOT EXISTS chatwoot_message_id BIGINT"))
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_whatsapp_events_chatwoot_message_id "
            "ON whatsapp_events (chatwoot_message_id)"
        )
    )
    op.execute(
        sa.text("ALTER TABLE whatsapp_events ADD COLUMN IF NOT EXISTS forwarded_chatwoot_conversation_id BIGINT")
    )
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_whatsapp_events_forwarded_chatwoot_conversation_id "
            "ON whatsapp_events (forwarded_chatwoot_conversation_id)"
        )
    )
    op.execute(sa.text("ALTER TABLE whatsapp_events ADD COLUMN IF NOT EXISTS whatsapp_message_id VARCHAR(128)"))
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_whatsapp_events_whatsapp_message_id "
            "ON whatsapp_events (whatsapp_message_id)"
        )
    )

    # ── outbox_messages ─────────────────────────────────────────────────
    op.execute(sa.text("ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS chatwoot_conversation_id BIGINT"))
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_outbox_messages_chatwoot_conversation_id "
            "ON outbox_messages (chatwoot_conversation_id)"
        )
    )
    op.execute(sa.text("ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS chatwoot_message_id BIGINT"))
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_outbox_messages_chatwoot_message_id "
            "ON outbox_messages (chatwoot_message_id)"
        )
    )

    # ── Backfill (PostgreSQL-only: JSONB operators) ─────────────────────
    bind = op.get_bind()
    if bind.dialect.name != "postgresql":
        return

    op.execute(
        sa.text(
            "UPDATE outbox_messages "
            "SET chatwoot_message_id = (meta->>'chatwoot_message_id')::bigint "
            "WHERE chatwoot_message_id IS NULL "
            "AND meta->>'chatwoot_message_id' ~ '^[0-9]+$'"
        )
    )
    op.execute(
        sa.text(
            "UPDATE outbox_messages "
            "SET chatwoot_conversation_id = (meta->>'chatwoot_conversation_id')::bigint "
            "WHERE chatwoot_conversation_id IS NULL "
            "AND meta->>'chatwoot_conversation_id' ~ '^[0-9]+$'"
        )
    )
    # Chatwoot-origin events carry a synthetic Chatwoot message id in the
    # same payload path, so they are excluded to keep the column wamid-only.
    op.execute(
        sa.text(
            "UPDATE whatsapp_events "
            "SET whatsapp_message_id = payload #>> '{entry,0,changes,0,value,messages,0,id}' "
            "WHERE whatsapp_message_id IS NULL "
            "AND COALESCE(payload #>> '{entry,0,changes,0,value,messages,0,id}', '') <> '' "
            "AND dedupe_key NOT LIKE 'chatwoot:%' "
            "AND dedupe_key NOT LIKE 'chatwoot_out:%'"
        )
    )


def downgrade() -> None:
    op.execute(sa.text("DROP INDEX IF EXISTS ix_outbox_messages_chatwoot_message_id"))
    op.execute(sa.text("ALTER TABLE outbox_messages DROP COLUMN IF EXISTS chatwoot_message_id"))
    op.execute(sa.text("DROP INDEX IF EXISTS ix_outbox_messages_chatwoot_conversation_id"))
    op.execute(sa.text("ALTER TABLE outbox_messages DROP COLUMN IF EXISTS chatwoot_conversation_id"))

    op.execute(sa.text("DROP INDEX IF EXISTS ix_whatsapp_events_whatsapp_message_id"))
    op.execute(sa.text("ALTER TABLE whatsapp_events DROP COLUMN IF EXISTS whatsapp_message_id"))
    op.execute(sa.text("DROP INDEX IF EXISTS ix_whatsapp_events_forwarded_chatwoot_conversation_id"))
    op.execute(sa.text("ALTER TABLE whatsapp_events DROP COLUMN IF EXISTS forwarded_chatwoot_conversation_id"))
    op.execute(sa.text("DROP INDEX IF EXISTS ix_whatsapp_events_chatwoot_message_id"))
    op.execute(sa.text("ALTER TABLE whatsapp_events DROP COLUMN IF EXISTS chatwoot_message_id"))
