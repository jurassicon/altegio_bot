"""add operator relay durable outbox columns

Даёт operator-relay lifecycle durable send intent ДО первого обращения к Meta:

  * ``source_whatsapp_event_id`` (BIGINT, nullable, FK → whatsapp_events.id
    ON DELETE SET NULL) — однозначная связь Outbox с исходным ``WhatsAppEvent``;
  * partial unique index ``uq_outbox_source_whatsapp_event_id`` по этой колонке
    ``WHERE source_whatsapp_event_id IS NOT NULL`` — DB-level idempotency: один
    event не может породить два operator-relay attempt даже при конкуренции или
    crash-replay. Partial, поэтому историч./bot-строки с ``NULL`` не ограничены;
  * ``attempt_started_at`` (TIMESTAMPTZ, nullable) — момент commit перехода
    ``queued → sending``; по нему stale-recovery отличает зависшую попытку.

Строго аддитивная и идемпотентная:
  * старые строки не трогает и не получает фиктивных source event id (остаются
    ``NULL``);
  * ``IF NOT EXISTS`` делает повторный upgrade no-op;
  * чужих таблиц не касается.

Revision ID: 9a1f4c7b2e3d
Revises: 8705ec49cc73
Create Date: 2026-07-28 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "9a1f4c7b2e3d"
down_revision: Union[str, Sequence[str], None] = "8705ec49cc73"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema (additive, idempotent)."""
    op.execute(sa.text("ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS source_whatsapp_event_id BIGINT"))
    op.execute(sa.text("ALTER TABLE outbox_messages ADD COLUMN IF NOT EXISTS attempt_started_at TIMESTAMPTZ"))

    # FK is best-effort: guarded so a re-run (or a DB where it already exists)
    # stays a no-op. SET NULL keeps the audit row if an event is ever pruned.
    op.execute(
        sa.text(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'fk_outbox_source_whatsapp_event_id'
                ) THEN
                    ALTER TABLE outbox_messages
                        ADD CONSTRAINT fk_outbox_source_whatsapp_event_id
                        FOREIGN KEY (source_whatsapp_event_id)
                        REFERENCES whatsapp_events (id)
                        ON DELETE SET NULL;
                END IF;
            END $$;
            """
        )
    )

    # Partial unique index — the DB-level idempotency guarantee for operator
    # relay. Only non-null values are constrained, so the historical/bot rows
    # (all NULL) remain unconstrained.
    op.execute(
        sa.text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_outbox_source_whatsapp_event_id "
            "ON outbox_messages (source_whatsapp_event_id) "
            "WHERE source_whatsapp_event_id IS NOT NULL"
        )
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(sa.text("DROP INDEX IF EXISTS uq_outbox_source_whatsapp_event_id"))
    op.execute(sa.text("ALTER TABLE outbox_messages DROP CONSTRAINT IF EXISTS fk_outbox_source_whatsapp_event_id"))
    op.execute(sa.text("ALTER TABLE outbox_messages DROP COLUMN IF EXISTS attempt_started_at"))
    op.execute(sa.text("ALTER TABLE outbox_messages DROP COLUMN IF EXISTS source_whatsapp_event_id"))
