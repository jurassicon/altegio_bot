"""add easyweek_events processing fields (PR-4)

Даёт research-grade таблице захвата минимальную runtime-аудируемость, нужную
easyweek_inbox_worker:

  * ``processed_at`` (TIMESTAMPTZ, nullable) — момент перехода в терминальный
    статус (processed/failed). NULL, пока строка ещё ``captured``.
  * ``error_code`` (VARCHAR(64), nullable) — стабильный безопасный код причины
    отказа. Сознательно НЕ сырой ``str(exception)``: текст исключения драйвера
    или БД содержит SQL-параметры, а с ними телефон, e-mail и имя клиента —
    такой текст в колонке был бы утечкой PII.

Строго аддитивная и идемпотентная:
  * ``IF NOT EXISTS`` делает повторный upgrade no-op;
  * сырые колонки захвата (``body_raw``, ``body_text``, ``payload``) не
    затрагиваются — источник истины остаётся прежним;
  * чужих таблиц не касается;
  * downgrade снимает ТОЛЬКО две колонки этой ревизии.

Revision ID: d4e8a1c39f57
Revises: c1a7d3f905b2
Create Date: 2026-08-02 20:10:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d4e8a1c39f57"
down_revision: Union[str, Sequence[str], None] = "c1a7d3f905b2"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema (additive, idempotent)."""
    op.execute(sa.text("ALTER TABLE easyweek_events ADD COLUMN IF NOT EXISTS processed_at TIMESTAMPTZ"))
    op.execute(sa.text("ALTER TABLE easyweek_events ADD COLUMN IF NOT EXISTS error_code VARCHAR(64)"))

    # The worker claims strictly `status='captured'` oldest-first; this index
    # keeps that claim cheap once a real backlog exists.
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_easyweek_events_status_received_at "
            "ON easyweek_events (status, received_at)"
        )
    )


def downgrade() -> None:
    """Downgrade schema — removes ONLY what this revision added."""
    op.execute(sa.text("DROP INDEX IF EXISTS ix_easyweek_events_status_received_at"))
    op.execute(sa.text("ALTER TABLE easyweek_events DROP COLUMN IF EXISTS error_code"))
    op.execute(sa.text("ALTER TABLE easyweek_events DROP COLUMN IF EXISTS processed_at"))
