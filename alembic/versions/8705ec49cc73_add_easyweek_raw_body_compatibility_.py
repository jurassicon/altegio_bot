"""add easyweek raw body compatibility columns

Дописывает в ``easyweek_events`` две колонки research-grade capture:
  * ``body_raw`` (BYTEA) — исходные байты доставки (до 128 КиБ), источник истины
    по содержимому; ``payload`` (JSONB) — лишь их разбор;
  * ``body_size_bytes`` (BIGINT) — полный размер доставки, включая не влезший в
    лимит хвост.

Почему ОТДЕЛЬНАЯ ревизия, а не правка 8923be993170: сначала эти колонки дописали
прямо в CREATE TABLE базовой ревизии. Но среда, где ранняя версия 8923be993170
уже была применена, считает её выполненной и НЕ перезапустит — там таблица
осталась бы без колонок, а каждый вебхук падал бы на INSERT. Аддитивная ревизия
чинит обе схемы сразу и идемпотентна:
  * свежая БД (базовая 8923be993170 без колонок) → колонки добавляются;
  * БД, где применили раннюю 8923be993170 без колонок → колонки добавляются;
  * БД, где успели применить ОТредактированную 8923be993170 с колонками →
    ``IF NOT EXISTS`` делает upgrade no-op.

Строго аддитивная: чужих таблиц не касается.

Revision ID: 8705ec49cc73
Revises: 8923be993170
Create Date: 2026-07-24 07:08:14.686605

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8705ec49cc73"
down_revision: Union[str, Sequence[str], None] = "8923be993170"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(sa.text("ALTER TABLE easyweek_events ADD COLUMN IF NOT EXISTS body_raw BYTEA"))
    op.execute(
        sa.text("ALTER TABLE easyweek_events ADD COLUMN IF NOT EXISTS body_size_bytes BIGINT NOT NULL DEFAULT 0")
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(sa.text("ALTER TABLE easyweek_events DROP COLUMN IF EXISTS body_size_bytes"))
    op.execute(sa.text("ALTER TABLE easyweek_events DROP COLUMN IF EXISTS body_raw"))
