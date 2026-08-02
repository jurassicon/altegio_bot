"""add easyweek_events processing fields (PR-4)

Даёт research-grade таблице захвата минимальную runtime-аудируемость, нужную
easyweek_inbox_worker:

  * ``processed_at`` (TIMESTAMPTZ, nullable) — момент перехода в терминальный
    статус (processed/failed). NULL, пока строка ещё ``captured``.
  * ``error_code`` (VARCHAR(64), nullable) — стабильный безопасный код причины
    отказа. Сознательно НЕ сырой ``str(exception)``: текст исключения драйвера
    или БД содержит SQL-параметры, а с ними телефон, e-mail и имя клиента —
    такой текст в колонке был бы утечкой PII.
  * индекс ``(status, received_at)`` — воркер забирает строго
    ``status='captured'`` в порядке поступления.

Обычные Alembic-операции, БЕЗ ``IF NOT EXISTS`` / ``IF EXISTS``. Это осознанно:
защитные варианты принимают объект с правильным ИМЕНЕМ, но неправильным типом
или определением, и всё равно записывают ревизию как применённую — молчаливый
schema drift, который потом проявится как ошибка рантайма. Alembic и так не
выполняет уже применённую ревизию повторно, поэтому идемпотентность на уровне
DDL здесь не нужна, а fail-closed при дрейфе — нужен.

Строго аддитивная: сырые колонки захвата (``body_raw``, ``body_text``,
``payload``) не затрагиваются, чужих таблиц не касается, downgrade снимает
ТОЛЬКО объекты этой ревизии.

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

_TABLE = "easyweek_events"
_INDEX = "ix_easyweek_events_status_received_at"


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        _TABLE,
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        _TABLE,
        sa.Column("error_code", sa.String(length=64), nullable=True),
    )
    op.create_index(_INDEX, _TABLE, ["status", "received_at"], unique=False)


def downgrade() -> None:
    """Downgrade schema — removes ONLY what this revision added."""
    op.drop_index(_INDEX, table_name=_TABLE)
    op.drop_column(_TABLE, "error_code")
    op.drop_column(_TABLE, "processed_at")
