"""add easyweek_events processing fields (PR-4)

Даёт research-grade таблице захвата минимальную runtime-аудируемость и
per-event retry scheduling, нужные easyweek_inbox_worker:

  * ``processed_at`` (TIMESTAMPTZ, nullable) — момент перехода в терминальный
    статус (processed/failed). NULL, пока строка ещё ``captured``.
  * ``error_code`` (VARCHAR(64), nullable) — стабильный безопасный код причины
    отказа. Сознательно НЕ сырой ``str(exception)``: текст исключения драйвера
    или БД содержит SQL-параметры, а с ними телефон, e-mail и имя клиента —
    такой текст в колонке был бы утечкой PII.
  * ``processing_attempts`` (INTEGER NOT NULL DEFAULT 0) — сколько раз строка
    падала с транзиентной ошибкой.
  * ``next_retry_at`` (TIMESTAMPTZ, nullable) — не раньше этого момента строку
    можно взять снова. NULL = готова немедленно.

Почему нужны последние две: без per-event расписания одна «отравленная» строка
навсегда блокирует backlog. Claim берёт СТАРЕЙШУЮ ``captured`` строку, поэтому
после отката транзакции воркер выбирал бы ту же самую строку снова и снова, и
глобальный backoff лишь замедлял бы этот цикл, но не пропускал бы вперёд
остальные события.

Индексы повторяют реальный claim-запрос:

  * ``ix_easyweek_events_claim`` — ``WHERE status = 'captured' AND
    (next_retry_at IS NULL OR next_retry_at <= now()) ORDER BY received_at, id``;
  * ``ix_easyweek_events_pending_booking`` — коррелированный ``NOT EXISTS``,
    который сериализует доставки ОДНОГО booking UUID: partial по двум
    нетерминальным статусам, ключ ``(payload ->> 'uid', received_at, id)``.

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
_CLAIM_INDEX = "ix_easyweek_events_claim"
# Supports the correlated NOT EXISTS that serialises one booking UUID.
_PENDING_BOOKING_INDEX = "ix_easyweek_events_pending_booking"


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
    op.add_column(
        _TABLE,
        sa.Column(
            "processing_attempts",
            sa.Integer(),
            nullable=False,
            server_default=sa.text("0"),
        ),
    )
    op.add_column(
        _TABLE,
        sa.Column("next_retry_at", sa.DateTime(timezone=True), nullable=True),
    )
    # Mirrors the claim query's filter and ordering.
    op.create_index(
        _CLAIM_INDEX,
        _TABLE,
        ["status", "next_retry_at", "received_at", "id"],
        unique=False,
    )
    # The claim also has to answer "is there an EARLIER non-terminal delivery
    # for this same booking?" — a correlated NOT EXISTS keyed on the booking
    # UUID extracted from the JSONB payload, ordered by capture order. Partial
    # on the two non-terminal statuses, because terminal rows never block.
    #
    # `payload ->> 'uid'` is NULL-safe for every JSONB shape (object without the
    # key, array, string, number), so a malformed capture row can neither error
    # the query nor act as a blocker.
    op.create_index(
        _PENDING_BOOKING_INDEX,
        _TABLE,
        [sa.text("(payload ->> 'uid')"), "received_at", "id"],
        unique=False,
        postgresql_where=sa.text("status IN ('captured', 'processing')"),
    )


def downgrade() -> None:
    """Downgrade schema — removes ONLY what this revision added."""
    op.drop_index(_PENDING_BOOKING_INDEX, table_name=_TABLE)
    op.drop_index(_CLAIM_INDEX, table_name=_TABLE)
    op.drop_column(_TABLE, "next_retry_at")
    op.drop_column(_TABLE, "processing_attempts")
    op.drop_column(_TABLE, "error_code")
    op.drop_column(_TABLE, "processed_at")
