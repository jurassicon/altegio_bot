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
    нетерминальным статусам, ключ ``(booking_uuid, received_at, id)``.

Плюс сам ключ причинного порядка:

  * ``booking_uuid`` (UUID, nullable) — канонический booking UUID доставки.
    Заполняется на capture и backfill'ится для уже захваченного backlog тем же
    парсером, что использует нормализатор. Сырой текст ``payload ->> 'uid'``
    ключом быть не может: одна и та же UUID в lowercase, uppercase, в скобках,
    без дефисов или с пробелами — разные строки, но одна booking, и claim не
    увидел бы раннюю доставку предшественником поздней. Malformed/отсутствующий
    ``uid`` оставляет NULL: такая строка никого не блокирует, не блокируется
    сама и доходит до детерминированного отказа.

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

import uuid
from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

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
# Keyset batch size. Bounds BOTH the rows read per round trip and the parameter
# list of one UPDATE, so peak memory is one batch regardless of backlog size.
_BACKFILL_BATCH = 500


def _backfill_booking_uuid() -> None:
    """Fill ``booking_uuid`` for the already-captured backlog.

    Done in Python with the SAME parser the capture endpoint and the normalizer
    use, so a backfilled row and a freshly captured one can never disagree about
    a booking's identity. A SQL ``(payload ->> 'uid')::uuid`` was rejected on
    purpose: PostgreSQL raises on the first malformed value, which would abort
    the whole migration because of one bad research-capture row.

    Only rows that actually carry a string ``uid`` are examined, and only rows
    whose value parses are written. Malformed and missing ids stay NULL. Status,
    payload, payload_hash and body_raw are never touched.

    Bounded memory by construction: the backlog is walked with keyset pagination
    on ``id`` (``id > :last_id ORDER BY id LIMIT n``), never loaded whole. The
    cursor advances on every row it reads — including malformed ones — so one bad
    value can neither stall nor loop the scan.
    """
    bind = op.get_bind()
    if op.get_context().as_sql:
        # Offline SQL generation cannot read rows. Say so instead of emitting a
        # statement that looks like a completed backfill.
        op.execute(
            sa.text(
                "-- WARNING: booking_uuid backfill SKIPPED: offline SQL generation "
                "cannot read existing rows. Run this migration online."
            )
        )
        return

    # Keyset pagination on the primary key: `id > :last_id ORDER BY id LIMIT n`.
    # No OFFSET (which re-scans the prefix on every page) and no `.all()` over
    # the whole table — a research-grade capture backlog can be large, and at no
    # point may more than one batch be resident.
    select_batch = sa.text(
        f"SELECT id, payload ->> 'uid' AS raw_uid FROM {_TABLE} "
        "WHERE id > :last_id AND jsonb_typeof(payload -> 'uid') = 'string' "
        f"ORDER BY id LIMIT {_BACKFILL_BATCH}"
    )
    update_batch = sa.text(f"UPDATE {_TABLE} SET booking_uuid = CAST(:value AS uuid) WHERE id = :row_id")

    last_id = 0
    while True:
        rows = bind.execute(select_batch, {"last_id": last_id}).all()
        if not rows:
            break

        updates = []
        for row in rows:
            # The cursor advances on EVERY row, valid or not. Advancing only on
            # success would re-read a malformed row forever.
            last_id = row.id
            raw = row.raw_uid
            if not isinstance(raw, str):
                continue
            try:
                updates.append({"row_id": row.id, "value": str(uuid.UUID(raw.strip()))})
            except (ValueError, AttributeError, TypeError):
                # Malformed research-capture row: leave NULL, keep the raw
                # capture, and keep going.
                continue

        if updates:
            bind.execute(update_batch, updates)


def upgrade() -> None:
    """Upgrade schema."""
    op.add_column(
        _TABLE,
        sa.Column("booking_uuid", postgresql.UUID(as_uuid=True), nullable=True),
    )
    _backfill_booking_uuid()
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
    # for this same booking?" — a correlated NOT EXISTS keyed on the CANONICAL
    # booking UUID, ordered by capture order. Partial on the two non-terminal
    # statuses, because terminal rows never block.
    #
    # Keyed on the column, NOT on `payload ->> 'uid'`: the raw text differs
    # between lowercase, uppercase, braced, dash-less and whitespace-padded
    # spellings of ONE booking, so a raw-text key would let a later delivery
    # overtake an earlier one that is still retrying. Rows whose id is malformed
    # or missing hold NULL and are simply absent from this index, so they neither
    # block nor group with anything.
    op.create_index(
        _PENDING_BOOKING_INDEX,
        _TABLE,
        ["booking_uuid", "received_at", "id"],
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
    op.drop_column(_TABLE, "booking_uuid")
