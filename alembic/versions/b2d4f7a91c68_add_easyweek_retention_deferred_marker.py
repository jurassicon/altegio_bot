"""add EasyWeek retention deferred marker to easyweek_events (PR-12)

Одна колонка в ``easyweek_events``:

  * ``retention_deferred_at`` (TIMESTAMPTZ, nullable) — визит доказан и счётчик
    записан, но ``repeat_10d`` рассмотреть не удалось: мастер-фенс уведомлений
    был закрыт. Значение — момент, **начиная с которого** обязательство можно
    рассматривать: проход, не сумевший принять решение, сдвигает его вперёд,
    чтобы неразрешимая строка не занимала слот ограниченного batch на каждом
    проходе.

Почему отдельная колонка, а не переиспользование ``review_deferred_at``.

PR-12 в первой редакции хранил оба обязательства — review и repeat — в одной
отметке PR-11. Экономия оказалась дефектом: отметка говорила «эта доставка
что-то должна», но не говорила ЧТО. Восстановление вынуждено было спрашивать
ТЕКУЩИЕ флаги вместо того, что было заработано в момент события, и это даёт два
противоположных отказа сразу:

  * функция, выключенная в момент события, могла получить сообщение задним
    числом, когда оператор включал её позже — исторически не заработанная
    рассылка реальному человеку;
  * действительно заработанное обязательство терялось, как только чужой
    планировщик первым очищал общую отметку.

Тип обязательства обязан быть durable: только durable переживает изменение
флагов между событием и восстановлением. Каждая отметка теперь ставится лишь
своим планировщиком, снимается или сдвигается лишь своим проходом, и
recoverable-ошибка одного обязательства не уничтожает второе — у проходов
разные транзакции.

Миграция аддитивная и обратимая. Существующие ``review_deferred_at`` НЕ
трогаются и сохраняют прежнее review-значение: новая колонка добавляется как
NULL везде. Backfill намеренно отсутствует — исторические строки не доказывают,
что PR-12 был включён в момент их обработки, а придумать это доказательство
нельзя. Единственный корректный результат для них: обязательства нет.

Partial-индекс покрывает ровно предикат скана восстановления
(``WHERE retention_deferred_at IS NOT NULL``) и на подавляющем большинстве
строк, где отметки нет, места не занимает — тот же контракт, что у
``ix_easyweek_events_review_deferred``.

Revision ID: b2d4f7a91c68
Revises: e7b91d3a5c02
Create Date: 2026-09-02
"""

import sqlalchemy as sa
from alembic import op

revision = "b2d4f7a91c68"
down_revision = "e7b91d3a5c02"
branch_labels = None
depends_on = None

_RETENTION_DEFERRED_AT = "retention_deferred_at"
_RETENTION_DEFERRED_INDEX = "ix_easyweek_events_retention_deferred"


def upgrade() -> None:
    op.add_column(
        "easyweek_events",
        sa.Column(_RETENTION_DEFERRED_AT, sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        _RETENTION_DEFERRED_INDEX,
        "easyweek_events",
        [_RETENTION_DEFERRED_AT],
        unique=False,
        postgresql_where=sa.text(f"{_RETENTION_DEFERRED_AT} IS NOT NULL"),
    )


def downgrade() -> None:
    # Дропает только то, что создал upgrade(): review-отметка PR-11 и её индекс
    # этой миграции не принадлежат и остаются нетронутыми.
    op.drop_index(_RETENTION_DEFERRED_INDEX, table_name="easyweek_events")
    op.drop_column("easyweek_events", _RETENTION_DEFERRED_AT)
