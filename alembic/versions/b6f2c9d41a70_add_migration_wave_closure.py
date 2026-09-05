"""add easyweek_migration_wave_closure (PR-11.2, §30)

Одна таблица: durable факт «напоминания этой волны теперь принадлежат EasyWeek».

Зачем. Волна — это (source provider, source company, origin run). После
supervised reminder handover в неё нельзя добавлять бронирования: handover
доказал и пометил всё, что видел, а запись, созданная позже, осталась бы с
отозванными Altegio-напоминаниями и без EasyWeek-напоминаний.

Раньше признаком закрытия служил per-row marker `reminders_handed_over_at`. Он
существует только там, где есть `status=created` строка, поэтому пара
company/run, которую snapshot называл, но в которой созданных строк не было
(пустая пара или только `failed`), не могла его нести. После освобождения
advisory lock проверка закрытия отвечала «нет», и поздний retry миграции с этим
run_id создавал бронирование в уже закрытой волне. Advisory lock сериализует
транзакции; пережить их может только закоммиченная строка.

Строка пишется в той же транзакции, что и создание EasyWeek-напоминаний, отмена
Altegio-напоминаний и row markers, поэтому откат не оставляет закрытия, а commit
закрывает каждую заявленную пару, включая пустые.

Уникальный ключ (provider, company, run): повтор того же handover находит свою
строку и ничего не меняет, а другой plan digest — конфликт, а не обновление.

Существующие волны, закрытые до этой ревизии, продолжают распознаваться по
row-level marker: проверка закрытия принимает и строку closure, и помеченную
ledger-строку.

PII-free: идентификаторы, run id, digest и метка времени.

Revision ID: b6f2c9d41a70
Revises: c4b7e2f1a983
Create Date: 2026-09-05
"""

import sqlalchemy as sa

from alembic import op

revision = "b6f2c9d41a70"
down_revision = "c4b7e2f1a983"
branch_labels = None
depends_on = None

_TABLE = "easyweek_migration_wave_closure"
_UNIQUE = "uq_easyweek_migration_wave_closure_identity"


def _has_table(bind: sa.engine.Connection, table: str) -> bool:
    return sa.inspect(bind).has_table(table)


def upgrade() -> None:
    bind = op.get_bind()
    # Idempotent on purpose: this branch has been applied by hand in test
    # environments, and re-creating the table would abort the whole upgrade
    # rather than the one statement.
    if _has_table(bind, _TABLE):
        return
    op.create_table(
        _TABLE,
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source_provider", sa.String(length=32), server_default=sa.text("'altegio'"), nullable=False),
        sa.Column("source_company_id", sa.Integer(), nullable=False),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("plan_digest", sa.String(length=64), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source_provider", "source_company_id", "run_id", name=_UNIQUE),
    )


def downgrade() -> None:
    bind = op.get_bind()
    if _has_table(bind, _TABLE):
        op.drop_table(_TABLE)
