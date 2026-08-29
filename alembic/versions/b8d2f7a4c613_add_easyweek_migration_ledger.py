"""add easyweek_migration_ledger (PR-11.1)

Durable, source-scoped ledger for the one-off Altegio → EasyWeek cutover of
FUTURE ACTIVE bookings.

Почему таблица, а не файл отчёта: единственная защита от дубля при повторном
`apply` — это уникальное ограничение в базе. Отчёт можно потерять, перезаписать
или запустить инструмент с другой машины; строка в PostgreSQL переживает всё
перечисленное, и вторая попытка мигрировать ту же исходную запись падает на
`uq_easyweek_migration_ledger_source_identity` вместо того, чтобы создать второе
бронирование живому клиенту.

Почему ключ по ИСТОЧНИКУ, а не по цели: `target_booking_uuid` — это результат
попытки, и он неизвестен ровно у тех строк, ради которых ledger и существует
(`uncertain` после таймаута POST). Ключ по цели оставил бы их без ключа.

Почему в таблице нет PII: ledger читают операторы, он попадает в отчёты и живёт
дольше самой миграции. Телефон, имя и payload здесь не нужны ни для
идемпотентности, ни для reconciliation. `source_fingerprint` — это дайджест
расписания исходной записи (время, мастер, услуга, длительность, ключ клиента);
он не обратим в PII и служит только доказательством «источник не изменился».

CHECK `ck_easyweek_migration_ledger_created_has_target`: строка `created`
обязана назвать, что именно она создала. Иначе прерванный apply оставил бы
`created` без цели, и reconciliation отчитался бы о бронировании, которое никто
не может ни найти, ни откатить.

Существующие таблицы не изменяются: PR-11.1 ничего не добавляет к `clients`,
`records`, `message_jobs` и не трогает Altegio production path.

Revision ID: b8d2f7a4c613
Revises: a7c3e91b5d24
Create Date: 2026-08-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b8d2f7a4c613"
down_revision: str | Sequence[str] | None = "a7c3e91b5d24"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_TABLE = "easyweek_migration_ledger"


def upgrade() -> None:
    op.create_table(
        _TABLE,
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source_provider", sa.String(length=32), server_default=sa.text("'altegio'"), nullable=False),
        sa.Column("source_company_id", sa.Integer(), nullable=False),
        sa.Column("source_record_id", sa.BigInteger(), nullable=False),
        sa.Column("source_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("target_provider", sa.String(length=32), server_default=sa.text("'easyweek'"), nullable=False),
        sa.Column("target_booking_uuid", sa.String(length=64), nullable=True),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempts", sa.Integer(), server_default=sa.text("0"), nullable=False),
        sa.Column("reason_code", sa.String(length=64), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "source_provider",
            "source_company_id",
            "source_record_id",
            name="uq_easyweek_migration_ledger_source_identity",
        ),
        sa.CheckConstraint(
            "(status <> 'created') OR (target_booking_uuid IS NOT NULL)",
            name="ck_easyweek_migration_ledger_created_has_target",
        ),
        sa.CheckConstraint(
            "attempts >= 0",
            name="ck_easyweek_migration_ledger_attempts_non_negative",
        ),
    )
    op.create_index("ix_easyweek_migration_ledger_run", _TABLE, ["run_id"], unique=False)
    op.create_index("ix_easyweek_migration_ledger_status", _TABLE, ["status"], unique=False)


def downgrade() -> None:
    # Downgrade drops the migration bookkeeping only. It cannot and must not
    # touch the EasyWeek bookings a run already created — those live in another
    # system and are removed by the tool's own rollback path, under explicit
    # operator confirmation, never as a side effect of a schema downgrade.
    op.drop_index("ix_easyweek_migration_ledger_status", table_name=_TABLE)
    op.drop_index("ix_easyweek_migration_ledger_run", table_name=_TABLE)
    op.drop_table(_TABLE)
