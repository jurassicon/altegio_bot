"""add the rollback attempt marker to easyweek_migration_ledger (PR-11.2, §30.12)

Две nullable-колонки и один CHECK.

Зачем. Отмена бронирования может закончиться тремя способами, и только один из
них читается из EasyWeek: `PUT /bookings/{uuid}/status/cancel` мог отработать, а
ответ или контрольный GET — не дойти. Тогда запись в EasyWeek уже отменена, а
инструмент об этом не знает.

Без durable-отметки следующий запуск не может отличить два состояния, которые
выглядят одинаково — «booking отменён нашим PUT, результат которого мы не
увидели» и «booking отменил человек вручную». Первое можно безопасно завершить
как `rolled_back` без второго PUT; второе — чужое изменение, и приписывать его
себе нельзя. Отметка ставится и коммитится ДО возможного PUT, поэтому падение
между отметкой и запросом оставляет ровно то, что произошло: «cancel мог быть
отправлен», и следующий запуск разбирает это чтением, а не повторной мутацией.

Существующие строки получают NULL: до этой ревизии инструмент отметок не ставил,
и объявить прошлый rollback «нашей попыткой» задним числом было бы выдумкой. Для
них поведение не меняется — отменённый вручную target остаётся modified/unproven.

Обе колонки — одна и та же единица данных, поэтому под общим CHECK: половина
отметки означала бы либо попытку без запуска, либо запуск без попытки.

Revision ID: c4b7e2f1a983
Revises: a7d1f4c82b95
Create Date: 2026-09-05
"""

import sqlalchemy as sa

from alembic import op

revision = "c4b7e2f1a983"
down_revision = "a7d1f4c82b95"
branch_labels = None
depends_on = None

_LEDGER = "easyweek_migration_ledger"
_AT = "rollback_attempted_at"
_RUN = "rollback_attempt_run_id"
_CHECK = "ck_easyweek_migration_ledger_rollback_attempt_complete"


def _has_column(bind: sa.engine.Connection, table: str, column: str) -> bool:
    return column in {row["name"] for row in sa.inspect(bind).get_columns(table)}


def upgrade() -> None:
    bind = op.get_bind()
    # Idempotent on purpose: this branch has been deployed to environments that
    # applied revisions by hand, and re-adding a column would abort the whole
    # upgrade rather than the one statement.
    if not _has_column(bind, _LEDGER, _AT):
        op.add_column(_LEDGER, sa.Column(_AT, sa.DateTime(timezone=True), nullable=True))
    if not _has_column(bind, _LEDGER, _RUN):
        op.add_column(_LEDGER, sa.Column(_RUN, sa.String(length=64), nullable=True))

    existing = {check["name"] for check in sa.inspect(bind).get_check_constraints(_LEDGER)}
    if _CHECK not in existing:
        op.create_check_constraint(
            _CHECK,
            _LEDGER,
            f"({_AT} IS NULL) = ({_RUN} IS NULL)",
        )


def downgrade() -> None:
    bind = op.get_bind()
    existing = {check["name"] for check in sa.inspect(bind).get_check_constraints(_LEDGER)}
    if _CHECK in existing:
        op.drop_constraint(_CHECK, _LEDGER, type_="check")
    if _has_column(bind, _LEDGER, _RUN):
        op.drop_column(_LEDGER, _RUN)
    if _has_column(bind, _LEDGER, _AT):
        op.drop_column(_LEDGER, _AT)
