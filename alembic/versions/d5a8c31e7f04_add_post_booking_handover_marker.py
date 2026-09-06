"""add the post-booking marketing ownership marker (PR-12.1, §31.6)

Две nullable-колонки, один CHECK и partial index.

Зачем. После ledger-подтверждённого переноса booking и завершённого reminder
handover §30 у записи не должно оставаться открытых source Altegio `review_3d`,
`repeat_10d` и `comeback_3d`, а поздний Altegio webhook не должен их создать или
восстановить: `add_job` при конфликте возвращает `canceled`/`failed` job в
`queued`.

Почему это не `reminders_handed_over_at`. §30 доказывает, что созданы timed
EasyWeek reminders. PR-12.1 доказывает обратное по смыслу: source marketing
ownership отдан, а target obligation НЕ создан — мигрированная будущая запись не
доказывает состоявшийся визит, и право создать EasyWeek `review_3d`/`repeat_10d`
даёт только доказанный `booking-succeeded`, а `comeback_3d` — только доказанная
EasyWeek cancellation. Совмещение колонок позволило бы одному доказательству
подменять другое.

Marker ставится всем eligible строкам волны, включая те, у которых сейчас нет ни
одного такого job: иначе поздний Altegio event создал бы ПЕРВОЕ обязательство
уже после handover, и runtime fences не нашли бы, что защищать.

Обе колонки — одна единица данных, поэтому под общим CHECK. Partial index — под
runtime lookup: planner читает его в своей транзакции на каждой доставке
create/update/delete, outbox — непосредственно перед отправкой.

Существующие строки получают NULL: до этой ревизии инструмент отметок не ставил,
и объявить прошлый перенос «отданным» задним числом было бы выдумкой.

Revision ID: d5a8c31e7f04
Revises: b6f2c9d41a70
Create Date: 2026-09-05
"""

import sqlalchemy as sa

from alembic import op

revision = "d5a8c31e7f04"
down_revision = "b6f2c9d41a70"
branch_labels = None
depends_on = None

_LEDGER = "easyweek_migration_ledger"
_AT = "post_booking_jobs_handed_over_at"
_DIGEST = "post_booking_handover_plan_digest"
_CHECK = "ck_easyweek_migration_ledger_post_booking_handover_complete"
_INDEX = "ix_easyweek_migration_ledger_post_booking_handover"


def _has_column(bind: sa.engine.Connection, table: str, column: str) -> bool:
    return column in {row["name"] for row in sa.inspect(bind).get_columns(table)}


def upgrade() -> None:
    bind = op.get_bind()
    # Idempotent on purpose: this branch has been applied by hand in test
    # environments, and re-adding a column would abort the whole upgrade rather
    # than the one statement.
    if not _has_column(bind, _LEDGER, _AT):
        op.add_column(_LEDGER, sa.Column(_AT, sa.DateTime(timezone=True), nullable=True))
    if not _has_column(bind, _LEDGER, _DIGEST):
        op.add_column(_LEDGER, sa.Column(_DIGEST, sa.String(length=64), nullable=True))

    checks = {check["name"] for check in sa.inspect(bind).get_check_constraints(_LEDGER)}
    if _CHECK not in checks:
        op.create_check_constraint(_CHECK, _LEDGER, f"({_AT} IS NULL) = ({_DIGEST} IS NULL)")

    indexes = {index["name"] for index in sa.inspect(bind).get_indexes(_LEDGER)}
    if _INDEX not in indexes:
        op.create_index(
            _INDEX,
            _LEDGER,
            ["source_provider", "source_company_id", "source_record_id"],
            unique=False,
            postgresql_where=sa.text(f"{_AT} IS NOT NULL"),
        )


def downgrade() -> None:
    bind = op.get_bind()
    indexes = {index["name"] for index in sa.inspect(bind).get_indexes(_LEDGER)}
    if _INDEX in indexes:
        op.drop_index(_INDEX, table_name=_LEDGER)
    checks = {check["name"] for check in sa.inspect(bind).get_check_constraints(_LEDGER)}
    if _CHECK in checks:
        op.drop_constraint(_CHECK, _LEDGER, type_="check")
    if _has_column(bind, _LEDGER, _DIGEST):
        op.drop_column(_LEDGER, _DIGEST)
    if _has_column(bind, _LEDGER, _AT):
        op.drop_column(_LEDGER, _AT)
