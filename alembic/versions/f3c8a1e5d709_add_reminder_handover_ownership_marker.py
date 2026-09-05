"""add reminder handover ownership marker to easyweek_migration_ledger (PR-11.2)

Две nullable-колонки в ``easyweek_migration_ledger``:

  * ``reminders_handed_over_at`` (TIMESTAMPTZ) — момент, когда будущие
    напоминания этой записи перестали принадлежать Altegio и стали
    принадлежать EasyWeek;
  * ``reminder_handover_plan_digest`` (VARCHAR(64)) — digest проверенного
    оператором плана, под которым это произошло.

Зачем отдельная отметка, а не вывод из уже существующего состояния.

Handover создавал EasyWeek-напоминания, отменял старые Altegio-напоминания и
коммитил — но нигде не записывал, что владение передано. Altegio inbox и capture
при этом намеренно не останавливаются, а обычный Altegio planner использует
``add_job()``, который при конфликте dedupe key переводит ``canceled``/``failed``
задание обратно в ``queued``. Задержанная доставка ``create``/``update`` после
успешного handover поэтому:

  * переоткрывала то самое напоминание, которое handover только что отменил;
  * при reschedule создавала новое напоминание с новым dedupe key;
  * оставляла у одной записи открытые напоминания сразу с обеих сторон.

Ни одно из уже имеющихся состояний доказательством служить не может:

  * ``status='created'`` появляется при переносе БРОНИРОВАНИЯ, задолго до
    handover и независимо от него;
  * отменённый ``MessageJob`` и его ``last_error`` — ровно то, что ``add_job()``
    и воскрешает;
  * наличие EasyWeek-напоминания: у отменённой записи и у записи без будущего
    обязательства его законно нет;
  * apply report на файловой системе runtime planner не читает.

Отметка durable, provider-scoped и записывается атомарно вместе с отменой,
которую она описывает: волна, откатившаяся по любой причине, отметки не
оставляет, а существующая отметка всегда означает, что отмена закоммичена.

CHECK держит обе колонки как один факт. Половина отметки была бы либо строкой,
которая заявляет о передаче владения, не называя, по чьему решению, либо
digest-ом без самой передачи — а runtime-фенс читает момент, тогда как apply
сравнивает digest, и каждый из них тогда отвечал бы на свой вопрос по-разному.

Partial-индекс покрывает ровно предикат runtime-фенса: он выполняется внутри
транзакции планирования на каждой доставке ``create``/``update``, поэтому обязан
быть index-only, а спрашивают только про переданные строки.

Миграция аддитивная и обратимая. Backfill намеренно отсутствует: исторические
строки не доказывают, что handover для них выполнялся, и придумать это
доказательство нельзя. Единственный корректный результат для них — владение
осталось у Altegio.

Revision ID: f3c8a1e5d709
Revises: b2d4f7a91c68
Create Date: 2026-09-04
"""

import sqlalchemy as sa

from alembic import op

revision = "f3c8a1e5d709"
down_revision = "b2d4f7a91c68"
branch_labels = None
depends_on = None

_LEDGER = "easyweek_migration_ledger"
_HANDED_OVER_AT = "reminders_handed_over_at"
_PLAN_DIGEST = "reminder_handover_plan_digest"
_CHECK = "ck_easyweek_migration_ledger_reminder_handover_complete"
_INDEX = "ix_easyweek_migration_ledger_reminder_handover"


def upgrade() -> None:
    op.add_column(_LEDGER, sa.Column(_HANDED_OVER_AT, sa.DateTime(timezone=True), nullable=True))
    op.add_column(_LEDGER, sa.Column(_PLAN_DIGEST, sa.String(length=64), nullable=True))
    op.create_check_constraint(
        _CHECK,
        _LEDGER,
        f"({_HANDED_OVER_AT} IS NULL) = ({_PLAN_DIGEST} IS NULL)",
    )
    op.create_index(
        _INDEX,
        _LEDGER,
        ["source_provider", "source_company_id", "source_record_id"],
        unique=False,
        postgresql_where=sa.text(f"{_HANDED_OVER_AT} IS NOT NULL"),
    )


def downgrade() -> None:
    # Дропает ровно то, что создал upgrade(). Уникальный индекс source identity
    # и оба прежних CHECK этой миграции не принадлежат и остаются нетронутыми.
    op.drop_index(_INDEX, table_name=_LEDGER)
    op.drop_constraint(_CHECK, _LEDGER, type_="check")
    op.drop_column(_LEDGER, _PLAN_DIGEST)
    op.drop_column(_LEDGER, _HANDED_OVER_AT)
