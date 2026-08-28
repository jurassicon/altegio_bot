"""add EasyWeek visit counter to clients (PR-11)

Хранит последний ДОКАЗАННЫЙ снимок ``visits_total`` из ``booking-succeeded``
на строке клиента EasyWeek:

  * ``easyweek_visits_total`` (INTEGER, nullable) — количество завершённых
    визитов, как его назвал сам EasyWeek;
  * ``easyweek_visits_total_updated_at`` (TIMESTAMPTZ, nullable) — момент, когда
    это значение было принято.

Почему типизированные колонки, а не ключи в ``clients.raw``: значение читается
доменной логикой (PR-12 ``repeat_10d`` / ``comeback_3d``), сравнивается на
монотонность и участвует в CHECK-инвариантах. Ключ в неструктурированном JSONB
не даёт ни типа, ни ограничения, ни индексируемого сравнения, а ``raw``
переписывается lifecycle-событиями целиком.

Почему snapshot, а не счётчик-инкремент: Resend, повтор с другим payload hash и
настоящий второй визит на уровне «пришёл вебхук» неразличимы. ``current + 1``
разошёлся бы с EasyWeek при первом же Resend; сохранение объявленного числа
сходится к одному значению при любом количестве повторов.

Три CHECK-инварианта:

  * ``ck_clients_easyweek_visits_total_provider`` — счётчик EasyWeek не может
    появиться на строке Altegio. У Altegio своя живая правда
    (``count_attended_client_visits`` по API), и второе хранимое число стало бы
    молча расходящимся источником истины.
  * ``ck_clients_easyweek_visits_total_positive`` — ``booking-succeeded``
    доказывает состоявшийся визит, поэтому снимок не меньше 1. Ноль или
    отрицательное значение — признак малформед-доставки.
  * ``ck_clients_easyweek_visits_total_paired`` — значение и момент его принятия
    это один факт: NULL либо оба, либо ни одного.

Существующие строки не трогаются: обе колонки nullable и без server_default,
поэтому у всех клиентов — и EasyWeek, и Altegio — остаётся NULL. Backfill в
PR-11 намеренно не делается: прошлые визиты не доказаны ни одним сохранённым
``booking-succeeded``, а придумывать их значение нельзя.

Revision ID: a7c3e91b5d24
Revises: d4e8a1c39f57
Create Date: 2026-08-28
"""

from alembic import op
import sqlalchemy as sa

revision = "a7c3e91b5d24"
down_revision = "d4e8a1c39f57"
branch_labels = None
depends_on = None

_VISITS_TOTAL = "easyweek_visits_total"
_VISITS_TOTAL_UPDATED_AT = "easyweek_visits_total_updated_at"

_PROVIDER_CHECK = "ck_clients_easyweek_visits_total_provider"
_POSITIVE_CHECK = "ck_clients_easyweek_visits_total_positive"
_PAIRED_CHECK = "ck_clients_easyweek_visits_total_paired"


def upgrade() -> None:
    op.add_column("clients", sa.Column(_VISITS_TOTAL, sa.Integer(), nullable=True))
    op.add_column(
        "clients",
        sa.Column(_VISITS_TOTAL_UPDATED_AT, sa.DateTime(timezone=True), nullable=True),
    )

    op.create_check_constraint(
        _PROVIDER_CHECK,
        "clients",
        f"{_VISITS_TOTAL} IS NULL OR provider = 'easyweek'",
    )
    op.create_check_constraint(
        _POSITIVE_CHECK,
        "clients",
        f"{_VISITS_TOTAL} IS NULL OR {_VISITS_TOTAL} >= 1",
    )
    op.create_check_constraint(
        _PAIRED_CHECK,
        "clients",
        f"({_VISITS_TOTAL} IS NULL) = ({_VISITS_TOTAL_UPDATED_AT} IS NULL)",
    )


def downgrade() -> None:
    # Constraints first: dropping a column that a CHECK still references fails
    # on PostgreSQL, and the order must mirror upgrade() exactly so a
    # downgrade/upgrade cycle leaves the same schema.
    op.drop_constraint(_PAIRED_CHECK, "clients", type_="check")
    op.drop_constraint(_POSITIVE_CHECK, "clients", type_="check")
    op.drop_constraint(_PROVIDER_CHECK, "clients", type_="check")

    op.drop_column("clients", _VISITS_TOTAL_UPDATED_AT)
    op.drop_column("clients", _VISITS_TOTAL)
