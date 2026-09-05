"""add contract_kind to easyweek_migration_canary_proof (PR-11.2, §30.12)

Одна колонка и расширение уникального ключа.

Зачем. Canary доказывает ОДИН mutation contract. `single` — это
``POST /bookings`` с одним `service_uuid`; `cart_two` — ``POST /bookings/cart``
с массивом `items` и двумя услугами. Это разные эндпоинты, разные тела запроса и
разные формы readback, поэтому canary, доказавший один из них, не доказал о
втором ничего.

Без этой колонки успешный single-canary лицензировал бы bulk через cart-путь, по
которому ни одно реальное бронирование ещё не создавалось, и наоборот. Поэтому
contract kind — часть идентичности proof, а не подпись к ней: он входит в
уникальный ключ и в lookup-индекс, по которым bulk ищет свою лицензию.

Существующие строки получают `single`: на момент их записи другого контракта не
существовало, и объявить их доказательством cart было бы выдумкой.

Уникальный ключ пересоздаётся, а не дополняется: PostgreSQL не умеет добавить
колонку в существующее ограничение. Порядок — drop, затем create — внутри одной
транзакции миграции, поэтому окна без ограничения нет.

Revision ID: a7d1f4c82b95
Revises: f3c8a1e5d709
Create Date: 2026-09-04
"""

import sqlalchemy as sa

from alembic import op

revision = "a7d1f4c82b95"
down_revision = "f3c8a1e5d709"
branch_labels = None
depends_on = None

_PROOF = "easyweek_migration_canary_proof"
_COLUMN = "contract_kind"
_UNIQUE = "uq_easyweek_migration_canary_identity"
_LOOKUP = "ix_easyweek_migration_canary_lookup"

_OLD_UNIQUE_COLUMNS = [
    "manifest_digest",
    "request_schema_version",
    "cutover_at",
    "source_company_id",
    "source_record_id",
]
_NEW_UNIQUE_COLUMNS = [
    "manifest_digest",
    "request_schema_version",
    _COLUMN,
    "cutover_at",
    "source_company_id",
    "source_record_id",
]
_OLD_LOOKUP_COLUMNS = ["manifest_digest", "request_schema_version"]
_NEW_LOOKUP_COLUMNS = ["manifest_digest", "request_schema_version", _COLUMN]


def upgrade() -> None:
    op.add_column(
        _PROOF,
        sa.Column(_COLUMN, sa.String(length=16), nullable=False, server_default=sa.text("'single'")),
    )
    op.drop_constraint(_UNIQUE, _PROOF, type_="unique")
    op.create_unique_constraint(_UNIQUE, _PROOF, _NEW_UNIQUE_COLUMNS)
    op.drop_index(_LOOKUP, table_name=_PROOF)
    op.create_index(_LOOKUP, _PROOF, _NEW_LOOKUP_COLUMNS, unique=False)


def downgrade() -> None:
    # Восстанавливается ровно прежняя форма ключа и индекса. Строки `cart_two`
    # при этом могут столкнуться с прежним уникальным ключом — это правильно:
    # откат к схеме, которая не различает контракты, обязан отказаться, а не
    # молча склеить два разных доказательства в одно.
    op.drop_index(_LOOKUP, table_name=_PROOF)
    op.create_index(_LOOKUP, _PROOF, _OLD_LOOKUP_COLUMNS, unique=False)
    op.drop_constraint(_UNIQUE, _PROOF, type_="unique")
    op.create_unique_constraint(_UNIQUE, _PROOF, _OLD_UNIQUE_COLUMNS)
    op.drop_column(_PROOF, _COLUMN)
