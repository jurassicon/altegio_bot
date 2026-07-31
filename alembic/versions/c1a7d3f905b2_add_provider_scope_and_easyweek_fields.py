"""add provider scope and easyweek identity fields

PR-3 (INTEGRATION_PLAN §3.1): готовит схему к одновременной работе Altegio и
EasyWeek, НЕ меняя поведение Altegio-пути.

  * ``provider VARCHAR(32) NOT NULL DEFAULT 'altegio'`` в пяти таблицах:
    ``clients``, ``records``, ``message_templates``, ``message_jobs``,
    ``whatsapp_senders``. Все существующие строки backfill'ятся ``'altegio'`` —
    семантика не меняется. Ни enum, ни CHECK: третья CRM должна добавляться
    кодом, а не миграцией против ограничивающего типа.
  * Уники становятся provider-scoped. Numeric id EasyWeek (location, booking,
    customer) делят пространство значений с Altegio, поэтому коллизия
    ``(company_id, external_id)`` между провайдерами реальна.
  * ``records.easyweek_booking_uuid`` (+ partial unique только для
    ``provider='easyweek'``) и ``records.easyweek_booking_hash_id`` —
    identity EasyWeek (§1.6.2–3). Hash — строка, а не BigInteger: числовой тип
    предположил бы формат и потерял ведущие нули.
  * ``message_templates.meta_template_name`` — DB-first резолв имени
    Meta-шаблона вместо глобального ``META_TEMPLATE_MAP`` (§1.6.9).

Порядок upgrade выбран так, чтобы быть безопасным на живой базе: сначала
колонки с database default (в PostgreSQL 11+ это не переписывает таблицу),
затем явный backfill, затем ``SET NOT NULL``, и только потом обмен уников и
индексов. Строки не удаляются и не переписываются.

Downgrade **fail-closed**: если к моменту отката в таблице уже есть строки
разных провайдеров с одинаковым ``(company_id, external_id)``, восстановить
старый уник нельзя. Миграция об этом сообщает и останавливается, а НЕ
схлопывает и не удаляет данные.

Revision ID: c1a7d3f905b2
Revises: 9a1f4c7b2e3d
Create Date: 2026-07-31 00:00:00.000000

"""

from typing import Sequence, Union

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c1a7d3f905b2"
down_revision: Union[str, Sequence[str], None] = "9a1f4c7b2e3d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


# The five provider-scoped tables (INTEGRATION_PLAN §3.1). Deliberately NOT
# record_services / outbox_messages / service_sender_rules / campaign tables:
# those are reached through their parent row, which already carries a provider.
_PROVIDER_TABLES = (
    "clients",
    "records",
    "message_templates",
    "message_jobs",
    "whatsapp_senders",
)

# (table, old constraint, new constraint, scoped columns)
_UNIQUE_SWAPS = (
    (
        "clients",
        "uq_clients_company_altegio_id",
        "uq_clients_provider_company_altegio_id",
        ("provider", "company_id", "altegio_client_id"),
    ),
    (
        "records",
        "uq_records_company_altegio_id",
        "uq_records_provider_company_altegio_id",
        ("provider", "company_id", "altegio_record_id"),
    ),
    (
        "whatsapp_senders",
        "uq_whatsapp_senders_company_code",
        "uq_whatsapp_senders_provider_company_code",
        ("provider", "company_id", "sender_code"),
    ),
)


_PROVIDER_TYPE = sa.String(length=32)
_PROVIDER_DEFAULT = sa.text("'altegio'")
_EASYWEEK_UUID_PREDICATE = sa.text("provider = 'easyweek' AND easyweek_booking_uuid IS NOT NULL")


def upgrade() -> None:
    """Upgrade schema (additive; existing rows are preserved and backfilled).

    Every statement is a plain Alembic operation, deliberately WITHOUT
    ``IF NOT EXISTS`` / ``IF EXISTS``. A name-only existence check would accept
    a same-named object with different columns, order, type or predicate and
    call the drifted production schema "already correct". Failing instead is the
    point: PostgreSQL runs this in one transaction, so an unexpected object
    aborts the whole migration and leaves the previous schema intact.
    """
    # ---------------------------------------------------------------- 1. columns
    # Added nullable first, with the server default. ADD COLUMN ... DEFAULT on
    # PostgreSQL 11+ does not rewrite the table, so this stays cheap even on the
    # large tables.
    for table in _PROVIDER_TABLES:
        op.add_column(
            table,
            sa.Column("provider", _PROVIDER_TYPE, nullable=True, server_default=_PROVIDER_DEFAULT),
        )

    op.add_column("records", sa.Column("easyweek_booking_uuid", postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column("records", sa.Column("easyweek_booking_hash_id", sa.String(length=64), nullable=True))
    op.add_column("message_templates", sa.Column("meta_template_name", sa.String(length=128), nullable=True))

    # --------------------------------------------------------------- 2. backfill
    # Explicit even though the DEFAULT already filled existing rows: this is the
    # step the plan requires to be visible, and it must complete before the
    # column can be made NOT NULL.
    for table in _PROVIDER_TABLES:
        op.execute(sa.text(f"UPDATE {table} SET provider = 'altegio' WHERE provider IS NULL"))

    # --------------------------------------------------------------- 3. NOT NULL
    # `existing_server_default` keeps the default in place: application INSERTs
    # that never name `provider` (the whole existing Altegio path) must go on
    # landing as 'altegio'.
    for table in _PROVIDER_TABLES:
        op.alter_column(
            table,
            "provider",
            existing_type=_PROVIDER_TYPE,
            existing_server_default=_PROVIDER_DEFAULT,
            nullable=False,
        )

    # ------------------------------------------------- 4. provider-scoped uniques
    for table, old_name, new_name, columns in _UNIQUE_SWAPS:
        op.create_unique_constraint(new_name, table, list(columns))
        op.drop_constraint(old_name, table, type_="unique")

    # ----------------------------------------------------- 5. scoped index rebuild
    # Narrow by design: only the composite company_id indexes whose lookup
    # becomes provider-scoped. Single-column indexes are left alone.
    op.drop_index("ix_clients_company_phone", table_name="clients")
    op.create_index(
        "ix_clients_provider_company_phone",
        "clients",
        ["provider", "company_id", "phone_e164"],
    )

    op.drop_index("ix_message_jobs_company_type_status", table_name="message_jobs")
    op.create_index(
        "ix_message_jobs_provider_company_type_status",
        "message_jobs",
        ["provider", "company_id", "job_type", "status"],
    )

    # New: message_templates had no composite index at all, and PR-5 resolves a
    # template by (provider, company, code, language).
    op.create_index(
        "ix_message_templates_provider_company_code_lang",
        "message_templates",
        ["provider", "company_id", "code", "language"],
    )

    # --------------------------------------- 6. EasyWeek booking UUID uniqueness
    # Partial: Altegio rows (all NULL) are unconstrained, and several EasyWeek
    # rows may still have no captured UUID because NULLs are distinct here.
    op.create_index(
        "uq_records_easyweek_booking_uuid",
        "records",
        ["easyweek_booking_uuid"],
        unique=True,
        postgresql_where=_EASYWEEK_UUID_PREDICATE,
    )


def _fail_closed_if_cross_provider_duplicates() -> None:
    """Refuse to downgrade when the old, narrower uniqueness cannot hold.

    Once EasyWeek rows exist, ``(company_id, external_id)`` may legitimately
    repeat across providers. Restoring the pre-PR-3 constraint would then be
    impossible, and the only ways to force it — deleting or merging rows — are
    exactly what a downgrade must never do. Stop instead, with the query the
    operator needs.
    """
    if op.get_context().as_sql:
        # Offline (--sql) generation has no database to inspect, so the guard
        # CANNOT run. Say that loudly in the generated script rather than
        # emitting statements that look like a verified downgrade.
        op.execute(
            sa.text(
                "-- WARNING: NOT VERIFIED. The cross-provider duplicate check was NOT executed: "
                "offline SQL generation cannot read the target database. Running this script "
                "without checking first may fail on the restored unique constraints."
            )
        )
        return

    bind = op.get_bind()
    for table, old_name, _new_name, columns in _UNIQUE_SWAPS:
        legacy_columns = [column for column in columns if column != "provider"]
        column_list = ", ".join(legacy_columns)
        conflicts = bind.execute(
            sa.text(
                f"SELECT count(*) FROM ("
                f"  SELECT 1 FROM {table} GROUP BY {column_list} HAVING count(*) > 1"
                f") AS duplicated"
            )
        ).scalar_one()
        if conflicts:
            raise RuntimeError(
                f"Cannot downgrade: {table} has {conflicts} ({column_list}) group(s) that exist for more than "
                f"one provider, so the pre-PR-3 constraint {old_name} cannot be restored. "
                f"Resolve the rows deliberately first: "
                f"SELECT {column_list}, count(*) FROM {table} GROUP BY {column_list} HAVING count(*) > 1;"
            )


def downgrade() -> None:
    """Downgrade schema (drops only what this revision added; deletes no rows).

    Plain Alembic operations here too: rolling back an object that is not the
    one this revision created must fail rather than be silently tolerated.
    """
    # Checked BEFORE anything is dropped, so a refusal leaves the schema intact.
    _fail_closed_if_cross_provider_duplicates()

    op.drop_index("uq_records_easyweek_booking_uuid", table_name="records")

    op.drop_index("ix_message_templates_provider_company_code_lang", table_name="message_templates")

    op.drop_index("ix_message_jobs_provider_company_type_status", table_name="message_jobs")
    op.create_index(
        "ix_message_jobs_company_type_status",
        "message_jobs",
        ["company_id", "job_type", "status"],
    )

    op.drop_index("ix_clients_provider_company_phone", table_name="clients")
    op.create_index("ix_clients_company_phone", "clients", ["company_id", "phone_e164"])

    for table, old_name, new_name, columns in _UNIQUE_SWAPS:
        legacy_columns = [column for column in columns if column != "provider"]
        op.create_unique_constraint(old_name, table, legacy_columns)
        op.drop_constraint(new_name, table, type_="unique")

    op.drop_column("message_templates", "meta_template_name")
    op.drop_column("records", "easyweek_booking_hash_id")
    op.drop_column("records", "easyweek_booking_uuid")

    for table in _PROVIDER_TABLES:
        op.drop_column(table, "provider")
