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


def _add_constraint_if_absent(table: str, name: str, columns: Sequence[str]) -> None:
    """PostgreSQL has no ``ADD CONSTRAINT IF NOT EXISTS``; emulate it."""
    column_list = ", ".join(columns)
    op.execute(
        sa.text(
            f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = '{name}'
                ) THEN
                    ALTER TABLE {table} ADD CONSTRAINT {name} UNIQUE ({column_list});
                END IF;
            END $$;
            """
        )
    )


def upgrade() -> None:
    """Upgrade schema (additive; existing rows are preserved and backfilled)."""
    # ---------------------------------------------------------------- 1. columns
    # ADD COLUMN ... DEFAULT on PostgreSQL 11+ does not rewrite the table, so
    # this stays cheap even on the large tables.
    for table in _PROVIDER_TABLES:
        op.execute(sa.text(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS provider VARCHAR(32) DEFAULT 'altegio'"))

    op.execute(sa.text("ALTER TABLE records ADD COLUMN IF NOT EXISTS easyweek_booking_uuid UUID"))
    op.execute(sa.text("ALTER TABLE records ADD COLUMN IF NOT EXISTS easyweek_booking_hash_id VARCHAR(64)"))
    op.execute(sa.text("ALTER TABLE message_templates ADD COLUMN IF NOT EXISTS meta_template_name VARCHAR(128)"))

    # --------------------------------------------------------------- 2. backfill
    # Explicit even though the DEFAULT already filled existing rows: an
    # environment where the column was added without a default (or by a partial
    # re-run) must still end up fully populated before SET NOT NULL.
    for table in _PROVIDER_TABLES:
        op.execute(sa.text(f"UPDATE {table} SET provider = 'altegio' WHERE provider IS NULL"))

    # --------------------------------------------------------------- 3. NOT NULL
    for table in _PROVIDER_TABLES:
        op.execute(sa.text(f"ALTER TABLE {table} ALTER COLUMN provider SET NOT NULL"))

    # ------------------------------------------- 4. keep the database default
    # Never dropped: application INSERTs that do not name `provider` (the whole
    # existing Altegio path) must keep landing as 'altegio'.
    for table in _PROVIDER_TABLES:
        op.execute(sa.text(f"ALTER TABLE {table} ALTER COLUMN provider SET DEFAULT 'altegio'"))

    # ------------------------------------------------- 5. provider-scoped uniques
    for table, old_name, new_name, columns in _UNIQUE_SWAPS:
        _add_constraint_if_absent(table, new_name, columns)
        op.execute(sa.text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {old_name}"))

    # ----------------------------------------------------- 6. scoped index rebuild
    # Narrow by design: only the composite company_id indexes whose lookup
    # becomes provider-scoped. Single-column indexes are left alone.
    op.execute(sa.text("DROP INDEX IF EXISTS ix_clients_company_phone"))
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_clients_provider_company_phone ON clients (provider, company_id, phone_e164)"
        )
    )

    op.execute(sa.text("DROP INDEX IF EXISTS ix_message_jobs_company_type_status"))
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_message_jobs_provider_company_type_status "
            "ON message_jobs (provider, company_id, job_type, status)"
        )
    )

    # New: message_templates had no composite index at all, and PR-5 resolves a
    # template by (provider, company, code, language).
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_message_templates_provider_company_code_lang "
            "ON message_templates (provider, company_id, code, language)"
        )
    )

    # --------------------------------------- 7. EasyWeek booking UUID uniqueness
    # Partial: Altegio rows (all NULL) are unconstrained, and several EasyWeek
    # rows may still have no captured UUID because NULLs are distinct here.
    op.execute(
        sa.text(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_records_easyweek_booking_uuid "
            "ON records (easyweek_booking_uuid) "
            "WHERE provider = 'easyweek' AND easyweek_booking_uuid IS NOT NULL"
        )
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
        # Offline (--sql) generation has no database to inspect. Say so instead
        # of pretending the check passed.
        op.execute(
            sa.text(
                "-- SKIPPED cross-provider duplicate check: offline SQL generation "
                "cannot read the target database. Verify manually before applying."
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
    """Downgrade schema (drops only what this revision added; deletes no rows)."""
    # Checked BEFORE anything is dropped, so a refusal leaves the schema intact.
    _fail_closed_if_cross_provider_duplicates()

    op.execute(sa.text("DROP INDEX IF EXISTS uq_records_easyweek_booking_uuid"))

    op.execute(sa.text("DROP INDEX IF EXISTS ix_message_templates_provider_company_code_lang"))

    op.execute(sa.text("DROP INDEX IF EXISTS ix_message_jobs_provider_company_type_status"))
    op.execute(
        sa.text(
            "CREATE INDEX IF NOT EXISTS ix_message_jobs_company_type_status "
            "ON message_jobs (company_id, job_type, status)"
        )
    )

    op.execute(sa.text("DROP INDEX IF EXISTS ix_clients_provider_company_phone"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_clients_company_phone ON clients (company_id, phone_e164)"))

    for table, old_name, new_name, columns in _UNIQUE_SWAPS:
        legacy_columns = [column for column in columns if column != "provider"]
        _add_constraint_if_absent(table, old_name, legacy_columns)
        op.execute(sa.text(f"ALTER TABLE {table} DROP CONSTRAINT IF EXISTS {new_name}"))

    op.execute(sa.text("ALTER TABLE message_templates DROP COLUMN IF EXISTS meta_template_name"))
    op.execute(sa.text("ALTER TABLE records DROP COLUMN IF EXISTS easyweek_booking_hash_id"))
    op.execute(sa.text("ALTER TABLE records DROP COLUMN IF EXISTS easyweek_booking_uuid"))

    for table in _PROVIDER_TABLES:
        op.execute(sa.text(f"ALTER TABLE {table} DROP COLUMN IF EXISTS provider"))
