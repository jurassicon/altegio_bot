"""add canary proof and target snapshot to the cutover ledger (PR-11.1, ревизия 16)

Три изменения, каждое закрывает подтверждённый блокер первой версии PR-11.1.

**1. `easyweek_migration_canary_proof`.** Раньше доказательством canary считался
`--limit 1`. Это не доказательство: limit подтверждает только, что один POST
вернул 2xx, и ничего не говорит о том, попала ли запись в нужный филиал, к нужному
мастеру и на нужное время. Вдобавок он брал *первую попавшуюся* строку из ответа
Altegio API — на каждом прогоне другого живого клиента.

Теперь canary выбирается по точной source identity, после POST перечитывается
через GET и сверяется по всем write-critical полям, а результат сохраняется
строкой. Bulk apply требует подходящий verified proof.

Поля привязки (`manifest_digest`, `request_schema_version`, `cutover_at`,
`branch_identity_digest`) существуют ровно затем, чтобы старый proof переставал
подходить, когда меняется то, о чём он свидетельствовал.

**2. `easyweek_migration_ledger.target_snapshot_fingerprint`.** Прежний rollback
считал запись неизменённой, если marker на месте и она не отменена и не завершена.
Обе проверки проходят и у записи, которую вручную перенесли на другой день,
отдали другому мастеру или переназначили другому клиенту, — а отмена такой записи
уничтожает чужую работу. Теперь при создании сохраняется PII-free дайджест живого
target (location, staff, service, customer uuid, start, duration, marker, active),
и rollback сравнивает с ним свежий GET.

**3. `easyweek_migration_ledger.last_resolution_run_id`.** `run_id` — это run,
который ПЕРВЫМ заклеймил исходную запись, и он больше не переписывается.
Reconciliation, resolution и rollback пишут свой идентификатор в отдельную
колонку. Иначе разрешение uncertain-строки переписывало бы origin run, и запись
молча выпадала бы из rollback того самого apply, который её создал.

Существующие строки не затрагиваются: все новые колонки nullable, а
`last_resolution_run_id` заполняется только при следующем изменении статуса.
Данные не мигрируются и не переписываются.

Revision ID: c4e91a70b3d8
Revises: b8d2f7a4c613
Create Date: 2026-08-29
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c4e91a70b3d8"
down_revision: str | Sequence[str] | None = "b8d2f7a4c613"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_LEDGER = "easyweek_migration_ledger"
_CANARY = "easyweek_migration_canary_proof"


def upgrade() -> None:
    op.add_column(_LEDGER, sa.Column("target_snapshot_fingerprint", sa.String(length=64), nullable=True))
    op.add_column(_LEDGER, sa.Column("last_resolution_run_id", sa.String(length=64), nullable=True))

    op.create_table(
        _CANARY,
        sa.Column("id", sa.BigInteger(), autoincrement=True, nullable=False),
        sa.Column("source_company_id", sa.Integer(), nullable=False),
        sa.Column("source_record_id", sa.BigInteger(), nullable=False),
        sa.Column("source_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("target_booking_uuid", sa.String(length=64), nullable=True),
        sa.Column("target_snapshot_fingerprint", sa.String(length=64), nullable=True),
        sa.Column("manifest_digest", sa.String(length=64), nullable=False),
        sa.Column("request_schema_version", sa.String(length=16), nullable=False),
        sa.Column("cutover_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("branch_identity_digest", sa.String(length=64), nullable=False),
        sa.Column("verified", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column("failure_reason", sa.String(length=128), nullable=True),
        sa.Column("run_id", sa.String(length=64), nullable=False),
        sa.Column("verified_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "manifest_digest",
            "request_schema_version",
            "cutover_at",
            "source_company_id",
            "source_record_id",
            name="uq_easyweek_migration_canary_identity",
        ),
        sa.CheckConstraint(
            "(verified IS NOT TRUE) OR (target_booking_uuid IS NOT NULL)",
            name="ck_easyweek_migration_canary_verified_has_target",
        ),
    )
    op.create_index(
        "ix_easyweek_migration_canary_lookup",
        _CANARY,
        ["manifest_digest", "request_schema_version"],
        unique=False,
    )


def downgrade() -> None:
    # Как и в b8d2f7a4c613: downgrade убирает только учётные структуры. Он не
    # трогает и не может трогать бронирования, уже созданные в EasyWeek — они
    # живут в другой системе и отменяются только явным rollback инструмента под
    # операторским подтверждением, а не побочным эффектом схемы.
    op.drop_index("ix_easyweek_migration_canary_lookup", table_name=_CANARY)
    op.drop_table(_CANARY)
    op.drop_column(_LEDGER, "last_resolution_run_id")
    op.drop_column(_LEDGER, "target_snapshot_fingerprint")
