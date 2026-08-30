"""add wave scope to the cutover canary proof (PR-11.1, ревизия 18)

Две колонки в ``easyweek_migration_canary_proof``. Обе расширяют уже
существующий canary binding, а не создают параллельную модель scope.

**Зачем.** Canary proof — единственная durable запись о том, *какую именно волну*
разрешили переносить: её пишет canary, её требует bulk apply. Финальная
reconciliation обязана доказывать ту же волну, а не ту, которую оператор назвал
сегодня. Без привязки существовал обход: запустить ``reconcile --final`` без
``--cutover-at`` (код подставлял текущее время) или перевести уже перенесённого
мастера в ``deferred_altegio_staff_ids`` — и его записи выпадали из проверки
вместе со своими EasyWeek targets, а команда могла вернуть PASS.

``staff_scope_digest`` — дайджест только selected/deferred списков. Он уже
входит в ``manifest_digest``, но отдельно хранится ради однозначного диагноза:
``migration_scope_staff_scope_mismatch`` вместо общего «manifest изменился».
Перевод мастера между волнами — ровно то изменение, которое прячет его записи от
проверки, и оно должно называться своим именем.

``horizon_days`` — окно, на которое волна смотрела вперёд. Более узкий горизонт
на этапе reconciliation тихо отбросил бы дальний край волны из доказательства.

Обе колонки **nullable**: строки, записанные до этой ревизии, backfill'ить
нечем — горизонта и selector-дайджеста в них никогда не было. NULL трактуется
как отсутствие доказанного scope, то есть fail closed: такой proof не подходит
ни под одну волну и финальная reconciliation его не принимает. Придумывать
значение было бы ровно тем, что весь PR-11.1 запрещает.

Существующие данные не переписываются, никакие другие таблицы не затрагиваются.

Revision ID: d5f0c8a91b47
Revises: c4e91a70b3d8
Create Date: 2026-08-30
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d5f0c8a91b47"
down_revision: str | Sequence[str] | None = "c4e91a70b3d8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CANARY = "easyweek_migration_canary_proof"


def upgrade() -> None:
    op.add_column(_CANARY, sa.Column("staff_scope_digest", sa.String(length=64), nullable=True))
    op.add_column(_CANARY, sa.Column("horizon_days", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column(_CANARY, "horizon_days")
    op.drop_column(_CANARY, "staff_scope_digest")
