"""add easyweek_events

Research-grade capture доставок вебхуков EasyWeek. Каждая аутентифицированная
доставка — включая ретраи, Resend и не-JSON тела — сохраняется отдельной
строкой. Дедупликации нет: ``payload_hash`` индексируется, но сознательно НЕ
уникален, чтобы повторные доставки сохранялись как данные. Индекс по ``status``
— задел под inbox-воркер PR-4, который будет забирать строки status='captured'.

Автоматического TTL нет: таблица содержит PII, политика хранения ручная (см.
docs/easyweek/capture_runbook.md).

Миграция строго аддитивная: чужих таблиц не касается.

ВНИМАНИЕ про совместимость: эта ревизия создаёт БАЗОВУЮ таблицу. Колонки
``body_raw``/``body_size_bytes`` добавляет ОТДЕЛЬНАЯ более поздняя ревизия
(8705ec49cc73). Их сознательно нет здесь: раньше их дописали прямо в CREATE
TABLE этого файла, но среда, где ранняя версия ревизии уже была применена,
никогда не перезапустит её и осталась бы без колонок. Дополнять существующую
применённую ревизию нельзя — только новой аддитивной.

Revision ID: 8923be993170
Revises: e9a7c6b5d4f3
Create Date: 2026-07-23 17:41:39.770621

"""

from typing import Sequence, Union

import sqlalchemy as sa

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "8923be993170"
down_revision: Union[str, Sequence[str], None] = "e9a7c6b5d4f3"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    op.execute(
        sa.text(
            "CREATE TABLE IF NOT EXISTS easyweek_events ("
            "id BIGSERIAL PRIMARY KEY, "
            "received_at TIMESTAMPTZ NOT NULL DEFAULT now(), "
            "status VARCHAR(32) NOT NULL DEFAULT 'captured', "
            "event_hint VARCHAR(32), "
            "auth_via VARCHAR(16), "
            "payload_hash VARCHAR(64), "
            "content_type VARCHAR(128), "
            "body_text TEXT, "
            "body_truncated BOOLEAN NOT NULL DEFAULT false, "
            "query JSONB NOT NULL DEFAULT '{}'::jsonb, "
            "headers JSONB NOT NULL DEFAULT '{}'::jsonb, "
            "payload JSONB NOT NULL DEFAULT '{}'::jsonb"
            ")"
        )
    )
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_easyweek_events_received_at ON easyweek_events (received_at)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_easyweek_events_event_hint ON easyweek_events (event_hint)"))
    # НЕ unique: ретраи/Resend обязаны становиться отдельными строками.
    # Повторные доставки анализируются группировкой по этому хэшу.
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_easyweek_events_payload_hash ON easyweek_events (payload_hash)"))
    op.execute(sa.text("CREATE INDEX IF NOT EXISTS ix_easyweek_events_status ON easyweek_events (status)"))


def downgrade() -> None:
    """Downgrade schema."""
    op.execute(sa.text("DROP TABLE IF EXISTS easyweek_events"))
