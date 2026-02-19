from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy import pool
from sqlalchemy.ext.asyncio import async_engine_from_config

from altegio_bot.settings import settings


config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# ВАЖНО: импортируем settings и Base.metadata
# 1) settings.database_url берём из .env (pydantic-settings)
# 2) Base.metadata — это "карта" таблиц из SQLAlchemy моделей

from altegio_bot.models import Base
target_metadata = Base.metadata


def run_migrations_offline() -> None:
    """Миграции без подключения к БД."""
    url = settings.database_url
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection) -> None:
    context.configure(
        connection=connection,
        target_metadata=target_metadata,
        compare_type=True,
    )

    with context.begin_transaction():
        context.run_migrations()


async def run_migrations_online() -> None:
    """Async-подключение к БД и запуск миграций."""
    config.set_main_option("sqlalchemy.url", settings.database_url)

    connectable = async_engine_from_config(
        config.get_section(config.config_ini_section, {}),
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
        future=True,
    )

    async with connectable.connect() as connection:
        await connection.run_sync(do_run_migrations)

    await connectable.dispose()


def run_migrations() -> None:
    if context.is_offline_mode():
        run_migrations_offline()
    else:
        asyncio.run(run_migrations_online())


run_migrations()
