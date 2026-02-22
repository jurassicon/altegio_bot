from __future__ import annotations

import os

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from altegio_bot.models.models import Base


def _require_env(name: str) -> str:
    value = (os.getenv(name) or '').strip()
    if not value:
        raise RuntimeError(f'Missing required env var: {name}')
    return value


@pytest_asyncio.fixture()
async def engine() -> AsyncEngine:
    database_url = _require_env('DATABASE_URL')

    engine = create_async_engine(
        database_url,
        echo=False,
        poolclass=NullPool,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    yield engine
    await engine.dispose()


@pytest_asyncio.fixture()
async def session_maker(engine: AsyncEngine):
    async with engine.begin() as conn:
        table_names = [t.name for t in Base.metadata.sorted_tables]
        if table_names:
            quoted = ', '.join([f'\"{name}\"' for name in table_names])
            await conn.execute(
                text(f'TRUNCATE {quoted} RESTART IDENTITY CASCADE')
            )

    return async_sessionmaker(engine, expire_on_commit=False)