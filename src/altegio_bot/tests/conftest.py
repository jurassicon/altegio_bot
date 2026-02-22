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

from altegio_bot.models.models import Base, Client


def _get_db_url() -> str:
    url = (os.getenv('DATABASE_URL') or '').strip()
    if not url:
        raise RuntimeError('DATABASE_URL env var is required for tests')
    return url


@pytest_asyncio.fixture
async def engine() -> AsyncEngine:
    engine = create_async_engine(
        _get_db_url(),
        poolclass=NullPool,
    )
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def session_maker(engine: AsyncEngine):
    async with engine.begin() as conn:
        table_names = [t.name for t in Base.metadata.sorted_tables]
        if table_names:
            quoted = ', '.join([f'"{name}"' for name in table_names])
            sql = f'TRUNCATE {quoted} RESTART IDENTITY CASCADE'
            await conn.execute(text(sql))

    maker = async_sessionmaker(engine, expire_on_commit=False)

    async with maker() as session:
        async with session.begin():
            session.add(
                Client(
                    id=10,
                    company_id=1,
                    altegio_client_id=10,
                    phone_e164='+10000000000',
                    display_name='Test Client',
                )
            )

    return maker