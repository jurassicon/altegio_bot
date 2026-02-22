from __future__ import annotations

from collections.abc import AsyncGenerator, AsyncIterator

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from altegio_bot.models.models import Base, Client
from altegio_bot.settings import Settings


@pytest_asyncio.fixture(scope='function')
async def engine() -> AsyncGenerator[AsyncEngine, None]:
    settings = Settings()
    engine = create_async_engine(settings.database_url, future=True)

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture(scope='function')
async def session_maker(
    engine: AsyncEngine,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    async with SessionLocal() as session:
        async with session.begin():
            tables = [t.name for t in Base.metadata.sorted_tables]
            if tables:
                await session.execute(
                    text(
                        'TRUNCATE ' + ', '.join(tables)
                        + ' RESTART IDENTITY CASCADE'
                    )
                )

            session.add_all(
                [
                    Client(
                        id=1,
                        company_id=1,
                        altegio_client_id=1,
                        display_name='Client 1',
                        phone_e164='+10000000001',
                        raw={},
                    ),
                    Client(
                        id=10,
                        company_id=1,
                        altegio_client_id=10,
                        display_name='Client 10',
                        phone_e164='+10000000010',
                        raw={},
                    ),
                ]
            )

            await session.flush()

            await session.execute(
                text(
                    "SELECT setval("
                    "pg_get_serial_sequence('clients', 'id'), "
                    "(SELECT max(id) FROM clients)"
                    ")"
                )
            )

    yield SessionLocal
