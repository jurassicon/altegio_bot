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
from sqlalchemy.pool import NullPool

from altegio_bot.models.models import Base, Client
from altegio_bot.settings import Settings


@pytest_asyncio.fixture(scope="session")
async def engine() -> AsyncGenerator[AsyncEngine, None]:
    """Session-scoped database engine using NullPool.

    Why session-scoped instead of function-scoped
    -----------------------------------------------
    The previous function-scoped engine ran Base.metadata.drop_all +
    Base.metadata.create_all for EVERY test (1000+ tests).  DROP TABLE
    acquires a PostgreSQL AccessExclusiveLock on each table.  When the
    async connection-pool teardown from one test overlapped with the
    DROP TABLE setup of the next test, those two sessions entered a
    circular lock-wait and PostgreSQL raised DeadlockDetectedError.

    Lifting the engine and DDL to session scope eliminates the race:
    drop_all / create_all run exactly once at session start and the
    schema is ready for all tests.

    Why NullPool
    ------------
    SQLAlchemy's default connection pool binds each pooled connection to
    the event loop that created it.  With pytest-asyncio in asyncio_mode
    'auto', each test function runs on its own event loop; if the engine
    (session-scoped) created pooled connections on the session loop, tests
    running on their own function-scoped loops would encounter asyncpg's
    "Future attached to a different loop" RuntimeError.

    NullPool disables connection pooling entirely: every engine.begin() /
    engine.connect() opens a brand-new physical connection on the caller's
    current event loop and closes it when the context manager exits.  This
    makes the engine object itself loop-agnostic, so a session-scoped engine
    is safely shared across tests that each run on their own event loop.
    """
    settings = Settings()
    engine = create_async_engine(
        settings.database_url,
        future=True,
        poolclass=NullPool,
    )

    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture(scope="function")
async def session_maker(
    engine: AsyncEngine,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    """Function-scoped session factory.

    Truncates every table and re-seeds the two baseline clients before each
    test so data does not bleed between tests.  RESTART IDENTITY resets all
    sequences (so auto-increment IDs are predictable).  CASCADE handles
    foreign-key dependencies in a single statement.

    No DDL is performed here — only data cleanup.  The schema is owned by
    the session-scoped engine fixture above, which eliminates the
    AccessExclusiveLock deadlock described there.
    """
    SessionLocal = async_sessionmaker(engine, expire_on_commit=False)

    async with SessionLocal() as session:
        async with session.begin():
            tables = [t.name for t in Base.metadata.sorted_tables]
            if tables:
                await session.execute(text("TRUNCATE " + ", ".join(tables) + " RESTART IDENTITY CASCADE"))

            session.add_all(
                [
                    Client(
                        id=1,
                        company_id=1,
                        altegio_client_id=1,
                        display_name="Client 1",
                        phone_e164="+10000000001",
                        raw={},
                    ),
                    Client(
                        id=10,
                        company_id=1,
                        altegio_client_id=10,
                        display_name="Client 10",
                        phone_e164="+10000000010",
                        raw={},
                    ),
                ]
            )

            await session.flush()

            await session.execute(
                text("SELECT setval(pg_get_serial_sequence('clients', 'id'), (SELECT max(id) FROM clients))")
            )

    yield SessionLocal
