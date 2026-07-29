from __future__ import annotations

import logging
from collections.abc import AsyncGenerator, AsyncIterator

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

import altegio_bot.db as app_db
from altegio_bot.models.models import Base, Client
from altegio_bot.settings import Settings


@pytest_asyncio.fixture(autouse=True)
async def _dispose_global_engine_pool() -> AsyncGenerator[None, None]:
    """Never let a pooled connection on the GLOBAL app engine outlive its event loop.

    Some production code deliberately uses the module-global ``SessionLocal`` /
    engine rather than an injected factory — ``meta_circuit.close_meta_circuit()``
    is the one the operator-relay send-failure paths reach. That global engine uses
    a normal QueuePool, while pytest-asyncio gives every test its own event loop.
    So the first test that closes the Meta circuit leaves a live asyncpg connection
    checked into the global pool, bound to a loop that is closed moments later.

    From then on that connection is poison: when it is next reused, recycled or
    finalized, asyncpg schedules ``Connection._cancel`` on the dead loop
    (``protocol.pyx`` -> ``Connection._cancel_current_command``). ``create_task``
    on a closed loop never runs it, which surfaces as the unraisable
    ``RuntimeWarning: coroutine 'Connection._cancel' was never awaited`` and, far
    worse, as order-dependent failures in later, unrelated tests.

    Disposing the global pool at the end of each test closes those connections
    while their own loop is still running. This is test-infrastructure lifecycle
    only: no production behaviour, semantics or warning filter is changed, and
    disposing an already-empty pool is a no-op.
    """
    try:
        yield
    finally:
        await app_db.engine.dispose()


@pytest.fixture(scope="session", autouse=True)
def _silence_httpx_request_log() -> None:
    """Keep webhook secrets out of test diagnostics.

    httpx logs every request at INFO as a full URL, and webhook tests carry the
    shared secret in the query string (``?token=``/``?secret=``). pytest prints
    captured logs on failure and CI archives them, so that INFO line is a real
    leak channel — one that no amount of masking inside the app can close.
    """
    logging.getLogger("httpx").setLevel(logging.WARNING)


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
