"""Release production preflight read transactions before provider I/O.

Preflight ORM rows are read facts, not a unit of work. Detaching them before
closing the session preserves their loaded attributes without expiring them or
flushing anything. Subsequent local reads start a fresh short transaction; the
separate durable claim still rechecks identity and consent under its own locks.
"""

from __future__ import annotations

from functools import wraps
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession


async def release_read_session(session: AsyncSession) -> None:
    """End reads without committing or silently discarding pending ORM writes."""
    if session.new or session.dirty or session.deleted:
        raise RuntimeError("voucher_production_preflight_session_has_writes")
    # Session.close normally expunges too; doing it explicitly before closing
    # documents why rollback cannot expire the read facts held by the proof.
    session.expunge_all()
    await session.close()


class _ReleasedReadTransport:
    def __init__(self, reader: Any, session: AsyncSession) -> None:
        self.reader = reader
        self.session = session

    def __getattr__(self, name: str) -> Any:
        target = getattr(self.reader, name)
        if not callable(target):
            return target

        @wraps(target)
        async def read(*args: Any, **kwargs: Any) -> Any:
            await release_read_session(self.session)
            return await target(*args, **kwargs)

        return read


def release_reads_before_http(*reader_names: str):
    """Bound a production read proof, including its final local SELECTs.

    The wrapped functions take their AsyncSession first and reader transports as
    keyword-only arguments. Nested proof boundaries reuse the same proxy.
    """

    def decorate(proof):
        @wraps(proof)
        async def wrapped(session: AsyncSession, *args: Any, **kwargs: Any):
            # Reject a caller's pending unit of work before any SELECT could
            # autoflush it and hide it from the pending-write guard.
            await release_read_session(session)
            for name in reader_names:
                reader = kwargs[name]
                if not isinstance(reader, _ReleasedReadTransport) or reader.session is not session:
                    kwargs[name] = _ReleasedReadTransport(reader, session)
            try:
                return await proof(session, *args, **kwargs)
            finally:
                # Proofs may finish with entitlement/local identity reads after
                # their last GET. Release those before run_* performs any POST.
                await release_read_session(session)

        return wrapped

    return decorate
