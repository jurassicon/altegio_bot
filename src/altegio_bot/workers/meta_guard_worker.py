"""Meta circuit recovery guard worker.

This worker never sends customer messages and never mutates MessageJob rows. It
only watches the global circuit, probes a safe Meta metadata endpoint when a
probe is due, and opens the circuit after a successful probe.

The circuit is global and the probe uses the first active sender. Per-sender
circuits are future work.
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

from sqlalchemy import select

from altegio_bot.db import SessionLocal
from altegio_bot.models.models import WhatsAppSender
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.services.meta_circuit import (
    get_meta_circuit_state,
    mark_meta_circuit_probe_failed,
    mark_meta_circuit_probing,
    open_meta_circuit,
)
from altegio_bot.settings import settings

logger = logging.getLogger("meta_guard_worker")


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


@dataclass(frozen=True)
class ProbeResult:
    ok: bool
    error_kind: str | None = None
    error_code: str | None = None


def _probe_backoff_seconds(attempt: int) -> int:
    schedule = settings.meta_circuit_probe_backoff_seconds or [300]
    cap = int(settings.meta_circuit_probe_max_delay_seconds)
    idx = max(0, attempt - 1)
    delay = schedule[-1] if idx >= len(schedule) else schedule[idx]
    return min(int(delay), cap)


def _probe_error_kind(exc: BaseException) -> tuple[str | None, str | None]:
    text = str(exc).lower()
    m = re.search(r"status=(\d{3})", text)
    if m:
        return "http", m.group(1)
    if "timeout" in text or "timed out" in text:
        return "timeout", None
    return "network", None


async def _resolve_probe_phone_number_id(
    *,
    session_factory: Callable[..., Any] | None = None,
) -> str | None:
    factory = session_factory if session_factory is not None else SessionLocal
    try:
        async with factory() as session:
            stmt = (
                select(WhatsAppSender.phone_number_id)
                .where(WhatsAppSender.is_active.is_(True))
                .order_by(WhatsAppSender.id.asc())
                .limit(1)
            )
            value = (await session.execute(stmt)).scalars().first()
            return str(value).strip() if value else None
    except Exception:
        logger.warning("meta guard: failed to resolve probe phone_number_id")
        return None


async def _probe_meta(
    provider: WhatsAppProvider,
    *,
    phone_number_id: str | None,
    timeout: float,
) -> ProbeResult:
    check = getattr(provider, "check_metadata", None)
    if check is None:
        return ProbeResult(ok=False, error_kind="probe_not_supported")
    if phone_number_id is None:
        return ProbeResult(ok=False, error_kind="missing_probe_sender")
    try:
        await check(phone_number_id, timeout=timeout)
        return ProbeResult(ok=True)
    except Exception as exc:
        kind, code = _probe_error_kind(exc)
        return ProbeResult(ok=False, error_kind=kind, error_code=code)


async def tick(
    provider: WhatsAppProvider,
    *,
    session_factory: Callable[..., Any] | None = None,
) -> str:
    state = await get_meta_circuit_state(session_factory=session_factory)
    if state.state == "open":
        return "open_idle"

    now = utcnow()
    if state.next_probe_at is not None and now < state.next_probe_at:
        return "waiting"

    probe_token = await mark_meta_circuit_probing(session_factory=session_factory)
    if probe_token is None:
        refreshed = await get_meta_circuit_state(session_factory=session_factory)
        return "probe_in_progress" if refreshed.state == "half_open" else "waiting"

    phone_number_id = await _resolve_probe_phone_number_id(session_factory=session_factory)
    provider_cls = type(provider).__name__
    result = await _probe_meta(
        provider,
        phone_number_id=phone_number_id,
        timeout=float(settings.meta_circuit_probe_timeout_seconds),
    )

    if result.ok:
        opened = await open_meta_circuit(
            session_factory=session_factory,
            reason="probe_succeeded",
            probe_token=probe_token,
        )
        if not opened:
            logger.warning("meta guard: stale successful probe ignored provider=%s", provider_cls)
            return "probe_stale"
        logger.warning(
            "meta guard: probe succeeded provider=%s phone_number_id=%s",
            provider_cls,
            phone_number_id,
        )
        return "opened"

    attempt = int(state.probe_attempts or 0) + 1
    delay = _probe_backoff_seconds(attempt)
    failed_attempt = await mark_meta_circuit_probe_failed(
        reason=f"probe_failed:{result.error_kind}" if result.error_kind else "probe_failed",
        next_probe_at=utcnow() + timedelta(seconds=delay),
        probe_token=probe_token,
        session_factory=session_factory,
    )
    if failed_attempt == 0:
        logger.warning("meta guard: stale failed probe ignored provider=%s", provider_cls)
        return "probe_stale"
    logger.warning(
        "meta guard: probe failed probe_attempt=%s delay_seconds=%s error_kind=%s "
        "error_code=%s provider=%s phone_number_id=%s",
        failed_attempt,
        delay,
        result.error_kind,
        result.error_code,
        provider_cls,
        phone_number_id,
    )
    return "stayed_closed"


async def run_loop(
    provider: WhatsAppProvider,
    *,
    poll_sec: float = 30.0,
    session_factory: Callable[..., Any] | None = None,
) -> None:
    logger.info("Meta guard worker started. poll=%ss", poll_sec)
    while True:
        if settings.meta_circuit_breaker_enabled:
            try:
                await tick(provider, session_factory=session_factory)
            except Exception:
                logger.warning("meta guard: tick failed; ignored")
        await asyncio.sleep(poll_sec)
