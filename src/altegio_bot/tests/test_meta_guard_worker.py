from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest

from altegio_bot.models.models import WhatsAppSender
from altegio_bot.services import meta_circuit as mc
from altegio_bot.workers import meta_guard_worker as guard


class FakeProvider:
    def __init__(self, *, available: bool, fail_status: str = "500") -> None:
        self.available = available
        self.fail_status = fail_status
        self.metadata_calls: list[str] = []
        self.send_calls: list[Any] = []
        self.send_template_calls: list[Any] = []

    async def check_metadata(self, phone_number_id: str, *, timeout: float | None = None) -> None:
        self.metadata_calls.append(phone_number_id)
        if not self.available:
            raise RuntimeError(f"Meta metadata probe failed status={self.fail_status} code=2")

    async def send(self, *args: Any, **kwargs: Any) -> str:
        self.send_calls.append((args, kwargs))
        return "not-used"

    async def send_template(self, *args: Any, **kwargs: Any) -> str:
        self.send_template_calls.append((args, kwargs))
        return "not-used"


class NoProbeProvider:
    async def send(self, *args: Any, **kwargs: Any) -> str:
        return "not-used"

    async def send_template(self, *args: Any, **kwargs: Any) -> str:
        return "not-used"


class FakeChatwoot:
    async def aclose(self) -> None:
        return None


async def _seed_sender(session_maker: Any, phone_number_id: str = "PHONE_NUM_ID") -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(
                WhatsAppSender(
                    company_id=1,
                    sender_code="main",
                    phone_number_id=phone_number_id,
                    is_active=True,
                )
            )


async def _close_circuit_due_now(session_maker: Any) -> None:
    await mc.close_meta_circuit(
        session_factory=session_maker,
        reason="transient_send_error",
        error_kind="http",
        error_code="500",
        next_probe_at=mc._utcnow() - timedelta(seconds=1),
    )


@pytest.mark.asyncio
async def test_open_state_returns_open_idle_and_does_not_probe(session_maker: Any) -> None:
    await _seed_sender(session_maker)
    provider = FakeProvider(available=True)

    result = await guard.tick(provider, session_factory=session_maker)

    assert result == "open_idle"
    assert provider.metadata_calls == []


@pytest.mark.asyncio
async def test_closed_future_probe_returns_waiting(session_maker: Any) -> None:
    await _seed_sender(session_maker)
    await mc.close_meta_circuit(
        session_factory=session_maker,
        reason="transient_send_error",
        error_kind="http",
        error_code="500",
        next_probe_at=mc._utcnow() + timedelta(minutes=5),
    )

    result = await guard.tick(FakeProvider(available=True), session_factory=session_maker)

    assert result == "waiting"


@pytest.mark.asyncio
async def test_due_successful_probe_opens_circuit(session_maker: Any) -> None:
    await _seed_sender(session_maker)
    await _close_circuit_due_now(session_maker)
    provider = FakeProvider(available=True)

    result = await guard.tick(provider, session_factory=session_maker)

    assert result == "opened"
    assert provider.metadata_calls == ["PHONE_NUM_ID"]
    state = await mc.get_meta_circuit_state(session_factory=session_maker)
    assert state.state == "open"
    assert state.probe_token is None


@pytest.mark.asyncio
async def test_due_failed_probe_stays_closed(session_maker: Any) -> None:
    await _seed_sender(session_maker)
    await _close_circuit_due_now(session_maker)
    provider = FakeProvider(available=False)

    result = await guard.tick(provider, session_factory=session_maker)

    assert result == "stayed_closed"
    state = await mc.get_meta_circuit_state(session_factory=session_maker)
    assert state.state == "closed"
    assert state.probe_attempts == 1
    assert state.next_probe_at is not None


@pytest.mark.asyncio
async def test_provider_without_check_metadata_stays_closed(session_maker: Any) -> None:
    await _seed_sender(session_maker)
    await _close_circuit_due_now(session_maker)

    result = await guard.tick(NoProbeProvider(), session_factory=session_maker)  # type: ignore[arg-type]

    assert result == "stayed_closed"
    assert (await mc.get_meta_circuit_state(session_factory=session_maker)).state == "closed"


@pytest.mark.asyncio
async def test_missing_active_sender_stays_closed(session_maker: Any) -> None:
    await _close_circuit_due_now(session_maker)
    provider = FakeProvider(available=True)

    result = await guard.tick(provider, session_factory=session_maker)

    assert result == "stayed_closed"
    assert provider.metadata_calls == []
    assert (await mc.get_meta_circuit_state(session_factory=session_maker)).state == "closed"


@pytest.mark.asyncio
async def test_active_probe_lease_returns_probe_in_progress(session_maker: Any) -> None:
    await _seed_sender(session_maker)
    await _close_circuit_due_now(session_maker)
    token = await mc.mark_meta_circuit_probing(session_factory=session_maker)
    assert token is not None
    provider = FakeProvider(available=True)

    result = await guard.tick(provider, session_factory=session_maker)

    assert result == "probe_in_progress"
    assert provider.metadata_calls == []


@pytest.mark.asyncio
async def test_chatwoot_hybrid_delegates_metadata_probe(session_maker: Any) -> None:
    from altegio_bot.providers.chatwoot_hybrid import ChatwootHybridProvider

    await _seed_sender(session_maker)
    await _close_circuit_due_now(session_maker)
    primary = FakeProvider(available=True)
    hybrid = ChatwootHybridProvider(primary=primary, chatwoot=FakeChatwoot())  # type: ignore[arg-type]

    result = await guard.tick(hybrid, session_factory=session_maker)

    assert result == "opened"
    assert primary.metadata_calls == ["PHONE_NUM_ID"]
