"""Where the consent section of a write transaction may sit (§46.4.3, §46.4.6).

Two properties carry this module, and both are about CALLERS rather than about
the helper they call.

The first is that a consent address lock is never held across an external
request. Both providers write a card early in their event transaction and then
make an HTTP call before they commit — Altegio looks up a service category,
EasyWeek proves the two-service snapshot — so a lock taken at the write site
would be held across that call, and a STOP for the same number could not even
have its receipt stored until the CRM answered. That is the one thing the
durable inbound boundary exists to prevent, so the tests here stall the stubbed
external response and then require the real ingress to commit anyway, under a
short diagnostic ``lock_timeout``.

The second is that the whole transaction takes its consent addresses in ONE
sorted acquisition, including the ones a nested callback contributes. Sorting
inside each helper call was not enough: the UUID resolver's nested client upsert
took the proven number while the resolver itself later asked for the old one.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy import text as sql_text

from altegio_bot import easyweek_uuid_identity as identity
from altegio_bot import service_filter
from altegio_bot.models.models import (
    INBOUND_AUTHENTICITY_SIGNED,
    AltegioEvent,
    Client,
    EasyWeekEvent,
    WhatsAppConsentSuppression,
    WhatsAppEvent,
    WhatsAppInboundReceipt,
    WhatsAppSender,
)
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import (
    TEST_CUSTOMER_ID,
    TEST_LOCATION_ID,
    booking_created,
    booking_created_multi_service,
)
from altegio_bot.tests.test_easyweek_uuid_identity_worker import CUSTOMER, PHONE, Reader
from altegio_bot.tests.test_easyweek_uuid_identity_worker import (
    configuration as configuration,  # noqa: PLC0414 - autouse fixture, imported on purpose
)
from altegio_bot.whatsapp_consent import (
    CONSENT_STATE_OPTED_OUT,
    final_send_guard,
    lock_consent_address,
    record_inbound_receipts,
)
from altegio_bot.workers import easyweek_inbox_worker as ew_worker
from altegio_bot.workers import inbox_worker as altegio_worker
from altegio_bot.workers import whatsapp_inbox_worker as wa_worker

LASH_COMPANY = 758285
ALTEGIO_CLIENT_ID = 7
ALTEGIO_PHONE = "+491600000099"
WA_PNID = "PNID-CARD-LOCKS"


class _CaptureProvider:
    """Counts what would have been sent. Never sends."""

    def __init__(self) -> None:
        self.sent: list[tuple[int, str, str]] = []

    async def send(self, sender_id: int, phone_e164: str, text: str, contact_name: str | None = None) -> str:
        self.sent.append((sender_id, phone_e164, text))
        return f"wamid.ACK{len(self.sent)}"

    async def send_template(self, *args: Any, **kwargs: Any) -> str:  # pragma: no cover - never reached
        raise AssertionError("an inbound command never sends a template")


def _marketing_verdict(card: Client) -> str | None:
    from altegio_bot.campaigns.segment import compute_excluded_reason

    return compute_excluded_reason(
        wa_opted_out=bool(card.wa_opted_out),
        phone_e164=card.phone_e164,
        count_before=0,
        lash_count=1,
        attended_lash_count=1,
    )


def _stop_payload(*addresses: str, epoch: int = 1_760_000_000) -> dict[str, Any]:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA-LOCKS",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {"phone_number_id": WA_PNID},
                            "messages": [
                                {
                                    "from": address.lstrip("+"),
                                    "id": f"wamid.LOCKS-{index}-{uuid.uuid4().hex[:6]}",
                                    "timestamp": str(epoch + index),
                                    "type": "text",
                                    "text": {"body": "STOP"},
                                }
                                for index, address in enumerate(addresses)
                            ],
                        },
                    }
                ],
            }
        ],
    }


async def _ingest_stop(session_maker, *addresses: str, lock_timeout_ms: int = 2000) -> int:
    """The real ingress half, under a short diagnostic lock timeout.

    ``record_inbound_receipts`` is what ``whatsapp_ingest`` calls in the same
    transaction as the stored event and before the 200 the sender sees. The
    timeout is a probe, not a setting: if any transaction were holding a consent
    lock for these addresses — across an HTTP request, say — this would fail
    instead of quietly waiting for a CRM round trip.
    """
    now = datetime.now(timezone.utc)
    payload = _stop_payload(*addresses)
    async with session_maker() as session:
        async with session.begin():
            await session.execute(sql_text(f"SET LOCAL lock_timeout = '{int(lock_timeout_ms)}ms'"))
            event = WhatsAppEvent(
                dedupe_key=f"wa:locks-{uuid.uuid4().hex}",
                status="received",
                payload=payload,
                inbound_authenticity=INBOUND_AUTHENTICITY_SIGNED,
                received_at=now,
            )
            session.add(event)
            await session.flush()
            await record_inbound_receipts(
                session,
                dedupe_key=str(event.dedupe_key),
                event_id=int(event.id),
                payload=payload,
                signature_verified=True,
                observed_at=now,
                allowed_phone_number_ids={WA_PNID},
            )
            return int(event.id)


async def _seed_sender(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            existing = await session.scalar(select(WhatsAppSender.id).where(WhatsAppSender.phone_number_id == WA_PNID))
            if existing is None:
                session.add(
                    WhatsAppSender(
                        company_id=1,
                        sender_code="card-locks",
                        phone_number_id=WA_PNID,
                        display_phone="+4930000011",
                        is_active=True,
                    )
                )


async def _process_whatsapp(session_maker, event_id: int, provider: Any) -> None:
    original = wa_worker.SessionLocal
    try:
        wa_worker.SessionLocal = session_maker  # type: ignore[assignment]
        await wa_worker.process_one_event(event_id, provider)
    finally:
        wa_worker.SessionLocal = original


async def _receipts(session_maker) -> list[WhatsAppInboundReceipt]:
    async with session_maker() as session:
        return list((await session.scalars(select(WhatsAppInboundReceipt).order_by(WhatsAppInboundReceipt.id))).all())


async def _wait_until_a_transaction_is_queued(session_maker, *, timeout: float = 30.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        async with session_maker() as probe:
            waiting = await probe.scalar(sql_text("SELECT count(*) FROM pg_locks WHERE NOT granted"))
        if int(waiting or 0) >= 1:
            return
        assert loop.time() < deadline, "nothing ever queued on a lock"
        await asyncio.sleep(0.05)


# ===========================================================================
# Review fix 3 — the Altegio event transaction
# ===========================================================================


async def _altegio_record_event(session_maker) -> int:
    async with session_maker() as session:
        async with session.begin():
            event = AltegioEvent(
                dedupe_key=f"altegio:locks-{uuid.uuid4().hex}",
                company_id=LASH_COMPANY,
                resource="record",
                resource_id=123456789,
                event_status="create",
                status="received",
                payload={
                    "company_id": LASH_COMPANY,
                    "resource": "record",
                    "status": "create",
                    "data": {
                        "id": 123456789,
                        "client": {
                            "id": ALTEGIO_CLIENT_ID,
                            "display_name": "Lock Probe",
                            "phone": ALTEGIO_PHONE,
                        },
                        "services": [{"id": 111, "title": "Allowed"}],
                        "date": "2026-05-20 12:00:00",
                        "staff_id": 5,
                    },
                },
            )
            session.add(event)
            await session.flush()
            return int(event.id)


async def _process_altegio(session_maker, event_id: int) -> None:
    original = altegio_worker.SessionLocal
    try:
        altegio_worker.SessionLocal = session_maker  # type: ignore[assignment]
        await altegio_worker.process_one_event(event_id)
    finally:
        altegio_worker.SessionLocal = original


@pytest.fixture
def _cold_service_cache():
    service_filter._LRU_CACHE.clear()
    yield
    service_filter._LRU_CACHE.clear()


async def test_a_stop_is_stored_while_the_altegio_service_lookup_is_stalled(
    session_maker, monkeypatch, _cold_service_cache
) -> None:
    """The Altegio window of review fix 3, with the real worker and a cold cache.

    ``handle_event(record:create)`` writes the card and then asks Altegio for the
    service category over HTTP. A consent lock taken at the write was still held
    at that point, so the ingress could not store a STOP for the same number
    until the CRM answered — the durable inbound boundary blocked by a service
    lookup. The card write now only NOTES the address; the consent section runs
    after every external call, just before the commit.
    """
    await _seed_sender(session_maker)
    stalled = asyncio.Event()
    release = asyncio.Event()

    async def _stalled_fetch(*, company_id: int, service_id: int) -> int:
        stalled.set()
        await release.wait()
        return sorted(service_filter.LASH_CATEGORY_IDS_BY_COMPANY[LASH_COMPANY])[0]

    monkeypatch.setattr(service_filter, "_fetch_service_category_id", _stalled_fetch)
    event_id = await _altegio_record_event(session_maker)

    worker = asyncio.create_task(_process_altegio(session_maker, event_id))
    await asyncio.wait_for(stalled.wait(), timeout=30)

    # The whole point: this must commit while the CRM request is still in
    # flight, and under a lock timeout that would fail if it had to wait.
    stop_event = await _ingest_stop(session_maker, ALTEGIO_PHONE)
    assert [receipt.wa_address for receipt in await _receipts(session_maker)] == [ALTEGIO_PHONE]

    release.set()
    await asyncio.wait_for(worker, timeout=60)

    async with session_maker() as session:
        card = (await session.execute(select(Client).where(Client.phone_e164 == ALTEGIO_PHONE))).scalar_one()
        event = await session.get(AltegioEvent, event_id)
    assert event.status == "processed"
    # Nothing has decided the refusal yet, so the card is untouched: the ledger
    # is the authority and the inbox worker is what records it.
    assert card.wa_opted_out is False

    await _process_whatsapp(session_maker, stop_event, _CaptureProvider())
    async with session_maker() as session:
        card = (await session.execute(select(Client).where(Client.phone_e164 == ALTEGIO_PHONE))).scalar_one()
        guard = await final_send_guard(session, phone=ALTEGIO_PHONE)
    assert card.wa_opted_out is True
    assert _marketing_verdict(card) == "opted_out"
    assert guard.allowed is False


async def test_a_failing_service_lookup_keeps_the_stored_stop(session_maker, monkeypatch, _cold_service_cache) -> None:
    """The external call fails: the event may fail with it, the STOP may not."""
    await _seed_sender(session_maker)
    stop_event = await _ingest_stop(session_maker, ALTEGIO_PHONE)

    async def _broken_fetch(*, company_id: int, service_id: int) -> int:
        raise RuntimeError("altegio is down")

    monkeypatch.setattr(service_filter, "_fetch_service_category_id", _broken_fetch)
    event_id = await _altegio_record_event(session_maker)
    await _process_altegio(session_maker, event_id)

    assert [receipt.wa_address for receipt in await _receipts(session_maker)] == [ALTEGIO_PHONE]
    await _process_whatsapp(session_maker, stop_event, _CaptureProvider())

    async with session_maker() as session:
        rows = list((await session.scalars(select(WhatsAppConsentSuppression))).all())
        guard = await final_send_guard(session, phone=ALTEGIO_PHONE)
    assert [row.state for row in rows] == [CONSENT_STATE_OPTED_OUT]
    assert guard.allowed is False


async def test_the_altegio_upsert_itself_holds_no_consent_lock(session_maker) -> None:
    """The write site takes no lock at all, which is why the caller can.

    Checked from another session while the writing transaction is still open: if
    ``upsert_client`` still took the address lock, this would time out.
    """
    opened = asyncio.Event()
    release = asyncio.Event()

    async def _writer() -> None:
        async with session_maker() as session:
            async with session.begin():
                await altegio_worker.upsert_client(
                    session,
                    LASH_COMPANY,
                    {"id": ALTEGIO_CLIENT_ID, "name": "Open", "phone": ALTEGIO_PHONE},
                )
                opened.set()
                await release.wait()

    writer = asyncio.create_task(_writer())
    await asyncio.wait_for(opened.wait(), timeout=30)
    async with session_maker() as probe:
        async with probe.begin():
            await probe.execute(sql_text("SET LOCAL lock_timeout = '2000ms'"))
            await lock_consent_address(probe, ALTEGIO_PHONE)
    release.set()
    await asyncio.wait_for(writer, timeout=30)


# ===========================================================================
# Review fix 3 — the EasyWeek claim transaction
# ===========================================================================


async def _capture(session_maker, payload: dict[str, Any]) -> int:
    async with session_maker() as session:
        async with session.begin():
            event = EasyWeekEvent(
                status="captured",
                event_hint="booking-created",
                auth_via="query",
                payload_hash=f"locks-{uuid.uuid4().hex}",
                payload=payload,
                body_truncated=False,
                booking_uuid=uuid.UUID(payload["uid"]),
            )
            session.add(event)
            await session.flush()
            return int(event.id)


async def test_a_stop_is_stored_while_the_easyweek_snapshot_is_stalled(session_maker, monkeypatch) -> None:
    """The EasyWeek window of review fix 3, with a real two-service booking.

    ``apply_booking`` writes the card and then proves the two-service snapshot
    over HTTP. Same shape, same consequence, same fix: the write notes the
    address and the transaction's consent section runs after the proof.
    """
    await _seed_sender(session_maker)
    monkeypatch.setattr(settings, "easyweek_multi_service_notifications_enabled", True)
    payload = booking_created_multi_service()
    payload["customer_phone"] = payload["customer_attributes.customer_phone"] = PHONE
    await _capture(session_maker, payload)

    stalled = asyncio.Event()
    release = asyncio.Event()

    async def _stalled_proof(*, client: Any, webhook: Any) -> Any:
        stalled.set()
        await release.wait()
        raise ew_worker.RecoverableCategoryConfigurationError("multi_service_api_unavailable")

    monkeypatch.setattr(ew_worker, "fetch_and_prove_exactly_two_service_snapshot", _stalled_proof)
    monkeypatch.setattr(ew_worker, "EasyWeekClient", lambda: object())

    worker = asyncio.create_task(ew_worker.process_one())
    await asyncio.wait_for(stalled.wait(), timeout=30)

    stop_event = await _ingest_stop(session_maker, PHONE)
    assert [receipt.wa_address for receipt in await _receipts(session_maker)] == [PHONE]

    release.set()
    await asyncio.wait_for(worker, timeout=60)

    await _process_whatsapp(session_maker, stop_event, _CaptureProvider())
    async with session_maker() as session:
        guard = await final_send_guard(session, phone=PHONE)
    assert guard.allowed is False


# ===========================================================================
# Review fix 2 — one sorted acquisition for the whole UUID transaction
# ===========================================================================


def _booking_of(payload: dict[str, Any]):
    registry = identity.configured_easyweek_locations()
    return ew_worker.normalize_event(
        event_hint="booking-created",
        payload=payload,
        body_truncated=False,
        location_registry=registry.locations,
    )


async def _stale_contact_setup(session_maker, monkeypatch, other: str) -> tuple[int, Any, Any]:
    """A captured booking whose contact is stale, and the real nested writer.

    The resolver's own transaction is the subject: it adopts the UUID, runs the
    event's business write — which is where a nested client upsert touches a
    card — and only then binds the refusal. The business write here is the real
    ``easyweek_inbox_worker.upsert_client`` rather than the whole booking
    pipeline, which needs configuration this module does not set up; everything
    that matters for the lock order is production code.
    """
    from dataclasses import replace

    monkeypatch.setattr(settings, "easyweek_processing_enabled", True)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Client(
                    provider="easyweek",
                    company_id=TEST_LOCATION_ID,
                    # A numeric binding the resolver can recognise, which is
                    # what makes it prove today's contact against the UUID and
                    # take its stale-contact branch.
                    altegio_client_id=TEST_CUSTOMER_ID,
                    easyweek_customer_uuid=uuid.UUID(CUSTOMER),
                    easyweek_identity_assigned_at=datetime.now(timezone.utc),
                    phone_e164=other,
                    display_name="Stale",
                    raw={},
                )
            )
    payload = booking_created()
    payload["customer_phone"] = payload["customer_attributes.customer_phone"] = other
    event_id = await _capture(session_maker, payload)
    booking = _booking_of(payload)

    async def _apply_stale_contact(session: Any, event: Any, phone: str) -> None:
        event.status = ew_worker.STATUS_PROCESSING
        await ew_worker.upsert_client(session, replace(booking, phone_e164=phone))

    return event_id, booking, _apply_stale_contact


def _booking_of(payload: dict[str, Any]):
    registry = identity.configured_easyweek_locations()
    return ew_worker.normalize_event(
        event_hint="booking-created",
        payload=payload,
        body_truncated=False,
        location_registry=registry.locations,
    )


@pytest.mark.parametrize("other", ["+4915100000001", "+4915100000999"])
async def test_the_stale_contact_transaction_takes_every_consent_address_at_once(
    session_maker, monkeypatch, other: str
) -> None:
    """Both mutual orders of the two addresses, through the real resolver.

    The resolver's transaction has two sets of consent addresses: the one its
    nested client upsert writes (the proven number) and the ones the refusal is
    bound to (the captured number among them). Taking them in two acquisitions
    is what deadlocked — an ingress holding the lower address and waiting for
    the higher one closed the cycle against a resolver holding the higher and
    asking for the lower.

    So the address that sorts FIRST is held here from a third transaction, and
    the resolver must be queued on it while holding NOTHING: that is what lets
    the holder go on to take the other one. ``other`` is parametrised either
    side of the proven number, so both orders are covered.
    """
    event_id, booking, apply_stale_contact = await _stale_contact_setup(session_maker, monkeypatch, other)
    first, second = sorted([PHONE, other])
    release = asyncio.Event()
    took_the_second = asyncio.Event()

    async def _holder() -> None:
        async with session_maker() as session:
            async with session.begin():
                await lock_consent_address(session, first)
                await release.wait()
                await asyncio.wait_for(lock_consent_address(session, second), timeout=20)
                took_the_second.set()

    holder = asyncio.create_task(_holder())
    await asyncio.sleep(0)
    resolver = asyncio.create_task(
        identity.resolve_webhook_identity(
            session_maker,
            event_id=event_id,
            booking=booking,
            reader=Reader(booking_created(), session_maker, check_lock=False),
            apply_stale_contact=apply_stale_contact,
        )
    )
    await _wait_until_a_transaction_is_queued(session_maker)
    release.set()
    await asyncio.wait_for(took_the_second.wait(), timeout=30)
    await asyncio.wait_for(holder, timeout=30)
    assert await asyncio.wait_for(resolver, timeout=60) is True

    async with session_maker() as session:
        cards = list(
            (
                await session.scalars(
                    select(Client).where(Client.easyweek_customer_uuid == uuid.UUID(CUSTOMER)).order_by(Client.id)
                )
            ).all()
        )
    assert [card.phone_e164 for card in cards] == [PHONE], "the proven contact really was adopted"


@pytest.mark.parametrize("other", ["+4915100000001", "+4915100000999"])
async def test_the_ingress_and_the_resolver_both_finish(session_maker, monkeypatch, other: str) -> None:
    """The same window with the real ingress as the competitor.

    Both sides sort the same full set, so one simply waits for the other: the
    receipts are durable, the adoption lands, and both refusals end up applying.
    """
    await _seed_sender(session_maker)
    event_id, booking, apply_stale_contact = await _stale_contact_setup(session_maker, monkeypatch, other)

    resolver = asyncio.create_task(
        identity.resolve_webhook_identity(
            session_maker,
            event_id=event_id,
            booking=booking,
            reader=Reader(booking_created(), session_maker, check_lock=False),
            apply_stale_contact=apply_stale_contact,
        )
    )
    ingress = asyncio.create_task(_ingest_stop(session_maker, PHONE, other, lock_timeout_ms=20000))
    assert await asyncio.wait_for(resolver, timeout=60) is True
    stop_event = await asyncio.wait_for(ingress, timeout=60)

    assert sorted(receipt.wa_address for receipt in await _receipts(session_maker)) == sorted([PHONE, other])
    await _process_whatsapp(session_maker, stop_event, _CaptureProvider())
    async with session_maker() as session:
        rows = sorted(str(row.wa_address) for row in (await session.scalars(select(WhatsAppConsentSuppression))).all())
        for address in (PHONE, other):
            guard = await final_send_guard(session, phone=address)
            assert guard.allowed is False, address
    assert rows == sorted([PHONE, other])
