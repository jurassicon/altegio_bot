"""PostgreSQL contract for the PR-12 send path: when a retention message goes out.

Every test runs against a real database with real rows, and every send goes
through a capture provider that records the call instead of performing it — no
HTTP request ever leaves the process, and every Altegio helper the EasyWeek path
must never touch is replaced by a landmine that fails the test if it is called.

What is proven here:

* the counter comparison decides the message, and only equality lets it through;
* a customer who has already booked again is not invited back, and the lookup
  that establishes that is provider- and branch-scoped;
* the send fence holds at the claim AND at the moment of processing, and a
  closed fence never spends an attempt;
* the message renders from EasyWeek's own branch-bound template row, with the
  branch's own booking page and the exact positional parameters the approved
  Meta template expects;
* nothing on the Altegio path changes.

The colliding ``company_id`` is deliberate: EasyWeek's ``company_id`` is the
numeric EasyWeek ``:location_id`` and shares an integer space with Altegio
company ids, so every provider-blind query is a cross-tenant leak waiting for
the two spaces to overlap.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_branches import BRANCH_PROFILES, BranchProfile, branch_template_contract
from altegio_bot.easyweek_policy import COMEBACK_3D, REPEAT_10D
from altegio_bot.easyweek_retention import (
    COMEBACK_DELAY,
    PAYLOAD_SOURCE_SERVICE_ID,
    REPEAT_DELAY,
    RETENTION_BOOKING_UUID_MISMATCH,
    RETENTION_CLIENT_RETURNED,
    RETENTION_CLIENT_UNSUBSCRIBED,
    RETENTION_COMEBACK_ALREADY_SENT,
    RETENTION_COUNTER_MISSING,
    RETENTION_COUNTER_REGRESSED,
    RETENTION_FUTURE_BOOKING,
    RETENTION_PROOF_VERSION_UNKNOWN,
    RETENTION_SERVICE_CHANGED,
    RETENTION_SERVICE_UNPROVEN,
    RETENTION_SOURCE_NOT_CANCELED,
    RETENTION_SOURCE_NOT_FINISHED,
    RETENTION_SOURCE_START_MISMATCH,
    comeback_job_payload,
    repeat_job_payload,
)
from altegio_bot.easyweek_service_category import (
    record_raw_with_service_category,
    record_raw_with_services_count,
)
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    Client,
    MessageJob,
    MessageTemplate,
    OutboxMessage,
    Record,
    RecordService,
    WhatsAppSender,
)
from altegio_bot.settings import settings
from altegio_bot.workers import outbox_worker as ow

pytestmark = pytest.mark.asyncio

# One numeric id, two tenants. That is the collision, not an accident.
COLLIDING_COMPANY_ID = 758285
OTHER_EASYWEEK_COMPANY_ID = 999002

STATIC_BOOKING_PAGE = "https://example.invalid/book"
BOOKING_PAGE_ALLOWED_HOSTS = "example.invalid"
BOOKING_HASH = "90000001"
VERIFIED_MANAGE_PAGE = f"https://eyw.me/r/{BOOKING_HASH}"

CLIENT_PHONE = "+491700000001"
BOOKING_UUID = uuid.UUID("11111111-2222-4333-8444-555555555555")
OTHER_BOOKING_UUID = uuid.UUID("11111111-2222-4333-8444-999999999999")
SERVICE_TITLE = "Wimpernverlängerung"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _pr12_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "whatsapp_send_mode", "template", raising=False)
    monkeypatch.setattr(settings, "bot_template_text_inside_24h_enabled", False, raising=False)
    monkeypatch.setattr(settings, "meta_circuit_breaker_enabled", False, raising=False)
    # The MASTER notification fence is explicitly OPEN here, and that is not a
    # detail: a retention job in the queue is still a customer message, so the
    # master gates sending as well as planning. A fixture that left it shut would
    # have every happy-path test below quietly proving that the master can be
    # bypassed — which is exactly the defect these tests now guard against.
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_retention_enabled", False, raising=False)
    # The PR-12 fence is OPEN in most of these tests: they are about what the
    # send path proves once it is allowed to run. The fences' own behaviour has
    # its own tests below, which close them explicitly.
    monkeypatch.setattr(settings, "easyweek_retention_send_enabled", True, raising=False)
    # No canary restriction by default; the canary tests set it explicitly.
    monkeypatch.setattr(settings, "easyweek_retention_canary_job_id", "", raising=False)
    monkeypatch.setattr(settings, "easyweek_default_language", "de", raising=False)
    monkeypatch.setattr(settings, "easyweek_booking_page_allowed_hosts", BOOKING_PAGE_ALLOWED_HOSTS, raising=False)
    monkeypatch.setattr(
        settings,
        "easyweek_allowed_service_categories",
        json.dumps([SERVICE_TITLE]),
        raising=False,
    )
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "colliding": {
                    "location_id": COLLIDING_COMPANY_ID,
                    "location_uuid": "cccccccc-dddd-4eee-8fff-000000000001",
                    "meta_template_prefix": "cc",
                    "booking_page_url": STATIC_BOOKING_PAGE,
                },
                "other": {
                    "location_id": OTHER_EASYWEEK_COMPANY_ID,
                    "location_uuid": "cccccccc-dddd-4eee-8fff-000000000002",
                    "meta_template_prefix": "ot",
                    "booking_page_url": STATIC_BOOKING_PAGE,
                },
            }
        ),
        raising=False,
    )
    for slug, prefix in (("colliding", "cc"), ("other", "ot")):
        monkeypatch.setitem(
            BRANCH_PROFILES,
            slug,
            BranchProfile(
                slug=slug,
                api_name=f"Synthetic {slug}",
                meta_template_prefix=prefix,
                content=BRANCH_PROFILES["durlach"].content,
            ),
        )


@pytest.fixture(autouse=True)
def _no_altegio(monkeypatch: pytest.MonkeyPatch) -> None:
    """Every Altegio helper the EasyWeek retention path must never reach.

    Replaced by landmines rather than merely "not asserted": the whole point of
    PR-12 is that this path has no Altegio client id to ask with, so a call would
    either explode in production or — far worse — answer for another salon.
    """
    for name in (
        "client_has_future_appointments",
        "count_attended_client_visits",
        "client_has_any_future_record",
        "_client_returned_since",
    ):
        monkeypatch.setattr(
            ow,
            name,
            AsyncMock(side_effect=AssertionError(f"EasyWeek retention must never call {name}")),
        )


class CaptureProvider:
    """Records what would have been sent. Performs no I/O whatsoever."""

    def __init__(self) -> None:
        self.template_calls: list[dict[str, Any]] = []
        self.text_calls: list[dict[str, Any]] = []


@pytest.fixture
def capture(monkeypatch: pytest.MonkeyPatch) -> CaptureProvider:
    cap = CaptureProvider()

    async def _fake_send_template(*args: Any, **kwargs: Any) -> tuple[str, None]:
        cap.template_calls.append(dict(kwargs))
        return "capture-msg-1", None

    async def _fake_send(*args: Any, **kwargs: Any) -> tuple[str, None]:
        cap.text_calls.append(dict(kwargs))
        return "capture-text-1", None

    monkeypatch.setattr(ow, "safe_send_template", _fake_send_template)
    monkeypatch.setattr(ow, "safe_send", _fake_send)
    return cap


@pytest_asyncio.fixture
async def db(session_maker: async_sessionmaker[AsyncSession]) -> AsyncSession:
    async with session_maker() as session:
        yield session


# ---------------------------------------------------------------------------
# Row builders
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def _seed_client(
    db: AsyncSession,
    *,
    company_id: int = COLLIDING_COMPANY_ID,
    provider: str = PROVIDER_EASYWEEK,
    visits_total: int | None = 4,
    altegio_client_id: int = 7300002,
    opted_out: bool = False,
) -> Client:
    client = Client(
        provider=provider,
        company_id=company_id,
        altegio_client_id=altegio_client_id,
        phone_e164=CLIENT_PHONE,
        display_name="Anna Müller",
        wa_opted_out=opted_out,
        easyweek_visits_total=visits_total if provider == PROVIDER_EASYWEEK else None,
        easyweek_visits_total_updated_at=_utcnow() if (provider == PROVIDER_EASYWEEK and visits_total) else None,
        raw={},
    )
    db.add(client)
    await db.flush()
    return client


async def _seed_record(
    db: AsyncSession,
    client: Client,
    *,
    company_id: int = COLLIDING_COMPANY_ID,
    provider: str = PROVIDER_EASYWEEK,
    starts_at: datetime | None = None,
    is_deleted: bool = False,
    booking_uuid: uuid.UUID | None = BOOKING_UUID,
    altegio_record_id: int = 4200001,
    services: tuple[tuple[int, str | None], ...] = ((11, SERVICE_TITLE),),
    services_count: int = 1,
) -> Record:
    record = Record(
        provider=provider,
        company_id=company_id,
        altegio_record_id=altegio_record_id,
        easyweek_booking_uuid=booking_uuid if provider == PROVIDER_EASYWEEK else None,
        easyweek_booking_hash_id=BOOKING_HASH if provider == PROVIDER_EASYWEEK else None,
        client_id=client.id,
        altegio_client_id=client.altegio_client_id,
        staff_name="Tanja",
        starts_at=starts_at if starts_at is not None else _utcnow() - timedelta(days=10),
        short_link=VERIFIED_MANAGE_PAGE if provider == PROVIDER_EASYWEEK else None,
        total_cost=Decimal("60.00"),
        is_deleted=is_deleted,
        raw=(
            record_raw_with_services_count(record_raw_with_service_category({}, SERVICE_TITLE), services_count)
            if provider == PROVIDER_EASYWEEK
            else {}
        ),
    )
    db.add(record)
    await db.flush()
    for service_id, title in services:
        db.add(
            RecordService(
                record_id=record.id,
                service_id=service_id,
                title=title,
                cost_to_pay=Decimal("60.00"),
                raw={},
            )
        )
    await db.flush()
    return record


async def _seed_template(
    db: AsyncSession,
    *,
    code: str,
    company_id: int = COLLIDING_COMPANY_ID,
    slug: str = "colliding",
) -> MessageTemplate:
    contract = branch_template_contract(BRANCH_PROFILES[slug], code)
    assert contract is not None
    template = MessageTemplate(
        provider=PROVIDER_EASYWEEK,
        company_id=company_id,
        code=code,
        language="de",
        body=contract.raw_body,
        meta_template_name=contract.meta_template_name,
        is_active=True,
    )
    db.add(template)
    await db.flush()
    return template


async def _seed_sender(db: AsyncSession, *, company_id: int = COLLIDING_COMPANY_ID) -> WhatsAppSender:
    sender = WhatsAppSender(
        provider=PROVIDER_EASYWEEK,
        company_id=company_id,
        sender_code="default",
        phone_number_id="pn-easyweek-1",
        is_active=True,
    )
    db.add(sender)
    await db.flush()
    return sender


async def _seed_repeat_job(
    db: AsyncSession,
    client: Client,
    record: Record,
    *,
    baseline: int = 4,
    payload: dict[str, Any] | None = None,
    run_at: datetime | None = None,
    dedupe_key: str = "easyweek_retention:repeat_10d:test",
    service_id: int = 11,
) -> MessageJob:
    assert record.starts_at is not None
    body = (
        payload
        if payload is not None
        else repeat_job_payload(
            booking_uuid=BOOKING_UUID,
            company_id=record.company_id,
            starts_at=record.starts_at,
            visits_baseline=baseline,
            service_id=service_id,
        )
    )
    job = MessageJob(
        provider=PROVIDER_EASYWEEK,
        company_id=record.company_id,
        record_id=record.id,
        client_id=client.id,
        job_type=REPEAT_10D,
        run_at=run_at if run_at is not None else record.starts_at + REPEAT_DELAY,
        status="queued",
        dedupe_key=dedupe_key,
        payload=body,
    )
    db.add(job)
    await db.flush()
    return job


async def _seed_comeback_job(
    db: AsyncSession,
    client: Client,
    record: Record,
    *,
    baseline: int = 4,
    cancelled_at: datetime | None = None,
    payload: dict[str, Any] | None = None,
    dedupe_key: str = "easyweek_retention:comeback_3d:test",
) -> MessageJob:
    moment = cancelled_at if cancelled_at is not None else _utcnow() - COMEBACK_DELAY
    body = (
        payload
        if payload is not None
        else comeback_job_payload(
            booking_uuid=BOOKING_UUID,
            company_id=record.company_id,
            cancelled_at=moment,
            visits_baseline=baseline,
        )
    )
    job = MessageJob(
        provider=PROVIDER_EASYWEEK,
        company_id=record.company_id,
        record_id=record.id,
        client_id=client.id,
        job_type=COMEBACK_3D,
        run_at=moment + COMEBACK_DELAY,
        status="queued",
        dedupe_key=dedupe_key,
        payload=body,
    )
    db.add(job)
    await db.flush()
    return job


async def _run_job(db: AsyncSession, job: MessageJob) -> MessageJob:
    await ow.process_job_in_session(db, job.id, object())  # type: ignore[arg-type]
    await db.flush()
    await db.refresh(job)
    return job


async def _outbox_rows(db: AsyncSession, job: MessageJob) -> list[OutboxMessage]:
    return list((await db.execute(select(OutboxMessage).where(OutboxMessage.job_id == job.id))).scalars().all())


async def _ready_repeat(db: AsyncSession, **client_kwargs: Any) -> tuple[Client, Record, MessageJob]:
    """Everything a repeat needs to reach the provider."""
    client = await _seed_client(db, **client_kwargs)
    record = await _seed_record(db, client)
    await _seed_template(db, code=REPEAT_10D)
    await _seed_sender(db)
    job = await _seed_repeat_job(db, client, record, baseline=client.easyweek_visits_total or 4)
    return client, record, job


async def _ready_comeback(db: AsyncSession, **client_kwargs: Any) -> tuple[Client, Record, MessageJob]:
    client = await _seed_client(db, **client_kwargs)
    record = await _seed_record(db, client, is_deleted=True, starts_at=_utcnow() + timedelta(days=1))
    await _seed_template(db, code=COMEBACK_3D)
    await _seed_sender(db)
    job = await _seed_comeback_job(db, client, record, baseline=client.easyweek_visits_total or 4)
    return client, record, job


def _refusal(job: MessageJob) -> str:
    assert job.last_error is not None
    assert job.last_error.startswith(f"{ow.EASYWEEK_RETENTION_REFUSED}: "), job.last_error
    return job.last_error.split(": ", 1)[1]


# ===========================================================================
# The happy path, and what it proves
# ===========================================================================


async def test_a_repeat_sends_from_its_own_branch_template_and_page(db: AsyncSession, capture) -> None:
    _client, _record, job = await _ready_repeat(db)

    await _run_job(db, job)

    assert job.status == "done"
    assert len(capture.template_calls) == 1
    call = capture.template_calls[0]
    assert call["template_name"] == "kitilash_cc_repeat_10d_v1"
    # Exactly three positional parameters, in the approved order.
    assert call["params"] == ["Anna Müller", SERVICE_TITLE, STATIC_BOOKING_PAGE]
    assert call["tenant_provider"] == PROVIDER_EASYWEEK
    sender = (
        (
            await db.execute(
                select(WhatsAppSender)
                .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
                .where(WhatsAppSender.company_id == COLLIDING_COMPANY_ID)
            )
        )
        .scalars()
        .one()
    )
    assert call["sender_id"] == sender.id
    rows = await _outbox_rows(db, job)
    assert [row.status for row in rows] == ["sent"]
    assert VERIFIED_MANAGE_PAGE not in rows[0].body, "a finished visit's manage link is never the call to action"


async def test_a_comeback_sends_two_parameters_and_no_service(db: AsyncSession, capture) -> None:
    _client, _record, job = await _ready_comeback(db)

    await _run_job(db, job)

    assert job.status == "done"
    call = capture.template_calls[0]
    assert call["template_name"] == "kitilash_cc_comeback_3d_v1"
    assert call["params"] == ["Anna Müller", STATIC_BOOKING_PAGE]
    assert SERVICE_TITLE not in call["params"], "a cancelled booking has no service to promise"


# ===========================================================================
# The counter comparison
# ===========================================================================


async def test_an_equal_counter_lets_the_message_through(db: AsyncSession, capture) -> None:
    _client, _record, job = await _ready_repeat(db, visits_total=4)

    await _run_job(db, job)

    assert job.status == "done"


async def test_a_higher_counter_suppresses_the_message(db: AsyncSession, capture) -> None:
    """The customer already completed another visit. Nothing to invite them to."""
    client, _record, job = await _ready_repeat(db, visits_total=4)
    client.easyweek_visits_total = 5
    await db.flush()

    await _run_job(db, job)

    assert job.status == "canceled"
    assert _refusal(job) == RETENTION_CLIENT_RETURNED
    assert capture.template_calls == []
    assert await _outbox_rows(db, job) == []


async def test_a_lower_counter_blocks_the_message(db: AsyncSession, capture) -> None:
    """A counter that went backwards contradicts PR-11's monotonic snapshot."""
    client, _record, job = await _ready_repeat(db, visits_total=4)
    client.easyweek_visits_total = 3
    await db.flush()

    await _run_job(db, job)

    assert job.status == "canceled"
    assert _refusal(job) == RETENTION_COUNTER_REGRESSED
    assert capture.template_calls == []


async def test_a_missing_counter_blocks_the_message(db: AsyncSession, capture) -> None:
    """Absent is not zero, and zero would mean "new customer"."""
    client, _record, job = await _ready_repeat(db, visits_total=4)
    client.easyweek_visits_total = None
    client.easyweek_visits_total_updated_at = None
    await db.flush()

    await _run_job(db, job)

    assert job.status == "canceled"
    assert _refusal(job) == RETENTION_COUNTER_MISSING
    assert capture.template_calls == []


async def test_a_foreign_tenants_counter_is_never_read(db: AsyncSession, capture) -> None:
    """Another provider's row with the same numeric ids must not answer.

    The Altegio client here shares the company id and the external client id, and
    carries no EasyWeek counter at all — a provider-blind re-read would find it
    and refuse (or, with the columns swapped, send) on someone else's data.
    """
    await _seed_client(db, provider=PROVIDER_ALTEGIO, visits_total=None)
    _client, _record, job = await _ready_repeat(db, visits_total=4)

    await _run_job(db, job)

    assert job.status == "done", "the EasyWeek row is the one that answers"


# ===========================================================================
# Return / future booking
# ===========================================================================


async def test_a_future_easyweek_booking_suppresses_a_repeat(db: AsyncSession, capture) -> None:
    client, _record, job = await _ready_repeat(db)
    await _seed_record(
        db,
        client,
        starts_at=_utcnow() + timedelta(days=3),
        booking_uuid=OTHER_BOOKING_UUID,
        altegio_record_id=4200002,
    )

    await _run_job(db, job)

    assert job.status == "canceled"
    assert _refusal(job) == RETENTION_FUTURE_BOOKING
    assert capture.template_calls == []


async def test_a_future_easyweek_booking_suppresses_a_comeback(db: AsyncSession, capture) -> None:
    client, _record, job = await _ready_comeback(db)
    await _seed_record(
        db,
        client,
        starts_at=_utcnow() + timedelta(days=3),
        booking_uuid=OTHER_BOOKING_UUID,
        altegio_record_id=4200002,
    )

    await _run_job(db, job)

    assert job.status == "canceled"
    assert _refusal(job) == RETENTION_FUTURE_BOOKING


async def test_a_cancelled_future_record_does_not_suppress(db: AsyncSession, capture) -> None:
    """A booking the customer called off is not a booking they have."""
    client, _record, job = await _ready_repeat(db)
    await _seed_record(
        db,
        client,
        starts_at=_utcnow() + timedelta(days=3),
        booking_uuid=OTHER_BOOKING_UUID,
        altegio_record_id=4200002,
        is_deleted=True,
    )

    await _run_job(db, job)

    assert job.status == "done"


async def test_an_altegio_record_with_the_same_numeric_ids_does_not_suppress(db: AsyncSession, capture) -> None:
    """THE collision test: same company id, same client id, different CRM."""
    client, _record, job = await _ready_repeat(db)
    altegio_client = await _seed_client(db, provider=PROVIDER_ALTEGIO, visits_total=None)
    assert altegio_client.company_id == client.company_id
    assert altegio_client.altegio_client_id == client.altegio_client_id
    await _seed_record(
        db,
        altegio_client,
        provider=PROVIDER_ALTEGIO,
        starts_at=_utcnow() + timedelta(days=3),
        altegio_record_id=999123,
    )

    await _run_job(db, job)

    assert job.status == "done", "an Altegio booking is not this customer's EasyWeek booking"


async def test_another_easyweek_branch_does_not_leak_into_the_lookup(db: AsyncSession, capture) -> None:
    client, _record, job = await _ready_repeat(db)
    other_branch_client = await _seed_client(db, company_id=OTHER_EASYWEEK_COMPANY_ID)
    assert other_branch_client.altegio_client_id == client.altegio_client_id
    await _seed_record(
        db,
        other_branch_client,
        company_id=OTHER_EASYWEEK_COMPANY_ID,
        starts_at=_utcnow() + timedelta(days=3),
        booking_uuid=OTHER_BOOKING_UUID,
        altegio_record_id=4200003,
    )

    await _run_job(db, job)

    assert job.status == "done", "another branch's booking is another branch's business"


async def test_the_source_booking_itself_never_suppresses_its_own_repeat(db: AsyncSession, capture) -> None:
    """A repeat is planned from a booking; it must not be cancelled by it."""
    _client, record, job = await _ready_repeat(db)
    # The source moved into the future without moving its start instant would be
    # a mismatch; here it stays exactly where the payload froze it.
    assert record.starts_at is not None

    await _run_job(db, job)

    assert job.status == "done"


# ===========================================================================
# The source booking's own state
# ===========================================================================


async def test_a_cancelled_source_blocks_a_repeat(db: AsyncSession, capture) -> None:
    _client, record, job = await _ready_repeat(db)
    record.is_deleted = True
    await db.flush()

    await _run_job(db, job)

    assert job.status == "canceled"
    assert _refusal(job) == RETENTION_SOURCE_NOT_FINISHED


async def test_a_restored_source_blocks_a_comeback(db: AsyncSession, capture) -> None:
    """The cancellation was undone; the invitation no longer applies."""
    _client, record, job = await _ready_comeback(db)
    record.is_deleted = False
    await db.flush()

    await _run_job(db, job)

    assert job.status == "canceled"
    assert _refusal(job) == RETENTION_SOURCE_NOT_CANCELED
    assert capture.template_calls == []


async def test_a_moved_appointment_blocks_its_repeat(db: AsyncSession, capture) -> None:
    _client, record, job = await _ready_repeat(db)
    record.starts_at = record.starts_at + timedelta(hours=2)
    await db.flush()

    await _run_job(db, job)

    assert job.status == "canceled"
    assert _refusal(job) == RETENTION_SOURCE_START_MISMATCH


async def test_a_booking_uuid_that_no_longer_matches_blocks_the_send(db: AsyncSession, capture) -> None:
    _client, record, job = await _ready_repeat(db)
    record.easyweek_booking_uuid = OTHER_BOOKING_UUID
    await db.flush()

    await _run_job(db, job)

    assert job.status == "canceled"
    assert _refusal(job) == RETENTION_BOOKING_UUID_MISMATCH


async def test_an_ambiguous_service_snapshot_blocks_a_repeat(db: AsyncSession, capture) -> None:
    """`repeat_10d` prints one service name; two services means none is right.

    Refused by the PR-7.1 category fence, which runs first for every EasyWeek
    customer job and already treats an ambiguous multi-service booking as
    ineligible. The retention guard carries its own single-service proof for the
    cases that fence does not reach — see the blank-title test below — but this
    one is caught earlier, and that is the correct outcome rather than a gap.
    """
    client = await _seed_client(db)
    record = await _seed_record(
        db,
        client,
        services=((11, SERVICE_TITLE), (12, "Second Service")),
        services_count=2,
    )
    await _seed_template(db, code=REPEAT_10D)
    await _seed_sender(db)
    job = await _seed_repeat_job(db, client, record)

    await _run_job(db, job)

    assert job.status == "canceled"
    assert job.last_error == "category_ambiguous_multi_service"
    assert capture.template_calls == []


async def test_a_blank_service_title_blocks_a_repeat_before_meta(db: AsyncSession, capture) -> None:
    """Caught here rather than by the param preflight, which runs after an attempt."""
    client = await _seed_client(db)
    record = await _seed_record(db, client, services=((11, None),))
    await _seed_template(db, code=REPEAT_10D)
    await _seed_sender(db)
    job = await _seed_repeat_job(db, client, record)

    await _run_job(db, job)

    assert job.status == "canceled"
    assert _refusal(job) == RETENTION_SERVICE_UNPROVEN
    assert job.attempts == 0, "a local refusal must not spend a Meta attempt"


async def test_an_unknown_proof_version_blocks_the_send(db: AsyncSession, capture) -> None:
    client = await _seed_client(db)
    record = await _seed_record(db, client)
    await _seed_template(db, code=REPEAT_10D)
    await _seed_sender(db)
    assert record.starts_at is not None
    payload = repeat_job_payload(
        booking_uuid=BOOKING_UUID,
        company_id=record.company_id,
        starts_at=record.starts_at,
        visits_baseline=4,
        service_id=11,
    )
    payload["proof_version"] = 99
    job = await _seed_repeat_job(db, client, record, payload=payload)

    await _run_job(db, job)

    assert job.status == "canceled"
    assert _refusal(job) == RETENTION_PROOF_VERSION_UNKNOWN


# ===========================================================================
# Existing marketing fences
# ===========================================================================


async def test_an_opted_out_client_is_never_sent_to(db: AsyncSession, capture) -> None:
    _client, _record, job = await _ready_repeat(db, opted_out=True)

    await _run_job(db, job)

    assert job.status == "canceled"
    assert capture.template_calls == []


async def test_the_retention_guard_also_refuses_an_opted_out_client(db: AsyncSession) -> None:
    """Defence in depth: the guard does not rely on the shared marketing check."""
    client, record, job = await _ready_repeat(db, opted_out=True)

    reason = await ow._easyweek_retention_presend_error(db, job, record, client)

    assert reason == RETENTION_CLIENT_UNSUBSCRIBED


async def test_a_recent_comeback_suppresses_the_next_one(db: AsyncSession, capture) -> None:
    client, record, job = await _ready_comeback(db)
    db.add(
        OutboxMessage(
            company_id=record.company_id,
            client_id=client.id,
            record_id=record.id,
            job_id=None,
            sender_id=None,
            phone_e164=CLIENT_PHONE,
            template_code=COMEBACK_3D,
            language="de",
            body="",
            status="delivered",
            scheduled_at=_utcnow() - timedelta(days=5),
            sent_at=_utcnow() - timedelta(days=5),
            meta={},
        )
    )
    await db.flush()

    await _run_job(db, job)

    assert job.status == "canceled"
    assert _refusal(job) == RETENTION_COMEBACK_ALREADY_SENT


async def test_a_comeback_outside_the_window_does_not_suppress(db: AsyncSession, capture) -> None:
    client, record, job = await _ready_comeback(db)
    db.add(
        OutboxMessage(
            company_id=record.company_id,
            client_id=client.id,
            record_id=record.id,
            job_id=None,
            sender_id=None,
            phone_e164=CLIENT_PHONE,
            template_code=COMEBACK_3D,
            language="de",
            body="",
            status="delivered",
            scheduled_at=_utcnow() - timedelta(days=90),
            sent_at=_utcnow() - timedelta(days=90),
            meta={},
        )
    )
    await db.flush()

    await _run_job(db, job)

    assert job.status == "done"


async def test_repeated_131026_failures_still_suppress(db: AsyncSession, capture, monkeypatch) -> None:
    """The existing marketing suppression is unchanged for these job types."""
    monkeypatch.setattr(settings, "wa_131026_suppression_enabled", True, raising=False)
    monkeypatch.setattr(settings, "wa_131026_suppression_threshold", 1, raising=False)
    monkeypatch.setattr(settings, "wa_131026_suppression_window_days", 14, raising=False)
    monkeypatch.setattr(ow, "_count_131026_failures", AsyncMock(return_value=3))
    _client, _record, job = await _ready_repeat(db)

    await _run_job(db, job)

    assert job.status == "canceled"
    assert capture.template_calls == []
    rows = await _outbox_rows(db, job)
    assert [row.status for row in rows] == ["canceled"]
    assert "suppressed_131026" in (rows[0].error or "")


async def test_a_stale_backlog_is_refused_by_the_marketing_deadline(db: AsyncSession, capture) -> None:
    """Opening the fence must not release a queue that went stale behind it."""
    client = await _seed_client(db)
    record = await _seed_record(db, client, starts_at=_utcnow() - timedelta(days=20))
    await _seed_template(db, code=REPEAT_10D)
    await _seed_sender(db)
    job = await _seed_repeat_job(db, client, record, run_at=_utcnow() - timedelta(days=10))

    await _run_job(db, job)

    assert job.status == "canceled"
    assert "Retry deadline exceeded" in (job.last_error or "")
    assert capture.template_calls == []


async def test_a_recoverable_category_outage_spends_no_meta_attempt(db: AsyncSession, capture, monkeypatch) -> None:
    """A broken allowlist is an inability to decide, not a decision to cancel."""
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", "{not json", raising=False)
    _client, _record, job = await _ready_repeat(db)

    await _run_job(db, job)

    assert job.status == "queued", "the job waits for the configuration to be fixed"
    assert job.attempts == 0
    assert capture.template_calls == []
    assert await _outbox_rows(db, job) == []


# ===========================================================================
# The send fence
# ===========================================================================


async def test_a_closed_fence_never_claims_a_retention_job(db: AsyncSession, monkeypatch) -> None:
    client = await _seed_client(db)
    record = await _seed_record(db, client)
    job = await _seed_repeat_job(db, client, record, run_at=_utcnow() - timedelta(minutes=1))
    monkeypatch.setattr(settings, "easyweek_retention_send_enabled", False, raising=False)

    claimed = await ow._lock_next_jobs(db, 10)

    assert [row.id for row in claimed] == []
    await db.refresh(job)
    assert job.status == "queued"
    assert job.attempts == 0


async def test_an_open_fence_claims_it(db: AsyncSession) -> None:
    client = await _seed_client(db)
    record = await _seed_record(db, client)
    job = await _seed_repeat_job(db, client, record, run_at=_utcnow() - timedelta(minutes=1))

    claimed = await ow._lock_next_jobs(db, 10)

    assert [row.id for row in claimed] == [job.id]


async def test_the_fence_closing_after_the_claim_still_blocks_the_send(db: AsyncSession, capture, monkeypatch) -> None:
    """The race an operator can actually cause: claim, then close the fence."""
    _client, _record, job = await _ready_repeat(db)
    job.status = "processing"
    job.locked_at = _utcnow()
    await db.flush()
    monkeypatch.setattr(settings, "easyweek_retention_send_enabled", False, raising=False)

    await _run_job(db, job)

    assert job.status == "queued", "handed back, not cancelled"
    assert job.attempts == 0, "and not charged for a decision nobody made"
    assert job.locked_at is None
    assert capture.template_calls == []
    assert await _outbox_rows(db, job) == []


async def test_the_fence_does_not_touch_altegio_retention(db: AsyncSession, monkeypatch) -> None:
    """Same job type, different provider — and the Altegio queue keeps flowing."""
    monkeypatch.setattr(settings, "easyweek_retention_send_enabled", False, raising=False)
    client = await _seed_client(db, provider=PROVIDER_ALTEGIO, visits_total=None)
    record = await _seed_record(db, client, provider=PROVIDER_ALTEGIO)
    job = MessageJob(
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        record_id=record.id,
        client_id=client.id,
        job_type=REPEAT_10D,
        run_at=_utcnow() - timedelta(minutes=1),
        status="queued",
        dedupe_key="altegio-repeat-1",
        payload={},
    )
    db.add(job)
    await db.flush()

    claimed = await ow._lock_next_jobs(db, 10)

    assert job.id in [row.id for row in claimed]


# ===========================================================================
# Provider isolation
# ===========================================================================


async def test_the_send_needs_no_altegio_client_id(db: AsyncSession, capture) -> None:
    """Altegio's own repeat cancels without one; EasyWeek's must not need it."""
    client, record, job = await _ready_repeat(db)
    record.altegio_client_id = None
    await db.flush()

    await _run_job(db, job)

    assert job.status == "done"


async def test_the_easyweek_template_row_is_the_only_source_of_the_meta_name(db: AsyncSession, capture) -> None:
    """An Altegio row for the same company and code must not be reachable."""
    client, record, job = await _ready_repeat(db)
    db.add(
        MessageTemplate(
            provider=PROVIDER_ALTEGIO,
            company_id=record.company_id,
            code=REPEAT_10D,
            language="de",
            body="ALTEGIO REPEAT BODY",
            meta_template_name="kitilash_ka_repeat_10d_v1",
            is_active=True,
        )
    )
    await db.flush()

    await _run_job(db, job)

    call = capture.template_calls[0]
    assert call["template_name"] == "kitilash_cc_repeat_10d_v1"
    assert "ALTEGIO REPEAT BODY" not in (await _outbox_rows(db, job))[0].body


async def test_a_missing_easyweek_sender_fails_closed(db: AsyncSession, capture) -> None:
    client = await _seed_client(db)
    record = await _seed_record(db, client)
    await _seed_template(db, code=REPEAT_10D)
    db.add(
        WhatsAppSender(
            provider=PROVIDER_ALTEGIO,
            company_id=record.company_id,
            sender_code="default",
            phone_number_id="pn-altegio-1",
            is_active=True,
        )
    )
    await db.flush()
    job = await _seed_repeat_job(db, client, record)

    await _run_job(db, job)

    assert job.status == "failed"
    assert capture.template_calls == []


async def test_a_branch_removed_from_the_registry_stops_sending(db: AsyncSession, capture, monkeypatch) -> None:
    _client, _record, job = await _ready_repeat(db)
    monkeypatch.setattr(settings, "easyweek_location_map", "{}", raising=False)

    await _run_job(db, job)

    assert job.status in ("failed", "canceled")
    assert capture.template_calls == []


# ===========================================================================
# The master notification fence (P1)
# ===========================================================================


async def test_a_closed_master_fence_never_claims_a_retention_job(db: AsyncSession, monkeypatch) -> None:
    """A queued retention job is still a customer message.

    The master fence gated PLANNING from the start; it did not gate SENDING, so a
    queue planned while it was open could be released after it was shut. That is
    the one state an operator reaches by pausing outbound messaging.
    """
    client = await _seed_client(db)
    record = await _seed_record(db, client)
    job = await _seed_repeat_job(db, client, record, run_at=_utcnow() - timedelta(minutes=1))
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)

    claimed = await ow._lock_next_jobs(db, 10)

    assert [row.id for row in claimed] == []
    await db.refresh(job)
    assert job.status == "queued"
    assert job.attempts == 0
    assert job.locked_at is None


async def test_the_master_fence_closing_after_the_claim_still_blocks_the_send(
    db: AsyncSession, capture, monkeypatch
) -> None:
    """The race an operator can actually cause: claim, then pause messaging."""
    _client, _record, job = await _ready_repeat(db)
    original_run_at = job.run_at
    original_payload = dict(job.payload)
    job.status = "processing"
    job.locked_at = _utcnow()
    await db.flush()
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)

    await _run_job(db, job)

    assert job.status == "queued", "handed back, not cancelled"
    assert job.attempts == 0, "and not charged for a decision nobody made"
    assert job.locked_at is None
    assert job.run_at == original_run_at
    assert dict(job.payload) == original_payload
    assert capture.template_calls == []
    assert await _outbox_rows(db, job) == []


async def test_the_master_fence_does_not_change_other_easyweek_or_altegio_jobs(db: AsyncSession, monkeypatch) -> None:
    """Scoped to retention: the master fence's effect on other paths is unchanged.

    `record_created` and Altegio jobs are claimed exactly as before — their own
    gates decide what happens to them, and PR-12 must not become a second,
    wider fence over paths it does not own.
    """
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)
    client = await _seed_client(db)
    record = await _seed_record(db, client)
    lifecycle = MessageJob(
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        record_id=record.id,
        client_id=client.id,
        job_type="record_created",
        run_at=_utcnow() - timedelta(minutes=1),
        status="queued",
        dedupe_key="easyweek-lifecycle-1",
        payload={},
    )
    altegio_client = await _seed_client(db, provider=PROVIDER_ALTEGIO, visits_total=None)
    altegio_record = await _seed_record(db, altegio_client, provider=PROVIDER_ALTEGIO, altegio_record_id=999123)
    altegio = MessageJob(
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        record_id=altegio_record.id,
        client_id=altegio_client.id,
        job_type=REPEAT_10D,
        run_at=_utcnow() - timedelta(minutes=1),
        status="queued",
        dedupe_key="altegio-repeat-1",
        payload={},
    )
    db.add_all([lifecycle, altegio])
    await db.flush()

    claimed = {row.id for row in await ow._lock_next_jobs(db, 10)}

    assert lifecycle.id in claimed
    assert altegio.id in claimed


# ===========================================================================
# The controlled canary (P1)
# ===========================================================================


def _canary(monkeypatch, value: object) -> None:
    monkeypatch.setattr(settings, "easyweek_retention_canary_job_id", value, raising=False)


async def test_the_canary_lets_exactly_one_due_job_reach_meta(db: AsyncSession, capture, monkeypatch) -> None:
    """THE canary property: two due jobs, one message, and nothing else moves.

    "The queue happens to hold one job" is not a controlled canary — the queue
    can grow between the preflight and the fence opening. This proves the
    restriction is mechanical.
    """
    chosen_client, chosen_record, chosen = await _ready_repeat(db)
    other_client = await _seed_client(db, altegio_client_id=7300099)
    other_record = await _seed_record(
        db,
        other_client,
        starts_at=_utcnow() - timedelta(days=10),
        booking_uuid=OTHER_BOOKING_UUID,
        altegio_record_id=4200055,
    )
    other = await _seed_repeat_job(
        db,
        other_client,
        other_record,
        dedupe_key="easyweek_retention:repeat_10d:other",
    )
    # The other job's payload names its own booking, so it would be perfectly
    # sendable on its own — the canary is the only thing holding it.
    other.payload = dict(other.payload, booking_uuid=str(OTHER_BOOKING_UUID))
    await db.flush()
    assert chosen_record.id != other_record.id

    _canary(monkeypatch, str(chosen.id))

    claimed = await ow._lock_next_jobs(db, 10)
    assert [row.id for row in claimed] == [chosen.id], "only the named job is claimed"

    for row in claimed:
        await _run_job(db, row)

    assert len(capture.template_calls) == 1
    await db.refresh(chosen)
    await db.refresh(other)
    assert chosen.status == "done"
    # The untouched job keeps everything the preflight is meant to inspect.
    assert other.status == "queued"
    assert other.attempts == 0
    assert other.locked_at is None
    assert await _outbox_rows(db, other) == []


async def test_the_canary_blocks_a_non_canary_job_that_slipped_past_the_claim(
    db: AsyncSession, capture, monkeypatch
) -> None:
    """Checked before Meta as well as at the claim, for the same race as the fence."""
    _client, _record, job = await _ready_repeat(db)
    job.status = "processing"
    job.locked_at = _utcnow()
    await db.flush()
    _canary(monkeypatch, str(job.id + 1000))

    await _run_job(db, job)

    assert job.status == "queued"
    assert job.attempts == 0
    assert job.locked_at is None
    assert capture.template_calls == []


# Whitespace-only is deliberately NOT here: an env variable set to spaces is
# "unset" in every other reading of the file, and `test_an_empty_canary_...`
# below pins that. These are values an operator meant as an id and got wrong.
@pytest.mark.parametrize("bad", ["0", "-3", "abc", "12.5", "١٢", "+7", "1 2"])
async def test_an_invalid_canary_fails_closed(db: AsyncSession, capture, monkeypatch, bad: str) -> None:
    """A typo must never read as "no restriction" and release the whole queue."""
    _client, _record, job = await _ready_repeat(db)
    _canary(monkeypatch, bad)

    claimed = await ow._lock_next_jobs(db, 10)
    assert [row.id for row in claimed] == []

    job.status = "processing"
    job.locked_at = _utcnow()
    await db.flush()
    await _run_job(db, job)

    assert job.status == "queued"
    assert job.attempts == 0
    assert capture.template_calls == []


async def test_an_empty_canary_is_ordinary_bulk_behaviour(db: AsyncSession, capture, monkeypatch) -> None:
    _client, _record, job = await _ready_repeat(db)
    _canary(monkeypatch, "   ")

    await _run_job(db, job)

    assert job.status == "done"


async def test_the_canary_does_not_restrict_altegio_or_other_easyweek_types(db: AsyncSession, monkeypatch) -> None:
    client = await _seed_client(db)
    record = await _seed_record(db, client)
    lifecycle = MessageJob(
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        record_id=record.id,
        client_id=client.id,
        job_type="record_created",
        run_at=_utcnow() - timedelta(minutes=1),
        status="queued",
        dedupe_key="easyweek-lifecycle-1",
        payload={},
    )
    altegio_client = await _seed_client(db, provider=PROVIDER_ALTEGIO, visits_total=None)
    altegio_record = await _seed_record(db, altegio_client, provider=PROVIDER_ALTEGIO, altegio_record_id=999123)
    altegio = MessageJob(
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        record_id=altegio_record.id,
        client_id=altegio_client.id,
        job_type=COMEBACK_3D,
        run_at=_utcnow() - timedelta(minutes=1),
        status="queued",
        dedupe_key="altegio-comeback-1",
        payload={},
    )
    db.add_all([lifecycle, altegio])
    await db.flush()
    _canary(monkeypatch, "999999")

    claimed = {row.id for row in await ow._lock_next_jobs(db, 10)}

    assert lifecycle.id in claimed
    assert altegio.id in claimed


# ===========================================================================
# Frozen service identity (P2)
# ===========================================================================


async def test_a_swapped_service_cancels_the_repeat_before_meta(db: AsyncSession, capture) -> None:
    """The booking's service changed after the visit was proven."""
    _client, record, job = await _ready_repeat(db)
    # `booking-updated` replaced the single service with a different one.
    await db.execute(RecordService.__table__.delete().where(RecordService.record_id == record.id))
    db.add(RecordService(record_id=record.id, service_id=77, title="Something Else", cost_to_pay=None, raw={}))
    await db.flush()

    await _run_job(db, job)

    assert job.status == "canceled"
    assert _refusal(job) == RETENTION_SERVICE_CHANGED
    assert job.attempts == 0
    assert capture.template_calls == []


async def test_an_unchanged_service_still_sends(db: AsyncSession, capture) -> None:
    _client, _record, job = await _ready_repeat(db)

    await _run_job(db, job)

    assert job.status == "done"
    assert capture.template_calls[0]["params"][1] == SERVICE_TITLE


async def test_a_renamed_service_still_sends_with_the_current_title(db: AsyncSession, capture) -> None:
    """Only the ID is frozen. A salon rewording a title is not a swapped service.

    The customer-facing text is read from the CURRENT row, so the rename is what
    the customer sees — but only because the identity matched first.
    """
    _client, record, job = await _ready_repeat(db)
    service = (await db.execute(select(RecordService).where(RecordService.record_id == record.id))).scalars().one()
    service.title = "Wimpernverlängerung Classic"
    service.cost_to_pay = Decimal("70.00")
    await db.flush()

    await _run_job(db, job)

    assert job.status == "done"
    assert capture.template_calls[0]["params"][1] == "Wimpernverlängerung Classic"


async def test_a_payload_without_a_frozen_service_is_refused(db: AsyncSession, capture) -> None:
    """The version-1 shape. Assuming the missing field would weaken the contract."""
    client = await _seed_client(db)
    record = await _seed_record(db, client)
    await _seed_template(db, code=REPEAT_10D)
    await _seed_sender(db)
    assert record.starts_at is not None
    payload = repeat_job_payload(
        booking_uuid=BOOKING_UUID,
        company_id=record.company_id,
        starts_at=record.starts_at,
        visits_baseline=4,
        service_id=11,
    )
    del payload[PAYLOAD_SOURCE_SERVICE_ID]
    job = await _seed_repeat_job(db, client, record, payload=payload)

    await _run_job(db, job)

    assert job.status == "canceled"
    assert _refusal(job) == RETENTION_SERVICE_UNPROVEN
    assert capture.template_calls == []


async def test_a_comeback_carries_no_service_identity(db: AsyncSession, capture) -> None:
    """Its template names no service, so freezing one would be a field nobody reads."""
    _client, _record, job = await _ready_comeback(db)

    assert PAYLOAD_SOURCE_SERVICE_ID not in job.payload

    await _run_job(db, job)
    assert job.status == "done"


# ===========================================================================
# The expired-retention cleanup (P2)
# ===========================================================================


async def test_the_cleanup_terminalizes_an_expired_job_with_the_fence_shut(
    db: AsyncSession, capture, monkeypatch
) -> None:
    """The deadlock this exists to break: never claimable, never green."""
    monkeypatch.setattr(settings, "easyweek_retention_send_enabled", False, raising=False)
    client = await _seed_client(db)
    record = await _seed_record(db, client, starts_at=_utcnow() - timedelta(days=20))
    job = await _seed_repeat_job(db, client, record, run_at=_utcnow() - timedelta(days=10))

    cancelled = await ow.cancel_expired_easyweek_retention_jobs(db)
    await db.flush()
    await db.refresh(job)

    assert cancelled == 1
    assert job.status == "canceled"
    assert job.last_error == ow.RETENTION_DEADLINE_EXPIRED_REASON
    assert job.attempts == 0
    assert job.locked_at is None
    assert capture.template_calls == []
    assert await _outbox_rows(db, job) == []


async def test_the_cleanup_leaves_a_fresh_job_alone(db: AsyncSession) -> None:
    client = await _seed_client(db)
    record = await _seed_record(db, client)
    job = await _seed_repeat_job(db, client, record, run_at=_utcnow() - timedelta(minutes=1))

    cancelled = await ow.cancel_expired_easyweek_retention_jobs(db)
    await db.flush()
    await db.refresh(job)

    assert cancelled == 0
    assert job.status == "queued"


async def test_the_cleanup_is_idempotent(db: AsyncSession) -> None:
    client = await _seed_client(db)
    record = await _seed_record(db, client, starts_at=_utcnow() - timedelta(days=20))
    await _seed_repeat_job(db, client, record, run_at=_utcnow() - timedelta(days=10))

    first = await ow.cancel_expired_easyweek_retention_jobs(db)
    await db.flush()
    second = await ow.cancel_expired_easyweek_retention_jobs(db)

    assert (first, second) == (1, 0)


async def test_the_cleanup_never_touches_altegio_or_other_easyweek_types(db: AsyncSession) -> None:
    client = await _seed_client(db)
    record = await _seed_record(db, client, starts_at=_utcnow() - timedelta(days=20))
    reminder = MessageJob(
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        record_id=record.id,
        client_id=client.id,
        job_type="reminder_24h",
        run_at=_utcnow() - timedelta(days=10),
        status="queued",
        dedupe_key="easyweek-reminder-1",
        payload={},
    )
    altegio_client = await _seed_client(db, provider=PROVIDER_ALTEGIO, visits_total=None)
    altegio_record = await _seed_record(
        db,
        altegio_client,
        provider=PROVIDER_ALTEGIO,
        starts_at=_utcnow() - timedelta(days=20),
        altegio_record_id=999123,
    )
    altegio = MessageJob(
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        record_id=altegio_record.id,
        client_id=altegio_client.id,
        job_type=REPEAT_10D,
        run_at=_utcnow() - timedelta(days=10),
        status="queued",
        dedupe_key="altegio-repeat-1",
        payload={},
    )
    db.add_all([reminder, altegio])
    await db.flush()

    cancelled = await ow.cancel_expired_easyweek_retention_jobs(db)
    await db.flush()
    await db.refresh(reminder)
    await db.refresh(altegio)

    assert cancelled == 0
    assert reminder.status == "queued"
    assert altegio.status == "queued"


async def test_a_closed_master_fence_holds_retention_while_lifecycle_stays_claimable(
    db: AsyncSession, monkeypatch
) -> None:
    """Both halves of the narrow contract, in ONE queue and one claim.

    `test_the_master_fence_does_not_change_other_easyweek_or_altegio_jobs` proves
    the other paths keep flowing. This proves the two outcomes side by side,
    which is the thing the runbook now has to describe accurately: after closing
    the master flag the retention job is held, and the lifecycle job beside it is
    still claimed and would still be sent by its own contract.

    It confirms the existing behaviour; it does not ask for a wider fence. A
    global master gate over EASYWEEK_CUSTOMER_JOB_TYPES would break this test,
    and that is deliberate — the emergency stop for the whole EasyWeek queue is
    the operator procedure in §8.2, not this flag.
    """
    client = await _seed_client(db)
    record = await _seed_record(db, client)
    retention = await _seed_repeat_job(db, client, record, run_at=_utcnow() - timedelta(minutes=1))
    lifecycle = MessageJob(
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        record_id=record.id,
        client_id=client.id,
        job_type="record_created",
        run_at=_utcnow() - timedelta(minutes=1),
        status="queued",
        dedupe_key="easyweek-lifecycle-alongside-retention",
        payload={},
    )
    db.add(lifecycle)
    await db.flush()

    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)
    claimed = {row.id for row in await ow._lock_next_jobs(db, 10)}

    assert retention.id not in claimed, "the retention job is held by the master fence"
    assert lifecycle.id in claimed, "the lifecycle job is not, and would still be sent"

    await db.refresh(retention)
    assert retention.status == "queued"
    assert retention.attempts == 0
    assert retention.locked_at is None


# ===========================================================================
# Resuming after a master-flag pause: what actually holds a queue
#
# The runbook used to say that restoring the master flag "permits nothing on its
# own". These three states show why that is only true while the send fence is
# shut — and therefore why the resume procedure has to close the fence in a
# recreated outbox BEFORE the master flag comes back.
# ===========================================================================


async def _two_due_retention_jobs(db: AsyncSession) -> tuple[MessageJob, MessageJob]:
    """Two independent due retention jobs, on a clean queue.

    Two clients and two bookings, not one job looked at twice: a queue is
    released or held as a whole, and a single row cannot show that.
    """
    first_client = await _seed_client(db)
    first_record = await _seed_record(db, first_client)
    first = await _seed_repeat_job(
        db,
        first_client,
        first_record,
        run_at=_utcnow() - timedelta(minutes=5),
        dedupe_key="easyweek_retention:resume:1",
    )

    second_client = await _seed_client(db, altegio_client_id=7300077)
    second_record = await _seed_record(
        db,
        second_client,
        starts_at=_utcnow() - timedelta(days=10),
        booking_uuid=OTHER_BOOKING_UUID,
        altegio_record_id=4200077,
    )
    second = await _seed_repeat_job(
        db,
        second_client,
        second_record,
        run_at=_utcnow() - timedelta(minutes=4),
        dedupe_key="easyweek_retention:resume:2",
    )
    return first, second


def _held_state(job: MessageJob, run_at: datetime) -> tuple[str, int, object, bool]:
    return (job.status, job.attempts, job.locked_at, job.run_at == run_at)


async def test_a_paused_master_holds_the_whole_due_retention_queue(db: AsyncSession, capture, monkeypatch) -> None:
    """master=false, send fence still true — the state a pause actually leaves."""
    first, second = await _two_due_retention_jobs(db)
    run_ats = (first.run_at, second.run_at)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_retention_send_enabled", True, raising=False)

    claimed = {row.id for row in await ow._lock_next_jobs(db, 10)}

    assert claimed == set(), "both jobs are held while the master flag is shut"
    for job, run_at in zip((first, second), run_ats, strict=True):
        await db.refresh(job)
        assert _held_state(job, run_at) == ("queued", 0, None, True)
    assert capture.template_calls == []


async def test_a_closed_send_fence_holds_the_whole_due_retention_queue(db: AsyncSession, capture, monkeypatch) -> None:
    """master=true, fence false — the state the resume procedure creates.

    This is what makes the resume safe: the master flag can come back while the
    queue stays exactly where the preflight will find it.
    """
    first, second = await _two_due_retention_jobs(db)
    run_ats = (first.run_at, second.run_at)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_retention_send_enabled", False, raising=False)

    claimed = {row.id for row in await ow._lock_next_jobs(db, 10)}

    assert claimed == set(), "both jobs are held while the send fence is shut"
    for job, run_at in zip((first, second), run_ats, strict=True):
        await db.refresh(job)
        assert _held_state(job, run_at) == ("queued", 0, None, True)
    assert capture.template_calls == []


async def test_master_and_fence_both_open_release_the_whole_queue(db: AsyncSession, monkeypatch) -> None:
    """The control, and the reason the runbook needed fixing.

    Nothing else is consulted — no stored preflight verdict, no canary — so a
    master flag restored while the fence is still true releases the entire
    backlog that accumulated during the pause, at once.
    """
    first, second = await _two_due_retention_jobs(db)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_retention_send_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_retention_canary_job_id", "", raising=False)

    claimed = {row.id for row in await ow._lock_next_jobs(db, 10)}

    assert claimed == {first.id, second.id}, "both become claimable the moment both gates are open"


async def test_retention_planning_does_not_hold_an_existing_queue(db: AsyncSession, monkeypatch) -> None:
    """`EASYWEEK_RETENTION_ENABLED` is a PLANNING flag, and only that.

    Turning planning off is not a way to hold a queue that already exists — the
    send gate never reads it. That is why the resume procedure closes the send
    fence instead.
    """
    first, second = await _two_due_retention_jobs(db)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_retention_send_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_retention_enabled", False, raising=False)

    claimed = {row.id for row in await ow._lock_next_jobs(db, 10)}

    assert claimed == {first.id, second.id}, "planning=false does not fence an existing queue"
