"""PostgreSQL contract for the EasyWeek outbox path (PR-5).

Every test here runs against a real database with real rows, and every send goes
through a capture provider that records the call instead of performing it — no
HTTP request ever leaves the process.

What is proven:

* a template is reachable only through its own provider, at every fallback step;
* the Meta template name comes from ``message_templates.meta_template_name`` and
  from nowhere else — not from ``META_TEMPLATE_MAP``, not from the numeric
  company id, not from the code;
* the three lifecycle codes build exactly the params their approved layout
  expects, in order;
* the only link an EasyWeek message can carry is a re-verified manage link or
  the static booking page;
* a sender is chosen inside one provider, and a missing one fails closed;
* none of the above changes anything on the Altegio path.

The colliding ``company_id`` used throughout is the point, not an accident:
EasyWeek's ``company_id`` is the numeric EasyWeek ``:location_id`` and shares an
integer space with Altegio company ids, so every provider-blind query is a
cross-tenant leak waiting for the two spaces to overlap.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import altegio_bot.db as app_db
from altegio_bot.campaigns.runner import CAMPAIGN_EXECUTION_JOB_TYPE
from altegio_bot.delivery_retry_identity import RetryIdentity, resolve_retry_identity
from altegio_bot.easyweek_normalizer import canonical_booking_uuid
from altegio_bot.easyweek_policy import (
    EASYWEEK_LIFECYCLE_JOB_TYPES,
    easyweek_job_type_error,
    validate_static_booking_page,
)
from altegio_bot.meta_templates import (
    META_TEMPLATE_MAP,
    TEMPLATE_LANGUAGE,
    build_lifecycle_template_params,
    resolve_meta_template,
)
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    Client,
    EasyWeekEvent,
    MessageJob,
    MessageTemplate,
    OutboxMessage,
    Record,
    RecordService,
    ServiceSenderRule,
    WhatsAppEvent,
    WhatsAppSender,
)
from altegio_bot.providers.base import WhatsAppProvider
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import (
    TEST_LOCATION_ID,
    booking_canceled,
    booking_created,
    booking_rescheduled,
    booking_updated,
)
from altegio_bot.whatsapp_routing import pick_sender_id, pick_sender_id_by_code
from altegio_bot.workers import campaign_worker
from altegio_bot.workers import easyweek_inbox_worker as eyw_worker
from altegio_bot.workers import outbox_worker as ow
from altegio_bot.workers import whatsapp_inbox_worker as wa_worker
from altegio_bot.workers.outbox_worker import PRE_APPOINTMENT_NOTES_DE

pytestmark = pytest.mark.asyncio

# One numeric id, two tenants. Deliberate: it is the collision the provider
# predicates exist to survive.
COLLIDING_COMPANY_ID = 758285
OTHER_EASYWEEK_COMPANY_ID = 999002

BOOKING_HASH = "90000001"
VERIFIED_PAGE = f"https://eyw.me/r/{BOOKING_HASH}"
STATIC_BOOKING_PAGE = "https://example.invalid/book"

# A name no Python constant knows: if it reaches the provider it can only have
# come from the database row.
EASYWEEK_CREATED_TEMPLATE = "eyw_zzz_created_v9"
EASYWEEK_UPDATED_TEMPLATE = "eyw_zzz_updated_v9"
EASYWEEK_CANCELED_TEMPLATE = "eyw_zzz_canceled_v9"

# Never asserted *into* an outgoing message — only asserted ABSENT from errors.
CLIENT_PHONE = "+491700000001"
CLIENT_EMAIL = "anna.pii@example.invalid"

# Distinguishes "caller said nothing" from "caller said None", which is the whole
# point of the PR-4 price semantics these fixtures have to be able to express.
_UNSET: Any = object()


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _pr5_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Template send mode, a static booking page, notifications still OFF.

    ``easyweek_notifications_enabled`` gates job CREATION in the inbox worker and
    stays False here exactly as in production: these tests build their own
    ``MessageJob`` rows and never ask the inbox worker to emit one.
    """
    monkeypatch.setattr(settings, "whatsapp_send_mode", "template", raising=False)
    monkeypatch.setattr(settings, "bot_template_text_inside_24h_enabled", False, raising=False)
    monkeypatch.setattr(settings, "meta_circuit_breaker_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_booking_page_url", STATIC_BOOKING_PAGE, raising=False)
    monkeypatch.setattr(settings, "easyweek_default_language", "de", raising=False)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)


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


def _template(
    *,
    provider: str,
    company_id: int,
    code: str,
    language: str = "de",
    body: str = "Hallo {{1}}",
    meta_template_name: str | None = None,
    is_active: bool = True,
) -> MessageTemplate:
    return MessageTemplate(
        provider=provider,
        company_id=company_id,
        code=code,
        language=language,
        body=body,
        meta_template_name=meta_template_name,
        is_active=is_active,
    )


def _sender(
    *,
    provider: str,
    company_id: int,
    sender_code: str = "default",
    phone_number_id: str,
    is_active: bool = True,
) -> WhatsAppSender:
    return WhatsAppSender(
        provider=provider,
        company_id=company_id,
        sender_code=sender_code,
        phone_number_id=phone_number_id,
        is_active=is_active,
    )


async def _seed_easyweek_client(db: AsyncSession, *, company_id: int = COLLIDING_COMPANY_ID) -> Client:
    client = Client(
        provider=PROVIDER_EASYWEEK,
        company_id=company_id,
        altegio_client_id=7300002,
        phone_e164=CLIENT_PHONE,
        display_name="Anna Müller",
        email=CLIENT_EMAIL,
        raw={},
    )
    db.add(client)
    await db.flush()
    return client


async def _seed_easyweek_record(
    db: AsyncSession,
    client: Client,
    *,
    company_id: int = COLLIDING_COMPANY_ID,
    short_link: str | None = VERIFIED_PAGE,
    booking_hash_id: str | None = BOOKING_HASH,
    starts_at: datetime | None = None,
    services: tuple[tuple[int, str | None, str | None], ...] = ((11, "Wimpernverlängerung", "60.00"),),
    total_cost: str | None = _UNSET,
    provider: str = PROVIDER_EASYWEEK,
) -> Record:
    """Seed a record whose snapshot obeys PR-4's price invariant by default.

    ``Record.total_cost`` and ``RecordService.cost_to_pay`` are the same
    booking-level number in PR-4, so the default mirrors the single service's
    cost. Tests that want an inconsistent or unknown snapshot pass ``total_cost``
    explicitly (``None`` for unknown).
    """
    if total_cost is _UNSET:
        first_cost = services[0][2] if services else None
        resolved_total = Decimal(first_cost) if first_cost is not None else None
    else:
        resolved_total = Decimal(total_cost) if total_cost is not None else None

    record = Record(
        provider=provider,
        company_id=company_id,
        altegio_record_id=4200001,
        easyweek_booking_uuid=(
            uuid.UUID("11111111-2222-4333-8444-555555555555") if provider == PROVIDER_EASYWEEK else None
        ),
        easyweek_booking_hash_id=booking_hash_id,
        client_id=client.id,
        staff_name="Tanja",
        starts_at=starts_at or (datetime.now(timezone.utc) + timedelta(days=3)),
        short_link=short_link,
        total_cost=resolved_total,
        raw={},
    )
    db.add(record)
    await db.flush()
    for service_id, title, cost in services:
        db.add(
            RecordService(
                record_id=record.id,
                service_id=service_id,
                title=title,
                cost_to_pay=Decimal(cost) if cost is not None else None,
                raw={},
            )
        )
    await db.flush()
    return record


async def _seed_job(
    db: AsyncSession,
    *,
    provider: str,
    company_id: int,
    job_type: str,
    record: Record | None,
    client: Client | None,
    dedupe_key: str,
) -> MessageJob:
    job = MessageJob(
        provider=provider,
        company_id=company_id,
        record_id=record.id if record else None,
        client_id=client.id if client else None,
        job_type=job_type,
        run_at=datetime.now(timezone.utc),
        status="queued",
        dedupe_key=dedupe_key,
        payload={},
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
    res = await db.execute(select(OutboxMessage).where(OutboxMessage.job_id == job.id))
    return list(res.scalars().all())


async def _seed_easyweek_happy_path(
    db: AsyncSession,
    *,
    job_type: str = "record_created",
    meta_template_name: str | None = EASYWEEK_CREATED_TEMPLATE,
    short_link: str | None = VERIFIED_PAGE,
    booking_hash_id: str | None = BOOKING_HASH,
    services: tuple[tuple[int, str | None, str | None], ...] = ((11, "Wimpernverlängerung", "60.00"),),
    total_cost: str | None = _UNSET,
    language: str = "de",
    with_sender: bool = True,
) -> MessageJob:
    """Everything an EasyWeek lifecycle job needs to reach the provider."""
    client = await _seed_easyweek_client(db)
    record = await _seed_easyweek_record(
        db,
        client,
        short_link=short_link,
        booking_hash_id=booking_hash_id,
        services=services,
        total_cost=total_cost,
    )
    db.add(
        _template(
            provider=PROVIDER_EASYWEEK,
            company_id=COLLIDING_COMPANY_ID,
            code=job_type,
            language=language,
            meta_template_name=meta_template_name,
        )
    )
    if with_sender:
        db.add(
            _sender(
                provider=PROVIDER_EASYWEEK,
                company_id=COLLIDING_COMPANY_ID,
                phone_number_id="eyw-phone-id",
            )
        )
    await db.flush()
    return await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type=job_type,
        record=record,
        client=client,
        dedupe_key=f"eyw-{job_type}-1",
    )


ALTEGIO_SHORT_LINK = "https://n1234567.yclients.com/record/1"


async def _seed_altegio_client_and_record(
    db: AsyncSession,
    *,
    service_title: str | None = "Schnitt",
    service_cost: str | None = "40.00",
    total_cost: str | None = "40.00",
) -> tuple[Client, Record]:
    client = Client(
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        altegio_client_id=555001,
        phone_e164="+491700000002",
        display_name="Berta",
        raw={},
    )
    db.add(client)
    await db.flush()
    record = Record(
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        altegio_record_id=555002,
        client_id=client.id,
        staff_name="Tanja",
        starts_at=datetime.now(timezone.utc) + timedelta(days=3),
        short_link=ALTEGIO_SHORT_LINK,
        total_cost=Decimal(total_cost) if total_cost is not None else None,
        raw={},
    )
    db.add(record)
    await db.flush()
    db.add(
        RecordService(
            record_id=record.id,
            service_id=21,
            title=service_title,
            cost_to_pay=Decimal(service_cost) if service_cost is not None else None,
            raw={},
        )
    )
    await db.flush()
    return client, record


async def _seed_altegio_happy_path(
    db: AsyncSession,
    *,
    job_type: str = "record_updated",
    service_title: str | None = "Schnitt",
    service_cost: str | None = "40.00",
    total_cost: str | None = "40.00",
    mismatched_client: bool = False,
) -> MessageJob:
    """A working Altegio job — the control group for every EasyWeek gate.

    ``mismatched_client`` points ``job.client_id`` at a different Altegio client
    than ``record.client_id``. That is a hard failure under the EasyWeek scope
    gate and must stay a normal send here.
    """
    client, record = await _seed_altegio_client_and_record(
        db,
        service_title=service_title,
        service_cost=service_cost,
        total_cost=total_cost,
    )
    job_client = client
    if mismatched_client:
        job_client = Client(
            provider=PROVIDER_ALTEGIO,
            company_id=COLLIDING_COMPANY_ID,
            altegio_client_id=555099,
            phone_e164="+491700000099",
            display_name="Elke",
            raw={},
        )
        db.add(job_client)
        await db.flush()
    db.add(
        _template(
            provider=PROVIDER_ALTEGIO,
            company_id=COLLIDING_COMPANY_ID,
            code=job_type,
            body="ALTEGIO BODY",
            meta_template_name=None,
        )
    )
    db.add(_sender(provider=PROVIDER_ALTEGIO, company_id=COLLIDING_COMPANY_ID, phone_number_id="altegio-phone-id"))
    await db.flush()
    return await _seed_job(
        db,
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        job_type=job_type,
        record=record,
        client=job_client,
        dedupe_key=f"alt-{job_type}-1",
    )


# ---------------------------------------------------------------------------
# 1. Template isolation
# ---------------------------------------------------------------------------


async def test_easyweek_and_altegio_rows_collide_and_each_provider_gets_its_own(db: AsyncSession) -> None:
    """Identical company_id, code and language on both providers — no crosstalk."""
    db.add_all(
        [
            _template(
                provider=PROVIDER_ALTEGIO,
                company_id=COLLIDING_COMPANY_ID,
                code="record_created",
                body="ALTEGIO BODY",
                meta_template_name=None,
            ),
            _template(
                provider=PROVIDER_EASYWEEK,
                company_id=COLLIDING_COMPANY_ID,
                code="record_created",
                body="EASYWEEK BODY",
                meta_template_name=EASYWEEK_CREATED_TEMPLATE,
            ),
        ]
    )
    await db.flush()

    eyw_tmpl, eyw_lang = await ow._load_template(
        db,
        company_id=COLLIDING_COMPANY_ID,
        template_code="record_created",
        language="de",
        provider=PROVIDER_EASYWEEK,
    )
    alt_tmpl, alt_lang = await ow._load_template(
        db,
        company_id=COLLIDING_COMPANY_ID,
        template_code="record_created",
        language="de",
        provider=PROVIDER_ALTEGIO,
    )

    assert eyw_tmpl is not None and eyw_tmpl.body == "EASYWEEK BODY"
    assert eyw_tmpl.provider == PROVIDER_EASYWEEK
    assert eyw_lang == "de"
    assert alt_tmpl is not None and alt_tmpl.body == "ALTEGIO BODY"
    assert alt_tmpl.provider == PROVIDER_ALTEGIO
    assert alt_lang == "de"


async def test_easyweek_never_falls_back_to_an_altegio_row(db: AsyncSession) -> None:
    """Only an Altegio row exists — EasyWeek must find nothing, not borrow it."""
    db.add(
        _template(
            provider=PROVIDER_ALTEGIO,
            company_id=COLLIDING_COMPANY_ID,
            code="record_created",
            body="ALTEGIO BODY",
        )
    )
    await db.flush()

    tmpl, _lang = await ow._load_template(
        db,
        company_id=COLLIDING_COMPANY_ID,
        template_code="record_created",
        language="de",
        provider=PROVIDER_EASYWEEK,
    )
    assert tmpl is None


async def test_easyweek_does_not_use_another_easyweek_company_row(db: AsyncSession) -> None:
    db.add(
        _template(
            provider=PROVIDER_EASYWEEK,
            company_id=OTHER_EASYWEEK_COMPANY_ID,
            code="record_created",
            body="OTHER TENANT",
            meta_template_name=EASYWEEK_CREATED_TEMPLATE,
        )
    )
    await db.flush()

    tmpl, _lang = await ow._load_template(
        db,
        company_id=COLLIDING_COMPANY_ID,
        template_code="record_created",
        language="de",
        provider=PROVIDER_EASYWEEK,
    )
    assert tmpl is None


async def test_easyweek_has_no_cross_company_fallback_even_for_universal_codes(db: AsyncSession) -> None:
    """``review_3d`` is universal for Altegio; for EasyWeek it is still one tenant."""
    db.add(
        _template(
            provider=PROVIDER_EASYWEEK,
            company_id=OTHER_EASYWEEK_COMPANY_ID,
            code="review_3d",
            body="OTHER TENANT UNIVERSAL",
            meta_template_name="eyw_zzz_review_v9",
        )
    )
    await db.flush()

    tmpl, _lang = await ow._load_template(
        db,
        company_id=COLLIDING_COMPANY_ID,
        template_code="review_3d",
        language="de",
        provider=PROVIDER_EASYWEEK,
    )
    assert tmpl is None


async def test_easyweek_language_fallback_stays_inside_the_provider(db: AsyncSession) -> None:
    """An Altegio row in the REQUESTED language must lose to an EasyWeek row in another."""
    db.add_all(
        [
            _template(
                provider=PROVIDER_ALTEGIO,
                company_id=COLLIDING_COMPANY_ID,
                code="record_created",
                language="en",
                body="ALTEGIO EN",
            ),
            _template(
                provider=PROVIDER_EASYWEEK,
                company_id=COLLIDING_COMPANY_ID,
                code="record_created",
                language="de",
                body="EASYWEEK DE",
                meta_template_name=EASYWEEK_CREATED_TEMPLATE,
            ),
        ]
    )
    await db.flush()

    tmpl, used_lang = await ow._load_template(
        db,
        company_id=COLLIDING_COMPANY_ID,
        template_code="record_created",
        language="en",
        provider=PROVIDER_EASYWEEK,
    )
    assert tmpl is not None and tmpl.body == "EASYWEEK DE"
    assert used_lang == "de"


async def test_easyweek_inactive_template_is_not_used(db: AsyncSession) -> None:
    db.add(
        _template(
            provider=PROVIDER_EASYWEEK,
            company_id=COLLIDING_COMPANY_ID,
            code="record_created",
            body="DISABLED",
            meta_template_name=EASYWEEK_CREATED_TEMPLATE,
            is_active=False,
        )
    )
    await db.flush()

    tmpl, _lang = await ow._load_template(
        db,
        company_id=COLLIDING_COMPANY_ID,
        template_code="record_created",
        language="de",
        provider=PROVIDER_EASYWEEK,
    )
    assert tmpl is None


async def test_duplicate_rows_resolve_deterministically_to_lowest_id(db: AsyncSession) -> None:
    """Nothing enforces one row per (provider, company, code, language)."""
    first = _template(
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        code="record_created",
        body="FIRST",
        meta_template_name=EASYWEEK_CREATED_TEMPLATE,
    )
    second = _template(
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        code="record_created",
        body="SECOND",
        meta_template_name="eyw_zzz_created_v10",
    )
    db.add_all([first, second])
    await db.flush()

    for _ in range(3):
        tmpl, _lang = await ow._load_template(
            db,
            company_id=COLLIDING_COMPANY_ID,
            template_code="record_created",
            language="de",
            provider=PROVIDER_EASYWEEK,
        )
        assert tmpl is not None
        assert tmpl.id == min(first.id, second.id)
        assert tmpl.body == "FIRST"


async def test_missing_easyweek_template_fails_job_before_any_provider_call(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    client = await _seed_easyweek_client(db)
    record = await _seed_easyweek_record(db, client)
    db.add(_sender(provider=PROVIDER_EASYWEEK, company_id=COLLIDING_COMPANY_ID, phone_number_id="eyw-phone-id"))
    await db.flush()
    job = await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type="record_created",
        record=record,
        client=client,
        dedupe_key="eyw-missing-template",
    )

    await _run_job(db, job)

    assert job.status == "failed"
    assert "Template not found" in (job.last_error or "")
    assert capture.template_calls == []
    assert capture.text_calls == []


async def test_inactive_easyweek_template_fails_job_before_any_provider_call(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    client = await _seed_easyweek_client(db)
    record = await _seed_easyweek_record(db, client)
    db.add(
        _template(
            provider=PROVIDER_EASYWEEK,
            company_id=COLLIDING_COMPANY_ID,
            code="record_created",
            meta_template_name=EASYWEEK_CREATED_TEMPLATE,
            is_active=False,
        )
    )
    db.add(_sender(provider=PROVIDER_EASYWEEK, company_id=COLLIDING_COMPANY_ID, phone_number_id="eyw-phone-id"))
    await db.flush()
    job = await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type="record_created",
        record=record,
        client=client,
        dedupe_key="eyw-inactive-template",
    )

    await _run_job(db, job)

    assert job.status == "failed"
    assert capture.template_calls == []


async def test_easyweek_job_does_not_borrow_the_colliding_altegio_template(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """End to end: an Altegio row on the same numeric company must not be sent."""
    client = await _seed_easyweek_client(db)
    record = await _seed_easyweek_record(db, client)
    db.add(
        _template(
            provider=PROVIDER_ALTEGIO,
            company_id=COLLIDING_COMPANY_ID,
            code="record_created",
            body="ALTEGIO BODY WITH KARLSRUHE FOOTER",
        )
    )
    db.add(_sender(provider=PROVIDER_EASYWEEK, company_id=COLLIDING_COMPANY_ID, phone_number_id="eyw-phone-id"))
    await db.flush()
    job = await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type="record_created",
        record=record,
        client=client,
        dedupe_key="eyw-collision-template",
    )

    await _run_job(db, job)

    assert job.status == "failed"
    assert capture.template_calls == []


# ---------------------------------------------------------------------------
# 2. DB-first Meta template name
# ---------------------------------------------------------------------------


async def test_meta_template_name_from_db_row_reaches_the_provider(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    job = await _seed_easyweek_happy_path(db)

    await _run_job(db, job)

    assert job.status == "done", job.last_error
    assert len(capture.template_calls) == 1
    assert capture.template_calls[0]["template_name"] == EASYWEEK_CREATED_TEMPLATE


async def test_meta_template_map_holds_no_easyweek_company_and_cannot_produce_the_name() -> None:
    """The name is unreachable through the Altegio hardcode by construction."""
    # Only the two Altegio branches are keyed there — no EasyWeek location id.
    assert {company_id for company_id, _code in META_TEMPLATE_MAP} == {758285, 1271200}
    assert all(name.startswith("kitilash_") for name in META_TEMPLATE_MAP.values())
    assert (OTHER_EASYWEEK_COMPANY_ID, "record_created") not in META_TEMPLATE_MAP
    assert resolve_meta_template(OTHER_EASYWEEK_COMPANY_ID, "record_created") is None


async def test_easyweek_name_is_not_derived_from_company_id_or_code(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """The colliding company id maps to a Karlsruhe name — which must NOT be sent."""
    job = await _seed_easyweek_happy_path(db)

    await _run_job(db, job)

    sent_name = capture.template_calls[0]["template_name"]
    assert sent_name == EASYWEEK_CREATED_TEMPLATE
    assert sent_name != META_TEMPLATE_MAP[(COLLIDING_COMPANY_ID, "record_created")]
    assert "kitilash" not in sent_name
    assert "_ka_" not in sent_name and "_ra_" not in sent_name
    assert sent_name != "record_created"


@pytest.mark.parametrize("bad_name", [None, "", "   ", "\t\n "])
async def test_blank_meta_template_name_fails_closed(
    db: AsyncSession,
    capture: CaptureProvider,
    bad_name: str | None,
) -> None:
    job = await _seed_easyweek_happy_path(db, meta_template_name=bad_name)

    await _run_job(db, job)

    assert job.status == "failed"
    assert "meta_template_name" in (job.last_error or "")
    assert capture.template_calls == []
    assert capture.text_calls == []


async def test_fail_closed_error_carries_no_pii(db: AsyncSession, capture: CaptureProvider) -> None:
    job = await _seed_easyweek_happy_path(db, meta_template_name=None)

    await _run_job(db, job)

    err = job.last_error or ""
    assert err
    assert CLIENT_PHONE not in err
    assert CLIENT_EMAIL not in err
    assert "Anna" not in err
    assert BOOKING_HASH not in err
    assert "eyw.me" not in err


# ---------------------------------------------------------------------------
# 3. Lifecycle render — exact count and order
# ---------------------------------------------------------------------------


async def _run_and_get_params(db: AsyncSession, capture: CaptureProvider, job: MessageJob) -> list[str]:
    await _run_job(db, job)
    assert job.status == "done", job.last_error
    assert len(capture.template_calls) == 1
    return list(capture.template_calls[0]["params"])


async def test_record_created_builds_seven_params_in_order(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    starts_at = datetime(2026, 9, 14, 8, 30, tzinfo=timezone.utc)
    client = await _seed_easyweek_client(db)
    record = await _seed_easyweek_record(db, client, starts_at=starts_at)
    db.add(
        _template(
            provider=PROVIDER_EASYWEEK,
            company_id=COLLIDING_COMPANY_ID,
            code="record_created",
            meta_template_name=EASYWEEK_CREATED_TEMPLATE,
        )
    )
    db.add(_sender(provider=PROVIDER_EASYWEEK, company_id=COLLIDING_COMPANY_ID, phone_number_id="eyw-phone-id"))
    await db.flush()
    job = await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type="record_created",
        record=record,
        client=client,
        dedupe_key="eyw-created-params",
    )

    params = await _run_and_get_params(db, capture, job)

    assert params == [
        "Anna Müller",
        "Tanja",
        ow._fmt_date(starts_at),
        ow._fmt_time(starts_at),
        "Wimpernverlängerung — 60.00€",
        "60.00",
        VERIFIED_PAGE,
    ]
    assert len(params) == 7


async def test_record_updated_builds_seven_params_in_order(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    starts_at = datetime(2026, 9, 14, 8, 30, tzinfo=timezone.utc)
    client = await _seed_easyweek_client(db)
    record = await _seed_easyweek_record(db, client, starts_at=starts_at)
    db.add(
        _template(
            provider=PROVIDER_EASYWEEK,
            company_id=COLLIDING_COMPANY_ID,
            code="record_updated",
            meta_template_name=EASYWEEK_UPDATED_TEMPLATE,
        )
    )
    db.add(_sender(provider=PROVIDER_EASYWEEK, company_id=COLLIDING_COMPANY_ID, phone_number_id="eyw-phone-id"))
    await db.flush()
    job = await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type="record_updated",
        record=record,
        client=client,
        dedupe_key="eyw-updated-params",
    )

    params = await _run_and_get_params(db, capture, job)

    assert params == [
        "Anna Müller",
        "Tanja",
        ow._fmt_date(starts_at),
        ow._fmt_time(starts_at),
        "Wimpernverlängerung — 60.00€",
        "60.00",
        VERIFIED_PAGE,
    ]
    assert capture.template_calls[0]["template_name"] == EASYWEEK_UPDATED_TEMPLATE


async def test_record_canceled_builds_five_params_with_the_static_page(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    starts_at = datetime(2026, 9, 14, 8, 30, tzinfo=timezone.utc)
    client = await _seed_easyweek_client(db)
    record = await _seed_easyweek_record(db, client, starts_at=starts_at)
    db.add(
        _template(
            provider=PROVIDER_EASYWEEK,
            company_id=COLLIDING_COMPANY_ID,
            code="record_canceled",
            meta_template_name=EASYWEEK_CANCELED_TEMPLATE,
        )
    )
    db.add(_sender(provider=PROVIDER_EASYWEEK, company_id=COLLIDING_COMPANY_ID, phone_number_id="eyw-phone-id"))
    await db.flush()
    job = await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type="record_canceled",
        record=record,
        client=client,
        dedupe_key="eyw-canceled-params",
    )

    params = await _run_and_get_params(db, capture, job)

    assert params == [
        "Anna Müller",
        ow._fmt_date(starts_at),
        ow._fmt_time(starts_at),
        "Wimpernverlängerung — 60.00€",
        STATIC_BOOKING_PAGE,
    ]
    assert len(params) == 5


async def test_multi_line_services_are_flattened_into_one_parameter() -> None:
    """Meta rejects a newline inside a parameter, so the list is joined.

    Exercised at the builder, because a multi-ROW EasyWeek snapshot is itself
    rejected upstream (see ``test_multi_row_service_snapshot_fails_closed``):
    PR-4 stores one flat service per booking. The normalisation still has to
    hold — ``services`` is assembled as newline-separated text, and Altegio
    genuinely produces several lines.
    """
    params = build_lifecycle_template_params(
        "record_created",
        {
            "client_name": "Anna Müller",
            "staff_name": "Tanja",
            "date": "14.09.2026",
            "time": "10:30",
            "services": "Wimpernverlängerung — 60.00€\nAuffüllen — 25.50€",
            "total_cost": "85.50",
            "booking_link": STATIC_BOOKING_PAGE,
        },
    )

    assert params[4] == "Wimpernverlängerung — 60.00€, Auffüllen — 25.50€"
    assert "\n" not in params[4]
    assert len(params) == 7


async def test_wrong_arity_is_caught_locally_and_never_sent(
    db: AsyncSession,
    capture: CaptureProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A 5-param list under a 7-param code must fail preflight, not Meta."""
    monkeypatch.setattr(
        ow,
        "build_lifecycle_template_params",
        lambda code, ctx: ["a", "b", "c", "d", "e"],
    )
    job = await _seed_easyweek_happy_path(db)

    await _run_job(db, job)

    assert job.status == "failed"
    assert "expected 7 params, got 5" in (job.last_error or "")
    assert capture.template_calls == []


# ---------------------------------------------------------------------------
# 4. Link safety
# ---------------------------------------------------------------------------


async def test_verified_pair_is_used_for_created(db: AsyncSession, capture: CaptureProvider) -> None:
    job = await _seed_easyweek_happy_path(db)
    params = await _run_and_get_params(db, capture, job)
    assert params[6] == VERIFIED_PAGE


async def test_hash_mismatch_falls_back_to_the_static_page(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    job = await _seed_easyweek_happy_path(db, booking_hash_id="90000002")
    params = await _run_and_get_params(db, capture, job)
    assert params[6] == STATIC_BOOKING_PAGE
    assert VERIFIED_PAGE not in params


@pytest.mark.parametrize(
    "hostile_link",
    [
        "https://evil.invalid/r/90000001",
        "https://eyw.me.evil.invalid/r/90000001",
        "http://eyw.me/r/90000001",
        "https://eyw.me:8443/r/90000001",
        "https://user:pw@eyw.me/r/90000001",
        "https://eyw.me/r/90000001?next=https://evil.invalid",
        "https://eyw.me/r/90000001#fragment",
        "https://eyw.me/booking/90000001",
        "https://eyw.me/r/90000001/../../evil",
        "//eyw.me/r/90000001",
        "javascript:alert(1)",
    ],
)
async def test_hostile_short_link_never_reaches_the_client(
    db: AsyncSession,
    capture: CaptureProvider,
    hostile_link: str,
) -> None:
    job = await _seed_easyweek_happy_path(db, short_link=hostile_link)

    params = await _run_and_get_params(db, capture, job)

    assert params[6] == STATIC_BOOKING_PAGE
    assert hostile_link not in params
    for value in params:
        assert "evil.invalid" not in value
        assert "javascript:" not in value


async def test_link_is_never_synthesised_from_uuid_or_hash(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """Hash and UUID are present, ``short_link`` is not — nothing may be built."""
    job = await _seed_easyweek_happy_path(db, short_link=None)

    params = await _run_and_get_params(db, capture, job)

    assert params[6] == STATIC_BOOKING_PAGE
    assert BOOKING_HASH not in params[6]
    assert "11111111-2222-4333-8444-555555555555" not in params[6]


async def test_canceled_ignores_even_a_verified_link(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    job = await _seed_easyweek_happy_path(
        db,
        job_type="record_canceled",
        meta_template_name=EASYWEEK_CANCELED_TEMPLATE,
        short_link=VERIFIED_PAGE,
        booking_hash_id=BOOKING_HASH,
    )

    params = await _run_and_get_params(db, capture, job)

    assert params[4] == STATIC_BOOKING_PAGE
    assert VERIFIED_PAGE not in params


async def test_no_safe_link_and_no_static_page_fails_locally(
    db: AsyncSession,
    capture: CaptureProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_booking_page_url", "", raising=False)
    job = await _seed_easyweek_happy_path(db, short_link=None)

    await _run_job(db, job)

    assert job.status == "failed"
    assert "missing required param #7" in (job.last_error or "")
    assert capture.template_calls == []
    assert capture.text_calls == []


async def test_canceled_without_static_page_fails_locally(
    db: AsyncSession,
    capture: CaptureProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_booking_page_url", "   ", raising=False)
    job = await _seed_easyweek_happy_path(
        db,
        job_type="record_canceled",
        meta_template_name=EASYWEEK_CANCELED_TEMPLATE,
    )

    await _run_job(db, job)

    assert job.status == "failed"
    assert "missing required param #5" in (job.last_error or "")
    assert capture.template_calls == []


async def test_easyweek_effective_link_helper_is_the_single_gate() -> None:
    """Unit-level restatement of the two rules the send path depends on."""

    class _Rec:
        short_link = VERIFIED_PAGE
        easyweek_booking_hash_id = BOOKING_HASH

    assert ow.easyweek_effective_booking_link(_Rec(), "record_created") == VERIFIED_PAGE  # type: ignore[arg-type]
    assert ow.easyweek_effective_booking_link(_Rec(), "record_updated") == VERIFIED_PAGE  # type: ignore[arg-type]
    assert (
        ow.easyweek_effective_booking_link(_Rec(), "record_canceled")  # type: ignore[arg-type]
        == (settings.easyweek_booking_page_url or "").strip()
    )
    assert (
        ow.easyweek_effective_booking_link(None, "record_created") == (settings.easyweek_booking_page_url or "").strip()
    )


# ---------------------------------------------------------------------------
# 5. Sender isolation
# ---------------------------------------------------------------------------


async def test_colliding_senders_resolve_per_provider(db: AsyncSession) -> None:
    altegio = _sender(provider=PROVIDER_ALTEGIO, company_id=COLLIDING_COMPANY_ID, phone_number_id="altegio-phone-id")
    easyweek = _sender(provider=PROVIDER_EASYWEEK, company_id=COLLIDING_COMPANY_ID, phone_number_id="eyw-phone-id")
    db.add_all([altegio, easyweek])
    await db.flush()

    eyw_id = await pick_sender_id(db, COLLIDING_COMPANY_ID, "default", provider=PROVIDER_EASYWEEK)
    alt_id = await pick_sender_id(db, COLLIDING_COMPANY_ID, "default", provider=PROVIDER_ALTEGIO)

    assert eyw_id == easyweek.id
    assert alt_id == altegio.id
    assert eyw_id != alt_id


async def test_missing_easyweek_sender_does_not_fall_through_to_altegio(db: AsyncSession) -> None:
    db.add(_sender(provider=PROVIDER_ALTEGIO, company_id=COLLIDING_COMPANY_ID, phone_number_id="altegio-phone-id"))
    await db.flush()

    assert await pick_sender_id(db, COLLIDING_COMPANY_ID, "default", provider=PROVIDER_EASYWEEK) is None
    assert await pick_sender_id_by_code(db, COLLIDING_COMPANY_ID, "default", provider=PROVIDER_EASYWEEK) is None


async def test_default_fallback_does_not_cross_the_provider_boundary(db: AsyncSession) -> None:
    """No ``vip`` sender for EasyWeek; the Altegio ``default`` must not fill in."""
    db.add(_sender(provider=PROVIDER_ALTEGIO, company_id=COLLIDING_COMPANY_ID, phone_number_id="altegio-phone-id"))
    await db.flush()

    assert await pick_sender_id(db, COLLIDING_COMPANY_ID, "vip", provider=PROVIDER_EASYWEEK) is None


async def test_default_fallback_works_inside_the_same_provider(db: AsyncSession) -> None:
    easyweek = _sender(provider=PROVIDER_EASYWEEK, company_id=COLLIDING_COMPANY_ID, phone_number_id="eyw-phone-id")
    db.add(easyweek)
    await db.flush()

    assert await pick_sender_id(db, COLLIDING_COMPANY_ID, "vip", provider=PROVIDER_EASYWEEK) == easyweek.id


async def test_inactive_easyweek_sender_is_not_selected(db: AsyncSession) -> None:
    db.add(
        _sender(
            provider=PROVIDER_EASYWEEK,
            company_id=COLLIDING_COMPANY_ID,
            phone_number_id="eyw-phone-id",
            is_active=False,
        )
    )
    await db.flush()

    assert await pick_sender_id(db, COLLIDING_COMPANY_ID, "default", provider=PROVIDER_EASYWEEK) is None


async def test_missing_easyweek_sender_fails_the_job_without_a_provider_call(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    db.add(_sender(provider=PROVIDER_ALTEGIO, company_id=COLLIDING_COMPANY_ID, phone_number_id="altegio-phone-id"))
    await db.flush()
    job = await _seed_easyweek_happy_path(db, with_sender=False)

    await _run_job(db, job)

    assert job.status == "failed"
    assert "No active sender" in (job.last_error or "")
    assert capture.template_calls == []
    assert capture.text_calls == []


async def test_easyweek_job_uses_the_easyweek_sender_end_to_end(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    altegio = _sender(provider=PROVIDER_ALTEGIO, company_id=COLLIDING_COMPANY_ID, phone_number_id="altegio-phone-id")
    db.add(altegio)
    await db.flush()
    job = await _seed_easyweek_happy_path(db)

    await _run_job(db, job)

    assert job.status == "done", job.last_error
    rows = await _outbox_rows(db, job)
    assert len(rows) == 1
    res = await db.execute(
        select(WhatsAppSender)
        .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
        .where(WhatsAppSender.id == rows[0].sender_id)
    )
    assert res.scalar_one().phone_number_id == "eyw-phone-id"
    assert rows[0].sender_id != altegio.id


async def test_easyweek_ignores_an_altegio_service_sender_rule(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """``service_sender_rules`` has no provider column — EasyWeek stays on default."""
    db.add(ServiceSenderRule(company_id=COLLIDING_COMPANY_ID, service_id=11, sender_code="vip"))
    db.add(
        _sender(
            provider=PROVIDER_ALTEGIO,
            company_id=COLLIDING_COMPANY_ID,
            sender_code="vip",
            phone_number_id="altegio-vip-phone-id",
        )
    )
    await db.flush()
    job = await _seed_easyweek_happy_path(db)

    await _run_job(db, job)

    assert job.status == "done", job.last_error
    rows = await _outbox_rows(db, job)
    res = await db.execute(select(WhatsAppSender).where(WhatsAppSender.id == rows[0].sender_id))
    chosen = res.scalar_one()
    assert chosen.provider == PROVIDER_EASYWEEK
    assert chosen.sender_code == "default"


# ---------------------------------------------------------------------------
# 6. Altegio regression
# ---------------------------------------------------------------------------


async def test_altegio_cross_company_universal_fallback_still_works(db: AsyncSession) -> None:
    row = _template(
        provider=PROVIDER_ALTEGIO,
        company_id=758285,
        code="review_3d",
        body="KA UNIVERSAL",
    )
    db.add(row)
    await db.flush()

    tmpl, used_lang = await ow._load_template(
        db,
        company_id=1271200,
        template_code="review_3d",
        language="de",
    )
    assert tmpl is not None and tmpl.id == row.id
    assert used_lang == "de"


async def test_altegio_cross_company_fallback_never_reaches_an_easyweek_row(db: AsyncSession) -> None:
    db.add(
        _template(
            provider=PROVIDER_EASYWEEK,
            company_id=758285,
            code="review_3d",
            body="EASYWEEK UNIVERSAL",
            meta_template_name="eyw_zzz_review_v9",
        )
    )
    await db.flush()

    tmpl, _lang = await ow._load_template(
        db,
        company_id=1271200,
        template_code="review_3d",
        language="de",
    )
    assert tmpl is None


async def test_altegio_branch_specific_code_still_skips_cross_company(db: AsyncSession) -> None:
    db.add(
        _template(
            provider=PROVIDER_ALTEGIO,
            company_id=758285,
            code="record_created",
            body="KA BRANCH SPECIFIC",
        )
    )
    await db.flush()

    tmpl, _lang = await ow._load_template(
        db,
        company_id=1271200,
        template_code="record_created",
        language="de",
    )
    assert tmpl is None


async def test_altegio_job_still_uses_the_hardcoded_meta_name(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """No ``meta_template_name`` on the row — the Altegio path must not need one."""
    job = await _seed_altegio_happy_path(db)

    await _run_job(db, job)

    assert job.status == "done", job.last_error
    assert len(capture.template_calls) == 1
    call = capture.template_calls[0]
    assert call["template_name"] == META_TEMPLATE_MAP[(COLLIDING_COMPANY_ID, "record_updated")]
    assert call["template_name"] == "kitilash_ka_record_updated_v1"
    # Altegio keeps its own positional contract: the 7th param is the raw
    # Altegio short_link, untouched by the EasyWeek link rules.
    assert len(call["params"]) == 7
    assert call["params"][6] == ALTEGIO_SHORT_LINK


async def test_altegio_sender_lookup_is_unchanged_when_provider_is_not_passed(db: AsyncSession) -> None:
    """Existing call sites pass no ``provider`` and must keep resolving Altegio."""
    altegio = _sender(provider=PROVIDER_ALTEGIO, company_id=COLLIDING_COMPANY_ID, phone_number_id="altegio-phone-id")
    easyweek = _sender(provider=PROVIDER_EASYWEEK, company_id=COLLIDING_COMPANY_ID, phone_number_id="eyw-phone-id")
    db.add_all([altegio, easyweek])
    await db.flush()

    assert await pick_sender_id(db, COLLIDING_COMPANY_ID, "default") == altegio.id
    assert await pick_sender_id_by_code(db, COLLIDING_COMPANY_ID, "default") == altegio.id
    assert await pick_sender_id(db, COLLIDING_COMPANY_ID, "vip") == altegio.id


async def test_altegio_job_without_provider_column_value_stays_on_the_altegio_path(db: AsyncSession) -> None:
    """A row whose ``provider`` is the server default must behave exactly as before."""
    res = await db.execute(select(MessageJob).limit(0))
    assert res.scalars().all() == []

    client = Client(
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        altegio_client_id=555003,
        phone_e164="+491700000003",
        display_name="Clara",
        raw={},
    )
    db.add(client)
    await db.flush()
    job = MessageJob(
        company_id=COLLIDING_COMPANY_ID,
        client_id=client.id,
        job_type="record_updated",
        run_at=datetime.now(timezone.utc),
        status="queued",
        dedupe_key="alt-default-provider-1",
        payload={},
    )
    db.add(job)
    await db.flush()
    await db.refresh(job)

    assert job.provider == PROVIDER_ALTEGIO


# ---------------------------------------------------------------------------
# 7. Domain scope — the Record and Client must belong to the job
# ---------------------------------------------------------------------------


async def _seed_easyweek_template_and_sender(db: AsyncSession, *, job_type: str = "record_created") -> None:
    db.add(
        _template(
            provider=PROVIDER_EASYWEEK,
            company_id=COLLIDING_COMPANY_ID,
            code=job_type,
            meta_template_name=EASYWEEK_CREATED_TEMPLATE,
        )
    )
    db.add(_sender(provider=PROVIDER_EASYWEEK, company_id=COLLIDING_COMPANY_ID, phone_number_id="eyw-phone-id"))
    await db.flush()


def _assert_scope_failure(job: MessageJob, capture: CaptureProvider) -> None:
    """Terminal local failure, no provider call, and nothing identifying in it."""
    assert job.status == "failed"
    assert job.locked_at is None
    err = job.last_error or ""
    assert "EasyWeek domain scope violation" in err
    assert capture.template_calls == []
    assert capture.text_calls == []
    for secret in (CLIENT_PHONE, CLIENT_EMAIL, "Anna", "Berta", "Tanja", "Wimpernverlängerung", BOOKING_HASH):
        assert secret not in err


async def test_easyweek_job_pointing_at_an_altegio_record_fails_closed(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    client = await _seed_easyweek_client(db)
    record = await _seed_easyweek_record(db, client, provider=PROVIDER_ALTEGIO)
    await _seed_easyweek_template_and_sender(db)
    job = await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type="record_created",
        record=record,
        client=client,
        dedupe_key="eyw-scope-altegio-record",
    )

    await _run_job(db, job)

    _assert_scope_failure(job, capture)
    assert "provider" in (job.last_error or "")


async def test_easyweek_job_pointing_at_another_locations_record_fails_closed(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    client = await _seed_easyweek_client(db)
    record = await _seed_easyweek_record(db, client, company_id=OTHER_EASYWEEK_COMPANY_ID)
    await _seed_easyweek_template_and_sender(db)
    job = await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type="record_created",
        record=record,
        client=client,
        dedupe_key="eyw-scope-other-location-record",
    )

    await _run_job(db, job)

    _assert_scope_failure(job, capture)
    assert "company" in (job.last_error or "")


async def test_easyweek_job_pointing_at_an_altegio_client_fails_closed(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    altegio_client = Client(
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        altegio_client_id=555010,
        phone_e164=CLIENT_PHONE,
        display_name="Berta",
        raw={},
    )
    db.add(altegio_client)
    await db.flush()
    record = await _seed_easyweek_record(db, altegio_client)
    await _seed_easyweek_template_and_sender(db)
    job = await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type="record_created",
        record=record,
        client=altegio_client,
        dedupe_key="eyw-scope-altegio-client",
    )

    await _run_job(db, job)

    _assert_scope_failure(job, capture)


async def test_easyweek_record_whose_client_is_another_providers_fails_closed(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """The job names no client, so it is resolved from the record — still checked."""
    altegio_client = Client(
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        altegio_client_id=555011,
        phone_e164=CLIENT_PHONE,
        display_name="Berta",
        raw={},
    )
    db.add(altegio_client)
    await db.flush()
    record = await _seed_easyweek_record(db, altegio_client)
    await _seed_easyweek_template_and_sender(db)
    job = await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type="record_created",
        record=record,
        client=None,
        dedupe_key="eyw-scope-record-foreign-client",
    )

    await _run_job(db, job)

    _assert_scope_failure(job, capture)


async def test_mismatched_job_and_record_client_ids_fail_closed(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    record_client = await _seed_easyweek_client(db)
    other_client = Client(
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        altegio_client_id=7300003,
        phone_e164="+491700000009",
        display_name="Clara",
        raw={},
    )
    db.add(other_client)
    await db.flush()
    record = await _seed_easyweek_record(db, record_client)
    await _seed_easyweek_template_and_sender(db)
    job = await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type="record_created",
        record=record,
        client=other_client,
        dedupe_key="eyw-scope-client-id-mismatch",
    )

    await _run_job(db, job)

    _assert_scope_failure(job, capture)
    assert "client_id" in (job.last_error or "")


async def test_client_from_another_company_fails_closed(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    foreign_client = Client(
        provider=PROVIDER_EASYWEEK,
        company_id=OTHER_EASYWEEK_COMPANY_ID,
        altegio_client_id=7300004,
        phone_e164=CLIENT_PHONE,
        display_name="Dora",
        raw={},
    )
    db.add(foreign_client)
    await db.flush()
    record = await _seed_easyweek_record(db, foreign_client)
    await _seed_easyweek_template_and_sender(db)
    job = await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type="record_created",
        record=record,
        client=foreign_client,
        dedupe_key="eyw-scope-foreign-company-client",
    )

    await _run_job(db, job)

    _assert_scope_failure(job, capture)


async def test_easyweek_lifecycle_job_without_a_record_fails_closed(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    client = await _seed_easyweek_client(db)
    await _seed_easyweek_template_and_sender(db)
    job = await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type="record_created",
        record=None,
        client=client,
        dedupe_key="eyw-scope-no-record",
    )

    await _run_job(db, job)

    _assert_scope_failure(job, capture)


async def test_altegio_job_is_untouched_by_the_easyweek_scope_gate(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """An Altegio job whose client_id differs from the record's still sends.

    The gate is deliberately EasyWeek-lifecycle-only. Altegio has always allowed
    a job to name a client other than the record's, and tightening that here
    would silently cancel live traffic this PR never set out to touch.
    """
    job = await _seed_altegio_happy_path(db, mismatched_client=True)

    await _run_job(db, job)

    assert job.status == "done", job.last_error
    assert len(capture.template_calls) == 1


# ---------------------------------------------------------------------------
# 8. Service snapshot — never invent a name or a price
# ---------------------------------------------------------------------------


def _assert_snapshot_failure(job: MessageJob, capture: CaptureProvider, fragment: str) -> None:
    assert job.status == "failed"
    err = job.last_error or ""
    assert fragment in err
    assert capture.template_calls == []
    assert capture.text_calls == []
    assert "None" not in err
    assert "0.00" not in err


async def test_unknown_service_title_fails_closed_instead_of_rendering_none(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    job = await _seed_easyweek_happy_path(db, services=((11, None, "60.00"),))

    await _run_job(db, job)

    _assert_snapshot_failure(job, capture, "no service title")


@pytest.mark.parametrize("blank_title", ["", "   ", "\t\n "])
async def test_blank_service_title_fails_closed(
    db: AsyncSession,
    capture: CaptureProvider,
    blank_title: str,
) -> None:
    job = await _seed_easyweek_happy_path(db, services=((11, blank_title, "60.00"),))

    await _run_job(db, job)

    _assert_snapshot_failure(job, capture, "no service title")


async def test_unknown_price_fails_closed_instead_of_rendering_zero(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """``NULL`` is "nobody knows", not "free" — 0.00 would be a lie."""
    job = await _seed_easyweek_happy_path(db, services=((11, "Wimpernverlängerung", None),), total_cost="60.00")

    await _run_job(db, job)

    _assert_snapshot_failure(job, capture, "no price")


async def test_unknown_title_and_price_together_fail_closed(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    job = await _seed_easyweek_happy_path(db, services=((11, None, None),), total_cost=None)

    await _run_job(db, job)

    _assert_snapshot_failure(job, capture, "EasyWeek service snapshot")


async def test_unknown_record_total_cost_fails_closed(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    job = await _seed_easyweek_happy_path(db, total_cost=None)

    await _run_job(db, job)

    _assert_snapshot_failure(job, capture, "no total_cost")


async def test_divergent_record_total_and_service_cost_fail_closed(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """PR-4 keeps them identical; a divergence means somebody else wrote one."""
    job = await _seed_easyweek_happy_path(
        db,
        services=((11, "Wimpernverlängerung", "60.00"),),
        total_cost="35.00",
    )

    await _run_job(db, job)

    _assert_snapshot_failure(job, capture, "disagree")


async def test_multi_row_service_snapshot_fails_closed(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """PR-4 stores one flat service per booking; two rows means stale state."""
    job = await _seed_easyweek_happy_path(
        db,
        services=((11, "Wimpernverlängerung", "60.00"), (12, "Auffüllen", "25.50")),
        total_cost="60.00",
    )

    await _run_job(db, job)

    _assert_snapshot_failure(job, capture, "exactly one service")


async def test_a_real_zero_price_is_a_valid_snapshot(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """The gate rejects UNKNOWN, not zero — a free service must still send."""
    job = await _seed_easyweek_happy_path(db, services=((11, "Probetermin", "0.00"),), total_cost="0.00")

    params = await _run_and_get_params(db, capture, job)

    assert params[4] == "Probetermin — 0.00€"
    assert params[5] == "0.00"


async def test_a_known_consistent_snapshot_sends(db: AsyncSession, capture: CaptureProvider) -> None:
    job = await _seed_easyweek_happy_path(db)

    params = await _run_and_get_params(db, capture, job)

    assert params[4] == "Wimpernverlängerung — 60.00€"
    assert params[5] == "60.00"
    assert "None" not in params


async def test_altegio_keeps_its_lenient_service_formatting(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """The strict snapshot contract is EasyWeek-only; Altegio policy is unchanged."""
    job = await _seed_altegio_happy_path(db, service_title=None, service_cost=None, total_cost=None)

    await _run_job(db, job)

    assert job.status == "done", job.last_error
    assert len(capture.template_calls) == 1
    assert capture.template_calls[0]["params"][4] == "None — 0.00€"


# ---------------------------------------------------------------------------
# 9. Effective Meta language — the language of the row actually used
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("db_language", ["de", "en"])
async def test_meta_language_matches_the_selected_db_row(
    db: AsyncSession,
    capture: CaptureProvider,
    monkeypatch: pytest.MonkeyPatch,
    db_language: str,
) -> None:
    monkeypatch.setattr(settings, "easyweek_default_language", db_language, raising=False)
    job = await _seed_easyweek_happy_path(db, language=db_language)

    await _run_job(db, job)

    assert job.status == "done", job.last_error
    assert capture.template_calls[0]["language"] == db_language
    rows = await _outbox_rows(db, job)
    assert rows[0].language == db_language
    assert rows[0].meta["lang"] == db_language


async def test_language_fallback_sends_the_row_language_not_the_requested_one(
    db: AsyncSession,
    capture: CaptureProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Requested ``en``, only a ``de`` row exists — Meta must be told ``de``.

    Telling Meta ``en`` here is a guaranteed rejection: the body and the
    template name came from the ``de`` row.
    """
    monkeypatch.setattr(settings, "easyweek_default_language", "en", raising=False)
    job = await _seed_easyweek_happy_path(db, language="de")

    await _run_job(db, job)

    assert job.status == "done", job.last_error
    assert capture.template_calls[0]["language"] == "de"
    rows = await _outbox_rows(db, job)
    assert rows[0].language == "de"
    assert rows[0].meta["lang"] == "de"


async def test_preflight_audit_records_the_row_language(
    db: AsyncSession,
    capture: CaptureProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_default_language", "en", raising=False)
    monkeypatch.setattr(ow, "build_lifecycle_template_params", lambda code, ctx: ["a", "b", "c"])
    job = await _seed_easyweek_happy_path(db, language="en")

    await _run_job(db, job)

    assert job.status == "failed"
    rows = await _outbox_rows(db, job)
    assert rows[0].meta["lang"] == "en"
    assert rows[0].meta["validation"] == "local_preflight_failure"
    assert capture.template_calls == []


async def test_template_fallback_after_a_window_policy_error_uses_the_row_language(
    db: AsyncSession,
    capture: CaptureProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The 24h text route fails with a policy error; the template retry must not
    silently revert to the global ``de``."""
    monkeypatch.setattr(settings, "easyweek_default_language", "en", raising=False)
    _enable_text_inside_24h(monkeypatch, window_open=True)

    async def _policy_error_text(*args: Any, **kwargs: Any) -> tuple[None, str]:
        capture.text_calls.append(dict(kwargs))
        return None, "131047: Re-engagement message outside the allowed window"

    monkeypatch.setattr(ow, "safe_send", _policy_error_text)
    job = await _seed_easyweek_happy_path(db, language="en")

    await _run_job(db, job)

    assert job.status == "done", job.last_error
    assert len(capture.text_calls) == 1
    assert len(capture.template_calls) == 1
    assert capture.template_calls[0]["language"] == "en"
    rows = await _outbox_rows(db, job)
    assert rows[0].meta["original_template_language"] == "en"


async def test_altegio_still_sends_the_global_template_language(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    job = await _seed_altegio_happy_path(db)

    await _run_job(db, job)

    assert job.status == "done", job.last_error
    assert capture.template_calls[0]["language"] == TEMPLATE_LANGUAGE
    assert TEMPLATE_LANGUAGE == "de"
    rows = await _outbox_rows(db, job)
    assert rows[0].meta["lang"] == TEMPLATE_LANGUAGE


# ---------------------------------------------------------------------------
# 10. Altegio pre-appointment notes stay out of EasyWeek
# ---------------------------------------------------------------------------


def _enable_text_inside_24h(monkeypatch: pytest.MonkeyPatch, *, window_open: bool) -> None:
    monkeypatch.setattr(settings, "bot_template_text_inside_24h_enabled", True, raising=False)
    monkeypatch.setattr(settings, "bot_template_text_inside_24h_fallback_enabled", True, raising=False)

    async def _fake_window(**kwargs: Any) -> tuple[bool, datetime | None]:
        return window_open, datetime.now(timezone.utc) if window_open else None

    monkeypatch.setattr(ow, "is_whatsapp_customer_window_open", _fake_window)


async def test_new_easyweek_client_gets_no_altegio_pre_appointment_notes(
    db: AsyncSession,
    capture: CaptureProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """First-ever booking, German template, open window → text route.

    This is exactly the shape that triggers ``PRE_APPOINTMENT_NOTES_DE`` on
    Altegio, and the text route is where a body actually reaches the customer
    verbatim. The notes are KitiLash lash-prep copy — EasyWeek must not carry it.
    """
    _enable_text_inside_24h(monkeypatch, window_open=True)
    job = await _seed_easyweek_happy_path(db, language="de")
    # A first booking: no earlier record exists for this client.
    tmpl_res = await db.execute(select(MessageTemplate).where(MessageTemplate.provider == PROVIDER_EASYWEEK))
    tmpl_res.scalars().one().body = "Hallo {{1}}, Termin am {{3}} um {{4}}.{pre_appointment_notes}"
    await db.flush()

    await _run_job(db, job)

    assert job.status == "done", job.last_error
    assert len(capture.text_calls) == 1
    body = capture.text_calls[0]["text"]
    assert PRE_APPOINTMENT_NOTES_DE not in body
    assert "Anna Müller" in body, "the template still rendered normally"
    rows = await _outbox_rows(db, job)
    assert PRE_APPOINTMENT_NOTES_DE not in (rows[0].body or "")


async def test_easyweek_render_context_has_empty_pre_appointment_notes(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    client = await _seed_easyweek_client(db)
    record = await _seed_easyweek_record(db, client)
    await _seed_easyweek_template_and_sender(db)

    _body, _sender_id, _lang, ctx = await ow._render_message(
        db,
        company_id=COLLIDING_COMPANY_ID,
        template_code="record_created",
        record=record,
        client=client,
        provider=PROVIDER_EASYWEEK,
    )

    assert ctx["pre_appointment_notes"] == ""
    assert capture.template_calls == []


async def test_altegio_new_client_still_gets_its_pre_appointment_notes(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """The Altegio behaviour this PR must not disturb."""
    client, record = await _seed_altegio_client_and_record(db)
    db.add(
        _template(
            provider=PROVIDER_ALTEGIO,
            company_id=COLLIDING_COMPANY_ID,
            code="record_created",
            body="ALTEGIO BODY",
        )
    )
    db.add(_sender(provider=PROVIDER_ALTEGIO, company_id=COLLIDING_COMPANY_ID, phone_number_id="altegio-phone-id"))
    await db.flush()

    _body, _sender_id, _lang, ctx = await ow._render_message(
        db,
        company_id=COLLIDING_COMPANY_ID,
        template_code="record_created",
        record=record,
        client=client,
        provider=PROVIDER_ALTEGIO,
    )

    assert ctx["pre_appointment_notes"] == PRE_APPOINTMENT_NOTES_DE


# ---------------------------------------------------------------------------
# 11. Static booking page validation
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "raw,expected",
    [
        ("https://book.example.com/durlach", "https://book.example.com/durlach"),
        ("  https://book.example.com/durlach  ", "https://book.example.com/durlach"),
        ("https://book.example.com/durlach?loc=1", "https://book.example.com/durlach?loc=1"),
        ("https://book.example.com", "https://book.example.com"),
    ],
)
async def test_valid_static_booking_pages_are_accepted_and_normalized(raw: str, expected: str) -> None:
    assert validate_static_booking_page(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "http://book.example.com/durlach",
        "javascript:alert(1)",
        "//book.example.com/durlach",
        "book.example.com/durlach",
        "https://user:pw@book.example.com/durlach",
        "https://:pw@book.example.com/durlach",
        "https://book.example.com/durlach#frag",
        "https://book.example.com:notaport/durlach",
        "https://[oops/durlach",
        "https:///durlach",
        "",
        "   ",
        "\t\n ",
        "https://book.example.com/dur\nlach",
        "https://book.example.com/dur\tlach",
        "https://book.example.com/dur\x00lach",
        "https://book.example.com/dur lach",
        "﻿https://book.example.com/durlach",
        "data:text/html,<h1>x</h1>",
        "ftp://book.example.com/durlach",
    ],
)
async def test_unsafe_static_booking_pages_are_rejected(raw: str) -> None:
    assert validate_static_booking_page(raw) is None


@pytest.mark.parametrize("raw", [None, 123, b"https://book.example.com/", ["https://x"]])
async def test_non_string_static_booking_pages_are_rejected(raw: Any) -> None:
    assert validate_static_booking_page(raw) is None


async def test_no_host_allowlist_is_applied() -> None:
    """PR-6 decides the approved host; guessing one here would block the real value."""
    assert validate_static_booking_page("https://eyw.me/book") == "https://eyw.me/book"
    assert validate_static_booking_page("https://any-other-host.example/book") is not None


@pytest.mark.parametrize(
    "bad_static",
    ["http://book.example.com/x", "javascript:alert(1)", "//book.example.com/x", "https://u:p@book.example.com/x"],
)
async def test_invalid_static_page_fails_canceled_locally(
    db: AsyncSession,
    capture: CaptureProvider,
    monkeypatch: pytest.MonkeyPatch,
    bad_static: str,
) -> None:
    """``record_canceled`` has no other link, so an unusable static page is fatal."""
    monkeypatch.setattr(settings, "easyweek_booking_page_url", bad_static, raising=False)
    job = await _seed_easyweek_happy_path(
        db,
        job_type="record_canceled",
        meta_template_name=EASYWEEK_CANCELED_TEMPLATE,
    )

    await _run_job(db, job)

    assert job.status == "failed"
    assert "missing required param #5" in (job.last_error or "")
    assert capture.template_calls == []
    assert capture.text_calls == []
    assert bad_static not in (job.last_error or "")


async def test_invalid_static_page_fails_created_when_manage_link_is_unusable(
    db: AsyncSession,
    capture: CaptureProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "easyweek_booking_page_url", "http://book.example.com/x", raising=False)
    job = await _seed_easyweek_happy_path(db, short_link=None)

    await _run_job(db, job)

    assert job.status == "failed"
    assert "missing required param #7" in (job.last_error or "")
    assert capture.template_calls == []


async def test_a_valid_manage_link_survives_an_invalid_static_page(
    db: AsyncSession,
    capture: CaptureProvider,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The static page is a fallback, not a filter — a verified link still wins."""
    monkeypatch.setattr(settings, "easyweek_booking_page_url", "javascript:alert(1)", raising=False)
    job = await _seed_easyweek_happy_path(db)

    params = await _run_and_get_params(db, capture, job)

    assert params[6] == VERIFIED_PAGE
    assert "javascript:" not in "".join(params)


# ---------------------------------------------------------------------------
# 12. Early EasyWeek job-type allowlist
# ---------------------------------------------------------------------------

_DISALLOWED_EASYWEEK_JOB_TYPES = [
    "review_3d",
    "repeat_10d",
    "comeback_3d",
    "reminder_24h",
    "reminder_2h",
    "promo_eligibility_check",
    "promo_apply_existing_booking",
    "promo_card_booking_reminder",
    "newsletter_new_clients_followup",
    "newsletter_new_clients_monthly",
    CAMPAIGN_EXECUTION_JOB_TYPE,
]


@pytest.mark.parametrize("job_type", _DISALLOWED_EASYWEEK_JOB_TYPES)
async def test_disallowed_easyweek_job_types_fail_terminally_in_the_outbox(
    db: AsyncSession,
    capture: CaptureProvider,
    monkeypatch: pytest.MonkeyPatch,
    job_type: str,
) -> None:
    """No routing, no handler, no Altegio API, no requeue — a terminal failure.

    ``campaign_execution`` is included on purpose: reaching the outbox worker
    normally requeues it, which for an EasyWeek job would be an infinite loop
    between two workers that both refuse it.
    """
    altegio_api = AsyncMock(side_effect=AssertionError("no Altegio API call for a rejected job"))
    monkeypatch.setattr(ow, "_process_promo_card_booking_reminder", altegio_api)
    monkeypatch.setattr(ow, "process_promo_eligibility_check_job", altegio_api)
    monkeypatch.setattr(ow, "process_promo_apply_existing_booking_job", altegio_api)
    monkeypatch.setattr(ow, "_load_record", AsyncMock(side_effect=AssertionError("no row load")))
    monkeypatch.setattr(ow, "_load_client", AsyncMock(side_effect=AssertionError("no row load")))

    client = await _seed_easyweek_client(db)
    job = await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type=job_type,
        record=None,
        client=client,
        dedupe_key=f"eyw-disallowed-{job_type}",
    )

    await _run_job(db, job)

    assert job.status == "failed"
    assert job.locked_at is None
    assert job.last_error == f"EasyWeek job type not enabled in this phase: {job_type}"
    assert capture.template_calls == []
    assert capture.text_calls == []
    assert await _outbox_rows(db, job) == []
    altegio_api.assert_not_awaited()


@pytest.mark.parametrize("job_type", sorted(EASYWEEK_LIFECYCLE_JOB_TYPES))
async def test_the_three_lifecycle_types_are_allowed(job_type: str) -> None:
    assert easyweek_job_type_error(PROVIDER_EASYWEEK, job_type) is None


@pytest.mark.parametrize("job_type", _DISALLOWED_EASYWEEK_JOB_TYPES)
async def test_altegio_is_never_blocked_by_the_easyweek_allowlist(job_type: str) -> None:
    assert easyweek_job_type_error(PROVIDER_ALTEGIO, job_type) is None


async def test_the_allowlist_has_exactly_one_definition() -> None:
    """Two copies of a security boundary drift; the inbox worker re-exports this one."""
    assert eyw_worker.EASYWEEK_LIFECYCLE_JOB_TYPES is EASYWEEK_LIFECYCLE_JOB_TYPES
    assert ow.EASYWEEK_LIFECYCLE_JOB_TYPES is EASYWEEK_LIFECYCLE_JOB_TYPES
    assert EASYWEEK_LIFECYCLE_JOB_TYPES == {"record_created", "record_updated", "record_canceled"}


async def test_altegio_campaign_execution_job_is_still_requeued(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """The pre-existing hand-off to campaign_worker must survive the new guard."""
    client, _record = await _seed_altegio_client_and_record(db)
    job = await _seed_job(
        db,
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        job_type=CAMPAIGN_EXECUTION_JOB_TYPE,
        record=None,
        client=client,
        dedupe_key="alt-campaign-execution-1",
    )

    await _run_job(db, job)

    assert job.status == "queued"
    assert job.locked_at is None
    assert capture.template_calls == []


# ---------------------------------------------------------------------------
# 13. Campaign worker provider guard
# ---------------------------------------------------------------------------


async def test_easyweek_campaign_execution_job_dies_in_the_campaign_worker(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The outbox guard does not cover this path — campaign jobs are claimed here."""
    runner = AsyncMock(side_effect=AssertionError("campaign runner must not be reached"))
    monkeypatch.setattr(campaign_worker, "execute_queued_send_real", runner)

    client = await _seed_easyweek_client(db)
    job = await _seed_job(
        db,
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        job_type=CAMPAIGN_EXECUTION_JOB_TYPE,
        record=None,
        client=client,
        dedupe_key="eyw-campaign-execution-1",
    )
    job.payload = {"campaign_run_id": 4242}
    await db.flush()

    await campaign_worker.process_job_in_session(db, job.id)
    await db.flush()
    await db.refresh(job)

    assert job.status == "failed"
    assert job.locked_at is None
    assert job.last_error == f"EasyWeek job type not enabled in this phase: {CAMPAIGN_EXECUTION_JOB_TYPE}"
    runner.assert_not_awaited()


async def test_altegio_campaign_execution_job_still_reaches_the_runner(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[int] = []

    async def _fake_runner(run_id: int) -> None:
        seen.append(run_id)

    monkeypatch.setattr(campaign_worker, "execute_queued_send_real", _fake_runner)

    client, _record = await _seed_altegio_client_and_record(db)
    job = await _seed_job(
        db,
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        job_type=CAMPAIGN_EXECUTION_JOB_TYPE,
        record=None,
        client=client,
        dedupe_key="alt-campaign-execution-2",
    )
    job.payload = {"campaign_run_id": 4242}
    await db.flush()

    await campaign_worker.process_job_in_session(db, job.id)
    await db.flush()
    await db.refresh(job)

    assert job.status == "done", job.last_error
    assert seen == [4242]


# ---------------------------------------------------------------------------
# 14. Delivery retry inherits the provider of the chain
# ---------------------------------------------------------------------------


class _NullProvider(WhatsAppProvider):
    """Satisfies handle_event's signature; sending through it is a test failure."""

    async def send(
        self,
        sender_id: int,
        phone_e164: str,
        text: str,
        contact_name: str | None = None,
    ) -> str:
        raise AssertionError("the status webhook path must not send anything")


def _failed_status_payload(wamid: str, *, code: int = 131000) -> dict[str, Any]:
    return {
        "object": "whatsapp_business_account",
        "entry": [
            {
                "id": "WABA",
                "changes": [
                    {
                        "field": "messages",
                        "value": {
                            "metadata": {"phone_number_id": "eyw-phone-id"},
                            "statuses": [
                                {
                                    "id": wamid,
                                    "status": "failed",
                                    "timestamp": "1700000001",
                                    "recipient_id": "491700000001",
                                    "errors": [
                                        {
                                            "code": code,
                                            "title": "Temporary failure",
                                            "error_data": {"details": "transient provider failure"},
                                        }
                                    ],
                                }
                            ],
                        },
                    }
                ],
            }
        ],
    }


async def _deliver_failed_status(db: AsyncSession, wamid: str, *, dedupe: str) -> None:
    evt = WhatsAppEvent(
        dedupe_key=dedupe,
        status="received",
        payload=_failed_status_payload(wamid),
        query={},
        headers={},
    )
    db.add(evt)
    await db.flush()
    await wa_worker.handle_event(db, evt, _NullProvider())
    await db.flush()


@pytest.fixture
def no_contact_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    """Let a retry send in the same test as its original.

    The first send records a per-phone rate limit, so a retry running seconds
    later is legitimately deferred in production. These tests are about which
    PROVIDER the retry sends as, not about pacing.
    """

    async def _no_delay(session: Any, phone: str) -> None:
        return None

    monkeypatch.setattr(ow, "_apply_rate_limit", _no_delay)


async def _retry_jobs_for(db: AsyncSession, outbox_id: int) -> list[MessageJob]:
    res = await db.execute(
        select(MessageJob).where(MessageJob.dedupe_key.like(f"delivery_retry:{outbox_id}:%")).order_by(MessageJob.id)
    )
    return list(res.scalars().all())


async def test_easyweek_delivery_retry_inherits_easyweek_end_to_end(
    db: AsyncSession,
    capture: CaptureProvider,
    no_contact_rate_limit: None,
) -> None:
    """Send → failed callback → retry → send again, all inside EasyWeek.

    A colliding Altegio template and sender exist on the same numeric company for
    the whole run: if the retry lost its provider, this is exactly what it would
    pick up.
    """
    db.add(
        _template(
            provider=PROVIDER_ALTEGIO,
            company_id=COLLIDING_COMPANY_ID,
            code="record_created",
            body="ALTEGIO BODY",
        )
    )
    altegio_sender = _sender(
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        phone_number_id="altegio-phone-id",
    )
    db.add(altegio_sender)
    await db.flush()

    job = await _seed_easyweek_happy_path(db)
    await _run_job(db, job)
    assert job.status == "done", job.last_error

    rows = await _outbox_rows(db, job)
    assert len(rows) == 1
    original_outbox_id = rows[0].id
    wamid = rows[0].provider_message_id
    assert wamid is not None

    await _deliver_failed_status(db, wamid, dedupe="wa:eyw-failed-1")

    retries = await _retry_jobs_for(db, original_outbox_id)
    assert len(retries) == 1
    retry = retries[0]
    assert retry.provider == PROVIDER_EASYWEEK
    assert retry.company_id == job.company_id
    assert retry.record_id == job.record_id
    assert retry.client_id == job.client_id
    assert retry.job_type == "record_created"

    # The retry actually sends, and it sends as EasyWeek.
    retry.run_at = datetime.now(timezone.utc)
    await db.flush()
    capture.template_calls.clear()
    await _run_job(db, retry)

    assert retry.status == "done", retry.last_error
    assert len(capture.template_calls) == 1
    assert capture.template_calls[0]["template_name"] == EASYWEEK_CREATED_TEMPLATE
    retry_rows = await _outbox_rows(db, retry)
    assert len(retry_rows) == 1
    assert retry_rows[0].sender_id != altegio_sender.id
    sender_res = await db.execute(select(WhatsAppSender).where(WhatsAppSender.id == retry_rows[0].sender_id))
    assert sender_res.scalar_one().provider == PROVIDER_EASYWEEK


async def test_second_attempt_in_the_same_chain_also_stays_easyweek(
    db: AsyncSession,
    capture: CaptureProvider,
    no_contact_rate_limit: None,
) -> None:
    job = await _seed_easyweek_happy_path(db)
    await _run_job(db, job)
    rows = await _outbox_rows(db, job)
    original_outbox_id = rows[0].id

    await _deliver_failed_status(db, rows[0].provider_message_id, dedupe="wa:eyw-chain-1")
    retry = (await _retry_jobs_for(db, original_outbox_id))[0]

    retry.run_at = datetime.now(timezone.utc)
    await db.flush()
    await _run_job(db, retry)
    retry_rows = await _outbox_rows(db, retry)
    assert len(retry_rows) == 1

    await _deliver_failed_status(db, retry_rows[0].provider_message_id, dedupe="wa:eyw-chain-2")

    chain = await _retry_jobs_for(db, original_outbox_id)
    assert len(chain) == 2
    assert {j.provider for j in chain} == {PROVIDER_EASYWEEK}
    assert chain[1].payload["delivery_retry_attempt"] == 2


async def test_altegio_delivery_retry_still_stays_altegio(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    job = await _seed_altegio_happy_path(db, job_type="record_created")
    await _run_job(db, job)
    assert job.status == "done", job.last_error

    rows = await _outbox_rows(db, job)
    await _deliver_failed_status(db, rows[0].provider_message_id, dedupe="wa:alt-failed-1")

    retries = await _retry_jobs_for(db, rows[0].id)
    assert len(retries) == 1
    assert retries[0].provider == PROVIDER_ALTEGIO
    assert retries[0].job_type == "record_created"
    assert retries[0].status == "queued"


async def _outbox_without_a_usable_job(
    db: AsyncSession,
    capture: CaptureProvider,
    mutate: Any,
) -> OutboxMessage:
    """Send an EasyWeek job, then break the link the retry has to prove."""
    job = await _seed_easyweek_happy_path(db)
    await _run_job(db, job)
    rows = await _outbox_rows(db, job)
    await mutate(job, rows[0])
    await db.flush()
    return rows[0]


async def test_missing_original_job_refuses_the_retry(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    async def _detach(job: MessageJob, outbox: OutboxMessage) -> None:
        outbox.job_id = None

    outbox = await _outbox_without_a_usable_job(db, capture, _detach)

    await _deliver_failed_status(db, outbox.provider_message_id, dedupe="wa:eyw-no-job")

    assert await _retry_jobs_for(db, outbox.id) == []
    await db.refresh(outbox)
    assert outbox.meta["delivery_retry_skipped"] is True
    assert outbox.meta["delivery_retry_skip_reason"] == "original_job_missing"


@pytest.mark.parametrize(
    "field,value,reason",
    [
        ("company_id", 999123, "company_mismatch"),
        ("record_id", None, "record_mismatch"),
        ("job_type", "record_updated", "job_type_mismatch"),
    ],
)
async def test_job_and_anchor_outbox_divergence_refuses_the_retry(
    db: AsyncSession,
    capture: CaptureProvider,
    field: str,
    value: Any,
    reason: str,
) -> None:
    async def _diverge(job: MessageJob, outbox: OutboxMessage) -> None:
        setattr(job, field, value)

    outbox = await _outbox_without_a_usable_job(db, capture, _diverge)

    await _deliver_failed_status(db, outbox.provider_message_id, dedupe=f"wa:eyw-diverge-{field}")

    assert await _retry_jobs_for(db, outbox.id) == []
    await db.refresh(outbox)
    assert outbox.meta["delivery_retry_skip_reason"] == reason


async def test_blank_provider_on_the_original_job_refuses_the_retry(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """Provider must be PROVEN, never guessed from the colliding company id."""

    async def _blank(job: MessageJob, outbox: OutboxMessage) -> None:
        job.provider = "   "

    outbox = await _outbox_without_a_usable_job(db, capture, _blank)

    await _deliver_failed_status(db, outbox.provider_message_id, dedupe="wa:eyw-blank-provider")

    assert await _retry_jobs_for(db, outbox.id) == []
    await db.refresh(outbox)
    assert outbox.meta["delivery_retry_skip_reason"] == "original_job_provider_unknown"


async def test_easyweek_retry_refused_when_the_record_is_not_easyweek(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    async def _flip(job: MessageJob, outbox: OutboxMessage) -> None:
        record = await db.get(Record, job.record_id)
        record.provider = PROVIDER_ALTEGIO

    outbox = await _outbox_without_a_usable_job(db, capture, _flip)

    await _deliver_failed_status(db, outbox.provider_message_id, dedupe="wa:eyw-foreign-record")

    assert await _retry_jobs_for(db, outbox.id) == []
    await db.refresh(outbox)
    assert outbox.meta["delivery_retry_skip_reason"] == "easyweek_retry_record_provider_mismatch"


async def test_easyweek_retry_refused_when_the_client_is_not_easyweek(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    async def _flip(job: MessageJob, outbox: OutboxMessage) -> None:
        client = await db.get(Client, job.client_id)
        client.provider = PROVIDER_ALTEGIO

    outbox = await _outbox_without_a_usable_job(db, capture, _flip)

    await _deliver_failed_status(db, outbox.provider_message_id, dedupe="wa:eyw-foreign-client")

    assert await _retry_jobs_for(db, outbox.id) == []
    await db.refresh(outbox)
    assert outbox.meta["delivery_retry_skip_reason"] == "easyweek_retry_client_provider_mismatch"


async def test_easyweek_retry_refused_when_the_client_is_from_another_company(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    async def _move(job: MessageJob, outbox: OutboxMessage) -> None:
        client = await db.get(Client, job.client_id)
        assert client is not None
        client.company_id = OTHER_EASYWEEK_COMPANY_ID

    outbox = await _outbox_without_a_usable_job(db, capture, _move)

    await _deliver_failed_status(db, outbox.provider_message_id, dedupe="wa:eyw-foreign-client-company")

    assert await _retry_jobs_for(db, outbox.id) == []
    await db.refresh(outbox)
    assert outbox.meta["delivery_retry_skip_reason"] == "easyweek_retry_client_company_mismatch"


async def test_easyweek_retry_refused_when_the_record_reference_is_missing(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    async def _strip(job: MessageJob, outbox: OutboxMessage) -> None:
        job.record_id = None
        outbox.record_id = None

    outbox = await _outbox_without_a_usable_job(db, capture, _strip)

    await _deliver_failed_status(db, outbox.provider_message_id, dedupe="wa:eyw-no-record")

    assert await _retry_jobs_for(db, outbox.id) == []
    await db.refresh(outbox)
    assert outbox.meta["delivery_retry_skip_reason"] == "easyweek_retry_missing_record"


async def test_a_non_null_job_client_that_contradicts_the_record_is_refused(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """A job that NAMES a client and names a different one than the record.

    This is a contradiction, not a partial delivery — the fallback in
    :func:`resolve_retry_identity` only applies when the job names no client at
    all, so this must stay a hard refusal.
    """

    async def _contradict(job: MessageJob, outbox: OutboxMessage) -> None:
        other = Client(
            provider=PROVIDER_EASYWEEK,
            company_id=COLLIDING_COMPANY_ID,
            altegio_client_id=7300055,
            phone_e164="+491700000055",
            display_name="Frida",
            raw={},
        )
        db.add(other)
        await db.flush()
        job.client_id = other.id
        outbox.client_id = other.id

    outbox = await _outbox_without_a_usable_job(db, capture, _contradict)

    await _deliver_failed_status(db, outbox.provider_message_id, dedupe="wa:eyw-contradicting-client")

    assert await _retry_jobs_for(db, outbox.id) == []
    await db.refresh(outbox)
    assert outbox.meta["delivery_retry_skip_reason"] == "easyweek_retry_job_client_conflicts_with_record"


async def test_anchor_outbox_client_that_disagrees_with_the_effective_client_is_refused(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """The outbox row must name the client the send actually used."""

    async def _diverge(job: MessageJob, outbox: OutboxMessage) -> None:
        other = Client(
            provider=PROVIDER_EASYWEEK,
            company_id=COLLIDING_COMPANY_ID,
            altegio_client_id=7300056,
            phone_e164="+491700000056",
            display_name="Gerda",
            raw={},
        )
        db.add(other)
        await db.flush()
        outbox.client_id = other.id

    outbox = await _outbox_without_a_usable_job(db, capture, _diverge)

    await _deliver_failed_status(db, outbox.provider_message_id, dedupe="wa:eyw-anchor-client")

    assert await _retry_jobs_for(db, outbox.id) == []
    await db.refresh(outbox)
    assert outbox.meta["delivery_retry_skip_reason"] == "client_mismatch"


async def test_easyweek_retry_refused_when_the_record_has_no_client(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """Nothing to fall back to: neither the job nor the record names a client."""

    async def _strip(job: MessageJob, outbox: OutboxMessage) -> None:
        job.client_id = None
        record = await db.get(Record, job.record_id)
        assert record is not None
        record.client_id = None
        outbox.client_id = None

    outbox = await _outbox_without_a_usable_job(db, capture, _strip)

    await _deliver_failed_status(db, outbox.provider_message_id, dedupe="wa:eyw-no-client-anywhere")

    assert await _retry_jobs_for(db, outbox.id) == []
    await db.refresh(outbox)
    assert outbox.meta["delivery_retry_skip_reason"] == "easyweek_retry_missing_client"


async def test_an_existing_retry_with_the_wrong_provider_is_not_adopted(
    db: AsyncSession,
) -> None:
    """An Altegio row on the dedupe key is refused, not reused.

    Exercised at ``_create_delivery_retry_job_idempotent`` because that is the
    only place the adoption can happen: a concurrent writer wins the unique
    index and the loser reads the row back. Returning it would hand an EasyWeek
    chain a job that renders from Altegio templates and sends from the Altegio
    number.
    """
    client = await _seed_easyweek_client(db)
    record = await _seed_easyweek_record(db, client)
    squatter = MessageJob(
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        record_id=record.id,
        client_id=client.id,
        job_type="record_created",
        run_at=datetime.now(timezone.utc),
        status="queued",
        dedupe_key="delivery_retry:4242:1",
        payload={},
    )
    db.add(squatter)
    await db.flush()

    identity = RetryIdentity(
        provider=PROVIDER_EASYWEEK,
        company_id=COLLIDING_COMPANY_ID,
        record_id=record.id,
        client_id=client.id,
        job_type="record_created",
    )
    assert identity.mismatch_field(squatter) == "provider"

    refused = await wa_worker._create_delivery_retry_job_idempotent(
        db,
        dedupe_key="delivery_retry:4242:1",
        identity=identity,
        status="queued",
        run_at=datetime.now(timezone.utc),
        attempts=0,
        max_attempts=5,
        payload={},
    )
    assert refused is None

    await db.flush()
    await db.refresh(squatter)
    assert squatter.provider == PROVIDER_ALTEGIO, "the existing row must not be mutated"
    assert squatter.status == "canceled", "a racing mismatched winner must not remain sendable"
    assert squatter.locked_at is None

    # A different, matching row IS adopted — the guard rejects mismatches, not
    # idempotency. The terminal conflicting row above remains historical.
    matching = MessageJob(
        **identity.as_job_fields(),
        run_at=datetime.now(timezone.utc),
        status="queued",
        dedupe_key="delivery_retry:4242:2",
        payload={
            "kind": "delivery_failed_retry",
            "delivery_retry_of_outbox_id": 4242,
            "delivery_retry_attempt": 2,
        },
    )
    db.add(matching)
    await db.flush()
    adopted = await wa_worker._create_delivery_retry_job_idempotent(
        db,
        dedupe_key="delivery_retry:4242:2",
        identity=identity,
        status="queued",
        run_at=datetime.now(timezone.utc),
        attempts=0,
        max_attempts=5,
        payload={},
    )
    assert adopted is not None
    assert adopted.id == matching.id


async def test_a_squatting_retry_attempt_number_blocks_a_new_retry(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """The outer attempt-number dedupe already refuses to create a second job.

    Fail-closed rather than fail-safe: nothing new is created, and the EasyWeek
    chain does not silently gain an Altegio job of its own making.
    """
    job = await _seed_easyweek_happy_path(db)
    await _run_job(db, job)
    rows = await _outbox_rows(db, job)
    outbox_id = rows[0].id

    squatter = MessageJob(
        provider=PROVIDER_ALTEGIO,
        company_id=job.company_id,
        record_id=job.record_id,
        client_id=job.client_id,
        job_type="record_created",
        run_at=datetime.now(timezone.utc),
        status="queued",
        dedupe_key=f"delivery_retry:{outbox_id}:1",
        payload={},
    )
    db.add(squatter)
    await db.flush()

    await _deliver_failed_status(db, rows[0].provider_message_id, dedupe="wa:eyw-squatter")

    assert len(await _retry_jobs_for(db, outbox_id)) == 1
    await db.refresh(squatter)
    assert squatter.provider == PROVIDER_ALTEGIO, "the existing row must not be mutated"


async def test_retry_refusal_audit_carries_no_pii(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    async def _blank(job: MessageJob, outbox: OutboxMessage) -> None:
        job.provider = ""

    outbox = await _outbox_without_a_usable_job(db, capture, _blank)

    await _deliver_failed_status(db, outbox.provider_message_id, dedupe="wa:eyw-audit-pii")

    await db.refresh(outbox)
    # The send audit legitimately holds the rendered params from the successful
    # send; what must stay clean is everything the REFUSAL adds.
    refusal = {k: v for k, v in (outbox.meta or {}).items() if k.startswith("delivery_retry")}
    assert refusal["delivery_retry_skipped"] is True
    audit = str(refusal)
    for secret in (CLIENT_PHONE, CLIENT_EMAIL, "Anna", "Wimpernverlängerung", BOOKING_HASH):
        assert secret not in audit


# ---------------------------------------------------------------------------
# 15. Partial planner state: MessageJob.client_id IS NULL
# ---------------------------------------------------------------------------
#
# PR-4 supports partial deliveries on purpose. A booking-updated or
# booking-canceled payload that carries no `customer_id` leaves the already
# known `Record.client_id` alone, and the planner then creates a job with
# `client_id = NULL` because THAT delivery carried no Client. The outbox worker
# resolves the client through the record, so the send succeeds and the outbox
# row names the real client — which is why comparing `job.client_id` to
# `outbox.client_id` directly used to reject the whole booking on retry.


@pytest_asyncio.fixture
async def easyweek_inbox(
    session_maker: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> async_sessionmaker[AsyncSession]:
    """Run the real PR-4 normalizer against the test database.

    ``easyweek_notifications_enabled`` is turned on HERE only: these tests need
    the planner to actually emit jobs. The production default stays False, and
    no message leaves the process — every send goes through the capture provider.
    """
    monkeypatch.setattr(app_db, "SessionLocal", session_maker, raising=False)
    monkeypatch.setattr(eyw_worker, "SessionLocal", session_maker, raising=False)
    monkeypatch.setattr(settings, "easyweek_processing_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_location_id", TEST_LOCATION_ID, raising=False)
    return session_maker


async def _capture_and_process(
    session_maker: async_sessionmaker[AsyncSession],
    payload: dict[str, Any],
    *,
    event_hint: str,
    payload_hash: str,
) -> None:
    async with session_maker() as session:
        async with session.begin():
            session.add(
                EasyWeekEvent(
                    status="captured",
                    event_hint=event_hint,
                    auth_via="query",
                    payload_hash=payload_hash,
                    payload=payload,
                    body_truncated=False,
                    booking_uuid=canonical_booking_uuid(payload),
                )
            )
    for _ in range(20):
        if not await eyw_worker.process_one():
            break


def _future_booking(payload: dict[str, Any]) -> dict[str, Any]:
    """Move the fixture appointment ahead of the test clock.

    The delivery-retry deadline for ``record_created``/``record_updated`` is
    ``starts_at - 30 min``; the shared fixtures are pinned to a fixed date, so a
    run after it would legitimately refuse the retry and the test would be
    measuring the calendar rather than the identity rules.
    """
    start = datetime.now(timezone.utc) + timedelta(days=30)
    end = start + timedelta(hours=1)
    payload["booking_date_start"] = start.strftime("%Y-%m-%dT%H:%M:%S+0000")
    payload["booking_date_end"] = end.strftime("%Y-%m-%dT%H:%M:%S+0000")
    payload["booking_date_start_tz"] = start.strftime("%Y-%m-%dT%H:%M:%S+0000")
    return payload


def _partial_payload(builder: Any, *, explicit_null: bool = False) -> dict[str, Any]:
    """A follow-up delivery that says nothing about the customer."""
    payload = _future_booking(builder())
    if explicit_null:
        payload["customer_id"] = None
    else:
        payload.pop("customer_id", None)
    return payload


async def _seed_colliding_altegio_rows(db: AsyncSession, *, code: str) -> WhatsAppSender:
    """An Altegio template and sender on the same numeric company id."""
    db.add(
        _template(
            provider=PROVIDER_ALTEGIO,
            company_id=TEST_LOCATION_ID,
            code=code,
            body="ALTEGIO BODY",
        )
    )
    altegio_sender = _sender(
        provider=PROVIDER_ALTEGIO,
        company_id=TEST_LOCATION_ID,
        phone_number_id="altegio-phone-id",
    )
    db.add(altegio_sender)
    await db.flush()
    return altegio_sender


async def _job_by_type(db: AsyncSession, job_type: str) -> MessageJob:
    res = await db.execute(
        select(MessageJob)
        .where(MessageJob.provider == PROVIDER_EASYWEEK)
        .where(MessageJob.job_type == job_type)
        .order_by(MessageJob.id)
    )
    jobs = list(res.scalars().all())
    assert len(jobs) == 1, f"expected exactly one {job_type} job, got {len(jobs)}"
    return jobs[0]


@pytest.mark.parametrize(
    "job_type,event_hint,builder,meta_name",
    [
        ("record_updated", "booking-updated", booking_updated, EASYWEEK_UPDATED_TEMPLATE),
        ("record_updated", "booking-rescheduled", booking_rescheduled, EASYWEEK_UPDATED_TEMPLATE),
        ("record_canceled", "booking-canceled", booking_canceled, EASYWEEK_CANCELED_TEMPLATE),
    ],
)
@pytest.mark.parametrize("explicit_null", [False, True])
async def test_partial_job_without_a_client_sends_and_retries_as_easyweek(
    db: AsyncSession,
    capture: CaptureProvider,
    easyweek_inbox: async_sessionmaker[AsyncSession],
    no_contact_rate_limit: None,
    job_type: str,
    event_hint: str,
    builder: Any,
    meta_name: str,
    explicit_null: bool,
) -> None:
    await _capture_and_process(
        easyweek_inbox,
        _future_booking(booking_created()),
        event_hint="booking-created",
        payload_hash="h-1",
    )
    await _capture_and_process(
        easyweek_inbox,
        _partial_payload(builder, explicit_null=explicit_null),
        event_hint=event_hint,
        payload_hash="h-2",
    )

    # 3. The record keeps its client; 4. the new job carries none.
    record = (await db.execute(select(Record).where(Record.provider == PROVIDER_EASYWEEK))).scalars().one()
    client = (await db.execute(select(Client).where(Client.provider == PROVIDER_EASYWEEK))).scalars().one()
    assert record.client_id == client.id, "a partial delivery must not unlink the client"

    job = await _job_by_type(db, job_type)
    assert job.client_id is None, "the delivery carried no Client, so the job names none"

    altegio_sender = await _seed_colliding_altegio_rows(db, code=job_type)
    db.add(
        _template(
            provider=PROVIDER_EASYWEEK,
            company_id=TEST_LOCATION_ID,
            code=job_type,
            language="de",
            meta_template_name=meta_name,
        )
    )
    easyweek_sender = _sender(
        provider=PROVIDER_EASYWEEK,
        company_id=TEST_LOCATION_ID,
        phone_number_id="eyw-phone-id",
    )
    db.add(easyweek_sender)
    await db.flush()

    # 5. The first send succeeds and records the REAL client.
    await _run_job(db, job)
    assert job.status == "done", job.last_error
    rows = await _outbox_rows(db, job)
    assert len(rows) == 1
    assert rows[0].client_id == client.id
    assert rows[0].sender_id == easyweek_sender.id

    # 6-7. A retryable failure produces an EasyWeek retry with the effective client.
    await _deliver_failed_status(db, rows[0].provider_message_id, dedupe=f"wa:partial-{job_type}-{explicit_null}")
    retries = await _retry_jobs_for(db, rows[0].id)
    assert len(retries) == 1, "a partial job must not lose its retry"
    retry = retries[0]
    assert retry.provider == PROVIDER_EASYWEEK
    assert retry.client_id == client.id, "the proven effective client is materialized on the retry"
    assert retry.company_id == job.company_id
    assert retry.record_id == record.id
    assert retry.job_type == job_type

    # 8-9. The retry sends, and it sends as EasyWeek.
    retry.run_at = datetime.now(timezone.utc)
    await db.flush()
    capture.template_calls.clear()
    await _run_job(db, retry)

    assert retry.status == "done", retry.last_error
    assert len(capture.template_calls) == 1
    call = capture.template_calls[0]
    assert call["template_name"] == meta_name
    assert call["language"] == "de"
    retry_rows = await _outbox_rows(db, retry)
    assert retry_rows[0].sender_id == easyweek_sender.id
    assert retry_rows[0].sender_id != altegio_sender.id
    assert retry_rows[0].language == "de"
    assert retry_rows[0].client_id == client.id


async def test_a_partial_job_still_passes_the_resolver_directly(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """Unit-level restatement: NULL job client resolves through the record."""
    job = await _seed_easyweek_happy_path(db)
    await _run_job(db, job)
    rows = await _outbox_rows(db, job)
    record_client_id = job.client_id
    job.client_id = None
    await db.flush()

    resolution = await resolve_retry_identity(
        db,
        anchor_outbox=rows[0],
        original_job=job,
        job_type="record_created",
    )

    assert resolution.error is None
    assert resolution.identity is not None
    assert resolution.identity.client_id == record_client_id
    assert resolution.identity.provider == PROVIDER_EASYWEEK


# ---------------------------------------------------------------------------
# 16. A conflicting job on a taken attempt is neutralized, not left sendable
# ---------------------------------------------------------------------------


async def _seed_conflicting_altegio_retry(
    db: AsyncSession,
    *,
    job: MessageJob,
    outbox_id: int,
    payload: dict[str, Any] | None = None,
    status: str = "queued",
) -> MessageJob:
    """The exact row the old early-return left behind.

    Provider says Altegio, record and client point at EasyWeek domain rows, and
    the numeric company id collides — so a send would load the Altegio template
    and use the Altegio number for an EasyWeek customer.
    """
    conflicting = MessageJob(
        provider=PROVIDER_ALTEGIO,
        company_id=job.company_id,
        record_id=job.record_id,
        client_id=job.client_id,
        job_type=job.job_type,
        run_at=datetime.now(timezone.utc),
        status=status,
        dedupe_key=f"delivery_retry:{outbox_id}:1",
        payload=payload
        if payload is not None
        else {
            "kind": "delivery_failed_retry",
            "delivery_retry_of_outbox_id": outbox_id,
            "delivery_retry_attempt": 1,
        },
    )
    db.add(conflicting)
    await db.flush()
    return conflicting


async def test_conflicting_attempt_is_neutralized_by_the_callback(
    db: AsyncSession,
    capture: CaptureProvider,
    caplog: pytest.LogCaptureFixture,
) -> None:
    db.add(
        _template(
            provider=PROVIDER_ALTEGIO,
            company_id=COLLIDING_COMPANY_ID,
            code="record_created",
            body="ALTEGIO BODY",
        )
    )
    db.add(
        _sender(
            provider=PROVIDER_ALTEGIO,
            company_id=COLLIDING_COMPANY_ID,
            phone_number_id="altegio-phone-id",
        )
    )
    await db.flush()

    job = await _seed_easyweek_happy_path(db)
    await _run_job(db, job)
    assert job.status == "done", job.last_error
    rows = await _outbox_rows(db, job)
    outbox_id = rows[0].id

    conflicting = await _seed_conflicting_altegio_retry(db, job=job, outbox_id=outbox_id)
    capture.template_calls.clear()
    capture.text_calls.clear()
    caplog.set_level(logging.INFO)
    caplog.clear()

    await _deliver_failed_status(db, rows[0].provider_message_id, dedupe="wa:conflict-attempt")

    await db.refresh(conflicting)
    # Terminal, unlocked, and out of every claim query.
    assert conflicting.status == "canceled"
    assert conflicting.locked_at is None
    assert "provider" in (conflicting.last_error or "")
    # Not repaired: its provenance is unproven, so nothing about it is rewritten.
    assert conflicting.provider == PROVIDER_ALTEGIO
    assert conflicting.record_id == job.record_id
    assert conflicting.client_id == job.client_id

    # No second job on a globally unique dedupe key.
    assert [j.id for j in await _retry_jobs_for(db, outbox_id)] == [conflicting.id]

    # Stale recovery must not resurrect it.
    requeued = await ow._requeue_stale_processing_jobs(db)
    await db.refresh(conflicting)
    assert conflicting.status == "canceled"
    assert requeued == 0

    await db.refresh(rows[0])
    refusal = {k: v for k, v in (rows[0].meta or {}).items() if k.startswith("delivery_retry")}
    assert refusal["delivery_retry_skip_reason"] == "conflicting_retry_provider"
    audit = str(refusal)
    for secret in (CLIENT_PHONE, CLIENT_EMAIL, "Anna", "Wimpernverlängerung", BOOKING_HASH, VERIFIED_PAGE):
        assert secret not in audit

    # Running the exact terminal row after callback repair still cannot reach
    # template lookup, sender routing or either provider call.
    await _run_job(db, conflicting)
    assert capture.template_calls == []
    assert capture.text_calls == []
    assert await _outbox_rows(db, conflicting) == []
    assert "altegio-phone-id" not in caplog.text
    for secret in (CLIENT_PHONE, CLIENT_EMAIL, "Anna", "Wimpernverlängerung", BOOKING_HASH, VERIFIED_PAGE):
        assert secret not in caplog.text


async def test_a_neutralized_conflicting_job_cannot_send_through_the_outbox(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """Second line of defence: even forced back to queued, it must not send."""
    altegio_sender = _sender(
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        phone_number_id="altegio-phone-id",
    )
    db.add(altegio_sender)
    db.add(
        _template(
            provider=PROVIDER_ALTEGIO,
            company_id=COLLIDING_COMPANY_ID,
            code="record_created",
            body="ALTEGIO BODY {{1}}",
        )
    )
    await db.flush()

    job = await _seed_easyweek_happy_path(db)
    await _run_job(db, job)
    rows = await _outbox_rows(db, job)
    conflicting = await _seed_conflicting_altegio_retry(db, job=job, outbox_id=rows[0].id)
    capture.template_calls.clear()
    capture.text_calls.clear()

    await _run_job(db, conflicting)

    assert conflicting.status == "canceled"
    assert conflicting.locked_at is None
    assert "does not match the proven chain identity" in (conflicting.last_error or "")
    assert capture.template_calls == []
    assert capture.text_calls == []
    assert await _outbox_rows(db, conflicting) == []
    err = conflicting.last_error or ""
    for secret in (CLIENT_PHONE, CLIENT_EMAIL, "Anna", "Wimpernverlängerung", BOOKING_HASH, VERIFIED_PAGE):
        assert secret not in err


async def test_stale_recovery_terminally_rejects_a_wrong_provider_retry(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """Recovery must not make a legacy cross-provider retry sendable again."""
    job = await _seed_easyweek_happy_path(db)
    await _run_job(db, job)
    rows = await _outbox_rows(db, job)
    legacy = await _seed_conflicting_altegio_retry(
        db,
        job=job,
        outbox_id=rows[0].id,
        status="processing",
    )
    legacy.locked_at = datetime.now(timezone.utc) - timedelta(hours=1)
    await db.flush()
    capture.template_calls.clear()
    capture.text_calls.clear()

    recovered = await ow._requeue_stale_processing_jobs(db)

    await db.flush()
    await db.refresh(legacy)
    assert recovered == 0
    assert legacy.status == "canceled"
    assert legacy.locked_at is None
    assert "provider" in (legacy.last_error or "")

    # Even a direct invocation after recovery remains local and produces no send.
    await _run_job(db, legacy)
    assert capture.template_calls == []
    assert capture.text_calls == []
    assert await _outbox_rows(db, legacy) == []


async def test_stale_recovery_requeues_a_proven_easyweek_retry(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """The stale guard is selective: a valid retry keeps normal recovery."""
    job = await _seed_easyweek_happy_path(db)
    await _run_job(db, job)
    rows = await _outbox_rows(db, job)
    await _deliver_failed_status(db, rows[0].provider_message_id, dedupe="wa:valid-stale-retry")
    retry = (await _retry_jobs_for(db, rows[0].id))[0]
    retry.status = "processing"
    retry.locked_at = datetime.now(timezone.utc) - timedelta(hours=1)
    await db.flush()

    recovered = await ow._requeue_stale_processing_jobs(db)

    await db.flush()
    await db.refresh(retry)
    assert recovered == 1
    assert retry.status == "queued"
    assert retry.locked_at is None
    assert retry.last_error == "Recovered: stale processing job"


async def test_a_well_formed_retry_payload_with_the_wrong_provider_never_sends(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """The presend guard must not depend on the callback repair.

    This is the row an older build could have written: the payload is perfectly
    well formed, and only ``provider`` is wrong.
    """
    db.add(
        _sender(
            provider=PROVIDER_ALTEGIO,
            company_id=COLLIDING_COMPANY_ID,
            phone_number_id="altegio-phone-id",
        )
    )
    db.add(
        _template(
            provider=PROVIDER_ALTEGIO,
            company_id=COLLIDING_COMPANY_ID,
            code="record_created",
            body="ALTEGIO BODY {{1}}",
        )
    )
    await db.flush()

    job = await _seed_easyweek_happy_path(db)
    await _run_job(db, job)
    rows = await _outbox_rows(db, job)

    legacy = await _seed_conflicting_altegio_retry(
        db,
        job=job,
        outbox_id=rows[0].id,
        payload={
            "kind": "delivery_failed_retry",
            "delivery_retry_of_outbox_id": rows[0].id,
            "delivery_retry_attempt": 1,
            "delivery_retry_original_outbox_id": rows[0].id,
        },
    )
    capture.template_calls.clear()

    await _run_job(db, legacy)

    assert legacy.status == "canceled"
    assert (legacy.last_error or "").startswith("Canceled: delivery retry provider")
    assert capture.template_calls == []
    assert capture.text_calls == []


async def test_a_malformed_retry_namespace_job_never_sends(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """A row parked in the reserved namespace with no chain payload."""
    job = await _seed_easyweek_happy_path(db)
    await _run_job(db, job)
    rows = await _outbox_rows(db, job)

    malformed = await _seed_conflicting_altegio_retry(db, job=job, outbox_id=rows[0].id, payload={})
    capture.template_calls.clear()

    await _run_job(db, malformed)

    assert malformed.status == "canceled"
    assert malformed.locked_at is None
    assert "invalid delivery_retry_of_outbox_id" in (malformed.last_error or "")
    assert capture.template_calls == []
    assert capture.text_calls == []
    assert await _outbox_rows(db, malformed) == []


async def test_a_retry_payload_outside_the_reserved_namespace_never_sends(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """A retry payload cannot bypass chain idempotency with an ordinary key."""
    job = await _seed_easyweek_happy_path(db)
    await _run_job(db, job)
    rows = await _outbox_rows(db, job)
    retry = MessageJob(
        provider=PROVIDER_EASYWEEK,
        company_id=job.company_id,
        record_id=job.record_id,
        client_id=job.client_id,
        job_type=job.job_type,
        run_at=datetime.now(timezone.utc),
        status="queued",
        dedupe_key="ordinary-job-key",
        payload={
            "kind": "delivery_failed_retry",
            "delivery_retry_of_outbox_id": rows[0].id,
            "delivery_retry_attempt": 1,
        },
    )
    db.add(retry)
    await db.flush()
    capture.template_calls.clear()
    capture.text_calls.clear()

    await _run_job(db, retry)

    assert retry.status == "canceled"
    assert "delivery_retry_dedupe_namespace_missing" in (retry.last_error or "")
    assert capture.template_calls == []
    assert capture.text_calls == []
    assert await _outbox_rows(db, retry) == []


@pytest.mark.parametrize(
    "payload_change,error_fragment",
    [
        ({"delivery_retry_attempt": 2}, "delivery_retry_attempt_mismatch"),
        ({"delivery_retry_of_outbox_id": 999999999}, "delivery_retry_outbox_reference_mismatch"),
    ],
)
async def test_retry_namespace_and_payload_must_name_the_same_chain_and_attempt(
    db: AsyncSession,
    capture: CaptureProvider,
    payload_change: dict[str, int],
    error_fragment: str,
) -> None:
    """A syntactically plausible payload cannot disagree with its reserved key."""
    job = await _seed_easyweek_happy_path(db)
    await _run_job(db, job)
    rows = await _outbox_rows(db, job)
    payload = {
        "kind": "delivery_failed_retry",
        "delivery_retry_of_outbox_id": rows[0].id,
        "delivery_retry_attempt": 1,
        "delivery_retry_original_outbox_id": rows[0].id,
    }
    payload.update(payload_change)
    retry = MessageJob(
        provider=PROVIDER_EASYWEEK,
        company_id=job.company_id,
        record_id=job.record_id,
        client_id=job.client_id,
        job_type=job.job_type,
        run_at=datetime.now(timezone.utc),
        status="queued",
        dedupe_key=f"delivery_retry:{rows[0].id}:1",
        payload=payload,
    )
    db.add(retry)
    await db.flush()
    capture.template_calls.clear()
    capture.text_calls.clear()

    await _run_job(db, retry)

    assert retry.status == "canceled"
    assert error_fragment in (retry.last_error or "")
    assert capture.template_calls == []
    assert capture.text_calls == []
    assert await _outbox_rows(db, retry) == []


async def test_a_terminal_historical_conflicting_job_is_left_alone(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """A row that already finished is evidence, not something to rewrite."""
    job = await _seed_easyweek_happy_path(db)
    await _run_job(db, job)
    rows = await _outbox_rows(db, job)

    historical = await _seed_conflicting_altegio_retry(db, job=job, outbox_id=rows[0].id, status="done")
    historical.last_error = None
    await db.flush()

    await _deliver_failed_status(db, rows[0].provider_message_id, dedupe="wa:conflict-historical")

    await db.refresh(historical)
    assert historical.status == "done", "a terminal job must not be reopened or rewritten"
    assert historical.last_error is None
    assert [j.id for j in await _retry_jobs_for(db, rows[0].id)] == [historical.id]


async def test_a_matching_existing_retry_stays_idempotent(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """The same callback twice must not restart or duplicate the retry."""
    job = await _seed_easyweek_happy_path(db)
    await _run_job(db, job)
    rows = await _outbox_rows(db, job)

    await _deliver_failed_status(db, rows[0].provider_message_id, dedupe="wa:idempotent-1")
    first = (await _retry_jobs_for(db, rows[0].id))[0]
    first_run_at = first.run_at

    await _deliver_failed_status(db, rows[0].provider_message_id, dedupe="wa:idempotent-2")

    retries = await _retry_jobs_for(db, rows[0].id)
    assert len(retries) == 1
    await db.refresh(first)
    assert first.status == "queued"
    assert first.run_at == first_run_at, "an idempotent repeat must not reschedule the retry"
    assert first.provider == PROVIDER_EASYWEEK
