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

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.meta_templates import META_TEMPLATE_MAP, resolve_meta_template
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    Client,
    MessageJob,
    MessageTemplate,
    OutboxMessage,
    Record,
    RecordService,
    ServiceSenderRule,
    WhatsAppSender,
)
from altegio_bot.settings import settings
from altegio_bot.whatsapp_routing import pick_sender_id, pick_sender_id_by_code
from altegio_bot.workers import outbox_worker as ow

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
    services: tuple[tuple[int, str, str], ...] = ((11, "Wimpernverlängerung", "60.00"),),
) -> Record:
    record = Record(
        provider=PROVIDER_EASYWEEK,
        company_id=company_id,
        altegio_record_id=4200001,
        easyweek_booking_uuid=uuid.UUID("11111111-2222-4333-8444-555555555555"),
        easyweek_booking_hash_id=booking_hash_id,
        client_id=client.id,
        staff_name="Tanja",
        starts_at=starts_at or (datetime.now(timezone.utc) + timedelta(days=3)),
        short_link=short_link,
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
                cost_to_pay=Decimal(cost),
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
    services: tuple[tuple[int, str, str], ...] = ((11, "Wimpernverlängerung", "60.00"),),
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
    )
    db.add(
        _template(
            provider=PROVIDER_EASYWEEK,
            company_id=COLLIDING_COMPANY_ID,
            code=job_type,
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


async def test_multi_service_list_is_flattened_into_one_parameter(
    db: AsyncSession,
    capture: CaptureProvider,
) -> None:
    """Meta rejects a newline inside a parameter, so the list is joined."""
    job = await _seed_easyweek_happy_path(
        db,
        services=((11, "Wimpernverlängerung", "60.00"), (12, "Auffüllen", "25.50")),
    )

    params = await _run_and_get_params(db, capture, job)

    assert params[4] == "Wimpernverlängerung — 60.00€, Auffüllen — 25.50€"
    assert "\n" not in params[4]
    assert params[5] == "85.50"


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
        short_link="https://n1234567.yclients.com/record/1",
        raw={},
    )
    db.add(record)
    await db.flush()
    db.add(RecordService(record_id=record.id, service_id=21, title="Schnitt", cost_to_pay=Decimal("40.00"), raw={}))
    db.add(
        _template(
            provider=PROVIDER_ALTEGIO,
            company_id=COLLIDING_COMPANY_ID,
            code="record_updated",
            body="ALTEGIO BODY",
            meta_template_name=None,
        )
    )
    db.add(_sender(provider=PROVIDER_ALTEGIO, company_id=COLLIDING_COMPANY_ID, phone_number_id="altegio-phone-id"))
    await db.flush()
    job = await _seed_job(
        db,
        provider=PROVIDER_ALTEGIO,
        company_id=COLLIDING_COMPANY_ID,
        job_type="record_updated",
        record=record,
        client=client,
        dedupe_key="alt-updated-1",
    )

    await _run_job(db, job)

    assert job.status == "done", job.last_error
    assert len(capture.template_calls) == 1
    call = capture.template_calls[0]
    assert call["template_name"] == META_TEMPLATE_MAP[(COLLIDING_COMPANY_ID, "record_updated")]
    assert call["template_name"] == "kitilash_ka_record_updated_v1"
    # Altegio keeps its own positional contract: the 7th param is the raw
    # Altegio short_link, untouched by the EasyWeek link rules.
    assert len(call["params"]) == 7
    assert call["params"][6] == "https://n1234567.yclients.com/record/1"


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
