"""PR-6: the Dürlach activation seed, against a real database.

Covers the two things an activation seed can get wrong in a way nobody notices
until a customer is affected: writing rows the worker will not match, and
touching rows that belong to someone else.

Nothing here enables notifications or sends anything — `EASYWEEK_NOTIFICATIONS_ENABLED`
is an operator step and stays off.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_policy import RECORD_CANCELED, RECORD_CREATED, RECORD_UPDATED
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    Client,
    MessageTemplate,
    Record,
    RecordService,
    WhatsAppSender,
)
from altegio_bot.scripts import seed_easyweek_templates as seed_script
from altegio_bot.settings import settings
from altegio_bot.workers import outbox_worker as ow

pytestmark = pytest.mark.asyncio

DURLACH_LOCATION_ID = 999501
SHARED_PHONE_NUMBER_ID = "shared-bot-phone-number-id"
STATIC_BOOKING_PAGE = "https://example.invalid/durlach"
BOOKING_HASH = "90000123"
VERIFIED_PAGE = f"https://eyw.me/r/{BOOKING_HASH}"


@pytest.fixture(autouse=True)
def _durlach_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """The Dürlach configuration, with notifications deliberately OFF."""
    monkeypatch.setattr(settings, "easyweek_location_id", DURLACH_LOCATION_ID, raising=False)
    monkeypatch.setattr(settings, "easyweek_default_language", "de", raising=False)
    monkeypatch.setattr(settings, "easyweek_booking_page_url", STATIC_BOOKING_PAGE, raising=False)
    monkeypatch.setattr(settings, "meta_wa_phone_number_id", SHARED_PHONE_NUMBER_ID, raising=False)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)


@pytest_asyncio.fixture
async def db(session_maker: async_sessionmaker[AsyncSession]) -> AsyncSession:
    async with session_maker() as session:
        yield session


async def _run_seed(db: AsyncSession) -> seed_script.SeedResult:
    result = await seed_script.seed(db)
    await db.flush()
    return result


async def _easyweek_templates(db: AsyncSession) -> list[MessageTemplate]:
    res = await db.execute(
        select(MessageTemplate)
        .where(MessageTemplate.provider == PROVIDER_EASYWEEK)
        .order_by(MessageTemplate.code, MessageTemplate.id)
    )
    return list(res.scalars().all())


async def _count(db: AsyncSession, model: Any, **where: Any) -> int:
    stmt = select(func.count()).select_from(model)
    for column, value in where.items():
        stmt = stmt.where(getattr(model, column) == value)
    return int((await db.execute(stmt)).scalar_one())


# ---------------------------------------------------------------------------
# 1-2. What the seed writes, and that a second run changes nothing
# ---------------------------------------------------------------------------


async def test_the_seed_writes_exactly_the_three_phase_one_templates(db: AsyncSession) -> None:
    result = await _run_seed(db)

    assert result.templates_created == 3
    rows = await _easyweek_templates(db)
    assert [r.code for r in rows] == sorted([RECORD_CREATED, RECORD_UPDATED, RECORD_CANCELED])

    for row in rows:
        assert row.provider == PROVIDER_EASYWEEK
        assert row.company_id == DURLACH_LOCATION_ID
        assert row.language == "de"
        assert row.is_active is True
        assert row.meta_template_name == seed_script.META_TEMPLATE_NAMES[row.code]
        assert row.meta_template_name.startswith("kitilash_du_")


@pytest.mark.parametrize("absent_code", ["record_created_new_client", "reminder_24h", "reminder_2h"])
async def test_the_seed_writes_no_phase_two_codes(db: AsyncSession, absent_code: str) -> None:
    """Reminders are PR-7; the new-client variant is unreachable for EasyWeek."""
    await _run_seed(db)
    assert await _count(db, MessageTemplate, provider=PROVIDER_EASYWEEK, code=absent_code) == 0


async def test_running_the_seed_twice_creates_no_duplicates(db: AsyncSession) -> None:
    first = await _run_seed(db)
    before = [(r.id, r.code, r.body, r.meta_template_name) for r in await _easyweek_templates(db)]

    second = await _run_seed(db)
    after = [(r.id, r.code, r.body, r.meta_template_name) for r in await _easyweek_templates(db)]

    assert first.templates_created == 3
    assert second.templates_created == 0
    assert second.templates_updated == 3
    assert after == before, "a re-run must not add, renumber or rewrite rows"
    assert await _count(db, MessageTemplate, provider=PROVIDER_EASYWEEK) == 3


async def test_the_seed_reactivates_a_row_an_operator_disabled(db: AsyncSession) -> None:
    """Idempotent means converging on the intended state, not skipping work."""
    await _run_seed(db)
    rows = await _easyweek_templates(db)
    rows[0].is_active = False
    rows[0].meta_template_name = "stale_name_v0"
    await db.flush()

    await _run_seed(db)

    refreshed = await _easyweek_templates(db)
    assert all(r.is_active for r in refreshed)
    assert refreshed[0].meta_template_name == seed_script.META_TEMPLATE_NAMES[refreshed[0].code]


# ---------------------------------------------------------------------------
# 3. Nothing belonging to Altegio is touched
# ---------------------------------------------------------------------------


async def test_the_seed_leaves_altegio_rows_untouched(db: AsyncSession) -> None:
    """Including an Altegio row on the SAME numeric company id.

    `seed_templates.py` deletes by company_id alone; this seed must never
    behave that way, and the two CRMs share one integer space.
    """
    altegio_same_company = MessageTemplate(
        provider=PROVIDER_ALTEGIO,
        company_id=DURLACH_LOCATION_ID,
        code=RECORD_CREATED,
        language="de",
        body="ALTEGIO BODY",
        is_active=True,
    )
    altegio_other = MessageTemplate(
        provider=PROVIDER_ALTEGIO,
        company_id=758285,
        code=RECORD_CREATED,
        language="de",
        body="KARLSRUHE BODY",
        is_active=True,
    )
    altegio_sender = WhatsAppSender(
        provider=PROVIDER_ALTEGIO,
        company_id=DURLACH_LOCATION_ID,
        sender_code="default",
        phone_number_id="altegio-phone-number-id",
        is_active=True,
    )
    db.add_all([altegio_same_company, altegio_other, altegio_sender])
    await db.flush()
    before = [
        (altegio_same_company.id, altegio_same_company.body, altegio_same_company.is_active),
        (altegio_other.id, altegio_other.body, altegio_other.is_active),
    ]

    await _run_seed(db)

    for row in (altegio_same_company, altegio_other, altegio_sender):
        await db.refresh(row)
    assert [
        (altegio_same_company.id, altegio_same_company.body, altegio_same_company.is_active),
        (altegio_other.id, altegio_other.body, altegio_other.is_active),
    ] == before
    assert altegio_sender.phone_number_id == "altegio-phone-number-id"
    assert await _count(db, MessageTemplate, provider=PROVIDER_ALTEGIO) == 2


# ---------------------------------------------------------------------------
# 4. The sender
# ---------------------------------------------------------------------------


async def test_the_sender_seed_is_idempotent(db: AsyncSession) -> None:
    first = await _run_seed(db)
    assert first.sender_created is True

    second = await _run_seed(db)
    assert second.sender_created is False
    assert second.sender_updated is True

    res = await db.execute(select(WhatsAppSender).where(WhatsAppSender.provider == PROVIDER_EASYWEEK))
    senders = list(res.scalars().all())
    assert len(senders) == 1
    assert senders[0].company_id == DURLACH_LOCATION_ID
    assert senders[0].sender_code == "default"
    assert senders[0].phone_number_id == SHARED_PHONE_NUMBER_ID
    assert senders[0].is_active is True


async def test_the_sender_shares_the_bot_number_with_altegio_without_colliding(db: AsyncSession) -> None:
    """One phone_number_id may serve several company_ids — the lookup is by id."""
    db.add(
        WhatsAppSender(
            provider=PROVIDER_ALTEGIO,
            company_id=758285,
            sender_code="default",
            phone_number_id=SHARED_PHONE_NUMBER_ID,
            is_active=True,
        )
    )
    await db.flush()

    await _run_seed(db)

    from altegio_bot.whatsapp_routing import pick_sender_id

    easyweek_id = await pick_sender_id(db, DURLACH_LOCATION_ID, "default", provider=PROVIDER_EASYWEEK)
    altegio_id = await pick_sender_id(db, 758285, "default", provider=PROVIDER_ALTEGIO)
    assert easyweek_id is not None
    assert altegio_id is not None
    assert easyweek_id != altegio_id, "same number, different rows"


@pytest.mark.parametrize(
    "field,value",
    [
        ("easyweek_location_id", 0),
        ("easyweek_default_language", "   "),
        ("meta_wa_phone_number_id", ""),
    ],
)
async def test_the_seed_refuses_an_unconfigured_environment(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    field: str,
    value: Any,
) -> None:
    """Half-configured rows would fail closed at send time, one booking at a time."""
    monkeypatch.setattr(settings, field, value, raising=False)

    with pytest.raises(seed_script.SeedConfigError):
        await seed_script.seed(db)


# ---------------------------------------------------------------------------
# 5-7. End to end: the seeded rows are what the worker actually resolves
# ---------------------------------------------------------------------------


async def _seed_durlach_domain(db: AsyncSession, *, short_link: str | None = VERIFIED_PAGE) -> tuple[Client, Record]:
    client = Client(
        provider=PROVIDER_EASYWEEK,
        company_id=DURLACH_LOCATION_ID,
        altegio_client_id=7300777,
        phone_e164="+491700000777",
        display_name="Anna Müller",
        raw={},
    )
    db.add(client)
    await db.flush()
    record = Record(
        provider=PROVIDER_EASYWEEK,
        company_id=DURLACH_LOCATION_ID,
        altegio_record_id=4200777,
        easyweek_booking_hash_id=BOOKING_HASH,
        client_id=client.id,
        staff_name="Tanja",
        starts_at=datetime(2026, 9, 14, 8, 30, tzinfo=timezone.utc) + timedelta(days=0),
        short_link=short_link,
        total_cost=Decimal("60.00"),
        raw={},
    )
    db.add(record)
    await db.flush()
    db.add(
        RecordService(
            record_id=record.id,
            service_id=11,
            title="Wimpernverlängerung",
            cost_to_pay=Decimal("60.00"),
            raw={},
        )
    )
    await db.flush()
    return client, record


@pytest.mark.parametrize("code", [RECORD_CREATED, RECORD_UPDATED, RECORD_CANCELED])
async def test_the_seeded_rows_render_end_to_end(db: AsyncSession, code: str) -> None:
    await _run_seed(db)
    client, record = await _seed_durlach_domain(db)

    body, sender_id, language, ctx = await ow._render_message(
        db,
        company_id=DURLACH_LOCATION_ID,
        template_code=code,
        record=record,
        client=client,
        provider=PROVIDER_EASYWEEK,
    )

    assert language == "de"
    assert sender_id is not None
    assert ctx["meta_template_name"] == seed_script.META_TEMPLATE_NAMES[code]

    rendered = body.format(**ctx)
    assert "{" not in rendered, "every placeholder in the seeded body must exist in ctx"
    assert "Anna Müller" in rendered
    assert "Wimpernverlängerung" in rendered
    assert "KitiLash Durlach" in rendered
    assert "Pfinztalstraße 4, 76227 Karlsruhe-Durlach" in rendered
    # Phase 1 is transactional, so the marketing opt-out line must not appear.
    assert "abbestellen" not in rendered


async def test_created_and_updated_use_the_verified_manage_link(db: AsyncSession) -> None:
    await _run_seed(db)
    client, record = await _seed_durlach_domain(db)

    for code in (RECORD_CREATED, RECORD_UPDATED):
        _body, _sender_id, _lang, ctx = await ow._render_message(
            db,
            company_id=DURLACH_LOCATION_ID,
            template_code=code,
            record=record,
            client=client,
            provider=PROVIDER_EASYWEEK,
        )
        assert ctx["booking_link"] == VERIFIED_PAGE


async def test_canceled_uses_the_static_booking_page_not_the_manage_link(db: AsyncSession) -> None:
    """§1.6.4: a cancelled booking must not link to managing itself."""
    await _run_seed(db)
    client, record = await _seed_durlach_domain(db)

    body, _sender_id, _lang, ctx = await ow._render_message(
        db,
        company_id=DURLACH_LOCATION_ID,
        template_code=RECORD_CANCELED,
        record=record,
        client=client,
        provider=PROVIDER_EASYWEEK,
    )

    assert ctx["booking_link"] == STATIC_BOOKING_PAGE
    rendered = body.format(**ctx)
    assert STATIC_BOOKING_PAGE in rendered
    assert VERIFIED_PAGE not in rendered


async def test_an_unseeded_code_still_fails_closed(db: AsyncSession) -> None:
    """`reminder_24h` has no EasyWeek row, and must not borrow the Altegio one."""
    await _run_seed(db)
    client, record = await _seed_durlach_domain(db)
    db.add(
        MessageTemplate(
            provider=PROVIDER_ALTEGIO,
            company_id=DURLACH_LOCATION_ID,
            code="reminder_24h",
            language="de",
            body="ALTEGIO REMINDER",
            is_active=True,
        )
    )
    await db.flush()

    with pytest.raises(ValueError) as excinfo:
        await ow._render_message(
            db,
            company_id=DURLACH_LOCATION_ID,
            template_code="reminder_24h",
            record=record,
            client=client,
            provider=PROVIDER_EASYWEEK,
        )

    message = str(excinfo.value)
    assert "Template not found" in message
    assert PROVIDER_EASYWEEK in message
    assert "ALTEGIO REMINDER" not in message
