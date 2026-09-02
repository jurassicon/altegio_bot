"""PR-6: the Durlach activation seed, against a real database.

Covers the two things an activation seed can get wrong in a way nobody notices
until a customer is affected: writing rows the worker will not match, and
touching rows that belong to someone else.

Nothing here enables notifications or sends anything — `EASYWEEK_NOTIFICATIONS_ENABLED`
is an operator step and stays off.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_branches import BRANCH_PROFILES, branch_template_contract
from altegio_bot.easyweek_locations import configured_easyweek_locations
from altegio_bot.easyweek_policy import (
    RECORD_CANCELED,
    RECORD_CREATED,
    RECORD_CREATED_NEW_CLIENT,
    RECORD_UPDATED,
)
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

# A stand-in, never the production id — that lives in easyweek.env only, and a
# repository-wide test enforces it. What these tests exercise is the MECHANISM:
# the registry and the read-only API must agree, whatever the real number is.
DURLACH_LOCATION_ID = 999501
DURLACH_LOCATION_UUID = "dddddddd-eeee-4fff-8000-000000000001"
SHARED_PHONE_NUMBER_ID = "shared-bot-phone-number-id"
BOOKING_PAGE_HOST = "book.durlach.invalid"
STATIC_BOOKING_PAGE = f"https://{BOOKING_PAGE_HOST}/durlach"
BOOKING_HASH = "90000123"
VERIFIED_PAGE = f"https://eyw.me/r/{BOOKING_HASH}"


@pytest.fixture(autouse=True)
def _durlach_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """The Durlach configuration, with notifications deliberately OFF."""
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "durlach": {
                    "location_id": DURLACH_LOCATION_ID,
                    "location_uuid": DURLACH_LOCATION_UUID,
                    "meta_template_prefix": "du",
                    "booking_page_url": STATIC_BOOKING_PAGE,
                }
            }
        ),
        raising=False,
    )
    monkeypatch.setattr(settings, "easyweek_booking_page_allowed_hosts", BOOKING_PAGE_HOST, raising=False)
    monkeypatch.setattr(settings, "easyweek_default_language", "de", raising=False)
    monkeypatch.setattr(settings, "meta_wa_phone_number_id", SHARED_PHONE_NUMBER_ID, raising=False)
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", False, raising=False)


@pytest_asyncio.fixture
async def db(session_maker: async_sessionmaker[AsyncSession]) -> AsyncSession:
    async with session_maker() as session:
        yield session


class _FakeLocationsClient:
    def __init__(self, locations: list[dict[str, Any]]) -> None:
        self.locations = locations

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args) -> None:
        return None

    async def list_locations(self) -> list[dict[str, Any]]:
        return self.locations


def _client_factory(locations: list[dict[str, Any]] | None = None):
    visible = locations or [{"uuid": DURLACH_LOCATION_UUID, "name": "KitiLash Durlach"}]
    return lambda: _FakeLocationsClient(visible)


async def _run_seed(db: AsyncSession, *, api_locations: list[dict[str, Any]] | None = None) -> seed_script.SeedResult:
    result = await seed_script.seed(db, client_factory=_client_factory(api_locations))
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


async def test_the_seed_writes_exactly_the_branch_template_suite(db: AsyncSession) -> None:
    """Four lifecycle codes, the two PR-8 reminders, review, and PR-12 retention.

    The list is exhaustive on purpose: seeding a row is the first half of being
    able to send it, so a code appearing here without a deliberate decision is
    exactly the drift this assertion exists to catch.
    """
    result = await _run_seed(db)

    assert result.templates_created == 9
    rows = await _easyweek_templates(db)
    assert [r.code for r in rows] == sorted(
        [
            RECORD_CREATED,
            RECORD_CREATED_NEW_CLIENT,
            RECORD_UPDATED,
            RECORD_CANCELED,
            "reminder_24h",
            "reminder_2h",
            "review_3d",
            "repeat_10d",
            "comeback_3d",
        ]
    )

    for row in rows:
        assert row.provider == PROVIDER_EASYWEEK
        assert row.company_id == DURLACH_LOCATION_ID
        assert row.language == "de"
        assert row.is_active is True
        contract = branch_template_contract(BRANCH_PROFILES["durlach"], row.code)
        assert contract is not None
        assert row.meta_template_name == contract.meta_template_name
        assert row.body == contract.raw_body
        assert row.meta_template_name.startswith("kitilash_du_")


async def test_the_seed_writes_two_branch_specific_suites(db: AsyncSession, monkeypatch: pytest.MonkeyPatch) -> None:
    rastatt_id = 999502
    rastatt_uuid = "dddddddd-eeee-4fff-8000-000000000002"
    rastatt_page = "https://book.rastatt.invalid/rastatt"
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "durlach": {
                    "location_id": DURLACH_LOCATION_ID,
                    "location_uuid": DURLACH_LOCATION_UUID,
                    "meta_template_prefix": "du",
                    "booking_page_url": STATIC_BOOKING_PAGE,
                },
                "rastatt": {
                    "location_id": rastatt_id,
                    "location_uuid": rastatt_uuid,
                    "meta_template_prefix": "ra",
                    "booking_page_url": rastatt_page,
                },
            }
        ),
        raising=False,
    )
    monkeypatch.setattr(
        settings,
        "easyweek_booking_page_allowed_hosts",
        f"{BOOKING_PAGE_HOST},book.rastatt.invalid",
        raising=False,
    )
    api_locations = [
        {"uuid": DURLACH_LOCATION_UUID, "name": "KitiLash Durlach"},
        {"uuid": rastatt_uuid, "name": "KitiLash Rastatt"},
    ]

    result = await _run_seed(db, api_locations=api_locations)
    assert result.templates_created == 18
    assert result.senders_created == 2

    rows = await _easyweek_templates(db)
    assert len(rows) == 18
    by_company: dict[int, list[MessageTemplate]] = {}
    for row in rows:
        by_company.setdefault(row.company_id, []).append(row)
    assert all(row.meta_template_name.startswith("kitilash_du_") for row in by_company[DURLACH_LOCATION_ID])
    assert all(row.meta_template_name.startswith("kitilash_ra_") for row in by_company[rastatt_id])
    assert all(
        "KitiLash Durlach" in row.body and "Pfinztalstraße" in row.body for row in by_company[DURLACH_LOCATION_ID]
    )
    assert all("KitiLash Rastatt" in row.body and "Rathausstraße" in row.body for row in by_company[rastatt_id])

    senders = list(
        (
            await db.execute(
                select(WhatsAppSender)
                .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
                .order_by(WhatsAppSender.company_id)
            )
        )
        .scalars()
        .all()
    )
    assert [sender.company_id for sender in senders] == [DURLACH_LOCATION_ID, rastatt_id]
    assert {sender.phone_number_id for sender in senders} == {SHARED_PHONE_NUMBER_ID}

    plan = await seed_script.build_seed_plan(client_factory=_client_factory(api_locations))
    assert {branch.location.booking_page_url for branch in plan.branches} == {STATIC_BOOKING_PAGE, rastatt_page}

    second = await seed_script.seed(db, plan=plan)
    await db.flush()
    assert second.templates_created == 0
    assert second.templates_updated == 18
    assert await _count(db, MessageTemplate, provider=PROVIDER_EASYWEEK) == 18
    assert await _count(db, WhatsAppSender, provider=PROVIDER_EASYWEEK) == 2


@pytest.mark.parametrize(
    "absent_code",
    ["newsletter_new_clients_monthly", "newsletter_new_clients_followup", "promo_card_booking_reminder"],
)
async def test_the_seed_writes_no_deferred_marketing_codes(db: AsyncSession, absent_code: str) -> None:
    """PR-12 admitted repeat and comeback; it admitted nothing else.

    Newsletters and promo each need something EasyWeek has no equivalent for — an
    Altegio-keyed link map, a campaign runner built around Altegio client ids —
    and seeding a row for one would be the first half of sending it.
    """
    await _run_seed(db)
    assert await _count(db, MessageTemplate, provider=PROVIDER_EASYWEEK, code=absent_code) == 0


@pytest.mark.parametrize("code", ["reminder_24h", "reminder_2h"])
async def test_each_reminder_row_belongs_to_its_branch(db: AsyncSession, code: str) -> None:
    """A reminder is bound to one branch by the same contract as a lifecycle row."""
    await _run_seed(db)

    rows = [r for r in await _easyweek_templates(db) if r.code == code]
    assert len(rows) == 1
    row = rows[0]

    contract = branch_template_contract(BRANCH_PROFILES["durlach"], code)
    assert contract is not None
    assert row.meta_template_name == contract.meta_template_name == f"kitilash_du_{code}_v1"
    assert row.body == contract.raw_body
    assert "KitiLash Durlach" in row.body and "Rastatt" not in row.body
    assert row.provider == PROVIDER_EASYWEEK
    assert row.is_active is True


async def test_running_the_seed_twice_creates_no_duplicates(db: AsyncSession) -> None:
    first = await _run_seed(db)
    before = [(r.id, r.code, r.body, r.meta_template_name) for r in await _easyweek_templates(db)]

    second = await _run_seed(db)
    after = [(r.id, r.code, r.body, r.meta_template_name) for r in await _easyweek_templates(db)]

    assert first.templates_created == 9
    assert second.templates_created == 0
    assert second.templates_updated == 9
    assert after == before, "a re-run must not add, renumber or rewrite rows"
    assert await _count(db, MessageTemplate, provider=PROVIDER_EASYWEEK) == 9


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
    contract = branch_template_contract(BRANCH_PROFILES["durlach"], refreshed[0].code)
    assert contract is not None
    assert refreshed[0].meta_template_name == contract.meta_template_name
    assert refreshed[0].body == contract.raw_body


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
    assert first.senders_created == 1

    second = await _run_seed(db)
    assert second.senders_created == 0
    assert second.senders_updated == 1

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
        ("easyweek_location_map", "{}"),
        ("easyweek_location_map", "{not json"),
        ("easyweek_default_language", "   "),
        # German bodies naming German Meta templates must not be filed under `en`.
        ("easyweek_default_language", "en"),
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
        await _run_seed(db)

    await db.flush()
    assert await _count(db, MessageTemplate, provider=PROVIDER_EASYWEEK) == 0
    assert await _count(db, WhatsAppSender, provider=PROVIDER_EASYWEEK) == 0


# ---------------------------------------------------------------------------
# The independent GET /locations confirmation
# ---------------------------------------------------------------------------


async def test_a_matching_api_uuid_seeds(db: AsyncSession, capsys) -> None:
    result = await _run_seed(db)

    assert result.templates_created == 9
    assert await _count(db, WhatsAppSender, provider=PROVIDER_EASYWEEK) == 1
    assert "KitiLash Durlach" in capsys.readouterr().out


async def test_a_registry_uuid_missing_from_api_writes_nothing(db: AsyncSession) -> None:
    with pytest.raises(seed_script.SeedConfigError):
        await _run_seed(
            db,
            api_locations=[{"uuid": "eeeeeeee-ffff-4000-8111-000000000002", "name": "Another branch"}],
        )

    await db.flush()
    assert await _count(db, MessageTemplate, provider=PROVIDER_EASYWEEK) == 0
    assert await _count(db, WhatsAppSender, provider=PROVIDER_EASYWEEK) == 0


async def test_an_unavailable_api_writes_nothing(db: AsyncSession) -> None:
    def unavailable():
        raise RuntimeError("offline")

    with pytest.raises(seed_script.SeedConfigError):
        await seed_script.seed(db, client_factory=unavailable)

    await db.flush()
    assert await _count(db, MessageTemplate, provider=PROVIDER_EASYWEEK) == 0
    assert await _count(db, WhatsAppSender, provider=PROVIDER_EASYWEEK) == 0


async def test_cli_has_no_expect_location_id_escape_hatch() -> None:
    seed_script._parse_args([])
    with pytest.raises(SystemExit):
        seed_script._parse_args(["--expect-location-id", "42"])


# ---------------------------------------------------------------------------
# 5-7. End to end: the seeded rows are what the worker actually resolves
# ---------------------------------------------------------------------------


async def _seed_durlach_domain(
    db: AsyncSession,
    *,
    short_link: str | None = VERIFIED_PAGE,
    company_id: int = DURLACH_LOCATION_ID,
    identity_offset: int = 0,
) -> tuple[Client, Record]:
    client = Client(
        provider=PROVIDER_EASYWEEK,
        company_id=company_id,
        altegio_client_id=7300777 + identity_offset,
        phone_e164="+491700000777",
        display_name="Anna Müller",
        raw={},
    )
    db.add(client)
    await db.flush()
    record = Record(
        provider=PROVIDER_EASYWEEK,
        company_id=company_id,
        altegio_record_id=4200777 + identity_offset,
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
    # A returning customer, so `record_created` resolves to the ORDINARY row.
    # The first-time variant has its own tests further down.
    await _seed_previous_visit(db, client, record)

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
    contract = branch_template_contract(BRANCH_PROFILES["durlach"], code)
    assert contract is not None
    assert ctx["meta_template_name"] == contract.meta_template_name

    rendered = body.format(**ctx)
    assert "{" not in rendered, "every placeholder in the seeded body must exist in ctx"
    assert "Anna Müller" in rendered
    assert "Wimpernverlängerung" in rendered
    assert "KitiLash Durlach" in rendered
    assert "Pfinztalstraße 4, 76227 Karlsruhe-Durlach" in rendered
    # Phase 1 is transactional, so the marketing opt-out line must not appear.
    assert "abbestellen" not in rendered


async def test_seeded_two_branches_render_every_lifecycle_and_new_repeat_variant(
    db: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    rastatt_id = 999502
    rastatt_uuid = "dddddddd-eeee-4fff-8000-000000000002"
    rastatt_page = "https://book.rastatt.invalid/rastatt"
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "durlach": {
                    "location_id": DURLACH_LOCATION_ID,
                    "location_uuid": DURLACH_LOCATION_UUID,
                    "meta_template_prefix": "du",
                    "booking_page_url": STATIC_BOOKING_PAGE,
                },
                "rastatt": {
                    "location_id": rastatt_id,
                    "location_uuid": rastatt_uuid,
                    "meta_template_prefix": "ra",
                    "booking_page_url": rastatt_page,
                },
            }
        ),
        raising=False,
    )
    monkeypatch.setattr(
        settings,
        "easyweek_booking_page_allowed_hosts",
        f"{BOOKING_PAGE_HOST},book.rastatt.invalid",
        raising=False,
    )
    api_locations = [
        {"uuid": DURLACH_LOCATION_UUID, "name": "KitiLash Durlach"},
        {"uuid": rastatt_uuid, "name": "KitiLash Rastatt"},
    ]
    await _run_seed(db, api_locations=api_locations)

    for offset, (company_id, prefix, brand, static_page) in enumerate(
        [
            (DURLACH_LOCATION_ID, "du", "KitiLash Durlach", STATIC_BOOKING_PAGE),
            (rastatt_id, "ra", "KitiLash Rastatt", rastatt_page),
        ]
    ):
        client, record = await _seed_durlach_domain(db, company_id=company_id, identity_offset=offset * 100)

        new_body, _sender, _language, new_ctx = await ow._render_message(
            db,
            company_id=company_id,
            template_code=RECORD_CREATED,
            record=record,
            client=client,
            provider=PROVIDER_EASYWEEK,
        )
        assert new_ctx["meta_template_name"] == f"kitilash_{prefix}_record_created_new_client_v1"
        assert "Wichtige Hinweise" in new_body

        await _seed_previous_visit(db, client, record)
        for code in (RECORD_CREATED, RECORD_UPDATED, RECORD_CANCELED):
            body, _sender, _language, ctx = await ow._render_message(
                db,
                company_id=company_id,
                template_code=code,
                record=record,
                client=client,
                provider=PROVIDER_EASYWEEK,
            )
            assert ctx["meta_template_name"] == f"kitilash_{prefix}_{code}_v1"
            assert brand in body.format(**ctx)
            if code == RECORD_CANCELED:
                assert ctx["booking_link"] == static_page


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
    """An EasyWeek job must never borrow an Altegio row of the same code.

    PR-12 seeded the two retention codes, so the code used here is one that is
    still deliberately unseeded for EasyWeek — and it is a UNIVERSAL code on the
    Altegio side, which is what makes it the sharp case: universal codes are the
    ones reachable by a cross-company fallback. The invariant is unchanged and is
    exactly what keeps a newsletter template from becoming reachable the moment
    someone adds an Altegio row for the same company id.
    """
    await _run_seed(db)
    client, record = await _seed_durlach_domain(db)
    db.add(
        MessageTemplate(
            provider=PROVIDER_ALTEGIO,
            company_id=DURLACH_LOCATION_ID,
            code="newsletter_new_clients_monthly",
            language="de",
            body="ALTEGIO NEWSLETTER",
            is_active=True,
        )
    )
    await db.flush()

    with pytest.raises(ValueError) as excinfo:
        await ow._render_message(
            db,
            company_id=DURLACH_LOCATION_ID,
            template_code="newsletter_new_clients_monthly",
            record=record,
            client=client,
            provider=PROVIDER_EASYWEEK,
        )

    message = str(excinfo.value)
    assert "Template not found" in message
    assert PROVIDER_EASYWEEK in message
    assert "ALTEGIO NEWSLETTER" not in message


@pytest.mark.parametrize("code", ["repeat_10d", "comeback_3d"])
async def test_a_seeded_retention_code_renders_from_the_easyweek_row(db: AsyncSession, code: str) -> None:
    """PR-12: EasyWeek now owns these codes, and still never reads Altegio's row.

    Both providers have a `repeat_10d` and a `comeback_3d` for the SAME numeric
    company id here, which is the collision the provider predicate exists to
    survive. The EasyWeek render must resolve its own branch-bound row — Meta
    name, body and footer — and the Altegio body must not appear anywhere in it.
    """
    await _run_seed(db)
    client, record = await _seed_durlach_domain(db)
    db.add(
        MessageTemplate(
            provider=PROVIDER_ALTEGIO,
            company_id=DURLACH_LOCATION_ID,
            code=code,
            language="de",
            body="ALTEGIO RETENTION",
            meta_template_name="kitilash_ka_altegio_v1",
            is_active=True,
        )
    )
    await db.flush()

    body, _sender_id, _lang, ctx = await ow._render_message(
        db,
        company_id=DURLACH_LOCATION_ID,
        template_code=code,
        record=record,
        client=client,
        provider=PROVIDER_EASYWEEK,
    )

    assert ctx["meta_template_name"] == f"kitilash_du_{code}_v1"
    assert "ALTEGIO RETENTION" not in body
    assert "KitiLash Durlach" in body
    # Both retention messages call the customer back to the branch's own booking
    # page — never the manage link of a booking that is over or cancelled.
    assert ctx["booking_link"] == STATIC_BOOKING_PAGE
    assert VERIFIED_PAGE not in body.format(**ctx)


async def test_a_seeded_reminder_renders_from_the_easyweek_row(db: AsyncSession) -> None:
    """The other half: the reminder EasyWeek now owns resolves to its own row."""
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

    body, _sender_id, _language, ctx = await ow._render_message(
        db,
        company_id=DURLACH_LOCATION_ID,
        template_code="reminder_24h",
        record=record,
        client=client,
        provider=PROVIDER_EASYWEEK,
    )

    assert ctx["meta_template_name"] == "kitilash_du_reminder_24h_v1"
    assert "ALTEGIO REMINDER" not in body
    assert "KitiLash Durlach" in body, "the branch footer belongs to the branch"


# ---------------------------------------------------------------------------
# The first-time-customer variant
# ---------------------------------------------------------------------------


async def _seed_previous_visit(db: AsyncSession, client: Client, record: Record) -> Record:
    """An earlier EasyWeek booking for the same customer — so they are not new."""
    earlier = Record(
        provider=PROVIDER_EASYWEEK,
        company_id=record.company_id,
        altegio_record_id=record.altegio_record_id - 1,
        client_id=client.id,
        staff_name="Tanja",
        starts_at=record.starts_at - timedelta(days=30),
        total_cost=Decimal("60.00"),
        raw={},
    )
    db.add(earlier)
    await db.flush()
    return earlier


async def test_a_new_easyweek_client_gets_the_new_client_template(db: AsyncSession) -> None:
    await _run_seed(db)
    client, record = await _seed_durlach_domain(db)

    body, _sender_id, _lang, ctx = await ow._render_message(
        db,
        company_id=DURLACH_LOCATION_ID,
        template_code=RECORD_CREATED,
        record=record,
        client=client,
        provider=PROVIDER_EASYWEEK,
    )

    assert ctx["meta_template_name"] == "kitilash_du_record_created_new_client_v1"
    assert "Wichtige Hinweise" in body


async def test_a_returning_easyweek_client_gets_the_ordinary_template(db: AsyncSession) -> None:
    await _run_seed(db)
    client, record = await _seed_durlach_domain(db)
    await _seed_previous_visit(db, client, record)

    body, _sender_id, _lang, ctx = await ow._render_message(
        db,
        company_id=DURLACH_LOCATION_ID,
        template_code=RECORD_CREATED,
        record=record,
        client=client,
        provider=PROVIDER_EASYWEEK,
    )

    assert ctx["meta_template_name"] == "kitilash_du_record_created_v1"
    assert "Wichtige Hinweise" not in body


async def test_an_altegio_booking_does_not_make_an_easyweek_client_returning(db: AsyncSession) -> None:
    """The two CRMs share one integer space for company_id."""
    await _run_seed(db)
    client, record = await _seed_durlach_domain(db)
    db.add(
        Record(
            provider=PROVIDER_ALTEGIO,
            company_id=DURLACH_LOCATION_ID,
            altegio_record_id=555999,
            client_id=client.id,
            staff_name="Tanja",
            starts_at=record.starts_at - timedelta(days=30),
            raw={},
        )
    )
    await db.flush()

    _body, _sender_id, _lang, ctx = await ow._render_message(
        db,
        company_id=DURLACH_LOCATION_ID,
        template_code=RECORD_CREATED,
        record=record,
        client=client,
        provider=PROVIDER_EASYWEEK,
    )

    assert ctx["meta_template_name"] == "kitilash_du_record_created_new_client_v1"


async def test_the_new_client_variant_builds_the_record_created_param_contract(db: AsyncSession) -> None:
    """THE trap: params key on job_type, which stays `record_created`."""
    from altegio_bot.meta_templates import build_lifecycle_template_params
    from altegio_bot.template_validation import validate_lifecycle_template_params

    await _run_seed(db)
    client, record = await _seed_durlach_domain(db)

    _body, _sender_id, _lang, ctx = await ow._render_message(
        db,
        company_id=DURLACH_LOCATION_ID,
        template_code=RECORD_CREATED,
        record=record,
        client=client,
        provider=PROVIDER_EASYWEEK,
    )

    # The JOB type, not the template code — that is what makes this work.
    params = build_lifecycle_template_params(RECORD_CREATED, ctx)
    assert len(params) == 7
    assert all(params), "an empty slot would be rejected by Meta, not by us"
    assert validate_lifecycle_template_params(RECORD_CREATED, params) is None

    # And the template code on its own has no contract, which is exactly why the
    # builder must never be keyed on it.
    assert build_lifecycle_template_params(RECORD_CREATED_NEW_CLIENT, ctx) == []


async def test_the_new_client_body_renders_with_no_missing_placeholders(db: AsyncSession) -> None:
    await _run_seed(db)
    client, record = await _seed_durlach_domain(db)

    body, _sender_id, _lang, ctx = await ow._render_message(
        db,
        company_id=DURLACH_LOCATION_ID,
        template_code=RECORD_CREATED,
        record=record,
        client=client,
        provider=PROVIDER_EASYWEEK,
    )

    rendered = body.format(**ctx)
    assert "{" not in rendered
    assert "Anna Müller" in rendered
    assert "KitiLash Durlach" in rendered
    assert "Wichtige Hinweise" in rendered
    assert VERIFIED_PAGE in rendered


async def test_the_new_client_body_matches_the_shared_notes_constant(db: AsyncSession) -> None:
    """The Meta template was cloned from the Karlsruhe one; the text must not drift."""
    contract = branch_template_contract(BRANCH_PROFILES["durlach"], RECORD_CREATED_NEW_CLIENT)
    assert contract is not None
    assert ow.PRE_APPOINTMENT_NOTES_DE in contract.raw_body


async def test_a_missing_new_client_row_falls_back_instead_of_failing(db: AsyncSession) -> None:
    """A nicety must not cost a booking its confirmation."""
    await _run_seed(db)
    rows = await _easyweek_templates(db)
    for row in rows:
        if row.code == RECORD_CREATED_NEW_CLIENT:
            row.is_active = False
    await db.flush()
    client, record = await _seed_durlach_domain(db)

    _body, _sender_id, _lang, ctx = await ow._render_message(
        db,
        company_id=DURLACH_LOCATION_ID,
        template_code=RECORD_CREATED,
        record=record,
        client=client,
        provider=PROVIDER_EASYWEEK,
    )

    assert ctx["meta_template_name"] == "kitilash_du_record_created_v1"


# ---------------------------------------------------------------------------
# Booking page host allowlist (PR-5 debt closed in PR-6)
# ---------------------------------------------------------------------------


async def test_a_booking_page_on_an_unlisted_host_is_rejected(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from altegio_bot.easyweek_policy import validate_static_booking_page

    assert validate_static_booking_page(STATIC_BOOKING_PAGE) == STATIC_BOOKING_PAGE
    assert validate_static_booking_page("https://typo.example.invalid/durlach") is None


async def test_an_empty_allowlist_rejects_everything(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unconfirmed host must stop the activation, not wave any host through."""
    from altegio_bot.easyweek_policy import validate_static_booking_page

    monkeypatch.setattr(settings, "easyweek_booking_page_allowed_hosts", "", raising=False)
    assert validate_static_booking_page(STATIC_BOOKING_PAGE) is None


async def test_a_canceled_send_fails_closed_while_the_host_is_unconfirmed(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """record_canceled has no other link, so an unlisted host stops it locally."""
    await _run_seed(db)
    client, record = await _seed_durlach_domain(db)
    monkeypatch.setattr(settings, "easyweek_booking_page_allowed_hosts", "", raising=False)

    _body, _sender_id, _lang, ctx = await ow._render_message(
        db,
        company_id=DURLACH_LOCATION_ID,
        template_code=RECORD_CANCELED,
        record=record,
        client=client,
        provider=PROVIDER_EASYWEEK,
    )

    assert ctx["booking_link"] == "", "no link is better than an unverified one"


async def test_the_allowlist_is_an_origin_check_not_a_hostname_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A different port is a different service behind the same name.

    `urlsplit(...).hostname` strips the port, so matching on it alone let
    `https://allowed.host:4443/` through verbatim — and this value is the link a
    customer taps after a cancellation.
    """
    from altegio_bot.easyweek_policy import validate_static_booking_page

    monkeypatch.setattr(settings, "easyweek_booking_page_allowed_hosts", "booking.example.com", raising=False)

    # Same origin: no port, and the redundant but equivalent :443.
    assert validate_static_booking_page("https://booking.example.com/durlach") == (
        "https://booking.example.com/durlach"
    )
    assert validate_static_booking_page("https://booking.example.com:443/durlach") == (
        "https://booking.example.com:443/durlach"
    )

    # Any other port is a different origin.
    assert validate_static_booking_page("https://booking.example.com:4443/durlach") is None
    assert validate_static_booking_page("https://booking.example.com:80/durlach") is None
    assert validate_static_booking_page("https://booking.example.com:8443/durlach") is None


# ===========================================================================
# PR-7: branch identity is indivisible
# ===========================================================================

RASTATT_LOCATION_ID = 999502
RASTATT_LOCATION_UUID = "dddddddd-eeee-4fff-8000-000000000002"
RASTATT_PAGE = f"https://{BOOKING_PAGE_HOST}/rastatt"

_BOTH_API_LOCATIONS = [
    {"uuid": DURLACH_LOCATION_UUID, "name": "KitiLash Durlach"},
    {"uuid": RASTATT_LOCATION_UUID, "name": "KitiLash Rastatt"},
]

# PR-11.1 moved Karlsruhe onto EasyWeek, so the production registry now holds
# three branches. Without a source-controlled profile the seed, the preflight and
# the send path all refuse it — which is why the profile exists and why these
# fixtures cover it like any other branch.
KARLSRUHE_LOCATION_ID = 999503
KARLSRUHE_LOCATION_UUID = "dddddddd-eeee-4fff-8000-000000000003"
KARLSRUHE_PAGE = f"https://{BOOKING_PAGE_HOST}/karlsruhe"

_ALL_API_LOCATIONS = [
    *_BOTH_API_LOCATIONS,
    {"uuid": KARLSRUHE_LOCATION_UUID, "name": "KitiLash Karlsruhe"},
]


def _all_three_registry(monkeypatch: pytest.MonkeyPatch) -> None:
    _set_registry(
        monkeypatch,
        {
            "durlach": _entry(DURLACH_LOCATION_ID, DURLACH_LOCATION_UUID, "du", STATIC_BOOKING_PAGE),
            "rastatt": _entry(RASTATT_LOCATION_ID, RASTATT_LOCATION_UUID, "ra", RASTATT_PAGE),
            "karlsruhe": _entry(KARLSRUHE_LOCATION_ID, KARLSRUHE_LOCATION_UUID, "ka", KARLSRUHE_PAGE),
        },
    )
    monkeypatch.setattr(
        settings,
        "easyweek_booking_page_allowed_hosts",
        BOOKING_PAGE_HOST,
        raising=False,
    )


def _set_registry(monkeypatch: pytest.MonkeyPatch, entries: dict[str, dict[str, Any]]) -> None:
    monkeypatch.setattr(settings, "easyweek_location_map", json.dumps(entries), raising=False)


def _entry(location_id: int, location_uuid: str, prefix: str, page: str) -> dict[str, Any]:
    return {
        "location_id": location_id,
        "location_uuid": location_uuid,
        "meta_template_prefix": prefix,
        "booking_page_url": page,
    }


async def _easyweek_state(db: AsyncSession) -> tuple[int, int]:
    templates = await _count(db, MessageTemplate, provider=PROVIDER_EASYWEEK)
    senders = await _count(db, WhatsAppSender, provider=PROVIDER_EASYWEEK)
    return templates, senders


@pytest.mark.parametrize(
    ("slug", "location_id", "location_uuid", "wrong_prefix", "page"),
    [
        ("durlach", DURLACH_LOCATION_ID, DURLACH_LOCATION_UUID, "ra", STATIC_BOOKING_PAGE),
        ("rastatt", RASTATT_LOCATION_ID, RASTATT_LOCATION_UUID, "du", RASTATT_PAGE),
    ],
)
async def test_a_crossed_meta_prefix_is_refused_before_any_write(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    slug: str,
    location_id: int,
    location_uuid: str,
    wrong_prefix: str,
    page: str,
) -> None:
    """The §10 defect: real ids of one branch wearing the other's prefix."""
    before = await _easyweek_state(db)
    _set_registry(monkeypatch, {slug: _entry(location_id, location_uuid, wrong_prefix, page)})

    with pytest.raises(seed_script.SeedConfigError) as excinfo:
        await _run_seed(db, api_locations=_BOTH_API_LOCATIONS)

    assert "meta_template_prefix" in str(excinfo.value)
    assert await _easyweek_state(db) == before, "a refused seed still wrote to the database"


async def test_the_other_branchs_uuid_under_a_slug_is_refused_even_with_a_matching_prefix(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Prefix and slug agree; only the live API can expose the swapped UUID.

    This is the case a prefix check alone cannot catch, and the one that
    actually happened: every local field is self-consistent, and the identity is
    still wrong.
    """
    before = await _easyweek_state(db)
    _set_registry(
        monkeypatch,
        {"durlach": _entry(DURLACH_LOCATION_ID, RASTATT_LOCATION_UUID, "du", STATIC_BOOKING_PAGE)},
    )

    with pytest.raises(seed_script.SeedConfigError) as excinfo:
        await _run_seed(db, api_locations=_BOTH_API_LOCATIONS)

    message = str(excinfo.value)
    assert "API identity mismatch" in message
    assert "durlach" in message
    assert await _easyweek_state(db) == before


async def test_an_unknown_branch_slug_is_refused(db: AsyncSession, monkeypatch: pytest.MonkeyPatch) -> None:
    """A branch cannot be seeded until its profile is approved in source.

    The slug here is one that genuinely has no profile. Karlsruhe used to play
    that role and no longer can — PR-11.1 put it in the production registry, so
    it has a real approved profile now — but the RULE is unchanged, and this
    keeps proving it.
    """
    before = await _easyweek_state(db)
    _set_registry(
        monkeypatch,
        {"ettlingen": _entry(999504, "dddddddd-eeee-4fff-8000-000000000004", "et", STATIC_BOOKING_PAGE)},
    )

    with pytest.raises(seed_script.SeedConfigError) as excinfo:
        await _run_seed(db, api_locations=_ALL_API_LOCATIONS)

    assert "no source-controlled profile" in str(excinfo.value)
    assert await _easyweek_state(db) == before


async def test_both_correct_profiles_seed_a_full_suite_each_and_two_senders(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _set_registry(
        monkeypatch,
        {
            "durlach": _entry(DURLACH_LOCATION_ID, DURLACH_LOCATION_UUID, "du", STATIC_BOOKING_PAGE),
            "rastatt": _entry(RASTATT_LOCATION_ID, RASTATT_LOCATION_UUID, "ra", RASTATT_PAGE),
        },
    )
    await _run_seed(db, api_locations=_BOTH_API_LOCATIONS)

    templates, senders = await _easyweek_state(db)
    assert (templates, senders) == (18, 2)

    rows = await _easyweek_templates(db)
    by_company: dict[int, set[str]] = {}
    for row in rows:
        by_company.setdefault(row.company_id, set()).add(row.meta_template_name or "")

    assert all(name.startswith("kitilash_du_") for name in by_company[DURLACH_LOCATION_ID])
    assert all(name.startswith("kitilash_ra_") for name in by_company[RASTATT_LOCATION_ID])

    durlach_bodies = " ".join(r.body for r in rows if r.company_id == DURLACH_LOCATION_ID)
    rastatt_bodies = " ".join(r.body for r in rows if r.company_id == RASTATT_LOCATION_ID)
    assert "Durlach" in durlach_bodies and "Rastatt" not in durlach_bodies
    assert "Rastatt" in rastatt_bodies and "Durlach" not in rastatt_bodies


async def test_contaminated_rows_converge_to_the_right_branch(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Rastatt rows previously seeded with Durlach names are repaired in place."""
    _set_registry(
        monkeypatch,
        {"rastatt": _entry(RASTATT_LOCATION_ID, RASTATT_LOCATION_UUID, "ra", RASTATT_PAGE)},
    )
    contaminated = MessageTemplate(
        provider=PROVIDER_EASYWEEK,
        company_id=RASTATT_LOCATION_ID,
        code="record_created",
        language="de",
        body="Durlach body that never belonged here",
        meta_template_name="kitilash_du_record_created_v1",
        is_active=True,
    )
    db.add(contaminated)
    await db.flush()

    await _run_seed(db, api_locations=_BOTH_API_LOCATIONS)

    rows = [r for r in await _easyweek_templates(db) if r.company_id == RASTATT_LOCATION_ID]
    assert rows, "the branch was not seeded"
    assert all((r.meta_template_name or "").startswith("kitilash_ra_") for r in rows), (
        "a du_* name survived a Rastatt seed"
    )
    assert all("Durlach" not in r.body for r in rows)


async def test_three_synthetic_branches_parse_and_scope_independently(monkeypatch: pytest.MonkeyPatch) -> None:
    """No 'exactly two' assumption anywhere in the registry.

    Uses synthetic entries on purpose: a real third branch needs approved
    metadata, and inventing one here would put unverified production data in
    source. What is proven is that parsing, membership and provider-scoped
    company ids already work for three.
    """
    entries = {
        f"branch{n}": _entry(999600 + n, f"dddddddd-eeee-4fff-8000-00000000010{n}", f"b{n}", STATIC_BOOKING_PAGE)
        for n in (1, 2, 3)
    }
    _set_registry(monkeypatch, entries)

    registry = configured_easyweek_locations()
    assert registry.ready
    assert len(registry.locations) == 3
    assert {loc.company_id for loc in registry.locations.values()} == {999601, 999602, 999603}
    for n in (1, 2, 3):
        location = registry.locations[999600 + n]
        assert location.name == f"branch{n}"
        assert location.company_id == location.location_id


async def test_an_unprofiled_branch_is_member_but_cannot_seed(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Membership and profile are different questions, and both are enforced."""
    _set_registry(
        monkeypatch,
        {"branch1": _entry(999601, "dddddddd-eeee-4fff-8000-000000000101", "b1", STATIC_BOOKING_PAGE)},
    )
    assert 999601 in configured_easyweek_locations().locations

    before = await _easyweek_state(db)
    with pytest.raises(seed_script.SeedConfigError):
        await _run_seed(db, api_locations=_BOTH_API_LOCATIONS)
    assert await _easyweek_state(db) == before


# ---------------------------------------------------------------------------
# Karlsruhe: the third production branch (PR-11.1 put it on EasyWeek)
# ---------------------------------------------------------------------------


async def test_the_three_branch_production_registry_seeds(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """durlach + rastatt + karlsruhe — the registry production actually holds.

    Without an approved Karlsruhe profile this raised before writing a single
    row, which made the seed, the preflight and the send path unexecutable for a
    branch whose customers are already on EasyWeek.
    """
    _all_three_registry(monkeypatch)

    result = await _run_seed(db, api_locations=_ALL_API_LOCATIONS)

    assert result.templates_created == 27, "nine codes for each of three branches"
    assert result.senders_created == 3

    rows = await _easyweek_templates(db)
    by_company: dict[int, list[MessageTemplate]] = {}
    for row in rows:
        by_company.setdefault(row.company_id, []).append(row)
    assert set(by_company) == {DURLACH_LOCATION_ID, RASTATT_LOCATION_ID, KARLSRUHE_LOCATION_ID}
    assert all(row.meta_template_name.startswith("kitilash_ka_") for row in by_company[KARLSRUHE_LOCATION_ID])


async def test_the_karlsruhe_footer_is_its_own_source_controlled_content(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The address a customer reads must be Karlsruhe's, not a neighbour's."""
    _all_three_registry(monkeypatch)
    await _run_seed(db, api_locations=_ALL_API_LOCATIONS)

    rows = [row for row in await _easyweek_templates(db) if row.company_id == KARLSRUHE_LOCATION_ID]
    assert rows
    for row in rows:
        assert "76133 Karlsruhe, Kaiserstraße, 68" in row.body
        assert "Pfinztalstraße" not in row.body
        assert "Rathausstraße" not in row.body


async def test_karlsruhe_is_verified_against_its_exact_api_name(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """EasyWeek is the independent third party, for Karlsruhe like every branch."""
    before = await _easyweek_state(db)
    _all_three_registry(monkeypatch)
    wrong_name = [
        *_BOTH_API_LOCATIONS,
        {"uuid": KARLSRUHE_LOCATION_UUID, "name": "KitiLash Durlach"},
    ]

    with pytest.raises(seed_script.SeedConfigError) as excinfo:
        await _run_seed(db, api_locations=wrong_name)

    assert "identity mismatch" in str(excinfo.value)
    assert "karlsruhe" in str(excinfo.value)
    assert await _easyweek_state(db) == before, "nothing is written before identity is proven"


async def test_a_wrong_karlsruhe_prefix_is_refused_before_any_write(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The prefix is data an operator supplies; the profile owns the truth.

    A prefix DUPLICATED across two branches is refused even earlier, by the
    registry parser itself. This is the subtler case: a unique prefix that simply
    is not the one Karlsruhe owns, which only the profile can catch.
    """
    before = await _easyweek_state(db)
    _set_registry(
        monkeypatch,
        {
            "durlach": _entry(DURLACH_LOCATION_ID, DURLACH_LOCATION_UUID, "du", STATIC_BOOKING_PAGE),
            "karlsruhe": _entry(KARLSRUHE_LOCATION_ID, KARLSRUHE_LOCATION_UUID, "kx", KARLSRUHE_PAGE),
        },
    )
    monkeypatch.setattr(settings, "easyweek_booking_page_allowed_hosts", BOOKING_PAGE_HOST, raising=False)

    with pytest.raises(seed_script.SeedConfigError) as excinfo:
        await _run_seed(db, api_locations=_ALL_API_LOCATIONS)

    assert "meta_template_prefix" in str(excinfo.value)
    assert await _easyweek_state(db) == before


async def test_the_three_branch_seed_stays_idempotent(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _all_three_registry(monkeypatch)
    plan = await seed_script.build_seed_plan(client_factory=_client_factory(_ALL_API_LOCATIONS))

    first = await seed_script.seed(db, plan=plan)
    await db.flush()
    second = await seed_script.seed(db, plan=plan)
    await db.flush()

    assert first.templates_created == 27
    assert second.templates_created == 0
    assert second.templates_updated == 27
    assert await _easyweek_state(db) == (27, 3)


@pytest.mark.parametrize("code", ["repeat_10d", "comeback_3d"])
async def test_karlsruhe_retention_rows_are_bound_to_karlsruhe(
    db: AsyncSession,
    monkeypatch: pytest.MonkeyPatch,
    code: str,
) -> None:
    """Send and preflight accept Karlsruhe only on a complete branch contract."""
    _all_three_registry(monkeypatch)
    await _run_seed(db, api_locations=_ALL_API_LOCATIONS)

    rows = [
        row for row in await _easyweek_templates(db) if row.company_id == KARLSRUHE_LOCATION_ID and row.code == code
    ]
    assert len(rows) == 1
    row = rows[0]
    contract = branch_template_contract(BRANCH_PROFILES["karlsruhe"], code)
    assert contract is not None
    assert row.meta_template_name == contract.meta_template_name == f"kitilash_ka_{code}_v1"
    assert row.body == contract.raw_body
    assert row.is_active is True
