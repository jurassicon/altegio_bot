"""PR-12: the read-only preflight that earns the right to open the send fence.

Two properties carry the whole design, and this file exists to pin them:

* **Green is narrow.** At least one real candidate, nothing truncated, every
  candidate proven, and the rollout in exactly the state the report claims to
  describe. An empty queue is not a clean bill of health — it means there was
  nothing to find problems in.
* **It writes nothing and leaks nothing.** No job, record, client or outbox row
  changes, and the report carries counts, internal ids and stable reason codes —
  never a phone, a name, a booking uuid or a visit count.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_branches import BRANCH_PROFILES, BranchProfile, branch_template_contract
from altegio_bot.easyweek_policy import COMEBACK_3D, REPEAT_10D
from altegio_bot.easyweek_retention import (
    COMEBACK_DELAY,
    REPEAT_DELAY,
    RETENTION_CLIENT_RETURNED,
    comeback_job_payload,
    repeat_job_payload,
)
from altegio_bot.easyweek_service_category import (
    record_raw_with_service_category,
    record_raw_with_services_count,
)
from altegio_bot.models.models import (
    PROVIDER_EASYWEEK,
    Client,
    MessageJob,
    MessageTemplate,
    Record,
    RecordService,
    WhatsAppSender,
)
from altegio_bot.scripts import easyweek_retention_preflight as pf
from altegio_bot.settings import settings

pytestmark = pytest.mark.asyncio

COMPANY_ID = 999501
LOCATION_UUID = "cccccccc-dddd-4eee-8fff-000000000001"
STATIC_BOOKING_PAGE = "https://example.invalid/book"
BOOKING_UUID = uuid.UUID("11111111-2222-4333-8444-555555555555")
SERVICE_TITLE = "Wimpernverlängerung"
CLIENT_PHONE = "+491700000001"
CLIENT_NAME = "Anna Müller"


@pytest.fixture(autouse=True)
def _rollout_state(monkeypatch: pytest.MonkeyPatch) -> None:
    """The one state a green report is a statement about."""
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_retention_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_retention_send_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_default_language", "de", raising=False)
    monkeypatch.setattr(settings, "easyweek_booking_page_allowed_hosts", "example.invalid", raising=False)
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", json.dumps([SERVICE_TITLE]), raising=False)
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "durlach": {
                    "location_id": COMPANY_ID,
                    "location_uuid": LOCATION_UUID,
                    "meta_template_prefix": "du",
                    "booking_page_url": STATIC_BOOKING_PAGE,
                }
            }
        ),
        raising=False,
    )
    monkeypatch.setitem(
        BRANCH_PROFILES,
        "durlach",
        BranchProfile(
            slug="durlach",
            api_name="KitiLash Durlach",
            meta_template_prefix="du",
            content=BRANCH_PROFILES["durlach"].content,
        ),
    )


@pytest_asyncio.fixture
async def db(session_maker: async_sessionmaker[AsyncSession]) -> AsyncSession:
    async with session_maker() as session:
        yield session


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def _seed_candidate(
    db: AsyncSession,
    *,
    job_type: str = REPEAT_10D,
    visits_total: int | None = 4,
    baseline: int = 4,
    with_template: bool = True,
    with_sender: bool = True,
    dedupe_key: str = "easyweek_retention:test:1",
    index: int = 0,
) -> MessageJob:
    """One provable candidate. ``index`` seeds a SECOND independent booking.

    Client, record and booking uuid all have to differ: they are unique per
    tenant, which is the point — two candidates in a queue are two people.
    """
    client = Client(
        provider=PROVIDER_EASYWEEK,
        company_id=COMPANY_ID,
        altegio_client_id=7300002 + index,
        phone_e164=CLIENT_PHONE,
        display_name=CLIENT_NAME,
        easyweek_visits_total=visits_total,
        easyweek_visits_total_updated_at=_utcnow() if visits_total is not None else None,
        raw={},
    )
    db.add(client)
    await db.flush()

    cancelled = job_type == COMEBACK_3D
    booking_uuid = BOOKING_UUID if index == 0 else uuid.UUID(int=BOOKING_UUID.int + index)
    record = Record(
        provider=PROVIDER_EASYWEEK,
        company_id=COMPANY_ID,
        altegio_record_id=4200001 + index,
        easyweek_booking_uuid=booking_uuid,
        easyweek_booking_hash_id="90000001",
        client_id=client.id,
        staff_name="Tanja",
        starts_at=_utcnow() + timedelta(days=1) if cancelled else _utcnow() - timedelta(days=10),
        total_cost=Decimal("60.00"),
        is_deleted=cancelled,
        raw=record_raw_with_services_count(record_raw_with_service_category({}, SERVICE_TITLE), 1),
    )
    db.add(record)
    await db.flush()
    db.add(RecordService(record_id=record.id, service_id=11, title=SERVICE_TITLE, cost_to_pay=Decimal("60.00"), raw={}))

    if with_template and index == 0:
        contract = branch_template_contract(BRANCH_PROFILES["durlach"], job_type)
        assert contract is not None
        db.add(
            MessageTemplate(
                provider=PROVIDER_EASYWEEK,
                company_id=COMPANY_ID,
                code=job_type,
                language="de",
                body=contract.raw_body,
                meta_template_name=contract.meta_template_name,
                is_active=True,
            )
        )
    if with_sender and index == 0:
        db.add(
            WhatsAppSender(
                provider=PROVIDER_EASYWEEK,
                company_id=COMPANY_ID,
                sender_code="default",
                phone_number_id="pn-easyweek-1",
                is_active=True,
            )
        )
    await db.flush()

    assert record.starts_at is not None
    if job_type == REPEAT_10D:
        payload: dict[str, Any] = repeat_job_payload(
            booking_uuid=booking_uuid,
            company_id=COMPANY_ID,
            starts_at=record.starts_at,
            visits_baseline=baseline,
        )
        run_at = record.starts_at + REPEAT_DELAY
    else:
        cancelled_at = _utcnow() - COMEBACK_DELAY
        payload = comeback_job_payload(
            booking_uuid=booking_uuid,
            company_id=COMPANY_ID,
            cancelled_at=cancelled_at,
            visits_baseline=baseline,
        )
        run_at = cancelled_at + COMEBACK_DELAY

    job = MessageJob(
        provider=PROVIDER_EASYWEEK,
        company_id=COMPANY_ID,
        record_id=record.id,
        client_id=client.id,
        job_type=job_type,
        run_at=run_at,
        status="queued",
        dedupe_key=dedupe_key,
        payload=payload,
    )
    db.add(job)
    await db.flush()
    return job


# ===========================================================================
# Rollout state
# ===========================================================================


@pytest.mark.parametrize(
    ("field", "value", "expected"),
    [
        ("easyweek_notifications_enabled", False, pf.REASON_NOTIFICATIONS_DISABLED),
        ("easyweek_retention_enabled", False, pf.REASON_PLANNING_DISABLED),
        ("easyweek_retention_send_enabled", True, pf.REASON_SEND_FENCE_OPEN),
        ("easyweek_visit_counter_enabled", False, pf.REASON_VISIT_COUNTER_DISABLED),
    ],
)
async def test_the_wrong_rollout_state_is_reported_and_the_queue_is_not_read(
    db: AsyncSession, monkeypatch, field: str, value: bool, expected: str
) -> None:
    await _seed_candidate(db)
    monkeypatch.setattr(settings, field, value, raising=False)

    report = await pf.run_retention_preflight(db)

    assert report.config_error == expected
    assert report.candidate_count == 0, "an audit that does not apply reads nothing"
    assert report.ready is False


async def test_an_unconfigured_registry_is_named_rather_than_read_as_an_empty_queue(
    db: AsyncSession, monkeypatch
) -> None:
    monkeypatch.setattr(settings, "easyweek_location_map", "{}", raising=False)

    report = await pf.run_retention_preflight(db)

    assert report.config_error == pf.REASON_LOCATION_REGISTRY_UNCONFIGURED
    assert report.ready is False


# ===========================================================================
# The verdict
# ===========================================================================


async def test_an_empty_queue_is_never_green(db: AsyncSession) -> None:
    """A fence opened on "no problems found" is opened blind."""
    report = await pf.run_retention_preflight(db)

    assert report.candidate_count == 0
    assert report.ready is False


@pytest.mark.parametrize("job_type", [REPEAT_10D, COMEBACK_3D])
async def test_one_provable_candidate_is_green(db: AsyncSession, job_type: str) -> None:
    await _seed_candidate(db, job_type=job_type)

    report = await pf.run_retention_preflight(db)

    assert report.candidate_count == 1
    assert report.green_count == 1
    assert report.blocked_count == 0
    assert report.job_types == {job_type: 1}
    assert report.ready is True


async def test_a_returned_customer_blocks_the_report(db: AsyncSession) -> None:
    """The verdict is the runtime's, not a second implementation's."""
    await _seed_candidate(db, visits_total=9, baseline=4)

    report = await pf.run_retention_preflight(db)

    assert report.reasons[RETENTION_CLIENT_RETURNED] == 1
    assert report.blocked_count == 1
    assert report.ready is False


async def test_a_missing_template_row_blocks_the_report(db: AsyncSession) -> None:
    await _seed_candidate(db, with_template=False)

    report = await pf.run_retention_preflight(db)

    assert report.reasons[pf.REASON_TEMPLATE_MISSING] == 1
    assert report.ready is False


async def test_a_missing_sender_blocks_the_report(db: AsyncSession) -> None:
    await _seed_candidate(db, with_sender=False)

    report = await pf.run_retention_preflight(db)

    assert report.reasons[pf.REASON_SENDER_MISSING] == 1
    assert report.ready is False


async def test_a_claimed_job_is_reported_rather_than_skipped(db: AsyncSession) -> None:
    """With the fence shut nothing should ever be `processing`."""
    job = await _seed_candidate(db)
    job.status = "processing"
    await db.flush()

    report = await pf.run_retention_preflight(db)

    assert report.reasons[pf.REASON_CLAIMED_WHILE_FENCED] == 1
    assert report.ready is False


async def test_a_truncated_queue_is_never_green(db: AsyncSession) -> None:
    await _seed_candidate(db, dedupe_key="k1")

    report = await pf.run_retention_preflight(db, limit=1)
    assert report.ready is True, "a queue that fits is green"

    # A second, equally provable candidate pushes the queue past the limit.
    await _seed_candidate(db, dedupe_key="k2", index=1)
    truncated = await pf.run_retention_preflight(db, limit=1)

    assert truncated.truncated is True
    assert truncated.ready is False


async def test_altegio_retention_jobs_are_not_audited(db: AsyncSession) -> None:
    """This fence governs one provider; auditing the other's queue would lie."""
    await _seed_candidate(db)
    db.add(
        MessageJob(
            provider="altegio",
            company_id=COMPANY_ID,
            record_id=None,
            client_id=None,
            job_type=REPEAT_10D,
            run_at=_utcnow(),
            status="queued",
            dedupe_key="altegio-repeat-1",
            payload={},
        )
    )
    await db.flush()

    report = await pf.run_retention_preflight(db)

    assert report.candidate_count == 1


# ===========================================================================
# Read-only, and PII-free
# ===========================================================================


async def test_the_preflight_writes_nothing(db: AsyncSession) -> None:
    job = await _seed_candidate(db, visits_total=9, baseline=4)
    before = (job.status, job.attempts, job.last_error, dict(job.payload))

    await pf.run_retention_preflight(db)
    await db.refresh(job)

    assert (job.status, job.attempts, job.last_error, dict(job.payload)) == before
    client = (await db.execute(select(Client).where(Client.provider == PROVIDER_EASYWEEK))).scalars().one()
    assert client.easyweek_visits_total == 9, "the counter is read, never adjusted"


async def test_the_report_carries_no_customer_data(db: AsyncSession) -> None:
    await _seed_candidate(db, visits_total=9, baseline=4)

    rendered = json.dumps((await pf.run_retention_preflight(db)).as_safe_dict(), default=str)

    for secret in (CLIENT_PHONE, CLIENT_NAME, str(BOOKING_UUID), SERVICE_TITLE, STATIC_BOOKING_PAGE):
        assert secret not in rendered, secret
    # The visit counts themselves are a customer fact, and never reported.
    assert '"9"' not in rendered and ": 9" not in rendered


async def test_the_report_states_that_it_authorises_nothing(db: AsyncSession) -> None:
    await _seed_candidate(db)

    payload = (await pf.run_retention_preflight(db)).as_safe_dict()

    assert payload["read_only"] is True
    assert payload["send_authorized"] is False
    assert payload["migration_authorized"] is False


async def test_a_check_that_raises_is_never_a_pass(db: AsyncSession, monkeypatch) -> None:
    """ "We could not check" must not read as "everything is fine"."""
    await _seed_candidate(db)

    async def _boom(*args: Any, **kwargs: Any) -> str:
        raise RuntimeError("nope")

    monkeypatch.setattr(pf, "check_retention_job", _boom)

    report = await pf.run_retention_preflight(db)

    assert report.reasons["check_failed"] == 1
    assert report.ready is False
