"""PR-9: the review preflight must be hard to pass and impossible to write through.

This command is what earns the right to open the review send fence, so its
failure mode is a false green: reporting ready on an empty queue, on a bounded
slice of a longer one, or on jobs whose branch template or sender is not
actually configured. Each of those is pinned here.

It is also read next to production data, so the other half of these tests is
that it writes nothing at all and that its report carries no booking uuid, no
hash, no review URL and nothing belonging to a customer.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta

import pytest
from sqlalchemy import select

from altegio_bot.easyweek_branches import BRANCH_PROFILES, branch_template_contract
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
from altegio_bot.scripts import easyweek_review_preflight as preflight
from altegio_bot.scripts.easyweek_review_preflight import (
    EASYWEEK_SENDER_CODE,
    OPEN_STATUSES,
    PROVEN,
    REASON_CATEGORY,
    REASON_CATEGORY_CONFIG,
    REASON_CLAIMED_WHILE_FENCED,
    REASON_DEADLINE,
    REASON_DOMAIN,
    REASON_NOT_OWNED,
    REASON_NOTIFICATIONS_DISABLED,
    REASON_PHONE_MISSING,
    REASON_PLANNING_DISABLED,
    REASON_SEND_FENCE_OPEN,
    REASON_SENDER_MISSING,
    REASON_SENDER_PHONE_ID_EMPTY,
    REASON_TEMPLATE_CONTRACT,
    REASON_TEMPLATE_DUPLICATE,
    REASON_TEMPLATE_MISSING,
    REASON_TEMPLATE_PARAMS,
    ReviewPreflightReport,
    _parse_args,
    main,
    rollout_state_error,
    run_review_preflight,
    select_open_review_jobs,
)
from altegio_bot.settings import settings
from altegio_bot.utils import utcnow

BOOKING = uuid.UUID("11111111-2222-4333-8444-555555555555")
HASH = "90000001"
REVIEW_URL = "https://g.page/r/CaV0vSmrSYkdEAE/review"
# PR-10: the link is resolved from our own configuration by company_id.
OTHER_REVIEW_URL = "https://g.page/r/DifferentTokenAB/review"
COMPANY_ID = 999501
OTHER_COMPANY_ID = 999502
LOCATION_UUID = "dddddddd-eeee-4fff-8000-000000000001"
OTHER_LOCATION_UUID = "dddddddd-eeee-4fff-8000-000000000002"
CATEGORY = "Wimpernverlängerung"


@pytest.fixture(autouse=True)
def _pr9_settings(monkeypatch: pytest.MonkeyPatch) -> None:
    """Two configured branches, review planning on, the send fence shut."""
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_reviews_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_review_send_enabled", False, raising=False)
    # §31.11: the visit-counter contract is what makes a review provable at all,
    # so the preflight's baseline has it on. "send on, counter off" has its own
    # case below and is a red configuration, not a supported mode.
    monkeypatch.setattr(settings, "easyweek_visit_counter_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_default_language", "de", raising=False)
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", json.dumps([CATEGORY]), raising=False)
    monkeypatch.setattr(
        settings,
        "easyweek_google_review_links",
        json.dumps({str(COMPANY_ID): REVIEW_URL, str(OTHER_COMPANY_ID): OTHER_REVIEW_URL}),
        raising=False,
    )
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "durlach": {
                    "location_id": COMPANY_ID,
                    "location_uuid": LOCATION_UUID,
                    "meta_template_prefix": "du",
                    "booking_page_url": "https://book.durlach.invalid/d",
                },
                "rastatt": {
                    "location_id": OTHER_COMPANY_ID,
                    "location_uuid": OTHER_LOCATION_UUID,
                    "meta_template_prefix": "ra",
                    "booking_page_url": "https://book.rastatt.invalid/r",
                },
            }
        ),
        raising=False,
    )
    monkeypatch.setattr(settings, "easyweek_booking_page_allowed_hosts", "book.durlach.invalid", raising=False)


class _UnsetPhone:
    """Distinguishes "left at the default" from an explicit ``None``."""


_UNSET_PHONE = _UnsetPhone()


def _branch(company_id: int) -> str:
    return "durlach" if company_id == COMPANY_ID else "rastatt"


async def _seed_review(
    session,
    *,
    company_id: int = COMPANY_ID,
    provider: str = PROVIDER_EASYWEEK,
    booking: uuid.UUID | None = BOOKING,
    booking_hash: str | None = HASH,
    starts_at: datetime | None = None,
    planned_start: datetime | None = None,
    review_url: object = REVIEW_URL,
    job_type: str = "review_3d",
    status: str = "queued",
    is_deleted: bool = False,
    opted_out: bool = False,
    category: str | None = CATEGORY,
    services_count: int | None = 1,
    with_template: bool = True,
    template_name: str | None = None,
    template_body: str | None = None,
    template_language: str = "de",
    template_active: bool = True,
    duplicate_template: bool = False,
    with_sender: bool = True,
    sender_active: bool = True,
    sender_code: str = "default",
    sender_provider: str = PROVIDER_EASYWEEK,
    sender_company_id: int | None = None,
    sender_phone_number_id: str | None = None,
    phone_e164: object = _UNSET_PHONE,
    display_name: str | None = "Anna Müller",
    link_client: bool = True,
    suffix: str = "1",
    visits_total: int | None = 1,
) -> MessageJob:
    """One queued review job with every surrounding row it needs."""
    start = starts_at or (utcnow() - timedelta(hours=1))

    client = Client(
        provider=provider,
        company_id=company_id,
        altegio_client_id=7000 + int(suffix),
        display_name=display_name,
        phone_e164=(f"+4917000000{suffix}" if phone_e164 is _UNSET_PHONE else phone_e164),
        email="anna@example.invalid",
        wa_opted_out=opted_out,
        raw={},
        # Plan §31.11: the preflight asks the same visit question the sender
        # will. Stated explicitly — a first visit — so a green verdict here
        # means a customer who is genuinely eligible, and the limit itself is
        # exercised by its own cases below.
        easyweek_visits_total=visits_total if provider == PROVIDER_EASYWEEK else None,
        easyweek_visits_total_updated_at=(
            utcnow() if provider == PROVIDER_EASYWEEK and visits_total is not None else None
        ),
    )
    session.add(client)
    await session.flush()

    raw: dict = {}
    if category is not None:
        raw = record_raw_with_service_category(raw, category)
    if services_count is not None:
        raw = record_raw_with_services_count(raw, services_count)

    record = Record(
        provider=provider,
        company_id=company_id,
        altegio_record_id=4200000 + int(suffix),
        easyweek_booking_uuid=booking if provider == PROVIDER_EASYWEEK else None,
        easyweek_booking_hash_id=booking_hash,
        client_id=client.id if link_client else None,
        staff_name="Tanja",
        starts_at=start,
        is_deleted=is_deleted,
        raw=raw,
    )
    session.add(record)
    await session.flush()
    session.add(RecordService(record_id=record.id, service_id=11, title=CATEGORY, cost_to_pay=None, raw={}))

    existing_template = (
        (
            await session.execute(
                select(MessageTemplate)
                .where(MessageTemplate.provider == PROVIDER_EASYWEEK)
                .where(MessageTemplate.company_id == company_id)
                .where(MessageTemplate.code == "review_3d")
            )
        )
        .scalars()
        .first()
    )
    existing_sender = (
        (
            await session.execute(
                select(WhatsAppSender)
                .where(WhatsAppSender.provider == PROVIDER_EASYWEEK)
                .where(WhatsAppSender.company_id == company_id)
            )
        )
        .scalars()
        .first()
    )

    if with_template and existing_template is None:
        contract = branch_template_contract(BRANCH_PROFILES[_branch(company_id)], "review_3d")
        assert contract is not None
        for index in range(2 if duplicate_template else 1):
            session.add(
                MessageTemplate(
                    provider=PROVIDER_EASYWEEK,
                    company_id=company_id,
                    code="review_3d",
                    language=template_language,
                    body=template_body if template_body is not None else contract.raw_body,
                    meta_template_name=template_name if template_name is not None else contract.meta_template_name,
                    is_active=template_active,
                )
            )
    if with_sender and existing_sender is None:
        session.add(
            WhatsAppSender(
                provider=sender_provider,
                company_id=sender_company_id if sender_company_id is not None else company_id,
                sender_code=sender_code,
                phone_number_id=(
                    sender_phone_number_id if sender_phone_number_id is not None else f"eyw-phone-{suffix}"
                ),
                is_active=sender_active,
            )
        )
    await session.flush()

    payload: dict = {
        "provider": "easyweek",
        "company_id": company_id,
        "record_starts_at": (planned_start or start).isoformat() if (planned_start or start) else None,
        "job_type": "review_3d",
    }
    if booking is not None:
        payload["booking_uuid"] = str(booking)
    if review_url is not None:
        payload["review_url"] = review_url

    job = MessageJob(
        provider=provider,
        company_id=company_id,
        record_id=record.id,
        client_id=client.id,
        job_type=job_type,
        status=status,
        dedupe_key=f"review-preflight-{suffix}",
        run_at=start + timedelta(days=3),
        payload=payload,
    )
    session.add(job)
    await session.flush()
    return job


async def _run(session_maker, **kwargs) -> ReviewPreflightReport:
    async with session_maker() as session:
        return await run_review_preflight(session, **kwargs)


# ---------------------------------------------------------------------------
# The one way through
# ---------------------------------------------------------------------------


async def test_a_fully_proven_queued_review_is_green(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)

    report = await _run(session_maker)

    assert report.candidate_count == 1
    assert report.checked_count == 1
    assert report.truncated is False
    assert report.reasons == {PROVEN: 1}
    assert report.green_count == 1
    assert report.blocked_count == 0
    assert report.blocked_job_ids == []
    assert report.ready is True


async def test_two_branches_are_each_proven_against_their_own_configuration(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, suffix="1")
            await _seed_review(
                session,
                company_id=OTHER_COMPANY_ID,
                booking=uuid.UUID("22222222-2222-4333-8444-555555555555"),
                # Each branch is planned with ITS OWN configured link.
                review_url=OTHER_REVIEW_URL,
                suffix="2",
            )

    report = await _run(session_maker)

    assert report.reasons == {PROVEN: 2}
    assert report.company_ids == {COMPANY_ID, OTHER_COMPANY_ID}
    assert report.ready is True


# ---------------------------------------------------------------------------
# What counts as a candidate
# ---------------------------------------------------------------------------


async def test_only_open_easyweek_review_jobs_are_candidates(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            wanted = await _seed_review(session, suffix="1")
            # None of these may be selected.
            await _seed_review(
                session, job_type="reminder_24h", booking=uuid.UUID("33333333-2222-4333-8444-555555555555"), suffix="2"
            )
            await _seed_review(
                session,
                job_type="record_created",
                booking=uuid.UUID("44444444-2222-4333-8444-555555555555"),
                suffix="3",
            )
            await _seed_review(
                session, status="done", booking=uuid.UUID("55555555-2222-4333-8444-555555555555"), suffix="4"
            )
            await _seed_review(
                session, status="canceled", booking=uuid.UUID("66666666-2222-4333-8444-555555555555"), suffix="5"
            )
            altegio = await _seed_review(session, provider=PROVIDER_ALTEGIO, booking=None, suffix="6")

    async with session_maker() as session:
        jobs, truncated = await select_open_review_jobs(session, limit=50)

    ids = {job.id for job in jobs}
    assert ids == {wanted.id}
    assert altegio.id not in ids, "an Altegio review is a different subsystem"
    assert truncated is False


def test_the_open_statuses_are_exactly_queued_and_processing() -> None:
    assert set(OPEN_STATUSES) == {"queued", "processing"}


async def test_an_empty_queue_is_never_green(session_maker) -> None:
    report = await _run(session_maker)

    assert report.candidate_count == 0
    assert report.ready is False


async def test_a_truncated_queue_is_never_green(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            for index in range(3):
                await _seed_review(
                    session,
                    booking=uuid.UUID(f"1111111{index}-2222-4333-8444-555555555555"),
                    suffix=str(index + 1),
                )

    report = await _run(session_maker, limit=2)

    assert report.truncated is True
    assert report.candidate_count == 2
    assert report.reasons == {PROVEN: 2}
    assert report.ready is False, "all-proven does not rescue a bounded look"


async def test_a_claimed_review_behind_a_closed_fence_is_red(session_maker) -> None:
    """Nothing should have claimed it; that is a fact, not a row to skip."""
    async with session_maker() as session:
        async with session.begin():
            job = await _seed_review(session, status="processing")

    report = await _run(session_maker)

    assert report.reasons == {REASON_CLAIMED_WHILE_FENCED: 1}
    assert report.blocked_job_ids == [job.id]
    assert report.ready is False


# ---------------------------------------------------------------------------
# Every way a candidate fails to prove itself
# ---------------------------------------------------------------------------


async def test_a_branch_outside_the_registry_is_red(session_maker, monkeypatch) -> None:
    """A VALID registry that simply does not own this job's branch.

    An empty or broken registry is a different finding — it stops the whole
    audit before the queue is read — so this case keeps a registry that works
    and lists somebody else.
    """
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "elsewhere": {
                    "location_id": 999888,
                    "location_uuid": "aaaaaaaa-bbbb-4ccc-8ddd-000000000009",
                    "meta_template_prefix": "el",
                    "booking_page_url": "https://book.durlach.invalid/d",
                }
            }
        ),
        raising=False,
    )
    monkeypatch.setattr(settings, "easyweek_google_review_links", json.dumps({"999888": REVIEW_URL}), raising=False)

    report = await _run(session_maker)

    assert report.reasons == {REASON_NOT_OWNED: 1}
    assert report.ready is False


@pytest.mark.parametrize(
    ("label", "kwargs"),
    [
        ("no-booking-uuid", {"booking": None}),
        ("record-deleted", {"is_deleted": True}),
        ("client-opted-out", {"opted_out": True}),
        ("client-not-linked", {"link_client": False}),
        ("no-booking-hash", {"booking_hash": None}),
        ("services-count-two", {"services_count": 2}),
        ("services-count-missing", {"services_count": None}),
    ],
)
async def test_an_unprovable_domain_state_is_red(session_maker, label: str, kwargs: dict) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, **kwargs)

    report = await _run(session_maker)

    assert report.green_count == 0, label
    assert report.ready is False, label


async def test_a_start_that_no_longer_matches_the_plan_is_red(session_maker) -> None:
    start = utcnow() - timedelta(hours=1)
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, starts_at=start, planned_start=start + timedelta(hours=3))

    report = await _run(session_maker)

    assert report.reasons == {REASON_DOMAIN: 1}
    assert report.ready is False


async def test_a_naive_planned_start_that_names_another_instant_is_red(session_maker) -> None:
    """A naive value is read as UTC by the shared runtime parser.

    So a naive string that spells the SAME instant still matches — that is the
    accepted `_parse_payload_datetime` behaviour, shared with lifecycle and
    reminders, and PR-9 does not reopen it. What must still be caught is a naive
    value naming a DIFFERENT moment, which is the case that would deliver a
    review for a visit that did not happen then.
    """
    start = utcnow() - timedelta(hours=1)
    async with session_maker() as session:
        async with session.begin():
            job = await _seed_review(session, starts_at=start)
            payload = dict(job.payload)
            payload["record_starts_at"] = (start + timedelta(hours=2)).replace(tzinfo=None).isoformat()
            job.payload = payload

    report = await _run(session_maker)

    assert report.reasons == {REASON_DOMAIN: 1}


async def test_a_disallowed_category_is_red(session_maker, monkeypatch) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", json.dumps(["Other"]), raising=False)

    report = await _run(session_maker)

    assert report.reasons == {REASON_CATEGORY: 1}
    assert report.ready is False


@pytest.mark.parametrize("allowlist", ["", "[]", "{invalid"])
async def test_an_unusable_allowlist_is_red_rather_than_green(session_maker, monkeypatch, allowlist: str) -> None:
    """An inability to decide is not a decision that the review is fine."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", allowlist, raising=False)

    report = await _run(session_maker)

    assert report.green_count == 0
    assert set(report.reasons) <= {REASON_CATEGORY, REASON_CATEGORY_CONFIG}
    assert report.ready is False


async def test_an_expired_review_is_red(session_maker) -> None:
    """The marketing cap already ran out while the fence was shut."""
    async with session_maker() as session:
        async with session.begin():
            job = await _seed_review(session)
            job.run_at = utcnow() - timedelta(hours=30)

    report = await _run(session_maker)

    assert report.reasons == {REASON_DEADLINE: 1}
    assert report.ready is False


async def test_a_review_still_inside_its_window_is_not_called_expired(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            job = await _seed_review(session)
            job.run_at = utcnow() - timedelta(hours=2)

    report = await _run(session_maker)

    assert report.reasons == {PROVEN: 1}
    assert report.ready is True


# --- template and sender -----------------------------------------------------


async def test_a_missing_template_row_is_red(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, with_template=False)

    report = await _run(session_maker)

    assert report.reasons == {REASON_TEMPLATE_MISSING: 1}


async def test_an_inactive_template_row_is_red(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, template_active=False)

    report = await _run(session_maker)

    assert report.reasons == {REASON_TEMPLATE_MISSING: 1}


async def test_a_template_in_another_language_is_not_used(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, template_language="en")

    report = await _run(session_maker)

    assert report.reasons == {REASON_TEMPLATE_MISSING: 1}


async def test_two_active_template_rows_are_red(session_maker) -> None:
    """A send would pick one of them by chance."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, duplicate_template=True)

    report = await _run(session_maker)

    assert report.reasons == {REASON_TEMPLATE_DUPLICATE: 1}


async def test_another_branch_template_does_not_satisfy_this_branch(session_maker) -> None:
    """Rastatt's approved name on Durlach's row is the wrong salon's message."""
    other = branch_template_contract(BRANCH_PROFILES["rastatt"], "review_3d")
    assert other is not None
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, template_name=other.meta_template_name)

    report = await _run(session_maker)

    assert report.reasons == {REASON_TEMPLATE_CONTRACT: 1}


async def test_a_body_that_drifted_from_the_source_contract_is_red(session_maker) -> None:
    """The body carries the branch footer."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, template_body="Hallo {client_name} {review_url}")

    report = await _run(session_maker)

    assert report.reasons == {REASON_TEMPLATE_CONTRACT: 1}


async def test_an_altegio_template_for_the_same_company_is_not_used(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, with_template=False)
            session.add(
                MessageTemplate(
                    provider=PROVIDER_ALTEGIO,
                    company_id=COMPANY_ID,
                    code="review_3d",
                    language="de",
                    body="ALTEGIO REVIEW",
                    meta_template_name="kitilash_ka_review_3d_v1",
                    is_active=True,
                )
            )

    report = await _run(session_maker)

    assert report.reasons == {REASON_TEMPLATE_MISSING: 1}


@pytest.mark.parametrize(
    ("label", "kwargs"),
    [("missing", {"with_sender": False}), ("inactive", {"sender_active": False})],
)
async def test_a_branch_without_its_own_active_sender_is_red(session_maker, label: str, kwargs: dict) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, **kwargs)

    report = await _run(session_maker)

    assert report.reasons == {REASON_SENDER_MISSING: 1}, label


async def test_another_branch_sender_does_not_satisfy_this_branch(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, with_sender=False)
            session.add(
                WhatsAppSender(
                    provider=PROVIDER_EASYWEEK,
                    company_id=OTHER_COMPANY_ID,
                    sender_code="default",
                    phone_number_id="eyw-other",
                    is_active=True,
                )
            )

    report = await _run(session_maker)

    assert report.reasons == {REASON_SENDER_MISSING: 1}


async def test_an_altegio_sender_does_not_satisfy_an_easyweek_branch(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, with_sender=False)
            session.add(
                WhatsAppSender(
                    provider=PROVIDER_ALTEGIO,
                    company_id=COMPANY_ID,
                    sender_code="default",
                    phone_number_id="altegio-phone",
                    is_active=True,
                )
            )

    report = await _run(session_maker)

    assert report.reasons == {REASON_SENDER_MISSING: 1}


async def test_one_blocked_candidate_fails_an_otherwise_proven_queue(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, suffix="1")
            blocked = await _seed_review(
                session,
                company_id=OTHER_COMPANY_ID,
                booking=uuid.UUID("22222222-2222-4333-8444-555555555555"),
                review_url=OTHER_REVIEW_URL,
                with_sender=False,
                suffix="2",
            )

    report = await _run(session_maker)

    assert report.reasons == {PROVEN: 1, REASON_SENDER_MISSING: 1}
    assert report.blocked_job_ids == [blocked.id]
    assert report.ready is False


# ---------------------------------------------------------------------------
# The rollout state the report is a statement about
# ---------------------------------------------------------------------------
#
# "Every queued review would be sent correctly if the fence opened" is only a
# true sentence in one configuration. With notifications or planning off the
# queue is not being fed; with the fence already open there is nothing left to
# authorise. Reading the queue in those states and calling it green would
# authorise a rollout step against a world that no longer exists.


def test_the_only_auditable_state_is_planning_on_and_the_fence_shut() -> None:
    assert rollout_state_error() is None


@pytest.mark.parametrize(
    ("label", "notifications", "reviews", "send", "expected"),
    [
        ("notifications-off", False, True, False, REASON_NOTIFICATIONS_DISABLED),
        ("planning-off", True, False, False, REASON_PLANNING_DISABLED),
        ("fence-already-open", True, True, True, REASON_SEND_FENCE_OPEN),
        ("everything-off-but-send", False, False, True, REASON_NOTIFICATIONS_DISABLED),
    ],
)
async def test_a_wrong_rollout_state_is_red_and_never_reads_the_queue(
    session_maker, monkeypatch, label: str, notifications: bool, reviews: bool, send: bool, expected: str
) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)

    monkeypatch.setattr(settings, "easyweek_notifications_enabled", notifications, raising=False)
    monkeypatch.setattr(settings, "easyweek_reviews_enabled", reviews, raising=False)
    monkeypatch.setattr(settings, "easyweek_review_send_enabled", send, raising=False)

    report = await _run(session_maker)

    assert report.config_error == expected, label
    assert report.ready is False, label
    assert report.candidate_count == 0, f"{label}: the queue must not be read at all"
    assert report.checked_count == 0, label
    assert report.reasons == {}, label


async def test_a_wrong_rollout_state_exits_non_zero(session_maker, monkeypatch) -> None:
    import altegio_bot.scripts.easyweek_review_preflight as module

    monkeypatch.setattr(module, "SessionLocal", session_maker, raising=False)
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)

    monkeypatch.setattr(settings, "easyweek_review_send_enabled", True, raising=False)
    assert await main([]) == 1


# ---------------------------------------------------------------------------
# The values the send path itself needs
# ---------------------------------------------------------------------------
#
# `phone_e164` and `display_name` are both nullable and neither is covered by
# the domain guard, so a review could be proven here and then die at send time
# on "No phone_e164" or on an empty first template parameter. The parameters are
# built and validated with the SAME functions the outbox calls, so this cannot
# become a third copy of the contract.


@pytest.mark.parametrize("phone", [None, "", "   "])
async def test_a_client_without_a_usable_phone_is_red(session_maker, phone: str | None) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, phone_e164=phone)

    report = await _run(session_maker)

    assert report.reasons == {REASON_PHONE_MISSING: 1}
    assert report.ready is False


@pytest.mark.parametrize("name", [None, ""])
async def test_a_client_without_a_name_fails_the_param_contract(session_maker, name: str | None) -> None:
    """`client_name` is the first positional parameter; Meta rejects an empty one.

    A whitespace-only name is deliberately NOT covered: the shared
    `validate_lifecycle_template_params` tests emptiness rather than
    blankness, and PR-9 does not introduce a name validator of its own —
    doing so here would change the contract for every other job type.
    """
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, display_name=name)

    report = await _run(session_maker)

    assert report.reasons == {REASON_TEMPLATE_PARAMS: 1}
    assert report.ready is False


async def test_only_the_phone_missing_still_blocks_an_otherwise_proven_review(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, phone_e164=None)

    report = await _run(session_maker)

    assert report.green_count == 0
    assert report.blocked_count == 1


async def test_a_complete_client_proves_the_two_review_parameters(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)

    report = await _run(session_maker)

    assert report.reasons == {PROVEN: 1}


# ---------------------------------------------------------------------------
# The sender runtime will actually route to
# ---------------------------------------------------------------------------
#
# EasyWeek sends always resolve `sender_code="default"`. An audit that accepted
# any active row would pass a branch whose only sender is `vip`, and the send
# would then fail for want of a sender at all.


@pytest.mark.parametrize(
    ("label", "kwargs"),
    [
        ("only-a-vip-sender", {"sender_code": "vip"}),
        ("default-inactive", {"sender_active": False}),
        ("other-provider", {"sender_provider": PROVIDER_ALTEGIO}),
        ("other-company", {"sender_company_id": OTHER_COMPANY_ID}),
    ],
)
async def test_a_sender_runtime_would_not_pick_is_red(session_maker, label: str, kwargs: dict) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, **kwargs)

    report = await _run(session_maker)

    assert report.reasons == {REASON_SENDER_MISSING: 1}, label
    assert report.ready is False, label


@pytest.mark.parametrize("phone_number_id", ["", "   "])
async def test_a_default_sender_naming_no_whatsapp_number_is_red(session_maker, phone_number_id: str) -> None:
    """A row that exists but names no number is not a sender."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, sender_phone_number_id=phone_number_id)

    report = await _run(session_maker)

    assert report.reasons == {REASON_SENDER_PHONE_ID_EMPTY: 1}
    assert report.ready is False


async def test_the_audited_sender_code_is_the_one_runtime_resolves() -> None:
    """Pinned so the audit cannot drift from the send path."""
    assert EASYWEEK_SENDER_CODE == "default"


# ---------------------------------------------------------------------------
# Read-only, and quiet about the booking
# ---------------------------------------------------------------------------


async def _snapshot(session_maker) -> tuple:
    async with session_maker() as session:
        return (
            (
                await session.execute(
                    select(
                        MessageJob.id, MessageJob.status, MessageJob.attempts, MessageJob.run_at, MessageJob.locked_at
                    )
                )
            ).all(),
            (await session.execute(select(Record.id, Record.starts_at, Record.is_deleted, Record.raw))).all(),
            (await session.execute(select(Client.id, Client.wa_opted_out))).all(),
            (await session.execute(select(OutboxMessage.id))).all(),
            (await session.execute(select(MessageTemplate.id, MessageTemplate.is_active))).all(),
        )


async def test_the_preflight_changes_no_row_at_all(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, suffix="1")
            await _seed_review(
                session,
                booking=uuid.UUID("22222222-2222-4333-8444-555555555555"),
                with_sender=False,
                suffix="2",
            )

    before = await _snapshot(session_maker)
    await _run(session_maker)
    after = await _snapshot(session_maker)

    assert after == before, "a preflight must not touch a single row"
    assert before[3] == [], "and it must never create an Outbox row"


async def test_the_report_names_ids_and_reason_codes_but_no_booking_data(session_maker) -> None:
    """This output is read in a terminal and pasted into tickets."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, with_sender=False)

    text = str((await _run(session_maker)).as_safe_dict())

    for leak in (
        str(BOOKING),
        HASH,
        REVIEW_URL,
        "eyw.me",
        "Anna",
        "+4917",
        "@example",
        CATEGORY,
        "review-preflight",
        "kitilash",
    ):
        assert leak not in text, f"the report leaked {leak!r}"
    assert "reasons" in text and "ready" in text
    assert "read-only" in text


def test_ready_requires_candidates_no_truncation_and_all_proven() -> None:
    proven = ReviewPreflightReport(candidate_count=2, checked_count=2)
    proven.reasons[PROVEN] = 2
    assert proven.ready is True

    for broken in (
        ReviewPreflightReport(candidate_count=0, checked_count=0),
        ReviewPreflightReport(candidate_count=2, checked_count=2, truncated=True),
        ReviewPreflightReport(candidate_count=2, checked_count=1),
    ):
        broken.reasons[PROVEN] = broken.checked_count
        assert broken.ready is False


# ---------------------------------------------------------------------------
# The command line
# ---------------------------------------------------------------------------


def test_the_default_limit_is_bounded() -> None:
    assert _parse_args([]).limit > 0


@pytest.mark.parametrize("argv", [["--limit", "0"], ["--limit", "-3"]])
def test_a_nonsensical_limit_is_refused(argv: list[str]) -> None:
    with pytest.raises(SystemExit):
        _parse_args(argv)


async def test_the_exit_code_is_zero_only_for_a_fully_green_report(session_maker, monkeypatch) -> None:
    import altegio_bot.scripts.easyweek_review_preflight as module

    monkeypatch.setattr(module, "SessionLocal", session_maker, raising=False)

    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)
    assert await main([]) == 0

    async with session_maker() as session:
        async with session.begin():
            await session.execute(
                MessageTemplate.__table__.delete().where(MessageTemplate.provider == PROVIDER_EASYWEEK)
            )
    assert await main([]) == 1, "a blocked candidate must not exit zero"


async def test_an_empty_queue_exits_non_zero(session_maker, monkeypatch) -> None:
    import altegio_bot.scripts.easyweek_review_preflight as module

    monkeypatch.setattr(module, "SessionLocal", session_maker, raising=False)
    assert await main([]) == 1


async def test_a_database_failure_is_red_rather_than_green(monkeypatch) -> None:
    """ "We could not look" must never read as "everything is fine"."""
    import altegio_bot.scripts.easyweek_review_preflight as module

    class _Boom:
        def __call__(self):
            raise RuntimeError("database unavailable")

    monkeypatch.setattr(module, "SessionLocal", _Boom(), raising=False)
    assert await main([]) == 1


# ---------------------------------------------------------------------------
# PR-10: the link is ours, so its failure modes are reported as ours
# ---------------------------------------------------------------------------


async def test_a_live_branch_missing_from_the_map_stops_before_the_queue(session_maker, monkeypatch) -> None:
    """The most invisible gap, and the one this preflight exists to catch.

    A branch absent from the map plans no jobs, so it can never appear as a
    ROW in this report — its events are sitting in configuration deferral. The
    location registry is the definition of "live", so the difference between
    the two key sets is the finding.
    """
    monkeypatch.setattr(settings, "easyweek_google_review_links", json.dumps({"999999": REVIEW_URL}), raising=False)
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)

    report = await _run(session_maker)

    assert report.config_error == "review_links_incomplete"
    assert report.ready is False
    assert report.reasons == {}, "the queue must not be read at all"


async def test_a_map_covering_every_live_branch_is_not_flagged(session_maker) -> None:
    """The control: full coverage must not be reported as a gap."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)

    report = await _run(session_maker)

    assert report.config_error is None
    assert report.reasons == {PROVEN: 1}


async def test_the_per_row_missing_link_reason_survives_as_defence_in_depth(session_maker, monkeypatch) -> None:
    """`check_review_job` keeps its own answer even though the gate precedes it.

    Not an operator path any more — `rollout_state_error()` catches the same
    states first — but the row-level guard must not silently rot.
    """
    monkeypatch.setattr(settings, "easyweek_google_review_links", json.dumps({"999999": REVIEW_URL}), raising=False)
    async with session_maker() as session:
        async with session.begin():
            job = await _seed_review(session)
        reason = await preflight.check_review_job(session, job)

    assert reason == "review_link_missing"


@pytest.mark.parametrize(
    ("label", "raw"),
    [
        ("not-json", "{not json"),
        ("legacy-array", '["%s"]' % REVIEW_URL),
        ("bad-url", json.dumps({str(COMPANY_ID): "http://g.page/r/T/review"})),
        ("duplicate", '{"%d": "%s", "%d": "%s"}' % (COMPANY_ID, REVIEW_URL, COMPANY_ID, OTHER_REVIEW_URL)),
    ],
)
async def test_an_unusable_link_map_stops_before_the_queue(session_maker, monkeypatch, label: str, raw: str) -> None:
    """The map is a statement about the whole rollout, not about one row.

    Reported per-row it would be invisible on the very rollout that needs it:
    a broken map means the planner created nothing, so the queue is empty and
    the report would read "candidate_count=0, STOP" with no hint of the cause.
    """
    monkeypatch.setattr(settings, "easyweek_google_review_links", raw, raising=False)
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)

    report = await _run(session_maker)

    assert report.config_error == "review_links_invalid", label
    assert report.ready is False
    assert report.reasons == {}, "the queue must not be read at all"
    assert report.candidate_count == 0


async def test_an_unconfigured_link_map_is_distinguished_from_an_invalid_one(session_maker, monkeypatch) -> None:
    """ "Never set up" and "set up wrongly" are different operator actions."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)

    monkeypatch.setattr(settings, "easyweek_google_review_links", "", raising=False)
    assert (await _run(session_maker)).config_error == "review_links_unconfigured"

    monkeypatch.setattr(settings, "easyweek_google_review_links", "{not json", raising=False)
    assert (await _run(session_maker)).config_error == "review_links_invalid"


async def test_a_link_that_changed_after_planning_is_red(session_maker) -> None:
    """Identity is bound to the planned link, so a swap is refused, not applied."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, review_url=OTHER_REVIEW_URL)

    report = await _run(session_maker)

    assert report.reasons == {"review_link_changed": 1}
    assert report.ready is False


async def test_an_unconfigured_map_stops_the_whole_queue(session_maker, monkeypatch) -> None:
    """Nothing can be proven without the map; the queue is not even read."""
    monkeypatch.setattr(settings, "easyweek_google_review_links", "", raising=False)
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)

    report = await _run(session_maker)

    assert report.ready is False
    assert report.config_error == "review_links_unconfigured"
    assert report.reasons == {}


# ---------------------------------------------------------------------------
# The report must NAME the gap, not merely detect it
# ---------------------------------------------------------------------------


async def test_the_report_names_the_branches_missing_from_the_map(session_maker, monkeypatch) -> None:
    """Knowing "a branch is missing" without knowing which one is a diff by hand."""
    monkeypatch.setattr(
        settings, "easyweek_google_review_links", json.dumps({str(COMPANY_ID): REVIEW_URL}), raising=False
    )
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)

    report = await _run(session_maker)

    assert report.config_error == "review_links_incomplete"
    assert report.uncovered_company_ids == [OTHER_COMPANY_ID]
    assert report.as_safe_dict()["uncovered_company_ids"] == [OTHER_COMPANY_ID]


async def test_the_report_never_prints_a_link(session_maker, monkeypatch) -> None:
    monkeypatch.setattr(
        settings, "easyweek_google_review_links", json.dumps({str(COMPANY_ID): REVIEW_URL}), raising=False
    )
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)

    rendered = str((await _run(session_maker)).as_safe_dict())

    assert REVIEW_URL not in rendered
    assert "g.page" not in rendered


async def test_a_covered_map_reports_no_uncovered_branches(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)

    report = await _run(session_maker)

    assert report.config_error is None
    assert report.uncovered_company_ids == []


# ---------------------------------------------------------------------------
# An unusable location registry is the same class of blindness
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "raw", "expected"),
    [
        ("unconfigured", "", "location_registry_unconfigured"),
        ("empty-object", "{}", "location_registry_unconfigured"),
        ("not-json", "{not json", "location_registry_invalid"),
        ("legacy-shape", '{"du": 999501}', "location_registry_invalid"),
    ],
)
async def test_an_unusable_location_registry_stops_before_the_queue(
    session_maker, monkeypatch, label: str, raw: str, expected: str
) -> None:
    """With the registry broken the worker claims nothing, so the queue is
    empty for a reason that has nothing to do with the queue."""
    monkeypatch.setattr(settings, "easyweek_location_map", raw, raising=False)
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session)

    report = await _run(session_maker)

    assert report.config_error == expected, label
    assert report.ready is False
    assert report.reasons == {}, "the queue must not be read at all"
    assert report.candidate_count == 0


# ---------------------------------------------------------------------------
# Plan §31.11: the preflight sizes the backlog by visit verdict
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "visits_total", "bucket", "green"),
    [
        ("at-the-limit", 3, "review_visit_count_eligible", True),
        ("over-the-limit", 4, "review_visit_limit_exceeded", False),
        ("no-snapshot", None, "review_visit_count_unproven", False),
    ],
)
async def test_the_preflight_buckets_every_review_by_visit_verdict(
    session_maker, label: str, visits_total: int | None, bucket: str, green: bool
) -> None:
    """The same question the sender asks, answered before the fence is opened."""
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, visits_total=visits_total)

    report = await _run(session_maker)

    assert report.visit_buckets == {bucket: 1}, label
    assert report.as_safe_dict()["review_visit_buckets"] == {bucket: 1}
    assert (report.reasons == {PROVEN: 1}) is green, label
    assert report.ready is green, label
    if not green:
        assert report.reasons == {bucket: 1}, "the blocking reason names the visit verdict"


async def test_a_review_blocked_for_another_reason_still_gets_a_visit_verdict(session_maker) -> None:
    """The audit covers the whole backlog, not only the jobs that got that far.

    `reasons` records what stopped a job FIRST. An operator sizing the queue
    needs the visit answer for every job in it, including one already blocked by
    a missing template.
    """
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, with_template=False, visits_total=4)

    report = await _run(session_maker)

    assert report.reasons == {REASON_TEMPLATE_MISSING: 1}
    assert report.visit_buckets == {"review_visit_limit_exceeded": 1}


async def test_a_switched_off_counter_is_red_for_every_review(session_maker, monkeypatch) -> None:
    """ "Send on, counter off" is a red configuration: the send guard holds them all."""
    monkeypatch.setattr(preflight.settings, "easyweek_visit_counter_enabled", False, raising=False)
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, visits_total=1)

    report = await _run(session_maker)

    assert report.visit_buckets == {"review_visit_counter_disabled": 1}
    assert report.reasons == {"review_visit_counter_disabled": 1}
    assert report.ready is False


async def test_no_visit_bucket_carries_anything_but_a_count(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed_review(session, visits_total=4)

    payload = json.dumps((await _run(session_maker)).as_safe_dict()["review_visit_buckets"])

    for leaked in ("phone", "@", "+49", "http", "uuid"):
        assert leaked not in payload
