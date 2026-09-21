"""§38.9 PostgreSQL contract: what the global send fence would actually release.

The structural preflight starts from ACTIVE, FUTURE records. The fence does
not, so this audit starts from the jobs — and every scenario below exists
because it is a job the structural selector would never have looked at, or a
job whose fate is decided by a rule that runs long before the send.

The sharpest one is the deleted record with two held jobs, straight from
production evidence: its ``record_created`` is cancelled locally and its
``record_canceled`` is a real message to a real customer. Two green preflights
do not distinguish them; this does.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import func, select

from altegio_bot.easyweek_branches import BRANCH_PROFILES, BranchProfile
from altegio_bot.easyweek_multi_service import (
    MULTI_SERVICE_JOB_DIGEST_KEY,
    MULTI_SERVICE_JOB_VERSION_KEY,
    WebhookServicePair,
    clear_multi_service_catalog_cache,
    multi_service_job_payload,
    prove_exactly_two_service_snapshot,
    record_raw_with_multi_service_snapshot,
)
from altegio_bot.easyweek_multi_service_rollout import (
    EASYWEEK_REMINDER_API_GUARD_DISABLED,
    MULTI_SERVICE_CANARY_JOB_MISMATCH,
    MULTI_SERVICE_CANARY_JOB_NOT_DUE,
    MULTI_SERVICE_CANARY_JOB_NOT_FOUND,
    MULTI_SERVICE_SEND_FENCE_OPEN,
    RolloutPhase,
)
from altegio_bot.easyweek_service_category import record_raw_with_services_count
from altegio_bot.models.models import PROVIDER_ALTEGIO, PROVIDER_EASYWEEK, Client, MessageJob, OutboxMessage, Record
from altegio_bot.scripts import easyweek_multi_service_release_audit as audit_cli
from altegio_bot.scripts.easyweek_multi_service_release_audit import (
    BULK_PROVIDER_CANDIDATE,
    FUTURE_NOT_DUE,
    HELD_BY_CANARY,
    HELD_BY_SEND_FENCE,
    LOCAL_CANCEL_OR_NOOP,
    NONTERMINAL_OUTBOX_PRESENT,
    PROCESSING_OR_LOCKED,
    REASON_DEADLINE_EXPIRED,
    REASON_RECORD_DELETED,
    REASON_RECORD_IN_PAST,
    SELECTED_CANARY_PROVIDER_CANDIDATE,
    UNSAFE_OR_UNPROVEN,
    run_release_audit,
)
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import TEST_BOOKING_UUID, TEST_LOCATION_ID, TEST_LOCATION_UUID
from altegio_bot.utils import utcnow

pytestmark = pytest.mark.asyncio

_BRANCH_SLUG = "release-audit-branch"
_PREFIX = "ra"
_TOTAL = Decimal("80.00")
_ALLOWED_CATEGORY = "Fixture Category"


@pytest.fixture(autouse=True)
def _configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    import json

    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                _BRANCH_SLUG: {
                    "location_id": TEST_LOCATION_ID,
                    "location_uuid": TEST_LOCATION_UUID,
                    "meta_template_prefix": _PREFIX,
                    "booking_page_url": "https://booking.example.invalid/test",
                }
            }
        ),
        raising=False,
    )
    monkeypatch.setitem(
        BRANCH_PROFILES,
        _BRANCH_SLUG,
        BranchProfile(
            slug=_BRANCH_SLUG,
            api_name="Release audit fixture",
            meta_template_prefix=_PREFIX,
            content=BRANCH_PROFILES["durlach"].content,
        ),
    )
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", json.dumps([_ALLOWED_CATEGORY]), raising=False)
    # The pre-open configuration the runbook deploys before step 47.
    monkeypatch.setattr(settings, "easyweek_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_reminders_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_reminder_api_guard_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_multi_service_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_resource_shadow_proof_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_multi_service_canary_job_id", "", raising=False)
    clear_multi_service_catalog_cache()


# ---------------------------------------------------------------------------
# Fixture data: one proven pair, built by the production proof itself.
# ---------------------------------------------------------------------------


def _line(line_uuid: str, name: str, price: int, duration: int) -> dict[str, Any]:
    return {
        "uuid": line_uuid,
        "name": name,
        "currency": "EUR",
        "price": price,
        "original_price": price,
        "discount": 0,
        "quantity": 1,
        "duration": {"value": duration, "label": "minutes"},
        "original_duration": {"value": duration, "label": "minutes"},
    }


def _booking() -> dict[str, Any]:
    return {
        "uuid": TEST_BOOKING_UUID,
        "location_uuid": TEST_LOCATION_UUID,
        "currency": "EUR",
        "order": {"subtotal": 8000, "total": 8000},
        "ordered_services": [
            _line("11111111-1111-4111-8111-111111111111", "Fixture Service", 3500, 30),
            _line("22222222-2222-4222-8222-222222222222", "Second Fixture Service", 4500, 45),
        ],
    }


def _catalog(*, category: str = _ALLOWED_CATEGORY) -> list[dict[str, Any]]:
    return [
        {
            "uuid": "aaaaaaaa-1111-4111-8111-111111111111",
            "name": "Fixture Service",
            "currency": "EUR",
            "price": 3500,
            "duration": {"value": 30, "label": "minutes"},
            "category": {"name": category},
        },
        {
            "uuid": "aaaaaaaa-2222-4222-8222-222222222222",
            "name": "Second Fixture Service",
            "currency": "EUR",
            "price": 4500,
            "duration": {"value": 45, "label": "minutes"},
            "category": {"name": category},
        },
    ]


def _snapshot(*, category: str = _ALLOWED_CATEGORY):
    return prove_exactly_two_service_snapshot(
        webhook=WebhookServicePair(
            booking_uuid=uuid.UUID(TEST_BOOKING_UUID),
            location_uuid=TEST_LOCATION_UUID,
            service_name="Fixture Service",
            service_related="Second Fixture Service",
            services_description="Fixture Service, Second Fixture Service",
            services_count=2,
            quantity=2,
            booking_currency="EUR",
            total_cost=_TOTAL,
        ),
        booking_payload=_booking(),
        catalog_rows=_catalog(category=category),
    )


async def _seed_record(
    session,
    *,
    deleted: bool = False,
    starts_in: timedelta = timedelta(days=3),
    category: str = _ALLOWED_CATEGORY,
) -> tuple[Client, Record, Any]:
    client = Client(
        provider=PROVIDER_EASYWEEK,
        company_id=TEST_LOCATION_ID,
        altegio_client_id=7300002,
        display_name="Release audit fixture",
        phone_e164="+49000000000",
        raw={},
    )
    session.add(client)
    await session.flush()

    snapshot = _snapshot(category=category)
    record = Record(
        provider=PROVIDER_EASYWEEK,
        company_id=TEST_LOCATION_ID,
        altegio_record_id=4200001,
        easyweek_booking_uuid=uuid.UUID(TEST_BOOKING_UUID),
        client_id=client.id,
        starts_at=utcnow() + starts_in,
        total_cost=_TOTAL,
        is_deleted=deleted,
        raw=record_raw_with_multi_service_snapshot(record_raw_with_services_count({}, 2), snapshot),
    )
    session.add(record)
    await session.flush()
    return client, record, snapshot


async def _seed_pair_job(
    session,
    client: Client,
    record: Record,
    snapshot: Any,
    *,
    job_type: str = "record_created",
    run_at: timedelta = timedelta(minutes=-1),
    status: str = "queued",
    locked: bool = False,
    payload: dict[str, Any] | None = None,
    dedupe_key: str | None = None,
) -> MessageJob:
    body = payload if payload is not None else multi_service_job_payload(snapshot)
    if job_type.startswith("reminder") and "record_starts_at" not in body:
        body = {**body, "record_starts_at": record.starts_at.isoformat()}
    job = MessageJob(
        provider=PROVIDER_EASYWEEK,
        company_id=TEST_LOCATION_ID,
        record_id=record.id,
        client_id=client.id,
        job_type=job_type,
        run_at=utcnow() + run_at,
        status=status,
        locked_at=utcnow() if locked else None,
        dedupe_key=dedupe_key or f"release-audit-{job_type}-{run_at}-{status}-{locked}",
        payload=body,
    )
    session.add(job)
    await session.flush()
    return job


async def _state(session) -> list[tuple[Any, ...]]:
    """Every field the audit could plausibly have touched, before and after."""
    rows: list[tuple[Any, ...]] = []
    for job in (await session.execute(select(MessageJob).order_by(MessageJob.id))).scalars():
        rows.append((job.id, job.status, job.attempts, job.locked_at, job.run_at, job.last_error, job.payload))
    for record in (await session.execute(select(Record).order_by(Record.id))).scalars():
        rows.append((record.id, record.is_deleted, record.starts_at, record.raw, record.total_cost))
    for outbox in (await session.execute(select(OutboxMessage).order_by(OutboxMessage.id))).scalars():
        rows.append((outbox.id, outbox.status))
    return rows


# ===========================================================================
# The release set itself
# ===========================================================================


async def test_a_due_provable_pair_job_is_held_only_by_the_global_fence(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            job = await _seed_pair_job(session, client, record, snapshot)
        job_id = job.id

    async with session_maker() as session:
        report = await run_release_audit(session)

    assert report.config_error is None
    assert report.open_pair_jobs == 1
    assert report.classifications[HELD_BY_SEND_FENCE] == 1
    assert report.provider_candidate_job_ids == [job_id]
    assert report.audit_sound is True
    assert report.bulk_ready is True


async def test_a_future_reminder_is_not_due_yet_but_stays_in_the_inventory(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session, starts_in=timedelta(days=3))
            job = await _seed_pair_job(
                session,
                client,
                record,
                snapshot,
                job_type="reminder_24h",
                run_at=timedelta(days=2),
            )
        job_id = job.id

    async with session_maker() as session:
        report = await run_release_audit(session)

    assert report.classifications[FUTURE_NOT_DUE] == 1
    assert report.provider_candidate_job_ids == []
    assert report.future_provider_candidate_job_ids == [job_id]
    assert report.release_set_size == 1


async def test_a_deleted_record_cancels_its_created_job_and_still_releases_its_cancellation(
    session_maker,
) -> None:
    """Production evidence, and the whole reason this audit exists."""
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session, deleted=True)
            created = await _seed_pair_job(session, client, record, snapshot, job_type="record_created")
            canceled = await _seed_pair_job(session, client, record, snapshot, job_type="record_canceled")
        created_id, canceled_id = created.id, canceled.id

    async with session_maker() as session:
        report = await run_release_audit(session)

    assert report.open_pair_jobs == 2
    assert report.classifications[LOCAL_CANCEL_OR_NOOP] == 1
    assert report.reasons[REASON_RECORD_DELETED] == 1
    assert report.classifications[HELD_BY_SEND_FENCE] == 1
    assert report.provider_candidate_job_ids == [canceled_id]
    assert created_id not in report.provider_candidate_job_ids
    # The audit is still sound: a local cancel is a known, safe outcome.
    assert report.audit_sound is True


async def test_a_past_record_job_is_a_local_cancel_not_a_provider_candidate(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session, starts_in=timedelta(hours=-48))
            await _seed_pair_job(session, client, record, snapshot, job_type="record_created")

    async with session_maker() as session:
        report = await run_release_audit(session)

    assert report.classifications[LOCAL_CANCEL_OR_NOOP] == 1
    assert report.reasons[REASON_RECORD_IN_PAST] == 1
    assert report.provider_candidate_job_ids == []


async def test_an_expired_reminder_deadline_is_a_local_cancel(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session, starts_in=timedelta(hours=20))
            await _seed_pair_job(
                session,
                client,
                record,
                snapshot,
                job_type="reminder_24h",
                run_at=timedelta(days=-3),
            )

    async with session_maker() as session:
        report = await run_release_audit(session)

    assert report.classifications[LOCAL_CANCEL_OR_NOOP] == 1
    assert report.reasons[REASON_DEADLINE_EXPIRED] == 1


async def test_a_category_the_allowlist_refuses_is_a_local_noop(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session, category="Nagelservice")
            await _seed_pair_job(session, client, record, snapshot)

    async with session_maker() as session:
        report = await run_release_audit(session)

    assert report.classifications[LOCAL_CANCEL_OR_NOOP] == 1
    assert report.reasons["multi_service_category_not_allowed"] == 1
    assert report.provider_candidate_job_ids == []
    assert report.audit_sound is True


# ===========================================================================
# Everything that makes the audit itself untrustworthy
# ===========================================================================


async def test_a_stale_job_digest_is_unsafe_and_blocks_every_readiness(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            payload = multi_service_job_payload(snapshot)
            payload[MULTI_SERVICE_JOB_DIGEST_KEY] = "0" * 64
            await _seed_pair_job(session, client, record, snapshot, payload=payload)

    async with session_maker() as session:
        report = await run_release_audit(session)

    assert report.classifications[UNSAFE_OR_UNPROVEN] == 1
    assert report.reasons["multi_service_snapshot_digest_mismatch"] == 1
    assert report.audit_sound is False
    assert report.bulk_ready is False


async def test_a_malformed_payload_version_is_unsafe(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            payload = multi_service_job_payload(snapshot)
            payload[MULTI_SERVICE_JOB_VERSION_KEY] = 99
            await _seed_pair_job(session, client, record, snapshot, payload=payload)

    async with session_maker() as session:
        report = await run_release_audit(session)

    assert report.classifications[UNSAFE_OR_UNPROVEN] == 1
    assert report.audit_sound is False


async def test_a_claimed_or_locked_job_blocks_readiness(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            await _seed_pair_job(session, client, record, snapshot, status="processing", locked=True)

    async with session_maker() as session:
        report = await run_release_audit(session)

    assert report.classifications[PROCESSING_OR_LOCKED] == 1
    assert report.audit_sound is False


async def test_a_non_terminal_outbox_row_blocks_readiness(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            job = await _seed_pair_job(session, client, record, snapshot)
            session.add(
                OutboxMessage(
                    company_id=TEST_LOCATION_ID,
                    client_id=client.id,
                    record_id=record.id,
                    job_id=job.id,
                    phone_e164="+49000000000",
                    template_code="record_created",
                    body="fixture",
                    status="queued",
                    scheduled_at=utcnow(),
                    meta={},
                )
            )

    async with session_maker() as session:
        report = await run_release_audit(session)

    assert report.classifications[NONTERMINAL_OUTBOX_PRESENT] == 1
    assert report.nonterminal_outbox_rows == 1
    assert report.audit_sound is False


async def test_a_truncated_scope_is_never_ready(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            for index in range(3):
                await _seed_pair_job(
                    session,
                    client,
                    record,
                    snapshot,
                    job_type="record_updated",
                    dedupe_key=f"truncation-{index}",
                )

    async with session_maker() as session:
        report = await run_release_audit(session, limit=2)

    assert report.truncated is True
    assert report.open_pair_jobs == 2
    assert report.audit_sound is False
    assert report.bulk_ready is False


# ===========================================================================
# Scope: what the release set deliberately does NOT contain
# ===========================================================================


async def test_terminal_history_is_outside_the_release_set(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            for status in ("done", "canceled", "failed"):
                await _seed_pair_job(
                    session,
                    client,
                    record,
                    snapshot,
                    status=status,
                    dedupe_key=f"historical-{status}",
                )
            session.add(
                OutboxMessage(
                    company_id=TEST_LOCATION_ID,
                    client_id=client.id,
                    record_id=record.id,
                    job_id=None,
                    phone_e164="+49000000000",
                    template_code="record_created",
                    body="fixture",
                    status="sent",
                    scheduled_at=utcnow(),
                    meta={},
                )
            )

    async with session_maker() as session:
        report = await run_release_audit(session)

    assert report.open_pair_jobs == 0
    assert report.nonterminal_outbox_rows == 0


async def test_single_service_and_altegio_jobs_are_outside_the_release_set(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            await _seed_pair_job(session, client, record, snapshot, payload={}, dedupe_key="single-service")
            session.add(
                MessageJob(
                    provider=PROVIDER_ALTEGIO,
                    company_id=TEST_LOCATION_ID,
                    record_id=record.id,
                    client_id=client.id,
                    job_type="record_created",
                    run_at=utcnow(),
                    status="queued",
                    dedupe_key="altegio-beside-pair",
                    payload=multi_service_job_payload(snapshot),
                )
            )
            await _seed_pair_job(
                session,
                client,
                record,
                snapshot,
                job_type="review_3d",
                dedupe_key="review-beside-pair",
            )

    async with session_maker() as session:
        report = await run_release_audit(session)

    assert report.open_pair_jobs == 0


# ===========================================================================
# Readiness
# ===========================================================================


async def test_canary_readiness_names_exactly_one_job_from_the_release_set(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            chosen = await _seed_pair_job(session, client, record, snapshot)
            other = await _seed_pair_job(
                session,
                client,
                record,
                snapshot,
                job_type="record_updated",
                dedupe_key="second-candidate",
            )
        chosen_id, other_id = chosen.id, other.id

    async with session_maker() as session:
        report = await run_release_audit(session, intended_canary_job_id=chosen_id)

    assert report.canary_error is None
    assert report.intended_canary_job_id == chosen_id
    assert report.canary_ready is True
    assert sorted(report.provider_candidate_job_ids) == sorted([chosen_id, other_id])


async def test_a_canary_id_outside_the_release_set_is_refused(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            job = await _seed_pair_job(session, client, record, snapshot)
        stranger = job.id + 10_000

    async with session_maker() as session:
        report = await run_release_audit(session, intended_canary_job_id=stranger)

    assert report.canary_error == MULTI_SERVICE_CANARY_JOB_NOT_FOUND
    assert report.canary_ready is False
    assert report.intended_canary_job_id is None


async def test_the_named_canary_is_the_only_provider_candidate_while_it_is_set(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            chosen = await _seed_pair_job(session, client, record, snapshot)
            await _seed_pair_job(
                session,
                client,
                record,
                snapshot,
                job_type="record_updated",
                dedupe_key="held-by-canary",
            )
        chosen_id = chosen.id

    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_multi_service_canary_job_id", str(chosen_id), raising=False)

    async with session_maker() as session:
        report = await run_release_audit(session, phase=RolloutPhase.CANARY)

    assert report.config_error is None
    assert report.canary_job_id == chosen_id
    assert report.classifications[SELECTED_CANARY_PROVIDER_CANDIDATE] == 1
    assert report.classifications[HELD_BY_CANARY] == 1
    assert report.bulk_ready is False, "a restriction in force is not a bulk rollout"


async def test_the_bulk_phase_sees_every_releasable_job(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            first = await _seed_pair_job(session, client, record, snapshot)
            second = await _seed_pair_job(
                session,
                client,
                record,
                snapshot,
                job_type="record_updated",
                dedupe_key="bulk-second",
            )
        expected = sorted([first.id, second.id])

    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", True, raising=False)

    async with session_maker() as session:
        report = await run_release_audit(session, phase=RolloutPhase.BULK)

    assert report.classifications[BULK_PROVIDER_CANDIDATE] == 2
    assert sorted(report.provider_candidate_job_ids) == expected
    assert report.bulk_ready is True


async def test_a_wrong_phase_configuration_blocks_readiness(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            await _seed_pair_job(session, client, record, snapshot)

    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", True, raising=False)

    async with session_maker() as session:
        report = await run_release_audit(session, phase=RolloutPhase.PRE_OPEN)

    assert report.config_error == MULTI_SERVICE_SEND_FENCE_OPEN
    assert report.audit_sound is False
    assert report.bulk_ready is False


async def test_a_missing_runtime_guard_blocks_readiness(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            await _seed_pair_job(session, client, record, snapshot)

    monkeypatch.setattr(settings, "easyweek_reminder_api_guard_enabled", False, raising=False)

    async with session_maker() as session:
        report = await run_release_audit(session)

    assert report.config_error == EASYWEEK_REMINDER_API_GUARD_DISABLED
    assert report.audit_sound is False


# ===========================================================================
# Read-only, and safe to paste into a ticket
# ===========================================================================


async def test_the_audit_changes_no_row_and_no_field(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session, deleted=True)
            await _seed_pair_job(session, client, record, snapshot, job_type="record_created")
            await _seed_pair_job(session, client, record, snapshot, job_type="record_canceled")

    async with session_maker() as session:
        before = await _state(session)
        counts_before = [
            int((await session.execute(select(func.count()).select_from(model))).scalar_one())
            for model in (Record, MessageJob, OutboxMessage)
        ]

    async with session_maker() as session:
        await run_release_audit(session)
        assert not session.dirty
        assert not session.new
        assert not session.deleted

    async with session_maker() as session:
        assert await _state(session) == before
        counts_after = [
            int((await session.execute(select(func.count()).select_from(model))).scalar_one())
            for model in (Record, MessageJob, OutboxMessage)
        ]
    assert counts_after == counts_before


async def test_the_report_carries_no_booking_uuid_name_phone_or_price(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            await _seed_pair_job(session, client, record, snapshot)

    async with session_maker() as session:
        report = await run_release_audit(session)

    text = str(report.as_safe_dict())
    for forbidden in (
        TEST_BOOKING_UUID,
        TEST_LOCATION_UUID,
        "Release audit fixture",
        "+49000000000",
        "Fixture Service",
        "80.00",
        "8000",
    ):
        assert forbidden not in text, forbidden
    assert report.as_safe_dict()["read_only"] is True
    assert report.as_safe_dict()["send_authorized"] is False
    assert report.as_safe_dict()["config_changed"] is False


# ===========================================================================
# §38.9: the canary must be DUE, not merely in the release set
# ===========================================================================


async def test_a_future_release_set_job_can_never_be_the_canary(session_maker) -> None:
    """Runbook step 48, as a machine check.

    A future job IS in the release set and WILL go out with the bulk opening.
    Naming it as the canary produces no message at all — and an operator
    watching a silent queue would read that as a successful canary.
    """
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session, starts_in=timedelta(days=3))
            future = await _seed_pair_job(
                session,
                client,
                record,
                snapshot,
                job_type="reminder_24h",
                run_at=timedelta(days=2),
            )
        future_id = future.id

    async with session_maker() as session:
        report = await run_release_audit(session, intended_canary_job_id=future_id)

    assert report.canary_error == MULTI_SERVICE_CANARY_JOB_NOT_DUE
    assert report.canary_ready is False
    assert report.intended_canary_job_id is None
    # It stays part of the bulk inventory; it is only unusable as a canary.
    assert report.future_provider_candidate_job_ids == [future_id]


async def test_a_due_candidate_beside_a_future_one_is_still_a_valid_canary(session_maker) -> None:
    """Positive control: the new rule must not reject a correct choice."""
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            due = await _seed_pair_job(session, client, record, snapshot)
            await _seed_pair_job(
                session,
                client,
                record,
                snapshot,
                job_type="reminder_24h",
                run_at=timedelta(days=2),
                dedupe_key="future-beside-due",
            )
        due_id = due.id

    async with session_maker() as session:
        report = await run_release_audit(session, intended_canary_job_id=due_id)

    assert report.canary_error is None
    assert report.canary_ready is True
    assert report.intended_canary_job_id == due_id


async def test_an_unknown_id_is_still_reported_as_not_found(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            job = await _seed_pair_job(session, client, record, snapshot)
        stranger = job.id + 10_000

    async with session_maker() as session:
        report = await run_release_audit(session, intended_canary_job_id=stranger)

    assert report.canary_error == MULTI_SERVICE_CANARY_JOB_NOT_FOUND


async def test_a_mismatch_with_the_configured_canary_stays_fail_closed(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            first = await _seed_pair_job(session, client, record, snapshot)
            second = await _seed_pair_job(
                session,
                client,
                record,
                snapshot,
                job_type="record_updated",
                dedupe_key="mismatch-second",
            )
        first_id, second_id = first.id, second.id

    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_multi_service_canary_job_id", str(second_id), raising=False)

    async with session_maker() as session:
        report = await run_release_audit(session, phase=RolloutPhase.CANARY, intended_canary_job_id=first_id)

    assert report.canary_error == MULTI_SERVICE_CANARY_JOB_MISMATCH
    assert report.canary_ready is False


async def test_the_not_due_reason_is_stable_and_free_of_customer_data(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            future = await _seed_pair_job(
                session,
                client,
                record,
                snapshot,
                job_type="reminder_24h",
                run_at=timedelta(days=2),
            )
        future_id = future.id

    async with session_maker() as session:
        report = await run_release_audit(session, intended_canary_job_id=future_id)

    text = str(report.as_safe_dict())
    for forbidden in (TEST_BOOKING_UUID, TEST_LOCATION_UUID, "Release audit fixture", "+49000000000"):
        assert forbidden not in text, forbidden
    assert report.canary_error == "multi_service_canary_job_not_due"


async def test_the_cli_exits_non_zero_for_a_future_canary(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            future = await _seed_pair_job(
                session,
                client,
                record,
                snapshot,
                job_type="reminder_24h",
                run_at=timedelta(days=2),
            )
            due = await _seed_pair_job(session, client, record, snapshot, dedupe_key="cli-due")
        future_id, due_id = future.id, due.id

    monkeypatch.setattr(audit_cli, "SessionLocal", session_maker, raising=False)

    assert await audit_cli.main(["--canary-job-id", str(future_id)]) == 1
    assert await audit_cli.main(["--canary-job-id", str(due_id)]) == 0


async def test_the_cli_exits_non_zero_when_the_release_set_changed(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            await _seed_pair_job(session, client, record, snapshot)

    async with session_maker() as session:
        approved = await run_release_audit(session)

    monkeypatch.setattr(audit_cli, "SessionLocal", session_maker, raising=False)
    assert await audit_cli.main(["--expect-release-digest", approved.release_set_digest]) == 0
    assert await audit_cli.main(["--expect-release-digest", "0" * 64]) == 1


async def test_the_digest_argument_must_be_a_sha256_hex_string() -> None:
    for bad in ("", "nope", "0" * 63, "g" * 64):
        with pytest.raises(SystemExit):
            audit_cli._parse_args(["--expect-release-digest", bad])
    parsed = audit_cli._parse_args(["--expect-release-digest", "A" * 64])
    assert parsed.expect_release_digest == "a" * 64


# ===========================================================================
# §38.9 step 58: ordinary delivery is not a blocker
# ===========================================================================


async def test_a_processing_batch_blocks_the_pre_open_audit_but_not_the_bulk_one(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The false red that used to start rollbacks mid-delivery.

    `_lock_next_jobs` commits a whole batch as `processing` and only then
    works through it one job at a time, so a healthy opening looks exactly
    like this. Before the fence opens the same picture is an anomaly — nothing
    should be claimed while the queue is held — so only the bulk phase
    tolerates it.
    """
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            for index in range(3):
                await _seed_pair_job(
                    session,
                    client,
                    record,
                    snapshot,
                    status="processing",
                    locked=True,
                    dedupe_key=f"inflight-batch-{index}",
                )

    async with session_maker() as session:
        pre_open = await run_release_audit(session)
    assert pre_open.classifications[PROCESSING_OR_LOCKED] == 3
    assert pre_open.in_flight == 3
    assert pre_open.audit_sound is False

    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", True, raising=False)
    async with session_maker() as session:
        bulk = await run_release_audit(session, phase=RolloutPhase.BULK)

    assert bulk.classifications[PROCESSING_OR_LOCKED] == 3
    assert bulk.in_flight == 3
    assert bulk.audit_sound is True, "normal delivery must not read as a rollout blocker"


async def test_an_in_flight_outbox_row_is_tolerated_in_the_bulk_phase_only(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            job = await _seed_pair_job(session, client, record, snapshot)
            session.add(
                OutboxMessage(
                    company_id=TEST_LOCATION_ID,
                    client_id=client.id,
                    record_id=record.id,
                    job_id=job.id,
                    phone_e164="+49000000000",
                    template_code="record_created",
                    body="fixture",
                    status="sending",
                    scheduled_at=utcnow(),
                    meta={},
                )
            )

    async with session_maker() as session:
        pre_open = await run_release_audit(session)
    assert pre_open.audit_sound is False

    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", True, raising=False)
    async with session_maker() as session:
        bulk = await run_release_audit(session, phase=RolloutPhase.BULK)
    assert bulk.classifications[NONTERMINAL_OUTBOX_PRESENT] == 1
    assert bulk.audit_sound is True


async def test_an_unprovable_job_still_blocks_the_bulk_audit(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The bulk exception covers in-flight rows, and nothing else."""
    async with session_maker() as session:
        async with session.begin():
            client, record, snapshot = await _seed_record(session)
            payload = multi_service_job_payload(snapshot)
            payload[MULTI_SERVICE_JOB_DIGEST_KEY] = "0" * 64
            await _seed_pair_job(session, client, record, snapshot, payload=payload)

    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", True, raising=False)
    async with session_maker() as session:
        bulk = await run_release_audit(session, phase=RolloutPhase.BULK)

    assert bulk.classifications[UNSAFE_OR_UNPROVEN] == 1
    assert bulk.audit_sound is False
