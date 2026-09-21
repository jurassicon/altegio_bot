"""§38.9 step 58: what the bulk opening actually sent, proven per approved id.

Every scenario here is one the old check could not tell apart. It selected
open jobs, so success, a permanent Meta refusal, an exhausted retry chain, a
local cancel and a ``done`` job with no Outbox row behind it all left the
result set the same way — and an empty set was being read as proof.

The other half is the batch. ``_lock_next_jobs`` commits a whole batch as
``processing`` and only then works through it one job at a time, so the
first look after an opening is several rows mid-flight. That must read as
"not finished yet", never as a rollout failure.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Any

import pytest
from sqlalchemy import func, select

from altegio_bot.easyweek_multi_service import MULTI_SERVICE_JOB_DIGEST_KEY
from altegio_bot.easyweek_multi_service_rollout import JobIdListError, parse_job_id_list
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    Client,
    MessageJob,
    OutboxMessage,
    Record,
)
from altegio_bot.scripts import easyweek_multi_service_release_verify as verify_cli
from altegio_bot.scripts.easyweek_multi_service_release_verify import (
    CANCELED_WITHOUT_PROVIDER_SEND,
    FAILED,
    FUTURE_INDETERMINATE,
    FUTURE_MATURED_PENDING,
    FUTURE_PENDING,
    FUTURE_RELEASED_ON_SCHEDULE,
    FUTURE_SENT_EARLY,
    FUTURE_UNEXPECTED_STATE,
    IDENTITY_MISMATCH,
    IN_PROGRESS,
    MISSING,
    REASON_DONE_WITHOUT_PROVEN_SEND,
    REASON_EMPTY_DUE_SET,
    REASON_EMPTY_INVENTORY,
    REASON_OUTBOX_UNKNOWN,
    REASON_SETTLE_TIMEOUT,
    REASON_UNAPPROVED_SEND,
    RETRY_SCHEDULED,
    SUCCEEDED,
    UNKNOWN_OR_INDETERMINATE,
    classify_future_job,
    verify_release,
)
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import TEST_BOOKING_UUID, TEST_LOCATION_ID, TEST_LOCATION_UUID
from altegio_bot.utils import utcnow

pytestmark = pytest.mark.asyncio

OPENED_AT = utcnow() - timedelta(minutes=5)
_DIGEST = "a" * 64

# The configuration step 58 is a statement about: the bulk fence open, no
# canary left over, every mandatory prerequisite on. Checked by the verifier
# through the same shared resolver the preflights and the audit use, so the
# fixture deploys it rather than the tests each asserting around it.
_BULK_CONFIGURATION = {
    "easyweek_notifications_enabled": True,
    "easyweek_reminders_enabled": True,
    "easyweek_reminder_api_guard_enabled": True,
    "easyweek_multi_service_notifications_enabled": True,
    "easyweek_resource_shadow_proof_enabled": True,
    "easyweek_multi_service_send_enabled": True,
    "easyweek_multi_service_canary_job_id": "",
}


@pytest.fixture(autouse=True)
def _bulk_configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    for name, value in _BULK_CONFIGURATION.items():
        monkeypatch.setattr(settings, name, value, raising=False)


async def _no_sleep(_seconds: float) -> None:
    return None


# ---------------------------------------------------------------------------
# Fixture rows. The verifier checks identity, status and Outbox — not the
# proof itself — so the pair payload only needs its canonical digest key.
# ---------------------------------------------------------------------------


async def _seed_client_and_record(session) -> tuple[Client, Record]:
    client = Client(
        provider=PROVIDER_EASYWEEK,
        company_id=TEST_LOCATION_ID,
        altegio_client_id=7300002,
        display_name="Release verify fixture",
        phone_e164="+49000000000",
        raw={},
    )
    session.add(client)
    await session.flush()
    record = Record(
        provider=PROVIDER_EASYWEEK,
        company_id=TEST_LOCATION_ID,
        altegio_record_id=4200001,
        easyweek_booking_uuid=uuid.UUID(TEST_BOOKING_UUID),
        client_id=client.id,
        starts_at=utcnow() + timedelta(days=3),
        is_deleted=False,
        raw={},
    )
    session.add(record)
    await session.flush()
    return client, record


async def _seed_job(
    session,
    client: Client,
    record: Record,
    *,
    dedupe_key: str,
    provider: str = PROVIDER_EASYWEEK,
    job_type: str = "record_created",
    status: str = "queued",
    attempts: int = 0,
    locked: bool = False,
    run_at: timedelta = timedelta(minutes=-10),
    with_digest: bool = True,
) -> MessageJob:
    payload: dict[str, Any] = {MULTI_SERVICE_JOB_DIGEST_KEY: _DIGEST} if with_digest else {}
    job = MessageJob(
        provider=provider,
        company_id=TEST_LOCATION_ID,
        record_id=record.id,
        client_id=client.id,
        job_type=job_type,
        run_at=utcnow() + run_at,
        status=status,
        attempts=attempts,
        locked_at=utcnow() if locked else None,
        dedupe_key=dedupe_key,
        payload=payload,
    )
    session.add(job)
    await session.flush()
    return job


async def _seed_outbox(
    session,
    job: MessageJob,
    *,
    status: str,
    sent_offset: timedelta | None = timedelta(minutes=-1),
) -> OutboxMessage:
    row = OutboxMessage(
        company_id=job.company_id,
        client_id=job.client_id,
        record_id=job.record_id,
        job_id=job.id,
        phone_e164="+49000000000",
        template_code=job.job_type,
        body="fixture body",
        status=status,
        scheduled_at=job.run_at,
        sent_at=(utcnow() + sent_offset) if sent_offset is not None else None,
        meta={},
    )
    session.add(row)
    await session.flush()
    return row


async def _sent(session, client, record, *, key: str, outbox_status: str = "sent") -> MessageJob:
    job = await _seed_job(session, client, record, dedupe_key=key, status="done")
    await _seed_outbox(session, job, status=outbox_status)
    return job


async def _state(session) -> list[tuple[Any, ...]]:
    rows: list[tuple[Any, ...]] = []
    for job in (await session.execute(select(MessageJob).order_by(MessageJob.id))).scalars():
        rows.append((job.id, job.status, job.attempts, job.locked_at, job.run_at, job.payload))
    for row in (await session.execute(select(OutboxMessage).order_by(OutboxMessage.id))).scalars():
        rows.append((row.id, row.status, row.sent_at))
    return rows


# ===========================================================================
# The approved-id list
# ===========================================================================


async def test_the_approved_list_accepts_repeats_and_comma_groups() -> None:
    assert parse_job_id_list(["1", "2,3", " 4 , 5"]) == [1, 2, 3, 4, 5]


@pytest.mark.parametrize(
    "value",
    ["0", "-1", "+1", "1.0", "true", "١٤", "７", "1 2", "", "1,", ",1", "abc", "1e3"],
)
async def test_the_approved_list_refuses_anything_but_positive_decimals(value: str) -> None:
    with pytest.raises(JobIdListError):
        parse_job_id_list([value])


async def test_the_approved_list_refuses_duplicates() -> None:
    with pytest.raises(JobIdListError, match="duplicate"):
        parse_job_id_list(["7", "7"])
    with pytest.raises(JobIdListError, match="duplicate"):
        parse_job_id_list(["7,8,7"])


# ===========================================================================
# A due job's terminal outcome
# ===========================================================================


async def test_done_with_a_sent_outbox_row_is_the_only_plain_success(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            job = await _sent(session, client, record, key="verify-success")
        job_id = job.id

    report = await verify_release(
        session_maker,
        due_job_ids=[job_id],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )

    assert report.due_outcomes[SUCCEEDED] == 1
    assert report.succeeded_job_ids == [job_id]
    assert report.settled is True
    assert report.verified is True


@pytest.mark.parametrize("outbox_status", ["sent", "delivered", "read"])
async def test_every_proven_delivery_status_counts_as_success(session_maker, outbox_status: str) -> None:
    """The project's own SUCCESS_OUTBOX_STATUSES, not a second opinion."""
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            job = await _sent(session, client, record, key=f"verify-{outbox_status}", outbox_status=outbox_status)
        job_id = job.id

    report = await verify_release(session_maker, due_job_ids=[job_id], opened_at=OPENED_AT, sleep=_no_sleep)

    assert report.due_outcomes[SUCCEEDED] == 1
    assert report.verified is True


async def test_a_done_job_without_a_proven_outbox_row_is_a_failure(session_maker) -> None:
    """`job.status == 'done'` is a claim, not evidence."""
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            job = await _seed_job(session, client, record, dedupe_key="verify-done-bare", status="done")
        job_id = job.id

    report = await verify_release(session_maker, due_job_ids=[job_id], opened_at=OPENED_AT, sleep=_no_sleep)

    assert report.due_outcomes[FAILED] == 1
    assert report.reasons[REASON_DONE_WITHOUT_PROVEN_SEND] == 1
    assert report.unsuccessful_job_ids == [job_id]
    assert report.verified is False


@pytest.mark.parametrize("outbox_status", ["failed", "canceled"])
async def test_a_done_job_with_an_unsuccessful_outbox_row_is_a_failure(session_maker, outbox_status: str) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            job = await _seed_job(session, client, record, dedupe_key=f"verify-done-{outbox_status}", status="done")
            await _seed_outbox(session, job, status=outbox_status)
        job_id = job.id

    report = await verify_release(session_maker, due_job_ids=[job_id], opened_at=OPENED_AT, sleep=_no_sleep)

    assert report.due_outcomes[FAILED] == 1
    assert report.verified is False


async def test_a_failed_job_is_a_failure(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            job = await _seed_job(
                session,
                client,
                record,
                dedupe_key="verify-failed",
                status="failed",
                attempts=5,
            )
            await _seed_outbox(session, job, status="failed")
        job_id = job.id

    report = await verify_release(session_maker, due_job_ids=[job_id], opened_at=OPENED_AT, sleep=_no_sleep)

    assert report.due_outcomes[FAILED] == 1
    assert report.reasons["job_status_failed"] == 1
    assert report.verified is False


async def test_a_canceled_provider_candidate_has_its_own_unsuccessful_outcome(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            job = await _seed_job(session, client, record, dedupe_key="verify-canceled", status="canceled")
        job_id = job.id

    report = await verify_release(session_maker, due_job_ids=[job_id], opened_at=OPENED_AT, sleep=_no_sleep)

    assert report.due_outcomes[CANCELED_WITHOUT_PROVIDER_SEND] == 1
    assert report.due_outcomes[FAILED] == 0
    assert report.verified is False


async def test_an_unknown_outbox_outcome_is_indeterminate_and_never_success(session_maker) -> None:
    """The model's own contract: never auto-retried, requires manual review."""
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            job = await _seed_job(session, client, record, dedupe_key="verify-unknown", status="done")
            await _seed_outbox(session, job, status="unknown")
        job_id = job.id

    report = await verify_release(session_maker, due_job_ids=[job_id], opened_at=OPENED_AT, sleep=_no_sleep)

    assert report.due_outcomes[UNKNOWN_OR_INDETERMINATE] == 1
    assert report.reasons[REASON_OUTBOX_UNKNOWN] == 1
    assert report.due_outcomes[SUCCEEDED] == 0
    assert report.pending is False, "an indeterminate outcome is terminal, not something to wait out"
    assert report.verified is False


async def test_a_proven_send_under_a_failed_job_is_indeterminate_not_success(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            job = await _seed_job(session, client, record, dedupe_key="verify-contradiction", status="failed")
            await _seed_outbox(session, job, status="sent")
        job_id = job.id

    report = await verify_release(session_maker, due_job_ids=[job_id], opened_at=OPENED_AT, sleep=_no_sleep)

    assert report.due_outcomes[UNKNOWN_OR_INDETERMINATE] == 1
    assert report.reasons["proven_send_contradicts_job_status"] == 1


async def test_a_queued_retry_is_pending_not_a_failure(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            job = await _seed_job(
                session,
                client,
                record,
                dedupe_key="verify-retry",
                status="queued",
                attempts=2,
                run_at=timedelta(minutes=3),
            )
        job_id = job.id

    report = await verify_release(
        session_maker,
        due_job_ids=[job_id],
        opened_at=OPENED_AT,
        settle_sec=0,
        sleep=_no_sleep,
    )

    assert report.due_outcomes[RETRY_SCHEDULED] == 1
    assert report.pending_job_ids == [job_id]
    assert report.due_outcomes[FAILED] == 0
    assert report.verified is False


async def test_a_missing_job_row_is_a_failure(session_maker) -> None:
    report = await verify_release(session_maker, due_job_ids=[987654], opened_at=OPENED_AT, sleep=_no_sleep)

    assert report.due_outcomes[MISSING] == 1
    assert report.reasons["job_row_missing"] == 1
    assert report.verified is False


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"provider": PROVIDER_ALTEGIO}, "job_provider_not_easyweek"),
        ({"job_type": "review_3d"}, "job_type_not_pair_supported"),
        ({"with_digest": False}, "job_payload_without_pair_digest"),
    ],
)
async def test_a_row_that_is_not_the_approved_pair_job_is_an_identity_mismatch(
    session_maker,
    kwargs: dict[str, Any],
    reason: str,
) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            job = await _seed_job(
                session,
                client,
                record,
                dedupe_key=f"verify-identity-{reason}",
                status="done",
                **kwargs,
            )
            await _seed_outbox(session, job, status="sent")
        job_id = job.id

    report = await verify_release(session_maker, due_job_ids=[job_id], opened_at=OPENED_AT, sleep=_no_sleep)

    assert report.due_outcomes[IDENTITY_MISMATCH] == 1
    assert report.reasons[reason] == 1
    assert report.due_outcomes[SUCCEEDED] == 0
    assert report.verified is False


# ===========================================================================
# The batch, and the bounded settle window
# ===========================================================================


async def test_a_batch_seen_mid_flight_first_is_not_a_failure_and_then_succeeds(session_maker) -> None:
    """The exact shape of a healthy opening: a committed `processing` batch.

    The worker marks the whole batch at once and only then works through it,
    so the first look is several rows in flight. Waiting is the correct
    response; calling it a rollout failure would start an emergency rollback
    in the middle of normal delivery.
    """
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            batch = [
                await _seed_job(
                    session,
                    client,
                    record,
                    dedupe_key=f"verify-batch-{index}",
                    status="processing",
                    locked=True,
                )
                for index in range(3)
            ]
        batch_ids = [job.id for job in batch]

    seen: list[list[str]] = []

    async def _finish_the_batch(_seconds: float) -> None:
        """Stand in for the worker completing the batch between two polls."""
        async with session_maker() as session:
            async with session.begin():
                for job_id in batch_ids:
                    job = await session.get(MessageJob, job_id)
                    assert job is not None
                    job.status = "done"
                    job.locked_at = None
                    session.add(
                        OutboxMessage(
                            company_id=job.company_id,
                            client_id=job.client_id,
                            record_id=job.record_id,
                            job_id=job.id,
                            phone_e164="+49000000000",
                            template_code=job.job_type,
                            body="fixture body",
                            status="sent",
                            scheduled_at=job.run_at,
                            sent_at=utcnow(),
                            meta={},
                        )
                    )

    async def _recording_sleep(seconds: float) -> None:
        seen.append(["polled"])
        await _finish_the_batch(seconds)

    report = await verify_release(
        session_maker,
        due_job_ids=batch_ids,
        opened_at=OPENED_AT,
        settle_sec=60,
        poll_sec=1.0,
        sleep=_recording_sleep,
    )

    assert seen, "the first look must have been pending, not final"
    assert report.polls >= 2
    assert report.due_outcomes[SUCCEEDED] == 3
    assert report.due_outcomes[FAILED] == 0
    assert report.settled is True
    assert report.verified is True


async def test_a_single_mid_flight_look_is_reported_as_in_progress(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            job = await _seed_job(
                session,
                client,
                record,
                dedupe_key="verify-inflight",
                status="processing",
                locked=True,
            )
        job_id = job.id

    report = await verify_release(
        session_maker,
        due_job_ids=[job_id],
        opened_at=OPENED_AT,
        settle_sec=0,
        sleep=_no_sleep,
    )

    assert report.due_outcomes[IN_PROGRESS] == 1
    assert report.due_outcomes[FAILED] == 0
    assert report.pending is True
    assert report.verified is False


async def test_a_sending_outbox_row_is_in_progress_too(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            job = await _seed_job(session, client, record, dedupe_key="verify-sending", status="processing")
            await _seed_outbox(session, job, status="sending", sent_offset=None)
        job_id = job.id

    report = await verify_release(
        session_maker,
        due_job_ids=[job_id],
        opened_at=OPENED_AT,
        settle_sec=0,
        sleep=_no_sleep,
    )

    assert report.due_outcomes[IN_PROGRESS] == 1


async def test_the_settle_window_is_bounded_and_stays_pending_rather_than_green(session_maker) -> None:
    """A rollout step that can hang is one nobody will run under pressure."""
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            job = await _seed_job(
                session,
                client,
                record,
                dedupe_key="verify-timeout",
                status="processing",
                locked=True,
            )
        job_id = job.id

    ticks = {"count": 0}
    start = utcnow()

    def _clock() -> Any:
        # Five seconds pass on every look, so the 20s window expires quickly
        # and deterministically without any real waiting.
        ticks["count"] += 1
        return start + timedelta(seconds=5 * ticks["count"])

    report = await verify_release(
        session_maker,
        due_job_ids=[job_id],
        opened_at=OPENED_AT,
        settle_sec=20,
        poll_sec=5.0,
        sleep=_no_sleep,
        now=_clock,
    )

    assert report.settled is False
    assert report.pending is True
    assert report.pending_job_ids == [job_id]
    assert report.reasons[REASON_SETTLE_TIMEOUT] == 1
    assert report.verified is False, "a timeout is never a success"
    assert report.polls <= (20 // 5) + 1, "the loop is bounded, not open-ended"


async def test_an_empty_due_set_is_refused_unless_the_operator_allowed_it(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            future = await _seed_job(
                session,
                client,
                record,
                dedupe_key="verify-empty-due-future",
                job_type="reminder_24h",
                run_at=timedelta(days=2),
            )
        future_id = future.id

    refused = await verify_release(
        session_maker,
        due_job_ids=[],
        future_job_ids=[future_id],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )
    assert refused.config_error == REASON_EMPTY_DUE_SET
    assert refused.empty_due_allowed is False
    assert refused.verified is False


# ===========================================================================
# Sends outside the approved set
# ===========================================================================


async def test_a_pair_job_sent_outside_the_approved_set_turns_the_report_red(session_maker) -> None:
    """With the producer paused, the approved inventory is the whole set."""
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            approved = await _sent(session, client, record, key="verify-approved")
            stranger = await _sent(session, client, record, key="verify-stranger")
        approved_id, stranger_id = approved.id, stranger.id

    report = await verify_release(
        session_maker,
        due_job_ids=[approved_id],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )

    assert report.due_outcomes[SUCCEEDED] == 1
    assert report.unapproved_sent_job_ids == [stranger_id]
    assert report.reasons[REASON_UNAPPROVED_SEND] == 1
    assert report.verified is False


async def test_a_send_before_the_opening_marker_is_outside_the_window(session_maker) -> None:
    """The marker bounds the window; history is not a rollout finding."""
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            approved = await _sent(session, client, record, key="verify-window-approved")
            historical = await _seed_job(
                session,
                client,
                record,
                dedupe_key="verify-window-historical",
                status="done",
            )
            await _seed_outbox(session, historical, status="sent", sent_offset=timedelta(days=-3))
        approved_id = approved.id

    report = await verify_release(
        session_maker,
        due_job_ids=[approved_id],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )

    assert report.unapproved_sent_job_ids == []
    assert report.verified is True


async def test_other_providers_and_job_families_are_never_pair_sends(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            approved = await _sent(session, client, record, key="verify-families-approved")
            for index, (provider, job_type, with_digest) in enumerate(
                (
                    (PROVIDER_ALTEGIO, "record_created", True),
                    (PROVIDER_EASYWEEK, "record_updated", False),
                    (PROVIDER_EASYWEEK, "review_3d", True),
                    (PROVIDER_EASYWEEK, "repeat_10d", True),
                    (PROVIDER_EASYWEEK, "comeback_3d", True),
                    (PROVIDER_ALTEGIO, "followup_14d", True),
                )
            ):
                other = await _seed_job(
                    session,
                    client,
                    record,
                    dedupe_key=f"verify-family-{index}",
                    provider=provider,
                    job_type=job_type,
                    status="done",
                    with_digest=with_digest,
                )
                await _seed_outbox(session, other, status="sent")
        approved_id = approved.id

    report = await verify_release(
        session_maker,
        due_job_ids=[approved_id],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )

    assert report.unapproved_sent_job_ids == []
    assert report.verified is True


# ===========================================================================
# Approved future jobs
# ===========================================================================


async def test_an_approved_future_job_that_has_not_fired_is_the_expected_result(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            due = await _sent(session, client, record, key="verify-future-due")
            future = await _seed_job(
                session,
                client,
                record,
                dedupe_key="verify-future-pending",
                job_type="reminder_24h",
                run_at=timedelta(days=2),
            )
        due_id, future_id = due.id, future.id

    report = await verify_release(
        session_maker,
        due_job_ids=[due_id],
        future_job_ids=[future_id],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )

    assert report.future_outcomes[FUTURE_PENDING] == 1
    assert report.future_problem_job_ids == []
    assert report.verified is True


async def test_a_future_job_sent_before_its_run_at_turns_the_report_red(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            due = await _sent(session, client, record, key="verify-early-due")
            future = await _seed_job(
                session,
                client,
                record,
                dedupe_key="verify-early-future",
                job_type="reminder_24h",
                status="done",
                run_at=timedelta(days=2),
            )
            await _seed_outbox(session, future, status="sent", sent_offset=timedelta(minutes=-1))
        due_id, future_id = due.id, future.id

    report = await verify_release(
        session_maker,
        due_job_ids=[due_id],
        future_job_ids=[future_id],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )

    assert report.future_outcomes[FUTURE_SENT_EARLY] == 1
    assert report.future_problem_job_ids == [future_id]
    assert report.verified is False


async def test_a_future_job_that_became_due_and_fired_on_schedule_is_accepted(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            due = await _sent(session, client, record, key="verify-onsched-due")
            matured = await _seed_job(
                session,
                client,
                record,
                dedupe_key="verify-onsched-future",
                job_type="reminder_24h",
                status="done",
                run_at=timedelta(minutes=-30),
            )
            await _seed_outbox(session, matured, status="sent", sent_offset=timedelta(minutes=-5))
        due_id, matured_id = due.id, matured.id

    report = await verify_release(
        session_maker,
        due_job_ids=[due_id],
        future_job_ids=[matured_id],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )

    assert report.future_outcomes[FUTURE_RELEASED_ON_SCHEDULE] == 1
    assert report.future_problem_job_ids == []
    assert report.verified is True


async def test_a_missing_or_foreign_future_job_is_still_a_problem(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            due = await _sent(session, client, record, key="verify-future-missing-due")
        due_id = due.id

    report = await verify_release(
        session_maker,
        due_job_ids=[due_id],
        future_job_ids=[987654],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )

    assert report.future_outcomes[MISSING] == 1
    assert report.future_problem_job_ids == [987654]
    assert report.verified is False


# ===========================================================================
# Read-only, and safe to paste into a ticket
# ===========================================================================


async def test_the_verifier_changes_no_row_and_no_field(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            await _sent(session, client, record, key="verify-readonly-1")
            await _seed_job(session, client, record, dedupe_key="verify-readonly-2", status="processing", locked=True)

    async with session_maker() as session:
        before = await _state(session)
        counts_before = [
            int((await session.execute(select(func.count()).select_from(model))).scalar_one())
            for model in (MessageJob, OutboxMessage, Record)
        ]

    async with session_maker() as session:
        jobs = list((await session.execute(select(MessageJob).order_by(MessageJob.id))).scalars())
    await verify_release(
        session_maker,
        due_job_ids=[job.id for job in jobs],
        opened_at=OPENED_AT,
        settle_sec=0,
        sleep=_no_sleep,
    )

    async with session_maker() as session:
        assert await _state(session) == before
        counts_after = [
            int((await session.execute(select(func.count()).select_from(model))).scalar_one())
            for model in (MessageJob, OutboxMessage, Record)
        ]
    assert counts_after == counts_before


async def test_the_report_carries_no_booking_uuid_phone_name_or_body(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            job = await _sent(session, client, record, key="verify-pii")
        job_id = job.id

    report = await verify_release(session_maker, due_job_ids=[job_id], opened_at=OPENED_AT, sleep=_no_sleep)

    text = str(report.as_safe_dict())
    for forbidden in (
        TEST_BOOKING_UUID,
        TEST_LOCATION_UUID,
        "Release verify fixture",
        "+49000000000",
        "fixture body",
        _DIGEST,
    ):
        assert forbidden not in text, forbidden
    safe = report.as_safe_dict()
    assert safe["read_only"] is True
    assert safe["send_authorized"] is False
    assert safe["config_changed"] is False
    assert safe["rollback_performed"] is False


# ===========================================================================
# The command line
# ===========================================================================


async def test_the_cli_exits_zero_only_on_a_verified_release(session_maker, monkeypatch) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            good = await _sent(session, client, record, key="verify-cli-good")
            bad = await _seed_job(session, client, record, dedupe_key="verify-cli-bad", status="failed")
        good_id, bad_id = good.id, bad.id

    monkeypatch.setattr(verify_cli, "SessionLocal", session_maker, raising=False)
    marker = OPENED_AT.isoformat()

    assert await verify_cli.main(["--due-job-id", str(good_id), "--opened-at", marker]) == 0
    assert await verify_cli.main(["--due-job-ids", f"{good_id},{bad_id}", "--opened-at", marker]) == 1


async def test_the_cli_refuses_a_naive_or_future_marker() -> None:
    for bad in ("2026-09-20T10:00:00", "not-a-time", (utcnow() + timedelta(days=1)).isoformat()):
        with pytest.raises(SystemExit):
            verify_cli._parse_args(["--due-job-id", "1", "--opened-at", bad])

    parsed = verify_cli._parse_args(["--due-job-id", "1", "--opened-at", OPENED_AT.isoformat()])
    assert parsed.due_ids == [1]
    assert parsed.opened_at_dt.tzinfo is not None


async def test_the_cli_refuses_an_id_in_both_sets() -> None:
    with pytest.raises(SystemExit):
        verify_cli._parse_args(["--due-job-id", "5", "--future-job-id", "5", "--opened-at", OPENED_AT.isoformat()])


async def test_the_cli_bounds_the_settle_window() -> None:
    with pytest.raises(SystemExit):
        verify_cli._parse_args(["--due-job-id", "1", "--opened-at", OPENED_AT.isoformat(), "--settle-sec", "99999"])
    with pytest.raises(SystemExit):
        verify_cli._parse_args(["--due-job-id", "1", "--opened-at", OPENED_AT.isoformat(), "--poll-sec", "0.1"])


# ===========================================================================
# The all-future inventory left behind by a successful canary
# ===========================================================================


async def _seed_future(session, client, record, *, key: str, **kwargs: Any) -> MessageJob:
    kwargs.setdefault("job_type", "reminder_24h")
    kwargs.setdefault("run_at", timedelta(days=2))
    return await _seed_job(session, client, record, dedupe_key=key, **kwargs)


async def test_an_all_future_inventory_can_be_verified_when_the_operator_allows_it(session_maker) -> None:
    """The rollout must be closable after the canary consumed the only due job.

    The canary's `record_created` is terminal and gone from the release set,
    so what remains is reminders that fire on their own schedule. Requiring a
    due job here would make step 58 unreachable and strand the inbox worker.
    """
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            first = await _seed_future(session, client, record, key="verify-allfuture-1")
            second = await _seed_future(
                session,
                client,
                record,
                key="verify-allfuture-2",
                job_type="reminder_2h",
                run_at=timedelta(days=3),
            )
        future_ids = [first.id, second.id]

    report = await verify_release(
        session_maker,
        due_job_ids=[],
        future_job_ids=future_ids,
        opened_at=OPENED_AT,
        allow_empty_due=True,
        sleep=_no_sleep,
    )

    assert report.config_error is None
    assert report.approved_due == 0
    assert report.empty_due_allowed is True
    assert report.future_outcomes[FUTURE_PENDING] == 2
    assert report.inventory_proven is True
    assert report.verified is True


async def test_an_entirely_empty_inventory_is_never_verified(session_maker) -> None:
    """`--allow-empty-due` widens the SHAPE of an inventory, not its emptiness."""
    report = await verify_release(
        session_maker,
        due_job_ids=[],
        future_job_ids=[],
        opened_at=OPENED_AT,
        allow_empty_due=True,
        sleep=_no_sleep,
    )

    assert report.config_error == REASON_EMPTY_INVENTORY
    assert report.inventory_proven is False
    assert report.verified is False


async def test_the_report_states_that_an_empty_due_set_was_allowed(session_maker) -> None:
    """An operator must never have to guess why a zero due count was accepted."""
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            future = await _seed_future(session, client, record, key="verify-allfuture-flag")
        future_id = future.id

    report = await verify_release(
        session_maker,
        due_job_ids=[],
        future_job_ids=[future_id],
        opened_at=OPENED_AT,
        allow_empty_due=True,
        sleep=_no_sleep,
    )

    safe = report.as_safe_dict()
    assert safe["empty_due_allowed"] is True
    assert safe["approved_due"] == 0
    assert safe["approved_future"] == 1
    assert safe["inventory_proven"] is True


@pytest.mark.parametrize(
    ("flag", "value", "expected"),
    [
        ("easyweek_multi_service_send_enabled", False, "multi_service_send_disabled"),
        ("easyweek_multi_service_canary_job_id", "4242", "multi_service_canary_configured"),
        ("easyweek_notifications_enabled", False, "easyweek_notifications_disabled"),
        ("easyweek_reminders_enabled", False, "easyweek_reminders_disabled"),
        ("easyweek_reminder_api_guard_enabled", False, "easyweek_reminder_api_guard_disabled"),
        ("easyweek_multi_service_notifications_enabled", False, "multi_service_disabled"),
        ("easyweek_resource_shadow_proof_enabled", False, "multi_service_resource_shadow_disabled"),
    ],
)
async def test_the_effective_bulk_configuration_is_part_of_the_verdict(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
    flag: str,
    value: Any,
    expected: str,
) -> None:
    """With no due job to send, a shut fence looks exactly like a quiet one.

    That is why the all-future case leans on this check: absence of a message
    is only evidence once the configuration proves a message COULD have gone.
    """
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            future = await _seed_future(session, client, record, key=f"verify-config-{flag}")
        future_id = future.id

    monkeypatch.setattr(settings, flag, value, raising=False)
    report = await verify_release(
        session_maker,
        due_job_ids=[],
        future_job_ids=[future_id],
        opened_at=OPENED_AT,
        allow_empty_due=True,
        sleep=_no_sleep,
    )

    assert report.config_error == expected
    assert report.verified is False


async def test_a_broken_bulk_configuration_does_not_burn_the_settle_window(
    session_maker,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nothing can settle behind a fence that is shut; waiting would be theatre."""
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            job = await _seed_job(session, client, record, dedupe_key="verify-config-nowait", status="processing")
        job_id = job.id

    slept: list[float] = []

    async def _record_sleep(seconds: float) -> None:
        slept.append(seconds)

    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", False, raising=False)
    report = await verify_release(
        session_maker,
        due_job_ids=[job_id],
        opened_at=OPENED_AT,
        settle_sec=600,
        poll_sec=5.0,
        sleep=_record_sleep,
    )

    assert slept == [], "a wrong configuration ends the wait immediately"
    assert report.polls == 1
    assert report.config_error == "multi_service_send_disabled"
    assert report.verified is False


async def test_a_due_set_still_has_to_succeed_even_when_empty_due_is_allowed(session_maker) -> None:
    """The flag must not become a way to wave a failed due job through."""
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            bad = await _seed_job(session, client, record, dedupe_key="verify-allowflag-bad", status="failed")
            future = await _seed_future(session, client, record, key="verify-allowflag-future")
        bad_id, future_id = bad.id, future.id

    report = await verify_release(
        session_maker,
        due_job_ids=[bad_id],
        future_job_ids=[future_id],
        opened_at=OPENED_AT,
        allow_empty_due=True,
        sleep=_no_sleep,
    )

    assert report.due_outcomes[FAILED] == 1
    assert report.inventory_proven is False
    assert report.verified is False


async def test_an_all_future_inventory_still_fails_on_an_unapproved_send(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            future = await _seed_future(session, client, record, key="verify-allfuture-stranger-future")
            stranger = await _sent(session, client, record, key="verify-allfuture-stranger")
        future_id, stranger_id = future.id, stranger.id

    report = await verify_release(
        session_maker,
        due_job_ids=[],
        future_job_ids=[future_id],
        opened_at=OPENED_AT,
        allow_empty_due=True,
        sleep=_no_sleep,
    )

    assert report.unapproved_sent_job_ids == [stranger_id]
    assert report.verified is False


# ===========================================================================
# A future job is only "pending" when it is actually in the approved state
# ===========================================================================


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        ({"status": "failed"}, "job_status_failed"),
        ({"status": "canceled"}, "job_status_canceled"),
        ({"status": "done"}, "done_without_proven_outbox_send"),
        ({"status": "paused"}, "job_status_unrecognised"),
        ({"status": "processing"}, "future_claimed_before_run_at"),
        ({"status": "queued", "locked": True}, "future_claimed_before_run_at"),
    ],
)
async def test_a_future_job_that_is_not_simply_scheduled_is_never_accepted(
    session_maker,
    kwargs: dict[str, Any],
    reason: str,
) -> None:
    """Absence of a send is not the same as being in the approved state."""
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            due = await _sent(session, client, record, key=f"verify-futurestate-due-{reason}-{kwargs}")
            future = await _seed_future(
                session,
                client,
                record,
                key=f"verify-futurestate-{reason}-{kwargs}",
                **kwargs,
            )
        due_id, future_id = due.id, future.id

    report = await verify_release(
        session_maker,
        due_job_ids=[due_id],
        future_job_ids=[future_id],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )

    assert report.future_outcomes[FUTURE_UNEXPECTED_STATE] == 1
    assert report.future_outcomes[FUTURE_PENDING] == 0
    assert report.reasons[reason] == 1
    assert report.future_problem_job_ids == [future_id]
    assert report.verified is False


async def test_a_future_job_with_a_sending_outbox_row_before_run_at_is_not_accepted(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            due = await _sent(session, client, record, key="verify-future-sending-due")
            future = await _seed_future(session, client, record, key="verify-future-sending")
            await _seed_outbox(session, future, status="sending", sent_offset=None)
        due_id, future_id = due.id, future.id

    report = await verify_release(
        session_maker,
        due_job_ids=[due_id],
        future_job_ids=[future_id],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )

    assert report.future_outcomes[FUTURE_UNEXPECTED_STATE] == 1
    assert report.reasons["future_claimed_before_run_at"] == 1
    assert report.verified is False


async def test_a_future_job_with_an_unknown_outbox_outcome_is_indeterminate(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            due = await _sent(session, client, record, key="verify-future-unknown-due")
            future = await _seed_future(session, client, record, key="verify-future-unknown", status="done")
            await _seed_outbox(session, future, status="unknown")
        due_id, future_id = due.id, future.id

    report = await verify_release(
        session_maker,
        due_job_ids=[due_id],
        future_job_ids=[future_id],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )

    assert report.future_outcomes[FUTURE_INDETERMINATE] == 1
    assert report.reasons[REASON_OUTBOX_UNKNOWN] == 1
    assert report.future_problem_job_ids == [future_id]
    assert report.verified is False


async def test_a_future_send_that_contradicts_its_job_status_is_indeterminate(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            due = await _sent(session, client, record, key="verify-future-contra-due")
            future = await _seed_future(
                session,
                client,
                record,
                key="verify-future-contra",
                status="failed",
                run_at=timedelta(minutes=-30),
            )
            await _seed_outbox(session, future, status="sent", sent_offset=timedelta(minutes=-5))
        due_id, future_id = due.id, future.id

    report = await verify_release(
        session_maker,
        due_job_ids=[due_id],
        future_job_ids=[future_id],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )

    assert report.future_outcomes[FUTURE_INDETERMINATE] == 1
    assert report.reasons["proven_send_contradicts_job_status"] == 1


async def test_a_future_send_with_no_sent_at_cannot_be_cleared(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            due = await _sent(session, client, record, key="verify-future-nosentat-due")
            future = await _seed_future(
                session,
                client,
                record,
                key="verify-future-nosentat",
                status="done",
                run_at=timedelta(minutes=-30),
            )
            await _seed_outbox(session, future, status="sent", sent_offset=None)
        due_id, future_id = due.id, future.id

    report = await verify_release(
        session_maker,
        due_job_ids=[due_id],
        future_job_ids=[future_id],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )

    assert report.future_outcomes[FUTURE_INDETERMINATE] == 1
    assert report.reasons["future_send_time_unprovable"] == 1


async def test_a_future_job_that_matured_during_the_run_gets_its_own_pending_state(session_maker) -> None:
    """It is neither wrong nor finished, and it must not hide inside future_pending."""
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            due = await _sent(session, client, record, key="verify-matured-due")
            matured = await _seed_future(
                session,
                client,
                record,
                key="verify-matured",
                run_at=timedelta(minutes=-2),
            )
        due_id, matured_id = due.id, matured.id

    report = await verify_release(
        session_maker,
        due_job_ids=[due_id],
        future_job_ids=[matured_id],
        opened_at=OPENED_AT,
        settle_sec=0,
        sleep=_no_sleep,
    )

    assert report.future_outcomes[FUTURE_MATURED_PENDING] == 1
    assert report.future_outcomes[FUTURE_PENDING] == 0
    assert report.future_maturing_job_ids == [matured_id]
    assert report.future_problem_job_ids == [], "maturing is not a problem, it is unfinished"
    assert report.pending is True
    assert report.settled is False
    assert report.verified is False


async def test_a_maturing_future_job_settles_into_success_within_the_window(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            due = await _sent(session, client, record, key="verify-matured-settle-due")
            matured = await _seed_future(
                session,
                client,
                record,
                key="verify-matured-settle",
                run_at=timedelta(minutes=-2),
            )
        due_id, matured_id = due.id, matured.id

    async def _finish(_seconds: float) -> None:
        async with session_maker() as session:
            async with session.begin():
                job = await session.get(MessageJob, matured_id)
                assert job is not None
                job.status = "done"
                session.add(
                    OutboxMessage(
                        company_id=job.company_id,
                        client_id=job.client_id,
                        record_id=job.record_id,
                        job_id=job.id,
                        phone_e164="+49000000000",
                        template_code=job.job_type,
                        body="fixture body",
                        status="sent",
                        scheduled_at=job.run_at,
                        sent_at=utcnow(),
                        meta={},
                    )
                )

    report = await verify_release(
        session_maker,
        due_job_ids=[due_id],
        future_job_ids=[matured_id],
        opened_at=OPENED_AT,
        settle_sec=60,
        poll_sec=1.0,
        sleep=_finish,
    )

    assert report.future_outcomes[FUTURE_RELEASED_ON_SCHEDULE] == 1
    assert report.future_maturing_job_ids == []
    assert report.settled is True
    assert report.verified is True


async def test_a_future_job_without_a_run_at_cannot_be_judged() -> None:
    """`run_at` is NOT NULL in the schema, so this is a defensive branch.

    Exercised directly rather than through the database: without the
    scheduled instant, "early" and "on schedule" both stop meaning anything,
    and the fail-closed answer must be indeterminate rather than pending.
    """
    job = MessageJob(
        provider=PROVIDER_EASYWEEK,
        company_id=TEST_LOCATION_ID,
        job_type="reminder_24h",
        status="queued",
        dedupe_key="verify-future-norunat",
        payload={MULTI_SERVICE_JOB_DIGEST_KEY: _DIGEST},
    )
    job.run_at = None

    outcome, reason = classify_future_job(job, [], now=utcnow())

    assert outcome == FUTURE_INDETERMINATE
    assert reason == "future_run_at_missing"


async def test_future_problems_never_leak_into_the_due_outcome_counters(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            due = await _sent(session, client, record, key="verify-separation-due")
            future = await _seed_future(session, client, record, key="verify-separation-future", status="failed")
        due_id, future_id = due.id, future.id

    report = await verify_release(
        session_maker,
        due_job_ids=[due_id],
        future_job_ids=[future_id],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )

    assert report.due_outcomes[SUCCEEDED] == 1
    assert report.due_outcomes[FAILED] == 0, "a future problem is not a due outcome"
    assert report.unsuccessful_job_ids == []
    assert report.future_problem_job_ids == [future_id]
    assert report.verified is False


async def test_a_red_future_report_still_carries_no_customer_data(session_maker) -> None:
    async with session_maker() as session:
        async with session.begin():
            client, record = await _seed_client_and_record(session)
            due = await _sent(session, client, record, key="verify-future-pii-due")
            future = await _seed_future(session, client, record, key="verify-future-pii", status="canceled")
        due_id, future_id = due.id, future.id

    report = await verify_release(
        session_maker,
        due_job_ids=[due_id],
        future_job_ids=[future_id],
        opened_at=OPENED_AT,
        sleep=_no_sleep,
    )

    text = str(report.as_safe_dict())
    for forbidden in (
        TEST_BOOKING_UUID,
        TEST_LOCATION_UUID,
        "Release verify fixture",
        "+49000000000",
        "fixture body",
        _DIGEST,
    ):
        assert forbidden not in text, forbidden
