"""The two runtime fences of the post-booking handover (plan §31.7).

Withdrawing the open jobs once is not enough. `add_job` resurrects a `canceled`
job on conflict, so a late Altegio `create` re-opens exactly what the handover
withdrew; a late `delete` plans a fresh `comeback_3d`; and a booking that had no
such job at all can acquire its first one afterwards. The outbox is the second
line: a job already sitting in the queue — inserted before the handover, or by a
delivery that raced its commit — must stop before the template, the sender,
Meta and Chatwoot, and without spending an attempt.

Both fences are narrow by construction: provider `altegio`, one company, one
source record, three job types, and only a completed PR-12.1 marker. Reminders
keep their §30 fence, lifecycle jobs, campaigns, other companies, unmigrated
records and every EasyWeek job are untouched.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from altegio_bot import message_planner as planner_mod
from altegio_bot.message_planner import (
    COMEBACK_3D,
    REMINDER_24H,
    REPEAT_10D,
    REVIEW_3D,
    plan_jobs_for_record_event,
)
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    Client,
    EasyWeekMigrationLedger,
    MessageJob,
    OutboxMessage,
    Record,
)
from altegio_bot.post_booking_ownership import (
    POST_BOOKING_JOB_TYPES,
    REASON_HANDED_OVER,
    PostBookingOwner,
    altegio_post_booking_jobs_are_suppressed,
)

COMPANY = 758285
SOURCE_RECORD_ID = 910001


@pytest.fixture(autouse=True)
def _mock_altegio_api(monkeypatch):
    """`review_3d` eligibility asks the Altegio API; this file is about the fence."""
    monkeypatch.setattr(planner_mod, "count_attended_client_visits", AsyncMock(return_value=1))


async def _seed_record(session_maker, *, migrated: bool, handed_over: bool, record_id: int = SOURCE_RECORD_ID):
    """One Altegio booking, optionally migrated and optionally handed over."""
    async with session_maker() as session:
        async with session.begin():
            client = Client(company_id=COMPANY, altegio_client_id=4242, phone_e164="+490000000000")
            session.add(client)
            await session.flush()
            record = Record(
                provider=PROVIDER_ALTEGIO,
                company_id=COMPANY,
                altegio_record_id=record_id,
                client_id=client.id,
                staff_name="Staff",
                starts_at=datetime.now(timezone.utc) + timedelta(hours=48),
            )
            session.add(record)
            await session.flush()
            if migrated:
                session.add(
                    EasyWeekMigrationLedger(
                        source_provider=PROVIDER_ALTEGIO,
                        source_company_id=COMPANY,
                        source_record_id=record_id,
                        source_fingerprint="f" * 64,
                        target_provider=PROVIDER_EASYWEEK,
                        target_booking_uuid="aaaaaaaa-0000-4000-8000-00000000000a",
                        run_id="run-1",
                        status="created",
                        post_booking_jobs_handed_over_at=datetime.now(timezone.utc) if handed_over else None,
                        post_booking_handover_plan_digest="a" * 64 if handed_over else None,
                    )
                )
            return record.id


async def _jobs(session_maker, record_pk: int) -> dict[str, str]:
    async with session_maker() as session:
        rows = (await session.execute(select(MessageJob).where(MessageJob.record_id == record_pk))).scalars()
        return {job.job_type: job.status for job in rows}


async def _plan_event(session_maker, record_pk: int, *, status: str):
    async with session_maker() as session:
        async with session.begin():
            await plan_jobs_for_record_event(
                session,
                company_id=COMPANY,
                record_id=record_pk,
                event_status=status,
            )


# ---------------------------------------------------------------------------
# fence 1: the Altegio planner
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("event", ["create", "update"])
async def test_a_late_delivery_does_not_recreate_review_or_repeat(session_maker, event):
    """The exact race the handover cannot prevent on its own."""
    record_pk = await _seed_record(session_maker, migrated=True, handed_over=True)

    await _plan_event(session_maker, record_pk, status=event)

    planned = await _jobs(session_maker, record_pk)
    assert REVIEW_3D not in planned
    assert REPEAT_10D not in planned
    # The ordinary lifecycle job of the same delivery is untouched: the fence
    # guards three job types and nothing else.
    assert any(job_type.startswith("record_") for job_type in planned), planned


async def test_a_late_delete_does_not_plan_a_comeback(session_maker):
    record_pk = await _seed_record(session_maker, migrated=True, handed_over=True)

    await _plan_event(session_maker, record_pk, status="delete")

    assert COMEBACK_3D not in await _jobs(session_maker, record_pk)


@pytest.mark.parametrize("event", ["create", "update"])
async def test_a_migrated_booking_without_the_marker_is_planned_as_before(session_maker, event):
    """Deploying the fence before the first apply changes nothing."""
    record_pk = await _seed_record(session_maker, migrated=True, handed_over=False)

    await _plan_event(session_maker, record_pk, status=event)

    planned = await _jobs(session_maker, record_pk)
    assert planned[REVIEW_3D] == "queued"
    assert planned[REPEAT_10D] == "queued"


async def test_an_unmigrated_booking_is_planned_as_before(session_maker):
    record_pk = await _seed_record(session_maker, migrated=False, handed_over=False, record_id=910002)

    await _plan_event(session_maker, record_pk, status="create")

    planned = await _jobs(session_maker, record_pk)
    assert planned[REVIEW_3D] == "queued"
    assert planned[REPEAT_10D] == "queued"
    assert planned[REMINDER_24H] == "queued", "the §30 fence is a separate question"


async def test_a_handed_over_booking_keeps_its_reminder_behaviour(session_maker):
    """Two markers, two questions. This one must not answer the other."""
    record_pk = await _seed_record(session_maker, migrated=True, handed_over=True)

    await _plan_event(session_maker, record_pk, status="create")

    planned = await _jobs(session_maker, record_pk)
    # The reminder marker was never written, so reminders are still Altegio's.
    assert planned[REMINDER_24H] == "queued"
    assert REVIEW_3D not in planned


async def test_a_resurrected_job_is_not_reopened_by_a_late_delivery(session_maker):
    """`add_job` sets a cancelled job back to `queued` on conflict.

    That is the mechanism the marker exists for: without it the withdrawal from
    the apply would be undone by the next delivery of the same booking.
    """
    record_pk = await _seed_record(session_maker, migrated=True, handed_over=False)
    await _plan_event(session_maker, record_pk, status="create")
    async with session_maker() as session:
        async with session.begin():
            rows = (
                await session.execute(
                    select(MessageJob)
                    .where(MessageJob.record_id == record_pk)
                    .where(MessageJob.job_type.in_(POST_BOOKING_JOB_TYPES))
                )
            ).scalars()
            for job in rows:
                job.status = "canceled"
                job.last_error = REASON_HANDED_OVER
            ledger = (
                (
                    await session.execute(
                        select(EasyWeekMigrationLedger).where(
                            EasyWeekMigrationLedger.source_record_id == SOURCE_RECORD_ID
                        )
                    )
                )
                .scalars()
                .one()
            )
            ledger.post_booking_jobs_handed_over_at = datetime.now(timezone.utc)
            ledger.post_booking_handover_plan_digest = "a" * 64

    await _plan_event(session_maker, record_pk, status="update")

    planned = await _jobs(session_maker, record_pk)
    assert planned[REVIEW_3D] == "canceled"
    assert planned[REPEAT_10D] == "canceled"


async def test_an_unanswerable_question_suppresses_rather_than_permits(session_maker):
    """UNKNOWN is not ALTEGIO. An identity we cannot state is not permission."""
    async with session_maker() as session:
        suppressed, owner = await altegio_post_booking_jobs_are_suppressed(
            session, company_id=COMPANY, altegio_record_id=None
        )
    assert owner is PostBookingOwner.UNKNOWN
    assert suppressed is True


# ---------------------------------------------------------------------------
# fence 2: the outbox, immediately before anything external
# ---------------------------------------------------------------------------


def test_the_outbox_fence_is_before_every_external_call():
    """Structural: the check must sit ahead of template, sender, Meta, Chatwoot.

    Placed after any of them, a job whose ownership moved would already have
    cost an Altegio API call, a rendered template or — worst — a message.
    """
    import inspect

    from altegio_bot.workers import outbox_worker

    source = inspect.getsource(outbox_worker._run_job_logic)
    fence = source.index("job.job_type in POST_BOOKING_JOB_TYPES")
    # Anchored on code, not on prose: a comment may mention the sender long
    # before the fence, and that says nothing about execution order.
    for later in ("_deadline_passed_for_send", "_send_", "template_code", "chatwoot"):
        for position, line in _code_positions(source, later):
            assert fence < position, f"the fence must precede {later!r} at: {line.strip()!r}"


def _code_positions(source: str, needle: str):
    """Every occurrence of `needle` on a line that is not a comment."""
    offset = 0
    for line in source.splitlines(keepends=True):
        if needle in line and not line.lstrip().startswith("#"):
            yield offset + line.index(needle), line
        offset += len(line)


async def test_a_claimed_job_is_canceled_without_an_attempt(session_maker, monkeypatch):
    """The second line, end to end: no external call, no attempt spent."""
    from altegio_bot.workers import outbox_worker

    record_pk = await _seed_record(session_maker, migrated=True, handed_over=True)
    async with session_maker() as session:
        async with session.begin():
            record = await session.get(Record, record_pk)
            job = MessageJob(
                provider=PROVIDER_ALTEGIO,
                company_id=COMPANY,
                record_id=record_pk,
                client_id=record.client_id,
                job_type=REVIEW_3D,
                run_at=datetime.now(timezone.utc),
                status="processing",
                dedupe_key="altegio-review-runtime",
                payload={"kind": REVIEW_3D},
                attempts=0,
            )
            session.add(job)
            await session.flush()
            job_id = job.id

    class _DummyProvider:
        """Sending is exactly what must not happen here."""

        name = "dummy"

        async def send_template(self, *args, **kwargs):  # pragma: no cover - must never run
            raise AssertionError("the fence let a handed-over job reach the provider")

        async def send_text(self, *args, **kwargs):  # pragma: no cover - must never run
            raise AssertionError("the fence let a handed-over job reach the provider")

    async with session_maker() as session:
        async with session.begin():
            await outbox_worker.process_job_in_session(
                session=session,
                job_id=job_id,
                provider=_DummyProvider(),
            )

    async with session_maker() as session:
        job = await session.get(MessageJob, job_id)
        outbox_rows = (
            (await session.execute(select(OutboxMessage).where(OutboxMessage.job_id == job_id))).scalars().all()
        )
    assert job.status == "canceled"
    assert job.last_error == REASON_HANDED_OVER
    assert job.attempts == 0, "nothing was attempted, so no attempt is spent"
    assert job.locked_at is None
    assert outbox_rows == [], "no OutboxMessage is written for a job that was never sent"


async def test_an_easyweek_job_of_the_same_type_is_never_suppressed(session_maker, monkeypatch):
    """The EasyWeek `review_3d` is what a proven outcome created. It stays."""
    async with session_maker() as session:
        suppressed, owner = await altegio_post_booking_jobs_are_suppressed(
            session,
            company_id=COMPANY,
            altegio_record_id=SOURCE_RECORD_ID,
        )
    # No ledger row at all: the ordinary answer, and the EasyWeek side is never
    # even asked — the fence is keyed on `provider == altegio` in the worker.
    assert owner is PostBookingOwner.ALTEGIO
    assert suppressed is False

    from altegio_bot.workers import outbox_worker

    source = inspect_source(outbox_worker)
    assert "job_provider == PROVIDER_ALTEGIO and job.job_type in POST_BOOKING_JOB_TYPES" in source


def inspect_source(module) -> str:
    import inspect

    return inspect.getsource(module)


def test_the_two_fences_do_not_share_their_job_types():
    """§30 owns the reminders, PR-12.1 owns the marketing follow-ups."""
    from altegio_bot.reminder_ownership import HANDOVER_JOB_TYPES

    assert POST_BOOKING_JOB_TYPES == frozenset({"review_3d", "repeat_10d", "comeback_3d"})
    assert not (POST_BOOKING_JOB_TYPES & HANDOVER_JOB_TYPES)


def test_the_two_fences_do_not_share_their_retry_counter():
    from altegio_bot.workers import outbox_worker

    assert outbox_worker._POST_BOOKING_OWNERSHIP_ATTEMPTS_KEY != outbox_worker._REMINDER_OWNERSHIP_ATTEMPTS_KEY


def test_the_reasons_are_stable_and_pii_free():
    from altegio_bot.post_booking_ownership import REASON_UNKNOWN

    for reason in (REASON_HANDED_OVER, REASON_UNKNOWN):
        assert "easyweek" in reason.lower() or "ownership" in reason.lower()
        for leaked in ("+49", "@", "http"):
            assert leaked not in reason


# ---------------------------------------------------------------------------
# the runbook is part of the safety, so it is pinned like code
# ---------------------------------------------------------------------------


def _runbook() -> str:
    from pathlib import Path

    return (Path(__file__).resolve().parents[3] / "docs/easyweek/post_booking_handover_runbook.md").read_text(
        encoding="utf-8"
    )


def test_the_runbook_restores_the_outbox_on_every_exit():
    """A stopped outbox worker is an outage, so the restore cannot be manual.

    The trap has to cover the ordinary exit AND the ways an operator actually
    leaves a terminal: Ctrl-C, a kill, a dropped SSH session.
    """
    body = _runbook()
    trap = [line for line in body.splitlines() if line.strip().startswith("trap ")]
    assert trap, "the apply step must arm a restore trap"
    for signal in ("EXIT", "INT", "TERM", "HUP"):
        assert signal in trap[0], f"the trap must cover {signal}"
    assert "start altegio-outbox-worker" in trap[0]
    # And an independent check afterwards, because a trap that did not fire is
    # exactly the case a trap cannot report.
    assert "ps altegio-outbox-worker" in body


def test_the_runbook_documents_the_whole_operator_flow():
    body = _runbook()
    for fragment in (
        "plan",
        "apply",
        "verify",
        "EASYWEEK_POST_BOOKING_HANDOVER_ALLOW_APPLY",
        "--plan-digest",
        "--confirm",
        "snapshot_invalidated",
        "source_job_processing",
        "source_job_outbox_non_terminal",
        "reminder_handover_incomplete",
        "TRANSACTION READ ONLY",
    ):
        assert fragment in body, fragment
    # Deferred work is named rather than quietly implied.
    for deferred in ("newsletter", "promo", "campaign"):
        assert deferred in body.lower(), deferred


def test_the_compose_service_grants_no_standing_write_permission():
    from pathlib import Path

    import yaml

    compose = yaml.safe_load((Path(__file__).resolve().parents[3] / "docker-compose.yml").read_text())
    service = compose["services"]["easyweek-migration-post-booking-handover"]

    assert service["profiles"] == ["ops"], "never part of the default stack"
    assert service["restart"] == "no"
    assert "altegio_bot.scripts.easyweek_post_booking_handover" in service["entrypoint"]
    # The permission is passed on the one command that writes, never baked in.
    body = yaml.safe_dump(service)
    assert "EASYWEEK_POST_BOOKING_HANDOVER_ALLOW_APPLY" not in body


# ---------------------------------------------------------------------------
# Review fix: an unanswerable question must not consume the obligation
# ---------------------------------------------------------------------------


async def test_an_unprovable_lookup_fails_the_delivery_rather_than_dropping_it(session_maker, monkeypatch):
    """Suppressing on UNKNOWN is right for a send and wrong for planning.

    The planner runs once per delivery and its caller acks the event, so a
    transient lookup failure that merely skipped would lose the follow-up for
    good — a client silently never asked for a review, with one log line as the
    only trace. Raising leaves the event visible and re-drivable, and still
    creates nothing.
    """
    from altegio_bot.post_booking_ownership import PostBookingOwner, PostBookingOwnershipUnproven

    record_pk = await _seed_record(session_maker, migrated=True, handed_over=False)

    async def unanswerable(*args, **kwargs):
        return True, PostBookingOwner.UNKNOWN

    monkeypatch.setattr(planner_mod, "altegio_post_booking_jobs_are_suppressed", unanswerable)

    with pytest.raises(PostBookingOwnershipUnproven):
        await _plan_event(session_maker, record_pk, status="create")

    # Nothing was created for the three types, and nothing was silently skipped.
    planned = await _jobs(session_maker, record_pk)
    assert REVIEW_3D not in planned
    assert REPEAT_10D not in planned


def test_the_unproven_error_carries_no_pii():
    from altegio_bot.post_booking_ownership import REASON_UNKNOWN, PostBookingOwnershipUnproven

    message = str(PostBookingOwnershipUnproven())
    assert message == REASON_UNKNOWN
    for leaked in ("+49", "@", "http"):
        assert leaked not in message
