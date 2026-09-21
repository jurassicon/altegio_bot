"""Operator-only recovery of a terminally failed `booking-canceled` delivery.

The production state these tests reproduce: a real `booking-canceled` webhook
whose `service_id` was a literal JSON null, rejected by the normalizer of the
day as `invalid_payload`, so the booking is cancelled in EasyWeek and still
active locally. The widened normalizer fixes the next such delivery; only this
recovery may finish the row that already failed.

Everything here runs against the project's real PostgreSQL. `FOR UPDATE`, the
partial unique index on the EasyWeek booking uuid and the all-or-nothing
transaction are the whole subject, and SQLite would answer none of them.
"""

from __future__ import annotations

import copy
import json
import re
import shlex
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from altegio_bot.easyweek_client import EasyWeekAuthError, EasyWeekNotFoundError, EasyWeekRetryableError
from altegio_bot.easyweek_failed_cancellation_recovery import (
    BLOCKER_API_MALFORMED,
    BLOCKER_API_NOT_FOUND,
    BLOCKER_API_UNAUTHORIZED,
    BLOCKER_API_UNAVAILABLE,
    BLOCKER_BODY_TRUNCATED,
    BLOCKER_ERROR_CODE_UNEXPECTED,
    BLOCKER_EVENT_NOT_FOUND,
    BLOCKER_HINT_NOT_CANCELLATION,
    BLOCKER_LATER_EVENT,
    BLOCKER_LIVE_NOT_CANCELED,
    BLOCKER_LIVE_START_DRIFT,
    BLOCKER_RECORD_COMPANY_MISMATCH,
    BLOCKER_RECORD_MISSING,
    BLOCKER_RECORD_NUMERIC_MISMATCH,
    BLOCKER_REMINDER_PROCESSING,
    BLOCKER_SERVICE_ID_NOT_NULL,
    BLOCKER_STATUS_NOT_FAILED,
    DISPOSITION_RECOVER,
    MAX_EVENT_IDS,
    OUTCOME_ALREADY_APPLIED,
    OUTCOME_APPLIED,
    RECOVERY_CANCEL_REASON,
    RecoveryError,
    apply_recovery_plan,
    build_recovery_plan,
    check_apply_authorization,
    confirmation_phrase,
    read_apply_report,
    read_plan,
    validate_event_ids,
    verify_recovery,
    write_plan,
)
from altegio_bot.easyweek_multi_service_recovery import write_private_json
from altegio_bot.easyweek_normalizer import canonical_booking_uuid
from altegio_bot.easyweek_policy import REMINDER_2H, REMINDER_24H
from altegio_bot.easyweek_reminders import easyweek_reminder_dedupe_key
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    Client,
    EasyWeekEvent,
    MessageJob,
    OutboxMessage,
    Record,
    RecordService,
)
from altegio_bot.scripts import easyweek_failed_cancellation_recovery as cli
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import (
    TEST_BOOKING_HASH_ID,
    TEST_BOOKING_ID,
    TEST_BOOKING_PAGE,
    TEST_BOOKING_UUID,
    TEST_CUSTOMER_ID,
    TEST_LOCATION_ID,
    TEST_LOCATION_UUID,
    booking_canceled,
)

pytestmark = pytest.mark.asyncio

NOW = datetime(2026, 9, 21, 9, 0, tzinfo=timezone.utc)
# The booking is in the past: this is a historical cancellation, which is
# exactly why no reminder may be planned for it and why the ordinary lifecycle
# path would have been the wrong tool.
STARTS_AT = NOW - timedelta(days=3)
SERVICE_ID = 5100003
OTHER_LOCATION_ID = 888003
OTHER_LOCATION_UUID = "cccccccc-bbbb-4ccc-8ddd-eeeeeeeeeeee"


@pytest.fixture(autouse=True)
def _configuration(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "test-branch": {
                    "location_id": TEST_LOCATION_ID,
                    "location_uuid": TEST_LOCATION_UUID,
                    "meta_template_prefix": "tb",
                    "booking_page_url": "https://booking.example.invalid/test",
                },
                "other-branch": {
                    "location_id": OTHER_LOCATION_ID,
                    "location_uuid": OTHER_LOCATION_UUID,
                    "meta_template_prefix": "ob",
                    "booking_page_url": "https://booking.example.invalid/other",
                },
            }
        ),
        raising=False,
    )
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", json.dumps(["Fixture Category"]))


# ---------------------------------------------------------------------------
# fixtures shaped like the confirmed production row
# ---------------------------------------------------------------------------


def null_service_payload(
    *,
    booking_uuid: str = TEST_BOOKING_UUID,
    booking_id: int = TEST_BOOKING_ID,
    location_id: int = TEST_LOCATION_ID,
    location_uuid: str = TEST_LOCATION_UUID,
    starts_at: datetime = STARTS_AT,
    service_id: object = None,
) -> dict[str, Any]:
    """The confirmed shape: a cancellation whose `service_id` is a JSON null."""
    payload = booking_canceled()
    payload["uid"] = booking_uuid
    payload["id"] = booking_id
    payload["location_id"] = location_id
    payload["location_uuid"] = location_uuid
    payload["service_id"] = service_id
    payload["booking_date_start"] = starts_at.strftime("%Y-%m-%dT%H:%M:%S+0000")
    payload["booking_date_end"] = (starts_at + timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%S+0000")
    return payload


def api_body(
    *,
    booking_uuid: str = TEST_BOOKING_UUID,
    location_uuid: str = TEST_LOCATION_UUID,
    starts_at: datetime = STARTS_AT,
    canceled: bool = True,
    completed: bool = False,
    status_type: str | None = "CANCELED",
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "uuid": booking_uuid,
        "location_uuid": location_uuid,
        "start_time": starts_at.isoformat().replace("+00:00", "Z"),
        "is_canceled": canceled,
        "is_completed": completed,
    }
    if status_type is not None:
        body["status"] = {"type": status_type}
    return body


class FakeReader:
    """The single GET-only endpoint this recovery is allowed to use."""

    def __init__(self, answer: Any = None, *, answers: dict[str, Any] | None = None) -> None:
        self.answer = answer if answer is not None else api_body()
        self.answers = answers
        self.calls: list[str] = []

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        self.calls.append(booking_uuid)
        answer = self.answers[booking_uuid] if self.answers is not None else self.answer
        if isinstance(answer, Exception):
            raise answer
        return copy.deepcopy(answer)

    async def aclose(self) -> None:
        return None


async def _no_sleep(_seconds: float) -> None:
    """Tests must not spend wall-clock time on the API rate limiter."""


async def seed_failed_cancellation(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    payload: dict[str, Any] | None = None,
    status: str = "failed",
    error_code: str = "invalid_payload",
    event_hint: str = "booking-canceled",
    body_truncated: bool = False,
    record_is_deleted: bool = False,
    record_company_id: int | None = None,
    record_booking_id: int | None = None,
    with_record: bool = True,
    queued_reminders: tuple[str, ...] = (REMINDER_24H, REMINDER_2H),
    keep_body_raw: bool = True,
) -> dict[str, Any]:
    body = payload if payload is not None else null_service_payload()
    async with session_maker() as session:
        async with session.begin():
            client = await session.get(Client, 1)
            client.provider = PROVIDER_EASYWEEK
            client.company_id = TEST_LOCATION_ID
            client.altegio_client_id = TEST_CUSTOMER_ID

            record_pk: int | None = None
            if with_record:
                record = Record(
                    provider=PROVIDER_EASYWEEK,
                    company_id=record_company_id or TEST_LOCATION_ID,
                    altegio_record_id=record_booking_id or int(body["id"]),
                    easyweek_booking_uuid=uuid.UUID(str(body["uid"])),
                    easyweek_booking_hash_id=TEST_BOOKING_HASH_ID,
                    short_link=TEST_BOOKING_PAGE,
                    client_id=1,
                    altegio_client_id=TEST_CUSTOMER_ID,
                    starts_at=STARTS_AT,
                    ends_at=STARTS_AT + timedelta(hours=1),
                    duration_sec=3600,
                    staff_name="Fixture Specialist",
                    total_cost=Decimal("35.00"),
                    is_deleted=record_is_deleted,
                    raw={"easyweek": {"service_category": "Fixture Category", "services_count": 1}},
                )
                session.add(record)
                await session.flush()
                record_pk = record.id
                session.add(
                    RecordService(
                        record_id=record_pk,
                        service_id=SERVICE_ID,
                        title="Fixture Service",
                        amount=1,
                        cost_to_pay=Decimal("35.00"),
                    )
                )
                for job_type in queued_reminders:
                    session.add(
                        MessageJob(
                            provider=PROVIDER_EASYWEEK,
                            company_id=TEST_LOCATION_ID,
                            record_id=record_pk,
                            client_id=1,
                            job_type=job_type,
                            run_at=STARTS_AT - timedelta(hours=24 if job_type == REMINDER_24H else 2),
                            status="queued",
                            dedupe_key=easyweek_reminder_dedupe_key(
                                booking_uuid=uuid.UUID(str(body["uid"])),
                                job_type=job_type,
                                starts_at=STARTS_AT,
                            ),
                            payload={},
                        )
                    )

            event = EasyWeekEvent(
                status=status,
                event_hint=event_hint,
                auth_via="query",
                payload_hash="p" * 64,
                payload=body,
                body_raw=json.dumps(body).encode("utf-8") if keep_body_raw else None,
                body_size_bytes=len(json.dumps(body)),
                body_truncated=body_truncated,
                booking_uuid=canonical_booking_uuid(body),
                received_at=NOW - timedelta(days=2),
                processed_at=NOW - timedelta(days=2),
                error_code=error_code,
            )
            session.add(event)
            await session.flush()
            return {"event_id": event.id, "record_id": record_pk, "payload": body}


async def build(
    session_maker: async_sessionmaker[AsyncSession],
    *,
    event_ids: list[int],
    reader: FakeReader | None = None,
):
    async with session_maker() as session:
        return await build_recovery_plan(
            session,
            event_ids=event_ids,
            client=reader or FakeReader(),
            now=NOW,
            sleep=_no_sleep,
        )


async def freeze(session_maker, tmp_path: Path, **kwargs):
    plan = await build(session_maker, **kwargs)
    path = write_plan(plan, tmp_path / "plan.json")
    return plan, read_plan(path), path


async def run_apply(session_maker, frozen, *, reader: FakeReader | None = None, now: datetime = NOW):
    async with session_maker() as session:
        return await apply_recovery_plan(
            session,
            frozen=frozen,
            client=reader or FakeReader(),
            now=now,
            sleep=_no_sleep,
        )


async def table_snapshot(session_maker) -> dict[str, Any]:
    """Every row of the four tables an unintended write could land in."""
    async with session_maker() as session:
        jobs = [
            (j.id, j.provider, j.company_id, j.record_id, j.job_type, j.status, j.run_at, j.dedupe_key, j.last_error)
            for j in (await session.execute(select(MessageJob).order_by(MessageJob.id))).scalars()
        ]
        events = [
            (e.id, e.status, e.error_code, e.next_retry_at, e.processed_at)
            for e in (await session.execute(select(EasyWeekEvent).order_by(EasyWeekEvent.id))).scalars()
        ]
        records = [
            (r.id, r.provider, r.company_id, r.is_deleted, r.starts_at, r.total_cost, r.staff_name, r.comment, r.raw)
            for r in (await session.execute(select(Record).order_by(Record.id))).scalars()
        ]
        services = [
            (s.record_id, s.service_id, s.title, s.amount, s.cost_to_pay)
            for s in (await session.execute(select(RecordService).order_by(RecordService.record_id))).scalars()
        ]
        outbox = [o.id for o in (await session.execute(select(OutboxMessage).order_by(OutboxMessage.id))).scalars()]
    return {"jobs": jobs, "events": events, "records": records, "services": services, "outbox": outbox}


# ===========================================================================
# plan: the admission test
# ===========================================================================


async def test_the_confirmed_production_shape_is_recoverable(session_maker) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    reader = FakeReader()

    plan = await build(session_maker, event_ids=[seeded["event_id"]], reader=reader)

    assert plan.apply_ready is True
    assert [row["disposition"] for row in plan.events] == [DISPOSITION_RECOVER]
    row = plan.events[0]
    assert row["blockers"] == []
    assert row["record_id"] == seeded["record_id"]
    assert row["booking_uuid"] == TEST_BOOKING_UUID
    assert row["record_is_deleted"] is False
    assert len(row["queued_reminder_job_ids"]) == 2
    assert row["live"] == {
        "starts_at": STARTS_AT.isoformat().replace("+00:00", "Z"),
        "is_canceled": True,
        "is_completed": False,
        "status_type": "canceled",
    }
    assert reader.calls == [TEST_BOOKING_UUID]


async def test_plan_writes_nothing_at_all(session_maker) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    before = await table_snapshot(session_maker)

    await build(session_maker, event_ids=[seeded["event_id"]])

    assert await table_snapshot(session_maker) == before


async def test_plan_rolls_back_even_when_the_orm_was_left_dirty(session_maker) -> None:
    """A plan that flushed an accidental attribute change would be a write."""
    seeded = await seed_failed_cancellation(session_maker)
    before = await table_snapshot(session_maker)

    async with session_maker() as session:
        record = await session.get(Record, seeded["record_id"])
        record.comment = "an accidental in-memory edit"
        plan = await build_recovery_plan(
            session,
            event_ids=[seeded["event_id"]],
            client=FakeReader(),
            now=NOW,
            sleep=_no_sleep,
        )
        assert plan.apply_ready is True

    assert await table_snapshot(session_maker) == before


async def test_only_the_explicitly_named_event_is_considered(session_maker) -> None:
    """Four sibling failures exist in production; naming one must mean one."""
    target = await seed_failed_cancellation(session_maker)
    other = await seed_failed_cancellation(
        session_maker,
        payload=null_service_payload(
            booking_uuid="22222222-2222-4333-8444-555555555555",
            booking_id=TEST_BOOKING_ID + 1,
            service_id=SERVICE_ID,
        ),
    )
    reader = FakeReader()

    plan = await build(session_maker, event_ids=[target["event_id"]], reader=reader)

    assert plan.requested_event_ids == (target["event_id"],)
    assert [row["event_id"] for row in plan.events] == [target["event_id"]]
    assert other["event_id"] not in [row["event_id"] for row in plan.events]
    assert reader.calls == [TEST_BOOKING_UUID], "a row nobody named must not even cost an API call"


async def test_there_is_no_way_to_ask_for_every_failed_event() -> None:
    parser = cli.build_parser()
    options = {action.option_strings[0] for action in parser._actions if action.option_strings}
    assert "--all" not in options
    assert "--event-id" in options
    with pytest.raises(SystemExit):
        parser.parse_args(["plan"])  # --event-id is required


async def test_event_ids_are_explicit_unique_and_bounded() -> None:
    assert validate_event_ids([360, 360]) == (360,)
    with pytest.raises(RecoveryError):
        validate_event_ids([])
    with pytest.raises(RecoveryError):
        validate_event_ids([0])
    with pytest.raises(RecoveryError):
        validate_event_ids([-1])
    with pytest.raises(RecoveryError):
        validate_event_ids(list(range(1, MAX_EVENT_IDS + 2)))


async def test_an_unknown_event_id_is_refused(session_maker) -> None:
    plan = await build(session_maker, event_ids=[999999])
    assert plan.apply_ready is False
    assert plan.events[0]["blockers"] == [BLOCKER_EVENT_NOT_FOUND]


@pytest.mark.parametrize(
    ("kwargs", "blocker"),
    [
        ({"status": "processed"}, BLOCKER_STATUS_NOT_FAILED),
        ({"status": "captured"}, BLOCKER_STATUS_NOT_FAILED),
        ({"error_code": "truncated_payload"}, BLOCKER_ERROR_CODE_UNEXPECTED),
        ({"error_code": "identity_conflict"}, BLOCKER_ERROR_CODE_UNEXPECTED),
        ({"event_hint": "booking-updated"}, BLOCKER_HINT_NOT_CANCELLATION),
        ({"event_hint": "booking-created"}, BLOCKER_HINT_NOT_CANCELLATION),
        ({"body_truncated": True}, BLOCKER_BODY_TRUNCATED),
    ],
)
async def test_every_wrong_event_shape_is_refused(session_maker, kwargs: dict[str, Any], blocker: str) -> None:
    seeded = await seed_failed_cancellation(session_maker, **kwargs)
    plan = await build(session_maker, event_ids=[seeded["event_id"]])

    assert plan.apply_ready is False
    assert blocker in plan.events[0]["blockers"]


@pytest.mark.parametrize("service_id", [SERVICE_ID, 0, -1, "null", True, 1.5])
async def test_a_payload_whose_service_id_is_not_a_literal_null_is_refused(session_maker, service_id: object) -> None:
    seeded = await seed_failed_cancellation(session_maker, payload=null_service_payload(service_id=service_id))
    plan = await build(session_maker, event_ids=[seeded["event_id"]])

    assert plan.apply_ready is False
    assert BLOCKER_SERVICE_ID_NOT_NULL in plan.events[0]["blockers"]


async def test_a_payload_without_the_service_id_key_at_all_is_refused(session_maker) -> None:
    """Absent is not null: the proven production shape carries the key."""
    payload = null_service_payload()
    del payload["service_id"]
    seeded = await seed_failed_cancellation(session_maker, payload=payload)

    plan = await build(session_maker, event_ids=[seeded["event_id"]])
    assert BLOCKER_SERVICE_ID_NOT_NULL in plan.events[0]["blockers"]


async def test_captured_bytes_that_disagree_with_the_parsed_payload_are_refused(session_maker) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    async with session_maker() as session:
        async with session.begin():
            event = await session.get(EasyWeekEvent, seeded["event_id"])
            rewritten = dict(seeded["payload"])
            rewritten["service_id"] = SERVICE_ID
            event.body_raw = json.dumps(rewritten).encode("utf-8")

    plan = await build(session_maker, event_ids=[seeded["event_id"]])
    assert plan.apply_ready is False
    assert "captured_body_disagrees_with_payload" in plan.events[0]["blockers"]


async def test_a_missing_local_record_is_refused(session_maker) -> None:
    seeded = await seed_failed_cancellation(session_maker, with_record=False)
    plan = await build(session_maker, event_ids=[seeded["event_id"]])

    assert plan.apply_ready is False
    assert plan.events[0]["blockers"] == [BLOCKER_RECORD_MISSING]


async def test_a_company_mismatch_is_refused(session_maker) -> None:
    seeded = await seed_failed_cancellation(session_maker, record_company_id=OTHER_LOCATION_ID)
    plan = await build(session_maker, event_ids=[seeded["event_id"]])

    assert plan.apply_ready is False
    assert BLOCKER_RECORD_COMPANY_MISMATCH in plan.events[0]["blockers"]


async def test_a_numeric_booking_id_mismatch_is_refused(session_maker) -> None:
    seeded = await seed_failed_cancellation(session_maker, record_booking_id=TEST_BOOKING_ID + 7)
    plan = await build(session_maker, event_ids=[seeded["event_id"]])

    assert plan.apply_ready is False
    assert BLOCKER_RECORD_NUMERIC_MISMATCH in plan.events[0]["blockers"]


async def test_a_uuid_mismatch_finds_no_record_and_is_refused(session_maker) -> None:
    """Identity is UUID-first: a different uid is a different booking, full stop."""
    seeded = await seed_failed_cancellation(session_maker)
    async with session_maker() as session:
        async with session.begin():
            record = await session.get(Record, seeded["record_id"])
            record.easyweek_booking_uuid = uuid.UUID("33333333-2222-4333-8444-555555555555")

    plan = await build(session_maker, event_ids=[seeded["event_id"]])
    assert plan.apply_ready is False
    assert plan.events[0]["blockers"] == [BLOCKER_RECORD_MISSING]


async def test_a_later_event_for_the_same_booking_is_refused(session_maker) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                EasyWeekEvent(
                    status="processed",
                    event_hint="booking-rescheduled",
                    payload=seeded["payload"],
                    booking_uuid=uuid.UUID(TEST_BOOKING_UUID),
                    received_at=NOW - timedelta(hours=1),
                )
            )

    plan = await build(session_maker, event_ids=[seeded["event_id"]])
    assert plan.apply_ready is False
    assert BLOCKER_LATER_EVENT in plan.events[0]["blockers"]


async def test_an_earlier_event_for_the_same_booking_is_not_a_blocker(session_maker) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                EasyWeekEvent(
                    status="processed",
                    event_hint="booking-created",
                    payload=seeded["payload"],
                    booking_uuid=uuid.UUID(TEST_BOOKING_UUID),
                    received_at=NOW - timedelta(days=5),
                )
            )

    plan = await build(session_maker, event_ids=[seeded["event_id"]])
    assert plan.apply_ready is True


@pytest.mark.parametrize(
    ("answer", "blocker"),
    [
        (api_body(canceled=False, status_type="ACTIVE"), BLOCKER_LIVE_NOT_CANCELED),
        (api_body(canceled=False, completed=True, status_type="SUCCESSFUL"), BLOCKER_LIVE_NOT_CANCELED),
        (api_body(canceled=True, completed=True, status_type="CANCELED"), BLOCKER_API_MALFORMED),
        (api_body(canceled=True, status_type="SUCCESSFUL"), BLOCKER_API_MALFORMED),
        (api_body(canceled=True, status_type=None), BLOCKER_API_MALFORMED),
        (api_body(status_type="CANCELED", starts_at=STARTS_AT + timedelta(hours=2)), BLOCKER_LIVE_START_DRIFT),
        (api_body(location_uuid=OTHER_LOCATION_UUID), "live_identity_mismatch"),
        (api_body(booking_uuid="44444444-2222-4333-8444-555555555555"), "live_identity_mismatch"),
        ({"uuid": TEST_BOOKING_UUID}, BLOCKER_API_MALFORMED),
        ("not an object", BLOCKER_API_MALFORMED),
        (EasyWeekNotFoundError("gone"), BLOCKER_API_NOT_FOUND),
        (EasyWeekAuthError("nope"), BLOCKER_API_UNAUTHORIZED),
        (EasyWeekRetryableError("later"), BLOCKER_API_UNAVAILABLE),
    ],
)
async def test_every_contradictory_or_unreadable_live_state_is_refused(
    session_maker, answer: Any, blocker: str
) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    plan = await build(session_maker, event_ids=[seeded["event_id"]], reader=FakeReader(answer))

    assert plan.apply_ready is False
    assert blocker in plan.events[0]["blockers"]


async def test_a_processing_reminder_blocks_the_plan(session_maker) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    async with session_maker() as session:
        async with session.begin():
            job = (await session.execute(select(MessageJob).where(MessageJob.job_type == REMINDER_2H))).scalars().one()
            job.status = "processing"

    plan = await build(session_maker, event_ids=[seeded["event_id"]])
    assert plan.apply_ready is False
    assert BLOCKER_REMINDER_PROCESSING in plan.events[0]["blockers"]


async def test_a_plan_holds_no_names_phones_emails_or_payload(session_maker, tmp_path: Path) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    _plan, _frozen, path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])

    text = path.read_text(encoding="utf-8")
    for secret in (
        "Test Person",
        "+49000000000",
        "test.person@example.invalid",
        "fixture comment",
        "Fixture Specialist",
        "Fixture Service",
    ):
        assert secret not in text, f"{secret!r} must never reach the plan file"
    assert "booking_status" not in text and "customer_phone" not in text


async def test_a_plan_file_is_private(session_maker, tmp_path: Path) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    _plan, _frozen, path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])

    assert path.stat().st_mode & 0o777 == 0o600
    assert path.parent.stat().st_mode & 0o777 == 0o700


# ===========================================================================
# apply authorization
# ===========================================================================


async def test_apply_needs_the_digest_the_phrase_and_a_fresh_plan(session_maker, tmp_path: Path) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])
    phrase = confirmation_phrase(frozen.digest)

    with pytest.raises(RecoveryError, match="plan_digest_mismatch"):
        check_apply_authorization(frozen, supplied_digest=None, supplied_confirmation=phrase, now=NOW)
    with pytest.raises(RecoveryError, match="plan_digest_mismatch"):
        check_apply_authorization(frozen, supplied_digest="0" * 64, supplied_confirmation=phrase, now=NOW)
    with pytest.raises(RecoveryError, match="confirmation_mismatch"):
        check_apply_authorization(frozen, supplied_digest=frozen.digest, supplied_confirmation=None, now=NOW)
    with pytest.raises(RecoveryError, match="confirmation_mismatch"):
        check_apply_authorization(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation="recover easyweek failed cancellation",
            now=NOW,
        )
    with pytest.raises(RecoveryError, match="plan_expired"):
        check_apply_authorization(
            frozen, supplied_digest=frozen.digest, supplied_confirmation=phrase, now=NOW + timedelta(hours=2)
        )
    check_apply_authorization(frozen, supplied_digest=frozen.digest, supplied_confirmation=phrase, now=NOW)


async def test_a_changed_plan_file_is_refused(session_maker, tmp_path: Path) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    _plan, _frozen, path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])

    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["events"][0]["record_is_deleted"] = True
    write_private_json(payload, path)

    with pytest.raises(RecoveryError, match="plan_digest_mismatch"):
        read_plan(path)


async def test_a_configuration_change_invalidates_the_plan(
    session_maker, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])

    monkeypatch.setattr(
        settings,
        "easyweek_location_map",
        json.dumps(
            {
                "test-branch": {
                    "location_id": TEST_LOCATION_ID,
                    "location_uuid": TEST_LOCATION_UUID,
                    "meta_template_prefix": "tb",
                    "booking_page_url": "https://booking.example.invalid/test",
                }
            }
        ),
        raising=False,
    )
    with pytest.raises(RecoveryError, match="configuration_digest_changed"):
        check_apply_authorization(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation=confirmation_phrase(frozen.digest),
            now=NOW,
        )


async def test_a_blocked_plan_can_never_be_applied(session_maker, tmp_path: Path) -> None:
    seeded = await seed_failed_cancellation(session_maker, status="processed")
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])

    with pytest.raises(RecoveryError, match="plan_not_apply_ready"):
        check_apply_authorization(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation=confirmation_phrase(frozen.digest),
            now=NOW,
        )
    with pytest.raises(RecoveryError, match="plan_not_apply_ready"):
        await run_apply(session_maker, frozen)


async def test_the_cli_needs_both_the_flag_and_the_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    args = cli.build_parser().parse_args(["apply", "--event-id", "360", "--apply"])
    monkeypatch.delenv(cli.APPLY_ENV_FLAG, raising=False)
    assert cli._apply_permitted(args) is False
    monkeypatch.setenv(cli.APPLY_ENV_FLAG, "true")
    assert cli._apply_permitted(args) is True
    without_flag = cli.build_parser().parse_args(["apply", "--event-id", "360"])
    assert cli._apply_permitted(without_flag) is False


# ===========================================================================
# apply: the silent historical cancellation
# ===========================================================================


async def test_apply_performs_only_the_silent_cancellation(session_maker, tmp_path: Path) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])
    before = await table_snapshot(session_maker)

    result = await run_apply(session_maker, frozen)

    assert result.outcome == OUTCOME_APPLIED
    assert len(result.canceled_reminder_job_ids) == 2

    async with session_maker() as session:
        event = await session.get(EasyWeekEvent, seeded["event_id"])
        record = await session.get(Record, seeded["record_id"])
        jobs = list((await session.execute(select(MessageJob).order_by(MessageJob.id))).scalars())
        services = list((await session.execute(select(RecordService))).scalars())

    assert event.status == "processed"
    assert event.error_code is None
    assert event.next_retry_at is None
    assert record.is_deleted is True
    assert {job.status for job in jobs} == {"canceled"}
    assert {job.last_error for job in jobs} == {RECOVERY_CANCEL_REASON}
    assert all(job.locked_at is None for job in jobs)

    # The historical booking itself is untouched apart from the cancellation.
    assert record.starts_at == before["records"][0][4]
    assert record.total_cost == Decimal("35.00")
    assert record.staff_name == "Fixture Specialist"
    assert [(s.service_id, s.title, s.cost_to_pay) for s in services] == [
        (SERVICE_ID, "Fixture Service", Decimal("35.00"))
    ]


async def test_apply_creates_no_lifecycle_comeback_review_retention_or_outbox_row(
    session_maker, tmp_path: Path
) -> None:
    """The exact reason a naive replay through apply_booking was unacceptable."""
    seeded = await seed_failed_cancellation(session_maker, queued_reminders=())
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])

    await run_apply(session_maker, frozen)

    async with session_maker() as session:
        job_types = set((await session.execute(select(MessageJob.job_type))).scalars())
        outbox = (await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one()
    assert job_types == set(), "a historical recovery must not plan a single job"
    assert outbox == 0


async def test_apply_leaves_every_non_target_row_byte_for_byte_unchanged(session_maker, tmp_path: Path) -> None:
    target = await seed_failed_cancellation(session_maker)
    bystander = await seed_failed_cancellation(
        session_maker,
        payload=null_service_payload(
            booking_uuid="55555555-2222-4333-8444-555555555555",
            booking_id=TEST_BOOKING_ID + 2,
        ),
    )
    async with session_maker() as session:
        async with session.begin():
            session.add(
                MessageJob(
                    provider=PROVIDER_ALTEGIO,
                    company_id=758285,
                    record_id=bystander["record_id"],
                    job_type=REMINDER_24H,
                    run_at=NOW + timedelta(hours=10),
                    status="queued",
                    dedupe_key="altegio-untouched",
                    payload={},
                )
            )
    before = await table_snapshot(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[target["event_id"]])

    await run_apply(session_maker, frozen)
    after = await table_snapshot(session_maker)

    touched_records = {target["record_id"]}
    assert [row for row in after["records"] if row[0] not in touched_records] == [
        row for row in before["records"] if row[0] not in touched_records
    ]
    assert [row for row in after["events"] if row[0] != target["event_id"]] == [
        row for row in before["events"] if row[0] != target["event_id"]
    ]
    target_job_ids = {row[0] for row in before["jobs"] if row[3] == target["record_id"]}
    assert [row for row in after["jobs"] if row[0] not in target_job_ids] == [
        row for row in before["jobs"] if row[0] not in target_job_ids
    ]
    assert after["services"] == before["services"]
    assert after["outbox"] == before["outbox"]


async def test_apply_refuses_when_a_scoped_reminder_started_processing(session_maker, tmp_path: Path) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])
    async with session_maker() as session:
        async with session.begin():
            job = (await session.execute(select(MessageJob).where(MessageJob.job_type == REMINDER_2H))).scalars().one()
            job.status = "processing"
    before = await table_snapshot(session_maker)

    with pytest.raises(RecoveryError, match=BLOCKER_REMINDER_PROCESSING):
        await run_apply(session_maker, frozen)

    assert await table_snapshot(session_maker) == before, "a halted apply must change nothing"


@pytest.mark.parametrize(
    "answer",
    [
        api_body(canceled=False, status_type="ACTIVE"),
        api_body(starts_at=STARTS_AT + timedelta(hours=1)),
        EasyWeekRetryableError("later"),
    ],
)
async def test_apply_reproves_the_live_state_and_rolls_back(session_maker, tmp_path: Path, answer: Any) -> None:
    """The plan's proof may be minutes old; the world may have moved since."""
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])
    before = await table_snapshot(session_maker)

    with pytest.raises(RecoveryError):
        await run_apply(session_maker, frozen, reader=FakeReader(answer))

    assert await table_snapshot(session_maker) == before


@pytest.mark.parametrize("drift", ["record", "job", "event", "service"])
async def test_apply_refuses_any_local_drift_since_the_plan(session_maker, tmp_path: Path, drift: str) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])

    async with session_maker() as session:
        async with session.begin():
            if drift == "record":
                record = await session.get(Record, seeded["record_id"])
                record.starts_at = STARTS_AT + timedelta(hours=3)
            elif drift == "job":
                job = (
                    (await session.execute(select(MessageJob).where(MessageJob.job_type == REMINDER_2H)))
                    .scalars()
                    .one()
                )
                job.status = "done"
            elif drift == "event":
                session.add(
                    EasyWeekEvent(
                        status="captured",
                        event_hint="booking-updated",
                        payload=seeded["payload"],
                        booking_uuid=uuid.UUID(TEST_BOOKING_UUID),
                        received_at=NOW - timedelta(days=4),
                    )
                )
            else:
                service = (await session.execute(select(RecordService))).scalars().one()
                service.title = "Something else"
    before = await table_snapshot(session_maker)

    with pytest.raises(RecoveryError):
        await run_apply(session_maker, frozen)

    assert await table_snapshot(session_maker) == before


async def test_a_second_apply_of_the_same_plan_is_idempotent(session_maker, tmp_path: Path) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])

    first = await run_apply(session_maker, frozen)
    after_first = await table_snapshot(session_maker)

    second = await run_apply(session_maker, frozen)

    assert first.outcome == OUTCOME_APPLIED
    assert second.outcome == OUTCOME_ALREADY_APPLIED
    assert second.canceled_reminder_job_ids == ()
    assert second.events[0]["already_recovered"] is True
    assert await table_snapshot(session_maker) == after_first


async def test_an_apply_after_a_replanned_world_still_refuses(session_maker, tmp_path: Path) -> None:
    """Idempotence is for the SAME end state, not for any state that looks done."""
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])
    await run_apply(session_maker, frozen)

    async with session_maker() as session:
        async with session.begin():
            job = (await session.execute(select(MessageJob).where(MessageJob.job_type == REMINDER_2H))).scalars().one()
            job.status = "queued"
    before = await table_snapshot(session_maker)

    with pytest.raises(RecoveryError):
        await run_apply(session_maker, frozen)

    assert await table_snapshot(session_maker) == before


# ===========================================================================
# verify
# ===========================================================================


async def test_verify_proves_the_exact_end_state(session_maker, tmp_path: Path) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])
    result = await run_apply(session_maker, frozen)
    report_path = write_private_json(result.report(), tmp_path / "apply.json")
    apply_report = read_apply_report(report_path, frozen=frozen)

    async with session_maker() as session:
        report = await verify_recovery(session, frozen=frozen, apply_report=apply_report)

    assert report["passed"] is True
    assert report["failures"] == []
    assert report["mutation_counts"] == {
        "records_marked_deleted": 1,
        "reminders_canceled": 2,
        "events_terminalized": 1,
        "message_jobs_created": 0,
        "outbox_messages_created": 0,
    }
    assert report["events"][0]["open_easyweek_reminder_ids"] == []
    assert report_path.stat().st_mode & 0o777 == 0o600


async def test_verify_is_repeatable_and_survives_a_second_apply(session_maker, tmp_path: Path) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])
    result = await run_apply(session_maker, frozen)
    apply_report = read_apply_report(write_private_json(result.report(), tmp_path / "apply.json"), frozen=frozen)

    async with session_maker() as session:
        first = await verify_recovery(session, frozen=frozen, apply_report=apply_report)
    await run_apply(session_maker, frozen)
    async with session_maker() as session:
        second = await verify_recovery(session, frozen=frozen, apply_report=apply_report)

    assert first["passed"] is True
    assert second["passed"] is True


async def test_verify_fails_when_a_reminder_is_re_opened_afterwards(session_maker, tmp_path: Path) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])
    result = await run_apply(session_maker, frozen)
    apply_report = read_apply_report(write_private_json(result.report(), tmp_path / "apply.json"), frozen=frozen)

    async with session_maker() as session:
        async with session.begin():
            job = (await session.execute(select(MessageJob).where(MessageJob.job_type == REMINDER_2H))).scalars().one()
            job.status = "queued"

    async with session_maker() as session:
        report = await verify_recovery(session, frozen=frozen, apply_report=apply_report)

    assert report["passed"] is False
    assert "open_easyweek_reminder_remains" in report["failures"]


async def test_verify_fails_when_a_job_appears_out_of_nowhere(session_maker, tmp_path: Path) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])
    result = await run_apply(session_maker, frozen)
    apply_report = read_apply_report(write_private_json(result.report(), tmp_path / "apply.json"), frozen=frozen)

    async with session_maker() as session:
        async with session.begin():
            session.add(
                MessageJob(
                    provider=PROVIDER_EASYWEEK,
                    company_id=TEST_LOCATION_ID,
                    record_id=seeded["record_id"],
                    job_type="comeback_3d",
                    run_at=NOW + timedelta(days=3),
                    status="queued",
                    dedupe_key="unexpected-comeback",
                    payload={},
                )
            )

    async with session_maker() as session:
        report = await verify_recovery(session, frozen=frozen, apply_report=apply_report)

    assert report["passed"] is False
    assert "record_job_set_changed" in report["failures"]


async def test_a_tampered_apply_report_is_refused(session_maker, tmp_path: Path) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])
    result = await run_apply(session_maker, frozen)
    report_path = write_private_json(result.report(), tmp_path / "apply.json")

    payload = json.loads(report_path.read_text(encoding="utf-8"))
    payload["mutation_counts"]["message_jobs_created"] = 0
    payload["outcome"] = "applied-ish"
    write_private_json(payload, report_path)

    with pytest.raises(RecoveryError, match="apply_report_digest_mismatch"):
        read_apply_report(report_path, frozen=frozen)


async def test_an_apply_report_from_another_plan_is_refused(session_maker, tmp_path: Path) -> None:
    first = await seed_failed_cancellation(session_maker)
    second = await seed_failed_cancellation(
        session_maker,
        payload=null_service_payload(
            booking_uuid="66666666-2222-4333-8444-555555555555",
            booking_id=TEST_BOOKING_ID + 3,
        ),
    )
    _p1, frozen_one, _path1 = await freeze(session_maker, tmp_path, event_ids=[first["event_id"]])
    _p2, frozen_two, _path2 = await freeze(
        session_maker,
        tmp_path / "second",
        event_ids=[second["event_id"]],
        reader=FakeReader(api_body(booking_uuid="66666666-2222-4333-8444-555555555555")),
    )
    result = await run_apply(session_maker, frozen_one)
    report_path = write_private_json(result.report(), tmp_path / "apply.json")

    with pytest.raises(RecoveryError, match="plan_apply_digest_mismatch"):
        read_apply_report(report_path, frozen=frozen_two)


async def test_the_apply_report_holds_no_pii(session_maker, tmp_path: Path) -> None:
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])
    result = await run_apply(session_maker, frozen)
    report_path = write_private_json(result.report(), tmp_path / "apply.json")

    text = report_path.read_text(encoding="utf-8")
    for secret in ("Test Person", "+49000000000", "test.person@example.invalid", "fixture comment"):
        assert secret not in text


# ===========================================================================
# The runbook and the compose service are part of the contract
# ===========================================================================
#
# The runtime is fail-closed; the danger lives in the instruction. A runbook
# that names a stale flag, a stale phrase or a service that does not exist
# sends an operator down a path the code will refuse — or, worse, invites a
# manual SQL edit. These bind the document to the code so neither drifts alone.

REPO_ROOT = Path(__file__).resolve().parents[3]
RUNBOOK = REPO_ROOT / "docs/easyweek/failed_cancellation_recovery_runbook.md"
COMPOSE = REPO_ROOT / "docker-compose.yml"
PLAN_DOC = REPO_ROOT / "docs/easyweek/INTEGRATION_PLAN.md"
COMPOSE_SERVICE = "easyweek-failed-cancellation-recovery"
# The canonical plan is deliberately kept out of Git (.gitignore), so the
# established repo convention is to guard the assertion on its presence
# rather than to fail every checkout that legitimately does not have it.
_PLAN_PRESENT = pytest.mark.skipif(not PLAN_DOC.exists(), reason="INTEGRATION_PLAN.md is untracked (.gitignore)")


async def test_the_runbook_names_the_real_permissions_and_commands() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert cli.APPLY_ENV_FLAG in text, "the environment half of the permission must be named exactly"
    assert confirmation_phrase("<PLAN_DIGEST>") in text
    assert COMPOSE_SERVICE in text
    for mode in ("plan", "apply", "verify"):
        assert f"{COMPOSE_SERVICE} {mode} " in text, f"the {mode} command must be copyable"
    assert "--event-id 360" in text
    assert "cd /opt/altegio_bot" in text


async def test_the_runbook_states_every_hard_boundary() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")

    assert "`--all` не существует" in text
    assert "ручные `UPDATE`/`DELETE` в PostgreSQL" in text.lower() or "ручные `UPDATE`/`DELETE`" in text
    assert "558, 559, 613 и 614" in text, "the four out-of-scope events must be named"
    assert "0600" in text and "0700" in text


async def test_the_compose_service_is_read_only_by_default() -> None:
    text = COMPOSE.read_text(encoding="utf-8")

    assert f"  {COMPOSE_SERVICE}:" in text
    block = text.split(f"  {COMPOSE_SERVICE}:", 1)[1]
    assert "altegio_bot.scripts.easyweek_failed_cancellation_recovery" in block
    assert "profiles:" in block.split("\nvolumes:", 1)[0]
    # The apply authorisation is passed per invocation, never baked in.
    assert cli.APPLY_ENV_FLAG not in block.split("\nvolumes:", 1)[0]


@_PLAN_PRESENT
async def test_the_canonical_plan_carries_the_dated_authorisation() -> None:
    text = PLAN_DOC.read_text(encoding="utf-8")

    assert "Ревизия 43" in text
    assert "2026-09-21" in text
    assert "5b5e9222-a0c4-4b44-a3d6-61132fcfea10" in text
    assert "easyweek_failed_cancellation_recovery" in text


async def test_verify_fails_when_an_outbox_row_appears_for_the_target(session_maker, tmp_path: Path) -> None:
    """An OutboxMessage is what a customer actually receives; there must be none."""
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])
    result = await run_apply(session_maker, frozen)
    apply_report = read_apply_report(write_private_json(result.report(), tmp_path / "apply.json"), frozen=frozen)

    async with session_maker() as session:
        async with session.begin():
            session.add(
                OutboxMessage(
                    company_id=TEST_LOCATION_ID,
                    client_id=1,
                    record_id=seeded["record_id"],
                    phone_e164="+49000000777",
                    template_code="record_canceled",
                    body="fixture",
                    status="queued",
                    scheduled_at=NOW,
                    meta={},
                )
            )

    async with session_maker() as session:
        report = await verify_recovery(session, frozen=frozen, apply_report=apply_report)

    assert report["passed"] is False
    assert "record_outbox_changed" in report["failures"]


async def test_unrelated_traffic_elsewhere_does_not_veto_a_correct_apply(session_maker, tmp_path: Path) -> None:
    """Capture and the workers keep running; that must not fail a sound apply.

    The guarantee is record-scoped, not table-wide, precisely so a live system
    can run this procedure without a maintenance window.
    """
    seeded = await seed_failed_cancellation(session_maker)
    _plan, frozen, _path = await freeze(session_maker, tmp_path, event_ids=[seeded["event_id"]])

    async with session_maker() as session:
        async with session.begin():
            # An unrelated booking's webhook lands between plan and apply, and
            # its own reminder is planned.
            other = Record(
                provider=PROVIDER_EASYWEEK,
                company_id=TEST_LOCATION_ID,
                altegio_record_id=TEST_BOOKING_ID + 99,
                easyweek_booking_uuid=uuid.UUID("77777777-2222-4333-8444-555555555555"),
                starts_at=NOW + timedelta(days=2),
                is_deleted=False,
            )
            session.add(other)
            await session.flush()
            session.add(
                EasyWeekEvent(
                    status="captured",
                    event_hint="booking-created",
                    payload={"uid": "77777777-2222-4333-8444-555555555555"},
                    booking_uuid=uuid.UUID("77777777-2222-4333-8444-555555555555"),
                    received_at=NOW,
                )
            )
            session.add(
                MessageJob(
                    provider=PROVIDER_EASYWEEK,
                    company_id=TEST_LOCATION_ID,
                    record_id=other.id,
                    job_type=REMINDER_24H,
                    run_at=NOW + timedelta(days=1),
                    status="queued",
                    dedupe_key="unrelated-booking-reminder",
                    payload={},
                )
            )

    result = await run_apply(session_maker, frozen)
    assert result.outcome == OUTCOME_APPLIED

    apply_report = read_apply_report(write_private_json(result.report(), tmp_path / "apply.json"), frozen=frozen)
    async with session_maker() as session:
        report = await verify_recovery(session, frozen=frozen, apply_report=apply_report)
    assert report["passed"] is True, "unrelated rows must not make a correct recovery look broken"


# ===========================================================================
# The handover section of the runbook must carry the HANNA scope, not CORE
# ===========================================================================
#
# The first draft of steps 5-7 told the operator to "repeat the handover for
# the same thirteen rows" and then pasted the CORE command underneath: the
# Karlsruhe API-contract manifest, five CORE run IDs and the shared
# `reminder_handover.v5.json`. Prose said HANNA, the copyable line said CORE.
# Running it would have frozen the wrong wave into a snapshot and reached
# bookings this recovery has nothing to do with.
#
# So these tests read the EXECUTABLE bash blocks, not the prose. The warning
# paragraph deliberately names the CORE identifiers so an operator recognises
# them; what must never contain them is a line one can paste into a shell.

HANDOVER_SERVICE = "easyweek-migration-prepare-handover"

HANNA_MANIFEST = "/migration/input/manifest.handover.hanna.json"
HANNA_COMPANY_ID = 758285
HANNA_RUN_IDS = ("55be3c0a62164e06", "f61323dc49384c62")
HANNA_SNAPSHOT = "/migration/state/reminder_handover.hanna.after-failed-cancellation.v5.json"
HANNA_APPLY_REPORT = "/migration/state/reminder_handover.hanna.after-failed-cancellation.apply-report.v3.json"
HANNA_REPEAT_APPLY_REPORT = (
    "/migration/state/reminder_handover.hanna.after-failed-cancellation.repeat-apply-report.v3.json"
)

# Everything that belongs to the OTHER wave. None of it may appear in a line
# an operator can execute from this runbook.
CORE_TOKENS = (
    "manifest.karlsruhe.api-contract.20260831.json",
    "27d8b9b5c59a446c",
    "887cfbbe881149ad",
    "90b183e121294f49",
    "9f895ed02dc64073",
    "f6897b60b99b4860",
    "/migration/state/reminder_handover.v5.json",
    "/migration/state/reminder_handover.apply-report.v3.json",
)
# Irina and Alena are a later wave and are out of scope for this procedure too.
FOREIGN_RUN_IDS = ("b4ca41ac1ad54591", "c52bb4f62fb64a35", "02d514703aec466f", "de193299b9974859")


def _bash_blocks(text: str) -> list[str]:
    return [block.strip() for block in re.findall(r"```bash\n(.*?)```", text, flags=re.S)]


def _handover_section(text: str) -> str:
    """Steps 5-10: everything from the handover plan to the read-only audit."""
    start = text.index("### Шаг 5 —")
    end = text.index("## 4. Стоп-условия")
    return text[start:end]


def _service_lines(text: str, service: str) -> list[str]:
    return [line.strip() for block in _bash_blocks(text) for line in block.splitlines() if service in line]


def _argv_after(line: str, service: str) -> list[str]:
    tokens = shlex.split(line)
    return tokens[tokens.index(service) + 1 :]


def _handover_scope(argv: list[str]) -> tuple[str, int, tuple[str, ...]]:
    """The contiguous identity of a wave: manifest, company and run IDs."""
    parser = _handover_parser()
    args = parser.parse_args(argv)
    return args.manifest, tuple(args.company_id)[0], tuple(sorted(args.run_id))


def _handover_parser():
    from altegio_bot.scripts import easyweek_reminder_handover as tool

    return tool.build_parser()


async def test_the_handover_commands_parse_with_the_real_parser() -> None:
    """A runbook whose flags do not exist fails where somebody is mid-procedure."""
    section = _handover_section(RUNBOOK.read_text(encoding="utf-8"))
    lines = _service_lines(section, HANDOVER_SERVICE)
    assert lines, "the recovery runbook documents no handover commands"

    parser = _handover_parser()
    for line in lines:
        args = parser.parse_args(_argv_after(line, HANDOVER_SERVICE))
        assert args.company_id == [HANNA_COMPANY_ID], line
        assert args.manifest == HANNA_MANIFEST, line
        assert sorted(args.run_id) == sorted(HANNA_RUN_IDS), line


async def test_all_three_handover_modes_use_one_identical_hanna_scope() -> None:
    """plan, apply and verify must freeze, write and prove the SAME wave."""
    section = _handover_section(RUNBOOK.read_text(encoding="utf-8"))
    parser = _handover_parser()
    by_mode: dict[str, list[list[str]]] = {}
    for line in _service_lines(section, HANDOVER_SERVICE):
        argv = _argv_after(line, HANDOVER_SERVICE)
        by_mode.setdefault(parser.parse_args(argv).mode, []).append(argv)

    assert set(by_mode) == {"plan", "apply", "verify"}, by_mode.keys()

    scopes = {_handover_scope(argv) for argvs in by_mode.values() for argv in argvs}
    assert scopes == {(HANNA_MANIFEST, HANNA_COMPANY_ID, tuple(sorted(HANNA_RUN_IDS)))}, scopes


async def test_the_handover_commands_use_hanna_specific_snapshot_and_reports() -> None:
    """Separate files, so CORE evidence and HANNA evidence cannot overwrite each other."""
    section = _handover_section(RUNBOOK.read_text(encoding="utf-8"))
    parser = _handover_parser()
    snapshots: set[str] = set()
    apply_reports: set[str] = set()
    for line in _service_lines(section, HANDOVER_SERVICE):
        args = parser.parse_args(_argv_after(line, HANDOVER_SERVICE))
        snapshots.add(args.snapshot)
        if args.mode in ("apply", "verify"):
            apply_reports.add(args.apply_report)

    assert snapshots == {HANNA_SNAPSHOT}, snapshots
    # The idempotence re-apply writes its own report so the first apply's
    # evidence survives; verify reads the first one.
    assert apply_reports == {HANNA_APPLY_REPORT, HANNA_REPEAT_APPLY_REPORT}, apply_reports


async def test_no_core_identifier_reaches_an_executable_line() -> None:
    """The prose may name the other wave; a copyable command may not."""
    text = RUNBOOK.read_text(encoding="utf-8")
    section = _handover_section(text)

    for block in _bash_blocks(section):
        for token in (*CORE_TOKENS, *FOREIGN_RUN_IDS):
            assert token not in block, f"{token} reached an executable line:\n{block}"

    # And nowhere else in the runbook either — the recovery steps have their
    # own service and must not grow a stray handover line.
    for block in _bash_blocks(text):
        for token in (*CORE_TOKENS, *FOREIGN_RUN_IDS):
            assert token not in block, f"{token} reached an executable line:\n{block}"


async def test_the_warning_still_names_the_core_scope_in_prose() -> None:
    """Recognising the wrong wave is the point; the warning must stay readable."""
    section = _handover_section(RUNBOOK.read_text(encoding="utf-8"))
    prose = re.sub(r"```bash\n.*?```", "", section, flags=re.S)
    for token in ("manifest.karlsruhe.api-contract.20260831.json", "27d8b9b5c59a446c", "f6897b60b99b4860"):
        assert token in prose, token
    assert "HANNA" in prose


async def test_only_the_apply_handover_command_can_write() -> None:
    from altegio_bot.scripts import easyweek_reminder_handover as tool

    section = _handover_section(RUNBOOK.read_text(encoding="utf-8"))
    parser = tool.build_parser()
    for line in _service_lines(section, HANDOVER_SERVICE):
        args = parser.parse_args(_argv_after(line, HANDOVER_SERVICE))
        if args.mode == "apply":
            assert args.apply is True, line
            assert args.plan_digest, line
            assert args.confirm, line
            assert args.confirm == tool.confirmation_phrase(args.plan_digest), line
            assert tool.APPLY_ENV_FLAG in line, line
        else:
            assert args.apply is False, line
            assert tool.APPLY_ENV_FLAG not in line, line


async def test_the_recovery_steps_still_name_only_event_360() -> None:
    """The recovery half must not widen while the handover half is corrected."""
    text = RUNBOOK.read_text(encoding="utf-8")
    parser = cli.build_parser()
    lines = _service_lines(text, COMPOSE_SERVICE)
    assert lines, "the runbook documents no recovery commands"

    modes = set()
    for line in lines:
        args = parser.parse_args(_argv_after(line, COMPOSE_SERVICE))
        assert args.event_id == [360], line
        modes.add(args.mode)
        if args.mode == "apply":
            assert args.apply is True, line
            assert cli.APPLY_ENV_FLAG in line, line
        else:
            assert args.apply is False, line
            assert cli.APPLY_ENV_FLAG not in line, line
    assert modes == {"plan", "apply", "verify"}, modes


async def test_the_documented_order_puts_recovery_before_the_handover() -> None:
    """Recovery first: the handover cannot classify a row that is still active."""
    text = RUNBOOK.read_text(encoding="utf-8")
    steps = [text.index(f"### Шаг {number} —") for number in range(1, 11)]
    assert steps == sorted(steps), "the runbook steps are out of order"

    first_handover = min(text.index(line) for line in _service_lines(text, HANDOVER_SERVICE))
    last_recovery = max(text.index(line) for line in _service_lines(text, COMPOSE_SERVICE))
    assert last_recovery < first_handover, "every recovery command must precede the handover"
