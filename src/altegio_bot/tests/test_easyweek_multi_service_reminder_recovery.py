"""PR-7.4 controlled reminder recovery, including PostgreSQL mutations."""

from __future__ import annotations

import copy
import json
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest
from sqlalchemy import func, select

from altegio_bot.easyweek_multi_service import clear_multi_service_catalog_cache
from altegio_bot.easyweek_multi_service_recovery import (
    ALREADY_DONE,
    ALREADY_PROCESSING,
    ALREADY_QUEUED,
    CATEGORY_NOT_ALLOWED,
    CREATE,
    IDENTITY_MISMATCH,
    LIVE_BOOKING_NOT_ACTIVE,
    NON_TERMINAL_OUTBOX_PRESENT,
    PROOF_FAILED,
    TERMINAL_HISTORY_PRESENT,
    WINDOW_PASSED,
    RecoveryError,
    apply_recovery_plan,
    build_recovery_plan,
    check_apply_authorization,
    confirmation_phrase,
    read_apply_report,
    read_snapshot,
    verify_recovery,
    write_private_json,
    write_snapshot,
)
from altegio_bot.easyweek_normalizer import canonical_booking_uuid
from altegio_bot.easyweek_service_category import record_raw_with_services_count
from altegio_bot.models.models import Client, EasyWeekEvent, MessageJob, OutboxMessage, Record
from altegio_bot.settings import settings
from altegio_bot.tests.easyweek_fixtures import (
    TEST_BOOKING_ID,
    TEST_BOOKING_UUID,
    TEST_CUSTOMER_ID,
    TEST_LOCATION_ID,
    TEST_LOCATION_UUID,
    booking_created_multi_service,
    set_booking_price,
)

pytestmark = pytest.mark.asyncio

NOW = datetime(2026, 9, 9, 10, 0, tzinfo=timezone.utc)


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
                }
            }
        ),
        raising=False,
    )
    monkeypatch.setattr(
        settings,
        "easyweek_allowed_service_categories",
        json.dumps(["Fixture Category"]),
        raising=False,
    )
    monkeypatch.setattr(settings, "easyweek_multi_service_notifications_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_reminders_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_reminder_api_guard_enabled", True, raising=False)
    clear_multi_service_catalog_cache()


def _webhook(*, first: str = "Fixture Service", second: str = "Second Fixture Service") -> dict[str, Any]:
    payload = booking_created_multi_service()
    payload["service_name"] = first
    payload["service_related"] = second
    payload["services_description"] = f"{first}, {second}"
    payload["services_count"] = 2
    payload["quantity"] = 2
    payload["booking_price_currency"] = "EUR"
    set_booking_price(payload, 8000)
    return payload


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


def _api(
    *,
    starts_at: datetime,
    canceled: bool = False,
    completed: bool = False,
    first: str = "Fixture Service",
    second: str = "Second Fixture Service",
    second_price: int = 4500,
) -> dict[str, Any]:
    status = "canceled" if canceled else "completed" if completed else "active"
    return {
        "uuid": TEST_BOOKING_UUID,
        "location_uuid": TEST_LOCATION_UUID,
        "start_time": starts_at.isoformat(),
        "is_canceled": canceled,
        "is_completed": completed,
        "status": {"type": status},
        "currency": "EUR",
        "order": {"subtotal": 3500 + second_price, "total": 3500 + second_price},
        "ordered_services": [
            _line("11111111-1111-4111-8111-111111111111", first, 3500, 30),
            _line("22222222-2222-4222-8222-222222222222", second, second_price, 45),
        ],
    }


def _catalog(*, second_category: str = "Fixture Category") -> list[dict[str, Any]]:
    return [
        {
            "uuid": "aaaaaaaa-1111-4111-8111-111111111111",
            "name": "Fixture Service",
            "currency": "EUR",
            "price": 3500,
            "duration": {"value": 30, "label": "minutes"},
            "category": {"name": "Fixture Category"},
        },
        {
            "uuid": "aaaaaaaa-2222-4222-8222-222222222222",
            "name": "Second Fixture Service",
            "currency": "EUR",
            "price": 4500,
            "duration": {"value": 45, "label": "minutes"},
            "category": {"name": second_category},
        },
    ]


class FakeReader:
    def __init__(
        self,
        *,
        starts_at: datetime,
        canceled: bool = False,
        completed: bool = False,
        second_category: str = "Fixture Category",
        fail: bool = False,
    ) -> None:
        self.starts_at = starts_at
        self.canceled = canceled
        self.completed = completed
        self.second_category = second_category
        self.fail = fail

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        assert booking_uuid == TEST_BOOKING_UUID
        if self.fail:
            raise TimeoutError
        return _api(starts_at=self.starts_at, canceled=self.canceled, completed=self.completed)

    async def list_location_services(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        assert (location_uuid, page) == (TEST_LOCATION_UUID, 1)
        rows = _catalog(second_category=self.second_category)
        return {"data": rows, "meta": {"current_page": 1, "last_page": 1, "total": len(rows)}}


async def _seed(
    session,
    *,
    starts_at: datetime,
    client_provider: str = "easyweek",
    job_status: str | None = None,
    add_non_terminal_outbox: bool = False,
) -> tuple[Record, MessageJob | None]:
    client = Client(
        provider=client_provider,
        company_id=TEST_LOCATION_ID,
        altegio_client_id=TEST_CUSTOMER_ID,
        display_name="Recovery fixture",
        phone_e164="+49000000000",
        raw={},
    )
    session.add(client)
    await session.flush()
    record = Record(
        provider="easyweek",
        company_id=TEST_LOCATION_ID,
        altegio_record_id=TEST_BOOKING_ID,
        easyweek_booking_uuid=uuid.UUID(TEST_BOOKING_UUID),
        client_id=client.id,
        starts_at=starts_at,
        total_cost=Decimal("80.00"),
        is_deleted=False,
        raw=record_raw_with_services_count({}, 2),
    )
    session.add(record)
    await session.flush()
    payload = _webhook()
    session.add(
        EasyWeekEvent(
            status="processed",
            event_hint="booking-created",
            auth_via="query",
            payload_hash="recovery-fixture",
            payload=payload,
            booking_uuid=canonical_booking_uuid(payload),
            body_truncated=False,
        )
    )
    await session.flush()
    job = None
    if job_status is not None or add_non_terminal_outbox:
        # First plan obtains the canonical digest/key; callers add state later.
        plan = await build_recovery_plan(
            session,
            client=FakeReader(starts_at=starts_at),
            now=NOW,
            pause_sec=0,
        )
        reminder = plan.records[0]["reminders"][0]
        job = MessageJob(
            provider="easyweek",
            company_id=TEST_LOCATION_ID,
            record_id=record.id,
            client_id=client.id,
            job_type=reminder["job_type"],
            run_at=datetime.fromisoformat(str(reminder["run_at"]).replace("Z", "+00:00")),
            status=job_status or "queued",
            dedupe_key=reminder["dedupe_key"],
            payload={},
        )
        session.add(job)
        await session.flush()
        if add_non_terminal_outbox:
            session.add(
                OutboxMessage(
                    company_id=TEST_LOCATION_ID,
                    client_id=client.id,
                    record_id=record.id,
                    job_id=job.id,
                    phone_e164="+49000000000",
                    template_code=str(reminder["job_type"]),
                    body="fixture",
                    status="queued",
                    scheduled_at=NOW,
                    meta={},
                )
            )
            await session.flush()
    return record, job


async def _counts(session) -> tuple[int, int, int, int]:
    values: list[int] = []
    for model in (Record, EasyWeekEvent, MessageJob, OutboxMessage):
        values.append(int((await session.execute(select(func.count()).select_from(model))).scalar_one()))
    return tuple(values)  # type: ignore[return-value]


async def _no_sleep(_seconds: float) -> None:
    return None


async def test_plan_is_read_only_and_two_future_windows_are_create(session_maker) -> None:
    starts = NOW + timedelta(days=2)
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, starts_at=starts)
        before = await _counts(session)
        plan = await build_recovery_plan(
            session,
            client=FakeReader(starts_at=starts),
            now=NOW,
            pause_sec=0,
            sleep=_no_sleep,
        )
        after = await _counts(session)
    assert before == after == (1, 1, 0, 0)
    assert [item["disposition"] for item in plan.records[0]["reminders"]] == [CREATE, CREATE]
    assert plan.summary["reminders_to_create"] == 2
    assert plan.summary["apply_ready"] is True
    safe_output = json.dumps(plan.safe_report())
    assert "Recovery fixture" not in safe_output
    assert "+49000000000" not in safe_output
    assert "customer" not in safe_output.casefold()


async def test_single_service_and_altegio_records_are_out_of_scope(session_maker) -> None:
    starts = NOW + timedelta(days=2)
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, starts_at=starts)
            session.add_all(
                [
                    Record(
                        provider="altegio",
                        company_id=1,
                        altegio_record_id=777,
                        client_id=1,
                        starts_at=starts,
                        is_deleted=False,
                        raw=record_raw_with_services_count({}, 2),
                    ),
                    Record(
                        provider="easyweek",
                        company_id=TEST_LOCATION_ID,
                        altegio_record_id=778,
                        easyweek_booking_uuid=uuid.UUID("99999999-2222-4333-8444-555555555555"),
                        starts_at=starts,
                        is_deleted=False,
                        raw=record_raw_with_services_count({}, 1),
                    ),
                ]
            )
        plan = await build_recovery_plan(session, client=FakeReader(starts_at=starts), now=NOW, pause_sec=0)
    assert plan.summary["records_seen"] == 1


@pytest.mark.parametrize(
    ("starts", "expected"),
    [
        (NOW + timedelta(hours=10), [WINDOW_PASSED, CREATE]),
        (NOW + timedelta(hours=1), [WINDOW_PASSED, WINDOW_PASSED]),
    ],
)
async def test_plan_never_creates_an_overdue_reminder(session_maker, starts, expected) -> None:
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, starts_at=starts)
        plan = await build_recovery_plan(session, client=FakeReader(starts_at=starts), now=NOW, pause_sec=0)
    assert [item["disposition"] for item in plan.records[0]["reminders"]] == expected
    assert plan.summary["apply_ready"] is True


@pytest.mark.parametrize("terminal", ["canceled", "completed"])
async def test_terminal_live_booking_never_creates(session_maker, terminal: str) -> None:
    starts = NOW + timedelta(days=2)
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, starts_at=starts)
        reader = FakeReader(starts_at=starts, canceled=terminal == "canceled", completed=terminal == "completed")
        plan = await build_recovery_plan(session, client=reader, now=NOW, pause_sec=0)
    assert {item["disposition"] for item in plan.records[0]["reminders"]} == {LIVE_BOOKING_NOT_ACTIVE}


async def test_disallowed_or_mixed_categories_never_create(session_maker) -> None:
    starts = NOW + timedelta(days=2)
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, starts_at=starts)
        plan = await build_recovery_plan(
            session,
            client=FakeReader(starts_at=starts, second_category="Nagelservice"),
            now=NOW,
            pause_sec=0,
        )
    assert {item["disposition"] for item in plan.records[0]["reminders"]} == {CATEGORY_NOT_ALLOWED}
    assert plan.summary["disallowed_records"] == 1


async def test_api_failure_is_fail_closed_and_read_only(session_maker) -> None:
    starts = NOW + timedelta(days=2)
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, starts_at=starts)
        before = await _counts(session)
        plan = await build_recovery_plan(
            session,
            client=FakeReader(starts_at=starts, fail=True),
            now=NOW,
            pause_sec=0,
        )
        after = await _counts(session)
    assert before == after
    assert {item["disposition"] for item in plan.records[0]["reminders"]} == {PROOF_FAILED}


async def test_client_identity_mismatch_is_fail_closed(session_maker) -> None:
    starts = NOW + timedelta(days=2)
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, starts_at=starts, client_provider="altegio")
        plan = await build_recovery_plan(session, client=FakeReader(starts_at=starts), now=NOW, pause_sec=0)
    assert {item["disposition"] for item in plan.records[0]["reminders"]} == {IDENTITY_MISMATCH}


@pytest.mark.parametrize(
    ("status", "disposition"),
    [
        ("queued", ALREADY_QUEUED),
        ("processing", ALREADY_PROCESSING),
        ("done", ALREADY_DONE),
        ("canceled", TERMINAL_HISTORY_PRESENT),
        ("failed", TERMINAL_HISTORY_PRESENT),
    ],
)
async def test_existing_exact_job_is_never_reopened_or_duplicated(session_maker, status, disposition) -> None:
    starts = NOW + timedelta(days=2)
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, starts_at=starts, job_status=status)
        clear_multi_service_catalog_cache()
        plan = await build_recovery_plan(session, client=FakeReader(starts_at=starts), now=NOW, pause_sec=0)
    assert plan.records[0]["reminders"][0]["disposition"] == disposition


async def test_non_terminal_outbox_blocks_exact_reminder(session_maker) -> None:
    starts = NOW + timedelta(days=2)
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, starts_at=starts, add_non_terminal_outbox=True)
        clear_multi_service_catalog_cache()
        plan = await build_recovery_plan(session, client=FakeReader(starts_at=starts), now=NOW, pause_sec=0)
    assert plan.records[0]["reminders"][0]["disposition"] == NON_TERMINAL_OUTBOX_PRESENT


async def test_snapshot_authorization_rejects_digest_confirmation_age_and_fences(
    session_maker,
    tmp_path,
    monkeypatch,
) -> None:
    starts = NOW + timedelta(days=2)
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, starts_at=starts)
        plan = await build_recovery_plan(session, client=FakeReader(starts_at=starts), now=NOW, pause_sec=0)
    path = tmp_path / "plan.json"
    write_snapshot(plan, path)
    frozen = read_snapshot(path)
    tampered = json.loads(path.read_text())
    tampered["planned_at"] = (NOW + timedelta(seconds=1)).isoformat().replace("+00:00", "Z")
    path.write_text(json.dumps(tampered))
    with pytest.raises(RecoveryError, match="snapshot_digest_mismatch"):
        read_snapshot(path)
    write_snapshot(plan, path)
    frozen = read_snapshot(path)
    with pytest.raises(RecoveryError, match="plan_digest_mismatch"):
        check_apply_authorization(frozen, supplied_digest="bad", supplied_confirmation="bad", now=NOW)
    with pytest.raises(RecoveryError, match="confirmation_mismatch"):
        check_apply_authorization(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation="bad",
            now=NOW,
        )
    with pytest.raises(RecoveryError, match="snapshot_expired"):
        check_apply_authorization(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation=confirmation_phrase(frozen.digest),
            now=NOW + timedelta(hours=1),
        )
    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", True, raising=False)
    with pytest.raises(RecoveryError, match="multi_service_send_fence_open"):
        check_apply_authorization(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation=confirmation_phrase(frozen.digest),
            now=NOW,
        )
    monkeypatch.setattr(settings, "easyweek_reminders_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_allowed_service_categories", json.dumps(["Other"]), raising=False)
    with pytest.raises(RecoveryError, match="configuration_digest_changed"):
        check_apply_authorization(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation=confirmation_phrase(frozen.digest),
            now=NOW,
        )
    monkeypatch.setattr(
        settings,
        "easyweek_allowed_service_categories",
        json.dumps(["Fixture Category"]),
        raising=False,
    )
    monkeypatch.setattr(settings, "easyweek_multi_service_send_enabled", False, raising=False)
    monkeypatch.setattr(settings, "easyweek_reminder_api_guard_enabled", False, raising=False)
    with pytest.raises(RecoveryError, match="reminder_api_guard_disabled"):
        check_apply_authorization(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation=confirmation_phrase(frozen.digest),
            now=NOW,
        )
    monkeypatch.setattr(settings, "easyweek_reminder_api_guard_enabled", True, raising=False)
    monkeypatch.setattr(settings, "easyweek_reminders_enabled", False, raising=False)
    with pytest.raises(RecoveryError, match="reminder_planning_disabled"):
        check_apply_authorization(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation=confirmation_phrase(frozen.digest),
            now=NOW,
        )


async def _frozen_plan(session, starts: datetime, tmp_path):
    plan = await build_recovery_plan(session, client=FakeReader(starts_at=starts), now=NOW, pause_sec=0)
    path = tmp_path / "plan.json"
    write_snapshot(plan, path)
    return read_snapshot(path)


async def test_apply_creates_only_expected_jobs_and_is_idempotent(session_maker, tmp_path) -> None:
    starts = NOW + timedelta(days=2)
    async with session_maker() as session:
        async with session.begin():
            record, _job = await _seed(session, starts_at=starts)
        raw_before = copy.deepcopy(record.raw)
        frozen = await _frozen_plan(session, starts, tmp_path)
        clear_multi_service_catalog_cache()
        first = await apply_recovery_plan(
            session,
            frozen=frozen,
            client=FakeReader(starts_at=starts),
            now=NOW + timedelta(seconds=1),
            pause_sec=0,
        )
        assert len(first.created_job_ids) == 2
        assert first.already_present_job_ids == ()
        assert int((await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one()) == 0
        await session.refresh(record)
        assert record.raw == raw_before

        clear_multi_service_catalog_cache()
        second = await apply_recovery_plan(
            session,
            frozen=frozen,
            client=FakeReader(starts_at=starts),
            now=NOW + timedelta(seconds=2),
            pause_sec=0,
        )
        jobs = list((await session.execute(select(MessageJob).order_by(MessageJob.id))).scalars())
    assert len(jobs) == 2
    assert second.created_job_ids == ()
    assert set(second.already_present_job_ids) == set(first.created_job_ids)
    assert all(job.payload["multi_service_recovery_plan_digest"] == frozen.digest for job in jobs)
    assert all(
        job.payload["multi_service_snapshot"]["digest"] == job.payload["multi_service_snapshot_digest"] for job in jobs
    )
    assert {job.job_type for job in jobs} == {"reminder_24h", "reminder_2h"}


@pytest.mark.parametrize("drift", ["starts_at", "service", "configuration", "processing"])
async def test_apply_refuses_any_scope_drift(session_maker, tmp_path, monkeypatch, drift: str) -> None:
    starts = NOW + timedelta(days=2)
    async with session_maker() as session:
        async with session.begin():
            record, _job = await _seed(session, starts_at=starts)
        frozen = await _frozen_plan(session, starts, tmp_path)
        reader = FakeReader(starts_at=starts)
        if drift == "starts_at":
            record.starts_at = starts + timedelta(hours=1)
            await session.commit()
        elif drift == "service":
            reader = FakeReader(starts_at=starts, second_category="Other")
        elif drift == "configuration":
            monkeypatch.setattr(settings, "easyweek_allowed_service_categories", json.dumps(["Other"]), raising=False)
        else:
            reminder = frozen.records[0]["reminders"][0]
            session.add(
                MessageJob(
                    provider="easyweek",
                    company_id=TEST_LOCATION_ID,
                    record_id=record.id,
                    client_id=record.client_id,
                    job_type=reminder["job_type"],
                    run_at=datetime.fromisoformat(reminder["run_at"].replace("Z", "+00:00")),
                    status="processing",
                    dedupe_key=reminder["dedupe_key"],
                    payload={},
                )
            )
            await session.commit()
        clear_multi_service_catalog_cache()
        with pytest.raises(RecoveryError):
            await apply_recovery_plan(
                session,
                frozen=frozen,
                client=reader,
                now=NOW + timedelta(seconds=1),
                pause_sec=0,
            )
        assert int((await session.execute(select(func.count()).select_from(OutboxMessage))).scalar_one()) == 0


async def test_verify_detects_missing_changed_and_overdue_jobs(session_maker, tmp_path) -> None:
    starts = NOW + timedelta(days=2)
    async with session_maker() as session:
        async with session.begin():
            await _seed(session, starts_at=starts)
        frozen = await _frozen_plan(session, starts, tmp_path)
        clear_multi_service_catalog_cache()
        applied = await apply_recovery_plan(
            session,
            frozen=frozen,
            client=FakeReader(starts_at=starts),
            now=NOW + timedelta(seconds=1),
            pause_sec=0,
        )
        report_path = tmp_path / "apply.json"
        write_private_json(applied.report(), report_path)
        report = read_apply_report(report_path, frozen=frozen)
        clean = await verify_recovery(session, frozen=frozen, apply_report=report, now=NOW + timedelta(seconds=2))
        assert clean["passed"] is True

        first = await session.get(MessageJob, applied.created_job_ids[0])
        assert first is not None
        first.payload = {**first.payload, "multi_service_snapshot_digest": "0" * 64}
        first.run_at = NOW
        await session.commit()
        changed = await verify_recovery(session, frozen=frozen, apply_report=report, now=NOW + timedelta(days=3))
        second = await session.get(MessageJob, applied.created_job_ids[1])
        assert second is not None
        await session.delete(second)
        await session.commit()
        missing = await verify_recovery(session, frozen=frozen, apply_report=report, now=NOW + timedelta(seconds=3))
    assert changed["passed"] is False
    assert changed["digest_mismatches"] == [applied.created_job_ids[0]]
    assert changed["overdue_jobs"] == [applied.created_job_ids[0]]
    assert missing["passed"] is False
    assert missing["missing_expected_jobs"] == 1


async def test_verify_detects_job_for_disallowed_record_and_unexpected_outbox(session_maker, tmp_path) -> None:
    starts = NOW + timedelta(days=2)
    async with session_maker() as session:
        async with session.begin():
            record, _job = await _seed(session, starts_at=starts)
        plan = await build_recovery_plan(
            session,
            client=FakeReader(starts_at=starts, second_category="Nagelservice"),
            now=NOW,
            pause_sec=0,
        )
        path = tmp_path / "disallowed-plan.json"
        write_snapshot(plan, path)
        frozen = read_snapshot(path)
        clear_multi_service_catalog_cache()
        applied = await apply_recovery_plan(
            session,
            frozen=frozen,
            client=FakeReader(starts_at=starts, second_category="Nagelservice"),
            now=NOW + timedelta(seconds=1),
            pause_sec=0,
        )
        reminder = frozen.records[0]["reminders"][0]
        bad_job = MessageJob(
            provider="easyweek",
            company_id=TEST_LOCATION_ID,
            record_id=record.id,
            client_id=record.client_id,
            job_type=reminder["job_type"],
            run_at=datetime.fromisoformat(reminder["run_at"].replace("Z", "+00:00")),
            status="queued",
            dedupe_key=reminder["dedupe_key"],
            payload={},
            created_at=NOW + timedelta(seconds=1),
        )
        session.add(bad_job)
        await session.flush()
        session.add(
            OutboxMessage(
                company_id=TEST_LOCATION_ID,
                client_id=record.client_id,
                record_id=record.id,
                job_id=bad_job.id,
                phone_e164="+49000000000",
                template_code=bad_job.job_type,
                body="fixture",
                status="queued",
                scheduled_at=NOW,
                meta={},
            )
        )
        await session.commit()
        report_path = tmp_path / "disallowed-apply.json"
        write_private_json(applied.report(), report_path)
        report = read_apply_report(report_path, frozen=frozen)
        verified = await verify_recovery(session, frozen=frozen, apply_report=report, now=NOW + timedelta(seconds=2))
    assert verified["passed"] is False
    assert verified["disallowed_jobs"] == [bad_job.id]
    assert verified["unexpected_outbox_ids"]
