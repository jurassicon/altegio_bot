"""The reminder handover's rules, without a database (plan §30).

After a wave migrates, the customer's appointment lives in EasyWeek while its
future reminders are still queued on the Altegio side — pointed at a booking
nobody works from — and the EasyWeek side has none at all. This module's rules
decide what is owed, what may be created, what may be withdrawn, and what has to
stop and wait for a person.

The database half is exercised against a real PostgreSQL in
``test_easyweek_reminder_handover_db.py``; everything provable without one is
here, because these are the rules that decide whether a customer gets a message.
"""

from __future__ import annotations

import json
import os
import uuid as uuid_module
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest

from altegio_bot.easyweek_migration.reminder_handover import (
    CANCEL_REASON,
    OBLIGATION_DONE,
    OBLIGATION_MISSING,
    OBLIGATION_OCCUPIED_CANCELED,
    OBLIGATION_OCCUPIED_FAILED,
    OBLIGATION_OCCUPIED_UNKNOWN,
    OBLIGATION_PRESENT_OPEN,
    OBLIGATION_PROCESSING,
    SNAPSHOT_VERSION,
    HandoverPlan,
    HandoverRow,
    SnapshotError,
    boundary_still_future,
    canonical_uuid,
    check_snapshot_usable,
    confirmation_phrase,
    insert_values,
    obligations_for,
    read_snapshot,
    write_snapshot,
)
from altegio_bot.easyweek_policy import REMINDER_2H, REMINDER_24H
from altegio_bot.easyweek_reminders import easyweek_reminder_dedupe_key
from altegio_bot.models.models import PROVIDER_EASYWEEK

BOOKING = uuid_module.UUID("aaaaaaaa-0000-4000-8000-000000000001")
NOW = datetime(2026, 9, 4, 12, 0, tzinfo=timezone.utc)


def key(job_type: str, starts_at: datetime) -> str:
    return easyweek_reminder_dedupe_key(booking_uuid=BOOKING, job_type=job_type, starts_at=starts_at)


def owed(hours: float, *, existing: dict[str, tuple[int, str]] | None = None, active: bool = True):
    starts = NOW + timedelta(hours=hours)
    return obligations_for(
        booking_uuid=BOOKING,
        starts_at=starts,
        now=NOW,
        is_active=active,
        existing=existing or {},
    )


# ---------------------------------------------------------------------------
# What a live booking owes
# ---------------------------------------------------------------------------


def test_more_than_a_day_away_owes_both_reminders() -> None:
    assert [item.job_type for item in owed(48)] == [REMINDER_24H, REMINDER_2H]
    assert all(item.outcome == OBLIGATION_MISSING for item in owed(48))


def test_inside_the_day_owes_only_the_two_hour_reminder() -> None:
    """The 24h moment has passed; sending it late is worse than not sending it."""
    assert [item.job_type for item in owed(6)] == [REMINDER_2H]


def test_two_hours_away_or_less_owes_nothing() -> None:
    assert owed(2) == ()
    assert owed(0.5) == ()


def test_a_booking_in_the_past_owes_nothing() -> None:
    assert owed(-3) == ()


def test_a_cancelled_or_completed_booking_owes_nothing() -> None:
    assert owed(48, active=False) == ()


def test_the_windows_come_from_the_canonical_planner() -> None:
    """Not reimplemented here: a second copy of the bounds would drift."""
    starts = NOW + timedelta(hours=48)
    assert {item.dedupe_key for item in owed(48)} == {key(REMINDER_24H, starts), key(REMINDER_2H, starts)}


# ---------------------------------------------------------------------------
# What an existing key means
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("status", "outcome", "insertable"),
    [
        ("queued", OBLIGATION_PRESENT_OPEN, False),
        ("processing", OBLIGATION_PROCESSING, False),
        ("done", OBLIGATION_DONE, False),
        ("canceled", OBLIGATION_OCCUPIED_CANCELED, False),
        ("failed", OBLIGATION_OCCUPIED_FAILED, False),
        ("something_new", OBLIGATION_OCCUPIED_UNKNOWN, False),
    ],
)
def test_an_occupied_key_is_never_re_created(status: str, outcome: str, insertable: bool) -> None:
    starts = NOW + timedelta(hours=48)
    existing = {key(REMINDER_24H, starts): (7, status)}
    rows = {item.job_type: item for item in owed(48, existing=existing)}

    assert rows[REMINDER_24H].outcome == outcome
    assert rows[REMINDER_24H].needs_insert is insertable
    assert rows[REMINDER_2H].outcome == OBLIGATION_MISSING, "the other reminder is unaffected"


def test_a_done_reminder_is_not_re_opened() -> None:
    """It already went out. Creating it again would message the customer twice."""
    starts = NOW + timedelta(hours=48)
    rows = {i.job_type: i for i in owed(48, existing={key(REMINDER_24H, starts): (7, "done")})}

    assert rows[REMINDER_24H].outcome == OBLIGATION_DONE
    assert rows[REMINDER_24H].is_blocker is False, "done is satisfied, not a blocker"


@pytest.mark.parametrize("status", ["canceled", "failed", "something_new"])
def test_a_key_held_by_a_cancelled_or_failed_job_is_a_blocker(status: str) -> None:
    """Unique key: it cannot be re-created, and re-opening it is an operator call."""
    starts = NOW + timedelta(hours=48)
    rows = {i.job_type: i for i in owed(48, existing={key(REMINDER_24H, starts): (7, status)})}

    assert rows[REMINDER_24H].is_blocker is True


# ---------------------------------------------------------------------------
# The row a missing reminder becomes
# ---------------------------------------------------------------------------


def handover_row(**overrides: Any) -> HandoverRow:
    base: dict[str, Any] = {
        "ledger_id": 1,
        "source_company_id": 758285,
        "source_record_id": 900001,
        "source_record_pk": 11,
        "target_record_pk": 22,
        # The EasyWeek LOCATION id — what an EasyWeek Record and job carry.
        "target_company_id": 308001,
        "target_booking_uuid": str(BOOKING),
        "target_starts_at": NOW + timedelta(hours=48),
    }
    base.update(overrides)
    return HandoverRow(**base)


def test_a_created_job_carries_production_identity() -> None:
    row = handover_row()
    [obligation] = [i for i in owed(48) if i.job_type == REMINDER_24H]

    values = insert_values(row, obligation, client_id=55)

    assert values["provider"] == PROVIDER_EASYWEEK
    assert values["record_id"] == 22, "the EasyWeek target, never the Altegio source"
    assert values["company_id"] == 308001, "the EasyWeek location, never the Altegio company"
    assert values["client_id"] == 55
    assert values["status"] == "queued"
    assert values["dedupe_key"] == obligation.dedupe_key
    assert values["run_at"] == obligation.run_at


def test_the_created_payload_is_the_canonical_one() -> None:
    row = handover_row()
    [obligation] = [i for i in owed(48) if i.job_type == REMINDER_2H]
    payload = insert_values(row, obligation, client_id=None)["payload"]

    assert payload["provider"] == "easyweek"
    assert payload["booking_uuid"] == str(BOOKING)
    assert payload["record_starts_at"] == (NOW + timedelta(hours=48)).isoformat()
    for leaked in ("phone", "name", "email", "client_name"):
        assert leaked not in payload


def test_a_handover_row_carries_no_personal_data() -> None:
    blob = json.dumps(handover_row(obligations=owed(48)).as_safe_dict())

    for leaked in ("phone", "email", "first_name", "customer_uuid"):
        assert leaked not in blob


# ---------------------------------------------------------------------------
# The three readiness questions
# ---------------------------------------------------------------------------


def plan_with(*rows: HandoverRow, **kwargs: Any) -> HandoverPlan:
    return HandoverPlan(company_ids=(758285,), created_at=NOW, rows=rows, ledger_rows_seen=len(rows), **kwargs)


def test_an_empty_easyweek_queue_is_not_coverage() -> None:
    """The trap the standalone preflight falls into after a migration."""
    plan = plan_with(handover_row(obligations=owed(48)))

    assert plan.guard_ready is True, "nothing existing is wrong — there is nothing existing"
    assert plan.coverage_ready is False, "and two reminders are missing"
    assert plan.to_create == 2


def test_coverage_is_ready_only_when_every_obligation_exists() -> None:
    starts = NOW + timedelta(hours=48)
    existing = {key(REMINDER_24H, starts): (1, "queued"), key(REMINDER_2H, starts): (2, "queued")}
    plan = plan_with(handover_row(obligations=owed(48, existing=existing)))

    assert plan.coverage_ready is True
    assert plan.to_create == 0


def test_a_blocker_fails_the_guard_question() -> None:
    starts = NOW + timedelta(hours=48)
    plan = plan_with(handover_row(obligations=owed(48, existing={key(REMINDER_24H, starts): (1, "failed")})))

    assert plan.guard_ready is False
    assert plan.coverage_ready is False
    assert plan.cutover_ready is False


def test_a_processing_source_job_fails_only_the_cutover_question() -> None:
    """The queue is fine; this is simply not the moment to switch ownership."""
    plan = plan_with(handover_row(obligations=owed(48), processing_source_job_ids=(9,)))

    assert plan.guard_ready is True
    assert plan.cutover_ready is False


def test_the_report_answers_all_three_separately() -> None:
    report = plan_with(handover_row(obligations=owed(48))).as_safe_dict()

    assert {"guard_ready", "coverage_ready", "cutover_ready"} <= set(report)
    assert report["mode"] == "read-only"


def test_the_report_carries_no_personal_data() -> None:
    blob = json.dumps(plan_with(handover_row(obligations=owed(48), stale_source_job_ids=(3,))).as_safe_dict())

    for leaked in ("phone", "email", "first_name", "name"):
        assert leaked not in blob


# ---------------------------------------------------------------------------
# The plan digest and the snapshot
# ---------------------------------------------------------------------------


def test_the_digest_covers_what_would_be_created() -> None:
    before = plan_with(handover_row(obligations=owed(48))).digest()
    after = plan_with(handover_row(obligations=owed(6))).digest()

    assert before != after


def test_the_digest_covers_what_would_be_cancelled() -> None:
    before = plan_with(handover_row(obligations=owed(48), stale_source_job_ids=(3,))).digest()
    after = plan_with(handover_row(obligations=owed(48), stale_source_job_ids=(3, 4))).digest()

    assert before != after


def test_the_digest_covers_the_frozen_identity() -> None:
    before = plan_with(handover_row(obligations=owed(48))).digest()
    after = plan_with(handover_row(obligations=owed(48), target_record_pk=999)).digest()

    assert before != after


def test_the_digest_ignores_when_the_plan_was_taken() -> None:
    """The age is checked explicitly; folding it in would make the digest unquotable."""
    rows = (handover_row(obligations=owed(48)),)
    early = HandoverPlan(company_ids=(758285,), created_at=NOW, rows=rows)
    later = HandoverPlan(company_ids=(758285,), created_at=NOW + timedelta(minutes=5), rows=rows)

    assert early.digest() == later.digest()


def test_a_snapshot_round_trips_and_is_not_world_readable(tmp_path: Path) -> None:
    plan = plan_with(handover_row(obligations=owed(48), stale_source_job_ids=(3,)))
    path = write_snapshot(plan, tmp_path / "state" / "plan.json")

    assert (os.stat(path).st_mode & 0o077) == 0
    assert (os.stat(path.parent).st_mode & 0o077) == 0

    frozen = read_snapshot(path)
    assert frozen.digest == plan.digest()
    assert frozen.company_ids == (758285,)
    assert len(frozen.rows) == 1


@pytest.mark.parametrize(
    "content",
    [
        "{ not json",
        json.dumps(
            {"version": 99, "plan_digest": "d", "created_at": "2026-09-04T12:00:00Z", "rows": [], "company_ids": []}
        ),
        json.dumps({"version": SNAPSHOT_VERSION, "created_at": "2026-09-04T12:00:00Z", "rows": [], "company_ids": []}),
        json.dumps({"version": SNAPSHOT_VERSION, "plan_digest": "d", "rows": [], "company_ids": []}),
        json.dumps({"version": SNAPSHOT_VERSION, "plan_digest": "d", "created_at": "2026-09-04T12:00:00Z"}),
    ],
    ids=["corrupt", "wrong_version", "no_digest", "no_created_at", "no_scope"],
)
def test_a_damaged_snapshot_authorises_nothing(content: str, tmp_path: Path) -> None:
    path = tmp_path / "plan.json"
    path.write_text(content)

    with pytest.raises(SnapshotError):
        read_snapshot(path)


# ---------------------------------------------------------------------------
# The two permission gates
# ---------------------------------------------------------------------------


def frozen_for(tmp_path: Path, plan: HandoverPlan):
    return read_snapshot(write_snapshot(plan, tmp_path / "plan.json"))


def test_the_right_digest_and_phrase_together_authorise(tmp_path: Path) -> None:
    frozen = frozen_for(tmp_path, plan_with(handover_row(obligations=owed(48))))

    check_snapshot_usable(
        frozen,
        supplied_digest=frozen.digest,
        supplied_confirmation=confirmation_phrase(frozen.digest),
        now=NOW,
    )


def test_a_missing_digest_authorises_nothing(tmp_path: Path) -> None:
    frozen = frozen_for(tmp_path, plan_with(handover_row(obligations=owed(48))))

    with pytest.raises(SnapshotError):
        check_snapshot_usable(
            frozen, supplied_digest=None, supplied_confirmation=confirmation_phrase(frozen.digest), now=NOW
        )


def test_a_digest_from_another_plan_authorises_nothing(tmp_path: Path) -> None:
    frozen = frozen_for(tmp_path, plan_with(handover_row(obligations=owed(48))))

    with pytest.raises(SnapshotError):
        check_snapshot_usable(
            frozen, supplied_digest="somebody elses digest", supplied_confirmation=confirmation_phrase("x"), now=NOW
        )


def test_the_phrase_must_carry_this_plans_digest(tmp_path: Path) -> None:
    """A phrase copied out of yesterday's terminal is not today's permission."""
    frozen = frozen_for(tmp_path, plan_with(handover_row(obligations=owed(48))))

    with pytest.raises(SnapshotError):
        check_snapshot_usable(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation=confirmation_phrase("a-different-digest"),
            now=NOW,
        )


def test_a_tampered_snapshot_no_longer_matches_its_digest(tmp_path: Path) -> None:
    path = write_snapshot(plan_with(handover_row(obligations=owed(48), stale_source_job_ids=(3,))), tmp_path / "p.json")
    payload = json.loads(path.read_text())
    payload["rows"][0]["stale_source_job_ids"] = [3, 4, 5]
    path.write_text(json.dumps(payload))

    frozen = read_snapshot(path)
    rebuilt = HandoverPlan(
        company_ids=frozen.company_ids,
        created_at=frozen.created_at,
        rows=(handover_row(obligations=owed(48), stale_source_job_ids=(3, 4, 5)),),
    )
    assert rebuilt.digest() != frozen.digest, "the file's own digest no longer describes its rows"


def test_a_stale_snapshot_is_refused(tmp_path: Path) -> None:
    frozen = frozen_for(tmp_path, plan_with(handover_row(obligations=owed(48))))

    with pytest.raises(SnapshotError):
        check_snapshot_usable(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation=confirmation_phrase(frozen.digest),
            now=NOW + timedelta(hours=3),
        )


def test_a_snapshot_from_the_future_is_refused(tmp_path: Path) -> None:
    frozen = frozen_for(tmp_path, plan_with(handover_row(obligations=owed(48))))

    with pytest.raises(SnapshotError):
        check_snapshot_usable(
            frozen,
            supplied_digest=frozen.digest,
            supplied_confirmation=confirmation_phrase(frozen.digest),
            now=NOW - timedelta(hours=1),
        )


# ---------------------------------------------------------------------------
# The reminder boundary
# ---------------------------------------------------------------------------


def test_a_reminder_that_crossed_its_moment_needs_a_new_plan(tmp_path: Path) -> None:
    """Queueing it would message somebody about an appointment they are at."""
    frozen = frozen_for(tmp_path, plan_with(handover_row(obligations=owed(3))))

    assert boundary_still_future(frozen.rows, now=NOW) is None
    assert boundary_still_future(frozen.rows, now=NOW + timedelta(hours=2)) == "reminder_boundary_passed"


def test_an_obligation_that_already_exists_does_not_hold_the_boundary(tmp_path: Path) -> None:
    starts = NOW + timedelta(hours=3)
    existing = {key(REMINDER_2H, starts): (1, "queued")}
    frozen = frozen_for(tmp_path, plan_with(handover_row(obligations=owed(3, existing=existing))))

    assert boundary_still_future(frozen.rows, now=NOW + timedelta(hours=2)) is None


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------


def test_only_a_canonical_uuid_is_acted_on() -> None:
    assert canonical_uuid(str(BOOKING)) == BOOKING
    assert canonical_uuid(str(BOOKING).upper()) is None, "somebody typed this"
    assert canonical_uuid("{%s}" % BOOKING) is None
    assert canonical_uuid("not-a-uuid") is None
    assert canonical_uuid(None) is None


def test_the_cancel_reason_is_stable_and_free_of_personal_data() -> None:
    assert "reminder handover" in CANCEL_REASON
    for leaked in ("phone", "@", "+49"):
        assert leaked not in CANCEL_REASON


# ---------------------------------------------------------------------------
# The runtime send guard is unchanged by the extraction (plan §30.4)
# ---------------------------------------------------------------------------


def guard_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "uuid": str(BOOKING),
        "location_uuid": "11111111-1111-4111-8111-111111111111",
        "start_time": "2026-09-06T12:00:00Z",
        "is_canceled": False,
        "is_completed": False,
    }
    body.update(overrides)
    return body


class FakeLocation:
    location_uuid = "11111111-1111-4111-8111-111111111111"


def test_the_send_guard_still_reports_a_start_mismatch_before_a_malformed_flag() -> None:
    """The extraction must not have reordered the send guard's refusals.

    ``check_api_response`` checks the start BEFORE it parses ``is_canceled``, so
    a body that is wrong in both ways reports the start mismatch. Sharing a code
    path with the handover's reader would have flipped that, changing what the
    outbox worker records for a real refusal.
    """
    from altegio_bot.easyweek_reminder_guard import GuardOutcome, check_api_response

    result = check_api_response(
        guard_body(start_time="2026-09-09T09:00:00Z", is_canceled="maybe"),
        booking_uuid=BOOKING,
        location=FakeLocation(),
        expected_start=datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc),
    )

    assert result.outcome is GuardOutcome.START_TIME_MISMATCH


def test_the_send_guard_still_proves_a_matching_booking() -> None:
    from altegio_bot.easyweek_reminder_guard import GuardOutcome, check_api_response

    result = check_api_response(
        guard_body(),
        booking_uuid=BOOKING,
        location=FakeLocation(),
        expected_start=datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc),
    )

    assert result.outcome is GuardOutcome.PROVEN_CURRENT


def test_the_handover_reader_reports_the_booking_as_it_is() -> None:
    """No expectation to compare against: it reads the state out."""
    from altegio_bot.easyweek_reminder_guard import ObservedBooking, read_booking_state

    observed = read_booking_state(guard_body(), booking_uuid=BOOKING, location=FakeLocation())

    assert isinstance(observed, ObservedBooking)
    assert observed.starts_at == datetime(2026, 9, 6, 12, 0, tzinfo=timezone.utc)
    assert observed.is_active is True


@pytest.mark.parametrize(
    ("override", "outcome"),
    [
        ({"is_canceled": True}, "canceled"),
        ({"is_completed": True}, "completed"),
    ],
    ids=["canceled", "completed"],
)
def test_the_handover_reader_reports_a_dead_booking_as_inactive(override: dict[str, Any], outcome: str) -> None:
    from altegio_bot.easyweek_reminder_guard import ObservedBooking, read_booking_state

    observed = read_booking_state(guard_body(**override), booking_uuid=BOOKING, location=FakeLocation())

    assert isinstance(observed, ObservedBooking)
    assert observed.is_active is False


@pytest.mark.parametrize(
    "override",
    [
        {"uuid": "bbbbbbbb-0000-4000-8000-000000000002"},
        {"location_uuid": "22222222-2222-4222-8222-222222222222"},
        {"is_canceled": "maybe"},
        {"start_time": None},
    ],
    ids=["other_booking", "other_branch", "unreadable_flag", "no_start"],
)
def test_the_handover_reader_refuses_rather_than_guessing(override: dict[str, Any]) -> None:
    from altegio_bot.easyweek_reminder_guard import GuardResult, read_booking_state

    result = read_booking_state(guard_body(**override), booking_uuid=BOOKING, location=FakeLocation())

    assert isinstance(result, GuardResult), "a refusal, never an optimistic read"
