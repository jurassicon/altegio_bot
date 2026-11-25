"""A zero-counter ``booking-succeeded`` must leave a visible excluded row.

The production case, in one sentence: EasyWeek delivered a processed
``booking-succeeded`` whose ``visits_total`` was the JSON integer ``0``, the
local counter stayed NULL, and the person vanished from the preview entirely —
the only trace was a number in an aggregate. An operator could see that a
delivery had been refused and could not see whose card to look at.

Everything below is synthetic. No real booking number, phone, UUID, name or
captured payload is used anywhere in this file.

What is asserted is the observable preview: the rows a `CampaignRun` ends up
with, their status, reason and source-proof columns, and the counters an
operator reads. Nothing here asserts an internal call order.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta
from typing import Any

import pytest
from sqlalchemy import func, select

import altegio_bot.campaigns.easyweek_segment as segment
import altegio_bot.campaigns.runner as runner
from altegio_bot.campaigns.easyweek_eligibility import OPTED_OUT
from altegio_bot.campaigns.easyweek_segment import build_easyweek_segment
from altegio_bot.campaigns.easyweek_zero_visit import (
    ZERO_VISITS_REPORTED,
    payload_reports_zero_visits_total,
    read_zero_visit_source_meta,
)
from altegio_bot.campaigns.runner import RunParams
from altegio_bot.models.models import (
    PROVIDER_ALTEGIO,
    PROVIDER_EASYWEEK,
    CampaignRecipient,
    CampaignRun,
    Client,
    EasyWeekEvent,
    MessageJob,
    OutboxMessage,
    Record,
)
from altegio_bot.tests.easyweek_zero_visit_fixtures import (
    CATEGORY,
    COMPANY_ID,
    LOCATION_UUID,
    NOW,
    OTHER_COMPANY_ID,
    OTHER_LOCATION_UUID,
    PERIOD_END,
    PERIOD_START,
    PG_INT_MAX,
    STAMP,
    START,
    Person,
    record_raw,
    registry,
    seed_person,
    succeeded_payload,
)


@pytest.fixture(autouse=True)
def _configuration(easyweek_zero_visit_configuration: None) -> None:
    """Every test in this module runs against the synthetic branch registry."""


async def _segment(session_maker, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(segment, "SessionLocal", session_maker)
    return await build_easyweek_segment(
        company_id=COMPANY_ID,
        period_start=PERIOD_START,
        period_end=PERIOD_END,
        registry=registry(),
        allowed_categories_raw=json.dumps([CATEGORY]),
        now=NOW,
    )


async def _preview(session_maker, monkeypatch: pytest.MonkeyPatch) -> int:
    """Create the preview through the ordinary operator entry point."""
    monkeypatch.setattr(segment, "SessionLocal", session_maker)
    monkeypatch.setattr(runner, "SessionLocal", session_maker)
    run = await runner.run_preview(
        RunParams(
            provider=PROVIDER_EASYWEEK,
            company_id=COMPANY_ID,
            location_id=COMPANY_ID,
            period_start=PERIOD_START,
            period_end=PERIOD_END,
            mode="preview",
            campaign_code="new_clients_monthly",
        )
    )
    return run.id


async def _rows(session_maker, run_id: int) -> list[CampaignRecipient]:
    async with session_maker() as session:
        return list(
            (
                await session.execute(
                    select(CampaignRecipient)
                    .where(CampaignRecipient.campaign_run_id == run_id)
                    .order_by(CampaignRecipient.id.asc())
                )
            )
            .scalars()
            .all()
        )


# ===========================================================================
# 1. The regression itself, through the ordinary preview path
# ===========================================================================


async def test_zero_counter_delivery_leaves_one_visible_excluded_row(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The defect: this person used to have no row in the preview at all."""
    person = await seed_person(session_maker, 1)

    run_id = await _preview(session_maker, monkeypatch)
    rows = await _rows(session_maker, run_id)

    assert len(rows) == 1
    row = rows[0]
    assert row.client_id == person.client_id
    assert row.provider == PROVIDER_EASYWEEK
    assert row.company_id == COMPANY_ID
    assert row.status == "skipped"
    assert row.excluded_reason == ZERO_VISITS_REPORTED
    # Reviewed and excluded: never an active recipient, and never source proof.
    assert row.recipient_basis == "earned_first_visit"
    assert row.auto_excluded_reason is None
    assert (
        row.source_easyweek_event_id,
        row.source_record_id,
        row.source_booking_uuid,
        row.source_visits_total,
        row.source_visits_total_updated_at,
    ) == (None, None, None, None, None)

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        assert run is not None
        assert run.status == "completed"
        # One person was looked at; none of them is in the audience.
        assert run.total_clients_seen == 1
        assert run.candidates_count == 0
        coverage = dict(run.meta["coverage"])
        assert coverage["zero_visits_total_reported_events"] == 1
        assert coverage["zero_visits_total_unique_clients"] == 1
        assert coverage["visits_total_out_of_range"] == 1
        assert coverage["eligible_proven_rows"] == 0
        assert coverage["eligible_unique_clients"] == 0
        assert run.meta["reason_counts"][ZERO_VISITS_REPORTED] == 1

        # The counter itself was not invented: no 1 was written anywhere.
        client = await session.get(Client, person.client_id)
        assert client is not None
        assert client.easyweek_visits_total is None
        assert client.easyweek_visits_total_updated_at is None

        # A preview is a preview: nothing was queued, sent or issued.
        assert int(await session.scalar(select(func.count()).select_from(MessageJob)) or 0) == 0
        assert int(await session.scalar(select(func.count()).select_from(OutboxMessage)) or 0) == 0
        assert row.loyalty_card_id is None
        assert row.message_job_id is None


async def test_segment_reports_the_row_without_any_source_proof(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """The positive control at segment level: reachable, and proof-free."""
    person = await seed_person(session_maker, 1)
    result = await _segment(session_maker, monkeypatch)

    assert len(result.candidates) == 1
    item = result.candidates[0]
    assert item.proof is None
    assert item.candidate.excluded_reason == ZERO_VISITS_REPORTED
    assert item.candidate.is_eligible is False
    assert item.candidate.client.id == person.client_id
    # The operator needs to recognise the person; the phone comes from the local
    # card through the shared normalizer, never from the payload.
    assert item.candidate.client.phone_e164 == person.phone


# ===========================================================================
# 2. The mixed set: every other decision and the nominal are unchanged
# ===========================================================================


async def test_mixed_set_keeps_the_active_audience_and_its_nominal(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    proven = await seed_person(session_maker, 1, visits_total=1, local_counter=1, local_stamp=STAMP)
    zero = await seed_person(session_maker, 2)
    opted = await seed_person(session_maker, 3, visits_total=1, local_counter=1, local_stamp=STAMP, opted_out=True)

    run_id = await _preview(session_maker, monkeypatch)
    rows = {row.client_id: row for row in await _rows(session_maker, run_id)}

    assert set(rows) == {proven.client_id, zero.client_id, opted.client_id}
    assert rows[proven.client_id].status == "candidate"
    assert rows[proven.client_id].excluded_reason is None
    assert rows[proven.client_id].source_visits_total == 1
    assert rows[opted.client_id].excluded_reason == OPTED_OUT
    assert rows[zero.client_id].excluded_reason == ZERO_VISITS_REPORTED

    async with session_maker() as session:
        run = await session.get(CampaignRun, run_id)
        assert run is not None
        assert run.total_clients_seen == 3
        # Exactly one recipient is active, so the voucher nominal covers one
        # person: the diagnostic row did not widen the audience.
        assert run.candidates_count == 1
        active = int(
            await session.scalar(
                select(func.count())
                .select_from(CampaignRecipient)
                .where(CampaignRecipient.campaign_run_id == run_id)
                .where(CampaignRecipient.status == "candidate")
                .where(CampaignRecipient.excluded_reason.is_(None))
            )
            or 0
        )
        assert active == 1


async def test_active_audience_is_identical_with_and_without_the_zero_delivery(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The nominal-safety control, stated as a comparison rather than a count."""
    proven = await seed_person(session_maker, 1, visits_total=1, local_counter=1, local_stamp=STAMP)
    first_run = await _preview(session_maker, monkeypatch)
    before = [
        (row.client_id, row.status, row.excluded_reason, row.source_visits_total)
        for row in await _rows(session_maker, first_run)
    ]

    await seed_person(session_maker, 2)
    second_run = await _preview(session_maker, monkeypatch)
    after = [
        (row.client_id, row.status, row.excluded_reason, row.source_visits_total)
        for row in await _rows(session_maker, second_run)
        if row.client_id == proven.client_id
    ]
    assert before == after


# ===========================================================================
# 3. Only the integer zero gets this treatment
# ===========================================================================


@pytest.mark.parametrize(
    "value",
    [
        pytest.param(False, id="bool-false"),
        pytest.param(True, id="bool-true"),
        pytest.param("0", id="string-zero"),
        pytest.param(0.0, id="float-zero"),
        pytest.param(None, id="null"),
        pytest.param(-1, id="negative"),
        pytest.param(PG_INT_MAX + 1, id="overflow"),
    ],
)
async def test_other_counter_shapes_get_no_diagnostic_row(
    session_maker, monkeypatch: pytest.MonkeyPatch, value: Any
) -> None:
    await seed_person(session_maker, 1, visits_total=value)
    result = await _segment(session_maker, monkeypatch)
    assert result.candidates == []
    assert result.reason_counts[ZERO_VISITS_REPORTED] == 0
    assert result.coverage["zero_visits_total_reported_events"] == 0


async def test_absent_counter_field_gets_no_diagnostic_row(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    await seed_person(session_maker, 1, omit_visits_total=True)
    result = await _segment(session_maker, monkeypatch)
    assert result.candidates == []
    assert result.reason_counts[ZERO_VISITS_REPORTED] == 0


@pytest.mark.parametrize(
    ("payload", "expected"),
    [
        ({"visits_total": 0}, True),
        ({"visits_total": False}, False),
        ({"visits_total": True}, False),
        ({"visits_total": "0"}, False),
        ({"visits_total": 0.0}, False),
        ({"visits_total": None}, False),
        ({"visits_total": -1}, False),
        ({"visits_total": 1}, False),
        ({}, False),
        ("not-a-dict", False),
        (None, False),
    ],
)
def test_zero_predicate_accepts_only_the_json_integer_zero(payload: object, expected: bool) -> None:
    assert payload_reports_zero_visits_total(payload) is expected


# ===========================================================================
# 4. The chain must be proven, or nobody is named
# ===========================================================================


async def test_unprocessed_delivery_is_not_attributed(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    await seed_person(session_maker, 1, event_status="pending")
    result = await _segment(session_maker, monkeypatch)
    assert result.candidates == []
    assert result.reason_counts[ZERO_VISITS_REPORTED] == 0


async def test_truncated_payload_is_not_attributed(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    await seed_person(session_maker, 1, truncated=True)
    result = await _segment(session_maker, monkeypatch)
    assert result.candidates == []
    assert result.reason_counts[ZERO_VISITS_REPORTED] == 0


async def test_booking_reassigned_to_another_client_is_not_attributed(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Record now points at a different card than the delivery named."""
    await seed_person(session_maker, 1, record_points_at_other_client=True)
    result = await _segment(session_maker, monkeypatch)
    assert result.candidates == []
    assert result.reason_counts[ZERO_VISITS_REPORTED] == 0


async def test_numeric_booking_id_mismatch_is_not_attributed(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    await seed_person(session_maker, 1, record_booking_id=777_111)
    result = await _segment(session_maker, monkeypatch)
    assert result.candidates == []
    assert result.reason_counts[ZERO_VISITS_REPORTED] == 0


async def test_customer_id_mismatch_is_not_attributed(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    await seed_person(session_maker, 1, client_customer_id=444_222, record_customer_id=444_222)
    result = await _segment(session_maker, monkeypatch)
    assert result.candidates == []
    assert result.reason_counts[ZERO_VISITS_REPORTED] == 0


async def test_record_of_another_branch_is_not_attributed(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    await seed_person(session_maker, 1, record_company=OTHER_COMPANY_ID, client_company=OTHER_COMPANY_ID)
    result = await _segment(session_maker, monkeypatch)
    assert result.candidates == []
    assert result.reason_counts[ZERO_VISITS_REPORTED] == 0


async def test_delivery_of_another_branch_is_not_in_this_preview(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    await seed_person(
        session_maker,
        1,
        payload_location_id=OTHER_COMPANY_ID,
        payload_location_uuid=OTHER_LOCATION_UUID,
    )
    result = await _segment(session_maker, monkeypatch)
    assert result.candidates == []
    assert result.coverage["succeeded_events_seen"] == 0


async def test_altegio_rows_are_never_attributed(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    await seed_person(session_maker, 1, record_provider=PROVIDER_ALTEGIO, client_provider=PROVIDER_ALTEGIO)
    result = await _segment(session_maker, monkeypatch)
    assert result.candidates == []
    assert result.reason_counts[ZERO_VISITS_REPORTED] == 0


async def test_deleted_source_record_keeps_its_own_reason(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    await seed_person(session_maker, 1, deleted=True)
    result = await _segment(session_maker, monkeypatch)
    assert result.candidates == []
    assert result.reason_counts[ZERO_VISITS_REPORTED] == 0


async def test_record_without_a_start_is_not_attributed(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    await seed_person(session_maker, 1, starts_at=None)  # type: ignore[arg-type]
    result = await _segment(session_maker, monkeypatch)
    assert result.candidates == []
    assert result.reason_counts[ZERO_VISITS_REPORTED] == 0


# ===========================================================================
# 5. The campaign period is the existing half-open interval
# ===========================================================================


@pytest.mark.parametrize(
    ("starts_at", "attributed"),
    [
        pytest.param(PERIOD_START, True, id="period-start-included"),
        pytest.param(PERIOD_END - timedelta(microseconds=1), True, id="just-before-end-included"),
        pytest.param(PERIOD_END, False, id="period-end-excluded"),
        pytest.param(PERIOD_START - timedelta(microseconds=1), False, id="just-before-start-excluded"),
    ],
)
async def test_period_is_the_half_open_interval_of_record_starts_at(
    session_maker, monkeypatch: pytest.MonkeyPatch, starts_at: datetime, attributed: bool
) -> None:
    await seed_person(session_maker, 1, starts_at=starts_at)
    result = await _segment(session_maker, monkeypatch)
    assert bool(result.candidates) is attributed
    assert result.reason_counts[ZERO_VISITS_REPORTED] == int(attributed)


# ===========================================================================
# 6. A replayed webhook is one more delivery, not one more person
# ===========================================================================


async def test_repeated_delivery_produces_one_row_and_one_reason(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    person = await seed_person(session_maker, 1, deliveries=3)
    result = await _segment(session_maker, monkeypatch)

    assert len(result.candidates) == 1
    assert result.candidates[0].candidate.client.id == person.client_id
    # Three deliveries, one person, one reason in the recipient breakdown.
    assert result.coverage["zero_visits_total_reported_events"] == 3
    assert result.coverage["zero_visits_total_unique_clients"] == 1
    assert result.reason_counts[ZERO_VISITS_REPORTED] == 1

    run_id = await _preview(session_maker, monkeypatch)
    rows = await _rows(session_maker, run_id)
    assert len(rows) == 1


# ===========================================================================
# 7. The diagnostic never overrides an existing verdict, in either order
# ===========================================================================


async def _seed_second_delivery(
    session_maker,
    person: Person,
    *,
    visits_total: Any,
    booking_uuid: uuid.UUID,
    booking_id: int,
    starts_at: datetime,
) -> None:
    """A second booking of the SAME customer, with its own delivery."""
    async with session_maker() as session:
        async with session.begin():
            record = Record(
                provider=PROVIDER_EASYWEEK,
                company_id=COMPANY_ID,
                altegio_record_id=booking_id,
                easyweek_booking_uuid=booking_uuid,
                client_id=person.client_id,
                altegio_client_id=person.customer_id,
                starts_at=starts_at,
                is_deleted=False,
                raw=record_raw(),
            )
            session.add(record)
            session.add(
                EasyWeekEvent(
                    status="processed",
                    event_hint="booking-succeeded",
                    body_truncated=False,
                    payload=succeeded_payload(
                        booking_uuid=booking_uuid,
                        booking_id=booking_id,
                        customer_id=person.customer_id,
                        visits_total=visits_total,
                    ),
                    payload_hash=f"event-{uuid.uuid4()}",
                )
            )


SECOND_BOOKING_UUID = uuid.UUID(int=0x2200_0001, version=4)


async def test_zero_then_valid_delivery_keeps_the_proven_candidate(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    person = await seed_person(session_maker, 1, visits_total=0, local_counter=1, local_stamp=STAMP)
    await _seed_second_delivery(
        session_maker,
        person,
        visits_total=1,
        booking_uuid=SECOND_BOOKING_UUID,
        booking_id=person.booking_id + 1,
        starts_at=START + timedelta(hours=2),
    )

    result = await _segment(session_maker, monkeypatch)

    assert len(result.candidates) == 1
    item = result.candidates[0]
    assert item.candidate.client.id == person.client_id
    assert item.candidate.excluded_reason is None
    assert item.proof is not None
    assert item.proof.visits_total == 1
    # The diagnostic did not fire for somebody the segmenter already decided on.
    assert result.reason_counts[ZERO_VISITS_REPORTED] == 0
    assert result.coverage["zero_visits_total_unique_clients"] == 0


async def test_valid_then_zero_delivery_keeps_the_proven_candidate(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The same conclusion with the deliveries captured the other way round."""
    person = await seed_person(session_maker, 1, visits_total=1, local_counter=1, local_stamp=STAMP)
    await _seed_second_delivery(
        session_maker,
        person,
        visits_total=0,
        booking_uuid=SECOND_BOOKING_UUID,
        booking_id=person.booking_id + 1,
        starts_at=START + timedelta(hours=2),
    )

    result = await _segment(session_maker, monkeypatch)

    assert len(result.candidates) == 1
    item = result.candidates[0]
    assert item.candidate.client.id == person.client_id
    assert item.proof is not None
    assert result.reason_counts[ZERO_VISITS_REPORTED] == 0


async def test_zero_delivery_does_not_bypass_an_existing_exclusion(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """An opted-out person keeps the opt-out reason, not the diagnostic one."""
    person = await seed_person(session_maker, 1, visits_total=1, local_counter=1, local_stamp=STAMP, opted_out=True)
    await _seed_second_delivery(
        session_maker,
        person,
        visits_total=0,
        booking_uuid=SECOND_BOOKING_UUID,
        booking_id=person.booking_id + 1,
        starts_at=START + timedelta(hours=3),
    )

    result = await _segment(session_maker, monkeypatch)

    assert len(result.candidates) == 1
    assert result.candidates[0].candidate.excluded_reason == OPTED_OUT
    assert result.candidates[0].proof is None
    assert result.reason_counts[ZERO_VISITS_REPORTED] == 0


async def test_two_people_one_zero_one_proven_are_both_reported(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    proven = await seed_person(session_maker, 1, visits_total=1, local_counter=1, local_stamp=STAMP)
    zero = await seed_person(session_maker, 2)

    result = await _segment(session_maker, monkeypatch)
    by_client = {item.candidate.client.id: item for item in result.candidates}

    assert set(by_client) == {proven.client_id, zero.client_id}
    assert by_client[proven.client_id].proof is not None
    assert by_client[zero.client_id].proof is None
    assert by_client[zero.client_id].candidate.excluded_reason == ZERO_VISITS_REPORTED
    assert result.coverage["eligible_unique_clients"] == 1
    assert result.coverage["zero_visits_total_unique_clients"] == 1


# ===========================================================================
# 8. The preview records which delivery the row came from — review round 1
# ===========================================================================


async def test_the_row_carries_the_binding_of_the_delivery_it_came_from(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Ids and a version, stored apart from the five earned-proof columns.

    The binding exists so that opening the row re-proves the chain the preview
    decided on, instead of re-deriving a new one that a later delivery could
    have changed.
    """
    person = await seed_person(session_maker, 1)

    run_id = await _preview(session_maker, monkeypatch)
    rows = await _rows(session_maker, run_id)

    stored = read_zero_visit_source_meta(rows[0].meta)
    assert stored is not None
    assert stored.event_id == person.event_ids[0]
    assert stored.record_id == person.record_id
    assert stored.booking_uuid == person.booking_uuid
    assert stored.booking_id == person.booking_id
    assert stored.customer_id == person.customer_id
    assert stored.distinct_sources == 1
    assert stored.reported_visits_total == 0
    # Separate from the proof, and the row still has none of it.
    assert rows[0].source_easyweek_event_id is None


async def test_a_replayed_delivery_is_still_one_source(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """Three deliveries of ONE booking are one source, not three."""
    person = await seed_person(session_maker, 1, deliveries=3)

    run_id = await _preview(session_maker, monkeypatch)
    rows = await _rows(session_maker, run_id)

    assert len(rows) == 1
    stored = read_zero_visit_source_meta(rows[0].meta)
    assert stored is not None
    assert stored.distinct_sources == 1
    # The earliest delivery of that booking is the one recorded.
    assert stored.event_id == min(person.event_ids)


async def test_two_zero_counter_bookings_record_the_ambiguity(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """Two different zero-counter visits for one card: recorded, not resolved."""
    person = await seed_person(session_maker, 1)
    second_booking = uuid.UUID(int=0x7100_0001, version=4)
    async with session_maker() as session:
        async with session.begin():
            session.add(
                Record(
                    provider=PROVIDER_EASYWEEK,
                    company_id=COMPANY_ID,
                    altegio_record_id=person.booking_id + 1,
                    easyweek_booking_uuid=second_booking,
                    client_id=person.client_id,
                    altegio_client_id=person.customer_id,
                    starts_at=START + timedelta(days=2),
                    is_deleted=False,
                    raw=record_raw(),
                )
            )
            session.add(
                EasyWeekEvent(
                    status="processed",
                    event_hint="booking-succeeded",
                    body_truncated=False,
                    payload=succeeded_payload(
                        booking_uuid=second_booking,
                        booking_id=person.booking_id + 1,
                        customer_id=person.customer_id,
                        visits_total=0,
                    ),
                    payload_hash=f"event-{uuid.uuid4()}",
                )
            )

    run_id = await _preview(session_maker, monkeypatch)
    rows = await _rows(session_maker, run_id)

    # Still one person, one row, one reason.
    assert len(rows) == 1
    stored = read_zero_visit_source_meta(rows[0].meta)
    assert stored is not None
    assert stored.distinct_sources == 2


async def test_the_diagnostic_row_claims_no_confirmed_in_category_visit(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """It never went through the evaluation, so it asserts nothing about it."""
    await seed_person(session_maker, 1)
    result = await _segment(session_maker, monkeypatch)

    candidate = result.candidates[0].candidate
    # The source record IS proven to exist inside the period.
    assert candidate.total_records_in_period == 1
    # Nothing else was checked, so nothing else is claimed.
    assert candidate.confirmed_records_in_period == 0
    assert candidate.lash_records_in_period == 0
    assert candidate.confirmed_lash_records_in_period == 0
    assert candidate.service_titles_in_period == []


async def test_a_proven_candidate_keeps_its_evaluated_counters(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """The ordinary earned row is untouched by the diagnostic's own shape."""
    await seed_person(session_maker, 1, visits_total=1, local_counter=1, local_stamp=STAMP)
    result = await _segment(session_maker, monkeypatch)

    candidate = result.candidates[0].candidate
    assert result.candidates[0].proof is not None
    assert result.candidates[0].zero_source is None
    assert candidate.confirmed_records_in_period == 1
    assert candidate.lash_records_in_period == 1
    assert candidate.confirmed_lash_records_in_period == 1


async def test_an_excluded_ordinary_row_carries_no_binding(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """Only the zero-counter diagnostic gets one."""
    await seed_person(session_maker, 1, visits_total=1, local_counter=1, local_stamp=STAMP, opted_out=True)

    run_id = await _preview(session_maker, monkeypatch)
    rows = await _rows(session_maker, run_id)

    assert len(rows) == 1
    assert rows[0].excluded_reason == OPTED_OUT
    assert read_zero_visit_source_meta(rows[0].meta) is None
    assert rows[0].meta == {}


# ===========================================================================
# 9. The contact source, recorded apart from the counter — review round 2
# ===========================================================================


async def test_the_binding_records_the_contact_source_separately(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Which delivery carried the counter and which carried the contact.

    Two different questions, so two different fields. The number itself is not
    one of them: a preview snapshot is not a contact store, and the check can
    read it back from the delivery the binding names.
    """
    person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=True)

    run_id = await _preview(session_maker, monkeypatch)
    rows = await _rows(session_maker, run_id)

    stored = read_zero_visit_source_meta(rows[0].meta)
    assert stored is not None
    assert stored.event_id == person.event_ids[0]
    assert stored.contact_sources == 1
    # The zero-counter delivery carried it itself, so no pointer is needed.
    assert stored.contact_event_id is None
    assert person.phone not in json.dumps(rows[0].meta or {})


async def test_a_booking_with_no_captured_contact_records_none(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """Zero contacts is a recorded fact, not an absent field to be guessed at."""
    await seed_person(session_maker, 1, customer_uuid=None, capture_phone=False)

    run_id = await _preview(session_maker, monkeypatch)
    rows = await _rows(session_maker, run_id)

    stored = read_zero_visit_source_meta(rows[0].meta)
    assert stored is not None
    assert stored.contact_sources == 0
    assert stored.contact_event_id is None


async def test_the_period_count_follows_records_not_deliveries(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """Three deliveries of one booking are one record in the period."""
    await seed_person(session_maker, 1, deliveries=3)
    result = await _segment(session_maker, monkeypatch)

    candidate = result.candidates[0].candidate
    assert candidate.total_records_in_period == 1
    assert candidate.confirmed_records_in_period == 0


# ===========================================================================
# 10. The archive is read once per preview — review round 3, item 4
# ===========================================================================
# The contact rule asks which of a booking's deliveries carried a contact. That
# question used to be asked per recipient, and its predicate — this booking uuid
# plus ``status = 'processed'`` — is served by no index this schema has: the one
# index on ``booking_uuid`` is partial on the two NON-terminal statuses. So the
# preview scanned the delivery archive once for every zero-counter person in it.
# It is now one read for the whole phase; what the rule itself accepts and
# refuses is unchanged and covered above.


async def _contact_sibling(
    session_maker,
    person: Person,
    *,
    phone: str,
    booking_uuid: uuid.UUID | None = None,
    status: str = "processed",
    location_id: int = COMPANY_ID,
    location_uuid: str = LOCATION_UUID,
    event_hint: str = "booking-created",
) -> int:
    """One more processed delivery of (nominally) the same booking."""
    identity = booking_uuid or person.booking_uuid
    async with session_maker() as session:
        async with session.begin():
            event = EasyWeekEvent(
                status=status,
                event_hint=event_hint,
                booking_uuid=identity,
                body_truncated=False,
                payload={
                    "uid": str(identity),
                    "id": person.booking_id,
                    "location_id": location_id,
                    "location_uuid": location_uuid,
                    "customer_id": person.customer_id,
                    "customer_phone": phone,
                },
                payload_hash=f"sibling-{uuid.uuid4()}",
            )
            session.add(event)
            await session.flush()
            return int(event.id)


def _statement_counter(engine, needle: str) -> tuple[dict[str, int], Any]:
    """Count the statements that touch one table, with their own listener."""
    from sqlalchemy import event as sa_event

    counted = {"statements": 0}

    def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
        if needle in statement.lower():
            counted["statements"] += 1

    sa_event.listen(engine.sync_engine, "after_cursor_execute", after_cursor_execute)
    return counted, after_cursor_execute


async def _contact_reads_during_preview(session_maker, engine, monkeypatch: pytest.MonkeyPatch) -> tuple[int, int]:
    """``(reads of the delivery archive, rows the preview produced)``."""
    from sqlalchemy import event as sa_event

    counted, listener = _statement_counter(engine, "from easyweek_events")
    try:
        result = await _segment(session_maker, monkeypatch)
    finally:
        sa_event.remove(engine.sync_engine, "after_cursor_execute", listener)
    diagnostic = [item for item in result.candidates if item.candidate.excluded_reason == ZERO_VISITS_REPORTED]
    return counted["statements"], len(diagnostic)


async def test_the_delivery_archive_is_not_read_once_per_recipient(
    session_maker, engine, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Round 3, item 4: three diagnostic rows must not cost three archive reads.

    A count of statements, not a stopwatch: the point is that the number of
    reads does not follow the number of recipients, and a timing threshold
    would say nothing about that on a machine of a different speed.
    """
    await seed_person(session_maker, 1, customer_uuid=None, capture_phone=True)
    one_reads, one_rows = await _contact_reads_during_preview(session_maker, engine, monkeypatch)

    for index in (2, 3):
        await seed_person(session_maker, index, customer_uuid=None, capture_phone=True)
    three_reads, three_rows = await _contact_reads_during_preview(session_maker, engine, monkeypatch)

    assert (one_rows, three_rows) == (1, 3)
    assert one_reads > 0
    assert three_reads == one_reads


async def test_the_batched_read_still_finds_each_booking_s_own_contact(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One read for several bookings must not mix their deliveries up.

    Two people, each with a sibling delivery carrying their own number, and a
    third sibling carrying a number for a booking nobody in this preview owns.
    Each row must record its own contact, and neither may borrow the other's.
    """
    first = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=False)
    second = await seed_person(session_maker, 2, customer_uuid=None, capture_phone=False)
    first_contact = await _contact_sibling(session_maker, first, phone=first.phone)
    second_contact = await _contact_sibling(session_maker, second, phone=second.phone)
    # A delivery of a booking this preview knows nothing about.
    await _contact_sibling(
        session_maker,
        first,
        phone="+4915100088888",
        booking_uuid=uuid.UUID(int=0x5150_0001, version=4),
    )

    run_id = await _preview(session_maker, monkeypatch)
    rows = await _rows(session_maker, run_id)

    bindings = {row.client_id: read_zero_visit_source_meta(row.meta) for row in rows}
    assert len(bindings) == 2
    assert bindings[first.client_id] is not None
    assert bindings[second.client_id] is not None
    assert bindings[first.client_id].contact_event_id == first_contact
    assert bindings[second.client_id].contact_event_id == second_contact
    assert {binding.contact_sources for binding in bindings.values()} == {1}


async def test_a_delivery_of_another_branch_is_still_no_contact(session_maker, monkeypatch: pytest.MonkeyPatch) -> None:
    """Every check the per-recipient read made is still made: the branch."""
    person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=False)
    await _contact_sibling(
        session_maker,
        person,
        phone=person.phone,
        location_id=OTHER_COMPANY_ID,
        location_uuid=OTHER_LOCATION_UUID,
    )

    run_id = await _preview(session_maker, monkeypatch)
    rows = await _rows(session_maker, run_id)

    stored = read_zero_visit_source_meta(rows[0].meta)
    assert stored is not None
    assert stored.contact_sources == 0
    assert stored.contact_event_id is None


async def test_an_unprocessed_sibling_delivery_is_still_no_contact(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """And the status: a delivery that was never processed proves nothing."""
    person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=False)
    await _contact_sibling(session_maker, person, phone=person.phone, status="captured")

    run_id = await _preview(session_maker, monkeypatch)
    rows = await _rows(session_maker, run_id)

    stored = read_zero_visit_source_meta(rows[0].meta)
    assert stored is not None
    assert stored.contact_sources == 0


async def test_contradictory_contacts_are_still_recorded_as_such(
    session_maker, monkeypatch: pytest.MonkeyPatch
) -> None:
    """And the contradiction: two numbers for one booking choose nobody."""
    person = await seed_person(session_maker, 1, customer_uuid=None, capture_phone=True)
    await _contact_sibling(session_maker, person, phone="+4915100066666")

    run_id = await _preview(session_maker, monkeypatch)
    rows = await _rows(session_maker, run_id)

    stored = read_zero_visit_source_meta(rows[0].meta)
    assert stored is not None
    assert stored.contact_sources == 2
    assert stored.contact_event_id is None
