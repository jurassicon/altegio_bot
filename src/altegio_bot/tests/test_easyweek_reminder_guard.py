"""PR-8: the reminder guard must refuse everything it cannot prove.

A reminder is planned hours or days before it fires. Between planning and
sending, the appointment can move, be cancelled, be completed, or turn out to
belong to a different branch — and none of those necessarily produce a webhook
we saw. The guard is the last thing standing between that and a real person
being told to show up.

So these tests are written from the refusal side. The happy path is one test;
everything else pins a way the guard could be talked into saying yes: a flag
that is the string ``"false"`` instead of ``False``, a timestamp with no
offset, a ``status.type`` that disagrees with the booleans, an exception class
that means "we could not ask" being read as "nothing is wrong".

No network: the reader is a fake, and it is a fake that satisfies a protocol
with exactly one read-only method — nothing here can even express a POST.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from altegio_bot.easyweek_client import (
    EasyWeekAuthError,
    EasyWeekConfigError,
    EasyWeekNotFoundError,
    EasyWeekPermanentError,
    EasyWeekProtocolError,
    EasyWeekRetryableError,
)
from altegio_bot.easyweek_locations import EasyWeekLocation
from altegio_bot.easyweek_multi_service import (
    MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH,
    WebhookServicePair,
    clear_multi_service_catalog_cache,
    multi_service_job_payload,
    prove_exactly_two_service_snapshot,
    record_raw_with_multi_service_snapshot,
)
from altegio_bot.easyweek_reminder_guard import (
    GuardOutcome,
    check_api_response,
    classify_client_error,
    verify_reminder_is_current,
)
from altegio_bot.easyweek_service_category import record_raw_with_services_count
from altegio_bot.models.models import PROVIDER_ALTEGIO, PROVIDER_EASYWEEK, MessageJob, Record

BOOKING = uuid.UUID("11111111-2222-4333-8444-555555555555")
OTHER_BOOKING = uuid.UUID("99999999-8888-4777-8666-555555555555")
LOCATION_UUID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
OTHER_LOCATION_UUID = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
COMPANY_ID = 999001
STARTS_AT = datetime(2026, 9, 14, 8, 30, tzinfo=timezone.utc)


def _location(location_uuid: str = LOCATION_UUID) -> EasyWeekLocation:
    return EasyWeekLocation(
        name="test-branch",
        location_id=COMPANY_ID,
        location_uuid=location_uuid,
        meta_template_prefix="tb",
        booking_page_url="https://booking.example.invalid/test",
    )


def _job(
    *,
    provider: str = PROVIDER_EASYWEEK,
    job_type: str = "reminder_24h",
    company_id: int = COMPANY_ID,
    record_id: int | None = 77,
    booking_uuid: object = str(BOOKING),
    record_starts_at: object = STARTS_AT.isoformat(),
) -> MessageJob:
    payload: dict[str, Any] = {}
    if booking_uuid is not _MISSING:
        payload["booking_uuid"] = booking_uuid
    if record_starts_at is not _MISSING:
        payload["record_starts_at"] = record_starts_at
    return MessageJob(
        provider=provider,
        company_id=company_id,
        record_id=record_id,
        job_type=job_type,
        status="processing",
        dedupe_key="eyw-reminder-guard-1",
        run_at=STARTS_AT - timedelta(hours=24),
        payload=payload,
    )


class _Missing:
    pass


_MISSING = _Missing()


def _record(
    *,
    provider: str = PROVIDER_EASYWEEK,
    company_id: int = COMPANY_ID,
    booking_uuid: object = BOOKING,
    starts_at: datetime | None = STARTS_AT,
    is_deleted: bool = False,
) -> Record:
    return Record(
        id=77,
        provider=provider,
        company_id=company_id,
        altegio_record_id=4200001,
        easyweek_booking_uuid=booking_uuid,
        client_id=1,
        staff_name="Tanja",
        starts_at=starts_at,
        is_deleted=is_deleted,
        raw={},
    )


def _api(**overrides: Any) -> dict[str, Any]:
    payload: dict[str, Any] = {
        "uuid": str(BOOKING),
        "location_uuid": LOCATION_UUID,
        "start_time": STARTS_AT.isoformat(),
        "is_canceled": False,
        "is_completed": False,
    }
    payload.update(overrides)
    return payload


class FakeReader:
    """One read-only method, recorded. It cannot express a mutation."""

    def __init__(self, payload: Any = None, *, error: Exception | None = None) -> None:
        self._payload = payload if payload is not None else _api()
        self._error = error
        self.calls: list[str] = []

    async def get_booking(self, booking_uuid: str) -> dict[str, Any]:
        self.calls.append(booking_uuid)
        if self._error is not None:
            raise self._error
        return self._payload


async def _verify(
    *,
    job: MessageJob | None = None,
    record: Record | None = _MISSING,  # type: ignore[assignment]
    location: EasyWeekLocation | None = _MISSING,  # type: ignore[assignment]
    reader: FakeReader | None = None,
):
    return await verify_reminder_is_current(
        job=job if job is not None else _job(),
        record=_record() if record is _MISSING else record,
        location=_location() if location is _MISSING else location,
        client=reader if reader is not None else FakeReader(),
    )


# ---------------------------------------------------------------------------
# The one way through
# ---------------------------------------------------------------------------


async def test_a_booking_that_matches_on_every_axis_is_proven() -> None:
    reader = FakeReader()
    result = await _verify(reader=reader)

    assert result.outcome is GuardOutcome.PROVEN_CURRENT
    assert result.proven is True
    assert reader.calls == [str(BOOKING)], "exactly one read-only lookup, by canonical uuid"


@pytest.mark.parametrize("job_type", ["reminder_24h", "reminder_2h"])
async def test_both_reminder_kinds_are_verified_the_same_way(job_type: str) -> None:
    result = await _verify(job=_job(job_type=job_type))
    assert result.proven is True


def _pair_line(line_uuid: str, name: str, price: int, duration: int) -> dict[str, Any]:
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


def _pair_catalog() -> list[dict[str, Any]]:
    return [
        {
            "uuid": "cccccccc-1111-4111-8111-111111111111",
            "name": "Erste Leistung",
            "currency": "EUR",
            "price": 3000,
            "duration": {"value": 30, "label": "minutes"},
            "category": {"name": "Wimpernverlängerung"},
        },
        {
            "uuid": "cccccccc-2222-4222-8222-222222222222",
            "name": "Zweite Leistung",
            "currency": "EUR",
            "price": 6500,
            "duration": {"value": 65, "label": "minutes"},
            "category": {"name": "Wimpernverlängerung"},
        },
    ]


def _pair_api(*, second_price: int = 6500, include_shadow: bool = False) -> dict[str, Any]:
    first = _pair_line("aaaaaaaa-1111-4111-8111-111111111111", "Erste Leistung", 3000, 30)
    second = _pair_line("aaaaaaaa-2222-4222-8222-222222222222", "Zweite Leistung", second_price, 65)
    ordered = [first, second]
    if include_shadow:
        ordered.insert(
            1,
            {**first, "uuid": "aaaaaaaa-3333-4333-8333-333333333333"},
        )
    total = 3000 + second_price
    return _api(
        currency="EUR",
        order={"subtotal": total, "total": total},
        ordered_services=ordered,
    )


def _stored_pair() -> tuple[MessageJob, Record]:
    snapshot = prove_exactly_two_service_snapshot(
        webhook=WebhookServicePair(
            booking_uuid=BOOKING,
            location_uuid=LOCATION_UUID,
            service_name="Erste Leistung",
            service_related="Zweite Leistung",
            services_description="Erste Leistung, Zweite Leistung",
            services_count=2,
            quantity=2,
            booking_currency="EUR",
            total_cost=Decimal("95.00"),
        ),
        booking_payload=_pair_api(),
        catalog_rows=_pair_catalog(),
    )
    record = _record()
    record.total_cost = Decimal("95.00")
    record.raw = record_raw_with_multi_service_snapshot(
        record_raw_with_services_count({}, 2),
        snapshot,
    )
    job = _job()
    job.payload.update(multi_service_job_payload(snapshot))
    return job, record


class MultiFakeReader(FakeReader):
    def __init__(self, payload: dict[str, Any], *, catalog_error: Exception | None = None) -> None:
        super().__init__(payload)
        self.catalog_error = catalog_error
        self.catalog_calls: list[tuple[str, int]] = []

    async def list_location_services(self, location_uuid: str, *, page: int) -> dict[str, Any]:
        self.catalog_calls.append((location_uuid, page))
        if self.catalog_error is not None:
            raise self.catalog_error
        rows = _pair_catalog()
        return {
            "data": rows,
            "meta": {"current_page": 1, "last_page": 1, "total": len(rows)},
        }


@pytest.mark.parametrize("job_type", ["reminder_24h", "reminder_2h"])
@pytest.mark.parametrize("include_shadow", [False, True])
async def test_multi_reminder_reproves_live_pair_and_ignores_exact_shadow(
    job_type: str,
    include_shadow: bool,
) -> None:
    clear_multi_service_catalog_cache()
    job, record = _stored_pair()
    job.job_type = job_type
    reader = MultiFakeReader(_pair_api(include_shadow=include_shadow))

    result = await _verify(job=job, record=record, reader=reader)

    assert result.outcome is GuardOutcome.PROVEN_CURRENT
    assert reader.calls == [str(BOOKING)]
    assert reader.catalog_calls == [(LOCATION_UUID, 1)]


async def test_recovery_snapshot_in_job_reproves_without_mutating_record() -> None:
    clear_multi_service_catalog_cache()
    job, record = _stored_pair()
    snapshot_payload = record.raw["easyweek"]["multi_service_snapshot"]
    record.raw = record_raw_with_services_count({}, 2)
    job.payload = {
        **job.payload,
        "multi_service_snapshot": snapshot_payload,
    }
    reader = MultiFakeReader(_pair_api())

    result = await _verify(job=job, record=record, reader=reader)

    assert result.outcome is GuardOutcome.PROVEN_CURRENT
    assert "multi_service_snapshot" not in record.raw["easyweek"]


async def test_changed_live_pair_cancels_stale_reminder() -> None:
    clear_multi_service_catalog_cache()
    job, record = _stored_pair()
    reader = MultiFakeReader(_pair_api(second_price=6600))

    result = await _verify(job=job, record=record, reader=reader)

    assert result.outcome is GuardOutcome.MULTI_SERVICE_MISMATCH
    assert result.proven is False


async def test_stale_job_digest_is_refused_before_live_api_or_catalog() -> None:
    clear_multi_service_catalog_cache()
    job, record = _stored_pair()
    job.payload["multi_service_snapshot_digest"] = "0" * 64
    reader = MultiFakeReader(_pair_api())

    result = await _verify(job=job, record=record, reader=reader)

    assert result.outcome is GuardOutcome.MULTI_SERVICE_MISMATCH
    assert result.reason.endswith(MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH)
    assert reader.calls == []
    assert reader.catalog_calls == []


async def test_catalog_unavailability_is_recoverable_for_multi_reminder() -> None:
    clear_multi_service_catalog_cache()
    job, record = _stored_pair()
    reader = MultiFakeReader(
        _pair_api(),
        catalog_error=EasyWeekRetryableError("no catalogue", operation="list_location_services"),
    )

    result = await _verify(job=job, record=record, reader=reader)

    assert result.outcome is GuardOutcome.RETRYABLE_UNAVAILABLE
    assert result.recoverable is True


@pytest.mark.parametrize(
    "start_form",
    [
        "2026-09-14T08:30:00+00:00",
        "2026-09-14T08:30:00Z",
        "2026-09-14T10:30:00+02:00",
    ],
)
async def test_the_same_instant_written_differently_still_matches(start_form: str) -> None:
    """Offsets are normalised; the comparison is of instants, not of strings."""
    result = await _verify(reader=FakeReader(_api(start_time=start_form)))
    assert result.proven is True


# ---------------------------------------------------------------------------
# Local refusals — no API call is spent on a job that is already wrong
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "job", "expected"),
    [
        ("altegio-job", _job(provider=PROVIDER_ALTEGIO), GuardOutcome.IDENTITY_MISMATCH),
        ("lifecycle-job", _job(job_type="record_created"), GuardOutcome.IDENTITY_MISMATCH),
        ("marketing-job", _job(job_type="review_3d"), GuardOutcome.IDENTITY_MISMATCH),
        ("no-record-id", _job(record_id=None), GuardOutcome.IDENTITY_MISMATCH),
        ("no-booking-uuid", _job(booking_uuid=_MISSING), GuardOutcome.IDENTITY_MISMATCH),
        ("null-booking-uuid", _job(booking_uuid=None), GuardOutcome.IDENTITY_MISMATCH),
        ("malformed-booking-uuid", _job(booking_uuid="not-a-uuid"), GuardOutcome.IDENTITY_MISMATCH),
        ("no-planned-start", _job(record_starts_at=_MISSING), GuardOutcome.START_TIME_MISMATCH),
        ("naive-planned-start", _job(record_starts_at="2026-09-14T08:30:00"), GuardOutcome.START_TIME_MISMATCH),
        ("garbage-planned-start", _job(record_starts_at="soon"), GuardOutcome.START_TIME_MISMATCH),
    ],
)
async def test_an_unusable_job_is_refused_without_an_api_call(
    label: str, job: MessageJob, expected: GuardOutcome
) -> None:
    reader = FakeReader()
    result = await _verify(job=job, reader=reader)

    assert result.outcome is expected, label
    assert reader.calls == [], f"{label}: a locally broken job must not cost an API call"


@pytest.mark.parametrize(
    ("label", "record", "expected"),
    [
        ("missing", None, GuardOutcome.IDENTITY_MISMATCH),
        ("altegio-record", _record(provider=PROVIDER_ALTEGIO), GuardOutcome.IDENTITY_MISMATCH),
        ("other-company", _record(company_id=COMPANY_ID + 1), GuardOutcome.IDENTITY_MISMATCH),
        ("other-booking", _record(booking_uuid=OTHER_BOOKING), GuardOutcome.IDENTITY_MISMATCH),
        ("no-booking-uuid", _record(booking_uuid=None), GuardOutcome.IDENTITY_MISMATCH),
        ("deleted", _record(is_deleted=True), GuardOutcome.CANCELED),
        ("no-start", _record(starts_at=None), GuardOutcome.START_TIME_MISMATCH),
        ("moved", _record(starts_at=STARTS_AT + timedelta(hours=1)), GuardOutcome.START_TIME_MISMATCH),
    ],
)
async def test_a_record_that_is_not_this_reminders_record_is_refused(
    label: str, record: Record | None, expected: GuardOutcome
) -> None:
    reader = FakeReader()
    result = await _verify(record=record, reader=reader)

    assert result.outcome is expected, label
    assert reader.calls == [], label


async def test_a_rescheduled_record_is_caught_before_the_api_is_asked() -> None:
    """The reminder names an hour the appointment no longer has."""
    result = await _verify(record=_record(starts_at=STARTS_AT + timedelta(days=1)))
    assert result.outcome is GuardOutcome.START_TIME_MISMATCH
    assert result.proven is False


@pytest.mark.parametrize(
    ("label", "location"),
    [
        ("not-in-registry", None),
        ("unusable-uuid", _location("not-a-uuid")),
    ],
)
async def test_a_branch_we_cannot_identify_is_refused(label: str, location: EasyWeekLocation | None) -> None:
    reader = FakeReader()
    result = await _verify(location=location, reader=reader)

    assert result.outcome is GuardOutcome.LOCATION_MISMATCH, label
    assert reader.calls == [], label


# ---------------------------------------------------------------------------
# The API answer, judged field by field
# ---------------------------------------------------------------------------


def _check(payload: object, *, location: EasyWeekLocation | None = None):
    return check_api_response(
        payload,
        booking_uuid=BOOKING,
        location=location if location is not None else _location(),
        expected_start=STARTS_AT,
    )


@pytest.mark.parametrize(
    ("label", "payload", "expected"),
    [
        ("not-an-object", ["a"], GuardOutcome.MALFORMED_RESPONSE),
        ("string-body", "ok", GuardOutcome.MALFORMED_RESPONSE),
        ("empty", {}, GuardOutcome.MALFORMED_RESPONSE),
        ("no-uuid", _api(uuid=None), GuardOutcome.MALFORMED_RESPONSE),
        ("malformed-uuid", _api(uuid="not-a-uuid"), GuardOutcome.MALFORMED_RESPONSE),
        ("other-uuid", _api(uuid=str(OTHER_BOOKING)), GuardOutcome.IDENTITY_MISMATCH),
        ("no-location", _api(location_uuid=None), GuardOutcome.MALFORMED_RESPONSE),
        ("malformed-location", _api(location_uuid="nope"), GuardOutcome.MALFORMED_RESPONSE),
        ("other-location", _api(location_uuid=OTHER_LOCATION_UUID), GuardOutcome.LOCATION_MISMATCH),
        ("no-start", _api(start_time=None), GuardOutcome.MALFORMED_RESPONSE),
        ("naive-start", _api(start_time="2026-09-14T08:30:00"), GuardOutcome.MALFORMED_RESPONSE),
        ("garbage-start", _api(start_time="tomorrow"), GuardOutcome.MALFORMED_RESPONSE),
        ("moved-start", _api(start_time="2026-09-14T09:30:00+00:00"), GuardOutcome.START_TIME_MISMATCH),
        ("canceled", _api(is_canceled=True), GuardOutcome.CANCELED),
        ("completed", _api(is_completed=True), GuardOutcome.COMPLETED),
    ],
)
def test_every_broken_or_contradicting_field_is_refused(label: str, payload: object, expected: GuardOutcome) -> None:
    assert _check(payload).outcome is expected, label


@pytest.mark.parametrize("flag", ["is_canceled", "is_completed"])
@pytest.mark.parametrize("value", ["false", "true", 0, 1, "", None, [], {}])
def test_a_flag_that_is_not_a_real_bool_is_never_read_by_truthiness(flag: str, value: object) -> None:
    """`"false"` is truthy; `0` is falsy. Both are "the API did not tell us"."""
    result = _check(_api(**{flag: value}))
    assert result.outcome is GuardOutcome.MALFORMED_RESPONSE, f"{flag}={value!r}"


@pytest.mark.parametrize("flag", ["is_canceled", "is_completed"])
def test_a_missing_flag_is_refused_rather_than_assumed_false(flag: str) -> None:
    payload = _api()
    del payload[flag]
    assert _check(payload).outcome is GuardOutcome.MALFORMED_RESPONSE


@pytest.mark.parametrize("status_type", ["canceled", "cancelled", "COMPLETED", "succeeded", "finished", "successful"])
def test_a_status_type_contradicting_the_booleans_fails_closed(status_type: str) -> None:
    """Two halves of one response disagree; neither is trusted."""
    result = _check(_api(status={"type": status_type}))
    assert result.outcome is GuardOutcome.MALFORMED_RESPONSE


@pytest.mark.parametrize("status", [{"type": "confirmed"}, {"type": 5}, {}, "confirmed", None])
def test_an_agreeing_or_unusable_status_block_does_not_block_a_proven_booking(status: object) -> None:
    """It is an optional cross-check, not a second source of truth."""
    assert _check(_api(status=status)).outcome is GuardOutcome.PROVEN_CURRENT


def test_the_localized_status_prose_is_never_consulted() -> None:
    """`booking_status` is salon-editable text."""
    result = _check(_api(booking_status="Storniert"))
    assert result.outcome is GuardOutcome.PROVEN_CURRENT


# ---------------------------------------------------------------------------
# Client failures: "we could not ask" is not "nothing is wrong"
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("label", "exc", "expected", "recoverable"),
    [
        ("timeout", EasyWeekRetryableError("t", operation="get_booking"), GuardOutcome.RETRYABLE_UNAVAILABLE, True),
        (
            "rate-limited",
            EasyWeekRetryableError("t", operation="get_booking", status_code=429),
            GuardOutcome.RETRYABLE_UNAVAILABLE,
            True,
        ),
        (
            "server-error",
            EasyWeekRetryableError("t", operation="get_booking", status_code=503),
            GuardOutcome.RETRYABLE_UNAVAILABLE,
            True,
        ),
        ("missing-key", EasyWeekConfigError("no key"), GuardOutcome.CONFIGURATION_UNAVAILABLE, True),
        (
            "unauthorized",
            EasyWeekAuthError("a", operation="get_booking", status_code=401),
            GuardOutcome.CONFIGURATION_UNAVAILABLE,
            True,
        ),
        (
            "forbidden",
            EasyWeekAuthError("a", operation="get_booking", status_code=403),
            GuardOutcome.CONFIGURATION_UNAVAILABLE,
            True,
        ),
        (
            "not-found",
            EasyWeekNotFoundError("n", operation="get_booking", status_code=404),
            GuardOutcome.NOT_FOUND,
            False,
        ),
        ("protocol", EasyWeekProtocolError("p", operation="get_booking"), GuardOutcome.MALFORMED_RESPONSE, False),
        (
            "permanent-4xx",
            EasyWeekPermanentError("p", operation="get_booking", status_code=422),
            GuardOutcome.PERMANENT_ERROR,
            False,
        ),
        ("unexpected", RuntimeError("boom"), GuardOutcome.PERMANENT_ERROR, False),
    ],
)
async def test_each_client_failure_maps_to_one_outcome(
    label: str, exc: Exception, expected: GuardOutcome, recoverable: bool
) -> None:
    result = await _verify(reader=FakeReader(error=exc))

    assert result.outcome is expected, label
    assert result.proven is False, label
    assert result.recoverable is recoverable, label


def test_only_the_two_could_not_ask_outcomes_are_recoverable() -> None:
    """Everything else means the reminder is provably wrong and must not retry."""
    from altegio_bot.easyweek_reminder_guard import RECOVERABLE_OUTCOMES

    assert RECOVERABLE_OUTCOMES == {
        GuardOutcome.RETRYABLE_UNAVAILABLE,
        GuardOutcome.CONFIGURATION_UNAVAILABLE,
    }
    # An unexpected exception class is evidence of nothing, so it is terminal.
    assert classify_client_error(RuntimeError()).recoverable is False


# ---------------------------------------------------------------------------
# Nothing from the booking escapes
# ---------------------------------------------------------------------------


SECRETS = [
    "Anna Müller",
    "+491701234567",
    "anna@example.invalid",
    "Wimpernverlängerung",
    "130.00",
    "Bearer super-secret",
]


async def test_no_verdict_ever_carries_a_value_from_the_response() -> None:
    """These strings reach MessageJob.last_error and the logs."""
    hostile = _api(
        uuid=str(OTHER_BOOKING),
        customer={"name": "Anna Müller", "phone": "+491701234567", "email": "anna@example.invalid"},
        service_name="Wimpernverlängerung",
        booking_price="13000",
        booking_price_formatted="€130.00",
    )
    result = await _verify(reader=FakeReader(hostile))

    assert result.outcome is GuardOutcome.IDENTITY_MISMATCH
    for leak in SECRETS:
        assert leak not in result.reason
    assert str(OTHER_BOOKING) not in result.reason, "not even a uuid"


async def test_a_client_exception_message_is_never_kept() -> None:
    exc = EasyWeekPermanentError(
        "https://my.easyweek.io/api/public/v2/bookings/11111111-2222-4333-8444-555555555555 rejected for Anna Müller",
        operation="get_booking",
        status_code=422,
    )
    result = await _verify(reader=FakeReader(error=exc))

    assert "Anna" not in result.reason
    assert "easyweek.io" not in result.reason
    assert str(BOOKING) not in result.reason


@pytest.mark.parametrize("outcome", list(GuardOutcome))
def test_every_reason_is_a_short_stable_prefixed_code(outcome: GuardOutcome) -> None:
    """Reasons are grepped and counted; they must not be prose."""
    from altegio_bot.easyweek_reminder_guard import _refuse

    result = _refuse(outcome, "field")
    assert result.reason.startswith("easyweek_reminder_guard:")
    assert outcome.value in result.reason
    assert len(result.reason) < 120
