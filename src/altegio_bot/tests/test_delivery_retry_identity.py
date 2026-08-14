from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from altegio_bot.delivery_retry_identity import (
    DELIVERY_RETRY_MAX_ATTEMPTS,
    POSTGRES_BIGINT_MAX,
    easyweek_reminder_retry_identity,
    parse_bounded_positive_int,
    resolve_retry_reference,
)
from altegio_bot.workers import outbox_worker as ow


def _retry_job(
    *,
    payload_id: object = 1,
    key_id: object = 1,
    payload_attempt: object = 1,
    key_attempt: object = 1,
    repeated_original: object | None = None,
    job_type: str = "record_created",
) -> SimpleNamespace:
    payload: dict[str, object] = {
        "kind": "delivery_failed_retry",
        "delivery_retry_of_outbox_id": payload_id,
        "delivery_retry_attempt": payload_attempt,
    }
    if repeated_original is not None:
        payload["delivery_retry_original_outbox_id"] = repeated_original
    return SimpleNamespace(
        job_type=job_type,
        dedupe_key=f"delivery_retry:{key_id}:{key_attempt}",
        payload=payload,
    )


@pytest.mark.parametrize(
    "value",
    [True, 1.0, 0, -1, "0", "-1", "01", " 1", "1 ", "1.0"],
)
def test_bounded_positive_integer_rejects_noncanonical_values(value: object) -> None:
    assert parse_bounded_positive_int(value, maximum=POSTGRES_BIGINT_MAX) is None


def test_maximum_postgres_bigint_is_accepted() -> None:
    result = resolve_retry_reference(
        _retry_job(
            payload_id=POSTGRES_BIGINT_MAX,
            key_id=POSTGRES_BIGINT_MAX,
        )
    )

    assert result.reference is not None
    assert result.reference.original_outbox_id == POSTGRES_BIGINT_MAX


@pytest.mark.parametrize(
    "value",
    [2**63, "9" * 100, "9" * 5000],
)
def test_payload_outbox_id_above_bigint_is_rejected_without_exception(value: object) -> None:
    result = resolve_retry_reference(_retry_job(payload_id=value))

    assert result.reference is None
    assert result.error == "invalid delivery_retry_of_outbox_id"


def test_oversized_outbox_id_in_dedupe_key_is_rejected() -> None:
    result = resolve_retry_reference(_retry_job(payload_id=1, key_id=2**63))

    assert result.reference is None
    assert result.error == "delivery_retry_dedupe_outbox_id_invalid"


def test_oversized_attempt_in_payload_is_rejected() -> None:
    result = resolve_retry_reference(_retry_job(payload_attempt=DELIVERY_RETRY_MAX_ATTEMPTS + 1))

    assert result.reference is None
    assert result.error == "delivery_retry_attempt_invalid"


def test_oversized_attempt_in_dedupe_key_is_rejected() -> None:
    result = resolve_retry_reference(
        _retry_job(
            payload_attempt=DELIVERY_RETRY_MAX_ATTEMPTS,
            key_attempt=DELIVERY_RETRY_MAX_ATTEMPTS + 1,
        )
    )

    assert result.reference is None
    assert result.error == "delivery_retry_dedupe_attempt_invalid"


def test_oversized_repeated_original_reference_is_rejected() -> None:
    result = resolve_retry_reference(_retry_job(repeated_original=2**63))

    assert result.reference is None
    assert result.error == "delivery_retry_original_reference_mismatch"


def test_retry_claim_with_campaign_job_type_is_rejected() -> None:
    result = resolve_retry_reference(_retry_job(job_type="campaign_execute_new_clients_monthly"))

    assert result.reference is None
    assert result.error == "delivery_retry_job_type_not_enabled"


@pytest.mark.asyncio
async def test_invalid_reference_is_rejected_before_any_sql_lookup() -> None:
    class _NoSQLSession:
        async def execute(self, *_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("invalid retry reference must not execute SQL")

        async def get(self, *_args: Any, **_kwargs: Any) -> Any:
            raise AssertionError("invalid retry reference must not load a row")

    reason = await ow._delivery_retry_presend_guard(
        _NoSQLSession(),  # type: ignore[arg-type]
        _retry_job(payload_id="9" * 5000),  # type: ignore[arg-type]
        None,
    )

    assert reason == "Canceled: invalid delivery_retry_of_outbox_id"


# ---------------------------------------------------------------------------
# PR-8: the identity a reminder retry inherits from its root job
# ---------------------------------------------------------------------------
#
# A reminder retry re-proves itself against the live EasyWeek API, and the guard
# needs two values to do it: the canonical booking uuid and the start instant
# the reminder was PLANNED for. Both must come from the root job — reading the
# current Record would quietly adopt a reschedule that happened between the send
# and the failed callback, and deliver a reminder the customer no longer owes.


def _reminder_job(**payload_overrides: object) -> SimpleNamespace:
    payload: dict[str, object] = {
        "provider": "easyweek",
        "booking_uuid": "11111111-2222-4333-8444-555555555555",
        "record_starts_at": "2026-09-14T08:30:00+00:00",
    }
    payload.update(payload_overrides)
    return SimpleNamespace(payload=payload)


def test_a_complete_root_payload_yields_the_two_inherited_values() -> None:
    identity = easyweek_reminder_retry_identity(_reminder_job())

    assert identity is not None
    assert identity.booking_uuid == "11111111-2222-4333-8444-555555555555"
    assert identity.record_starts_at == "2026-09-14T08:30:00+00:00"


def test_the_booking_uuid_is_canonicalised() -> None:
    """Upper case, braces and urn form all name one booking."""
    identity = easyweek_reminder_retry_identity(
        _reminder_job(booking_uuid="  {11111111-2222-4333-8444-555555555555}  ")
    )
    assert identity is not None
    assert identity.booking_uuid == "11111111-2222-4333-8444-555555555555"


def test_the_planned_start_is_normalised_to_utc() -> None:
    """The guard compares instants; two offsets for one moment must agree."""
    identity = easyweek_reminder_retry_identity(_reminder_job(record_starts_at="2026-09-14T10:30:00+02:00"))
    assert identity is not None
    assert identity.record_starts_at == "2026-09-14T08:30:00+00:00"


def test_a_trailing_z_is_accepted_as_utc() -> None:
    identity = easyweek_reminder_retry_identity(_reminder_job(record_starts_at="2026-09-14T08:30:00Z"))
    assert identity is not None
    assert identity.record_starts_at == "2026-09-14T08:30:00+00:00"


@pytest.mark.parametrize(
    ("label", "job"),
    [
        ("no-payload", SimpleNamespace(payload=None)),
        ("payload-not-a-dict", SimpleNamespace(payload=["a"])),
        ("no-job", None),
        ("missing-uuid", SimpleNamespace(payload={"record_starts_at": "2026-09-14T08:30:00+00:00"})),
        ("null-uuid", _reminder_job(booking_uuid=None)),
        ("malformed-uuid", _reminder_job(booking_uuid="not-a-uuid")),
        ("numeric-uuid", _reminder_job(booking_uuid=12345)),
        ("missing-start", SimpleNamespace(payload={"booking_uuid": "11111111-2222-4333-8444-555555555555"})),
        ("null-start", _reminder_job(record_starts_at=None)),
        ("naive-start", _reminder_job(record_starts_at="2026-09-14T08:30:00")),
        ("garbage-start", _reminder_job(record_starts_at="soon")),
        ("numeric-start", _reminder_job(record_starts_at=1757836200)),
    ],
)
def test_an_unusable_root_identity_is_refused_rather_than_patched_up(label: str, job: object) -> None:
    """Every one of these would build a retry on an identity nobody proved."""
    assert easyweek_reminder_retry_identity(job) is None, label
