from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from altegio_bot.delivery_retry_identity import (
    DELIVERY_RETRY_MAX_ATTEMPTS,
    POSTGRES_BIGINT_MAX,
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
