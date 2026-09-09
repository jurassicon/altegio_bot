from __future__ import annotations

import copy
import json
import uuid
from decimal import Decimal

import pytest

from altegio_bot.easyweek_client import EasyWeekRetryableError
from altegio_bot.easyweek_multi_service import (
    MULTI_SERVICE_BUSINESS_COUNT_MISMATCH,
    MULTI_SERVICE_CATALOG_MATCH_MISSING,
    MULTI_SERVICE_CATEGORY_AMBIGUOUS,
    MULTI_SERVICE_CATEGORY_NOT_ALLOWED,
    MULTI_SERVICE_CURRENCY_MISMATCH,
    MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED,
    MULTI_SERVICE_CUSTOM_PRICE_UNSUPPORTED,
    MULTI_SERVICE_DESCRIPTION_MISMATCH,
    MULTI_SERVICE_DISCOUNT_UNSUPPORTED,
    MULTI_SERVICE_DUPLICATE_AMBIGUOUS,
    MULTI_SERVICE_JOB_DIGEST_KEY,
    MULTI_SERVICE_NAMES_NOT_DISTINCT,
    MULTI_SERVICE_QUANTITY_UNSUPPORTED,
    MULTI_SERVICE_RELATED_MISSING,
    MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH,
    MULTI_SERVICE_SNAPSHOT_MISSING,
    MULTI_SERVICE_TOTAL_MISMATCH,
    MultiServiceProofError,
    ServiceEligibilityPurpose,
    WebhookServicePair,
    clear_multi_service_catalog_cache,
    evaluate_service_eligibility,
    fetch_and_prove_exactly_two_service_snapshot,
    multi_service_job_payload,
    multi_service_send_guard,
    multi_service_snapshot_from_record_raw,
    prove_exactly_two_service_snapshot,
    read_catalog_rows_cached,
    record_raw_with_multi_service_snapshot,
)

BOOKING_UUID = uuid.UUID("11111111-2222-4333-8444-555555555555")
OTHER_BOOKING_UUID = uuid.UUID("99999999-2222-4333-8444-555555555555")
LOCATION_UUID = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
LINE_A_UUID = "10000000-0000-4000-8000-000000000001"
LINE_B_UUID = "10000000-0000-4000-8000-000000000002"
CATEGORY_UUID = "20000000-0000-4000-8000-000000000001"


def _duration(minutes: int) -> dict[str, object]:
    return {"value": minutes, "label": "minutes", "iso_8601": f"PT{minutes}M"}


def _line(name: str, price: int, minutes: int, line_uuid: str) -> dict[str, object]:
    return {
        "uuid": line_uuid,
        "name": name,
        "quantity": 1,
        "currency": "EUR",
        "price": price,
        "original_price": price,
        "discount": 0,
        "duration": _duration(minutes),
        "original_duration": _duration(minutes),
        "price_formatted": f"{price / 100:.2f} €",
    }


def _pair(**overrides: object) -> WebhookServicePair:
    values: dict[str, object] = {
        "booking_uuid": BOOKING_UUID,
        "location_uuid": LOCATION_UUID,
        "service_name": "Erste Leistung",
        "service_related": "Zweite Leistung",
        "services_description": "Erste Leistung, Zweite Leistung",
        "services_count": 2,
        "quantity": 2,
        "booking_currency": "EUR",
        "total_cost": Decimal("95.00"),
    }
    values.update(overrides)
    return WebhookServicePair(**values)  # type: ignore[arg-type]


def _booking(*, duplicate: bool = False) -> dict[str, object]:
    first = _line("Erste Leistung", 3000, 30, LINE_A_UUID)
    second = _line("Zweite Leistung", 6500, 65, LINE_B_UUID)
    rows = [first, second]
    if duplicate:
        shadow = copy.deepcopy(first)
        shadow["uuid"] = "10000000-0000-4000-8000-000000000003"
        rows = [first, shadow, second]
    return {
        "uuid": str(BOOKING_UUID),
        "location_uuid": LOCATION_UUID,
        "currency": "EUR",
        "order": {"subtotal": 9500, "total": 9500},
        "ordered_services": rows,
    }


def _catalog(*, first_category: str = "Wimpernverlängerung", second_category: str | None = None):
    second_category = second_category or first_category
    return [
        {
            "uuid": "30000000-0000-4000-8000-000000000001",
            "name": "Erste Leistung",
            "currency": "EUR",
            "price": 3000,
            "duration": _duration(30),
            "category": {"uuid": CATEGORY_UUID, "name": first_category},
        },
        {
            "uuid": "30000000-0000-4000-8000-000000000002",
            "name": "Zweite Leistung",
            "currency": "EUR",
            "price": 6500,
            "duration": _duration(65),
            "category": {"uuid": CATEGORY_UUID, "name": second_category},
        },
    ]


def _prove(*, pair: WebhookServicePair | None = None, booking: object | None = None, catalog=None):
    return prove_exactly_two_service_snapshot(
        webhook=pair or _pair(),
        booking_payload=_booking() if booking is None else booking,
        catalog_rows=_catalog() if catalog is None else catalog,
    )


def _reason(**kwargs: object) -> str:
    with pytest.raises(MultiServiceProofError) as caught:
        _prove(**kwargs)  # type: ignore[arg-type]
    return caught.value.reason


def test_two_business_lines_are_kept_in_order_with_their_actual_prices() -> None:
    snapshot = _prove()
    assert [line.display_name for line in snapshot.lines] == ["Erste Leistung", "Zweite Leistung"]
    assert [line.actual_price_minor for line in snapshot.lines] == [3000, 6500]
    assert snapshot.total_minor == 9500


def test_one_full_resource_shadow_is_collapsed_without_doubling_total_or_digest() -> None:
    ordinary = _prove()
    duplicated = _prove(booking=_booking(duplicate=True))
    assert duplicated.lines == ordinary.lines
    assert duplicated.total_minor == 9500
    assert duplicated.digest == ordinary.digest


def test_two_identical_returned_lines_are_not_assumed_to_be_a_shadow() -> None:
    booking = _booking()
    rows = booking["ordered_services"]
    assert isinstance(rows, list)
    rows[1] = copy.deepcopy(rows[0])
    rows[1]["uuid"] = LINE_B_UUID
    assert _reason(booking=booking) == MULTI_SERVICE_DUPLICATE_AMBIGUOUS


def test_three_different_lines_and_four_lines_fail_closed() -> None:
    booking = _booking()
    rows = booking["ordered_services"]
    assert isinstance(rows, list)
    rows.append(_line("Dritte Leistung", 0, 10, "10000000-0000-4000-8000-000000000004"))
    assert _reason(booking=booking) == MULTI_SERVICE_DUPLICATE_AMBIGUOUS
    rows.append(copy.deepcopy(rows[-1]))
    assert _reason(booking=booking) == MULTI_SERVICE_BUSINESS_COUNT_MISMATCH

    two_groups = _booking(duplicate=True)
    grouped_rows = two_groups["ordered_services"]
    assert isinstance(grouped_rows, list)
    second_shadow = copy.deepcopy(grouped_rows[-1])
    second_shadow["uuid"] = "10000000-0000-4000-8000-000000000005"
    grouped_rows.append(second_shadow)
    assert _reason(booking=two_groups) == MULTI_SERVICE_BUSINESS_COUNT_MISMATCH


def test_same_name_with_different_price_is_not_collapsed_as_a_duplicate() -> None:
    booking = _booking(duplicate=True)
    rows = booking["ordered_services"]
    assert isinstance(rows, list)
    rows[1]["price"] = 3100
    rows[1]["original_price"] = 3100
    assert _reason(booking=booking) == MULTI_SERVICE_DUPLICATE_AMBIGUOUS


@pytest.mark.parametrize("value", [None, "1", True, False, 0, 2])
def test_every_unproven_order_line_quantity_fails(value: object) -> None:
    booking = _booking()
    rows = booking["ordered_services"]
    assert isinstance(rows, list)
    rows[0]["quantity"] = value
    assert _reason(booking=booking) == MULTI_SERVICE_QUANTITY_UNSUPPORTED


def test_missing_order_line_quantity_fails() -> None:
    booking = _booking()
    rows = booking["ordered_services"]
    assert isinstance(rows, list)
    del rows[0]["quantity"]
    assert _reason(booking=booking) == MULTI_SERVICE_QUANTITY_UNSUPPORTED


@pytest.mark.parametrize("value", [None, "2", True, False, 0, 1, 3])
def test_webhook_quantity_must_be_the_exact_integer_two(value: object) -> None:
    assert _reason(pair=_pair(quantity=value)) == "multi_service_webhook_shape_unproven"


@pytest.mark.parametrize("value", [None, "0", False, 1])
def test_discount_must_be_the_exact_integer_zero(value: object) -> None:
    booking = _booking()
    rows = booking["ordered_services"]
    assert isinstance(rows, list)
    rows[0]["discount"] = value
    assert _reason(booking=booking) == MULTI_SERVICE_DISCOUNT_UNSUPPORTED


def test_custom_price_and_duration_fail_closed() -> None:
    booking = _booking()
    rows = booking["ordered_services"]
    assert isinstance(rows, list)
    rows[0]["original_price"] = 4000
    assert _reason(booking=booking) == MULTI_SERVICE_CUSTOM_PRICE_UNSUPPORTED

    booking = _booking()
    rows = booking["ordered_services"]
    assert isinstance(rows, list)
    rows[0]["original_duration"] = _duration(45)
    assert _reason(booking=booking) == MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED


def test_currency_and_all_three_totals_must_agree() -> None:
    booking = _booking()
    rows = booking["ordered_services"]
    assert isinstance(rows, list)
    rows[1]["currency"] = "USD"
    assert _reason(booking=booking) == MULTI_SERVICE_CURRENCY_MISMATCH

    for target in ("subtotal", "total"):
        booking = _booking()
        order = booking["order"]
        assert isinstance(order, dict)
        order[target] = 9400
        assert _reason(booking=booking) == MULTI_SERVICE_TOTAL_MISMATCH
    assert _reason(pair=_pair(total_cost=Decimal("94.00"))) == MULTI_SERVICE_TOTAL_MISMATCH


@pytest.mark.parametrize("value", [None, "", "  ", 7, ["Zweite Leistung"]])
def test_related_service_is_required_as_a_scalar_nonblank_string(value: object) -> None:
    assert _reason(pair=_pair(service_related=value)) == MULTI_SERVICE_RELATED_MISSING


def test_webhook_names_must_be_distinct_after_unicode_normalization() -> None:
    assert (
        _reason(
            pair=_pair(
                service_name="Wimpernverlängerung",
                service_related="Wimpernverla\u0308ngerung",
                services_description="Wimpernverlängerung, Wimpernverla\u0308ngerung",
            )
        )
        == MULTI_SERVICE_NAMES_NOT_DISTINCT
    )


def test_description_is_an_exact_whole_set_not_a_comma_split() -> None:
    assert _reason(pair=_pair(services_description="Erste Leistung,Zweite Leistung,Dritte")) == (
        MULTI_SERVICE_DESCRIPTION_MISMATCH
    )


def test_booking_and_location_identity_must_match() -> None:
    booking = _booking()
    booking["uuid"] = "99999999-2222-4333-8444-555555555555"
    assert _reason(booking=booking) == "multi_service_webhook_shape_unproven"
    booking = _booking()
    booking["location_uuid"] = "bbbbbbbb-bbbb-4ccc-8ddd-eeeeeeeeeeee"
    assert _reason(booking=booking) == "multi_service_webhook_shape_unproven"


def test_catalog_matches_each_name_and_requires_unanimous_categories() -> None:
    catalog = _catalog()
    duplicate = copy.deepcopy(catalog[0])
    duplicate["uuid"] = "30000000-0000-4000-8000-000000000003"
    catalog.append(duplicate)
    assert _prove(catalog=catalog).lines[0].category == "Wimpernverlängerung"

    duplicate["category"] = {"uuid": CATEGORY_UUID, "name": "Nagelservice"}
    assert _reason(catalog=catalog) == MULTI_SERVICE_CATEGORY_AMBIGUOUS


def test_missing_catalog_match_fails_closed() -> None:
    assert _reason(catalog=_catalog()[:1]) == MULTI_SERVICE_CATALOG_MATCH_MISSING


def test_all_categories_policy_never_uses_any_category() -> None:
    snapshot = _prove(catalog=_catalog(second_category="Nagelservice"))
    raw = record_raw_with_multi_service_snapshot({"easyweek": {"services_count": 2}}, snapshot)
    result = evaluate_service_eligibility(
        record_raw=raw,
        allowed_categories_raw=json.dumps(["Wimpernverlängerung"]),
        purpose=ServiceEligibilityPurpose.LIFECYCLE_REMINDER,
    )
    assert result.allowed is False
    assert result.reason == MULTI_SERVICE_CATEGORY_NOT_ALLOWED


def test_single_only_purpose_still_rejects_a_proven_pair() -> None:
    raw = record_raw_with_multi_service_snapshot({"easyweek": {"services_count": 2}}, _prove())
    result = evaluate_service_eligibility(
        record_raw=raw,
        allowed_categories_raw=json.dumps(["Wimpernverlängerung"]),
        purpose=ServiceEligibilityPurpose.SINGLE_SERVICE_ONLY,
    )
    assert result.allowed is False
    assert result.reason == "category_ambiguous_multi_service"


def test_snapshot_round_trip_preserves_unrelated_raw_and_detects_corruption() -> None:
    snapshot = _prove()
    original = {"outside": {"kept": True}, "easyweek": {"services_count": 2, "other": "kept"}}
    raw = record_raw_with_multi_service_snapshot(original, snapshot)
    parsed, reason = multi_service_snapshot_from_record_raw(raw)
    assert reason is None and parsed == snapshot
    assert original == {"outside": {"kept": True}, "easyweek": {"services_count": 2, "other": "kept"}}

    corrupt = copy.deepcopy(raw)
    corrupt["easyweek"]["multi_service_snapshot"]["lines"][0]["actual_price_minor"] += 1
    assert multi_service_snapshot_from_record_raw(corrupt) == (None, MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH)


def test_job_digest_must_match_current_snapshot_and_total() -> None:
    snapshot = _prove()
    raw = record_raw_with_multi_service_snapshot({"easyweek": {"services_count": 2}}, snapshot)
    payload = multi_service_job_payload(snapshot)
    identity = {
        "expected_booking_uuid": BOOKING_UUID,
        "expected_location_uuid": LOCATION_UUID,
    }
    assert payload[MULTI_SERVICE_JOB_DIGEST_KEY] == snapshot.digest
    assert (
        multi_service_send_guard(
            record_raw=raw,
            job_payload=payload,
            record_total_cost=Decimal("95.00"),
            **identity,
        )
        is None
    )

    stale = dict(payload)
    stale[MULTI_SERVICE_JOB_DIGEST_KEY] = "0" * 64
    assert (
        multi_service_send_guard(
            record_raw=raw,
            job_payload=stale,
            record_total_cost=Decimal("95.00"),
            **identity,
        )
        == MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
    )
    assert (
        multi_service_send_guard(
            record_raw={},
            job_payload=payload,
            record_total_cost=Decimal("95.00"),
            **identity,
        )
        == MULTI_SERVICE_SNAPSHOT_MISSING
    )

    recovery_payload = multi_service_job_payload(snapshot, include_snapshot=True)
    assert (
        multi_service_send_guard(
            record_raw={"easyweek": {"services_count": 2}},
            job_payload=recovery_payload,
            record_total_cost=Decimal("95.00"),
            **identity,
        )
        is None
    )
    corrupt_recovery = copy.deepcopy(recovery_payload)
    corrupt_recovery["multi_service_snapshot"]["lines"][0]["actual_price_minor"] += 1
    assert (
        multi_service_send_guard(
            record_raw={"easyweek": {"services_count": 2}},
            job_payload=corrupt_recovery,
            record_total_cost=Decimal("95.00"),
            **identity,
        )
        == MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
    )
    assert (
        multi_service_send_guard(
            record_raw=raw,
            job_payload=payload,
            record_total_cost=Decimal("95.00"),
            expected_booking_uuid=OTHER_BOOKING_UUID,
            expected_location_uuid=LOCATION_UUID,
        )
        == MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
    )


class _UnavailableReader:
    async def get_booking(self, booking_uuid: str):  # noqa: ARG002
        raise EasyWeekRetryableError("unavailable", operation="get_booking")

    async def list_location_services(self, location_uuid: str, *, page: int):  # pragma: no cover
        raise AssertionError


async def test_api_unavailability_is_recoverable_and_pii_free() -> None:
    with pytest.raises(MultiServiceProofError) as caught:
        await fetch_and_prove_exactly_two_service_snapshot(client=_UnavailableReader(), webhook=_pair())
    assert caught.value.reason == "multi_service_api_unavailable"
    assert caught.value.recoverable is True
    assert "Erste Leistung" not in str(caught.value)


class _CatalogUnavailableReader:
    async def get_booking(self, booking_uuid: str):  # noqa: ARG002
        return _booking()

    async def list_location_services(self, location_uuid: str, *, page: int):  # noqa: ARG002
        raise EasyWeekRetryableError("unavailable", operation="list_location_services")


async def test_catalog_unavailability_is_recoverable_and_never_allows() -> None:
    clear_multi_service_catalog_cache()
    with pytest.raises(MultiServiceProofError) as caught:
        await fetch_and_prove_exactly_two_service_snapshot(client=_CatalogUnavailableReader(), webhook=_pair())
    assert caught.value.reason == "multi_service_catalog_unavailable"
    assert caught.value.recoverable is True


async def test_catalog_cache_is_scoped_by_exact_location_uuid() -> None:
    clear_multi_service_catalog_cache()
    other_location = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"

    class Reader:
        def __init__(self) -> None:
            self.calls: list[tuple[str, int]] = []

        async def list_location_services(self, location_uuid: str, *, page: int):
            self.calls.append((location_uuid, page))
            rows = copy.deepcopy(_catalog())
            rows[0]["category"] = {
                "uuid": CATEGORY_UUID,
                "name": "Wimpernverlängerung" if location_uuid == LOCATION_UUID else "Nagelservice",
            }
            return {
                "data": rows,
                "meta": {"current_page": 1, "last_page": 1, "total": len(rows)},
            }

    reader = Reader()
    first = await read_catalog_rows_cached(reader, location_uuid=LOCATION_UUID)
    second = await read_catalog_rows_cached(reader, location_uuid=other_location)
    first_again = await read_catalog_rows_cached(reader, location_uuid=LOCATION_UUID)

    assert reader.calls == [(LOCATION_UUID, 1), (other_location, 1)]
    assert first_again == first
    assert first[0]["category"]["name"] == "Wimpernverlängerung"  # type: ignore[index]
    assert second[0]["category"]["name"] == "Nagelservice"  # type: ignore[index]


@pytest.mark.parametrize("allowlist", ["", "[]", "{bad", '["Wimpernverlängerung", 7]'])
def test_bad_allowlist_is_recoverable_and_never_allows_pair(allowlist: str) -> None:
    raw = record_raw_with_multi_service_snapshot({"easyweek": {"services_count": 2}}, _prove())
    result = evaluate_service_eligibility(
        record_raw=raw,
        allowed_categories_raw=allowlist,
        purpose=ServiceEligibilityPurpose.LIFECYCLE_REMINDER,
    )
    assert result.allowed is False
    assert result.recoverable_configuration is True
