from __future__ import annotations

import copy
import hashlib
import json
import uuid
from decimal import Decimal

import pytest

from altegio_bot.easyweek_client import EasyWeekRetryableError
from altegio_bot.easyweek_multi_service import (
    EXACT_PAIR_PROOF_KIND,
    MULTI_SERVICE_BUSINESS_COUNT_MISMATCH,
    MULTI_SERVICE_CATALOG_MATCH_MISSING,
    MULTI_SERVICE_CATEGORY_AMBIGUOUS,
    MULTI_SERVICE_CATEGORY_NOT_ALLOWED,
    MULTI_SERVICE_CONTRACT_MISMATCH,
    MULTI_SERVICE_CURRENCY_MISMATCH,
    MULTI_SERVICE_CUSTOM_DURATION_EXCLUSION_NOT_PROVEN,
    MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED,
    MULTI_SERVICE_CUSTOM_PRICE_UNSUPPORTED,
    MULTI_SERVICE_DESCRIPTION_MISMATCH,
    MULTI_SERVICE_DISCOUNT_UNSUPPORTED,
    MULTI_SERVICE_DUPLICATE_AMBIGUOUS,
    MULTI_SERVICE_JOB_DIGEST_KEY,
    MULTI_SERVICE_JOB_VERSION_KEY,
    MULTI_SERVICE_NAMES_NOT_DISTINCT,
    MULTI_SERVICE_ORDERED_SERVICES_MALFORMED,
    MULTI_SERVICE_QUANTITY_UNSUPPORTED,
    MULTI_SERVICE_RECORD_COUNT_MISMATCH,
    MULTI_SERVICE_RELATED_MISSING,
    MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH,
    MULTI_SERVICE_SNAPSHOT_KEY,
    MULTI_SERVICE_SNAPSHOT_MISSING,
    MULTI_SERVICE_SNAPSHOT_PROOF_KEY,
    MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION,
    MULTI_SERVICE_SNAPSHOT_VERSION,
    MULTI_SERVICE_SNAPSHOT_VERSION_UNSUPPORTED,
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
    prove_exactly_two_service_custom_duration_exclusion,
    prove_exactly_two_service_snapshot,
    read_catalog_rows_cached,
    record_raw_with_multi_service_snapshot,
    resolve_effective_multi_service_snapshot,
)
from altegio_bot.easyweek_resource_shadow_contract import (
    KARLSRUHE_COMPANY_ID,
    KARLSRUHE_CONTRACT_REVISION,
    KARLSRUHE_LOCATION_UUID,
    KARLSRUHE_NUMERIC_SERVICE_NAMES,
    KARLSRUHE_RESOURCE_BACKED_SERVICE_NAMES,
    KARLSRUHE_SERVICE_CATEGORY,
    RESOURCE_SHADOW_PROOF_KIND,
    resolve_resource_shadow_contract,
)
from altegio_bot.settings import settings

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


def _exclusion_reason(*, booking: object, catalog=None) -> str:
    with pytest.raises(MultiServiceProofError) as caught:
        prove_exactly_two_service_custom_duration_exclusion(
            webhook=_pair(),
            booking_payload=booking,
            catalog_rows=_catalog() if catalog is None else catalog,
        )
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


def test_custom_duration_exclusion_proves_all_other_fields_and_resource_shadow() -> None:
    booking = _booking()
    rows = booking["ordered_services"]
    assert isinstance(rows, list)
    rows[0]["original_duration"] = _duration(15)
    assert _reason(booking=booking) == MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED
    digest = prove_exactly_two_service_custom_duration_exclusion(
        webhook=_pair(),
        booking_payload=booking,
        catalog_rows=_catalog(),
    )
    assert len(digest) == 64

    shadow_booking = copy.deepcopy(booking)
    shadow_rows = shadow_booking["ordered_services"]
    assert isinstance(shadow_rows, list)
    shadow = copy.deepcopy(shadow_rows[0])
    shadow["uuid"] = "10000000-0000-4000-8000-000000000003"
    shadow_rows.insert(1, shadow)
    assert (
        prove_exactly_two_service_custom_duration_exclusion(
            webhook=_pair(),
            booking_payload=shadow_booking,
            catalog_rows=_catalog(),
        )
        == digest
    )


def test_custom_duration_exclusion_refuses_a_standard_duration_pair() -> None:
    assert _exclusion_reason(booking=_booking()) == MULTI_SERVICE_CUSTOM_DURATION_EXCLUSION_NOT_PROVEN


@pytest.mark.parametrize(
    ("damage", "expected_reason"),
    [
        ("total", MULTI_SERVICE_TOTAL_MISMATCH),
        ("currency", MULTI_SERVICE_CURRENCY_MISMATCH),
        ("custom_price", MULTI_SERVICE_CUSTOM_PRICE_UNSUPPORTED),
        ("discount", MULTI_SERVICE_DISCOUNT_UNSUPPORTED),
        ("quantity", MULTI_SERVICE_QUANTITY_UNSUPPORTED),
        ("missing_catalog", MULTI_SERVICE_CATALOG_MATCH_MISSING),
        ("ambiguous_catalog", MULTI_SERVICE_CATEGORY_AMBIGUOUS),
        ("malformed", MULTI_SERVICE_ORDERED_SERVICES_MALFORMED),
        ("duplicate", MULTI_SERVICE_DUPLICATE_AMBIGUOUS),
        ("third_service", MULTI_SERVICE_DUPLICATE_AMBIGUOUS),
    ],
)
def test_custom_duration_exclusion_does_not_hide_a_second_error(
    damage: str,
    expected_reason: str,
) -> None:
    booking = _booking()
    rows = booking["ordered_services"]
    assert isinstance(rows, list)
    rows[0]["original_duration"] = _duration(15)
    catalog = _catalog()

    if damage == "total":
        booking["order"] = {"subtotal": 9500, "total": 9400}
    elif damage == "currency":
        booking["currency"] = "USD"
    elif damage == "custom_price":
        rows[1]["original_price"] = 6400
    elif damage == "discount":
        rows[1]["discount"] = 1
    elif damage == "quantity":
        rows[1]["quantity"] = 2
    elif damage == "missing_catalog":
        catalog = catalog[:1]
    elif damage == "ambiguous_catalog":
        duplicate = copy.deepcopy(catalog[1])
        duplicate["uuid"] = "30000000-0000-4000-8000-000000000003"
        duplicate["category"] = {"uuid": CATEGORY_UUID, "name": "Nagelservice"}
        catalog.append(duplicate)
    elif damage == "malformed":
        rows[1] = "not-a-service-line"
    elif damage == "duplicate":
        rows[1] = copy.deepcopy(rows[0])
        rows[1]["uuid"] = LINE_B_UUID
    else:
        rows.append(_line("Dritte Leistung", 0, 10, "10000000-0000-4000-8000-000000000004"))

    assert _reason(booking=booking, catalog=catalog) == MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED
    assert _exclusion_reason(booking=booking, catalog=catalog) == expected_reason


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


def test_effective_recovery_snapshot_is_bound_to_current_record_and_job_metadata() -> None:
    snapshot = _prove()
    payload = multi_service_job_payload(snapshot, include_snapshot=True)
    identity = {
        "expected_booking_uuid": BOOKING_UUID,
        "expected_location_uuid": LOCATION_UUID,
    }
    effective, error = resolve_effective_multi_service_snapshot(
        record_raw={"easyweek": {"services_count": 2}},
        job_payload=payload,
        record_total_cost=Decimal("95.00"),
        **identity,
    )
    assert error is None and effective == snapshot

    without_digest = dict(payload)
    without_digest.pop(MULTI_SERVICE_JOB_DIGEST_KEY)
    assert (
        multi_service_send_guard(
            record_raw={"easyweek": {"services_count": 2}},
            job_payload=without_digest,
            record_total_cost=Decimal("95.00"),
            **identity,
        )
        == MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
    )

    wrong_version = {**payload, "multi_service_snapshot_version": 999}
    assert (
        multi_service_send_guard(
            record_raw={"easyweek": {"services_count": 2}},
            job_payload=wrong_version,
            record_total_cost=Decimal("95.00"),
            **identity,
        )
        == MULTI_SERVICE_SNAPSHOT_VERSION_UNSUPPORTED
    )
    unsupported_snapshot = copy.deepcopy(payload)
    unsupported_snapshot["multi_service_snapshot"]["version"] = 999
    assert (
        multi_service_send_guard(
            record_raw={"easyweek": {"services_count": 2}},
            job_payload=unsupported_snapshot,
            record_total_cost=Decimal("95.00"),
            **identity,
        )
        == MULTI_SERVICE_SNAPSHOT_VERSION_UNSUPPORTED
    )
    assert (
        multi_service_send_guard(
            record_raw={"easyweek": {"services_count": 1}},
            job_payload=payload,
            record_total_cost=Decimal("95.00"),
            **identity,
        )
        == MULTI_SERVICE_RECORD_COUNT_MISMATCH
    )
    assert (
        multi_service_send_guard(
            record_raw={"easyweek": {"services_count": 2}},
            job_payload=payload,
            record_total_cost=Decimal("94.00"),
            **identity,
        )
        == MULTI_SERVICE_TOTAL_MISMATCH
    )
    assert (
        multi_service_send_guard(
            record_raw={"easyweek": {"services_count": 2}},
            job_payload=payload,
            record_total_cost=Decimal("95.00"),
            expected_booking_uuid=BOOKING_UUID,
            expected_location_uuid="bbbbbbbb-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        )
        == MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
    )


def test_stored_and_embedded_snapshots_must_match_and_use_one_category_policy() -> None:
    stored = _prove()
    embedded = _prove(catalog=_catalog(first_category="Nagelservice"))
    raw = record_raw_with_multi_service_snapshot({"easyweek": {"services_count": 2}}, stored)
    assert (
        multi_service_send_guard(
            record_raw=raw,
            job_payload=multi_service_job_payload(embedded, include_snapshot=True),
            record_total_cost=Decimal("95.00"),
            expected_booking_uuid=BOOKING_UUID,
            expected_location_uuid=LOCATION_UUID,
        )
        == MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
    )
    malformed_embedded = multi_service_job_payload(stored)
    malformed_embedded["multi_service_snapshot"] = None
    assert (
        multi_service_send_guard(
            record_raw=raw,
            job_payload=malformed_embedded,
            record_total_cost=Decimal("95.00"),
            expected_booking_uuid=BOOKING_UUID,
            expected_location_uuid=LOCATION_UUID,
        )
        == MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
    )
    malformed_stored = {"easyweek": {"services_count": 2, "multi_service_snapshot": None}}
    assert (
        multi_service_send_guard(
            record_raw=malformed_stored,
            job_payload=multi_service_job_payload(stored, include_snapshot=True),
            record_total_cost=Decimal("95.00"),
            expected_booking_uuid=BOOKING_UUID,
            expected_location_uuid=LOCATION_UUID,
        )
        == MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH
    )

    eligibility = evaluate_service_eligibility(
        record_raw={"easyweek": {"services_count": 2}},
        allowed_categories_raw=json.dumps(["Wimpernverlängerung"]),
        purpose=ServiceEligibilityPurpose.LIFECYCLE_REMINDER,
        effective_multi_service_snapshot=embedded,
    )
    assert eligibility.allowed is False
    assert eligibility.reason == MULTI_SERVICE_CATEGORY_NOT_ALLOWED


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


# ---------------------------------------------------------------------------
# PR-7.5: the Karlsruhe resource-shadow proof
#
# The 16 production bookings behind this section all report webhook
# services_count=2 / quantity=2 and return three ordered rows in which one
# pedicure name appears twice.  The two pedicure rows agree on every business
# field and differ only in technical API fields, so the PR-7.4 full-row
# signature digest cannot recognise them as one business line.  The fixtures
# below therefore give EVERY row its own technical field: none of these shapes
# can accidentally fall through to the established exact-signature path.
# ---------------------------------------------------------------------------

KARLSRUHE_BOOKING_UUID = uuid.UUID("77777777-2222-4333-8444-555555555555")
KARLSRUHE_CATEGORY_UUID = "60000000-0000-4000-8000-000000000001"

MANIKUERE_DAMEN = "Hygienische Maniküre für Damen"
MANIKUERE_HERREN = "Hygienische Maniküre für Herren"
MANIKUERE_SHELLAC = "Maniküre mit Gel-Lack / Shellac"
MANIKUERE_FRENCH = "Maniküre mit French/Design"
PEDIKUERE_DAMEN = "Hygienische Pediküre für Damen"
PEDIKUERE_GEL = "Pediküre mit Gel-Lack"
PEDIKUERE_FRENCH = "Pediküre Mit French"

# One deterministic price/duration per contract name; the exact numbers are
# fixture values, never production data.
_KARLSRUHE_SERVICE_FACTS = {
    MANIKUERE_DAMEN: (2500, 30),
    MANIKUERE_HERREN: (2700, 35),
    MANIKUERE_SHELLAC: (4200, 60),
    MANIKUERE_FRENCH: (4900, 70),
    PEDIKUERE_DAMEN: (3800, 50),
    PEDIKUERE_GEL: (5100, 75),
    PEDIKUERE_FRENCH: (5600, 80),
}

# Exactly the six shapes the production audit observed, plus the control pair.
OBSERVED_KARLSRUHE_SHAPES = [
    [MANIKUERE_DAMEN, PEDIKUERE_DAMEN, PEDIKUERE_DAMEN],
    [PEDIKUERE_DAMEN, PEDIKUERE_DAMEN, MANIKUERE_FRENCH],
    [PEDIKUERE_DAMEN, PEDIKUERE_DAMEN, MANIKUERE_SHELLAC],
    [MANIKUERE_SHELLAC, PEDIKUERE_DAMEN, PEDIKUERE_DAMEN],
    [MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL],
    [PEDIKUERE_GEL, PEDIKUERE_GEL, MANIKUERE_DAMEN],
]
CONTROL_KARLSRUHE_PAIR = [MANIKUERE_SHELLAC, PEDIKUERE_GEL]

_KARLSRUHE_SERVICE_IDS = {name: service_id for service_id, name in KARLSRUHE_NUMERIC_SERVICE_NAMES.items()}


@pytest.fixture
def karlsruhe_fence_open(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "easyweek_resource_shadow_proof_enabled", True, raising=False)


def _karlsruhe_catalog(
    *,
    drop: str | None = None,
    rename: tuple[str, str] | None = None,
    duplicate: str | None = None,
    category_override: tuple[str, str] | None = None,
    extra_service: str | None = None,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, name in enumerate(sorted(KARLSRUHE_NUMERIC_SERVICE_NAMES.values())):
        if name == drop:
            continue
        display = rename[1] if rename is not None and rename[0] == name else name
        category = KARLSRUHE_SERVICE_CATEGORY
        if category_override is not None and category_override[0] == name:
            category = category_override[1]
        rows.append(
            {
                "uuid": f"40000000-0000-4000-8000-{index:012d}",
                "name": display,
                "category": {"uuid": KARLSRUHE_CATEGORY_UUID, "name": category},
            }
        )
    if duplicate is not None:
        rows.append(
            {
                "uuid": "40000000-0000-4000-8000-000000000099",
                "name": duplicate,
                "category": {"uuid": KARLSRUHE_CATEGORY_UUID, "name": KARLSRUHE_SERVICE_CATEGORY},
            }
        )
    if extra_service is not None:
        rows.append(
            {
                "uuid": "40000000-0000-4000-8000-000000000098",
                "name": extra_service,
                "category": {"uuid": KARLSRUHE_CATEGORY_UUID, "name": KARLSRUHE_SERVICE_CATEGORY},
            }
        )
    return rows


def _karlsruhe_business_names(names: list[str]) -> list[str]:
    """The names that survive removing only the SECOND repeated occurrence."""
    business: list[str] = []
    seen: set[str] = set()
    for name in names:
        if name in seen:
            continue
        if names.count(name) == 2:
            seen.add(name)
        business.append(name)
    return business


def _karlsruhe_rows(names: list[str], *, technical_drift: bool = True) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index, name in enumerate(names):
        price, minutes = _KARLSRUHE_SERVICE_FACTS[name]
        row = _line(name, price, minutes, f"50000000-0000-4000-8000-{index:012d}")
        if technical_drift:
            # An API field this module does not model.  It differs for every
            # row, including between a service and its resource copy.
            row["resource"] = {"uuid": f"51000000-0000-4000-8000-{index:012d}", "position": index}
        rows.append(row)
    return rows


def _karlsruhe_case(
    names: list[str],
    *,
    business_names: list[str] | None = None,
    catalog: list[dict[str, object]] | None = None,
    technical_drift: bool = True,
    total_override: int | None = None,
    **pair_overrides: object,
):
    business = business_names if business_names is not None else _karlsruhe_business_names(names)
    rows = _karlsruhe_rows(names, technical_drift=technical_drift)
    total = (
        total_override if total_override is not None else sum(_KARLSRUHE_SERVICE_FACTS[name][0] for name in business)
    )
    values: dict[str, object] = {
        "booking_uuid": KARLSRUHE_BOOKING_UUID,
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "service_name": business[0],
        "service_related": business[1] if len(business) > 1 else None,
        "services_description": ", ".join(business),
        "services_count": 2,
        "quantity": 2,
        "booking_currency": "EUR",
        "total_cost": Decimal(total) / Decimal(100),
        "company_id": KARLSRUHE_COMPANY_ID,
        "service_id": _KARLSRUHE_SERVICE_IDS.get(business[0]),
    }
    values.update(pair_overrides)
    pair = WebhookServicePair(**values)  # type: ignore[arg-type]
    booking = {
        "uuid": str(KARLSRUHE_BOOKING_UUID),
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "currency": "EUR",
        "order": {"subtotal": total, "total": total},
        "ordered_services": rows,
    }
    return pair, booking, (catalog if catalog is not None else _karlsruhe_catalog())


def _replace_pair(pair: WebhookServicePair, **overrides: object) -> WebhookServicePair:
    values = {field: getattr(pair, field) for field in pair.__dataclass_fields__}
    values.update(overrides)
    return WebhookServicePair(**values)  # type: ignore[arg-type]


def _prove_karlsruhe(names: list[str], **kwargs: object):
    pair, booking, catalog = _karlsruhe_case(names, **kwargs)  # type: ignore[arg-type]
    return prove_exactly_two_service_snapshot(
        webhook=pair,
        booking_payload=booking,
        catalog_rows=catalog,
    )


def _karlsruhe_reason(names: list[str], **kwargs: object) -> str:
    with pytest.raises(MultiServiceProofError) as caught:
        _prove_karlsruhe(names, **kwargs)
    return caught.value.reason


@pytest.mark.parametrize("names", OBSERVED_KARLSRUHE_SHAPES, ids=lambda value: "-".join(value))
def test_every_observed_karlsruhe_shape_collapses_to_its_business_pair(
    karlsruhe_fence_open: None,
    names: list[str],
) -> None:
    business = _karlsruhe_business_names(names)
    snapshot = _prove_karlsruhe(names)

    assert [line.display_name for line in snapshot.lines] == business
    assert snapshot.version == MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION
    assert snapshot.proof_kind == RESOURCE_SHADOW_PROOF_KIND
    proof = snapshot.resource_shadow_proof
    assert proof is not None
    assert proof.company_id == KARLSRUHE_COMPANY_ID
    assert proof.contract_revision == KARLSRUHE_CONTRACT_REVISION
    assert len(proof.contract_digest) == 64
    assert proof.primary_service_id == _KARLSRUHE_SERVICE_IDS[business[0]]
    assert snapshot.total_minor == sum(_KARLSRUHE_SERVICE_FACTS[name][0] for name in business)
    # Every category is proved from the live catalogue, not from the contract.
    assert {line.category for line in snapshot.lines} == {KARLSRUHE_SERVICE_CATEGORY}


def test_control_ordinary_karlsruhe_pair_keeps_the_version_one_projection(
    karlsruhe_fence_open: None,
) -> None:
    snapshot = _prove_karlsruhe(CONTROL_KARLSRUHE_PAIR)
    assert [line.display_name for line in snapshot.lines] == CONTROL_KARLSRUHE_PAIR
    assert snapshot.version == MULTI_SERVICE_SNAPSHOT_VERSION
    assert snapshot.resource_shadow_proof is None
    assert snapshot.proof_kind == EXACT_PAIR_PROOF_KIND
    assert MULTI_SERVICE_SNAPSHOT_PROOF_KEY not in snapshot.as_dict()


@pytest.mark.parametrize("resource_name", sorted(KARLSRUHE_RESOURCE_BACKED_SERVICE_NAMES))
def test_all_three_resource_backed_names_are_accepted_as_the_duplicate(
    karlsruhe_fence_open: None,
    resource_name: str,
) -> None:
    snapshot = _prove_karlsruhe([MANIKUERE_HERREN, resource_name, resource_name])
    assert [line.display_name for line in snapshot.lines] == [MANIKUERE_HERREN, resource_name]


def test_resource_duplicate_is_accepted_as_primary_and_as_secondary(
    karlsruhe_fence_open: None,
) -> None:
    secondary = _prove_karlsruhe([MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL])
    primary = _prove_karlsruhe([PEDIKUERE_GEL, PEDIKUERE_GEL, MANIKUERE_SHELLAC])

    assert [line.display_name for line in secondary.lines] == [MANIKUERE_SHELLAC, PEDIKUERE_GEL]
    assert [line.display_name for line in primary.lines] == [PEDIKUERE_GEL, MANIKUERE_SHELLAC]
    # The reversed order is a different booking, so it is a different digest.
    assert secondary.digest != primary.digest


def test_hidden_technical_fields_may_differ_but_business_fields_may_not(
    karlsruhe_fence_open: None,
) -> None:
    names = [MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL]
    pair, booking, catalog = _karlsruhe_case(names)
    rows = booking["ordered_services"]
    assert isinstance(rows, list)
    # The two resource rows are NOT byte-identical: the established
    # exact-signature path cannot be what accepts them.
    assert rows[1] != rows[2]
    rows[2]["resource"] = {"uuid": "51000000-0000-4000-8000-000000009999", "position": 9}
    rows[2]["internal_note_id"] = 4242

    snapshot = prove_exactly_two_service_snapshot(
        webhook=pair,
        booking_payload=booking,
        catalog_rows=catalog,
    )
    assert [line.display_name for line in snapshot.lines] == [MANIKUERE_SHELLAC, PEDIKUERE_GEL]


@pytest.mark.parametrize(
    ("mutation", "expected"),
    [
        ({"currency": "CHF"}, MULTI_SERVICE_DUPLICATE_AMBIGUOUS),
        ({"price": 9999, "original_price": 9999}, MULTI_SERVICE_DUPLICATE_AMBIGUOUS),
        ({"original_price": 9999}, MULTI_SERVICE_CUSTOM_PRICE_UNSUPPORTED),
        ({"quantity": 2}, MULTI_SERVICE_QUANTITY_UNSUPPORTED),
        ({"discount": 100}, MULTI_SERVICE_DISCOUNT_UNSUPPORTED),
        (
            {"duration": {"value": 99, "label": "minutes"}, "original_duration": {"value": 99, "label": "minutes"}},
            MULTI_SERVICE_DUPLICATE_AMBIGUOUS,
        ),
        ({"duration": {"value": 99, "label": "minutes"}}, MULTI_SERVICE_CUSTOM_DURATION_UNSUPPORTED),
    ],
)
def test_every_known_business_field_mismatch_between_resource_copies_fails_closed(
    karlsruhe_fence_open: None,
    mutation: dict[str, object],
    expected: str,
) -> None:
    names = [MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL]
    pair, booking, catalog = _karlsruhe_case(names)
    rows = booking["ordered_services"]
    assert isinstance(rows, list)
    rows[2].update(mutation)

    with pytest.raises(MultiServiceProofError) as caught:
        prove_exactly_two_service_snapshot(webhook=pair, booking_payload=booking, catalog_rows=catalog)
    assert caught.value.reason == expected


def test_duplicate_manicure_and_unknown_duplicate_name_stay_ambiguous(
    karlsruhe_fence_open: None,
) -> None:
    # A manicure is in the contract but is NOT resource-backed.
    assert _karlsruhe_reason([PEDIKUERE_GEL, MANIKUERE_SHELLAC, MANIKUERE_SHELLAC]) == (
        MULTI_SERVICE_DUPLICATE_AMBIGUOUS
    )
    # A name the contract does not know at all, present in the live catalogue.
    unknown = "Wimpernverlängerung Auffüllen"
    pair, booking, _unused = _karlsruhe_case([MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL])
    rows = booking["ordered_services"]
    assert isinstance(rows, list)
    rows[1]["name"] = unknown
    rows[2]["name"] = unknown
    pair = _replace_pair(pair, service_related=unknown, services_description=f"{MANIKUERE_SHELLAC}, {unknown}")
    with pytest.raises(MultiServiceProofError) as caught:
        prove_exactly_two_service_snapshot(
            webhook=pair,
            booking_payload=booking,
            catalog_rows=_karlsruhe_catalog(extra_service=unknown),
        )
    assert caught.value.reason == MULTI_SERVICE_DUPLICATE_AMBIGUOUS


def test_three_identical_rows_three_distinct_services_and_four_rows_fail_closed(
    karlsruhe_fence_open: None,
) -> None:
    assert (
        _karlsruhe_reason(
            [PEDIKUERE_GEL, PEDIKUERE_GEL, PEDIKUERE_GEL],
            business_names=[PEDIKUERE_GEL, MANIKUERE_SHELLAC],
        )
        == MULTI_SERVICE_DUPLICATE_AMBIGUOUS
    )
    assert (
        _karlsruhe_reason(
            [MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_DAMEN],
            business_names=[MANIKUERE_SHELLAC, PEDIKUERE_GEL],
        )
        == MULTI_SERVICE_DUPLICATE_AMBIGUOUS
    )
    assert (
        _karlsruhe_reason(
            [MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL, PEDIKUERE_GEL],
            business_names=[MANIKUERE_SHELLAC, PEDIKUERE_GEL],
        )
        == MULTI_SERVICE_BUSINESS_COUNT_MISMATCH
    )


def test_two_identical_real_rows_never_become_a_pair(karlsruhe_fence_open: None) -> None:
    assert (
        _karlsruhe_reason(
            [PEDIKUERE_GEL, PEDIKUERE_GEL],
            business_names=[MANIKUERE_SHELLAC, PEDIKUERE_GEL],
        )
        == MULTI_SERVICE_BUSINESS_COUNT_MISMATCH
    )


@pytest.mark.parametrize(
    "service_id",
    [None, "1030234", True, 999999, _KARLSRUHE_SERVICE_IDS[PEDIKUERE_GEL]],
    ids=["missing", "string", "bool", "unknown", "mismatching"],
)
def test_primary_numeric_service_id_must_be_exact_and_match_the_name(
    karlsruhe_fence_open: None,
    service_id: object,
) -> None:
    assert (
        _karlsruhe_reason([MANIKUERE_SHELLAC, PEDIKUERE_DAMEN, PEDIKUERE_DAMEN], service_id=service_id)
        == MULTI_SERVICE_DUPLICATE_AMBIGUOUS
    )


@pytest.mark.parametrize(
    "catalog",
    [
        [],
        _karlsruhe_catalog(drop=PEDIKUERE_DAMEN),
        _karlsruhe_catalog(duplicate=PEDIKUERE_GEL),
        _karlsruhe_catalog(rename=(MANIKUERE_SHELLAC, "Maniküre mit Gel-Lack")),
        _karlsruhe_catalog(category_override=(PEDIKUERE_GEL, "Wimpernverlängerung")),
    ],
    ids=["empty", "missing", "duplicated", "renamed", "category-drift"],
)
def test_an_incomplete_or_drifted_catalogue_closes_the_resource_path(
    karlsruhe_fence_open: None,
    catalog: list[dict[str, object]],
) -> None:
    reason = _karlsruhe_reason([MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL], catalog=catalog)
    # Either the contract cannot be proved (ambiguous duplicate) or the pair's
    # own category cannot be read; both are fail-closed and carry no value.
    assert reason in {MULTI_SERVICE_DUPLICATE_AMBIGUOUS, MULTI_SERVICE_CATALOG_MATCH_MISSING}


def test_the_global_fence_alone_decides_whether_the_path_exists(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    names = [MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL]
    monkeypatch.setattr(settings, "easyweek_resource_shadow_proof_enabled", False, raising=False)
    assert _karlsruhe_reason(names) == MULTI_SERVICE_DUPLICATE_AMBIGUOUS
    monkeypatch.setattr(settings, "easyweek_resource_shadow_proof_enabled", True, raising=False)
    assert _prove_karlsruhe(names).version == MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION


@pytest.mark.parametrize(
    "overrides",
    [
        {"company_id": 308697},
        {"company_id": None},
        {"company_id": str(KARLSRUHE_COMPANY_ID)},
        {"location_uuid": "b9d689f2-0e41-47cd-812a-e3c33197753d"},
    ],
    ids=["other-company", "missing-company", "string-company", "durlach-location"],
)
def test_no_other_provider_company_or_location_may_borrow_the_contract(
    karlsruhe_fence_open: None,
    overrides: dict[str, object],
) -> None:
    names = [MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL]
    if "location_uuid" in overrides:
        pair, booking, catalog = _karlsruhe_case(names, **overrides)  # type: ignore[arg-type]
        booking["location_uuid"] = overrides["location_uuid"]
        with pytest.raises(MultiServiceProofError) as caught:
            prove_exactly_two_service_snapshot(webhook=pair, booking_payload=booking, catalog_rows=catalog)
        assert caught.value.reason == MULTI_SERVICE_DUPLICATE_AMBIGUOUS
        return
    assert _karlsruhe_reason(names, **overrides) == MULTI_SERVICE_DUPLICATE_AMBIGUOUS


def test_a_proven_nagelservice_pair_is_still_suppressed_by_the_category_allowlist(
    karlsruhe_fence_open: None,
) -> None:
    snapshot = _prove_karlsruhe([MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL])
    raw = record_raw_with_multi_service_snapshot({"easyweek": {"services_count": 2}}, snapshot)

    suppressed = evaluate_service_eligibility(
        record_raw=raw,
        allowed_categories_raw='["Wimpernverlängerung"]',
        purpose=ServiceEligibilityPurpose.LIFECYCLE_REMINDER,
    )
    assert suppressed.allowed is False
    assert suppressed.reason == MULTI_SERVICE_CATEGORY_NOT_ALLOWED
    assert suppressed.recoverable_configuration is False

    # And the structural proof itself is genuinely complete: with the category
    # allowed the SAME snapshot passes, which is what makes the suppression a
    # category decision rather than a hidden structural failure.
    allowed = evaluate_service_eligibility(
        record_raw=raw,
        allowed_categories_raw=f'["{KARLSRUHE_SERVICE_CATEGORY}"]',
        purpose=ServiceEligibilityPurpose.LIFECYCLE_REMINDER,
    )
    assert allowed.allowed is True


def test_a_version_two_snapshot_round_trips_and_is_bound_to_the_live_contract(
    karlsruhe_fence_open: None,
) -> None:
    snapshot = _prove_karlsruhe([MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL])
    raw = record_raw_with_multi_service_snapshot({"easyweek": {"services_count": 2}}, snapshot)

    parsed, error = multi_service_snapshot_from_record_raw(raw)
    assert error is None
    assert parsed == snapshot

    stored = raw["easyweek"][MULTI_SERVICE_SNAPSHOT_KEY]
    assert stored["version"] == MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION
    assert stored[MULTI_SERVICE_SNAPSHOT_PROOF_KEY]["proof_kind"] == RESOURCE_SHADOW_PROOF_KIND
    # No raw API row and no technical API field reaches the durable projection.
    serialized = json.dumps(stored)
    assert '"resource":' not in serialized
    assert "51000000-0000-4000-8000" not in serialized


@pytest.mark.parametrize(
    "mutation",
    [
        {"contract_revision": KARLSRUHE_CONTRACT_REVISION + 1},
        {"contract_digest": "f" * 64},
        {"primary_service_id": _KARLSRUHE_SERVICE_IDS[PEDIKUERE_FRENCH]},
        {"company_id": 308697},
    ],
    ids=["revision", "digest", "primary-id", "company"],
)
def test_contract_drift_holds_a_stored_version_two_snapshot(
    karlsruhe_fence_open: None,
    mutation: dict[str, object],
) -> None:
    snapshot = _prove_karlsruhe([MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL])
    raw = record_raw_with_multi_service_snapshot({"easyweek": {"services_count": 2}}, snapshot)
    raw["easyweek"][MULTI_SERVICE_SNAPSHOT_KEY][MULTI_SERVICE_SNAPSHOT_PROOF_KEY].update(mutation)

    parsed, error = multi_service_snapshot_from_record_raw(raw)
    assert parsed is None
    assert error == MULTI_SERVICE_CONTRACT_MISMATCH


def test_a_version_one_snapshot_claiming_a_contract_is_refused() -> None:
    raw = record_raw_with_multi_service_snapshot({"easyweek": {"services_count": 2}}, _prove())
    raw["easyweek"][MULTI_SERVICE_SNAPSHOT_KEY][MULTI_SERVICE_SNAPSHOT_PROOF_KEY] = {
        "proof_kind": RESOURCE_SHADOW_PROOF_KIND,
        "company_id": KARLSRUHE_COMPANY_ID,
        "contract_revision": KARLSRUHE_CONTRACT_REVISION,
        "contract_digest": "a" * 64,
        "primary_service_id": _KARLSRUHE_SERVICE_IDS[MANIKUERE_SHELLAC],
    }
    parsed, error = multi_service_snapshot_from_record_raw(raw)
    assert parsed is None
    assert error == MULTI_SERVICE_SNAPSHOT_DIGEST_MISMATCH


def test_closing_the_fence_does_not_invalidate_an_existing_version_one_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Durlach/Rastatt pairs never depend on the PR-7.5 switch."""
    monkeypatch.setattr(settings, "easyweek_resource_shadow_proof_enabled", True, raising=False)
    with_fence_open = _prove()
    monkeypatch.setattr(settings, "easyweek_resource_shadow_proof_enabled", False, raising=False)
    with_fence_closed = _prove()

    assert with_fence_open == with_fence_closed
    assert with_fence_closed.version == MULTI_SERVICE_SNAPSHOT_VERSION
    raw = record_raw_with_multi_service_snapshot({"easyweek": {"services_count": 2}}, with_fence_closed)
    assert multi_service_snapshot_from_record_raw(raw) == (with_fence_closed, None)


def test_a_version_two_job_is_bound_to_its_version_two_snapshot(karlsruhe_fence_open: None) -> None:
    snapshot = _prove_karlsruhe([MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL])
    raw = record_raw_with_multi_service_snapshot({"easyweek": {"services_count": 2}}, snapshot)
    payload = multi_service_job_payload(snapshot)
    assert payload[MULTI_SERVICE_JOB_VERSION_KEY] == MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION

    resolved, error = resolve_effective_multi_service_snapshot(
        record_raw=raw,
        job_payload=payload,
        record_total_cost=Decimal(snapshot.total_minor) / Decimal(100),
        expected_booking_uuid=KARLSRUHE_BOOKING_UUID,
        expected_location_uuid=KARLSRUHE_LOCATION_UUID,
    )
    assert error is None
    assert resolved == snapshot

    # A job that claims version 1 cannot consume the version 2 projection.
    stale = dict(payload)
    stale[MULTI_SERVICE_JOB_VERSION_KEY] = MULTI_SERVICE_SNAPSHOT_VERSION
    assert (
        multi_service_send_guard(
            record_raw=raw,
            job_payload=stale,
            record_total_cost=Decimal(snapshot.total_minor) / Decimal(100),
            expected_booking_uuid=KARLSRUHE_BOOKING_UUID,
            expected_location_uuid=KARLSRUHE_LOCATION_UUID,
        )
        == MULTI_SERVICE_SNAPSHOT_VERSION_UNSUPPORTED
    )


def test_the_static_contract_is_the_exact_owner_approved_table() -> None:
    assert KARLSRUHE_COMPANY_ID == 322579
    assert KARLSRUHE_LOCATION_UUID == "8395fab6-7ee8-4702-88d9-fd78f92539c1"
    assert KARLSRUHE_NUMERIC_SERVICE_NAMES == {
        1030228: "Hygienische Maniküre für Damen",
        1030231: "Hygienische Maniküre für Herren",
        1030234: "Maniküre mit Gel-Lack / Shellac",
        1030237: "Maniküre mit French/Design",
        1030240: "Hygienische Pediküre für Damen",
        1030243: "Pediküre mit Gel-Lack",
        1030246: "Pediküre Mit French",
    }
    assert KARLSRUHE_RESOURCE_BACKED_SERVICE_NAMES == frozenset(
        {
            "Hygienische Pediküre für Damen",
            "Pediküre mit Gel-Lack",
            "Pediküre Mit French",
        }
    )
    # No catalogue UUID is pinned anywhere in the contract module.
    assert "uuid" not in {name.casefold() for name in KARLSRUHE_NUMERIC_SERVICE_NAMES.values()}
    assert (
        resolve_resource_shadow_contract(
            provider="altegio",
            company_id=KARLSRUHE_COMPANY_ID,
            location_uuid=KARLSRUHE_LOCATION_UUID,
        )
        is None
    )


# ---------------------------------------------------------------------------
# Review defect 1: the SURVIVING pair must be owner-approved, not just the
# duplicate.  Proving only the repeated row let an arbitrary catalogue service
# ride along as the second business line.
# ---------------------------------------------------------------------------

UNKNOWN_NAIL_SERVICE = "Unknown Nail Service"


def _unknown_singleton_case(*, resource_is_primary: bool):
    """`[B,B,X]` / `[X,B,B]` where X is a live service outside the contract."""
    rows: list[dict[str, object]] = []
    names = (
        [PEDIKUERE_GEL, PEDIKUERE_GEL, UNKNOWN_NAIL_SERVICE]
        if resource_is_primary
        else [UNKNOWN_NAIL_SERVICE, PEDIKUERE_GEL, PEDIKUERE_GEL]
    )
    prices = {PEDIKUERE_GEL: _KARLSRUHE_SERVICE_FACTS[PEDIKUERE_GEL][0], UNKNOWN_NAIL_SERVICE: 3300}
    for index, name in enumerate(names):
        row = _line(name, prices[name], 60, f"70000000-0000-4000-8000-{index:012d}")
        row["resource"] = {"uuid": f"71000000-0000-4000-8000-{index:012d}", "position": index}
        rows.append(row)

    business = [PEDIKUERE_GEL, UNKNOWN_NAIL_SERVICE] if resource_is_primary else [UNKNOWN_NAIL_SERVICE, PEDIKUERE_GEL]
    total = prices[business[0]] + prices[business[1]]
    pair = WebhookServicePair(
        booking_uuid=KARLSRUHE_BOOKING_UUID,
        location_uuid=KARLSRUHE_LOCATION_UUID,
        service_name=business[0],
        service_related=business[1],
        services_description=f"{business[0]}, {business[1]}",
        services_count=2,
        quantity=2,
        booking_currency="EUR",
        total_cost=Decimal(total) / Decimal(100),
        company_id=KARLSRUHE_COMPANY_ID,
        # The webhook primary id is a real contract id whenever the primary is
        # the contract service; the unknown-primary case has no contract id.
        service_id=_KARLSRUHE_SERVICE_IDS.get(business[0]),
    )
    booking = {
        "uuid": str(KARLSRUHE_BOOKING_UUID),
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "currency": "EUR",
        "order": {"subtotal": total, "total": total},
        "ordered_services": rows,
    }
    return pair, booking, _karlsruhe_catalog(extra_service=UNKNOWN_NAIL_SERVICE)


@pytest.mark.parametrize("resource_is_primary", [True, False], ids=["resource-primary", "resource-secondary"])
def test_an_unknown_surviving_service_never_becomes_a_resource_shadow_pair(
    karlsruhe_fence_open: None,
    resource_is_primary: bool,
) -> None:
    pair, booking, catalog = _unknown_singleton_case(resource_is_primary=resource_is_primary)
    # The unknown service really is in the live catalogue: being catalogued is
    # exactly what must NOT be enough.
    assert any(row["name"] == UNKNOWN_NAIL_SERVICE for row in catalog)

    with pytest.raises(MultiServiceProofError) as caught:
        prove_exactly_two_service_snapshot(webhook=pair, booking_payload=booking, catalog_rows=catalog)
    assert caught.value.reason == MULTI_SERVICE_DUPLICATE_AMBIGUOUS
    assert UNKNOWN_NAIL_SERVICE not in str(caught.value)


def _sign(unsigned: dict[str, object]) -> str:
    """The module's canonical digest, recomputed so a forgery is truly valid."""
    encoded = json.dumps(unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _forged_v2_raw(lines: list[dict[str, object]], *, primary_service_id: int) -> dict[str, object]:
    unsigned: dict[str, object] = {
        "version": MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION,
        "provider": "easyweek",
        "booking_uuid": str(KARLSRUHE_BOOKING_UUID),
        "location_uuid": KARLSRUHE_LOCATION_UUID,
        "services_count": 2,
        "lines": lines,
        MULTI_SERVICE_SNAPSHOT_PROOF_KEY: {
            "proof_kind": RESOURCE_SHADOW_PROOF_KIND,
            "company_id": KARLSRUHE_COMPANY_ID,
            "contract_revision": KARLSRUHE_CONTRACT_REVISION,
            "contract_digest": _karlsruhe_contract_digest(),
            "primary_service_id": primary_service_id,
        },
    }
    return {"easyweek": {"services_count": 2, MULTI_SERVICE_SNAPSHOT_KEY: {**unsigned, "digest": _sign(unsigned)}}}


def _karlsruhe_contract_digest() -> str:
    snapshot = _prove_karlsruhe([MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL])
    assert snapshot.resource_shadow_proof is not None
    return snapshot.resource_shadow_proof.contract_digest


def test_a_digest_valid_v2_snapshot_naming_an_unknown_service_is_refused(
    karlsruhe_fence_open: None,
) -> None:
    """The stored digest proves integrity, never reachability."""
    genuine = _prove_karlsruhe([MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL])
    lines = [dict(line.as_dict()) for line in genuine.lines]
    lines[1]["display_name"] = UNKNOWN_NAIL_SERVICE
    lines[1]["normalized_name"] = UNKNOWN_NAIL_SERVICE.casefold()
    raw = _forged_v2_raw(lines, primary_service_id=_KARLSRUHE_SERVICE_IDS[MANIKUERE_SHELLAC])

    # The forgery is genuinely digest-valid, so only the contract can catch it.
    stored = raw["easyweek"][MULTI_SERVICE_SNAPSHOT_KEY]
    assert stored["digest"] == _sign({key: value for key, value in stored.items() if key != "digest"})

    parsed, error = multi_service_snapshot_from_record_raw(raw)
    assert parsed is None
    assert error == MULTI_SERVICE_CONTRACT_MISMATCH


def test_a_digest_valid_v2_snapshot_without_a_resource_backed_line_is_refused(
    karlsruhe_fence_open: None,
) -> None:
    """Two manicures can never have produced a collapsed resource shadow."""
    genuine = _prove_karlsruhe([MANIKUERE_SHELLAC, PEDIKUERE_GEL, PEDIKUERE_GEL])
    lines = [dict(line.as_dict()) for line in genuine.lines]
    lines[1]["display_name"] = MANIKUERE_FRENCH
    lines[1]["normalized_name"] = MANIKUERE_FRENCH.casefold()
    raw = _forged_v2_raw(lines, primary_service_id=_KARLSRUHE_SERVICE_IDS[MANIKUERE_SHELLAC])

    parsed, error = multi_service_snapshot_from_record_raw(raw)
    assert parsed is None
    assert error == MULTI_SERVICE_CONTRACT_MISMATCH


@pytest.mark.parametrize("resource_name", sorted(KARLSRUHE_RESOURCE_BACKED_SERVICE_NAMES))
@pytest.mark.parametrize(
    "partner",
    [MANIKUERE_DAMEN, MANIKUERE_HERREN, MANIKUERE_SHELLAC, MANIKUERE_FRENCH],
)
def test_every_contract_name_still_pairs_with_every_resource_name(
    karlsruhe_fence_open: None,
    resource_name: str,
    partner: str,
) -> None:
    """All seven contract names and all three resource names keep working."""
    secondary = _prove_karlsruhe([partner, resource_name, resource_name])
    primary = _prove_karlsruhe([resource_name, resource_name, partner])

    assert [line.display_name for line in secondary.lines] == [partner, resource_name]
    assert [line.display_name for line in primary.lines] == [resource_name, partner]
    assert secondary.version == primary.version == MULTI_SERVICE_SNAPSHOT_RESOURCE_SHADOW_VERSION
