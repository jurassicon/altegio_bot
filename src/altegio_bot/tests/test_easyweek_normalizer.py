"""Unit contract for the EasyWeek normalizer (PR-4).

Every rejection here is deterministic and fail-closed: the normalizer refuses
rather than guesses, and the reason it reports is a fixed code that can never
carry a payload value.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from altegio_bot.easyweek_locations import EasyWeekLocation
from altegio_bot.easyweek_normalizer import (
    CREATE,
    DELETE,
    IGNORE,
    UPDATE,
    NormalizationError,
    canonical_booking_uuid,
    easyweek_job_dedupe_key,
    extract_manage_link,
    map_event_hint,
    normalize_booking_hash_id,
    normalize_event,
    parse_iso_utc,
)
from altegio_bot.easyweek_service_category import (
    ALLOWED,
    ALLOWED_CATEGORIES_INVALID,
    ALLOWED_CATEGORIES_UNCONFIGURED,
    CATEGORY_AMBIGUOUS_MULTI_SERVICE,
    CATEGORY_MISSING,
    CATEGORY_NOT_ALLOWED,
    MAX_ALLOWED_SERVICE_CATEGORIES,
    MAX_SERVICE_CATEGORY_LENGTH,
    SERVICE_COUNT_UNPROVEN,
    evaluate_service_category,
    normalize_service_category,
    parse_allowed_service_categories,
    record_raw_with_service_category,
    record_raw_with_services_count,
    services_count_from_record_raw,
)
from altegio_bot.tests.easyweek_fixtures import (
    FOREIGN_LOCATION_ID,
    TEST_BOOKING_HASH_ID,
    TEST_BOOKING_ID,
    TEST_BOOKING_PAGE,
    TEST_BOOKING_UUID,
    TEST_CUSTOMER_ID,
    TEST_LOCATION_ID,
    TEST_LOCATION_UUID,
    booking_canceled,
    booking_created,
    booking_created_full_shape,
    booking_created_resend,
    booking_rescheduled,
    booking_updated,
    clear_booking_price,
    drop_booking_price,
    set_booking_price,
)


def _location(location_id: int = TEST_LOCATION_ID, location_uuid: str = TEST_LOCATION_UUID) -> EasyWeekLocation:
    return EasyWeekLocation(
        name="test-branch",
        location_id=location_id,
        location_uuid=location_uuid,
        meta_template_prefix="tb",
        booking_page_url="https://booking.example.invalid/test",
    )


def _normalize(payload, *, event_hint="booking-created", truncated=False, registry=None):
    return normalize_event(
        event_hint=event_hint,
        payload=payload,
        body_truncated=truncated,
        location_registry=registry if registry is not None else {TEST_LOCATION_ID: _location()},
    )


# ===========================================================================
# Event mapping — exact trigger names only
# ===========================================================================


@pytest.mark.parametrize(
    ("hint", "expected"),
    [
        ("booking-created", CREATE),
        ("booking-updated", UPDATE),
        ("booking-rescheduled", UPDATE),
        ("booking-canceled", DELETE),
        ("booking-succeeded", IGNORE),
    ],
)
def test_exact_trigger_names_map_to_actions(hint: str, expected: str) -> None:
    assert map_event_hint(hint) == expected


@pytest.mark.parametrize(
    "hint",
    [
        # Legacy short forms EasyWeek never sends.
        "created",
        "updated",
        "rescheduled",
        "canceled",
        # Our own internal verbs must not be accepted as triggers.
        "create",
        "update",
        "delete",
        # Synthetic/smoke values from early capture rows.
        "smoke",
        "test",
        "booking-created-x",
        "BOOKING-CREATED",
        "",
        None,
        123,
    ],
)
def test_legacy_and_unknown_hints_are_rejected(hint) -> None:
    with pytest.raises(NormalizationError) as excinfo:
        map_event_hint(hint)
    assert excinfo.value.code == NormalizationError.INVALID_EVENT_HINT


def test_booking_succeeded_produces_no_side_effects() -> None:
    assert _normalize(booking_created(), event_hint="booking-succeeded") is None


def test_event_type_never_comes_from_localized_status() -> None:
    """A canceled delivery whose localized status still says 'New' is a cancel."""
    payload = booking_canceled()
    payload["booking_status"] = "New appointment"
    booking = _normalize(payload, event_hint="booking-canceled")
    assert booking is not None
    assert booking.action == DELETE


# ===========================================================================
# Payload validation
# ===========================================================================


@pytest.mark.parametrize("payload", [None, [], "string", 42, b"bytes", {}])
def test_non_object_payload_is_rejected(payload) -> None:
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    assert excinfo.value.code == NormalizationError.INVALID_PAYLOAD


def test_truncated_body_is_rejected() -> None:
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(booking_created(), truncated=True)
    assert excinfo.value.code == NormalizationError.TRUNCATED_PAYLOAD


def test_missing_uid_is_rejected() -> None:
    payload = booking_created()
    del payload["uid"]
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    assert excinfo.value.code == NormalizationError.MISSING_BOOKING_UUID


@pytest.mark.parametrize("bad", ["not-a-uuid", "1234", TEST_BOOKING_UUID[:-1], "  "])
def test_invalid_uid_is_rejected(bad: str) -> None:
    payload = booking_created()
    payload["uid"] = bad
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    assert excinfo.value.code in {
        NormalizationError.INVALID_BOOKING_UUID,
        NormalizationError.MISSING_BOOKING_UUID,
    }


@pytest.mark.parametrize("bad", [TEST_BOOKING_ID, 12345, {"a": 1}, [1]])
def test_non_string_uid_is_rejected(bad) -> None:
    payload = booking_created()
    payload["uid"] = bad
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    assert excinfo.value.code == NormalizationError.INVALID_BOOKING_UUID


@pytest.mark.parametrize("bad", [None, "4200001", True, False, {"id": 1}, 1.5])
def test_non_numeric_booking_id_is_rejected(bad) -> None:
    payload = booking_created()
    payload["id"] = bad
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    assert excinfo.value.code == NormalizationError.MISSING_BOOKING_ID


@pytest.mark.parametrize("bad", ["7300002", True, 1.5, {"a": 1}])
def test_non_numeric_customer_id_is_rejected(bad) -> None:
    payload = booking_created()
    payload["customer_id"] = bad
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    assert excinfo.value.code == NormalizationError.INVALID_PAYLOAD


@pytest.mark.parametrize("bad", [None, "999001", True, 1.5, {"a": 1}])
def test_non_numeric_location_id_is_rejected(bad) -> None:
    payload = booking_created()
    payload["location_id"] = bad
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    assert excinfo.value.code == NormalizationError.INVALID_LOCATION_ID


# ===========================================================================
# Location isolation
# ===========================================================================


def test_foreign_location_is_rejected() -> None:
    payload = booking_created()
    payload["location_id"] = FOREIGN_LOCATION_ID
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    assert excinfo.value.code == NormalizationError.FOREIGN_LOCATION


def test_unconfigured_location_fails_closed() -> None:
    """The payload must never be able to supply the location we own."""
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(booking_created(), registry={})
    assert excinfo.value.code == NormalizationError.FOREIGN_LOCATION


def test_company_id_comes_from_verified_payload_location() -> None:
    booking = _normalize(booking_created())
    assert booking is not None
    assert booking.company_id == TEST_LOCATION_ID


def test_known_location_with_wrong_uuid_has_distinct_identity_error() -> None:
    payload = booking_created()
    payload["location_uuid"] = "bbbbbbbb-cccc-4ddd-8eee-ffffffffffff"
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    assert excinfo.value.code == NormalizationError.LOCATION_IDENTITY_MISMATCH


# ===========================================================================
# Timestamps
# ===========================================================================


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("2026-08-03T10:00:00+0000", datetime(2026, 8, 3, 10, 0, tzinfo=timezone.utc)),
        ("2026-08-03T12:00:00+02:00", datetime(2026, 8, 3, 10, 0, tzinfo=timezone.utc)),
        ("2026-08-03T10:00:00Z", datetime(2026, 8, 3, 10, 0, tzinfo=timezone.utc)),
        ("2026-08-03T07:00:00-0300", datetime(2026, 8, 3, 10, 0, tzinfo=timezone.utc)),
    ],
)
def test_iso_offsets_convert_to_utc(raw: str, expected: datetime) -> None:
    assert parse_iso_utc(raw, required=True) == expected


@pytest.mark.parametrize("raw", ["2026-08-03T10:00:00", "not-a-date", "2026-13-99T00:00:00+0000"])
def test_invalid_or_naive_datetimes_are_rejected(raw: str) -> None:
    """A naive timestamp has no defensible zone — guessing would shift times."""
    with pytest.raises(NormalizationError) as excinfo:
        parse_iso_utc(raw, required=True)
    assert excinfo.value.code == NormalizationError.INVALID_DATETIME


def test_rescheduled_moves_the_stored_start() -> None:
    created = _normalize(booking_created())
    moved = _normalize(booking_rescheduled(), event_hint="booking-rescheduled")
    assert created is not None and moved is not None
    assert moved.starts_at is not None and created.starts_at is not None
    assert moved.starts_at > created.starts_at
    assert moved.starts_at.tzinfo is not None


def test_display_date_fields_are_not_the_source_of_truth() -> None:
    payload = booking_created()
    payload["booking_date_start_formatted"] = "01.01.1999 00:00"
    payload["date"] = "01.01.1999"
    payload["time"] = "00:00"
    booking = _normalize(payload)
    assert booking is not None
    assert booking.starts_at == datetime(2026, 8, 3, 10, 0, tzinfo=timezone.utc)


# ===========================================================================
# Manage link — strictly fail-closed
# ===========================================================================


def test_valid_pair_is_accepted() -> None:
    link, present = extract_manage_link(booking_created())
    assert present is True
    assert link is not None
    assert link.url == TEST_BOOKING_PAGE
    assert link.hash_id == TEST_BOOKING_HASH_ID


@pytest.mark.parametrize(
    ("page", "why"),
    [
        ("http://eyw.me/r/90000001", "plain http"),
        ("https://evil.example/r/90000001", "foreign host"),
        ("https://eyw.me.evil.example/r/90000001", "host suffix attack"),
        ("https://eyw.me:8443/r/90000001", "explicit port"),
        ("https://user:pw@eyw.me/r/90000001", "credentials"),
        ("https://eyw.me/r/90000001?x=1", "query string"),
        ("https://eyw.me/r/90000001#frag", "fragment"),
        ("https://eyw.me/f/90000001", "wrong path prefix"),
        ("https://eyw.me/90000001", "no /r/ prefix"),
        ("https://eyw.me/r/99999999", "hash mismatch"),
        ("https://eyw.me/r/", "empty hash"),
        ("//eyw.me/r/90000001", "scheme-relative"),
        ("eyw.me/r/90000001", "no scheme"),
        ("", "empty string"),
    ],
)
def test_untrusted_pages_are_rejected(page: str, why: str) -> None:
    payload = booking_created()
    payload["booking_page"] = page
    link, present = extract_manage_link(payload)
    assert present is True, why
    assert link is None, f"accepted {why}: {page!r}"


def test_alias_fields_are_never_accepted() -> None:
    """Only `booking_page` is trusted; `manage_url` and friends are not."""
    payload = booking_created()
    del payload["booking_page"]
    payload["manage_url"] = TEST_BOOKING_PAGE
    payload["short_link"] = TEST_BOOKING_PAGE
    link, present = extract_manage_link(payload)
    assert link is None
    assert present is True  # booking_hash_id is still present


def test_hash_changed_without_a_valid_page_yields_no_link() -> None:
    payload = booking_created()
    payload["booking_hash_id"] = "90000999"  # page still points at the old hash
    link, present = extract_manage_link(payload)
    assert present is True
    assert link is None


def test_both_link_fields_absent_reports_not_present() -> None:
    """The caller must be able to tell 'says nothing' from 'said something bad'."""
    payload = booking_created()
    del payload["booking_page"]
    del payload["booking_hash_id"]
    link, present = extract_manage_link(payload)
    assert link is None
    assert present is False


def test_link_is_never_synthesised_from_uid_or_hash() -> None:
    payload = booking_created()
    del payload["booking_page"]
    link, present = extract_manage_link(payload)
    assert link is None, "a link was invented from booking_hash_id alone"
    assert present is True


def test_hash_is_kept_as_a_string() -> None:
    assert normalize_booking_hash_id("00090") == "00090", "leading zeros must survive"
    assert normalize_booking_hash_id(90000001) == "90000001"
    assert normalize_booking_hash_id(True) is None
    assert normalize_booking_hash_id(None) is None
    assert normalize_booking_hash_id("") is None


def test_overlong_hash_is_rejected() -> None:
    """Bounded by records.easyweek_booking_hash_id (String(64))."""
    assert normalize_booking_hash_id("9" * 65) is None
    assert normalize_booking_hash_id("9" * 64) is not None


# ===========================================================================
# Whole-payload behaviour
# ===========================================================================


def test_created_payload_normalizes_completely() -> None:
    booking = _normalize(booking_created())
    assert booking is not None
    assert booking.action == CREATE
    assert booking.booking_uuid == uuid.UUID(TEST_BOOKING_UUID)
    assert booking.booking_id == TEST_BOOKING_ID
    assert booking.customer_id == TEST_CUSTOMER_ID
    assert booking.company_id == TEST_LOCATION_ID
    assert booking.duration_sec == 3600
    assert booking.manage_link is not None


def test_literal_dotted_root_keys_are_read_as_flat_keys() -> None:
    """`booking_attributes.booking_comment` is one key, not a nested path."""
    booking = _normalize(booking_created())
    assert booking is not None
    assert booking.comment == "fixture comment"


def test_unknown_root_fields_are_ignored() -> None:
    booking = _normalize(booking_created_full_shape())
    assert booking is not None
    assert booking.booking_uuid == uuid.UUID(TEST_BOOKING_UUID)
    assert booking.comment == "fixture comment"


def test_missing_optional_contacts_do_not_fail() -> None:
    payload = booking_created()
    del payload["customer_phone"]
    del payload["customer_email"]
    booking = _normalize(payload)
    assert booking is not None
    assert booking.phone_e164 is None
    assert booking.email is None


def test_updated_without_reschedule_keeps_the_time() -> None:
    created = _normalize(booking_created())
    updated = _normalize(booking_updated(), event_hint="booking-updated")
    assert created is not None and updated is not None
    assert updated.action == UPDATE
    assert updated.starts_at == created.starts_at


# ===========================================================================
# Dedupe key
# ===========================================================================


def test_resend_of_the_same_delivery_produces_one_key() -> None:
    first = easyweek_job_dedupe_key(
        event_hint="booking-created",
        booking_uuid=uuid.UUID(TEST_BOOKING_UUID),
        payload_hash="abc123",
        job_type="record_created",
    )
    resend = easyweek_job_dedupe_key(
        event_hint="booking-created",
        booking_uuid=uuid.UUID(TEST_BOOKING_UUID),
        payload_hash="abc123",
        job_type="record_created",
    )
    assert first == resend


@pytest.mark.parametrize(
    "changed",
    [
        {"event_hint": "booking-updated"},
        {"payload_hash": "different"},
        {"booking_uuid": uuid.UUID("99999999-2222-4333-8444-555555555555")},
        {"job_type": "record_updated"},
    ],
)
def test_dedupe_key_changes_with_every_component(changed: dict) -> None:
    base = {
        "event_hint": "booking-created",
        "booking_uuid": uuid.UUID(TEST_BOOKING_UUID),
        "payload_hash": "abc123",
        "job_type": "record_created",
    }
    assert easyweek_job_dedupe_key(**base) != easyweek_job_dedupe_key(**{**base, **changed})


def test_dedupe_key_is_provider_scoped_and_bounded() -> None:
    key = easyweek_job_dedupe_key(
        event_hint="booking-created",
        booking_uuid=uuid.UUID(TEST_BOOKING_UUID),
        payload_hash="a" * 64,
        job_type="record_created",
    )
    assert key.startswith("easyweek:")
    assert len(key) <= 128, "must fit message_jobs.dedupe_key (String(128))"


def test_resend_fixture_is_byte_identical() -> None:
    assert booking_created_resend() == booking_created()


# ===========================================================================
# No PII in anything the normalizer reports
# ===========================================================================


def test_error_codes_are_a_closed_safe_set() -> None:
    for code in NormalizationError.ALL_CODES:
        assert code.replace("_", "").isalpha(), f"{code} is not a plain identifier"
        assert NormalizationError(code).code == code


def test_unknown_error_code_cannot_be_constructed() -> None:
    with pytest.raises(ValueError):
        NormalizationError("customer_phone=+49123456789")


@pytest.mark.parametrize(
    "mutate",
    [
        lambda p: p.update({"location_id": FOREIGN_LOCATION_ID}),
        lambda p: p.update({"uid": "not-a-uuid"}),
        lambda p: p.update({"id": "nope"}),
        lambda p: p.update({"booking_date_start": "garbage"}),
    ],
)
def test_rejections_never_leak_payload_values(mutate) -> None:
    payload = booking_created()
    mutate(payload)
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    message = str(excinfo.value)
    assert message == excinfo.value.code
    for secret in (
        payload.get("customer_phone", ""),
        payload.get("customer_email", ""),
        payload.get("customer_full_name", ""),
        str(payload.get("uid", "")),
    ):
        if secret:
            assert secret not in message


# ===========================================================================
# Malformed manage URLs must never raise (review fix 4)
# ===========================================================================


@pytest.mark.parametrize(
    ("page", "why"),
    [
        ("https://eyw.me:bad/r/90000001", "non-numeric port"),
        ("https://eyw.me:99999/r/90000001", "out-of-range port"),
        ("https://eyw.me:-1/r/90000001", "negative port"),
        ("https://[oops/r/90000001", "unterminated bracketed host"),
        ("https://[::1]:bad/r/90000001", "bracketed host with bad port"),
        ("https://user:pw@eyw.me:bad/r/90000001", "credentials plus bad port"),
    ],
)
def test_malformed_urls_are_rejected_without_raising(page: str, why: str) -> None:
    """`SplitResult.port` raises ValueError lazily, at attribute access.

    Letting that escape would bypass the deterministic path entirely and leave
    the row stuck at the head of the queue.
    """
    payload = booking_created()
    payload["booking_page"] = page
    link, present = extract_manage_link(payload)
    assert link is None, f"accepted {why}: {page!r}"
    assert present is True, why


@pytest.mark.parametrize(
    "page",
    [
        "https://eyw.me:bad/r/90000001",
        "https://[oops/r/90000001",
        "https://eyw.me:99999/r/90000001",
    ],
)
def test_whole_event_still_normalizes_when_the_link_is_malformed(page: str) -> None:
    """A bad link clears the link; it must not fail the booking."""
    payload = booking_created()
    payload["booking_page"] = page
    booking = _normalize(payload)
    assert booking is not None
    assert booking.manage_link is None
    assert booking.manage_link_present is True


def test_canonical_url_is_still_accepted() -> None:
    link, present = extract_manage_link(booking_created())
    assert present is True
    assert link is not None and link.url == TEST_BOOKING_PAGE


# ===========================================================================
# booking-succeeded shares the common validation (review fix 8)
# ===========================================================================


def test_succeeded_for_our_location_is_ignored_cleanly() -> None:
    assert _normalize(booking_created(), event_hint="booking-succeeded") is None


@pytest.mark.parametrize(
    ("mutate", "truncated", "expected"),
    [
        (lambda p: p.update({"location_id": FOREIGN_LOCATION_ID}), False, NormalizationError.FOREIGN_LOCATION),
        (lambda p: p.pop("location_id"), False, NormalizationError.INVALID_LOCATION_ID),
        (lambda p: p.update({"location_id": "999001"}), False, NormalizationError.INVALID_LOCATION_ID),
        (lambda p: None, True, NormalizationError.TRUNCATED_PAYLOAD),
    ],
)
def test_succeeded_is_not_a_bypass_for_integrity_or_isolation(mutate, truncated: bool, expected: str) -> None:
    """A foreign or truncated `booking-succeeded` must NOT reach `processed`."""
    payload = booking_created()
    mutate(payload)
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload, event_hint="booking-succeeded", truncated=truncated)
    assert excinfo.value.code == expected


@pytest.mark.parametrize("payload", [None, {}, [], "text"])
def test_succeeded_with_a_non_object_payload_is_rejected(payload) -> None:
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload, event_hint="booking-succeeded")
    assert excinfo.value.code == NormalizationError.INVALID_PAYLOAD


def test_succeeded_does_not_require_a_booking_uuid() -> None:
    """Nothing on the ignore path is keyed by the UUID, so it is not demanded."""
    payload = booking_created()
    del payload["uid"]
    assert _normalize(payload, event_hint="booking-succeeded") is None


# ===========================================================================
# Service and price normalization (review fix 7)
# ===========================================================================


# The production defect this contract replaced: `booking_price_int` was read as
# a cent count, so a real 120.00 € booking was stored — and would have been sent
# to the customer — as 1.20 €. Production capture settled the field semantics:
#
#     120.00 €  ->  booking_price_int=120, booking_price="12000",
#                   booking_price_float="120.00", booking_price_formatted="€120.00"
#
# `booking_price` is the authoritative storage value in exact minor units,
# `booking_price_float` is a cross-check, and the other two are never parsed.


@pytest.mark.parametrize(
    ("minor_units", "expected"),
    [
        (12000, Decimal("120.00")),
        (15000, Decimal("150.00")),
        (3000, Decimal("30.00")),
        (3500, Decimal("35.00")),
        # A price that is not a whole euro: the parser must keep both decimals.
        (3550, Decimal("35.50")),
        (99, Decimal("0.99")),
        (1, Decimal("0.01")),
    ],
)
def test_the_authoritative_storage_value_is_read_as_exact_minor_units(minor_units: int, expected: Decimal) -> None:
    booking = _normalize(set_booking_price(booking_created(), minor_units))
    assert booking is not None
    assert booking.total_cost == expected
    assert booking.total_cost.as_tuple().exponent == -2, "money is always scale 2"


def test_the_confirmed_production_payload_is_not_divided_by_a_hundred() -> None:
    """The regression itself, spelled out with the captured 120.00 € delivery."""
    payload = booking_created()
    payload["booking_price_int"] = 120
    payload["booking_price"] = "12000"
    payload["booking_price_float"] = "120.00"
    payload["booking_price_formatted"] = "€120.00"

    booking = _normalize(payload)
    assert booking is not None
    assert booking.total_cost == Decimal("120.00")
    assert booking.total_cost != Decimal("1.20"), "booking_price_int is not a cent count"


def test_booking_price_int_alone_never_produces_a_price() -> None:
    """It is not authoritative, so on its own it proves nothing — and must not
    quietly resurrect the old value through a fallback."""
    payload = drop_booking_price(booking_created())
    payload["booking_price_int"] = 120

    booking = _normalize(payload)
    assert booking is not None
    assert booking.total_cost is None
    assert not booking.carries("total_cost"), "an unproven price must not overwrite a known one"


def test_the_formatted_string_is_never_a_fallback() -> None:
    """Display text carries the salon's currency and separator; parsing it would
    inherit both."""
    payload = drop_booking_price(booking_created())
    payload["booking_price_formatted"] = "€120.00"

    booking = _normalize(payload)
    assert booking is not None
    assert booking.total_cost is None
    assert not booking.carries("total_cost")


def test_a_real_zero_price_is_a_price_and_not_an_absence() -> None:
    booking = _normalize(set_booking_price(booking_created(), 0))
    assert booking is not None
    assert booking.total_cost == Decimal("0.00")
    assert booking.carries("total_cost"), "0.00 is authoritative, not silence"


def test_a_delivery_without_any_price_field_keeps_the_known_price() -> None:
    """Absent is "unchanged", not an error and not a clear."""
    booking = _normalize(drop_booking_price(booking_created()))
    assert booking is not None
    assert booking.total_cost is None
    assert not booking.carries("total_cost")


def test_an_explicit_consistent_clear_is_authoritative() -> None:
    booking = _normalize(clear_booking_price(booking_created()))
    assert booking is not None
    assert booking.total_cost is None
    assert booking.carries("total_cost"), "an explicit null really clears the snapshot"


def test_a_clear_contradicted_by_another_price_field_is_refused() -> None:
    """Half a delivery says "no price", the other half names one. We do not pick."""
    payload = booking_created()
    payload["booking_price"] = None
    payload["booking_price_float"] = "120.00"

    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    assert excinfo.value.code == NormalizationError.PRICE_FIELDS_CONFLICT


def test_a_price_claimed_without_the_authoritative_field_is_refused() -> None:
    payload = drop_booking_price(booking_created())
    payload["booking_price_float"] = "120.00"

    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    assert excinfo.value.code == NormalizationError.PRICE_FIELDS_CONFLICT


def test_the_projection_must_describe_the_same_amount() -> None:
    payload = set_booking_price(booking_created(), 12000)
    payload["booking_price_float"] = "1.20"

    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    assert excinfo.value.code == NormalizationError.PRICE_FIELDS_CONFLICT


@pytest.mark.parametrize("projection", ["120.0", "120", "120.00"])
def test_an_agreeing_projection_is_accepted_in_any_equivalent_form(projection: str) -> None:
    """The comparison is numeric: the same amount written differently agrees."""
    payload = set_booking_price(booking_created(), 12000)
    payload["booking_price_float"] = projection

    booking = _normalize(payload)
    assert booking is not None
    assert booking.total_cost == Decimal("120.00")


@pytest.mark.parametrize(
    ("label", "bad", "expected"),
    [
        # A JSON number where the contract says storage string: the very
        # ambiguity between major and minor units that caused the defect.
        ("json-int", 12000, NormalizationError.INVALID_PAYLOAD),
        ("json-float", 12000.0, NormalizationError.INVALID_PAYLOAD),
        ("bool", True, NormalizationError.INVALID_PAYLOAD),
        ("exponent", "1.2e4", NormalizationError.INVALID_PAYLOAD),
        ("comma", "120,00", NormalizationError.INVALID_PAYLOAD),
        ("decimal-point", "120.00", NormalizationError.INVALID_PAYLOAD),
        ("currency", "€120.00", NormalizationError.INVALID_PAYLOAD),
        ("padded", " 12000 ", NormalizationError.INVALID_PAYLOAD),
        ("empty", "", NormalizationError.INVALID_PAYLOAD),
        ("words", "twelve thousand", NormalizationError.INVALID_PAYLOAD),
        ("nan", "NaN", NormalizationError.INVALID_PAYLOAD),
        ("infinity", "Infinity", NormalizationError.INVALID_PAYLOAD),
        ("negative", "-12000", NormalizationError.INVALID_NUMERIC_RANGE),
        # Numeric(12, 2) holds 9999999999.99 and not a cent more.
        ("too-large", "1000000000000", NormalizationError.INVALID_NUMERIC_RANGE),
    ],
)
def test_a_malformed_price_is_a_deterministic_rejection(label: str, bad: object, expected: str) -> None:
    """Never a silent None: a bad price must be visible, once, as failed."""
    payload = booking_created()
    payload["booking_price"] = bad

    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    assert excinfo.value.code == expected, label


def test_the_largest_representable_price_is_still_accepted() -> None:
    booking = _normalize(set_booking_price(booking_created(), 999999999999))
    assert booking is not None
    assert booking.total_cost == Decimal("9999999999.99")


@pytest.mark.parametrize(
    "projection",
    ["120,00", "€120.00", "1.2e2", " 120.00 ", "", "120.000", 120.0, True],
)
def test_a_malformed_projection_is_refused_rather_than_ignored(projection: object) -> None:
    payload = set_booking_price(booking_created(), 12000)
    payload["booking_price_float"] = projection

    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    assert excinfo.value.code == NormalizationError.INVALID_PAYLOAD


def test_no_price_rejection_ever_echoes_the_amount() -> None:
    """Error codes reach the logs; a price is customer data."""
    payload = booking_created()
    payload["booking_price"] = "€1234,56"

    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    text = f"{excinfo.value.code} {excinfo.value}"
    assert "1234" not in text and "€" not in text
    assert excinfo.value.code in NormalizationError.ALL_CODES


def test_service_fields_are_normalized() -> None:
    booking = _normalize(booking_created())
    assert booking is not None
    assert booking.service_id == 5100003
    assert booking.service_name == "Fixture Service"
    assert booking.service_quantity == 1


# ===========================================================================
# PR-7.1 service-category eligibility
# ===========================================================================


def _category_eligibility(category: object, raw_allowlist: object = '["Wimpernverlängerung"]'):
    normalized = normalize_service_category(category)
    record_raw = record_raw_with_service_category(
        {"unrelated": {"kept": True}},
        normalized.value if normalized is not None else None,
    )
    record_raw = record_raw_with_services_count(record_raw, 1)
    return evaluate_service_category(record_raw=record_raw, allowed_categories_raw=raw_allowlist)


def test_production_service_category_is_allowed() -> None:
    result = _category_eligibility("Wimpernverlängerung")
    assert result.allowed is True
    assert result.reason == ALLOWED


@pytest.mark.parametrize(
    "category",
    [
        "wimpernverlängerung",
        " WIMPERNVERLÄNGERUNG ",
        "Wimpernverlängerung\u00a0",
        "Wimpernverlängerung\u2003",
    ],
)
def test_case_and_safe_unicode_whitespace_normalize_for_exact_match(category: str) -> None:
    assert _category_eligibility(category).allowed is True


def test_category_matching_is_exact_not_prefix_or_substring() -> None:
    result = _category_eligibility("Wimpernverlängerung Extra")
    assert result == type(result)(allowed=False, reason=CATEGORY_NOT_ALLOWED)


def test_service_name_never_substitutes_for_a_different_category() -> None:
    payload = booking_created()
    payload["service_name"] = "Wimpernverlängerung"
    payload["services_description"] = "Wimpernverlängerung"
    payload["service_category"] = "Nails"
    booking = _normalize(payload)
    assert booking is not None
    assert booking.service_category == "Nails"
    assert _category_eligibility(booking.service_category).reason == CATEGORY_NOT_ALLOWED


@pytest.mark.parametrize("value", [None, "", "   ", 123, [], "Wimpern\nverlängerung", "\u200b"])
def test_missing_blank_non_string_and_control_categories_are_not_proof(value: object) -> None:
    assert normalize_service_category(value) is None
    assert _category_eligibility(value).reason == CATEGORY_MISSING


def test_absent_category_is_not_reported_as_carried() -> None:
    payload = booking_created()
    payload.pop("service_category")
    booking = _normalize(payload)
    assert booking is not None
    assert booking.service_category is None
    assert not booking.carries("service_category")


@pytest.mark.parametrize("value", [None, "", "  ", {"bad": "shape"}, "bad\x00value"])
def test_present_unusable_category_is_an_explicit_clear(value: object) -> None:
    payload = booking_created()
    payload["service_category"] = value
    booking = _normalize(payload)
    assert booking is not None
    assert booking.service_category is None
    assert booking.carries("service_category")


@pytest.mark.parametrize("raw", ["{not-json", '"Wimpernverlängerung"', "{}", "1", "null"])
def test_malformed_or_non_array_allowlist_is_wholly_invalid(raw: str) -> None:
    parsed = parse_allowed_service_categories(raw)
    assert parsed.configured is True
    assert parsed.valid is False
    assert _category_eligibility("Wimpernverlängerung", raw).reason == ALLOWED_CATEGORIES_INVALID


@pytest.mark.parametrize("raw", ["", "   ", "[]"])
def test_empty_allowlist_is_unconfigured_and_allows_nothing(raw: str) -> None:
    parsed = parse_allowed_service_categories(raw)
    assert parsed.configured is False
    assert parsed.ready is False
    eligibility = _category_eligibility("Wimpernverlängerung", raw)
    assert eligibility.reason == ALLOWED_CATEGORIES_UNCONFIGURED
    assert eligibility.recoverable_configuration is True
    assert eligibility.terminal_business_suppression is False


@pytest.mark.parametrize(
    "values",
    [
        ["Wimpernverlängerung", 7],
        ["Wimpernverlängerung", ""],
        ["Wimpernverlängerung", "bad\nvalue"],
        ["Wimpernverlängerung", " WIMPERNVERLÄNGERUNG "],
    ],
)
def test_one_invalid_or_duplicate_entry_rejects_the_whole_allowlist(values: list[object]) -> None:
    parsed = parse_allowed_service_categories(json.dumps(values))
    assert parsed.valid is False
    assert parsed.keys == frozenset()


def test_allowlist_count_and_category_length_are_bounded() -> None:
    too_many = [f"category-{index}" for index in range(MAX_ALLOWED_SERVICE_CATEGORIES + 1)]
    assert parse_allowed_service_categories(json.dumps(too_many)).valid is False
    assert normalize_service_category("x" * (MAX_SERVICE_CATEGORY_LENGTH + 1)) is None
    assert parse_allowed_service_categories(json.dumps(["x" * (MAX_SERVICE_CATEGORY_LENGTH + 1)])).valid is False


def test_parser_result_and_eligibility_never_echo_raw_configuration() -> None:
    secret_like_raw = '["Wimpernverlängerung", "customer@example.invalid", 7]'
    parsed = parse_allowed_service_categories(secret_like_raw)
    result = _category_eligibility("Wimpernverlängerung", secret_like_raw)
    assert parsed.valid is False
    assert result.reason == ALLOWED_CATEGORIES_INVALID
    assert secret_like_raw not in repr(parsed)
    assert secret_like_raw not in repr(result)


def test_invalid_allowlist_is_explicitly_recoverable_not_business_suppression() -> None:
    result = _category_eligibility("Wimpernverlängerung", "{invalid")
    assert result.reason == ALLOWED_CATEGORIES_INVALID
    assert result.recoverable_configuration is True
    assert result.terminal_business_suppression is False


@pytest.mark.parametrize(
    ("services_count", "expected_reason"),
    [
        (None, SERVICE_COUNT_UNPROVEN),
        (0, SERVICE_COUNT_UNPROVEN),
        (2, CATEGORY_AMBIGUOUS_MULTI_SERVICE),
        (99, CATEGORY_AMBIGUOUS_MULTI_SERVICE),
    ],
)
def test_only_persisted_single_service_count_can_authorize(
    services_count: int | None,
    expected_reason: str,
) -> None:
    raw = record_raw_with_service_category({}, "Wimpernverlängerung")
    raw = record_raw_with_services_count(raw, services_count)
    result = evaluate_service_category(
        record_raw=raw,
        allowed_categories_raw='["Wimpernverlängerung"]',
    )
    assert result.reason == expected_reason
    assert result.terminal_business_suppression is True


@pytest.mark.parametrize("value", [None, -1, True, "1", 1.5, {}, 2**63])
def test_present_unusable_service_count_clears_proof_without_rejecting_event(value: object) -> None:
    payload = booking_created()
    payload["services_count"] = value
    booking = _normalize(payload)
    assert booking is not None
    assert booking.services_count is None
    assert booking.carries("services_count")


def test_zero_service_count_remains_a_display_value_but_not_eligibility_proof() -> None:
    payload = booking_created()
    payload["services_count"] = 0
    booking = _normalize(payload)
    assert booking is not None
    assert booking.services_count == 0
    raw = record_raw_with_services_count({}, booking.services_count)
    assert services_count_from_record_raw(raw) is None


def test_absent_service_count_preserves_patch_semantics() -> None:
    payload = booking_updated()
    payload.pop("services_count")
    booking = _normalize(payload)
    assert booking is not None
    assert booking.services_count is None
    assert not booking.carries("services_count")


def test_category_snapshot_returns_a_new_dict_and_preserves_unrelated_keys() -> None:
    original = {"outside": {"kept": True}, "easyweek": {"other": "kept"}}
    updated = record_raw_with_service_category(original, "Wimpernverlängerung")
    assert updated is not original
    assert original == {"outside": {"kept": True}, "easyweek": {"other": "kept"}}
    assert updated == {
        "outside": {"kept": True},
        "easyweek": {"other": "kept", "service_category": "Wimpernverlängerung"},
    }

    cleared = record_raw_with_service_category(updated, None)
    assert cleared == {"outside": {"kept": True}, "easyweek": {"other": "kept"}}


def test_service_count_snapshot_is_minimal_patchable_and_revalidated() -> None:
    original = {"outside": {"kept": True}, "easyweek": {"service_category": "Wimpernverlängerung"}}
    updated = record_raw_with_services_count(original, 2)
    assert updated is not original
    assert services_count_from_record_raw(updated) == 2
    assert original == {
        "outside": {"kept": True},
        "easyweek": {"service_category": "Wimpernverlängerung"},
    }

    for invalid in (None, 0, -1, True):
        cleared = record_raw_with_services_count(updated, invalid)  # type: ignore[arg-type]
        assert services_count_from_record_raw(cleared) is None
        assert cleared["easyweek"] == {"service_category": "Wimpernverlängerung"}


# ===========================================================================
# Present-field tracking drives patch semantics (review fix 6)
# ===========================================================================


def test_absent_fields_are_reported_as_not_carried() -> None:
    payload = booking_created()
    for key in ("customer_phone", "customer_email", "booking_date_start", "service_id"):
        payload.pop(key, None)
    booking = _normalize(payload)
    assert booking is not None
    for field in ("phone_e164", "email", "starts_at", "service_id"):
        assert not booking.carries(field), f"{field} must not be reported as carried"


def test_present_but_empty_is_still_carried() -> None:
    """An explicit clear is authoritative; only absence preserves."""
    payload = booking_created()
    payload["booking_attributes.booking_comment"] = ""
    booking = _normalize(payload)
    assert booking is not None
    assert booking.carries("comment")
    assert booking.comment is None


# ===========================================================================
# canonical_booking_uuid — the single definition of booking identity
# ===========================================================================
#
# Shared by capture, the PR-4 migration backfill, the claim ordering key and the
# normalizer, so all four agree on which booking a delivery belongs to.

_CANONICAL = uuid.UUID("ac15372d-7422-4fc6-8fcb-b520bbffa669")


@pytest.mark.parametrize(
    "raw_uid",
    [
        "ac15372d-7422-4fc6-8fcb-b520bbffa669",
        "AC15372D-7422-4FC6-8FCB-B520BBFFA669",
        "  ac15372d-7422-4fc6-8fcb-b520bbffa669  ",
        "\tac15372d-7422-4fc6-8fcb-b520bbffa669\n",
        "{ac15372d-7422-4fc6-8fcb-b520bbffa669}",
        "ac15372d74224fc68fcbb520bbffa669",
        "urn:uuid:ac15372d-7422-4fc6-8fcb-b520bbffa669",
    ],
    ids=["canonical", "uppercase", "spaces", "whitespace", "braced", "compact", "urn"],
)
def test_every_textual_form_collapses_to_one_key(raw_uid) -> None:
    assert canonical_booking_uuid({"uid": raw_uid}) == _CANONICAL


@pytest.mark.parametrize(
    "payload",
    [
        {"uid": "not-a-uuid"},
        {"uid": ""},
        {"uid": "   "},
        {"uid": 12345},
        {"uid": None},
        {"uid": ["ac15372d-7422-4fc6-8fcb-b520bbffa669"]},
        {"uid": {"value": "ac15372d-7422-4fc6-8fcb-b520bbffa669"}},
        {},
        [],
        "a string",
        None,
        42,
    ],
    ids=[
        "garbage",
        "empty",
        "blank",
        "number",
        "null",
        "list",
        "object",
        "missing",
        "array-payload",
        "string-payload",
        "none-payload",
        "int-payload",
    ],
)
def test_an_unusable_uid_yields_none_and_never_raises(payload) -> None:
    """Capture must not be broken by a malformed research-grade delivery."""
    assert canonical_booking_uuid(payload) is None


def test_the_normalizer_agrees_with_the_shared_parser() -> None:
    """The domain UUID and the ordering key must be the same value."""
    for raw_uid in (
        "ac15372d-7422-4fc6-8fcb-b520bbffa669",
        "AC15372D-7422-4FC6-8FCB-B520BBFFA669",
        "{ac15372d-7422-4fc6-8fcb-b520bbffa669}",
        "ac15372d74224fc68fcbb520bbffa669",
    ):
        payload = booking_created()
        payload["uid"] = raw_uid
        booking = _normalize(payload)
        assert booking is not None
        assert booking.booking_uuid == canonical_booking_uuid(payload)
