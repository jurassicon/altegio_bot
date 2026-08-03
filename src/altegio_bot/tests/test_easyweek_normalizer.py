"""Unit contract for the EasyWeek normalizer (PR-4).

Every rejection here is deterministic and fail-closed: the normalizer refuses
rather than guesses, and the reason it reports is a fixed code that can never
carry a payload value.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from altegio_bot.easyweek_normalizer import (
    CREATE,
    DELETE,
    IGNORE,
    UPDATE,
    NormalizationError,
    easyweek_job_dedupe_key,
    extract_manage_link,
    map_event_hint,
    normalize_booking_hash_id,
    normalize_event,
    parse_iso_utc,
)
from altegio_bot.tests.easyweek_fixtures import (
    FOREIGN_LOCATION_ID,
    TEST_BOOKING_HASH_ID,
    TEST_BOOKING_ID,
    TEST_BOOKING_PAGE,
    TEST_BOOKING_UUID,
    TEST_CUSTOMER_ID,
    TEST_LOCATION_ID,
    booking_canceled,
    booking_created,
    booking_created_full_shape,
    booking_created_resend,
    booking_rescheduled,
    booking_updated,
)


def _normalize(payload, *, event_hint="booking-created", truncated=False, location=TEST_LOCATION_ID):
    return normalize_event(
        event_hint=event_hint,
        payload=payload,
        body_truncated=truncated,
        expected_location_id=location,
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


@pytest.mark.parametrize("configured", [0, -1])
def test_unconfigured_location_fails_closed(configured: int) -> None:
    """The payload must never be able to supply the location we own."""
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(booking_created(), location=configured)
    assert excinfo.value.code == NormalizationError.INVALID_LOCATION_ID


def test_company_id_comes_from_config_not_from_payload() -> None:
    booking = _normalize(booking_created())
    assert booking is not None
    assert booking.company_id == TEST_LOCATION_ID


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


def test_price_comes_from_the_numeric_field_in_minor_units() -> None:
    booking = _normalize(booking_created())
    assert booking is not None
    assert booking.total_cost == Decimal("35.00"), "booking_price_int is cents"


def test_localized_price_strings_are_not_used() -> None:
    payload = booking_created()
    payload["booking_price"] = "999,99"
    payload["booking_price_formatted"] = "€999.99"
    payload["booking_price_float"] = "999.99"
    booking = _normalize(payload)
    assert booking is not None
    assert booking.total_cost == Decimal("35.00")


def test_absent_price_yields_no_total_cost() -> None:
    """Absent is "unchanged", not an error."""
    payload = booking_created()
    payload["booking_price_int"] = None
    booking = _normalize(payload)
    assert booking is not None
    assert booking.total_cost is None


@pytest.mark.parametrize(
    ("bad", "expected"),
    [
        ("3500", NormalizationError.INVALID_PAYLOAD),
        (True, NormalizationError.INVALID_PAYLOAD),
        (1.5, NormalizationError.INVALID_PAYLOAD),
        (-1, NormalizationError.INVALID_NUMERIC_RANGE),
    ],
)
def test_malformed_price_is_a_deterministic_rejection(bad, expected: str) -> None:
    """Never a silent None: a bad price must be visible, once, as failed."""
    payload = booking_created()
    payload["booking_price_int"] = bad
    with pytest.raises(NormalizationError) as excinfo:
        _normalize(payload)
    assert excinfo.value.code == expected


def test_service_fields_are_normalized() -> None:
    booking = _normalize(booking_created())
    assert booking is not None
    assert booking.service_id == 5100003
    assert booking.service_name == "Fixture Service"
    assert booking.service_quantity == 1


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
