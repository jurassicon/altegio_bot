"""Sanitized EasyWeek webhook fixtures.

These are **sanitized derivatives** of the real captured schema — NOT raw
production dumps. Every identifier, contact detail, name, address, coordinate,
comment and UTM value is fake. What is faithfully reproduced from the confirmed
live capture is the *structure*:

* every field lives at the ROOT of the object — the payload is flat;
* some root keys literally contain dots (``booking_attributes.booking_comment``,
  ``customer_attributes.customer_phone``). They are flat keys, not nested
  objects, and a parser that splits on ``.`` would silently miss them;
* ``uid``, ``booking_hash_id``, ``booking_page``, ``timezone`` and every
  timestamp arrive as JSON **strings**;
* ``id``, ``customer_id``, ``location_id``, ``booking_duration``,
  ``service_id`` and ``visits_total`` arrive as JSON **numbers**;
* ``booking_page`` is present on all four lifecycle events and forms the exact
  pair ``https://eyw.me/r/<booking_hash_id>``.

The only value shared with production is the *shape* of the manage-link host,
which is a public constant of the EasyWeek product, plus the test location id
below — deliberately NOT the production location id.
"""

from __future__ import annotations

import copy
from typing import Any

# A test location id that is deliberately NOT the production one. The real
# numeric location lives only in easyweek.env.
TEST_LOCATION_ID = 999001
FOREIGN_LOCATION_ID = 888002

TEST_BOOKING_UUID = "11111111-2222-4333-8444-555555555555"
TEST_BOOKING_ID = 4200001
TEST_CUSTOMER_ID = 7300002
TEST_BOOKING_HASH_ID = "90000001"
TEST_BOOKING_PAGE = f"https://eyw.me/r/{TEST_BOOKING_HASH_ID}"


def _base_payload() -> dict[str, Any]:
    """The root-level shape confirmed by live capture, with fake values."""
    return {
        # --- identity -----------------------------------------------------
        "uid": TEST_BOOKING_UUID,
        "id": TEST_BOOKING_ID,
        "customer_id": TEST_CUSTOMER_ID,
        "location_id": TEST_LOCATION_ID,
        "location_uuid": "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
        # --- manage link (string hash, exact pair) -------------------------
        "booking_hash_id": TEST_BOOKING_HASH_ID,
        "booking_page": TEST_BOOKING_PAGE,
        # --- timestamps: ISO with a real offset ----------------------------
        "booking_created_at": "2026-08-01T09:00:00+0000",
        "booking_date_start": "2026-08-03T10:00:00+0000",
        "booking_date_end": "2026-08-03T11:00:00+0000",
        "booking_date_start_tz": "2026-08-03T12:00:00+0200",
        "timezone": "Europe/Berlin",
        "booking_duration": 60,
        # --- localized display fields: never a source of truth -------------
        "booking_status": "New appointment",
        "booking_created_at_formatted": "01.08.2026 09:00",
        "booking_date_start_formatted": "03.08.2026 12:00",
        "booking_date_end_formatted": "03.08.2026 13:00",
        "booking_duration_formatted": "1 h",
        "date": "03.08.2026",
        "day": "Monday",
        "time": "12:00",
        "month_number": "08",
        # --- customer (all fake) -------------------------------------------
        "customer_full_name": "Test Person",
        "customer_name": "Test Person",
        "customer_first_name": "Test",
        "customer_last_name": "Person",
        "customer_middle_name": "",
        "customer_phone": "+49000000000",
        "customer_email": "test.person@example.invalid",
        "customer_birthday": "",
        "customer_comment": "",
        # --- literal flat keys containing dots ------------------------------
        "booking_attributes.booking_comment": "fixture comment",
        "booking_attributes.booking_internal_note": "fixture internal note",
        "customer_attributes.customer_first_name": "Test",
        "customer_attributes.customer_last_name": "Person",
        "customer_attributes.customer_phone": "+49000000000",
        "customer_attributes.customer_email": "test.person@example.invalid",
        "customer_attributes.customer_birthday": None,
        "customer_attributes.customer_comment": None,
        "customer_attributes.customer_middle_name": None,
        # --- service / staff ------------------------------------------------
        "service_id": 5100003,
        "service_name": "Fixture Service",
        "service_category": "Fixture Category",
        "service_related": "",
        "services_count": 1,
        "services_description": "Fixture Service",
        "users_description": "Fixture Specialist",
        "user_name": "Fixture Specialist",
        "user_email": "specialist@example.invalid",
        "user_phone": "+49000000001",
        # --- location / branding --------------------------------------------
        "location_name": "Fixture Location",
        "location_city": "Fixture City",
        "location_street": "Fixture Street 1",
        "location_zip": "00000",
        "location_apt": "",
        "location_description": None,
        "location_address_formatted": "Fixture Street 1, Fixture City",
        "location_lat": 0.0,
        "location_lng": 0.0,
        "address": "Fixture Street 1, Fixture City",
        "company_name": "Fixture Company",
        "company_page": "https://example.invalid/company",
        "company_logo": "https://example.invalid/logo.png",
        "ref_title": "Fixture",
        # --- money -----------------------------------------------------------
        "booking_price": "35.00",
        "booking_price_int": 3500,
        "booking_price_float": "35.00",
        "booking_price_formatted": "€35.00",
        "booking_price_currency": "EUR",
        "booking_paid_amount": "0.00",
        "booking_paid_amount_float": "0.00",
        "booking_paid_amount_formatted": "€0.00",
        "booked_sum_amount": 3500,
        "paid_sum_amount": 0,
        "customer_paid_amount": "0.00",
        "customer_paid_amount_float": "0.00",
        "customer_paid_amount_formatted": "€0.00",
        "quantity": 1,
        "slots_count": 1,
        "visits_total": 1,
        # --- misc -------------------------------------------------------------
        "booking_description": "",
        "booking_source": "fixture",
        "my_url": "https://my.easyweek.io/bookings/4200001",
        "utm_source": "",
        "utm_medium": "",
        "utm_campaign": "",
        "utm_content": "",
        "utm_term": "",
    }


def booking_created() -> dict[str, Any]:
    return _base_payload()


def booking_updated() -> dict[str, Any]:
    """An edit that did NOT move the appointment (no reschedule)."""
    payload = _base_payload()
    payload["booking_status"] = "Updated appointment"
    payload["booking_attributes.booking_comment"] = "fixture comment edited"
    payload["service_name"] = "Fixture Service Deluxe"
    payload["services_description"] = "Fixture Service Deluxe"
    return payload


def booking_rescheduled() -> dict[str, Any]:
    """Same booking, moved two hours later."""
    payload = _base_payload()
    payload["booking_status"] = "Rescheduled appointment"
    payload["booking_date_start"] = "2026-08-03T12:00:00+0000"
    payload["booking_date_end"] = "2026-08-03T13:00:00+0000"
    payload["booking_date_start_tz"] = "2026-08-03T14:00:00+0200"
    payload["booking_date_start_formatted"] = "03.08.2026 14:00"
    payload["time"] = "14:00"
    return payload


def booking_canceled() -> dict[str, Any]:
    payload = _base_payload()
    payload["booking_status"] = "Canceled appointment"
    return payload


def booking_created_resend() -> dict[str, Any]:
    """A byte-identical Resend of ``booking-created``.

    EasyWeek's Resend button produces another delivery of the same payload.
    Capture stores it as its own row on purpose, so idempotency has to hold at
    the domain/job level, not via a unique constraint on the capture table.
    """
    return copy.deepcopy(booking_created())


def booking_created_full_shape() -> dict[str, Any]:
    """Base payload plus root fields the parser must ignore.

    Proves the parser tolerates unknown root keys and does not choke on the
    literal dotted keys — rather than silently depending on an exact key set.
    """
    payload = _base_payload()
    payload["some_future_field"] = "ignored"
    payload["another.dotted.future.key"] = "ignored"
    payload["nested_object_future"] = {"a": 1, "b": [1, 2, 3]}
    payload["null_future_field"] = None
    return payload
