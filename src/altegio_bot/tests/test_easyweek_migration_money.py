"""PR-11.1 revision 16: zero is a value, absence is not.

The bug this file pins down: the classifier read prices through a helper that
returned ``None`` for anything not *positive*, so ``cost=90, cost_to_pay=0`` — a
booking the customer had been promised for free — read as "no override" and
migrated at 90 EUR.

The same helper made a missing service duration indistinguishable from a matching
one, so a slot hand-stretched to 90 minutes migrated as the standard 60.
"""

from __future__ import annotations

import json
from decimal import Decimal

import pytest

from altegio_bot.easyweek_migration.classify import (
    BLOCK_CUSTOM_DURATION,
    BLOCK_CUSTOM_PRICE,
    BLOCK_DURATION_UNKNOWN,
    BLOCK_PRICE_BASELINE_MISSING,
    BLOCK_PRICE_MALFORMED,
    BLOCKED,
    READY,
    classify_record,
)
from altegio_bot.easyweek_migration.cutover import parse_cutover
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, RASTATT_COMPANY_ID, parse_manifest
from altegio_bot.easyweek_migration.money import (
    ABSENT,
    Amount,
    AmountError,
    DurationError,
    amounts_differ,
    read_amount,
    read_duration_minutes,
    read_duration_seconds,
)
from altegio_bot.tests.test_easyweek_migration_planning import (
    CUSTOMER_PHONE,
    KA_LOCATION_UUID,
    KA_SERVICE_ID,
    KA_SERVICE_UUID,
    KA_STAFF_ID,
    KA_STAFF_UUID,
    RA_LOCATION_UUID,
    RA_SERVICE_ID,
    RA_SERVICE_UUID,
    RA_STAFF_ID,
    RA_STAFF_UUID,
)

# ---------------------------------------------------------------------------
# read_amount
# ---------------------------------------------------------------------------


def test_zero_is_a_value_not_an_absence():
    """The whole bug in one assertion."""
    zero = read_amount(0)
    assert zero.present
    assert zero.is_zero
    assert read_amount(None) == ABSENT
    assert not read_amount(None).present


def test_money_is_read_exactly_not_through_float():
    assert read_amount("0.1").value + read_amount("0.2").value == Decimal("0.3")
    # The same money written two ways is not an override.
    assert not amounts_differ(read_amount(90), read_amount("90.00"))
    # A cent of difference is.
    assert amounts_differ(read_amount("90.00"), read_amount("89.99"))


@pytest.mark.parametrize("raw", [True, False])
def test_a_boolean_is_never_a_price(raw):
    """`True == 1`, so a sloppy check reads `true` as a one-euro service."""
    with pytest.raises(AmountError):
        read_amount(raw)


@pytest.mark.parametrize("raw", [float("nan"), float("inf"), float("-inf"), -1, "-5.00", "abc", [], {}])
def test_malformed_or_negative_amounts_refuse(raw):
    with pytest.raises(AmountError):
        read_amount(raw)


def test_an_absent_amount_never_compares_as_different():
    assert not amounts_differ(ABSENT, read_amount(90))
    assert not amounts_differ(read_amount(90), ABSENT)
    assert not amounts_differ(Amount(present=False), Amount(present=False))


# ---------------------------------------------------------------------------
# read_duration
# ---------------------------------------------------------------------------


def test_a_whole_minute_duration_reads():
    assert read_duration_seconds(3600).minutes == 60
    assert read_duration_seconds(None).present is False


@pytest.mark.parametrize("raw", [0, -60, 90, 3630, True, float("nan"), float("inf"), "abc"])
def test_zero_negative_fractional_and_malformed_durations_refuse(raw):
    with pytest.raises(DurationError):
        read_duration_seconds(raw)


@pytest.mark.parametrize("raw", [0, -1, True, "60", 1.5])
def test_a_catalogue_duration_must_be_a_positive_whole_minute_integer(raw):
    with pytest.raises(DurationError):
        read_duration_minutes(raw)


# ---------------------------------------------------------------------------
# The classifier, with a real catalogue baseline
# ---------------------------------------------------------------------------

CUSTOMER_UUID = "77777777-7777-4777-8777-777777777777"


def manifest_with(*, catalog_price: str, catalog_minutes: int = 60):
    return parse_manifest(
        json.dumps(
            {
                "manifest_id": "money-test",
                "branches": {
                    str(KARLSRUHE_COMPANY_ID): {
                        "altegio_company_id": KARLSRUHE_COMPANY_ID,
                        "easyweek_location_id": 308001,
                        "easyweek_location_uuid": KA_LOCATION_UUID,
                        "staff": {str(KA_STAFF_ID): KA_STAFF_UUID},
                        "services": {
                            str(KA_SERVICE_ID): {
                                "easyweek_service_uuid": KA_SERVICE_UUID,
                                "catalog_duration_minutes": catalog_minutes,
                                "catalog_price": catalog_price,
                            }
                        },
                    },
                    str(RASTATT_COMPANY_ID): {
                        "altegio_company_id": RASTATT_COMPANY_ID,
                        "easyweek_location_id": 315001,
                        "easyweek_location_uuid": RA_LOCATION_UUID,
                        "staff": {str(RA_STAFF_ID): RA_STAFF_UUID},
                        "services": {
                            str(RA_SERVICE_ID): {
                                "easyweek_service_uuid": RA_SERVICE_UUID,
                                "catalog_duration_minutes": 60,
                                "catalog_price": "90.00",
                            }
                        },
                    },
                },
            }
        )
    )


def booking(*, service: dict, seance_length: object = 3600):
    return {
        "id": 900001,
        "date": "2026-09-10 14:00:00",
        "staff_id": KA_STAFF_ID,
        "seance_length": seance_length,
        "client": {"phone": CUSTOMER_PHONE},
        "services": [service],
    }


def classify(record, *, manifest, directory):
    return classify_record(
        record,
        company_id=KARLSRUHE_COMPANY_ID,
        manifest=manifest,
        directory=directory,
        cutover=parse_cutover("2026-09-01T00:00:00Z"),
        ledger=None,
    )


@pytest.fixture
def directory():
    from altegio_bot.easyweek_migration.customers import CustomerDirectory

    return CustomerDirectory(valid=True, by_phone={CUSTOMER_PHONE: [CUSTOMER_UUID]})


def test_a_full_discount_to_zero_is_blocked(directory):
    """cost=90, cost_to_pay=0. The regression this whole module exists for."""
    decision = classify(
        booking(service={"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 0}),
        manifest=manifest_with(catalog_price="90.00"),
        directory=directory,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_CUSTOM_PRICE


def test_a_zero_first_cost_against_a_paid_catalogue_is_blocked(directory):
    decision = classify(
        booking(service={"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "first_cost": 0}),
        manifest=manifest_with(catalog_price="90.00"),
        directory=directory,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_CUSTOM_PRICE


def test_a_genuinely_free_catalogue_service_migrates(directory):
    """Zero is only an override when the catalogue says otherwise."""
    decision = classify(
        booking(service={"id": KA_SERVICE_ID, "cost": 0, "cost_to_pay": 0}),
        manifest=manifest_with(catalog_price="0"),
        directory=directory,
    )
    assert decision.outcome == READY


def test_a_booking_with_no_price_at_all_is_blocked(directory):
    """No stated price means an override would be invisible."""
    decision = classify(
        booking(service={"id": KA_SERVICE_ID}),
        manifest=manifest_with(catalog_price="90.00"),
        directory=directory,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_PRICE_BASELINE_MISSING


@pytest.mark.parametrize("bad", [float("nan"), float("inf"), True, -5])
def test_a_malformed_price_is_blocked_not_ignored(directory, bad):
    decision = classify(
        booking(service={"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": bad}),
        manifest=manifest_with(catalog_price="90.00"),
        directory=directory,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_PRICE_MALFORMED


def test_a_zero_discount_is_not_a_discount(directory):
    decision = classify(
        booking(service={"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "discount": 0}),
        manifest=manifest_with(catalog_price="90.00"),
        directory=directory,
    )
    assert decision.outcome == READY


def test_a_price_that_differs_from_the_catalogue_is_blocked(directory):
    decision = classify(
        booking(service={"id": KA_SERVICE_ID, "cost": 70.0, "cost_to_pay": 70.0}),
        manifest=manifest_with(catalog_price="90.00"),
        directory=directory,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_CUSTOM_PRICE


def test_a_stretched_slot_is_blocked_even_when_altegio_states_no_service_duration(directory):
    """The second half of the bug: no service baseline used to mean no override."""
    decision = classify(
        booking(service={"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0}, seance_length=5400),
        manifest=manifest_with(catalog_price="90.00", catalog_minutes=60),
        directory=directory,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_CUSTOM_DURATION


def test_a_booking_with_no_duration_at_all_is_blocked(directory):
    decision = classify(
        booking(service={"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0}, seance_length=None),
        manifest=manifest_with(catalog_price="90.00"),
        directory=directory,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_DURATION_UNKNOWN


@pytest.mark.parametrize("bad", [0, -60, 3630, True])
def test_zero_negative_and_fractional_durations_are_blocked(directory, bad):
    decision = classify(
        booking(service={"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0}, seance_length=bad),
        manifest=manifest_with(catalog_price="90.00"),
        directory=directory,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_CUSTOM_DURATION


def test_a_service_duration_disagreeing_with_the_manifest_baseline_is_blocked(directory):
    """A stale manifest baseline must surface, not be quietly preferred."""
    decision = classify(
        booking(
            service={"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "seance_length": 5400},
            seance_length=3600,
        ),
        manifest=manifest_with(catalog_price="90.00", catalog_minutes=60),
        directory=directory,
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_CUSTOM_DURATION


def test_a_correct_price_and_duration_migrate(directory):
    decision = classify(
        booking(service={"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "seance_length": 3600}),
        manifest=manifest_with(catalog_price="90.00", catalog_minutes=60),
        directory=directory,
    )
    assert decision.outcome == READY
    assert decision.duration_minutes == 60


def test_a_manifest_service_entry_needs_both_baselines():
    """An unfinished entry is unfinished, not a service without a price."""
    for missing in ("catalog_price", "catalog_duration_minutes"):
        entry = {
            "easyweek_service_uuid": KA_SERVICE_UUID,
            "catalog_duration_minutes": 60,
            "catalog_price": "90.00",
        }
        entry.pop(missing)
        raw = json.dumps(
            {
                "manifest_id": "partial",
                "branches": {
                    str(KARLSRUHE_COMPANY_ID): {
                        "altegio_company_id": KARLSRUHE_COMPANY_ID,
                        "easyweek_location_id": 308001,
                        "easyweek_location_uuid": KA_LOCATION_UUID,
                        "staff": {str(KA_STAFF_ID): KA_STAFF_UUID},
                        "services": {str(KA_SERVICE_ID): entry},
                    }
                },
            }
        )
        assert not parse_manifest(raw).valid
