"""Rows migrated before the binding model must stay recognised (PR-11.2).

The source fingerprint grew: it now carries the mutation contract, every service
binding in source order and the quantity of each. Every ledger row already in
production carries the OLD hash, and nothing in PostgreSQL can be recomputed
into the new one without re-proving the booking end to end.

Without a deliberate answer, the first run after the deploy would read every
migrated booking as `source_changed_since_ledger` — which is not a warning but a
cascade: `already_migrated` stops working, so the previous-wave context fails,
so reconcile, resolve and rollback all refuse, and the next wave cannot run over
bookings that are perfectly fine.

The answer here is recognition, never repair: the legacy hash is recomputed from
the live source and compared cryptographically, and it is only offered for the
exact shape the legacy format ever described — one service, one unit, the single
contract. Everything else is a mismatch, and a mismatch still fails closed.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

import pytest

from altegio_bot.easyweek_migration.classify import (
    ALREADY_MIGRATED,
    BLOCK_CUSTOM_DURATION,
    BLOCK_CUSTOM_PRICE,
    BLOCK_SERVICE_QUANTITY,
    BLOCK_SOURCE_CHANGED,
    BLOCKED,
    READY,
    LedgerView,
    classify_record,
    fingerprint_matches_decision,
    legacy_source_fingerprint,
)
from altegio_bot.easyweek_migration.cutover import parse_cutover
from altegio_bot.easyweek_migration.manifest import KARLSRUHE_COMPANY_ID, parse_manifest
from altegio_bot.tests.test_easyweek_migration_planning import (
    CUSTOMER_PHONE,
    CUSTOMER_UUID,
    KA_SERVICE_ID,
    KA_SERVICE_UUID,
    KA_STAFF_ID,
    KA_STAFF_UUID,
    directory_with,
    manifest_text,
    record,
)

CUTOVER = "2026-09-01T00:00:00Z"
TARGET_UUID = "0e9a1111-2222-4333-8444-555566667777"


def manifest():
    parsed = parse_manifest(manifest_text())
    assert parsed.valid, parsed.reason
    return parsed


def classify(payload: dict[str, Any], *, ledger: LedgerView | None = None):
    return classify_record(
        payload,
        company_id=KARLSRUHE_COMPANY_ID,
        manifest=manifest(),
        directory=directory_with(),
        cutover=parse_cutover(CUTOVER),
        ledger=ledger,
    )


def legacy_of(payload: dict[str, Any]) -> str:
    """The hash the pre-binding production code would have stored for this row.

    Computed through the frozen legacy function from a decision the CURRENT
    classifier produced, so the test cannot drift from the code it is pinning.
    """
    decision = classify(payload)
    assert decision.outcome == READY, decision.reason
    assert decision.starts_at_utc is not None
    assert decision.easyweek_staff_uuid is not None
    assert decision.easyweek_service_uuid is not None
    assert decision.easyweek_customer_uuid is not None
    assert decision.duration_minutes is not None
    return legacy_source_fingerprint(
        company_id=decision.source_company_id,
        record_id=decision.source_record_id,
        starts_at_utc=decision.starts_at_utc,
        staff_uuid=decision.easyweek_staff_uuid,
        service_uuid=decision.easyweek_service_uuid,
        duration_minutes=decision.duration_minutes,
        customer_uuid=decision.easyweek_customer_uuid,
    )


def legacy_ledger(payload: dict[str, Any]) -> LedgerView:
    return LedgerView(status="created", target_booking_uuid=TARGET_UUID, source_fingerprint=legacy_of(payload))


# ---------------------------------------------------------------------------
# The frozen algorithm
# ---------------------------------------------------------------------------


def test_the_legacy_algorithm_is_the_one_production_actually_ran():
    """Pinned field for field against the shipped implementation.

    Not a re-derivation: this blob is the literal one from the code that wrote
    every existing ledger row. If a refactor ever changes the order, the
    separator or a single `str()`, this test fails — and it must, because the
    rows in PostgreSQL cannot be recomputed.
    """
    decision = classify(record())
    assert decision.starts_at_utc is not None
    blob = "|".join(
        [
            str(KARLSRUHE_COMPANY_ID),
            "900001",
            decision.starts_at_utc.isoformat(),
            KA_STAFF_UUID,
            KA_SERVICE_UUID,
            "60",
            CUSTOMER_UUID,
        ]
    )
    assert legacy_of(record()) == hashlib.sha256(blob.encode("utf-8")).hexdigest()


def test_the_new_fingerprint_is_not_the_legacy_one():
    """They must differ, or none of this would be needed — and quantity is why."""
    decision = classify(record())
    assert decision.source_fingerprint != legacy_of(record())


# ---------------------------------------------------------------------------
# An unchanged legacy row is still migrated
# ---------------------------------------------------------------------------


def test_an_unchanged_legacy_row_is_already_migrated():
    decision = classify(record(), ledger=legacy_ledger(record()))

    assert decision.outcome == ALREADY_MIGRATED
    assert decision.target_booking_uuid == TARGET_UUID
    # It carries the full resolution, which is what reconcile, rollback and the
    # previous-wave context all need in order to prove the target.
    assert decision.easyweek_staff_uuid == KA_STAFF_UUID
    assert decision.easyweek_service_uuid == KA_SERVICE_UUID
    assert [item.source_amount for item in decision.bindings] == [1]


def test_a_current_format_row_is_still_recognised():
    """The new format is not sacrificed to support the old one."""
    current = classify(record()).source_fingerprint
    assert current is not None
    decision = classify(
        record(),
        ledger=LedgerView(status="created", target_booking_uuid=TARGET_UUID, source_fingerprint=current),
    )
    assert decision.outcome == ALREADY_MIGRATED


def test_an_unrelated_hash_is_never_accepted_as_legacy():
    decision = classify(
        record(),
        ledger=LedgerView(status="created", target_booking_uuid=TARGET_UUID, source_fingerprint="0" * 64),
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_SOURCE_CHANGED


def test_an_empty_stored_fingerprint_is_not_a_match():
    decision = classify(
        record(),
        ledger=LedgerView(status="created", target_booking_uuid=TARGET_UUID, source_fingerprint=""),
    )
    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_SOURCE_CHANGED


# ---------------------------------------------------------------------------
# A changed source is still a changed source
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "change, expected_reason",
    [
        # The manifest maps one master, so "another master" leaves the wave
        # entirely — a different refusal, and still a refusal.
        pytest.param({"staff_id": 5002}, None, id="another-master"),
        pytest.param({"date": "2026-09-10 16:00:00"}, BLOCK_SOURCE_CHANGED, id="another-time"),
        pytest.param(
            {"services": [{"id": 6002, "cost": 50.0, "cost_to_pay": 50.0, "amount": 1}]},
            None,
            id="another-service",
        ),
        pytest.param({"seance_length": 5400}, BLOCK_CUSTOM_DURATION, id="stretched-slot"),
        pytest.param(
            {"services": [{"id": KA_SERVICE_ID, "cost": 120.0, "cost_to_pay": 120.0, "amount": 1}]},
            BLOCK_CUSTOM_PRICE,
            id="repriced",
        ),
    ],
)
def test_a_legacy_row_whose_source_moved_stays_fail_closed(change: dict[str, Any], expected_reason: str | None):
    """The legacy hash of the ORIGINAL booking, against a source that changed.

    Whether the classifier stops at the changed value or at the fingerprint,
    the answer is the same: blocked, and no write of any kind.
    """
    stored = legacy_ledger(record())
    decision = classify(record(**change), ledger=stored)

    assert decision.outcome == BLOCKED
    assert decision.reason is not None
    if expected_reason is not None:
        assert decision.reason == expected_reason
    assert decision.outcome != ALREADY_MIGRATED


def test_a_legacy_row_whose_customer_changed_stays_fail_closed():
    stored = legacy_ledger(record())
    moved = record(client={"phone": "+4915100000000"})

    decision = classify_record(
        moved,
        company_id=KARLSRUHE_COMPANY_ID,
        manifest=manifest(),
        directory=directory_with(),
        cutover=parse_cutover(CUTOVER),
        ledger=stored,
    )

    assert decision.outcome == BLOCKED
    assert decision.reason != ALREADY_MIGRATED


# ---------------------------------------------------------------------------
# Quantity is not forgiven by the old format
# ---------------------------------------------------------------------------


def test_a_legacy_row_whose_quantity_grew_is_blocked():
    """The sharpest case: the legacy hash cannot see `amount` at all.

    The old code never read the quantity, so `amount=2` recomputes to exactly
    the same legacy hash as the single unit that was migrated. Accepting the
    legacy format for a booking that is now two units would therefore report a
    doubled appointment as unchanged — which is why the legacy route demands an
    exact `1` before it is offered at all.
    """
    stored = legacy_ledger(record())
    doubled = record(services=[{"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "amount": 2}])

    decision = classify(doubled, ledger=stored)

    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_SERVICE_QUANTITY


@pytest.mark.parametrize(
    "amount",
    [2, 0, -1, 1.5, True, "1", None],
    ids=["two", "zero", "negative", "fractional", "boolean", "string", "null"],
)
def test_no_unproven_quantity_is_accepted_for_a_legacy_row(amount: Any):
    stored = legacy_ledger(record())
    payload = record(services=[{"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "amount": amount}])

    decision = classify(payload, ledger=stored)

    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_SERVICE_QUANTITY


def test_a_missing_quantity_is_not_read_as_one_even_for_a_legacy_row():
    stored = legacy_ledger(record())
    payload = record(services=[{"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0}])

    decision = classify(payload, ledger=stored)

    assert decision.outcome == BLOCKED
    assert decision.reason == BLOCK_SERVICE_QUANTITY


# ---------------------------------------------------------------------------
# What a new row gets
# ---------------------------------------------------------------------------


def test_a_new_decision_carries_only_the_current_fingerprint():
    decision = classify(record())

    assert decision.outcome == READY
    assert decision.source_fingerprint is not None
    assert decision.source_fingerprint != legacy_of(record())


def test_changing_the_quantity_changes_the_current_fingerprint():
    """Quantity is part of the identity, so a plan cannot survive it changing."""
    one = classify(record()).source_fingerprint
    # Two units never classify as ready, so the fingerprint is compared through
    # the binding the classifier would have built.
    from altegio_bot.easyweek_migration.bindings import ServiceBinding
    from altegio_bot.easyweek_migration.classify import source_fingerprint

    decision = classify(record())
    assert decision.starts_at_utc is not None
    assert decision.easyweek_staff_uuid is not None
    assert decision.easyweek_customer_uuid is not None
    assert decision.duration_minutes is not None
    doubled: tuple[ServiceBinding, ...] = (
        ServiceBinding(
            **{
                **{
                    field: getattr(decision.bindings[0], field)
                    for field in (
                        "altegio_service_id",
                        "easyweek_service_uuid",
                        "normalized_name",
                        "currency",
                        "catalog_price_minor",
                        "catalog_duration_minutes",
                        "staffer_uuid",
                    )
                },
                "source_amount": 2,
            }
        ),
    )
    two = source_fingerprint(
        company_id=decision.source_company_id,
        record_id=decision.source_record_id,
        starts_at_utc=decision.starts_at_utc,
        staff_uuid=decision.easyweek_staff_uuid,
        customer_uuid=decision.easyweek_customer_uuid,
        mutation_kind=decision.mutation_kind,
        bindings=doubled,
        booked_duration_minutes=decision.duration_minutes,
    )
    assert one != two


def test_no_reason_code_on_this_path_carries_pii():
    stored = legacy_ledger(record())
    blocked = classify(record(staff_id=5002), ledger=stored)
    blob = repr(blocked.as_safe_dict())

    assert CUSTOMER_PHONE not in blob
    assert "Testkundin" not in blob


# ---------------------------------------------------------------------------
# The legacy hash is about the SOURCE slot, not the catalogue total
# ---------------------------------------------------------------------------


def _normalizing_manifest():
    """The manifest with the owner-approved duration policy for this master."""
    payload = json.loads(manifest_text())
    payload["branches"][str(KARLSRUHE_COMPANY_ID)]["normalize_duration_to_catalog_for_staff_ids"] = [KA_STAFF_ID]
    parsed = parse_manifest(json.dumps(payload))
    assert parsed.valid, parsed.reason
    return parsed


def _classify_with(payload: dict[str, Any], manifest_obj):
    return classify_record(
        payload,
        company_id=KARLSRUHE_COMPANY_ID,
        manifest=manifest_obj,
        directory=directory_with(),
        cutover=parse_cutover(CUTOVER),
        ledger=None,
    )


def test_a_stretched_booking_never_matches_its_legacy_hash_under_normalization():
    """The regression: two paths must not disagree about the same booking.

    `classify_record` recomputes the legacy hash from the source slot, while
    `fingerprint_matches_decision` used the catalogue total. With the
    staff-scoped duration policy those differ, so a booking stretched from 60 to
    90 minutes read as CHANGED in a dry-run and as UNCHANGED in every path that
    asks the second question — rollback, resolve-created, the previous-wave
    context and the final reconciliation. A confirmed rollback would then cancel
    the EasyWeek appointment of a booking somebody had deliberately edited.
    """
    normalizing = _normalizing_manifest()
    unchanged = _classify_with(record(), normalizing)
    legacy_60 = legacy_source_fingerprint(
        company_id=KARLSRUHE_COMPANY_ID,
        record_id=900001,
        starts_at_utc=unchanged.starts_at_utc,
        staff_uuid=KA_STAFF_UUID,
        service_uuid=KA_SERVICE_UUID,
        duration_minutes=60,
        customer_uuid=CUSTOMER_UUID,
    )

    stretched = _classify_with(
        record(
            seance_length=5400,
            services=[{"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "seance_length": 5400, "amount": 1}],
        ),
        normalizing,
    )
    assert stretched.outcome == READY, "the policy still allows the booking to migrate"
    assert stretched.source_booked_duration_minutes == 90
    assert stretched.duration_minutes == 60

    assert fingerprint_matches_decision(legacy_60, unchanged) is True
    assert fingerprint_matches_decision(legacy_60, stretched) is False

    # And the classifier's own answer agrees, which is the point.
    blocked = classify_record(
        record(
            seance_length=5400,
            services=[{"id": KA_SERVICE_ID, "cost": 90.0, "cost_to_pay": 90.0, "seance_length": 5400, "amount": 1}],
        ),
        company_id=KARLSRUHE_COMPANY_ID,
        manifest=normalizing,
        directory=directory_with(),
        cutover=parse_cutover(CUTOVER),
        ledger=LedgerView(status="created", target_booking_uuid=TARGET_UUID, source_fingerprint=legacy_60),
    )
    assert blocked.outcome == BLOCKED
    assert blocked.reason == BLOCK_SOURCE_CHANGED


def test_an_unchanged_normalized_booking_is_still_recognised():
    """The compatibility this must not break while fixing the disagreement."""
    normalizing = _normalizing_manifest()
    decision = _classify_with(record(), normalizing)
    legacy = legacy_source_fingerprint(
        company_id=KARLSRUHE_COMPANY_ID,
        record_id=900001,
        starts_at_utc=decision.starts_at_utc,
        staff_uuid=KA_STAFF_UUID,
        service_uuid=KA_SERVICE_UUID,
        duration_minutes=60,
        customer_uuid=CUSTOMER_UUID,
    )
    again = classify_record(
        record(),
        company_id=KARLSRUHE_COMPANY_ID,
        manifest=normalizing,
        directory=directory_with(),
        cutover=parse_cutover(CUTOVER),
        ledger=LedgerView(status="created", target_booking_uuid=TARGET_UUID, source_fingerprint=legacy),
    )
    assert again.outcome == ALREADY_MIGRATED
